use crate::{
    stats::{DetailType, Direction, StatType, Stats},
    transport::MessageSender,
};
use rsnano_core::{utils::FairQueue, Block, BlockHash, Frontier};
use rsnano_ledger::Ledger;
use rsnano_messages::{
    AccountInfoAckPayload, AccountInfoReqPayload, AscPullAck, AscPullAckType, AscPullReq,
    AscPullReqType, BlocksAckPayload, BlocksReqPayload, FrontiersReqPayload, HashType, Message,
};
use rsnano_network::{Channel, ChannelId, DeadChannelCleanupStep, TrafficType};
use rsnano_store_lmdb::{LmdbReadTransaction, Transaction};
use std::{
    cmp::min,
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, MutexGuard,
    },
    thread::JoinHandle,
};

#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapServerConfig {
    pub max_queue: usize,
    pub threads: usize,
    pub batch_size: usize,
}

impl Default for BootstrapServerConfig {
    fn default() -> Self {
        Self {
            max_queue: 16,
            threads: 1,
            batch_size: 64,
        }
    }
}

/**
 * Processes bootstrap requests (`asc_pull_req` messages) and replies with bootstrap responses (`asc_pull_ack`)
 */
pub struct BootstrapServer {
    config: BootstrapServerConfig,
    stats: Arc<Stats>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) server_impl: Arc<BootstrapServerImpl>,
}

impl BootstrapServer {
    /** Maximum number of blocks to send in a single response, cannot be higher than capacity of a single `asc_pull_ack` message */
    pub const MAX_BLOCKS: usize = BlocksAckPayload::MAX_BLOCKS;
    pub const MAX_FRONTIERS: usize = AscPullAck::MAX_FRONTIERS;

    pub(crate) fn new(
        config: BootstrapServerConfig,
        stats: Arc<Stats>,
        ledger: Arc<Ledger>,
        message_sender: MessageSender,
    ) -> Self {
        let max_queue = config.max_queue;
        let server_impl = Arc::new(BootstrapServerImpl {
            stats: Arc::clone(&stats),
            ledger,
            batch_size: config.batch_size,
            on_response: Arc::new(Mutex::new(None)),
            condition: Condvar::new(),
            stopped: AtomicBool::new(false),
            queue: Mutex::new(FairQueue::new(move |_| max_queue, |_| 1)),
            message_sender: Mutex::new(message_sender),
        });

        Self {
            config,
            stats: Arc::clone(&stats),
            threads: Mutex::new(Vec::new()),
            server_impl,
        }
    }

    pub fn start(&self) {
        debug_assert!(self.threads.lock().unwrap().is_empty());

        let mut threads = self.threads.lock().unwrap();
        for _ in 0..self.config.threads {
            let server_impl = Arc::clone(&self.server_impl);
            threads.push(
                std::thread::Builder::new()
                    .name("Bootstrap serv".to_string())
                    .spawn(move || {
                        server_impl.run();
                    })
                    .unwrap(),
            );
        }
    }

    pub fn stop(&self) {
        self.server_impl.stopped.store(true, Ordering::SeqCst);
        self.server_impl.condition.notify_all();

        let mut threads = self.threads.lock().unwrap();
        for thread in threads.drain(..) {
            thread.join().unwrap();
        }
    }

    pub fn set_response_callback(&self, cb: Box<dyn Fn(&AscPullAck, ChannelId) + Send + Sync>) {
        *self.server_impl.on_response.lock().unwrap() = Some(cb);
    }

    pub fn request(&self, message: AscPullReq, channel: Arc<Channel>) -> bool {
        if !self.verify(&message) {
            self.stats
                .inc(StatType::BootstrapServer, DetailType::Invalid);
            return false;
        }

        // If channel is full our response will be dropped anyway, so filter that early
        if channel.should_drop(TrafficType::BootstrapServer) {
            self.stats.inc_dir(
                StatType::BootstrapServer,
                DetailType::ChannelFull,
                Direction::In,
            );
            return false;
        }

        let req_type = DetailType::from(&message.req_type);
        let added = {
            let mut guard = self.server_impl.queue.lock().unwrap();
            guard.push(channel.channel_id(), (message, channel.clone()))
        };

        if added {
            self.stats
                .inc(StatType::BootstrapServer, DetailType::Request);
            self.stats.inc(StatType::BootstrapServerRequest, req_type);

            self.server_impl.condition.notify_one();
        } else {
            self.stats
                .inc(StatType::BootstrapServer, DetailType::Overfill);
            self.stats.inc(StatType::BootstrapServerOverfill, req_type);
        }

        added
    }

    fn verify(&self, message: &AscPullReq) -> bool {
        match &message.req_type {
            AscPullReqType::Blocks(i) => i.count > 0 && i.count as usize <= Self::MAX_BLOCKS,
            AscPullReqType::AccountInfo(i) => !i.target.is_zero(),
            AscPullReqType::Frontiers(i) => i.count > 0 && i.count as usize <= Self::MAX_FRONTIERS,
        }
    }
}

impl Drop for BootstrapServer {
    fn drop(&mut self) {
        debug_assert!(self.threads.lock().unwrap().is_empty());
    }
}

pub(crate) struct BootstrapServerImpl {
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    on_response: Arc<Mutex<Option<Box<dyn Fn(&AscPullAck, ChannelId) + Send + Sync>>>>,
    stopped: AtomicBool,
    condition: Condvar,
    queue: Mutex<FairQueue<ChannelId, (AscPullReq, Arc<Channel>)>>,
    batch_size: usize,
    message_sender: Mutex<MessageSender>,
}

impl BootstrapServerImpl {
    fn run(&self) {
        let mut queue = self.queue.lock().unwrap();
        while !self.stopped.load(Ordering::SeqCst) {
            if !queue.is_empty() {
                self.stats.inc(StatType::BootstrapServer, DetailType::Loop);
                queue = self.run_batch(queue);
            } else {
                queue = self
                    .condition
                    .wait_while(queue, |q| {
                        q.is_empty() && !self.stopped.load(Ordering::SeqCst)
                    })
                    .unwrap();
            }
        }
    }

    fn run_batch<'a>(
        &'a self,
        mut queue: MutexGuard<'a, FairQueue<ChannelId, (AscPullReq, Arc<Channel>)>>,
    ) -> MutexGuard<'a, FairQueue<ChannelId, (AscPullReq, Arc<Channel>)>> {
        let batch = queue.next_batch(self.batch_size);
        drop(queue);

        let mut tx = self.ledger.read_txn();
        for (_, (request, channel)) in batch {
            tx.refresh_if_needed();

            if !channel.should_drop(TrafficType::BootstrapServer) {
                let response = self.process(&tx, request);
                self.respond(response, channel.channel_id());
            } else {
                self.stats.inc_dir(
                    StatType::BootstrapServer,
                    DetailType::ChannelFull,
                    Direction::Out,
                );
            }
        }

        self.queue.lock().unwrap()
    }

    fn process(&self, tx: &LmdbReadTransaction, message: AscPullReq) -> AscPullAck {
        match message.req_type {
            AscPullReqType::Blocks(blocks) => self.process_blocks(tx, message.id, blocks),
            AscPullReqType::AccountInfo(account) => self.process_account(tx, message.id, account),
            AscPullReqType::Frontiers(frontiers) => {
                self.process_frontiers(tx, message.id, frontiers)
            }
        }
    }

    fn process_blocks(
        &self,
        tx: &LmdbReadTransaction,
        id: u64,
        request: BlocksReqPayload,
    ) -> AscPullAck {
        let count = min(request.count as usize, BootstrapServer::MAX_BLOCKS);

        match request.start_type {
            HashType::Account => {
                if let Some(info) = self.ledger.account_info(tx, &request.start.into()) {
                    // Start from open block if pulling by account
                    return self.prepare_response(tx, id, info.open_block, count);
                }
            }
            HashType::Block => {
                if self.ledger.any().block_exists(tx, &request.start.into()) {
                    return self.prepare_response(tx, id, request.start.into(), count);
                }
            }
        }

        // Neither block nor account found, send empty response to indicate that
        self.prepare_empty_blocks_response(id)
    }

    /*
     * Account info request
     */

    fn process_account(
        &self,
        tx: &LmdbReadTransaction,
        id: u64,
        request: AccountInfoReqPayload,
    ) -> AscPullAck {
        let target = match request.target_type {
            HashType::Account => request.target.into(),
            HashType::Block => {
                // Try to lookup account assuming target is block hash
                self.ledger
                    .any()
                    .block_account(tx, &request.target.into())
                    .unwrap_or_default()
            }
        };

        let mut response_payload = AccountInfoAckPayload {
            account: target,
            ..Default::default()
        };

        if let Some(account_info) = self.ledger.account_info(tx, &target) {
            response_payload.account_open = account_info.open_block;
            response_payload.account_head = account_info.head;
            response_payload.account_block_count = account_info.block_count;

            if let Some(conf_info) = self.ledger.store.confirmation_height.get(tx, &target) {
                response_payload.account_conf_frontier = conf_info.frontier;
                response_payload.account_conf_height = conf_info.height;
            }
        }
        // If account is missing the response payload will contain all 0 fields, except for the target
        //
        AscPullAck {
            id,
            pull_type: AscPullAckType::AccountInfo(response_payload),
        }
    }

    /*
     * Frontiers request
     */
    fn process_frontiers(
        &self,
        tx: &LmdbReadTransaction,
        id: u64,
        request: FrontiersReqPayload,
    ) -> AscPullAck {
        let frontiers = self
            .ledger
            .any()
            .accounts_range(tx, request.start..)
            .map(|(account, info)| Frontier::new(account, info.head))
            .take(request.count as usize)
            .collect();

        AscPullAck {
            id,
            pull_type: AscPullAckType::Frontiers(frontiers),
        }
    }

    fn prepare_response(
        &self,
        tx: &LmdbReadTransaction,
        id: u64,
        start_block: BlockHash,
        count: usize,
    ) -> AscPullAck {
        let blocks = self.prepare_blocks(tx, start_block, count);
        let response_payload = BlocksAckPayload::new(blocks);

        AscPullAck {
            id,
            pull_type: AscPullAckType::Blocks(response_payload),
        }
    }

    fn prepare_empty_blocks_response(&self, id: u64) -> AscPullAck {
        AscPullAck {
            id,
            pull_type: AscPullAckType::Blocks(BlocksAckPayload::new(VecDeque::new())),
        }
    }

    fn prepare_blocks(
        &self,
        tx: &LmdbReadTransaction,
        start_block: BlockHash,
        count: usize,
    ) -> VecDeque<Block> {
        let mut result = VecDeque::new();
        if !start_block.is_zero() {
            let mut current = self.ledger.any().get_block(tx, &start_block);
            while let Some(c) = current.take() {
                let successor = c.successor().unwrap_or_default();
                result.push_back(c.into());

                if result.len() == count {
                    break;
                }
                current = self.ledger.any().get_block(tx, &successor);
            }
        }
        result
    }

    fn respond(&self, response: AscPullAck, channel_id: ChannelId) {
        self.stats.inc_dir(
            StatType::BootstrapServer,
            DetailType::Response,
            Direction::Out,
        );
        self.stats.inc(
            StatType::BootstrapServerResponse,
            DetailType::from(&response.pull_type),
        );

        // Increase relevant stats depending on payload type
        match &response.pull_type {
            AscPullAckType::Blocks(blocks) => {
                self.stats.add_dir(
                    StatType::BootstrapServer,
                    DetailType::Blocks,
                    Direction::Out,
                    blocks.blocks().len() as u64,
                );
            }
            AscPullAckType::AccountInfo(_) => {
                self.stats.inc_dir(
                    StatType::BootstrapServer,
                    DetailType::AccountInfo,
                    Direction::Out,
                );
            }
            AscPullAckType::Frontiers(frontiers) => {
                self.stats.add_dir(
                    StatType::BootstrapServer,
                    DetailType::Frontiers,
                    Direction::Out,
                    frontiers.len() as u64,
                );
            }
        }

        {
            let callback = self.on_response.lock().unwrap();
            if let Some(cb) = &*callback {
                (cb)(&response, channel_id);
            }
        }

        let msg = Message::AscPullAck(response);
        self.message_sender.lock().unwrap().try_send(
            channel_id,
            &msg,
            TrafficType::BootstrapServer,
        );
    }
}

impl From<&AscPullAckType> for DetailType {
    fn from(value: &AscPullAckType) -> Self {
        match value {
            AscPullAckType::Blocks(_) => DetailType::Blocks,
            AscPullAckType::AccountInfo(_) => DetailType::AccountInfo,
            AscPullAckType::Frontiers(_) => DetailType::Frontiers,
        }
    }
}

impl From<&AscPullReqType> for DetailType {
    fn from(value: &AscPullReqType) -> Self {
        match value {
            AscPullReqType::Blocks(_) => DetailType::Blocks,
            AscPullReqType::AccountInfo(_) => DetailType::AccountInfo,
            AscPullReqType::Frontiers(_) => DetailType::Frontiers,
        }
    }
}

pub(crate) struct BootstrapServerCleanup(Arc<BootstrapServerImpl>);

impl BootstrapServerCleanup {
    pub fn new(server: Arc<BootstrapServerImpl>) -> Self {
        Self(server)
    }
}

impl DeadChannelCleanupStep for BootstrapServerCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut queue = self.0.queue.lock().unwrap();
        for channel_id in dead_channel_ids {
            queue.remove(channel_id);
        }
    }
}
