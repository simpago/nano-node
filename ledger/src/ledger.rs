use super::DependentBlocksFinder;
use crate::{
    block_cementer::BlockCementer,
    block_insertion::{BlockInserter, BlockValidatorFactory},
    ledger_set_confirmed::LedgerSetConfirmed,
    BlockRollbackPerformer, GenerateCacheFlags, LedgerConstants, LedgerSetAny, RepWeightCache,
    RepWeightsUpdater, RepresentativeBlockFinder, WriteGuard, WriteQueue,
};
use rsnano_core::{
    block_priority,
    utils::{ContainerInfo, UnixTimestamp},
    Account, AccountInfo, Amount, Block, BlockHash, BlockSubType, ConfirmationHeightInfo,
    DependentBlocks, DetailedBlock, Epoch, Link, PendingInfo, PendingKey, PublicKey, Root,
    SavedBlock,
};
use rsnano_store_lmdb::{
    ConfiguredAccountDatabaseBuilder, ConfiguredBlockDatabaseBuilder,
    ConfiguredConfirmationHeightDatabaseBuilder, ConfiguredPeersDatabaseBuilder,
    ConfiguredPendingDatabaseBuilder, ConfiguredPrunedDatabaseBuilder, LedgerCache,
    LmdbAccountStore, LmdbBlockStore, LmdbConfirmationHeightStore, LmdbEnv, LmdbFinalVoteStore,
    LmdbOnlineWeightStore, LmdbPeerStore, LmdbPendingStore, LmdbPrunedStore, LmdbReadTransaction,
    LmdbRepWeightStore, LmdbStore, LmdbVersionStore, LmdbWriteTransaction, Transaction,
};
use std::{
    collections::HashMap,
    net::SocketAddrV6,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

#[derive(PartialEq, Eq, Debug, Clone, Copy, FromPrimitive)]
#[repr(u8)]
pub enum BlockStatus {
    Progress,      // Hasn't been seen before, signed correctly
    BadSignature,  // Signature was bad, forged or transmission error
    Old,           // Already seen and was valid
    NegativeSpend, // Malicious attempt to spend a negative amount
    Fork,          // Malicious fork based on previous
    /// Source block doesn't exist, has already been received, or requires an account upgrade (epoch blocks)
    Unreceivable,
    /// Block marked as previous is unknown
    GapPrevious,
    /// Block marked as source is unknown
    GapSource,
    /// Block marked as pending blocks required for epoch open block are unknown
    GapEpochOpenPending,
    /// Block attempts to open the burn account
    OpenedBurnAccount,
    /// Balance and amount delta don't match
    BalanceMismatch,
    /// Representative is changed when it is not allowed
    RepresentativeMismatch,
    /// This block cannot follow the previous block
    BlockPosition,
    /// Insufficient work for this block, even though it passed the minimal validation
    InsufficientWork,
}

impl BlockStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlockStatus::Progress => "Progress",
            BlockStatus::BadSignature => "Bad signature",
            BlockStatus::Old => "Old",
            BlockStatus::NegativeSpend => "Negative spend",
            BlockStatus::Fork => "Fork",
            BlockStatus::Unreceivable => "Unreceivable",
            BlockStatus::GapPrevious => "Gap previous",
            BlockStatus::GapSource => "Gap source",
            BlockStatus::GapEpochOpenPending => "Gap epoch open pendign",
            BlockStatus::OpenedBurnAccount => "Opened burn account",
            BlockStatus::BalanceMismatch => "Balance mismatch",
            BlockStatus::RepresentativeMismatch => "Representative mismatch",
            BlockStatus::BlockPosition => "Block position",
            BlockStatus::InsufficientWork => "Insufficient work",
        }
    }
}

pub trait LedgerObserver: Send + Sync {
    fn blocks_cemented(&self, _cemented_count: u64) {}
    fn block_rolled_back(&self, _block_type: BlockSubType) {}
    fn block_rolled_back2(&self, _block: &Block, _is_epoch: bool) {}
    fn block_added(&self, _block: &Block, _is_epoch: bool) {}
    fn dependent_unconfirmed(&self) {}
}

pub struct NullLedgerObserver {}

impl NullLedgerObserver {
    pub fn new() -> Self {
        Self {}
    }
}

impl LedgerObserver for NullLedgerObserver {}

pub struct Ledger {
    pub store: Arc<LmdbStore>,
    pub rep_weights_updater: RepWeightsUpdater,
    pub rep_weights: Arc<RepWeightCache>,
    pub constants: LedgerConstants,
    pub observer: Arc<dyn LedgerObserver>,
    pruning: AtomicBool,
    pub write_queue: Arc<WriteQueue>,
}

pub struct NullLedgerBuilder {
    blocks: ConfiguredBlockDatabaseBuilder,
    accounts: ConfiguredAccountDatabaseBuilder,
    pending: ConfiguredPendingDatabaseBuilder,
    pruned: ConfiguredPrunedDatabaseBuilder,
    peers: ConfiguredPeersDatabaseBuilder,
    confirmation_height: ConfiguredConfirmationHeightDatabaseBuilder,
    min_rep_weight: Amount,
}

impl NullLedgerBuilder {
    fn new() -> Self {
        Self {
            blocks: ConfiguredBlockDatabaseBuilder::new(),
            accounts: ConfiguredAccountDatabaseBuilder::new(),
            pending: ConfiguredPendingDatabaseBuilder::new(),
            pruned: ConfiguredPrunedDatabaseBuilder::new(),
            peers: ConfiguredPeersDatabaseBuilder::new(),
            confirmation_height: ConfiguredConfirmationHeightDatabaseBuilder::new(),
            min_rep_weight: Amount::zero(),
        }
    }

    pub fn block(mut self, block: &SavedBlock) -> Self {
        self.blocks = self.blocks.block(block);
        self
    }

    pub fn blocks<'a>(mut self, blocks: impl IntoIterator<Item = &'a SavedBlock>) -> Self {
        for b in blocks.into_iter() {
            self.blocks = self.blocks.block(b);
        }
        self
    }

    pub fn peers(mut self, peers: impl IntoIterator<Item = (SocketAddrV6, SystemTime)>) -> Self {
        for (peer, time) in peers.into_iter() {
            self.peers = self.peers.peer(peer, time)
        }
        self
    }

    pub fn confirmation_height(mut self, account: &Account, info: &ConfirmationHeightInfo) -> Self {
        self.confirmation_height = self.confirmation_height.height(account, info);
        self
    }

    pub fn account_info(mut self, account: &Account, info: &AccountInfo) -> Self {
        self.accounts = self.accounts.account(account, info);
        self
    }

    pub fn pending(mut self, key: &PendingKey, info: &PendingInfo) -> Self {
        self.pending = self.pending.pending(key, info);
        self
    }

    pub fn pruned(mut self, hash: &BlockHash) -> Self {
        self.pruned = self.pruned.pruned(hash);
        self
    }

    pub fn finish(self) -> Ledger {
        let env = Arc::new(
            LmdbEnv::new_null_with()
                .configured_database(self.blocks.build())
                .configured_database(self.accounts.build())
                .configured_database(self.pending.build())
                .configured_database(self.pruned.build())
                .configured_database(self.confirmation_height.build())
                .configured_database(self.peers.build())
                .build(),
        );

        let store = LmdbStore {
            cache: Arc::new(LedgerCache::new()),
            env: env.clone(),
            account: Arc::new(LmdbAccountStore::new(env.clone()).unwrap()),
            block: Arc::new(LmdbBlockStore::new(env.clone()).unwrap()),
            confirmation_height: Arc::new(LmdbConfirmationHeightStore::new(env.clone()).unwrap()),
            final_vote: Arc::new(LmdbFinalVoteStore::new(env.clone()).unwrap()),
            online_weight: Arc::new(LmdbOnlineWeightStore::new(env.clone()).unwrap()),
            peer: Arc::new(LmdbPeerStore::new(env.clone()).unwrap()),
            pending: Arc::new(LmdbPendingStore::new(env.clone()).unwrap()),
            pruned: Arc::new(LmdbPrunedStore::new(env.clone()).unwrap()),
            rep_weight: Arc::new(LmdbRepWeightStore::new(env.clone()).unwrap()),
            version: Arc::new(LmdbVersionStore::new(env.clone()).unwrap()),
        };
        Ledger::new(
            Arc::new(store),
            LedgerConstants::unit_test(),
            self.min_rep_weight,
            Arc::new(RepWeightCache::new()),
        )
        .unwrap()
    }
}

impl Ledger {
    pub fn new_null() -> Self {
        Self::new(
            Arc::new(LmdbStore::new_null()),
            LedgerConstants::unit_test(),
            Amount::zero(),
            Arc::new(RepWeightCache::new()),
        )
        .unwrap()
    }

    pub fn new_null_builder() -> NullLedgerBuilder {
        NullLedgerBuilder::new()
    }

    pub fn new(
        store: Arc<LmdbStore>,
        constants: LedgerConstants,
        min_rep_weight: Amount,
        rep_weights: Arc<RepWeightCache>,
    ) -> anyhow::Result<Self> {
        let rep_weights_updater =
            RepWeightsUpdater::new(store.rep_weight.clone(), min_rep_weight, &rep_weights);

        let mut ledger = Self {
            rep_weights,
            rep_weights_updater,
            store,
            constants,
            observer: Arc::new(NullLedgerObserver::new()),
            pruning: AtomicBool::new(false),
            write_queue: Arc::new(WriteQueue::new()),
        };

        ledger.initialize(&GenerateCacheFlags::new())?;

        Ok(ledger)
    }

    pub fn set_observer(&mut self, observer: Arc<dyn LedgerObserver>) {
        self.observer = observer;
    }

    pub fn read_txn(&self) -> LmdbReadTransaction {
        self.store.tx_begin_read()
    }

    pub fn rw_txn(&self) -> LmdbWriteTransaction {
        self.store.tx_begin_write()
    }

    fn initialize(&mut self, generate_cache: &GenerateCacheFlags) -> anyhow::Result<()> {
        if self.store.account.iter(&self.read_txn()).next().is_none() {
            self.add_genesis_block(&mut self.rw_txn());
        }

        if generate_cache.reps || generate_cache.account_count || generate_cache.block_count {
            self.store.account.for_each_par(|iter| {
                let mut block_count = 0;
                let mut account_count = 0;
                let mut rep_weights: HashMap<PublicKey, Amount> = HashMap::new();
                for (_, info) in iter {
                    block_count += info.block_count;
                    account_count += 1;
                    if !info.balance.is_zero() {
                        let total = rep_weights.entry(info.representative).or_default();
                        *total += info.balance;
                    }
                }
                self.store
                    .cache
                    .block_count
                    .fetch_add(block_count, Ordering::SeqCst);
                self.store
                    .cache
                    .account_count
                    .fetch_add(account_count, Ordering::SeqCst);
                self.rep_weights_updater.copy_from(&rep_weights);
            });
        }

        if generate_cache.cemented_count {
            self.store.confirmation_height.for_each_par(|iter| {
                let mut cemented_count = 0;
                for (_, info) in iter {
                    cemented_count += info.height;
                }
                self.store
                    .cache
                    .cemented_count
                    .fetch_add(cemented_count, Ordering::SeqCst);
            });
        }

        let transaction = self.store.tx_begin_read();
        self.store
            .cache
            .pruned_count
            .fetch_add(self.store.pruned.count(&transaction), Ordering::SeqCst);

        Ok(())
    }

    fn add_genesis_block(&self, txn: &mut LmdbWriteTransaction) {
        let genesis_hash = self.constants.genesis_block.hash();
        let genesis_account = self.constants.genesis_account;
        self.store.block.put(txn, &self.constants.genesis_block);

        self.store.confirmation_height.put(
            txn,
            &genesis_account,
            &ConfirmationHeightInfo::new(1, genesis_hash),
        );

        self.store.account.put(
            txn,
            &genesis_account,
            &AccountInfo {
                head: genesis_hash,
                representative: genesis_account.into(),
                open_block: genesis_hash,
                balance: u128::MAX.into(),
                modified: UnixTimestamp::ZERO,
                block_count: 1,
                epoch: Epoch::Epoch0,
            },
        );
        self.store
            .rep_weight
            .put(txn, genesis_account.into(), Amount::MAX);
    }

    pub fn refresh_if_needed(
        &self,
        write_guard: WriteGuard,
        mut tx: LmdbWriteTransaction,
    ) -> (WriteGuard, LmdbWriteTransaction) {
        if tx.elapsed() > Duration::from_millis(500) {
            let writer = write_guard.writer;
            tx.commit();
            drop(write_guard);

            let write_guard = self.write_queue.wait(writer);
            tx.renew();
            (write_guard, tx)
        } else {
            (write_guard, tx)
        }
    }

    pub fn any(&self) -> LedgerSetAny {
        LedgerSetAny::new(&self.store)
    }

    pub fn confirmed(&self) -> LedgerSetConfirmed {
        LedgerSetConfirmed::new(&self.store)
    }

    pub fn pruning_enabled(&self) -> bool {
        self.pruning.load(Ordering::SeqCst)
    }

    pub fn enable_pruning(&self) {
        self.pruning.store(true, Ordering::SeqCst);
    }

    pub fn bootstrap_weight_max_blocks(&self) -> u64 {
        self.rep_weights.bootstrap_weight_max_blocks()
    }

    pub fn unconfirmed_exists(&self, tx: &dyn Transaction, hash: &BlockHash) -> bool {
        self.any().block_exists(tx, hash) && !self.confirmed().block_exists(tx, hash)
    }

    pub fn account_receivable(
        &self,
        txn: &dyn Transaction,
        account: &Account,
        only_confirmed: bool,
    ) -> Amount {
        let mut result = Amount::zero();

        for (key, info) in
            self.any()
                .account_receivable_upper_bound(txn, *account, BlockHash::zero())
        {
            if !only_confirmed
                || self
                    .confirmed()
                    .block_exists_or_pruned(txn, &key.send_block_hash)
            {
                result += info.amount;
            }
        }

        result
    }

    pub fn random_blocks(&self, tx: &dyn Transaction, count: usize) -> Vec<SavedBlock> {
        let mut result = Vec::with_capacity(count);
        let starting_hash = BlockHash::random();

        // It is more efficient to choose a random starting point and pick a few sequential blocks from there
        let mut it = self.store.block.iter_range(tx, starting_hash..);
        while result.len() < count {
            match it.next() {
                Some(block) => result.push(block),
                None => {
                    // Wrap around when reaching the end
                    it = self.store.block.iter_range(tx, BlockHash::zero()..);
                }
            }
        }

        result
    }

    /// Returns the cached vote weight for the given representative.
    /// If the weight is below the cache limit it returns 0.
    /// During bootstrap it returns the preconfigured bootstrap weights.
    pub fn weight(&self, rep: &PublicKey) -> Amount {
        self.rep_weights.weight(rep)
    }

    /// Returns the exact vote weight for the given representative by doing a database lookup
    pub fn weight_exact(&self, txn: &dyn Transaction, representative: PublicKey) -> Amount {
        self.store
            .rep_weight
            .get(txn, &representative)
            .unwrap_or_default()
    }

    /// Return latest root for account, account number if there are no blocks for this account
    pub fn latest_root(&self, txn: &dyn Transaction, account: &Account) -> Root {
        match self.account_info(txn, account) {
            Some(info) => info.head.into(),
            None => account.into(),
        }
    }

    pub fn version(&self, txn: &dyn Transaction, hash: &BlockHash) -> Epoch {
        self.any()
            .get_block(txn, hash)
            .map(|block| block.epoch())
            .unwrap_or(Epoch::Epoch0)
    }

    pub fn is_epoch_link(&self, link: &Link) -> bool {
        self.constants.epochs.is_epoch_link(link)
    }

    pub fn epoch_signer(&self, link: &Link) -> Option<Account> {
        self.constants.epochs.epoch_signer(link)
    }

    /// Given the block hash of a send block, find the associated receive block that receives that send.
    /// The send block hash is not checked in any way, it is assumed to be correct.
    /// Return the receive block on success and None on failure
    pub fn find_receive_block_by_send_hash(
        &self,
        txn: &dyn Transaction,
        destination: &Account,
        send_block_hash: &BlockHash,
    ) -> Option<SavedBlock> {
        // get the cemented frontier
        let info = self.store.confirmation_height.get(txn, destination)?;
        let mut possible_receive_block = self.any().get_block(txn, &info.frontier);

        // walk down the chain until the source field of a receive block matches the send block hash
        while let Some(current) = possible_receive_block {
            if current.is_receive() && Some(*send_block_hash) == current.source() {
                // we have a match
                return Some(current);
            }

            possible_receive_block = self.any().get_block(txn, &current.previous());
        }

        None
    }

    pub fn epoch_link(&self, epoch: Epoch) -> Option<Link> {
        self.constants.epochs.link(epoch).cloned()
    }

    pub fn update_account(
        &self,
        txn: &mut LmdbWriteTransaction,
        account: &Account,
        old_info: &AccountInfo,
        new_info: &AccountInfo,
    ) {
        if !new_info.head.is_zero() {
            if old_info.head.is_zero() && new_info.open_block == new_info.head {
                self.store
                    .cache
                    .account_count
                    .fetch_add(1, Ordering::SeqCst);
            }
            if !old_info.head.is_zero() && old_info.epoch != new_info.epoch {
                // store.account ().put won't erase existing entries if they're in different tables
                self.store.account.del(txn, account);
            }
            self.store.account.put(txn, account, new_info);
        } else {
            debug_assert!(!self.store.confirmation_height.exists(txn, account));
            self.store.account.del(txn, account);
            debug_assert!(self.store.cache.account_count.load(Ordering::SeqCst) > 0);
            self.store
                .cache
                .account_count
                .fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn pruning_action(
        &self,
        txn: &mut LmdbWriteTransaction,
        hash: &BlockHash,
        batch_size: u64,
    ) -> u64 {
        let mut pruned_count = 0;
        let mut hash = *hash;
        let genesis_hash = self.constants.genesis_block.hash();

        while !hash.is_zero() && hash != genesis_hash {
            if let Some(block) = self.any().get_block(txn, &hash) {
                assert!(self.confirmed().block_exists_or_pruned(txn, &hash));
                self.store.block.del(txn, &hash);
                self.store.pruned.put(txn, &hash);
                hash = block.previous();
                pruned_count += 1;
                self.store.cache.pruned_count.fetch_add(1, Ordering::SeqCst);
                if pruned_count % batch_size == 0 {
                    txn.commit();
                    txn.renew();
                }
            } else if self.store.pruned.exists(txn, &hash) {
                hash = BlockHash::zero();
            } else {
                panic!("Error finding block for pruning");
            }
        }

        pruned_count
    }

    pub fn dependents_confirmed(&self, txn: &dyn Transaction, block: &SavedBlock) -> bool {
        self.dependent_blocks(txn, block)
            .iter()
            .all(|hash| self.confirmed().block_exists_or_pruned(txn, hash))
    }

    pub fn dependent_blocks(&self, txn: &dyn Transaction, block: &SavedBlock) -> DependentBlocks {
        DependentBlocksFinder::new(self, txn).find_dependent_blocks(block)
    }

    pub fn dependents_confirmed_for_unsaved_block(
        &self,
        txn: &dyn Transaction,
        block: &Block,
    ) -> bool {
        self.dependent_blocks_for_unsaved_block(txn, block)
            .iter()
            .all(|hash| self.confirmed().block_exists_or_pruned(txn, hash))
    }

    fn dependent_blocks_for_unsaved_block(
        &self,
        txn: &dyn Transaction,
        block: &Block,
    ) -> DependentBlocks {
        DependentBlocksFinder::new(self, txn).find_dependent_blocks_for_unsaved_block(block)
    }

    ///
    /// Rollback blocks until `block' doesn't exist or it tries to penetrate the confirmation height
    pub fn rollback(
        &self,
        txn: &mut LmdbWriteTransaction,
        block: &BlockHash,
    ) -> Result<Vec<SavedBlock>, (anyhow::Error, Vec<SavedBlock>)> {
        let mut performer = BlockRollbackPerformer::new(self, txn);
        match performer.roll_back(block) {
            Ok(()) => Ok(performer.rolled_back),
            Err(e) => Err((e, performer.rolled_back)),
        }
    }

    /// Returns the latest block with representative information
    pub fn representative_block_hash(&self, txn: &dyn Transaction, hash: &BlockHash) -> BlockHash {
        let hash = RepresentativeBlockFinder::new(txn, self.store.as_ref()).find_rep_block(*hash);
        debug_assert!(hash.is_zero() || self.store.block.exists(txn, &hash));
        hash
    }

    pub fn process(
        &self,
        txn: &mut LmdbWriteTransaction,
        block: &Block,
    ) -> Result<SavedBlock, BlockStatus> {
        let validator = BlockValidatorFactory::new(self, txn, block).create_validator();
        let instructions = validator.validate()?;
        let inserted = BlockInserter::new(self, txn, block, &instructions).insert();
        Ok(inserted)
    }

    pub fn get_block(&self, txn: &dyn Transaction, hash: &BlockHash) -> Option<SavedBlock> {
        self.store.block.get(txn, hash)
    }

    pub fn account_info(
        &self,
        transaction: &dyn Transaction,
        account: &Account,
    ) -> Option<AccountInfo> {
        self.store.account.get(transaction, account)
    }

    pub fn get_confirmation_height(
        &self,
        txn: &dyn Transaction,
        account: &Account,
    ) -> Option<ConfirmationHeightInfo> {
        self.store.confirmation_height.get(txn, account)
    }

    pub fn confirm(&self, txn: &mut LmdbWriteTransaction, hash: BlockHash) -> Vec<SavedBlock> {
        self.confirm_max(txn, hash, 1024 * 128)
    }

    /// Both stack and result set are bounded to limit maximum memory usage
    /// Callers must ensure that the target block was confirmed, and if not, call this function multiple times
    pub fn confirm_max(
        &self,
        txn: &mut LmdbWriteTransaction,
        target_hash: BlockHash,
        max_blocks: usize,
    ) -> Vec<SavedBlock> {
        BlockCementer::new(&self.store, self.observer.as_ref(), &self.constants).confirm(
            txn,
            target_hash,
            max_blocks,
        )
    }

    /// Returned priority balance is maximum of block balance and previous block balance
    /// to handle full account balance send cases.
    /// Returned timestamp is the previous block timestamp or the current timestamp
    /// if there's no previous block.
    pub fn block_priority(
        &self,
        tx: &dyn Transaction,
        block: &SavedBlock,
    ) -> (Amount, UnixTimestamp) {
        let previous_block = self.previous_block(tx, block);
        block_priority(block, previous_block.as_ref())
    }

    pub fn previous_block(&self, tx: &dyn Transaction, block: &SavedBlock) -> Option<SavedBlock> {
        if block.previous().is_zero() {
            None
        } else {
            self.any().get_block(tx, &block.previous())
        }
    }

    pub fn detailed_block(&self, tx: &dyn Transaction, hash: &BlockHash) -> Option<DetailedBlock> {
        let block = self.any().get_block(tx, hash)?;
        let amount = self.any().block_amount_for(tx, &block);
        let confirmed = self.confirmed().block_exists_or_pruned(tx, hash);
        Some(DetailedBlock {
            block,
            amount,
            confirmed,
        })
    }

    pub fn cemented_count(&self) -> u64 {
        self.store.cache.cemented_count.load(Ordering::SeqCst)
    }

    pub fn block_count(&self) -> u64 {
        self.store.cache.block_count.load(Ordering::SeqCst)
    }

    pub fn account_count(&self) -> u64 {
        self.store.cache.account_count.load(Ordering::SeqCst)
    }

    pub fn pruned_count(&self) -> u64 {
        self.store.cache.pruned_count.load(Ordering::SeqCst)
    }

    pub fn backlog_count(&self) -> u64 {
        let blocks = self.block_count();
        let cemented = self.cemented_count();
        if blocks > cemented {
            blocks - cemented
        } else {
            0
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .node("rep_weights", self.rep_weights.container_info())
            .finish()
    }
}
