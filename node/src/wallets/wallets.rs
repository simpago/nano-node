use super::{Wallet, WalletActionThread, WalletRepresentatives};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::{ConfirmingSet, ConfirmingSetConfig},
    config::{NetworkConstants, NetworkParams, NodeConfig},
    representatives::OnlineReps,
    stats::Stats,
    transport::MessageFlooder,
    utils::{ThreadPool, ThreadPoolImpl},
    work::DistributedWorkFactory,
};
use rand::{thread_rng, Rng};
use rsnano_core::{
    utils::{get_env_or_default_string, ContainerInfo},
    work::{WorkPoolImpl, WorkThresholds},
    Account, Amount, Block, BlockDetails, BlockHash, Epoch, KeyDerivationFunction, Link, Networks,
    PendingKey, PrivateKey, PublicKey, RawKey, Root, SavedBlock, StateBlockArgs, WalletId,
};
use rsnano_ledger::{Ledger, RepWeightCache};
use rsnano_messages::{Message, Publish};
use rsnano_nullable_lmdb::{DatabaseFlags, LmdbDatabase, WriteFlags};
use rsnano_store_lmdb::{
    create_backup_file, KeyType, LmdbEnv, LmdbIterator, LmdbWalletStore, LmdbWriteTransaction,
    Transaction,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::Permissions,
    mem::size_of,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use tracing::{info, warn};

#[derive(FromPrimitive, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalletsError {
    None,
    Generic,
    WalletNotFound,
    WalletLocked,
    AccountNotFound,
    InvalidPassword,
    BadPublicKey,
}

impl WalletsError {
    pub fn as_str(&self) -> &'static str {
        match self {
            WalletsError::None => "No error",
            WalletsError::Generic => "Unknown error",
            WalletsError::WalletNotFound => "Wallet not found",
            WalletsError::WalletLocked => "Wallet is locked",
            WalletsError::AccountNotFound => "Account not found",
            WalletsError::InvalidPassword => "Invalid password",
            WalletsError::BadPublicKey => "Bad public key",
        }
    }
}

impl fmt::Display for WalletsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::error::Error for WalletsError {}

pub enum PreparedSend {
    Cached(SavedBlock),
    New(Block, BlockDetails),
}

pub struct Wallets {
    db: Option<LmdbDatabase>,
    send_action_ids_handle: Option<LmdbDatabase>,
    env: Arc<LmdbEnv>,
    pub mutex: Mutex<HashMap<WalletId, Arc<Wallet>>>,
    node_config: NodeConfig,
    ledger: Arc<Ledger>,
    last_log: Mutex<Option<Instant>>,
    distributed_work: Arc<DistributedWorkFactory>,
    work_thresholds: WorkThresholds,
    network_params: NetworkParams,
    pub delayed_work: Mutex<HashMap<Account, Root>>,
    workers: Arc<dyn ThreadPool>,
    wallet_actions: WalletActionThread,
    block_processor: Arc<BlockProcessor>,
    pub wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    online_reps: Arc<Mutex<OnlineReps>>,
    pub kdf: KeyDerivationFunction,
    start_election: Mutex<Option<Box<dyn Fn(SavedBlock) + Send + Sync>>>,
    confirming_set: Arc<ConfirmingSet>,
    message_flooder: Mutex<MessageFlooder>,
}

impl Wallets {
    pub fn new_null_with_env(env: Arc<LmdbEnv>, tokio_handle: tokio::runtime::Handle) -> Self {
        Wallets::new(
            env,
            Arc::new(Ledger::new_null()),
            &NodeConfig::new_test_instance(),
            WorkThresholds::new(0, 0, 0),
            Arc::new(DistributedWorkFactory::new(
                Arc::new(WorkPoolImpl::disabled()),
                tokio_handle.clone(),
            )),
            NetworkParams::new(NetworkConstants::active_network()),
            Arc::new(ThreadPoolImpl::new_null()),
            Arc::new(BlockProcessor::new_null()),
            Arc::new(Mutex::new(OnlineReps::new(
                Arc::new(RepWeightCache::new()),
                Duration::default(),
                Amount::zero(),
                Amount::zero(),
            ))),
            Arc::new(ConfirmingSet::new(
                ConfirmingSetConfig::default(),
                Arc::new(Ledger::new_null()),
                Arc::new(Stats::default()),
            )),
            MessageFlooder::new_null(),
            Networks::NanoDevNetwork,
        )
    }

    pub fn new(
        env: Arc<LmdbEnv>,
        ledger: Arc<Ledger>,
        node_config: &NodeConfig,
        work: WorkThresholds,
        distributed_work: Arc<DistributedWorkFactory>,
        network_params: NetworkParams,
        workers: Arc<dyn ThreadPool>,
        block_processor: Arc<BlockProcessor>,
        online_reps: Arc<Mutex<OnlineReps>>,
        confirming_set: Arc<ConfirmingSet>,
        message_flooder: MessageFlooder,
        current_network: Networks,
    ) -> Self {
        let kdf = KeyDerivationFunction::new(Self::kdf_work_for(current_network));

        Self {
            db: None,
            send_action_ids_handle: None,
            mutex: Mutex::new(HashMap::new()),
            env,
            node_config: node_config.clone(),
            ledger: Arc::clone(&ledger),
            last_log: Mutex::new(None),
            distributed_work,
            work_thresholds: work.clone(),
            network_params,
            delayed_work: Mutex::new(HashMap::new()),
            workers,
            wallet_actions: WalletActionThread::new(),
            block_processor,
            wallet_reps: Arc::new(Mutex::new(WalletRepresentatives::new(
                node_config.vote_minimum,
                ledger.rep_weights.clone(),
            ))),
            online_reps,
            kdf: kdf.clone(),
            start_election: Mutex::new(None),
            confirming_set,
            message_flooder: Mutex::new(message_flooder),
        }
    }

    fn kdf_work_for(network: Networks) -> u32 {
        match network {
            Networks::NanoDevNetwork => 8,
            _ => 1024 * 64,
        }
    }

    pub fn start(&self) {
        self.wallet_actions.start();
    }

    pub fn stop(&self) {
        self.wallet_actions.stop();
    }

    pub fn set_start_election_callback(&self, callback: Box<dyn Fn(SavedBlock) + Send + Sync>) {
        *self.start_election.lock().unwrap() = Some(callback);
    }

    pub fn initialize(&mut self) -> anyhow::Result<()> {
        let mut txn = self.env.tx_begin_write();
        self.db = Some(unsafe { txn.rw_txn_mut().create_db(None, DatabaseFlags::empty())? });
        self.send_action_ids_handle = Some(unsafe {
            txn.rw_txn_mut()
                .create_db(Some("send_action_ids"), DatabaseFlags::empty())?
        });
        {
            let mut guard = self.mutex.lock().unwrap();
            let wallet_ids = self.get_wallet_ids_with_tx(&txn);
            for id in wallet_ids {
                assert!(!guard.contains_key(&id));
                let representative = self.node_config.random_representative();
                let text = PathBuf::from(id.encode_hex());
                let wallet = Wallet::new(
                    self.ledger.clone(),
                    self.work_thresholds.clone(),
                    &mut txn,
                    self.node_config.password_fanout as usize,
                    self.kdf.clone(),
                    representative,
                    &text,
                )?;

                guard.insert(id, Arc::new(wallet));
            }

            // Backup before upgrade wallets
            let mut backup_required = false;
            if self.node_config.backup_before_upgrade {
                let txn = self.env.tx_begin_read();
                for wallet in guard.values() {
                    if wallet.store.version(&txn) != LmdbWalletStore::VERSION_CURRENT {
                        backup_required = true;
                        break;
                    }
                }
            }
            if backup_required {
                create_backup_file(&self.env)?;
            }
        }
        Ok(())
    }

    pub fn voting_reps_count(&self) -> u64 {
        self.wallet_reps.lock().unwrap().voting_reps()
    }

    fn iter_wallets<'tx>(&self, tx: &'tx dyn Transaction) -> impl Iterator<Item = WalletId> + 'tx {
        let cursor = tx
            .open_ro_cursor(self.db.unwrap())
            .expect("Could not read from wallets db");

        LmdbIterator::new(cursor, |k, _| {
            // wallet tables are identified by their wallet id hex string which is 64 bytes
            let key = if k.len() == 64 {
                WalletId::decode_hex(std::str::from_utf8(k).unwrap()).unwrap()
            } else {
                WalletId::zero()
            };
            (key, ())
        })
        .filter_map(|(k, _)| if k.is_zero() { None } else { Some(k) })
    }

    pub fn wallet_ids(&self) -> Vec<WalletId> {
        let tx = self.env.tx_begin_read();
        self.get_wallet_ids_with_tx(&tx)
    }

    pub fn get_wallet_ids(&self) -> Vec<WalletId> {
        let tx = self.env.tx_begin_read();
        self.iter_wallets(&tx).collect()
    }

    pub fn get_wallet_ids_with_tx(&self, tx: &dyn Transaction) -> Vec<WalletId> {
        self.iter_wallets(tx).collect()
    }

    pub fn get_block_hash(
        &self,
        txn: &dyn Transaction,
        id: &str,
    ) -> anyhow::Result<Option<BlockHash>> {
        match txn.get(self.send_action_ids_handle.unwrap(), id.as_bytes()) {
            Ok(bytes) => Ok(Some(
                BlockHash::from_slice(bytes).ok_or_else(|| anyhow!("invalid block hash"))?,
            )),
            Err(rsnano_nullable_lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_block_hash(
        &self,
        txn: &mut LmdbWriteTransaction,
        id: &str,
        hash: &BlockHash,
    ) -> anyhow::Result<()> {
        txn.rw_txn_mut().put(
            self.send_action_ids_handle.unwrap(),
            id.as_bytes(),
            hash.as_bytes(),
            WriteFlags::empty(),
        )?;
        Ok(())
    }

    pub fn clear_send_ids(&self) {
        let mut tx = self.env.tx_begin_write();
        tx.clear_db(self.send_action_ids_handle.unwrap()).unwrap();
    }

    pub fn foreach_representative<F>(&self, mut action: F)
    where
        F: FnMut(&PrivateKey),
    {
        if self.node_config.enable_voting {
            let mut action_accounts_l: Vec<PrivateKey> = Vec::new();
            {
                let transaction_l = self.env.tx_begin_read();
                let ledger_txn = self.ledger.read_txn();
                let lock = self.mutex.lock().unwrap();
                for (wallet_id, wallet) in lock.iter() {
                    let representatives_l = wallet.representatives.lock().unwrap().clone();
                    for account in representatives_l {
                        if wallet.store.exists(&transaction_l, &account.into()) {
                            if !self.ledger.weight_exact(&ledger_txn, account).is_zero() {
                                if wallet.store.valid_password(&transaction_l) {
                                    let prv = wallet
                                        .store
                                        .fetch(&transaction_l, &account.into())
                                        .expect("could not fetch account from wallet");

                                    action_accounts_l.push(prv.into());
                                } else {
                                    let mut last_log_guard = self.last_log.lock().unwrap();
                                    let should_log = match last_log_guard.as_ref() {
                                        Some(i) => i.elapsed() >= Duration::from_secs(60),
                                        None => true,
                                    };
                                    if should_log {
                                        *last_log_guard = Some(Instant::now());
                                        warn!("Representative locked inside wallet {}", wallet_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for keys in action_accounts_l {
                action(&keys);
            }
        }
    }

    pub fn work_cache_blocking2(
        &self,
        wallet_id: &WalletId,
        pub_key: &PublicKey,
        root: &Root,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        self.work_cache_blocking(wallet, pub_key, root);
        Ok(())
    }

    fn work_cache_blocking(&self, wallet: &Wallet, pub_key: &PublicKey, root: &Root) {
        if self.distributed_work.work_generation_enabled() {
            let difficulty = self.work_thresholds.threshold_base();
            if let Some(work) =
                self.distributed_work
                    .make_blocking(*root, difficulty, Some(pub_key.into()))
            {
                let mut tx = self.env.tx_begin_write();
                if wallet.live() && wallet.store.exists(&tx, pub_key) {
                    wallet.work_update(&mut tx, pub_key, root, work);
                }
            } else {
                warn!(
                    "Could not precache work for root {} due to work generation failure",
                    root
                );
            }
        }
    }

    fn get_wallet<'a>(
        guard: &'a HashMap<WalletId, Arc<Wallet>>,
        wallet_id: &WalletId,
    ) -> Result<&'a Arc<Wallet>, WalletsError> {
        guard.get(wallet_id).ok_or(WalletsError::WalletNotFound)
    }

    pub fn insert_watch(
        &self,
        wallet_id: &WalletId,
        accounts: &[Account],
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        for account in accounts {
            if wallet.store.insert_watch(&mut tx, &account.into()).is_err() {
                return Err(WalletsError::BadPublicKey);
            }
        }

        Ok(())
    }

    pub fn valid_password(&self, wallet_id: &WalletId) -> Result<bool, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_read();
        Ok(wallet.store.valid_password(&tx))
    }

    pub fn attempt_password(
        &self,
        wallet_id: &WalletId,
        password: impl AsRef<str>,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_write();
        if wallet.store.attempt_password(&tx, password.as_ref()) {
            Ok(())
        } else {
            Err(WalletsError::InvalidPassword)
        }
    }

    pub fn lock(&self, wallet_id: &WalletId) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        wallet.store.lock();
        Ok(())
    }

    pub fn rekey(
        &self,
        wallet_id: &WalletId,
        password: impl AsRef<str>,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        wallet
            .store
            .rekey(&mut tx, password.as_ref())
            .map_err(|_| WalletsError::Generic)
    }

    pub fn set_observer(&self, observer: Box<dyn Fn(bool) + Send>) {
        self.wallet_actions.set_observer(observer);
    }

    pub fn compute_reps(&self) {
        let wallets_guard = self.mutex.lock().unwrap();
        let mut reps_guard = self.wallet_reps.lock().unwrap();
        reps_guard.clear();
        let half_principal_weight = self.online_reps.lock().unwrap().minimum_principal_weight() / 2;
        let tx = self.env.tx_begin_read();
        for (_, wallet) in wallets_guard.iter() {
            let mut representatives = HashSet::new();

            for (pub_key, _) in wallet.store.iter(&tx) {
                if reps_guard.check_rep(pub_key, half_principal_weight) {
                    representatives.insert(pub_key.into());
                }
            }

            *wallet.representatives.lock().unwrap() = representatives;
        }
    }

    pub fn exists(&self, pub_key: &PublicKey) -> bool {
        let guard = self.mutex.lock().unwrap();
        let tx = self.env.tx_begin_read();
        guard
            .values()
            .any(|wallet| wallet.store.exists(&tx, pub_key))
    }

    pub fn reload(&self) {
        let mut guard = self.mutex.lock().unwrap();
        let mut tx = self.env.tx_begin_write();
        let mut stored_items = HashSet::new();
        let wallet_ids = self.get_wallet_ids_with_tx(&tx);
        for id in wallet_ids {
            // New wallet
            if !guard.contains_key(&id) {
                let text = PathBuf::from(id.encode_hex());
                let representative = self.node_config.random_representative();
                if let Ok(wallet) = Wallet::new(
                    Arc::clone(&self.ledger),
                    self.work_thresholds.clone(),
                    &mut tx,
                    self.node_config.password_fanout as usize,
                    self.kdf.clone(),
                    representative,
                    &text,
                ) {
                    guard.insert(id, Arc::new(wallet));
                }
            }
            // List of wallets on disk
            stored_items.insert(id);
        }
        // Delete non existing wallets from memory
        let mut deleted_items = Vec::new();
        for &id in guard.keys() {
            if !stored_items.contains(&id) {
                deleted_items.push(id);
            }
        }
        for i in &deleted_items {
            guard.remove(i);
        }
    }

    pub fn wallet_exists(&self, wallet_id: &WalletId) -> bool {
        self.mutex.lock().unwrap().contains_key(wallet_id)
    }

    pub fn destroy(&self, id: &WalletId) {
        let mut guard = self.mutex.lock().unwrap();
        let mut tx = self.env.tx_begin_write();
        // action_mutex should be locked after transactions to prevent deadlocks in deterministic_insert () & insert_adhoc ()
        let _action_guard = self.wallet_actions.lock_safe();
        let wallet = guard.remove(id).unwrap();
        wallet.store.destroy(&mut tx);
    }

    pub fn remove_key(
        &self,
        wallet_id: &WalletId,
        pub_key: &PublicKey,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        if wallet.store.find(&tx, pub_key).is_none() {
            return Err(WalletsError::AccountNotFound);
        }
        wallet.store.erase(&mut tx, pub_key);
        Ok(())
    }

    pub fn work_set(
        &self,
        wallet_id: &WalletId,
        pub_key: &PublicKey,
        work: u64,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if wallet.store.find(&tx, pub_key).is_none() {
            return Err(WalletsError::AccountNotFound);
        }
        wallet.store.work_put(&mut tx, pub_key, work);
        Ok(())
    }

    pub fn move_accounts(
        &self,
        source_id: &WalletId,
        target_id: &WalletId,
        accounts: &[PublicKey],
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let source = Self::get_wallet(&guard, source_id)?;
        let target = Self::get_wallet(&guard, target_id)?;
        let tx = self.env.tx_begin_read();
        if !source.store.valid_password(&tx) || !target.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        let mut tx = self.env.tx_begin_write();
        target
            .store
            .move_keys(&mut tx, &source.store, accounts)
            .map_err(|_| WalletsError::AccountNotFound)
    }

    pub fn backup(&self, path: &Path) -> anyhow::Result<()> {
        let guard = self.mutex.lock().unwrap();
        let tx = self.env.tx_begin_read();
        for (id, wallet) in guard.iter() {
            std::fs::create_dir_all(path)?;
            std::fs::set_permissions(path, Permissions::from_mode(0o700))?;
            let mut backup_path = PathBuf::from(path);
            backup_path.push(format!("{}.json", id));
            wallet.store.write_backup(&tx, &backup_path)?;
        }
        Ok(())
    }

    pub fn deterministic_index_get(&self, wallet_id: &WalletId) -> Result<u32, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_read();
        Ok(wallet.store.deterministic_index_get(&tx))
    }

    fn prepare_send(
        &self,
        tx: &dyn Transaction,
        wallet: &Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        mut work: u64,
    ) -> anyhow::Result<PreparedSend> {
        let block_tx = self.ledger.read_txn();
        if !wallet.store.valid_password(tx) {
            bail!("invalid password");
        }
        let balance = self
            .ledger
            .any()
            .account_balance(&block_tx, &source)
            .unwrap_or_default();

        if balance.is_zero() || balance < amount {
            bail!("insufficient balance");
        }

        let info = self.ledger.account_info(&block_tx, &source).unwrap();
        let prv_key_raw = wallet.store.fetch(tx, &source.into()).unwrap();
        if work == 0 {
            work = wallet
                .store
                .work_get(tx, &source.into())
                .unwrap_or_default();
        }
        let priv_key = PrivateKey::from(prv_key_raw);
        let state_block: Block = StateBlockArgs {
            key: &priv_key,
            previous: info.head,
            representative: info.representative,
            balance: balance - amount,
            link: account.into(),
            work,
        }
        .into();
        let details = BlockDetails::new(info.epoch, true, false, false);
        Ok(PreparedSend::New(state_block, details))
    }

    fn prepare_send_with_id(
        &self,
        tx: &mut LmdbWriteTransaction,
        id: &str,
        wallet: &Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        mut work: u64,
    ) -> anyhow::Result<PreparedSend> {
        let block_tx = self.ledger.read_txn();

        let block = match self.get_block_hash(tx, id)? {
            Some(hash) => Some(self.ledger.any().get_block(&block_tx, &hash).unwrap()),
            None => None,
        };

        if let Some(block) = block {
            let msg = Message::Publish(Publish::new_forward(block.clone().into()));
            self.message_flooder.lock().unwrap().flood(
                &msg,
                rsnano_network::TrafficType::BlockBroadcastInitial,
                1.0,
            );
            Ok(PreparedSend::Cached(block))
        } else {
            if !wallet.store.valid_password(tx) {
                bail!("invalid password");
            }

            let balance = self
                .ledger
                .any()
                .account_balance(&block_tx, &source)
                .unwrap_or_default();

            if balance.is_zero() || balance < amount {
                bail!("insufficient balance");
            }

            let info = self.ledger.account_info(&block_tx, &source).unwrap();
            let prv_key_raw = wallet.store.fetch(tx, &source.into()).unwrap();
            if work == 0 {
                work = wallet
                    .store
                    .work_get(tx, &source.into())
                    .unwrap_or_default();
            }
            let priv_key = PrivateKey::from(prv_key_raw);
            let state_block: Block = StateBlockArgs {
                key: &priv_key,
                previous: info.head,
                representative: info.representative,
                balance: balance - amount,
                link: account.into(),
                work,
            }
            .into();
            let details = BlockDetails::new(info.epoch, true, false, false);
            self.set_block_hash(tx, id, &state_block.hash())?;
            Ok(PreparedSend::New(state_block, details))
        }
    }

    pub fn work_get(&self, wallet_id: &WalletId, pub_key: &PublicKey) -> u64 {
        let guard = self.mutex.lock().unwrap();
        let tx = self.env.tx_begin_read();
        let Some(wallet) = guard.get(&wallet_id) else {
            return 1;
        };
        wallet.store.work_get(&tx, pub_key).unwrap_or(1)
    }

    pub fn work_get2(
        &self,
        wallet_id: &WalletId,
        pub_key: &PublicKey,
    ) -> Result<u64, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let tx = self.env.tx_begin_read();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        if wallet.store.find(&tx, pub_key).is_none() {
            return Err(WalletsError::AccountNotFound);
        }
        Ok(wallet.store.work_get(&tx, pub_key).unwrap_or(1))
    }

    pub fn get_accounts(&self, max_results: usize) -> Vec<Account> {
        let mut accounts = Vec::new();
        let guard = self.mutex.lock().unwrap();
        let tx = self.env.tx_begin_read();
        for wallet in guard.values() {
            for (pub_key, _) in wallet.store.iter(&tx) {
                if accounts.len() >= max_results {
                    break;
                }

                accounts.push(pub_key.into());
            }
        }
        accounts
    }

    pub fn get_accounts_of_wallet(
        &self,
        wallet_id: &WalletId,
    ) -> Result<Vec<Account>, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_read();
        let mut accounts = Vec::new();
        for (account, _) in wallet.store.iter(&tx) {
            accounts.push(account.into());
        }
        Ok(accounts)
    }

    pub fn fetch(&self, wallet_id: &WalletId, pub_key: &PublicKey) -> Result<RawKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_read();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        if wallet.store.find(&tx, pub_key).is_none() {
            return Err(WalletsError::AccountNotFound);
        }
        wallet
            .store
            .fetch(&tx, pub_key)
            .map_err(|_| WalletsError::Generic)
    }

    pub fn import(&self, wallet_id: WalletId, json: &str) -> anyhow::Result<()> {
        let _guard = self.mutex.lock().unwrap();
        let mut tx = self.env.tx_begin_write();
        let _wallet = Wallet::new_from_json(
            Arc::clone(&self.ledger),
            self.work_thresholds.clone(),
            &mut tx,
            self.node_config.password_fanout as usize,
            self.kdf.clone(),
            &PathBuf::from(wallet_id.to_string()),
            json,
        )?;
        Ok(())
    }

    pub fn import_replace(
        &self,
        wallet_id: WalletId,
        json: &str,
        password: &str,
    ) -> anyhow::Result<()> {
        let guard = self.mutex.lock().unwrap();
        let existing = guard
            .get(&wallet_id)
            .ok_or_else(|| anyhow!("wallet not found"))?;
        let mut tx = self.env.tx_begin_write();
        let id = WalletId::from_bytes(thread_rng().gen());
        let temp = LmdbWalletStore::new_from_json(
            1,
            self.kdf.clone(),
            &mut tx,
            &PathBuf::from(id.to_string()),
            json,
        )?;

        let result = if temp.attempt_password(&tx, password) {
            existing.store.import(&mut tx, &temp)
        } else {
            Err(anyhow!("bad password"))
        };
        temp.destroy(&mut tx);
        result
    }

    pub fn get_seed(&self, wallet_id: WalletId) -> Result<RawKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_read();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        Ok(wallet.store.seed(&tx))
    }

    pub fn key_type(&self, wallet_id: WalletId, pub_key: &PublicKey) -> KeyType {
        let guard = self.mutex.lock().unwrap();
        match guard.get(&wallet_id) {
            Some(wallet) => {
                let tx = self.env.tx_begin_read();
                wallet.store.get_key_type(&tx, pub_key)
            }
            None => KeyType::Unknown,
        }
    }

    pub fn get_representative(&self, wallet_id: WalletId) -> Result<PublicKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_read();
        Ok(wallet.store.representative(&tx))
    }

    pub fn decrypt(&self, wallet_id: WalletId) -> Result<Vec<(PublicKey, RawKey)>, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_read();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        let mut result = Vec::new();
        for (account, _) in wallet.store.iter(&tx) {
            let key = wallet
                .store
                .fetch(&tx, &account)
                .map_err(|_| WalletsError::Generic)?;
            result.push((account, key));
        }

        Ok(result)
    }

    pub fn serialize(&self, wallet_id: WalletId) -> Result<String, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Self::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_read();
        Ok(wallet.store.serialize_json(&tx))
    }

    pub fn container_info(&self) -> ContainerInfo {
        [
            (
                "items",
                self.mutex.lock().unwrap().len(),
                size_of::<usize>() * size_of::<WalletId>(),
            ),
            ("actions", self.wallet_actions.len(), size_of::<usize>() * 2),
        ]
        .into()
    }
}

impl Drop for Wallets {
    fn drop(&mut self) {
        self.stop();
    }
}

const GENERATE_PRIORITY: Amount = Amount::MAX;
const HIGH_PRIORITY: Amount = Amount::raw(u128::MAX - 1);

pub trait WalletsExt {
    fn deterministic_insert(
        &self,
        wallet: &Arc<Wallet>,
        tx: &mut LmdbWriteTransaction,
        generate_work: bool,
    ) -> PublicKey;

    fn deterministic_insert_at(
        &self,
        wallet_id: &WalletId,
        index: u32,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError>;

    fn deterministic_insert2(
        &self,
        wallet_id: &WalletId,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError>;

    fn insert_adhoc(&self, wallet: &Arc<Wallet>, key: &RawKey, generate_work: bool) -> PublicKey;

    fn insert_adhoc2(
        &self,
        wallet_id: &WalletId,
        key: &RawKey,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError>;

    fn work_ensure(&self, wallet: &Arc<Wallet>, account: Account, root: Root);

    fn action_complete(
        &self,
        wallet: Arc<Wallet>,
        block: Block,
        account: Account,
        generate_work: bool,
        details: &BlockDetails,
    ) -> anyhow::Result<SavedBlock>;

    fn ongoing_compute_reps(&self);

    fn change_seed(
        &self,
        wallet_id: WalletId,
        prv_key: &RawKey,
        count: u32,
    ) -> Result<(u32, Account), WalletsError>;

    fn change_seed_wallet(
        &self,
        wallet: &Arc<Wallet>,
        tx: &mut LmdbWriteTransaction,
        prv_key: &RawKey,
        count: u32,
    ) -> PublicKey;

    fn send_action(
        &self,
        wallet: &Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> anyhow::Result<SavedBlock>;

    fn send_action2(
        &self,
        wallet_id: &WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> Result<SavedBlock, WalletsError>;

    fn change_action(
        &self,
        wallet: &Arc<Wallet>,
        source: Account,
        representative: PublicKey,
        work: u64,
        generate_work: bool,
    ) -> Option<Block>;

    fn change_action2(
        &self,
        wallet_id: &WalletId,
        source: Account,
        representative: PublicKey,
        work: u64,
        generate_work: bool,
    ) -> Option<Block>;

    fn receive_action2(
        &self,
        wallet_id: &WalletId,
        send_hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        work: u64,
        generate_work: bool,
    ) -> Result<Option<SavedBlock>, WalletsError>;

    fn receive_action(
        &self,
        wallet: &Arc<Wallet>,
        send_hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        work: u64,
        generate_work: bool,
    ) -> Option<SavedBlock>;

    fn receive_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        action: Box<dyn Fn(Option<SavedBlock>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    );

    fn receive_async(
        &self,
        wallet_id: WalletId,
        hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        action: Box<dyn Fn(Option<SavedBlock>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) -> Result<(), WalletsError>;

    fn receive_sync(
        &self,
        wallet: Arc<Wallet>,
        block: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        work: u64,
        generate_work: bool,
    ) -> Result<SavedBlock, ()>;

    fn send_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    );

    fn send_async(
        &self,
        wallet_id: WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> Result<(), WalletsError>;

    fn send_sync(
        &self,
        wallet_id: WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> BlockHash;

    fn search_receivable(
        &self,
        wallet: &Arc<Wallet>,
        wallet_tx: &dyn Transaction,
    ) -> Result<(), ()>;

    fn receive_confirmed(&self, hash: BlockHash, destinaton: Account);
    fn search_receivable_all(&self);
    fn search_receivable_wallet(&self, wallet_id: WalletId) -> Result<(), WalletsError>;

    fn enter_password(&self, wallet_id: WalletId, password: &str) -> Result<(), WalletsError>;

    fn enter_password_wallet(
        &self,
        wallet: &Arc<Wallet>,
        wallet_tx: &dyn Transaction,
        password: &str,
    ) -> Result<(), ()>;

    fn enter_initial_password(&self, wallet: &Arc<Wallet>);
    fn create(&self, wallet_id: WalletId);
    fn change_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        representative: PublicKey,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    );

    fn change_sync_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        representative: PublicKey,
    ) -> Result<(), ()>;

    fn change_async(
        &self,
        wallet_id: WalletId,
        source: Account,
        representative: PublicKey,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) -> Result<(), WalletsError>;

    fn set_representative(
        &self,
        wallet_id: WalletId,
        rep: PublicKey,
        update_existing_accounts: bool,
    ) -> Result<(), WalletsError>;

    fn ensure_wallet_is_unlocked(&self, wallet_id: WalletId, password: &str) -> bool;

    fn initialize2(&self);
}

impl WalletsExt for Arc<Wallets> {
    fn receive_action2(
        &self,
        wallet_id: &WalletId,
        send_hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        work: u64,
        generate_work: bool,
    ) -> Result<Option<SavedBlock>, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, wallet_id)?;
        let tx = self.env.tx_begin_read();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        if wallet.store.find(&tx, &account.into()).is_none() {
            return Err(WalletsError::AccountNotFound);
        }

        Ok(self.receive_action(
            wallet,
            send_hash,
            representative,
            amount,
            account,
            work,
            generate_work,
        ))
    }

    fn deterministic_insert(
        &self,
        wallet: &Arc<Wallet>,
        tx: &mut LmdbWriteTransaction,
        generate_work: bool,
    ) -> PublicKey {
        if !wallet.store.valid_password(tx) {
            return PublicKey::zero();
        }
        let key = wallet.store.deterministic_insert(tx);

        info!(account=%key.as_account().encode_account(), "Deterministically inserted new account");

        if generate_work {
            self.work_ensure(wallet, key.into(), key.into());
        }
        let half_principal_weight = self.online_reps.lock().unwrap().minimum_principal_weight() / 2;
        let mut reps = self.wallet_reps.lock().unwrap();
        if reps.check_rep(key, half_principal_weight) {
            info!(account=%key.as_account().encode_account(), "New account qualified as representative");
            wallet.representatives.lock().unwrap().insert(key);
        }
        key
    }

    fn deterministic_insert_at(
        &self,
        wallet_id: &WalletId,
        index: u32,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        let account = wallet.store.deterministic_insert_at(&mut tx, index);

        info!(account=%account.as_account().encode_account(), "Deterministically inserted new account");

        if generate_work {
            self.work_ensure(wallet, account.into(), account.into());
        }
        Ok(account)
    }

    fn deterministic_insert2(
        &self,
        wallet_id: &WalletId,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        Ok(self.deterministic_insert(wallet, &mut tx, generate_work))
    }

    fn insert_adhoc(&self, wallet: &Arc<Wallet>, key: &RawKey, generate_work: bool) -> PublicKey {
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return PublicKey::zero();
        }
        let key = wallet.store.insert_adhoc(&mut tx, key);
        let block_tx = self.ledger.read_txn();
        if generate_work {
            self.work_ensure(
                wallet,
                key.into(),
                self.ledger.latest_root(&block_tx, &key.into()),
            );
        }
        let half_principal_weight = self.online_reps.lock().unwrap().minimum_principal_weight() / 2;
        // Makes sure that the representatives container will
        // be in sync with any added keys.
        tx.commit();
        let mut rep_guard = self.wallet_reps.lock().unwrap();
        if rep_guard.check_rep(key, half_principal_weight) {
            wallet.representatives.lock().unwrap().insert(key);
        }
        key
    }

    fn insert_adhoc2(
        &self,
        wallet_id: &WalletId,
        key: &RawKey,
        generate_work: bool,
    ) -> Result<PublicKey, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, wallet_id)?;
        let mut tx = self.env.tx_begin_read();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        tx.reset();
        Ok(self.insert_adhoc(wallet, key, generate_work))
    }

    fn work_ensure(&self, wallet: &Arc<Wallet>, account: Account, root: Root) {
        let precache_delay = if self.network_params.network.is_dev_network() {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(10)
        };
        self.delayed_work.lock().unwrap().insert(account, root);
        let self_clone = Arc::clone(self);
        let wallet = Arc::clone(wallet);
        self.workers.post_delayed(
            precache_delay,
            Box::new(move || {
                let mut guard = self_clone.delayed_work.lock().unwrap();
                if let Some(&existing) = guard.get(&account) {
                    if existing == root {
                        guard.remove(&account);
                        let self_clone_2 = Arc::clone(&self_clone);
                        self_clone.wallet_actions.queue_wallet_action(
                            GENERATE_PRIORITY,
                            wallet,
                            Box::new(move |w| {
                                self_clone_2.work_cache_blocking(&w, &account.into(), &root);
                            }),
                        );
                    }
                }
            }),
        );
    }

    fn action_complete(
        &self,
        wallet: Arc<Wallet>,
        mut block: Block,
        account: Account,
        generate_work: bool,
        details: &BlockDetails,
    ) -> anyhow::Result<SavedBlock> {
        // Unschedule any work caching for this account
        self.delayed_work.lock().unwrap().remove(&account);
        let hash = block.hash();
        let required_difficulty = self.network_params.work.threshold(details);
        if self.network_params.work.difficulty_block(&block) < required_difficulty {
            info!(
                "Cached or provided work for block {} account {} is invalid, regenerating...",
                block.hash(),
                account.encode_account()
            );
            self.distributed_work
                .make_blocking_block(&mut block, required_difficulty)
                .ok_or_else(|| anyhow!("no work generated"))?;
        }
        let arc_block = Arc::new(block.clone());
        let saved_block = self
            .block_processor
            .add_blocking(arc_block.clone(), BlockSource::Local)?
            .map_err(|s| anyhow!("block processor failed: {:?}", s))?;

        if generate_work {
            // Pregenerate work for next block based on the block just created
            self.work_ensure(&wallet, account, hash.into());
        }
        Ok(saved_block)
    }

    fn ongoing_compute_reps(&self) {
        self.compute_reps();

        // Representation drifts quickly on the test network but very slowly on the live network
        let compute_delay = if self.network_params.network.is_dev_network() {
            Duration::from_millis(10)
        } else if self.network_params.network.is_test_network() {
            test_scan_wallet_reps_delay()
        } else {
            Duration::from_secs(60 * 15)
        };

        let self_l = Arc::clone(self);
        self.workers.post_delayed(
            compute_delay,
            Box::new(move || {
                self_l.ongoing_compute_reps();
            }),
        );
    }

    fn change_seed_wallet(
        &self,
        wallet: &Arc<Wallet>,
        tx: &mut LmdbWriteTransaction,
        prv_key: &RawKey,
        mut count: u32,
    ) -> PublicKey {
        info!("Changing wallet seed");
        wallet.store.set_seed(tx, prv_key);
        let mut account = self.deterministic_insert(wallet, tx, true);
        if count == 0 {
            count = wallet.deterministic_check(tx, 0);
            info!("Auto-detected {} accounts to generate", count);
        }
        for _ in 0..count {
            // Disable work generation to prevent weak CPU nodes stuck
            account = self.deterministic_insert(wallet, tx, false);
        }
        info!("Completed changing wallet seed and generating accounts");
        account
    }

    fn change_seed(
        &self,
        wallet_id: WalletId,
        prv_key: &RawKey,
        count: u32,
    ) -> Result<(u32, Account), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        let mut tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        let first_account = self.change_seed_wallet(wallet, &mut tx, prv_key, count);
        let restored_count = wallet.store.deterministic_index_get(&tx);
        Ok((restored_count, first_account.into()))
    }

    fn send_action2(
        &self,
        wallet_id: &WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> Result<SavedBlock, WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        self.send_action(wallet, source, account, amount, work, generate_work, id)
            .map_err(|_| WalletsError::Generic)
    }

    fn send_action(
        &self,
        wallet: &Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> anyhow::Result<SavedBlock> {
        let result = match id {
            Some(id) => {
                let mut tx = self.env.tx_begin_write();
                self.prepare_send_with_id(&mut tx, &id, wallet, source, account, amount, work)?
            }
            None => {
                let tx = self.env.tx_begin_read();
                self.prepare_send(&tx, wallet, source, account, amount, work)?
            }
        };

        match result {
            PreparedSend::Cached(block) => Ok(block),
            PreparedSend::New(block, details) => {
                self.action_complete(Arc::clone(wallet), block, source, generate_work, &details)
            }
        }
    }

    fn change_action(
        &self,
        wallet: &Arc<Wallet>,
        source: Account,
        representative: PublicKey,
        mut work: u64,
        generate_work: bool,
    ) -> Option<Block> {
        let mut epoch = Epoch::Epoch0;
        let mut block = None;
        {
            let wallet_tx = self.env.tx_begin_read();
            let block_tx = self.ledger.read_txn();
            if !wallet.store.valid_password(&wallet_tx) {
                warn!(
                    "Changing representative for account {} failed, wallet locked",
                    source.encode_account()
                );
                return None;
            }

            let existing = wallet.store.find(&wallet_tx, &source.into());
            if existing.is_some() && self.ledger.any().account_head(&block_tx, &source).is_some() {
                info!(
                    "Changing representative for account {} to {}",
                    source.encode_account(),
                    representative.as_account().encode_account()
                );
                let info = self.ledger.account_info(&block_tx, &source).unwrap();
                let prv = wallet.store.fetch(&wallet_tx, &source.into()).unwrap();
                if work == 0 {
                    work = wallet
                        .store
                        .work_get(&wallet_tx, &source.into())
                        .unwrap_or_default();
                }
                let priv_key = PrivateKey::from(prv);
                let state_block: Block = StateBlockArgs {
                    key: &priv_key,
                    previous: info.head,
                    representative,
                    balance: info.balance,
                    link: Link::zero(),
                    work,
                }
                .into();
                block = Some(state_block);
                epoch = info.epoch;
            } else {
                warn!("Changing representative for account {} failed, wallet locked or account not found",
                    source.encode_account());
            }
        }

        let block = block?;

        let details = BlockDetails::new(epoch, false, false, false);
        self.action_complete(Arc::clone(&wallet), block, source, generate_work, &details)
            .ok()
            .map(|b| b.into())
    }

    fn change_action2(
        &self,
        wallet_id: &WalletId,
        source: Account,
        representative: PublicKey,
        work: u64,
        generate_work: bool,
    ) -> Option<Block> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id).ok()?;
        self.change_action(&wallet, source, representative, work, generate_work)
    }

    fn receive_action(
        &self,
        wallet: &Arc<Wallet>,
        send_hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        mut work: u64,
        generate_work: bool,
    ) -> Option<SavedBlock> {
        if amount < self.node_config.receive_minimum {
            warn!(
                "Not receiving block {} due to minimum receive threshold",
                send_hash
            );
            return None;
        }

        let mut block = None;
        let mut epoch = Epoch::Epoch0;
        let block_tx = self.ledger.read_txn();
        let wallet_tx = self.env.tx_begin_read();
        if self
            .ledger
            .any()
            .block_exists_or_pruned(&block_tx, &send_hash)
        {
            if let Some(pending_info) = self
                .ledger
                .any()
                .get_pending(&block_tx, &PendingKey::new(account, send_hash))
            {
                if let Ok(prv) = wallet.store.fetch(&wallet_tx, &account.into()) {
                    info!(
                        "Receiving block {} from account {}, amount {}",
                        send_hash,
                        account.encode_account(),
                        pending_info.amount.number()
                    );
                    if work == 0 {
                        work = wallet
                            .store
                            .work_get(&wallet_tx, &account.into())
                            .unwrap_or_default();
                    }
                    let priv_key = PrivateKey::from(prv);
                    if let Some(info) = self.ledger.account_info(&block_tx, &account) {
                        block = Some(
                            StateBlockArgs {
                                key: &priv_key,
                                previous: info.head,
                                representative: info.representative,
                                balance: info.balance + pending_info.amount,
                                link: send_hash.into(),
                                work,
                            }
                            .into(),
                        );
                        epoch = std::cmp::max(info.epoch, pending_info.epoch);
                    } else {
                        block = Some(
                            StateBlockArgs {
                                key: &priv_key,
                                previous: BlockHash::zero(),
                                representative,
                                balance: pending_info.amount,
                                link: send_hash.into(),
                                work,
                            }
                            .into(),
                        );
                        epoch = pending_info.epoch;
                    }
                } else {
                    warn!(
                        "Unable to receive, wallet locked, block {} to account: {}",
                        send_hash,
                        account.encode_account()
                    );
                }
            } else {
                // Ledger doesn't have this marked as available to receive anymore
                warn!("Not receiving block {}, block already received", send_hash);
            }
        } else {
            // Ledger doesn't have this block anymore.
            warn!(
                "Not receiving block {}, block no longer exists or pruned",
                send_hash
            );
        }

        let block = block?;
        let details = BlockDetails::new(epoch, false, true, false);
        self.action_complete(Arc::clone(wallet), block, account, generate_work, &details)
            .ok()
    }

    fn receive_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        action: Box<dyn Fn(Option<SavedBlock>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) {
        let self_l = Arc::clone(self);
        self.wallet_actions.queue_wallet_action(
            amount,
            wallet,
            Box::new(move |wallet| {
                let block = self_l.receive_action(
                    &wallet,
                    hash,
                    representative,
                    amount,
                    account,
                    work,
                    generate_work,
                );
                action(block);
            }),
        );
    }

    fn receive_async(
        &self,
        wallet_id: WalletId,
        hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        action: Box<dyn Fn(Option<SavedBlock>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        if wallet.store.find(&tx, &account.into()).is_none() {
            return Err(WalletsError::AccountNotFound);
        }

        self.receive_async_wallet(
            Arc::clone(wallet),
            hash,
            representative,
            amount,
            account,
            action,
            work,
            generate_work,
        );
        Ok(())
    }

    fn receive_sync(
        &self,
        wallet: Arc<Wallet>,
        block_hash: BlockHash,
        representative: PublicKey,
        amount: Amount,
        account: Account,
        work: u64,
        generate_work: bool,
    ) -> Result<SavedBlock, ()> {
        let result = Arc::new((Condvar::new(), Mutex::new((false, None)))); // done, result
        let result_clone = Arc::clone(&result);
        self.receive_async_wallet(
            wallet,
            block_hash,
            representative,
            amount,
            account,
            Box::new(move |block| {
                *result_clone.1.lock().unwrap() = (true, block.clone());
                result_clone.0.notify_all();
            }),
            work,
            generate_work,
        );
        let mut guard = result.1.lock().unwrap();
        guard = result.0.wait_while(guard, |i| !i.0).unwrap();
        if let Some(block) = guard.1.clone() {
            Ok(block)
        } else {
            Err(())
        }
    }

    fn send_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        account: Account,
        amount: Amount,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) {
        let self_l = Arc::clone(self);
        self.wallet_actions.queue_wallet_action(
            HIGH_PRIORITY,
            wallet,
            Box::new(move |wallet| {
                let block = self_l
                    .send_action(
                        &wallet,
                        source,
                        account,
                        amount,
                        work,
                        generate_work,
                        id.clone(),
                    )
                    .ok()
                    .map(|b| b.into());
                action(block);
            }),
        );
    }

    fn send_async(
        &self,
        wallet_id: WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }
        if wallet.store.find(&tx, &source.into()).is_none() {
            return Err(WalletsError::AccountNotFound);
        }
        self.send_async_wallet(
            Arc::clone(wallet),
            source,
            account,
            amount,
            action,
            work,
            generate_work,
            id,
        );

        Ok(())
    }

    fn send_sync(
        &self,
        wallet_id: WalletId,
        source: Account,
        account: Account,
        amount: Amount,
        work: u64,
        generate_work: bool,
        id: Option<String>,
    ) -> BlockHash {
        let guard = self.mutex.lock().unwrap();
        let Some(wallet) = guard.get(&wallet_id) else {
            panic!("wallet not found")
        };

        let result = Arc::new((Condvar::new(), Mutex::new((false, BlockHash::zero())))); // done, result
        let result_clone = Arc::clone(&result);

        self.send_async_wallet(
            Arc::clone(wallet),
            source,
            account,
            amount,
            Box::new(move |block| {
                *result_clone.1.lock().unwrap() =
                    (true, block.map(|b| b.hash()).unwrap_or_default());
                result_clone.0.notify_all();
            }),
            work,
            generate_work,
            id,
        );

        let mut guard = result.1.lock().unwrap();
        guard = result.0.wait_while(guard, |i| !i.0).unwrap();
        guard.1
    }

    fn search_receivable(
        &self,
        wallet: &Arc<Wallet>,
        wallet_tx: &dyn Transaction,
    ) -> Result<(), ()> {
        if !wallet.store.valid_password(wallet_tx) {
            info!("Unable to search receivable blocks, wallet is locked");
            return Err(());
        }

        info!("Beginning receivable block search");

        for (account, wallet_value) in wallet.store.iter(wallet_tx) {
            let block_tx = self.ledger.read_txn();
            // Don't search pending for watch-only accounts
            if !wallet_value.key.is_zero() {
                for (key, info) in self.ledger.any().account_receivable_upper_bound(
                    &block_tx,
                    account.into(),
                    BlockHash::zero(),
                ) {
                    let hash = key.send_block_hash;
                    let amount = info.amount;
                    if self.node_config.receive_minimum <= amount {
                        info!(
                            "Found a receivable block {} for account {}",
                            hash,
                            info.source.encode_account()
                        );
                        if self
                            .ledger
                            .confirmed()
                            .block_exists_or_pruned(&block_tx, &hash)
                        {
                            let representative = wallet.store.representative(wallet_tx);
                            // Receive confirmed block
                            self.receive_async_wallet(
                                Arc::clone(wallet),
                                hash,
                                representative,
                                amount,
                                account.into(),
                                Box::new(|_| {}),
                                0,
                                true,
                            );
                        } else if !self.confirming_set.contains(&hash) {
                            let block = self.ledger.any().get_block(&block_tx, &hash);
                            if let Some(block) = block {
                                // Request confirmation for block which is not being processed yet
                                let guard = self.start_election.lock().unwrap();
                                if let Some(callback) = guard.as_ref() {
                                    callback(block);
                                }
                            }
                        }
                    }
                }
            }
        }

        info!("Receivable block search phase completed");
        Ok(())
    }

    fn receive_confirmed(&self, hash: BlockHash, destination: Account) {
        //std::unordered_map<nano::wallet_id, std::shared_ptr<nano::wallet>> wallets_l;
        let (wallet_tx, wallets) = {
            let guard = self.mutex.lock().unwrap();
            (self.env.tx_begin_read(), guard.clone())
        };

        for (_id, wallet) in wallets {
            if wallet.store.exists(&wallet_tx, &destination.into()) {
                let representative = wallet.store.representative(&wallet_tx);
                let pending = self
                    .ledger
                    .any()
                    .get_pending(&self.ledger.read_txn(), &PendingKey::new(destination, hash));
                if let Some(pending) = pending {
                    let amount = pending.amount;
                    self.receive_async_wallet(
                        wallet,
                        hash,
                        representative,
                        amount,
                        destination,
                        Box::new(|_| {}),
                        0,
                        true,
                    );
                } else {
                    if !self
                        .ledger
                        .any()
                        .block_exists_or_pruned(&self.ledger.read_txn(), &hash)
                    {
                        warn!("Confirmed block is missing:  {}", hash);
                        debug_assert!(false);
                    } else {
                        warn!("Block %1% has already been received: {}", hash);
                    }
                }
            }
        }
    }

    fn search_receivable_all(&self) {
        let wallets = self.mutex.lock().unwrap().clone();
        let wallet_tx = self.env.tx_begin_read();
        for (_, wallet) in wallets {
            let _ = self.search_receivable(&wallet, &wallet_tx);
        }
    }

    fn search_receivable_wallet(&self, wallet_id: WalletId) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        if let Some(wallet) = guard.get(&wallet_id) {
            let tx = self.env.tx_begin_read();
            if wallet.store.valid_password(&tx) {
                let _ = self.search_receivable(wallet, &tx);
                Ok(())
            } else {
                Err(WalletsError::WalletLocked)
            }
        } else {
            Err(WalletsError::WalletNotFound)
        }
    }

    fn enter_password(&self, wallet_id: WalletId, password: &str) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_write();
        let result = self
            .enter_password_wallet(wallet, &tx, password)
            .map_err(|_| WalletsError::InvalidPassword);
        if result.is_ok() {
            info!("Wallet unlocked");
        } else {
            warn!("Invalid password, wallet locked");
        }
        result
    }

    fn enter_password_wallet(
        &self,
        wallet: &Arc<Wallet>,
        wallet_tx: &dyn Transaction,
        password: &str,
    ) -> Result<(), ()> {
        if !wallet.store.attempt_password(wallet_tx, password) {
            warn!("Invalid password, wallet locked");
            Err(())
        } else {
            info!("Wallet unlocked");
            let self_l = Arc::clone(self);
            self.wallet_actions.queue_wallet_action(
                HIGH_PRIORITY,
                Arc::clone(wallet),
                Box::new(move |wallet| {
                    // Wallets must survive node lifetime
                    let tx = self_l.env.tx_begin_read();
                    let _ = self_l.search_receivable(&wallet, &tx);
                }),
            );
            Ok(())
        }
    }

    fn enter_initial_password(&self, wallet: &Arc<Wallet>) {
        let password = wallet.store.password();
        if password.is_zero() {
            let mut tx = self.env.tx_begin_write();
            if wallet.store.valid_password(&tx) {
                // Newly created wallets have a zero key
                let _ = wallet.store.rekey(&mut tx, "");
            } else {
                let _ = self.enter_password_wallet(wallet, &tx, "");
            }
        }
    }

    fn create(&self, wallet_id: WalletId) {
        let mut guard = self.mutex.lock().unwrap();
        debug_assert!(!guard.contains_key(&wallet_id));
        let wallet = {
            let mut tx = self.env.tx_begin_write();
            let Ok(wallet) = Wallet::new(
                Arc::clone(&self.ledger),
                self.work_thresholds.clone(),
                &mut tx,
                self.node_config.password_fanout as usize,
                self.kdf.clone(),
                self.node_config.random_representative(),
                &PathBuf::from(wallet_id.to_string()),
            ) else {
                return;
            };
            Arc::new(wallet)
        };
        guard.insert(wallet_id, Arc::clone(&wallet));
        self.enter_initial_password(&wallet);
    }

    fn change_async_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        representative: PublicKey,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) {
        let self_l = Arc::clone(self);
        self.wallet_actions.queue_wallet_action(
            HIGH_PRIORITY,
            wallet,
            Box::new(move |wallet| {
                let block =
                    self_l.change_action(&wallet, source, representative, work, generate_work);
                action(block);
            }),
        );
    }

    fn change_sync_wallet(
        &self,
        wallet: Arc<Wallet>,
        source: Account,
        representative: PublicKey,
    ) -> Result<(), ()> {
        let result = Arc::new((Condvar::new(), Mutex::new((false, false)))); // done, result
        let result_clone = Arc::clone(&result);
        self.change_async_wallet(
            wallet,
            source,
            representative,
            Box::new(move |block| {
                *result_clone.1.lock().unwrap() = (true, block.is_some());
                result_clone.0.notify_all();
            }),
            0,
            true,
        );
        let mut guard = result.1.lock().unwrap();
        guard = result.0.wait_while(guard, |i| !i.0).unwrap();
        if guard.1 {
            Ok(())
        } else {
            Err(())
        }
    }

    fn change_async(
        &self,
        wallet_id: WalletId,
        source: Account,
        representative: PublicKey,
        action: Box<dyn Fn(Option<Block>) + Send + Sync>,
        work: u64,
        generate_work: bool,
    ) -> Result<(), WalletsError> {
        let guard = self.mutex.lock().unwrap();
        let wallet = Wallets::get_wallet(&guard, &wallet_id)?;
        let tx = self.env.tx_begin_write();
        if !wallet.store.valid_password(&tx) {
            return Err(WalletsError::WalletLocked);
        }

        if wallet.store.find(&tx, &source.into()).is_none() {
            return Err(WalletsError::AccountNotFound);
        }

        self.change_async_wallet(
            Arc::clone(wallet),
            source,
            representative,
            action,
            work,
            generate_work,
        );
        Ok(())
    }

    fn set_representative(
        &self,
        wallet_id: WalletId,
        rep: PublicKey,
        update_existing_accounts: bool,
    ) -> Result<(), WalletsError> {
        let mut accounts = Vec::new();
        {
            let guard = self.mutex.lock().unwrap();
            let wallet = Wallets::get_wallet(&guard, &wallet_id)?;

            {
                let mut tx = self.env.tx_begin_write();
                if update_existing_accounts && !wallet.store.valid_password(&tx) {
                    return Err(WalletsError::WalletLocked);
                }

                wallet.store.representative_set(&mut tx, &rep);
            }

            // Change representative for all wallet accounts
            if update_existing_accounts {
                let tx = self.env.tx_begin_read();
                let block_tx = self.ledger.read_txn();
                for (account, _) in wallet.store.iter(&tx) {
                    if let Some(info) = self.ledger.account_info(&block_tx, &account.into()) {
                        if info.representative != rep {
                            accounts.push(account);
                        }
                    }
                }
            }
        }

        for account in accounts {
            self.change_async(wallet_id, account.into(), rep, Box::new(|_| {}), 0, false)?;
        }

        Ok(())
    }

    fn ensure_wallet_is_unlocked(&self, wallet_id: WalletId, password: &str) -> bool {
        let guard = self.mutex.lock().unwrap();
        let Some(existing) = guard.get(&wallet_id) else {
            return false;
        };
        let tx = self.env.tx_begin_write();
        let mut valid = existing.store.valid_password(&tx);
        if !valid {
            valid = self.enter_password_wallet(existing, &tx, password).is_ok();
        }

        valid
    }

    fn initialize2(&self) {
        {
            let guard = self.mutex.lock().unwrap();
            for (_, wallet) in guard.iter() {
                self.enter_initial_password(wallet);
            }
        }
        if self.node_config.enable_voting {
            self.ongoing_compute_reps();
        }
    }
}

fn test_scan_wallet_reps_delay() -> Duration {
    let test_env = get_env_or_default_string("NANO_TEST_WALLET_SCAN_REPS_DELAY", "900000"); // 15 minutes by default
    Duration::from_millis(test_env.parse().unwrap())
}
