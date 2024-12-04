use std::time::Duration;

use rsnano_core::{Amount, Block, BlockHash, PrivateKey, StateBlock, WalletId, DEV_GENESIS_KEY};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY};
use rsnano_node::{
    config::{NodeConfig, NodeFlags},
    consensus::ActiveElectionsExt,
    wallets::WalletsExt,
};
use test_helpers::{assert_timely, assert_timely_eq, System};

#[test]
fn open_create() {
    let mut system = System::new();
    let node = system.make_node();
    assert_eq!(node.wallets.mutex.lock().unwrap().len(), 1); // it starts out with a default wallet
    let id = WalletId::random();
    assert_eq!(node.wallets.wallet_exists(&id), false);
    node.wallets.create(id);
    assert_eq!(node.wallets.wallet_exists(&id), true);
}

#[test]
fn vote_minimum() {
    let mut system = System::new();
    let node = system.make_node();
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - node.config.vote_minimum,
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    node.process(send1.clone()).unwrap();

    let open1 = Block::State(StateBlock::new(
        key1.account(),
        BlockHash::zero(),
        key1.public_key(),
        node.config.vote_minimum,
        send1.hash().into(),
        &key1,
        node.work_generate_dev(&key1),
    ));
    node.process(open1.clone()).unwrap();

    // send2 with amount vote_minimum - 1 (not voting representative)
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send1.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - node.config.vote_minimum * 2 + Amount::raw(1),
        key2.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send1.hash()),
    ));
    node.process(send2.clone()).unwrap();

    let open2 = Block::State(StateBlock::new(
        key2.account(),
        BlockHash::zero(),
        key2.public_key(),
        node.config.vote_minimum - Amount::raw(1),
        send2.hash().into(),
        &key2,
        node.work_generate_dev(&key2),
    ));
    node.process(open2.clone()).unwrap();

    let wallet_id = node.wallets.wallet_ids()[0];
    assert_eq!(
        node.wallets
            .mutex
            .lock()
            .unwrap()
            .get(&wallet_id)
            .unwrap()
            .representatives
            .lock()
            .unwrap()
            .len(),
        0
    );

    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), false)
        .unwrap();
    node.wallets
        .insert_adhoc2(&wallet_id, &key1.private_key(), false)
        .unwrap();
    node.wallets
        .insert_adhoc2(&wallet_id, &key2.private_key(), false)
        .unwrap();
    node.wallets.compute_reps();
    assert_eq!(
        node.wallets
            .mutex
            .lock()
            .unwrap()
            .get(&wallet_id)
            .unwrap()
            .representatives
            .lock()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn exists() {
    let mut system = System::new();
    let node = system.make_node();
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();
    let wallet_id = node.wallets.wallet_ids()[0];

    assert_eq!(node.wallets.exists(&key1.public_key()), false);
    assert_eq!(node.wallets.exists(&key2.public_key()), false);

    node.wallets
        .insert_adhoc2(&wallet_id, &key1.private_key(), false)
        .unwrap();
    assert_eq!(node.wallets.exists(&key1.public_key()), true);
    assert_eq!(node.wallets.exists(&key2.public_key()), false);

    node.wallets
        .insert_adhoc2(&wallet_id, &key2.private_key(), false)
        .unwrap();
    assert_eq!(node.wallets.exists(&key1.public_key()), true);
    assert_eq!(node.wallets.exists(&key2.public_key()), true);
}

#[test]
fn search_receivable() {
    for search_all in [false, true] {
        let mut system = System::new();
        let node = system
            .build_node()
            .config(NodeConfig {
                enable_voting: false,
                ..System::default_config_without_backlog_population()
            })
            .flags(NodeFlags {
                disable_search_pending: true,
                ..Default::default()
            })
            .finish();
        let wallet_id = node.wallets.wallet_ids()[0];

        node.wallets
            .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), false)
            .unwrap();

        let send = Block::State(StateBlock::new(
            *DEV_GENESIS_ACCOUNT,
            *DEV_GENESIS_HASH,
            *DEV_GENESIS_PUB_KEY,
            Amount::MAX - node.config.receive_minimum,
            (*DEV_GENESIS_ACCOUNT).into(),
            &DEV_GENESIS_KEY,
            node.work_generate_dev(*DEV_GENESIS_HASH),
        ));
        node.process(send.clone()).unwrap();

        // Pending search should start an election
        assert_eq!(node.active.len(), 0);
        if search_all {
            node.wallets.search_receivable_all();
        } else {
            node.wallets.search_receivable_wallet(wallet_id).unwrap();
        }
        let mut election = None;
        assert_timely(Duration::from_secs(5), || {
            match node.active.election(&send.qualified_root()) {
                Some(e) => {
                    election = Some(e);
                    true
                }
                None => false,
            }
        });

        // Erase the key so the confirmation does not trigger an automatic receive
        node.wallets
            .remove_key(&wallet_id, &DEV_GENESIS_PUB_KEY)
            .unwrap();

        // Now confirm the election
        node.active.force_confirm(&election.unwrap());

        assert_timely(Duration::from_secs(5), || {
            node.block_confirmed(&send.hash()) && node.active.len() == 0
        });

        // Re-insert the key
        node.wallets
            .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), false)
            .unwrap();

        // Pending search should create the receive block
        assert_eq!(node.ledger.block_count(), 2);
        if search_all {
            node.wallets.search_receivable_all();
        } else {
            node.wallets.search_receivable_wallet(wallet_id).unwrap();
        }
        assert_timely_eq(
            Duration::from_secs(3),
            || node.balance(&DEV_GENESIS_ACCOUNT),
            Amount::MAX,
        );
        let receive_hash = node
            .ledger
            .any()
            .account_head(&node.ledger.read_txn(), &DEV_GENESIS_ACCOUNT)
            .unwrap();
        let receive = node.block(&receive_hash).unwrap();
        assert_eq!(receive.height(), 3);
        assert_eq!(send.hash(), receive.source().unwrap());
    }
}
