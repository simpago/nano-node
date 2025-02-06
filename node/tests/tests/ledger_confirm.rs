use rsnano_core::{Amount, PrivateKey, UnsavedBlockLatticeBuilder, DEV_GENESIS_KEY};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_PUB_KEY};
use rsnano_node::stats::{DetailType, Direction, StatType};
use std::time::Duration;
use test_helpers::{assert_timely_eq, System};

#[test]
fn single() {
    let mut system = System::new();
    let node = system.make_node();
    let key1 = PrivateKey::new();
    node.insert_into_wallet(&DEV_GENESIS_KEY);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let latest1 = node.latest(&DEV_GENESIS_ACCOUNT);
    let send1 = lattice.genesis().send(&key1, 100);
    node.process(send1.clone());
    let mut tx = node.ledger.rw_txn();
    assert_eq!(
        node.ledger.confirmed().block_exists(&tx, &send1.hash()),
        false
    );
    node.ledger.confirm(&mut tx, send1.hash());
    assert_eq!(
        node.ledger.confirmed().block_exists(&tx, &send1.hash()),
        true
    );
    let conf_height = node
        .ledger
        .get_confirmation_height(&tx, &DEV_GENESIS_ACCOUNT)
        .unwrap();
    assert_eq!(conf_height.height, 2);
    assert_eq!(conf_height.frontier, send1.hash());

    // Rollbacks should fail as these blocks have been cemented
    assert!(node.ledger.rollback(&mut tx, &latest1).is_err());
    assert!(node.ledger.rollback(&mut tx, &send1.hash()).is_err());
    assert_eq!(
        node.stats.count(
            StatType::ConfirmationHeight,
            DetailType::BlocksConfirmed,
            Direction::In
        ),
        1
    );
    assert_eq!(node.ledger.cemented_count(), 2);
}

#[test]
fn multiple_accounts() {
    let mut system = System::new();
    let cfg = System::default_config_without_backlog_scan();
    let node = system.build_node().config(cfg).finish();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();
    let key3 = PrivateKey::new();

    let quorum_delta = node.online_reps.lock().unwrap().quorum_delta();

    // Send to all accounts
    let send1 = lattice
        .genesis()
        .send_all_except(&key1, quorum_delta + Amount::raw(300));
    let send2 = lattice.genesis().send(&key2, 100);
    let send3 = lattice.genesis().send(&key3, 100);

    // Open all accounts
    let open1 = lattice.account(&key1).receive(&send1);
    let open2 = lattice.account(&key2).receive(&send2);
    let open3 = lattice.account(&key3).receive(&send3);

    // Send and receive various blocks to these accounts
    let send4 = lattice.account(&key1).send_all_except(&key2, 50);
    let send5 = lattice.account(&key1).send(&key2, 40);
    let receive1 = lattice.account(&key2).receive(&send4);
    let send6 = lattice.account(&key2).send_all_except(&key3, 10);
    let receive2 = lattice.account(&key2).receive(&send5);

    node.process_multi(&[
        send1.clone(),
        send2.clone(),
        send3.clone(),
        open1.clone(),
        open2.clone(),
        open3.clone(),
        send4.clone(),
        send5.clone(),
        receive1.clone(),
        send6.clone(),
        receive2.clone(),
    ]);

    // Check confirmation heights of all the accounts (except genesis) are uninitialized (0),
    // as we have any just added them to the ledger and not processed any live transactions yet.
    let mut tx = node.ledger.rw_txn();
    assert_eq!(
        node.ledger
            .get_confirmation_height(&tx, &DEV_GENESIS_ACCOUNT)
            .unwrap()
            .height,
        1
    );
    assert!(node
        .ledger
        .get_confirmation_height(&tx, &key1.public_key().as_account())
        .is_none());
    assert!(node
        .ledger
        .get_confirmation_height(&tx, &key2.public_key().as_account())
        .is_none());
    assert!(node
        .ledger
        .get_confirmation_height(&tx, &key3.public_key().as_account())
        .is_none());

    // The nodes process a live receive which propagates across to all accounts
    let receive3 = lattice.account(&key3).receive(&send6);
    node.ledger.process(&mut tx, &receive3).unwrap();
    let confirmed = node.ledger.confirm(&mut tx, receive3.hash());
    assert_eq!(confirmed.len(), 10);
    assert_eq!(
        node.stats.count(
            StatType::ConfirmationHeight,
            DetailType::BlocksConfirmed,
            Direction::In
        ),
        10
    );
    assert_eq!(node.ledger.cemented_count(), 11);
    assert!(node.ledger.confirmed().block_exists(&tx, &receive3.hash()));
    assert_eq!(
        node.ledger
            .any()
            .get_account(&tx, &DEV_GENESIS_ACCOUNT)
            .unwrap()
            .block_count,
        4
    );
    assert_eq!(
        node.ledger
            .get_confirmation_height(&tx, &DEV_GENESIS_ACCOUNT)
            .unwrap()
            .height,
        4
    );
    assert_eq!(
        node.ledger
            .get_confirmation_height(&tx, &DEV_GENESIS_ACCOUNT)
            .unwrap()
            .frontier,
        send3.hash()
    );
    assert_eq!(
        node.ledger
            .any()
            .get_account(&tx, &key1.public_key().as_account())
            .unwrap()
            .block_count,
        3
    );
    assert_eq!(
        node.ledger
            .get_confirmation_height(&tx, &key1.public_key().as_account())
            .unwrap()
            .height,
        2
    );

    // The accounts for key1 and key2 have 1 more block in the chain than is confirmed.
    // So this can be rolled back, but the one before that cannot. Check that this is the case
    assert!(node.ledger.rollback(&mut tx, &receive2.hash()).is_ok());
    assert!(node.ledger.rollback(&mut tx, &send5.hash()).is_ok());
    assert!(node.ledger.rollback(&mut tx, &send4.hash()).is_err());
    assert!(node.ledger.rollback(&mut tx, &send6.hash()).is_err());

    // Confirm the other latest can't be rolled back either
    assert!(node.ledger.rollback(&mut tx, &receive3.hash()).is_err());
    assert!(node.ledger.rollback(&mut tx, &send3.hash()).is_err());

    // Attempt some others which have been cemented
    assert!(node.ledger.rollback(&mut tx, &open1.hash()).is_err());
    assert!(node.ledger.rollback(&mut tx, &send2.hash()).is_err());
}

#[test]
fn send_receive_between_2_accounts() {
    let mut system = System::new();
    let cfg = System::default_config_without_backlog_scan();
    let node = system.build_node().config(cfg).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();

    let quorum_delta = node.online_reps.lock().unwrap().quorum_delta();

    let send1 = lattice
        .genesis()
        .send_all_except(&key1, quorum_delta + Amount::raw(2));

    let open1 = lattice.account(&key1).receive(&send1);
    let send2 = lattice
        .account(&key1)
        .send_all_except(&*DEV_GENESIS_PUB_KEY, 1000);

    let send3 = lattice.account(&key1).send(&*DEV_GENESIS_KEY, 100);
    let send4 = lattice.account(&key1).send(&*DEV_GENESIS_KEY, 400);
    let receive1 = lattice.genesis().receive(&send2);
    let receive2 = lattice.genesis().receive(&send3);
    let receive3 = lattice.genesis().receive(&send4);
    let send5 = lattice
        .genesis()
        .send_all_except(&key1, quorum_delta + Amount::raw(1));
    let receive4 = lattice.account(&key1).receive(&send5);
    let key2 = PrivateKey::new();
    let send6 = lattice.genesis().send(&key2, Amount::raw(1));
    // Unpocketed send

    node.process_multi(&[
        send1.clone(),
        open1.clone(),
        send2.clone(),
        receive1.clone(),
        send3.clone(),
        send4.clone(),
        receive2.clone(),
        receive3.clone(),
        send5.clone(),
        send6.clone(),
        receive4.clone(),
    ]);

    let mut tx = node.ledger.rw_txn();
    let confirmed = node.ledger.confirm(&mut tx, receive4.hash());
    assert_eq!(confirmed.len(), 10);
    assert_eq!(
        node.stats.count(
            StatType::ConfirmationHeight,
            DetailType::BlocksConfirmed,
            Direction::In
        ),
        10
    );
    assert_eq!(node.ledger.cemented_count(), 11);
}

#[test]
fn send_receive_self() {
    let mut system = System::new();
    let cfg = System::default_config_without_backlog_scan();
    let node = system.build_node().config(cfg).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice.genesis().send(&*DEV_GENESIS_KEY, 2);
    let receive1 = lattice.genesis().receive(&send1);
    let send2 = lattice.genesis().send(&*DEV_GENESIS_KEY, 2);
    let send3 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    let receive2 = lattice.genesis().receive(&send2);
    let receive3 = lattice.genesis().receive(&send3);

    // Send to another account to prevent automatic receiving on the genesis account
    let key1 = PrivateKey::new();
    let send4 = lattice
        .genesis()
        .send_all_except(&key1, node.online_reps.lock().unwrap().quorum_delta());

    node.process_multi(&[
        send1.clone(),
        receive1.clone(),
        send2.clone(),
        send3.clone(),
        receive2.clone(),
        receive3.clone(),
        send4.clone(),
    ]);

    let mut tx = node.ledger.rw_txn();
    let confirmed = node.ledger.confirm(&mut tx, receive3.hash());
    assert_eq!(confirmed.len(), 6);
    assert!(node.ledger.confirmed().block_exists(&tx, &receive3.hash()));
    assert_eq!(
        node.ledger
            .any()
            .get_account(&tx, &DEV_GENESIS_ACCOUNT)
            .unwrap()
            .block_count,
        8
    );
    assert_eq!(node.ledger.cemented_count(), 7);
}

#[test]
fn all_block_types() {
    let mut system = System::new();
    let cfg = System::default_config_without_backlog_scan();
    let node = system.build_node().config(cfg).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();

    let send = lattice.genesis().legacy_send(&key1, Amount::nano(1000));
    let send1 = lattice.genesis().legacy_send(&key2, Amount::nano(1000));
    let open = lattice.account(&key1).legacy_open(&send);
    let state_open = lattice.account(&key2).receive(&send1);
    let send2 = lattice
        .account(&key1)
        .legacy_send(&key2, Amount::nano(1000));

    let state_receive = lattice.account(&key2).receive(&send2);
    let state_send = lattice.account(&key2).send(&key1, Amount::nano(1000));
    let receive = lattice.account(&key1).legacy_receive(&state_send);
    let change = lattice.account(&key1).legacy_change(&key2);
    let state_change = lattice.account(&key2).change(&*DEV_GENESIS_KEY);
    let epoch = lattice.account(&key2).epoch1();
    let epoch1 = lattice.account(&key1).epoch1();
    let state_send1 = lattice.account(&key1).send(&key2, 1);
    let state_receive2 = lattice.account(&key2).receive(&state_send1);
    let state_send2 = lattice.account(&key2).send(&key1, 1);
    let state_send3 = lattice.account(&key2).send(&key1, 1);
    let state_send4 = lattice.account(&key1).send(&*DEV_GENESIS_KEY, 1);
    let state_receive3 = lattice.genesis().receive(&state_send4);
    node.process_multi(&[
        send,
        send1,
        open,
        state_open,
        send2,
        state_receive,
        state_send,
        receive,
        change,
        state_change,
        epoch,
        epoch1,
        state_send1,
        state_receive2,
        state_send2.clone(),
        state_send3,
        state_send4,
        state_receive3,
    ]);
    let mut tx = node.ledger.rw_txn();
    let confirmed = node.ledger.confirm(&mut tx, state_send2.hash());
    assert_eq!(confirmed.len(), 15);
    assert_eq!(node.ledger.cemented_count(), 16);
}

#[test]
// This test ensures a block that's cemented cannot be rolled back by the node
// A block is inserted and confirmed then later a different block is force inserted with a rollback attempt
fn conflict_rollback_cemented() {
    let mut system = System::new();
    let node1 = system.make_node();

    let key1 = PrivateKey::new();
    // create one side of a forked transaction on node1
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let fork1a = lattice.genesis().send(&key1, 100);
    node1.process(fork1a.clone());
    node1.confirm(fork1a.hash());

    // create the other side of the fork on node2
    let key2 = PrivateKey::new();
    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let fork1b = fork_lattice.genesis().send(&key2, 100);
    node1.block_processor.force(fork1b.into());
    // node2 already has send2 forced confirmed whilst node1 should have confirmed send1 and therefore we have a cemented fork on node2
    // and node2 should print an error message on the log that it cannot rollback send2 because it is already cemented
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node1
                .stats
                .count(StatType::Ledger, DetailType::RollbackFailed, Direction::In)
        },
        1,
    );
    // fork1a should still remain after the rollback failed event
    assert!(node1.block_confirmed(&fork1a.hash()));
}

#[test]
fn observers() {
    let mut system = System::new();
    let node1 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send = lattice.genesis().send(&key1, 100);
    node1.process(send.clone());
    node1.confirm(send.hash());
    assert_eq!(
        node1.stats.count(
            StatType::ConfirmationHeight,
            DetailType::BlocksConfirmed,
            Direction::In
        ),
        1
    );
}
