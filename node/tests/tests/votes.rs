use rsnano_core::{
    Amount, Epoch, PrivateKey, Signature, UnsavedBlockLatticeBuilder, Vote, VoteCode, VoteSource,
    WalletId, DEV_GENESIS_KEY,
};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY};
use rsnano_node::{
    config::NodeFlags,
    stats::{DetailType, Direction, StatType},
    wallets::WalletsExt,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use test_helpers::{
    assert_timely, assert_timely_eq2, make_fake_channel, start_election, upgrade_epoch, System,
};

#[test]
fn check_signature() {
    let mut system = System::new();
    let mut config = System::default_config();
    config.online_weight_minimum = Amount::MAX;
    let node = system.build_node().config(config).finish();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send1 = lattice.genesis().send(&key1, 100);
    node.process(send1.clone()).unwrap();
    let election1 = start_election(&node, &send1.hash());
    assert_eq!(1, election1.vote_count());
    let mut vote1 = Vote::new(&DEV_GENESIS_KEY, Vote::TIMESTAMP_MIN, 0, vec![send1.hash()]);
    let good_signature = vote1.signature;
    vote1.signature = Signature::new();
    let channel = make_fake_channel(&node);
    assert_eq!(
        VoteCode::Invalid,
        node.vote_processor.vote_blocking(
            &Arc::new(vote1.clone()),
            Some(channel.clone()),
            VoteSource::Live
        )
    );

    vote1.signature = good_signature;
    assert_eq!(
        VoteCode::Vote,
        node.vote_processor.vote_blocking(
            &Arc::new(vote1.clone()),
            Some(channel.clone()),
            VoteSource::Live
        )
    );
    assert_eq!(
        VoteCode::Replay,
        node.vote_processor.vote_blocking(
            &Arc::new(vote1.clone()),
            Some(channel.clone()),
            VoteSource::Live
        )
    );
}

// Lower timestamps are ignored
#[test]
fn add_old() {
    let mut system = System::new();
    let node = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send1 = lattice.genesis().send_max(&key1);
    node.process(send1.clone()).unwrap();
    start_election(&node, &send1.hash());
    assert_timely(Duration::from_secs(5), || {
        node.active.election(&send1.qualified_root()).is_some()
    });
    let election1 = node.active.election(&send1.qualified_root()).unwrap();
    let vote1 = Arc::new(Vote::new(
        &DEV_GENESIS_KEY,
        Vote::TIMESTAMP_MIN * 2,
        0,
        vec![send1.hash()],
    ));
    let channel = make_fake_channel(&node);
    node.vote_processor
        .vote_blocking(&vote1, Some(channel.clone()), VoteSource::Live);

    let key2 = PrivateKey::new();
    let send2 = fork_lattice.genesis().send_max(&key2);
    let vote2 = Arc::new(Vote::new(
        &DEV_GENESIS_KEY,
        Vote::TIMESTAMP_MIN * 1,
        0,
        vec![send2.hash()],
    ));

    election1
        .mutex
        .lock()
        .unwrap()
        .last_votes
        .get_mut(&DEV_GENESIS_PUB_KEY)
        .unwrap()
        .time = SystemTime::now() - Duration::from_secs(20);
    node.vote_processor
        .vote_blocking(&vote2, Some(channel), VoteSource::Live);
    assert_eq!(2, election1.vote_count());
    let votes = election1.mutex.lock().unwrap().last_votes.clone();
    assert!(votes.contains_key(&DEV_GENESIS_PUB_KEY));
    assert_eq!(send1.hash(), votes.get(&DEV_GENESIS_PUB_KEY).unwrap().hash);
    assert_eq!(send1.hash(), election1.winner_hash().unwrap());
}

// The voting cooldown is respected
#[test]
fn add_cooldown() {
    let mut system = System::new();
    let node = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send1 = lattice.genesis().send_max(&key1);
    node.process(send1.clone()).unwrap();
    start_election(&node, &send1.hash());
    assert_timely(Duration::from_secs(5), || {
        node.active.election(&send1.qualified_root()).is_some()
    });
    let election1 = node.active.election(&send1.qualified_root()).unwrap();
    let vote1 = Arc::new(Vote::new(
        &DEV_GENESIS_KEY,
        Vote::TIMESTAMP_MIN * 1,
        0,
        vec![send1.hash()],
    ));
    let channel = make_fake_channel(&node);
    node.vote_processor
        .vote_blocking(&vote1, Some(channel.clone()), VoteSource::Live);

    let key2 = PrivateKey::new();
    let send2 = fork_lattice.genesis().send_max(&key2);
    let vote2 = Arc::new(Vote::new(
        &DEV_GENESIS_KEY,
        Vote::TIMESTAMP_MIN * 2,
        0,
        vec![send2.hash()],
    ));

    node.vote_processor
        .vote_blocking(&vote2, Some(channel), VoteSource::Live);
    assert_eq!(2, election1.vote_count());
    let votes = election1.mutex.lock().unwrap().last_votes.clone();
    assert!(votes.contains_key(&DEV_GENESIS_PUB_KEY));
    assert_eq!(send1.hash(), votes.get(&DEV_GENESIS_PUB_KEY).unwrap().hash);
    assert_eq!(send1.hash(), election1.winner_hash().unwrap());
}

// Assuming necessary imports and module declarations are present
#[test]
fn vote_generator_cache() {
    let mut system = System::new();
    let node = system.make_node();

    let epoch1 = upgrade_epoch(node.clone(), Epoch::Epoch1);
    let wallet_id = WalletId::random();

    node.wallets.create(wallet_id);
    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.raw_key(), true)
        .unwrap();

    node.vote_generators
        .generate_non_final_vote(&epoch1.root(), &epoch1.hash());

    // Wait until the votes are available
    assert_timely(Duration::from_secs(1), || {
        !node
            .history
            .votes(&epoch1.root(), &epoch1.hash(), false)
            .is_empty()
    });

    let votes = node.history.votes(&epoch1.root(), &epoch1.hash(), false);
    assert!(!votes.is_empty());

    let hashes = &votes[0].hashes;
    assert!(hashes.contains(&epoch1.hash()));
}

#[test]
fn vote_generator_multiple_representatives() {
    let mut system = System::new();
    let node = system.make_node();
    let wallet_id = WalletId::random();
    node.wallets.create(wallet_id);
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();
    let key3 = PrivateKey::new();

    // Insert keys into the wallet
    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.raw_key(), true)
        .unwrap();
    node.wallets
        .insert_adhoc2(&wallet_id, &key1.raw_key(), true)
        .unwrap();
    node.wallets
        .insert_adhoc2(&wallet_id, &key2.raw_key(), true)
        .unwrap();
    node.wallets
        .insert_adhoc2(&wallet_id, &key3.raw_key(), true)
        .unwrap();

    let amount = Amount::nano(100_000);
    node.wallets.send_sync(
        wallet_id,
        *DEV_GENESIS_ACCOUNT,
        key1.account(),
        amount,
        0,
        true,
        None,
    );
    node.wallets.send_sync(
        wallet_id,
        *DEV_GENESIS_ACCOUNT,
        key2.account(),
        amount,
        0,
        true,
        None,
    );
    node.wallets.send_sync(
        wallet_id,
        *DEV_GENESIS_ACCOUNT,
        key3.account(),
        amount,
        0,
        true,
        None,
    );

    // Assert balances
    assert_timely(Duration::from_secs(3), || {
        node.balance(&key1.account()) == amount
            && node.balance(&key2.account()) == amount
            && node.balance(&key3.account()) == amount
    });

    // Change representatives
    node.wallets
        .change_action2(&wallet_id, key1.account(), key1.public_key(), 0, true);
    node.wallets
        .change_action2(&wallet_id, key2.account(), key2.public_key(), 0, true);
    node.wallets
        .change_action2(&wallet_id, key3.account(), key3.public_key(), 0, true);

    assert_eq!(node.ledger.weight(&key1.public_key()), amount);
    assert_eq!(node.ledger.weight(&key2.public_key()), amount);
    assert_eq!(node.ledger.weight(&key3.public_key()), amount);

    node.wallets.compute_reps();
    assert_eq!(node.wallets.voting_reps_count(), 4);

    let hash = node.wallets.send_sync(
        wallet_id,
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_ACCOUNT,
        Amount::raw(1),
        0,
        true,
        None,
    );
    let send = node.block(&hash).unwrap();

    // Wait until the votes are available
    assert_timely(Duration::from_secs(5), || {
        node.history.votes(&send.root(), &send.hash(), false).len() == 4
    });

    let votes = node.history.votes(&send.root(), &send.hash(), false);
    for account in &[
        key1.public_key(),
        key2.public_key(),
        key3.public_key(),
        DEV_GENESIS_KEY.public_key(),
    ] {
        let existing = votes.iter().find(|vote| vote.voting_account == *account);
        assert!(existing.is_some());
    }
}

#[test]
fn vote_spacing_vote_generator() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_scan();
    config.active_elections.hinted_limit_percentage = 0;
    let node = system
        .build_node()
        .config(config)
        .flags(NodeFlags {
            disable_search_pending: true,
            ..Default::default()
        })
        .finish();

    node.insert_into_wallet(&DEV_GENESIS_KEY);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1000));

    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let send2 = fork_lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1001));

    node.ledger
        .process(&mut node.store.tx_begin_write(), &send1)
        .unwrap();
    assert_eq!(
        node.stats.count(
            StatType::VoteGenerator,
            DetailType::GeneratorBroadcasts,
            Direction::In
        ),
        0
    );
    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send1.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorBroadcasts,
                Direction::In,
            )
        },
        1,
    );

    node.ledger
        .rollback(&mut node.store.tx_begin_write(), &send1.hash())
        .unwrap();
    node.ledger
        .process(&mut node.store.tx_begin_write(), &send2)
        .unwrap();
    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send2.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorSpacing,
                Direction::In,
            )
        },
        1,
    );

    assert_eq!(
        1,
        node.stats.count(
            StatType::VoteGenerator,
            DetailType::GeneratorBroadcasts,
            Direction::In
        )
    );
    std::thread::sleep(node.vote_generators.voting_delay());

    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send2.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorBroadcasts,
                Direction::In,
            )
        },
        2,
    );
}

#[test]
fn vote_spacing_rapid() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_scan();
    config.active_elections.hinted_limit_percentage = 0; // Disable election hinting
    let node = system
        .build_node()
        .config(config)
        .flags(NodeFlags {
            disable_search_pending: true,
            ..Default::default()
        })
        .finish();

    node.insert_into_wallet(&DEV_GENESIS_KEY);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1000));

    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let send2 = fork_lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1001));

    node.process(send1.clone()).unwrap();

    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send1.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorBroadcasts,
                Direction::In,
            )
        },
        1,
    );

    node.ledger
        .rollback(&mut node.ledger.rw_txn(), &send1.hash())
        .unwrap();
    node.ledger
        .process(&mut node.ledger.rw_txn(), &send2)
        .unwrap();
    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send2.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorSpacing,
                Direction::In,
            )
        },
        1,
    );

    std::thread::sleep(node.vote_generators.voting_delay());

    node.vote_generators
        .generate_non_final_vote(&(*DEV_GENESIS_HASH).into(), &send2.hash().into());

    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::VoteGenerator,
                DetailType::GeneratorBroadcasts,
                Direction::In,
            )
        },
        2,
    );
}
