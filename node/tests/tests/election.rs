use rsnano_core::{
    Amount, Block, BlockHash, KeyPair, StateBlock, Vote, VoteSource, DEV_GENESIS_KEY,
};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY};
use rsnano_node::{
    config::NodeConfig,
    consensus::ElectionBehavior,
    stats::{DetailType, Direction, StatType},
    wallets::WalletsExt,
};
use std::{sync::Arc, time::Duration};
use test_helpers::{
    assert_timely, assert_timely_eq, get_available_port, setup_chain, start_election, System,
};

// FIXME: this test fails on rare occasions. It needs a review.
#[test]
fn quorum_minimum_update_weight_before_quorum_checks() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let node1 = system.build_node().config(config.clone()).finish();
    let wallet_id1 = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id1, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    let key1 = KeyPair::new();
    let amount = (config.online_weight_minimum / 100
        * node1.online_reps.lock().unwrap().quorum_percent() as u128)
        - Amount::raw(1);

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        amount,
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node1.block(&send1.hash()).is_some()
    });

    let open1 = Block::State(StateBlock::new(
        key1.account(),
        BlockHash::zero(),
        key1.public_key(),
        Amount::MAX - amount,
        send1.hash().into(),
        &key1,
        node1.work_generate_dev(&key1),
    ));
    node1.process(open1.clone()).unwrap();

    let key2 = KeyPair::new();
    let send2 = Block::State(StateBlock::new(
        key1.account(),
        open1.hash(),
        key1.public_key(),
        Amount::raw(3),
        key2.account().into(),
        &key1,
        node1.work_generate_dev(open1.hash()),
    ));
    node1.process(send2.clone()).unwrap();
    assert_timely_eq(Duration::from_secs(5), || node1.ledger.block_count(), 4);

    let config2 = NodeConfig {
        peering_port: Some(get_available_port()),
        ..config
    };
    let node2 = system.build_node().config(config2).finish();
    let wallet_id2 = node2.wallets.wallet_ids()[0];
    node2
        .wallets
        .insert_adhoc2(&wallet_id2, &key1.private_key(), true)
        .unwrap();
    assert_timely_eq(Duration::from_secs(15), || node2.ledger.block_count(), 4);

    assert_timely(Duration::from_secs(5), || {
        node1
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });
    let election = node1
        .active_elections
        .election(&send1.qualified_root())
        .unwrap();
    assert_eq!(1, election.mutex.lock().unwrap().last_blocks.len());

    let vote1 = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send1.hash()]));
    node1.vote_router.vote(&vote1, VoteSource::Live);

    let channel = node1
        .network_info
        .read()
        .unwrap()
        .find_node_id(&node2.get_node_id())
        .unwrap()
        .clone();

    let vote2 = Arc::new(Vote::new_final(&key1, vec![send1.hash()]));
    node1
        .rep_crawler
        .force_process(vote2.clone(), channel.channel_id());

    assert_eq!(node1.active_elections.confirmed(&election), false);
    // Modify online_m for online_reps to more than is available, this checks that voting below updates it to current online reps.
    node1
        .online_reps
        .lock()
        .unwrap()
        .set_online(config.online_weight_minimum + Amount::raw(20));
    node1.vote_router.vote(&vote2, VoteSource::Live);
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.confirmed(&election)
    });
    assert!(node1.block(&send1.hash()).is_some());
}

#[test]
fn continuous_voting() {
    let mut system = System::new();
    let node1 = system.build_node().finish();
    let wallet_id = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    // We want genesis to have just enough voting weight to be a principal rep, but not enough to confirm blocks on their own
    let key1 = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.balance(&*DEV_GENESIS_ACCOUNT) / 10 * 1,
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    node1.process(send1.clone()).unwrap();
    node1.confirm(send1.hash());
    node1.stats.clear();

    // Create a block that should be staying in AEC but not get confirmed
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send1.hash(),
        *DEV_GENESIS_PUB_KEY,
        node1.balance(&*DEV_GENESIS_ACCOUNT) - Amount::raw(1),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(send1.hash()),
    ));

    node1.process(send2.clone()).unwrap();
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.active(&send2)
    });

    // Ensure votes are broadcasted in continuous manner
    assert_timely(Duration::from_secs(5), || {
        node1
            .stats
            .count(StatType::Election, DetailType::BroadcastVote, Direction::In)
            >= 5
    });
}

// checks that block cannot be confirmed if there is no enough votes to reach quorum
#[test]
fn quorum_minimum_confirm_fail() {
    let mut system = System::new();
    let config = NodeConfig {
        online_weight_minimum: Amount::MAX,
        ..System::default_config_without_backlog_population()
    };
    let node1 = system.build_node().config(config).finish();
    let wallet_id = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    let key = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta() - Amount::raw(1),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node1
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });
    let election = node1
        .active_elections
        .election(&send1.qualified_root())
        .unwrap();
    assert_eq!(1, election.mutex.lock().unwrap().last_blocks.len());

    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send1.hash()]));
    node1.vote_router.vote(&vote, VoteSource::Live);

    // Give the election a chance to confirm
    std::thread::sleep(Duration::from_secs(1));

    // It should not confirm because there should not be enough quorum
    assert!(node1.block(&send1.hash()).is_some());
    assert_eq!(node1.active_elections.confirmed(&election), false);
}

// This test ensures blocks can be confirmed precisely at the quorum minimum
#[test]
fn quorum_minimum_confirm_success() {
    let mut system = System::new();
    let config = NodeConfig {
        online_weight_minimum: Amount::MAX,
        ..System::default_config_without_backlog_population()
    };
    let node1 = system.build_node().config(config).finish();
    let wallet_id = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    let key1 = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta(), // Only minimum quorum remains
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node1
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });
    let election = node1
        .active_elections
        .election(&send1.qualified_root())
        .unwrap();
    assert_eq!(1, election.mutex.lock().unwrap().last_blocks.len());

    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send1.hash()]));
    node1.vote_router.vote(&vote, VoteSource::Live);

    assert!(node1.block(&send1.hash()).is_some());
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.confirmed(&election)
    });
}

#[test]
fn quorum_minimum_flip_fail() {
    let mut system = System::new();
    let config = NodeConfig {
        online_weight_minimum: Amount::MAX,
        ..System::default_config_without_backlog_population()
    };
    let node1 = system.build_node().config(config).finish();

    let key1 = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta() - Amount::raw(1),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let key2 = KeyPair::new();
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta() - Amount::raw(1),
        key2.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    // Process send1 and wait until its election appears
    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node1
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });

    // Process send2 and wait until it is added to the existing election
    node1.process_active(send2.clone());
    assert_timely(Duration::from_secs(5), || {
        let election = node1
            .active_elections
            .election(&send2.qualified_root())
            .unwrap();
        let election_guard = election.mutex.lock().unwrap();
        election_guard.last_blocks.len() == 2
    });

    // Genesis generates a final vote for send2 but it should not be enough to reach quorum
    // due to the online_weight_minimum being so high
    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send2.hash()]));
    node1.vote_router.vote(&vote, VoteSource::Live);

    // Give the election some time before asserting it is not confirmed
    std::thread::sleep(Duration::from_secs(1));

    let election = node1
        .active_elections
        .election(&send2.qualified_root())
        .unwrap();
    assert_eq!(node1.active_elections.confirmed(&election), false);
    assert_eq!(node1.block_confirmed(&send2.hash()), false);
}

#[test]
fn quorum_minimum_flip_success() {
    let mut system = System::new();
    let config = NodeConfig {
        online_weight_minimum: Amount::MAX,
        ..System::default_config_without_backlog_population()
    };
    let node1 = system.build_node().config(config).finish();

    let key1 = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta(),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let key2 = KeyPair::new();
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        node1.online_reps.lock().unwrap().quorum_delta(),
        key2.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    // Process send1 and wait until its election appears
    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node1
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });

    // Process send2 and wait until it is added to the existing election
    node1.process_active(send2.clone());
    assert_timely(Duration::from_secs(5), || {
        let election = node1
            .active_elections
            .election(&send2.qualified_root())
            .unwrap();
        let election_guard = election.mutex.lock().unwrap();
        election_guard.last_blocks.len() == 2
    });

    // Genesis generates a final vote for send2
    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send2.hash()]));
    node1.vote_router.vote(&vote, VoteSource::Live);

    // Wait for the election to be confirmed
    let election = node1
        .active_elections
        .election(&send2.qualified_root())
        .unwrap();
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.confirmed(&election)
    });

    // Check that send2 is the winner
    let winner = election.winner_hash();
    assert!(winner.is_some());
    assert_eq!(winner.unwrap(), send2.hash());
}

#[test]
fn election_behavior() {
    let mut system = System::new();
    let node = system.build_node().finish();
    let chain = setup_chain(&node, 1, &DEV_GENESIS_KEY, false);

    let election = start_election(&node, &chain[0].hash());
    assert_eq!(election.behavior, ElectionBehavior::Manual);
}
