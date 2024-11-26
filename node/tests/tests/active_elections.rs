use rsnano_core::{
    utils::MemoryStream, work::WorkPool, Account, Amount, Block, BlockHash, KeyPair, StateBlock,
    Vote, VoteCode, VoteSource, DEV_GENESIS_KEY,
};
use rsnano_ledger::{
    BlockStatus, Writer, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY,
};
use rsnano_network::ChannelId;
use rsnano_node::{
    config::{NodeConfig, NodeFlags},
    consensus::{ActiveElectionsExt, ElectionBehavior},
    stats::{DetailType, Direction, StatType},
    wallets::WalletsExt,
};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
    thread::sleep,
    time::Duration,
    usize,
};
use test_helpers::{
    assert_always_eq, assert_never, assert_timely, assert_timely_eq, assert_timely_msg,
    get_available_port, process_open_block, process_send_block, setup_independent_blocks,
    start_election, start_elections, System,
};

/// What this test is doing:
/// Create 20 representatives with minimum principal weight each
/// Create a send block on the genesis account (the last send block)
/// Create 20 forks of the last send block using genesis as representative (no votes produced)
/// Check that only 10 blocks remain in the election (due to max 10 forks per election object limit)
/// Create 20 more forks of the last send block using the new reps as representatives and produce votes for them
///     (9 votes from this batch should survive and replace existing blocks in the election, why not 10?)
/// Then send winning block and it should replace one of the existing blocks
#[test]
fn fork_replacement_tally() {
    //let mut system = System::new();
    //let mut node_config = System::default_config();
    //node_config.frontiers_confirmation = FrontiersConfirmationMode::Disabled;
    //let node1 = system.build_node().config(node_config).finish();

    //const REPS_COUNT: usize = 20;
    //const MAX_BLOCKS: usize = 10;

    //let keys: Vec<_> = std::iter::repeat_with(|| KeyPair::new())
    //    .take(REPS_COUNT)
    //    .collect();
    //let mut latest = *DEV_GENESIS_HASH;
    //let mut balance = Amount::MAX;
    //let amount = node1.online_reps.lock().unwrap().minimum_principal_weight();

    //// Create 20 representatives & confirm blocks
    //for i in 0..REPS_COUNT {
    //    balance = balance - (amount + Amount::raw(i as u128));
    //    let send = BlockEnum::State(StateBlock::new(
    //        *DEV_GENESIS_ACCOUNT,
    //        latest,
    //        *DEV_GENESIS_ACCOUNT,
    //        balance,
    //        keys[i].public_key().into(),
    //        &DEV_GENESIS_KEY,
    //        system.work.generate_dev2(latest.into()).unwrap(),
    //    ));
    //    node1.process_active(send.clone());
    //    latest = send.hash();
    //    let open = BlockEnum::State(StateBlock::new(
    //        keys[i].public_key(),
    //        BlockHash::zero(),
    //        keys[i].public_key(),
    //        amount + Amount::raw(i as u128),
    //        send.hash().into(),
    //        &keys[i],
    //        system
    //            .work
    //            .generate_dev2(keys[i].public_key().into())
    //            .unwrap(),
    //    ));
    //    node1.process_active(open.clone());
    //    // Confirmation
    //    let vote = Vote::new_final(&DEV_GENESIS_KEY, vec![send.hash(), open.hash()]);
    //    node1
    //        .vote_processor_queue
    //        .vote(Arc::new(vote), channel, source)
    //}
    // TODO port remainig part
}

#[test]
fn inactive_votes_cache_basic() {
    let mut system = System::new();
    let node = system.make_node();
    let key = KeyPair::new();
    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send.hash()]));
    node.vote_processor_queue
        .vote(vote, ChannelId::from(111), VoteSource::Live);
    assert_timely_eq(
        Duration::from_secs(5),
        || node.vote_cache.lock().unwrap().size(),
        1,
    );
    node.process_active(send.clone());
    assert_timely_eq(
        Duration::from_secs(5),
        || node.block_confirmed(&send.hash()),
        true,
    );
    assert_eq!(
        1,
        node.stats
            .count(StatType::ElectionVote, DetailType::Cache, Direction::In)
    )
}

// This test case confirms that a non final vote cannot cause an election to become confirmed
#[test]
fn non_final() {
    let mut system = System::new();
    let node = system.make_node();

    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        Account::from(42).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    // Non-final vote
    let vote = Arc::new(Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![send.hash()]));
    node.vote_processor_queue
        .vote(vote, ChannelId::from(111), VoteSource::Live);
    assert_timely_eq(
        Duration::from_secs(5),
        || node.vote_cache.lock().unwrap().size(),
        1,
    );

    node.process_active(send.clone());

    assert_timely_msg(
        Duration::from_secs(5),
        || {
            node.active_elections
                .election(&send.qualified_root())
                .is_some()
        },
        "election not found",
    );

    let election = node
        .active_elections
        .election(&send.qualified_root())
        .unwrap();
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats
                .count(StatType::ElectionVote, DetailType::Cache, Direction::In)
        },
        1,
    );

    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.active_elections
                .vote_applier
                .tally_impl(&mut election.mutex.lock().unwrap())
                .first_key_value()
                .unwrap()
                .0
                 .0
        },
        Amount::MAX - Amount::raw(100),
    );
    assert_eq!(node.active_elections.confirmed(&election), false);
}

#[test]
fn inactive_votes_cache_fork() {
    let mut system = System::new();
    let node = system.make_node();
    let key = KeyPair::new();

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(200),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send1.hash()]));
    node.vote_processor_queue
        .vote(vote, ChannelId::from(111), VoteSource::Live);

    assert_timely_eq(
        Duration::from_secs(5),
        || node.vote_cache.lock().unwrap().size(),
        1,
    );

    node.process_active(send2.clone());

    assert_timely_msg(
        Duration::from_secs(5),
        || {
            node.active_elections
                .election(&send1.qualified_root())
                .is_some()
        },
        "election not found",
    );

    let election = node
        .active_elections
        .election(&send1.qualified_root())
        .unwrap();

    node.process_active(send1.clone());

    assert_timely_eq(
        Duration::from_secs(5),
        || election.mutex.lock().unwrap().last_blocks.len(),
        2,
    );

    assert_timely_eq(
        Duration::from_secs(5),
        || node.block_confirmed(&send1.hash()),
        true,
    );
    assert_eq!(
        1,
        node.stats
            .count(StatType::ElectionVote, DetailType::Cache, Direction::In)
    )
}

#[test]
fn inactive_votes_cache_existing_vote() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let node = system.build_node().config(config).finish();
    let key = KeyPair::new();
    let rep_weight = Amount::nano(100_000);

    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - rep_weight,
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let open = Block::State(StateBlock::new(
        key.account(),
        BlockHash::zero(),
        key.public_key(),
        rep_weight,
        send.hash().into(),
        &key,
        node.work_generate_dev(&key),
    ));

    node.process(send.clone()).unwrap();
    node.process(open.clone()).unwrap();

    let election = start_election(&node, &send.hash());
    assert!(
        node.ledger.weight(&key.public_key())
            > node.online_reps.lock().unwrap().minimum_principal_weight()
    );

    // Insert vote
    let vote1 = Arc::new(Vote::new(&key, 0, 0, vec![send.hash()]));
    node.vote_processor_queue
        .vote(vote1.clone(), ChannelId::from(111), VoteSource::Live);

    assert_timely_eq(Duration::from_secs(5), || election.vote_count(), 2);

    assert_eq!(
        1,
        node.stats
            .count(StatType::Election, DetailType::Vote, Direction::In)
    );

    let last_vote1 = election
        .mutex
        .lock()
        .unwrap()
        .last_votes
        .get(&key.public_key())
        .unwrap()
        .clone();

    assert_eq!(send.hash(), last_vote1.hash);

    // Attempt to change vote with inactive_votes_cache
    node.vote_cache
        .lock()
        .unwrap()
        .insert(&vote1, rep_weight, &HashMap::new());

    let cached = node.vote_cache.lock().unwrap().find(&send.hash());
    assert_eq!(cached.len(), 1);
    node.vote_router.vote(&cached[0], VoteSource::Live);

    // Check that election data is not changed
    assert_eq!(election.vote_count(), 2);
    let last_vote2 = election
        .mutex
        .lock()
        .unwrap()
        .last_votes
        .get(&key.public_key())
        .unwrap()
        .clone();
    assert_eq!(send.hash(), last_vote2.hash);
    assert_eq!(
        0,
        node.stats
            .count(StatType::ElectionVote, DetailType::Cache, Direction::In)
    );
}

#[test]
fn inactive_votes_cache_multiple_votes() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let node = system.build_node().config(config).finish();
    let key = KeyPair::new();

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(100_000),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send1.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::nano(100_000),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send1.hash()),
    ));

    let open = Block::State(StateBlock::new(
        key.account(),
        BlockHash::zero(),
        key.public_key(),
        Amount::nano(100_000),
        send1.hash().into(),
        &key,
        node.work_generate_dev(&key),
    ));

    // put the blocks in the ledger witout triggering an election
    node.process(send1.clone()).unwrap();
    node.process(send2.clone()).unwrap();
    node.process(open.clone()).unwrap();

    // Process votes
    let vote1 = Arc::new(Vote::new(&key, 0, 0, vec![send1.hash()]));
    node.vote_processor_queue
        .vote(vote1, ChannelId::from(111), VoteSource::Live);

    let vote2 = Arc::new(Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![send1.hash()]));
    node.vote_processor_queue
        .vote(vote2, ChannelId::from(222), VoteSource::Live);

    assert_timely_eq(
        Duration::from_secs(5),
        || node.vote_cache.lock().unwrap().find(&send1.hash()).len(),
        2,
    );
    assert_eq!(1, node.vote_cache.lock().unwrap().size());
    let election = start_election(&node, &send1.hash());
    assert_timely_eq(Duration::from_secs(5), || election.vote_count(), 3); // 2 votes and 1 default not_an_account
    assert_eq!(
        2,
        node.stats
            .count(StatType::ElectionVote, DetailType::Cache, Direction::In)
    );
}

#[test]
fn inactive_votes_cache_election_start() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_population();
    config.optimistic_scheduler.enabled = false;
    config.priority_scheduler_enabled = false;
    let node = system.build_node().config(config).finish();
    let key1 = KeyPair::new();
    let key2 = KeyPair::new();

    // Enough weight to trigger election hinting but not enough to confirm block on its own
    let amount = ((node
        .online_reps
        .lock()
        .unwrap()
        .trended_weight_or_minimum_online_weight()
        / 100)
        * node.config.hinted_scheduler.hinting_threshold_percent as u128)
        / 2
        + Amount::nano(1_000_000);

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - amount,
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send1.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - (amount * 2),
        key2.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send1.hash()),
    ));
    let open1 = Block::State(StateBlock::new(
        key1.account(),
        BlockHash::zero(),
        key1.public_key(),
        amount,
        send1.hash().into(),
        &key1,
        node.work_generate_dev(&key1),
    ));
    let open2 = Block::State(StateBlock::new(
        key2.account(),
        BlockHash::zero(),
        key2.public_key(),
        amount,
        send2.hash().into(),
        &key2,
        node.work_generate_dev(&key2),
    ));
    node.process(send1.clone()).unwrap();
    node.process(send2.clone()).unwrap();
    node.process(open1.clone()).unwrap();
    node.process(open2.clone()).unwrap();

    // These blocks will be processed later
    let send3 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send2.hash(),
        *DEV_GENESIS_PUB_KEY,
        send2.balance() - Amount::raw(1),
        Account::from(2).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send2.hash()),
    ));
    let send4 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send3.hash(),
        *DEV_GENESIS_PUB_KEY,
        send3.balance() - Amount::raw(1),
        Account::from(3).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send3.hash()),
    ));

    // Inactive votes
    let vote1 = Arc::new(Vote::new(
        &key1,
        0,
        0,
        vec![open1.hash(), open2.hash(), send4.hash()],
    ));
    let channel = ChannelId::from(111);
    node.vote_processor_queue
        .vote(vote1, channel, VoteSource::Live);
    assert_timely_eq(
        Duration::from_secs(5),
        || node.vote_cache.lock().unwrap().size(),
        3,
    );
    assert_eq!(node.active_elections.len(), 0);
    assert_eq!(1, node.ledger.cemented_count());

    // 2 votes are required to start election (dev network)
    let vote2 = Arc::new(Vote::new(
        &key2,
        0,
        0,
        vec![open1.hash(), open2.hash(), send4.hash()],
    ));
    node.vote_processor_queue
        .vote(vote2, channel, VoteSource::Live);
    // Only election for send1 should start, other blocks are missing dependencies and don't have enough final weight
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 1);
    assert!(node.vote_router.active(&send1.hash()));

    // Confirm elections with weight quorum
    let vote0 = Arc::new(Vote::new_final(
        &DEV_GENESIS_KEY,
        vec![open1.hash(), open2.hash(), send4.hash()],
    ));
    node.vote_processor_queue
        .vote(vote0, channel, VoteSource::Live);
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 0);
    assert_timely_eq(Duration::from_secs(5), || node.ledger.cemented_count(), 5);
    assert!(node.blocks_confirmed(&[send1, send2, open1, open2]));

    // A late block arrival also checks the inactive votes cache
    assert_eq!(node.active_elections.len(), 0);
    let send4_cache = node.vote_cache.lock().unwrap().find(&send4.hash());
    assert_eq!(3, send4_cache.len());
    node.process_active(send3.clone());
    // An election is started for send6 but does not
    let tx = node.ledger.read_txn();
    assert_eq!(
        node.ledger.confirmed().block_exists(&tx, &send3.hash()),
        false
    );
    assert_eq!(node.confirming_set.exists(&send3.hash()), false);
    // send7 cannot be voted on but an election should be started from inactive votes
    assert_eq!(node.ledger.dependents_confirmed(&tx, &send4), false);
    node.process_active(send4);
    assert_timely_eq(Duration::from_secs(5), || node.ledger.cemented_count(), 7);
}

#[test]
fn republish_winner() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_population();
    let node1 = system.build_node().config(config.clone()).finish();
    config.peering_port = Some(get_available_port());
    let node2 = system.build_node().config(config).finish();

    let key = KeyPair::new();
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    node1.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || node1.block_exists(&send1.hash()));

    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node2
                .stats
                .count(StatType::Message, DetailType::Publish, Direction::In)
        },
        1,
    );

    // Several forks
    for i in 0..5 {
        let fork = Block::State(StateBlock::new(
            *DEV_GENESIS_ACCOUNT,
            *DEV_GENESIS_HASH,
            *DEV_GENESIS_PUB_KEY,
            Amount::MAX - Amount::raw(1 + i),
            key.account().into(),
            &DEV_GENESIS_KEY,
            node1.work_generate_dev(*DEV_GENESIS_HASH),
        ));
        node1.process_active(fork.clone());
        assert_timely(Duration::from_secs(5), || {
            node1.active_elections.active(&fork)
        });
    }

    assert_timely(Duration::from_secs(3), || node1.active_elections.len() > 0);
    assert_eq!(
        1,
        node2
            .stats
            .count(StatType::Message, DetailType::Publish, Direction::In)
    );

    // Process new fork with vote to change winner
    let fork = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(2000),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    node1.process_active(fork.clone());
    assert_timely(Duration::from_secs(5), || {
        node1.vote_router.active(&fork.hash())
    });

    let election = node1
        .active_elections
        .election(&fork.qualified_root())
        .unwrap();
    let vote = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![fork.hash()]));
    node1
        .vote_processor_queue
        .vote(vote, ChannelId::from(111), VoteSource::Live);
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.confirmed(&election)
    });

    assert_eq!(fork.hash(), election.winner_hash().unwrap());

    assert_timely(Duration::from_secs(5), || {
        node2.block_confirmed(&fork.hash())
    });
}

/*
 * Tests that an election can be confirmed as the result of a confirmation request
 *
 * Set-up:
 * - node1 with:
 * 		- enabled frontiers_confirmation (default) -> allows it to confirm blocks and subsequently generates votes
 * - node2 with:
 * 		- disabled rep crawler -> this inhibits node2 from learning that node1 is a rep
 */
#[test]
fn confirm_election_by_request() {
    let mut system = System::new();
    let node1 = system.make_node();

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        1.into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    // Process send1 locally on node1
    node1.process(send1.clone()).unwrap();

    // Add rep key to node1
    let wallet_id = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    // Ensure election on node1 is already confirmed before connecting with node2
    assert_timely(Duration::from_secs(5), || {
        node1.block_confirmed(&send1.hash())
    });

    // Wait for the election to be removed and give time for any in-flight vote broadcasts to settle
    assert_timely(Duration::from_secs(5), || node1.active_elections.len() == 0);
    sleep(Duration::from_secs(1));

    // At this point node1 should not generate votes for send1 block unless it receives a request

    // Create a second node
    let flags = NodeFlags {
        disable_rep_crawler: true,
        ..Default::default()
    };
    let node2 = system.build_node().flags(flags).finish();

    // Process send1 block as live block on node2, this should start an election
    node2.process_active(send1.clone());

    // Ensure election is started on node2
    assert_timely(Duration::from_secs(5), || {
        node2
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });

    let election = node2
        .active_elections
        .election(&send1.qualified_root())
        .unwrap();

    // Ensure election on node2 did not get confirmed without us requesting votes
    sleep(Duration::from_secs(1));
    assert_eq!(node2.active_elections.confirmed(&election), false);

    // Expect that node2 has nobody to send a confirmation_request to (no reps)
    assert_eq!(
        election.confirmation_request_count.load(Ordering::SeqCst),
        0
    );

    // Get random peer list (of size 1) from node2 -- so basically just node2
    let peers = node2
        .network_info
        .read()
        .unwrap()
        .random_realtime_channels(1, 0);
    assert_eq!(peers.is_empty(), false);

    // Add representative (node1) to disabled rep crawler of node2
    node2.online_reps.lock().unwrap().vote_observed_directly(
        *DEV_GENESIS_PUB_KEY,
        peers[0].channel_id(),
        node2.steady_clock.now(),
    );

    // Expect a vote to come back
    assert_timely(Duration::from_secs(5), || election.vote_count() >= 1);

    // There needs to be at least one request to get the election confirmed,
    // Rep has this block already confirmed so should reply with final vote only
    assert_timely(Duration::from_secs(5), || {
        election.confirmation_request_count.load(Ordering::SeqCst) >= 1
    });

    // Expect election was confirmed
    assert_timely(Duration::from_secs(5), || {
        node2.active_elections.confirmed(&election)
    });
    assert_timely(Duration::from_secs(5), || {
        node1.block_confirmed(&send1.hash())
    });
    assert_timely(Duration::from_secs(5), || {
        node2.block_confirmed(&send1.hash())
    });
}

#[test]
fn confirm_frontier() {
    let mut system = System::new();

    // send 100 raw from genesis to a random account
    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        1.into(),
        &DEV_GENESIS_KEY,
        system
            .work
            .generate_dev2((*DEV_GENESIS_HASH).into())
            .unwrap(),
    ));

    // Voting node
    let flags = NodeFlags {
        disable_request_loop: true,
        disable_ongoing_bootstrap: true,
        disable_ascending_bootstrap: true,
        ..Default::default()
    };
    let node1 = system.build_node().flags(flags).finish();
    let wallet_id = node1.wallets.wallet_ids()[0];
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    node1.process(send.clone()).unwrap();
    node1.confirm(send.hash());

    // The rep crawler would otherwise request confirmations in order to find representatives
    let flags2 = NodeFlags {
        disable_ongoing_bootstrap: true,
        disable_ascending_bootstrap: true,
        disable_rep_crawler: true,
        ..Default::default()
    };
    // start node2 later so that we do not get the gossip traffic
    let node2 = system.build_node().flags(flags2).finish();

    // Add representative to disabled rep crawler
    let peers = node2
        .network_info
        .read()
        .unwrap()
        .random_realtime_channels(1, 0);
    assert!(!peers.is_empty());
    node2.online_reps.lock().unwrap().vote_observed_directly(
        *DEV_GENESIS_PUB_KEY,
        peers[0].channel_id(),
        node2.steady_clock.now(),
    );

    node2.process(send.clone()).unwrap();
    assert_timely(Duration::from_secs(5), || node2.active_elections.len() > 0);

    // Save election to check request count afterwards
    assert_timely(Duration::from_secs(5), || {
        node2
            .active_elections
            .election(&send.qualified_root())
            .is_some()
    });
    let election2 = node2
        .active_elections
        .election(&send.qualified_root())
        .unwrap();

    assert_timely(Duration::from_secs(5), || {
        node2.block_confirmed(&send.hash())
    });
    assert_timely_eq(Duration::from_secs(5), || node2.ledger.cemented_count(), 2);
    assert_timely(Duration::from_secs(5), || node2.active_elections.len() == 0);
    assert!(election2.confirmation_request_count.load(Ordering::SeqCst) > 0);
}

#[test]
fn vacancy() {
    let mut system = System::new();
    let mut config = System::default_config();
    config.active_elections.size = 1;
    let node = system.build_node().config(config).finish();
    let notify_tracker = node.election_schedulers.track_notify();

    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        (*DEV_GENESIS_ACCOUNT).into(),
        &DEV_GENESIS_KEY,
        system
            .work
            .generate_dev2((*DEV_GENESIS_HASH).into())
            .unwrap(),
    ));
    node.process(send.clone()).unwrap();
    assert_eq!(1, node.active_elections.vacancy(ElectionBehavior::Priority),);
    assert_eq!(0, node.active_elections.len());
    let election1 = start_election(&node, &send.hash());
    assert_timely(Duration::from_secs(1), || notify_tracker.output().len() > 0);
    notify_tracker.clear();
    assert_eq!(0, node.active_elections.vacancy(ElectionBehavior::Priority));
    assert_eq!(1, node.active_elections.len());
    node.active_elections.force_confirm(&election1);
    assert_timely(Duration::from_secs(1), || notify_tracker.output().len() > 0);
    assert_eq!(1, node.active_elections.vacancy(ElectionBehavior::Priority));
    assert_eq!(0, node.active_elections.len());
}

/// Ensures that election winners set won't grow without bounds when cementing
/// is slower that the rate of confirming new elections
#[test]
fn bound_election_winners() {
    let mut system = System::new();
    let mut config = System::default_config();
    // Set election winner limit to a low value
    config.active_elections.max_election_winners = 5;
    let node = system.build_node().config(config).finish();

    // Start elections for a couple of blocks, number of elections is larger than the election winner set limit
    let blocks = setup_independent_blocks(&node, 10, &DEV_GENESIS_KEY);
    assert_timely(Duration::from_secs(5), || {
        blocks
            .iter()
            .all(|block| node.active_elections.active(block))
    });

    {
        // Prevent cementing of confirmed blocks
        let _write_guard = node.ledger.write_queue.wait(Writer::Testing);
        let _tx = node.ledger.rw_txn();

        // Ensure that when the number of election winners reaches the limit, AEC vacancy reflects that
        assert!(node.active_elections.vacancy(ElectionBehavior::Priority) > 0);

        for index in 0..node.config.active_elections.max_election_winners {
            let election = node.vote_router.election(&blocks[index].hash()).unwrap();
            node.active_elections.force_confirm(&election);
        }

        assert_timely_eq(
            Duration::from_secs(5),
            || node.active_elections.vacancy(ElectionBehavior::Priority),
            0,
        );

        // Confirming more elections should make the vacancy negative
        for index in 0..blocks.len() {
            let election = node.vote_router.election(&blocks[index].hash()).unwrap();
            node.active_elections.force_confirm(&election);
        }

        assert_timely(Duration::from_secs(5), || {
            node.active_elections.vacancy(ElectionBehavior::Priority) < 0
        });
        // Release the guard to allow cementing, there should be some vacancy now
    }

    assert_timely(Duration::from_secs(5), || {
        node.active_elections.vacancy(ElectionBehavior::Priority) > 0
    });
}

/// Blocks should only be broadcasted when they are active in the AEC
#[test]
fn broadcast_block_on_activation() {
    let mut system = System::new();
    let mut config1 = System::default_config();
    // Deactivates elections on both nodes.
    config1.active_elections.size = 0;
    config1.bootstrap_ascending.enable = false;

    let mut config2 = System::default_config();
    config2.active_elections.size = 0;
    config2.bootstrap_ascending.enable = false;

    // Disables bootstrap listener to make sure the block won't be shared by this channel.
    let flags = NodeFlags {
        disable_bootstrap_listener: true,
        ..Default::default()
    };

    let node1 = system
        .build_node()
        .config(config1)
        .flags(flags.clone())
        .finish();
    let node2 = system.build_node().config(config2).flags(flags).finish();

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        (*DEV_GENESIS_ACCOUNT).into(),
        &DEV_GENESIS_KEY,
        system
            .work
            .generate_dev2((*DEV_GENESIS_HASH).into())
            .unwrap(),
    ));

    // Adds a block to the first node
    node1.process_active(send1.clone());

    // The second node should not have the block
    assert_never(Duration::from_millis(500), || {
        node2.block(&send1.hash()).is_some()
    });

    // Activating the election should broadcast the block
    node1.election_schedulers.add_manual(send1.clone().into());
    assert_timely(Duration::from_secs(5), || {
        node1.active_elections.active_root(&send1.qualified_root())
    });
    assert_timely(Duration::from_secs(5), || node2.block_exists(&send1.hash()));
}

// Tests that blocks are correctly cleared from the duplicate filter for unconfirmed elections
#[test]
fn dropped_cleanup() {
    let mut system = System::new();
    let flags = NodeFlags {
        disable_request_loop: true,
        ..Default::default()
    };
    let node = system.build_node().flags(flags).finish();
    let chain = setup_independent_blocks(&node, 1, &DEV_GENESIS_KEY);
    let hash = chain[0].hash();
    let qual_root = chain[0].qualified_root();

    // Add to network filter to ensure proper cleanup after the election is dropped
    let mut stream = MemoryStream::new();
    chain[0].serialize(&mut stream);
    let block_bytes = stream.as_bytes();
    assert!(!node.network_filter.apply(&block_bytes).1);
    assert!(node.network_filter.apply(&block_bytes).1);

    let election = start_election(&node, &hash);

    // Not yet removed
    assert!(node.network_filter.apply(&block_bytes).1);
    assert!(node.active_elections.election(&qual_root).is_some());

    // Now simulate dropping the election
    assert!(!node.active_elections.confirmed(&election));
    node.active_elections.erase(&qual_root);

    // The filter must have been cleared
    assert!(node.network_filter.apply(&block_bytes).1);

    // An election was recently dropped
    assert_eq!(
        1,
        node.stats.count(
            StatType::ActiveElectionsDropped,
            DetailType::Manual,
            Direction::In
        )
    );

    // Block cleared from active
    assert!(node.active_elections.election(&qual_root).is_none());

    // Repeat test for a confirmed election
    assert!(node.network_filter.apply(&block_bytes).1);

    let election = start_election(&node, &hash);
    node.active_elections.force_confirm(&election);
    assert_timely(Duration::from_secs(5), || {
        node.active_elections.confirmed(&election)
    });
    node.active_elections.erase(&qual_root);

    // The filter should not have been cleared
    assert!(node.network_filter.apply(&block_bytes).1);

    // Not dropped
    assert_eq!(
        1,
        node.stats.count(
            StatType::ActiveElectionsDropped,
            DetailType::Manual,
            Direction::In
        )
    );

    // Block cleared from active
    assert!(node.active_elections.election(&qual_root).is_none());
}

#[test]
fn confirmation_consistency() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let node = system.build_node().config(config).finish();
    let wallet_id = node.wallets.wallet_ids()[0];
    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    for i in 0..10 {
        let block = node
            .wallets
            .send_action2(
                &wallet_id,
                *DEV_GENESIS_ACCOUNT,
                Account::from(0),
                node.config.receive_minimum,
                0,
                true,
                None,
            )
            .unwrap();

        assert_timely(Duration::from_secs(5), || {
            node.block_confirmed(&block.hash())
        });

        assert_timely(Duration::from_secs(1), || {
            let recently_confirmed_size = node.active_elections.recently_confirmed_count();
            let latest_recently_confirmed_root = node.active_elections.latest_recently_confirmed();
            let recently_cemented_size = node.active_elections.recently_cemented_list();

            recently_confirmed_size == i + 1
                && latest_recently_confirmed_root == Some((block.qualified_root(), block.hash()))
                && recently_cemented_size.len() == i + 1
        });
    }
}

#[test]
fn fork_filter_cleanup() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_population();
    let node1 = system.build_node().config(config.clone()).finish();

    let key = KeyPair::new();
    let latest_hash = *DEV_GENESIS_HASH;

    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        latest_hash,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(latest_hash),
    ));

    let mut stream = MemoryStream::new();
    send1.serialize(&mut stream);
    let send_block_bytes = stream.as_bytes();

    // Generate 10 forks to prevent new block insertion to election
    for i in 0..10 {
        let fork = Block::State(StateBlock::new(
            *DEV_GENESIS_ACCOUNT,
            latest_hash,
            *DEV_GENESIS_PUB_KEY,
            Amount::MAX - Amount::raw(1 + i),
            key.account().into(),
            &DEV_GENESIS_KEY,
            node1.work_generate_dev(latest_hash),
        ));

        node1.process_active(fork.clone());
        assert_timely(Duration::from_secs(5), || {
            node1
                .active_elections
                .election(&fork.qualified_root())
                .is_some()
        });
    }

    // All forks were merged into the same election
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
    assert_timely_eq(
        Duration::from_secs(5),
        || election.mutex.lock().unwrap().last_blocks.len(),
        10,
    );
    assert_eq!(1, node1.active_elections.len());

    // Instantiate a new node
    config.peering_port = Some(get_available_port());
    let node2 = system.build_node().config(config).finish();

    // Process the first initial block on node2
    node2.process_active(send1.clone());
    assert_timely(Duration::from_secs(5), || {
        node2
            .active_elections
            .election(&send1.qualified_root())
            .is_some()
    });

    // TODO: questions: why doesn't node2 pick up "fork" from node1? because it connected to node1 after node1
    //                  already process_active()d the fork? shouldn't it broadcast it anyway, even later?
    //
    //                  how about node1 picking up "send1" from node2? we know it does because we assert at
    //                  the end that it is within node1's AEC, but why node1.block_count doesn't increase?
    //
    assert_timely_eq(Duration::from_secs(5), || node2.ledger.block_count(), 2);
    assert_timely_eq(Duration::from_secs(5), || node1.ledger.block_count(), 2);

    // Block is erased from the duplicate filter
    assert_timely(Duration::from_secs(5), || {
        !node1.network_filter.apply(&send_block_bytes).1
    });
}

// Ensures votes are tallied on election::publish even if no vote is inserted through inactive_votes_cache
#[test]
fn conflicting_block_vote_existing_election() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let flags = NodeFlags {
        disable_request_loop: true,
        ..Default::default()
    };
    let node = system.build_node().config(config).flags(flags).finish();
    let key = KeyPair::new();

    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let fork = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(200),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let vote_fork = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![fork.hash()]));

    assert_eq!(
        node.process_local(send.clone()).unwrap(),
        BlockStatus::Progress
    );
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 1);

    // Vote for conflicting block, but the block does not yet exist in the ledger
    node.vote_processor_queue
        .vote(vote_fork, ChannelId::from(111), VoteSource::Live);

    // Block now gets processed
    assert_eq!(node.process_local(fork.clone()).unwrap(), BlockStatus::Fork);

    // Election must be confirmed
    assert_timely(Duration::from_secs(5), || {
        node.active_elections
            .election(&fork.qualified_root())
            .is_some()
    });
    let election = node
        .active_elections
        .election(&fork.qualified_root())
        .unwrap();
    assert_timely(Duration::from_secs(3), || {
        node.active_elections.confirmed(&election)
    });
}

#[test]
fn activate_account_chain() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_population();
    let node = system.build_node().config(config).finish();

    let key = KeyPair::new();
    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(1),
        (*DEV_GENESIS_ACCOUNT).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(2),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send.hash()),
    ));
    let send3 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send2.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(3),
        key.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send2.hash()),
    ));
    let open = Block::State(StateBlock::new(
        key.account(),
        BlockHash::zero(),
        key.public_key(),
        Amount::raw(1),
        send2.hash().into(),
        &key,
        node.work_generate_dev(&key),
    ));
    let receive = Block::State(StateBlock::new(
        key.account(),
        open.hash(),
        key.public_key(),
        Amount::raw(2),
        send3.hash().into(),
        &key,
        node.work_generate_dev(open.hash()),
    ));

    assert_eq!(
        node.process_local(send.clone()).unwrap(),
        BlockStatus::Progress
    );
    assert_eq!(
        node.process_local(send2.clone()).unwrap(),
        BlockStatus::Progress
    );
    assert_eq!(
        node.process_local(send3.clone()).unwrap(),
        BlockStatus::Progress
    );
    assert_eq!(
        node.process_local(open.clone()).unwrap(),
        BlockStatus::Progress
    );
    assert_eq!(
        node.process_local(receive.clone()).unwrap(),
        BlockStatus::Progress
    );

    let election1 = start_election(&node, &send.hash());
    assert_eq!(1, node.active_elections.len());
    assert!(election1
        .mutex
        .lock()
        .unwrap()
        .last_blocks
        .contains_key(&send.hash()));
    node.active_elections.force_confirm(&election1);
    assert_timely(Duration::from_secs(3), || {
        node.block_confirmed(&send.hash())
    });

    // On cementing, the next election is started
    assert_timely(Duration::from_secs(3), || {
        node.active_elections.active_root(&send2.qualified_root())
    });
    let election3 = node
        .active_elections
        .election(&send2.qualified_root())
        .unwrap();
    assert!(election3
        .mutex
        .lock()
        .unwrap()
        .last_blocks
        .contains_key(&send2.hash()));
    node.active_elections.force_confirm(&election3);
    assert_timely(Duration::from_secs(3), || {
        node.block_confirmed(&send2.hash())
    });

    // On cementing, the next election is started
    assert_timely(Duration::from_secs(3), || {
        node.active_elections.active_root(&open.qualified_root())
    }); // Destination account activated
    assert_timely(Duration::from_secs(3), || {
        node.active_elections.active_root(&send3.qualified_root())
    }); // Block successor activated
    let election4 = node
        .active_elections
        .election(&send3.qualified_root())
        .unwrap();
    assert!(election4
        .mutex
        .lock()
        .unwrap()
        .last_blocks
        .contains_key(&send3.hash()));
    let election5 = node
        .active_elections
        .election(&open.qualified_root())
        .unwrap();
    assert!(election5
        .mutex
        .lock()
        .unwrap()
        .last_blocks
        .contains_key(&open.hash()));
    node.active_elections.force_confirm(&election5);
    assert_timely(Duration::from_secs(3), || {
        node.block_confirmed(&open.hash())
    });

    // Until send3 is also confirmed, the receive block should not activate
    sleep(Duration::from_millis(200));
    assert!(!node.active_elections.active_root(&receive.qualified_root()));
    node.active_elections.force_confirm(&election4);
    assert_timely(Duration::from_secs(3), || {
        node.block_confirmed(&send3.hash())
    });
    assert_timely(Duration::from_secs(3), || {
        node.active_elections.active_root(&receive.qualified_root())
    }); // Destination account activated
}

#[test]
fn list_active() {
    let mut system = System::new();
    let node = system.make_node();

    let key = KeyPair::new();

    let send = process_send_block(node.clone(), *DEV_GENESIS_ACCOUNT, Amount::raw(1));

    let send2 = process_send_block(node.clone(), key.account(), Amount::raw(1));

    let open = process_open_block(node.clone(), key);

    start_elections(&node, &[send.hash(), send2.hash(), open.hash()], false);
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 3);

    assert_eq!(node.active_elections.list_active(1).len(), 1);
    assert_eq!(node.active_elections.list_active(2).len(), 2);
    assert_eq!(node.active_elections.list_active(3).len(), 3);
    assert_eq!(node.active_elections.list_active(4).len(), 3);
    assert_eq!(node.active_elections.list_active(99999).len(), 3);
    assert_eq!(node.active_elections.list_active(usize::MAX).len(), 3);

    node.active_elections.list_active(usize::MAX);
}

#[test]
fn vote_replays() {
    let mut system = System::new();
    let node = system
        .build_node()
        .config(NodeConfig {
            enable_voting: false,
            ..System::default_config_without_backlog_population()
        })
        .finish();
    let key = KeyPair::new();

    // send 1000 nano from genesis to key
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        (&key).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    // create open block for key receing 1000 nano
    let open1 = Block::State(StateBlock::new(
        key.public_key().as_account(),
        BlockHash::zero(),
        key.public_key(),
        Amount::nano(1000),
        send1.hash().into(),
        &key,
        node.work_generate_dev(&key),
    ));

    // wait for elections objects to appear in the AEC
    node.process_active(send1.clone());
    node.process_active(open1.clone());
    start_elections(&node, &[send1.hash(), open1.hash()], false);
    assert_eq!(node.active_elections.len(), 2);

    // First vote is not a replay and confirms the election, second vote should be a replay since the election has confirmed but not yet removed
    let vote_send1 = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send1.hash()]));
    assert_eq!(
        node.vote_router
            .vote(&vote_send1, VoteSource::Live)
            .get(&send1.hash())
            .unwrap(),
        &VoteCode::Vote
    );
    assert_eq!(
        node.vote_router
            .vote(&vote_send1, VoteSource::Live)
            .get(&send1.hash())
            .unwrap(),
        &VoteCode::Replay
    );

    // Wait until the election is removed, at which point the vote is still a replay since it's been recently confirmed
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 1);
    assert_eq!(
        node.vote_router
            .vote(&vote_send1, VoteSource::Live)
            .get(&send1.hash())
            .unwrap(),
        &VoteCode::Replay
    );

    // Open new account
    let vote_open1 = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![open1.hash()]));
    assert_eq!(
        node.vote_router
            .vote(&vote_open1, VoteSource::Live)
            .get(&open1.hash())
            .unwrap(),
        &VoteCode::Vote
    );
    assert_eq!(
        node.vote_router
            .vote(&vote_open1, VoteSource::Live)
            .get(&open1.hash())
            .unwrap(),
        &VoteCode::Replay
    );

    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 0);

    assert_eq!(
        node.vote_router
            .vote(&vote_open1, VoteSource::Live)
            .get(&open1.hash())
            .unwrap(),
        &VoteCode::Replay
    );
    assert_eq!(node.ledger.weight(&key.public_key()), Amount::nano(1000));

    // send 1 raw to key to key
    let send2 = Block::State(StateBlock::new(
        key.public_key().as_account(),
        open1.hash(),
        key.public_key(),
        Amount::nano(999),
        (&key).into(),
        &key,
        node.work_generate_dev(open1.hash()),
    ));
    node.process_active(send2.clone());
    start_elections(&node, &[send2.hash()], false);
    assert_eq!(node.active_elections.len(), 1);

    // vote2_send2 is a non final vote with little weight, vote1_send2 is the vote that confirms the election
    let vote1_send2 = Arc::new(Vote::new_final(&DEV_GENESIS_KEY, vec![send2.hash()]));
    let vote2_send2 = Arc::new(Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![send2.hash()]));

    // this vote cannot confirm the election
    assert_eq!(
        node.vote_router
            .vote(&vote2_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Vote
    );
    assert_eq!(node.active_elections.len(), 1);

    // this vote confirms the election
    assert_eq!(
        node.vote_router
            .vote(&vote1_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Vote
    );

    // this should still return replay, either because the election is still in the AEC or because it is recently confirmed
    assert_eq!(
        node.vote_router
            .vote(&vote1_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Replay
    );
    assert_timely_eq(Duration::from_secs(5), || node.active_elections.len(), 0);
    assert_eq!(
        node.vote_router
            .vote(&vote1_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Replay
    );
    assert_eq!(
        node.vote_router
            .vote(&vote2_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Replay
    );

    // Removing blocks as recently confirmed makes every vote indeterminate
    node.active_elections.clear_recently_confirmed();
    assert_eq!(
        node.vote_router
            .vote(&vote_send1, VoteSource::Live)
            .get(&send1.hash())
            .unwrap(),
        &VoteCode::Indeterminate
    );
    assert_eq!(
        node.vote_router
            .vote(&vote_open1, VoteSource::Live)
            .get(&open1.hash())
            .unwrap(),
        &VoteCode::Indeterminate
    );
    assert_eq!(
        node.vote_router
            .vote(&vote1_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Indeterminate
    );
    assert_eq!(
        node.vote_router
            .vote(&vote2_send2, VoteSource::Live)
            .get(&send2.hash())
            .unwrap(),
        &VoteCode::Indeterminate
    );
}

#[test]
fn confirm_new() {
    let mut system = System::new();
    let node1 = system.make_node();
    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(100),
        1.into(),
        &DEV_GENESIS_KEY,
        node1.work_generate_dev(*DEV_GENESIS_HASH),
    ));
    node1.process_active(send.clone());
    assert_timely_eq(Duration::from_secs(5), || node1.active_elections.len(), 1);
    let node2 = system.make_node();
    // Add key to node2
    node2.insert_into_wallet(&DEV_GENESIS_KEY);
    // Let node2 know about the block
    assert_timely(Duration::from_secs(5), || node2.block_exists(&send.hash()));
    // Wait confirmation
    assert_timely_eq(Duration::from_secs(5), || node1.ledger.cemented_count(), 2);
    assert_timely_eq(Duration::from_secs(5), || node2.ledger.cemented_count(), 2);
}

#[test]
#[ignore = "TODO"]
/*
 * Ensures we limit the number of vote hinted elections in AEC
 */
fn limit_vote_hinted_elections() {
    // disabled because it doesn't run after tokio switch
    // TODO reimplement in Rust
}

#[test]
fn active_inactive() {
    let mut system = System::new();
    let node = system
        .build_node()
        .config(System::default_config_without_backlog_population())
        .finish();

    let key = KeyPair::new();

    let send = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(1),
        (&key).into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    let send2 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(2),
        1.into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send.hash()),
    ));

    let open = Block::State(StateBlock::new(
        (&key).into(),
        BlockHash::zero(),
        key.public_key(),
        Amount::raw(1),
        send.hash().into(),
        &key,
        node.work_generate_dev(&key),
    ));

    node.process_multi(&[send.clone(), send2.clone(), open]);

    let election = start_election(&node, &send2.hash());
    node.active_elections.force_confirm(&election);

    assert_timely(Duration::from_secs(5), || {
        !node.confirming_set.exists(&send2.hash())
    });
    assert_timely(Duration::from_secs(5), || {
        node.block_confirmed(&send2.hash())
    });
    assert_timely(Duration::from_secs(5), || {
        node.block_confirmed(&send.hash())
    });

    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::InactiveConfHeight,
                Direction::Out,
            )
        },
        1,
    );
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            )
        },
        1,
    );
    assert_always_eq(
        Duration::from_millis(50),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::ActiveConfHeight,
                Direction::Out,
            )
        },
        0,
    );
}
