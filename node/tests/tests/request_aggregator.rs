use rsnano_core::{Amount, PrivateKey, UnsavedBlockLatticeBuilder, DEV_GENESIS_KEY};
use rsnano_messages::ConfirmAck;
use rsnano_node::{
    config::NodeFlags,
    consensus::{AggregatorRequest, VoteGenerationEvent},
    stats::{DetailType, Direction, StatType},
    wallets::WalletsExt,
};
use rsnano_output_tracker::OutputTrackerMt;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    assert_timely2, assert_timely_eq, assert_timely_msg, make_fake_channel, System,
};

#[test]
fn one() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_scan();
    let node = system.build_node().config(config).finish();
    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let mut send1 = lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1000));

    let request = AggregatorRequest {
        roots_hashes: vec![(send1.hash(), send1.root())],
    };

    let channel = make_fake_channel(&node);

    node.request_aggregator
        .request(request.clone(), channel.channel_id());
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator not empty",
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsUnknown,
                Direction::In,
            )
        },
        1,
    );

    // Process and confirm
    node.ledger
        .process(&mut node.ledger.rw_txn(), &mut send1)
        .unwrap();
    node.confirm(send1.hash());

    let channel = make_fake_channel(&node);
    // In the ledger but no vote generated yet
    node.request_aggregator
        .request(request.clone(), channel.channel_id());
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator not empty",
    );
    assert_timely_msg(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            ) > 0
        },
        "no votes generated",
    );

    // Already cached
    // TODO: This is outdated, aggregator should not be using cache
    let dummy_channel = make_fake_channel(&node);
    node.request_aggregator
        .request(request, dummy_channel.channel_id());
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator not empty",
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Aggregator,
                DetailType::AggregatorAccepted,
                Direction::In,
            )
        },
        3,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Aggregator,
                DetailType::AggregatorDropped,
                Direction::In,
            )
        },
        0,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsUnknown,
                Direction::In,
            )
        },
        1,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            )
        },
        2,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsCannotVote,
                Direction::In,
            )
        },
        0,
    );
}

#[test]
fn one_update() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_scan();
    let node = system.build_node().config(config).finish();
    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();

    let send1 = lattice.genesis().send(&key1, Amount::nano(1000));
    node.process(send1.clone()).unwrap();
    node.confirm(send1.hash());

    let send2 = lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1000));
    node.process(send2.clone()).unwrap();
    node.confirm(send2.hash());

    let receive1 = lattice.account(&key1).receive(&send1);
    node.process(receive1.clone()).unwrap();
    node.confirm(receive1.hash());

    let dummy_channel = make_fake_channel(&node);

    let request1 = AggregatorRequest {
        roots_hashes: vec![(send2.hash(), send2.root())],
    };
    node.request_aggregator
        .request(request1, dummy_channel.channel_id());

    // Update the pool of requests with another hash
    let request2 = AggregatorRequest {
        roots_hashes: vec![(receive1.hash(), receive1.root())],
    };
    node.request_aggregator
        .request(request2, dummy_channel.channel_id());

    // In the ledger but no vote generated yet
    assert_timely_msg(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            ) > 0
        },
        "generated votes",
    );
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Aggregator,
                DetailType::AggregatorAccepted,
                Direction::In,
            )
        },
        2,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedHashes,
                Direction::In,
            )
        },
        2,
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsUnknown,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsCachedHashes,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsCachedVotes,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsCannotVote,
            Direction::In,
        ),
        0
    );
}

#[test]
fn two() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_scan();
    let node = system.build_node().config(config).finish();
    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send1 = lattice.genesis().send(&key1, 1);
    let send2 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    let receive1 = lattice.account(&key1).receive(&send1);

    node.process_and_confirm_multi(&[send1, send2.clone(), receive1.clone()]);

    let request = AggregatorRequest {
        roots_hashes: vec![
            (send2.hash(), send2.root()),
            (receive1.hash(), receive1.root()),
        ],
    };
    let dummy_channel = make_fake_channel(&node);

    // Process both blocks
    node.request_aggregator
        .request(request.clone(), dummy_channel.channel_id());
    // One vote should be generated for both blocks
    assert_timely_msg(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            ) > 0
        },
        "generated votes",
    );
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );
    // The same request should now send the cached vote
    node.request_aggregator
        .request(request.clone(), dummy_channel.channel_id());
    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorAccepted,
            Direction::In,
        ),
        2
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In,
        ),
        0
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsUnknown,
                Direction::In,
            )
        },
        0,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedHashes,
                Direction::In,
            )
        },
        4,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            )
        },
        2,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsCannotVote,
                Direction::In,
            )
        },
        0,
    );
    // Make sure the cached vote is for both hashes
    let vote1 = node.history.votes(&send2.root(), &send2.hash(), false);
    let vote2 = node
        .history
        .votes(&receive1.root(), &receive1.hash(), false);
    assert_eq!(vote1.len(), 1);
    assert_eq!(vote2.len(), 1);
    assert!(Arc::ptr_eq(&vote1[0], &vote2[0]));
}

#[test]
fn split() {
    const MAX_VBH: usize = ConfirmAck::HASHES_MAX;
    let mut system = System::new();
    let config = System::default_config_without_backlog_scan();
    let node = system.build_node().config(config).finish();
    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    let mut request = AggregatorRequest {
        roots_hashes: Vec::new(),
    };
    let mut blocks = Vec::new();
    let mut lattice = UnsavedBlockLatticeBuilder::new();

    for _ in 0..=MAX_VBH {
        let block = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
        node.process(block.clone()).unwrap();
        request.roots_hashes.push((block.hash(), block.root()));
        blocks.push(block);
    }
    // Confirm all blocks
    node.confirm(blocks.last().unwrap().hash());
    assert_eq!(node.ledger.cemented_count(), MAX_VBH as u64 + 2);
    assert_eq!(MAX_VBH + 1, request.roots_hashes.len());
    let dummy_channel = make_fake_channel(&node);
    node.request_aggregator
        .request(request, dummy_channel.channel_id());
    // In the ledger but no vote generated yet
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            )
        },
        2,
    );
    assert!(node.request_aggregator.is_empty());
    // Two votes were sent, the first one for 12 hashes and the second one for 1 hash
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorAccepted,
            Direction::In,
        ),
        1
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In,
        ),
        0
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedHashes,
                Direction::In,
            )
        },
        255 + 1,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            )
        },
        2,
    );
}

#[test]
fn channel_max_queue() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_scan();
    config.request_aggregator.max_queue = 0;
    let node = system.build_node().config(config).finish();
    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice
        .genesis()
        .send(&*DEV_GENESIS_KEY, Amount::nano(1000));
    node.process(send1.clone()).unwrap();

    let request = AggregatorRequest {
        roots_hashes: vec![(send1.hash(), send1.root())],
    };
    let channel = make_fake_channel(&node);
    node.request_aggregator
        .request(request.clone(), channel.channel_id());
    node.request_aggregator
        .request(request.clone(), channel.channel_id());

    assert!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In
        ) > 0
    );
}

#[test]
fn cannot_vote() {
    let mut system = System::new();
    let mut flags = NodeFlags::default();
    flags.disable_request_loop = true;
    let node = system.build_node().flags(flags).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    let send2 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    node.process(send1.clone()).unwrap();
    let send2 = node.process(send2.clone()).unwrap();

    node.wallets
        .insert_adhoc2(
            &node.wallets.wallet_ids()[0],
            &DEV_GENESIS_KEY.raw_key(),
            true,
        )
        .unwrap();

    assert_eq!(
        node.ledger
            .dependents_confirmed(&node.ledger.read_txn(), &send2),
        false
    );

    // correct + incorrect
    let request = AggregatorRequest {
        roots_hashes: vec![(send2.hash(), send2.root()), (1.into(), send2.root())],
    };
    let dummy_channel = make_fake_channel(&node);
    node.request_aggregator
        .request(request.clone(), dummy_channel.channel_id());

    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorAccepted,
            Direction::In,
        ),
        1
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In,
        ),
        0
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsNonFinal,
                Direction::In,
            )
        },
        2,
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsGeneratedVotes,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsUnknown,
            Direction::In,
        ),
        0
    );

    // With an ongoing election
    node.election_schedulers.add_manual(send2.clone());
    assert_timely_msg(
        Duration::from_secs(5),
        || node.active.election(&send2.qualified_root()).is_some(),
        "no election",
    );

    node.request_aggregator
        .request(request.clone(), dummy_channel.channel_id());

    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorAccepted,
            Direction::In,
        ),
        2
    );
    assert_eq!(
        node.stats.count(
            StatType::Aggregator,
            DetailType::AggregatorDropped,
            Direction::In,
        ),
        0
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsNonFinal,
                Direction::In,
            )
        },
        4,
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsGeneratedVotes,
            Direction::In,
        ),
        0
    );
    assert_eq!(
        node.stats.count(
            StatType::Requests,
            DetailType::RequestsUnknown,
            Direction::In,
        ),
        0
    );

    // Confirm send1 and send2
    node.confirm(send1.hash());
    node.confirm(send2.hash());

    node.request_aggregator
        .request(request.clone(), dummy_channel.channel_id());

    assert_timely_msg(
        Duration::from_secs(3),
        || node.request_aggregator.is_empty(),
        "aggregator empty",
    );

    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedHashes,
                Direction::In,
            )
        },
        1,
    );
    assert_timely_eq(
        Duration::from_secs(3),
        || {
            node.stats.count(
                StatType::Requests,
                DetailType::RequestsGeneratedVotes,
                Direction::In,
            )
        },
        1,
    );
}

/// Request for a forked open block should return vote for the correct fork alternative
#[test]
fn forked_open() {
    let mut system = System::new();
    let node = system.make_node();

    // Voting needs a rep key set up on the node
    node.insert_into_wallet(&DEV_GENESIS_KEY);

    // Setup two forks of the open block
    let key = PrivateKey::new();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send0 = lattice.genesis().send(&key, 500);
    let mut fork_lattice = lattice.clone();
    let open0 = lattice.account(&key).receive_and_change(&send0, 1);
    let open1 = fork_lattice.account(&key).receive_and_change(&send0, 2);

    node.process(send0).unwrap();
    node.process(open0.clone()).unwrap();
    node.confirm(open0.hash());

    let vote_tracker = node.vote_generators.track();

    let channel = make_fake_channel(&node);

    // Request vote for the wrong fork
    let request = AggregatorRequest {
        roots_hashes: vec![(open1.hash(), open1.root())],
    };
    node.request_aggregator
        .request(request, channel.channel_id());

    let vote_event = wait_vote_event(&vote_tracker);

    assert_eq!(vote_event.blocks.len(), 1);
    // Vote for the correct fork alternative
    assert_eq!(vote_event.blocks[0].hash(), open0.hash());
}

/// Request for a conflicting epoch block should return vote for the correct alternative
#[test]
fn epoch_conflict() {
    let mut system = System::new();
    let node = system.make_node();

    // Voting needs a rep key set up on the node
    node.insert_into_wallet(&DEV_GENESIS_KEY);

    // Setup the initial chain and the conflicting blocks
    let key = PrivateKey::new();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(&key, 1);
    let open = lattice.account(&key).receive(&send);

    // Change block root is the open block hash, qualified root: {open, open}
    let change = lattice.account(&key).change(&key);

    // Pending entry is needed first to process the epoch open block
    let pending = lattice.genesis().send(change.root(), 1);

    // Create conflicting epoch block with the same root as the change block, qualified root: {open, 0}
    // This block is intentionally not processed immediately so the node doesn't know about it
    let epoch_open = lattice.epoch_open(change.root());

    // Process and confirm the initial chain with the change block
    node.process_multi(&[send, open, change.clone()]);
    node.confirm(change.hash());
    assert_timely2(|| node.block_confirmed(&change.hash()));

    let vote_tracker = node.vote_generators.track();
    let channel = make_fake_channel(&node);

    // Request vote for conflicting epoch block
    let request = AggregatorRequest {
        roots_hashes: vec![(epoch_open.hash(), epoch_open.root())],
    };
    node.request_aggregator
        .request(request.clone(), channel.channel_id());

    let vote_event = wait_vote_event(&vote_tracker);

    assert_eq!(vote_event.blocks.len(), 1);
    // Vote for the correct alternative (change block)
    assert_eq!(vote_event.blocks[0].hash(), change.hash());
    vote_tracker.clear();

    // Process the conflicting epoch block
    node.process_multi(&[pending.clone(), epoch_open.clone()]);
    node.confirm_multi(&[pending.clone(), epoch_open.clone()]);

    // Workaround for vote spacing dropping requests with the same root
    // FIXME: Vote spacing should use full qualified root
    std::thread::sleep(Duration::from_secs(1));
    let channel = make_fake_channel(&node);

    // Request vote for the conflicting epoch block again
    node.request_aggregator
        .request(request, channel.channel_id());

    let vote_event = wait_vote_event(&vote_tracker);
    assert_eq!(vote_event.blocks.len(), 1);
    // Vote for epoch block
    assert_eq!(vote_event.blocks[0].hash(), epoch_open.hash());
}

// Request for multiple cemented blocks in a chain should generate votes regardless of vote spacing
#[test]
fn cemented_no_spacing() {
    let mut system = System::new();
    let node = system.make_node();

    // Voting needs a rep key set up on the node
    node.insert_into_wallet(&DEV_GENESIS_KEY);

    // Create a chain of 3 blocks: send1 -> send2 -> send3
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    let send2 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);
    let send3 = lattice.genesis().send(&*DEV_GENESIS_KEY, 1);

    // Process and confirm all blocks in the chain
    node.process_multi(&[send1.clone(), send2.clone(), send3.clone()]);
    node.confirm_multi(&[send1.clone(), send2.clone(), send3.clone()]);

    let vote_tracker = node.vote_generators.track();
    let channel = make_fake_channel(&node);

    // Request votes for blocks at different positions in the chain
    let request = AggregatorRequest {
        roots_hashes: vec![
            (send1.hash(), send1.root()),
            (send2.hash(), send2.root()),
            (send3.hash(), send3.root()),
        ],
    };

    // Request votes for all blocks
    node.request_aggregator
        .request(request, channel.channel_id());

    let vote_event = wait_vote_event(&vote_tracker);

    assert_eq!(vote_event.blocks.len(), 3);
    assert!(vote_event.blocks.iter().any(|b| b.hash() == send1.hash()));
    assert!(vote_event.blocks.iter().any(|b| b.hash() == send2.hash()));
    assert!(vote_event.blocks.iter().any(|b| b.hash() == send3.hash()));
}

fn wait_vote_event(tracker: &OutputTrackerMt<VoteGenerationEvent>) -> VoteGenerationEvent {
    let start = Instant::now();
    loop {
        let output = tracker.output();
        if output.len() > 0 {
            return output[0].clone();
        }

        if start.elapsed() > Duration::from_secs(5) {
            panic!("timeout!");
        }

        std::thread::yield_now();
    }
}
