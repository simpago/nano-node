use rsnano_core::{Amount, PrivateKey, Signature, Vote, VoteCode, VoteSource, DEV_GENESIS_KEY};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY};
use rsnano_network::ChannelId;
use rsnano_node::{
    config::NodeFlags,
    consensus::RepTier,
    stats::{DetailType, Direction, StatType},
    wallets::WalletsExt,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    assert_timely, assert_timely_eq, assert_timely_eq2, setup_chain, start_election, System,
};

#[test]
fn codes() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_scan();
    config.enable_hinted_scheduler = false;
    config.enable_optimistic_scheduler = false;
    let node = system.build_node().config(config).finish();
    let blocks = setup_chain(&node, 1, &DEV_GENESIS_KEY, false);
    let vote = Vote::new(
        &DEV_GENESIS_KEY,
        Vote::TIMESTAMP_MIN,
        0,
        vec![blocks[0].hash()],
    );
    let mut vote_invalid = vote.clone();
    vote_invalid.signature = Signature::new();

    let vote = Arc::new(vote);
    let vote_invalid = Arc::new(vote_invalid);
    let channel_id = ChannelId::from(42);

    // Invalid signature
    assert_eq!(
        VoteCode::Invalid,
        node.vote_processor
            .vote_blocking(&vote_invalid, channel_id, VoteSource::Live)
    );

    // No ongoing election (vote goes to vote cache)
    assert_eq!(
        VoteCode::Indeterminate,
        node.vote_processor
            .vote_blocking(&vote, channel_id, VoteSource::Live)
    );

    // Clear vote cache before starting election
    node.vote_cache.lock().unwrap().clear();

    // First vote from an account for an ongoing election
    start_election(&node, &blocks[0].hash());
    assert_timely(Duration::from_secs(5), || {
        node.active.election(&blocks[0].qualified_root()).is_some()
    });
    let _election = node.active.election(&blocks[0].qualified_root()).unwrap();
    assert_eq!(
        VoteCode::Vote,
        node.vote_processor
            .vote_blocking(&vote, channel_id, VoteSource::Live)
    );

    // Processing the same vote is a replay
    assert_eq!(
        VoteCode::Replay,
        node.vote_processor
            .vote_blocking(&vote, channel_id, VoteSource::Live)
    );

    // Invalid takes precedence
    assert_eq!(
        VoteCode::Invalid,
        node.vote_processor
            .vote_blocking(&vote_invalid, channel_id, VoteSource::Live)
    );

    // Once the election is removed (confirmed / dropped) the vote is again indeterminate
    assert!(node.active.erase(&blocks[0].qualified_root()));
    assert_eq!(
        VoteCode::Indeterminate,
        node.vote_processor
            .vote_blocking(&vote, channel_id, VoteSource::Live)
    );
}

#[test]
fn invalid_signature() {
    let mut system = System::new();
    let node = system.make_node();
    let chain = setup_chain(&node, 1, &DEV_GENESIS_KEY, false);
    let key = PrivateKey::new();
    let vote = Vote::new(&key, Vote::TIMESTAMP_MIN, 0, vec![chain[0].hash()]);
    let mut vote_invalid = vote.clone();
    vote_invalid.signature = Signature::new();

    let vote = Arc::new(vote);
    let vote_invalid = Arc::new(vote_invalid);
    let election = start_election(&node, &chain[0].hash());
    assert_eq!(1, election.vote_count());
    let channel_id = ChannelId::from(42);

    node.vote_processor_queue
        .vote(vote_invalid, channel_id, VoteSource::Live);

    assert_timely_eq(Duration::from_secs(5), || election.vote_count(), 1);

    node.vote_processor_queue
        .vote(vote, channel_id, VoteSource::Live);

    assert_timely_eq(Duration::from_secs(5), || election.vote_count(), 2);
}

#[test]
fn overflow() {
    let mut system = System::new();
    let flags = NodeFlags {
        vote_processor_capacity: 1,
        ..Default::default()
    };
    let node = system.build_node().flags(flags).finish();
    let key = PrivateKey::new();
    let vote = Arc::new(Vote::new(
        &key,
        Vote::TIMESTAMP_MIN,
        0,
        vec![*DEV_GENESIS_HASH],
    ));
    let start_time = Instant::now();
    // No way to lock the processor, but queueing votes in quick succession must result in overflow
    let mut not_processed = 0;
    const TOTAL: usize = 1000;
    for _ in 0..TOTAL {
        if !node
            .vote_processor_queue
            .vote(vote.clone(), ChannelId::from(42), VoteSource::Live)
        {
            not_processed += 1;
        }
    }

    assert!(not_processed > 0);
    assert!(not_processed < TOTAL);
    assert_eq!(
        not_processed as u64,
        node.stats
            .count(StatType::VoteProcessor, DetailType::Overfill, Direction::In)
    );

    // check that it did not timeout
    assert!(start_time.elapsed() < Duration::from_secs(10));
}

/**
 * Test that a vote can encode an empty hash set
 */
#[test]
fn empty_hashes() {
    let key = PrivateKey::new();
    let vote = Arc::new(Vote::new(&key, Vote::TIMESTAMP_MIN, 0, vec![]));

    assert_eq!(vote.voting_account, key.public_key());
    assert_eq!(vote.timestamp, Vote::TIMESTAMP_MIN);
    assert_eq!(vote.hashes.len(), 0);
}

/**
 * basic test to check that the timestamp mask is applied correctly on vote timestamp and duration fields
 */
#[test]
fn timestamp_and_duration_masking() {
    let key = PrivateKey::new();
    let hash = vec![*DEV_GENESIS_HASH];
    let vote = Arc::new(Vote::new(&key, 0x123f, 0xf, hash));

    assert_eq!(vote.timestamp(), 0x1230);
    assert_eq!(vote.duration().as_millis(), 524288);
    assert_eq!(vote.duration_bits(), 0xf);
}

#[test]
fn weights() {
    let mut system = System::new();
    let node0 = system.make_node();
    let node1 = system.make_node();
    let node2 = system.make_node();
    let node3 = system.make_node();

    // Create representatives of different weight levels
    let stake = Amount::MAX;
    let level0 = stake / 5000; // 0.02%
    let level1 = stake / 500; // 0.2%
    let level2 = stake / 50; // 2%

    let key0 = PrivateKey::new();
    let key1 = PrivateKey::new();
    let key2 = PrivateKey::new();

    let wallet_id0 = node0.wallets.wallet_ids()[0];
    let wallet_id1 = node1.wallets.wallet_ids()[0];
    let wallet_id2 = node2.wallets.wallet_ids()[0];
    let wallet_id3 = node3.wallets.wallet_ids()[0];

    node0.insert_into_wallet(&DEV_GENESIS_KEY);
    node1.insert_into_wallet(&key0);
    node2.insert_into_wallet(&key1);
    node3.insert_into_wallet(&key2);

    node1
        .wallets
        .set_representative(wallet_id1, key0.public_key(), false)
        .unwrap();
    node2
        .wallets
        .set_representative(wallet_id2, key1.public_key(), false)
        .unwrap();
    node3
        .wallets
        .set_representative(wallet_id3, key2.public_key(), false)
        .unwrap();

    node0.wallets.send_sync(
        wallet_id0,
        *DEV_GENESIS_ACCOUNT,
        key0.account(),
        level0,
        0,
        true,
        None,
    );
    node0.wallets.send_sync(
        wallet_id0,
        *DEV_GENESIS_ACCOUNT,
        key1.account(),
        level1,
        0,
        true,
        None,
    );

    node0.wallets.send_sync(
        wallet_id0,
        *DEV_GENESIS_ACCOUNT,
        key2.account(),
        level2,
        0,
        true,
        None,
    );

    // Wait for representatives
    assert_timely(Duration::from_secs(10), || {
        node0.ledger.rep_weights.len() == 4
    });

    // Wait for rep tiers to be updated
    node0.stats.clear();
    assert_timely(Duration::from_secs(5), || {
        node0
            .stats
            .count(StatType::RepTiers, DetailType::Updated, Direction::In)
            >= 2
    });

    assert_timely_eq2(|| node0.rep_tiers.tier(&key0.public_key()), RepTier::None);
    assert_timely_eq2(|| node0.rep_tiers.tier(&key1.public_key()), RepTier::Tier1);
    assert_timely_eq2(|| node0.rep_tiers.tier(&key2.public_key()), RepTier::Tier2);
    assert_timely_eq2(
        || node0.rep_tiers.tier(&DEV_GENESIS_PUB_KEY),
        RepTier::Tier3,
    );
}
