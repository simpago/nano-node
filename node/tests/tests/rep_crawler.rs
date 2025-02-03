use rsnano_core::{Amount, PrivateKey, UnsavedBlockLatticeBuilder, Vote, DEV_GENESIS_KEY};
use rsnano_ledger::{
    DEV_GENESIS_ACCOUNT, DEV_GENESIS_BLOCK, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY,
};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::{ChannelId, ChannelMode, TrafficType};
use std::{sync::Arc, time::Duration};
use test_helpers::{assert_always_eq, assert_never, assert_timely_eq, assert_timely_eq2, System};

#[test]
fn ignore_rebroadcast() {
    let mut system = System::new();
    let node1 = system.make_node();
    let node2 = system.make_node();

    let channel1to2 = node1
        .network
        .read()
        .unwrap()
        .find_node_id(&node2.node_id())
        .expect("channel not found 1 to 2")
        .channel_id();

    let channel2to1 = node2
        .network
        .read()
        .unwrap()
        .find_node_id(&node1.node_id())
        .expect("channel not found 2 to 1")
        .channel_id();

    node1
        .rep_crawler
        .force_query(*DEV_GENESIS_HASH, channel1to2);

    assert_always_eq(
        Duration::from_millis(100),
        || node1.online_reps.lock().unwrap().peered_reps_count(),
        0,
    );

    // Now we spam the vote for genesis, so it appears as a rebroadcasted vote
    let vote = Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![*DEV_GENESIS_HASH]);
    node1
        .rep_crawler
        .force_query(*DEV_GENESIS_HASH, channel1to2);

    let tick = || {
        let msg = Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote.clone()));
        node2
            .message_sender
            .lock()
            .unwrap()
            .try_send(channel2to1, &msg, TrafficType::RepCrawler);
        false
    };

    assert_never(Duration::from_secs(1), || {
        tick() || node1.online_reps.lock().unwrap().peered_reps_count() > 0
    })
}

// Votes from local channels should be ignored
#[test]
fn ignore_local() {
    let mut system = System::new();
    let node = system.make_node();

    let vote = Arc::new(Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![*DEV_GENESIS_HASH]));
    node.rep_crawler.force_process(vote, ChannelId::LOOPBACK);
    assert_always_eq(
        Duration::from_millis(500),
        || node.online_reps.lock().unwrap().peered_reps_count(),
        0,
    )
}

#[test]
fn rep_weight() {
    let mut system = System::new();
    let node = system.make_node();
    let node1 = system.make_node();
    let node2 = system.make_node();
    let node3 = system.make_node();
    let key_non_pr = PrivateKey::new();
    let key_pr = PrivateKey::new();
    let amount_pr = Amount::nano(600_000);
    let amount_not_pr = Amount::nano(10_000);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send_non_pr = lattice.genesis().send(&key_non_pr, amount_not_pr);
    let open_no_pr = lattice.account(&key_non_pr).receive(&send_non_pr);
    let send_pr = lattice.genesis().send(&key_pr, amount_pr);
    let open_pr = lattice.account(&key_pr).receive(&send_pr);
    let blocks = [send_non_pr, open_no_pr, send_pr, open_pr];
    node.process_multi(&blocks);
    node1.process_multi(&blocks);
    node2.process_multi(&blocks);
    node3.process_multi(&blocks);
    assert_eq!(node.online_reps.lock().unwrap().online_reps().count(), 0);

    assert_timely_eq2(
        || {
            node.network
                .read()
                .unwrap()
                .count_by_mode(ChannelMode::Realtime)
        },
        3,
    );

    let (channel1, channel2, channel3) = {
        let network = node.network.read().unwrap();
        (
            network
                .find_node_id(&node1.get_node_id())
                .unwrap()
                .channel_id(),
            network
                .find_node_id(&node2.get_node_id())
                .unwrap()
                .channel_id(),
            network
                .find_node_id(&node3.get_node_id())
                .unwrap()
                .channel_id(),
        )
    };

    let vote0 = Arc::new(Vote::new(&DEV_GENESIS_KEY, 0, 0, vec![*DEV_GENESIS_HASH]));
    let vote1 = Arc::new(Vote::new(&key_non_pr, 0, 0, vec![*DEV_GENESIS_HASH]));
    let vote2 = Arc::new(Vote::new(&key_pr, 0, 0, vec![*DEV_GENESIS_HASH]));

    node.rep_crawler.force_process(vote0, channel1);
    node.rep_crawler.force_process(vote1, channel2);
    node.rep_crawler.force_process(vote2, channel3);

    assert_timely_eq2(|| node.online_reps.lock().unwrap().peered_reps_count(), 2);
    // Make sure we get the rep with the most weight first
    let rep = node.online_reps.lock().unwrap().peered_reps()[0].clone();
    assert_eq!(
        node.balance(&DEV_GENESIS_ACCOUNT),
        node.ledger.weight(&rep.account)
    );
    assert_eq!(channel1, rep.channel_id);
    assert_eq!(node.online_reps.lock().unwrap().is_pr(channel1), true);
    assert_eq!(node.online_reps.lock().unwrap().is_pr(channel2), false);
    assert_eq!(node.online_reps.lock().unwrap().is_pr(channel3), true);
}

// Test that nodes can track nodes that have rep weight for priority broadcasting
#[test]
fn rep_list() {
    let mut system = System::new();
    let node1 = system.make_node();
    let node2 = system.make_node();
    assert_eq!(0, node2.online_reps.lock().unwrap().peered_reps_count());
    // Node #1 has a rep
    node1.insert_into_wallet(&DEV_GENESIS_KEY);
    assert_timely_eq(
        Duration::from_secs(5),
        || node2.online_reps.lock().unwrap().peered_reps_count(),
        1,
    );
    assert_eq!(
        *DEV_GENESIS_PUB_KEY,
        node2.online_reps.lock().unwrap().peered_reps()[0].account
    );
}

#[test]
fn rep_connection_close() {
    let mut system = System::new();
    let node1 = system.make_node();
    let node2 = system.make_node();
    // Add working representative (node 2)
    node2.insert_into_wallet(&DEV_GENESIS_KEY);
    assert_timely_eq(
        Duration::from_secs(10),
        || node1.online_reps.lock().unwrap().peered_reps_count(),
        1,
    );
    system.stop_node(node2);
    assert_timely_eq(
        Duration::from_secs(10),
        || node1.online_reps.lock().unwrap().peered_reps_count(),
        0,
    );
}
