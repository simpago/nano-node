use rsnano_messages::{Message, TelemetryAck};
use rsnano_node::{
    config::NodeFlags,
    stats::{DetailType, Direction, StatType},
};
use std::{net::SocketAddrV6, thread::sleep, time::Duration};
use test_helpers::{
    assert_always_eq, assert_never, assert_timely, assert_timely2, make_fake_channel, System,
};

#[test]
fn invalid_signature() {
    let mut system = System::new();
    let node = system.make_node();

    let mut telemetry = node.telemetry.local_telemetry();
    telemetry.block_count = 9999; // Change data so signature is no longer valid
    let node_id = telemetry.node_id;
    let message = Message::TelemetryAck(TelemetryAck(Some(telemetry)));

    let channel = make_fake_channel(&node);
    node.network
        .read()
        .unwrap()
        .set_node_id(channel.channel_id(), node_id);
    node.inbound_message_queue.put(message, channel);

    assert_timely(Duration::from_secs(5), || {
        node.stats.count(
            StatType::Telemetry,
            DetailType::InvalidSignature,
            Direction::In,
        ) > 0
    });
    assert_never(Duration::from_secs(1), || {
        node.stats
            .count(StatType::Telemetry, DetailType::Process, Direction::In)
            > 0
    });
}

#[test]
fn basic() {
    let mut system = System::new();
    let node_client = system.make_node();
    let node_server = system.make_node();

    // Request telemetry metrics
    let channel = node_client
        .network
        .read()
        .unwrap()
        .find_node_id(&node_server.get_node_id())
        .unwrap()
        .clone();

    assert_timely2(|| {
        node_client
            .telemetry
            .get_telemetry(&channel.peer_addr())
            .is_some()
    });
    let telemetry_data = node_client
        .telemetry
        .get_telemetry(&channel.peer_addr())
        .unwrap();
    assert_eq!(node_server.get_node_id(), telemetry_data.node_id);

    // Check the metrics are correct
    // TODO

    // Call again straight away
    let telemetry_data2 = node_client
        .telemetry
        .get_telemetry(&channel.peer_addr())
        .unwrap();

    // Call again straight away
    let telemetry_data3 = node_client
        .telemetry
        .get_telemetry(&channel.peer_addr())
        .unwrap();

    // we expect at least one consecutive repeat of telemetry
    assert!(telemetry_data == telemetry_data2 || telemetry_data2 == telemetry_data3);

    // Wait the cache period and check cache is not used
    sleep(Duration::from_secs(3));

    let telemetry_data4 = node_client
        .telemetry
        .get_telemetry(&channel.peer_addr())
        .unwrap();

    assert_ne!(telemetry_data, telemetry_data4);
}

#[test]
fn disconnected() {
    let mut system = System::new();
    let node_client = system.make_node();
    let node_server = system.make_node();

    // Request telemetry metrics
    let channel = node_client
        .network
        .read()
        .unwrap()
        .find_node_id(&node_server.get_node_id())
        .unwrap()
        .clone();

    // Ensure telemetry is available before disconnecting
    assert_timely(Duration::from_secs(5), || {
        node_client
            .telemetry
            .get_telemetry(&channel.peer_addr())
            .is_some()
    });
    system.stop_node(node_server);

    // Ensure telemetry from disconnected peer is removed
    assert_timely(Duration::from_secs(5), || {
        node_client
            .telemetry
            .get_telemetry(&channel.peer_addr())
            .is_none()
    });
}

#[test]
fn disable_metrics() {
    let mut system = System::new();
    let node_client = system.make_node();
    let node_server = system
        .build_node()
        .flags(NodeFlags {
            disable_providing_telemetry_metrics: true,
            ..Default::default()
        })
        .finish();

    // Try and request metrics from a node which is turned off but a channel is not closed yet
    let channel = node_client
        .network
        .read()
        .unwrap()
        .find_node_id(&node_server.get_node_id())
        .unwrap()
        .clone();

    node_client.telemetry.trigger();

    assert_never(Duration::from_secs(1), || {
        node_client
            .telemetry
            .get_telemetry(&channel.peer_addr())
            .is_some()
    });

    // It should still be able to receive metrics though
    let channel1 = node_server
        .network
        .read()
        .unwrap()
        .find_node_id(&node_client.get_node_id())
        .unwrap()
        .clone();

    assert_timely(Duration::from_secs(5), || {
        node_server
            .telemetry
            .get_telemetry(&channel1.peer_addr())
            .is_some()
    });
}

#[test]
fn mismatched_node_id() {
    let mut system = System::new();
    let node = system.make_node();

    let telemetry = node.telemetry.local_telemetry();

    let message = Message::TelemetryAck(TelemetryAck(Some(telemetry)));
    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(message, channel);

    assert_timely(Duration::from_secs(5), || {
        node.stats.count(
            StatType::Telemetry,
            DetailType::NodeIdMismatch,
            Direction::In,
        ) > 0
    });
    assert_always_eq(
        Duration::from_secs(1),
        || {
            node.stats
                .count(StatType::Telemetry, DetailType::Process, Direction::In)
        },
        0,
    );
}

#[test]
fn no_peers() {
    let mut system = System::new();
    let node = system.make_node();
    let responses = node.telemetry.get_all_telemetries();
    assert_eq!(responses.len(), 0);
}

#[test]
fn invalid_endpoint() {
    let mut system = System::new();
    let node_client = system.make_node();
    let _node_server = system.make_node();
    node_client.telemetry.trigger();

    // Give some time for nodes to exchange telemetry
    sleep(Duration::from_secs(1));

    let endpoint: SocketAddrV6 = "[::ffff:240.0.0.0]:12345".parse().unwrap();
    assert!(node_client.telemetry.get_telemetry(&endpoint).is_none());
}

#[test]
fn ongoing_broadcasts() {
    let mut system = System::new();
    let node1 = system.make_node();
    let node2 = system.make_node();

    assert_timely(Duration::from_secs(5), || {
        node1
            .stats
            .count(StatType::Telemetry, DetailType::Process, Direction::In)
            >= 3
    });
    assert_timely(Duration::from_secs(5), || {
        node2
            .stats
            .count(StatType::Telemetry, DetailType::Process, Direction::In)
            >= 3
    });
}
