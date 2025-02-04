use rsnano_network::ChannelMode;
use std::time::Duration;
use test_helpers::{assert_never, System};

// Test a node cannot connect to its own endpoint.
#[test]
fn no_self_incoming() {
    let mut system = System::new();
    let node = system.make_node();
    let _ = node
        .peer_connector
        .connect_to(node.tcp_listener.local_address());
    assert_never(Duration::from_secs(2), || {
        node.network
            .read()
            .unwrap()
            .count_by_mode(ChannelMode::Realtime)
            > 0
    })
}
