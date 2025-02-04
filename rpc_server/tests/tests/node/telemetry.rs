use rsnano_rpc_messages::{TelemetryArgs, TelemetryDto};
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    time::Duration,
};
use test_helpers::{assert_timely_eq, setup_rpc_client_and_server, System};

#[test]
fn telemetry_single() {
    let mut system = System::new();
    let node = system.build_node().finish();
    let _peer = system.build_node().finish();

    // Wait until peers are stored
    assert_timely_eq(
        Duration::from_secs(10),
        || node.store.peer.count(&node.store.tx_begin_read()),
        1,
    );

    let server = setup_rpc_client_and_server(node.clone(), false);

    let args = TelemetryArgs::builder()
        .remote_addr(node.tcp_listener.local_address())
        .build();

    // Test with valid local address
    let response = node
        .runtime
        .block_on(async { server.client.telemetry(args).await });
    assert!(response.is_ok());
    assert!(matches!(response.unwrap().metrics[0], TelemetryDto { .. }));

    let args = TelemetryArgs::builder()
        .remote_addr(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 65, 0, 0))
        .build();

    // Test with invalid address
    let response = node
        .runtime
        .block_on(async { server.client.telemetry(args).await });
    assert!(response.is_err());
    assert_eq!(
        response.unwrap_err().to_string(),
        "node returned error: \"Peer not found\""
    );

    // Test with missing address (should return local telemetry)
    let response = node
        .runtime
        .block_on(async { server.client.telemetry(TelemetryArgs::new()).await });
    assert!(response.is_ok());
    assert!(matches!(response.unwrap().metrics[0], TelemetryDto { .. }));
}

#[test]
fn telemetry_all() {
    let mut system = System::new();
    let node = system.build_node().finish();
    let peer = system.build_node().finish();

    // Wait until peers are stored
    assert_timely_eq(
        Duration::from_secs(10),
        || node.store.peer.count(&node.store.tx_begin_read()),
        1,
    );

    let server = setup_rpc_client_and_server(node.clone(), false);

    // Test without raw flag (should return local telemetry)
    let response = node
        .runtime
        .block_on(async { server.client.telemetry(TelemetryArgs::new()).await });

    assert!(response.is_ok());
    let local_telemetry = response.unwrap();
    assert!(matches!(local_telemetry.metrics[0], TelemetryDto { .. }));

    // Test with raw flag
    let response = node.runtime.block_on(async {
        server
            .client
            .telemetry(TelemetryArgs::builder().raw().build())
            .await
    });

    assert!(response.is_ok());
    let raw_response = response.unwrap();

    assert_eq!(raw_response.metrics.len(), 1);

    let peer_telemetry = &raw_response.metrics[0];
    let local_telemetry = node.telemetry.local_telemetry();
    assert_eq!(peer_telemetry.genesis_block, local_telemetry.genesis_block);

    // Verify the endpoint matches a known peer
    let peer_address = peer_telemetry.address.unwrap();
    let peer_port = peer_telemetry.port.unwrap().inner();
    let peer_endpoint = SocketAddrV6::new(peer_address, peer_port, 0, 0);

    let network_info = node.network.read().unwrap();
    let matching_channels = network_info.find_channels_by_peering_addr(&peer_endpoint.into());
    assert!(
        !matching_channels.is_empty(),
        "Peer endpoint not found in network info"
    );
}
