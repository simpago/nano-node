use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn frontier_count() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node
        .runtime
        .block_on(async { server.client.frontier_count().await.unwrap() });

    assert_eq!(result.count, 1.into());
}
