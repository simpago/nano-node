use rsnano_core::BlockHash;
use rsnano_ledger::DEV_GENESIS_HASH;
use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn block_confirm() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node.runtime.block_on(async {
        server
            .client
            .block_confirm(DEV_GENESIS_HASH.to_owned())
            .await
            .unwrap()
    });

    assert_eq!(result.started, true.into());
}

#[test]
fn block_confirm_fails_with_block_not_found() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node
        .runtime
        .block_on(async { server.client.block_confirm(BlockHash::zero()).await });

    assert_eq!(
        result.err().map(|e| e.to_string()),
        Some("node returned error: \"Block not found\"".to_string())
    );
}
