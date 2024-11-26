use rsnano_core::{Account, Amount, Block, StateBlock, DEV_GENESIS_KEY};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY};
use rsnano_node::Node;
use rsnano_rpc_messages::UnopenedArgs;
use std::sync::Arc;
use std::time::Duration;
use test_helpers::{assert_timely_msg, setup_rpc_client_and_server, System};

fn send_block(node: Arc<Node>) {
    let send1 = Block::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        *DEV_GENESIS_HASH,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::raw(1),
        Account::zero().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(*DEV_GENESIS_HASH),
    ));

    node.process_active(send1.clone());
    assert_timely_msg(
        Duration::from_secs(5),
        || node.active_elections.active(&send1),
        "not active on node 1",
    );
}

#[test]
fn unopened() {
    let mut system = System::new();
    let node = system.make_node();

    send_block(node.clone());

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node.runtime.block_on(async {
        server
            .client
            .unopened(UnopenedArgs {
                account: Some(Account::zero()),
                ..Default::default()
            })
            .await
            .unwrap()
    });

    assert_eq!(
        result.accounts.get(&Account::zero()).unwrap(),
        &Amount::raw(1)
    );
}

#[test]
fn unopened_with_threshold() {
    let mut system = System::new();
    let node = system.make_node();

    send_block(node.clone());

    let server = setup_rpc_client_and_server(node.clone(), true);

    let args = UnopenedArgs {
        account: Some(Account::zero()),
        threshold: Some(Amount::nano(1)),
        ..Default::default()
    };

    let result = node
        .runtime
        .block_on(async { server.client.unopened(args).await.unwrap() });

    assert!(result.accounts.is_empty());
}

#[test]
fn unopened_fails_without_enable_control() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), false);

    let args = UnopenedArgs {
        account: Some(Account::zero()),
        ..Default::default()
    };

    let result = node
        .runtime
        .block_on(async { server.client.unopened(args).await });

    assert_eq!(
        result.err().map(|e| e.to_string()),
        Some("node returned error: \"RPC control is disabled\"".to_string())
    );
}
