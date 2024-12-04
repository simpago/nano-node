use rsnano_core::{Account, Amount, BlockBuilder, WalletId, DEV_GENESIS_KEY};
use rsnano_ledger::DEV_GENESIS_PUB_KEY;
use rsnano_node::wallets::WalletsExt;
use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn search_receivable() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    // Create a wallet and insert the genesis key
    let wallet_id = WalletId::zero();
    node.wallets.create(wallet_id);
    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.private_key(), true)
        .unwrap();

    // Get the latest block hash for the genesis account
    let genesis_pub: Account = (*DEV_GENESIS_PUB_KEY).into();
    let latest = node.latest(&genesis_pub);

    // Create a send block
    let receive_minimum = node.config.receive_minimum.clone();
    let send_amount = receive_minimum + Amount::raw(1);
    let block = BlockBuilder::legacy_send()
        .previous(latest)
        .destination(genesis_pub)
        .balance(Amount::MAX - send_amount)
        .sign(DEV_GENESIS_KEY.clone())
        .build();

    // Process the send block
    node.process_active(block);

    // Call search_receivable
    node.runtime.block_on(async {
        server.client.search_receivable(wallet_id).await.unwrap();
    });

    // Check that the balance has been updated
    let final_balance = node.runtime.block_on(async {
        let timeout = std::time::Duration::from_secs(10);
        let start = std::time::Instant::now();
        loop {
            let balance = node.balance(&genesis_pub.into());
            if balance == Amount::MAX || start.elapsed() > timeout {
                return balance;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    assert_eq!(final_balance, Amount::MAX);
}

#[test]
fn search_receivable_fails_without_enable_control() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), false);

    let result = node
        .runtime
        .block_on(async { server.client.search_receivable(WalletId::zero()).await });

    assert_eq!(
        result.err().map(|e| e.to_string()),
        Some("node returned error: \"RPC control is disabled\"".to_string())
    );
}

#[test]
fn search_receivable_fails_with_wallet_not_found() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node
        .runtime
        .block_on(async { server.client.search_receivable(WalletId::zero()).await });

    assert_eq!(
        result.err().map(|e| e.to_string()),
        Some("node returned error: \"Wallet not found\"".to_string())
    );
}
