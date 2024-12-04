use rsnano_core::{Account, Amount};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH};
use rsnano_rpc_messages::AccountInfoArgs;
use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn account_info() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node.runtime.block_on(async {
        server
            .client
            .account_info(
                AccountInfoArgs::build(
                    Account::decode_account(
                        "nano_1111111111111111111111111111111111111111111111111111hifc8npp",
                    )
                    .unwrap(),
                )
                .finish(),
            )
            .await
    });

    assert_eq!(
        result.err().map(|e| e.to_string()),
        Some("node returned error: \"Account not found\"".to_string())
    );

    let result = node.runtime.block_on(async {
        server
            .client
            .account_info(
                AccountInfoArgs::build(*DEV_GENESIS_ACCOUNT)
                    .include_representative()
                    .include_weight()
                    .include_receivable()
                    .include_confirmed()
                    .finish(),
            )
            .await
            .unwrap()
    });

    assert_eq!(result.frontier, *DEV_GENESIS_HASH);
    assert_eq!(result.open_block, *DEV_GENESIS_HASH);
    assert_eq!(result.representative_block, *DEV_GENESIS_HASH);
    assert_eq!(result.balance, Amount::MAX);
    assert!(result.modified_timestamp > 0.into());
    assert_eq!(result.block_count, 1.into());
    assert_eq!(result.account_version, 0.into());
    assert_eq!(result.confirmed_height, Some(1.into()));
    assert_eq!(result.confirmed_frontier, Some(*DEV_GENESIS_HASH));
    assert_eq!(result.representative, Some(*DEV_GENESIS_ACCOUNT));
    assert_eq!(result.weight, Some(Amount::MAX));
    assert_eq!(result.pending, Some(Amount::raw(0)));
    assert_eq!(result.receivable, Some(Amount::raw(0)));
    assert_eq!(result.confirmed_balance, Some(Amount::MAX));
    assert_eq!(result.confirmed_pending, Some(Amount::raw(0)));
    assert_eq!(result.confirmed_receivable, Some(Amount::raw(0)));
    assert_eq!(result.confirmed_representative, Some(*DEV_GENESIS_ACCOUNT));
}
