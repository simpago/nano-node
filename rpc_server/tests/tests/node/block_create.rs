use rsnano_core::{Amount, Block, BlockType, PrivateKey, WalletId, DEV_GENESIS_KEY};
use rsnano_ledger::{DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH};
use rsnano_node::wallets::WalletsExt;
use rsnano_rpc_messages::{BlockCreateArgs, BlockTypeDto};
use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn block_create_state() {
    let mut system = System::new();
    let mut config = System::default_config();
    config.online_weight_minimum = Amount::MAX;
    let node = system.build_node().config(config).finish();

    let wallet_id = WalletId::zero();
    node.wallets.create(wallet_id);
    node.wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.raw_key(), false)
        .unwrap();
    let key1 = PrivateKey::new();

    let server = setup_rpc_client_and_server(node.clone(), true);

    let result = node.runtime.block_on(async {
        server
            .client
            .block_create(BlockCreateArgs::new(
                BlockTypeDto::State,
                Some(Amount::MAX - Amount::raw(100)),
                Some(DEV_GENESIS_KEY.raw_key()),
                None,
                Some(*DEV_GENESIS_ACCOUNT),
                None,
                Some(key1.account()),
                Some(key1.account()),
                Some((*DEV_GENESIS_ACCOUNT).into()),
                Some(*DEV_GENESIS_HASH),
                None,
                None,
                None,
            ))
            .await
            .unwrap()
    });

    let block_hash = result.hash;
    let block: Block = result.block.into();

    assert_eq!(block.block_type(), BlockType::State);
    assert_eq!(block.hash(), block_hash);

    node.process(block.clone());

    let tx = node.ledger.read_txn();
    assert_eq!(
        node.ledger.any().block_account(&tx, &block.hash()),
        Some(*DEV_GENESIS_ACCOUNT)
    );
}
