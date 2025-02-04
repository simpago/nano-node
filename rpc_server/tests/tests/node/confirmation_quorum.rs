use rsnano_core::{WalletId, DEV_GENESIS_KEY};
use rsnano_node::wallets::WalletsExt;
use test_helpers::{establish_tcp, send_block, setup_rpc_client_and_server, System};

#[test]
fn confirmation_quorum() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), false);

    let result = node
        .runtime
        .block_on(async { server.client.confirmation_quorum().await.unwrap() });

    let reps = node.online_reps.lock().unwrap();

    assert_eq!(result.quorum_delta, reps.quorum_delta());
    assert_eq!(
        result.online_weight_quorum_percent,
        reps.quorum_percent().into()
    );
    assert_eq!(result.online_weight_minimum, reps.online_weight_minimum());
    assert_eq!(result.online_stake_total, reps.online_weight());
    assert_eq!(result.peers_stake_total, reps.peered_weight());
    assert_eq!(result.trended_stake_total, reps.trended_or_minimum_weight());
    assert_eq!(result.peers, None);
}

#[test]
fn confirmation_quorum_peer_details() {
    let mut system = System::new();

    let node0 = system.make_node();

    let mut node1_config = System::default_config();
    node1_config.tcp.max_inbound_connections = 0; // Prevent ephemeral node1->node0 channel replacement with incoming connection
    let node1 = system
        .build_node()
        .config(node1_config)
        .disconnected()
        .finish();

    establish_tcp(&node1, &node0);

    let wallet_id = WalletId::zero();
    node1.wallets.create(wallet_id);
    node1
        .wallets
        .insert_adhoc2(&wallet_id, &DEV_GENESIS_KEY.raw_key(), false)
        .unwrap();

    let hash = send_block(node0.clone());

    node0.confirm(hash);

    assert_eq!(node0.block_confirmed(&hash), true);

    let server = setup_rpc_client_and_server(node1.clone(), false);

    let result = node0.runtime.block_on(async {
        server
            .client
            .confirmation_quorum_with_details()
            .await
            .unwrap()
    });

    let reps = node0.online_reps.lock().unwrap();

    assert_eq!(result.quorum_delta, reps.quorum_delta());
    assert_eq!(
        result.online_weight_quorum_percent,
        reps.quorum_percent().into()
    );
    assert_eq!(result.online_weight_minimum, reps.online_weight_minimum());
    assert_eq!(result.online_stake_total, reps.online_weight());
    assert_eq!(result.peers_stake_total, reps.peered_weight());
    assert_eq!(result.trended_stake_total, reps.trended_or_minimum_weight());

    let peer_details = result.peers.unwrap();
    println!("{:?}", peer_details);
}
