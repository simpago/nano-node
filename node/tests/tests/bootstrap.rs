use rsnano_core::{Account, PrivateKey, UnsavedBlockLatticeBuilder};
use rsnano_node::{
    bootstrap::BootstrapConfig,
    config::{NodeConfig, NodeFlags},
};
use std::time::Duration;
use test_helpers::{assert_always_eq, assert_timely, System};

/**
 * Tests the base case for returning
 */
#[test]
fn account_base() {
    let mut system = System::new();
    let node0 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice.genesis().send(Account::zero(), 1);
    node0.process(send1.clone());
    let node1 = system.make_node();
    assert_timely(Duration::from_secs(5), || node1.block_exists(&send1.hash()));
}

/**
 * Tests that bootstrap_ascending will return multiple new blocks in-order
 */
#[test]
fn account_inductive() {
    let mut system = System::new();
    let node0 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send1 = lattice.genesis().send(Account::zero(), 1);
    let send2 = lattice.genesis().send(Account::zero(), 1);
    node0.process(send1);
    node0.process(send2.clone());
    let node1 = system.make_node();
    assert_timely(Duration::from_secs(50), || {
        node1.block_exists(&send2.hash())
    });
}

/**
 * Tests that bootstrap_ascending will return multiple new blocks in-order
 */

#[test]
fn trace_base() {
    let mut system = System::new();
    let node0 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key = PrivateKey::new();
    let send1 = lattice.genesis().send(&key, 1);
    let receive1 = lattice.account(&key).receive(&send1);
    node0.process(send1);
    node0.process(receive1.clone());
    let node1 = system.make_node();
    assert_timely(Duration::from_secs(10), || {
        node1.block_exists(&receive1.hash())
    });
}

/// Tests that bootstrap will prioritize existing accounts with outdated frontiers
#[test]
fn frontier_scan() {
    let mut system = System::new();
    let flags = NodeFlags {
        disable_legacy_bootstrap: true,
        ..Default::default()
    };

    let config = NodeConfig {
        bootstrap: BootstrapConfig {
            // Disable other bootstrap strategies
            enable_scan: false,
            enable_dependency_walker: false,
            ..Default::default()
        },
        // Disable election activation
        enable_priority_scheduler: false,
        enable_optimistic_scheduler: false,
        enable_hinted_scheduler: false,
        ..System::default_config_without_backlog_scan()
    };

    // Prepare blocks for frontier scan (genesis 10 sends -> 10 opens -> 10 updates)
    let mut sends = Vec::new();
    let mut opens = Vec::new();
    let mut updates = Vec::new();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    for _ in 0..10 {
        let key = PrivateKey::new();
        let send = lattice.genesis().send(&key, 1);
        let open = lattice.account(&key).receive(&send);
        let update = lattice.account(&key).change(0);
        sends.push(send);
        opens.push(open);
        updates.push(update);
    }

    // Initialize nodes with blocks without the `updates` frontiers
    let mut blocks = Vec::new();
    blocks.extend(sends);
    blocks.extend(opens);
    system.initialization_blocks = blocks.clone();

    let node0 = system
        .build_node()
        .flags(flags.clone())
        .config(config.clone())
        .finish();
    node0.process_multi(&updates);

    // No blocks should be broadcast to the other node
    let node1 = system
        .build_node()
        .flags(flags)
        .config(NodeConfig {
            peering_port: System::default_config().peering_port,
            ..config
        })
        .finish();

    assert_always_eq(
        Duration::from_millis(100),
        || node1.ledger.block_count() as usize,
        blocks.len() + 1,
    );

    // Frontier scan should detect all the accounts with missing blocks
    assert_timely(Duration::from_secs(10), || {
        updates.iter().all(|block| {
            node1
                .bootstrapper
                .prioritized(&block.account_field().unwrap())
        })
    });
}

/// Tests that bootstrap will prioritize not yet existing accounts with pending blocks
#[test]
fn frontier_scan_pending() {
    let mut system = System::new();
    let flags = NodeFlags {
        disable_legacy_bootstrap: true,
        ..Default::default()
    };

    let config = NodeConfig {
        bootstrap: BootstrapConfig {
            // Disable other bootstrap strategies
            enable_scan: false,
            enable_dependency_walker: false,
            ..Default::default()
        },
        // Disable election activation
        enable_priority_scheduler: false,
        enable_optimistic_scheduler: false,
        enable_hinted_scheduler: false,
        ..System::default_config_without_backlog_scan()
    };

    // Prepare blocks for frontier scan (genesis 10 sends -> 10 opens)
    let mut sends = Vec::new();
    let mut opens = Vec::new();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    for _ in 0..10 {
        let key = PrivateKey::new();
        let send = lattice.genesis().send(&key, 1);
        let open = lattice.account(&key).receive(&send);
        sends.push(send);
        opens.push(open);
    }

    // Initialize nodes with blocks without the `updates` frontiers
    let mut blocks = Vec::new();
    blocks.extend(sends);
    system.initialization_blocks = blocks.clone();

    let node0 = system
        .build_node()
        .flags(flags.clone())
        .config(config.clone())
        .finish();
    node0.process_multi(&opens);

    // No blocks should be broadcast to the other node
    let node1 = system
        .build_node()
        .flags(flags)
        .config(NodeConfig {
            peering_port: System::default_config().peering_port,
            ..config
        })
        .finish();

    assert_always_eq(
        Duration::from_millis(100),
        || node1.ledger.block_count() as usize,
        blocks.len() + 1,
    );

    // Frontier scan should detect all the accounts with missing blocks
    assert_timely(Duration::from_secs(10), || {
        opens.iter().all(|block| {
            node1
                .bootstrapper
                .prioritized(&block.account_field().unwrap())
        })
    });
}

/// Bootstrap should not attempt to prioritize accounts that can't be immediately connected
/// to the ledger (no pending blocks, no existing frontier)
#[test]
fn frontier_scan_cannot_prioritize() {
    let mut system = System::new();
    let flags = NodeFlags {
        disable_legacy_bootstrap: true,
        ..Default::default()
    };

    let config = NodeConfig {
        bootstrap: BootstrapConfig {
            // Disable other bootstrap strategies
            enable_scan: false,
            enable_dependency_walker: false,
            ..Default::default()
        },
        // Disable election activation
        enable_priority_scheduler: false,
        enable_optimistic_scheduler: false,
        enable_hinted_scheduler: false,
        ..System::default_config_without_backlog_scan()
    };

    // Prepare blocks for frontier scan (genesis 10 sends -> 10 opens -> 10 sends -> 10 opens)
    let mut sends = Vec::new();
    let mut opens = Vec::new();
    let mut sends2 = Vec::new();
    let mut opens2 = Vec::new();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    for _ in 0..10 {
        let key = PrivateKey::new();
        let key2 = PrivateKey::new();
        let send = lattice.genesis().send(&key, 1);
        let open = lattice.account(&key).receive(&send);
        let send2 = lattice.account(&key).send(&key2, 1);
        let open2 = lattice.account(&key2).receive(&send2);
        sends.push(send);
        opens.push(open);
        sends2.push(send2);
        opens2.push(open2);
    }

    // Initialize nodes with blocks without the `updates` frontiers
    let mut blocks = Vec::new();
    blocks.extend(sends);
    blocks.extend(opens);
    system.initialization_blocks = blocks.clone();

    let node0 = system
        .build_node()
        .flags(flags.clone())
        .config(config.clone())
        .finish();
    node0.process_multi(&sends2);
    node0.process_multi(&opens2);

    // No blocks should be broadcast to the other node
    let node1 = system
        .build_node()
        .flags(flags)
        .config(NodeConfig {
            peering_port: System::default_config().peering_port,
            ..config
        })
        .finish();

    assert_always_eq(
        Duration::from_millis(100),
        || node1.ledger.block_count() as usize,
        blocks.len() + 1,
    );
    // Frontier scan should not detect the accounts
    assert_always_eq(
        Duration::from_secs(1),
        || {
            opens2.iter().all(|block| {
                !node1
                    .bootstrapper
                    .prioritized(&block.account_field().unwrap())
            })
        },
        true,
    );
}
