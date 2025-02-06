use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use rsnano_core::{Amount, PrivateKey, UnsavedBlockLatticeBuilder, DEV_GENESIS_KEY};
use test_helpers::{assert_timely, assert_timely_eq, setup_independent_blocks, System};

/*
 * Ensures all not confirmed accounts get activated by backlog scan periodically
 */
#[test]
fn backlog_scan() {
    let activated = Arc::new(Mutex::new(HashSet::new()));
    let activated2 = activated.clone();
    let mut system = System::new();
    let node = system.make_node();

    node.backlog_scan.on_batch_activated(move |batch| {
        for info in batch {
            activated2.lock().unwrap().insert(info.account);
        }
    });

    let blocks = setup_independent_blocks(&node, 256, &DEV_GENESIS_KEY);

    // Checks if `activated` set contains all accounts we previously set up
    assert_timely(Duration::from_secs(5), || {
        let guard = activated.lock().unwrap();
        blocks.iter().all(|b| guard.contains(&b.account()))
    });

    // Clear activated set to ensure we activate those accounts more than once
    activated.lock().unwrap().clear();

    assert_timely(Duration::from_secs(5), || {
        let guard = activated.lock().unwrap();
        blocks.iter().all(|b| guard.contains(&b.account()))
    });
}

#[test]
fn election_activation() {
    let key = PrivateKey::new();
    let mut system = System::new();
    let node = system.build_node().finish();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(&key, Amount::nano(1000));
    node.process(send);
    assert_timely_eq(Duration::from_secs(5), || node.active.len(), 1);
}
