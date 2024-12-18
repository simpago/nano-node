use rsnano_core::Amount;
use rsnano_node::consensus::{Bucket, PriorityBucketConfig};
use test_helpers::System;

mod bucket {
    use super::*;
    use rsnano_core::SavedBlock;

    #[test]
    fn construction() {
        let mut system = System::new();
        let node = system.make_node();

        let bucket = Bucket::new(
            Amount::nano(1000),
            PriorityBucketConfig::default(),
            node.active.clone(),
            node.stats.clone(),
        );

        assert_eq!(bucket.can_accept(Amount::nano(999)), false);
        assert_eq!(bucket.can_accept(Amount::nano(1000)), true);
        assert_eq!(bucket.can_accept(Amount::nano(1001)), true);
        assert_eq!(bucket.len(), 0);
    }

    #[test]
    fn insert_one() {
        let mut system = System::new();
        let node = system.make_node();

        let bucket = Bucket::new(
            Amount::zero(),
            PriorityBucketConfig::default(),
            node.active.clone(),
            node.stats.clone(),
        );

        assert!(bucket.push(1000, SavedBlock::new_test_instance()));
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn insert_duplicate() {
        let mut system = System::new();
        let node = system.make_node();

        let bucket = Bucket::new(
            Amount::zero(),
            PriorityBucketConfig::default(),
            node.active.clone(),
            node.stats.clone(),
        );

        let block = SavedBlock::new_test_instance();
        assert_eq!(bucket.push(1000, block.clone()), true);
        assert_eq!(bucket.push(1000, block), false);
    }

    #[test]
    fn insert_many() {
        let mut system = System::new();
        let node = system.make_node();

        let bucket = Bucket::new(
            Amount::zero(),
            PriorityBucketConfig::default(),
            node.active.clone(),
            node.stats.clone(),
        );

        let block0 = SavedBlock::new_test_instance_with_key(1);
        let block1 = SavedBlock::new_test_instance_with_key(2);
        let block2 = SavedBlock::new_test_instance_with_key(3);
        let block3 = SavedBlock::new_test_instance_with_key(3);
        assert!(bucket.push(2000, block0.clone()));
        assert!(bucket.push(1001, block1.clone()));
        assert!(bucket.push(1000, block2.clone()));
        assert!(bucket.push(900, block3.clone()));

        assert_eq!(bucket.len(), 4);
        let blocks = bucket.blocks();
        assert_eq!(blocks.len(), 4);
        // Ensure correct order
        assert_eq!(blocks[0], block3.into());
        assert_eq!(blocks[1], block2.into());
        assert_eq!(blocks[2], block1.into());
        assert_eq!(blocks[3], block0.into());
    }

    #[test]
    fn max_blocks() {
        let mut system = System::new();
        let node = system.make_node();

        let config = PriorityBucketConfig {
            max_blocks: 2,
            ..Default::default()
        };
        let bucket = Bucket::new(
            Amount::zero(),
            config,
            node.active.clone(),
            node.stats.clone(),
        );

        let block0 = SavedBlock::new_test_instance_with_key(1);
        let block1 = SavedBlock::new_test_instance_with_key(2);
        let block2 = SavedBlock::new_test_instance_with_key(3);
        let block3 = SavedBlock::new_test_instance_with_key(3);

        assert_eq!(bucket.push(2000, block0.clone()), true);
        assert_eq!(bucket.push(900, block1.clone()), true);
        assert_eq!(bucket.push(3000, block2.clone()), false);
        assert_eq!(bucket.push(1001, block3.clone()), true); // Evicts 2000
        assert_eq!(bucket.push(1000, block0.clone()), true); // Evicts 1001

        assert_eq!(bucket.len(), 2);
        let blocks = bucket.blocks();
        // Ensure correct order
        assert_eq!(blocks[0], block1.into());
        assert_eq!(blocks[1], block0.into());
    }
}

mod election_scheduler {
    use rsnano_core::{Amount, PrivateKey, UnsavedBlockLatticeBuilder, DEV_GENESIS_KEY};
    use rsnano_ledger::DEV_GENESIS_ACCOUNT;
    use rsnano_node::{config::NodeConfig, consensus::ActiveElectionsExt};
    use std::time::Duration;
    use test_helpers::{assert_timely, assert_timely_eq, System};

    #[test]
    fn activate_one_timely() {
        let mut system = System::new();
        let node = system.make_node();

        let mut lattice = UnsavedBlockLatticeBuilder::new();
        let mut send1 = lattice
            .genesis()
            .send(&*DEV_GENESIS_KEY, Amount::nano(1000));

        node.ledger
            .process(&mut node.ledger.rw_txn(), &mut send1)
            .unwrap();

        node.election_schedulers
            .priority
            .activate(&node.store.tx_begin_read(), &*DEV_GENESIS_ACCOUNT);

        assert_timely(Duration::from_secs(5), || {
            node.active.election(&send1.qualified_root()).is_some()
        });
    }

    #[test]
    fn activate_one_flush() {
        let mut system = System::new();
        let node = system.make_node();
        let mut lattice = UnsavedBlockLatticeBuilder::new();

        // Create a send block
        let mut send1 = lattice
            .genesis()
            .send(&*DEV_GENESIS_KEY, Amount::nano(1000));

        // Process the block
        node.ledger
            .process(&mut node.store.tx_begin_write(), &mut send1)
            .unwrap();

        // Activate the account
        node.election_schedulers
            .priority
            .activate(&node.store.tx_begin_read(), &*DEV_GENESIS_ACCOUNT);

        // Assert that the election is created within 5 seconds
        assert_timely(Duration::from_secs(5), || {
            node.active.election(&send1.qualified_root()).is_some()
        });
    }

    #[test]
    /**
     * Tests that the election scheduler and the active transactions container (AEC)
     * work in sync with regards to the node configuration value "active_elections_size".
     *
     * The test sets up two forcefully cemented blocks -- a send on the genesis account and a receive on a second account.
     * It then creates two other blocks, each a successor to one of the previous two,
     * and processes them locally (without the node starting elections for them, but just saving them to disk).
     *
     * Elections for these latter two (B1 and B2) are started by the test code manually via `election_scheduler::activate`.
     * The test expects E1 to start right off and take its seat into the AEC.
     * E2 is expected not to start though (because the AEC is full), so B2 should be awaiting in the scheduler's queue.
     *
     * As soon as the test code manually confirms E1 (and thus evicts it out of the AEC),
     * it is expected that E2 begins and the scheduler's queue becomes empty again.
     */
    fn no_vacancy() {
        let mut system = System::new();
        let node = system
            .build_node()
            .config(NodeConfig {
                active_elections: rsnano_node::consensus::ActiveElectionsConfig {
                    size: 1,
                    ..Default::default()
                },
                ..System::default_config_without_backlog_population()
            })
            .finish();

        let mut lattice = UnsavedBlockLatticeBuilder::new();
        let key = PrivateKey::new();

        // Activating accounts depends on confirmed dependencies. First, prepare 2 accounts
        let send = lattice.genesis().send(&key, Amount::nano(1000));
        let send = node.process(send.clone()).unwrap();
        node.confirming_set.add(send.hash());

        let receive = lattice.account(&key).receive(&send);
        let receive = node.process(receive.clone()).unwrap();
        node.confirming_set.add(receive.hash());

        assert_timely(Duration::from_secs(5), || {
            node.block_confirmed(&send.hash()) && node.block_confirmed(&receive.hash())
        });

        // Second, process two eligible transactions
        let block1 = lattice
            .genesis()
            .send(&*DEV_GENESIS_KEY, Amount::nano(1000));
        node.process(block1.clone()).unwrap();

        // There is vacancy so it should be inserted
        node.election_schedulers
            .priority
            .activate(&node.ledger.read_txn(), &DEV_GENESIS_ACCOUNT);
        let mut election = None;
        assert_timely(Duration::from_secs(5), || {
            match node.active.election(&block1.qualified_root()) {
                Some(el) => {
                    election = Some(el);
                    true
                }
                None => false,
            }
        });

        let block2 = lattice.account(&key).send(&key, Amount::nano(1000));
        node.process(block2.clone()).unwrap();

        // There is no vacancy so it should stay queued
        node.election_schedulers
            .priority
            .activate(&node.ledger.read_txn(), &key.account());
        assert_timely_eq(
            Duration::from_secs(5),
            || node.election_schedulers.priority.len(),
            1,
        );
        assert!(node.active.election(&block2.qualified_root()).is_none());

        // Election confirmed, next in queue should begin
        node.active.force_confirm(&election.unwrap());
        assert_timely(Duration::from_secs(5), || {
            node.active.election(&block2.qualified_root()).is_some()
        });
        assert!(node.election_schedulers.priority.is_empty());
    }
}
