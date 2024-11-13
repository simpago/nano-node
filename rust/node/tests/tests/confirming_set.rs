use rsnano_core::{Amount, BlockEnum, KeyPair, StateBlock, DEV_GENESIS_KEY};
use rsnano_ledger::{Writer, DEV_GENESIS_ACCOUNT, DEV_GENESIS_PUB_KEY};
use rsnano_node::{
    config::{FrontiersConfirmationMode, NodeConfig, NodeFlags},
    consensus::ActiveElectionsExt,
    stats::{DetailType, Direction, StatType},
};
use std::time::Duration;
use test_helpers::{assert_always_eq, assert_timely, assert_timely_eq, start_election, System};

#[test]
fn observer_callbacks() {
    let mut system = System::new();
    let config = NodeConfig {
        frontiers_confirmation: FrontiersConfirmationMode::Disabled,
        ..System::default_config()
    };
    let node = system.build_node().config(config).finish();
    node.insert_into_wallet(&DEV_GENESIS_KEY);
    let latest = node.latest(&DEV_GENESIS_ACCOUNT);

    let key1 = KeyPair::new();
    let send = BlockEnum::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        latest,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(latest.into()),
    ));

    let send1 = BlockEnum::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(2000),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send.hash().into()),
    ));

    node.process_multi(&[send.clone(), send1.clone()]);

    node.confirming_set.add(send1.hash());

    // Callback is performed for all blocks that are confirmed
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats
                .count_all(StatType::ConfirmationObserver, Direction::Out)
        },
        2,
    );

    assert_eq!(
        node.stats.count(
            StatType::ConfirmationHeight,
            DetailType::BlocksConfirmed,
            Direction::In
        ),
        2
    );
    assert_eq!(node.ledger.cemented_count(), 3);
    assert_eq!(node.active.vote_applier.election_winner_details_len(), 0);
}

// The callback and confirmation history should only be updated after confirmation height is set (and not just after voting)
#[test]
fn confirmed_history() {
    let mut system = System::new();
    let flags = NodeFlags {
        force_use_write_queue: true,
        disable_ascending_bootstrap: true,
        ..Default::default()
    };
    let config = NodeConfig {
        frontiers_confirmation: FrontiersConfirmationMode::Disabled,
        ..System::default_config()
    };
    let node = system.build_node().flags(flags).config(config).finish();
    let latest = node.latest(&DEV_GENESIS_ACCOUNT);

    let key1 = KeyPair::new();
    let send = BlockEnum::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        latest,
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(1000),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(latest.into()),
    ));

    let send1 = BlockEnum::State(StateBlock::new(
        *DEV_GENESIS_ACCOUNT,
        send.hash(),
        *DEV_GENESIS_PUB_KEY,
        Amount::MAX - Amount::nano(2000),
        key1.account().into(),
        &DEV_GENESIS_KEY,
        node.work_generate_dev(send.hash().into()),
    ));

    node.process_multi(&[send.clone(), send1.clone()]);

    let election = start_election(&node, &send1.hash());
    {
        // The write guard prevents the confirmation height processor doing any writes
        let _write_guard = node.ledger.write_queue.wait(Writer::Testing);

        // Confirm send1
        node.active.force_confirm(&election);
        assert_timely_eq(Duration::from_secs(10), || node.active.len(), 0);
        assert_eq!(node.active.recently_cemented_count(), 0);
        assert_eq!(node.active.len(), 0);

        let tx = node.ledger.read_txn();
        assert_eq!(
            node.ledger.confirmed().block_exists(&tx, &send.hash()),
            false
        );

        assert_timely(Duration::from_secs(10), || {
            node.ledger.write_queue.contains(Writer::ConfirmationHeight)
        });

        // Confirm that no inactive callbacks have been called when the
        // confirmation height processor has already iterated over it, waiting to write
        assert_always_eq(
            Duration::from_millis(50),
            || {
                node.stats.count(
                    StatType::ConfirmationObserver,
                    DetailType::InactiveConfHeight,
                    Direction::Out,
                )
            },
            0,
        );
    }

    assert_timely(Duration::from_secs(10), || {
        !node.ledger.write_queue.contains(Writer::ConfirmationHeight)
    });

    let tx = node.ledger.read_txn();
    assert!(node.ledger.confirmed().block_exists(&tx, &send.hash()));

    assert_timely_eq(Duration::from_secs(10), || node.active.len(), 0);
    assert_timely_eq(
        Duration::from_secs(10),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            )
        },
        1,
    );

    // Each block that's confirmed is in the recently_cemented history
    assert_eq!(node.active.recently_cemented_count(), 2);
    assert_eq!(node.active.len(), 0);

    // Confirm the callback is not called under this circumstance
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            )
        },
        1,
    );
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::ConfirmationObserver,
                DetailType::InactiveConfHeight,
                Direction::Out,
            )
        },
        1,
    );
    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::ConfirmationHeight,
                DetailType::BlocksConfirmed,
                Direction::In,
            )
        },
        2,
    );
    assert_eq!(node.ledger.cemented_count(), 3);
    assert_eq!(node.active.vote_applier.election_winner_details_len(), 0);
}
