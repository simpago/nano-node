use rsnano_core::{
    Account, Amount, Block, BlockHash, Epoch, PrivateKey, Signature, StateBlockArgs,
    UnsavedBlockLatticeBuilder, Vote, VoteCode, VoteSource, DEV_GENESIS_KEY,
};
use rsnano_ledger::{BlockStatus, DEV_GENESIS_PUB_KEY};
use rsnano_network::ChannelId;
use rsnano_node::{block_processing::BlockSource, config::NodeConfig};
use std::{sync::Arc, time::Duration};
use test_helpers::{assert_timely, assert_timely_eq, start_elections, System};

mod votes {
    use super::*;
    use rsnano_core::UnsavedBlockLatticeBuilder;
    use rsnano_node::consensus::ActiveElectionsExt;
    use std::time::SystemTime;
    use test_helpers::start_election;

    #[test]
    fn add_one() {
        let mut system = System::new();
        let node1 = system.make_node();

        let mut lattice = UnsavedBlockLatticeBuilder::new();
        let key1 = PrivateKey::new();
        let send1 = lattice.genesis().legacy_send(&key1, 100);
        let send1 = node1.process(send1);
        node1
            .election_schedulers
            .manual
            .push(send1.clone().into(), None);

        assert_timely(Duration::from_secs(5), || {
            node1.active.election(&send1.qualified_root()).is_some()
        });

        let election1 = node1.active.election(&send1.qualified_root()).unwrap();
        assert_eq!(election1.vote_count(), 1);
        let vote1 = Arc::new(Vote::new(
            &DEV_GENESIS_KEY,
            Vote::TIMESTAMP_MIN,
            0,
            vec![send1.hash()],
        ));
        assert_eq!(
            node1
                .vote_router
                .vote(&vote1, VoteSource::Live)
                .values()
                .next()
                .unwrap(),
            &VoteCode::Vote
        );
        let vote2 = Arc::new(Vote::new(
            &DEV_GENESIS_KEY,
            Vote::TIMESTAMP_MIN * 2,
            0,
            vec![send1.hash()],
        ));

        // Ignored due to vote cooldown
        assert_eq!(
            node1
                .vote_router
                .vote(&vote2, VoteSource::Live)
                .values()
                .next()
                .unwrap(),
            &VoteCode::Ignored
        );

        assert_eq!(election1.vote_count(), 2);
        assert_eq!(
            election1
                .mutex
                .lock()
                .unwrap()
                .last_votes
                .get(&DEV_GENESIS_PUB_KEY)
                .unwrap()
                .hash,
            send1.hash()
        );

        let guard = election1.mutex.lock().unwrap();
        let (hash, amount) = guard.last_tally.iter().next().unwrap();
        assert_eq!(*hash, send1.hash());
        assert_eq!(*amount, Amount::MAX - Amount::raw(100));
    }

    #[test]
    fn add_existing() {
        let mut system = System::new();
        let config = NodeConfig {
            online_weight_minimum: Amount::MAX,
            ..System::default_config_without_backlog_scan()
        };
        let node1 = system.build_node().config(config).finish();
        let mut lattice = UnsavedBlockLatticeBuilder::new();
        let key1 = PrivateKey::new();

        // No representative, blocks can't confirm
        let send1 = lattice
            .genesis()
            .send_all_except(&key1, Amount::MAX / 2 - Amount::nano(1000));
        node1.process(send1.clone());
        let election1 = start_election(&node1, &send1.hash());
        let vote1 = Arc::new(Vote::new(
            &DEV_GENESIS_KEY,
            Vote::TIMESTAMP_MIN,
            0,
            vec![send1.hash()],
        ));
        node1.vote_router.vote(&vote1, VoteSource::Live);
        // Block is already processed from vote
        assert!(node1.active.publish_block(&send1));
        assert_eq!(
            election1
                .mutex
                .lock()
                .unwrap()
                .last_votes
                .get(&DEV_GENESIS_PUB_KEY)
                .unwrap()
                .timestamp,
            Vote::TIMESTAMP_MIN
        );

        let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
        let key2 = PrivateKey::new();

        // No representative, blocks can't confirm
        let send2 = fork_lattice
            .genesis()
            .send_all_except(&key2, Amount::MAX / 2 - Amount::nano(1000));

        assert_eq!(node1.active.publish_block(&send2), false);
        assert_timely(Duration::from_secs(5), || node1.active.active(&send2));
        let vote2 = Arc::new(Vote::new(
            &DEV_GENESIS_KEY,
            Vote::TIMESTAMP_MIN * 2,
            0,
            vec![send2.hash()],
        ));
        // Pretend we've waited the timeout
        election1
            .mutex
            .lock()
            .unwrap()
            .last_votes
            .get_mut(&DEV_GENESIS_PUB_KEY)
            .unwrap()
            .time = SystemTime::now() - Duration::from_secs(20);
        assert_eq!(
            node1
                .vote_router
                .vote(&vote2, VoteSource::Live)
                .get(&send2.hash())
                .unwrap(),
            &VoteCode::Vote
        );
        assert_eq!(
            election1
                .mutex
                .lock()
                .unwrap()
                .last_votes
                .get(&DEV_GENESIS_PUB_KEY)
                .unwrap()
                .timestamp,
            Vote::TIMESTAMP_MIN * 2
        );
        // Also resend the old vote, and see if we respect the timestamp
        election1
            .mutex
            .lock()
            .unwrap()
            .last_votes
            .get_mut(&DEV_GENESIS_PUB_KEY)
            .unwrap()
            .time = SystemTime::now() - Duration::from_secs(20);

        assert_eq!(
            node1
                .vote_router
                .vote(&vote1, VoteSource::Live)
                .get(&send1.hash())
                .unwrap(),
            &VoteCode::Replay
        );
        assert_eq!(
            election1
                .mutex
                .lock()
                .unwrap()
                .last_votes
                .get(&DEV_GENESIS_PUB_KEY)
                .unwrap()
                .timestamp,
            Vote::TIMESTAMP_MIN * 2
        );
        let votes = election1.mutex.lock().unwrap().last_votes.clone();
        assert_eq!(votes.len(), 2);
        assert!(votes.contains_key(&DEV_GENESIS_PUB_KEY));
        assert_eq!(votes.get(&DEV_GENESIS_PUB_KEY).unwrap().hash, send2.hash());
    }
}

#[test]
fn epoch_open_pending() {
    let mut system = System::new();
    let node1 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();

    let send1 = lattice.genesis().send(&key1, 100);
    let epoch_open = lattice.epoch_open(&key1);

    let status = node1.try_process(epoch_open.clone()).unwrap_err();
    assert_eq!(status, BlockStatus::GapEpochOpenPending);
    node1.block_processor.add(
        epoch_open.clone().into(),
        BlockSource::Live,
        ChannelId::LOOPBACK,
    );
    // Waits for the block to get saved in the database
    assert_timely_eq(Duration::from_secs(10), || node1.unchecked.len(), 1);
    // Open block should be inserted into unchecked
    let blocks = node1.unchecked.get(&key1.account().into());
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].block.hash(), epoch_open.hash());
    // New block to process epoch open
    node1
        .block_processor
        .add(send1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    assert_timely(Duration::from_secs(10), || {
        node1.block_exists(&epoch_open.hash())
    });
}

#[test]
fn block_hash_account_conflict() {
    let mut system = System::new();
    let node1 = system.make_node();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();

    /*
     * Generate a send block whose destination is a block hash already
     * in the ledger and not an account
     */
    let send1 = lattice.genesis().send(&key1, 100);
    let receive1 = lattice.account(&key1).receive(&send1);

    /*
     * Note that the below link is a block hash when this is intended
     * to represent a send state block. This can generally never be
     * received , except by epoch blocks, which can sign an open block
     * for arbitrary accounts.
     */
    let unreceivable_account = Account::from_bytes(*receive1.hash().as_bytes());
    let send2 = lattice.account(&key1).send(unreceivable_account, 10);

    /*
     * Generate an epoch open for the account with the same value as the block hash
     */
    let open_epoch1 = lattice.epoch_open(unreceivable_account);

    node1.process_multi(&[
        send1.clone(),
        receive1.clone(),
        send2.clone(),
        open_epoch1.clone(),
    ]);

    start_elections(
        &node1,
        &[
            send1.hash(),
            receive1.hash(),
            send2.hash(),
            open_epoch1.hash(),
        ],
        false,
    );
    let election1 = node1.active.election(&send1.qualified_root()).unwrap();
    let election2 = node1.active.election(&receive1.qualified_root()).unwrap();
    let election3 = node1.active.election(&send2.qualified_root()).unwrap();
    let election4 = node1
        .active
        .election(&open_epoch1.qualified_root())
        .unwrap();

    assert_eq!(election1.winner_hash().unwrap(), send1.hash());
    assert_eq!(election2.winner_hash().unwrap(), receive1.hash());
    assert_eq!(election3.winner_hash().unwrap(), send2.hash());
    assert_eq!(election4.winner_hash().unwrap(), open_epoch1.hash());
}

#[test]
fn unchecked_epoch() {
    let mut system = System::new();
    let node1 = system.make_node();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let destination = PrivateKey::new();

    let send1 = lattice.genesis().send(&destination, Amount::nano(1000));
    let open1 = lattice.account(&destination).receive(&send1);
    let epoch1 = lattice.account(&destination).epoch1();

    node1.block_processor.add(
        epoch1.clone().into(),
        BlockSource::Live,
        ChannelId::LOOPBACK,
    );

    // Waits for the epoch1 block to pass through block_processor and unchecked.put queues
    assert_timely_eq(Duration::from_secs(10), || node1.unchecked.len(), 1);
    node1
        .block_processor
        .add(send1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    node1
        .block_processor
        .add(open1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    assert_timely(Duration::from_secs(5), || {
        node1
            .ledger
            .any()
            .block_exists(&node1.ledger.read_txn(), &epoch1.hash())
    });

    // Waits for the last blocks to pass through block_processor and unchecked.put queues
    assert_timely_eq(Duration::from_secs(10), || node1.unchecked.len(), 0);
    let info = node1
        .ledger
        .any()
        .get_account(&node1.ledger.read_txn(), &destination.account())
        .unwrap();
    assert_eq!(info.epoch, Epoch::Epoch1);
}

#[test]
fn unchecked_epoch_invalid() {
    let mut system = System::new();
    let node1 = system
        .build_node()
        .config(System::default_config_without_backlog_scan())
        .finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let destination = PrivateKey::new();

    let send1 = lattice.genesis().send(&destination, Amount::nano(1000));
    let open1 = lattice.account(&destination).receive(&send1);

    // Epoch block with account own signature
    let epoch1: Block = StateBlockArgs {
        key: &destination,
        previous: open1.hash(),
        representative: destination.public_key(),
        balance: Amount::nano(1000),
        link: node1.ledger.epoch_link(Epoch::Epoch1).unwrap(),
        work: node1.work_generate_dev(open1.hash()),
    }
    .into();

    // Pseudo epoch block (send subtype, destination - epoch link)
    let epoch2: Block = StateBlockArgs {
        key: &destination,
        previous: open1.hash(),
        representative: destination.public_key(),
        balance: Amount::nano(999),
        link: node1.ledger.epoch_link(Epoch::Epoch1).unwrap(),
        work: node1.work_generate_dev(open1.hash()),
    }
    .into();

    node1.block_processor.add(
        epoch1.clone().into(),
        BlockSource::Live,
        ChannelId::LOOPBACK,
    );
    node1.block_processor.add(
        epoch2.clone().into(),
        BlockSource::Live,
        ChannelId::LOOPBACK,
    );

    // Waits for the last blocks to pass through block_processor and unchecked.put queues
    assert_timely_eq(Duration::from_secs(10), || node1.unchecked.len(), 2);
    node1
        .block_processor
        .add(send1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    node1
        .block_processor
        .add(open1.into(), BlockSource::Live, ChannelId::LOOPBACK);

    // Waits for the last blocks to pass through block_processor and unchecked.put queues
    assert_timely(Duration::from_secs(10), || {
        node1
            .ledger
            .any()
            .block_exists(&node1.ledger.read_txn(), &epoch2.hash())
    });

    let tx = node1.ledger.read_txn();
    assert_eq!(node1.ledger.any().block_exists(&tx, &epoch1.hash()), false);
    assert_eq!(node1.unchecked.len(), 0);
    let info = node1
        .ledger
        .any()
        .get_account(&tx, &destination.account())
        .unwrap();
    assert_eq!(info.epoch, Epoch::Epoch0);
    let epoch2_store = node1.block(&epoch2.hash()).unwrap();
    assert_eq!(epoch2_store.epoch(), Epoch::Epoch0);
    assert!(epoch2_store.is_send());
    assert_eq!(epoch2_store.is_epoch(), false);
    assert_eq!(epoch2_store.is_receive(), false);
}

#[test]
fn unchecked_open() {
    let mut system = System::new();
    let node1 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let destination = PrivateKey::new();
    let send1 = lattice.genesis().send(&destination, Amount::nano(1000));
    let open1 = lattice.account(&destination).receive(&send1);
    // Invalid signature for open block
    let mut open2 = open1.clone();
    open2.set_signature(Signature::from_bytes([1; 64]));

    // Insert open2 in to the queue before open1
    node1
        .block_processor
        .add(open2.into(), BlockSource::Live, ChannelId::LOOPBACK);
    node1
        .block_processor
        .add(open1.clone().into(), BlockSource::Live, ChannelId::LOOPBACK);

    // Waits for the last blocks to pass through block_processor and unchecked.put queues
    assert_timely_eq(Duration::from_secs(5), || node1.unchecked.len(), 1);
    // When open1 existists in unchecked, we know open2 has been processed.
    node1
        .block_processor
        .add(send1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    // Waits for the send1 block to pass through block_processor and unchecked.put queues
    assert_timely(Duration::from_secs(5), || node1.block_exists(&open1.hash()));
    assert_eq!(node1.unchecked.len(), 0);
}

#[test]
fn unchecked_receive() {
    let mut system = System::new();
    let node1 = system.make_node();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let destination = PrivateKey::new();
    let send1 = lattice.genesis().send(&destination, Amount::nano(1000));
    let send2 = lattice.genesis().send(&destination, Amount::nano(1000));
    let open1 = lattice.account(&destination).receive(&send1);
    let receive1 = lattice.account(&destination).receive(&send2);
    node1
        .block_processor
        .add(send1.into(), BlockSource::Live, ChannelId::LOOPBACK);
    node1.block_processor.add(
        receive1.clone().into(),
        BlockSource::Live,
        ChannelId::LOOPBACK,
    );
    let check_block_is_listed =
        |hash: &BlockHash| !node1.unchecked.get(&((*hash).into())).is_empty();
    // Previous block for receive1 is unknown, signature cannot be validated

    // Waits for the last blocks to pass through block_processor and unchecked.put queues
    assert_timely(Duration::from_secs(15), || {
        check_block_is_listed(&receive1.previous())
    });
    assert_eq!(node1.unchecked.get(&receive1.previous().into()).len(), 1);

    // Waits for the open1 block to pass through block_processor and unchecked.put queues
    node1
        .block_processor
        .add(open1.clone().into(), BlockSource::Live, ChannelId::LOOPBACK);
    assert_timely(Duration::from_secs(15), || {
        check_block_is_listed(&receive1.source_or_link())
    });
    // Previous block for receive1 is known, signature was validated
    assert_eq!(
        node1.unchecked.get(&receive1.source_or_link().into()).len(),
        1
    );
    node1
        .block_processor
        .add(send2.clone().into(), BlockSource::Live, ChannelId::LOOPBACK);
    assert_timely(Duration::from_secs(10), || {
        node1.block_exists(&receive1.hash())
    });
    assert_eq!(node1.unchecked.len(), 0);
}
