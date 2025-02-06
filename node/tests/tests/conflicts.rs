use rsnano_core::{Amount, BlockSideband, PrivateKey, SavedBlock, UnsavedBlockLatticeBuilder};
use rsnano_network::ChannelId;
use rsnano_node::block_processing::BlockSource;
use std::time::Duration;
use test_helpers::{
    assert_timely, assert_timely_eq, assert_timely_eq2, start_election, start_elections, System,
};

#[test]
fn start_stop() {
    let mut system = System::new();
    let node1 = system.make_node();
    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send1 = lattice.genesis().send(&key1, Amount::MAX);
    node1.process(send1.clone()).unwrap();
    assert_eq!(node1.active.len(), 0);
    let election1 = start_election(&node1, &send1.hash());
    assert_eq!(node1.active.len(), 1);
    assert_eq!(election1.vote_count(), 1);
}

#[test]
fn add_existing() {
    let mut system = System::new();
    let node1 = system.make_node();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();

    // create a send block to send all of the nano supply to key1
    let send1 = lattice.genesis().send(&key1, Amount::MAX);

    // add the block to ledger as an unconfirmed block
    node1.process(send1.clone()).unwrap();

    // instruct the election scheduler to trigger an election for send1
    start_election(&node1, &send1.hash());

    // wait for election to be started before processing send2
    assert_timely(Duration::from_secs(5), || node1.active.active(&send1));

    let mut fork_lattice = UnsavedBlockLatticeBuilder::new();
    let key2 = PrivateKey::new();
    let send2 = fork_lattice.genesis().send(&key2, Amount::MAX);
    let send2 = SavedBlock::new(send2, BlockSideband::new_test_instance());

    // the block processor will notice that the block is a fork and it will try to publish it
    // which will update the election object
    node1
        .block_processor
        .add(send2.clone().into(), BlockSource::Live, ChannelId::LOOPBACK);

    assert!(node1.active.active(&send1));
    assert_timely(Duration::from_secs(5), || {
        node1.active.active_root(&send2.qualified_root())
    });
}

#[test]
fn add_two() {
    let mut system = System::new();
    let node = system.make_node();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::from(1);
    let key2 = PrivateKey::from(2);
    let key3 = PrivateKey::from(3);

    // create 2 new accounts, that receive 1 raw each, all blocks are force confirmed
    let send1 = lattice.genesis().send(&key1, 1);
    let send2 = lattice.genesis().send(&key2, 1);
    let open1 = lattice.account(&key1).receive(&send1);
    let open2 = lattice.account(&key2).receive(&send2);
    node.process_and_confirm_multi(&[send1, open1.clone(), send2, open2.clone()]);

    let send_a = lattice.account(&key1).send(&key3, 1);
    let send_b = lattice.account(&key2).send(&key3, 1);

    // activate elections for the previous two send blocks (to account3) that we did not forcefully confirm
    node.process(send_a.clone()).unwrap();
    node.process(send_b.clone()).unwrap();
    start_elections(&node, &[send_a.hash(), send_b.hash()], false);

    assert!(node.active.election(&send_a.qualified_root()).is_some());
    assert!(node.active.election(&send_b.qualified_root()).is_some());
}
