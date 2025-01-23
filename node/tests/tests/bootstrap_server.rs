use rsnano_core::{Account, Block, BlockHash, HashOrAccount, SavedBlock, DEV_GENESIS_KEY};
use rsnano_ledger::DEV_GENESIS_ACCOUNT;
use rsnano_messages::{
    AccountInfoReqPayload, AscPullAck, AscPullAckType, AscPullReq, AscPullReqType,
    BlocksReqPayload, FrontiersReqPayload, HashType, Message,
};
use rsnano_node::{
    bootstrap::BootstrapResponder,
    stats::{DetailType, Direction, StatType},
    Node,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use test_helpers::{
    assert_always_eq, assert_timely_eq, assert_timely_eq2, make_fake_channel, setup_chains, System,
};

#[test]
fn serve_account_blocks() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let mut chains = setup_chains(&node, 1, 128, &DEV_GENESIS_KEY, true);
    let (first_account, first_blocks) = chains.pop().unwrap();

    // Request blocks from account root
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Blocks(BlocksReqPayload {
            start_type: HashType::Account,
            start: first_account.into(),
            count: BootstrapResponder::MAX_BLOCKS as u8,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq(Duration::from_secs(5), || responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Blocks(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.blocks().len(), 128);
    assert!(compare_blocks(response_payload.blocks(), &first_blocks));

    // Ensure we don't get any unexpected responses
    assert_always_eq(Duration::from_secs(1), || responses.len(), 1);
}

#[test]
fn serve_hash() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let mut chains = setup_chains(&node, 1, 256, &DEV_GENESIS_KEY, true);
    let (_, blocks) = chains.pop().unwrap();

    // Skip a few blocks to request hash in the middle of the chain
    let blocks = &blocks[9..];

    // Request blocks from the middle of the chain
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Blocks(BlocksReqPayload {
            start_type: HashType::Block,
            start: blocks[0].hash().into(),
            count: BootstrapResponder::MAX_BLOCKS as u8,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Blocks(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.blocks().len(), 128);
    assert!(compare_blocks(response_payload.blocks(), blocks));

    // Ensure we don't get any unexpected responses
    assert_always_eq(Duration::from_secs(1), || responses.len(), 1);
}

#[test]
fn serve_hash_one() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let mut chains = setup_chains(&node, 1, 256, &DEV_GENESIS_KEY, true);
    let (_account, blocks) = chains.pop().unwrap();

    // Skip a few blocks to request hash in the middle of the chain
    let blocks = &blocks[9..];

    // Request blocks from the middle of the chain
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Blocks(BlocksReqPayload {
            start_type: HashType::Block,
            start: blocks[0].hash().into(),
            count: 1,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Blocks(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.blocks().len(), 1);
    assert_eq!(response_payload.blocks()[0].hash(), blocks[0].hash());
}

#[test]
fn serve_end_of_chain() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let mut chains = setup_chains(&node, 1, 128, &DEV_GENESIS_KEY, true);
    let (_account, blocks) = chains.pop().unwrap();

    // Request blocks from account frontier
    //
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Blocks(BlocksReqPayload {
            start_type: HashType::Block,
            start: blocks.last().unwrap().hash().into(),
            count: BootstrapResponder::MAX_BLOCKS as u8,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq(Duration::from_secs(5), || responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Blocks(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.blocks().len(), 1);
    assert_eq!(
        response_payload.blocks()[0].hash(),
        blocks.last().unwrap().hash()
    );
}

#[test]
fn serve_missing() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    setup_chains(&node, 1, 128, &DEV_GENESIS_KEY, true);

    // Request blocks from account frontier
    //
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Blocks(BlocksReqPayload {
            start_type: HashType::Block,
            start: HashOrAccount::from(42),
            count: BootstrapResponder::MAX_BLOCKS as u8,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Blocks(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.blocks().len(), 0);
}

#[test]
fn serve_multiple() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let chains = setup_chains(&node, 32, 16, &DEV_GENESIS_KEY, true);

    {
        // Request blocks from multiple chains at once
        let mut next_id = 0;
        for (account, _) in &chains {
            // Request blocks from account root
            let request = Message::AscPullReq(AscPullReq {
                id: next_id,
                req_type: AscPullReqType::Blocks(BlocksReqPayload {
                    start_type: HashType::Account,
                    start: (*account).into(),
                    count: BootstrapResponder::MAX_BLOCKS as u8,
                }),
            });
            next_id += 1;

            let channel = make_fake_channel(&node);
            node.inbound_message_queue.put(request, channel);
        }
    }

    assert_timely_eq(Duration::from_secs(15), || responses.len(), chains.len());

    let all_responses = responses.get();
    {
        let mut next_id = 0;
        for (_, blocks) in &chains {
            // Find matching response
            let response = all_responses.iter().find(|r| r.id == next_id).unwrap();

            // Ensure we got response exactly for what we asked for

            let AscPullAckType::Blocks(ref response_payload) = response.pull_type else {
                panic!("wrong ack type")
            };

            assert_eq!(response_payload.blocks().len(), 17); // 1 open block + 16 random blocks
            assert!(compare_blocks(response_payload.blocks(), blocks));

            next_id += 1;
        }
    }
}

#[test]
fn serve_account_info() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let mut chains = setup_chains(&node, 1, 128, &DEV_GENESIS_KEY, true);
    let (account, blocks) = chains.pop().unwrap();

    // Request blocks from account root
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::AccountInfo(AccountInfoReqPayload {
            target: account.into(),
            target_type: HashType::Account,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::AccountInfo(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.account, account);
    assert_eq!(response_payload.account_open, blocks[0].hash());
    assert_eq!(response_payload.account_head, blocks.last().unwrap().hash());
    assert_eq!(response_payload.account_block_count as usize, blocks.len());
    assert_eq!(
        response_payload.account_conf_frontier,
        blocks.last().unwrap().hash()
    );
    assert_eq!(response_payload.account_conf_height as usize, blocks.len());

    // Ensure we don't get any unexpected responses
    assert_always_eq(Duration::from_secs(1), || responses.len(), 1);
}

#[test]
fn serve_account_info_missing() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    setup_chains(&node, 1, 128, &DEV_GENESIS_KEY, true);

    // Request blocks from account root
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::AccountInfo(AccountInfoReqPayload {
            target: HashOrAccount::from(42), // unknown account
            target_type: HashType::Account,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::AccountInfo(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.account, Account::from(42));
    assert_eq!(response_payload.account_open, BlockHash::zero());
    assert_eq!(response_payload.account_head, BlockHash::zero());
    assert_eq!(response_payload.account_block_count, 0);
    assert_eq!(response_payload.account_conf_frontier, BlockHash::zero());
    assert_eq!(response_payload.account_conf_height, 0);

    // Ensure we don't get any unexpected responses
    assert_always_eq(Duration::from_secs(1), || responses.len(), 1);
}

#[test]
fn serve_frontiers() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    let chains = setup_chains(&node, 32, 4, &DEV_GENESIS_KEY, true);

    // Request all frontiers
    let request = Message::AscPullReq(AscPullReq {
        id: 7,
        req_type: AscPullReqType::Frontiers(FrontiersReqPayload {
            start: Account::zero(),
            count: BootstrapResponder::MAX_FRONTIERS as u16,
        }),
    });

    let channel = make_fake_channel(&node);
    node.inbound_message_queue.put(request, channel);

    assert_timely_eq2(|| responses.len(), 1);

    let response = responses.get().pop().unwrap();
    // Ensure we got response exactly for what we asked for
    assert_eq!(response.id, 7);
    let AscPullAckType::Frontiers(response_payload) = response.pull_type else {
        panic!("wrong ack type")
    };

    assert_eq!(response_payload.len(), chains.len() + 1); // +1 for genesis

    // Ensure frontiers match what we expect
    let mut expected_frontiers: HashMap<Account, BlockHash> = chains
        .iter()
        .map(|(account, blocks)| (*account, blocks.last().unwrap().hash()))
        .collect();
    expected_frontiers.insert(*DEV_GENESIS_ACCOUNT, node.latest(&DEV_GENESIS_ACCOUNT));

    for frontier in response_payload {
        assert_eq!(frontier.hash, expected_frontiers[&frontier.account]);
        expected_frontiers.remove(&frontier.account);
    }
    assert!(expected_frontiers.is_empty());
}

#[test]
fn serve_frontiers_invalid_count() {
    let mut system = System::new();
    let node = system.make_node();

    let responses = ResponseHelper::new();
    responses.connect(&node);

    setup_chains(&node, 4, 4, &DEV_GENESIS_KEY, true);

    // Zero count
    {
        let request = Message::AscPullReq(AscPullReq {
            id: 7,
            req_type: AscPullReqType::Frontiers(FrontiersReqPayload {
                start: Account::zero(),
                count: 0,
            }),
        });

        let channel = make_fake_channel(&node);
        node.inbound_message_queue.put(request, channel);
    }

    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::BootstrapServer,
                DetailType::Invalid,
                Direction::In,
            )
        },
        1,
    );

    // Count larger than allowed
    {
        let request = Message::AscPullReq(AscPullReq {
            id: 7,
            req_type: AscPullReqType::Frontiers(FrontiersReqPayload {
                start: Account::zero(),
                count: BootstrapResponder::MAX_FRONTIERS as u16 + 1,
            }),
        });

        let channel = make_fake_channel(&node);
        node.inbound_message_queue.put(request, channel);
    }

    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::BootstrapServer,
                DetailType::Invalid,
                Direction::In,
            )
        },
        2,
    );

    // Max numeric value
    {
        let request = Message::AscPullReq(AscPullReq {
            id: 7,
            req_type: AscPullReqType::Frontiers(FrontiersReqPayload {
                start: Account::zero(),
                count: u16::MAX,
            }),
        });

        let channel = make_fake_channel(&node);
        node.inbound_message_queue.put(request, channel);
    }

    assert_timely_eq(
        Duration::from_secs(5),
        || {
            node.stats.count(
                StatType::BootstrapServer,
                DetailType::Invalid,
                Direction::In,
            )
        },
        3,
    );
}

struct ResponseHelper {
    responses: Arc<Mutex<Vec<AscPullAck>>>,
}

impl ResponseHelper {
    fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn len(&self) -> usize {
        self.responses.lock().unwrap().len()
    }

    fn get(&self) -> Vec<AscPullAck> {
        self.responses.lock().unwrap().clone()
    }

    fn connect(&self, node: &Node) {
        let responses = self.responses.clone();
        node.bootstrap_responder
            .set_response_callback(Box::new(move |response, _channel| {
                responses.lock().unwrap().push(response.clone());
            }));
    }
}

/// Checks if both lists contain the same blocks, with `blocks_b`
fn compare_blocks(blocks_a: &VecDeque<Block>, blocks_b: &[SavedBlock]) -> bool {
    blocks_a
        .iter()
        .zip(blocks_b.iter().map(|b| &**b))
        .all(|(a, b)| a == b)
}
