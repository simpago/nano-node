use rsnano_core::{Account, BlockHash};
use rsnano_node::Node;
use rsnano_rpc_messages::{FrontiersDto, RpcDto};
use std::{collections::HashMap, sync::Arc};

pub async fn frontiers(node: Arc<Node>, account: Account, count: u64) -> RpcDto {
    let tx = node.ledger.read_txn();
    let mut frontiers: HashMap<Account, BlockHash> = HashMap::new();

    let mut iterator = node.store.account.begin_account(&tx, &account);

    let mut collected = 0;

    while collected < count {
        if let Some((account, account_info)) = iterator.current() {
            frontiers.insert(*account, account_info.head);
            collected += 1;
            iterator.next();
        } else {
            break;
        }
    }

    RpcDto::Frontiers(FrontiersDto::new(frontiers))
}
