use crate::command_handler::RpcCommandHandler;
use rsnano_core::{BlockType, SavedBlock};
use rsnano_rpc_messages::{BlockInfoResponse, BlockSubTypeDto, HashRpcMessage};

impl RpcCommandHandler {
    pub(crate) fn block_info(&self, args: HashRpcMessage) -> anyhow::Result<BlockInfoResponse> {
        let tx = self.node.ledger.read_txn();
        let block = self.load_block_any(&tx, &args.hash)?;
        let amount = self.node.ledger.any().block_amount_for(&tx, &block);
        let confirmed = self
            .node
            .ledger
            .confirmed()
            .block_exists_or_pruned(&tx, &args.hash);

        Ok(BlockInfoResponse {
            block_account: block.account(),
            amount,
            balance: block.balance(),
            height: block.height().into(),
            local_timestamp: block.timestamp().as_u64().into(),
            successor: block.successor().unwrap_or_default(),
            confirmed: confirmed.into(),
            contents: block.json_representation(),
            subtype: Self::subtype_for(&block),
            source_account: None,
            receive_hash: None,
            receivable: None,
        })
    }

    fn subtype_for(block: &SavedBlock) -> Option<BlockSubTypeDto> {
        if block.block_type() == BlockType::State {
            Some(block.subtype().into())
        } else {
            None
        }
    }
}
