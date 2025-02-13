use crate::command_handler::RpcCommandHandler;
use anyhow::anyhow;
use rsnano_core::{BlockType, SavedBlock};
use rsnano_rpc_messages::{BlockInfoResponse, BlockSubTypeDto, HashRpcMessage};

impl RpcCommandHandler {
    pub(crate) fn block_info(&self, args: HashRpcMessage) -> anyhow::Result<BlockInfoResponse> {
        let tx = self.node.ledger.read_txn();
        let block = self
            .node
            .ledger
            .detailed_block(&tx, &args.hash)
            .ok_or_else(|| anyhow!(Self::BLOCK_NOT_FOUND))?;

        Ok(BlockInfoResponse {
            block_account: block.block.account(),
            amount: block.amount,
            balance: block.block.balance(),
            height: block.block.height().into(),
            local_timestamp: block.block.timestamp().as_u64().into(),
            successor: block.block.successor().unwrap_or_default(),
            confirmed: block.confirmed.into(),
            contents: block.block.json_representation(),
            subtype: Self::subtype_for(&block.block),
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
