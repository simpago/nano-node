use crate::command_handler::RpcCommandHandler;
use anyhow::{anyhow, bail};
use rsnano_core::{Amount, BlockDetails, PendingKey, Root};
use rsnano_node::wallets::{WalletsError, WalletsExt};
use rsnano_rpc_messages::{BlockDto, ReceiveArgs};
use std::cmp::max;

impl RpcCommandHandler {
    pub fn receive(&self, args: ReceiveArgs) -> anyhow::Result<BlockDto> {
        let txn = self.node.ledger.read_txn();

        if !self
            .node
            .ledger
            .any()
            .block_exists_or_pruned(&txn, &args.block)
        {
            bail!(Self::BLOCK_NOT_FOUND);
        }

        let Some(pending_info) = self
            .node
            .ledger
            .any()
            .get_pending(&txn, &PendingKey::new(args.account, args.block))
        else {
            bail!("Block is not receivable");
        };

        let work = if let Some(work) = args.work {
            let (head, epoch) =
                if let Some(info) = self.node.ledger.any().get_account(&txn, &args.account) {
                    // When receiving, epoch version is the higher between the previous and the source blocks
                    let epoch = max(info.epoch, pending_info.epoch);
                    (Root::from(info.head), epoch)
                } else {
                    (Root::from(args.account), pending_info.epoch)
                };
            let details = BlockDetails::new(epoch, false, true, false);
            if self.node.network_params.work.difficulty(&head, work.into())
                < self.node.network_params.work.threshold(&details)
            {
                bail!("Invalid work")
            }
            work.into()
        } else {
            if !self.node.distributed_work.work_generation_enabled() {
                bail!("Work generation is disabled");
            }
            0
        };

        // Representative is only used by receive_action when opening accounts
        // Set a wallet default representative for new accounts
        let representative = self.node.wallets.get_representative(args.wallet)?;

        // Disable work generation if "work" option is provided
        let generate_work = work == 0;

        let block = self
            .node
            .wallets
            .receive_sync(
                args.wallet,
                args.block,
                representative,
                Amount::MAX,
                args.account,
                work,
                generate_work,
            )
            .map_err(|e| match e {
                WalletsError::WalletNotFound => anyhow!("wallet not found"),
                _ => anyhow!("Error generating block"),
            })?;

        Ok(BlockDto::new(block.hash()))
    }
}
