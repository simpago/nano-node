use crate::command_handler::RpcCommandHandler;
use rsnano_rpc_messages::{HashRpcMessage, SuccessResponse};

impl RpcCommandHandler {
    pub(crate) fn work_cancel(&self, args: HashRpcMessage) -> SuccessResponse {
        self.node.distributed_work.cancel(args.hash.into());
        SuccessResponse::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::command_handler::{test_rpc_command_requires_control, test_rpc_command_with_node};
    use rsnano_core::Root;
    use rsnano_node::Node;
    use rsnano_rpc_messages::{HashRpcMessage, RpcCommand, SuccessResponse};
    use std::sync::Arc;

    #[tokio::test]
    async fn without_rpc_control_enabled() {
        test_rpc_command_requires_control(RpcCommand::WorkCancel(HashRpcMessage {
            hash: 1.into(),
        }));
    }

    #[tokio::test]
    async fn handle_work_cancel_command() {
        let node = Arc::new(Node::new_null());
        let cancel_tracker = node.distributed_work.track_cancellations();
        let root = Root::from(42);

        let result: SuccessResponse = test_rpc_command_with_node(
            RpcCommand::WorkCancel(HashRpcMessage { hash: root.into() }),
            node,
        );

        assert_eq!(cancel_tracker.output(), [root]);
    }
}
