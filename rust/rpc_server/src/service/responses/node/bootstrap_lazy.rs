use rsnano_core::BlockHash;
use rsnano_node::{bootstrap::BootstrapInitiatorExt, Node};
use rsnano_rpc_messages::{BootstrapLazyDto, ErrorDto2, RpcDto};
use std::sync::Arc;

pub async fn bootstrap_lazy(
    node: Arc<Node>,
    hash: BlockHash,
    force: Option<bool>,
    id: Option<String>,
) -> RpcDto {
    if node.flags.disable_lazy_bootstrap {
        return RpcDto::Error(ErrorDto2::LazyBootstrapDisabled)
    }

    let force = force.unwrap_or(false);

    let existed = node.bootstrap_initiator.current_lazy_attempt();

    let bootstrap_id = id.unwrap_or_default();

    let key_inserted = node
        .bootstrap_initiator
        .bootstrap_lazy(hash.into(), force, bootstrap_id);

    let started = !existed.is_some() && key_inserted;

    RpcDto::BootstrapLazy(BootstrapLazyDto::new(started, key_inserted))
}
