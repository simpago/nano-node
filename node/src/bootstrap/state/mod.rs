mod bootstrap_state;
mod candidate_accounts;
mod frontier_scan;
mod peer_scoring;
mod running_query;
mod running_query_container;

pub use bootstrap_state::BootstrapCounters;
pub(crate) use bootstrap_state::BootstrapState;
pub(crate) use candidate_accounts::*;
pub(crate) use frontier_scan::FrontierScan;
pub use frontier_scan::{FrontierHeadInfo, FrontierScanConfig};
pub(crate) use peer_scoring::PeerScoring;
pub(crate) use running_query::*;
pub(crate) use running_query_container::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum VerifyResult {
    Ok,
    NothingNew,
    Invalid,
}
