use crate::config::NetworkConstants;
use once_cell::sync::Lazy;
use rsnano_core::{work::WorkThresholds, Networks};
use rsnano_ledger::LedgerConstants;

pub static DEV_NETWORK_PARAMS: Lazy<NetworkParams> =
    Lazy::new(|| NetworkParams::new(Networks::NanoDevNetwork));

#[derive(Clone)]
pub struct NetworkParams {
    pub work: WorkThresholds,
    pub network: NetworkConstants,
    pub ledger: LedgerConstants,
}

impl NetworkParams {
    pub fn new(network: Networks) -> Self {
        let work = if network == Networks::NanoLiveNetwork {
            WorkThresholds::publish_full()
        } else if network == Networks::NanoBetaNetwork {
            WorkThresholds::publish_beta()
        } else if network == Networks::NanoTestNetwork {
            WorkThresholds::publish_test()
        } else {
            WorkThresholds::publish_dev()
        };
        let network_constants = NetworkConstants::new(work.clone(), network);
        Self {
            work: work.clone(),
            ledger: LedgerConstants::new(work.clone(), network),
            network: network_constants,
        }
    }
}
