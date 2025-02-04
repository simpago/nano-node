use crate::{config::NetworkConstants, NodeConstants};
use once_cell::sync::Lazy;
use rsnano_core::{work::WorkThresholds, Networks};
use rsnano_ledger::LedgerConstants;

pub static DEV_NETWORK_PARAMS: Lazy<NetworkParams> =
    Lazy::new(|| NetworkParams::new(Networks::NanoDevNetwork));

#[derive(Clone)]
pub struct NetworkParams {
    pub kdf_work: u32,
    pub work: WorkThresholds,
    pub network: NetworkConstants,
    pub ledger: LedgerConstants,
    pub node: NodeConstants,
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
        let kdf_full_work = 64 * 1024;
        let kdf_dev_work = 8;
        Self {
            kdf_work: if network_constants.is_dev_network() {
                kdf_dev_work
            } else {
                kdf_full_work
            },
            work: work.clone(),
            ledger: LedgerConstants::new(work.clone(), network),
            node: NodeConstants::new(&network_constants),
            network: network_constants,
        }
    }
}
