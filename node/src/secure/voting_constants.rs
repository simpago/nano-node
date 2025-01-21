use crate::config::NetworkConstants;
use std::time::Duration;

#[derive(Clone)]
pub struct VotingConstants {
    pub max_cache: usize,
    pub delay: Duration,
}

impl VotingConstants {
    pub fn new(network_constants: &NetworkConstants) -> Self {
        Self {
            max_cache: if network_constants.is_dev_network() {
                256
            } else {
                128 * 1024
            },
            delay: if network_constants.is_dev_network() {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(15)
            },
        }
    }
}
