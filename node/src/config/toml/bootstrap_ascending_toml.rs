use crate::bootstrap::{AccountSetsConfig, BootstrapAscendingConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize)]
pub struct BootstrapAscendingToml {
    pub enable: Option<bool>,
    pub enable_databaser_scan: Option<bool>,
    pub enable_dependency_walker: Option<bool>,
    pub block_processor_threshold: Option<usize>,
    pub database_rate_limit: Option<usize>,
    pub database_warmup_ratio: Option<usize>,
    pub max_pull_count: Option<usize>,
    pub channel_limit: Option<usize>,
    pub throttle_coefficient: Option<usize>,
    pub throttle_wait: Option<u64>,
    pub request_timeout: Option<u64>,
    pub max_requests: Option<usize>,
    pub account_sets: Option<AccountSetsToml>,
}

impl From<&BootstrapAscendingConfig> for BootstrapAscendingToml {
    fn from(config: &BootstrapAscendingConfig) -> Self {
        Self {
            enable: Some(config.enable),
            enable_databaser_scan: Some(config.enable_database_scan),
            enable_dependency_walker: Some(config.enable_dependency_walker),
            channel_limit: Some(config.channel_limit),
            database_rate_limit: Some(config.database_rate_limit),
            database_warmup_ratio: Some(config.database_warmup_ratio),
            max_pull_count: Some(config.max_pull_count),
            request_timeout: Some(config.request_timeout.as_millis() as u64),
            throttle_coefficient: Some(config.throttle_coefficient),
            throttle_wait: Some(config.throttle_wait.as_millis() as u64),
            account_sets: Some((&config.account_sets).into()),
            block_processor_threshold: Some(config.block_processor_theshold),
            max_requests: Some(config.max_requests),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct AccountSetsToml {
    pub blocking_max: Option<usize>,
    pub consideration_count: Option<usize>,
    pub cooldown: Option<u64>,
    pub priorities_max: Option<usize>,
}

impl Default for AccountSetsToml {
    fn default() -> Self {
        let config = AccountSetsConfig::default();
        Self {
            consideration_count: Some(config.consideration_count),
            priorities_max: Some(config.priorities_max),
            blocking_max: Some(config.blocking_max),
            cooldown: Some(config.cooldown.as_millis() as u64),
        }
    }
}

impl From<&AccountSetsConfig> for AccountSetsToml {
    fn from(value: &AccountSetsConfig) -> Self {
        Self {
            consideration_count: Some(value.consideration_count),
            priorities_max: Some(value.priorities_max),
            blocking_max: Some(value.blocking_max),
            cooldown: Some(value.cooldown.as_millis() as u64),
        }
    }
}

impl From<&AccountSetsToml> for AccountSetsConfig {
    fn from(toml: &AccountSetsToml) -> Self {
        let mut config = AccountSetsConfig::default();

        if let Some(blocking_max) = toml.blocking_max {
            config.blocking_max = blocking_max;
        }
        if let Some(consideration_count) = toml.consideration_count {
            config.consideration_count = consideration_count;
        }
        if let Some(priorities_max) = toml.priorities_max {
            config.priorities_max = priorities_max;
        }
        if let Some(cooldown) = &toml.cooldown {
            config.cooldown = Duration::from_millis(*cooldown);
        }
        config
    }
}
