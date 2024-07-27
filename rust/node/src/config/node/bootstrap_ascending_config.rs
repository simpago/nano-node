use crate::{
    bootstrap::AccountSetsConfig,
    config::{Miliseconds, TomlConfigOverride},
};
use rsnano_core::utils::TomlWriter;
use rsnano_messages::BlocksAckPayload;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone)]
pub struct BootstrapAscendingConfig {
    /// Maximum number of un-responded requests per channel
    pub requests_limit: usize,
    pub database_requests_limit: usize,
    pub pull_count: usize,
    pub timeout: Duration,
    pub throttle_coefficient: usize,
    pub throttle_wait: Duration,
    pub account_sets: AccountSetsConfig,
    pub block_wait_count: usize,
    /** Minimum accepted protocol version used when bootstrapping */
    pub min_protocol_version: u8,
}

impl Default for BootstrapAscendingConfig {
    fn default() -> Self {
        Self {
            requests_limit: 64,
            database_requests_limit: 1024,
            pull_count: BlocksAckPayload::MAX_BLOCKS,
            timeout: Duration::from_secs(3),
            throttle_coefficient: 16,
            throttle_wait: Duration::from_millis(100),
            account_sets: Default::default(),
            block_wait_count: 1000,
            min_protocol_version: 0x14,
        }
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct BootstrapAscendingConfigToml {
    pub requests_limit: Option<usize>,
    pub database_requests_limit: Option<usize>,
    pub pull_count: Option<usize>,
    pub timeout: Option<Miliseconds>,
    pub throttle_coefficient: Option<usize>,
    pub throttle_wait: Option<Miliseconds>,
    pub account_sets: Option<AccountSetsToml>,
    pub block_wait_count: Option<usize>,
}

impl BootstrapAscendingConfig {
    pub fn serialize_toml(&self, toml: &mut dyn TomlWriter) -> anyhow::Result<()> {
        toml.put_usize ("requests_limit", self.requests_limit, "Request limit to ascending bootstrap after which requests will be dropped.\nNote: changing to unlimited (0) is not recommended.\ntype:uint64")?;
        toml.put_usize ("database_requests_limit", self.database_requests_limit, "Request limit for accounts from database after which requests will be dropped.\nNote: changing to unlimited (0) is not recommended as this operation competes for resources on querying the database.\ntype:uint64")?;
        toml.put_usize(
            "pull_count",
            self.pull_count,
            "Number of requested blocks for ascending bootstrap request.\ntype:uint64",
        )?;
        toml.put_u64 ("timeout", self.timeout.as_millis() as u64, "Timeout in milliseconds for incoming ascending bootstrap messages to be processed.\ntype:milliseconds")?;
        toml.put_usize(
            "throttle_coefficient",
            self.throttle_coefficient,
            "Scales the number of samples to track for bootstrap throttling.\ntype:uint64",
        )?;
        toml.put_u64(
            "throttle_wait",
            self.throttle_wait.as_millis() as u64,
            "Length of time to wait between requests when throttled.\ntype:milliseconds",
        )?;
        toml.put_usize(
            "block_wait_count",
            self.block_wait_count,
            "Ascending bootstrap will wait while block processor has more than this many blocks queued.\ntype:uint64",
        )?;
        toml.put_child("account_sets", &mut |child| {
            self.account_sets.serialize_toml(child)
        })
    }
}

impl<'de> TomlConfigOverride<'de, BootstrapAscendingConfigToml> for BootstrapAscendingConfig {
    fn toml_config_override(&mut self, toml: &'de BootstrapAscendingConfigToml) {
        if let Some(account_sets) = &toml.account_sets {
            if let Some(blocking_max) = account_sets.blocking_max {
                self.account_sets.blocking_max = blocking_max;
            }
            if let Some(consideration_count) = account_sets.consideration_count {
                self.account_sets.consideration_count = consideration_count;
            }
            if let Some(priorities_max) = account_sets.priorities_max {
                self.account_sets.priorities_max = priorities_max;
            }
            if let Some(cooldown) = &account_sets.cooldown {
                self.account_sets.cooldown = Duration::from_millis(cooldown.0 as u64);
            }
        }
        if let Some(block_wait_count) = toml.block_wait_count {
            self.block_wait_count = block_wait_count;
        }
        if let Some(database_requests_limit) = toml.database_requests_limit {
            self.database_requests_limit = database_requests_limit;
        }
        if let Some(pull_count) = toml.pull_count {
            self.pull_count = pull_count;
        }
        if let Some(requests_limit) = toml.requests_limit {
            self.requests_limit = requests_limit;
        }
        if let Some(timeout) = &toml.timeout {
            self.timeout = Duration::from_millis(timeout.0 as u64);
        }
        if let Some(throttle_wait) = &toml.throttle_wait {
            self.throttle_wait = Duration::from_millis(throttle_wait.0 as u64);
        }
        if let Some(throttle_coefficient) = toml.throttle_coefficient {
            self.throttle_coefficient = throttle_coefficient;
        }
    }
}

impl From<BootstrapAscendingConfig> for BootstrapAscendingConfigToml {
    fn from(config: BootstrapAscendingConfig) -> Self {
        Self {
            requests_limit: Some(config.requests_limit),
            database_requests_limit: Some(config.database_requests_limit),
            pull_count: Some(config.pull_count),
            timeout: Some(Miliseconds(config.timeout.as_millis())),
            throttle_coefficient: Some(config.throttle_coefficient),
            throttle_wait: Some(Miliseconds(config.throttle_wait.as_millis())),
            account_sets: Some(config.account_sets.into()),
            block_wait_count: Some(config.block_wait_count),
        }
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct AccountSetsToml {
    pub consideration_count: Option<usize>,
    pub priorities_max: Option<usize>,
    pub blocking_max: Option<usize>,
    pub cooldown: Option<Miliseconds>,
}

impl From<AccountSetsConfig> for AccountSetsToml {
    fn from(value: AccountSetsConfig) -> Self {
        Self {
            consideration_count: Some(value.consideration_count),
            priorities_max: Some(value.priorities_max),
            blocking_max: Some(value.blocking_max),
            cooldown: Some(Miliseconds(value.cooldown.as_millis())),
        }
    }
}

impl AccountSetsConfig {
    pub(crate) fn serialize_toml(&self, toml: &mut dyn TomlWriter) -> anyhow::Result<()> {
        toml.put_usize ("consideration_count", self.consideration_count, "Limit the number of account candidates to consider and also the number of iterations.\ntype:uint64")?;
        toml.put_usize(
            "priorities_max",
            self.priorities_max,
            "Cutoff size limit for the priority list.\ntype:uint64",
        )?;
        toml.put_usize(
            "blocking_max",
            self.blocking_max,
            "Cutoff size limit for the blocked accounts from the priority list.\ntype:uint64",
        )?;
        toml.put_u64(
            "cooldown",
            self.cooldown.as_millis() as u64,
            "Waiting time for an account to become available.\ntype:milliseconds",
        )
    }
}
