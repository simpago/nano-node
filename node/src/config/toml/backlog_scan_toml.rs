use crate::block_processing::BacklogScanConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct BacklogScanToml {
    pub enable: Option<bool>,
    pub batch_size: Option<usize>,
    pub frequency: Option<usize>,
}

impl From<&BacklogScanConfig> for BacklogScanToml {
    fn from(value: &BacklogScanConfig) -> Self {
        Self {
            enable: Some(value.enabled),
            batch_size: Some(value.batch_size),
            frequency: Some(value.frequency),
        }
    }
}

impl BacklogScanConfig {
    pub(crate) fn merge_toml(&mut self, toml: &BacklogScanToml) {
        if let Some(enable) = toml.enable {
            self.enabled = enable;
        }

        if let Some(size) = toml.batch_size {
            self.batch_size = size;
        }

        if let Some(freq) = toml.frequency {
            self.frequency = freq;
        }
    }
}
