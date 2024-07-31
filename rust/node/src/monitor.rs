use crate::{
    consensus::ActiveElections,
    representatives::OnlineReps,
    transport::Network,
    utils::{CancellationToken, Runnable},
};
use rsnano_core::utils::TomlWriter;
use rsnano_ledger::Ledger;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::info;

#[derive(Clone)]
pub struct MonitorConfig {
    pub enabled: bool,
    pub interval: Duration,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(60),
        }
    }
}

impl MonitorConfig {
    pub fn serialize_toml(&self, toml: &mut dyn TomlWriter) -> anyhow::Result<()> {
        toml.put_bool(
            "enable",
            self.enabled,
            "Enable or disable periodic node status logging\ntype:bool",
        )?;

        toml.put_u64(
            "interval",
            self.interval.as_secs(),
            "Interval between status logs\ntype:seconds",
        )
    }
}

pub struct Monitor {
    ledger: Arc<Ledger>,
    network: Arc<Network>,
    online_reps: Arc<Mutex<OnlineReps>>,
    active: Arc<ActiveElections>,
    last_time: Option<Instant>,
    last_blocks_cemented: u64,
    last_blocks_total: u64,
}

impl Monitor {
    pub fn new(
        ledger: Arc<Ledger>,
        network: Arc<Network>,
        online_peers: Arc<Mutex<OnlineReps>>,
        active: Arc<ActiveElections>,
    ) -> Self {
        Self {
            ledger,
            network,
            online_reps: online_peers,
            active,
            last_time: None,
            last_blocks_total: 0,
            last_blocks_cemented: 0,
        }
    }

    fn log(&self, last: Instant, blocks_cemented: u64, blocks_total: u64) {
        // TODO: Maybe emphasize somehow that confirmed doesn't need to be equal to total; backlog is OK
        info!(
            "Blocks confirmed: {} | total: {}",
            blocks_cemented, blocks_total
        );

        // Calculate the rates
        let elapsed_secs = last.elapsed().as_secs() as f64;
        let blocks_confirmed_rate =
            (blocks_cemented - self.last_blocks_cemented) as f64 / elapsed_secs;
        let blocks_checked_rate = (blocks_total - self.last_blocks_total) as f64 / elapsed_secs;

        info!(
            "Blocks rate (average over last {}s: confirmed: {:.2}/s | total {:.2}/s)",
            elapsed_secs, blocks_confirmed_rate, blocks_checked_rate
        );

        let channels = self.network.channels_info();
        info!("Peers: {} (realtime: {} | bootstrap: {} | inbound connections: {} | outbound connections: {})",
            channels.total, channels.realtime, channels.bootstrap, channels.inbound, channels.outbound);

        {
            let (delta, online, peered) = {
                let online_reps = self.online_reps.lock().unwrap();
                (
                    online_reps.quorum_delta(),
                    online_reps.online_weight(),
                    online_reps.peered_weight(),
                )
            };
            info!(
                "Quorum: {} (stake peered: {} | online stake: {})",
                delta.format_balance(0),
                online.format_balance(0),
                peered.format_balance(0)
            );
        }

        let elections = self.active.info();
        info!(
            "Elections active: {} (priority: {} | hinted: {} | optimistic: {})",
            elections.total, elections.priority, elections.hinted, elections.optimistic
        );
    }
}

impl Runnable for Monitor {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let blocks_cemented = self.ledger.cemented_count();
        let blocks_total = self.ledger.block_count();

        if let Some(last) = self.last_time {
            self.log(last, blocks_cemented, blocks_total);
        } else {
            // Wait for node to warm up before logging
        }
        self.last_time = Some(Instant::now());
        self.last_blocks_cemented = blocks_cemented;
        self.last_blocks_total = blocks_total;
    }
}
