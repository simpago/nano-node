use crate::{
    config::NetworkParams,
    representatives::OnlineReps,
    stats::{DetailType, Direction, StatType, Stats},
};
use rsnano_core::{utils::ContainerInfo, Account, PublicKey};
use rsnano_ledger::RepWeightCache;
use std::{
    collections::HashSet,
    mem::size_of,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use strum_macros::EnumIter;
use tracing::debug;

// Higher number means higher priority
#[derive(FromPrimitive, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, EnumIter, Hash, Debug)]
pub enum RepTier {
    None,  // Not a principal representatives
    Tier1, // (0.1-1%) of online stake
    Tier2, // (1-5%) of online stake
    Tier3, // (> 5%) of online stake
}

impl From<RepTier> for DetailType {
    fn from(value: RepTier) -> Self {
        match value {
            RepTier::None => DetailType::None,
            RepTier::Tier1 => DetailType::Tier1,
            RepTier::Tier2 => DetailType::Tier2,
            RepTier::Tier3 => DetailType::Tier3,
        }
    }
}

pub struct RepTiers {
    network_params: NetworkParams,
    thread: Mutex<Option<JoinHandle<()>>>,
    stopped: Arc<Mutex<bool>>,
    condition: Arc<Condvar>,
    rep_tiers_impl: Arc<RepTiersImpl>,
}

impl RepTiers {
    pub fn new(
        rep_weights: Arc<RepWeightCache>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            network_params,
            thread: Mutex::new(None),
            stopped: Arc::new(Mutex::new(false)),
            condition: Arc::new(Condvar::new()),
            rep_tiers_impl: Arc::new(RepTiersImpl::new(stats, online_reps, rep_weights)),
        }
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let stopped_mutex = Arc::clone(&self.stopped);
        let condition = Arc::clone(&self.condition);
        let rep_tiers_impl = Arc::clone(&self.rep_tiers_impl);
        let interval = if self.network_params.network.is_dev_network() {
            Duration::from_millis(500)
        } else {
            Duration::from_secs(10 * 60)
        };

        let join_handle = std::thread::Builder::new()
            .name("Rep tiers".to_string())
            .spawn(move || {
                let mut stopped = stopped_mutex.lock().unwrap();
                while !*stopped {
                    drop(stopped);

                    rep_tiers_impl.calculate_tiers();

                    stopped = stopped_mutex.lock().unwrap();
                    stopped = condition
                        .wait_timeout_while(stopped, interval, |stop| !*stop)
                        .unwrap()
                        .0;
                }
            })
            .unwrap();
        *self.thread.lock().unwrap() = Some(join_handle);
    }

    pub fn stop(&self) {
        *self.stopped.lock().unwrap() = true;
        self.condition.notify_all();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
    }

    pub fn tier(&self, representative: &PublicKey) -> RepTier {
        let tiers = self.rep_tiers_impl.tiers.lock().unwrap();
        if tiers.representatives_3.contains(representative) {
            RepTier::Tier3
        } else if tiers.representatives_2.contains(representative) {
            RepTier::Tier2
        } else if tiers.representatives_1.contains(representative) {
            RepTier::Tier1
        } else {
            RepTier::None
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let tiers = self.rep_tiers_impl.tiers.lock().unwrap();
        [
            (
                "representatives_1",
                tiers.representatives_1.len(),
                size_of::<Account>(),
            ),
            (
                "representatives_2",
                tiers.representatives_2.len(),
                size_of::<Account>(),
            ),
            (
                "representatives_3",
                tiers.representatives_3.len(),
                size_of::<Account>(),
            ),
        ]
        .into()
    }
}

impl Drop for RepTiers {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

#[derive(Default)]
struct Tiers {
    /// 0.1% or above
    representatives_1: HashSet<PublicKey>,
    /// 1% or above
    representatives_2: HashSet<PublicKey>,
    /// 5% or above
    representatives_3: HashSet<PublicKey>,
}

struct RepTiersImpl {
    stats: Arc<Stats>,
    online_reps: Arc<Mutex<OnlineReps>>,
    rep_weights: Arc<RepWeightCache>,
    tiers: Mutex<Tiers>,
}

impl RepTiersImpl {
    fn new(
        stats: Arc<Stats>,
        online_reps: Arc<Mutex<OnlineReps>>,
        rep_weights: Arc<RepWeightCache>,
    ) -> Self {
        Self {
            stats,
            online_reps,
            rep_weights,
            tiers: Mutex::new(Tiers::default()),
        }
    }

    fn calculate_tiers(&self) {
        self.stats.inc(StatType::RepTiers, DetailType::Loop);
        let trended = self.online_reps.lock().unwrap().trended_or_minimum_weight();
        let mut representatives_1_l = HashSet::new();
        let mut representatives_2_l = HashSet::new();
        let mut representatives_3_l = HashSet::new();
        let mut ignored = 0;
        let reps_count;
        {
            let rep_weights = self.rep_weights.read();
            reps_count = rep_weights.len();
            for (&representative, &weight) in rep_weights.iter() {
                if weight > trended / 1000 {
                    // 0.1% or above (level 1)
                    representatives_1_l.insert(representative);
                    if weight > trended / 100 {
                        // 1% or above (level 2)
                        representatives_2_l.insert(representative);
                        if weight > trended / 20 {
                            // 5% or above (level 3)
                            representatives_3_l.insert(representative);
                        }
                    }
                } else {
                    ignored += 1;
                }
            }
        }

        self.stats.add_dir(
            StatType::RepTiers,
            DetailType::Processed,
            Direction::In,
            reps_count as u64,
        );

        self.stats.add_dir(
            StatType::RepTiers,
            DetailType::Ignored,
            Direction::In,
            ignored,
        );

        debug!(
            "Representative tiers updated, tier 1: {}, tier 2: {}, tier 3: {} ({} ignored)",
            representatives_1_l.len(),
            representatives_2_l.len(),
            representatives_3_l.len(),
            ignored
        );

        {
            let mut guard = self.tiers.lock().unwrap();
            guard.representatives_1 = representatives_1_l;
            guard.representatives_2 = representatives_2_l;
            guard.representatives_3 = representatives_3_l;
        }

        self.stats.inc(StatType::RepTiers, DetailType::Updated);
    }
}
