use crate::wallets::WalletRepresentatives;
use rsnano_core::PublicKey;
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

/// Caches the information about representatives in the wallets, so that
/// it can be accessed without requiring a lock on the wallets
pub(crate) struct WalletRepsCache {
    wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    wallet_reps_copy: WalletRepresentatives,
    last_refresh: Option<Timestamp>,
    clock: SteadyClock,
    refresh_count: usize,
}

impl WalletRepsCache {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

    pub fn new(wallet_reps: Arc<Mutex<WalletRepresentatives>>) -> Self {
        Self {
            wallet_reps,
            wallet_reps_copy: Default::default(),
            last_refresh: None,
            clock: SteadyClock::default(),
            refresh_count: 0,
        }
    }

    #[cfg(test)]
    pub fn set_clock(&mut self, clock: SteadyClock) {
        self.clock = clock;
    }

    pub fn have_half_rep(&self) -> bool {
        self.wallet_reps_copy.have_half_rep()
    }

    pub fn exists(&self, pub_key: impl Into<PublicKey>) -> bool {
        self.wallet_reps_copy.exists(&pub_key.into().into())
    }

    pub fn refresh_if_needed(&mut self) {
        self.refresh_count += 1;
        let now = self.clock.now();
        if self.should_refresh(now) {
            self.refresh(now);
        }
    }

    fn should_refresh(&self, now: Timestamp) -> bool {
        match &self.last_refresh {
            Some(last) => now - *last >= Self::REFRESH_INTERVAL,
            None => true,
        }
    }

    fn refresh(&mut self, now: Timestamp) {
        self.wallet_reps_copy = self.wallet_reps.lock().unwrap().clone();
        self.last_refresh = Some(now);
    }

    #[allow(dead_code)]
    pub fn refresh_count(&self) -> usize {
        self.refresh_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_nullable_clock::SteadyClock;

    #[test]
    fn default_values() {
        let wallet_reps = test_wallet_reps();
        let cache = WalletRepsCache::new(wallet_reps.clone());

        assert_eq!(cache.have_half_rep(), false);
        assert_eq!(cache.exists(TEST_REP), false);
    }

    #[test]
    fn returns_wallet_reps_data_when_refreshed() {
        let wallet_reps = test_wallet_reps();
        let mut cache = WalletRepsCache::new(wallet_reps.clone());

        cache.refresh_if_needed();

        assert_eq!(cache.have_half_rep(), true);
        assert_eq!(cache.exists(TEST_REP), true);
        assert_eq!(cache.exists(PublicKey::from(123)), false);
    }

    #[test]
    fn cache_stays_unchanged_if_not_refreshed() {
        let wallet_reps = test_wallet_reps();
        let mut cache = WalletRepsCache::new(wallet_reps.clone());

        cache.refresh_if_needed();
        wallet_reps.lock().unwrap().clear();

        assert_eq!(cache.have_half_rep(), true);
        assert_eq!(cache.exists(TEST_REP), true);
    }

    #[test]
    fn dont_refresh_if_refresh_interval_not_reached() {
        let wallet_reps = test_wallet_reps();
        let mut cache = WalletRepsCache::new(wallet_reps.clone());
        let clock = SteadyClock::new_null_with([Duration::from_millis(100)]);
        cache.set_clock(clock);

        cache.refresh_if_needed();
        wallet_reps.lock().unwrap().clear();
        cache.refresh_if_needed();

        assert_eq!(cache.have_half_rep(), true);
    }

    #[test]
    fn refresh_if_refresh_interval_reached() {
        let wallet_reps = test_wallet_reps();
        let mut cache = WalletRepsCache::new(wallet_reps.clone());
        let clock = SteadyClock::new_null_with([WalletRepsCache::REFRESH_INTERVAL]);
        cache.set_clock(clock);

        cache.refresh_if_needed();
        wallet_reps.lock().unwrap().clear();
        cache.refresh_if_needed();

        assert_eq!(cache.have_half_rep(), false);
    }

    #[test]
    fn refreshes_can_be_tracked() {
        let wallet_reps = test_wallet_reps();
        let mut cache = WalletRepsCache::new(wallet_reps.clone());
        assert_eq!(cache.refresh_count(), 0);

        cache.refresh_if_needed();
        assert_eq!(cache.refresh_count(), 1);
        cache.refresh_if_needed();
        assert_eq!(cache.refresh_count(), 2);
    }

    fn test_wallet_reps() -> Arc<Mutex<WalletRepresentatives>> {
        let mut wallet_reps = WalletRepresentatives::default();
        wallet_reps.set_have_half_rep(true);
        wallet_reps.insert(TEST_REP);
        Arc::new(Mutex::new(wallet_reps))
    }

    const TEST_REP: PublicKey = PublicKey::from_bytes([1; 32]);
}
