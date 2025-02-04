mod builder;
mod cleanup;
mod online_container;
mod peered_container;
mod peered_rep;

pub use builder::OnlineRepsBuilder;
pub use cleanup::*;
pub use peered_container::InsertResult;
pub use peered_rep::PeeredRep;
use primitive_types::U256;
use rsnano_core::{utils::ContainerInfo, Amount, Networks, PublicKey};
use rsnano_ledger::RepWeightCache;
use rsnano_network::{Channel, ChannelId};
use rsnano_nullable_clock::Timestamp;
use std::{cmp::max, sync::Arc, time::Duration};
use {online_container::OnlineContainer, peered_container::PeeredContainer};

const ONLINE_WEIGHT_QUORUM: u8 = 67;

/// Keeps track of all representatives that are online
/// and all representatives to which we have a direct connection
pub struct OnlineReps {
    rep_weights: Arc<RepWeightCache>,
    online_reps: OnlineContainer,
    peered_reps: PeeredContainer,
    trended_weight: Amount,
    online_weight: Amount,
    /// Time between collecting online representative samples
    weight_interval: Duration,
    online_weight_minimum: Amount,
    representative_weight_minimum: Amount,
}

impl OnlineReps {
    pub const DEFAULT_ONLINE_WEIGHT_MINIMUM: Amount = Amount::nano(60_000_000);

    pub const fn default_interval_for(network: Networks) -> Duration {
        match network {
            Networks::NanoDevNetwork => Duration::from_secs(1),
            _ => Duration::from_secs(5 * 60),
        }
    }

    pub(crate) fn new(
        rep_weights: Arc<RepWeightCache>,
        weight_interval: Duration,
        online_weight_minimum: Amount,
        representative_weight_minimum: Amount,
    ) -> Self {
        Self {
            rep_weights,
            online_reps: OnlineContainer::new(),
            peered_reps: PeeredContainer::new(),
            trended_weight: Amount::zero(),
            online_weight: Amount::zero(),
            weight_interval,
            online_weight_minimum,
            representative_weight_minimum,
        }
    }

    pub fn builder() -> OnlineRepsBuilder {
        OnlineRepsBuilder::new()
    }

    pub fn online_weight_minimum(&self) -> Amount {
        self.online_weight_minimum
    }

    // TODO remove
    pub fn set_online(&mut self, amount: Amount) {
        self.online_weight = amount;
    }

    #[allow(dead_code)]
    fn trended_weight(&self) -> Amount {
        self.trended_weight
    }

    pub fn trended_or_minimum_weight(&self) -> Amount {
        max(self.trended_weight, self.online_weight_minimum)
    }

    pub fn set_trended(&mut self, trended: Amount) {
        self.trended_weight = trended;
    }

    /** Returns the current online stake */
    pub fn online_weight(&self) -> Amount {
        // TODO calculate on the fly
        self.online_weight
    }

    pub fn minimum_principal_weight(&self) -> Amount {
        self.trended_or_minimum_weight() / 1000 // 0.1% of trended online weight
    }

    /// Query if a peer manages a principle representative
    pub fn is_pr(&self, channel_id: ChannelId) -> bool {
        let min_weight = self.minimum_principal_weight();
        self.peered_reps
            .accounts_by_channel(channel_id)
            .any(|account| self.rep_weights.weight(account) >= min_weight)
    }

    /// Get total available weight from peered representatives
    pub fn peered_weight(&self) -> Amount {
        let mut result = Amount::zero();
        let weights = self.rep_weights.read();
        for account in self.peered_reps.accounts() {
            result += weights.get(account).cloned().unwrap_or_default();
        }
        result
    }

    /// Total number of peered representatives
    pub fn peered_reps_count(&self) -> usize {
        self.peered_reps.len()
    }

    pub fn quorum_percent(&self) -> u8 {
        ONLINE_WEIGHT_QUORUM
    }

    /// Returns the quorum required for confirmation
    pub fn quorum_delta(&self) -> Amount {
        let weight = max(self.online_weight(), self.trended_or_minimum_weight());

        // Using a larger container to ensure maximum precision
        let delta =
            U256::from(weight.number()) * U256::from(ONLINE_WEIGHT_QUORUM) / U256::from(100);
        Amount::raw(delta.as_u128())
    }

    pub fn on_rep_request(&mut self, channel_id: ChannelId, now: Timestamp) {
        // Find and update the timestamp on all reps available on the endpoint (a single host may have multiple reps)
        self.peered_reps.modify_by_channel(channel_id, |rep| {
            rep.last_request = now;
        });
    }

    pub fn last_request_elapsed(&self, channel_id: ChannelId, now: Timestamp) -> Option<Duration> {
        self.peered_reps
            .iter_by_channel(channel_id)
            .next()
            .map(|rep| rep.last_request.elapsed(now))
    }

    /// List of online representatives, both the currently sampling ones and the ones observed in the previous sampling period
    pub fn online_reps(&self) -> impl Iterator<Item = &PublicKey> {
        self.online_reps.iter()
    }

    /// Request a list of the top \p count known representatives in descending order of weight, with at least \p weight_a voting weight, and optionally with a minimum version \p minimum_protocol_version
    pub fn peered_reps(&self) -> Vec<PeeredRep> {
        self.representatives_filter(Amount::zero())
    }

    /// Request a list of the top \p count known principal representatives in descending order of weight, optionally with a minimum version \p minimum_protocol_version
    pub fn peered_principal_reps(&self) -> Vec<PeeredRep> {
        self.representatives_filter(self.minimum_principal_weight())
    }

    /// Request a list of known representatives in descending order
    /// of weight, with at least **weight** voting weight
    pub fn representatives_filter(&self, min_weight: Amount) -> Vec<PeeredRep> {
        let mut reps_with_weight = Vec::new();

        for rep in self.peered_reps.iter() {
            let weight = self.rep_weights.weight(&rep.account);
            if weight > min_weight {
                reps_with_weight.push((rep.clone(), weight));
            }
        }

        reps_with_weight.sort_by(|a, b| b.1.cmp(&a.1));

        reps_with_weight.drain(..).map(|(rep, _)| rep).collect()
    }

    /// Add voting account rep_account to the set of online representatives.
    /// This can happen for directly connected or indirectly connected reps.
    /// Returns whether it is a rep which has more than min weight
    pub fn vote_observed(&mut self, rep_account: PublicKey, now: Timestamp) -> bool {
        if self.rep_weights.weight(&rep_account) < self.representative_weight_minimum {
            return false;
        }

        let new_insert = self.online_reps.insert(rep_account, now);
        let trimmed = self
            .online_reps
            .trim(now.checked_sub(self.weight_interval).unwrap_or_default());

        if new_insert || trimmed {
            self.calculate_online_weight();
        }
        true
    }

    fn calculate_online_weight(&mut self) {
        let mut current = Amount::zero();
        for account in self.online_reps.iter() {
            current += self.rep_weights.weight(account);
        }
        self.online_weight = current;
    }

    /// Add rep_account to the set of peered representatives
    pub fn vote_observed_directly(
        &mut self,
        rep_account: PublicKey,
        channel: Arc<Channel>,
        now: Timestamp,
    ) -> InsertResult {
        let is_rep = self.vote_observed(rep_account, now);
        if is_rep {
            self.peered_reps
                .update_or_insert(rep_account, channel.channel_id(), now)
        } else {
            InsertResult::Updated
        }
    }

    pub fn remove_peer(&mut self, channel_id: ChannelId) -> Vec<PublicKey> {
        self.peered_reps.remove(channel_id)
    }

    pub fn container_info(&self) -> ContainerInfo {
        [
            (
                "online",
                self.online_reps.len(),
                OnlineContainer::ELEMENT_SIZE,
            ),
            (
                "peered",
                self.peered_reps.len(),
                PeeredContainer::ELEMENT_SIZE,
            ),
        ]
        .into()
    }
}

impl Default for OnlineReps {
    fn default() -> Self {
        Self::builder().finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_nullable_clock::SteadyClock;
    use std::time::Duration;

    #[test]
    fn empty() {
        let online_reps = OnlineReps::default();
        assert_eq!(
            online_reps.online_weight_minimum(),
            Amount::nano(60_000_000)
        );
        assert_eq!(online_reps.trended_weight(), Amount::zero(), "trended");
        assert_eq!(
            online_reps.trended_or_minimum_weight(),
            Amount::nano(60_000_000),
            "trended"
        );
        assert_eq!(online_reps.online_weight(), Amount::zero(), "online");
        assert_eq!(online_reps.peered_weight(), Amount::zero(), "peered");
        assert_eq!(online_reps.peered_reps_count(), 0, "peered count");
        assert_eq!(online_reps.quorum_percent(), 67, "quorum percent");
        assert_eq!(
            online_reps.quorum_delta(),
            Amount::nano(40_200_000),
            "quorum delta"
        );

        assert_eq!(online_reps.minimum_principal_weight(), Amount::nano(60_000));
    }

    #[test]
    fn observe_vote() {
        let clock = SteadyClock::new_null();
        let account = PublicKey::from(1);
        let weight = Amount::nano(100_000);
        let weights = Arc::new(RepWeightCache::new());
        weights.set(account, weight);
        let mut online_reps = OnlineReps::builder().rep_weights(weights).finish();

        online_reps.vote_observed(account, clock.now());

        assert_eq!(online_reps.online_weight(), weight, "online");
        assert_eq!(online_reps.peered_weight(), Amount::zero(), "peered");
    }

    #[test]
    fn observe_direct_vote() {
        let clock = SteadyClock::new_null();
        let account = PublicKey::from(1);
        let weight = Amount::nano(100_000);
        let weights = Arc::new(RepWeightCache::new());
        weights.set(account, weight);
        let mut online_reps = OnlineReps::builder().rep_weights(weights).finish();

        let channel = Arc::new(Channel::new_test_instance());
        online_reps.vote_observed_directly(account, channel, clock.now());

        assert_eq!(online_reps.online_weight(), weight, "online");
        assert_eq!(online_reps.peered_weight(), weight, "peered");
    }

    #[test]
    fn trended_weight() {
        let mut online_reps = OnlineReps::default();
        online_reps.set_trended(Amount::nano(10_000));
        assert_eq!(online_reps.trended_weight(), Amount::nano(10_000));
        assert_eq!(
            online_reps.trended_or_minimum_weight(),
            Amount::nano(60_000_000)
        );

        online_reps.set_trended(Amount::nano(100_000_000));
        assert_eq!(online_reps.trended_weight(), Amount::nano(100_000_000));
        assert_eq!(
            online_reps.trended_or_minimum_weight(),
            Amount::nano(100_000_000)
        );
    }

    #[test]
    fn minimum_principal_weight() {
        let mut online_reps = OnlineReps::default();
        assert_eq!(online_reps.minimum_principal_weight(), Amount::nano(60_000));

        online_reps.set_trended(Amount::nano(110_000_000));
        // 0.1% of trended weight
        assert_eq!(
            online_reps.minimum_principal_weight(),
            Amount::nano(110_000)
        );
    }

    #[test]
    fn is_pr() {
        let clock = SteadyClock::new_null();
        let weights = Arc::new(RepWeightCache::new());
        let mut online_reps = OnlineReps::builder().rep_weights(weights.clone()).finish();
        let rep_account = PublicKey::from(42);
        let channel = Arc::new(Channel::new_test_instance());
        let channel_id = channel.channel_id();
        weights.set(rep_account, Amount::nano(50_000));

        // unknown channel
        assert_eq!(online_reps.is_pr(channel_id), false);

        // below PR limit
        online_reps.vote_observed_directly(rep_account, channel, clock.now());
        assert_eq!(online_reps.is_pr(channel_id), false);

        // above PR limit
        weights.set(rep_account, Amount::nano(100_000));
        assert_eq!(online_reps.is_pr(channel_id), true);
    }

    #[test]
    fn quorum_delta() {
        let weights = Arc::new(RepWeightCache::new());
        let mut online_reps = OnlineReps::builder().rep_weights(weights.clone()).finish();

        assert_eq!(online_reps.quorum_delta(), Amount::nano(40_200_000));

        let rep_account = PublicKey::from(42);
        weights.set(rep_account, Amount::nano(100_000_000));
        online_reps.vote_observed(rep_account, Timestamp::new_test_instance());

        assert_eq!(online_reps.quorum_delta(), Amount::nano(67_000_000));
    }

    #[test]
    fn discard_old_votes() {
        let rep_a = PublicKey::from(1);
        let rep_b = PublicKey::from(2);
        let rep_c = PublicKey::from(3);
        let weights = Arc::new(RepWeightCache::new());
        weights.set(rep_a, Amount::nano(100_000));
        weights.set(rep_b, Amount::nano(200_000));
        weights.set(rep_c, Amount::nano(400_000));
        let mut online_reps = OnlineReps::builder()
            .rep_weights(weights)
            .weight_interval(Duration::from_secs(30))
            .finish();

        let now = SteadyClock::new_null().now();
        online_reps.vote_observed(rep_a, now);
        online_reps.vote_observed(rep_b, now + Duration::from_secs(10));
        online_reps.vote_observed(rep_c, now + Duration::from_secs(31));

        assert_eq!(online_reps.online_weight(), Amount::nano(600_000));
    }
}
