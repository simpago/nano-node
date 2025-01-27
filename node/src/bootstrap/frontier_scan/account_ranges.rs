use super::heads_container::HeadsContainer;
use crate::{
    bootstrap::frontier_scan::heads_container::FrontierHead,
    stats::{DetailType, StatType, Stats},
};
use primitive_types::U256;
use rsnano_core::{utils::ContainerInfo, Account, Frontier};
use rsnano_nullable_clock::Timestamp;
use std::{sync::Arc, time::Duration};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AccountRangesConfig {
    pub head_parallelism: usize,
    pub consideration_count: usize,
    pub candidates: usize,
    pub cooldown: Duration,
    pub max_pending: usize,
}

impl Default for AccountRangesConfig {
    fn default() -> Self {
        Self {
            head_parallelism: 128,
            consideration_count: 4,
            candidates: 1000,
            cooldown: Duration::from_secs(5),
            max_pending: 16,
        }
    }
}

/// Divides the account space into ranges and scans each range for
/// outdated frontiers in parallel.
/// This class is used to track the progress of each range.
pub struct AccountRanges {
    config: AccountRangesConfig,
    stats: Arc<Stats>,
    heads: HeadsContainer,
}

impl AccountRanges {
    pub fn new(config: AccountRangesConfig, stats: Arc<Stats>) -> Self {
        // Divide nano::account numeric range into consecutive and equal ranges
        let max_account = Account::MAX.number();
        let range_size = max_account / config.head_parallelism;
        let mut heads = HeadsContainer::default();

        for i in 0..config.head_parallelism {
            // Start at 1 to avoid the burn account
            let start = if i == 0 {
                U256::from(1)
            } else {
                range_size * i
            };
            let end = if i == config.head_parallelism - 1 {
                max_account
            } else {
                start + range_size
            };
            heads.push_back(FrontierHead::new(start, end));
        }

        assert!(!heads.len() > 0);

        Self {
            config,
            stats,
            heads,
        }
    }

    pub fn next(&mut self, now: Timestamp) -> Account {
        let cutoff = now - self.config.cooldown;
        let mut next_account = Account::zero();
        let mut it = Account::zero();
        for head in self.heads.ordered_by_timestamp() {
            if head.requests < self.config.consideration_count || head.timestamp < cutoff {
                debug_assert!(head.next.number() >= head.start.number());
                debug_assert!(head.next.number() < head.end.number());

                self.stats.inc(
                    StatType::BootstrapFrontierScan,
                    if head.requests < self.config.consideration_count {
                        DetailType::NextByRequests
                    } else {
                        DetailType::NextByTimestamp
                    },
                );

                next_account = head.next;
                it = head.start;
                break;
            }
        }

        if next_account.is_zero() {
            self.stats
                .inc(StatType::BootstrapFrontierScan, DetailType::NextNone);
        } else {
            self.heads.modify(&it, |head| {
                head.requests += 1;
                head.timestamp = now
            });
        }

        next_account
    }

    pub fn process(&mut self, start: Account, response: &[Frontier]) -> bool {
        debug_assert!(response
            .iter()
            .all(|f| f.account.number() >= start.number()));

        self.stats
            .inc(StatType::BootstrapFrontierScan, DetailType::Process);

        // Find the first head with head.start <= start
        let it = self.heads.find_first_less_than_or_equal_to(start).unwrap();

        let mut done = false;
        self.heads.modify(&it, |entry| {
            entry.completed += 1;

            for frontier in response {
                // Only consider candidates that actually advance the current frontier
                if frontier.account.number() > entry.next.number() {
                    entry.candidates.insert(frontier.account);
                }
            }

            // Trim the candidates
            while entry.candidates.len() > self.config.candidates {
                entry.candidates.pop_last();
            }

            // Special case for the last frontier head that won't receive larger than max frontier
            if entry.completed >= self.config.consideration_count * 2 && entry.candidates.is_empty()
            {
                self.stats
                    .inc(StatType::BootstrapFrontierScan, DetailType::DoneEmpty);
                entry.candidates.insert(entry.end);
            }

            // Check if done
            if entry.completed >= self.config.consideration_count && !entry.candidates.is_empty() {
                self.stats
                    .inc(StatType::BootstrapFrontierScan, DetailType::Done);

                // Take the last candidate as the next frontier
                assert!(!entry.candidates.is_empty());
                let last = entry.candidates.last().unwrap();
                debug_assert!(entry.next.number() < last.number());
                entry.next = *last;
                entry.processed += entry.candidates.len();
                entry.candidates.clear();
                entry.requests = 0;
                entry.completed = 0;
                entry.timestamp = Timestamp::default();

                // Bound the search range
                if entry.next.number() >= entry.end.number() {
                    self.stats
                        .inc(StatType::BootstrapFrontierScan, DetailType::DoneRange);
                    entry.next = entry.start;
                }

                done = true;
            }
        });

        done
    }

    pub fn container_info(&self) -> ContainerInfo {
        // TODO port the detailed container info from nano_node
        let total_processed = self.heads.iter().map(|i| i.processed).sum();
        [("total_processed", total_processed, 0)].into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::BlockHash;

    #[test]
    fn next_basic() {
        let config = AccountRangesConfig {
            head_parallelism: 2,
            consideration_count: 3,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let mut ranges = AccountRanges::new(config, stats);
        let now = Timestamp::new_test_instance();

        // First call should return first head, account number 1 (avoiding burn account 0)
        let first = ranges.next(now);
        assert_eq!(first, Account::from(1));

        // Second call should return second head, account number 0x7FF... (half the range)
        let second = ranges.next(now);
        assert_eq!(
            second,
            Account::decode_hex("7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                .unwrap()
        );

        // Third call should return first head again, sequentially iterating through heads
        let third = ranges.next(now);
        assert_eq!(third, Account::from(1));
    }

    #[test]
    fn process_basic() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 3,
            candidates: 5,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let mut ranges = AccountRanges::new(config, stats);
        let now = Timestamp::new_test_instance();

        // Get initial account to scan
        let start = ranges.next(now);
        assert_eq!(start, Account::from(1));

        // Create response with some frontiers
        let response = [
            Frontier::new(Account::from(2), BlockHash::from(1)),
            Frontier::new(Account::from(3), BlockHash::from(2)),
        ];

        // Process should not be done until consideration_count is reached
        assert!(!ranges.process(start, &response));
        assert!(!ranges.process(start, &response));

        // Head should not advance before reaching `consideration_count` responses
        assert_eq!(ranges.next(now), Account::from(1));

        // After consideration_count responses, should be done
        assert!(ranges.process(start, &response));

        // Head should advance to next account and start subsequent scan from there
        assert_eq!(ranges.next(now), Account::from(3));
    }

    #[test]
    fn range_wrap_around() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 1,
            candidates: 1,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        let start = ranges.next(now);

        // Create response that would push next beyond the range end
        let response = [Frontier::new(Account::MAX, BlockHash::from(1))];

        // Process should succeed and wrap around
        assert!(ranges.process(start, &response));

        // Next account should be back at start of range
        let next = ranges.next(now);
        assert_eq!(next, Account::from(1));
    }

    #[test]
    fn cooldown() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 1,
            cooldown: Duration::from_millis(250),
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        // First call should succeed
        let first = ranges.next(now);
        assert!(!first.is_zero());

        // Immediate second call should fail (return 0)
        let second = ranges.next(now);
        assert!(second.is_zero());

        // After cooldown, should succeed again
        let third = ranges.next(now + Duration::from_millis(251));
        assert!(!third.is_zero());
    }

    #[test]
    fn candidate_trimming() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 2,
            candidates: 3, // Only keep the lowest candidates
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        let start = ranges.next(now);
        // Create response with more candidates than limit
        let response1 = [
            Frontier::new(Account::from(1), BlockHash::from(0)),
            Frontier::new(Account::from(4), BlockHash::from(3)),
            Frontier::new(Account::from(7), BlockHash::from(6)),
            Frontier::new(Account::from(10), BlockHash::from(9)),
        ];

        assert!(!ranges.process(start, &response1));

        let response2 = [
            Frontier::new(Account::from(1), BlockHash::from(0)),
            Frontier::new(Account::from(3), BlockHash::from(2)),
            Frontier::new(Account::from(5), BlockHash::from(4)),
            Frontier::new(Account::from(7), BlockHash::from(6)),
            Frontier::new(Account::from(9), BlockHash::from(8)),
        ];
        assert!(ranges.process(start, &response2));

        // After processing replies candidates should be ordered and trimmed
        let next = ranges.next(now);
        assert_eq!(next, Account::from(5));
    }

    #[test]
    fn heads_distribution() {
        let config = AccountRangesConfig {
            head_parallelism: 4,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        // Collect initial accounts from each head
        let first0 = ranges.next(now);
        let first1 = ranges.next(now);
        let first2 = ranges.next(now);
        let first3 = ranges.next(now);

        // Verify accounts are properly distributed across the range
        assert!(first1 > first0);
        assert!(first2 > first1);
        assert!(first3 > first2);
    }

    #[test]
    fn invalid_response_ordering() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 1,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        let start = ranges.next(now);

        // Create response with out-of-order accounts
        let response = [
            Frontier::new(Account::from(3), BlockHash::from(1)),
            Frontier::new(Account::from(2), BlockHash::from(2)),
        ];

        // Should still process successfully
        assert!(ranges.process(start, &response));
        assert_eq!(ranges.next(now), Account::from(3));
    }

    #[test]
    fn empty_responses() {
        let config = AccountRangesConfig {
            head_parallelism: 1,
            consideration_count: 2,
            ..Default::default()
        };
        let stats = Arc::new(Stats::default());
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config, stats);

        let start = ranges.next(now);

        // Empty response should not advance head even after receiving `consideration_count` responses
        assert!(!ranges.process(start, &[]));
        assert!(!ranges.process(start, &[]));
        assert_eq!(ranges.next(now), start);

        // Let the head advance
        let response = [Frontier::new(Account::from(2), BlockHash::from(1))];
        assert!(ranges.process(start, &response));
        assert_eq!(ranges.next(now), Account::from(2));

        // However, after receiving enough empty responses, head should wrap around to the start
        assert!(!ranges.process(start, &[]));
        assert!(!ranges.process(start, &[]));
        assert!(!ranges.process(start, &[]));
        assert_eq!(ranges.next(now), Account::from(2));
        assert!(ranges.process(start, &[]));
        // Wraps around:
        assert_eq!(ranges.next(now), Account::from(1));
    }

    #[test]
    fn container_info() {
        let ranges = AccountRanges::new(Default::default(), Arc::new(Stats::default()));
        let info = ranges.container_info();
        assert_eq!(info, [("total_processed", 0, 0)].into());
    }
}
