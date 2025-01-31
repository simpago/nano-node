use super::{heads_container::HeadsContainer, FrontierHeadsConfig};
use rsnano_core::{utils::ContainerInfo, Account, Frontier};
use rsnano_nullable_clock::Timestamp;
use std::time::Duration;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AccountRangesConfig {
    pub heads: FrontierHeadsConfig,
    pub cooldown: Duration,
    pub max_pending: usize,
}

impl Default for AccountRangesConfig {
    fn default() -> Self {
        Self {
            heads: Default::default(),
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
    heads: HeadsContainer,
}

impl AccountRanges {
    pub fn new(config: AccountRangesConfig) -> Self {
        assert!(!config.heads.parallelism > 0);
        Self {
            heads: HeadsContainer::with_heads(config.heads.clone()),
            config,
        }
    }

    pub fn next(&mut self, now: Timestamp) -> Account {
        let (next_account, head_start) = self.next_account(now);

        if !next_account.is_zero() {
            self.inc_requests(head_start, now);
        }

        next_account
    }

    fn next_account(&self, now: Timestamp) -> (Account, Account) {
        let cutoff = now - self.config.cooldown;
        for head in self.heads.ordered_by_timestamp() {
            if head.requests < self.config.heads.consideration_count || head.timestamp < cutoff {
                return (head.next, head.start);
            }
        }
        (Account::zero(), Account::zero())
    }

    fn inc_requests(&mut self, head_start: Account, now: Timestamp) {
        self.heads.modify(head_start, |head| {
            head.requests += 1;
            head.timestamp = now
        });
    }

    pub fn process(&mut self, start: Account, response: &[Frontier]) -> bool {
        debug_assert!(response
            .iter()
            .all(|f| f.account.number() >= start.number()));

        // Find the first head with head.start <= start
        let range_start = self.heads.find_first_less_than_or_equal_to(start).unwrap();

        let mut done = false;
        self.heads.modify(range_start, |head| {
            done = head.process(response);
        });

        done
    }

    pub fn total_processed(&self) -> usize {
        self.heads.iter().map(|i| i.processed).sum()
    }

    pub fn total_completed(&self) -> usize {
        self.heads.iter().map(|i| i.completed).sum()
    }

    pub fn container_info(&self) -> ContainerInfo {
        // TODO port the detailed container info from nano_node
        [("total_processed", self.total_processed(), 0)].into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::BlockHash;

    #[test]
    fn next_basic() {
        let config = AccountRangesConfig {
            heads: FrontierHeadsConfig {
                parallelism: 2,
                consideration_count: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut ranges = AccountRanges::new(config);
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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 3,
                candidates: 5,
            },
            ..Default::default()
        };
        let mut ranges = AccountRanges::new(config);
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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 1,
                candidates: 1,
            },
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 1,
                ..Default::default()
            },
            cooldown: Duration::from_millis(250),
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 2,
                candidates: 3, // Only keep the lowest candidates
            },
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
            heads: FrontierHeadsConfig {
                parallelism: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
            heads: FrontierHeadsConfig {
                parallelism: 1,
                consideration_count: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut ranges = AccountRanges::new(config);

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
        let ranges = AccountRanges::new(Default::default());
        let info = ranges.container_info();
        assert_eq!(info, [("total_processed", 0, 0)].into());
    }
}
