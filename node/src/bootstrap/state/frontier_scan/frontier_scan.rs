use super::{heads_container::HeadsContainer, FrontierScanConfig};
use primitive_types::U512;
use rsnano_core::{utils::ContainerInfo, Account, Frontier};
use rsnano_nullable_clock::Timestamp;

/// Divides the account space into ranges and scans each range for
/// outdated frontiers in parallel.
/// This class is used to track the progress of each range.
pub struct FrontierScan {
    heads: HeadsContainer,
}

impl FrontierScan {
    pub fn new(config: FrontierScanConfig) -> Self {
        assert!(!config.parallelism > 0);
        Self {
            heads: HeadsContainer::with_heads(config),
        }
    }

    /// Creates a test instance that doesn't progress, because max
    /// request limit reached
    #[cfg(test)]
    pub fn new_test_instance_blocked() -> Self {
        let mut scan = Self::new(FrontierScanConfig {
            parallelism: 1,
            consideration_count: 1,
            ..Default::default()
        });
        scan.next(Timestamp::new_test_instance());
        scan
    }

    pub fn next(&mut self, now: Timestamp) -> Account {
        let (next_account, head_start) = self.next_account(now);

        if !next_account.is_zero() {
            self.request_sent(head_start, now);
        }

        next_account
    }

    fn next_account(&self, now: Timestamp) -> (Account, Account) {
        for head in self.heads.ordered_by_timestamp() {
            if head.can_send_request(now) {
                return (head.next, head.start);
            }
        }
        (Account::zero(), Account::zero())
    }

    fn request_sent(&mut self, head_start: Account, now: Timestamp) {
        self.heads.modify(head_start, |head| head.request_sent(now));
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

    pub fn total_accounts_processed(&self) -> usize {
        self.heads.iter().map(|i| i.accounts_processed).sum()
    }

    #[allow(dead_code)]
    pub fn total_requests_completed(&self) -> usize {
        self.heads.iter().map(|i| i.requests_completed).sum()
    }

    pub fn heads(&self) -> Vec<FrontierHeadInfo> {
        self.heads
            .iter()
            .map(|i| FrontierHeadInfo {
                start: i.start,
                end: i.end,
                current: i.next,
            })
            .collect()
    }

    pub fn container_info(&self) -> ContainerInfo {
        // TODO port the detailed container info from nano_node
        [("total_processed", self.total_accounts_processed(), 0)].into()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct FrontierHeadInfo {
    pub start: Account,
    pub end: Account,
    pub current: Account,
}

impl FrontierHeadInfo {
    /// Returns how far the current frontier is in the range [0, 1]
    pub fn done_normalized(&self) -> f32 {
        let total: U512 = (self.end.number() - self.start.number()).into();
        let mut progress: U512 = (self.current.number() - self.start.number()).into();
        progress *= 1000;
        (progress / total).as_u64() as f32 / 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::BlockHash;
    use std::time::Duration;

    #[test]
    fn next_basic() {
        let config = FrontierScanConfig {
            parallelism: 2,
            consideration_count: 3,
            ..Default::default()
        };
        let mut scan = FrontierScan::new(config);
        let now = Timestamp::new_test_instance();

        // First call should return first head, account number 1 (avoiding burn account 0)
        let first = scan.next(now);
        assert_eq!(first, Account::from(1));

        // Second call should return second head, account number 0x800... (half the range)
        let second = scan.next(now);
        assert_eq!(
            second,
            Account::decode_hex("8000000000000000000000000000000000000000000000000000000000000000")
                .unwrap()
        );

        // Third call should return first head again, sequentially iterating through heads
        let third = scan.next(now);
        assert_eq!(third, Account::from(1));
    }

    #[test]
    fn process_basic() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 3,
            candidates: 5,
            ..Default::default()
        };
        let mut scan = FrontierScan::new(config);
        let now = Timestamp::new_test_instance();

        // Get initial account to scan
        let start = scan.next(now);
        assert_eq!(start, Account::from(1));

        // Create response with some frontiers
        let response = [
            Frontier::new(Account::from(2), BlockHash::from(1)),
            Frontier::new(Account::from(3), BlockHash::from(2)),
        ];

        // Process should not be done until consideration_count is reached
        assert!(!scan.process(start, &response));
        assert!(!scan.process(start, &response));

        // Head should not advance before reaching `consideration_count` responses
        assert_eq!(scan.next(now), Account::from(1));

        // After consideration_count responses, should be done
        assert!(scan.process(start, &response));

        // Head should advance to next account and start subsequent scan from there
        assert_eq!(scan.next(now), Account::from(3));
    }

    #[test]
    fn range_wrap_around() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 1,
            candidates: 1,
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        let start = scan.next(now);

        // Create response that would push next beyond the range end
        let response = [Frontier::new(Account::MAX, BlockHash::from(1))];

        // Process should succeed and wrap around
        assert!(scan.process(start, &response));

        // Next account should be back at start of range
        let next = scan.next(now);
        assert_eq!(next, Account::from(1));
    }

    #[test]
    fn cooldown() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 1,
            cooldown: Duration::from_millis(250),
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        // First call should succeed
        let first = scan.next(now);
        assert!(!first.is_zero());

        // Immediate second call should fail (return 0)
        let second = scan.next(now);
        assert!(second.is_zero());

        // After cooldown, should succeed again
        let third = scan.next(now + Duration::from_millis(251));
        assert!(!third.is_zero());
    }

    #[test]
    fn candidate_trimming() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 2,
            candidates: 3, // Only keep the lowest candidates
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        let start = scan.next(now);
        // Create response with more candidates than limit
        let response1 = [
            Frontier::new(Account::from(1), BlockHash::from(0)),
            Frontier::new(Account::from(4), BlockHash::from(3)),
            Frontier::new(Account::from(7), BlockHash::from(6)),
            Frontier::new(Account::from(10), BlockHash::from(9)),
        ];

        assert!(!scan.process(start, &response1));

        let response2 = [
            Frontier::new(Account::from(1), BlockHash::from(0)),
            Frontier::new(Account::from(3), BlockHash::from(2)),
            Frontier::new(Account::from(5), BlockHash::from(4)),
            Frontier::new(Account::from(7), BlockHash::from(6)),
            Frontier::new(Account::from(9), BlockHash::from(8)),
        ];
        assert!(scan.process(start, &response2));

        // After processing replies candidates should be ordered and trimmed
        let next = scan.next(now);
        assert_eq!(next, Account::from(5));
    }

    #[test]
    fn heads_distribution() {
        let config = FrontierScanConfig {
            parallelism: 4,
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        // Collect initial accounts from each head
        let first0 = scan.next(now);
        let first1 = scan.next(now);
        let first2 = scan.next(now);
        let first3 = scan.next(now);

        // Verify accounts are properly distributed across the range
        assert!(first1 > first0);
        assert!(first2 > first1);
        assert!(first3 > first2);
    }

    #[test]
    fn invalid_response_ordering() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 1,
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        let start = scan.next(now);

        // Create response with out-of-order accounts
        let response = [
            Frontier::new(Account::from(3), BlockHash::from(1)),
            Frontier::new(Account::from(2), BlockHash::from(2)),
        ];

        // Should still process successfully
        assert!(scan.process(start, &response));
        assert_eq!(scan.next(now), Account::from(3));
    }

    #[test]
    fn empty_responses() {
        let config = FrontierScanConfig {
            parallelism: 1,
            consideration_count: 2,
            ..Default::default()
        };
        let now = Timestamp::new_test_instance();
        let mut scan = FrontierScan::new(config);

        let start = scan.next(now);

        // Empty response should not advance head even after receiving `consideration_count` responses
        assert!(!scan.process(start, &[]));
        assert!(!scan.process(start, &[]));
        assert_eq!(scan.next(now), start);

        // Let the head advance
        let response = [Frontier::new(Account::from(2), BlockHash::from(1))];
        assert!(scan.process(start, &response));
        assert_eq!(scan.next(now), Account::from(2));

        // However, after receiving enough empty responses, head should wrap around to the start
        assert!(!scan.process(start, &[]));
        assert!(!scan.process(start, &[]));
        assert!(!scan.process(start, &[]));
        assert_eq!(scan.next(now), Account::from(2));
        assert!(scan.process(start, &[]));
        // Wraps around:
        assert_eq!(scan.next(now), Account::from(1));
    }

    #[test]
    fn container_info() {
        let scan = FrontierScan::new(Default::default());
        let info = scan.container_info();
        assert_eq!(info, [("total_processed", 0, 0)].into());
    }

    #[test]
    fn heads_info() {
        let config = FrontierScanConfig {
            parallelism: 4,
            ..Default::default()
        };
        let scan = FrontierScan::new(config);

        let heads = scan.heads();

        assert_eq!(heads.len(), 4);
        assert_eq!(
            heads[0],
            frontier_head_info(
                "0000000000000000000000000000000000000000000000000000000000000001",
                "4000000000000000000000000000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000000000000000000000000001"
            )
        );
        assert_eq!(
            heads[1],
            frontier_head_info(
                "4000000000000000000000000000000000000000000000000000000000000000",
                "8000000000000000000000000000000000000000000000000000000000000000",
                "4000000000000000000000000000000000000000000000000000000000000000",
            )
        );
        assert_eq!(
            heads[2],
            frontier_head_info(
                "8000000000000000000000000000000000000000000000000000000000000000",
                "C000000000000000000000000000000000000000000000000000000000000000",
                "8000000000000000000000000000000000000000000000000000000000000000",
            )
        );
        assert_eq!(
            heads[3],
            frontier_head_info(
                "C000000000000000000000000000000000000000000000000000000000000000",
                "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                "C000000000000000000000000000000000000000000000000000000000000000",
            )
        );
    }

    #[test]
    fn heads_info_done() {
        let info = FrontierHeadInfo {
            start: 1.into(),
            end: 13.into(),
            current: 10.into(),
        };
        let done = info.done_normalized();
        assert!((done - 0.75).abs() < 0.001, "done was: {}", done);
    }

    fn frontier_head_info(start: &str, end: &str, current: &str) -> FrontierHeadInfo {
        FrontierHeadInfo {
            start: Account::decode_hex(start).unwrap(),
            end: Account::decode_hex(end).unwrap(),
            current: Account::decode_hex(current).unwrap(),
        }
    }
}
