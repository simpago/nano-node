use crate::{bootstrap::AscPullQuerySpec, stats::DetailType};
use rsnano_core::{Account, BlockHash, Frontier, HashOrAccount};
use rsnano_messages::{AscPullAck, AscPullAckType, AscPullReqType, BlocksAckPayload, HashType};
use rsnano_nullable_clock::Timestamp;
use std::time::Duration;

use super::VerifyResult;

#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum QueryType {
    #[default]
    Invalid,
    BlocksByHash,
    BlocksByAccount,
    AccountInfoByHash,
    Frontiers,
}

impl From<QueryType> for DetailType {
    fn from(value: QueryType) -> Self {
        match value {
            QueryType::Invalid => DetailType::Invalid,
            QueryType::BlocksByHash => DetailType::BlocksByHash,
            QueryType::BlocksByAccount => DetailType::BlocksByAccount,
            QueryType::AccountInfoByHash => DetailType::AccountInfoByHash,
            QueryType::Frontiers => DetailType::Frontiers,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum QuerySource {
    Priority,
    Dependencies,
    Frontiers,
}

/// Information about a running bootstrap query that hasn't been responded yet
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunningQuery {
    pub id: u64,
    pub source: QuerySource,
    pub query_type: QueryType,
    pub start: HashOrAccount,
    pub account: Account,
    pub hash: BlockHash,
    pub count: usize,
    pub sent: Timestamp,
    pub response_cutoff: Timestamp,
}

impl RunningQuery {
    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self {
            source: QuerySource::Priority,
            query_type: QueryType::BlocksByHash,
            start: HashOrAccount::from(1),
            account: Account::from(2),
            hash: BlockHash::from(3),
            count: 4,
            response_cutoff: Timestamp::new_test_instance() + Duration::from_secs(30),
            sent: Timestamp::new_test_instance(),
            id: 42,
        }
    }

    pub fn from_request(
        id: u64,
        spec: &AscPullQuerySpec,
        now: Timestamp,
        timeout: Duration,
    ) -> Self {
        let (source, query_type, start, count) = match &spec.req_type {
            AscPullReqType::Frontiers(f) => (
                QuerySource::Frontiers,
                QueryType::Frontiers,
                HashOrAccount::from(f.start),
                0,
            ),
            AscPullReqType::Blocks(b) => match b.start_type {
                HashType::Account => (
                    QuerySource::Priority,
                    QueryType::BlocksByAccount,
                    b.start,
                    b.count,
                ),
                HashType::Block => (
                    QuerySource::Priority,
                    QueryType::BlocksByHash,
                    b.start,
                    b.count,
                ),
            },
            AscPullReqType::AccountInfo(i) => (
                QuerySource::Dependencies,
                QueryType::AccountInfoByHash,
                i.target,
                0,
            ),
        };

        Self {
            source,
            query_type,
            start,
            account: spec.account,
            hash: spec.hash,
            count: count.into(),
            id,
            sent: now,
            response_cutoff: now + timeout * 4,
        }
    }

    pub fn is_valid_response_type(&self, response: &AscPullAck) -> bool {
        match response.pull_type {
            AscPullAckType::Blocks(_) => matches!(
                self.query_type,
                QueryType::BlocksByHash | QueryType::BlocksByAccount
            ),
            AscPullAckType::AccountInfo(_) => self.query_type == QueryType::AccountInfoByHash,
            AscPullAckType::Frontiers(_) => self.query_type == QueryType::Frontiers,
        }
    }

    pub fn verify_frontiers(&self, frontiers: &[Frontier]) -> VerifyResult {
        if self.query_type != QueryType::Frontiers {
            return VerifyResult::Invalid;
        }

        if frontiers.is_empty() {
            return VerifyResult::NothingNew;
        }

        if !Self::are_accounts_in_ascending_order(frontiers) {
            return VerifyResult::Invalid;
        }

        // Ensure the frontiers are larger or equal to the requested frontier
        if frontiers[0].account.number() < self.start.number() {
            return VerifyResult::Invalid;
        }

        VerifyResult::Ok
    }

    fn are_accounts_in_ascending_order(frontiers: &[Frontier]) -> bool {
        let mut previous = &Account::zero();
        for f in frontiers {
            if f.account.number() <= previous.number() {
                return false;
            }
            previous = &f.account;
        }

        true
    }

    /// Verifies whether the received response is valid. Returns:
    /// - invalid: when received blocks do not correspond to requested hash/account or they do not make a valid chain
    /// - nothing_new: when received response indicates that the account chain does not have more blocks
    /// - ok: otherwise, if all checks pass
    pub fn verify_blocks(&self, response: &BlocksAckPayload) -> VerifyResult {
        if !matches!(
            self.query_type,
            QueryType::BlocksByHash | QueryType::BlocksByAccount
        ) {
            return VerifyResult::Invalid;
        }

        let blocks = response.blocks();
        if blocks.is_empty() {
            return VerifyResult::NothingNew;
        }
        if blocks.len() == 1 && blocks.front().unwrap().hash() == self.start.into() {
            return VerifyResult::NothingNew;
        }
        if blocks.len() > self.count {
            return VerifyResult::Invalid;
        }

        let first = blocks.front().unwrap();
        match self.query_type {
            QueryType::BlocksByHash => {
                if first.hash() != self.start.into() {
                    // TODO: Stat & log
                    return VerifyResult::Invalid;
                }
            }
            QueryType::BlocksByAccount => {
                // Open & state blocks always contain account field
                if first.account_field().unwrap() != self.start.into() {
                    // TODO: Stat & log
                    return VerifyResult::Invalid;
                }
            }
            QueryType::AccountInfoByHash | QueryType::Frontiers | QueryType::Invalid => {
                return VerifyResult::Invalid;
            }
        }

        // Verify blocks make a valid chain
        let mut previous_hash = first.hash();
        for block in blocks.iter().skip(1) {
            if block.previous() != previous_hash {
                // TODO: Stat & log
                return VerifyResult::Invalid; // Blocks do not make a chain
            }
            previous_hash = block.hash();
        }

        VerifyResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod verify_frontiers {
        use super::*;

        #[test]
        fn empty_frontiers() {
            let query = RunningQuery {
                query_type: QueryType::Frontiers,
                ..RunningQuery::new_test_instance()
            };

            assert_eq!(query.verify_frontiers(&[]), VerifyResult::NothingNew);
        }

        #[test]
        fn valid_frontiers() {
            let query = RunningQuery {
                query_type: QueryType::Frontiers,
                start: 1.into(),
                ..RunningQuery::new_test_instance()
            };

            assert_eq!(
                query.verify_frontiers(&[
                    Frontier::new(1.into(), 100.into()),
                    Frontier::new(2.into(), 200.into()),
                    Frontier::new(4.into(), 400.into()),
                ]),
                VerifyResult::Ok
            );
        }

        mod error_cases {
            use super::*;

            #[test]
            fn invalid_query_type() {
                let query = RunningQuery {
                    query_type: QueryType::BlocksByHash, // invalid!
                    ..RunningQuery::new_test_instance()
                };

                assert_eq!(query.verify_frontiers(&[]), VerifyResult::Invalid);
            }

            #[test]
            fn accounts_not_in_order() {
                let query = RunningQuery {
                    query_type: QueryType::Frontiers,
                    start: 1.into(),
                    ..RunningQuery::new_test_instance()
                };

                assert_eq!(
                    query.verify_frontiers(&[
                        Frontier::new(2.into(), 200.into()), // out of order!
                        Frontier::new(1.into(), 100.into()),
                        Frontier::new(4.into(), 400.into()),
                    ]),
                    VerifyResult::Invalid
                );
            }

            #[test]
            fn accounts_lower_than_requested() {
                let query = RunningQuery {
                    query_type: QueryType::Frontiers,
                    start: 2.into(),
                    ..RunningQuery::new_test_instance()
                };

                assert_eq!(
                    query.verify_frontiers(&[
                        Frontier::new(1.into(), 100.into()), // too low!
                        Frontier::new(2.into(), 200.into()),
                        Frontier::new(4.into(), 400.into()),
                    ]),
                    VerifyResult::Invalid
                );
            }
        }
    }
}
