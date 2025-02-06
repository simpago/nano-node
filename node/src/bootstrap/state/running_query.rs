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

    pub fn from_spec(id: u64, spec: &AscPullQuerySpec, now: Timestamp, timeout: Duration) -> Self {
        let (source, query_type, start, count) = match &spec.req_type {
            AscPullReqType::Frontiers(f) => (
                QuerySource::Frontiers,
                QueryType::Frontiers,
                HashOrAccount::from(f.start),
                f.count as usize,
            ),
            AscPullReqType::Blocks(b) => match b.start_type {
                HashType::Account => (
                    QuerySource::Priority,
                    QueryType::BlocksByAccount,
                    b.start,
                    b.count as usize,
                ),
                HashType::Block => (
                    QuerySource::Priority,
                    QueryType::BlocksByHash,
                    b.start,
                    b.count as usize,
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
            count,
            id,
            sent: now,
            response_cutoff: Self::initial_response_cutoff(now, timeout),
        }
    }

    fn initial_response_cutoff(now: Timestamp, timeout: Duration) -> Timestamp {
        now + timeout * 4
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
    use rsnano_messages::{
        AccountInfoAckPayload, AccountInfoReqPayload, BlocksReqPayload, FrontiersReqPayload,
    };

    #[test]
    fn query_from_frontiers_spec() {
        let id = 123;
        let start = Account::from(42);
        let count = 1000;

        let spec = AscPullQuerySpec {
            req_type: AscPullReqType::Frontiers(FrontiersReqPayload { start, count }),
            ..AscPullQuerySpec::new_test_instance()
        };

        let now = Timestamp::new_test_instance();
        let timeout = Duration::from_secs(3);

        let query = RunningQuery::from_spec(id, &spec, now, timeout);

        assert_eq!(query.id, id);
        assert_eq!(query.source, QuerySource::Frontiers);
        assert_eq!(query.query_type, QueryType::Frontiers);
        assert_eq!(query.start, start.into());
        assert_eq!(query.count, count as usize);
        assert_eq!(query.account, spec.account);
        assert_eq!(query.hash, spec.hash);
        assert_eq!(query.sent, now);
        assert_eq!(
            query.response_cutoff,
            RunningQuery::initial_response_cutoff(now, timeout)
        );
    }

    #[test]
    fn query_from_blocks_by_account_spec() {
        let id = 123;
        let start = Account::from(42);
        let count = 16;

        let spec = AscPullQuerySpec {
            req_type: AscPullReqType::Blocks(BlocksReqPayload {
                start_type: HashType::Account,
                start: start.into(),
                count,
            }),
            ..AscPullQuerySpec::new_test_instance()
        };

        let now = Timestamp::new_test_instance();
        let timeout = Duration::from_secs(3);

        let query = RunningQuery::from_spec(id, &spec, now, timeout);

        assert_eq!(query.id, id);
        assert_eq!(query.source, QuerySource::Priority);
        assert_eq!(query.query_type, QueryType::BlocksByAccount);
        assert_eq!(query.start, start.into());
        assert_eq!(query.count, count as usize);
        assert_eq!(query.account, spec.account);
        assert_eq!(query.hash, spec.hash);
        assert_eq!(query.sent, now);
        assert_eq!(
            query.response_cutoff,
            RunningQuery::initial_response_cutoff(now, timeout)
        );
    }

    #[test]
    fn query_from_blocks_by_hash_spec() {
        let id = 123;
        let start = BlockHash::from(42);
        let count = 16;

        let spec = AscPullQuerySpec {
            req_type: AscPullReqType::Blocks(BlocksReqPayload {
                start_type: HashType::Block,
                start: start.into(),
                count,
            }),
            ..AscPullQuerySpec::new_test_instance()
        };

        let now = Timestamp::new_test_instance();
        let timeout = Duration::from_secs(3);

        let query = RunningQuery::from_spec(id, &spec, now, timeout);

        assert_eq!(query.id, id);
        assert_eq!(query.source, QuerySource::Priority);
        assert_eq!(query.query_type, QueryType::BlocksByHash);
        assert_eq!(query.start, start.into());
        assert_eq!(query.count, count as usize);
        assert_eq!(query.account, spec.account);
        assert_eq!(query.hash, spec.hash);
        assert_eq!(query.sent, now);
        assert_eq!(
            query.response_cutoff,
            RunningQuery::initial_response_cutoff(now, timeout)
        );
    }

    #[test]
    fn query_from_account_info_spec() {
        let id = 123;
        let target = BlockHash::from(42);

        let spec = AscPullQuerySpec {
            req_type: AscPullReqType::AccountInfo(AccountInfoReqPayload {
                target: target.into(),
                target_type: HashType::Block,
            }),
            ..AscPullQuerySpec::new_test_instance()
        };

        let now = Timestamp::new_test_instance();
        let timeout = Duration::from_secs(3);

        let query = RunningQuery::from_spec(id, &spec, now, timeout);

        assert_eq!(query.id, id);
        assert_eq!(query.source, QuerySource::Dependencies);
        assert_eq!(query.query_type, QueryType::AccountInfoByHash);
        assert_eq!(query.start, target.into());
        assert_eq!(query.count, 0);
        assert_eq!(query.account, spec.account);
        assert_eq!(query.hash, spec.hash);
        assert_eq!(query.sent, now);
        assert_eq!(
            query.response_cutoff,
            RunningQuery::initial_response_cutoff(now, timeout)
        );
    }

    #[test]
    fn valid_response_types() {
        assert_valid_response_type(
            QueryType::BlocksByHash,
            AscPullAckType::Blocks(BlocksAckPayload::new_test_instance()),
            true,
        );
        assert_valid_response_type(
            QueryType::BlocksByAccount,
            AscPullAckType::Blocks(BlocksAckPayload::new_test_instance()),
            true,
        );
        assert_valid_response_type(
            QueryType::AccountInfoByHash,
            AscPullAckType::AccountInfo(AccountInfoAckPayload::new_test_instance()),
            true,
        );
        assert_valid_response_type(
            QueryType::Frontiers,
            AscPullAckType::Frontiers(Vec::new()),
            true,
        );
    }

    #[test]
    fn invalid_response_types() {
        assert_valid_response_type(
            QueryType::BlocksByHash,
            AscPullAckType::AccountInfo(AccountInfoAckPayload::new_test_instance()),
            false,
        );
        assert_valid_response_type(
            QueryType::BlocksByHash,
            AscPullAckType::AccountInfo(AccountInfoAckPayload::new_test_instance()),
            false,
        );
        assert_valid_response_type(
            QueryType::BlocksByHash,
            AscPullAckType::Frontiers(Vec::new()),
            false,
        );
        assert_valid_response_type(
            QueryType::Frontiers,
            AscPullAckType::Blocks(BlocksAckPayload::new_test_instance()),
            false,
        );
    }

    fn assert_valid_response_type(query_type: QueryType, pull_type: AscPullAckType, valid: bool) {
        let query = RunningQuery {
            query_type,
            ..RunningQuery::new_test_instance()
        };

        let response = AscPullAck { id: 1, pull_type };
        assert_eq!(query.is_valid_response_type(&response), valid);
    }

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
