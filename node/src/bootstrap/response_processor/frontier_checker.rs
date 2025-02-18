use super::database_crawler::{AccountCrawlSource, DatabaseCrawler, PendingCrawlSource};
use crate::bootstrap::state::OutdatedAccounts;
use rsnano_core::{Account, Frontier};
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;

pub(crate) enum FrontierCheckResult {
    /// Account doesn't exist in the ledger and has no pending blocks, can't be prioritized right now
    UnknownAccount,
    /// Account exists and frontier is up-to-date
    UpToDate,
    /// Account exists but is outdated
    Outdated,
    /// Account doesn't exist but has pending blocks in the ledger
    Pending,
}

/// Checks if a given frontier is up to date or outdated
pub(crate) struct FrontierChecker<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
    account_crawler: DatabaseCrawler<'a, AccountCrawlSource<'a>>,
    pending_crawler: DatabaseCrawler<'a, PendingCrawlSource<'a>>,
}

impl<'a> FrontierChecker<'a> {
    pub(crate) fn new(ledger: &'a Ledger, tx: &'a LmdbReadTransaction) -> Self {
        Self {
            ledger,
            tx,
            account_crawler: DatabaseCrawler::new(AccountCrawlSource::new(ledger, tx)),
            pending_crawler: DatabaseCrawler::new(PendingCrawlSource::new(ledger, tx)),
        }
    }

    pub fn get_outdated_accounts(&mut self, frontiers: &[Frontier]) -> OutdatedAccounts {
        if frontiers.is_empty() {
            return Default::default();
        }

        let mut outdated = 0;
        let mut pending = 0;
        let mut accounts = Vec::new();
        self.initialize(frontiers[0].account);

        for frontier in frontiers {
            match self.check_frontier(frontier) {
                FrontierCheckResult::Outdated => {
                    outdated += 1;
                    accounts.push(frontier.account);
                }
                FrontierCheckResult::Pending => {
                    pending += 1;
                    accounts.push(frontier.account);
                }
                FrontierCheckResult::UnknownAccount | FrontierCheckResult::UpToDate => {}
            }
        }

        OutdatedAccounts {
            accounts,
            outdated,
            pending,
            fontiers_received: frontiers.len(),
        }
    }

    fn check_frontier(&mut self, frontier: &Frontier) -> FrontierCheckResult {
        self.advance_to(frontier.account);

        // Check if account exists in our ledger
        if let Some((account, info)) = &self.account_crawler.current {
            if *account == frontier.account {
                // Check for frontier mismatch
                if info.head != frontier.hash {
                    // Check if frontier block exists in our ledger
                    if !self
                        .ledger
                        .any()
                        .block_exists_or_pruned(self.tx, &frontier.hash)
                    {
                        return FrontierCheckResult::Outdated; // Frontier is outdated
                    }
                }
                return FrontierCheckResult::UpToDate;
            }
        }

        // Check if account has pending blocks in our ledger
        if let Some((account, _)) = &self.pending_crawler.current {
            if *account == frontier.account {
                return FrontierCheckResult::Pending;
            }
        }

        FrontierCheckResult::UnknownAccount
    }

    fn initialize(&mut self, start: Account) {
        self.account_crawler.seek(start);
        self.pending_crawler.seek(start);
    }

    fn advance_to(&mut self, account: Account) {
        self.account_crawler.advance_to(account);
        self.pending_crawler.advance_to(account);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{AccountInfo, BlockHash, PendingInfo, PendingKey};

    #[test]
    fn no_frontiers_and_empty_ledger() {
        assert_frontier_check(LedgerSpec::default(), &[], OutdatedAccounts::default());
    }

    #[test]
    fn empty_ledger() {
        let frontiers = [Frontier::new_test_instance()];
        assert_frontier_check(
            LedgerSpec::default(),
            &frontiers,
            OutdatedAccounts {
                accounts: Vec::new(),
                outdated: 0,
                pending: 0,
                fontiers_received: 1,
            },
        );
    }

    #[test]
    fn one_outdated() {
        let account = Account::from(1);
        let ledger = LedgerSpec {
            frontiers: vec![Frontier::new(account, BlockHash::from(2))],
            ..Default::default()
        };
        let frontiers = [Frontier::new(account, BlockHash::from(3))];
        let expected = OutdatedAccounts {
            accounts: vec![account],
            outdated: 1,
            pending: 0,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &frontiers, expected);
    }

    #[test]
    fn one_up_to_date() {
        let frontier = Frontier::new(Account::from(1), BlockHash::from(2));
        let ledger = LedgerSpec {
            frontiers: vec![frontier.clone()],
            ..Default::default()
        };
        let expected = OutdatedAccounts {
            accounts: Vec::new(),
            outdated: 0,
            pending: 0,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &[frontier], expected);
    }

    #[test]
    fn one_pending() {
        let account = Account::from(1);
        let ledger = LedgerSpec {
            pending: vec![account],
            ..Default::default()
        };
        let frontiers = [Frontier::new(account, BlockHash::from(2))];
        let expected = OutdatedAccounts {
            accounts: vec![account],
            outdated: 0,
            pending: 1,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &frontiers, expected);
    }

    #[test]
    fn frontier_is_obsolete() {
        let account = Account::from(1);
        let frontier = BlockHash::from(3);

        let ledger = LedgerSpec {
            frontiers: vec![Frontier::new(account, BlockHash::from(2))],
            pruned: vec![frontier],
            ..Default::default()
        };
        let frontiers = [Frontier::new(account, frontier)];
        let expected = OutdatedAccounts {
            accounts: Vec::new(),
            outdated: 0,
            pending: 0,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &frontiers, expected);
    }

    #[test]
    fn unknown_frontier_with_unrelated_account_info() {
        let ledger = LedgerSpec {
            frontiers: vec![Frontier::new(Account::from(999), BlockHash::from(3))],
            ..Default::default()
        };
        let frontiers = [Frontier::new(Account::from(1), BlockHash::from(2))];
        let expected = OutdatedAccounts {
            accounts: Vec::new(),
            outdated: 0,
            pending: 0,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &frontiers, expected);
    }

    #[test]
    fn unrelated_pending_info() {
        let ledger = LedgerSpec {
            pending: vec![Account::from(999)],
            ..Default::default()
        };
        let frontiers = [Frontier::new(Account::from(1), BlockHash::from(2))];
        let expected = OutdatedAccounts {
            accounts: Vec::new(),
            outdated: 0,
            pending: 0,
            fontiers_received: 1,
        };
        assert_frontier_check(ledger, &frontiers, expected);
    }

    fn assert_frontier_check(
        ledger_spec: LedgerSpec,
        frontiers: &[Frontier],
        expected: OutdatedAccounts,
    ) {
        let ledger = build_ledger(ledger_spec);
        let result = get_outdated_accounts(&ledger, &frontiers);
        assert_eq!(result, expected);
    }

    fn get_outdated_accounts(ledger: &Ledger, frontiers: &[Frontier]) -> OutdatedAccounts {
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);
        checker.get_outdated_accounts(frontiers)
    }

    fn build_ledger(spec: LedgerSpec) -> Ledger {
        let mut ledger_builder = Ledger::new_null_builder();

        for frontier in spec.frontiers {
            ledger_builder = ledger_builder.account_info(
                &frontier.account,
                &AccountInfo {
                    head: frontier.hash,
                    ..Default::default()
                },
            );
        }

        for pending_acc in spec.pending {
            ledger_builder = ledger_builder.pending(
                &PendingKey::new(pending_acc, BlockHash::from(2)),
                &PendingInfo::new_test_instance(),
            )
        }

        for hash in spec.pruned {
            ledger_builder = ledger_builder.pruned(&hash)
        }

        ledger_builder.finish()
    }

    #[derive(Default)]
    struct LedgerSpec {
        frontiers: Vec<Frontier>,
        pending: Vec<Account>,
        pruned: Vec<BlockHash>,
    }
}
