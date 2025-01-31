use super::{account_crawler::AccountDatabaseCrawler, pending_crawler::PendingDatabaseCrawler};
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

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct OutdatedAccounts {
    pub accounts: Vec<Account>,
    /// Accounts that exist but are outdated
    pub outdated: usize,
    /// Accounts that don't exist but have pending blocks in the ledger
    pub pending: usize,
}

/// Checks if a given frontier is up to date or outdated
pub(crate) struct FrontierChecker<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
    account_crawler: AccountDatabaseCrawler<'a>,
    pending_crawler: PendingDatabaseCrawler<'a>,
}

impl<'a> FrontierChecker<'a> {
    pub(crate) fn new(ledger: &'a Ledger, tx: &'a LmdbReadTransaction) -> Self {
        Self {
            ledger,
            tx,
            account_crawler: AccountDatabaseCrawler::new(ledger, tx),
            pending_crawler: PendingDatabaseCrawler::new(ledger, tx),
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
        }
    }

    fn check_frontier(&mut self, frontier: &Frontier) -> FrontierCheckResult {
        self.advance_to(&frontier.account);

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
        if let Some((key, _)) = &self.pending_crawler.current {
            if key.receiving_account == frontier.account {
                return FrontierCheckResult::Pending;
            }
        }

        FrontierCheckResult::UnknownAccount
    }

    fn initialize(&mut self, start: Account) {
        self.account_crawler.seek(start);
        self.pending_crawler.seek(start);
    }

    fn advance_to(&mut self, account: &Account) {
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
        let ledger = Ledger::new_null();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);
        let result = checker.get_outdated_accounts(&[]);
        assert_eq!(result, Default::default());
    }

    #[test]
    fn empty_ledger() {
        let ledger = Ledger::new_null();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);
        let result = checker.get_outdated_accounts(&[Frontier::new_test_instance()]);
        assert_eq!(result, Default::default());
    }

    #[test]
    fn one_outdated() {
        let account = Account::from(1);
        let head = BlockHash::from(2);
        let info = AccountInfo {
            head,
            ..Default::default()
        };
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result = checker.get_outdated_accounts(&[Frontier::new(account, BlockHash::from(3))]);

        assert_eq!(
            result,
            OutdatedAccounts {
                accounts: vec![account],
                outdated: 1,
                pending: 0
            }
        );
    }

    #[test]
    fn one_up_to_date() {
        let account = Account::from(1);
        let head = BlockHash::from(2);
        let info = AccountInfo {
            head,
            ..Default::default()
        };
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result = checker.get_outdated_accounts(&[Frontier::new(account, head)]);

        assert_eq!(result, Default::default());
    }

    #[test]
    fn one_pending() {
        let account = Account::from(1);
        let ledger = Ledger::new_null_builder()
            .pending(
                &PendingKey {
                    receiving_account: account,
                    send_block_hash: BlockHash::from(2),
                },
                &PendingInfo::new_test_instance(),
            )
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result = checker.get_outdated_accounts(&[Frontier::new(account, BlockHash::from(2))]);

        assert_eq!(
            result,
            OutdatedAccounts {
                accounts: vec![account],
                outdated: 0,
                pending: 1
            }
        );
    }

    #[test]
    fn frontier_is_obsolete() {
        let account = Account::from(1);
        let head = BlockHash::from(2);
        let frontier = BlockHash::from(3);

        let info = AccountInfo {
            head,
            ..Default::default()
        };
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .pruned(&frontier)
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result = checker.get_outdated_accounts(&[Frontier::new(account, frontier)]);

        assert_eq!(result, Default::default());
    }

    #[test]
    fn unknown_frontier_with_unrelated_account_info() {
        let account = Account::from(999);
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &AccountInfo::new_test_instance())
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result =
            checker.get_outdated_accounts(&[Frontier::new(Account::from(1), BlockHash::from(2))]);

        assert_eq!(result, Default::default());
    }
    #[test]

    fn unrelated_pending_info() {
        let account = Account::from(999);
        let ledger = Ledger::new_null_builder()
            .pending(
                &PendingKey::new(account, BlockHash::from(2)),
                &PendingInfo::new_test_instance(),
            )
            .finish();
        let tx = ledger.read_txn();
        let mut checker = FrontierChecker::new(&ledger, &tx);

        let result =
            checker.get_outdated_accounts(&[Frontier::new(Account::from(1), BlockHash::from(2))]);

        assert_eq!(result, Default::default());
    }
}
