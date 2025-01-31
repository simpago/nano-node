use crate::bootstrap::response_processor::crawlers::{
    AccountDatabaseCrawler, PendingDatabaseCrawler,
};
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

#[derive(Default)]
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
        self.account_crawler.initialize(start);
        self.pending_crawler.initialize(start);
    }

    fn advance_to(&mut self, account: &Account) {
        self.account_crawler.advance_to(account);
        self.pending_crawler.advance_to(account);
    }
}
