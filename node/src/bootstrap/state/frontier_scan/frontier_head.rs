use rsnano_core::{Account, Frontier};
use rsnano_nullable_clock::Timestamp;
use std::{collections::BTreeSet, time::Duration};

/// Represents a range of accounts to scan, once the full range is scanned (goes past `end`)
/// the head wraps around (to the `start`)
pub(super) struct FrontierHead {
    /// The range of accounts to scan is [start, end)
    pub start: Account,
    pub end: Account,

    /// We scan the range by querying frontiers starting at 'next' and gathering candidates
    pub next: Account,
    candidates: BTreeSet<Account>,

    /// Total number of requests that were sent for the current starting account
    pub requests_sent: usize,

    /// Total number of completed requests for the current starting account
    pub requests_completed: usize,
    pub last_request_sent: Timestamp,

    /// Total number of accounts processed for the current starting account
    pub accounts_processed: usize,

    config: FrontierHeadsConfig,
}

impl FrontierHead {
    pub fn new(
        start: impl Into<Account>,
        end: impl Into<Account>,
        config: FrontierHeadsConfig,
    ) -> Self {
        let start = start.into();
        Self {
            start,
            end: end.into(),
            next: start,
            candidates: Default::default(),
            requests_sent: 0,
            requests_completed: 0,
            last_request_sent: Timestamp::default(),
            accounts_processed: 0,
            config,
        }
    }

    pub fn can_send_request(&self, now: Timestamp) -> bool {
        let cutoff = now - self.config.cooldown;
        self.requests_sent < self.config.consideration_count || self.last_request_sent < cutoff
    }

    pub fn request_sent(&mut self, now: Timestamp) {
        self.requests_sent += 1;
        self.last_request_sent = now
    }

    pub fn process(&mut self, response: &[Frontier]) -> bool {
        self.requests_completed += 1;
        self.insert_candidates(response);
        self.trim_candidates();
        self.wrap_around_if_no_candidates_found();

        let done = if self.should_advance() {
            self.advance();
            true
        } else {
            false
        };

        done
    }

    fn insert_candidates(&mut self, response: &[Frontier]) {
        for frontier in response {
            // Only consider candidates that actually advance the current frontier
            if frontier.account.number() > self.next.number() {
                self.candidates.insert(frontier.account);
            }
        }
    }

    fn trim_candidates(&mut self) {
        while self.candidates.len() > self.config.candidates {
            self.candidates.pop_last();
        }
    }

    /// Special case for the last frontier head that won't receive larger than max frontier
    fn wrap_around_if_no_candidates_found(&mut self) {
        if self.requests_completed >= self.config.consideration_count * 2
            && self.candidates.is_empty()
        {
            // inserting end causes a wrap around
            self.candidates.insert(self.end);
        }
    }

    fn should_advance(&self) -> bool {
        self.requests_completed >= self.config.consideration_count && !self.candidates.is_empty()
    }

    /// Take the last candidate as the next frontier or wraps around
    fn advance(&mut self) {
        self.next = self.next_start_account();
        self.accounts_processed += self.candidates.len();
        self.candidates.clear();
        self.requests_sent = 0;
        self.requests_completed = 0;
        self.last_request_sent = Timestamp::default();
    }

    fn next_start_account(&self) -> Account {
        let last = self.candidates.last().cloned().unwrap_or(self.next);
        // Bound the search range
        if last.number() >= self.end.number() {
            self.start
        } else {
            last
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FrontierHeadsConfig {
    pub parallelism: usize,
    pub consideration_count: usize,
    pub candidates: usize,
    pub cooldown: Duration,
}

impl Default for FrontierHeadsConfig {
    fn default() -> Self {
        Self {
            parallelism: 128,
            consideration_count: 4,
            candidates: 1000,
            cooldown: Duration::from_secs(5),
        }
    }
}
