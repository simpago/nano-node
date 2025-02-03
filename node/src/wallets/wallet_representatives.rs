use rsnano_core::{Account, Amount, PublicKey};
use rsnano_ledger::RepWeightCache;
use std::{collections::HashSet, sync::Arc};

#[derive(Clone)]
pub struct WalletRepresentatives {
    /// has representatives with at least 50% of principal representative requirements
    half_principal: bool,
    /// Number of representatives with at least the configured minimum voting weight
    voting: u64,
    /// Representatives with at least the configured minimum voting weight
    accounts: HashSet<Account>,
    vote_minimum: Amount,
    rep_weights: Arc<RepWeightCache>,
}

impl WalletRepresentatives {
    pub fn new(vote_minimum: Amount, rep_weights: Arc<RepWeightCache>) -> Self {
        Self {
            half_principal: false,
            voting: 0,
            accounts: HashSet::new(),
            vote_minimum,
            rep_weights,
        }
    }
    pub fn have_half_rep(&self) -> bool {
        self.half_principal
    }

    pub fn voting_reps(&self) -> u64 {
        self.voting
    }

    pub fn exists(&self, rep: &Account) -> bool {
        self.accounts.contains(rep)
    }

    pub fn clear(&mut self) {
        self.voting = 0;
        self.half_principal = false;
        self.accounts.clear();
    }

    pub fn check_rep(&mut self, pub_key: PublicKey, half_principal_weight: Amount) -> bool {
        let weight = self.rep_weights.weight(&pub_key);

        if weight < self.vote_minimum {
            return false; // account not a representative
        }

        if weight >= half_principal_weight {
            self.half_principal = true;
        }

        if !self.accounts.insert(pub_key.into()) {
            return false; // account already exists
        }

        self.voting += 1;
        true
    }
}

impl Default for WalletRepresentatives {
    fn default() -> Self {
        Self::new(Amount::nano(1), Arc::new(RepWeightCache::new()))
    }
}
