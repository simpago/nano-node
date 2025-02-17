use rsnano_core::Account;

use super::formatted_number;
use crate::frontier_scan::FrontierScanInfo;

pub(crate) struct FrontierScanViewModel {
    pub frontiers_rate: String,
    pub outdated_rate: String,
    pub frontiers_total: String,
    pub outdated_total: String,
    pub frontier_heads: Vec<FrontierHeadViewModel>,
    pub outdated_accounts: Vec<String>,
}

impl FrontierScanViewModel {
    pub(crate) fn new(info: &FrontierScanInfo) -> Self {
        let frontier_heads = info
            .frontier_heads
            .iter()
            .map(|head| FrontierHeadViewModel {
                start: abbrev(&head.start),
                end: abbrev(&head.end),
                current: format!("{:.1}%", head.done_normalized() * 100.0),
                done_normalized: head.done_normalized(),
            })
            .collect();

        let outdated_accounts = info
            .outdated_accounts
            .iter()
            .map(|a| a.encode_account())
            .collect();

        Self {
            frontiers_rate: format!("{} frontiers/s", formatted_number(info.frontiers_rate())),
            outdated_rate: format!("{} outdated/s", formatted_number(info.outdated_rate())),
            frontiers_total: format!("{} frontiers total", formatted_number(info.frontiers_total)),
            outdated_total: format!("{} outdated total", formatted_number(info.outdated_total)),
            frontier_heads,
            outdated_accounts,
        }
    }
}

pub(crate) struct FrontierHeadViewModel {
    pub start: String,
    pub end: String,
    pub current: String,
    pub done_normalized: f32,
}

fn abbrev(account: &Account) -> String {
    let mut result = account.encode_hex();
    result.truncate(4);
    result.push_str("...");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abbreviate_acccount() {
        assert_eq!(abbrev(&Account::from(0)), "0000...");
    }
}
