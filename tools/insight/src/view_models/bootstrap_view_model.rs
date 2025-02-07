use rsnano_core::Account;
use rsnano_node::bootstrap::Bootstrapper;

#[derive(Default)]
pub(crate) struct BootstrapViewModel {
    pub frontier_heads: Vec<FrontierHeadViewModel>,
}

impl BootstrapViewModel {
    pub(crate) fn update(&mut self, bootstrapper: &Bootstrapper) {
        self.frontier_heads.clear();
        for head in bootstrapper.frontier_heads() {
            self.frontier_heads.push(FrontierHeadViewModel {
                start: abbrev(&head.start),
                end: abbrev(&head.end),
                current: format!("{:.1}%", head.done_normalized() * 100.0),
                done_normalized: head.done_normalized(),
            });
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
    result.truncate(6);
    result.push_str("...");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abbreviate_acccount() {
        assert_eq!(abbrev(&Account::from(0)), "000000...");
    }
}
