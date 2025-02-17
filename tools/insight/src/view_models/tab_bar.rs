use crate::navigator::NavItem;

pub(crate) struct TabViewModel {
    pub selected: bool,
    pub label: &'static str,
    pub value: NavItem,
}
