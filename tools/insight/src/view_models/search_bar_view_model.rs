use rsnano_core::BlockHash;

#[derive(Default)]
pub(crate) struct SearchBarViewModel {
    pub input: String,
}
impl SearchBarViewModel {
    pub(crate) fn input_changed(&self) {
        if let Ok(hash) = BlockHash::decode_hex(&self.input) {}
    }
}
