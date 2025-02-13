use rsnano_core::DetailedBlock;

#[derive(Default)]
pub(crate) struct BlockViewModel {
    pub hash: String,
    pub block: String,
    pub amount: String,
    pub confirmed: String,
    pub balance: String,
    pub height: String,
    pub timestamp: String,
    pub subtype: &'static str,
    pub successor: String,
}

impl BlockViewModel {
    pub fn show(&mut self, block: &DetailedBlock) {
        self.hash = block.block.hash().to_string();
        self.block = serde_json::to_string_pretty(&block.block.json_representation()).unwrap();
        self.balance = block.block.balance().to_string_dec();
        self.height = block.block.height().to_string();
        self.amount = block.amount.unwrap_or_default().to_string_dec();
        self.confirmed = block.confirmed.to_string();
        self.timestamp = block.block.timestamp().utc().to_string();
        self.subtype = block.block.subtype().as_str();
        self.successor = block.block.successor().unwrap_or_default().to_string();
    }
}
