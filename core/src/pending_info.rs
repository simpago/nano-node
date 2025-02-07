use crate::{
    utils::{Deserialize, Stream},
    Account, Amount, Epoch,
};
use num::FromPrimitive;

/// Information on an uncollected send
/// This struct captures the data stored in a pending table entry
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct PendingInfo {
    /// The account sending the funds
    pub source: Account,
    /// Amount receivable in this transaction
    pub amount: Amount,
    /// Epoch of sending block, this info is stored here to make it possible to prune the send block
    pub epoch: Epoch,
}

impl Default for PendingInfo {
    fn default() -> Self {
        Self {
            source: Default::default(),
            amount: Default::default(),
            epoch: Epoch::Epoch0,
        }
    }
}

impl PendingInfo {
    pub fn new(source: Account, amount: Amount, epoch: Epoch) -> Self {
        Self {
            source,
            amount,
            epoch,
        }
    }

    pub fn to_bytes(&self) -> [u8; 49] {
        let mut bytes = [0; 49];
        bytes[..32].copy_from_slice(self.source.as_bytes());
        bytes[32..48].copy_from_slice(&self.amount.to_be_bytes());
        bytes[48] = self.epoch as u8;
        bytes
    }

    pub fn new_test_instance() -> Self {
        Self::new(Account::from(3), Amount::raw(4), Epoch::Epoch2)
    }
}

impl Deserialize for PendingInfo {
    type Target = Self;

    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self::Target> {
        let source = Account::deserialize(stream)?;
        let amount = Amount::deserialize(stream)?;
        let epoch =
            FromPrimitive::from_u8(stream.read_u8()?).ok_or_else(|| anyhow!("invalid epoch"))?;
        Ok(Self {
            source,
            amount,
            epoch,
        })
    }
}
