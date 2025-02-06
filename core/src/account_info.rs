use super::{BlockHash, Epoch};
use crate::{
    utils::{
        BufferWriter, Deserialize, MutStreamAdapter, Serialize, Stream, StreamExt, UnixTimestamp,
    },
    Amount, PublicKey,
};
use anyhow::Result;
use num_traits::FromPrimitive;

/// Latest information about an account
#[derive(PartialEq, Eq, Clone, Default, Debug)]
pub struct AccountInfo {
    pub head: BlockHash,
    pub representative: PublicKey,
    pub open_block: BlockHash,
    pub balance: Amount,
    /** Seconds since posix epoch */
    pub modified: UnixTimestamp,
    pub block_count: u64,
    pub epoch: Epoch,
}

impl AccountInfo {
    pub fn to_bytes(&self) -> [u8; 129] {
        let mut buffer = [0; 129];
        let mut stream = MutStreamAdapter::new(&mut buffer);
        self.serialize(&mut stream);
        buffer
    }

    pub fn new_test_instance() -> Self {
        Self {
            head: BlockHash::from(1),
            representative: PublicKey::from(2),
            open_block: BlockHash::from(3),
            balance: Amount::raw(42),
            modified: 4.into(),
            block_count: 5,
            epoch: Epoch::Epoch2,
        }
    }
}

impl Serialize for AccountInfo {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        self.head.serialize(stream);
        self.representative.serialize(stream);
        self.open_block.serialize(stream);
        self.balance.serialize(stream);
        stream.write_u64_ne_safe(self.modified.as_u64());
        stream.write_u64_ne_safe(self.block_count);
        stream.write_u8_safe(self.epoch as u8)
    }
}

impl Deserialize for AccountInfo {
    type Target = Self;
    fn deserialize(stream: &mut dyn Stream) -> Result<AccountInfo> {
        Ok(Self {
            head: BlockHash::deserialize(stream)?,
            representative: PublicKey::deserialize(stream)?,
            open_block: BlockHash::deserialize(stream)?,
            balance: Amount::deserialize(stream)?,
            modified: stream.read_u64_ne()?.into(),
            block_count: stream.read_u64_ne()?,
            epoch: Epoch::from_u8(stream.read_u8()?).ok_or_else(|| anyhow!("invalid epoch"))?,
        })
    }
}
