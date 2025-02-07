use super::{
    utils::{BufferWriter, Deserialize, FixedSizeSerialize, Stream},
    Account, BlockHash, BlockHashBuilder, PrivateKey, Signature,
};
use crate::{utils::Serialize, Amount, PublicKey};
use anyhow::Result;
use std::time::{Duration, SystemTime};

#[derive(FromPrimitive, Copy, Clone, PartialEq, Eq, Debug)]
pub enum VoteSource {
    Live,
    Rebroadcast,
    Cache,
}

#[derive(FromPrimitive, Clone, Copy, PartialEq, Eq, Debug)]
pub enum VoteCode {
    Invalid,       // Vote is not signed correctly
    Replay,        // Vote does not have the highest timestamp, it's a replay
    Vote,          // Vote has the highest timestamp
    Indeterminate, // Unknown if replay or vote
    Ignored,       // Vote is valid, but got ingored (e.g. due to cooldown)
}

impl VoteCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            VoteCode::Vote => "vote",
            VoteCode::Replay => "replay",
            VoteCode::Indeterminate => "indeterminate",
            VoteCode::Ignored => "ignored",
            VoteCode::Invalid => "invalid",
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Vote {
    pub timestamp: u64,

    // Account that's voting
    pub voting_account: PublicKey,

    // Signature of timestamp + block hashes
    pub signature: Signature,

    // The hashes for which this vote directly covers
    pub hashes: Vec<BlockHash>,
}

static HASH_PREFIX: &str = "vote ";

impl Vote {
    pub const MAX_HASHES: usize = 255;
    pub fn null() -> Self {
        Self {
            timestamp: 0,
            voting_account: PublicKey::zero(),
            signature: Signature::new(),
            hashes: Vec::new(),
        }
    }

    pub fn new_final(key: &PrivateKey, hashes: Vec<BlockHash>) -> Self {
        assert!(hashes.len() <= Self::MAX_HASHES);
        Self::new(key, Self::TIMESTAMP_MAX, Self::DURATION_MAX, hashes)
    }

    pub fn new(
        priv_key: &PrivateKey,
        timestamp: u64,
        duration: u8,
        hashes: Vec<BlockHash>,
    ) -> Self {
        assert!(hashes.len() <= Self::MAX_HASHES);
        let mut result = Self {
            voting_account: priv_key.public_key(),
            timestamp: packed_timestamp(timestamp, duration),
            signature: Signature::new(),
            hashes,
        };
        result.signature = priv_key.sign(result.hash().as_bytes());
        result
    }

    pub fn new_test_instance() -> Self {
        let key = PrivateKey::from(42);
        Self::new(&key, 1, 2, vec![BlockHash::from(5)])
    }

    /// Timestamp for final vote
    pub const FINAL_TIMESTAMP: u64 = u64::MAX;
    pub const DURATION_MAX: u8 = 0x0F;
    pub const TIMESTAMP_MAX: u64 = 0xFFFF_FFFF_FFFF_FFF0;
    pub const TIMESTAMP_MIN: u64 = 0x0000_0000_0000_0010;
    const TIMESTAMP_MASK: u64 = 0xFFFF_FFFF_FFFF_FFF0;

    /// Returns the timestamp of the vote (with the duration bits masked, set to zero)
    /// If it is a final vote, all the bits including duration bits are returned as they are, all FF
    pub fn timestamp(&self) -> u64 {
        if self.is_final() {
            self.timestamp //final vote
        } else {
            self.timestamp & Self::TIMESTAMP_MASK
        }
    }

    pub fn is_final(&self) -> bool {
        self.timestamp == Vote::FINAL_TIMESTAMP
    }

    pub fn duration_bits(&self) -> u8 {
        // Duration field is specified in the 4 low-order bits of the timestamp.
        // This makes the timestamp have a minimum granularity of 16ms
        // The duration is specified as 2^(duration + 4) giving it a range of 16-524,288ms in power of two increments
        let result = self.timestamp & !Self::TIMESTAMP_MASK;
        result as u8
    }

    pub fn duration(&self) -> Duration {
        Duration::from_millis(1 << (self.duration_bits() + 4))
    }

    pub fn hash(&self) -> BlockHash {
        let mut builder = BlockHashBuilder::new().update(HASH_PREFIX);

        for hash in &self.hashes {
            builder = builder.update(hash.as_bytes())
        }

        builder.update(self.timestamp.to_ne_bytes()).build()
    }

    pub fn deserialize(&mut self, stream: &mut impl Stream) -> Result<()> {
        self.voting_account = PublicKey::deserialize(stream)?;
        self.signature = Signature::deserialize(stream)?;
        let mut buffer = [0; 8];
        stream.read_bytes(&mut buffer, 8)?;
        self.timestamp = u64::from_le_bytes(buffer);
        self.hashes = Vec::new();
        while stream.in_avail()? > 0 && self.hashes.len() < Self::MAX_HASHES {
            self.hashes.push(BlockHash::deserialize(stream)?);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.voting_account
            .verify(self.hash().as_bytes(), &self.signature)
    }

    pub fn serialized_size(count: usize) -> usize {
        Account::serialized_size()
        + Signature::serialized_size()
        + std::mem::size_of::<u64>() // timestamp
        + (BlockHash::serialized_size() * count)
    }
}

impl Serialize for Vote {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        self.voting_account.serialize(writer);
        self.signature.serialize(writer);
        writer.write_bytes_safe(&self.timestamp.to_le_bytes());
        for hash in &self.hashes {
            hash.serialize(writer);
        }
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
            && self.voting_account == other.voting_account
            && self.signature == other.signature
            && self.hashes == other.hashes
    }
}

impl Eq for Vote {}

fn packed_timestamp(timestamp: u64, duration: u8) -> u64 {
    debug_assert!(duration <= Vote::DURATION_MAX);
    debug_assert!(timestamp != Vote::TIMESTAMP_MAX || duration == Vote::DURATION_MAX);
    (timestamp & Vote::TIMESTAMP_MASK) | (duration as u64)
}

#[derive(Clone, Debug)]
pub struct VoteWithWeightInfo {
    pub representative: PublicKey,
    pub time: SystemTime,
    pub timestamp: u64,
    pub hash: BlockHash,
    pub weight: Amount,
}
