use super::Account;
use crate::serialize_32_byte_string;
use crate::u256_struct;
use blake2::digest::Update;
use blake2::digest::VariableOutput;
use blake2::Blake2bVar;
use rand::Rng;

u256_struct!(BlockHash);
serialize_32_byte_string!(BlockHash);

impl BlockHash {
    pub fn random() -> Self {
        BlockHash::from_bytes(rand::rng().random())
    }
}

impl From<&Account> for BlockHash {
    fn from(account: &Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

impl From<Account> for BlockHash {
    fn from(account: Account) -> Self {
        Self::from_bytes(*account.as_bytes())
    }
}

pub struct BlockHashBuilder {
    blake: Blake2bVar,
}

impl Default for BlockHashBuilder {
    fn default() -> Self {
        Self {
            blake: Blake2bVar::new(32).unwrap(),
        }
    }
}

impl BlockHashBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn update(mut self, data: impl AsRef<[u8]>) -> Self {
        self.blake.update(data.as_ref());
        self
    }

    pub fn build(self) -> BlockHash {
        let mut hash_bytes = [0u8; 32];
        self.blake.finalize_variable(&mut hash_bytes).unwrap();
        BlockHash::from_bytes(hash_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_serialize() {
        let serialized = serde_json::to_string_pretty(&BlockHash::from(123)).unwrap();
        assert_eq!(
            serialized,
            "\"000000000000000000000000000000000000000000000000000000000000007B\""
        );
    }

    #[test]
    fn serde_deserialize() {
        let deserialized: BlockHash = serde_json::from_str(
            "\"000000000000000000000000000000000000000000000000000000000000007B\"",
        )
        .unwrap();
        assert_eq!(deserialized, BlockHash::from(123));
    }
}
