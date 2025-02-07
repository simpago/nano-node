#![allow(clippy::missing_safety_doc)]

#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate num_derive;

#[macro_use]
extern crate static_assertions;

mod account;
mod amount;
mod block_hash;
mod node_id;
mod public_key;
mod vote;

pub use account::Account;
pub use amount::Amount;
use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
pub use block_hash::{BlockHash, BlockHashBuilder};
pub use node_id::NodeId;
pub use public_key::PublicKey;
use serde::de::{Unexpected, Visitor};
pub use vote::*;

mod private_key;
pub use private_key::{PrivateKey, PrivateKeyFactory};

mod raw_key;
pub use raw_key::RawKey;

mod signature;
pub use signature::Signature;

mod u256_struct;

pub mod utils;

mod qualified_root;
pub use qualified_root::QualifiedRoot;

mod account_info;
pub use account_info::AccountInfo;

mod epoch;
pub use epoch::*;

mod confirmation_height_info;
pub use confirmation_height_info::ConfirmationHeightInfo;

mod pending_key;
pub use pending_key::PendingKey;

mod pending_info;
pub use pending_info::PendingInfo;

mod difficulty;
pub use difficulty::{Difficulty, DifficultyV1, StubDifficulty, WorkVersion};

mod blocks;
pub use blocks::*;

pub mod work;

mod unchecked_info;
pub use unchecked_info::{UncheckedInfo, UncheckedKey};

mod kdf;
pub use kdf::KeyDerivationFunction;
use utils::{BufferWriter, Deserialize, Serialize, Stream};

use std::{
    fmt::{Debug, Display, Write},
    str::FromStr,
    sync::Mutex,
};
use std::{num::ParseIntError, sync::LazyLock};

pub fn encode_hex(i: u128) -> String {
    let mut result = String::with_capacity(32);
    for byte in i.to_be_bytes() {
        write!(&mut result, "{:02X}", byte).unwrap();
    }
    result
}

pub fn write_hex_bytes(bytes: &[u8], f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    for &byte in bytes {
        write!(f, "{:02X}", byte)?;
    }
    Ok(())
}

pub fn to_hex_string(i: u64) -> String {
    format!("{:016X}", i)
}

pub fn u64_from_hex_str(s: impl AsRef<str>) -> Result<u64, ParseIntError> {
    u64::from_str_radix(s.as_ref(), 16)
}

u256_struct!(HashOrAccount);
serialize_32_byte_string!(HashOrAccount);
u256_struct!(Link);
serialize_32_byte_string!(Link);
u256_struct!(Root);
serialize_32_byte_string!(Root);
u256_struct!(WalletId);
serialize_32_byte_string!(WalletId);

impl WalletId {
    pub fn random() -> Self {
        let key = PrivateKey::new();
        Self::from_bytes(*key.public_key().as_bytes())
    }
}

impl From<HashOrAccount> for Account {
    fn from(source: HashOrAccount) -> Self {
        Account::from_bytes(*source.as_bytes())
    }
}

impl From<&HashOrAccount> for Account {
    fn from(source: &HashOrAccount) -> Self {
        Account::from_bytes(*source.as_bytes())
    }
}

impl From<Link> for Account {
    fn from(link: Link) -> Self {
        Account::from_bytes(*link.as_bytes())
    }
}

impl From<&Link> for Account {
    fn from(link: &Link) -> Self {
        Account::from_bytes(*link.as_bytes())
    }
}

impl From<Root> for Account {
    fn from(root: Root) -> Self {
        Account::from_bytes(*root.as_bytes())
    }
}

impl From<Account> for Link {
    fn from(account: Account) -> Self {
        Link::from_bytes(*account.as_bytes())
    }
}

impl From<&Account> for Link {
    fn from(account: &Account) -> Self {
        Link::from_bytes(*account.as_bytes())
    }
}

impl From<BlockHash> for Link {
    fn from(hash: BlockHash) -> Self {
        Link::from_bytes(*hash.as_bytes())
    }
}

impl From<HashOrAccount> for BlockHash {
    fn from(source: HashOrAccount) -> Self {
        BlockHash::from_bytes(*source.as_bytes())
    }
}

impl From<&HashOrAccount> for BlockHash {
    fn from(source: &HashOrAccount) -> Self {
        BlockHash::from_bytes(*source.as_bytes())
    }
}
impl From<Link> for BlockHash {
    fn from(link: Link) -> Self {
        BlockHash::from_bytes(*link.as_bytes())
    }
}

impl From<Root> for BlockHash {
    fn from(root: Root) -> Self {
        BlockHash::from_bytes(*root.as_bytes())
    }
}

impl From<Account> for HashOrAccount {
    fn from(account: Account) -> Self {
        HashOrAccount::from_bytes(*account.as_bytes())
    }
}

impl From<&BlockHash> for HashOrAccount {
    fn from(hash: &BlockHash) -> Self {
        HashOrAccount::from_bytes(*hash.as_bytes())
    }
}

impl From<BlockHash> for HashOrAccount {
    fn from(hash: BlockHash) -> Self {
        HashOrAccount::from_bytes(*hash.as_bytes())
    }
}

impl From<Link> for HashOrAccount {
    fn from(link: Link) -> Self {
        HashOrAccount::from_bytes(*link.as_bytes())
    }
}

impl From<&Link> for HashOrAccount {
    fn from(link: &Link) -> Self {
        HashOrAccount::from_bytes(*link.as_bytes())
    }
}

impl From<Account> for Root {
    fn from(key: Account) -> Self {
        Root::from_bytes(*key.as_bytes())
    }
}

impl From<&PublicKey> for Root {
    fn from(key: &PublicKey) -> Self {
        Root::from_bytes(*key.as_bytes())
    }
}

impl From<PublicKey> for Root {
    fn from(key: PublicKey) -> Self {
        Root::from_bytes(*key.as_bytes())
    }
}

impl From<&Account> for Root {
    fn from(hash: &Account) -> Self {
        Root::from_bytes(*hash.as_bytes())
    }
}

impl From<BlockHash> for Root {
    fn from(hash: BlockHash) -> Self {
        Root::from_bytes(*hash.as_bytes())
    }
}

impl From<&BlockHash> for Root {
    fn from(hash: &BlockHash) -> Self {
        Root::from_bytes(*hash.as_bytes())
    }
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, PartialOrd, Ord)]
pub struct NoValue {}

impl utils::FixedSizeSerialize for NoValue {
    fn serialized_size() -> usize {
        0
    }
}

impl utils::Serialize for NoValue {
    fn serialize(&self, _writer: &mut dyn BufferWriter) {}
}

impl utils::Deserialize for NoValue {
    type Target = Self;
    fn deserialize(_stream: &mut dyn utils::Stream) -> anyhow::Result<NoValue> {
        Ok(NoValue {})
    }
}

pub fn deterministic_key(seed: &RawKey, index: u32) -> RawKey {
    let mut buffer = [0; 32];
    let mut hasher = Blake2bVar::new(buffer.len()).unwrap();
    hasher.update(seed.as_bytes());
    hasher.update(&index.to_be_bytes());
    hasher.finalize_variable(&mut buffer).unwrap();
    RawKey::from_bytes(buffer)
}

/**
 * Network variants with different genesis blocks and network parameters
 */
#[repr(u16)]
#[derive(Clone, Copy, FromPrimitive, PartialEq, Eq, Debug)]
pub enum Networks {
    Invalid = 0x0,
    // Low work parameters, publicly known genesis key, dev IP ports
    NanoDevNetwork = 0x5241, // 'R', 'A'
    // Normal work parameters, secret beta genesis key, beta IP ports
    NanoBetaNetwork = 0x5242, // 'R', 'B'
    // Normal work parameters, secret live key, live IP ports
    NanoLiveNetwork = 0x5243, // 'R', 'C'
    // Normal work parameters, secret test genesis key, test IP ports
    NanoTestNetwork = 0x5258, // 'R', 'X'
}

impl Networks {
    pub fn as_str(&self) -> &str {
        match self {
            Networks::Invalid => "invalid",
            Networks::NanoDevNetwork => "dev",
            Networks::NanoBetaNetwork => "beta",
            Networks::NanoLiveNetwork => "live",
            Networks::NanoTestNetwork => "test",
        }
    }
}

impl FromStr for Networks {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Networks, Self::Err> {
        match s {
            "dev" => Ok(Networks::NanoDevNetwork),
            "beta" => Ok(Networks::NanoBetaNetwork),
            "live" => Ok(Networks::NanoLiveNetwork),
            "test" => Ok(Networks::NanoTestNetwork),
            _ => Err("Invalid network"),
        }
    }
}
//
//todo: make configurable in builld script again!
pub static ACTIVE_NETWORK: LazyLock<Mutex<Networks>> =
    LazyLock::new(|| Mutex::new(Networks::NanoBetaNetwork));

#[derive(PartialEq, Eq, Debug, Default, Clone)]
pub struct Frontier {
    pub account: Account,
    pub hash: BlockHash,
}

impl Frontier {
    pub fn new(account: Account, hash: BlockHash) -> Self {
        Self { account, hash }
    }

    pub fn new_test_instance() -> Self {
        Self::new(Account::from(1), BlockHash::from(2))
    }
}

impl Frontier {
    pub fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self> {
        let account = Account::deserialize(stream)?;
        let hash = BlockHash::deserialize(stream)?;
        Ok(Self::new(account, hash))
    }
}

impl Serialize for Frontier {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        self.account.serialize(stream);
        self.hash.serialize(stream);
    }
}

#[derive(PartialEq, Eq, Copy, Clone, PartialOrd, Ord, Default)]
pub struct WorkNonce(u64);

impl Display for WorkNonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016X}", self.0)
    }
}

impl Debug for WorkNonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl From<u64> for WorkNonce {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<WorkNonce> for u64 {
    fn from(value: WorkNonce) -> Self {
        value.0
    }
}

impl serde::Serialize for WorkNonce {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&to_hex_string(self.0))
    }
}

impl<'de> serde::Deserialize<'de> for WorkNonce {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = deserializer.deserialize_str(WorkNonceVisitor {})?;
        Ok(value)
    }
}

struct WorkNonceVisitor {}

impl<'de> Visitor<'de> for WorkNonceVisitor {
    type Value = WorkNonce;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a hex string containing 8 bytes")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let mut bytes = [0; 8];
        hex::decode_to_slice(v, &mut bytes).map_err(|_| {
            serde::de::Error::invalid_value(Unexpected::Str(v), &"a hex string containing 8 bytes")
        })?;
        Ok(WorkNonce(u64::from_be_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_key() {
        let seed = RawKey::from(1);
        let key = deterministic_key(&seed, 3);
        assert_eq!(
            key,
            RawKey::decode_hex("89A518E3B70A0843DE8470F87FF851F9C980B1B2802267A05A089677B8FA1926")
                .unwrap()
        );
    }

    #[test]
    fn serialize_work_nonce() {
        let serialized = serde_json::to_string(&WorkNonce::from(123)).unwrap();
        assert_eq!(serialized, "\"000000000000007B\"");
    }
}
