use super::{BlockBase, BlockType};
use crate::{
    utils::{BufferWriter, Deserialize, FixedSizeSerialize, Serialize, Stream},
    Account, Amount, BlockHash, BlockHashBuilder, JsonBlock, LazyBlockHash, Link, PrivateKey,
    PublicKey, Root, Signature, WorkNonce,
};
use anyhow::Result;

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct StateHashables {
    // Account# / public key that operates this account
    // Uses:
    // Bulk signature validation in advance of further ledger processing
    // Arranging uncomitted transactions by account
    pub account: Account,

    // Previous transaction in this chain
    pub previous: BlockHash,

    // Representative of this account
    pub representative: PublicKey,

    // Current balance of this account
    // Allows lookup of account balance simply by looking at the head block
    pub balance: Amount,

    // Link field contains source block_hash if receiving, destination account if sending
    pub link: Link,
}

impl From<&StateHashables> for BlockHash {
    fn from(hashables: &StateHashables) -> Self {
        let mut preamble = [0u8; 32];
        preamble[31] = BlockType::State as u8;
        BlockHashBuilder::new()
            .update(preamble)
            .update(hashables.account.as_bytes())
            .update(hashables.previous.as_bytes())
            .update(hashables.representative.as_bytes())
            .update(hashables.balance.to_be_bytes())
            .update(hashables.link.as_bytes())
            .build()
    }
}

#[derive(Clone, Default, Debug)]
pub struct StateBlock {
    pub work: u64,
    pub signature: Signature,
    pub hashables: StateHashables,
    pub hash: LazyBlockHash,
}

#[allow(clippy::too_many_arguments)]
impl StateBlock {
    pub fn new(
        account: Account,
        previous: BlockHash,
        representative: PublicKey,
        balance: Amount,
        link: Link,
        keys: &PrivateKey,
        work: u64,
    ) -> Self {
        Self::new_obsolete(account, previous, representative, balance, link, keys, work)
    }

    // Don't use this anymore
    pub fn new_obsolete(
        account: Account,
        previous: BlockHash,
        representative: PublicKey,
        balance: Amount,
        link: Link,
        prv_key: &PrivateKey,
        work: u64,
    ) -> Self {
        let hashables = StateHashables {
            account,
            previous,
            representative,
            balance,
            link,
        };

        let hash = LazyBlockHash::new();
        let signature = prv_key.sign(hash.hash(&hashables).as_bytes());

        Self {
            work,
            signature,
            hashables,
            hash,
        }
    }

    pub fn new_test_instance_with_key(key: PrivateKey) -> Self {
        Self::new(
            key.account(),
            BlockHash::from(456),
            PublicKey::from(789),
            Amount::raw(420),
            Link::from(111),
            &key,
            69420,
        )
    }

    pub fn new_test_instance() -> Self {
        let key = PrivateKey::from(42);
        Self::new_test_instance_with_key(key)
    }

    pub fn with_signature(
        account: Account,
        previous: BlockHash,
        representative: PublicKey,
        balance: Amount,
        link: Link,
        signature: Signature,
        work: u64,
    ) -> Self {
        Self {
            work,
            signature,
            hashables: StateHashables {
                account,
                previous,
                representative,
                balance,
                link,
            },
            hash: LazyBlockHash::new(),
        }
    }

    pub fn new_test_open() -> Self {
        let key = PrivateKey::from(42);
        Self::new(
            key.account(),
            BlockHash::zero(),
            PublicKey::from(789),
            Amount::raw(420),
            Link::from(111),
            &key,
            69420,
        )
    }

    pub fn verify_signature(&self) -> anyhow::Result<()> {
        self.account()
            .as_key()
            .verify(self.hash().as_bytes(), self.block_signature())
    }

    pub fn account(&self) -> Account {
        self.hashables.account
    }

    pub fn link(&self) -> Link {
        self.hashables.link
    }

    pub fn balance(&self) -> Amount {
        self.hashables.balance
    }

    pub fn source(&self) -> BlockHash {
        BlockHash::zero()
    }

    pub fn mandatory_representative(&self) -> PublicKey {
        self.hashables.representative
    }

    pub fn destination(&self) -> Account {
        Account::zero()
    }

    pub fn serialized_size() -> usize {
        Account::serialized_size() // Account
            + BlockHash::serialized_size() // Previous
            + Account::serialized_size() // Representative
            + Amount::serialized_size() // Balance
            + Link::serialized_size() // Link
            + Signature::serialized_size()
            + std::mem::size_of::<u64>() // Work
    }

    pub fn deserialize(stream: &mut dyn Stream) -> Result<Self> {
        let account = Account::deserialize(stream)?;
        let previous = BlockHash::deserialize(stream)?;
        let representative = PublicKey::deserialize(stream)?;
        let balance = Amount::deserialize(stream)?;
        let link = Link::deserialize(stream)?;
        let signature = Signature::deserialize(stream)?;
        let mut work_bytes = [0u8; 8];
        stream.read_bytes(&mut work_bytes, 8)?;
        let work = u64::from_be_bytes(work_bytes);
        Ok(Self {
            work,
            signature,
            hashables: StateHashables {
                account,
                previous,
                representative,
                balance,
                link,
            },
            hash: LazyBlockHash::new(),
        })
    }
}

impl PartialEq for StateBlock {
    fn eq(&self, other: &Self) -> bool {
        self.work == other.work
            && self.signature == other.signature
            && self.hashables == other.hashables
    }
}

impl Eq for StateBlock {}

impl BlockBase for StateBlock {
    fn block_type(&self) -> BlockType {
        BlockType::State
    }

    fn account_field(&self) -> Option<Account> {
        Some(self.hashables.account)
    }

    fn hash(&self) -> BlockHash {
        self.hash.hash(&self.hashables)
    }

    fn link_field(&self) -> Option<Link> {
        Some(self.hashables.link)
    }

    fn block_signature(&self) -> &Signature {
        &self.signature
    }

    fn set_block_signature(&mut self, signature: &Signature) {
        self.signature = signature.clone();
    }

    fn set_work(&mut self, work: u64) {
        self.work = work;
    }

    fn work(&self) -> u64 {
        self.work
    }

    fn previous(&self) -> BlockHash {
        self.hashables.previous
    }

    fn serialize_without_block_type(&self, writer: &mut dyn BufferWriter) {
        self.hashables.account.serialize(writer);
        self.hashables.previous.serialize(writer);
        self.hashables.representative.serialize(writer);
        self.hashables.balance.serialize(writer);
        self.hashables.link.serialize(writer);
        self.signature.serialize(writer);
        writer.write_bytes_safe(&self.work.to_be_bytes());
    }

    fn root(&self) -> Root {
        if !self.previous().is_zero() {
            self.previous().into()
        } else {
            self.hashables.account.into()
        }
    }

    fn balance_field(&self) -> Option<Amount> {
        Some(self.hashables.balance)
    }

    fn source_field(&self) -> Option<BlockHash> {
        None
    }

    fn representative_field(&self) -> Option<PublicKey> {
        Some(self.hashables.representative)
    }

    fn valid_predecessor(&self, _block_type: BlockType) -> bool {
        true
    }

    fn destination_field(&self) -> Option<Account> {
        None
    }

    fn json_representation(&self) -> JsonBlock {
        JsonBlock::State(JsonStateBlock {
            account: self.hashables.account,
            previous: self.hashables.previous,
            representative: self.hashables.representative.into(),
            balance: self.hashables.balance,
            link: self.hashables.link,
            link_as_account: Some(self.hashables.link.into()),
            signature: self.signature.clone(),
            work: self.work.into(),
        })
    }
}

impl From<JsonStateBlock> for StateBlock {
    fn from(value: JsonStateBlock) -> Self {
        let hashables = StateHashables {
            account: value.account,
            previous: value.previous,
            representative: value.representative.into(),
            balance: value.balance,
            link: value.link,
        };

        let hash = LazyBlockHash::new();

        Self {
            work: value.work.into(),
            signature: value.signature,
            hashables,
            hash,
        }
    }
}

#[derive(PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct JsonStateBlock {
    pub account: Account,
    pub previous: BlockHash,
    pub representative: Account,
    pub balance: Amount,
    pub link: Link,
    pub link_as_account: Option<Account>,
    pub signature: Signature,
    pub work: WorkNonce,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{utils::MemoryStream, Block, BlockBuilder, StateBlockBuilder};

    // original test: state_block.serialization
    #[test]
    fn serialization() {
        let block1 = BlockBuilder::state().work(5).build();
        let mut stream = MemoryStream::new();
        block1.serialize_without_block_type(&mut stream);
        assert_eq!(StateBlock::serialized_size(), stream.bytes_written());
        assert_eq!(stream.byte_at(215), 0x5); // Ensure work is serialized big-endian

        let block2 = StateBlock::deserialize(&mut stream).unwrap();
        assert_eq!(block1, Block::State(block2));
    }

    // original test: state_block.hashing
    #[test]
    fn hashing() {
        let block = BlockBuilder::state().build();
        let hash = block.hash().clone();
        assert_eq!(hash, block.hash()); // check cache works
        assert_eq!(hash, BlockBuilder::state().build().hash());

        let assert_different_hash = |b: StateBlockBuilder| {
            assert_ne!(hash, b.build().hash());
        };

        assert_different_hash(BlockBuilder::state().account(Account::from(1000)));
        assert_different_hash(BlockBuilder::state().previous(BlockHash::from(1000)));
        assert_different_hash(BlockBuilder::state().representative(Account::from(1000)));
        assert_different_hash(BlockBuilder::state().balance(Amount::from(1000)));
        assert_different_hash(BlockBuilder::state().link(Link::from(1000)));
    }

    #[test]
    fn serialize_serde() {
        let block = Block::State(StateBlock::new_test_instance());
        let serialized = serde_json::to_string_pretty(&block).unwrap();
        assert_eq!(
            serialized,
            r#"{
  "type": "state",
  "account": "nano_39y535msmkzb31bx73tdnf8iken5ucw9jt98re7nriduus6cgs6uonjdm8r5",
  "previous": "00000000000000000000000000000000000000000000000000000000000001C8",
  "representative": "nano_11111111111111111111111111111111111111111111111111ros3kc7wyy",
  "balance": "420",
  "link": "000000000000000000000000000000000000000000000000000000000000006F",
  "link_as_account": "nano_111111111111111111111111111111111111111111111111115hkrzwewgm",
  "signature": "F26EC6180795C63CFEC46F929DCF6269445208B6C1C837FA64925F1D61C218D4D263F9A73A4B76E3174888C6B842FC1380AC15183FA67E92B2091FEBCCBDB308",
  "work": "0000000000010F2C"
}"#
        );
    }
}
