use bitvec::prelude::BitArray;
use num_traits::FromPrimitive;
use rsnano_core::{
    utils::{BufferWriter, Deserialize, Serialize, Stream, StreamExt},
    Account, Block, BlockHash, BlockType, Frontier,
};
use serde::ser::SerializeStruct;
use serde_derive::Serialize;
use std::{collections::VecDeque, fmt::Display, mem::size_of};

use super::{AscPullPayloadId, MessageVariant};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AscPullAckType {
    Blocks(BlocksAckPayload),
    AccountInfo(AccountInfoAckPayload),
    Frontiers(Vec<Frontier>),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AscPullAck {
    pub id: u64,
    pub pull_type: AscPullAckType,
}

impl AscPullAck {
    pub const MAX_FRONTIERS: usize = 1000;

    pub fn new_test_instance_blocks() -> Self {
        Self {
            id: 12345,
            pull_type: AscPullAckType::Blocks(BlocksAckPayload(VecDeque::from([
                Block::new_test_instance(),
            ]))),
        }
    }

    pub fn new_test_instance_account() -> Self {
        Self {
            id: 12345,
            pull_type: AscPullAckType::AccountInfo(AccountInfoAckPayload::new_test_instance()),
        }
    }

    pub fn deserialize(stream: &mut impl Stream) -> Option<Self> {
        let pull_type_code = AscPullPayloadId::from_u8(stream.read_u8().ok()?)?;
        let id = stream.read_u64_be().ok()?;
        let pull_type = match pull_type_code {
            AscPullPayloadId::Blocks => {
                let mut payload = BlocksAckPayload::default();
                payload.deserialize(stream).ok()?;
                AscPullAckType::Blocks(payload)
            }
            AscPullPayloadId::AccountInfo => {
                let mut payload = AccountInfoAckPayload::default();
                payload.deserialize(stream).ok()?;
                AscPullAckType::AccountInfo(payload)
            }
            AscPullPayloadId::Frontiers => {
                let mut frontiers = Vec::new();
                let mut current = Frontier::deserialize(stream).ok()?;
                while current != Frontier::default() && frontiers.len() < Self::MAX_FRONTIERS {
                    frontiers.push(current);
                    current = Frontier::deserialize(stream).ok()?;
                }
                AscPullAckType::Frontiers(frontiers)
            }
        };

        Some(AscPullAck { id, pull_type })
    }

    pub fn payload_type(&self) -> AscPullPayloadId {
        match self.pull_type {
            AscPullAckType::Blocks(_) => AscPullPayloadId::Blocks,
            AscPullAckType::AccountInfo(_) => AscPullPayloadId::AccountInfo,
            AscPullAckType::Frontiers(_) => AscPullPayloadId::Frontiers,
        }
    }

    fn serialize_pull_type(&self, writer: &mut dyn BufferWriter) {
        match &self.pull_type {
            AscPullAckType::Blocks(blocks) => blocks.serialize(writer),
            AscPullAckType::AccountInfo(account_info) => account_info.serialize(writer),
            AscPullAckType::Frontiers(frontiers) => {
                debug_assert!(frontiers.len() <= Self::MAX_FRONTIERS);
                for frontier in frontiers {
                    frontier.serialize(writer);
                }
                Frontier::default().serialize(writer);
            }
        }
    }

    pub fn serialized_size(extensions: BitArray<u16>) -> usize {
        let payload_length = extensions.data as usize;

        size_of::<u8>() // type code 
        + size_of::<u64>() // id
        + payload_length
    }
}

impl Serialize for AscPullAck {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        writer.write_u8_safe(self.payload_type() as u8);
        writer.write_u64_be_safe(self.id);
        self.serialize_pull_type(writer);
    }
}

impl MessageVariant for AscPullAck {
    fn header_extensions(&self, payload_len: u16) -> BitArray<u16> {
        BitArray::new(
            payload_len
            -1 // pull_type
            - 8, // ID
        )
    }
}

impl Display for AscPullAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.pull_type {
            AscPullAckType::Blocks(blocks) => {
                for block in blocks.blocks() {
                    write!(f, "{}", block.to_json().map_err(|_| std::fmt::Error)?)?;
                }
            }
            AscPullAckType::AccountInfo(info) => {
                write!(
                    f,
                    "\naccount public key:{} account open:{} account head:{} block count:{} confirmation frontier:{} confirmation height:{}",
                    info.account.encode_account(),
                    info.account_open,
                    info.account_head,
                    info.account_block_count,
                    info.account_conf_frontier,
                    info.account_conf_height,
                )?;
            }
            AscPullAckType::Frontiers(_) => {}
        }
        Ok(())
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct BlocksAckPayload(VecDeque<Block>);

impl BlocksAckPayload {
    pub fn new(blocks: VecDeque<Block>) -> Self {
        if blocks.len() > Self::MAX_BLOCKS as usize {
            panic!(
                "too many blocks for BlocksAckPayload. Maximum is {}, but was {}",
                Self::MAX_BLOCKS,
                blocks.len()
            );
        }
        Self(blocks)
    }

    pub fn new_test_instance() -> Self {
        let mut blocks = VecDeque::new();
        blocks.push_back(Block::new_test_instance());
        Self::new(blocks)
    }

    /* Header allows for 16 bit extensions; 65535 bytes / 500 bytes (block size with some future margin) ~ 131 */
    pub const MAX_BLOCKS: u8 = 128;

    pub fn blocks(&self) -> &VecDeque<Block> {
        &self.0
    }

    pub fn deserialize(&mut self, stream: &mut dyn Stream) -> anyhow::Result<()> {
        while let Ok(current) = Block::deserialize(stream) {
            if self.0.len() >= Self::MAX_BLOCKS as usize {
                bail!("too many blocks")
            }
            self.0.push_back(current);
        }
        Ok(())
    }
}

impl Serialize for BlocksAckPayload {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        for block in self.blocks() {
            block.serialize(writer);
        }
        // For convenience, end with null block terminator
        writer.write_u8_safe(BlockType::NotABlock as u8)
    }
}

impl serde::Serialize for BlocksAckPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Block", 6)?;
        state.serialize_field("blocks", &self.0)?;
        state.end()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Serialize)]
pub struct AccountInfoAckPayload {
    pub account: Account,
    pub account_open: BlockHash,
    pub account_head: BlockHash,
    pub account_block_count: u64,
    pub account_conf_frontier: BlockHash,
    pub account_conf_height: u64,
}

impl AccountInfoAckPayload {
    pub fn deserialize(&mut self, stream: &mut dyn Stream) -> anyhow::Result<()> {
        self.account = Account::deserialize(stream)?;
        self.account_open = BlockHash::deserialize(stream)?;
        self.account_head = BlockHash::deserialize(stream)?;
        self.account_block_count = stream.read_u64_be()?;
        self.account_conf_frontier = BlockHash::deserialize(stream)?;
        self.account_conf_height = stream.read_u64_be()?;
        Ok(())
    }

    pub fn new_test_instance() -> AccountInfoAckPayload {
        Self {
            account: Account::from(1),
            account_open: BlockHash::from(2),
            account_head: BlockHash::from(3),
            account_block_count: 4,
            account_conf_frontier: BlockHash::from(5),
            account_conf_height: 3,
        }
    }
}

impl Serialize for AccountInfoAckPayload {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        self.account.serialize(writer);
        self.account_open.serialize(writer);
        self.account_head.serialize(writer);
        writer.write_u64_be_safe(self.account_block_count);
        self.account_conf_frontier.serialize(writer);
        writer.write_u64_be_safe(self.account_conf_height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_deserializable, Message};
    use rsnano_core::TestBlockBuilder;

    #[test]
    fn serialize_blocks() {
        let original = Message::AscPullAck(AscPullAck {
            id: 7,
            pull_type: AscPullAckType::Blocks(BlocksAckPayload::new(VecDeque::from([
                TestBlockBuilder::state().build(),
                TestBlockBuilder::state().build(),
            ]))),
        });

        assert_deserializable(&original);
    }

    #[test]
    fn serialize_account_info() {
        let original = Message::AscPullAck(AscPullAck {
            id: 7,
            pull_type: AscPullAckType::AccountInfo(AccountInfoAckPayload {
                account: Account::from(1),
                account_open: BlockHash::from(2),
                account_head: BlockHash::from(3),
                account_block_count: 4,
                account_conf_frontier: BlockHash::from(5),
                account_conf_height: 6,
            }),
        });

        assert_deserializable(&original);
    }

    #[test]
    fn serialize_frontiers() {
        let original = Message::AscPullAck(AscPullAck {
            id: 7,
            pull_type: AscPullAckType::Frontiers(vec![Frontier::new(
                Account::from(1),
                BlockHash::from(2),
            )]),
        });

        assert_deserializable(&original);
    }

    #[test]
    fn display() {
        let ack = Message::AscPullAck(AscPullAck {
            id: 7,
            pull_type: AscPullAckType::AccountInfo(AccountInfoAckPayload {
                account: Account::from(1),
                account_open: BlockHash::from(2),
                account_head: BlockHash::from(3),
                account_block_count: 4,
                account_conf_frontier: BlockHash::from(5),
                account_conf_height: 6,
            }),
        });
        assert_eq!(ack.to_string(), "\naccount public key:nano_1111111111111111111111111111111111111111111111111113b8661hfk account open:0000000000000000000000000000000000000000000000000000000000000002 account head:0000000000000000000000000000000000000000000000000000000000000003 block count:4 confirmation frontier:0000000000000000000000000000000000000000000000000000000000000005 confirmation height:6");
    }
}
