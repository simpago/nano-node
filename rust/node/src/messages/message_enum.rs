use crate::{
    utils::BlockUniquer,
    voting::{Vote, VoteUniquer},
};

use super::*;
use anyhow::Result;
use rsnano_core::{
    utils::{Serialize, Stream},
    BlockEnum, BlockHash, BlockType, Root,
};
use std::{any::Any, fmt::Display, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageEnum {
    pub header: MessageHeader,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Keepalive(KeepalivePayload),
    Publish(PublishPayload),
    AscPullAck(AscPullAckPayload),
    AscPullReq(AscPullReqPayload),
    BulkPull(BulkPullPayload),
    BulkPullAccount(BulkPullAccountPayload),
    BulkPush,
    ConfirmAck(ConfirmAckPayload),
    ConfirmReq(ConfirmReqPayload),
    FrontierReq(FrontierReqPayload),
}

impl Payload {
    fn serialize(&self, stream: &mut dyn Stream) -> std::result::Result<(), anyhow::Error> {
        match &self {
            Payload::Keepalive(x) => x.serialize(stream),
            Payload::Publish(x) => x.serialize(stream),
            Payload::AscPullAck(x) => x.serialize(stream),
            Payload::AscPullReq(x) => x.serialize(stream),
            Payload::BulkPull(x) => x.serialize(stream),
            Payload::BulkPullAccount(x) => x.serialize(stream),
            Payload::ConfirmAck(x) => x.serialize(stream),
            Payload::ConfirmReq(x) => x.serialize(stream),
            Payload::FrontierReq(x) => x.serialize(stream),
            Payload::BulkPush => Ok(()),
        }
    }
}

impl Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Payload::Keepalive(x) => x.fmt(f),
            Payload::Publish(x) => x.fmt(f),
            Payload::AscPullAck(x) => x.fmt(f),
            Payload::AscPullReq(x) => x.fmt(f),
            Payload::BulkPull(x) => x.fmt(f),
            Payload::BulkPullAccount(x) => x.fmt(f),
            Payload::ConfirmAck(x) => x.fmt(f),
            Payload::ConfirmReq(x) => x.fmt(f),
            Payload::FrontierReq(x) => x.fmt(f),
            Payload::BulkPush => Ok(()),
        }
    }
}

impl MessageEnum {
    pub fn new_keepalive(protocol_info: &ProtocolInfo) -> Self {
        Self {
            header: MessageHeader::new(MessageType::Keepalive, protocol_info),
            payload: Payload::Keepalive(Default::default()),
        }
    }

    pub fn new_publish(protocol_info: &ProtocolInfo, block: Arc<BlockEnum>) -> Self {
        let mut header = MessageHeader::new(MessageType::Publish, protocol_info);
        header.set_block_type(block.block_type());

        Self {
            header,
            payload: Payload::Publish(PublishPayload {
                block: Some(block),
                digest: 0,
            }),
        }
    }

    pub fn new_asc_pull_ack_blocks(
        protocol_info: &ProtocolInfo,
        id: u64,
        blocks: Vec<BlockEnum>,
    ) -> Self {
        let blocks = BlocksAckPayload::new(blocks);
        let header =
            MessageHeader::new_with_payload_len(MessageType::AscPullAck, protocol_info, &blocks);

        Self {
            header,
            payload: Payload::AscPullAck(AscPullAckPayload {
                id,
                pull_type: AscPullAckType::Blocks(blocks),
            }),
        }
    }

    pub fn new_asc_pull_ack_accounts(
        protocol_info: &ProtocolInfo,
        id: u64,
        accounts: AccountInfoAckPayload,
    ) -> Self {
        let header =
            MessageHeader::new_with_payload_len(MessageType::AscPullAck, protocol_info, &accounts);

        Self {
            header,
            payload: Payload::AscPullAck(AscPullAckPayload {
                id,
                pull_type: AscPullAckType::AccountInfo(accounts),
            }),
        }
    }

    pub fn new_asc_pull_req_blocks(
        protocol_info: &ProtocolInfo,
        id: u64,
        blocks: BlocksReqPayload,
    ) -> Self {
        let payload = AscPullReqPayload {
            req_type: AscPullReqType::Blocks(blocks),
            id,
        };

        let header = MessageHeader::new_with_payload_len(
            MessageType::AscPullReq,
            protocol_info,
            &payload.req_type,
        );
        Self {
            header,
            payload: Payload::AscPullReq(payload),
        }
    }

    pub fn new_asc_pull_req_accounts(
        protocol_info: &ProtocolInfo,
        id: u64,
        payload: AccountInfoReqPayload,
    ) -> Self {
        let payload = AscPullReqPayload {
            req_type: AscPullReqType::AccountInfo(payload),
            id,
        };
        let header = MessageHeader::new_with_payload_len(
            MessageType::AscPullReq,
            protocol_info,
            &payload.req_type,
        );
        Self {
            header,
            payload: Payload::AscPullReq(payload),
        }
    }

    pub fn new_bulk_pull(protocol_info: &ProtocolInfo, payload: BulkPullPayload) -> Self {
        let mut header = MessageHeader::new(MessageType::BulkPull, protocol_info);
        header
            .extensions
            .set(BulkPullPayload::COUNT_PRESENT_FLAG, payload.count > 0);
        header
            .extensions
            .set(BulkPullPayload::ASCENDING_FLAG, payload.ascending);
        Self {
            header,
            payload: Payload::BulkPull(payload),
        }
    }

    pub fn new_bulk_pull_account(
        protocol_info: &ProtocolInfo,
        payload: BulkPullAccountPayload,
    ) -> Self {
        Self {
            header: MessageHeader::new(MessageType::BulkPullAccount, protocol_info),
            payload: Payload::BulkPullAccount(payload),
        }
    }

    pub fn new_bulk_push(protocol_info: &ProtocolInfo) -> Self {
        Self {
            header: MessageHeader::new(MessageType::BulkPush, protocol_info),
            payload: Payload::BulkPush,
        }
    }

    pub fn new_confirm_ack(protocol_info: &ProtocolInfo, vote: Arc<Vote>) -> Self {
        let mut header = MessageHeader::new(MessageType::ConfirmAck, protocol_info);
        header.set_block_type(BlockType::NotABlock);
        debug_assert!(vote.hashes.len() < 16);
        header.set_count(vote.hashes.len() as u8);

        Self {
            header,
            payload: Payload::ConfirmAck(ConfirmAckPayload { vote }),
        }
    }

    pub fn new_confirm_req_with_block(protocol_info: &ProtocolInfo, block: Arc<BlockEnum>) -> Self {
        let mut header = MessageHeader::new(MessageType::ConfirmReq, protocol_info);
        header.set_block_type(block.block_type());

        Self {
            header,
            payload: Payload::ConfirmReq(ConfirmReqPayload {
                block: Some(block),
                roots_hashes: Vec::new(),
            }),
        }
    }

    pub fn new_confirm_req_with_roots_hashes(
        protocol_info: &ProtocolInfo,
        roots_hashes: Vec<(BlockHash, Root)>,
    ) -> Self {
        let mut header = MessageHeader::new(MessageType::ConfirmReq, protocol_info);
        // not_a_block (1) block type for hashes + roots request
        header.set_block_type(BlockType::NotABlock);

        debug_assert!(roots_hashes.len() < 16);
        header.set_count(roots_hashes.len() as u8);

        Self {
            header,
            payload: Payload::ConfirmReq(ConfirmReqPayload {
                block: None,
                roots_hashes,
            }),
        }
    }

    pub fn new_frontier_req(protocol_info: &ProtocolInfo, payload: FrontierReqPayload) -> Self {
        let mut header = MessageHeader::new(MessageType::FrontierReq, protocol_info);
        header
            .extensions
            .set(FrontierReqPayload::ONLY_CONFIRMED, payload.only_confirmed);
        Self {
            header,
            payload: Payload::FrontierReq(payload),
        }
    }

    pub fn deserialize(
        stream: &mut impl Stream,
        header: MessageHeader,
        digest: u128,
        block_uniquer: Option<&BlockUniquer>,
        vote_uniquer: Option<&VoteUniquer>,
    ) -> Result<Self> {
        let payload = match header.message_type {
            MessageType::Keepalive => {
                Payload::Keepalive(KeepalivePayload::deserialize(&header, stream)?)
            }
            MessageType::Publish => Payload::Publish(PublishPayload::deserialize(
                stream,
                &header,
                digest,
                block_uniquer,
            )?),
            MessageType::AscPullAck => {
                Payload::AscPullAck(AscPullAckPayload::deserialize(stream, &header)?)
            }
            MessageType::AscPullReq => {
                Payload::AscPullReq(AscPullReqPayload::deserialize(stream, &header)?)
            }
            MessageType::BulkPull => {
                Payload::BulkPull(BulkPullPayload::deserialize(stream, &header)?)
            }
            MessageType::BulkPullAccount => {
                Payload::BulkPullAccount(BulkPullAccountPayload::deserialize(stream, &header)?)
            }
            MessageType::BulkPush => Payload::BulkPush,
            MessageType::ConfirmAck => {
                Payload::ConfirmAck(ConfirmAckPayload::deserialize(stream, vote_uniquer)?)
            }
            MessageType::ConfirmReq => Payload::ConfirmReq(ConfirmReqPayload::deserialize(
                stream,
                &header,
                block_uniquer,
            )?),
            MessageType::FrontierReq => {
                Payload::FrontierReq(FrontierReqPayload::deserialize(stream, &header)?)
            }
            _ => unimplemented!(),
        };
        Ok(Self { header, payload })
    }
}

impl Message for MessageEnum {
    fn header(&self) -> &MessageHeader {
        &self.header
    }

    fn set_header(&mut self, header: &MessageHeader) {
        self.header = header.clone();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self, stream: &mut dyn Stream) -> Result<()> {
        self.header().serialize(stream)?;
        self.payload.serialize(stream)
    }

    fn visit(&self, visitor: &mut dyn MessageVisitor) {
        visitor.keepalive(self)
    }

    fn clone_box(&self) -> Box<dyn Message> {
        Box::new(self.clone())
    }

    fn message_type(&self) -> MessageType {
        self.header.message_type
    }
}

impl Display for MessageEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.header.fmt(f)?;
        self.payload.fmt(f)
    }
}
