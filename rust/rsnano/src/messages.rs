use crate::{utils::Stream, NetworkConstants, Networks};
use anyhow::Result;
use num_traits::FromPrimitive;
use std::mem::size_of;

/// Message types are serialized to the network and existing values must thus never change as
/// types are added, removed and reordered in the enum.
#[repr(u8)]
#[derive(FromPrimitive, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Invalid = 0x0,
    NotAType = 0x1,
    Keepalive = 0x2,
    Publish = 0x3,
    ConfirmReq = 0x4,
    ConfirmAck = 0x5,
    BulkPull = 0x6,
    BulkPush = 0x7,
    FrontierReq = 0x8,
    /* deleted 0x9 */
    NodeIdHandshake = 0x0a,
    BulkPullAccount = 0x0b,
    TelemetryReq = 0x0c,
    TelemetryAck = 0x0d,
}
impl MessageType {
    pub fn as_str(&self) -> &str {
        match self {
            MessageType::Invalid => "invalid",
            MessageType::NotAType => "not_a_type",
            MessageType::Keepalive => "keepalive",
            MessageType::Publish => "publish",
            MessageType::ConfirmReq => "confirm_req",
            MessageType::ConfirmAck => "confirm_ack",
            MessageType::BulkPull => "bulk_pull",
            MessageType::BulkPush => "bulk_push",
            MessageType::FrontierReq => "frontier_req",
            MessageType::NodeIdHandshake => "node_id_handshake",
            MessageType::BulkPullAccount => "bulk_pull_account",
            MessageType::TelemetryReq => "telemetry_req",
            MessageType::TelemetryAck => "telemetry_ack",
        }
    }
}

#[derive(Clone)]
pub struct MessageHeader {
    message_type: MessageType,
    version_using: u8,
    version_max: u8,
    version_min: u8,
    network: Networks,
}

impl MessageHeader {
    pub fn new(constants: &NetworkConstants, message_type: MessageType) -> Self {
        let version_using = constants.protocol_version;
        Self::with_version_using(constants, message_type, version_using)
    }

    pub fn with_version_using(
        constants: &NetworkConstants,
        message_type: MessageType,
        version_using: u8,
    ) -> Self {
        Self {
            message_type,
            version_using,
            version_max: constants.protocol_version,
            version_min: constants.protocol_version_min,
            network: constants.current_network,
        }
    }

    pub fn version_using(&self) -> u8 {
        self.version_using
    }

    pub fn version_max(&self) -> u8 {
        self.version_max
    }

    pub fn version_min(&self) -> u8 {
        self.version_min
    }

    pub fn network(&self) -> Networks {
        self.network
    }

    pub fn message_type(&self) -> MessageType {
        self.message_type
    }

    pub fn size() -> usize {
        size_of::<u8>() // version_using
        + size_of::<u8>() // version_min
        + size_of::<u8>() // version_max
        + size_of::<Networks>()
        + size_of::<MessageType>()
    }

    pub(crate) fn deserialize(&mut self, stream: &mut dyn Stream) -> Result<()> {
        let mut buffer = [0; 2];
        stream.read_bytes(&mut buffer, 2)?;
        self.network = Networks::from_u16(u16::from_be_bytes(buffer))
            .ok_or_else(|| anyhow!("invalid network"))?;
        self.version_max = stream.read_u8()?;
        self.version_using = stream.read_u8()?;
        self.version_min = stream.read_u8()?;
        self.message_type = MessageType::from_u8(stream.read_u8()?)
            .ok_or_else(|| anyhow!("invalid message type"))?;
        Ok(())
    }
}
