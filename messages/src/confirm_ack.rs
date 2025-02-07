use super::{ConfirmReq, MessageVariant};
use bitvec::prelude::BitArray;
use rsnano_core::{
    utils::{BufferWriter, Serialize, Stream},
    Vote,
};
use std::fmt::{Debug, Display};
/*
 * Binary Format:
 * [message_header] Common message header
 * [variable] Vote
 * - Serialized/deserialized by the `nano::vote` class.
 *
 * Header extensions:
 * - [0xf000] Count (for V1 protocol)
 * - [0x0f00] Block type
 *   - Not used anymore (V25.1+), but still present and set to `not_a_block = 0x1` for backwards compatibility
 * - [0xf000 (high), 0x00f0 (low)] Count V2 masks (for V2 protocol)
 * - [0x0001] Confirm V2 flag
 * - [0x0002] Reserved for V3+ versioning
 * - [0x0004] Rebroadcasted flag
 */
#[derive(Clone, Debug, serde::Serialize)]
pub struct ConfirmAck {
    vote: Vote,
    is_rebroadcasted: bool,
    /// Messages deserialized from network should have their digest set
    pub digest: u128,
}

impl ConfirmAck {
    pub const HASHES_MAX: usize = 255;
    pub const REBROADCASTED_FLAG: usize = 2;

    pub fn new_with_own_vote(vote: Vote) -> Self {
        assert!(vote.hashes.len() <= Self::HASHES_MAX);
        Self {
            vote,
            is_rebroadcasted: false,
            digest: 0,
        }
    }

    pub fn new_with_rebroadcasted_vote(vote: Vote) -> Self {
        assert!(vote.hashes.len() <= Self::HASHES_MAX);
        Self {
            vote,
            is_rebroadcasted: true,
            digest: 0,
        }
    }

    pub fn vote(&self) -> &Vote {
        &self.vote
    }

    pub fn is_rebroadcasted(&self) -> bool {
        self.is_rebroadcasted
    }

    pub fn serialized_size(extensions: BitArray<u16>) -> usize {
        let count = ConfirmReq::count(extensions);
        Vote::serialized_size(count as usize)
    }

    pub fn deserialize(
        stream: &mut impl Stream,
        extensions: BitArray<u16>,
        digest: u128,
    ) -> Option<Self> {
        let mut vote = Vote::null();
        vote.deserialize(stream).ok()?;

        let is_rebroadcasted = extensions[Self::REBROADCASTED_FLAG];
        let mut ack = if is_rebroadcasted {
            ConfirmAck::new_with_rebroadcasted_vote(vote)
        } else {
            ConfirmAck::new_with_own_vote(vote)
        };
        ack.digest = digest;

        Some(ack)
    }

    pub fn new_test_instance() -> Self {
        Self::new_with_own_vote(Vote::new_test_instance())
    }
}

impl Serialize for ConfirmAck {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        self.vote.serialize(writer);
    }
}

impl MessageVariant for ConfirmAck {
    fn header_extensions(&self, _payload_len: u16) -> BitArray<u16> {
        let mut extensions = BitArray::default();
        extensions |= ConfirmReq::count_bits(self.vote.hashes.len() as u8);
        extensions.set(Self::REBROADCASTED_FLAG, self.is_rebroadcasted);
        extensions
    }
}

impl PartialEq for ConfirmAck {
    fn eq(&self, other: &Self) -> bool {
        self.vote == other.vote
    }
}

impl Eq for ConfirmAck {}

impl Display for ConfirmAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.vote, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_deserializable, Message};
    use rsnano_core::{utils::MemoryStream, BlockHash, PrivateKey};

    #[test]
    fn serialize_v1() {
        let keys = PrivateKey::new();
        let hashes = vec![BlockHash::from(1)];
        let vote = Vote::new(&keys, 0, 0, hashes);
        let confirm = Message::ConfirmAck(ConfirmAck::new_with_own_vote(vote));

        assert_deserializable(&confirm);
    }

    #[test]
    fn serialize_v2() {
        let keys = PrivateKey::new();
        let mut hashes = Vec::new();
        for i in 0..ConfirmAck::HASHES_MAX {
            hashes.push(BlockHash::from(i as u64))
        }
        let vote = Vote::new(&keys, 0, 0, hashes);
        let confirm = Message::ConfirmAck(ConfirmAck::new_with_own_vote(vote));

        assert_deserializable(&confirm);
    }

    #[test]
    #[should_panic]
    fn panics_when_vote_contains_too_many_hashes() {
        let keys = PrivateKey::new();
        let hashes = vec![BlockHash::from(1); 256];
        let vote = Vote::new(&keys, 0, 0, hashes);
        Message::ConfirmAck(ConfirmAck::new_with_own_vote(vote));
    }

    #[test]
    fn rebroadcasted_vote() {
        let ack = ConfirmAck::new_with_rebroadcasted_vote(Vote::new_test_instance());
        assert_eq!(ack.is_rebroadcasted(), true);
    }

    #[test]
    fn extensions_without_rebroadcasted_flag() {
        let ack = ConfirmAck::new_with_own_vote(Vote::new_test_instance());
        let extensions = ack.header_extensions(0);
        assert_eq!(extensions[ConfirmAck::REBROADCASTED_FLAG], false);
    }

    #[test]
    fn extensions_with_rebroadcasted_flag() {
        let ack = ConfirmAck::new_with_rebroadcasted_vote(Vote::new_test_instance());
        let extensions = ack.header_extensions(0);
        assert_eq!(extensions[ConfirmAck::REBROADCASTED_FLAG], true);
    }

    #[test]
    fn deserialize_set_rebroadcasted_flag() {
        let mut stream = MemoryStream::new();
        let vote = Vote::new_test_instance();
        vote.serialize(&mut stream);

        let mut extensions = BitArray::<u16>::new(0);
        extensions.set(ConfirmAck::REBROADCASTED_FLAG, true);

        let ack = ConfirmAck::deserialize(&mut stream, extensions, 0).unwrap();

        assert_eq!(ack.is_rebroadcasted(), true);
    }

    #[test]
    fn deserialize_unset_rebroadcasted_flag() {
        let mut stream = MemoryStream::new();
        let vote = Vote::new_test_instance();
        vote.serialize(&mut stream);

        let mut extensions = BitArray::<u16>::new(0);
        extensions.set(ConfirmAck::REBROADCASTED_FLAG, false);

        let ack = ConfirmAck::deserialize(&mut stream, extensions, 0).unwrap();

        assert_eq!(ack.is_rebroadcasted(), false);
    }
}
