use super::MessageVariant;
use crate::Cookie;
use anyhow::Result;
use bitvec::prelude::BitArray;
use rand::Rng;
use rsnano_core::{
    utils::{BufferWriter, Deserialize, FixedSizeSerialize, MemoryStream, Serialize, Stream},
    write_hex_bytes, Account, BlockHash, NodeId, PrivateKey, PublicKey, Signature,
};
use serde::ser::SerializeStruct;
use std::fmt::{Display, Write};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeIdHandshakeQuery {
    pub cookie: [u8; 32],
}

impl serde::Serialize for NodeIdHandshakeQuery {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("NodeIdHandshakeQuery", 1)?;

        let mut short_cookie = String::with_capacity(32 * 2);
        for b in &self.cookie {
            write!(short_cookie, "{:02X}", b).unwrap();
        }
        state.serialize_field("cookie", &short_cookie)?;
        state.end()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize)]
pub struct NodeIdHandshakeResponse {
    pub node_id: NodeId,
    pub signature: Signature,
    pub v2: Option<V2Payload>,
}

impl NodeIdHandshakeResponse {
    pub fn new_v1(cookie: &Cookie, node_id: &PrivateKey) -> Self {
        let mut response = Self {
            node_id: node_id.public_key().into(),
            signature: Signature::default(),
            v2: None,
        };
        response.sign(cookie, node_id);
        response
    }

    pub fn new_v2(cookie: &Cookie, node_id: &PrivateKey, genesis: BlockHash) -> Self {
        let mut salt = [0; 32];
        rand::rng().fill(&mut salt);

        let mut response = Self {
            node_id: node_id.public_key().into(),
            signature: Signature::default(),
            v2: Some(V2Payload { salt, genesis }),
        };
        response.sign(cookie, node_id);
        response
    }

    pub fn sign(&mut self, cookie: &Cookie, key: &PrivateKey) {
        debug_assert!(NodeId::from(key.public_key()) == self.node_id);
        let data = self.data_to_sign(cookie);
        self.signature = key.sign(&data);
        debug_assert!(self.validate(cookie).is_ok());
    }

    pub fn validate(&self, cookie: &Cookie) -> anyhow::Result<()> {
        let data = self.data_to_sign(cookie);
        let pub_key: PublicKey = self.node_id.into();
        pub_key.verify(&data, &self.signature)
    }

    fn data_to_sign(&self, cookie: &Cookie) -> Vec<u8> {
        let mut stream = MemoryStream::new();
        match &self.v2 {
            Some(v2) => {
                stream.write_bytes_safe(cookie);
                stream.write_bytes_safe(&v2.salt);
                v2.genesis.serialize(&mut stream);
            }
            None => stream.write_bytes_safe(cookie),
        }
        stream.to_vec()
    }

    pub fn deserialize(stream: &mut dyn Stream, extensions: BitArray<u16>) -> Result<Self> {
        if NodeIdHandshake::has_v2_flag(extensions) {
            let node_id = NodeId::deserialize(stream)?;
            let mut salt = [0u8; 32];
            stream.read_bytes(&mut salt, 32)?;
            let genesis = BlockHash::deserialize(stream)?;
            let signature = Signature::deserialize(stream)?;
            Ok(Self {
                node_id,
                signature,
                v2: Some(V2Payload { salt, genesis }),
            })
        } else {
            let node_id = NodeId::deserialize(stream)?;
            let signature = Signature::deserialize(stream)?;
            Ok(Self {
                node_id,
                signature,
                v2: None,
            })
        }
    }

    pub fn serialized_size(extensions: BitArray<u16>) -> usize {
        if NodeIdHandshake::has_v2_flag(extensions) {
            Account::serialized_size()
                + 32 // salt
                + BlockHash::serialized_size()
                + Signature::serialized_size()
        } else {
            Account::serialized_size() + Signature::serialized_size()
        }
    }
}

impl Serialize for NodeIdHandshakeResponse {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        match &self.v2 {
            Some(v2) => {
                self.node_id.serialize(stream);
                stream.write_bytes_safe(&v2.salt);
                v2.genesis.serialize(stream);
                self.signature.serialize(stream);
            }
            None => {
                self.node_id.serialize(stream);
                self.signature.serialize(stream);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct V2Payload {
    pub salt: [u8; 32],
    pub genesis: BlockHash,
}

impl serde::Serialize for V2Payload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("V2Payload", 2)?;

        let mut short_salt = String::with_capacity(32 * 2);
        for b in &self.salt {
            write!(short_salt, "{:02X}", b).unwrap();
        }
        state.serialize_field("salt", &short_salt)?;
        state.serialize_field("genesis", &self.genesis)?;
        state.end()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize)]
pub struct NodeIdHandshake {
    pub query: Option<NodeIdHandshakeQuery>,
    pub response: Option<NodeIdHandshakeResponse>,
    pub is_v2: bool,
}

impl NodeIdHandshake {
    pub const QUERY_FLAG: usize = 0;
    pub const RESPONSE_FLAG: usize = 1;
    pub const V2_FLAG: usize = 2;

    pub fn is_query(extensions: BitArray<u16>) -> bool {
        extensions[NodeIdHandshake::QUERY_FLAG]
    }

    pub fn is_response(extensions: BitArray<u16>) -> bool {
        extensions[NodeIdHandshake::RESPONSE_FLAG]
    }

    pub fn has_v2_flag(extensions: BitArray<u16>) -> bool {
        extensions[NodeIdHandshake::V2_FLAG]
    }

    pub fn serialized_size(extensions: BitArray<u16>) -> usize {
        let mut size = 0;
        if Self::is_query(extensions) {
            size += 32
        }
        if Self::is_response(extensions) {
            size += NodeIdHandshakeResponse::serialized_size(extensions);
        }
        size
    }

    pub fn deserialize(stream: &mut dyn Stream, extensions: BitArray<u16>) -> Option<Self> {
        let query = if NodeIdHandshake::is_query(extensions) {
            let mut cookie = [0u8; 32];
            stream.read_bytes(&mut cookie, 32).ok()?;
            Some(NodeIdHandshakeQuery { cookie })
        } else {
            None
        };
        let response = if NodeIdHandshake::is_response(extensions) {
            Some(NodeIdHandshakeResponse::deserialize(stream, extensions).ok()?)
        } else {
            None
        };
        Some(Self {
            query,
            response,
            is_v2: Self::has_v2_flag(extensions),
        })
    }

    pub fn new_test_query() -> Self {
        let query = NodeIdHandshakeQuery { cookie: [42; 32] };
        Self {
            query: Some(query),
            response: None,
            is_v2: true,
        }
    }

    pub fn new_test_response_v1() -> Self {
        let response = NodeIdHandshakeResponse {
            node_id: NodeId::from(1),
            signature: Signature::from_bytes([42; 64]),
            v2: None,
        };
        Self {
            query: None,
            response: Some(response),
            is_v2: false,
        }
    }

    pub fn new_test_response_v2() -> Self {
        let response = NodeIdHandshakeResponse {
            node_id: NodeId::from(1),
            signature: Signature::from_bytes([42; 64]),
            v2: Some(V2Payload {
                salt: [7; 32],
                genesis: BlockHash::from(3),
            }),
        };
        Self {
            query: None,
            response: Some(response),
            is_v2: true,
        }
    }
}

impl Serialize for NodeIdHandshake {
    fn serialize(&self, writer: &mut dyn BufferWriter) {
        if let Some(query) = &self.query {
            writer.write_bytes_safe(&query.cookie);
        }
        if let Some(response) = &self.response {
            response.serialize(writer);
        }
    }
}

impl MessageVariant for NodeIdHandshake {
    fn header_extensions(&self, _payload_len: u16) -> BitArray<u16> {
        let mut extensions = BitArray::default();
        extensions.set(NodeIdHandshake::QUERY_FLAG, self.query.is_some());
        extensions.set(NodeIdHandshake::RESPONSE_FLAG, self.response.is_some());
        extensions.set(Self::V2_FLAG, self.is_v2);
        extensions
    }
}

impl Display for NodeIdHandshake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(query) = &self.query {
            write!(f, "\ncookie=")?;
            write_hex_bytes(&query.cookie, f)?;
        }

        if let Some(response) = &self.response {
            write!(
                f,
                "\nresp_node_id={}\nresp_sig={}",
                response.node_id,
                response.signature.encode_hex()
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_deserializable, Message};

    #[test]
    fn serialize_query() {
        let message = Message::NodeIdHandshake(NodeIdHandshake::new_test_query());
        assert_deserializable(&message);
    }

    #[test]
    fn serialize_response_v1() {
        let message = Message::NodeIdHandshake(NodeIdHandshake::new_test_response_v1());
        assert_deserializable(&message);
    }

    #[test]
    fn serialize_response_v2() {
        let message = Message::NodeIdHandshake(NodeIdHandshake::new_test_response_v2());
        assert_deserializable(&message);
    }

    #[test]
    fn valid_v1_signature() {
        let key = PrivateKey::new();
        let mut response = NodeIdHandshakeResponse {
            node_id: key.public_key().into(),
            signature: Signature::default(),
            v2: None,
        };
        let cookie = [42; 32];

        response.sign(&cookie, &key);

        assert_ne!(response.signature, Signature::default());
        assert!(response.validate(&cookie).is_ok());

        // invalid cookie
        assert!(response.validate(&[1; 32]).is_err());

        // invalid node_id
        response.node_id = NodeId::from(1);
        assert!(response.validate(&cookie).is_err());
    }

    #[test]
    fn valid_v2_signature() {
        let key = PrivateKey::new();
        let mut response = NodeIdHandshakeResponse {
            node_id: key.public_key().into(),
            signature: Signature::default(),
            v2: Some(V2Payload {
                salt: [1; 32],
                genesis: BlockHash::from(3),
            }),
        };
        let cookie = [42; 32];

        response.sign(&cookie, &key);

        assert_ne!(response.signature, Signature::default());
        assert!(response.validate(&cookie).is_ok());

        // invalid cookie
        assert!(response.validate(&[1; 32]).is_err());

        // invalid node_id
        let mut copy = response.clone();
        copy.node_id = NodeId::from(1);
        assert!(copy.validate(&cookie).is_err());

        // invalid salt
        let mut copy = response.clone();
        copy.v2.as_mut().unwrap().salt = [100; 32];
        assert!(copy.validate(&cookie).is_err());

        // invalid genesis
        let mut copy = response.clone();
        copy.v2.as_mut().unwrap().genesis = BlockHash::from(123);
        assert!(copy.validate(&cookie).is_err());
    }
}
