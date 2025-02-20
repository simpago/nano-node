use super::{PublicKey, RawKey, Signature};
use crate::{Account, Link, Root};
use anyhow::Context;
use ed25519_dalek::ed25519::signature::SignerMut;
use rsnano_nullable_random::NullableRng;

pub struct PrivateKeyFactory {
    rng: NullableRng,
}

impl PrivateKeyFactory {
    #[allow(dead_code)]
    fn new(rng: NullableRng) -> Self {
        Self { rng }
    }

    pub fn new_null() -> Self {
        Self {
            rng: NullableRng::new_null(),
        }
    }

    pub fn new_null_with(prv: RawKey) -> Self {
        Self {
            rng: NullableRng::new_null_bytes(prv.as_bytes()),
        }
    }

    pub fn create_key(&mut self) -> PrivateKey {
        let private_key = ed25519_dalek::SigningKey::generate(&mut self.rng);
        PrivateKey { private_key }
    }
}

impl Default for PrivateKeyFactory {
    fn default() -> Self {
        Self {
            rng: NullableRng::rng(),
        }
    }
}

#[derive(Clone)]
pub struct PrivateKey {
    private_key: ed25519_dalek::SigningKey,
}

impl Default for PrivateKey {
    fn default() -> Self {
        PrivateKeyFactory::default().create_key()
    }
}

impl PrivateKey {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn zero() -> Self {
        Self::from_bytes(&[0u8; 32])
    }

    pub fn is_zero(&self) -> bool {
        self.private_key.to_bytes() == [0; 32]
    }

    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(bytes);
        Self {
            private_key: signing_key,
        }
    }

    pub fn from_hex_str(s: impl AsRef<str>) -> anyhow::Result<Self> {
        let input = s.as_ref();
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(input, &mut bytes)
            .with_context(|| format!("input string: '{}'", input))?;
        Ok(Self::from_bytes(&bytes))
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        let mut signing_key = self.private_key.clone();
        let signature = signing_key.sign(data);
        Signature::from_bytes(signature.to_bytes())
    }

    pub fn account(&self) -> Account {
        Account::from_bytes(self.private_key.verifying_key().to_bytes())
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey::from_bytes(self.private_key.verifying_key().to_bytes())
    }

    pub fn raw_key(&self) -> RawKey {
        RawKey::from_bytes(self.private_key.to_bytes())
    }
}

impl From<u64> for PrivateKey {
    fn from(value: u64) -> Self {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_be_bytes());
        Self::from_bytes(&bytes)
    }
}

impl From<RawKey> for PrivateKey {
    fn from(value: RawKey) -> Self {
        Self::from_bytes(value.as_bytes())
    }
}

impl From<&PrivateKey> for Root {
    fn from(value: &PrivateKey) -> Self {
        value.public_key().as_account().into()
    }
}

impl From<&PrivateKey> for Link {
    fn from(value: &PrivateKey) -> Self {
        value.public_key().as_account().into()
    }
}

impl From<&PrivateKey> for Account {
    fn from(value: &PrivateKey) -> Self {
        value.public_key().as_account()
    }
}

impl From<&PrivateKey> for PublicKey {
    fn from(value: &PrivateKey) -> Self {
        value.public_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockHash;
    use ed25519_dalek::ed25519::signature::SignerMut;

    #[test]
    fn ed25519_signing() -> anyhow::Result<()> {
        let secret_key = ed25519_dalek::SecretKey::from([0u8; 32]);
        let message = [0u8; 32];
        let mut signing_key = ed25519_dalek::SigningKey::from(&secret_key);
        let public_key = ed25519_dalek::VerifyingKey::from(&signing_key);
        let signature = signing_key.sign(&message);
        public_key.verify_strict(&message, &signature).unwrap();

        let mut sig_bytes = signature.to_bytes();
        sig_bytes[32] ^= 0x1;
        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        assert!(public_key.verify_strict(&message, &signature).is_err());

        Ok(())
    }

    #[test]
    fn sign_message_test() -> anyhow::Result<()> {
        let prv_key = PrivateKey::new();
        let data = [0u8; 32];
        let signature = prv_key.sign(&data);
        prv_key.public_key().verify(&data, &signature)?;
        Ok(())
    }

    #[test]
    fn signing_same_message_twice_produces_equal_signatures() {
        // the C++ implementation adds random bytes and a padding when signing for extra security and for making side channel attacks more difficult.
        // Currently the Rust impl does not do that.
        // In C++ signing the same message twice will produce different signatures. In Rust we get the same signature.
        let prv_key = PrivateKey::new();
        let data = [1, 2, 3];
        let signature_a = prv_key.sign(&data);
        let signature_b = prv_key.sign(&data);
        assert_eq!(signature_a, signature_b);
    }

    // This block signature caused issues during live bootstrap. This was fixed by enabling the
    // feature "legacy-compatibility" for the crate ed25519-dalek-blake2b
    #[test]
    fn regression_validate_weird_signature() {
        let public_key = PublicKey::decode_hex(
            "49FEC0594D6E7F7040312E400F5F5285CB51FAF5DD8EB10CADBB02915058CCF7",
        )
        .unwrap();

        let hash = BlockHash::decode_hex(
            "E03D646E37DAE61E4D21281054418EF733CCFB9943B424B36B203ED063340A88",
        )
        .unwrap();

        let signature = Signature::decode_hex("3C14AF3E82BFC7DFD04EDF1639CDBF3580C02450CED478F269A4169A941617097D73A77721B62847558659371DBC3F6830724A7A55117750E5743562D1CF671E").unwrap();

        public_key.verify(hash.as_bytes(), &signature).unwrap();
    }

    // This block signature caused issues during live bootstrap. This was fixed by using verify() instead of verify_strict()
    #[test]
    fn regression_validate_weird_signature2() {
        let public_key = PublicKey::from(
            Account::decode_account(
                "nano_11a11111111111111111111111111111111111111111111111116iq5p4i8",
            )
            .unwrap(),
        );

        let hash = BlockHash::decode_hex(
            "150AFD70BD1E9845715F91D7CD7D5EE2683668199F19B4DF533FC7802CE07CA2",
        )
        .unwrap();

        let signature = Signature::decode_hex("1A8CFB63796525E47EBAF0B8696D95E2B893CBCC13454CB34530A59A3725C1A9FEA02A1F072BADE964BE5378CFA5AD50E743F167987444B1C9E3D7B3E6009F07").unwrap();

        public_key.verify(hash.as_bytes(), &signature).unwrap();
    }

    mod key_pair_factory {
        use super::*;

        #[test]
        fn create_key_pair() {
            let random_data = [
                0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44, 0x11, 0x22,
                0x33, 0x44, 0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44,
                0x11, 0x22, 0x33, 0x44,
            ];
            let rng = NullableRng::new_null_bytes(&random_data);
            let mut key_pair_factory = PrivateKeyFactory::new(rng);

            let key_pair = key_pair_factory.create_key();

            assert_eq!(key_pair.raw_key().as_bytes(), &random_data);
        }

        #[test]
        fn nullable() {
            let mut key_pair_factory = PrivateKeyFactory::new_null();
            let key_pair = key_pair_factory.create_key();
            assert_ne!(key_pair.raw_key(), RawKey::zero());
        }

        #[test]
        fn configured_response() {
            let expected = RawKey::from_bytes([3; 32]);
            let mut key_pair_factory = PrivateKeyFactory::new_null_with(expected);
            assert_eq!(key_pair_factory.create_key().raw_key(), expected);
        }
    }
}
