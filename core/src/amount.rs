use crate::utils::{BufferWriter, Deserialize, FixedSizeSerialize, Serialize, Stream};
use anyhow::Result;
use serde::de::{Unexpected, Visitor};
use std::{fmt::Debug, iter::Sum};

#[derive(Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Amount {
    raw: u128, // native endian!
}

impl Amount {
    pub const MAX: Amount = Amount::raw(u128::MAX);

    pub const fn raw(value: u128) -> Self {
        Self { raw: value }
    }

    /// 10^24 raw or 0.000001 nano
    pub const fn micronano(value: u128) -> Self {
        Self {
            raw: value * 10u128.pow(24),
        }
    }

    /// 10^27 raw or 0.001 nano
    pub const fn millinano(value: u128) -> Self {
        Self {
            raw: value * 10u128.pow(27),
        }
    }

    /// 10^30 raw
    pub const fn nano(value: u128) -> Self {
        Self {
            raw: value * 10u128.pow(30),
        }
    }

    pub fn zero() -> Self {
        Self::raw(0)
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::zero()
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self {
            raw: u128::from_be_bytes(bytes),
        }
    }

    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self {
            raw: u128::from_le_bytes(bytes),
        }
    }

    pub fn to_be_bytes(self) -> [u8; 16] {
        self.raw.to_be_bytes()
    }

    pub fn to_le_bytes(self) -> [u8; 16] {
        self.raw.to_le_bytes()
    }

    pub fn encode_hex(&self) -> String {
        format!("{:032X}", self.raw)
    }

    pub fn decode_hex(s: impl AsRef<str>) -> Result<Self> {
        let value = u128::from_str_radix(s.as_ref(), 16)?;
        Ok(Amount::raw(value))
    }

    pub fn decode_dec(s: impl AsRef<str>) -> Result<Self> {
        Ok(Self::raw(s.as_ref().parse::<u128>()?))
    }

    pub fn to_string_dec(self) -> String {
        self.raw.to_string()
    }

    pub fn number(&self) -> u128 {
        self.raw
    }

    pub fn format_balance(&self, precision: usize) -> String {
        let precision = std::cmp::min(precision, 30);
        let nano_ratio = Amount::nano(1).number();
        if self.raw == 0 || self.raw >= nano_ratio / num_traits::pow(10, precision) {
            let whole = self.raw / nano_ratio;
            let decimals = self.raw % nano_ratio;
            let mut buf = num_format::Buffer::default();
            buf.write_formatted(&whole, &num_format::Locale::en);
            let mut result = buf.to_string();
            if decimals != 0 && precision > 0 {
                result.push('.');
                let decimals_string = format!("{:030}", decimals);
                let trimmed = decimals_string.trim_end_matches('0');
                let decimals_count = std::cmp::min(
                    precision,
                    trimmed[..std::cmp::min(precision, trimmed.len())].len(),
                );
                result.push_str(&decimals_string[..decimals_count]);
            }
            result
        } else if precision == 0 {
            "< 1".to_owned()
        } else {
            format!("< 0.{:0width$}", 1, width = precision)
        }
    }

    pub fn wrapping_add(&self, other: Amount) -> Amount {
        self.raw.wrapping_add(other.raw).into()
    }

    pub fn wrapping_sub(&self, other: Amount) -> Amount {
        self.raw.wrapping_sub(other.raw).into()
    }
}

impl From<u128> for Amount {
    fn from(value: u128) -> Self {
        Amount::raw(value)
    }
}

impl Serialize for Amount {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        stream.write_bytes_safe(&self.raw.to_be_bytes());
    }
}

impl FixedSizeSerialize for Amount {
    fn serialized_size() -> usize {
        std::mem::size_of::<u128>()
    }
}

impl Deserialize for Amount {
    type Target = Self;
    fn deserialize(stream: &mut dyn Stream) -> Result<Self> {
        let mut buffer = [0u8; 16];
        let len = buffer.len();
        stream.read_bytes(&mut buffer, len)?;
        Ok(Amount::raw(u128::from_be_bytes(buffer)))
    }
}

impl Debug for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.raw, f)
    }
}

impl std::ops::AddAssign for Amount {
    fn add_assign(&mut self, rhs: Self) {
        self.raw += rhs.raw;
    }
}

impl std::ops::Add for Amount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Amount::raw(self.raw + rhs.raw)
    }
}

impl std::ops::Sub for Amount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Amount::raw(self.raw - rhs.raw)
    }
}

impl std::ops::Div<u128> for Amount {
    type Output = Self;

    fn div(self, rhs: u128) -> Self::Output {
        Amount::raw(self.number() / rhs)
    }
}

impl std::ops::Mul<u128> for Amount {
    type Output = Self;

    fn mul(self, rhs: u128) -> Self::Output {
        Amount::raw(self.number() * rhs)
    }
}

impl std::ops::SubAssign<u128> for Amount {
    fn sub_assign(&mut self, rhs: u128) {
        self.raw -= rhs;
    }
}

impl std::cmp::PartialOrd for Amount {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for Amount {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl Sum for Amount {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut sum = Amount::zero();
        for i in iter {
            sum += i;
        }
        sum
    }
}

impl serde::Serialize for Amount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string_dec())
    }
}

impl<'de> serde::Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = deserializer.deserialize_str(AmountVisitor {})?;
        Ok(value)
    }
}

struct AmountVisitor {}

impl<'de> Visitor<'de> for AmountVisitor {
    type Value = Amount;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an 128 bit amount in decimal")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let value = v.parse::<u128>().map_err(|_| {
            serde::de::Error::invalid_value(Unexpected::Str(v), &"a 128bit decimal string")
        })?;
        Ok(Amount::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_amount_in_nano() {
        assert_eq!(
            Amount::nano(1).to_string_dec(),
            "1000000000000000000000000000000"
        );
    }

    #[test]
    fn decode_dec_happy_path() {
        assert_eq!(Amount::decode_dec("0").unwrap(), Amount::zero());
        assert_eq!(Amount::decode_dec("01").unwrap(), Amount::raw(1));
        let amount = Amount::decode_dec("1230000000000000000000000000000").unwrap();
        assert_eq!(amount, Amount::raw(1230000000000000000000000000000));
        let amount = Amount::decode_dec("340282366920938463463374607431768211455").unwrap();
        assert_eq!(amount, Amount::MAX);
    }

    #[test]
    fn decode_dec_failures() {
        let err = Amount::decode_dec("-1").unwrap_err();
        assert_eq!(err.to_string(), "invalid digit found in string");
        assert_eq!(
            Amount::decode_dec("").unwrap_err().to_string(),
            "cannot parse integer from empty string"
        );
        let err = Amount::decode_dec("340282366920938463463374607431768211456").unwrap_err();
        assert_eq!(err.to_string(), "number too large to fit in target type");
    }

    #[test]
    fn format_balance() {
        assert_eq!("0", Amount::raw(0).format_balance(2));
        assert_eq!(
            "340,282,366",
            Amount::decode_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                .unwrap()
                .format_balance(0)
        );
        assert_eq!(
            "340,282,366.920938463463374607431768211455",
            Amount::decode_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                .unwrap()
                .format_balance(64)
        );
        assert_eq!(
            "340,282,366",
            Amount::decode_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE")
                .unwrap()
                .format_balance(0)
        );
        assert_eq!(
            "340,282,366.920938463463374607431768211454",
            Amount::decode_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE")
                .unwrap()
                .format_balance(64)
        );
        assert_eq!(
            "170,141,183",
            Amount::decode_hex("7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE")
                .unwrap()
                .format_balance(0)
        );
        assert_eq!(
            "170,141,183.460469231731687303715884105726",
            Amount::decode_hex("7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE")
                .unwrap()
                .format_balance(64)
        );
        assert_eq!(
            "1",
            Amount::decode_dec("1000000000000000000000000000000")
                .unwrap()
                .format_balance(2)
        );
        assert_eq!(
            "1.2",
            Amount::decode_dec("1200000000000000000000000000000")
                .unwrap()
                .format_balance(2)
        );
        assert_eq!(
            "1.23",
            Amount::decode_dec("1230000000000000000000000000000")
                .unwrap()
                .format_balance(2)
        );
        assert_eq!(
            "1.2",
            Amount::decode_dec("1230000000000000000000000000000")
                .unwrap()
                .format_balance(1)
        );
        assert_eq!(
            "1",
            Amount::decode_dec("1230000000000000000000000000000")
                .unwrap()
                .format_balance(0)
        );
        assert_eq!("< 0.01", Amount::micronano(10).format_balance(2));
        assert_eq!("< 0.1", Amount::micronano(10).format_balance(1));
        assert_eq!("< 1", Amount::micronano(10).format_balance(0));
        assert_eq!("< 0.01", Amount::micronano(9999).format_balance(2));
        assert_eq!("< 0.001", Amount::raw(1).format_balance(3));
        assert_eq!("0.01", Amount::micronano(10000).format_balance(2));
        assert_eq!("123,456,789", Amount::nano(123456789).format_balance(2));
        assert_eq!(
            "123,456,789.12",
            (Amount::nano(123456789) + Amount::millinano(123)).format_balance(2)
        );
    }

    #[test]
    fn serde_serialize() {
        let serialized = serde_json::to_string_pretty(&Amount::MAX).unwrap();
        assert_eq!(serialized, "\"340282366920938463463374607431768211455\"");
    }

    #[test]
    fn serde_deserialize() {
        let deserialized: Amount = serde_json::from_str("\"123\"").unwrap();
        assert_eq!(deserialized, Amount::raw(123));
    }

    #[test]
    fn implements_debug() {
        let formatted = format!("{:?}", Amount::raw(123));
        assert_eq!(formatted, "123");
    }

    #[test]
    fn from_le_bytes() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let account = Amount::from_le_bytes(bytes);
        assert_eq!(account.encode_hex(), "100F0E0D0C0B0A090807060504030201");
        let back = account.to_le_bytes();
        assert_eq!(back, bytes);
    }
}
