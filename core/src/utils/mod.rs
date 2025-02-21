mod container_info;
mod fair_queue;
mod peer;
mod stream;

use chrono::{DateTime, TimeZone, Utc};
pub use container_info::*;
pub use fair_queue::*;
pub use peer::*;
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    sync::{Arc, Condvar, Mutex},
    thread::available_parallelism,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};
pub use stream::*;

pub trait Serialize {
    fn serialize(&self, stream: &mut dyn BufferWriter);
}

pub trait FixedSizeSerialize: Serialize {
    fn serialized_size() -> usize;
}

pub trait Deserialize {
    type Target;
    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self::Target>;
}

impl Serialize for u64 {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        stream.write_u64_be_safe(*self)
    }
}

impl FixedSizeSerialize for u64 {
    fn serialized_size() -> usize {
        std::mem::size_of::<u64>()
    }
}

impl Deserialize for u64 {
    type Target = Self;
    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<u64> {
        stream.read_u64_be()
    }
}

impl Serialize for [u8; 64] {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        stream.write_bytes_safe(self)
    }
}

impl FixedSizeSerialize for [u8; 64] {
    fn serialized_size() -> usize {
        64
    }
}

impl Deserialize for [u8; 64] {
    type Target = Self;

    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self::Target> {
        let mut buffer = [0; 64];
        stream.read_bytes(&mut buffer, 64)?;
        Ok(buffer)
    }
}

pub fn get_cpu_count() -> usize {
    // Try to read overridden value from environment variable
    let value = std::env::var("NANO_HARDWARE_CONCURRENCY")
        .unwrap_or_else(|_| "0".into())
        .parse::<usize>()
        .unwrap_or_default();

    if value > 0 {
        return value;
    }

    available_parallelism().unwrap().get()
}

pub type MemoryIntensiveInstrumentationCallback = extern "C" fn() -> bool;

pub static mut MEMORY_INTENSIVE_INSTRUMENTATION: Option<MemoryIntensiveInstrumentationCallback> =
    None;

extern "C" fn default_is_sanitizer_build_callback() -> bool {
    false
}
pub static mut IS_SANITIZER_BUILD: MemoryIntensiveInstrumentationCallback =
    default_is_sanitizer_build_callback;

pub fn memory_intensive_instrumentation() -> bool {
    match std::env::var("NANO_MEMORY_INTENSIVE") {
        Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "on"),
        Err(_) => unsafe {
            match MEMORY_INTENSIVE_INSTRUMENTATION {
                Some(f) => f(),
                None => false,
            }
        },
    }
}

pub fn is_sanitizer_build() -> bool {
    unsafe { IS_SANITIZER_BUILD() }
}

pub fn milliseconds_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn system_time_as_seconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

/// Elapsed seconds since UNIX_EPOCH
#[derive(PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Default)]
pub struct UnixTimestamp(u64);

impl UnixTimestamp {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn new(seconds_since_epoch: u64) -> Self {
        Self(seconds_since_epoch)
    }

    pub fn now() -> Self {
        Self(Self::seconds_since_unix_epoch())
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    fn seconds_since_unix_epoch() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    pub fn add(&self, seconds: u64) -> Self {
        Self(self.0 + seconds)
    }

    pub fn utc(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.0 as i64, 0)
            .latest()
            .unwrap_or_default()
    }
}

impl From<u64> for UnixTimestamp {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl TryFrom<SystemTime> for UnixTimestamp {
    type Error = SystemTimeError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        Ok(Self(value.duration_since(UNIX_EPOCH)?.as_secs()))
    }
}

impl std::fmt::Display for UnixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for UnixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

pub fn get_env_or_default<T>(variable_name: &str, default: T) -> T
where
    T: core::str::FromStr + Copy,
{
    std::env::var(variable_name)
        .map(|v| v.parse::<T>().unwrap_or(default))
        .unwrap_or(default)
}

pub fn get_env_or_default_string(variable_name: &str, default: impl Into<String>) -> String {
    std::env::var(variable_name).unwrap_or_else(|_| default.into())
}

pub fn get_env_bool(variable_name: impl AsRef<str>) -> Option<bool> {
    let variable_name = variable_name.as_ref();
    std::env::var(variable_name)
        .ok()
        .map(|val| match val.to_lowercase().as_ref() {
            "1" | "true" | "on" => true,
            "0" | "false" | "off" => false,
            _ => panic!("Invalid environment boolean value: {variable_name} = {val}"),
        })
}

pub fn parse_endpoint(s: &str) -> SocketAddrV6 {
    s.parse().unwrap()
}

pub const NULL_ENDPOINT: SocketAddrV6 = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);

pub const TEST_ENDPOINT_1: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 1), 1111, 0, 0);

pub const TEST_ENDPOINT_2: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 2), 2222, 0, 0);

pub const TEST_ENDPOINT_3: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 3), 3333, 0, 0);

pub fn new_test_timestamp() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_000_000)
}

/// contains (done, Option<result>)
#[derive(Clone)]
pub struct OneShotNotification<T>(Arc<(Mutex<(bool, Option<T>)>, Condvar)>);

impl<T> OneShotNotification<T> {
    pub fn new() -> Self {
        Self(Arc::new((Mutex::new((false, None)), Condvar::new())))
    }

    pub fn notify(&self, t: T) {
        *self.0 .0.lock().unwrap() = (true, Some(t));
        self.0 .1.notify_one();
    }

    pub fn cancel(&self) {
        *self.0 .0.lock().unwrap() = (true, None);
        self.0 .1.notify_one();
    }

    pub fn wait(&self) -> Option<T> {
        let guard = self.0 .0.lock().unwrap();
        self.0 .1.wait_while(guard, |i| !i.0).unwrap().1.take()
    }
}
