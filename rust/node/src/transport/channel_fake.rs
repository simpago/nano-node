use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use rsnano_core::Account;

use super::Channel;

pub struct FakeChannelData {
    last_bootstrap_attempt: u64,
    last_packet_received: u64,
    last_packet_sent: u64,
    node_id: Option<Account>,
}

pub struct ChannelFake {
    channel_id: usize,
    temporary: AtomicBool,
    channel_mutex: Mutex<FakeChannelData>,
}

impl ChannelFake {
    pub fn new(now: u64, channel_id: usize) -> Self {
        Self {
            channel_id,
            temporary: AtomicBool::new(false),
            channel_mutex: Mutex::new(FakeChannelData {
                last_bootstrap_attempt: 0,
                last_packet_received: now,
                last_packet_sent: now,
                node_id: None,
            }),
        }
    }
}

impl Channel for ChannelFake {
    fn is_temporary(&self) -> bool {
        self.temporary.load(Ordering::SeqCst)
    }

    fn set_temporary(&self, temporary: bool) {
        self.temporary.store(temporary, Ordering::SeqCst)
    }

    fn get_last_bootstrap_attempt(&self) -> u64 {
        self.channel_mutex.lock().unwrap().last_bootstrap_attempt
    }

    fn set_last_bootstrap_attempt(&self, instant: u64) {
        self.channel_mutex.lock().unwrap().last_bootstrap_attempt = instant;
    }

    fn get_last_packet_received(&self) -> u64 {
        self.channel_mutex.lock().unwrap().last_packet_received
    }

    fn set_last_packet_received(&self, instant: u64) {
        self.channel_mutex.lock().unwrap().last_packet_received = instant;
    }

    fn get_last_packet_sent(&self) -> u64 {
        self.channel_mutex.lock().unwrap().last_packet_sent
    }

    fn set_last_packet_sent(&self, instant: u64) {
        self.channel_mutex.lock().unwrap().last_packet_sent = instant;
    }

    fn get_node_id(&self) -> Option<Account> {
        self.channel_mutex.lock().unwrap().node_id
    }

    fn set_node_id(&self, id: Account) {
        self.channel_mutex.lock().unwrap().node_id = Some(id);
    }

    fn is_alive(&self) -> bool {
        true
    }

    fn channel_id(&self) -> usize {
        self.channel_id
    }
}
