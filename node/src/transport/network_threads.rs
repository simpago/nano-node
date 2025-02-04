use super::{keepalive::KeepaliveMessageFactory, LatestKeepalives, MessageFlooder, SynCookies};
use crate::{
    config::{NetworkParams, NodeFlags},
    stats::{DetailType, StatType, Stats},
};
use rsnano_messages::{Keepalive, Message, NetworkFilter};
use rsnano_network::{DeadChannelCleanup, Network, PeerConnector, TrafficType};
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
    time::Duration,
};

pub(crate) struct NetworkThreads {
    cleanup_thread: Option<JoinHandle<()>>,
    keepalive_thread: Option<JoinHandle<()>>,
    reachout_thread: Option<JoinHandle<()>>,
    stopped: Arc<(Condvar, Mutex<bool>)>,
    network: Arc<RwLock<Network>>,
    peer_connector: Arc<PeerConnector>,
    flags: NodeFlags,
    network_params: NetworkParams,
    stats: Arc<Stats>,
    syn_cookies: Arc<SynCookies>,
    network_filter: Arc<NetworkFilter>,
    keepalive_factory: Arc<KeepaliveMessageFactory>,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    dead_channel_cleanup: Option<DeadChannelCleanup>,
    message_flooder: MessageFlooder,
    clock: Arc<SteadyClock>,
}

impl NetworkThreads {
    pub fn new(
        network: Arc<RwLock<Network>>,
        peer_connector: Arc<PeerConnector>,
        flags: NodeFlags,
        network_params: NetworkParams,
        stats: Arc<Stats>,
        syn_cookies: Arc<SynCookies>,
        network_filter: Arc<NetworkFilter>,
        keepalive_factory: Arc<KeepaliveMessageFactory>,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
        dead_channel_cleanup: DeadChannelCleanup,
        message_flooder: MessageFlooder,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            cleanup_thread: None,
            keepalive_thread: None,
            reachout_thread: None,
            stopped: Arc::new((Condvar::new(), Mutex::new(false))),
            network,
            peer_connector,
            flags,
            network_params,
            stats,
            syn_cookies,
            network_filter,
            keepalive_factory,
            latest_keepalives,
            dead_channel_cleanup: Some(dead_channel_cleanup),
            message_flooder,
            clock,
        }
    }

    pub fn start(&mut self) {
        let cleanup = CleanupLoop {
            stopped: self.stopped.clone(),
            network_params: self.network_params.clone(),
            flags: self.flags.clone(),
            syn_cookies: self.syn_cookies.clone(),
            network_filter: self.network_filter.clone(),
            dead_channel_cleanup: self.dead_channel_cleanup.take().unwrap(),
        };

        self.cleanup_thread = Some(
            std::thread::Builder::new()
                .name("Net cleanup".to_string())
                .spawn(move || cleanup.run())
                .unwrap(),
        );

        let mut keepalive = KeepaliveLoop {
            stopped: self.stopped.clone(),
            network: self.network.clone(),
            keepalive_period: self.network_params.network.keepalive_period,
            stats: Arc::clone(&self.stats),
            keepalive_factory: self.keepalive_factory.clone(),
            message_flooder: self.message_flooder.clone(),
            clock: self.clock.clone(),
        };

        self.keepalive_thread = Some(
            std::thread::Builder::new()
                .name("Net keepalive".to_string())
                .spawn(move || keepalive.run())
                .unwrap(),
        );

        if !self.network_params.network.merge_period.is_zero() {
            let reachout = ReachoutLoop {
                stopped: self.stopped.clone(),
                reachout_interval: self.network_params.network.merge_period,
                stats: self.stats.clone(),
                peer_connector: self.peer_connector.clone(),
                latest_keepalives: self.latest_keepalives.clone(),
            };

            self.reachout_thread = Some(
                std::thread::Builder::new()
                    .name("Net reachout".to_string())
                    .spawn(move || reachout.run())
                    .unwrap(),
            );
        }
    }
    pub fn stop(&mut self) {
        *self.stopped.1.lock().unwrap() = true;
        self.stopped.0.notify_all();
        self.network.write().unwrap().stop();
        if let Some(t) = self.keepalive_thread.take() {
            t.join().unwrap();
        }
        if let Some(t) = self.cleanup_thread.take() {
            t.join().unwrap();
        }
        if let Some(t) = self.reachout_thread.take() {
            t.join().unwrap();
        }
    }
}

impl Drop for NetworkThreads {
    fn drop(&mut self) {
        // All threads must be stopped before this destructor
        debug_assert!(self.cleanup_thread.is_none());
        debug_assert!(self.keepalive_thread.is_none());
    }
}

struct CleanupLoop {
    stopped: Arc<(Condvar, Mutex<bool>)>,
    network_params: NetworkParams,
    flags: NodeFlags,
    syn_cookies: Arc<SynCookies>,
    network_filter: Arc<NetworkFilter>,
    dead_channel_cleanup: DeadChannelCleanup,
}

impl CleanupLoop {
    fn run(&self) {
        let mut stopped = self.stopped.1.lock().unwrap();
        while !*stopped {
            let timeout = if self.network_params.network.is_dev_network() {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(5)
            };
            stopped = self.stopped.0.wait_timeout(stopped, timeout).unwrap().0;

            if *stopped {
                return;
            }
            drop(stopped);

            if !self.flags.disable_connection_cleanup {
                self.dead_channel_cleanup.clean_up();
            }

            self.syn_cookies
                .purge(self.network_params.network.sync_cookie_cutoff);

            self.network_filter.update(timeout.as_secs());

            stopped = self.stopped.1.lock().unwrap();
        }
    }
}

struct KeepaliveLoop {
    stopped: Arc<(Condvar, Mutex<bool>)>,
    stats: Arc<Stats>,
    network: Arc<RwLock<Network>>,
    keepalive_factory: Arc<KeepaliveMessageFactory>,
    message_flooder: MessageFlooder,
    clock: Arc<SteadyClock>,
    keepalive_period: Duration,
}

impl KeepaliveLoop {
    fn run(&mut self) {
        let mut stopped = self.stopped.1.lock().unwrap();
        while !*stopped {
            stopped = self
                .stopped
                .0
                .wait_timeout(stopped, self.keepalive_period)
                .unwrap()
                .0;

            if *stopped {
                return;
            }
            drop(stopped);

            self.stats.inc(StatType::Network, DetailType::LoopKeepalive);
            self.flood_keepalive(0.75);
            self.flood_keepalive_self(0.25);

            self.keepalive();

            stopped = self.stopped.1.lock().unwrap();
        }
    }

    fn keepalive(&mut self) {
        let (message, keepalive_list) = {
            let message = self.keepalive_factory.create_keepalive();

            let network = self.network.read().unwrap();
            let channels = network.idle_channels(self.keepalive_period, self.clock.now());
            (message, channels)
        };

        for channel in keepalive_list {
            self.message_flooder
                .try_send(&channel, &message, TrafficType::Keepalive);
        }
    }

    fn flood_keepalive(&mut self, scale: f32) {
        let mut keepalive = Keepalive::default();
        self.network
            .read()
            .unwrap()
            .random_fill_realtime(&mut keepalive.peers);
        self.message_flooder.flood(
            &Message::Keepalive(keepalive),
            TrafficType::Keepalive,
            scale,
        );
    }

    fn flood_keepalive_self(&mut self, scale: f32) {
        let keepalive = self.keepalive_factory.create_keepalive_self();
        self.message_flooder
            .flood(&keepalive, TrafficType::Keepalive, scale);
    }
}

struct ReachoutLoop {
    stopped: Arc<(Condvar, Mutex<bool>)>,
    reachout_interval: Duration,
    stats: Arc<Stats>,
    peer_connector: Arc<PeerConnector>,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
}

impl ReachoutLoop {
    fn run(&self) {
        let mut stopped = self.stopped.1.lock().unwrap();
        while !*stopped {
            stopped = self
                .stopped
                .0
                .wait_timeout(stopped, self.reachout_interval)
                .unwrap()
                .0;

            if *stopped {
                return;
            }
            drop(stopped);

            let keepalive = self.latest_keepalives.lock().unwrap().pop_random();
            if let Some(keepalive) = keepalive {
                for peer in keepalive.peers {
                    if peer.ip().is_unspecified() {
                        continue;
                    }
                    self.stats.inc(StatType::Network, DetailType::ReachoutLive);
                    self.peer_connector.connect_to(peer);

                    // Throttle reachout attempts
                    std::thread::sleep(self.reachout_interval);
                }
            }

            stopped = self.stopped.1.lock().unwrap();
        }
    }
}
