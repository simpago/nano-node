use super::ChannelDirection;
use crate::{
    attempt_container::AttemptContainer,
    bandwidth_limiter::{BandwidthLimiter, BandwidthLimiterConfig},
    peer_exclusion::PeerExclusion,
    utils::{is_ipv4_mapped, map_address_to_subnetwork, reserved_address},
    Channel, ChannelId, ChannelMode, DataReceiver, DataReceiverFactory, NetworkObserver,
    NullDataReceiverFactory, NullNetworkObserver, TrafficType,
};
use rand::{seq::SliceRandom, thread_rng};
use rsnano_core::{utils::ContainerInfo, Networks, NodeId};
use rsnano_nullable_clock::Timestamp;
use std::{
    cmp::max,
    collections::HashMap,
    net::{Ipv6Addr, SocketAddrV6},
    sync::Arc,
    time::Duration,
};
use tracing::{debug, warn};

pub struct NetworkConfig {
    pub max_inbound_connections: usize,
    pub max_outbound_connections: usize,

    /** Maximum number of peers per IP. It is also the max number of connections per IP*/
    pub max_peers_per_ip: u16,

    /** Maximum number of peers per subnetwork */
    pub max_peers_per_subnetwork: u16,
    pub max_attempts_per_ip: usize,

    pub allow_local_peers: bool,
    pub min_protocol_version: u8,
    pub disable_max_peers_per_ip: bool,         // For testing only
    pub disable_max_peers_per_subnetwork: bool, // For testing only
    pub disable_network: bool,
    pub listening_port: u16,
    pub limiter: BandwidthLimiterConfig,
}

impl NetworkConfig {
    pub fn default_for(network: Networks) -> Self {
        let is_dev = network == Networks::NanoDevNetwork;
        Self {
            max_inbound_connections: if is_dev { 128 } else { 2048 },
            max_outbound_connections: if is_dev { 128 } else { 2048 },
            allow_local_peers: true,
            max_peers_per_ip: match network {
                Networks::NanoDevNetwork | Networks::NanoBetaNetwork => 256,
                _ => 4,
            },
            max_peers_per_subnetwork: match network {
                Networks::NanoDevNetwork | Networks::NanoBetaNetwork => 256,
                _ => 16,
            },
            max_attempts_per_ip: if is_dev { 128 } else { 1 },
            min_protocol_version: 0x14, //TODO don't hard code
            disable_max_peers_per_ip: false,
            disable_max_peers_per_subnetwork: false,
            disable_network: false,
            listening_port: match network {
                Networks::NanoDevNetwork => 44000,
                Networks::NanoBetaNetwork => 54000,
                Networks::NanoTestNetwork => 17076,
                _ => 7075,
            },
            limiter: BandwidthLimiterConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NetworkError {
    MaxConnections,
    MaxConnectionsPerSubnetwork,
    MaxConnectionsPerIp,
    /// Peer is excluded due to bad behavior
    PeerExcluded,
    InvalidIp,
    /// We are already connected to that peer and we tried to connect a second time
    DuplicateConnection,
    Cancelled,
}

pub struct Network {
    next_channel_id: usize,
    channels: HashMap<ChannelId, Arc<Channel>>,
    stopped: bool,
    new_realtime_channel_observers: Vec<Arc<dyn Fn(Arc<Channel>) + Send + Sync>>,
    attempts: AttemptContainer,
    network_config: NetworkConfig,
    excluded_peers: PeerExclusion,
    bandwidth_limiter: Arc<BandwidthLimiter>,
    observer: Arc<dyn NetworkObserver>,
    data_receiver_factory: Box<dyn DataReceiverFactory + Send + Sync>,
}

impl Network {
    pub fn new(network_config: NetworkConfig) -> Self {
        Self {
            next_channel_id: 1,
            channels: HashMap::new(),
            stopped: false,
            new_realtime_channel_observers: Vec::new(),
            attempts: Default::default(),
            excluded_peers: PeerExclusion::new(),
            bandwidth_limiter: Arc::new(BandwidthLimiter::new(network_config.limiter.clone())),
            observer: Arc::new(NullNetworkObserver::new()),
            network_config,
            data_receiver_factory: Box::new(NullDataReceiverFactory::new()),
        }
    }

    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self::new(NetworkConfig::default_for(Networks::NanoDevNetwork))
    }

    pub fn set_observer(&mut self, observer: Arc<dyn NetworkObserver>) {
        self.observer = observer;
    }

    pub fn set_data_receiver_factory(
        &mut self,
        factory: Box<dyn DataReceiverFactory + Send + Sync>,
    ) {
        self.data_receiver_factory = factory;
    }

    pub fn on_new_realtime_channel(&mut self, callback: Arc<dyn Fn(Arc<Channel>) + Send + Sync>) {
        self.new_realtime_channel_observers.push(callback);
    }

    pub fn new_realtime_channel_observers(&self) -> Vec<Arc<dyn Fn(Arc<Channel>) + Send + Sync>> {
        self.new_realtime_channel_observers.clone()
    }

    pub fn is_inbound_slot_available(&self) -> bool {
        self.count_by_direction(ChannelDirection::Inbound)
            < self.network_config.max_inbound_connections
    }

    /// Perma bans are used for prohibiting a node to connect to itself.
    pub fn perma_ban(&mut self, peer_addr: SocketAddrV6) {
        self.excluded_peers.perma_ban(peer_addr);
    }

    pub fn is_excluded(&mut self, peer_addr: &SocketAddrV6, now: Timestamp) -> bool {
        self.excluded_peers.is_excluded(peer_addr, now)
    }

    pub fn add_outbound_attempt(
        &mut self,
        peer: SocketAddrV6,
        now: Timestamp,
    ) -> Result<(), NetworkError> {
        self.attempts.insert(peer, ChannelDirection::Outbound, now);

        if let Err(e) = self.validate_new_connection(&peer, ChannelDirection::Outbound, now) {
            self.remove_attempt(&peer);
            self.observer.error(e, &peer, ChannelDirection::Outbound);
            return Err(e);
        }

        self.observer.connection_attempt(&peer);
        self.observer.merge_peer();

        Ok(())
    }

    pub fn remove_attempt(&mut self, remote: &SocketAddrV6) {
        self.attempts.remove(&remote);
    }

    pub fn add_test_channel(&mut self) -> Arc<Channel> {
        use rsnano_core::utils::{TEST_ENDPOINT_1, TEST_ENDPOINT_2};

        let channel = self
            .add_new_channel(
                TEST_ENDPOINT_1,
                TEST_ENDPOINT_2,
                ChannelDirection::Outbound,
                Timestamp::new_test_instance(),
            )
            .unwrap();
        channel.set_mode(ChannelMode::Realtime);
        channel
    }

    pub fn add(
        &mut self,
        local_addr: SocketAddrV6,
        peer_addr: SocketAddrV6,
        direction: ChannelDirection,
        now: Timestamp,
    ) -> Result<(Arc<Channel>, Box<dyn DataReceiver + Send>), NetworkError> {
        let channel = self.add_new_channel(local_addr, peer_addr, direction, now)?;

        let receiver = self
            .data_receiver_factory
            .create_receiver_for(channel.clone());

        Ok((channel, receiver))
    }

    fn add_new_channel(
        &mut self,
        local_addr: SocketAddrV6,
        peer_addr: SocketAddrV6,
        direction: ChannelDirection,
        now: Timestamp,
    ) -> Result<Arc<Channel>, NetworkError> {
        let result = self.validate_new_connection(&peer_addr, direction, now);
        if let Err(e) = result {
            self.observer.error(e, &peer_addr, direction);
        }
        result?;

        let channel_id = self.get_next_channel_id();
        let channel = Arc::new(Channel::new(
            channel_id,
            local_addr,
            peer_addr,
            direction,
            self.network_config.min_protocol_version,
            now,
            self.bandwidth_limiter.clone(),
            self.observer.clone(),
        ));
        self.channels.insert(channel_id, channel.clone());
        self.observer.accepted(&peer_addr, direction);
        Ok(channel)
    }

    fn get_next_channel_id(&mut self) -> ChannelId {
        let id = self.next_channel_id.into();
        self.next_channel_id += 1;
        id
    }

    pub fn listening_port(&self) -> u16 {
        self.network_config.listening_port
    }

    pub fn set_listening_port(&mut self, port: u16) {
        self.network_config.listening_port = port
    }

    pub fn get(&self, channel_id: ChannelId) -> Option<&Arc<Channel>> {
        self.channels.get(&channel_id)
    }

    pub fn remove(&mut self, channel_id: ChannelId) {
        self.channels.remove(&channel_id);
    }

    pub fn set_node_id(&self, channel_id: ChannelId, node_id: NodeId) {
        if let Some(channel) = self.channels.get(&channel_id) {
            channel.set_node_id(node_id);
        }
    }

    pub fn find_node_id(&self, node_id: &NodeId) -> Option<&Arc<Channel>> {
        self.channels
            .values()
            .find(|c| c.node_id() == Some(*node_id) && c.is_alive())
    }

    pub fn find_realtime_channel_by_remote_addr(
        &self,
        endpoint: &SocketAddrV6,
    ) -> Option<&Arc<Channel>> {
        self.channels.values().find(|c| {
            c.mode() == ChannelMode::Realtime && c.is_alive() && c.peer_addr() == *endpoint
        })
    }

    pub fn find_realtime_channel_by_peering_addr(
        &self,
        peering_addr: &SocketAddrV6,
    ) -> Option<&Arc<Channel>> {
        self.channels.values().find(|c| {
            c.mode() == ChannelMode::Realtime
                && c.is_alive()
                && c.peering_addr() == Some(*peering_addr)
        })
    }

    pub fn random_fanout(&self, scale: f32) -> Vec<Arc<Channel>> {
        let mut channels = self.shuffled_channels();
        channels.truncate(self.fanout(scale));
        channels
    }

    pub fn shuffled_channels(&self) -> Vec<Arc<Channel>> {
        let mut channels: Vec<_> = self.channels().cloned().collect();
        let mut rng = thread_rng();
        channels.shuffle(&mut rng);
        channels
    }

    pub fn sorted_channels(&self) -> Vec<Arc<Channel>> {
        let mut result: Vec<_> = self.channels().cloned().collect();
        result.sort_by_key(|i| i.peer_addr());
        result
    }

    pub fn channels(&self) -> impl Iterator<Item = &Arc<Channel>> {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.mode() == ChannelMode::Realtime)
    }

    pub fn not_a_peer(&self, endpoint: &SocketAddrV6, allow_local_peers: bool) -> bool {
        endpoint.ip().is_unspecified()
            || reserved_address(endpoint, allow_local_peers)
            || endpoint == &SocketAddrV6::new(Ipv6Addr::LOCALHOST, self.listening_port(), 0, 0)
    }

    /// Returns channel IDs of removed channels
    pub fn purge(&mut self, now: Timestamp, cutoff_period: Duration) -> Vec<Arc<Channel>> {
        self.close_idle_channels(now, cutoff_period);

        // Check if any tcp channels belonging to old protocol versions which may still be alive due to async operations
        self.close_old_protocol_versions(self.network_config.min_protocol_version);

        // Remove channels with dead underlying sockets
        let purged_channels = self.remove_dead_channels();

        // Remove keepalive attempt tracking for attempts older than cutoff
        self.attempts.purge(now, cutoff_period);
        purged_channels
    }

    fn close_idle_channels(&mut self, now: Timestamp, cutoff_period: Duration) {
        for entry in self.channels.values() {
            if now - entry.last_activity() >= cutoff_period {
                debug!(remote_addr = ?entry.peer_addr(), channel_id = %entry.channel_id(), mode = ?entry.mode(), "Closing idle channel");
                entry.close();
            }
        }
    }

    fn close_old_protocol_versions(&mut self, min_version: u8) {
        for channel in self.channels.values() {
            if channel.protocol_version() < min_version {
                debug!(channel_id = %channel.channel_id(), peer_addr = ?channel.peer_addr(), version = channel.protocol_version(), min_version,
                    "Closing channel with old protocol version",
                );
                channel.close();
            }
        }
    }

    /// Removes dead channels and returns their channel ids
    fn remove_dead_channels(&mut self) -> Vec<Arc<Channel>> {
        let dead_channels: Vec<_> = self
            .channels
            .values()
            .filter(|c| !c.is_alive())
            .cloned()
            .collect();

        for channel in &dead_channels {
            debug!("Removing dead channel: {}", channel.peer_addr());
            self.channels.remove(&channel.channel_id());
        }

        dead_channels
    }

    pub fn should_drop(&self, channel_id: ChannelId, traffic_type: TrafficType) -> bool {
        self.channels
            .get(&channel_id)
            .map(|c| c.should_drop(traffic_type))
            .unwrap_or(true)
    }

    fn size_ln(&self) -> f32 {
        // Clamp size to domain of std::log
        let size = max(1, self.count_by_mode(ChannelMode::Realtime));
        (size as f32).ln()
    }

    /// Desired fanout for a given scale
    /// Simulating with sqrt_broadcast_simulate shows we only need to broadcast to sqrt(total_peers) random peers in order to successfully publish to everyone with high probability
    pub fn fanout(&self, scale: f32) -> usize {
        let fanout = 2.0_f32.max(self.size_ln());
        (scale * fanout).ceil() as usize
    }

    fn count_by_ip(&self, ip: &Ipv6Addr) -> usize {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.ipv4_address_or_ipv6_subnet() == *ip)
            .count()
    }

    fn count_by_subnet(&self, subnet: &Ipv6Addr) -> usize {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.subnetwork() == *subnet)
            .count()
    }

    pub fn count_by_direction(&self, direction: ChannelDirection) -> usize {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.direction() == direction)
            .count()
    }

    pub fn count_by_mode(&self, mode: ChannelMode) -> usize {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.mode() == mode)
            .count()
    }

    pub fn bootstrap_peer(&mut self, now: Timestamp) -> SocketAddrV6 {
        let mut peering_endpoint = None;
        let mut channel = None;
        for i in self.iter_by_last_bootstrap_attempt() {
            if i.mode() == ChannelMode::Realtime
                && i.protocol_version() >= self.network_config.min_protocol_version
            {
                if let Some(peering) = i.peering_addr() {
                    channel = Some(i);
                    peering_endpoint = Some(peering);
                    break;
                }
            }
        }

        match (channel, peering_endpoint) {
            (Some(c), Some(peering)) => {
                c.set_last_bootstrap_attempt(now);
                peering
            }
            _ => SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0),
        }
    }

    pub fn iter_by_last_bootstrap_attempt(&self) -> Vec<Arc<Channel>> {
        let mut channels: Vec<_> = self
            .channels
            .values()
            .filter(|c| c.is_alive())
            .cloned()
            .collect();
        channels.sort_by(|a, b| a.last_bootstrap_attempt().cmp(&b.last_bootstrap_attempt()));
        channels
    }

    pub fn find_channels_by_remote_addr(&self, remote_addr: &SocketAddrV6) -> Vec<Arc<Channel>> {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.peer_addr() == *remote_addr)
            .cloned()
            .collect()
    }

    pub fn find_channels_by_peering_addr(&self, peering_addr: &SocketAddrV6) -> Vec<Arc<Channel>> {
        self.channels
            .values()
            .filter(|c| c.is_alive() && c.peering_addr() == Some(*peering_addr))
            .cloned()
            .collect()
    }

    fn max_ip_connections(&self, endpoint: &SocketAddrV6) -> bool {
        if self.network_config.disable_max_peers_per_ip {
            return false;
        }
        let count =
            self.count_by_ip(&endpoint.ip()) + self.attempts.count_by_address(&endpoint.ip());
        count >= self.network_config.max_peers_per_ip as usize
    }

    fn max_subnetwork_connections(&self, peer: &SocketAddrV6) -> bool {
        if self.network_config.disable_max_peers_per_subnetwork {
            return false;
        }

        // If the address is IPv4 we don't check for a network limit, since its address space isn't big as IPv6/64.
        if is_ipv4_mapped(&peer.ip()) {
            return false;
        }

        let subnet = map_address_to_subnetwork(peer.ip());
        let subnet_count =
            self.count_by_subnet(&subnet) + self.attempts.count_by_subnetwork(&subnet);

        subnet_count >= self.network_config.max_peers_per_subnetwork as usize
    }

    pub fn validate_new_connection(
        &mut self,
        peer: &SocketAddrV6,
        direction: ChannelDirection,
        now: Timestamp,
    ) -> Result<(), NetworkError> {
        if self.network_config.disable_network {
            return Err(NetworkError::MaxConnections);
        }

        let count = self.count_by_direction(direction);
        if count >= self.max_connections(direction) {
            return Err(NetworkError::MaxConnections);
        }

        if self.excluded_peers.is_excluded(peer, now) {
            return Err(NetworkError::PeerExcluded);
        }

        if !self.network_config.disable_max_peers_per_ip {
            let count = self.count_by_ip(peer.ip());
            if count >= self.network_config.max_peers_per_ip as usize {
                return Err(NetworkError::MaxConnectionsPerIp);
            }
        }

        // Don't overload single IP
        if self.max_ip_connections(peer) {
            return Err(NetworkError::MaxConnectionsPerIp);
        }

        if self.max_subnetwork_connections(peer) {
            return Err(NetworkError::MaxConnectionsPerSubnetwork);
        }

        // Don't contact invalid IPs
        if self.not_a_peer(peer, self.network_config.allow_local_peers) {
            return Err(NetworkError::InvalidIp);
        }

        if direction == ChannelDirection::Outbound {
            // Don't connect to nodes that already sent us something
            if self.find_channels_by_remote_addr(peer).len() > 0 {
                return Err(NetworkError::DuplicateConnection);
            }
            if self.find_channels_by_peering_addr(peer).len() > 0 {
                return Err(NetworkError::DuplicateConnection);
            }
        }

        Ok(())
    }

    fn max_connections(&self, direction: ChannelDirection) -> usize {
        match direction {
            ChannelDirection::Inbound => self.network_config.max_inbound_connections,
            ChannelDirection::Outbound => self.network_config.max_outbound_connections,
        }
    }

    pub fn set_peering_addr(&self, channel_id: ChannelId, peering_addr: SocketAddrV6) {
        if let Some(channel) = self.channels.get(&channel_id) {
            channel.set_peering_addr(peering_addr);
        }
    }

    pub fn peer_misbehaved(&mut self, channel_id: ChannelId, now: Timestamp) {
        let Some(channel) = self.channels.get(&channel_id) else {
            return;
        };
        let channel = channel.clone();

        // Add to peer exclusion list

        self.excluded_peers
            .peer_misbehaved(&channel.peer_addr(), now);

        let peer_addr = channel.peer_addr();
        let mode = channel.mode();
        let direction = channel.direction();

        channel.close();
        warn!(?peer_addr, ?mode, ?direction, "Peer misbehaved!");
    }

    pub fn close(&mut self) {}

    pub fn stop(&mut self) -> bool {
        if self.stopped {
            false
        } else {
            for channel in self.channels.values() {
                channel.close();
            }
            self.channels.clear();
            self.stopped = true;
            true
        }
    }

    pub fn random_fill_realtime(&self, endpoints: &mut [SocketAddrV6]) {
        // Don't include channels with ephemeral remote ports
        let mut peers: Vec<_> = self
            .channels()
            .filter(|c| c.peering_addr().is_some())
            .cloned()
            .collect();

        let mut rng = thread_rng();
        peers.shuffle(&mut rng);
        peers.truncate(endpoints.len());

        let null_endpoint = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);

        for (i, target) in endpoints.iter_mut().enumerate() {
            let endpoint = if i < peers.len() {
                peers[i].peering_addr().unwrap_or(null_endpoint)
            } else {
                null_endpoint
            };
            *target = endpoint;
        }
    }

    pub fn upgrade_to_realtime_connection(
        &self,
        channel_id: ChannelId,
        node_id: NodeId,
    ) -> Option<(Arc<Channel>, Vec<Arc<dyn Fn(Arc<Channel>) + Send + Sync>>)> {
        if self.is_stopped() {
            return None;
        }

        let Some(channel) = self.channels.get(&channel_id) else {
            return None;
        };

        if let Some(other) = self.find_node_id(&node_id) {
            if other.ipv4_address_or_ipv6_subnet() == channel.ipv4_address_or_ipv6_subnet() {
                // We already have a connection to that node. We allow duplicate node ids, but
                // only if they come from different IP addresses
                return None;
            }
        }

        channel.set_node_id(node_id);
        channel.set_mode(ChannelMode::Realtime);

        let observers = self.new_realtime_channel_observers();
        let channel = channel.clone();
        Some((channel, observers))
    }

    pub fn idle_channels(&self, min_idle_time: Duration, now: Timestamp) -> Vec<Arc<Channel>> {
        let mut result = Vec::new();
        for channel in self.channels.values() {
            if channel.mode() == ChannelMode::Realtime
                && now - channel.last_activity() >= min_idle_time
            {
                result.push(channel.clone());
            }
        }

        result
    }

    pub fn channels_info(&self) -> ChannelsInfo {
        let mut info = ChannelsInfo::default();
        for channel in self.channels.values() {
            info.total += 1;
            match channel.mode() {
                ChannelMode::Realtime => info.realtime += 1,
                _ => {}
            }
            match channel.direction() {
                ChannelDirection::Inbound => info.inbound += 1,
                ChannelDirection::Outbound => info.outbound += 1,
            }
        }
        info
    }

    pub fn try_send_buffer(
        &self,
        channel_id: ChannelId,
        buffer: &[u8],
        traffic_type: TrafficType,
    ) -> bool {
        let channel = self.channels.get(&channel_id);
        if let Some(channel) = channel {
            channel.send(buffer, traffic_type)
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.channels.len()
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .leaf("channels", self.channels.len(), size_of::<Arc<Channel>>())
            .leaf(
                "attempts",
                self.attempts.len(),
                AttemptContainer::ELEMENT_SIZE,
            )
            .node("excluded_peers", self.excluded_peers.container_info())
            .node("bandwidth", self.bandwidth_limiter.container_info())
            .finish()
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Default)]
pub struct ChannelsInfo {
    pub total: usize,
    pub realtime: usize,
    pub inbound: usize,
    pub outbound: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::utils::{NULL_ENDPOINT, TEST_ENDPOINT_1, TEST_ENDPOINT_2, TEST_ENDPOINT_3};

    #[test]
    fn newly_added_channel_is_not_a_realtime_channel() {
        let mut network = Network::new_test_instance();
        network
            .add(
                TEST_ENDPOINT_1,
                TEST_ENDPOINT_2,
                ChannelDirection::Inbound,
                Timestamp::new_test_instance(),
            )
            .unwrap();
        assert_eq!(network.channels().count(), 0);
    }

    #[test]
    fn reserved_ip_is_not_a_peer() {
        let network = Network::new_test_instance();

        assert!(network.not_a_peer(
            &SocketAddrV6::new(Ipv6Addr::new(0xff00u16, 0, 0, 0, 0, 0, 0, 0), 1000, 0, 0),
            true
        ));
        assert!(network.not_a_peer(&SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 10000, 0, 0), true));
        assert!(network.not_a_peer(
            &SocketAddrV6::new(Ipv6Addr::LOCALHOST, network.listening_port(), 0, 0),
            false
        ));

        // Test with a valid IP address
        assert_eq!(
            network.not_a_peer(
                &SocketAddrV6::new(Ipv6Addr::from_bits(0x08080808), 10000, 0, 0),
                true
            ),
            false
        );
    }

    #[test]
    fn upgrade_channel_to_realtime_channel() {
        let mut network = Network::new_test_instance();
        let (channel, _receiver) = network
            .add(
                TEST_ENDPOINT_1,
                TEST_ENDPOINT_2,
                ChannelDirection::Inbound,
                Timestamp::new_test_instance(),
            )
            .unwrap();

        assert!(network
            .upgrade_to_realtime_connection(channel.channel_id(), NodeId::from(456))
            .is_some());
        assert_eq!(network.channels().count(), 1);
    }

    #[test]
    fn random_fill_peering_endpoints_empty() {
        let network = Network::new_test_instance();
        let mut endpoints = [NULL_ENDPOINT; 3];
        network.random_fill_realtime(&mut endpoints);
        assert_eq!(endpoints, [NULL_ENDPOINT; 3]);
    }

    #[test]
    fn random_fill_peering_endpoints_part() {
        let mut network = Network::new_test_instance();
        add_realtime_channel_with_peering_addr(&mut network, TEST_ENDPOINT_1);
        add_realtime_channel_with_peering_addr(&mut network, TEST_ENDPOINT_2);
        let mut endpoints = [NULL_ENDPOINT; 3];
        network.random_fill_realtime(&mut endpoints);
        assert!(endpoints.contains(&TEST_ENDPOINT_1));
        assert!(endpoints.contains(&TEST_ENDPOINT_2));
        assert_eq!(endpoints[2], NULL_ENDPOINT);
    }

    #[test]
    fn random_fill_peering_endpoints() {
        let mut network = Network::new_test_instance();
        add_realtime_channel_with_peering_addr(&mut network, TEST_ENDPOINT_1);
        add_realtime_channel_with_peering_addr(&mut network, TEST_ENDPOINT_2);
        add_realtime_channel_with_peering_addr(&mut network, TEST_ENDPOINT_3);
        let mut endpoints = [NULL_ENDPOINT; 3];
        network.random_fill_realtime(&mut endpoints);
        assert!(endpoints.contains(&TEST_ENDPOINT_1));
        assert!(endpoints.contains(&TEST_ENDPOINT_2));
        assert!(endpoints.contains(&TEST_ENDPOINT_3));
    }

    fn add_realtime_channel_with_peering_addr(network: &mut Network, peering_addr: SocketAddrV6) {
        let (channel, _receiver) = network
            .add(
                TEST_ENDPOINT_1,
                peering_addr,
                ChannelDirection::Inbound,
                Timestamp::new_test_instance(),
            )
            .unwrap();
        channel.set_peering_addr(peering_addr);
        network.upgrade_to_realtime_connection(
            channel.channel_id(),
            NodeId::from(peering_addr.ip().to_bits()),
        );
    }

    mod purging {
        use super::*;

        #[test]
        fn purge_empty() {
            let mut network = Network::new_test_instance();
            network.purge(Timestamp::new_test_instance(), Duration::from_secs(1));
            assert_eq!(network.len(), 0);
        }

        #[test]
        fn dont_purge_new_channel() {
            let mut network = Network::new_test_instance();
            let now = Timestamp::new_test_instance();
            network
                .add(
                    TEST_ENDPOINT_1,
                    TEST_ENDPOINT_2,
                    ChannelDirection::Outbound,
                    now,
                )
                .unwrap();
            network.purge(now, Duration::from_secs(1));
            assert_eq!(network.len(), 1);
        }

        #[test]
        fn purge_if_last_activitiy_is_above_timeout() {
            let mut network = Network::new_test_instance();
            let now = Timestamp::new_test_instance();
            let (channel, _) = network
                .add(
                    TEST_ENDPOINT_1,
                    TEST_ENDPOINT_2,
                    ChannelDirection::Outbound,
                    now,
                )
                .unwrap();
            channel.set_last_activity(now - Duration::from_secs(300));
            network.purge(now, Duration::from_secs(1));
            assert_eq!(network.len(), 0);
        }

        #[test]
        fn dont_purge_if_packet_sent_within_timeout() {
            let mut network = Network::new_test_instance();
            let now = Timestamp::new_test_instance();
            let (channel, _) = network
                .add(
                    TEST_ENDPOINT_1,
                    TEST_ENDPOINT_2,
                    ChannelDirection::Outbound,
                    now,
                )
                .unwrap();
            channel.set_last_activity(now);
            network.purge(now, Duration::from_secs(1));
            assert_eq!(network.len(), 1);
        }
    }
}
