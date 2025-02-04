use crate::stats::{DetailType, Direction, StatType, Stats};
use anyhow::Error;
use rsnano_network::{Channel, ChannelDirection, NetworkError, NetworkObserver, TrafficType};
use std::{net::SocketAddrV6, sync::Arc};
use tracing::{debug, trace};

#[derive(Clone)]
pub struct NetworkStats(Arc<Stats>);

impl NetworkStats {
    pub fn new(stats: Arc<Stats>) -> Self {
        Self(stats)
    }
}

impl NetworkObserver for NetworkStats {
    fn send_succeeded(&self, buf_size: usize, traffic_type: TrafficType) {
        self.0.add_dir_aggregate(
            StatType::TrafficTcp,
            DetailType::All,
            Direction::Out,
            buf_size as u64,
        );
        self.0.add_dir(
            StatType::TrafficTcpType,
            traffic_type.into(),
            Direction::Out,
            buf_size as u64,
        );
    }

    fn send_failed(&self) {
        self.0
            .inc_dir(StatType::Tcp, DetailType::TcpWriteError, Direction::In);
    }

    fn channel_timed_out(&self, channel: &Channel) {
        self.0.inc_dir(
            StatType::Tcp,
            DetailType::TcpIoTimeoutDrop,
            if channel.direction() == ChannelDirection::Inbound {
                Direction::In
            } else {
                Direction::Out
            },
        );
        debug!(
            channel_id = %channel.channel_id(),
            remote_addr = ?channel.peer_addr(),
            mode = ?channel.mode(),
            direction = ?channel.direction(),
            "Closing channel due to timeout");
    }

    fn read_succeeded(&self, count: usize) {
        self.0.add_dir(
            StatType::TrafficTcp,
            DetailType::All,
            Direction::In,
            count as u64,
        );
    }

    fn read_failed(&self) {
        self.0
            .inc_dir(StatType::Tcp, DetailType::TcpReadError, Direction::In);
    }

    fn connection_attempt(&self, peer: &SocketAddrV6) {
        self.0.inc_dir(
            StatType::TcpListener,
            DetailType::ConnectInitiate,
            Direction::Out,
        );
        debug!(?peer, "Initiate outgoing connection");
    }

    fn accepted(&self, peer: &SocketAddrV6, direction: ChannelDirection) {
        if direction == ChannelDirection::Outbound {
            self.0.inc_dir(
                StatType::TcpListener,
                DetailType::ConnectSuccess,
                direction.into(),
            );
        } else {
            self.0.inc_dir(
                StatType::TcpListener,
                DetailType::AcceptSuccess,
                direction.into(),
            );
        }
        debug!(%peer, ?direction, "New channel added");
    }

    fn error(&self, error: NetworkError, peer: &SocketAddrV6, direction: ChannelDirection) {
        match direction {
            ChannelDirection::Inbound => {
                self.0.inc_dir(
                    StatType::TcpListener,
                    DetailType::AcceptRejected,
                    Direction::In,
                );
            }
            ChannelDirection::Outbound => {
                self.0.inc_dir(
                    StatType::TcpListener,
                    DetailType::ConnectRejected,
                    Direction::Out,
                );
            }
        }

        match error {
            NetworkError::MaxConnections => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::MaxAttempts,
                    direction.into(),
                );
                debug!(
                    %peer,
                    ?direction,
                    "Max connections reached, unable to make new connection",
                );
            }
            NetworkError::PeerExcluded => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::Excluded,
                    direction.into(),
                );
                debug!(
                    %peer,
                    ?direction,
                    "Peer excluded, unable to make new connection",
                );
            }
            NetworkError::MaxConnectionsPerSubnetwork => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::MaxPerSubnetwork,
                    direction.into(),
                );
                self.0.inc_dir(
                    StatType::Tcp,
                    DetailType::MaxPerSubnetwork,
                    direction.into(),
                );
                debug!(
                    %peer,
                    ?direction,
                    "Max connections per subnetwork reached, unable to open new connection",
                );
            }
            NetworkError::MaxConnectionsPerIp => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::MaxPerIp,
                    direction.into(),
                );
                self.0
                    .inc_dir(StatType::Tcp, DetailType::MaxPerIp, direction.into());
                debug!(
                    %peer,
                    ?direction,
                    "Max connections per IP reached, unable to open new connection");
            }
            NetworkError::InvalidIp => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::NotAPeer,
                    direction.into(),
                );
                debug!(
                    %peer,
                    ?direction,
                    "Invalid IP, unable to open new connection");
            }
            NetworkError::DuplicateConnection => {
                self.0.inc_dir(
                    StatType::TcpListenerRejected,
                    DetailType::Duplicate,
                    direction.into(),
                );
                trace!(
                    %peer,
                    ?direction,
                    "Already connected to that peer, unable to open new connection");
            }
            NetworkError::Cancelled => {}
        }
    }

    fn connect_error(&self, peer: SocketAddrV6, e: Error) {
        self.0.inc_dir(
            StatType::TcpListener,
            DetailType::ConnectError,
            Direction::Out,
        );
        debug!("Error connecting to: {} ({:?})", peer, e);
    }

    fn attempt_timeout(&self, peer: SocketAddrV6) {
        self.0
            .inc(StatType::TcpListener, DetailType::AttemptTimeout);
        debug!("Connection attempt timed out: {}", peer);
    }

    fn attempt_cancelled(&self, peer: SocketAddrV6) {
        debug!("Connection attempt cancelled: {}", peer,);
    }

    fn merge_peer(&self) {
        self.0.inc(StatType::Network, DetailType::MergePeer);
    }

    fn accept_failure(&self) {
        self.0.inc_dir(
            StatType::TcpListener,
            DetailType::AcceptFailure,
            Direction::In,
        );
    }
}

impl From<ChannelDirection> for Direction {
    fn from(value: ChannelDirection) -> Self {
        match value {
            ChannelDirection::Inbound => Direction::In,
            ChannelDirection::Outbound => Direction::Out,
        }
    }
}

impl From<TrafficType> for DetailType {
    fn from(value: TrafficType) -> Self {
        match value {
            TrafficType::Generic => DetailType::Generic,
            TrafficType::BootstrapServer => DetailType::BootstrapServer,
            TrafficType::BootstrapRequests => DetailType::BootstrapRequests,
            TrafficType::BlockBroadcast => DetailType::BlockBroadcast,
            TrafficType::BlockBroadcastInitial => DetailType::BlockBroadcastInitial,
            TrafficType::BlockBroadcastRpc => DetailType::BlockBroadcastRpc,
            TrafficType::ConfirmationRequests => DetailType::ConfirmationRequests,
            TrafficType::Keepalive => DetailType::Keepalive,
            TrafficType::Vote => DetailType::Vote,
            TrafficType::VoteRebroadcast => DetailType::VoteRebroadcast,
            TrafficType::RepCrawler => DetailType::RepCrawler,
            TrafficType::VoteReply => DetailType::VoteReply,
            TrafficType::Telemetry => DetailType::Telemetry,
        }
    }
}
