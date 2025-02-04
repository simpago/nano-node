use crate::stats::{DetailType, StatType};
use crate::{
    stats::Stats,
    utils::{CancellationToken, Runnable},
};
use rsnano_ledger::Ledger;
use rsnano_network::PeerConnector;
use std::{net::SocketAddrV6, sync::Arc, time::Duration};
use tracing::info;

// Tries to connect to peers that are stored in the peer cache
pub struct PeerCacheConnector {
    ledger: Arc<Ledger>,
    peer_connector: Arc<PeerConnector>,
    stats: Arc<Stats>,
    first_run: bool,
    ///Delay between each connection attempt. This throttles new connections.
    reach_out_delay: Duration,
}

impl PeerCacheConnector {
    pub fn new(
        ledger: Arc<Ledger>,
        peer_connector: Arc<PeerConnector>,
        stats: Arc<Stats>,
        reach_out_delay: Duration,
    ) -> Self {
        Self {
            ledger,
            peer_connector,
            stats,
            first_run: true,
            reach_out_delay,
        }
    }

    fn load_peers_from_cache(&self) -> Vec<SocketAddrV6> {
        let tx = self.ledger.read_txn();
        self.ledger
            .store
            .peer
            .iter(&tx)
            .map(|(peer, _)| peer)
            .collect()
    }
}

impl Runnable for PeerCacheConnector {
    fn run(&mut self, cancel_token: &CancellationToken) {
        self.stats
            .inc(StatType::Network, DetailType::LoopReachoutCached);
        let cached_peers = self.load_peers_from_cache();

        if self.first_run {
            info!("Adding cached initial peers: {}", cached_peers.len());
            self.first_run = false;
        }

        for peer in cached_peers {
            self.stats
                .inc(StatType::Network, DetailType::ReachoutCached);
            let _ = self.peer_connector.connect_to(peer);
            // Throttle reachout attempts
            if cancel_token.wait_for_cancellation(self.reach_out_delay) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Direction;
    use rsnano_core::utils::{parse_endpoint, TEST_ENDPOINT_1, TEST_ENDPOINT_2, TEST_ENDPOINT_3};
    use rsnano_output_tracker::OutputTrackerMt;
    use std::time::UNIX_EPOCH;
    use tracing_test::traced_test;

    const REACHOUT_DELAY: Duration = Duration::from_secs(3);

    #[tokio::test]
    async fn no_cached_peers() {
        let merged_peers = run_connector([]).await;
        assert_eq!(merged_peers, Vec::new());
    }

    #[tokio::test]
    async fn connect_to_cached_peers() {
        let peer1 = parse_endpoint("[::ffff:10.0.0.1]:1234");
        let peer2 = parse_endpoint("[::ffff:10.0.0.2]:1234");

        let merged_peers = run_connector([peer1, peer2]).await;

        assert_eq!(merged_peers, [peer1, peer2]);
    }

    #[tokio::test]
    #[traced_test]
    async fn log_initial_peers() {
        let peer1 = parse_endpoint("[::ffff:10.0.0.1]:1234");
        let peer2 = parse_endpoint("[::ffff:10.0.0.2]:1234");

        run_connector([peer1, peer2]).await;

        assert!(logs_contain("Adding cached initial peers: 2"));
    }

    #[tokio::test]
    #[traced_test]
    async fn log_initial_peers_only_once() {
        let (mut connector, _, _) = create_test_connector([]).await;

        let cancel = CancellationToken::new_null();
        connector.run(&cancel);
        connector.run(&cancel);

        logs_assert(|lines| {
            match lines
                .iter()
                .filter(|l| l.contains("Adding cached initial peers"))
                .count()
            {
                1 => Ok(()),
                c => Err(format!("Should only log once, but was {}", c)),
            }
        })
    }

    #[tokio::test]
    async fn wait_between_connection_attempts() {
        let (mut connector, _, _) =
            create_test_connector([TEST_ENDPOINT_1, TEST_ENDPOINT_2, TEST_ENDPOINT_3]).await;
        let cancel_token = CancellationToken::new_null();
        let wait_tracker = cancel_token.track_waits();

        connector.run(&cancel_token);

        assert_eq!(wait_tracker.output(), [REACHOUT_DELAY; 3]);
    }

    #[tokio::test]
    async fn cancel_during_connection_attempts() {
        let (mut connector, merge_tracker, _) =
            create_test_connector([TEST_ENDPOINT_1, TEST_ENDPOINT_2, TEST_ENDPOINT_3]).await;
        let cancel_token = CancellationToken::new_null_with_uncancelled_waits(1);
        let wait_tracker = cancel_token.track_waits();

        connector.run(&cancel_token);

        assert_eq!(merge_tracker.output(), [TEST_ENDPOINT_1, TEST_ENDPOINT_2]);
        assert_eq!(wait_tracker.output(), [REACHOUT_DELAY; 2]);
    }

    #[tokio::test]
    async fn inc_stats_when_run() {
        let (mut connector, _, stats) = create_test_connector([]).await;
        connector.run(&CancellationToken::new_null());
        assert_eq!(
            stats.count(
                StatType::Network,
                DetailType::LoopReachoutCached,
                Direction::In
            ),
            1
        )
    }

    #[tokio::test]
    async fn inc_stats_for_each_reachout() {
        let (mut connector, _, stats) =
            create_test_connector([TEST_ENDPOINT_1, TEST_ENDPOINT_2]).await;
        connector.run(&CancellationToken::new_null());
        assert_eq!(
            stats.count(StatType::Network, DetailType::ReachoutCached, Direction::In),
            2
        )
    }

    async fn run_connector(
        cached_peers: impl IntoIterator<Item = SocketAddrV6>,
    ) -> Vec<SocketAddrV6> {
        let (mut connector, merge_tracker, _) = create_test_connector(cached_peers).await;
        connector.run(&CancellationToken::new_null());
        merge_tracker.output()
    }

    async fn create_test_connector(
        cached_peers: impl IntoIterator<Item = SocketAddrV6>,
    ) -> (
        PeerCacheConnector,
        Arc<OutputTrackerMt<SocketAddrV6>>,
        Arc<Stats>,
    ) {
        let ledger = ledger_with_peers(cached_peers);
        let peer_connector = Arc::new(PeerConnector::new_null(tokio::runtime::Handle::current()));
        let merge_tracker = peer_connector.track_connections();
        let stats = Arc::new(Stats::default());
        let connector =
            PeerCacheConnector::new(ledger, peer_connector, stats.clone(), REACHOUT_DELAY);
        (connector, merge_tracker, stats)
    }

    fn ledger_with_peers(cached_peers: impl IntoIterator<Item = SocketAddrV6>) -> Arc<Ledger> {
        Arc::new(
            Ledger::new_null_builder()
                .peers(cached_peers.into_iter().map(|peer| (peer, UNIX_EPOCH)))
                .finish(),
        )
    }
}
