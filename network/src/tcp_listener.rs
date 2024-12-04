use crate::{ChannelDirection, ChannelMode, Network, NetworkObserver, ResponseServerSpawner};
use async_trait::async_trait;
use rsnano_nullable_tcp::TcpStream;
use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Server side portion of tcp sessions. Listens for new socket connections and spawns tcp_server objects when connected.
pub struct TcpListener {
    port: AtomicU16,
    network: Arc<Network>,
    network_observer: Arc<dyn NetworkObserver>,
    tokio: tokio::runtime::Handle,
    data: Mutex<TcpListenerData>,
    condition: Condvar,
    cancel_token: CancellationToken,
    response_server_spawner: Arc<dyn ResponseServerSpawner>,
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        debug_assert!(self.data.lock().unwrap().stopped);
    }
}

struct TcpListenerData {
    stopped: bool,
    local_addr: SocketAddrV6,
}

impl TcpListener {
    pub fn new(
        port: u16,
        network: Arc<Network>,
        network_observer: Arc<dyn NetworkObserver>,
        tokio: tokio::runtime::Handle,
        response_server_spawner: Arc<dyn ResponseServerSpawner>,
    ) -> Self {
        Self {
            port: AtomicU16::new(port),
            network,
            network_observer,
            data: Mutex::new(TcpListenerData {
                stopped: true,
                local_addr: SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0),
            }),
            tokio,
            condition: Condvar::new(),
            cancel_token: CancellationToken::new(),
            response_server_spawner,
        }
    }

    pub fn stop(&self) {
        self.data.lock().unwrap().stopped = true;
        self.cancel_token.cancel();
        self.condition.notify_all();
    }

    pub fn local_address(&self) -> SocketAddrV6 {
        let guard = self.data.lock().unwrap();
        if !guard.stopped {
            guard.local_addr
        } else {
            SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)
        }
    }
}

#[async_trait]
pub trait TcpListenerExt {
    fn start(&self);
    async fn run(&self, listener: tokio::net::TcpListener);
}

#[async_trait]
impl TcpListenerExt for Arc<TcpListener> {
    fn start(&self) {
        self.data.lock().unwrap().stopped = false;
        let self_l = Arc::clone(self);
        self.tokio.spawn(async move {
            let port = self_l.port.load(Ordering::SeqCst);
            let Ok(listener) = tokio::net::TcpListener::bind(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                port,
            ))
            .await
            else {
                error!("Error while binding for incoming connections on: {}", port);
                return;
            };

            let addr = listener
                .local_addr()
                .map(|a| match a {
                    SocketAddr::V6(v6) => v6,
                    _ => unreachable!(), // We only use V6 addresses
                })
                .unwrap_or(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0));
            debug!("Listening for incoming connections on: {}", addr);

            self_l
                .network
                .info
                .write()
                .unwrap()
                .set_listening_port(addr.port());

            self_l.data.lock().unwrap().local_addr =
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, addr.port(), 0, 0);

            self_l.run(listener).await
        });
    }

    async fn run(&self, listener: tokio::net::TcpListener) {
        let run_loop = async {
            loop {
                self.network.wait_for_available_inbound_slot().await;

                let Ok((stream, _)) = listener.accept().await else {
                    warn!("Could not accept incoming connection");
                    self.network_observer.accept_failure();
                    continue;
                };

                let tcp_stream = TcpStream::new(stream);
                match self.network.add(
                    tcp_stream,
                    ChannelDirection::Inbound,
                    ChannelMode::Undefined,
                ) {
                    Ok(channel) => {
                        self.response_server_spawner.spawn(channel);
                    }
                    Err(e) => {
                        warn!("Could not accept incoming connection: {:?}", e);
                    }
                };

                // Sleep for a while to prevent busy loop
                sleep(Duration::from_millis(10)).await;
            }
        };

        tokio::select! {
            _ = self.cancel_token.cancelled() => { },
            _ = run_loop => {}
        }
    }
}
