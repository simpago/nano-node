use crate::{ChannelDirection, NetworkObserver, TcpNetworkAdapter};
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
    network_adapter: Arc<TcpNetworkAdapter>,
    network_observer: Arc<dyn NetworkObserver>,
    tokio: tokio::runtime::Handle,
    data: Mutex<TcpListenerData>,
    started: Condvar,
    cancel_token: CancellationToken,
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        debug_assert!(self.data.lock().unwrap().stopped);
    }
}

struct TcpListenerData {
    started: bool,
    stopped: bool,
    local_addr: SocketAddrV6,
}

impl TcpListener {
    pub fn new(
        port: u16,
        network_adapter: Arc<TcpNetworkAdapter>,
        network_observer: Arc<dyn NetworkObserver>,
        tokio: tokio::runtime::Handle,
    ) -> Self {
        Self {
            port: AtomicU16::new(port),
            network_adapter,
            network_observer,
            data: Mutex::new(TcpListenerData {
                started: false,
                stopped: true,
                local_addr: SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0),
            }),
            tokio,
            started: Condvar::new(),
            cancel_token: CancellationToken::new(),
        }
    }

    pub fn stop(&self) {
        self.data.lock().unwrap().stopped = true;
        self.cancel_token.cancel();
        self.started.notify_all();
    }

    pub fn local_address(&self) -> SocketAddrV6 {
        let guard = self.data.lock().unwrap();
        if !guard.stopped {
            guard.local_addr
        } else {
            SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)
        }
    }

    fn wait_until_listener_started(&self) {
        let guard = self.data.lock().unwrap();
        let timed_out = self
            .started
            .wait_timeout_while(guard, Duration::from_secs(5), |i| !i.started)
            .unwrap()
            .1
            .timed_out();
        if timed_out {
            panic!("Timeout while starting tcp listener");
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
                self_l.data.lock().unwrap().started = true;
                self_l.started.notify_all();
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
            self_l.network_adapter.set_listening_port(addr.port());

            {
                let mut data = self_l.data.lock().unwrap();
                data.local_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, addr.port(), 0, 0);
                data.started = true;
            }
            self_l.started.notify_all();

            self_l.run(listener).await
        });

        self.wait_until_listener_started();
    }

    async fn run(&self, listener: tokio::net::TcpListener) {
        let run_loop = async {
            loop {
                self.network_adapter.wait_for_available_inbound_slot().await;

                let Ok((stream, _)) = listener.accept().await else {
                    warn!("Could not accept incoming connection");
                    self.network_observer.accept_failure();
                    continue;
                };

                let tcp_stream = TcpStream::new(stream);
                match self
                    .network_adapter
                    .add(tcp_stream, ChannelDirection::Inbound)
                {
                    Ok(()) => {}
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
