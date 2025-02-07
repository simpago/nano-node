use std::sync::{Arc, Condvar, Mutex};

use tokio::sync::oneshot;

/// Spawns the tokio runtime
pub struct TokioRunner {
    io_threads: usize,
    handle: Option<tokio::runtime::Handle>,
    tx_stop: Option<oneshot::Sender<()>>,
    stopped: Arc<(Condvar, Mutex<bool>)>,
}

impl TokioRunner {
    pub fn new(io_threads: usize) -> Self {
        Self {
            io_threads,
            handle: None,
            tx_stop: None,
            stopped: Arc::new((Condvar::new(), Mutex::new(false))),
        }
    }

    pub fn start(&mut self) {
        let (tx_stop, rx_stop) = oneshot::channel::<()>();
        self.tx_stop = Some(tx_stop);

        let handle: Arc<(Condvar, Mutex<Option<tokio::runtime::Handle>>)> =
            Arc::new((Condvar::new(), Mutex::new(None)));
        let handle2 = handle.clone();

        let io_threads = self.io_threads;
        let stopped = self.stopped.clone();
        std::thread::spawn(move || {
            let runtime = if io_threads > 1 {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(io_threads)
                    .enable_all()
                    .build()
                    .unwrap()
            } else {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            };

            *handle2.1.lock().unwrap() = Some(runtime.handle().clone());
            handle2.0.notify_all();

            runtime.block_on(async move {
                let _ = rx_stop.await;
            });
            *stopped.1.lock().unwrap() = true;
            stopped.0.notify_all();
        });

        let mut guard = handle.1.lock().unwrap();
        guard = handle.0.wait_while(guard, |i| i.is_none()).unwrap();
        self.handle = guard.take();
    }

    pub fn stop(&mut self) {
        if let Some(tx) = self.tx_stop.take() {
            let _ = tx.send(());
        }
        let guard = self.stopped.1.lock().unwrap();
        drop(self.stopped.0.wait_while(guard, |stopped| !*stopped));
    }

    pub fn handle(&self) -> &tokio::runtime::Handle {
        self.handle.as_ref().expect("no tokio handle present")
    }
}

impl Drop for TokioRunner {
    fn drop(&mut self) {
        self.stop();
    }
}
