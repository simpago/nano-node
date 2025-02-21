use super::{
    CpuWorkGenerator, StubWorkPool, WorkItem, WorkQueueCoordinator, WorkThread, WorkThresholds,
    WorkTicket, WORK_THRESHOLDS_STUB,
};
use crate::{utils::ContainerInfo, Root};
use std::{
    mem::size_of,
    sync::{Arc, Condvar, LazyLock, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

pub trait WorkPool: Send + Sync {
    fn generate_async(
        &self,
        root: Root,
        difficulty: u64,
        done: Option<Box<dyn FnOnce(Option<u64>) + Send>>,
    );

    fn generate_dev(&self, root: Root, difficulty: u64) -> Option<u64>;

    fn generate_dev2(&self, root: Root) -> Option<u64>;

    fn generate(&self, root: Root, difficulty: u64) -> Option<u64>;
}

pub struct WorkPoolImpl {
    threads: Vec<JoinHandle<()>>,
    work_queue: Arc<WorkQueueCoordinator>,
    work_thresholds: WorkThresholds,
    pow_rate_limiter: Duration,
}

impl WorkPoolImpl {
    pub fn new(
        work_thresholds: WorkThresholds,
        thread_count: usize,
        pow_rate_limiter: Duration,
    ) -> Self {
        let mut pool = Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds,
            pow_rate_limiter,
        };

        pool.spawn_threads(thread_count);
        pool
    }

    pub fn new_dev() -> Self {
        Self::new(WorkThresholds::publish_dev().clone(), 1, Duration::ZERO)
    }

    pub fn new_null(configured_work: u64) -> Self {
        let mut pool = Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds: WORK_THRESHOLDS_STUB.clone(),
            pow_rate_limiter: Duration::ZERO,
        };

        pool.threads
            .push(pool.spawn_stub_worker_thread(configured_work));
        pool
    }

    pub fn disabled() -> Self {
        Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds: WORK_THRESHOLDS_STUB.clone(),
            pow_rate_limiter: Duration::ZERO,
        }
    }

    fn spawn_threads(&mut self, thread_count: usize) {
        for _ in 0..thread_count {
            self.threads.push(self.spawn_cpu_worker_thread())
        }
    }

    fn spawn_cpu_worker_thread(&self) -> JoinHandle<()> {
        self.spawn_worker_thread(CpuWorkGenerator::new(self.pow_rate_limiter))
    }

    fn spawn_stub_worker_thread(&self, configured_work: u64) -> JoinHandle<()> {
        self.spawn_worker_thread(StubWorkGenerator(configured_work))
    }

    fn spawn_worker_thread<T>(&self, work_generator: T) -> JoinHandle<()>
    where
        T: WorkGenerator + Send + Sync + 'static,
    {
        let work_queue = Arc::clone(&self.work_queue);
        thread::Builder::new()
            .name("Work pool".to_string())
            .spawn(move || {
                WorkThread::new(work_generator, work_queue).work_loop();
            })
            .unwrap()
    }

    pub fn has_opencl(&self) -> bool {
        false
    }

    pub fn work_generation_enabled(&self) -> bool {
        !self.threads.is_empty()
    }

    pub fn cancel(&self, root: &Root) {
        self.work_queue.cancel(root);
    }

    pub fn stop(&self) {
        self.work_queue.stop();
    }

    pub fn size(&self) -> usize {
        self.work_queue.lock_work_queue().len()
    }

    pub fn pending_value_size() -> usize {
        size_of::<WorkItem>()
    }

    pub fn thread_count(&self) -> usize {
        self.threads.len()
    }

    pub fn threshold_base(&self) -> u64 {
        self.work_thresholds.threshold_base()
    }

    pub fn difficulty(&self, root: &Root, work: u64) -> u64 {
        self.work_thresholds.difficulty(root, work)
    }

    pub fn container_info(&self) -> ContainerInfo {
        [("pending", self.size(), Self::pending_value_size())].into()
    }
}

impl WorkPool for WorkPoolImpl {
    fn generate_async(
        &self,
        root: Root,
        difficulty: u64,
        done: Option<Box<dyn FnOnce(Option<u64>) + Send>>,
    ) {
        debug_assert!(!root.is_zero());
        if !self.threads.is_empty() {
            self.work_queue.enqueue(WorkItem {
                root,
                min_difficulty: difficulty,
                callback: done,
            });
        } else if let Some(callback) = done {
            callback(None);
        }
    }

    fn generate_dev(&self, root: Root, difficulty: u64) -> Option<u64> {
        self.generate(root, difficulty)
    }

    fn generate_dev2(&self, root: Root) -> Option<u64> {
        self.generate(root, self.work_thresholds.base)
    }

    fn generate(&self, root: Root, difficulty: u64) -> Option<u64> {
        if self.threads.is_empty() {
            return None;
        }

        let done_notifier = WorkDoneNotifier::new();
        let done_notifier_clone = done_notifier.clone();

        self.generate_async(
            root,
            difficulty,
            Some(Box::new(move |work| {
                done_notifier_clone.signal_done(work);
            })),
        );

        done_notifier.wait()
    }
}

#[derive(Default)]
struct WorkDoneState {
    work: Option<u64>,
    done: bool,
}

#[derive(Clone)]
struct WorkDoneNotifier {
    state: Arc<(Mutex<WorkDoneState>, Condvar)>,
}

impl WorkDoneNotifier {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(WorkDoneState::default()), Condvar::new())),
        }
    }

    fn signal_done(&self, work: Option<u64>) {
        {
            let mut lock = self.state.0.lock().unwrap();
            lock.work = work;
            lock.done = true;
        }
        self.state.1.notify_one();
    }

    fn wait(&self) -> Option<u64> {
        let mut lock = self.state.0.lock().unwrap();
        loop {
            if lock.done {
                return lock.work;
            }
            lock = self.state.1.wait(lock).unwrap();
        }
    }
}

impl Drop for WorkPoolImpl {
    fn drop(&mut self) {
        self.stop();
        for handle in self.threads.drain(..) {
            handle.join().unwrap();
        }
    }
}

pub(crate) trait WorkGenerator {
    fn create(&mut self, item: &Root, min_difficulty: u64, work_ticket: &WorkTicket)
        -> Option<u64>;
}

struct StubWorkGenerator(u64);

impl WorkGenerator for StubWorkGenerator {
    fn create(
        &mut self,
        _item: &Root,
        _min_difficulty: u64,
        _work_ticket: &WorkTicket,
    ) -> Option<u64> {
        Some(self.0)
    }
}

pub static STUB_WORK_POOL: LazyLock<StubWorkPool> =
    LazyLock::new(|| StubWorkPool::new(WorkThresholds::publish_dev().base));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Block, TestBlockBuilder};
    use std::sync::mpsc;

    pub static WORK_POOL: LazyLock<WorkPoolImpl> = LazyLock::new(|| {
        WorkPoolImpl::new(
            WorkThresholds::publish_dev().clone(),
            crate::utils::get_cpu_count(),
            Duration::ZERO,
        )
    });

    #[test]
    fn work_disabled() {
        let pool = WorkPoolImpl::new(WorkThresholds::publish_dev().clone(), 0, Duration::ZERO);
        let result = pool.generate_dev2(Root::from(1));
        assert_eq!(result, None);
    }

    #[test]
    fn work_one() {
        let pool = &WORK_POOL;
        let mut block = TestBlockBuilder::state().build();
        let root = block.root();
        block.set_work(pool.generate_dev2(root).unwrap());
        assert!(pool.threshold_base() < difficulty(&block));
    }

    #[test]
    fn work_validate() {
        let pool = &WORK_POOL;
        let mut block = TestBlockBuilder::legacy_send().work(6).build();
        assert!(difficulty(&block) < pool.threshold_base());
        let root = block.root();
        block
            .as_block_mut()
            .set_work(pool.generate_dev2(root).unwrap());
        assert!(difficulty(&block) > pool.threshold_base());
    }

    #[test]
    fn work_cancel() {
        let (tx, rx) = mpsc::channel();
        let key = Root::from(12345);
        WORK_POOL.generate_async(
            key,
            WorkThresholds::publish_dev().base,
            Some(Box::new(move |_done| {
                tx.send(()).unwrap();
            })),
        );
        WORK_POOL.cancel(&key);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)), Ok(()))
    }

    #[test]
    fn work_difficulty() {
        let root = Root::from(1);
        let difficulty1 = 0xff00000000000000;
        let difficulty2 = 0xfff0000000000000;
        let difficulty3 = 0xffff000000000000;
        let mut result_difficulty = u64::MAX;

        while result_difficulty > difficulty2 {
            let work = WORK_POOL.generate(root, difficulty1).unwrap();
            result_difficulty = WorkThresholds::publish_dev().difficulty(&root, work);
        }
        assert!(result_difficulty > difficulty1);

        result_difficulty = u64::MAX;
        while result_difficulty > difficulty3 {
            let work = WORK_POOL.generate(root, difficulty2).unwrap();
            result_difficulty = WorkThresholds::publish_dev().difficulty(&root, work);
        }
        assert!(result_difficulty > difficulty2);
    }

    fn difficulty(block: &Block) -> u64 {
        WorkThresholds::publish_dev().difficulty_block(block)
    }
}
