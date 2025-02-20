mod cpu_work_generator;
mod gpu;
mod gpu_work_generator;
mod opencl_work_generator;
mod stub_work_pool;
mod work_pool;
mod work_queue;
mod work_thread;
mod work_thresholds;
mod xorshift;

pub(crate) use cpu_work_generator::CpuWorkGenerator;
pub use stub_work_pool::StubWorkPool;
pub(crate) use work_pool::WorkGenerator;
pub use work_pool::{WorkPool, WorkPoolImpl, STUB_WORK_POOL};
pub use work_queue::WorkTicket;
pub(crate) use work_queue::{WorkItem, WorkQueueCoordinator};
pub(crate) use work_thread::WorkThread;
pub use work_thresholds::{WorkThresholds, WORK_THRESHOLDS_STUB};
pub(crate) use xorshift::XorShift1024Star;

pub(crate) trait WorkRng {
    fn next_work(&mut self) -> u64;
}
