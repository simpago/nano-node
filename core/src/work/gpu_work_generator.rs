use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{spawn, JoinHandle},
};

use rand::Rng;

use super::{gpu::Gpu, WorkGenerator, WorkItem, WorkTicket};
use crate::{utils::OneShotNotification, Difficulty, DifficultyV1, Root, WorkNonce};

#[derive(Default)]
struct WorkState {
    work_item: Option<WorkItem>,
    task_complete: Arc<AtomicBool>,
    unsuccessful_workers: usize,
    random_mode: bool,
    future_work: Vec<WorkItem>,
}

impl WorkState {
    fn set_task(&mut self, cond_var: &Condvar) {
        if self.work_item.is_none() {
            self.task_complete.store(true, Ordering::Relaxed);
            if self.future_work.len() > 0 {
                let max_range = if self.random_mode {
                    self.future_work.len()
                } else {
                    1
                };
                let i = rand::rng().random_range(0..max_range);
                let work_item = self.future_work.remove(i);
                self.work_item = Some(work_item);
                cond_var.notify_all();
            }
        }
    }
}

enum WorkError {
    Canceled,
    Errored,
}
/// Generates the proof of work using a GPU with OpenCL
pub struct GpuWorkGenerator {
    handle: Option<JoinHandle<()>>,
    work_state: Arc<(Mutex<WorkState>, Condvar)>,
}

const N_WORKERS: usize = 1;
const GPU_I: usize = 0;
const PLATFORM_IDX: usize = 0;
const DEVICE_IDX: usize = 0;
const THREADS: usize = 1048576;

impl GpuWorkGenerator {
    pub fn new() -> Self {
        Self {
            handle: None,
            work_state: Arc::new((Mutex::new(Default::default()), Condvar::new())),
        }
    }

    pub fn start(&mut self) {
        let work_state = self.work_state.clone();
        self.handle = Some(spawn(move || {
            println!("Entering work thread");
            let mut gpu =
                Gpu::new(PLATFORM_IDX, DEVICE_IDX, THREADS, None).expect("failed to create GPU");
            let mut failed = false;
            let mut root = Root::zero();
            let mut difficulty = 0u64;
            let mut consecutive_gpu_errors = 0;
            let mut consecutive_gpu_invalid_work_errors = 0;
            let mut attempts = 0;
            let task_complete = work_state.0.lock().unwrap().task_complete.clone();
            task_complete.store(true, Ordering::SeqCst);
            loop {
                if failed || task_complete.load(Ordering::Relaxed) {
                    println!("inside failed branch");
                    let mut state = work_state.0.lock().unwrap();
                    if root != state.work_item.as_ref().map(|i| i.root).unwrap_or_default() {
                        failed = false;
                    }
                    if failed {
                        state.unsuccessful_workers += 1;
                        if state.unsuccessful_workers == N_WORKERS {
                            if let Some(mut work_item) = state.work_item.take() {
                                if let Some(callback) = work_item.callback.take() {
                                    callback(None);
                                    state.set_task(&work_state.1);
                                }
                            }
                        }
                        state = work_state.1.wait(state).unwrap();
                    }
                    while state.work_item.is_none() {
                        state = work_state.1.wait(state).unwrap();
                    }
                    let work_item = state.work_item.as_ref().unwrap();
                    root = work_item.root;
                    difficulty = work_item.min_difficulty;
                    if failed {
                        state.unsuccessful_workers -= 1;
                    }
                    task_complete.store(false, Ordering::SeqCst);
                    println!("Calling gpu.set_task");
                    attempts = 0;
                    if let Err(err) = gpu.set_task(root.as_bytes(), difficulty) {
                        eprintln!(
                            "Failed to set GPU {}'s task, abandoning it for this work: {:?}",
                            GPU_I, err,
                        );
                        failed = true;
                        continue;
                    }
                    failed = false;
                    consecutive_gpu_errors = 0;
                }

                let attempt = rand::rng().random();
                attempts += 1;
                let mut out = [0u8; 8];
                match gpu.run(&mut out, attempt) {
                    Ok(true) => {
                        let work = WorkNonce::from(u64::from_le_bytes(out));
                        let work_valid =
                            DifficultyV1 {}.get_difficulty(&root, work.into()) >= difficulty;
                        if work_valid {
                            println!("attempts taken: {attempts}");
                            let mut state = work_state.0.lock().unwrap();
                            if let Some(mut work_item) = state.work_item.take() {
                                if root == work_item.root {
                                    if let Some(callback) = work_item.callback.take() {
                                        callback(Some(work.into()));
                                        state.set_task(&work_state.1);
                                    }
                                }
                            }
                            consecutive_gpu_errors = 0;
                            consecutive_gpu_invalid_work_errors = 0;
                        } else {
                            eprintln!(
                                "GPU {} returned invalid work {} for root {}",
                                GPU_I, work, root,
                            );
                            if consecutive_gpu_invalid_work_errors >= 3 {
                                eprintln!("GPU {} returned invalid work 3 consecutive times, abandoning it for this work", GPU_I);
                                failed = true;
                            } else {
                                consecutive_gpu_errors += 1;
                                consecutive_gpu_invalid_work_errors += 1;
                            }
                        }
                    }
                    Ok(false) => {
                        consecutive_gpu_errors = 0;
                    }
                    Err(err) => {
                        eprintln!("Error computing work on GPU {}: {:?}", GPU_I, err);
                        if let Err(err) = gpu.reset_bufs() {
                            eprintln!(
                                    "Failed to reset GPU {}'s buffers, abandoning it for this work: {:?}",
                                    GPU_I, err,
                                );
                            failed = true;
                        }
                        consecutive_gpu_errors += 1;
                    }
                }
                if consecutive_gpu_errors >= 3 {
                    eprintln!(
                        "3 consecutive GPU {} errors, abandoning it for this work",
                        GPU_I,
                    );
                    failed = true;
                }
            }
        }));
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

impl Drop for GpuWorkGenerator {
    fn drop(&mut self) {
        self.stop()
    }
}

impl WorkGenerator for GpuWorkGenerator {
    fn create(
        &mut self,
        root: &Root,
        min_difficulty: u64,
        work_ticket: &WorkTicket,
    ) -> Option<u64> {
        println!("entering gpu work generator");
        let done = OneShotNotification::new();
        let done2 = done.clone();
        {
            let mut state = self.work_state.0.lock().unwrap();
            let work_item = WorkItem {
                root: *root,
                min_difficulty,
                callback: Some(Box::new(move |result| match result {
                    Some(work) => done2.notify(work),
                    None => done2.cancel(),
                })),
            };
            state.future_work.push(work_item);
            state.set_task(&self.work_state.1);
        }
        println!("Waiting until done...");
        let work = done.wait();
        println!("Done!");
        work
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_work() {
        let mut work_generator = GpuWorkGenerator::new();
        work_generator.start();
        let min_difficulty = 0xfffffff800000000;
        let work_ticket = WorkTicket::never_expires();

        let root = Root::from(123);
        let result = work_generator.create(&root, min_difficulty, &work_ticket);

        let nonce = WorkNonce::from(result.unwrap_or_default());
        println!("result is: {nonce} for root {root}");
    }
}
