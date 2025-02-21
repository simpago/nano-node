use super::{work_queue::WorkQueue, WorkGenerator, WorkQueueCoordinator, WorkTicket};
use std::sync::{Arc, MutexGuard};

pub(crate) struct WorkThread<T>
where
    T: WorkGenerator + Send + Sync,
{
    work_queue: Arc<WorkQueueCoordinator>,
    work_generator: T,
}

/// A single thread to generate PoW
impl<T> WorkThread<T>
where
    T: WorkGenerator + Send + Sync,
{
    pub fn new(work_generator: T, work_queue: Arc<WorkQueueCoordinator>) -> Self {
        Self {
            work_generator,
            work_queue,
        }
    }

    pub fn work_loop(mut self) {
        let mut queue_lock = self.work_queue.lock_work_queue();
        while !self.work_queue.should_stop() {
            if let Some(current) = queue_lock.first() {
                let root = current.root;
                let min_difficulty = current.min_difficulty;
                let work_ticket = self.work_queue.create_work_ticket();

                // drop work_queue lock, because work generation will take some time
                drop(queue_lock);

                let result = self
                    .work_generator
                    .create(&root, min_difficulty, &work_ticket);

                queue_lock = Self::handle_work_result(result, &self.work_queue, &work_ticket);
            } else {
                queue_lock = self.work_queue.wait_for_new_work_item(queue_lock);
            }
        }
    }

    fn handle_work_result<'a>(
        result: Option<u64>,
        work_queue: &'a WorkQueueCoordinator,
        work_ticket: &WorkTicket,
    ) -> MutexGuard<'a, WorkQueue> {
        let mut queue_lock = work_queue.lock_work_queue();
        if let Some(work) = result {
            if !work_ticket.expired() {
                queue_lock = Self::notify_work_found(work_queue, queue_lock, work);
            }
        } else {
            // A different thread found a solution
        }

        queue_lock
    }

    fn notify_work_found<'a>(
        work_queue: &'a WorkQueueCoordinator,
        mut queue_lock: MutexGuard<'a, WorkQueue>,
        work: u64,
    ) -> MutexGuard<'a, WorkQueue> {
        // Signal other threads to stop their work next time they check their ticket
        work_queue.expire_work_tickets();
        let mut current = queue_lock.dequeue();

        // work_found callback can take some time, to let's drop the lock
        drop(queue_lock);
        current.work_found(work);
        work_queue.lock_work_queue()
    }
}
