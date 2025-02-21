use crate::Root;
use std::sync::{
    atomic::{AtomicBool, AtomicI32, Ordering},
    Condvar, Mutex, MutexGuard,
};

static NEVER_EXPIRES: AtomicI32 = AtomicI32::new(0);

#[derive(Clone)]
pub struct WorkTicket<'a> {
    ticket: &'a AtomicI32,
    ticket_copy: i32,
}

impl<'a> WorkTicket<'a> {
    pub fn never_expires() -> Self {
        Self::new(&NEVER_EXPIRES)
    }

    pub fn already_expired() -> Self {
        Self {
            ticket: &NEVER_EXPIRES,
            ticket_copy: 1,
        }
    }

    pub fn new(ticket: &'a AtomicI32) -> Self {
        Self {
            ticket,
            ticket_copy: ticket.load(Ordering::SeqCst),
        }
    }

    pub fn expired(&self) -> bool {
        self.ticket_copy != self.ticket.load(Ordering::SeqCst)
    }
}

pub(crate) struct WorkItem {
    pub root: Root,
    pub min_difficulty: u64,
    pub callback: Option<Box<dyn FnOnce(Option<u64>) + Send>>,
}

impl WorkItem {
    pub fn work_found(&mut self, work: u64) {
        // we're the ones that found the solution
        if let Some(callback) = self.callback.take() {
            (callback)(Some(work));
        }
    }
}

pub(crate) struct WorkQueue(Vec<WorkItem>);

impl WorkQueue {
    pub fn new() -> Self {
        WorkQueue(Vec::new())
    }

    pub fn first(&self) -> Option<&WorkItem> {
        self.0.first()
    }

    pub fn is_first(&self, root: &Root) -> bool {
        if let Some(front) = self.first() {
            front.root == *root
        } else {
            false
        }
    }

    pub fn cancel(&mut self, root: &Root) -> Vec<Box<dyn FnOnce(Option<u64>) + Send>> {
        let mut cancelled = Vec::new();
        self.0.retain_mut(|item| {
            let retain = item.root != *root;
            if !retain {
                if let Some(callback) = item.callback.take() {
                    cancelled.push(callback);
                }
            }
            retain
        });
        cancelled
    }

    pub fn enqueue(&mut self, item: WorkItem) {
        self.0.push(item);
    }

    pub fn dequeue(&mut self) -> WorkItem {
        self.0.remove(0)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Coordinates access to the work queue between multiple threads
pub(crate) struct WorkQueueCoordinator {
    work_queue: Mutex<WorkQueue>,
    should_stop: AtomicBool,
    producer_condition: Condvar,
    ticket: AtomicI32,
}

impl WorkQueueCoordinator {
    pub fn new() -> Self {
        Self {
            work_queue: Mutex::new(WorkQueue::new()),
            should_stop: AtomicBool::new(false),
            producer_condition: Condvar::new(),
            ticket: AtomicI32::new(0),
        }
    }

    pub fn should_stop(&self) -> bool {
        self.should_stop.load(Ordering::Relaxed)
    }

    pub fn lock_work_queue(&self) -> MutexGuard<WorkQueue> {
        self.work_queue.lock().unwrap()
    }

    pub fn wait_for_new_work_item<'a>(
        &'a self,
        guard: MutexGuard<'a, WorkQueue>,
    ) -> MutexGuard<'a, WorkQueue> {
        self.producer_condition.wait(guard).unwrap()
    }

    pub fn enqueue(&self, work_item: WorkItem) {
        {
            let mut pending = self.work_queue.lock().unwrap();
            pending.enqueue(work_item)
        }
        self.producer_condition.notify_all();
    }

    pub fn notify_new_work_ticket(&self) {
        self.producer_condition.notify_all()
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
        self.expire_work_tickets();
        self.notify_new_work_ticket();
    }

    pub fn create_work_ticket(&'_ self) -> WorkTicket<'_> {
        WorkTicket::new(&self.ticket)
    }

    pub fn expire_work_tickets(&self) {
        self.ticket.fetch_add(1, Ordering::SeqCst);
    }

    pub fn cancel(&self, root: &Root) {
        let mut cancelled = Vec::new();
        {
            let mut lock = self.lock_work_queue();
            if !self.should_stop() {
                if lock.is_first(root) {
                    self.expire_work_tickets();
                }

                cancelled = lock.cancel(root);
            }
        }

        for callback in cancelled {
            callback(None);
        }
    }
}
