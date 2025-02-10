use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};

// Multi threaded output tracker
pub struct OutputTrackerMt<T: Clone + 'static> {
    output: Mutex<Vec<T>>,
}

impl<T: Clone + 'static> OutputTrackerMt<T> {
    pub fn new() -> Self {
        Self {
            output: Mutex::new(Vec::new()),
        }
    }

    pub fn add(&self, t: T) {
        self.output.lock().unwrap().push(t);
    }

    pub fn output(&self) -> Vec<T> {
        self.output.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.output.lock().unwrap().clear();
    }
}

impl<T> Default for OutputTrackerMt<T>
where
    T: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct OutputListenerMt<T: Clone + 'static> {
    trackers: Mutex<Vec<Weak<OutputTrackerMt<T>>>>,
    count: AtomicUsize,
}

impl<T: Clone + 'static> OutputListenerMt<T> {
    pub fn new() -> Self {
        Self {
            trackers: Mutex::new(Vec::new()),
            count: AtomicUsize::new(0),
        }
    }

    pub fn is_tracked(&self) -> bool {
        self.count.load(Ordering::Relaxed) > 0
    }

    pub fn track(&self) -> Arc<OutputTrackerMt<T>> {
        let tracker = Arc::new(OutputTrackerMt::new());
        let mut guard = self.trackers.lock().unwrap();
        guard.push(Arc::downgrade(&tracker));
        self.count.store(guard.len(), Ordering::SeqCst);
        tracker
    }

    pub fn emit(&self, t: T) {
        if self.count.load(Ordering::SeqCst) == 0 {
            return;
        }

        let mut guard = self.trackers.lock().unwrap();
        let mut should_clean = false;
        for tracker in guard.iter() {
            if let Some(tracker) = tracker.upgrade() {
                tracker.add(t.clone());
            } else {
                should_clean = true;
            }
        }

        if should_clean {
            guard.retain(|t| t.strong_count() > 0);
            self.count.store(guard.len(), Ordering::SeqCst);
        }
    }

    pub fn tracker_count(&self) -> usize {
        self.trackers.lock().unwrap().len()
    }
}

impl<T> Default for OutputListenerMt<T>
where
    T: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_active_trackers() {
        let listener = OutputListenerMt::new();
        listener.emit("foo");
    }

    #[test]
    fn track_one_output() {
        let listener = OutputListenerMt::new();
        let tracker = listener.track();
        listener.emit("foo");
        assert_eq!(tracker.output(), vec!["foo"]);
    }

    #[test]
    fn track_multiple_outputs() {
        let listener = OutputListenerMt::new();
        let tracker = listener.track();
        listener.emit("foo");
        listener.emit("bar");
        listener.emit("test");
        assert_eq!(tracker.output(), vec!["foo", "bar", "test"]);
    }

    #[test]
    fn multiple_trackers() {
        let listener = OutputListenerMt::new();
        let tracker1 = listener.track();
        listener.emit("foo");
        let tracker2 = listener.track();
        listener.emit("bar");
        listener.emit("test");
        assert_eq!(tracker1.output(), vec!["foo", "bar", "test"]);
        assert_eq!(tracker2.output(), vec!["bar", "test"]);
    }

    #[test]
    fn stop_tracking_if_tracker_dropped() {
        let listener = OutputListenerMt::new();
        let tracker = listener.track();
        listener.emit("foo");
        assert_eq!(listener.tracker_count(), 1);
        drop(tracker);
        listener.emit("bar");
        assert_eq!(listener.tracker_count(), 0);
    }
}
