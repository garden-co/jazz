use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use jazz::db::{TickScheduler, TickUrgency};

use crate::TickSchedulerCallback;

#[derive(Clone, Copy)]
enum SchedulerJob {
    Immediate,
    Deferred,
}

impl SchedulerJob {
    fn urgency(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Deferred => "deferred",
        }
    }
}

type SharedCallback = Arc<Mutex<Option<Arc<dyn TickSchedulerCallback>>>>;

const MIN_DISPATCH_INTERVAL: Duration = Duration::from_millis(1);

fn dispatch_delay(last_dispatch: Option<Instant>, now: Instant) -> Duration {
    last_dispatch
        .and_then(|last_dispatch| {
            MIN_DISPATCH_INTERVAL.checked_sub(now.duration_since(last_dispatch))
        })
        .unwrap_or_default()
}

/// Bridges Jazz's same-thread scheduler to a detached foreign-callback worker.
#[derive(Clone, Default)]
pub(crate) struct RnScheduler {
    immediate_scheduled: Arc<AtomicBool>,
    deferred_scheduled: Arc<AtomicBool>,
    callback: SharedCallback,
    worker: Arc<Mutex<Option<mpsc::Sender<SchedulerJob>>>>,
    shutdown: Arc<AtomicBool>,
}

impl RnScheduler {
    pub(crate) fn set_callback(&self, callback: Option<Box<dyn TickSchedulerCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = callback.map(Arc::from);
        }
    }

    fn flag(&self, job: SchedulerJob) -> &AtomicBool {
        match job {
            SchedulerJob::Immediate => &self.immediate_scheduled,
            SchedulerJob::Deferred => &self.deferred_scheduled,
        }
    }

    fn schedule(&self, job: SchedulerJob) {
        let flag = self.flag(job);
        if self.shutdown.load(Ordering::SeqCst) || flag.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut worker = match self.worker.lock() {
            Ok(worker) => worker,
            Err(_) => {
                flag.store(false, Ordering::SeqCst);
                return;
            }
        };
        if self.shutdown.load(Ordering::SeqCst) {
            flag.store(false, Ordering::SeqCst);
            return;
        }
        if worker.is_none() {
            *worker = Self::spawn_worker(
                Arc::clone(&self.immediate_scheduled),
                Arc::clone(&self.deferred_scheduled),
                Arc::clone(&self.callback),
            );
        }
        if worker
            .as_ref()
            .is_none_or(|sender| sender.send(job).is_err())
        {
            flag.store(false, Ordering::SeqCst);
            *worker = None;
        }
    }

    fn spawn_worker(
        immediate_scheduled: Arc<AtomicBool>,
        deferred_scheduled: Arc<AtomicBool>,
        callback: SharedCallback,
    ) -> Option<mpsc::Sender<SchedulerJob>> {
        let (sender, receiver) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("jazz-rn-notifier".to_owned())
            .spawn(move || {
                let mut last_dispatch = None;
                while let Ok(job) = receiver.recv() {
                    // Throttle only callbacks that follow a recent dispatch.
                    // The first wake after an idle period should not pay the
                    // anti-hot-loop delay.
                    let delay = dispatch_delay(last_dispatch, Instant::now());
                    if !delay.is_zero() {
                        std::thread::sleep(delay);
                    }
                    last_dispatch = Some(Instant::now());
                    match job {
                        SchedulerJob::Immediate => {
                            immediate_scheduled.store(false, Ordering::SeqCst)
                        }
                        SchedulerJob::Deferred => deferred_scheduled.store(false, Ordering::SeqCst),
                    }
                    let callback = callback.lock().ok().and_then(|slot| slot.clone());
                    if let Some(callback) = callback {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            callback.on_tick_needed(job.urgency().to_owned());
                        }));
                    }
                }
            });
        match spawned {
            Ok(_) => Some(sender),
            Err(error) => {
                eprintln!("jazz-rn: failed to spawn notifier thread: {error}");
                None
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.immediate_scheduled.store(false, Ordering::SeqCst);
        self.deferred_scheduled.store(false, Ordering::SeqCst);
        self.set_callback(None);
        // Never join this worker: it may currently be blocked in a JS callback
        // while close() itself is running on the JS thread.
        if let Ok(mut worker) = self.worker.lock() {
            *worker = None;
        }
    }
}

impl TickScheduler for RnScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.schedule(match urgency {
            TickUrgency::Immediate => SchedulerJob::Immediate,
            TickUrgency::Deferred => SchedulerJob::Deferred,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    struct Callback(mpsc::Sender<String>);

    impl TickSchedulerCallback for Callback {
        fn on_tick_needed(&self, urgency: String) {
            let _ = self.0.send(urgency);
        }
    }

    #[test]
    fn coalesces_same_urgency_bursts() {
        // Internal scheduler behavior cannot be observed through Jazz's public
        // database API, so this deliberately tests the binding mechanism.
        let scheduler = RnScheduler::default();
        let (sender, receiver) = mpsc::channel();
        scheduler.set_callback(Some(Box::new(Callback(sender))));
        for _ in 0..16 {
            scheduler.schedule_tick(TickUrgency::Immediate);
        }
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            "immediate"
        );
        assert!(receiver.recv_timeout(Duration::from_millis(30)).is_err());
        scheduler.shutdown();
    }

    #[test]
    fn shutdown_drops_future_callbacks() {
        // Internal scheduler behavior cannot be observed through Jazz's public
        // database API, so this deliberately tests the binding mechanism.
        let scheduler = RnScheduler::default();
        let (sender, receiver) = mpsc::channel();
        scheduler.set_callback(Some(Box::new(Callback(sender))));
        scheduler.shutdown();
        scheduler.schedule_tick(TickUrgency::Immediate);
        assert!(receiver.recv_timeout(Duration::from_millis(30)).is_err());
    }

    #[test]
    fn dispatch_throttle_skips_idle_wakes_and_spaces_bursts() {
        // Dispatch timing belongs to the foreign-callback scheduler rather
        // than Jazz's public database behavior, so exercise the deterministic
        // throttle calculation directly instead of relying on wall-clock
        // sleeps in a test.
        let now = Instant::now();
        assert_eq!(dispatch_delay(None, now), Duration::ZERO);
        assert_eq!(
            dispatch_delay(Some(now), now + Duration::from_micros(400)),
            Duration::from_micros(600)
        );
        assert_eq!(
            dispatch_delay(Some(now), now + MIN_DISPATCH_INTERVAL),
            Duration::ZERO
        );
    }
}
