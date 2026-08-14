use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use crate::MutationErrorCallback;

type SharedCallback = Arc<Mutex<Option<Arc<dyn MutationErrorCallback>>>>;

/// Moves mutation-error callbacks off the thread-affine database actor.
///
/// The worker is deliberately detached: a foreign callback may be blocked on
/// the JavaScript thread while `RnDb.close()` is running on that same thread.
#[derive(Clone, Default)]
pub(crate) struct RnMutationErrorNotifier {
    callback: SharedCallback,
    worker: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    shutdown: Arc<AtomicBool>,
}

impl RnMutationErrorNotifier {
    pub(crate) fn set_callback(&self, callback: Box<dyn MutationErrorCallback>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = Some(Arc::from(callback));
        }
    }

    pub(crate) fn notify(&self, event_json: String) {
        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }

        let mut worker = match self.worker.lock() {
            Ok(worker) => worker,
            Err(_) => return,
        };
        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }
        if worker.is_none() {
            *worker = Self::spawn_worker(Arc::clone(&self.callback));
        }
        if worker
            .as_ref()
            .is_none_or(|sender| sender.send(event_json).is_err())
        {
            *worker = None;
        }
    }

    fn spawn_worker(callback: SharedCallback) -> Option<mpsc::Sender<String>> {
        let (sender, receiver) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("jazz-rn-mutation-errors".to_owned())
            .spawn(move || {
                while let Ok(event_json) = receiver.recv() {
                    let callback = callback.lock().ok().and_then(|slot| slot.clone());
                    if let Some(callback) = callback {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            callback.on_mutation_error(event_json);
                        }));
                    }
                }
            });
        match spawned {
            Ok(_) => Some(sender),
            Err(error) => {
                eprintln!("jazz-rn: failed to spawn mutation-error notifier: {error}");
                None
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Ok(mut callback) = self.callback.lock() {
            *callback = None;
        }
        // Never join this worker; see the type-level lifecycle note above.
        if let Ok(mut worker) = self.worker.lock() {
            *worker = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    struct Callback(mpsc::Sender<String>);

    impl MutationErrorCallback for Callback {
        fn on_mutation_error(&self, event_json: String) {
            let _ = self.0.send(event_json);
        }
    }

    #[test]
    fn dispatches_serialized_events_and_stops_after_shutdown() {
        // Thread handoff is a binding-only safety mechanism that cannot be
        // observed through Jazz's public database API.
        let notifier = RnMutationErrorNotifier::default();
        let (sender, receiver) = mpsc::channel();
        notifier.set_callback(Box::new(Callback(sender)));
        notifier.notify("{\"code\":\"permission_denied\"}".to_owned());
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            "{\"code\":\"permission_denied\"}"
        );

        notifier.shutdown();
        notifier.notify("{\"code\":\"late\"}".to_owned());
        assert!(receiver.recv_timeout(Duration::from_millis(30)).is_err());
    }
}
