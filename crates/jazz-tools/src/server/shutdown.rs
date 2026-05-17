use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownPhase {
    Running,
    ShuttingDown,
    DrainingConnections,
    FlushingRuntime,
    ClosingStorage,
    StorageClosed,
    Failed,
}

impl ShutdownPhase {
    pub fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }
}

#[derive(Clone)]
pub struct ShutdownController {
    inner: Arc<ShutdownInner>,
}

struct ShutdownInner {
    requested: AtomicBool,
    timeout: Duration,
    phase_tx: watch::Sender<ShutdownPhase>,
    drain_notify: Notify,
    active_app_requests: AtomicUsize,
    active_websockets: AtomicUsize,
}

pub struct ActiveAppRequestGuard {
    controller: ShutdownController,
}

pub struct ActiveWebSocketGuard {
    controller: ShutdownController,
}

impl ShutdownController {
    pub fn new(timeout: Duration) -> Self {
        let (phase_tx, _) = watch::channel(ShutdownPhase::Running);
        Self {
            inner: Arc::new(ShutdownInner {
                requested: AtomicBool::new(false),
                timeout,
                phase_tx,
                drain_notify: Notify::new(),
                active_app_requests: AtomicUsize::new(0),
                active_websockets: AtomicUsize::new(0),
            }),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    pub fn phase(&self) -> ShutdownPhase {
        *self.inner.phase_tx.borrow()
    }

    pub fn is_shutting_down(&self) -> bool {
        !self.phase().is_running()
    }

    pub fn subscribe(&self) -> watch::Receiver<ShutdownPhase> {
        self.inner.phase_tx.subscribe()
    }

    pub fn set_phase(&self, phase: ShutdownPhase) {
        self.inner.phase_tx.send_replace(phase);
    }

    pub fn request_shutdown(&self) -> bool {
        let was_requested = self.inner.requested.swap(true, Ordering::SeqCst);
        if !was_requested {
            self.set_phase(ShutdownPhase::ShuttingDown);
            self.inner.drain_notify.notify_waiters();
        }
        !was_requested
    }

    pub async fn wait_requested(&self) {
        if self.inner.requested.load(Ordering::SeqCst) {
            return;
        }

        let mut rx = self.subscribe();
        while rx.borrow().is_running() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub fn try_enter_app_request(&self) -> Option<ActiveAppRequestGuard> {
        if self.is_shutting_down() {
            return None;
        }

        self.inner
            .active_app_requests
            .fetch_add(1, Ordering::SeqCst);
        if self.is_shutting_down() {
            self.inner
                .active_app_requests
                .fetch_sub(1, Ordering::SeqCst);
            self.inner.drain_notify.notify_waiters();
            return None;
        }

        Some(ActiveAppRequestGuard {
            controller: self.clone(),
        })
    }

    pub fn try_enter_websocket(&self) -> Option<ActiveWebSocketGuard> {
        if self.is_shutting_down() {
            return None;
        }

        self.inner.active_websockets.fetch_add(1, Ordering::SeqCst);
        if self.is_shutting_down() {
            self.inner.active_websockets.fetch_sub(1, Ordering::SeqCst);
            self.inner.drain_notify.notify_waiters();
            return None;
        }

        Some(ActiveWebSocketGuard {
            controller: self.clone(),
        })
    }

    pub fn active_app_requests(&self) -> usize {
        self.inner.active_app_requests.load(Ordering::SeqCst)
    }

    pub fn active_websockets(&self) -> usize {
        self.inner.active_websockets.load(Ordering::SeqCst)
    }

    pub async fn wait_for_websocket_drain(&self) -> bool {
        let deadline = tokio::time::Instant::now() + self.timeout();
        loop {
            if self.active_websockets() == 0 {
                return true;
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }

            let sleep = tokio::time::sleep_until(deadline);
            tokio::select! {
                _ = self.inner.drain_notify.notified() => {}
                _ = sleep => return self.active_websockets() == 0,
            }
        }
    }

    pub async fn wait_for_app_request_drain(&self) {
        while self.active_app_requests() > 0 {
            self.inner.drain_notify.notified().await;
        }
    }
}

impl Drop for ActiveAppRequestGuard {
    fn drop(&mut self) {
        self.controller
            .inner
            .active_app_requests
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.inner.drain_notify.notify_waiters();
    }
}

impl Drop for ActiveWebSocketGuard {
    fn drop(&mut self) {
        self.controller
            .inner
            .active_websockets
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.inner.drain_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_shutdown_is_idempotent() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));

        assert_eq!(controller.phase(), ShutdownPhase::Running);
        assert!(controller.request_shutdown());
        assert_eq!(controller.phase(), ShutdownPhase::ShuttingDown);
        assert!(!controller.request_shutdown());
        assert_eq!(controller.phase(), ShutdownPhase::ShuttingDown);
    }

    #[test]
    fn app_request_guard_rejects_after_shutdown_starts() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));
        assert_eq!(controller.active_app_requests(), 0);

        let guard = controller
            .try_enter_app_request()
            .expect("running server accepts request");
        assert_eq!(controller.active_app_requests(), 1);
        drop(guard);
        assert_eq!(controller.active_app_requests(), 0);

        assert!(controller.request_shutdown());
        assert!(controller.try_enter_app_request().is_none());
    }

    #[tokio::test]
    async fn wait_for_shutdown_request_observes_request() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));
        let waiter = controller.clone();

        let task = tokio::spawn(async move {
            waiter.wait_requested().await;
            waiter.phase()
        });

        assert!(controller.request_shutdown());

        let phase = task.await.expect("wait task");
        assert_eq!(phase, ShutdownPhase::ShuttingDown);
    }
}
