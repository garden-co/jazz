//! Runtime abstraction for async task spawning.
//!
//! This module provides a platform-agnostic way to spawn async tasks,
//! allowing SyncedNode to work with both tokio (native) and
//! wasm_bindgen_futures (browser).

use std::future::Future;

/// Runtime abstraction for spawning async tasks.
///
/// Implementations provide platform-specific task spawning:
/// - `TokioRuntime`: Uses `tokio::spawn` for native environments
/// - `WasmRuntime`: Uses `wasm_bindgen_futures::spawn_local` for browsers
pub trait Runtime: Clone + Send + Sync + 'static {
    /// Spawn an async task to run in the background.
    ///
    /// The task runs independently and the caller does not wait for completion.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

/// Tokio-based runtime for native environments.
///
/// Uses `tokio::spawn` to spawn tasks on the tokio runtime.
#[cfg(feature = "sync-server")]
#[derive(Clone, Debug, Default)]
pub struct TokioRuntime;

#[cfg(feature = "sync-server")]
impl Runtime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}

/// Test runtime that executes futures synchronously.
///
/// Useful for testing where we want deterministic execution.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct TestRuntime;

#[cfg(test)]
impl Runtime for TestRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // In tests, we don't actually spawn - the test controls execution
        // This is a no-op; tests manually drive the async code
        let _ = future;
    }
}
