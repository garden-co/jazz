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
///
/// The trait bounds differ by platform:
/// - Native: Requires `Send + Sync` on the runtime and `Send` on futures
/// - WASM: No `Send` requirements (single-threaded)
#[cfg(not(target_arch = "wasm32"))]
pub trait Runtime: Clone + Send + Sync + 'static {
    /// Spawn an async task to run in the background.
    ///
    /// The task runs independently and the caller does not wait for completion.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

/// Runtime abstraction for spawning async tasks (WASM version).
///
/// In WASM, futures don't need to be `Send` since everything runs
/// on a single thread.
#[cfg(target_arch = "wasm32")]
pub trait Runtime: Clone + 'static {
    /// Spawn an async task to run in the background.
    ///
    /// The task runs independently and the caller does not wait for completion.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static;
}

/// Tokio-based runtime for native environments.
///
/// Uses `tokio::spawn` to spawn tasks on the tokio runtime.
#[cfg(all(feature = "sync-server", not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Default)]
pub struct TokioRuntime;

#[cfg(all(feature = "sync-server", not(target_arch = "wasm32")))]
impl Runtime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}

/// WASM-based runtime for browser environments.
///
/// Uses `wasm_bindgen_futures::spawn_local` to spawn tasks on the browser's event loop.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, Default)]
pub struct WasmRuntime;

#[cfg(target_arch = "wasm32")]
impl Runtime for WasmRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        wasm_bindgen_futures::spawn_local(future);
    }
}

/// Test runtime that executes futures synchronously.
///
/// Useful for testing where we want deterministic execution.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Default)]
pub struct TestRuntime;

#[cfg(all(test, not(target_arch = "wasm32")))]
impl Runtime for TestRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // In tests, we don't actually spawn - the test controls execution
        // This is a no-op; tests manually drive the async code
        drop(future);
    }
}
