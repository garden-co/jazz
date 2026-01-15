//! Runtime abstraction for async operations.
//!
//! This module provides a platform-agnostic way to perform async operations,
//! allowing sync code to work with both tokio (native) and browser APIs (WASM).
//!
//! The sync layer is single-threaded on all platforms - no Send/Sync bounds.
//! On native, use `tokio::task::LocalSet` to run the sync code.
//!
//! The `Runtime` trait abstracts:
//! - Task spawning (`spawn`)
//! - Async sleeping (`sleep`)
//! - Random number generation (`random_f64`)

use std::future::Future;
use std::pin::Pin;

/// Runtime abstraction for async operations.
///
/// Implementations provide platform-specific async primitives:
/// - `TokioRuntime`: Uses tokio for native environments (with LocalSet)
/// - `WasmRuntime`: Uses browser APIs for WASM environments
///
/// No Send/Sync bounds - the sync layer is single-threaded on all platforms.
pub trait Runtime: Clone + 'static {
    /// Spawn an async task to run in the background.
    ///
    /// The task runs independently and the caller does not wait for completion.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static;

    /// Sleep for the given duration.
    ///
    /// Returns a future that completes after `duration_ms` milliseconds.
    fn sleep(&self, duration_ms: u64) -> Pin<Box<dyn Future<Output = ()> + 'static>>;

    /// Generate a random f64 in the range [0.0, 1.0).
    ///
    /// Used for jitter calculations in reconnection logic.
    fn random_f64(&self) -> f64;
}

// ============================================================================
// Tokio Runtime (Native)
// ============================================================================

/// Tokio-based runtime for native environments.
///
/// Uses tokio for async operations:
/// - `tokio::task::spawn_local` for task spawning (single-threaded)
/// - `tokio::time::sleep` for sleeping
/// - Hash-based PRNG for random numbers
///
/// **Important**: Must be run within a `tokio::task::LocalSet` context.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Default)]
pub struct TokioRuntime;

#[cfg(not(target_arch = "wasm32"))]
impl Runtime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        tokio::task::spawn_local(future);
    }

    fn sleep(&self, duration_ms: u64) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(tokio::time::sleep(std::time::Duration::from_millis(
            duration_ms,
        )))
    }

    fn random_f64(&self) -> f64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        );
        (hasher.finish() as f64) / (u64::MAX as f64)
    }
}

// ============================================================================
// WASM Runtime
// ============================================================================

// Note: WasmRuntime is defined in groove-wasm crate since it requires
// web_sys/js_sys dependencies. It implements this Runtime trait.

// ============================================================================
// Test Runtime
// ============================================================================

/// Test runtime that provides controllable behavior.
///
/// Useful for testing where we want deterministic execution.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Default)]
pub struct TestRuntime;

#[cfg(all(test, not(target_arch = "wasm32")))]
impl Runtime for TestRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        // In tests, we don't actually spawn - the test controls execution
        // This is a no-op; tests manually drive the async code
        drop(future);
    }

    fn sleep(&self, _duration_ms: u64) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        // Immediate completion for tests
        Box::pin(async {})
    }

    fn random_f64(&self) -> f64 {
        // Deterministic value for tests
        0.5
    }
}

// ============================================================================
// Reconnection Utilities
// ============================================================================

/// Configuration for reconnection behavior.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnection attempt (ms).
    pub initial_delay_ms: u64,
    /// Maximum delay between reconnection attempts (ms).
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff.
    pub backoff_multiplier: f64,
    /// Maximum number of reconnection attempts (None = unlimited).
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 1.5,
            max_attempts: None,
        }
    }
}

/// Calculate the next reconnection delay with exponential backoff.
pub fn calculate_reconnect_delay(attempt: u32, config: &ReconnectConfig) -> u64 {
    let delay = (config.initial_delay_ms as f64) * config.backoff_multiplier.powi(attempt as i32);
    (delay as u64).min(config.max_delay_ms)
}

/// Calculate reconnection delay with jitter to prevent thundering herd.
///
/// Adds random jitter of up to 25% of the base delay.
pub fn calculate_reconnect_delay_with_jitter(
    attempt: u32,
    config: &ReconnectConfig,
    random: f64,
) -> u64 {
    let base_delay = calculate_reconnect_delay(attempt, config);
    // Add jitter: 0 to 25% of base delay
    let jitter = (base_delay as f64 * 0.25 * random) as u64;
    base_delay + jitter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnect_delay_exponential() {
        let config = ReconnectConfig::default();

        assert_eq!(calculate_reconnect_delay(0, &config), 1000);
        assert_eq!(calculate_reconnect_delay(1, &config), 1500);
        assert_eq!(calculate_reconnect_delay(2, &config), 2250);
        assert_eq!(calculate_reconnect_delay(3, &config), 3375);
    }

    #[test]
    fn test_reconnect_delay_max() {
        let config = ReconnectConfig {
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
            max_attempts: None,
        };

        // 1000 * 2^10 = 1024000, but capped at 5000
        assert_eq!(calculate_reconnect_delay(10, &config), 5000);
    }

    #[test]
    fn test_reconnect_delay_with_jitter() {
        let config = ReconnectConfig::default();

        // With random = 0.0, jitter should be 0
        let delay_no_jitter = calculate_reconnect_delay_with_jitter(0, &config, 0.0);
        assert_eq!(delay_no_jitter, 1000);

        // With random = 1.0, jitter should be 25% of base
        let delay_max_jitter = calculate_reconnect_delay_with_jitter(0, &config, 1.0);
        assert_eq!(delay_max_jitter, 1250);

        // With random = 0.5, jitter should be 12.5% of base
        let delay_half_jitter = calculate_reconnect_delay_with_jitter(0, &config, 0.5);
        assert_eq!(delay_half_jitter, 1125);
    }
}
