//! WASM Runtime implementation.
//!
//! Provides the WasmRuntime type that implements groove's Runtime trait
//! using browser APIs.

use std::future::Future;
use std::pin::Pin;

use groove::sync::Runtime;

/// WASM-based runtime for browser environments.
///
/// Uses browser APIs for async operations:
/// - `wasm_bindgen_futures::spawn_local` for task spawning
/// - JavaScript `setTimeout` via Promise for sleeping
/// - `Math.random()` for random numbers
#[derive(Clone, Debug, Default)]
pub struct WasmRuntime;

impl Runtime for WasmRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        wasm_bindgen_futures::spawn_local(future);
    }

    fn sleep(&self, duration_ms: u64) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            let promise = js_sys::Promise::new(&mut |resolve, _| {
                let window = web_sys::window().expect("no global window");
                window
                    .set_timeout_with_callback_and_timeout_and_arguments_0(
                        &resolve,
                        duration_ms as i32,
                    )
                    .expect("setTimeout failed");
            });
            let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
        })
    }

    fn random_f64(&self) -> f64 {
        js_sys::Math::random()
    }
}
