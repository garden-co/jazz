//! The wall-clock the benchmark runner times with. Kept in the worker (not in
//! `bench-core`) so the core stays platform-pure; both workers pass `now_ms`
//! into `bench_core::run`.

use wasm_bindgen::prelude::*;

/// High-resolution time in milliseconds, preferring `performance.now()` and
/// falling back to `Date.now()`.
pub fn now_ms() -> f64 {
    let global = js_sys::global();
    let perf_key = JsValue::from_str("performance");
    if let Ok(perf) = js_sys::Reflect::get(&global, &perf_key)
        && !perf.is_undefined()
        && !perf.is_null()
    {
        let now_key = JsValue::from_str("now");
        if let Ok(now_fn) = js_sys::Reflect::get(&perf, &now_key)
            && let Some(now_fn) = now_fn.dyn_ref::<js_sys::Function>()
            && let Ok(v) = now_fn.call0(&perf)
            && let Some(ms) = v.as_f64()
        {
            return ms;
        }
    }

    js_sys::Date::now()
}
