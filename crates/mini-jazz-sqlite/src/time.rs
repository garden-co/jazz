#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now_ms() -> i64 {
    platform_now_ms()
}

#[cfg(not(target_arch = "wasm32"))]
fn platform_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(target_arch = "wasm32")]
fn platform_now_ms() -> i64 {
    js_sys::Date::now() as i64
}
