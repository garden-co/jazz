#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ProfileTimer {
    started_at: Instant,
}

#[cfg(target_arch = "wasm32")]
pub(crate) struct ProfileTimer {
    started_at_ms: f64,
}

impl ProfileTimer {
    pub(crate) fn start() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self {
                started_at: Instant::now(),
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self {
                started_at_ms: js_sys::Date::now(),
            }
        }
    }

    pub(crate) fn elapsed_ms(&self) -> f64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started_at.elapsed().as_secs_f64() * 1000.0
        }
        #[cfg(target_arch = "wasm32")]
        {
            js_sys::Date::now() - self.started_at_ms
        }
    }
}
