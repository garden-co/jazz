//! Diagnostic environment switches read on hot paths.
//!
//! Each switch is read once per process: `env::var_os` locks the process
//! environment and allocates, and these checks run per send, per group and
//! per delta. Setting one of these variables after the first check has no
//! effect; set it before the process starts.

use std::sync::OnceLock;

fn cached(slot: &'static OnceLock<bool>, name: &str) -> bool {
    *slot.get_or_init(|| std::env::var_os(name).is_some())
}

/// `JAZZ_COVERED_INPUT_TRACE`: covered-input and delivery tracing.
pub(crate) fn covered_input_trace() -> bool {
    static SLOT: OnceLock<bool> = OnceLock::new();
    cached(&SLOT, "JAZZ_COVERED_INPUT_TRACE")
}

/// `JAZZ_REHYDRATE_TRACE`: publication rehydration tracing.
pub(crate) fn rehydrate_trace() -> bool {
    static SLOT: OnceLock<bool> = OnceLock::new();
    cached(&SLOT, "JAZZ_REHYDRATE_TRACE")
}

/// `JAZZ_QUERY_TEMPLATE_TRACE`: query template lowering tracing.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn query_template_trace() -> bool {
    static SLOT: OnceLock<bool> = OnceLock::new();
    cached(&SLOT, "JAZZ_QUERY_TEMPLATE_TRACE")
}

/// `JAZZ_FORCE_SINGLETON_VERSION_CARRIERS`: disable outbound run building.
pub(crate) fn force_singleton_version_carriers() -> bool {
    static SLOT: OnceLock<bool> = OnceLock::new();
    cached(&SLOT, "JAZZ_FORCE_SINGLETON_VERSION_CARRIERS")
}
