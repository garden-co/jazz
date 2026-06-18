#![cfg(feature = "otel-core")]

//! Integration coverage for the OTLP panic hook (`otel::install_panic_hook`).
//!
//! This is an integration test (its own test binary / process) on purpose, not
//! a lib unit test: the hook is installed via `std::panic::set_hook`, which is
//! process-global. Lib unit tests run in parallel within a single process, so
//! mutating the global panic hook there could route another test's panic
//! through this one's hook. Isolating it in its own process keeps that global
//! mutation from influencing unrelated tests — even though it also saves and
//! restores the prior hook below.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jazz_tools::otel;
use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
use tracing_subscriber::prelude::*;

/// Exercises the crash-telemetry contract: when the server process panics, the
/// hook installed by `otel::install_panic_hook` must turn the panic into a
/// flushed, structured OTLP log record (carrying the panic message and source
/// location), and must re-invoke the previously installed hook so the stderr
/// backtrace — the filelog crash safety net — still prints.
///
/// Actor: the server process itself, panicking and observing its own crash
/// telemetry. An in-memory OTLP log exporter stands in for the node collector,
/// so the test needs no external endpoint.
///
/// ```text
/// panic! ──► panic hook ──► error! ──► OTLP bridge ──► force_flush ──► exporter
///                 └────────► previous hook ──► stderr backtrace (filelog net)
/// ```
#[test]
fn panic_emits_flushed_otlp_log_and_preserves_previous_hook() {
    let exporter = InMemoryLogExporter::default();
    let provider = SdkLoggerProvider::builder()
        .with_batch_exporter(exporter.clone())
        .build();

    // Route `tracing` events to the OTLP bridge for this thread. The panic hook
    // runs on the panicking thread, so this thread-local default is active when
    // the hook emits its `error!` event.
    let subscriber = tracing_subscriber::registry()
        .with(otel::log_bridge::<tracing_subscriber::Registry>(&provider));
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    // Sentinel previous hook (in a local Arc) — install_panic_hook must capture
    // and re-invoke it. Save and restore the prior global hook so this test is
    // self-contained.
    let saved_hook = std::panic::take_hook();
    let previous_ran = Arc::new(AtomicBool::new(false));
    let previous_ran_in_hook = previous_ran.clone();
    std::panic::set_hook(Box::new(move |_| {
        previous_ran_in_hook.store(true, Ordering::SeqCst);
    }));
    otel::install_panic_hook(provider.clone());

    let result = std::panic::catch_unwind(|| panic!("boom-from-test"));
    std::panic::set_hook(saved_hook);

    assert!(result.is_err(), "the panic should have been caught");
    assert!(
        previous_ran.load(Ordering::SeqCst),
        "install_panic_hook must re-invoke the previously installed hook",
    );

    let logs = exporter.get_emitted_logs().expect("read emitted logs");
    let panic_record = logs
        .iter()
        .find(|log| format!("{:?}", log.record.body()).contains("process panicked"))
        .expect("panic hook should emit a 'process panicked' log record");

    let attrs = format!(
        "{:?}",
        panic_record.record.attributes_iter().collect::<Vec<_>>()
    );
    assert!(
        attrs.contains("panic.message") && attrs.contains("boom-from-test"),
        "panic log must carry the panic message as an attribute; got: {attrs}",
    );
    assert!(
        attrs.contains("panic.location"),
        "panic log must carry the source location; got: {attrs}",
    );
}
