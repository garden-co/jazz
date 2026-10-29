//! Refuse benchmark and receipt runs whose instrumentation changes the numbers.

/// Explicit allocator for native benchmark leaves. Opt into `mimalloc` to
/// match jazz-tools' Rust allocator; default System preserves older receipts.
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
pub use mimalloc::MiMalloc as Allocator;
#[cfg(not(all(feature = "mimalloc", not(target_arch = "wasm32"))))]
pub use std::alloc::System as Allocator;

#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
pub const ALLOCATOR_NAME: &str = "mimalloc";
#[cfg(not(all(feature = "mimalloc", not(target_arch = "wasm32"))))]
pub const ALLOCATOR_NAME: &str = "system";

/// An environment switch that changes the work performed by a measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContaminatingInstrumentation {
    /// The environment variable that enables the instrumentation.
    pub name: &'static str,
    /// Why numbers collected while it is set are not trustworthy.
    pub reason: &'static str,
}

/// The complete, single-source list of instrumentation that contaminates ordinary
/// wall-clock or storage-counter benchmark and receipt measurements.
pub const CONTAMINATING_INSTRUMENTATION: &[ContaminatingInstrumentation] = &[
    ContaminatingInstrumentation {
        name: "JAZZ_PERF_CONTROL_FIFO",
        reason: "it enables externally controlled CPU profiling during cold-load attribution",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_PERF_ACK_FIFO",
        reason: "it configures CPU profiler acknowledgement during cold-load attribution",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_PROFILE_OUT",
        reason: "it enables CPU profiling around benchmark phases",
    },
];

/// Return the first configured contaminating switch, if any.
pub fn contaminating_instrumentation() -> Option<&'static ContaminatingInstrumentation> {
    first_contaminating_instrumentation(|name| std::env::var_os(name).is_some())
}

/// Refuse to produce an ordinary benchmark or receipt measurement under
/// instrumentation that would corrupt its wall-clock or counter results.
///
/// Attribution-only profiling paths deliberately do not call this function.
pub fn refuse_contaminated_measurement() {
    if let Some(instrumentation) = contaminating_instrumentation() {
        eprintln!(
            "refusing to run: {} is set; {}. Wall-clock and storage-counter numbers would be wrong. Unset it and re-run.",
            instrumentation.name, instrumentation.reason,
        );
        std::process::exit(2);
    }
}

fn first_contaminating_instrumentation(
    is_set: impl Fn(&str) -> bool,
) -> Option<&'static ContaminatingInstrumentation> {
    CONTAMINATING_INSTRUMENTATION
        .iter()
        .find(|instrumentation| is_set(instrumentation.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn process_refuses_contamination_and_permits_a_clean_environment() {
        const PROBE_ENV: &str = "JAZZ_BENCHMARK_GUARD_TEST_PROBE";

        if std::env::var_os(PROBE_ENV).is_some() {
            refuse_contaminated_measurement();
            return;
        }

        let clean = run_probe(PROBE_ENV, None);
        assert!(clean.status.success(), "clean probe failed: {clean:?}");

        let contaminated = run_probe(PROBE_ENV, Some(("JAZZ_PROFILE_OUT", "1")));
        assert_eq!(contaminated.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&contaminated.stderr).contains("JAZZ_PROFILE_OUT"),
            "refusal should name the contaminating variable"
        );
    }

    fn run_probe(probe_env: &str, contamination: Option<(&str, &str)>) -> std::process::Output {
        let mut command = Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--exact")
            .arg("tests::process_refuses_contamination_and_permits_a_clean_environment")
            .arg("--nocapture")
            .env(probe_env, "1");
        for instrumentation in CONTAMINATING_INSTRUMENTATION {
            command.env_remove(instrumentation.name);
        }
        if let Some((name, value)) = contamination {
            command.env(name, value);
        }
        command.output().expect("run guard probe")
    }

    #[test]
    fn finds_the_first_configured_contaminating_variable() {
        let refusal = first_contaminating_instrumentation(|name| name == "JAZZ_PROFILE_OUT")
            .expect("JAZZ_PROFILE_OUT should be refused");

        assert_eq!(refusal.name, "JAZZ_PROFILE_OUT");
        assert!(first_contaminating_instrumentation(|_| false).is_none());
    }
}
