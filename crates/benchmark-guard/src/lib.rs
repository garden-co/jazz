//! Refuse benchmark and receipt runs whose instrumentation changes the numbers.

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
        name: "JAZZ_REHYDRATE_TRACE",
        reason: "it resets storage-read metrics and adds timing plus formatted stderr output inside rehydration",
    },
    ContaminatingInstrumentation {
        name: "GROOVE_PROFILE_HYDRATION_OPERATORS",
        reason: "it times every hydration join, inflating wall-clock measurements",
    },
    ContaminatingInstrumentation {
        name: "GROOVE_TRACE_INDEX_BY",
        reason: "it times and formats IndexBy and Persist work inside the measured runtime path",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_CLOSURE_TRACE",
        reason: "it formats and writes recursive-runtime trace records during measured work",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_CAPABILITY_TRACE",
        reason: "it enables capability trace formatting during query compilation",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_CAPABILITY_TRACE_FILE",
        reason: "it writes capability traces and captures backtraces during query compilation",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_PROFILE_OUT",
        reason: "it enables CPU profiling around benchmark phases",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_CUSTOMER_TRACE_TICKS",
        reason: "it formats and writes per-tick customer cold-start trace output during settling",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_ALLOC_SITE_SAMPLE_RATE",
        reason: "it configures allocation stack sampling during the customer cold-start receipt",
    },
    ContaminatingInstrumentation {
        name: "JAZZ_ALLOC_SITE_MAX_SAMPLES",
        reason: "it configures allocation stack sampling during the customer cold-start receipt",
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

        let contaminated = run_probe(PROBE_ENV, Some(("JAZZ_REHYDRATE_TRACE", "1")));
        assert_eq!(contaminated.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&contaminated.stderr).contains("JAZZ_REHYDRATE_TRACE"),
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
        let refusal = first_contaminating_instrumentation(|name| name == "JAZZ_REHYDRATE_TRACE")
            .expect("JAZZ_REHYDRATE_TRACE should be refused");

        assert_eq!(refusal.name, "JAZZ_REHYDRATE_TRACE");
        assert!(first_contaminating_instrumentation(|_| false).is_none());
    }
}
