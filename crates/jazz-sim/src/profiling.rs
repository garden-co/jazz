//! Optional CPU profiling hooks for benchmark smoke scenarios.

#[cfg(feature = "profiling")]
use std::collections::BTreeMap;
#[cfg(feature = "profiling")]
use std::fs::{self, File};
#[cfg(feature = "profiling")]
use std::io::Write;
#[cfg(feature = "profiling")]
use std::path::PathBuf;
#[cfg(feature = "profiling")]
use std::time::Instant;

/// Run a benchmark phase under pprof when profiling is enabled.
///
/// Profiling is active only when the `profiling` feature is compiled and
/// `JAZZ_PROFILE_OUT` points at an output directory. Normal smoke runs pay only
/// the closure call cost.
#[cfg(feature = "profiling")]
pub fn maybe_profile_phase<T>(scenario: &str, phase: &str, work: impl FnOnce() -> T) -> T {
    let Some(out_dir) = profile_out_dir() else {
        return work();
    };

    fs::create_dir_all(&out_dir).expect("create JAZZ_PROFILE_OUT directory");
    let stem = format!(
        "{}__{}",
        sanitize_file_component(scenario),
        sanitize_file_component(phase)
    );
    let svg_path = out_dir.join(format!("{stem}.svg"));
    let top_path = out_dir.join(format!("{stem}.top.txt"));

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(profile_frequency())
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("start pprof guard");

    let started = Instant::now();
    let result = work();
    let elapsed = started.elapsed();

    if let Ok(report) = guard.report().build() {
        let file = File::create(&svg_path).expect("create flamegraph SVG");
        report.flamegraph(file).expect("write flamegraph SVG");
        write_top_table(&report, &top_path, elapsed);
        eprintln!(
            "wrote profile scenario={scenario} phase={phase} svg={} top={}",
            svg_path.display(),
            top_path.display()
        );
    }

    result
}

/// Run a benchmark phase. No-op unless built with the `profiling` feature.
#[cfg(not(feature = "profiling"))]
pub fn maybe_profile_phase<T>(_scenario: &str, _phase: &str, work: impl FnOnce() -> T) -> T {
    work()
}

#[cfg(feature = "profiling")]
fn profile_out_dir() -> Option<PathBuf> {
    std::env::var_os("JAZZ_PROFILE_OUT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(feature = "profiling")]
fn profile_frequency() -> i32 {
    std::env::var("JAZZ_PROFILE_FREQUENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(997)
}

#[cfg(feature = "profiling")]
fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(feature = "profiling")]
fn write_top_table(report: &pprof::Report, path: &PathBuf, elapsed: std::time::Duration) {
    let mut self_samples = BTreeMap::<String, isize>::new();
    let mut total_samples = 0_isize;
    for (frames, count) in &report.data {
        total_samples += *count;
        if let Some(symbols) = frames.frames.first()
            && let Some(symbol) = symbols.first()
        {
            *self_samples.entry(symbol.name()).or_default() += *count;
        }
    }

    let mut rows: Vec<_> = self_samples.into_iter().collect();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut file = File::create(path).expect("create profile top table");
    writeln!(file, "| Rank | Self Samples | Self % | Function |").expect("write profile table");
    writeln!(file, "| ---: | ---: | ---: | --- |").expect("write profile table");
    for (idx, (name, samples)) in rows.into_iter().take(10).enumerate() {
        let pct = if total_samples == 0 {
            0.0
        } else {
            samples as f64 * 100.0 / total_samples as f64
        };
        writeln!(
            file,
            "| {} | {} | {:.2}% | `{}` |",
            idx + 1,
            samples,
            pct,
            name.replace('`', "'")
        )
        .expect("write profile table");
    }
    writeln!(
        file,
        "\nSamples: {total_samples}; elapsed: {:.3}s; frequency_hz: {}",
        elapsed.as_secs_f64(),
        profile_frequency()
    )
    .expect("write profile table");
}
