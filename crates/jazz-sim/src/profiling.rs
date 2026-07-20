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
    let thread_path = out_dir.join(format!("{stem}.threads.txt"));

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(profile_frequency())
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("start pprof guard");

    let started = Instant::now();
    let result = work();
    let elapsed = started.elapsed();

    match guard.report().build() {
        Ok(report) => {
            let file = File::create(&svg_path).expect("create flamegraph SVG");
            report.flamegraph(file).expect("write flamegraph SVG");
            write_top_table(&report, &top_path, elapsed);
            write_thread_tables(&report, &thread_path, elapsed);
            eprintln!(
                "wrote profile scenario={scenario} phase={phase} svg={} top={} threads={}",
                svg_path.display(),
                top_path.display(),
                thread_path.display()
            );
        }
        Err(error) => {
            eprintln!("profile report unavailable scenario={scenario} phase={phase}: {error}");
        }
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

#[cfg(feature = "profiling")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ThreadClass {
    Foreground,
    RocksDbBackground,
    Other,
}

#[cfg(feature = "profiling")]
fn write_thread_tables(report: &pprof::Report, path: &PathBuf, elapsed: std::time::Duration) {
    let mut class_self_samples = BTreeMap::<ThreadClass, BTreeMap<String, isize>>::new();
    let mut class_samples = BTreeMap::<ThreadClass, isize>::new();
    let mut thread_samples = BTreeMap::<(ThreadClass, String), isize>::new();
    let mut total_samples = 0_isize;

    for (frames, count) in &report.data {
        total_samples += *count;
        let class = classify_thread_sample(frames);
        *class_samples.entry(class).or_default() += *count;
        *thread_samples
            .entry((class, frames.thread_name_or_id()))
            .or_default() += *count;
        if let Some(symbols) = frames.frames.first()
            && let Some(symbol) = symbols.first()
        {
            *class_self_samples
                .entry(class)
                .or_default()
                .entry(symbol.name())
                .or_default() += *count;
        }
    }

    let mut file = File::create(path).expect("create profile thread table");
    writeln!(
        file,
        "Samples: {total_samples}; elapsed: {:.3}s; frequency_hz: {}",
        elapsed.as_secs_f64(),
        profile_frequency()
    )
    .expect("write profile thread table");

    writeln!(file, "\n## Thread Classes\n").expect("write profile thread table");
    writeln!(file, "| Class | Samples | Sample % |").expect("write profile thread table");
    writeln!(file, "| --- | ---: | ---: |").expect("write profile thread table");
    for class in [
        ThreadClass::Foreground,
        ThreadClass::RocksDbBackground,
        ThreadClass::Other,
    ] {
        let samples = class_samples.get(&class).copied().unwrap_or_default();
        writeln!(
            file,
            "| {} | {} | {:.2}% |",
            thread_class_name(class),
            samples,
            sample_pct(samples, total_samples)
        )
        .expect("write profile thread table");
    }

    writeln!(file, "\n## Threads\n").expect("write profile thread table");
    writeln!(file, "| Class | Thread | Samples | Sample % |").expect("write profile thread table");
    writeln!(file, "| --- | --- | ---: | ---: |").expect("write profile thread table");
    let mut thread_rows: Vec<_> = thread_samples.into_iter().collect();
    thread_rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    for ((class, thread), samples) in thread_rows {
        writeln!(
            file,
            "| {} | `{}` | {} | {:.2}% |",
            thread_class_name(class),
            thread.replace('`', "'"),
            samples,
            sample_pct(samples, total_samples)
        )
        .expect("write profile thread table");
    }

    for class in [
        ThreadClass::Foreground,
        ThreadClass::RocksDbBackground,
        ThreadClass::Other,
    ] {
        let class_total = class_samples.get(&class).copied().unwrap_or_default();
        writeln!(file, "\n## {} Top Self-Time\n", thread_class_name(class))
            .expect("write profile thread table");
        writeln!(
            file,
            "| Rank | Self Samples | Class % | Total % | Function |"
        )
        .expect("write profile thread table");
        writeln!(file, "| ---: | ---: | ---: | ---: | --- |").expect("write profile thread table");
        let mut rows: Vec<_> = class_self_samples
            .remove(&class)
            .unwrap_or_default()
            .into_iter()
            .collect();
        rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (idx, (name, samples)) in rows.into_iter().take(15).enumerate() {
            writeln!(
                file,
                "| {} | {} | {:.2}% | {:.2}% | `{}` |",
                idx + 1,
                samples,
                sample_pct(samples, class_total),
                sample_pct(samples, total_samples),
                name.replace('`', "'")
            )
            .expect("write profile thread table");
        }
    }
}

#[cfg(feature = "profiling")]
fn classify_thread_sample(frames: &pprof::Frames) -> ThreadClass {
    let thread = frames.thread_name_or_id().to_ascii_lowercase();
    if thread.contains("rocksdb")
        || thread.contains("rocks")
        || thread.contains("flush")
        || thread.contains("compact")
    {
        return ThreadClass::RocksDbBackground;
    }

    let mut saw_foreground_symbol = false;
    for frame in &frames.frames {
        for symbol in frame {
            let name = symbol.name();
            let lower = name.to_ascii_lowercase();
            if lower.contains("background")
                || lower.contains("bgthread")
                || lower.contains("threadpoolimpl")
                || lower.contains("flushjob")
                || lower.contains("compactionjob")
            {
                return ThreadClass::RocksDbBackground;
            }
            if lower.contains("jazz_sim")
                || lower.contains("jazz::")
                || lower.contains("groove::")
                || lower.contains("s1_saas")
                || lower.contains("s3_permissions")
                || lower.contains("s4_order_processing")
            {
                saw_foreground_symbol = true;
            }
        }
    }

    if saw_foreground_symbol {
        ThreadClass::Foreground
    } else {
        ThreadClass::Other
    }
}

#[cfg(feature = "profiling")]
fn thread_class_name(class: ThreadClass) -> &'static str {
    match class {
        ThreadClass::Foreground => "foreground",
        ThreadClass::RocksDbBackground => "rocksdb_background",
        ThreadClass::Other => "other",
    }
}

#[cfg(feature = "profiling")]
fn sample_pct(samples: isize, total: isize) -> f64 {
    if total == 0 {
        0.0
    } else {
        samples as f64 * 100.0 / total as f64
    }
}
