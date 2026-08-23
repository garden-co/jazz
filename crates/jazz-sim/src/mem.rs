//! Process memory measurement helpers for scenario benchmarks.

/// Return the process peak resident set size in bytes.
///
/// `getrusage(RUSAGE_SELF).ru_maxrss` is platform-specific: Linux reports
/// kibibytes, while macOS reports bytes. The benchmark JSON uses bytes on both.
pub fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return 0;
    }

    let max_rss = unsafe { usage.assume_init().ru_maxrss };
    let max_rss = u64::try_from(max_rss).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max_rss
    } else {
        max_rss.saturating_mul(1024)
    }
}
