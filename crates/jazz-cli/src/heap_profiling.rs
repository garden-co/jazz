//! Always-on sampled heap profiling for the Linux server binary.
//!
//! `jazz-tools` on Linux runs on jemalloc (see `bin/jazz-tools.rs`), which
//! records a stack trace for one allocation per ~512 KiB allocated on average
//! (`lg_prof_sample:19`). That keeps the cost low enough to leave on in
//! production while still showing which code holds memory when a server grows.
//! Because jemalloc also replaces `malloc` on Linux, allocations from bundled
//! C/C++ code such as RocksDB are sampled too.
//!
//! Sampling starts inactive and is switched on by [`activate`] only when
//! jemalloc was built with a real unwinder. Without one, jemalloc silently
//! falls back to walking frames with `__builtin_return_address`, which crashes
//! in code built without frame pointers. That happens when configure cannot
//! find `_Unwind_Backtrace`, for example under `cargo zigbuild`; the release
//! workflow links LLVM libunwind into jemalloc's configure checks to avoid it.

use jazz_server::profiling::HeapProfileDump;
use tikv_jemalloc_ctl::raw;

/// Switch on heap sampling and return the profile dumper for the server.
///
/// Returns `None`, after logging why, when this build cannot sample safely.
pub fn activate() -> Option<HeapProfileDump> {
    if let Err(reason) = activate_sampling() {
        tracing::warn!("Heap profiling is disabled: {reason}");
        return None;
    }
    // SAFETY: "prof.lg_sample" is documented as readable and returning size_t.
    let lg_sample: usize = unsafe { raw::read(b"prof.lg_sample\0") }.unwrap_or_default();
    tracing::info!(
        "Heap profiling active, sampling every ~{} KiB allocated",
        (1usize << lg_sample) / 1024
    );
    Some(dump_heap_profile)
}

fn activate_sampling() -> Result<(), String> {
    // SAFETY: the "config.*" and "opt.prof" keys are documented as readable
    // booleans.
    let (prof_enabled, libgcc, libunwind) = unsafe {
        (
            raw::read::<bool>(b"opt.prof\0"),
            raw::read::<bool>(b"config.prof_libgcc\0"),
            raw::read::<bool>(b"config.prof_libunwind\0"),
        )
    };
    let prof_enabled = prof_enabled.map_err(|error| format!("read opt.prof: {error}"))?;
    if !prof_enabled {
        return Err("jemalloc was started with prof:false".to_owned());
    }
    let has_unwinder = libgcc.unwrap_or(false) || libunwind.unwrap_or(false);
    if !has_unwinder {
        return Err("jemalloc was built without a libgcc or libunwind unwinder".to_owned());
    }
    let ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or("jemalloc profiling control is unavailable")?;
    let mut ctl = ctl
        .try_lock()
        .map_err(|_| "jemalloc profiling control is busy".to_owned())?;
    ctl.activate()
        .map_err(|error| format!("set prof.active: {error}"))
}

fn dump_heap_profile() -> Result<Vec<u8>, String> {
    let ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or("jemalloc profiling control is unavailable")?;
    ctl.blocking_lock()
        .dump_pprof()
        .map_err(|error| format!("{error:#}"))
}
