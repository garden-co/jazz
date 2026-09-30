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
//!
//! Profiles name functions from the executable's symbol table but carry no
//! file or line: DWARF-based symbolization would keep hundreds of MB of
//! parsed debug info resident in the server. Each mapping carries its build
//! ID instead, so `go tool pprof` or a continuous profiler adds file and line
//! offline from the unstripped binary the release workflow keeps.

mod pprof;
mod symbols;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use jazz_server::profiling::HeapProfileDump;
use tikv_jemalloc_ctl::raw;

use symbols::ExecutableSymbols;

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
        "Heap profiling active, sampling every ~{} bytes allocated",
        1usize << lg_sample
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
    let (profile, lg_sample) = {
        let mut ctl = ctl.blocking_lock();
        let profile = ctl.dump_profile().map_err(|error| format!("{error:#}"))?;
        (profile, ctl.lg_sample())
    };
    let executable = executable_symbols().map(|(path, symbols)| (path.as_path(), symbols));
    Ok(pprof::encode(&profile, 1i64 << lg_sample, executable))
}

/// Symbols of the running executable, indexed on the first dump.
fn executable_symbols() -> Option<&'static (PathBuf, ExecutableSymbols)> {
    static SYMBOLS: OnceLock<Option<(PathBuf, ExecutableSymbols)>> = OnceLock::new();
    SYMBOLS
        .get_or_init(|| {
            // Mappings name the executable by `current_exe`, but the file at
            // that path may have been replaced since start; `/proc/self/exe`
            // is always the running one.
            let path = std::env::current_exe().ok()?;
            match ExecutableSymbols::open(Path::new("/proc/self/exe")) {
                Ok(symbols) => Some((path, symbols)),
                Err(error) => {
                    tracing::warn!("Heap profiles will have no function names: {error}");
                    None
                }
            }
        })
        .as_ref()
}
