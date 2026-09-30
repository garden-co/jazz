//! Always-on sampled heap profiling for the Linux server binary.
//!
//! `jazz-tools` on Linux wraps mimalloc in [`SamplingAllocator`], which
//! records the stack of one allocation per ~512 KiB allocated on average.
//! That keeps the cost low enough to leave on in production while still
//! showing which code holds memory when a server grows. It only sees Rust's
//! allocations: memory that bundled C/C++ such as RocksDB takes from
//! `malloc` directly is not in the profile.
//!
//! Profiles name functions from the executable's symbol table but carry no
//! file or line: DWARF-based symbolization would keep hundreds of MB of
//! parsed debug info resident in the server. Each mapping carries its build
//! ID instead, so pprof or a continuous profiler adds file and line offline
//! from the unstripped binary the release workflow keeps.

mod pprof;
mod sampler;
mod symbols;

use std::path::Path;
use std::sync::OnceLock;

use jazz_server::profiling::HeapProfileDump;

pub use sampler::{
    DEFAULT_SAMPLE_INTERVAL, LiveSample, SamplingAllocator, for_each_live_sample, sample_interval,
    set_sample_interval,
};
use symbols::ExecutableSymbols;

/// Environment variable overriding the mean bytes allocated between samples.
pub const SAMPLE_INTERVAL_ENV: &str = "JAZZ_HEAP_PROFILE_SAMPLE_BYTES";

/// Apply the sampling configuration and return the profile dumper for the
/// server. Only meaningful when [`SamplingAllocator`] is the global
/// allocator.
pub fn activate() -> HeapProfileDump {
    if let Ok(bytes) = std::env::var(SAMPLE_INTERVAL_ENV) {
        match bytes.parse::<u64>() {
            Ok(bytes) if bytes > 0 => set_sample_interval(bytes),
            _ => tracing::warn!(
                "Ignoring {SAMPLE_INTERVAL_ENV}={bytes}: expected a positive byte count"
            ),
        }
    }
    tracing::info!(
        "Heap profiling active, sampling every ~{} bytes allocated",
        sample_interval()
    );
    dump_heap_profile
}

fn dump_heap_profile() -> Result<Vec<u8>, String> {
    let mut samples = Vec::new();
    for_each_live_sample(|sample| {
        samples.push(pprof::HeapSample {
            weight: sample.weight,
            stack: sample.stack.to_vec(),
        });
    });
    let mappings =
        pprof::process_mappings().ok_or("the process's loaded segments could not be read")?;
    // Mappings name the executable as it was invoked (relative, or through
    // `PATH`), so find it by an address inside it instead of by path.
    let here = dump_heap_profile as fn() -> Result<Vec<u8>, String> as usize;
    let executable_path = mappings
        .iter()
        .find(|mapping| (mapping.memory_start..mapping.memory_end).contains(&here))
        .map(|mapping| mapping.pathname.clone());
    let executable = executable_path.as_deref().zip(executable_symbols());
    Ok(pprof::encode(
        &samples,
        &mappings,
        sample_interval() as i64,
        executable,
    ))
}

/// Symbols of the running executable, indexed on the first dump.
fn executable_symbols() -> Option<&'static ExecutableSymbols> {
    static SYMBOLS: OnceLock<Option<ExecutableSymbols>> = OnceLock::new();
    SYMBOLS
        .get_or_init(|| {
            // The file the executable was started from may have been
            // replaced since; `/proc/self/exe` is always the running one.
            match ExecutableSymbols::open(Path::new("/proc/self/exe")) {
                Ok(symbols) => Some(symbols),
                Err(error) => {
                    tracing::warn!("Heap profiles will have no function names: {error}");
                    None
                }
            }
        })
        .as_ref()
}
