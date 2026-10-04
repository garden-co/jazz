//! Sampled heap profiling for the Linux server binary.
//!
//! `jazz-tools` on Linux wraps mimalloc in [`SamplingAllocator`], which
//! records the stack of one allocation per [`DEFAULT_SAMPLE_INTERVAL`] bytes
//! allocated on average (or per [`SAMPLE_INTERVAL_ENV`] bytes; `0` turns
//! sampling off), which shows which code holds memory when a server grows.
//! Stacks come from frame pointers, so a write-heavy load costs about the
//! same CPU with sampling as without. It only sees Rust's allocations:
//! memory that bundled C/C++ such as RocksDB takes from `malloc` directly is
//! not in the profile.
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

use jazz_server::profiling::{HeapProfileDump, HeapProfileError};

pub use sampler::{
    DEFAULT_SAMPLE_INTERVAL, LiveSample, SamplingAllocator, enable_sampling, for_each_live_sample,
    sample_interval, sampling_enabled,
};
use symbols::ExecutableSymbols;

/// Environment variable with the mean bytes allocated between samples;
/// `0` turns sampling off.
pub const SAMPLE_INTERVAL_ENV: &str = "JAZZ_HEAP_PROFILE_SAMPLE_BYTES";

/// Value of [`SAMPLE_INTERVAL_ENV`] that [`configure`] could not use.
static INVALID_SAMPLE_INTERVAL: OnceLock<String> = OnceLock::new();

/// Turn sampling on, every [`SAMPLE_INTERVAL_ENV`] bytes when it is set and
/// [`DEFAULT_SAMPLE_INTERVAL`] otherwise, unless it is `0`. Call it at the
/// start of `main`, before any other thread starts: threads that already
/// allocated never sample.
pub fn configure() {
    let mean = match std::env::var(SAMPLE_INTERVAL_ENV) {
        Err(_) => DEFAULT_SAMPLE_INTERVAL,
        Ok(bytes) => match bytes.parse::<u64>() {
            Ok(0) => return,
            Ok(bytes) => bytes,
            Err(_) => {
                let _ = INVALID_SAMPLE_INTERVAL.set(bytes);
                DEFAULT_SAMPLE_INTERVAL
            }
        },
    };
    enable_sampling(mean);
}

/// Report the sampling configuration and return the profile dumper for the
/// server. Only meaningful when [`SamplingAllocator`] is the global
/// allocator and [`configure`] ran first.
pub fn activate() -> HeapProfileDump {
    if let Some(bytes) = INVALID_SAMPLE_INTERVAL.get() {
        tracing::warn!("Ignoring {SAMPLE_INTERVAL_ENV}={bytes}: expected a byte count");
    }
    if sampling_enabled() {
        tracing::info!(
            "Heap profiling active, sampling every ~{} bytes allocated",
            sample_interval()
        );
    }
    dump_heap_profile
}

fn dump_heap_profile() -> Result<Vec<u8>, HeapProfileError> {
    if !sampling_enabled() {
        return Err(HeapProfileError::NotEnabled(format!(
            "Heap profiling is turned off on this server ({SAMPLE_INTERVAL_ENV}=0). \
             Restart it without {SAMPLE_INTERVAL_ENV}, or with the mean bytes \
             between samples, to sample the heap."
        )));
    }
    let mut samples = Vec::new();
    for_each_live_sample(|sample| {
        samples.push(pprof::HeapSample {
            weight: sample.weight,
            stack: sample.stack.to_vec(),
        });
    });
    let mappings = pprof::process_mappings().ok_or_else(|| {
        HeapProfileError::Failed("the process's loaded segments could not be read".to_owned())
    })?;
    // Mappings name the executable as it was invoked (relative, or through
    // `PATH`), so find it by an address inside it instead of by path.
    let here = dump_heap_profile as HeapProfileDump as usize;
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
