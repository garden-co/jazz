//! Encoding of the sampled live heap as a gzipped pprof protobuf.
//!
//! Locations carry their runtime address, and mappings carry the runtime
//! range, file offset, path and build ID of each loaded segment, so
//! `go tool pprof` and continuous profilers can add file and line offline
//! from the unstripped binary with the same build ID. Frames in the
//! executable itself also get their function name from its symbol table.

use std::collections::HashMap;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message;

use super::symbols::ExecutableSymbols;

/// A live sampled allocation.
pub(super) struct HeapSample {
    /// Estimated bytes in use this sample stands for.
    pub weight: f64,
    /// Instruction pointers, innermost first; all but the first are return
    /// addresses.
    pub stack: Vec<usize>,
}

/// A loaded segment of the executable or a shared library.
pub(super) struct SegmentMapping {
    pub memory_start: usize,
    pub memory_end: usize,
    /// Link-time virtual address of `memory_start`.
    pub memory_offset: usize,
    pub file_offset: u64,
    pub pathname: std::path::PathBuf,
    pub build_id: Option<String>,
}

/// The segments loaded into this process, or `None` if they could not be
/// read.
pub(super) fn process_mappings() -> Option<Vec<SegmentMapping>> {
    let mappings = mappings::MAPPINGS.as_deref()?;
    Some(
        mappings
            .iter()
            .map(|mapping| SegmentMapping {
                memory_start: mapping.memory_start,
                memory_end: mapping.memory_end,
                memory_offset: mapping.memory_offset,
                file_offset: mapping.file_offset,
                pathname: mapping.pathname.clone(),
                build_id: mapping.build_id.as_ref().map(ToString::to_string),
            })
            .collect(),
    )
}

/// Gzipped `inuse_space` profile of `samples`, taken once per `period` bytes
/// allocated on average. `executable` names the frames in the mappings whose
/// path is its path.
pub(super) fn encode(
    samples: &[HeapSample],
    mappings: &[SegmentMapping],
    period: i64,
    executable: Option<(&std::path::Path, &ExecutableSymbols)>,
) -> Vec<u8> {
    let mut strings = StringTable::default();
    let mut out = Profile {
        sample_type: vec![ValueType {
            r#type: strings.index("inuse_space"),
            unit: strings.index("bytes"),
        }],
        period_type: Some(ValueType {
            r#type: strings.index("space"),
            unit: strings.index("bytes"),
        }),
        period,
        time_nanos: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as i64),
        ..Default::default()
    };

    let mut symbolized = Vec::with_capacity(mappings.len());
    for (mapping, id) in mappings.iter().zip(1..) {
        let symbols = executable
            .filter(|(path, _)| mapping.pathname == *path)
            .map(|(_, symbols)| symbols);
        symbolized.push(symbols);
        out.mapping.push(Mapping {
            id,
            memory_start: mapping.memory_start as u64,
            memory_limit: mapping.memory_end as u64,
            file_offset: mapping.file_offset,
            filename: strings.index(&mapping.pathname.to_string_lossy()),
            build_id: mapping
                .build_id
                .as_deref()
                .map_or(0, |build_id| strings.index(build_id)),
            has_functions: symbols.is_some(),
        });
    }

    let mut frames: HashMap<usize, Frame> = HashMap::new();
    let mut function_ids = HashMap::new();
    for heap_sample in samples {
        let mut sample = Sample {
            value: vec![heap_sample.weight.trunc() as i64],
            ..Default::default()
        };
        let mut in_allocator = true;
        for &return_address in &heap_sample.stack {
            // Point into the call instruction rather than just after it, so
            // the frame resolves to the caller's line, as pprof expects.
            let address = return_address.saturating_sub(1);
            let frame = *frames.entry(address).or_insert_with(|| {
                let id = out.location.len() as u64 + 1;
                let mapping_index = mappings.iter().position(|mapping| {
                    (mapping.memory_start..mapping.memory_end).contains(&address)
                });
                let name = mapping_index.and_then(|index| {
                    let symbols = symbolized[index]?;
                    let mapping = &mappings[index];
                    let vaddr = address - mapping.memory_start + mapping.memory_offset;
                    symbols.function_at(vaddr as u64)
                });
                let allocator = name.as_deref().is_some_and(is_allocator_frame);
                let mut line = Vec::new();
                if let Some(name) = name {
                    let function_id = *function_ids.entry(name).or_insert_with_key(|name| {
                        let function_id = out.function.len() as u64 + 1;
                        let name = strings.index(name);
                        out.function.push(Function {
                            id: function_id,
                            name,
                            system_name: name,
                        });
                        function_id
                    });
                    line.push(Line { function_id });
                }
                out.location.push(Location {
                    id,
                    mapping_id: mapping_index.map_or(0, |index| index as u64 + 1),
                    address: address as u64,
                    line,
                });
                Frame {
                    location_id: id,
                    allocator,
                }
            });
            // Attribute each sample to the code that allocated, not to the
            // sampler and allocator shim that every sampled stack ends in.
            in_allocator &= frame.allocator;
            if !in_allocator {
                sample.location_id.push(frame.location_id);
            }
        }
        out.sample.push(sample);
    }
    out.string_table = strings.strings;

    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&out.encode_to_vec())
        .expect("writing to a Vec cannot fail");
    gzip.finish().expect("writing to a Vec cannot fail")
}

#[derive(Clone, Copy)]
struct Frame {
    location_id: u64,
    allocator: bool,
}

/// Frames of the sampler and of Rust's allocator shims at the leaf of a
/// sampled stack. The shims demangle as `__rustc::__rust_alloc` on current
/// toolchains and as bare `__rust_alloc` on older ones.
fn is_allocator_frame(name: &str) -> bool {
    name.starts_with("jazz_cli::heap_profiling::sampler::")
        || name.starts_with("<jazz_cli::heap_profiling::sampler::")
        || matches!(
            name.strip_prefix("__rustc::").unwrap_or(name),
            "__rust_alloc" | "__rust_alloc_zeroed" | "__rust_realloc"
        )
}

struct StringTable {
    strings: Vec<String>,
    indices: HashMap<String, i64>,
}

impl Default for StringTable {
    fn default() -> Self {
        // pprof requires the empty string at index 0.
        Self {
            strings: vec![String::new()],
            indices: HashMap::from([(String::new(), 0)]),
        }
    }
}

impl StringTable {
    fn index(&mut self, string: &str) -> i64 {
        if let Some(&index) = self.indices.get(string) {
            return index;
        }
        let index = self.strings.len() as i64;
        self.strings.push(string.to_owned());
        self.indices.insert(string.to_owned(), index);
        index
    }
}

// The subset of `perftools.profiles` (pprof's `profile.proto`) written here,
// with the field numbers of the upstream schema.

#[derive(Clone, PartialEq, Message)]
struct Profile {
    #[prost(message, repeated, tag = "1")]
    sample_type: Vec<ValueType>,
    #[prost(message, repeated, tag = "2")]
    sample: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    mapping: Vec<Mapping>,
    #[prost(message, repeated, tag = "4")]
    location: Vec<Location>,
    #[prost(message, repeated, tag = "5")]
    function: Vec<Function>,
    #[prost(string, repeated, tag = "6")]
    string_table: Vec<String>,
    #[prost(int64, tag = "9")]
    time_nanos: i64,
    #[prost(message, optional, tag = "11")]
    period_type: Option<ValueType>,
    #[prost(int64, tag = "12")]
    period: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ValueType {
    #[prost(int64, tag = "1")]
    r#type: i64,
    #[prost(int64, tag = "2")]
    unit: i64,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(uint64, repeated, tag = "1")]
    location_id: Vec<u64>,
    #[prost(int64, repeated, tag = "2")]
    value: Vec<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct Mapping {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(uint64, tag = "2")]
    memory_start: u64,
    #[prost(uint64, tag = "3")]
    memory_limit: u64,
    #[prost(uint64, tag = "4")]
    file_offset: u64,
    #[prost(int64, tag = "5")]
    filename: i64,
    #[prost(int64, tag = "6")]
    build_id: i64,
    #[prost(bool, tag = "7")]
    has_functions: bool,
}

#[derive(Clone, PartialEq, Message)]
struct Location {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(uint64, tag = "2")]
    mapping_id: u64,
    #[prost(uint64, tag = "3")]
    address: u64,
    #[prost(message, repeated, tag = "4")]
    line: Vec<Line>,
}

#[derive(Clone, PartialEq, Message)]
struct Line {
    #[prost(uint64, tag = "1")]
    function_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct Function {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(int64, tag = "2")]
    name: i64,
    #[prost(int64, tag = "3")]
    system_name: i64,
}

#[cfg(test)]
mod tests {
    //! Unit-level because which frames a sample keeps is only visible by
    //! decoding the profile; the process test only sees that names exist.

    use std::io::Read;
    use std::path::Path;

    use flate2::read::GzDecoder;

    use super::*;

    #[inline(never)]
    fn pprof_allocating_caller_marker() -> usize {
        std::hint::black_box(11)
    }

    fn decode(
        samples: &[HeapSample],
        mappings: &[SegmentMapping],
        symbols: &ExecutableSymbols,
    ) -> Profile {
        let exe = std::env::current_exe().unwrap();
        let gzipped = encode(samples, mappings, 1, Some((exe.as_path(), symbols)));
        let mut encoded = Vec::new();
        GzDecoder::new(gzipped.as_slice())
            .read_to_end(&mut encoded)
            .unwrap();
        Profile::decode(encoded.as_slice()).unwrap()
    }

    fn frame_names(profile: &Profile, sample: &Sample) -> Vec<String> {
        sample
            .location_id
            .iter()
            .map(|&id| {
                let location = &profile.location[id as usize - 1];
                location.line.first().map_or_else(String::new, |line| {
                    let function = &profile.function[line.function_id as usize - 1];
                    profile.string_table[function.name as usize].clone()
                })
            })
            .collect()
    }

    /// A sample taken inside the sampler, reached through Rust's allocation
    /// shim, is attributed to the function that allocated, and its mapping
    /// keeps the runtime range and file offset offline symbolizers need.
    #[test]
    fn samples_start_at_the_allocating_caller() {
        let symbols = ExecutableSymbols::open(Path::new("/proc/self/exe")).unwrap();
        let mappings = process_mappings().unwrap();
        let runtime_address = |vaddr: usize| {
            mappings
                .iter()
                .find(|mapping| {
                    let len = mapping.memory_end - mapping.memory_start;
                    (mapping.memory_offset..mapping.memory_offset + len).contains(&vaddr)
                })
                .map(|mapping| vaddr - mapping.memory_offset + mapping.memory_start)
                .unwrap()
        };
        let caller = pprof_allocating_caller_marker as *const () as usize;
        // Rust's allocation shim, under whatever name this toolchain
        // demangles it to.
        let shim = runtime_address(
            symbols
                .address_of("__rustc::__rust_alloc")
                .or_else(|| symbols.address_of("__rust_alloc"))
                .unwrap() as usize,
        );
        // Any function of the sampler; taking its address keeps it linked.
        std::hint::black_box(super::super::sample_interval as fn() -> u64);
        let sampler = runtime_address(
            symbols
                .address_of("jazz_cli::heap_profiling::sampler::sample_interval")
                .unwrap() as usize,
        );
        // Leaf first; +1 because stacks hold return addresses.
        let samples = [HeapSample {
            weight: 4096.0,
            stack: vec![sampler + 1, shim + 1, caller + 1],
        }];

        let decoded = decode(&samples, &mappings, &symbols);

        assert_eq!(
            frame_names(&decoded, &decoded.sample[0]),
            ["jazz_cli::heap_profiling::pprof::tests::pprof_allocating_caller_marker"]
        );
        assert_eq!(decoded.sample[0].value, [4096]);
        let location = &decoded.location[decoded.sample[0].location_id[0] as usize - 1];
        let mapping = &decoded.mapping[location.mapping_id as usize - 1];
        assert!((mapping.memory_start..mapping.memory_limit).contains(&(caller as u64)));
        assert_eq!(location.address, caller as u64);
        assert_eq!(pprof_allocating_caller_marker(), 11);
    }
}
