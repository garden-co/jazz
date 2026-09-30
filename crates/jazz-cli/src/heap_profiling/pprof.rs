//! Encoding of a jemalloc heap profile as a gzipped pprof protobuf.
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
use jemalloc_pprof::StackProfile;
use prost::Message;

use super::symbols::ExecutableSymbols;

/// Gzipped `inuse_space` profile of `profile`, sampled once per `period`
/// bytes allocated. `executable` names the frames of the mappings whose path
/// is `executable_path`.
pub(super) fn encode(
    profile: &StackProfile,
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

    let mut symbolized = Vec::with_capacity(profile.mappings.len());
    for (mapping, id) in profile.mappings.iter().zip(1..) {
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
                .as_ref()
                .map_or(0, |build_id| strings.index(&build_id.to_string())),
            has_functions: symbols.is_some(),
        });
    }

    let mut frames: HashMap<usize, Frame> = HashMap::new();
    let mut function_ids = HashMap::new();
    for (stack, _) in profile.iter() {
        let mut sample = Sample {
            value: vec![stack.weight.trunc() as i64],
            ..Default::default()
        };
        let mut in_allocator = true;
        // `parse_jeheap` stores stacks root first; pprof wants the leaf first.
        for &return_address in stack.addrs.iter().rev() {
            // Point into the call instruction rather than just after it, so
            // the frame resolves to the caller's line, as pprof expects.
            let address = return_address.saturating_sub(1);
            let frame = *frames.entry(address).or_insert_with(|| {
                let id = out.location.len() as u64 + 1;
                let mapping_index = profile.mappings.iter().position(|mapping| {
                    (mapping.memory_start..mapping.memory_end).contains(&address)
                });
                let name = mapping_index.and_then(|index| {
                    let symbols = symbolized[index]?;
                    let mapping = &profile.mappings[index];
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
            // Attribute each sample to the code that allocated, not to
            // jemalloc's sampling path that every sampled stack ends in.
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

/// Frames of jemalloc and the Rust allocator shims at the leaf of a sampled
/// stack: the sampling path (`prof_backtrace_impl`, `_rjem_je_prof_*`) and
/// the allocation entry points, which `unprefixed_malloc_on_supported_platforms`
/// exports under their libc names.
fn is_allocator_frame(name: &str) -> bool {
    name.starts_with("_rjem_")
        || name.starts_with("prof_")
        || name.starts_with("tikv_jemallocator::")
        || matches!(
            name,
            "malloc"
                | "calloc"
                | "realloc"
                | "posix_memalign"
                | "aligned_alloc"
                | "memalign"
                | "valloc"
                | "mallocx"
                | "rallocx"
                | "xallocx"
                | "do_rallocx"
                | "__rust_alloc"
                | "__rust_alloc_zeroed"
                | "__rust_realloc"
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
    use jemalloc_pprof::WeightedStack;

    use super::*;

    #[inline(never)]
    fn pprof_allocating_caller_marker() -> usize {
        std::hint::black_box(11)
    }

    fn decode(profile: &StackProfile, symbols: &ExecutableSymbols) -> Profile {
        let exe = std::env::current_exe().unwrap();
        let gzipped = encode(profile, 1, Some((exe.as_path(), symbols)));
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

    /// A sample taken inside `malloc` is attributed to the function that
    /// called it, and its mapping keeps the runtime range and file offset
    /// offline symbolizers need.
    #[test]
    fn samples_start_at_the_allocating_caller() {
        let symbols = ExecutableSymbols::open(Path::new("/proc/self/exe")).unwrap();
        let mut profile = StackProfile::default();
        for mapping in mappings::MAPPINGS.as_deref().unwrap() {
            profile.push_mapping(jemalloc_pprof::Mapping {
                memory_start: mapping.memory_start,
                memory_end: mapping.memory_end,
                memory_offset: mapping.memory_offset,
                file_offset: mapping.file_offset,
                pathname: mapping.pathname.clone(),
                build_id: None,
            });
        }
        let caller = pprof_allocating_caller_marker as *const () as usize;
        let malloc = libc::malloc as *const () as usize;
        // Root first, as `parse_jeheap` stores stacks; +1 because stacks
        // hold return addresses.
        profile.push_stack(
            WeightedStack {
                addrs: vec![caller + 1, malloc + 1],
                weight: 4096.0,
            },
            None,
        );

        let decoded = decode(&profile, &symbols);

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
