//! Function names for heap profile frames, read from the executable's ELF
//! symbol table.
//!
//! Resolving frames through DWARF, as `backtrace` does, keeps the parsed debug
//! info in memory for the rest of the process: ~200 MB on the first scrape of
//! a server that may already be short on memory. This index keeps only the
//! extent and name offset of each function (a few MB) and reads the names it
//! needs from disk on every dump. File and line come from offline
//! symbolization against the unstripped binary with the same build ID.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

const ELF_HEADER_LEN: usize = 64;
const SECTION_HEADER_LEN: usize = 64;
const SYMBOL_LEN: usize = 24;
const SHT_SYMTAB: u32 = 2;
const STT_FUNC: u8 = 2;
const MAX_NAME_LEN: usize = 4096;

/// Functions of one ELF file, keyed by their link-time virtual address.
pub(super) struct ExecutableSymbols {
    file: File,
    strtab_offset: u64,
    strtab_size: u64,
    /// Sorted by `start`.
    functions: Vec<FunctionSymbol>,
}

#[derive(Debug, Clone, Copy)]
struct FunctionSymbol {
    start: u64,
    end: u64,
    name: u32,
}

impl ExecutableSymbols {
    /// Index the function symbols of a 64-bit little-endian ELF file.
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let header = read_vec(&file, 0, ELF_HEADER_LEN)?;
        if &header[..4] != b"\x7fELF" || header[4] != 2 || header[5] != 1 {
            return Err(unsupported("not a 64-bit little-endian ELF file"));
        }
        let section_headers_offset = u64_at(&header, 0x28);
        let section_header_count = usize::from(u16_at(&header, 0x3c));

        let section_headers = read_vec(
            &file,
            section_headers_offset,
            section_header_count * SECTION_HEADER_LEN,
        )?;
        let sections: Vec<&[u8]> = section_headers.chunks_exact(SECTION_HEADER_LEN).collect();
        let symtab = sections
            .iter()
            .find(|section| u32_at(section, 4) == SHT_SYMTAB)
            .ok_or_else(|| unsupported("the executable has no symbol table"))?;
        let strtab = sections
            .get(u32_at(symtab, 40) as usize)
            .ok_or_else(|| unsupported("the symbol table links to no string table"))?;

        let mut functions = Vec::new();
        let (symtab_offset, symtab_size) = (u64_at(symtab, 24), u64_at(symtab, 32));
        const CHUNK: u64 = (SYMBOL_LEN * 4096) as u64;
        let mut position = 0;
        while position < symtab_size {
            let len = CHUNK.min(symtab_size - position);
            let chunk = read_vec(&file, symtab_offset + position, len as usize)?;
            for symbol in chunk.chunks_exact(SYMBOL_LEN) {
                let (name, info, section) = (u32_at(symbol, 0), symbol[4], u16_at(symbol, 6));
                let (value, size) = (u64_at(symbol, 8), u64_at(symbol, 16));
                if info & 0xf != STT_FUNC || section == 0 || size == 0 {
                    continue;
                }
                functions.push(FunctionSymbol {
                    start: value,
                    end: value + size,
                    name,
                });
            }
            position += len;
        }
        functions.sort_unstable_by_key(|function| function.start);
        functions.dedup_by_key(|function| function.start);
        functions.shrink_to_fit();

        Ok(Self {
            file,
            strtab_offset: u64_at(strtab, 24),
            strtab_size: u64_at(strtab, 32),
            functions,
        })
    }

    /// Demangled name of the function containing the link-time address
    /// `vaddr`.
    pub(super) fn function_at(&self, vaddr: u64) -> Option<String> {
        let index = self
            .functions
            .partition_point(|function| function.start <= vaddr)
            .checked_sub(1)?;
        let function = self.functions[index];
        if vaddr >= function.end {
            return None;
        }
        let raw = self.read_name(function.name)?;
        Some(match rustc_demangle::try_demangle(&raw) {
            Ok(demangled) => format!("{demangled:#}"),
            Err(_) => raw,
        })
    }

    fn read_name(&self, name: u32) -> Option<String> {
        let start = u64::from(name);
        let available = self.strtab_size.checked_sub(start)?;
        let len = (MAX_NAME_LEN as u64).min(available) as usize;
        let bytes = read_vec(&self.file, self.strtab_offset + start, len).ok()?;
        let end = bytes.iter().position(|&byte| byte == 0)?;
        Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
    }
}

fn read_vec(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0; len];
    file.read_exact_at(&mut buffer, offset)?;
    Ok(buffer)
}

fn unsupported(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap())
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    //! Unit-level because the symbolizer's contract (a runtime address maps
    //! back to its function through the loaded segment's address) is only
    //! observable through a profile otherwise, where a miss just shows as an
    //! unnamed frame.

    use super::*;

    #[inline(never)]
    fn heap_profiling_symbol_marker() -> usize {
        std::hint::black_box(7)
    }

    /// A function of this test binary resolves to its demangled Rust path
    /// through the link-time address its runtime address maps back to.
    #[test]
    fn resolves_a_function_of_the_running_executable() {
        let symbols = ExecutableSymbols::open(Path::new("/proc/self/exe")).unwrap();
        let address = heap_profiling_symbol_marker as *const () as usize;
        let mapping = mappings::MAPPINGS
            .as_deref()
            .unwrap()
            .iter()
            .find(|mapping| (mapping.memory_start..mapping.memory_end).contains(&address))
            .unwrap();
        let vaddr = address - mapping.memory_start + mapping.memory_offset;

        let name = symbols.function_at(vaddr as u64 + 1).unwrap();

        assert_eq!(
            name,
            "jazz_cli::heap_profiling::symbols::tests::heap_profiling_symbol_marker"
        );
        assert_eq!(heap_profiling_symbol_marker(), 7);
    }
}
