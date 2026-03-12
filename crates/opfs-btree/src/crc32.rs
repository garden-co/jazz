const IEEE_POLYNOMIAL: u32 = 0xEDB8_8320;

// `static` avoids large table copies in debug builds when indexing.
#[cfg(any(target_arch = "wasm32", test))]
static CRC32_TABLES: [[u32; 256]; 16] = build_crc32_tables();

#[cfg(all(not(target_arch = "wasm32"), not(test)))]
static CRC32_TABLE: [u32; 256] = build_crc32_table();

const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < table.len() {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ IEEE_POLYNOMIAL
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

#[cfg(any(target_arch = "wasm32", test))]
const fn build_crc32_tables() -> [[u32; 256]; 16] {
    let mut tables = [[0u32; 256]; 16];
    tables[0] = build_crc32_table();

    let mut table = 1;
    while table < tables.len() {
        let mut i = 0;
        while i < tables[table].len() {
            let crc = tables[table - 1][i];
            tables[table][i] = (crc >> 8) ^ tables[0][(crc & 0xFF) as usize];
            i += 1;
        }
        table += 1;
    }

    tables
}

#[inline]
#[cfg(any(target_arch = "wasm32", test))]
fn update_chunk_16(crc: u32, bytes: &[u8]) -> u32 {
    debug_assert!(bytes.len() >= 16);

    CRC32_TABLES[0x0][bytes[0xf] as usize]
        ^ CRC32_TABLES[0x1][bytes[0xe] as usize]
        ^ CRC32_TABLES[0x2][bytes[0xd] as usize]
        ^ CRC32_TABLES[0x3][bytes[0xc] as usize]
        ^ CRC32_TABLES[0x4][bytes[0xb] as usize]
        ^ CRC32_TABLES[0x5][bytes[0xa] as usize]
        ^ CRC32_TABLES[0x6][bytes[0x9] as usize]
        ^ CRC32_TABLES[0x7][bytes[0x8] as usize]
        ^ CRC32_TABLES[0x8][bytes[0x7] as usize]
        ^ CRC32_TABLES[0x9][bytes[0x6] as usize]
        ^ CRC32_TABLES[0xa][bytes[0x5] as usize]
        ^ CRC32_TABLES[0xb][bytes[0x4] as usize]
        ^ CRC32_TABLES[0xc][bytes[0x3] as usize ^ ((crc >> 0x18) & 0xFF) as usize]
        ^ CRC32_TABLES[0xd][bytes[0x2] as usize ^ ((crc >> 0x10) & 0xFF) as usize]
        ^ CRC32_TABLES[0xe][bytes[0x1] as usize ^ ((crc >> 0x08) & 0xFF) as usize]
        ^ CRC32_TABLES[0xf][bytes[0x0] as usize ^ (crc & 0xFF) as usize]
}

#[inline]
#[cfg(any(target_arch = "wasm32", test))]
fn update_state_fast_16(mut crc: u32, mut bytes: &[u8]) -> u32 {
    while bytes.len() >= 64 {
        crc = update_chunk_16(crc, &bytes[..16]);
        crc = update_chunk_16(crc, &bytes[16..32]);
        crc = update_chunk_16(crc, &bytes[32..48]);
        crc = update_chunk_16(crc, &bytes[48..64]);
        bytes = &bytes[64..];
    }

    while bytes.len() >= 16 {
        crc = update_chunk_16(crc, &bytes[..16]);
        bytes = &bytes[16..];
    }

    update_state_slow(crc, bytes)
}

#[inline]
fn update_state_slow(mut crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(any(target_arch = "wasm32", test))]
    let table = &CRC32_TABLES[0];
    #[cfg(all(not(target_arch = "wasm32"), not(test)))]
    let table = &CRC32_TABLE;

    for &byte in bytes {
        crc = table[((crc as u8) ^ byte) as usize] ^ (crc >> 8);
    }
    crc
}

#[inline]
#[cfg(target_arch = "wasm32")]
fn update_state(crc: u32, bytes: &[u8]) -> u32 {
    update_state_fast_16(crc, bytes)
}

#[inline]
#[cfg(not(target_arch = "wasm32"))]
fn update_state(crc: u32, bytes: &[u8]) -> u32 {
    update_state_slow(crc, bytes)
}

#[inline]
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    !update_state(!0, bytes)
}

#[inline]
pub(crate) fn update(crc: u32, bytes: &[u8]) -> u32 {
    !update_state(!crc, bytes)
}

#[cfg(test)]
mod tests {
    use super::{hash, update, update_state_fast_16, update_state_slow};

    fn finalize_state(state: u32) -> u32 {
        !state
    }

    #[test]
    fn crc32_matches_standard_test_vector() {
        assert_eq!(hash(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn incremental_updates_match_one_shot_hash() {
        let full = hash(b"hello world");
        let partial = update(update(0, b"hello "), b"world");
        assert_eq!(partial, full);
    }

    #[test]
    fn split_updates_match_one_shot_hash() {
        let full = hash(b"page-headerpayload");
        let split = update(update(0, b"page-header"), b"payload");
        assert_eq!(split, full);
    }

    #[test]
    fn fast_16_matches_slow_for_varied_lengths_and_seeds() {
        let mut bytes = [0u8; 257];
        for (idx, byte) in bytes.iter_mut().enumerate() {
            *byte = (idx as u8).wrapping_mul(17).wrapping_add(31);
        }

        for seed in [0, 1, 0x1234_5678, 0xFFFF_FFFF] {
            for len in 0..=bytes.len() {
                let slow = finalize_state(update_state_slow(!seed, &bytes[..len]));
                let fast = finalize_state(update_state_fast_16(!seed, &bytes[..len]));
                assert_eq!(fast, slow, "seed={seed:#010x} len={len}");
            }
        }
    }

    #[test]
    fn fast_16_matches_slow_across_incremental_chunks() {
        let mut bytes = [0u8; 513];
        for (idx, byte) in bytes.iter_mut().enumerate() {
            *byte = (idx as u8).wrapping_mul(29).wrapping_add(7);
        }

        for split in 0..=128 {
            let slow = finalize_state(update_state_slow(
                update_state_slow(!0, &bytes[..split]),
                &bytes[split..],
            ));
            let fast = finalize_state(update_state_fast_16(
                update_state_fast_16(!0, &bytes[..split]),
                &bytes[split..],
            ));
            assert_eq!(fast, slow, "split={split}");
        }
    }
}
