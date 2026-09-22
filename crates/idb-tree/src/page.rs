use std::collections::HashSet;

use xxhash_rust::xxh3::xxh3_64;

pub type PageId = u64;

const MAGIC: &[u8; 8] = b"IDBTREE\0";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1 + 8;

const PAGE_LEAF_TAG: u8 = 0;
const PAGE_INTERNAL_TAG: u8 = 1;
const PAGE_OVERFLOW_TAG: u8 = 2;

const VALUE_INLINE_TAG: u8 = 0;
const VALUE_OVERFLOW_TAG: u8 = 1;

const OPTION_NONE_TAG: u8 = 0;
const OPTION_SOME_TAG: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Page {
    Leaf {
        entries: Vec<(Vec<u8>, ValueCell)>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
    Overflow {
        next: Option<PageId>,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueCell {
    Inline(Vec<u8>),
    /// `len` is a durable logical byte count, not a host allocation size.
    /// Keep it fixed-width even on wasm32; materialization, rather than page
    /// decoding, is the only place a host-sized allocation can be relevant.
    Overflow {
        head: PageId,
        len: u64,
    },
}

impl Page {
    pub fn leaf() -> Self {
        Self::Leaf {
            entries: Vec::new(),
        }
    }
}

/// Encode one page using the fixed IDBTree v1 storage format.
///
/// All integers are little-endian. Page, value-cell, and option tags are the
/// permanent constants above; collections and byte strings carry `u32`
/// lengths, while page IDs and logical overflow lengths are `u64`.
pub fn encode_page(page: &Page) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    match page {
        Page::Leaf { entries } => {
            validate_strictly_increasing(
                entries.iter().map(|(key, _)| key.as_slice()),
                "leaf keys",
            )?;
            put_u8(&mut payload, PAGE_LEAF_TAG);
            put_len(&mut payload, entries.len(), "leaf entry count")?;
            for (key, value) in entries {
                put_bytes(&mut payload, key, "leaf key")?;
                encode_value_cell(&mut payload, value)?;
            }
        }
        Page::Internal { keys, children } => {
            validate_internal_shape(keys, children)?;
            put_u8(&mut payload, PAGE_INTERNAL_TAG);
            put_byte_strings(&mut payload, keys, "internal keys")?;
            put_len(&mut payload, children.len(), "internal child count")?;
            for child in children {
                put_u64(&mut payload, *child);
            }
        }
        Page::Overflow { next, bytes } => {
            if bytes.is_empty() {
                return Err("overflow page bytes must not be empty".to_owned());
            }
            put_u8(&mut payload, PAGE_OVERFLOW_TAG);
            match next {
                None => put_u8(&mut payload, OPTION_NONE_TAG),
                Some(next) => {
                    put_u8(&mut payload, OPTION_SOME_TAG);
                    put_u64(&mut payload, *next);
                }
            }
            put_bytes(&mut payload, bytes, "overflow page bytes")?;
        }
    }

    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.push(FORMAT_VERSION);
    encoded.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub fn decode_page(bytes: &[u8]) -> Result<Page, String> {
    if bytes.len() < HEADER_LEN || &bytes[..MAGIC.len()] != MAGIC {
        return Err("bad magic".to_owned());
    }
    if bytes[MAGIC.len()] != FORMAT_VERSION {
        return Err(format!("unsupported format version {}", bytes[MAGIC.len()]));
    }
    let checksum_offset = MAGIC.len() + 1;
    let expected = u64::from_le_bytes(
        bytes[checksum_offset..HEADER_LEN]
            .try_into()
            .expect("checksum has fixed width"),
    );
    let payload = &bytes[HEADER_LEN..];
    if xxh3_64(payload) != expected {
        return Err("checksum mismatch".to_owned());
    }

    let mut decoder = Decoder::new(payload);
    let page = match decoder.u8("page tag")? {
        PAGE_LEAF_TAG => {
            let count = decoder.count("leaf entry count", 9)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decoder.bytes("leaf key")?;
                let value = decode_value_cell(&mut decoder)?;
                entries.push((key, value));
            }
            validate_strictly_increasing(
                entries.iter().map(|(key, _)| key.as_slice()),
                "leaf keys",
            )?;
            Page::Leaf { entries }
        }
        PAGE_INTERNAL_TAG => {
            let keys = decoder.byte_strings("internal keys")?;
            let child_count = decoder.count("internal child count", 8)?;
            let mut children = Vec::with_capacity(child_count);
            for _ in 0..child_count {
                children.push(decoder.u64("internal child page ID")?);
            }
            validate_internal_shape(&keys, &children)?;
            Page::Internal { keys, children }
        }
        PAGE_OVERFLOW_TAG => {
            let next = match decoder.u8("overflow next tag")? {
                OPTION_NONE_TAG => None,
                OPTION_SOME_TAG => Some(decoder.u64("overflow next page ID")?),
                tag => return Err(format!("unknown overflow next tag {tag}")),
            };
            let bytes = decoder.bytes("overflow page bytes")?;
            if bytes.is_empty() {
                return Err("overflow page bytes must not be empty".to_owned());
            }
            Page::Overflow { next, bytes }
        }
        tag => return Err(format!("unknown page tag {tag}")),
    };
    decoder.finish()?;
    Ok(page)
}

fn encode_value_cell(output: &mut Vec<u8>, value: &ValueCell) -> Result<(), String> {
    match value {
        ValueCell::Inline(bytes) => {
            put_u8(output, VALUE_INLINE_TAG);
            put_bytes(output, bytes, "inline value")
        }
        ValueCell::Overflow { head, len } => {
            if *len == 0 {
                return Err("overflow value length must not be zero".to_owned());
            }
            put_u8(output, VALUE_OVERFLOW_TAG);
            put_u64(output, *head);
            put_u64(output, *len);
            Ok(())
        }
    }
}

fn decode_value_cell(decoder: &mut Decoder<'_>) -> Result<ValueCell, String> {
    match decoder.u8("value-cell tag")? {
        VALUE_INLINE_TAG => Ok(ValueCell::Inline(decoder.bytes("inline value")?)),
        VALUE_OVERFLOW_TAG => {
            let head = decoder.u64("overflow value head")?;
            let encoded_len = decoder.u64("overflow value length")?;
            if encoded_len == 0 {
                return Err("overflow value length must not be zero".to_owned());
            }
            Ok(ValueCell::Overflow {
                head,
                len: encoded_len,
            })
        }
        tag => Err(format!("unknown value-cell tag {tag}")),
    }
}

fn validate_internal_shape(keys: &[Vec<u8>], children: &[PageId]) -> Result<(), String> {
    validate_strictly_increasing(keys.iter().map(Vec::as_slice), "internal keys")?;
    let expected_children = keys
        .len()
        .checked_add(1)
        .ok_or_else(|| "internal key count overflow".to_owned())?;
    if children.len() != expected_children {
        return Err(format!(
            "internal page has {} keys but {} children",
            keys.len(),
            children.len()
        ));
    }
    let mut seen = HashSet::with_capacity(children.len());
    if children.iter().any(|child| !seen.insert(*child)) {
        return Err("internal page has a shared child".to_owned());
    }
    Ok(())
}

fn validate_strictly_increasing<'a>(
    values: impl IntoIterator<Item = &'a [u8]>,
    name: &str,
) -> Result<(), String> {
    let mut previous: Option<&[u8]> = None;
    for value in values {
        if previous.is_some_and(|previous| previous >= value) {
            return Err(format!("{name} must be strictly increasing"));
        }
        previous = Some(value);
    }
    Ok(())
}

fn put_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_len(output: &mut Vec<u8>, len: usize, name: &str) -> Result<(), String> {
    put_u32(
        output,
        u32::try_from(len).map_err(|_| format!("{name} exceeds u32"))?,
    );
    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8], name: &str) -> Result<(), String> {
    put_len(output, bytes.len(), name)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn put_byte_strings(output: &mut Vec<u8>, values: &[Vec<u8>], name: &str) -> Result<(), String> {
    put_len(output, values.len(), name)?;
    for value in values {
        put_bytes(output, value, name)?;
    }
    Ok(())
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, len: usize, name: &str) -> Result<&'a [u8], String> {
        let Some((value, remaining)) = self.remaining.split_at_checked(len) else {
            return Err(format!("truncated {name}"));
        };
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self, name: &str) -> Result<u8, String> {
        Ok(self.take(1, name)?[0])
    }

    fn u32(&mut self, name: &str) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.take(4, name)?.try_into().expect("u32 has fixed width"),
        ))
    }

    fn u64(&mut self, name: &str) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.take(8, name)?.try_into().expect("u64 has fixed width"),
        ))
    }

    fn count(&mut self, name: &str, minimum_item_bytes: usize) -> Result<usize, String> {
        let count = usize::try_from(self.u32(name)?)
            .map_err(|_| format!("{name} exceeds this architecture"))?;
        if count > self.remaining.len() / minimum_item_bytes {
            return Err(format!("{name} exceeds the remaining payload"));
        }
        Ok(count)
    }

    fn bytes(&mut self, name: &str) -> Result<Vec<u8>, String> {
        let len = usize::try_from(self.u32(&format!("{name} length"))?)
            .map_err(|_| format!("{name} length exceeds this architecture"))?;
        Ok(self.take(len, name)?.to_vec())
    }

    fn byte_strings(&mut self, name: &str) -> Result<Vec<Vec<u8>>, String> {
        let count = self.count(&format!("{name} count"), 4)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.bytes(name)?);
        }
        Ok(values)
    }

    fn finish(self) -> Result<(), String> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err("trailing page payload bytes".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn envelope(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.push(FORMAT_VERSION);
        encoded.extend_from_slice(&xxh3_64(payload).to_le_bytes());
        encoded.extend_from_slice(payload);
        encoded
    }

    // These are intentionally internal codec tests: exact page bytes and
    // malformed-page rejection are below the public B-tree API. Public reopen
    // behavior remains covered by the engine contract tests in `lib.rs`.
    #[test]
    fn page_v1_encoding_has_golden_bytes_for_every_variant() {
        let fixtures = [
            Page::Leaf {
                entries: vec![
                    (b"a".to_vec(), ValueCell::Inline(b"xy".to_vec())),
                    (
                        b"b".to_vec(),
                        ValueCell::Overflow {
                            head: 0x0102_0304_0506_0708,
                            len: 9,
                        },
                    ),
                ],
            },
            Page::Internal {
                keys: vec![b"m".to_vec()],
                children: vec![3, 5],
            },
            Page::Overflow {
                next: Some(9),
                bytes: vec![0xaa, 0xbb],
            },
            Page::Overflow {
                next: None,
                bytes: vec![0xcc],
            },
            Page::Leaf {
                entries: vec![(
                    b"wide".to_vec(),
                    ValueCell::Overflow {
                        head: 17,
                        len: 0x1_0000_0000,
                    },
                )],
            },
        ];
        let encoded = fixtures
            .iter()
            .map(|page| encode_page(page).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            encoded
                .iter()
                .map(|bytes| to_hex(bytes))
                .collect::<Vec<_>>(),
            [
                "494442545245450001da359ee721015e03000200000001000000610002000000787901000000620108070605040302010900000000000000",
                "49444254524545000136071af932f362cf0101000000010000006d0200000003000000000000000500000000000000",
                "494442545245450001f716bc5b181bbaec0201090000000000000002000000aabb",
                "494442545245450001d6ccf3da52f0ff30020001000000cc",
                "494442545245450001f5cd4e6dc50a39de000100000004000000776964650111000000000000000000000001000000",
            ]
        );
        assert_eq!(
            to_hex(&encoded[4]),
            include_str!("../fixtures/page-v1-leaf.hex").trim()
        );
        for (page, encoded) in fixtures.iter().zip(encoded) {
            assert_eq!(decode_page(&encoded).unwrap(), *page);
        }
    }

    #[test]
    fn page_v1_decoder_rejects_noncanonical_or_malformed_payloads() {
        let valid = encode_page(&Page::Leaf {
            entries: Vec::new(),
        })
        .unwrap();

        let mut wrong_version = valid.clone();
        wrong_version[MAGIC.len()] = FORMAT_VERSION + 1;
        assert_eq!(
            decode_page(&wrong_version).unwrap_err(),
            "unsupported format version 2"
        );
        assert_eq!(
            decode_page(&envelope(&[9])).unwrap_err(),
            "unknown page tag 9"
        );
        assert!(decode_page(&envelope(&[PAGE_LEAF_TAG, 1, 0, 0, 0])).is_err());

        let mut bad_checksum = valid.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        assert_eq!(decode_page(&bad_checksum).unwrap_err(), "checksum mismatch");

        let unknown_value_tag = [PAGE_LEAF_TAG, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0];
        assert_eq!(
            decode_page(&envelope(&unknown_value_tag)).unwrap_err(),
            "unknown value-cell tag 9"
        );
        assert_eq!(
            decode_page(&envelope(&[PAGE_OVERFLOW_TAG, 9])).unwrap_err(),
            "unknown overflow next tag 9"
        );

        let invalid_internal = [PAGE_INTERNAL_TAG, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            decode_page(&envelope(&invalid_internal)).unwrap_err(),
            "internal page has 0 keys but 0 children"
        );

        let mut trailing = valid[HEADER_LEN..].to_vec();
        trailing.push(0);
        assert_eq!(
            decode_page(&envelope(&trailing)).unwrap_err(),
            "trailing page payload bytes"
        );

        let mut unordered_payload = encode_page(&Page::Leaf {
            entries: vec![
                (b"a".to_vec(), ValueCell::Inline(Vec::new())),
                (b"b".to_vec(), ValueCell::Inline(Vec::new())),
            ],
        })
        .unwrap()[HEADER_LEN..]
            .to_vec();
        unordered_payload[9] = b'b';
        unordered_payload[19] = b'a';
        assert_eq!(
            decode_page(&envelope(&unordered_payload)).unwrap_err(),
            "leaf keys must be strictly increasing"
        );

        let unordered = Page::Leaf {
            entries: vec![
                (b"b".to_vec(), ValueCell::Inline(Vec::new())),
                (b"a".to_vec(), ValueCell::Inline(Vec::new())),
            ],
        };
        assert_eq!(
            encode_page(&unordered).unwrap_err(),
            "leaf keys must be strictly increasing"
        );

        let bad_internal = Page::Internal {
            keys: vec![b"m".to_vec()],
            children: vec![3],
        };
        assert_eq!(
            encode_page(&bad_internal).unwrap_err(),
            "internal page has 1 keys but 1 children"
        );

        let shared_child = Page::Internal {
            keys: vec![b"m".to_vec()],
            children: vec![7, 7],
        };
        assert_eq!(
            encode_page(&shared_child).unwrap_err(),
            "internal page has a shared child"
        );
        let shared_child_payload = [
            PAGE_INTERNAL_TAG,
            1,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            b'm',
            2,
            0,
            0,
            0,
            7,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            7,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        assert_eq!(
            decode_page(&envelope(&shared_child_payload)).unwrap_err(),
            "internal page has a shared child"
        );
    }
}
