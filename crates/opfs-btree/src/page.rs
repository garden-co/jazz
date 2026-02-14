use crate::BTreeError;

pub(crate) type PageId = u64;

const PAGE_MAGIC: [u8; 4] = *b"OPPG";
const PAGE_HEADER_BYTES: usize = 24;

const KIND_INTERNAL: u8 = 1;
const KIND_LEAF: u8 = 2;
const KIND_OVERFLOW: u8 = 3;
const KIND_FREELIST: u8 = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ValueCell {
    Inline(Vec<u8>),
    Overflow {
        head_page_id: PageId,
        total_len: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Page {
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
    Leaf {
        entries: Vec<(Vec<u8>, ValueCell)>,
        next: Option<PageId>,
    },
    Overflow {
        data: Vec<u8>,
        next: Option<PageId>,
    },
    Freelist {
        ids: Vec<PageId>,
        next: Option<PageId>,
    },
}

struct EncodedFields {
    kind: u8,
    next_page_id: u64,
    item_count: u32,
    payload: Vec<u8>,
}

pub(crate) fn overflow_chunk_capacity(page_size: usize) -> Result<usize, BTreeError> {
    page_payload_capacity(page_size)
}

pub(crate) fn freelist_ids_per_page(page_size: usize) -> Result<usize, BTreeError> {
    let payload = page_payload_capacity(page_size)?;
    Ok(payload / 8)
}

pub(crate) fn page_fits(page: &Page, page_size: usize) -> Result<bool, BTreeError> {
    let encoded = encode_fields(page)?;
    Ok(encoded.payload.len() <= page_payload_capacity(page_size)?)
}

pub(crate) fn encode_page(page: &Page, page_size: usize) -> Result<Vec<u8>, BTreeError> {
    if page_size < PAGE_HEADER_BYTES {
        return Err(BTreeError::InvalidOptions(format!(
            "page_size {} is too small",
            page_size
        )));
    }

    let encoded = encode_fields(page)?;
    if encoded.payload.len() > page_payload_capacity(page_size)? {
        return Err(BTreeError::InvalidOptions(format!(
            "page payload {} exceeds page size {}",
            encoded.payload.len(),
            page_size
        )));
    }

    let mut raw = vec![0u8; page_size];
    raw[..4].copy_from_slice(&PAGE_MAGIC);
    raw[4] = encoded.kind;
    raw[5] = 0;
    raw[6] = 0;
    raw[7] = 0;
    raw[8..16].copy_from_slice(&encoded.next_page_id.to_le_bytes());
    raw[16..20].copy_from_slice(&encoded.item_count.to_le_bytes());
    raw[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + encoded.payload.len()]
        .copy_from_slice(&encoded.payload);

    let checksum = page_checksum(&raw);
    raw[20..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(raw)
}

pub(crate) fn decode_page(raw: &[u8], expected_page_size: usize) -> Result<Page, BTreeError> {
    if raw.len() != expected_page_size {
        return Err(BTreeError::Corrupt(format!(
            "page length mismatch: found {}, expected {}",
            raw.len(),
            expected_page_size
        )));
    }
    if raw.len() < PAGE_HEADER_BYTES {
        return Err(BTreeError::Corrupt("page too small".to_string()));
    }
    if raw[..4] != PAGE_MAGIC {
        return Err(BTreeError::Corrupt("page magic mismatch".to_string()));
    }

    let kind = raw[4];
    let next_page_id =
        u64::from_le_bytes(raw[8..16].try_into().expect("next page id header slice"));
    let item_count = u32::from_le_bytes(raw[16..20].try_into().expect("item count header slice"));
    let expected_checksum =
        u32::from_le_bytes(raw[20..24].try_into().expect("checksum header slice"));

    let payload = &raw[PAGE_HEADER_BYTES..];
    let mut cursor = 0usize;

    let decoded = match kind {
        KIND_INTERNAL => {
            let key_count = item_count as usize;
            let mut keys = Vec::with_capacity(key_count);
            for _ in 0..key_count {
                let key_len = take_u32(payload, &mut cursor, "internal key length")? as usize;
                let key = take_bytes(payload, &mut cursor, key_len, "internal key")?.to_vec();
                keys.push(key);
            }

            let child_count = key_count
                .checked_add(1)
                .ok_or_else(|| BTreeError::Corrupt("internal child count overflow".to_string()))?;
            let mut children = Vec::with_capacity(child_count);
            for _ in 0..child_count {
                children.push(take_u64(payload, &mut cursor, "internal child")?);
            }

            Page::Internal { keys, children }
        }
        KIND_LEAF => {
            let entry_count = item_count as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let key_len = take_u32(payload, &mut cursor, "leaf key length")? as usize;
                let key = take_bytes(payload, &mut cursor, key_len, "leaf key")?.to_vec();
                let tag = take_u8(payload, &mut cursor, "leaf value tag")?;
                let value = match tag {
                    0 => {
                        let value_len =
                            take_u32(payload, &mut cursor, "inline value length")? as usize;
                        let value =
                            take_bytes(payload, &mut cursor, value_len, "inline value")?.to_vec();
                        ValueCell::Inline(value)
                    }
                    1 => {
                        let head_page_id = take_u64(payload, &mut cursor, "overflow head")?;
                        let total_len = take_u32(payload, &mut cursor, "overflow length")?;
                        ValueCell::Overflow {
                            head_page_id,
                            total_len,
                        }
                    }
                    _ => {
                        return Err(BTreeError::Corrupt(format!(
                            "invalid leaf value tag {}",
                            tag
                        )));
                    }
                };
                entries.push((key, value));
            }

            Page::Leaf {
                entries,
                next: nonzero(next_page_id),
            }
        }
        KIND_OVERFLOW => {
            let data_len = item_count as usize;
            let data = take_bytes(payload, &mut cursor, data_len, "overflow payload")?.to_vec();
            Page::Overflow {
                data,
                next: nonzero(next_page_id),
            }
        }
        KIND_FREELIST => {
            let id_count = item_count as usize;
            let mut ids = Vec::with_capacity(id_count);
            for _ in 0..id_count {
                ids.push(take_u64(payload, &mut cursor, "freelist id")?);
            }
            Page::Freelist {
                ids,
                next: nonzero(next_page_id),
            }
        }
        _ => {
            return Err(BTreeError::Corrupt(format!("unknown page kind {}", kind)));
        }
    };

    let actual_checksum = page_checksum(raw);
    if expected_checksum != actual_checksum {
        return Err(BTreeError::Corrupt(format!(
            "page checksum mismatch: expected {}, got {}",
            expected_checksum, actual_checksum
        )));
    }

    Ok(decoded)
}

fn encode_fields(page: &Page) -> Result<EncodedFields, BTreeError> {
    match page {
        Page::Internal { keys, children } => {
            if children.len() != keys.len() + 1 {
                return Err(BTreeError::Corrupt(format!(
                    "internal children/key mismatch: children={}, keys={}",
                    children.len(),
                    keys.len()
                )));
            }

            let mut payload = Vec::new();
            for key in keys {
                let key_len = u32::try_from(key.len()).map_err(|_| {
                    BTreeError::InvalidOptions("internal key too large".to_string())
                })?;
                payload.extend_from_slice(&key_len.to_le_bytes());
                payload.extend_from_slice(key);
            }
            for child in children {
                payload.extend_from_slice(&child.to_le_bytes());
            }

            let item_count = u32::try_from(keys.len())
                .map_err(|_| BTreeError::InvalidOptions("too many internal keys".to_string()))?;
            Ok(EncodedFields {
                kind: KIND_INTERNAL,
                next_page_id: 0,
                item_count,
                payload,
            })
        }
        Page::Leaf { entries, next } => {
            let mut payload = Vec::new();
            for (key, value) in entries {
                let key_len = u32::try_from(key.len())
                    .map_err(|_| BTreeError::InvalidOptions("leaf key too large".to_string()))?;
                payload.extend_from_slice(&key_len.to_le_bytes());
                payload.extend_from_slice(key);

                match value {
                    ValueCell::Inline(v) => {
                        let value_len = u32::try_from(v.len()).map_err(|_| {
                            BTreeError::InvalidOptions("inline value too large".to_string())
                        })?;
                        payload.push(0);
                        payload.extend_from_slice(&value_len.to_le_bytes());
                        payload.extend_from_slice(v);
                    }
                    ValueCell::Overflow {
                        head_page_id,
                        total_len,
                    } => {
                        payload.push(1);
                        payload.extend_from_slice(&head_page_id.to_le_bytes());
                        payload.extend_from_slice(&total_len.to_le_bytes());
                    }
                }
            }

            let item_count = u32::try_from(entries.len())
                .map_err(|_| BTreeError::InvalidOptions("too many leaf entries".to_string()))?;
            Ok(EncodedFields {
                kind: KIND_LEAF,
                next_page_id: next.unwrap_or(0),
                item_count,
                payload,
            })
        }
        Page::Overflow { data, next } => {
            let item_count = u32::try_from(data.len())
                .map_err(|_| BTreeError::InvalidOptions("overflow chunk too large".to_string()))?;
            Ok(EncodedFields {
                kind: KIND_OVERFLOW,
                next_page_id: next.unwrap_or(0),
                item_count,
                payload: data.clone(),
            })
        }
        Page::Freelist { ids, next } => {
            let mut payload = Vec::new();
            for id in ids {
                payload.extend_from_slice(&id.to_le_bytes());
            }
            let item_count = u32::try_from(ids.len())
                .map_err(|_| BTreeError::InvalidOptions("too many freelist ids".to_string()))?;
            Ok(EncodedFields {
                kind: KIND_FREELIST,
                next_page_id: next.unwrap_or(0),
                item_count,
                payload,
            })
        }
    }
}

fn page_payload_capacity(page_size: usize) -> Result<usize, BTreeError> {
    page_size
        .checked_sub(PAGE_HEADER_BYTES)
        .ok_or_else(|| BTreeError::InvalidOptions(format!("page_size {} is too small", page_size)))
}

fn page_checksum(raw: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&raw[..20]);
    hasher.update(&raw[24..]);
    hasher.finalize()
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn take_u8(buf: &[u8], cursor: &mut usize, label: &str) -> Result<u8, BTreeError> {
    let bytes = take_bytes(buf, cursor, 1, label)?;
    Ok(bytes[0])
}

fn take_u32(buf: &[u8], cursor: &mut usize, label: &str) -> Result<u32, BTreeError> {
    let bytes = take_bytes(buf, cursor, 4, label)?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("u32 decode slice"),
    ))
}

fn take_u64(buf: &[u8], cursor: &mut usize, label: &str) -> Result<u64, BTreeError> {
    let bytes = take_bytes(buf, cursor, 8, label)?;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("u64 decode slice"),
    ))
}

fn take_bytes<'a>(
    buf: &'a [u8],
    cursor: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8], BTreeError> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| BTreeError::Corrupt(format!("{} cursor overflow", label)))?;
    if end > buf.len() {
        return Err(BTreeError::Corrupt(format!(
            "{} exceeds payload bounds",
            label
        )));
    }
    let bytes = &buf[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_page_round_trip() {
        let page = Page::Leaf {
            entries: vec![
                (b"a".to_vec(), ValueCell::Inline(b"1".to_vec())),
                (
                    b"b".to_vec(),
                    ValueCell::Overflow {
                        head_page_id: 44,
                        total_len: 999,
                    },
                ),
            ],
            next: Some(12),
        };

        let encoded = encode_page(&page, 4096).expect("encode leaf page");
        let decoded = decode_page(&encoded, 4096).expect("decode leaf page");
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_checksum_detects_corruption() {
        let page = Page::Overflow {
            data: vec![1, 2, 3, 4, 5],
            next: None,
        };

        let mut encoded = encode_page(&page, 4096).expect("encode overflow page");
        encoded[100] ^= 0xFF;

        let err = decode_page(&encoded, 4096).expect_err("must fail checksum");
        assert!(matches!(err, BTreeError::Corrupt(_)));
    }
}
