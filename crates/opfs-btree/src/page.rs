use crate::BTreeError;

pub(crate) type PageId = u64;

const PAGE_MAGIC: [u8; 4] = *b"OPPG";
const PAGE_HEADER_BYTES: usize = 24;

const KIND_INTERNAL: u8 = 1;
const KIND_LEAF: u8 = 2;
const KIND_OVERFLOW: u8 = 3;
const KIND_FREELIST: u8 = 4;

const INTERNAL_LEFT_CHILD_BYTES: usize = 8;
const INTERNAL_SLOT_BYTES: usize = 16; // key_off(u32), key_len(u32), right_child(u64)
const LEAF_SLOT_BYTES: usize = 12; // key_off(u32), key_len(u32), value_off(u32)

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageKind {
    Internal,
    Leaf,
    Overflow,
    Freelist,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ValueCell {
    Inline(Vec<u8>),
    Overflow {
        head_page_id: PageId,
        total_len: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueCellRef<'a> {
    Inline(&'a [u8]),
    Overflow {
        head_page_id: PageId,
        total_len: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OverflowRef {
    pub head_page_id: PageId,
    pub total_len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawLeafUpsertResult {
    Inserted,
    Updated { old_overflow: Option<OverflowRef> },
    NeedSplit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawLeafDeleteResult {
    NotFound,
    Deleted {
        old_overflow: Option<OverflowRef>,
        is_empty: bool,
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
    let header = parse_header(raw, expected_page_size, true)?;
    let payload = header.payload;

    match header.kind {
        PageKind::Internal => {
            let key_count = header.item_count as usize;
            let slots_bytes = internal_slots_bytes(key_count)?;
            if payload.len() < slots_bytes {
                return Err(BTreeError::Corrupt(
                    "internal page payload shorter than slot directory".to_string(),
                ));
            }

            let left_child = read_u64_at(payload, 0, "internal left child")?;
            let mut keys = Vec::with_capacity(key_count);
            let mut children = Vec::with_capacity(key_count.saturating_add(1));
            children.push(left_child);

            for idx in 0..key_count {
                let (key_off, key_len, right_child) = internal_slot(payload, key_count, idx)?;
                let key = slice_payload(payload, key_off, key_len, "internal key")?.to_vec();
                keys.push(key);
                children.push(right_child);
            }

            Ok(Page::Internal { keys, children })
        }
        PageKind::Leaf => {
            let entry_count = header.item_count as usize;
            let slots_bytes = leaf_slots_bytes(entry_count)?;
            if payload.len() < slots_bytes {
                return Err(BTreeError::Corrupt(
                    "leaf page payload shorter than slot directory".to_string(),
                ));
            }

            let mut entries = Vec::with_capacity(entry_count);
            for idx in 0..entry_count {
                let (key_off, key_len, value_off) = leaf_slot(payload, entry_count, idx)?;
                let key = slice_payload(payload, key_off, key_len, "leaf key")?.to_vec();
                let value = decode_leaf_value_cell_at(payload, value_off)?;
                entries.push((key, value));
            }

            Ok(Page::Leaf {
                entries,
                next: header.next_page_id,
            })
        }
        PageKind::Overflow => {
            let mut cursor = 0usize;
            let data_len = header.item_count as usize;
            let data = take_bytes(payload, &mut cursor, data_len, "overflow payload")?.to_vec();
            Ok(Page::Overflow {
                data,
                next: header.next_page_id,
            })
        }
        PageKind::Freelist => {
            let id_count = header.item_count as usize;
            let mut cursor = 0usize;
            let mut ids = Vec::with_capacity(id_count);
            for _ in 0..id_count {
                ids.push(take_u64(payload, &mut cursor, "freelist id")?);
            }
            Ok(Page::Freelist {
                ids,
                next: header.next_page_id,
            })
        }
    }
}

pub(crate) fn validate_page(raw: &[u8], expected_page_size: usize) -> Result<PageKind, BTreeError> {
    let header = parse_header(raw, expected_page_size, true)?;
    Ok(header.kind)
}

pub(crate) fn raw_page_kind(raw: &[u8], expected_page_size: usize) -> Result<PageKind, BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    Ok(header.kind)
}

pub(crate) fn raw_internal_child_for_key(
    raw: &[u8],
    expected_page_size: usize,
    key: &[u8],
) -> Result<PageId, BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    if header.kind != PageKind::Internal {
        return Err(BTreeError::Corrupt("expected internal page".to_string()));
    }

    let key_count = header.item_count as usize;
    let slots_bytes = internal_slots_bytes(key_count)?;
    if header.payload.len() < slots_bytes {
        return Err(BTreeError::Corrupt(
            "internal page payload shorter than slot directory".to_string(),
        ));
    }

    let mut lo = 0usize;
    let mut hi = key_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (key_off, key_len, _) = internal_slot(header.payload, key_count, mid)?;
        let current_key = slice_payload(header.payload, key_off, key_len, "internal key")?;
        if current_key <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if lo == 0 {
        return read_u64_at(header.payload, 0, "internal left child");
    }

    let (_, _, right_child) = internal_slot(header.payload, key_count, lo - 1)?;
    Ok(right_child)
}

pub(crate) fn raw_leaf_find_value<'a>(
    raw: &'a [u8],
    expected_page_size: usize,
    key: &[u8],
) -> Result<Option<ValueCellRef<'a>>, BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    if header.kind != PageKind::Leaf {
        return Err(BTreeError::Corrupt("expected leaf page".to_string()));
    }

    let entry_count = header.item_count as usize;
    let slots_bytes = leaf_slots_bytes(entry_count)?;
    if header.payload.len() < slots_bytes {
        return Err(BTreeError::Corrupt(
            "leaf page payload shorter than slot directory".to_string(),
        ));
    }

    let mut lo = 0usize;
    let mut hi = entry_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (key_off, key_len, _) = leaf_slot(header.payload, entry_count, mid)?;
        let current_key = slice_payload(header.payload, key_off, key_len, "leaf key")?;
        match current_key.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                let (_, _, value_off) = leaf_slot(header.payload, entry_count, mid)?;
                let value = parse_leaf_value_cell_at(header.payload, value_off)?;
                return Ok(Some(value));
            }
        }
    }

    Ok(None)
}

pub(crate) fn raw_leaf_scan<'a>(
    raw: &'a [u8],
    expected_page_size: usize,
    start: &[u8],
    end: &[u8],
    limit: usize,
    mut visit: impl FnMut(&'a [u8], ValueCellRef<'a>) -> Result<(), BTreeError>,
) -> Result<Option<PageId>, BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    if header.kind != PageKind::Leaf {
        return Err(BTreeError::Corrupt("expected leaf page".to_string()));
    }
    if limit == 0 {
        return Ok(header.next_page_id);
    }

    let entry_count = header.item_count as usize;
    let slots_bytes = leaf_slots_bytes(entry_count)?;
    if header.payload.len() < slots_bytes {
        return Err(BTreeError::Corrupt(
            "leaf page payload shorter than slot directory".to_string(),
        ));
    }

    let mut lo = 0usize;
    let mut hi = entry_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (key_off, key_len, _) = leaf_slot(header.payload, entry_count, mid)?;
        let current_key = slice_payload(header.payload, key_off, key_len, "leaf key")?;
        if current_key < start {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let mut emitted = 0usize;
    let mut idx = lo;
    while idx < entry_count && emitted < limit {
        let (key_off, key_len, value_off) = leaf_slot(header.payload, entry_count, idx)?;
        let key = slice_payload(header.payload, key_off, key_len, "leaf key")?;
        if key >= end {
            break;
        }
        let value = parse_leaf_value_cell_at(header.payload, value_off)?;
        visit(key, value)?;
        emitted += 1;
        idx += 1;
    }

    Ok(header.next_page_id)
}

pub(crate) fn raw_leaf_upsert_in_place(
    raw: &mut [u8],
    expected_page_size: usize,
    key: &[u8],
    value: &ValueCell,
) -> Result<RawLeafUpsertResult, BTreeError> {
    let (new_payload, new_count, result) = {
        let header = parse_header(raw, expected_page_size, false)?;
        if header.kind != PageKind::Leaf {
            return Err(BTreeError::Corrupt("expected leaf page".to_string()));
        }

        let entry_count = header.item_count as usize;
        let slots_bytes = leaf_slots_bytes(entry_count)?;
        if header.payload.len() < slots_bytes {
            return Err(BTreeError::Corrupt(
                "leaf page payload shorter than slot directory".to_string(),
            ));
        }

        let pos = leaf_search_position(header.payload, entry_count, key)?;
        let (replace_idx, insert_idx, old_overflow, new_count) = match pos {
            Ok(idx) => (
                Some(idx),
                idx,
                Some(leaf_entry(header.payload, entry_count, idx)?.1)
                    .and_then(overflow_from_value_ref),
                entry_count,
            ),
            Err(idx) => (None, idx, None, entry_count.saturating_add(1)),
        };

        let new_slots_bytes = leaf_slots_bytes(new_count)?;
        let mut slots = Vec::with_capacity(new_slots_bytes);
        let mut data = Vec::new();
        let new_value_ref = value_cell_as_ref(value);

        if let Some(replace_idx) = replace_idx {
            for idx in 0..entry_count {
                let (entry_key, entry_value) = leaf_entry(header.payload, entry_count, idx)?;
                if idx == replace_idx {
                    push_leaf_slot_entry(
                        &mut slots,
                        &mut data,
                        new_slots_bytes,
                        entry_key,
                        new_value_ref,
                    )?;
                } else {
                    push_leaf_slot_entry(
                        &mut slots,
                        &mut data,
                        new_slots_bytes,
                        entry_key,
                        entry_value,
                    )?;
                }
            }
        } else {
            for out_idx in 0..new_count {
                if out_idx == insert_idx {
                    push_leaf_slot_entry(
                        &mut slots,
                        &mut data,
                        new_slots_bytes,
                        key,
                        new_value_ref,
                    )?;
                    continue;
                }
                let src_idx = if out_idx < insert_idx {
                    out_idx
                } else {
                    out_idx.saturating_sub(1)
                };
                let (entry_key, entry_value) = leaf_entry(header.payload, entry_count, src_idx)?;
                push_leaf_slot_entry(
                    &mut slots,
                    &mut data,
                    new_slots_bytes,
                    entry_key,
                    entry_value,
                )?;
            }
        }

        let mut new_payload = Vec::with_capacity(slots.len().saturating_add(data.len()));
        new_payload.extend_from_slice(&slots);
        new_payload.extend_from_slice(&data);

        let result = if replace_idx.is_some() {
            RawLeafUpsertResult::Updated { old_overflow }
        } else {
            RawLeafUpsertResult::Inserted
        };

        (new_payload, new_count, result)
    };

    if new_payload.len() > page_payload_capacity(expected_page_size)? {
        return Ok(RawLeafUpsertResult::NeedSplit);
    }
    write_leaf_payload(raw, expected_page_size, new_count as u32, &new_payload)?;
    Ok(result)
}

pub(crate) fn raw_leaf_delete_in_place(
    raw: &mut [u8],
    expected_page_size: usize,
    key: &[u8],
) -> Result<RawLeafDeleteResult, BTreeError> {
    let (new_payload, new_count, old_overflow) = {
        let header = parse_header(raw, expected_page_size, false)?;
        if header.kind != PageKind::Leaf {
            return Err(BTreeError::Corrupt("expected leaf page".to_string()));
        }

        let entry_count = header.item_count as usize;
        let slots_bytes = leaf_slots_bytes(entry_count)?;
        if header.payload.len() < slots_bytes {
            return Err(BTreeError::Corrupt(
                "leaf page payload shorter than slot directory".to_string(),
            ));
        }

        let delete_idx = match leaf_search_position(header.payload, entry_count, key)? {
            Ok(idx) => idx,
            Err(_) => return Ok(RawLeafDeleteResult::NotFound),
        };
        let (_, old_value) = leaf_entry(header.payload, entry_count, delete_idx)?;
        let old_overflow = overflow_from_value_ref(old_value);

        let new_count = entry_count.saturating_sub(1);
        let new_slots_bytes = leaf_slots_bytes(new_count)?;
        let mut slots = Vec::with_capacity(new_slots_bytes);
        let mut data = Vec::new();

        for idx in 0..entry_count {
            if idx == delete_idx {
                continue;
            }
            let (entry_key, entry_value) = leaf_entry(header.payload, entry_count, idx)?;
            push_leaf_slot_entry(
                &mut slots,
                &mut data,
                new_slots_bytes,
                entry_key,
                entry_value,
            )?;
        }

        let mut new_payload = Vec::with_capacity(slots.len().saturating_add(data.len()));
        new_payload.extend_from_slice(&slots);
        new_payload.extend_from_slice(&data);
        (new_payload, new_count, old_overflow)
    };

    write_leaf_payload(raw, expected_page_size, new_count as u32, &new_payload)?;
    Ok(RawLeafDeleteResult::Deleted {
        old_overflow,
        is_empty: new_count == 0,
    })
}

pub(crate) fn raw_overflow_chunk(
    raw: &[u8],
    expected_page_size: usize,
) -> Result<(&[u8], Option<PageId>), BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    if header.kind != PageKind::Overflow {
        return Err(BTreeError::Corrupt("expected overflow page".to_string()));
    }

    let mut cursor = 0usize;
    let len = header.item_count as usize;
    let data = take_bytes(header.payload, &mut cursor, len, "overflow payload")?;
    Ok((data, header.next_page_id))
}

pub(crate) fn raw_freelist_page(
    raw: &[u8],
    expected_page_size: usize,
) -> Result<(Vec<PageId>, Option<PageId>), BTreeError> {
    let header = parse_header(raw, expected_page_size, false)?;
    if header.kind != PageKind::Freelist {
        return Err(BTreeError::Corrupt("expected freelist page".to_string()));
    }

    let mut cursor = 0usize;
    let mut ids = Vec::with_capacity(header.item_count as usize);
    for _ in 0..header.item_count {
        ids.push(take_u64(header.payload, &mut cursor, "freelist id")?);
    }
    Ok((ids, header.next_page_id))
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

            let key_count = keys.len();
            let slots_bytes = key_count.checked_mul(INTERNAL_SLOT_BYTES).ok_or_else(|| {
                BTreeError::InvalidOptions("internal slot bytes overflow".to_string())
            })?;
            let data_base = INTERNAL_LEFT_CHILD_BYTES
                .checked_add(slots_bytes)
                .ok_or_else(|| {
                    BTreeError::InvalidOptions("internal data base overflow".to_string())
                })?;

            let mut slots = Vec::with_capacity(slots_bytes);
            let mut data = Vec::new();
            for (idx, key) in keys.iter().enumerate() {
                let key_len = u32::try_from(key.len()).map_err(|_| {
                    BTreeError::InvalidOptions("internal key too large".to_string())
                })?;
                let key_off = data_base.checked_add(data.len()).ok_or_else(|| {
                    BTreeError::InvalidOptions("internal key offset overflow".to_string())
                })?;
                let key_off = u32::try_from(key_off).map_err(|_| {
                    BTreeError::InvalidOptions("internal key offset too large".to_string())
                })?;

                slots.extend_from_slice(&key_off.to_le_bytes());
                slots.extend_from_slice(&key_len.to_le_bytes());
                slots.extend_from_slice(&children[idx + 1].to_le_bytes());
                data.extend_from_slice(key);
            }

            let mut payload = Vec::with_capacity(
                INTERNAL_LEFT_CHILD_BYTES
                    .saturating_add(slots.len())
                    .saturating_add(data.len()),
            );
            payload.extend_from_slice(&children[0].to_le_bytes());
            payload.extend_from_slice(&slots);
            payload.extend_from_slice(&data);

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
            let entry_count = entries.len();
            let slots_bytes = entry_count.checked_mul(LEAF_SLOT_BYTES).ok_or_else(|| {
                BTreeError::InvalidOptions("leaf slot bytes overflow".to_string())
            })?;

            let mut slots = Vec::with_capacity(slots_bytes);
            let mut data = Vec::new();
            for (key, value) in entries {
                let key_len = u32::try_from(key.len())
                    .map_err(|_| BTreeError::InvalidOptions("leaf key too large".to_string()))?;
                let key_off = slots_bytes.checked_add(data.len()).ok_or_else(|| {
                    BTreeError::InvalidOptions("leaf key offset overflow".to_string())
                })?;
                let key_off = u32::try_from(key_off).map_err(|_| {
                    BTreeError::InvalidOptions("leaf key offset too large".to_string())
                })?;
                data.extend_from_slice(key);

                let value_off = slots_bytes.checked_add(data.len()).ok_or_else(|| {
                    BTreeError::InvalidOptions("leaf value offset overflow".to_string())
                })?;
                let value_off = u32::try_from(value_off).map_err(|_| {
                    BTreeError::InvalidOptions("leaf value offset too large".to_string())
                })?;

                encode_leaf_value_cell(value, &mut data)?;

                slots.extend_from_slice(&key_off.to_le_bytes());
                slots.extend_from_slice(&key_len.to_le_bytes());
                slots.extend_from_slice(&value_off.to_le_bytes());
            }

            let mut payload = Vec::with_capacity(slots.len().saturating_add(data.len()));
            payload.extend_from_slice(&slots);
            payload.extend_from_slice(&data);

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

struct RawPageHeader<'a> {
    kind: PageKind,
    next_page_id: Option<PageId>,
    item_count: u32,
    payload: &'a [u8],
}

fn parse_header<'a>(
    raw: &'a [u8],
    expected_page_size: usize,
    verify_checksum: bool,
) -> Result<RawPageHeader<'a>, BTreeError> {
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

    let kind = decode_kind(raw[4])?;
    let next_page_id = nonzero(u64::from_le_bytes(
        raw[8..16].try_into().expect("next page id header slice"),
    ));
    let item_count = u32::from_le_bytes(raw[16..20].try_into().expect("item count header slice"));

    if verify_checksum {
        let expected_checksum =
            u32::from_le_bytes(raw[20..24].try_into().expect("checksum header slice"));
        let actual_checksum = page_checksum(raw);
        if expected_checksum != actual_checksum {
            return Err(BTreeError::Corrupt(format!(
                "page checksum mismatch: expected {}, got {}",
                expected_checksum, actual_checksum
            )));
        }
    }

    Ok(RawPageHeader {
        kind,
        next_page_id,
        item_count,
        payload: &raw[PAGE_HEADER_BYTES..],
    })
}

fn decode_kind(kind: u8) -> Result<PageKind, BTreeError> {
    match kind {
        KIND_INTERNAL => Ok(PageKind::Internal),
        KIND_LEAF => Ok(PageKind::Leaf),
        KIND_OVERFLOW => Ok(PageKind::Overflow),
        KIND_FREELIST => Ok(PageKind::Freelist),
        _ => Err(BTreeError::Corrupt(format!("unknown page kind {}", kind))),
    }
}

fn parse_leaf_value_cell<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
) -> Result<ValueCellRef<'a>, BTreeError> {
    let tag = take_u8(payload, cursor, "leaf value tag")?;
    match tag {
        0 => {
            let value_len = take_u32(payload, cursor, "inline value length")? as usize;
            let value = take_bytes(payload, cursor, value_len, "inline value")?;
            Ok(ValueCellRef::Inline(value))
        }
        1 => {
            let head_page_id = take_u64(payload, cursor, "overflow head")?;
            let total_len = take_u32(payload, cursor, "overflow length")?;
            Ok(ValueCellRef::Overflow {
                head_page_id,
                total_len,
            })
        }
        _ => Err(BTreeError::Corrupt(format!(
            "invalid leaf value tag {}",
            tag
        ))),
    }
}

fn decode_leaf_value_cell_at(payload: &[u8], value_offset: usize) -> Result<ValueCell, BTreeError> {
    let value = parse_leaf_value_cell_at(payload, value_offset)?;
    match value {
        ValueCellRef::Inline(value) => Ok(ValueCell::Inline(value.to_vec())),
        ValueCellRef::Overflow {
            head_page_id,
            total_len,
        } => Ok(ValueCell::Overflow {
            head_page_id,
            total_len,
        }),
    }
}

fn parse_leaf_value_cell_at<'a>(
    payload: &'a [u8],
    value_offset: usize,
) -> Result<ValueCellRef<'a>, BTreeError> {
    let mut cursor = value_offset;
    parse_leaf_value_cell(payload, &mut cursor)
}

fn encode_leaf_value_cell(value: &ValueCell, out: &mut Vec<u8>) -> Result<(), BTreeError> {
    encode_leaf_value_cell_ref(value_cell_as_ref(value), out)
}

fn encode_leaf_value_cell_ref(
    value: ValueCellRef<'_>,
    out: &mut Vec<u8>,
) -> Result<(), BTreeError> {
    match value {
        ValueCellRef::Inline(v) => {
            let value_len = u32::try_from(v.len())
                .map_err(|_| BTreeError::InvalidOptions("inline value too large".to_string()))?;
            out.push(0);
            out.extend_from_slice(&value_len.to_le_bytes());
            out.extend_from_slice(v);
            Ok(())
        }
        ValueCellRef::Overflow {
            head_page_id,
            total_len,
        } => {
            out.push(1);
            out.extend_from_slice(&head_page_id.to_le_bytes());
            out.extend_from_slice(&total_len.to_le_bytes());
            Ok(())
        }
    }
}

fn value_cell_as_ref(value: &ValueCell) -> ValueCellRef<'_> {
    match value {
        ValueCell::Inline(v) => ValueCellRef::Inline(v.as_slice()),
        ValueCell::Overflow {
            head_page_id,
            total_len,
        } => ValueCellRef::Overflow {
            head_page_id: *head_page_id,
            total_len: *total_len,
        },
    }
}

fn overflow_from_value_ref(value: ValueCellRef<'_>) -> Option<OverflowRef> {
    match value {
        ValueCellRef::Inline(_) => None,
        ValueCellRef::Overflow {
            head_page_id,
            total_len,
        } => Some(OverflowRef {
            head_page_id,
            total_len,
        }),
    }
}

fn leaf_search_position(
    payload: &[u8],
    entry_count: usize,
    key: &[u8],
) -> Result<Result<usize, usize>, BTreeError> {
    let mut lo = 0usize;
    let mut hi = entry_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (current_key, _) = leaf_entry(payload, entry_count, mid)?;
        match current_key.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(lo))
}

fn leaf_entry<'a>(
    payload: &'a [u8],
    entry_count: usize,
    idx: usize,
) -> Result<(&'a [u8], ValueCellRef<'a>), BTreeError> {
    let (key_off, key_len, value_off) = leaf_slot(payload, entry_count, idx)?;
    let key = slice_payload(payload, key_off, key_len, "leaf key")?;
    let value = parse_leaf_value_cell_at(payload, value_off)?;
    Ok((key, value))
}

fn push_leaf_slot_entry(
    slots: &mut Vec<u8>,
    data: &mut Vec<u8>,
    slots_bytes: usize,
    key: &[u8],
    value: ValueCellRef<'_>,
) -> Result<(), BTreeError> {
    let key_len = u32::try_from(key.len())
        .map_err(|_| BTreeError::InvalidOptions("leaf key too large".to_string()))?;
    let key_off = slots_bytes
        .checked_add(data.len())
        .ok_or_else(|| BTreeError::InvalidOptions("leaf key offset overflow".to_string()))?;
    let key_off = u32::try_from(key_off)
        .map_err(|_| BTreeError::InvalidOptions("leaf key offset too large".to_string()))?;
    data.extend_from_slice(key);

    let value_off = slots_bytes
        .checked_add(data.len())
        .ok_or_else(|| BTreeError::InvalidOptions("leaf value offset overflow".to_string()))?;
    let value_off = u32::try_from(value_off)
        .map_err(|_| BTreeError::InvalidOptions("leaf value offset too large".to_string()))?;
    encode_leaf_value_cell_ref(value, data)?;

    slots.extend_from_slice(&key_off.to_le_bytes());
    slots.extend_from_slice(&key_len.to_le_bytes());
    slots.extend_from_slice(&value_off.to_le_bytes());
    Ok(())
}

fn write_leaf_payload(
    raw: &mut [u8],
    expected_page_size: usize,
    item_count: u32,
    payload: &[u8],
) -> Result<(), BTreeError> {
    if raw.len() != expected_page_size {
        return Err(BTreeError::Corrupt(format!(
            "page length mismatch: found {}, expected {}",
            raw.len(),
            expected_page_size
        )));
    }
    if payload.len() > page_payload_capacity(expected_page_size)? {
        return Err(BTreeError::InvalidOptions(
            "leaf payload exceeds page size".to_string(),
        ));
    }

    raw[16..20].copy_from_slice(&item_count.to_le_bytes());
    raw[20..24].copy_from_slice(&0u32.to_le_bytes());
    raw[PAGE_HEADER_BYTES..].fill(0);
    raw[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + payload.len()].copy_from_slice(payload);
    let checksum = page_checksum(raw);
    raw[20..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

fn internal_slots_bytes(key_count: usize) -> Result<usize, BTreeError> {
    INTERNAL_LEFT_CHILD_BYTES
        .checked_add(
            key_count.checked_mul(INTERNAL_SLOT_BYTES).ok_or_else(|| {
                BTreeError::Corrupt("internal slot byte count overflow".to_string())
            })?,
        )
        .ok_or_else(|| BTreeError::Corrupt("internal slot layout overflow".to_string()))
}

fn leaf_slots_bytes(entry_count: usize) -> Result<usize, BTreeError> {
    entry_count
        .checked_mul(LEAF_SLOT_BYTES)
        .ok_or_else(|| BTreeError::Corrupt("leaf slot byte count overflow".to_string()))
}

fn internal_slot(
    payload: &[u8],
    key_count: usize,
    idx: usize,
) -> Result<(usize, usize, PageId), BTreeError> {
    if idx >= key_count {
        return Err(BTreeError::Corrupt(format!(
            "internal slot index {} out of bounds {}",
            idx, key_count
        )));
    }
    let slots_end = internal_slots_bytes(key_count)?;
    if payload.len() < slots_end {
        return Err(BTreeError::Corrupt(
            "internal payload shorter than slot directory".to_string(),
        ));
    }

    let base = INTERNAL_LEFT_CHILD_BYTES
        .checked_add(
            idx.checked_mul(INTERNAL_SLOT_BYTES)
                .ok_or_else(|| BTreeError::Corrupt("internal slot offset overflow".to_string()))?,
        )
        .ok_or_else(|| BTreeError::Corrupt("internal slot base overflow".to_string()))?;

    let key_off = read_u32_at(payload, base, "internal key offset")? as usize;
    let key_len = read_u32_at(payload, base + 4, "internal key length")? as usize;
    let right_child = read_u64_at(payload, base + 8, "internal right child")?;
    Ok((key_off, key_len, right_child))
}

fn leaf_slot(
    payload: &[u8],
    entry_count: usize,
    idx: usize,
) -> Result<(usize, usize, usize), BTreeError> {
    if idx >= entry_count {
        return Err(BTreeError::Corrupt(format!(
            "leaf slot index {} out of bounds {}",
            idx, entry_count
        )));
    }
    let slots_end = leaf_slots_bytes(entry_count)?;
    if payload.len() < slots_end {
        return Err(BTreeError::Corrupt(
            "leaf payload shorter than slot directory".to_string(),
        ));
    }

    let base = idx
        .checked_mul(LEAF_SLOT_BYTES)
        .ok_or_else(|| BTreeError::Corrupt("leaf slot offset overflow".to_string()))?;
    let key_off = read_u32_at(payload, base, "leaf key offset")? as usize;
    let key_len = read_u32_at(payload, base + 4, "leaf key length")? as usize;
    let value_off = read_u32_at(payload, base + 8, "leaf value offset")? as usize;
    Ok((key_off, key_len, value_off))
}

fn slice_payload<'a>(
    payload: &'a [u8],
    offset: usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8], BTreeError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| BTreeError::Corrupt(format!("{} offset overflow", label)))?;
    if end > payload.len() {
        return Err(BTreeError::Corrupt(format!(
            "{} exceeds payload bounds",
            label
        )));
    }
    Ok(&payload[offset..end])
}

fn read_u32_at(payload: &[u8], offset: usize, label: &str) -> Result<u32, BTreeError> {
    let bytes = slice_payload(payload, offset, 4, label)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 at offset")))
}

fn read_u64_at(payload: &[u8], offset: usize, label: &str) -> Result<u64, BTreeError> {
    let bytes = slice_payload(payload, offset, 8, label)?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("u64 at offset")))
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

    #[test]
    fn leaf_upsert_in_place_insert_update_and_delete() {
        let mut raw = encode_page(
            &Page::Leaf {
                entries: vec![
                    (b"a".to_vec(), ValueCell::Inline(b"1".to_vec())),
                    (b"c".to_vec(), ValueCell::Inline(b"3".to_vec())),
                ],
                next: Some(9),
            },
            4096,
        )
        .expect("encode");

        let inserted =
            raw_leaf_upsert_in_place(&mut raw, 4096, b"b", &ValueCell::Inline(b"2".to_vec()))
                .expect("insert in place");
        assert_eq!(inserted, RawLeafUpsertResult::Inserted);

        let updated =
            raw_leaf_upsert_in_place(&mut raw, 4096, b"c", &ValueCell::Inline(b"30".to_vec()))
                .expect("update in place");
        assert_eq!(updated, RawLeafUpsertResult::Updated { old_overflow: None });

        let deleted = raw_leaf_delete_in_place(&mut raw, 4096, b"a").expect("delete in place");
        assert_eq!(
            deleted,
            RawLeafDeleteResult::Deleted {
                old_overflow: None,
                is_empty: false,
            }
        );

        let decoded = decode_page(&raw, 4096).expect("decode");
        assert_eq!(
            decoded,
            Page::Leaf {
                entries: vec![
                    (b"b".to_vec(), ValueCell::Inline(b"2".to_vec())),
                    (b"c".to_vec(), ValueCell::Inline(b"30".to_vec())),
                ],
                next: Some(9),
            }
        );
    }

    #[test]
    fn leaf_upsert_in_place_returns_need_split_when_payload_overflows() {
        let mut raw = encode_page(
            &Page::Leaf {
                entries: vec![(b"a".to_vec(), ValueCell::Inline(vec![1u8; 80]))],
                next: None,
            },
            128,
        )
        .expect("encode");

        let result =
            raw_leaf_upsert_in_place(&mut raw, 128, b"b", &ValueCell::Inline(vec![2u8; 80]))
                .expect("upsert");
        assert_eq!(result, RawLeafUpsertResult::NeedSplit);
    }

    #[test]
    fn leaf_delete_in_place_reports_deleted_overflow() {
        let mut raw = encode_page(
            &Page::Leaf {
                entries: vec![(
                    b"k".to_vec(),
                    ValueCell::Overflow {
                        head_page_id: 77,
                        total_len: 1024,
                    },
                )],
                next: None,
            },
            4096,
        )
        .expect("encode");

        let result = raw_leaf_delete_in_place(&mut raw, 4096, b"k").expect("delete");
        assert_eq!(
            result,
            RawLeafDeleteResult::Deleted {
                old_overflow: Some(OverflowRef {
                    head_page_id: 77,
                    total_len: 1024,
                }),
                is_empty: true,
            }
        );
    }
}
