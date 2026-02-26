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
    leaf_data_start_hint: u32,
    next_page_id: u64,
    item_count: u32,
    payload: Vec<u8>,
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
    set_leaf_data_start_hint(&mut raw, encoded.leaf_data_start_hint)?;
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

/// Scan a leaf page for key-value pairs in the range [start, end).
/// Calls the `visit` function for each key-value pair in the range.
/// If the end of the range (or the limit of results) is reached, the function returns None.
/// Otherwise, the function returns the next page ID so the caller can continue scanning.
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
        return Ok(None);
    }

    let entry_count = header.item_count as usize;
    let payload = header.payload;
    let slots_bytes = leaf_slots_bytes(entry_count)?;
    if payload.len() < slots_bytes {
        return Err(BTreeError::Corrupt(
            "leaf page payload shorter than slot directory".to_string(),
        ));
    }
    let slots = &payload[..slots_bytes];

    let mut lo = 0usize;
    let mut hi = entry_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let slot_base = leaf_slot_base(mid)?;
        let slot = &slots[slot_base..slot_base + LEAF_SLOT_BYTES];
        let key_off =
            u32::from_le_bytes(slot[0..4].try_into().expect("leaf key offset slot bytes")) as usize;
        let key_len =
            u32::from_le_bytes(slot[4..8].try_into().expect("leaf key length slot bytes")) as usize;
        let key_end = key_off
            .checked_add(key_len)
            .ok_or_else(|| BTreeError::Corrupt("leaf key offset overflow".to_string()))?;
        if key_end > payload.len() {
            return Err(BTreeError::Corrupt(
                "leaf key exceeds payload bounds".to_string(),
            ));
        }
        let current_key = &payload[key_off..key_end];
        if current_key < start {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let mut emitted = 0usize;
    let mut reached_end = false;
    let mut slot_base = leaf_slot_base(lo)?;
    while slot_base < slots_bytes && emitted < limit {
        let slot = &slots[slot_base..slot_base + LEAF_SLOT_BYTES];
        let key_off =
            u32::from_le_bytes(slot[0..4].try_into().expect("leaf key offset slot bytes")) as usize;
        let key_len =
            u32::from_le_bytes(slot[4..8].try_into().expect("leaf key length slot bytes")) as usize;
        let value_off = u32::from_le_bytes(
            slot[8..12]
                .try_into()
                .expect("leaf value offset slot bytes"),
        ) as usize;
        let key_end = key_off
            .checked_add(key_len)
            .ok_or_else(|| BTreeError::Corrupt("leaf key offset overflow".to_string()))?;
        if key_end > payload.len() {
            return Err(BTreeError::Corrupt(
                "leaf key exceeds payload bounds".to_string(),
            ));
        }
        let key = &payload[key_off..key_end];
        if key >= end {
            reached_end = true;
            break;
        }
        let value = parse_leaf_value_cell_at(payload, value_off)?;
        visit(key, value)?;
        emitted += 1;
        slot_base += LEAF_SLOT_BYTES;
    }

    if reached_end || emitted == limit {
        Ok(None)
    } else {
        Ok(header.next_page_id)
    }
}

pub(crate) fn raw_leaf_upsert_in_place(
    raw: &mut [u8],
    expected_page_size: usize,
    key: &[u8],
    value: &ValueCell,
) -> Result<RawLeafUpsertResult, BTreeError> {
    let mut value_bytes = Vec::new();
    encode_leaf_value_cell_ref(value_cell_as_ref(value), &mut value_bytes)?;

    let mut compacted = false;
    loop {
        enum Plan {
            Update {
                entry_count: usize,
                idx: usize,
                old_value_off: usize,
                old_value_len: usize,
                old_overflow: Option<OverflowRef>,
                data_start: usize,
            },
            Insert {
                entry_count: usize,
                insert_idx: usize,
                data_start: usize,
            },
        }

        let plan = {
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
            let data_start = leaf_data_start_with_hint(raw, header.payload, entry_count)?;
            match pos {
                Ok(idx) => {
                    let (_, _, old_value_off) = leaf_slot(header.payload, entry_count, idx)?;
                    let (old_value_ref, old_value_len) =
                        parse_leaf_value_cell_with_len_at(header.payload, old_value_off)?;
                    Plan::Update {
                        entry_count,
                        idx,
                        old_value_off,
                        old_value_len,
                        old_overflow: overflow_from_value_ref(old_value_ref),
                        data_start,
                    }
                }
                Err(insert_idx) => Plan::Insert {
                    entry_count,
                    insert_idx,
                    data_start,
                },
            }
        };

        match plan {
            Plan::Update {
                entry_count,
                idx,
                old_value_off,
                old_value_len,
                old_overflow,
                data_start,
            } => {
                let mut target_value_off = old_value_off;
                let mut new_data_start_hint = data_start;
                if value_bytes.len() > old_value_len {
                    let slots_end = leaf_slots_bytes(entry_count)?;
                    if slots_end.saturating_add(value_bytes.len()) > data_start {
                        if compacted {
                            return Ok(RawLeafUpsertResult::NeedSplit);
                        }
                        compact_leaf_in_place(raw, expected_page_size)?;
                        compacted = true;
                        continue;
                    }
                    target_value_off = data_start.saturating_sub(value_bytes.len());
                    new_data_start_hint = target_value_off;
                }

                {
                    let payload = raw.get_mut(PAGE_HEADER_BYTES..).ok_or_else(|| {
                        BTreeError::Corrupt("leaf payload slice out of bounds".to_string())
                    })?;
                    let end = target_value_off
                        .checked_add(value_bytes.len())
                        .ok_or_else(|| {
                            BTreeError::Corrupt("leaf value write offset overflow".to_string())
                        })?;
                    if end > payload.len() {
                        return Err(BTreeError::Corrupt(
                            "leaf value write exceeds payload bounds".to_string(),
                        ));
                    }
                    payload[target_value_off..end].copy_from_slice(&value_bytes);
                    let slot_base = leaf_slot_base(idx)?;
                    write_u32_at_mut(
                        payload,
                        slot_base + 8,
                        target_value_off as u32,
                        "leaf value offset",
                    )?;
                }

                set_leaf_data_start_hint(raw, new_data_start_hint as u32)?;
                finish_leaf_mutation(raw, expected_page_size, entry_count as u32)?;
                return Ok(RawLeafUpsertResult::Updated { old_overflow });
            }
            Plan::Insert {
                entry_count,
                insert_idx,
                data_start,
            } => {
                let new_entry_data_len = key
                    .len()
                    .checked_add(value_bytes.len())
                    .ok_or_else(|| BTreeError::Corrupt("leaf insert data overflow".to_string()))?;
                let slots_end_new = leaf_slots_bytes(entry_count.saturating_add(1))?;
                if slots_end_new.saturating_add(new_entry_data_len) > data_start {
                    if compacted {
                        return Ok(RawLeafUpsertResult::NeedSplit);
                    }
                    compact_leaf_in_place(raw, expected_page_size)?;
                    compacted = true;
                    continue;
                }

                let new_key_off = data_start
                    .checked_sub(new_entry_data_len)
                    .ok_or_else(|| BTreeError::Corrupt("leaf key offset underflow".to_string()))?;
                let new_value_off = new_key_off
                    .checked_add(key.len())
                    .ok_or_else(|| BTreeError::Corrupt("leaf value offset overflow".to_string()))?;

                {
                    let payload = raw.get_mut(PAGE_HEADER_BYTES..).ok_or_else(|| {
                        BTreeError::Corrupt("leaf payload slice out of bounds".to_string())
                    })?;
                    if new_value_off
                        .checked_add(value_bytes.len())
                        .ok_or_else(|| {
                            BTreeError::Corrupt("leaf value write offset overflow".to_string())
                        })?
                        > payload.len()
                    {
                        return Err(BTreeError::Corrupt(
                            "leaf insert write exceeds payload bounds".to_string(),
                        ));
                    }

                    payload[new_key_off..new_key_off + key.len()].copy_from_slice(key);
                    payload[new_value_off..new_value_off + value_bytes.len()]
                        .copy_from_slice(&value_bytes);

                    let old_slots_end = leaf_slots_bytes(entry_count)?;
                    let insert_base = leaf_slot_base(insert_idx)?;
                    payload.copy_within(insert_base..old_slots_end, insert_base + LEAF_SLOT_BYTES);
                    write_leaf_slot_mut(
                        payload,
                        insert_idx,
                        new_key_off as u32,
                        key.len() as u32,
                        new_value_off as u32,
                    )?;
                }

                set_leaf_data_start_hint(raw, new_key_off as u32)?;
                finish_leaf_mutation(raw, expected_page_size, (entry_count + 1) as u32)?;
                return Ok(RawLeafUpsertResult::Inserted);
            }
        }
    }
}

pub(crate) fn raw_leaf_delete_in_place(
    raw: &mut [u8],
    expected_page_size: usize,
    key: &[u8],
) -> Result<RawLeafDeleteResult, BTreeError> {
    let (entry_count, delete_idx, old_overflow, old_key_off, old_value_off) = {
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
        let (old_key_off, _, old_value_off) = leaf_slot(header.payload, entry_count, delete_idx)?;
        let (_, old_value) = leaf_entry(header.payload, entry_count, delete_idx)?;
        let old_overflow = overflow_from_value_ref(old_value);
        (
            entry_count,
            delete_idx,
            old_overflow,
            old_key_off,
            old_value_off,
        )
    };

    let old_hint = leaf_data_start_hint(raw);
    {
        let payload = raw
            .get_mut(PAGE_HEADER_BYTES..)
            .ok_or_else(|| BTreeError::Corrupt("leaf payload slice out of bounds".to_string()))?;
        let slots_end = leaf_slots_bytes(entry_count)?;
        let delete_base = leaf_slot_base(delete_idx)?;
        let src_start = delete_base.saturating_add(LEAF_SLOT_BYTES);
        payload.copy_within(src_start..slots_end, delete_base);
        let trail_start = slots_end.saturating_sub(LEAF_SLOT_BYTES);
        payload[trail_start..slots_end].fill(0);
    }

    let new_count = entry_count.saturating_sub(1);
    if new_count == 0 {
        set_leaf_data_start_hint(raw, page_payload_capacity(expected_page_size)? as u32)?;
    } else {
        let removed_min = old_key_off.min(old_value_off) == old_hint;
        if removed_min || old_hint == 0 {
            let payload = raw.get(PAGE_HEADER_BYTES..).ok_or_else(|| {
                BTreeError::Corrupt("leaf payload slice out of bounds".to_string())
            })?;
            let refreshed = leaf_data_start_offset(payload, new_count)?;
            set_leaf_data_start_hint(raw, refreshed as u32)?;
        }
    }
    finish_leaf_mutation(raw, expected_page_size, new_count as u32)?;
    Ok(RawLeafDeleteResult::Deleted {
        old_overflow,
        is_empty: new_count == 0,
    })
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn encode_overflow_page_chunk(
    chunk: &[u8],
    next: Option<PageId>,
    page_size: usize,
) -> Result<Vec<u8>, BTreeError> {
    if page_size < PAGE_HEADER_BYTES {
        return Err(BTreeError::InvalidOptions(format!(
            "page_size {} is too small",
            page_size
        )));
    }
    let payload_capacity = page_payload_capacity(page_size)?;
    if chunk.len() > payload_capacity {
        return Err(BTreeError::InvalidOptions(format!(
            "overflow chunk {} exceeds page payload capacity {}",
            chunk.len(),
            payload_capacity
        )));
    }

    let item_count = u32::try_from(chunk.len())
        .map_err(|_| BTreeError::InvalidOptions("overflow chunk too large".to_string()))?;

    let mut raw = vec![0u8; page_size];
    raw[..4].copy_from_slice(&PAGE_MAGIC);
    raw[4] = KIND_OVERFLOW;
    set_leaf_data_start_hint(&mut raw, 0)?;
    raw[8..16].copy_from_slice(&next.unwrap_or(0).to_le_bytes());
    raw[16..20].copy_from_slice(&item_count.to_le_bytes());
    raw[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + chunk.len()].copy_from_slice(chunk);

    let checksum = page_checksum(&raw);
    raw[20..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(raw)
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
                leaf_data_start_hint: 0,
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
                leaf_data_start_hint: slots_bytes as u32,
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
                leaf_data_start_hint: 0,
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
                leaf_data_start_hint: 0,
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

fn parse_leaf_value_cell_with_len_at<'a>(
    payload: &'a [u8],
    value_offset: usize,
) -> Result<(ValueCellRef<'a>, usize), BTreeError> {
    let mut cursor = value_offset;
    let value = parse_leaf_value_cell(payload, &mut cursor)?;
    Ok((value, cursor.saturating_sub(value_offset)))
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
        let current_key = leaf_key(payload, entry_count, mid)?;
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
    let (key, value_off) = leaf_key_and_value_off(payload, entry_count, idx)?;
    let value = parse_leaf_value_cell_at(payload, value_off)?;
    Ok((key, value))
}

fn leaf_key(payload: &[u8], entry_count: usize, idx: usize) -> Result<&[u8], BTreeError> {
    let (key, _) = leaf_key_and_value_off(payload, entry_count, idx)?;
    Ok(key)
}

fn leaf_key_and_value_off(
    payload: &[u8],
    entry_count: usize,
    idx: usize,
) -> Result<(&[u8], usize), BTreeError> {
    let (key_off, key_len, value_off) = leaf_slot(payload, entry_count, idx)?;
    let key = slice_payload(payload, key_off, key_len, "leaf key")?;
    Ok((key, value_off))
}

fn compact_leaf_in_place(raw: &mut [u8], expected_page_size: usize) -> Result<(), BTreeError> {
    let (entry_count, compacted_payload, data_start_hint) = {
        let header = parse_header(raw, expected_page_size, false)?;
        if header.kind != PageKind::Leaf {
            return Err(BTreeError::Corrupt("expected leaf page".to_string()));
        }
        let entry_count = header.item_count as usize;
        let slots_end = leaf_slots_bytes(entry_count)?;
        if header.payload.len() < slots_end {
            return Err(BTreeError::Corrupt(
                "leaf page payload shorter than slot directory".to_string(),
            ));
        }

        let mut compacted = vec![0u8; header.payload.len()];
        let mut data_cursor = header.payload.len();
        for idx in (0..entry_count).rev() {
            let (key_off, key_len, value_off) = leaf_slot(header.payload, entry_count, idx)?;
            let key = slice_payload(header.payload, key_off, key_len, "leaf key")?;
            let (_, value_len) = parse_leaf_value_cell_with_len_at(header.payload, value_off)?;
            let value_bytes = slice_payload(header.payload, value_off, value_len, "leaf value")?;

            data_cursor = data_cursor.checked_sub(value_len).ok_or_else(|| {
                BTreeError::Corrupt("leaf value compaction underflow".to_string())
            })?;
            compacted[data_cursor..data_cursor + value_len].copy_from_slice(value_bytes);
            let compact_value_off = data_cursor;

            data_cursor = data_cursor
                .checked_sub(key_len)
                .ok_or_else(|| BTreeError::Corrupt("leaf key compaction underflow".to_string()))?;
            compacted[data_cursor..data_cursor + key_len].copy_from_slice(key);
            let compact_key_off = data_cursor;

            write_leaf_slot_mut(
                &mut compacted,
                idx,
                compact_key_off as u32,
                key_len as u32,
                compact_value_off as u32,
            )?;
        }
        if data_cursor < slots_end {
            return Err(BTreeError::Corrupt(
                "leaf compaction produced overlapping slots/data".to_string(),
            ));
        }
        (entry_count, compacted, data_cursor as u32)
    };

    raw[PAGE_HEADER_BYTES..].copy_from_slice(&compacted_payload);
    set_leaf_data_start_hint(raw, data_start_hint)?;
    finish_leaf_mutation(raw, expected_page_size, entry_count as u32)?;
    Ok(())
}

fn leaf_data_start_offset(payload: &[u8], entry_count: usize) -> Result<usize, BTreeError> {
    let slots_end = leaf_slots_bytes(entry_count)?;
    if payload.len() < slots_end {
        return Err(BTreeError::Corrupt(
            "leaf page payload shorter than slot directory".to_string(),
        ));
    }
    if entry_count == 0 {
        return Ok(payload.len());
    }

    let mut min_offset = payload.len();
    for idx in 0..entry_count {
        let (key_off, _, value_off) = leaf_slot(payload, entry_count, idx)?;
        min_offset = min_offset.min(key_off).min(value_off);
    }
    if min_offset < slots_end {
        return Err(BTreeError::Corrupt(
            "leaf slot points into slot directory".to_string(),
        ));
    }
    Ok(min_offset)
}

fn finish_leaf_mutation(
    raw: &mut [u8],
    expected_page_size: usize,
    item_count: u32,
) -> Result<(), BTreeError> {
    if raw.len() != expected_page_size {
        return Err(BTreeError::Corrupt(format!(
            "page length mismatch: found {}, expected {}",
            raw.len(),
            expected_page_size
        )));
    }

    raw[16..20].copy_from_slice(&item_count.to_le_bytes());
    raw[20..24].copy_from_slice(&0u32.to_le_bytes());
    let checksum = page_checksum(raw);
    raw[20..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

fn leaf_data_start_with_hint(
    raw: &[u8],
    payload: &[u8],
    entry_count: usize,
) -> Result<usize, BTreeError> {
    let slots_end = leaf_slots_bytes(entry_count)?;
    let hint = leaf_data_start_hint(raw);
    if hint >= slots_end && hint <= payload.len() {
        return Ok(hint);
    }
    leaf_data_start_offset(payload, entry_count)
}

fn leaf_data_start_hint(raw: &[u8]) -> usize {
    (raw.get(5).copied().unwrap_or(0) as usize)
        | ((raw.get(6).copied().unwrap_or(0) as usize) << 8)
        | ((raw.get(7).copied().unwrap_or(0) as usize) << 16)
}

fn set_leaf_data_start_hint(raw: &mut [u8], data_start: u32) -> Result<(), BTreeError> {
    if data_start > 0x00FF_FFFF {
        return Err(BTreeError::InvalidOptions(
            "leaf data start hint exceeds u24".to_string(),
        ));
    }
    if raw.len() < 8 {
        return Err(BTreeError::Corrupt(
            "page too small for leaf hint bytes".to_string(),
        ));
    }
    raw[5] = (data_start & 0xFF) as u8;
    raw[6] = ((data_start >> 8) & 0xFF) as u8;
    raw[7] = ((data_start >> 16) & 0xFF) as u8;
    Ok(())
}

fn leaf_slot_base(idx: usize) -> Result<usize, BTreeError> {
    idx.checked_mul(LEAF_SLOT_BYTES)
        .ok_or_else(|| BTreeError::Corrupt("leaf slot offset overflow".to_string()))
}

fn write_leaf_slot_mut(
    payload: &mut [u8],
    idx: usize,
    key_off: u32,
    key_len: u32,
    value_off: u32,
) -> Result<(), BTreeError> {
    let base = leaf_slot_base(idx)?;
    write_u32_at_mut(payload, base, key_off, "leaf key offset")?;
    write_u32_at_mut(payload, base + 4, key_len, "leaf key length")?;
    write_u32_at_mut(payload, base + 8, value_off, "leaf value offset")?;
    Ok(())
}

fn write_u32_at_mut(
    payload: &mut [u8],
    offset: usize,
    value: u32,
    label: &str,
) -> Result<(), BTreeError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| BTreeError::Corrupt(format!("{} offset overflow", label)))?;
    let dst = payload
        .get_mut(offset..end)
        .ok_or_else(|| BTreeError::Corrupt(format!("{} exceeds payload bounds", label)))?;
    dst.copy_from_slice(&value.to_le_bytes());
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

    fn sample_leaf_page(next: Option<PageId>) -> Vec<u8> {
        encode_page(
            &Page::Leaf {
                entries: vec![
                    (b"a".to_vec(), ValueCell::Inline(b"1".to_vec())),
                    (b"b".to_vec(), ValueCell::Inline(b"2".to_vec())),
                    (b"c".to_vec(), ValueCell::Inline(b"3".to_vec())),
                    (b"d".to_vec(), ValueCell::Inline(b"4".to_vec())),
                ],
                next,
            },
            4096,
        )
        .expect("encode sample leaf")
    }

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
    fn overflow_chunk_fast_encode_round_trip() {
        let chunk = vec![7u8; 1234];
        let raw = encode_overflow_page_chunk(&chunk, Some(42), 4096).expect("fast encode");
        let (decoded_chunk, next) = raw_overflow_chunk(&raw, 4096).expect("raw overflow");
        assert_eq!(decoded_chunk, chunk.as_slice());
        assert_eq!(next, Some(42));
        let decoded_page = decode_page(&raw, 4096).expect("decode");
        assert_eq!(
            decoded_page,
            Page::Overflow {
                data: chunk,
                next: Some(42),
            }
        );
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

    #[test]
    fn raw_leaf_scan_limit_zero_returns_none_and_does_not_visit() {
        let raw = sample_leaf_page(Some(55));
        let mut visited = Vec::<Vec<u8>>::new();
        let next = raw_leaf_scan(&raw, 4096, b"a", b"z", 0, |key, _| {
            visited.push(key.to_vec());
            Ok(())
        })
        .expect("scan");
        assert_eq!(next, None);
        assert!(visited.is_empty());
    }

    #[test]
    fn raw_leaf_scan_honors_start_inclusive_end_exclusive() {
        let raw = sample_leaf_page(Some(77));
        let mut visited = Vec::<Vec<u8>>::new();
        let next = raw_leaf_scan(&raw, 4096, b"b", b"d", 10, |key, _| {
            visited.push(key.to_vec());
            Ok(())
        })
        .expect("scan");
        assert_eq!(visited, vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(next, None);
    }

    #[test]
    fn raw_leaf_scan_returns_next_when_page_exhausted_before_end() {
        let raw = sample_leaf_page(Some(88));
        let mut visited = Vec::<Vec<u8>>::new();
        let next = raw_leaf_scan(&raw, 4096, b"b", b"z", 10, |key, _| {
            visited.push(key.to_vec());
            Ok(())
        })
        .expect("scan");
        assert_eq!(visited, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        assert_eq!(next, Some(88));
    }

    #[test]
    fn raw_leaf_scan_returns_none_when_limit_is_reached() {
        let raw = sample_leaf_page(Some(99));
        let mut visited = Vec::<Vec<u8>>::new();
        let next = raw_leaf_scan(&raw, 4096, b"a", b"z", 2, |key, _| {
            visited.push(key.to_vec());
            Ok(())
        })
        .expect("scan");
        assert_eq!(visited, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(next, None);
    }
}
