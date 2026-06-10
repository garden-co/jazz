use std::collections::{BTreeMap, BTreeSet};

use xxhash_rust::xxh3;

const DEFAULT_PAGE_SIZE: usize = 16 * 1024;

const SUPERBLOCK_MAGIC: [u8; 8] = *b"OPFSBT01";
const SUPERBLOCK_FORMAT_VERSION: u32 = 1;
const SUPERBLOCK_RESERVED_BYTES: usize = 32;
const SUPERBLOCK_OFFSET_MAGIC: usize = 0;
const SUPERBLOCK_OFFSET_VERSION: usize = SUPERBLOCK_OFFSET_MAGIC + 8;
const SUPERBLOCK_OFFSET_PAGE_SIZE: usize = SUPERBLOCK_OFFSET_VERSION + 4;
const SUPERBLOCK_OFFSET_GENERATION: usize = SUPERBLOCK_OFFSET_PAGE_SIZE + 4;
const SUPERBLOCK_OFFSET_ROOT_PAGE_ID: usize = SUPERBLOCK_OFFSET_GENERATION + 8;
const SUPERBLOCK_OFFSET_FREELIST_HEAD_PAGE_ID: usize = SUPERBLOCK_OFFSET_ROOT_PAGE_ID + 8;
const SUPERBLOCK_OFFSET_TOTAL_PAGES: usize = SUPERBLOCK_OFFSET_FREELIST_HEAD_PAGE_ID + 8;
const SUPERBLOCK_OFFSET_RESERVED: usize = SUPERBLOCK_OFFSET_TOTAL_PAGES + 8;
const SUPERBLOCK_OFFSET_CHECKSUM: usize = SUPERBLOCK_OFFSET_RESERVED + SUPERBLOCK_RESERVED_BYTES;
const SUPERBLOCK_ENCODED_BYTES: usize = SUPERBLOCK_OFFSET_CHECKSUM + 4;

const PAGE_MAGIC: [u8; 4] = *b"OPPG";
const PAGE_HEADER_BYTES: usize = 24;
const KIND_INTERNAL: u8 = 1;
const KIND_LEAF: u8 = 2;
const KIND_FREELIST: u8 = 4;
const INTERNAL_LEFT_CHILD_BYTES: usize = 8;
const INTERNAL_SLOT_BYTES: usize = 16;
const LEAF_SLOT_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawPageKind {
    SuperblockA,
    SuperblockB,
    Internal,
    Leaf,
    Overflow,
    Freelist,
    Corrupt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPageSummary {
    pub page_id: u64,
    pub kind: RawPageKind,
    pub byte_offset: u64,
    pub byte_len: usize,
    pub item_count: usize,
    pub next_page_id: Option<u64>,
    pub is_root: bool,
    pub is_free: bool,
    pub is_active: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RawEntryBatch {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next_cursor: Option<RawEntryCursor>,
    pub done: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawEntryCursor {
    page_id: u64,
    entry_index: usize,
}

#[derive(Debug)]
pub struct RawStore {
    bytes: Vec<u8>,
    page_size: usize,
    active_slot: SuperblockSlot,
    active: Superblock,
    free_pages: BTreeSet<u64>,
    overflow_pages: BTreeMap<u64, OverflowPage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SuperblockSlot {
    A,
    B,
}

impl SuperblockSlot {
    fn page_id(self) -> u64 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    fn byte_offset(self, page_size: usize) -> u64 {
        self.page_id() * page_size as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Superblock {
    page_size: u32,
    generation: u64,
    root_page_id: u64,
    freelist_head_page_id: u64,
    total_pages: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OverflowPage {
    item_count: usize,
    next_page_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawPageHeader<'a> {
    kind: RawPageKind,
    next_page_id: Option<u64>,
    item_count: u32,
    payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeafPage {
    entries: Vec<(Vec<u8>, RawValue)>,
    next_page_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawValue {
    Inline(Vec<u8>),
    Overflow { head_page_id: u64, total_len: usize },
}

impl RawStore {
    pub fn open(bytes: Vec<u8>) -> Result<Self, String> {
        let page_size = DEFAULT_PAGE_SIZE;
        let slot_a = read_superblock_slot(&bytes, SuperblockSlot::A, page_size);
        let slot_b = read_superblock_slot(&bytes, SuperblockSlot::B, page_size);
        let (active_slot, active) = choose_active(slot_a, slot_b)
            .ok_or_else(|| "no valid opfs-btree superblock found".to_string())?;

        let mut store = Self {
            bytes,
            page_size,
            active_slot,
            active,
            free_pages: BTreeSet::new(),
            overflow_pages: BTreeMap::new(),
        };
        store.free_pages = store.read_freelist_pages()?;
        store.overflow_pages = store.discover_overflow_pages().unwrap_or_default();
        Ok(store)
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn total_pages(&self) -> u64 {
        self.active.total_pages.max(2)
    }

    pub fn raw_entries_batch(
        &self,
        cursor: Option<RawEntryCursor>,
        limit: usize,
    ) -> Result<RawEntryBatch, String> {
        if limit == 0 {
            return Ok(RawEntryBatch {
                entries: Vec::new(),
                next_cursor: cursor,
                done: false,
            });
        }

        let mut out = Vec::new();
        let mut current = match cursor {
            Some(cursor) => Some(cursor.page_id),
            None => self.leftmost_leaf_page_id()?,
        };
        let mut entry_index = cursor.map_or(0, |cursor| cursor.entry_index);
        let mut visited = BTreeSet::new();

        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err("leaf chain contains a cycle".to_string());
            }

            let page = self.parse_leaf_page(page_id)?;
            if entry_index > page.entries.len() {
                return Err(format!("raw entry cursor points past leaf page {page_id}"));
            }

            let entry_count = page.entries.len();
            for (index, (key, value)) in page.entries.into_iter().enumerate().skip(entry_index) {
                let value = self.resolve_value(value)?;
                out.push((key, value));
                if out.len() == limit {
                    let next_cursor = if index + 1 < entry_count {
                        Some(RawEntryCursor {
                            page_id,
                            entry_index: index + 1,
                        })
                    } else {
                        page.next_page_id.map(|page_id| RawEntryCursor {
                            page_id,
                            entry_index: 0,
                        })
                    };
                    return Ok(RawEntryBatch {
                        entries: out,
                        next_cursor,
                        done: next_cursor.is_none(),
                    });
                }
            }

            current = page.next_page_id;
            entry_index = 0;
        }

        Ok(RawEntryBatch {
            entries: out,
            next_cursor: None,
            done: true,
        })
    }

    pub fn raw_page_summaries_batch(&self, start_page_id: u64, limit: usize) -> RawPageBatch {
        if limit == 0 {
            return RawPageBatch {
                pages: Vec::new(),
                next_page_id: (start_page_id < self.total_pages()).then_some(start_page_id),
                done: start_page_id >= self.total_pages(),
            };
        }

        let mut pages = Vec::new();
        let mut page_id = start_page_id;
        while page_id < self.total_pages() && pages.len() < limit {
            pages.push(self.raw_page_summary(page_id));
            page_id += 1;
        }

        RawPageBatch {
            pages,
            next_page_id: (page_id < self.total_pages()).then_some(page_id),
            done: page_id >= self.total_pages(),
        }
    }

    fn raw_page_summary(&self, page_id: u64) -> RawPageSummary {
        let byte_offset = page_id.saturating_mul(self.page_size as u64);
        if page_id == 0 || page_id == 1 {
            let kind = if page_id == 0 {
                RawPageKind::SuperblockA
            } else {
                RawPageKind::SuperblockB
            };
            return RawPageSummary {
                page_id,
                kind,
                byte_offset,
                byte_len: self.page_size,
                item_count: 0,
                next_page_id: None,
                is_root: false,
                is_free: false,
                is_active: self.active_slot.page_id() == page_id,
                error: None,
            };
        }

        if let Some(page) = self.overflow_pages.get(&page_id) {
            return RawPageSummary {
                page_id,
                kind: RawPageKind::Overflow,
                byte_offset,
                byte_len: self.page_size,
                item_count: page.item_count,
                next_page_id: page.next_page_id,
                is_root: false,
                is_free: self.free_pages.contains(&page_id),
                is_active: false,
                error: None,
            };
        }

        let decoded = self.decode_page_summary(page_id);
        let (kind, item_count, next_page_id, error) = match decoded {
            Ok(summary) => summary,
            Err(err) => (RawPageKind::Corrupt, 0, None, Some(err)),
        };

        RawPageSummary {
            page_id,
            kind,
            byte_offset,
            byte_len: self.page_size,
            item_count,
            next_page_id,
            is_root: self.active.root_page_id == page_id,
            is_free: self.free_pages.contains(&page_id),
            is_active: false,
            error,
        }
    }

    fn decode_page_summary(
        &self,
        page_id: u64,
    ) -> Result<(RawPageKind, usize, Option<u64>, Option<String>), String> {
        let raw = self.page_bytes(page_id)?;
        let header = parse_header(raw, self.page_size)?;
        match header.kind {
            RawPageKind::Internal => {
                validate_internal_page(header.payload, header.item_count as usize)?;
                Ok((
                    RawPageKind::Internal,
                    header.item_count as usize,
                    None,
                    None,
                ))
            }
            RawPageKind::Leaf => {
                validate_leaf_page(header.payload, header.item_count as usize)?;
                Ok((
                    RawPageKind::Leaf,
                    header.item_count as usize,
                    header.next_page_id,
                    None,
                ))
            }
            RawPageKind::Freelist => {
                let ids = parse_freelist_ids(header.payload, header.item_count as usize)?;
                Ok((RawPageKind::Freelist, ids.len(), header.next_page_id, None))
            }
            RawPageKind::Overflow
            | RawPageKind::SuperblockA
            | RawPageKind::SuperblockB
            | RawPageKind::Corrupt => Err("unexpected page kind".to_string()),
        }
    }

    fn leftmost_leaf_page_id(&self) -> Result<Option<u64>, String> {
        let mut current = match self.active.root_page_id {
            0 => return Ok(None),
            id => id,
        };

        loop {
            let raw = self.page_bytes(current)?;
            let header = parse_header(raw, self.page_size)?;
            match header.kind {
                RawPageKind::Leaf => return Ok(Some(current)),
                RawPageKind::Internal => {
                    current = read_u64_at(header.payload, 0, "internal left child")?;
                }
                RawPageKind::Freelist => {
                    return Err(format!("unexpected freelist page {current} in tree path"));
                }
                RawPageKind::Overflow
                | RawPageKind::SuperblockA
                | RawPageKind::SuperblockB
                | RawPageKind::Corrupt => {
                    return Err(format!("unexpected page {current} in tree path"));
                }
            }
        }
    }

    fn parse_leaf_page(&self, page_id: u64) -> Result<LeafPage, String> {
        let raw = self.page_bytes(page_id)?;
        let header = parse_header(raw, self.page_size)?;
        if header.kind != RawPageKind::Leaf {
            return Err("expected leaf page".to_string());
        }

        let entry_count = header.item_count as usize;
        let mut entries = Vec::with_capacity(entry_count);
        for index in 0..entry_count {
            let (key_off, key_len, value_off) = leaf_slot(header.payload, entry_count, index)?;
            let key = slice(header.payload, key_off, key_len, "leaf key")?.to_vec();
            let value = parse_leaf_value_at(header.payload, value_off)?;
            entries.push((key, value));
        }

        Ok(LeafPage {
            entries,
            next_page_id: header.next_page_id,
        })
    }

    fn resolve_value(&self, value: RawValue) -> Result<Vec<u8>, String> {
        match value {
            RawValue::Inline(value) => Ok(value),
            RawValue::Overflow {
                head_page_id,
                total_len,
            } => self.read_overflow_value(head_page_id, total_len),
        }
    }

    fn read_overflow_value(&self, head_page_id: u64, total_len: usize) -> Result<Vec<u8>, String> {
        let page_count = total_len.div_ceil(self.page_size).max(1);
        let mut out = Vec::with_capacity(total_len);
        for index in 0..page_count {
            let page_id = head_page_id
                .checked_add(index as u64)
                .ok_or_else(|| "overflow extent page id overflow".to_string())?;
            let raw = self.page_bytes(page_id)?;
            let remaining = total_len.saturating_sub(out.len());
            if remaining == 0 {
                break;
            }
            let take = remaining.min(self.page_size);
            out.extend_from_slice(&raw[..take]);
        }
        if out.len() != total_len {
            return Err(format!(
                "overflow payload truncated: expected {total_len}, found {}",
                out.len()
            ));
        }
        Ok(out)
    }

    fn read_freelist_pages(&self) -> Result<BTreeSet<u64>, String> {
        let mut out = BTreeSet::new();
        let mut current = self.active.freelist_head_page_id;
        let mut visited = BTreeSet::new();

        while current != 0 {
            if !visited.insert(current) {
                return Err("freelist pages contain a cycle".to_string());
            }
            let raw = self.page_bytes(current)?;
            let header = parse_header(raw, self.page_size)?;
            if header.kind != RawPageKind::Freelist {
                return Err("expected freelist page".to_string());
            }
            for id in parse_freelist_ids(header.payload, header.item_count as usize)? {
                out.insert(id);
            }
            current = header.next_page_id.unwrap_or(0);
        }

        Ok(out)
    }

    fn discover_overflow_pages(&self) -> Result<BTreeMap<u64, OverflowPage>, String> {
        let mut out = BTreeMap::new();
        let mut current = self.leftmost_leaf_page_id()?;
        let mut visited = BTreeSet::new();

        while let Some(page_id) = current {
            if !visited.insert(page_id) {
                return Err("leaf chain contains a cycle".to_string());
            }

            let page = self.parse_leaf_page(page_id)?;
            for (_, value) in &page.entries {
                let RawValue::Overflow {
                    head_page_id,
                    total_len,
                } = value
                else {
                    continue;
                };

                let page_count = total_len.div_ceil(self.page_size).max(1);
                let mut remaining = *total_len;
                for index in 0..page_count {
                    let overflow_page_id = head_page_id
                        .checked_add(index as u64)
                        .ok_or_else(|| "overflow extent page id overflow".to_string())?;
                    if overflow_page_id >= self.total_pages() {
                        break;
                    }
                    let item_count = remaining.min(self.page_size);
                    remaining = remaining.saturating_sub(item_count);
                    let next_page_id =
                        (index + 1 < page_count).then_some(overflow_page_id.saturating_add(1));
                    out.insert(
                        overflow_page_id,
                        OverflowPage {
                            item_count,
                            next_page_id,
                        },
                    );
                }
            }

            current = page.next_page_id;
        }

        Ok(out)
    }

    fn page_bytes(&self, page_id: u64) -> Result<&[u8], String> {
        if page_id >= self.total_pages() {
            return Err(format!(
                "page id {page_id} out of bounds for total_pages {}",
                self.total_pages()
            ));
        }
        let start = page_id
            .checked_mul(self.page_size as u64)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| "page offset overflow".to_string())?;
        let end = start
            .checked_add(self.page_size)
            .ok_or_else(|| "page end offset overflow".to_string())?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| format!("page {page_id} exceeds file bounds"))
    }
}

#[derive(Debug, Clone)]
pub struct RawPageBatch {
    pub pages: Vec<RawPageSummary>,
    pub next_page_id: Option<u64>,
    pub done: bool,
}

fn choose_active(
    a: Option<Superblock>,
    b: Option<Superblock>,
) -> Option<(SuperblockSlot, Superblock)> {
    match (a, b) {
        (Some(a), Some(b)) => {
            if b.generation > a.generation {
                Some((SuperblockSlot::B, b))
            } else {
                Some((SuperblockSlot::A, a))
            }
        }
        (Some(a), None) => Some((SuperblockSlot::A, a)),
        (None, Some(b)) => Some((SuperblockSlot::B, b)),
        (None, None) => None,
    }
}

fn read_superblock_slot(
    bytes: &[u8],
    slot: SuperblockSlot,
    page_size: usize,
) -> Option<Superblock> {
    let offset = usize::try_from(slot.byte_offset(page_size)).ok()?;
    let end = offset.checked_add(page_size)?;
    let page = bytes.get(offset..end)?;
    if page.iter().all(|byte| *byte == 0) {
        return None;
    }
    decode_superblock(page, page_size).ok()
}

fn decode_superblock(page: &[u8], expected_page_size: usize) -> Result<Superblock, String> {
    if page.len() < SUPERBLOCK_ENCODED_BYTES {
        return Err(format!("superblock page too small: {}", page.len()));
    }
    if page[SUPERBLOCK_OFFSET_MAGIC..SUPERBLOCK_OFFSET_MAGIC + 8] != SUPERBLOCK_MAGIC {
        return Err("superblock magic mismatch".to_string());
    }

    let version = read_u32_at(page, SUPERBLOCK_OFFSET_VERSION, "superblock version")?;
    if version != SUPERBLOCK_FORMAT_VERSION {
        return Err(format!("unsupported superblock version {version}"));
    }

    let page_size = read_u32_at(page, SUPERBLOCK_OFFSET_PAGE_SIZE, "superblock page size")?;
    if page_size as usize != expected_page_size {
        return Err(format!(
            "page size mismatch: found {page_size}, expected {expected_page_size}"
        ));
    }

    let expected_checksum = read_u32_at(page, SUPERBLOCK_OFFSET_CHECKSUM, "superblock checksum")?;
    let actual_checksum = xxh3::xxh3_64(&page[..SUPERBLOCK_OFFSET_CHECKSUM]) as u32;
    if expected_checksum != actual_checksum {
        return Err(format!(
            "superblock checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
        ));
    }

    Ok(Superblock {
        page_size,
        generation: read_u64_at(page, SUPERBLOCK_OFFSET_GENERATION, "superblock generation")?,
        root_page_id: read_u64_at(page, SUPERBLOCK_OFFSET_ROOT_PAGE_ID, "superblock root")?,
        freelist_head_page_id: read_u64_at(
            page,
            SUPERBLOCK_OFFSET_FREELIST_HEAD_PAGE_ID,
            "superblock freelist",
        )?,
        total_pages: read_u64_at(
            page,
            SUPERBLOCK_OFFSET_TOTAL_PAGES,
            "superblock total pages",
        )?,
    })
}

fn parse_header<'a>(raw: &'a [u8], expected_page_size: usize) -> Result<RawPageHeader<'a>, String> {
    if raw.len() != expected_page_size {
        return Err(format!(
            "page length mismatch: found {}, expected {}",
            raw.len(),
            expected_page_size
        ));
    }
    if raw.len() < PAGE_HEADER_BYTES {
        return Err("page too small".to_string());
    }
    if raw[..4] != PAGE_MAGIC {
        return Err("page magic mismatch".to_string());
    }

    Ok(RawPageHeader {
        kind: decode_kind(raw[4])?,
        next_page_id: nonzero(read_u64_at(raw, 8, "page next")?),
        item_count: read_u32_at(raw, 16, "page item count")?,
        payload: &raw[PAGE_HEADER_BYTES..],
    })
}

fn decode_kind(kind: u8) -> Result<RawPageKind, String> {
    match kind {
        KIND_INTERNAL => Ok(RawPageKind::Internal),
        KIND_LEAF => Ok(RawPageKind::Leaf),
        KIND_FREELIST => Ok(RawPageKind::Freelist),
        _ => Err(format!("unknown page kind {kind}")),
    }
}

fn validate_internal_page(payload: &[u8], key_count: usize) -> Result<(), String> {
    let slots_end = internal_slots_bytes(key_count)?;
    if payload.len() < slots_end {
        return Err("internal page payload shorter than slot directory".to_string());
    }
    let _ = read_u64_at(payload, 0, "internal left child")?;
    for index in 0..key_count {
        let (key_off, key_len) = internal_slot(payload, key_count, index)?;
        let _ = slice(payload, key_off, key_len, "internal key")?;
    }
    Ok(())
}

fn validate_leaf_page(payload: &[u8], entry_count: usize) -> Result<(), String> {
    let slots_end = leaf_slots_bytes(entry_count)?;
    if payload.len() < slots_end {
        return Err("leaf page payload shorter than slot directory".to_string());
    }
    for index in 0..entry_count {
        let (key_off, key_len, value_off) = leaf_slot(payload, entry_count, index)?;
        let _ = slice(payload, key_off, key_len, "leaf key")?;
        let _ = parse_leaf_value_at(payload, value_off)?;
    }
    Ok(())
}

fn parse_freelist_ids(payload: &[u8], id_count: usize) -> Result<Vec<u64>, String> {
    let mut ids = Vec::with_capacity(id_count);
    let mut cursor = 0usize;
    for _ in 0..id_count {
        ids.push(take_u64(payload, &mut cursor, "freelist id")?);
    }
    Ok(ids)
}

fn parse_leaf_value_at(payload: &[u8], value_offset: usize) -> Result<RawValue, String> {
    let mut cursor = value_offset;
    let tag = take_u8(payload, &mut cursor, "leaf value tag")?;
    match tag {
        0 => {
            let value_len = take_u32(payload, &mut cursor, "inline value length")? as usize;
            let value = take_bytes(payload, &mut cursor, value_len, "inline value")?.to_vec();
            Ok(RawValue::Inline(value))
        }
        1 => {
            let head_page_id = take_u64(payload, &mut cursor, "overflow head")?;
            let total_len = take_u32(payload, &mut cursor, "overflow length")? as usize;
            Ok(RawValue::Overflow {
                head_page_id,
                total_len,
            })
        }
        _ => Err(format!("invalid leaf value tag {tag}")),
    }
}

fn internal_slots_bytes(key_count: usize) -> Result<usize, String> {
    INTERNAL_LEFT_CHILD_BYTES
        .checked_add(
            key_count
                .checked_mul(INTERNAL_SLOT_BYTES)
                .ok_or_else(|| "internal slot byte count overflow".to_string())?,
        )
        .ok_or_else(|| "internal slot layout overflow".to_string())
}

fn leaf_slots_bytes(entry_count: usize) -> Result<usize, String> {
    entry_count
        .checked_mul(LEAF_SLOT_BYTES)
        .ok_or_else(|| "leaf slot byte count overflow".to_string())
}

fn internal_slot(payload: &[u8], key_count: usize, index: usize) -> Result<(usize, usize), String> {
    if index >= key_count {
        return Err(format!(
            "internal slot index {index} out of bounds {key_count}"
        ));
    }
    let base = INTERNAL_LEFT_CHILD_BYTES
        .checked_add(
            index
                .checked_mul(INTERNAL_SLOT_BYTES)
                .ok_or_else(|| "internal slot offset overflow".to_string())?,
        )
        .ok_or_else(|| "internal slot base overflow".to_string())?;
    Ok((
        read_u32_at(payload, base, "internal key offset")? as usize,
        read_u32_at(payload, base + 4, "internal key length")? as usize,
    ))
}

fn leaf_slot(
    payload: &[u8],
    entry_count: usize,
    index: usize,
) -> Result<(usize, usize, usize), String> {
    if index >= entry_count {
        return Err(format!(
            "leaf slot index {index} out of bounds {entry_count}"
        ));
    }
    let slots_end = leaf_slots_bytes(entry_count)?;
    if payload.len() < slots_end {
        return Err("leaf payload shorter than slot directory".to_string());
    }
    let base = index
        .checked_mul(LEAF_SLOT_BYTES)
        .ok_or_else(|| "leaf slot offset overflow".to_string())?;
    Ok((
        read_u32_at(payload, base, "leaf key offset")? as usize,
        read_u32_at(payload, base + 4, "leaf key length")? as usize,
        read_u32_at(payload, base + 8, "leaf value offset")? as usize,
    ))
}

fn slice<'a>(bytes: &'a [u8], offset: usize, len: usize, label: &str) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| format!("{label} offset overflow"))?;
    bytes
        .get(offset..end)
        .ok_or_else(|| format!("{label} exceeds payload bounds"))
}

fn read_u32_at(bytes: &[u8], offset: usize, label: &str) -> Result<u32, String> {
    let bytes = slice(bytes, offset, 4, label)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
}

fn read_u64_at(bytes: &[u8], offset: usize, label: &str) -> Result<u64, String> {
    let bytes = slice(bytes, offset, 8, label)?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("u64 bytes")))
}

fn take_u8(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<u8, String> {
    let bytes = take_bytes(bytes, cursor, 1, label)?;
    Ok(bytes[0])
}

fn take_u32(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<u32, String> {
    let bytes = take_bytes(bytes, cursor, 4, label)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
}

fn take_u64(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<u64, String> {
    let bytes = take_bytes(bytes, cursor, 8, label)?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("u64 bytes")))
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| format!("{label} cursor overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| format!("{label} exceeds payload bounds"))?;
    *cursor = end;
    Ok(value)
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use opfs_btree::{BTreeError, BTreeOptions, OpfsBTree, SyncFile};

    use super::*;

    #[derive(Clone, Default)]
    struct TestFile {
        inner: Rc<RefCell<Vec<u8>>>,
    }

    impl TestFile {
        fn bytes(&self) -> Vec<u8> {
            self.inner.borrow().clone()
        }
    }

    impl SyncFile for TestFile {
        fn len(&self) -> Result<u64, BTreeError> {
            Ok(self.inner.borrow().len() as u64)
        }

        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BTreeError> {
            let offset = usize::try_from(offset)
                .map_err(|_| BTreeError::Io("offset does not fit in usize".to_string()))?;
            let end = offset
                .checked_add(buf.len())
                .ok_or_else(|| BTreeError::Io("read overflow".to_string()))?;
            let data = self.inner.borrow();
            let src = data
                .get(offset..end)
                .ok_or_else(|| BTreeError::Io("unexpected eof".to_string()))?;
            buf.copy_from_slice(src);
            Ok(())
        }

        fn write_all_at(&self, offset: u64, buf: &[u8]) -> Result<(), BTreeError> {
            let offset = usize::try_from(offset)
                .map_err(|_| BTreeError::Io("offset does not fit in usize".to_string()))?;
            let end = offset
                .checked_add(buf.len())
                .ok_or_else(|| BTreeError::Io("write overflow".to_string()))?;
            let mut data = self.inner.borrow_mut();
            if end > data.len() {
                data.resize(end, 0);
            }
            data[offset..end].copy_from_slice(buf);
            Ok(())
        }

        fn truncate(&self, len: u64) -> Result<(), BTreeError> {
            let len = usize::try_from(len)
                .map_err(|_| BTreeError::Io("truncate length does not fit in usize".to_string()))?;
            self.inner.borrow_mut().resize(len, 0);
            Ok(())
        }

        fn flush(&self) -> Result<(), BTreeError> {
            Ok(())
        }
    }

    #[test]
    fn scans_entries_from_persisted_bytes() {
        let file = TestFile::default();
        let mut tree =
            OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open btree fixture");
        tree.put(b"raw:coValues:a", b"alpha").expect("put alpha");
        tree.put(b"raw:coValues:b", b"beta").expect("put beta");
        tree.checkpoint().expect("checkpoint fixture");

        let store = RawStore::open(file.bytes()).expect("open raw store");
        let batch = store.raw_entries_batch(None, 10).expect("scan entries");

        assert!(batch.done);
        assert_eq!(
            batch.entries,
            vec![
                (b"raw:coValues:a".to_vec(), b"alpha".to_vec()),
                (b"raw:coValues:b".to_vec(), b"beta".to_vec()),
            ]
        );
    }

    #[test]
    fn classifies_superblocks_tree_pages_and_overflow_extents() {
        let file = TestFile::default();
        let mut tree =
            OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open btree fixture");
        tree.put(b"raw:coValues:a", b"alpha").expect("put alpha");
        tree.put(b"raw:coValues:big", &vec![7u8; 25_000])
            .expect("put big");
        tree.checkpoint().expect("checkpoint fixture");

        let store = RawStore::open(file.bytes()).expect("open raw store");
        let batch = store.raw_page_summaries_batch(0, usize::MAX);

        assert!(batch.done);
        assert_eq!(batch.pages[0].kind, RawPageKind::SuperblockA);
        assert_eq!(batch.pages[1].kind, RawPageKind::SuperblockB);
        assert_eq!(batch.pages.iter().filter(|page| page.is_active).count(), 1);
        assert!(batch.pages.iter().any(|page| page.is_root));
        assert!(
            batch
                .pages
                .iter()
                .any(|page| page.kind == RawPageKind::Leaf)
        );
        assert!(
            batch
                .pages
                .iter()
                .any(|page| page.kind == RawPageKind::Overflow)
        );
    }
}
