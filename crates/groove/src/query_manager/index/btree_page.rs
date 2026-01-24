//! B-tree page types and serialization.
//!
//! Pages are the storage unit for B-tree indices. Each page is stored as a separate
//! storage key, enabling incremental updates (only changed pages are written) and
//! lazy loading (pages are loaded on demand as queries traverse the tree).
//!
//! Binary format optimized for compact storage:
//!
//! Internal node:
//! ```text
//! [type: u8 = 0]
//! [key_count: u16]
//! [children: PageId × (key_count + 1)]  // PageId = u64
//! [keys: (len: u16, data: bytes) × key_count]
//! ```
//!
//! Leaf node:
//! ```text
//! [type: u8 = 1]
//! [entry_count: u16]
//! [entries × entry_count]:
//!   [key_len: u16][key: bytes]
//!   [row_count: u32][row_ids: 16 bytes × row_count]
//! ```

use std::collections::HashSet;

use uuid::Uuid;

use crate::object::ObjectId;

/// Page ID for B-tree nodes.
///
/// Page IDs are local to each index and monotonically increasing.
/// Page 0 is always the root page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u64);

impl PageId {
    pub const ROOT: PageId = PageId(0);

    pub fn next(self) -> PageId {
        PageId(self.0 + 1)
    }
}

/// A B-tree page (internal or leaf).
#[derive(Debug, Clone)]
pub enum BTreePage {
    /// Internal node: contains keys and child page pointers.
    Internal {
        /// Separator keys (n keys for n+1 children).
        keys: Vec<Vec<u8>>,
        /// Child page IDs (one more than keys).
        children: Vec<PageId>,
    },
    /// Leaf node: contains key-value entries.
    Leaf {
        /// Entries sorted by key. Each entry maps a key to a set of row IDs.
        entries: Vec<LeafEntry>,
        /// Pointer to the next leaf page (for range scans).
        next_leaf: Option<PageId>,
    },
}

/// A leaf entry: key → set of row IDs.
#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub row_ids: HashSet<ObjectId>,
}

impl LeafEntry {
    pub fn new(key: Vec<u8>, row_id: ObjectId) -> Self {
        let mut row_ids = HashSet::new();
        row_ids.insert(row_id);
        Self { key, row_ids }
    }

    pub fn with_row_ids(key: Vec<u8>, row_ids: HashSet<ObjectId>) -> Self {
        Self { key, row_ids }
    }
}

impl BTreePage {
    /// Create a new empty leaf page.
    pub fn new_leaf() -> Self {
        BTreePage::Leaf {
            entries: Vec::new(),
            next_leaf: None,
        }
    }

    /// Create a new internal page with a single child (for root split).
    pub fn new_internal(left_child: PageId) -> Self {
        BTreePage::Internal {
            keys: Vec::new(),
            children: vec![left_child],
        }
    }

    /// Check if this is a leaf page.
    pub fn is_leaf(&self) -> bool {
        matches!(self, BTreePage::Leaf { .. })
    }

    /// Serialize page to binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            BTreePage::Internal { keys, children } => {
                // Type byte
                buf.push(0);

                // Key count
                let key_count = keys.len() as u16;
                buf.extend_from_slice(&key_count.to_le_bytes());

                // Children (key_count + 1 page IDs)
                for child in children {
                    buf.extend_from_slice(&child.0.to_le_bytes());
                }

                // Keys
                for key in keys {
                    buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
                    buf.extend_from_slice(key);
                }
            }
            BTreePage::Leaf { entries, next_leaf } => {
                // Type byte
                buf.push(1);

                // Entry count
                let entry_count = entries.len() as u16;
                buf.extend_from_slice(&entry_count.to_le_bytes());

                // Next leaf pointer (0 = None, otherwise page_id + 1)
                let next_leaf_val = next_leaf.map(|p| p.0 + 1).unwrap_or(0);
                buf.extend_from_slice(&next_leaf_val.to_le_bytes());

                // Entries
                for entry in entries {
                    // Key
                    buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
                    buf.extend_from_slice(&entry.key);

                    // Row IDs
                    let row_count = entry.row_ids.len() as u32;
                    buf.extend_from_slice(&row_count.to_le_bytes());
                    for row_id in &entry.row_ids {
                        buf.extend_from_slice(row_id.uuid().as_bytes());
                    }
                }
            }
        }

        buf
    }

    /// Deserialize page from binary format.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let page_type = data[0];
        let mut pos = 1;

        match page_type {
            0 => {
                // Internal node
                if pos + 2 > data.len() {
                    return None;
                }
                let key_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;

                // Children
                let child_count = key_count + 1;
                let mut children = Vec::with_capacity(child_count);
                for _ in 0..child_count {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    let page_id = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    children.push(PageId(page_id));
                    pos += 8;
                }

                // Keys
                let mut keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    if pos + 2 > data.len() {
                        return None;
                    }
                    let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;

                    if pos + key_len > data.len() {
                        return None;
                    }
                    keys.push(data[pos..pos + key_len].to_vec());
                    pos += key_len;
                }

                Some(BTreePage::Internal { keys, children })
            }
            1 => {
                // Leaf node
                if pos + 2 > data.len() {
                    return None;
                }
                let entry_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;

                // Next leaf pointer
                if pos + 8 > data.len() {
                    return None;
                }
                let next_leaf_val = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                let next_leaf = if next_leaf_val == 0 {
                    None
                } else {
                    Some(PageId(next_leaf_val - 1))
                };
                pos += 8;

                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    // Key
                    if pos + 2 > data.len() {
                        return None;
                    }
                    let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;

                    if pos + key_len > data.len() {
                        return None;
                    }
                    let key = data[pos..pos + key_len].to_vec();
                    pos += key_len;

                    // Row IDs
                    if pos + 4 > data.len() {
                        return None;
                    }
                    let row_count =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;

                    let mut row_ids = HashSet::with_capacity(row_count);
                    for _ in 0..row_count {
                        if pos + 16 > data.len() {
                            return None;
                        }
                        let uuid = Uuid::from_slice(&data[pos..pos + 16]).ok()?;
                        row_ids.insert(ObjectId::from_uuid(uuid));
                        pos += 16;
                    }

                    entries.push(LeafEntry { key, row_ids });
                }

                Some(BTreePage::Leaf { entries, next_leaf })
            }
            _ => None,
        }
    }

    /// Estimate memory size of this page.
    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();

        match self {
            BTreePage::Internal { keys, children } => {
                size += children.capacity() * std::mem::size_of::<PageId>();
                for key in keys {
                    size += key.capacity() + std::mem::size_of::<Vec<u8>>();
                }
            }
            BTreePage::Leaf { entries, .. } => {
                for entry in entries {
                    size += entry.key.capacity();
                    size += entry.row_ids.capacity() * std::mem::size_of::<ObjectId>();
                    size += std::mem::size_of::<LeafEntry>();
                }
            }
        }

        size
    }
}

/// Index metadata: stored separately from pages.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    /// ID of the root page.
    pub root_page_id: PageId,
    /// Next page ID to allocate.
    pub next_page_id: u64,
    /// Total number of entries in the index.
    pub entry_count: u64,
}

impl IndexMeta {
    pub fn new() -> Self {
        Self {
            root_page_id: PageId::ROOT,
            next_page_id: 1, // 0 is root
            entry_count: 0,
        }
    }

    /// Serialize metadata to binary.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(24);
        buf.extend_from_slice(&self.root_page_id.0.to_le_bytes());
        buf.extend_from_slice(&self.next_page_id.to_le_bytes());
        buf.extend_from_slice(&self.entry_count.to_le_bytes());
        buf
    }

    /// Deserialize metadata from binary.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }

        let root_page_id = PageId(u64::from_le_bytes(data[0..8].try_into().ok()?));
        let next_page_id = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let entry_count = u64::from_le_bytes(data[16..24].try_into().ok()?);

        Some(Self {
            root_page_id,
            next_page_id,
            entry_count,
        })
    }
}

impl Default for IndexMeta {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_page_serialize_roundtrip() {
        let mut entries = Vec::new();
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        entries.push(LeafEntry::new(b"key1".to_vec(), row1));
        entries.push(LeafEntry::new(b"key2".to_vec(), row2));

        let page = BTreePage::Leaf {
            entries,
            next_leaf: None,
        };
        let serialized = page.serialize();
        let deserialized = BTreePage::deserialize(&serialized).unwrap();

        match deserialized {
            BTreePage::Leaf { entries, .. } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].key, b"key1");
                assert!(entries[0].row_ids.contains(&row1));
                assert_eq!(entries[1].key, b"key2");
                assert!(entries[1].row_ids.contains(&row2));
            }
            _ => panic!("expected leaf"),
        }
    }

    #[test]
    fn internal_page_serialize_roundtrip() {
        let page = BTreePage::Internal {
            keys: vec![b"abc".to_vec(), b"xyz".to_vec()],
            children: vec![PageId(1), PageId(2), PageId(3)],
        };

        let serialized = page.serialize();
        let deserialized = BTreePage::deserialize(&serialized).unwrap();

        match deserialized {
            BTreePage::Internal { keys, children } => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], b"abc");
                assert_eq!(keys[1], b"xyz");
                assert_eq!(children.len(), 3);
                assert_eq!(children[0], PageId(1));
                assert_eq!(children[1], PageId(2));
                assert_eq!(children[2], PageId(3));
            }
            _ => panic!("expected internal"),
        }
    }

    #[test]
    fn empty_leaf_serialize_roundtrip() {
        let page = BTreePage::new_leaf();
        let serialized = page.serialize();
        let deserialized = BTreePage::deserialize(&serialized).unwrap();

        match deserialized {
            BTreePage::Leaf { entries, .. } => {
                assert!(entries.is_empty());
            }
            _ => panic!("expected leaf"),
        }
    }

    #[test]
    fn index_meta_serialize_roundtrip() {
        let meta = IndexMeta {
            root_page_id: PageId(42),
            next_page_id: 100,
            entry_count: 500,
        };

        let serialized = meta.serialize();
        let deserialized = IndexMeta::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.root_page_id, PageId(42));
        assert_eq!(deserialized.next_page_id, 100);
        assert_eq!(deserialized.entry_count, 500);
    }

    #[test]
    fn leaf_entry_with_multiple_row_ids() {
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let mut row_ids = HashSet::new();
        row_ids.insert(row1);
        row_ids.insert(row2);

        let entry = LeafEntry::with_row_ids(b"key".to_vec(), row_ids.clone());
        let page = BTreePage::Leaf {
            entries: vec![entry],
            next_leaf: None,
        };

        let serialized = page.serialize();
        let deserialized = BTreePage::deserialize(&serialized).unwrap();

        match deserialized {
            BTreePage::Leaf { entries, .. } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].row_ids.len(), 2);
                assert!(entries[0].row_ids.contains(&row1));
                assert!(entries[0].row_ids.contains(&row2));
            }
            _ => panic!("expected leaf"),
        }
    }
}
