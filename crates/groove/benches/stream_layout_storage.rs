//! Opt-in compressed RocksDB receipt for ordinary-row stream layouts.
//!
//! `JAZZ_STREAM_DISK_BENCH=1 cargo bench -p groove --bench stream_layout_storage`

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use rocksdb::{ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch};
use serde::Serialize;
use tempfile::TempDir;

const HISTORY: &str = "history";
const PARTS: &str = "parts";
const NODES: &str = "nodes";
const FANOUT: usize = 32;
const INLINE_TAIL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Layout {
    Flat,
    PersistentTree,
    PersistentTreeInlineTail,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum PayloadKind {
    Binary,
    Text,
}

#[derive(Clone, Debug)]
struct Node {
    height: u32,
    children: Vec<(u64, u64)>,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct DiskBytes {
    apparent: u64,
    allocated: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Receipt {
    layout: Layout,
    payload_kind: PayloadKind,
    appends: usize,
    append_bytes: usize,
    logical_payload_bytes: usize,
    history_rows: usize,
    part_rows: usize,
    node_rows: usize,
    consolidations: usize,
    tree_depth: u32,
    compression: &'static str,
    empty_store: DiskBytes,
    before_memtable_flush: DiskBytes,
    after_memtable_flush: DiskBytes,
    after_full_compaction: DiskBytes,
}

struct Writer<'a> {
    db: &'a DB,
    layout: Layout,
    nodes: BTreeMap<u64, Node>,
    next_id: u64,
    root: Option<u64>,
    prefix_bytes: u64,
    tail: Vec<u8>,
    part_ids: Vec<(u64, u64)>,
    part_rows: usize,
    node_rows: usize,
    consolidations: usize,
}

impl<'a> Writer<'a> {
    fn new(db: &'a DB, layout: Layout) -> Self {
        Self {
            db,
            layout,
            nodes: BTreeMap::new(),
            next_id: 1,
            root: None,
            prefix_bytes: 0,
            tail: Vec::new(),
            part_ids: Vec::new(),
            part_rows: 0,
            node_rows: 0,
            consolidations: 0,
        }
    }

    fn append(&mut self, ordinal: usize, bytes: &[u8]) {
        let mut batch = WriteBatch::default();
        match self.layout {
            Layout::Flat => {
                let part = self.write_part(&mut batch, bytes);
                self.part_ids.push((part, bytes.len() as u64));
            }
            Layout::PersistentTree => {
                let part = self.write_part(&mut batch, bytes);
                self.append_part(&mut batch, part, bytes.len() as u64);
            }
            Layout::PersistentTreeInlineTail => {
                self.tail.extend_from_slice(bytes);
                if self.tail.len() > INLINE_TAIL_BYTES {
                    let consolidated = std::mem::take(&mut self.tail);
                    let length = consolidated.len() as u64;
                    let part = self.write_part(&mut batch, &consolidated);
                    self.append_part(&mut batch, part, length);
                    self.consolidations += 1;
                }
            }
        }
        let value = self.root_value();
        batch.put_cf(
            self.db.cf_handle(HISTORY).expect("history CF"),
            history_key(ordinal),
            value,
        );
        self.db.write(&batch).expect("append batch");
    }

    fn write_part(&mut self, batch: &mut WriteBatch, bytes: &[u8]) -> u64 {
        let id = self.allocate_id();
        batch.put_cf(
            self.db.cf_handle(PARTS).expect("parts CF"),
            id.to_be_bytes(),
            bytes,
        );
        self.part_rows += 1;
        id
    }

    fn append_part(&mut self, batch: &mut WriteBatch, part: u64, length: u64) {
        self.prefix_bytes += length;
        self.root = Some(match self.root {
            None => self.write_node(
                batch,
                Node {
                    height: 0,
                    children: vec![(part, length)],
                },
            ),
            Some(root) => {
                let (updated, overflow) = self.append_to_node(batch, root, part, length);
                if let Some(overflow) = overflow {
                    let height = self.nodes[&updated].height + 1;
                    let left_length = node_length(&self.nodes[&updated]);
                    let right_length = node_length(&self.nodes[&overflow]);
                    self.write_node(
                        batch,
                        Node {
                            height,
                            children: vec![(updated, left_length), (overflow, right_length)],
                        },
                    )
                } else {
                    updated
                }
            }
        });
    }

    fn append_to_node(
        &mut self,
        batch: &mut WriteBatch,
        node_id: u64,
        part: u64,
        length: u64,
    ) -> (u64, Option<u64>) {
        let mut node = self.nodes[&node_id].clone();
        if node.height == 0 {
            node.children.push((part, length));
        } else {
            let (right_id, _) = *node.children.last().expect("non-empty node");
            let (updated, overflow) = self.append_to_node(batch, right_id, part, length);
            let last = node.children.last_mut().expect("non-empty node");
            *last = (updated, node_length(&self.nodes[&updated]));
            if let Some(overflow) = overflow {
                node.children
                    .push((overflow, node_length(&self.nodes[&overflow])));
            }
        }
        if node.children.len() <= FANOUT {
            return (self.write_node(batch, node), None);
        }
        let right_children = node.children.split_off(FANOUT / 2);
        let height = node.height;
        let left = self.write_node(batch, node);
        let right = self.write_node(
            batch,
            Node {
                height,
                children: right_children,
            },
        );
        (left, Some(right))
    }

    fn write_node(&mut self, batch: &mut WriteBatch, node: Node) -> u64 {
        let id = self.allocate_id();
        batch.put_cf(
            self.db.cf_handle(NODES).expect("nodes CF"),
            id.to_be_bytes(),
            encode_node(&node),
        );
        self.nodes.insert(id, node);
        self.node_rows += 1;
        id
    }

    fn root_value(&self) -> Vec<u8> {
        let mut value = Vec::new();
        match self.layout {
            Layout::Flat => {
                value.extend_from_slice(&(self.part_ids.len() as u64).to_le_bytes());
                for (id, length) in &self.part_ids {
                    value.extend_from_slice(&id.to_le_bytes());
                    value.extend_from_slice(&length.to_le_bytes());
                }
            }
            Layout::PersistentTree | Layout::PersistentTreeInlineTail => {
                value.extend_from_slice(&self.root.unwrap_or_default().to_le_bytes());
                value.extend_from_slice(&self.prefix_bytes.to_le_bytes());
                value.extend_from_slice(&(self.tail.len() as u64).to_le_bytes());
                value.extend_from_slice(&self.tail);
            }
        }
        value
    }

    fn allocate_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

fn main() {
    if std::env::var("JAZZ_STREAM_DISK_BENCH").as_deref() != Ok("1") {
        eprintln!("skipped; set JAZZ_STREAM_DISK_BENCH=1");
        return;
    }
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let short_appends = env_usize("JAZZ_STREAM_DISK_SHORT_APPENDS", 1_000);
    let large_appends = env_usize("JAZZ_STREAM_DISK_LARGE_APPENDS", 256);
    for payload_kind in [PayloadKind::Binary, PayloadKind::Text] {
        for (appends, append_bytes) in [
            (short_appends, 32),
            (short_appends, 1_024),
            (large_appends, 64 * 1024),
        ] {
            for layout in [
                Layout::Flat,
                Layout::PersistentTree,
                Layout::PersistentTreeInlineTail,
            ] {
                println!(
                    "{}",
                    serde_json::to_string(&measure(layout, payload_kind, appends, append_bytes))
                        .expect("serialize receipt")
                );
            }
        }
    }
}

fn measure(
    layout: Layout,
    payload_kind: PayloadKind,
    appends: usize,
    append_bytes: usize,
) -> Receipt {
    let dir = TempDir::new().expect("temporary RocksDB directory");
    let db = open_db(dir.path());
    flush_and_compact(&db);
    let empty_store = disk_bytes(dir.path());
    let mut writer = Writer::new(&db, layout);
    for ordinal in 0..appends {
        writer.append(ordinal, &payload(payload_kind, ordinal, append_bytes));
    }
    db.flush_wal(true).expect("flush WAL before measuring");
    let before_memtable_flush = disk_bytes(dir.path());
    flush_memtables(&db);
    let after_memtable_flush = disk_bytes(dir.path());
    compact(&db);
    let after_full_compaction = disk_bytes(dir.path());
    let tree_depth = writer
        .root
        .map(|root| writer.nodes[&root].height + 1)
        .unwrap_or(0);
    Receipt {
        layout,
        payload_kind,
        appends,
        append_bytes,
        logical_payload_bytes: appends * append_bytes,
        history_rows: appends,
        part_rows: writer.part_rows,
        node_rows: writer.node_rows,
        consolidations: writer.consolidations,
        tree_depth,
        compression: "history=zstd; parts/nodes=lz4; bottommost=zstd",
        empty_store,
        before_memtable_flush,
        after_memtable_flush,
        after_full_compaction,
    }
}

fn open_db(path: &Path) -> DB {
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    let descriptors = [HISTORY, PARTS, NODES].map(|name| {
        let mut options = Options::default();
        options.set_compression_type(if name == HISTORY {
            DBCompressionType::Zstd
        } else {
            DBCompressionType::Lz4
        });
        options.set_bottommost_compression_type(DBCompressionType::Zstd);
        ColumnFamilyDescriptor::new(name, options)
    });
    DB::open_cf_descriptors(&db_options, path, descriptors).expect("open RocksDB")
}

fn flush_memtables(db: &DB) {
    for cf in [HISTORY, PARTS, NODES] {
        db.flush_cf(db.cf_handle(cf).expect("column family"))
            .expect("flush memtable");
    }
}

fn compact(db: &DB) {
    for cf in [HISTORY, PARTS, NODES] {
        db.compact_range_cf(
            db.cf_handle(cf).expect("column family"),
            None::<&[u8]>,
            None::<&[u8]>,
        );
    }
}

fn flush_and_compact(db: &DB) {
    flush_memtables(db);
    compact(db);
}

fn payload(kind: PayloadKind, ordinal: usize, length: usize) -> Vec<u8> {
    match kind {
        PayloadKind::Text => (0..length)
            .map(|index| b"The quick brown fox jumps over the lazy dog. "[(index + ordinal) % 45])
            .collect(),
        PayloadKind::Binary => {
            let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            (0..length)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect()
        }
    }
}

fn history_key(ordinal: usize) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(b"stream-0");
    key[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    key
}

fn encode_node(node: &Node) -> Vec<u8> {
    let mut value = Vec::with_capacity(8 + node.children.len() * 16);
    value.extend_from_slice(&node.height.to_le_bytes());
    value.extend_from_slice(&(node.children.len() as u32).to_le_bytes());
    for (id, length) in &node.children {
        value.extend_from_slice(&id.to_le_bytes());
        value.extend_from_slice(&length.to_le_bytes());
    }
    value
}

fn node_length(node: &Node) -> u64 {
    node.children.iter().map(|(_, length)| length).sum()
}

fn disk_bytes(path: &Path) -> DiskBytes {
    let mut result = DiskBytes {
        apparent: 0,
        allocated: if cfg!(unix) { Some(0) } else { None },
    };
    for entry in fs::read_dir(path).expect("read RocksDB directory") {
        let entry = entry.expect("directory entry");
        let metadata = entry.metadata().expect("entry metadata");
        if metadata.is_dir() {
            let nested = disk_bytes(&entry.path());
            result.apparent += nested.apparent;
            result.allocated = result
                .allocated
                .zip(nested.allocated)
                .map(|(left, right)| left + right);
        } else {
            result.apparent += metadata.len();
            #[cfg(unix)]
            if let Some(allocated) = result.allocated.as_mut() {
                *allocated += metadata.blocks() * 512;
            }
        }
    }
    result
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
