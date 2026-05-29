use crate::{Error, Result};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const CHUNK_BYTES: usize = 4096;
const DELTA_MAGIC: &[u8; 5] = b"JTOP1";

pub type TextRoot = Option<i64>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeltaWatermark {
    pub op_id: i64,
    pub snapshot_id: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeltaStats {
    pub ops: usize,
    pub snapshots: usize,
    pub chunks: usize,
    pub uncompressed_bytes: usize,
    pub compressed_bytes: usize,
}

pub struct EncodedDelta {
    pub bytes: Vec<u8>,
    pub stats: DeltaStats,
}

pub fn install(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jazz_text_op (
          op_id INTEGER PRIMARY KEY,
          parent_op_id INTEGER,
          start_byte INTEGER NOT NULL,
          delete_bytes INTEGER NOT NULL,
          insert_text TEXT NOT NULL,
          resulting_len INTEGER NOT NULL,
          depth_since_snapshot INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS jazz_text_chunk (
          chunk_hash BLOB PRIMARY KEY,
          text TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_text_snapshot (
          snapshot_id INTEGER PRIMARY KEY,
          op_id INTEGER NOT NULL UNIQUE,
          byte_len INTEGER NOT NULL,
          chunk_hashes BLOB NOT NULL
        );
        "#,
    )?;
    ensure_depth_since_snapshot_column(conn)?;
    Ok(())
}

fn ensure_depth_since_snapshot_column(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(jazz_text_op)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == "depth_since_snapshot" {
            return Ok(());
        }
    }
    conn.execute(
        "ALTER TABLE jazz_text_op
         ADD COLUMN depth_since_snapshot INTEGER NOT NULL DEFAULT 1",
        [],
    )?;
    Ok(())
}

pub fn append(
    conn: &Connection,
    root: TextRoot,
    text: &str,
    snapshot_every: usize,
) -> Result<TextRoot> {
    if text.is_empty() {
        return Ok(root);
    }
    let start = root_len(conn, root)? as usize;
    replace_range(conn, root, start, 0, text, snapshot_every)
}

pub(crate) fn append_with_materialized(
    conn: &Connection,
    root: TextRoot,
    materialized_text: &mut String,
    text: &str,
    snapshot_every: usize,
) -> Result<TextRoot> {
    let depth_since_snapshot = next_depth_since_snapshot(conn, root)?;
    append_with_materialized_at_depth(
        conn,
        root,
        materialized_text,
        text,
        snapshot_every,
        depth_since_snapshot,
    )
}

pub(crate) fn append_with_materialized_at_depth(
    conn: &Connection,
    root: TextRoot,
    materialized_text: &mut String,
    text: &str,
    snapshot_every: usize,
    depth_since_snapshot: i64,
) -> Result<TextRoot> {
    if text.is_empty() {
        return Ok(root);
    }
    let start_byte = materialized_text.len();
    conn.prepare_cached(
        "INSERT INTO jazz_text_op
           (parent_op_id, start_byte, delete_bytes, insert_text, resulting_len, depth_since_snapshot)
         VALUES (?, ?, 0, ?, ?, ?)",
    )?
    .execute(params![
        root,
        start_byte as i64,
        text,
        (start_byte + text.len()) as i64,
        depth_since_snapshot
    ])?;
    let op_id = conn.last_insert_rowid();
    materialized_text.push_str(text);
    if snapshot_every > 0 && depth_since_snapshot >= snapshot_every as i64 {
        snapshot_known_text(conn, Some(op_id), materialized_text)?;
    }
    Ok(Some(op_id))
}

pub(crate) fn replace_range_with_materialized(
    conn: &Connection,
    root: TextRoot,
    materialized_text: &mut String,
    start_byte: usize,
    delete_bytes: usize,
    insert: &str,
    snapshot_every: usize,
) -> Result<TextRoot> {
    let depth_since_snapshot = next_depth_since_snapshot(conn, root)?;
    replace_range_with_materialized_at_depth(
        conn,
        root,
        materialized_text,
        TextRangeEdit {
            start_byte,
            delete_bytes,
            insert,
        },
        snapshot_every,
        depth_since_snapshot,
    )
}

pub(crate) fn replace_range_with_materialized_at_depth(
    conn: &Connection,
    root: TextRoot,
    materialized_text: &mut String,
    range: TextRangeEdit<'_>,
    snapshot_every: usize,
    depth_since_snapshot: i64,
) -> Result<TextRoot> {
    let TextRangeEdit {
        start_byte,
        delete_bytes,
        insert,
    } = range;
    if start_byte > materialized_text.len() || start_byte + delete_bytes > materialized_text.len() {
        return Err(Error::new("text op replace range out of bounds"));
    }
    if !materialized_text.is_char_boundary(start_byte)
        || !materialized_text.is_char_boundary(start_byte + delete_bytes)
    {
        return Err(Error::new(
            "text op replace range must use UTF-8 boundaries",
        ));
    }
    conn.prepare_cached(
        "INSERT INTO jazz_text_op
           (parent_op_id, start_byte, delete_bytes, insert_text, resulting_len, depth_since_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?
    .execute(params![
        root,
        start_byte as i64,
        delete_bytes as i64,
        insert,
        (materialized_text.len() + insert.len() - delete_bytes) as i64,
        depth_since_snapshot
    ])?;
    let op_id = conn.last_insert_rowid();
    materialized_text.replace_range(start_byte..start_byte + delete_bytes, insert);
    if snapshot_every > 0 && depth_since_snapshot >= snapshot_every as i64 {
        snapshot_known_text(conn, Some(op_id), materialized_text)?;
    }
    Ok(Some(op_id))
}

pub(crate) struct TextRangeEdit<'a> {
    pub(crate) start_byte: usize,
    pub(crate) delete_bytes: usize,
    pub(crate) insert: &'a str,
}

pub(crate) fn current_depth_since_snapshot(conn: &Connection, root: TextRoot) -> Result<i64> {
    let Some(root_op_id) = root else {
        return Ok(0);
    };
    if snapshot_exists(conn, root_op_id)? {
        return Ok(0);
    }
    conn.prepare_cached("SELECT depth_since_snapshot FROM jazz_text_op WHERE op_id = ?")?
        .query_row(params![root_op_id], |row| row.get::<_, i64>(0))
        .optional()?
        .ok_or_else(|| Error::new("unknown text op root"))
}

pub fn replace_range(
    conn: &Connection,
    root: TextRoot,
    start_byte: usize,
    delete_bytes: usize,
    insert: &str,
    snapshot_every: usize,
) -> Result<TextRoot> {
    let total_len = root_len(conn, root)? as usize;
    if start_byte > total_len || start_byte + delete_bytes > total_len {
        return Err(Error::new("text op replace range out of bounds"));
    }
    if start_byte > 0 || delete_bytes > 0 {
        let current = materialize(conn, root)?;
        if !current.is_char_boundary(start_byte)
            || !current.is_char_boundary(start_byte + delete_bytes)
        {
            return Err(Error::new(
                "text op replace range must use UTF-8 boundaries",
            ));
        }
    }
    replace_range_known_len(
        conn,
        root,
        total_len,
        start_byte,
        delete_bytes,
        insert,
        snapshot_every,
    )
}

pub(crate) fn replace_range_known_len(
    conn: &Connection,
    root: TextRoot,
    total_len: usize,
    start_byte: usize,
    delete_bytes: usize,
    insert: &str,
    snapshot_every: usize,
) -> Result<TextRoot> {
    if start_byte > total_len || start_byte + delete_bytes > total_len {
        return Err(Error::new("text op replace range out of bounds"));
    }
    let depth_since_snapshot = next_depth_since_snapshot(conn, root)?;
    conn.prepare_cached(
        "INSERT INTO jazz_text_op
           (parent_op_id, start_byte, delete_bytes, insert_text, resulting_len, depth_since_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )?
    .execute(params![
        root,
        start_byte as i64,
        delete_bytes as i64,
        insert,
        (total_len + insert.len() - delete_bytes) as i64,
        depth_since_snapshot
    ])?;
    let op_id = conn.last_insert_rowid();
    if snapshot_every > 0 && depth_since_snapshot >= snapshot_every as i64 {
        snapshot(conn, Some(op_id))?;
    }
    Ok(Some(op_id))
}

pub fn materialize(conn: &Connection, root: TextRoot) -> Result<String> {
    let Some(root_op_id) = root else {
        return Ok(String::new());
    };
    let (snapshot, mut ops) = ancestor_ops_to_replay(conn, root_op_id)?;
    let mut text = if let Some(snapshot) = snapshot {
        snapshot_text(conn, &snapshot.chunk_hashes)?
    } else {
        String::new()
    };
    ops.reverse();
    for op in ops {
        let start = op.start_byte as usize;
        let delete_bytes = op.delete_bytes as usize;
        if start > text.len()
            || start + delete_bytes > text.len()
            || !text.is_char_boundary(start)
            || !text.is_char_boundary(start + delete_bytes)
        {
            return Err(Error::new(format!("invalid text op range {}", op.op_id)));
        }
        text.replace_range(start..start + delete_bytes, &op.insert_text);
    }
    Ok(text)
}

pub fn snapshot(conn: &Connection, root: TextRoot) -> Result<()> {
    let Some(op_id) = root else {
        return Ok(());
    };
    let text = materialize(conn, Some(op_id))?;
    snapshot_known_text(conn, Some(op_id), &text)
}

fn snapshot_known_text(conn: &Connection, root: TextRoot, text: &str) -> Result<()> {
    let Some(op_id) = root else {
        return Ok(());
    };
    if snapshot_exists(conn, op_id)? {
        return Ok(());
    }
    let chunk_hashes = store_chunks(conn, text)?;
    conn.prepare_cached(
        "INSERT OR IGNORE INTO jazz_text_snapshot (op_id, byte_len, chunk_hashes)
         VALUES (?, ?, ?)",
    )?
    .execute(params![op_id, text.len() as i64, chunk_hashes])?;
    Ok(())
}

pub fn root_len(conn: &Connection, root: TextRoot) -> Result<i64> {
    let Some(op_id) = root else {
        return Ok(0);
    };
    conn.prepare_cached("SELECT resulting_len FROM jazz_text_op WHERE op_id = ?")?
        .query_row(params![op_id], |row| row.get(0))
        .optional()?
        .ok_or_else(|| Error::new("unknown text op root"))
}

pub fn database_bytes(conn: &Connection) -> Result<i64> {
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    Ok(page_count * page_size)
}

pub fn bundle_bytes(conn: &Connection) -> Result<usize> {
    Ok(export_delta(conn, DeltaWatermark::default())?
        .stats
        .compressed_bytes)
}

pub fn export_delta(conn: &Connection, watermark: DeltaWatermark) -> Result<EncodedDelta> {
    let mut ops = Vec::new();
    let mut op_stmt = conn.prepare_cached(
        "SELECT op_id, parent_op_id, start_byte, delete_bytes, insert_text, resulting_len
         FROM jazz_text_op
         WHERE op_id > ?
         ORDER BY op_id",
    )?;
    let op_rows = op_stmt.query_map(params![watermark.op_id], |row| {
        Ok(TextOpRow {
            op_id: row.get(0)?,
            parent_op_id: row.get(1)?,
            start_byte: row.get(2)?,
            delete_bytes: row.get(3)?,
            insert_text: row.get(4)?,
            resulting_len: row.get(5)?,
        })
    })?;
    for row in op_rows {
        ops.push(row?);
    }

    let mut snapshots = Vec::new();
    let mut chunk_hashes = BTreeSet::new();
    let mut snapshot_stmt = conn.prepare_cached(
        "SELECT snapshot_id, op_id, byte_len, chunk_hashes
         FROM jazz_text_snapshot
         WHERE snapshot_id > ?
         ORDER BY snapshot_id",
    )?;
    let snapshot_rows = snapshot_stmt.query_map(params![watermark.snapshot_id], |row| {
        Ok(SnapshotRow {
            snapshot_id: row.get(0)?,
            op_id: row.get(1)?,
            byte_len: row.get(2)?,
            chunk_hashes: row.get(3)?,
        })
    })?;
    for row in snapshot_rows {
        let snapshot = row?;
        for hash in snapshot.chunk_hashes.chunks_exact(32) {
            chunk_hashes.insert(hash.to_vec());
        }
        snapshots.push(snapshot);
    }

    let mut chunks = Vec::new();
    let mut chunk_stmt =
        conn.prepare_cached("SELECT text FROM jazz_text_chunk WHERE chunk_hash = ?")?;
    for hash in chunk_hashes {
        let text: String = chunk_stmt.query_row(params![hash.as_slice()], |row| row.get(0))?;
        chunks.push(ChunkRow { hash, text });
    }

    let mut uncompressed = Vec::new();
    encode_delta_payload(&mut uncompressed, &ops, &snapshots, &chunks);
    let compressed = lz4_flex::compress_prepend_size(&uncompressed);
    Ok(EncodedDelta {
        bytes: compressed.clone(),
        stats: DeltaStats {
            ops: ops.len(),
            snapshots: snapshots.len(),
            chunks: chunks.len(),
            uncompressed_bytes: uncompressed.len(),
            compressed_bytes: compressed.len(),
        },
    })
}

pub fn export_delta_for_roots(
    conn: &Connection,
    roots: impl IntoIterator<Item = TextRoot>,
    watermark: DeltaWatermark,
) -> Result<EncodedDelta> {
    export_delta_for_roots_and_ranges(conn, roots, std::iter::empty(), watermark)
}

pub fn export_delta_for_roots_and_ranges(
    conn: &Connection,
    roots: impl IntoIterator<Item = TextRoot>,
    root_ranges: impl IntoIterator<Item = (i64, i64)>,
    watermark: DeltaWatermark,
) -> Result<EncodedDelta> {
    let mut op_ids = BTreeSet::new();
    let mut snapshot_op_ids = BTreeSet::new();
    let mut ancestor_stmt =
        conn.prepare_cached("SELECT parent_op_id FROM jazz_text_op WHERE op_id = ?")?;
    let mut snapshot_stmt =
        conn.prepare_cached("SELECT snapshot_id FROM jazz_text_snapshot WHERE op_id = ?")?;
    for root in roots {
        let mut current = root;
        while let Some(op_id) = current {
            if op_id <= watermark.op_id {
                break;
            }
            if !op_ids.insert(op_id) {
                break;
            }
            if let Some(snapshot_id) = snapshot_stmt
                .query_row(params![op_id], |row| row.get::<_, i64>(0))
                .optional()?
            {
                if snapshot_id > watermark.snapshot_id {
                    snapshot_op_ids.insert(op_id);
                }
                break;
            }
            current = ancestor_stmt
                .query_row(params![op_id], |row| row.get::<_, Option<i64>>(0))
                .optional()?
                .ok_or_else(|| Error::new("unknown text op root"))?;
        }
    }
    let mut range_op_stmt = conn.prepare_cached(
        "SELECT op_id
         FROM jazz_text_op
         WHERE op_id BETWEEN ? AND ?
         ORDER BY op_id",
    )?;
    for (min_root, max_root) in root_ranges {
        if min_root > max_root {
            return Err(Error::new("invalid text op root range"));
        }
        if min_root > watermark.op_id {
            let mut current = Some(min_root);
            while let Some(op_id) = current {
                if op_id <= watermark.op_id {
                    break;
                }
                let newly_inserted = op_ids.insert(op_id);
                if let Some(snapshot_id) = snapshot_stmt
                    .query_row(params![op_id], |row| row.get::<_, i64>(0))
                    .optional()?
                {
                    if snapshot_id > watermark.snapshot_id {
                        snapshot_op_ids.insert(op_id);
                    }
                    break;
                }
                if !newly_inserted {
                    break;
                }
                current = ancestor_stmt
                    .query_row(params![op_id], |row| row.get::<_, Option<i64>>(0))
                    .optional()?
                    .ok_or_else(|| Error::new("unknown text op root"))?;
            }
        }
        let first_needed = min_root.max(watermark.op_id + 1);
        if first_needed <= max_root {
            let rows = range_op_stmt
                .query_map(params![first_needed, max_root], |row| row.get::<_, i64>(0))?;
            for row in rows {
                let op_id = row?;
                op_ids.insert(op_id);
                if let Some(snapshot_id) = snapshot_stmt
                    .query_row(params![op_id], |row| row.get::<_, i64>(0))
                    .optional()?
                {
                    if snapshot_id > watermark.snapshot_id {
                        snapshot_op_ids.insert(op_id);
                    }
                }
            }
        }
    }
    encode_delta_for_op_ids(conn, &op_ids, &snapshot_op_ids)
}

fn encode_delta_for_op_ids(
    conn: &Connection,
    op_ids: &BTreeSet<i64>,
    snapshot_op_ids: &BTreeSet<i64>,
) -> Result<EncodedDelta> {
    let mut ops = Vec::with_capacity(op_ids.len());
    let mut op_stmt = conn.prepare_cached(
        "SELECT op_id, parent_op_id, start_byte, delete_bytes, insert_text, resulting_len
         FROM jazz_text_op
         WHERE op_id = ?",
    )?;
    for op_id in op_ids {
        ops.push(op_stmt.query_row(params![op_id], |row| {
            Ok(TextOpRow {
                op_id: row.get(0)?,
                parent_op_id: row.get(1)?,
                start_byte: row.get(2)?,
                delete_bytes: row.get(3)?,
                insert_text: row.get(4)?,
                resulting_len: row.get(5)?,
            })
        })?);
    }

    let mut snapshots = Vec::new();
    let mut chunk_hashes = BTreeSet::new();
    let mut snapshot_stmt = conn.prepare_cached(
        "SELECT snapshot_id, op_id, byte_len, chunk_hashes
         FROM jazz_text_snapshot
         WHERE op_id = ?",
    )?;
    for op_id in snapshot_op_ids {
        let snapshot = snapshot_stmt.query_row(params![op_id], |row| {
            Ok(SnapshotRow {
                snapshot_id: row.get(0)?,
                op_id: row.get(1)?,
                byte_len: row.get(2)?,
                chunk_hashes: row.get(3)?,
            })
        })?;
        for hash in snapshot.chunk_hashes.chunks_exact(32) {
            chunk_hashes.insert(hash.to_vec());
        }
        snapshots.push(snapshot);
    }

    let mut chunks = Vec::new();
    let mut chunk_stmt =
        conn.prepare_cached("SELECT text FROM jazz_text_chunk WHERE chunk_hash = ?")?;
    for hash in chunk_hashes {
        let text: String = chunk_stmt.query_row(params![hash.as_slice()], |row| row.get(0))?;
        chunks.push(ChunkRow { hash, text });
    }

    let mut uncompressed = Vec::new();
    encode_delta_payload(&mut uncompressed, &ops, &snapshots, &chunks);
    let compressed = lz4_flex::compress_prepend_size(&uncompressed);
    Ok(EncodedDelta {
        bytes: compressed.clone(),
        stats: DeltaStats {
            ops: ops.len(),
            snapshots: snapshots.len(),
            chunks: chunks.len(),
            uncompressed_bytes: uncompressed.len(),
            compressed_bytes: compressed.len(),
        },
    })
}

pub fn current_watermark(conn: &Connection) -> Result<DeltaWatermark> {
    let op_id = conn.query_row(
        "SELECT COALESCE(MAX(op_id), 0) FROM jazz_text_op",
        [],
        |row| row.get(0),
    )?;
    let snapshot_id = conn.query_row(
        "SELECT COALESCE(MAX(snapshot_id), 0) FROM jazz_text_snapshot",
        [],
        |row| row.get(0),
    )?;
    Ok(DeltaWatermark { op_id, snapshot_id })
}

pub fn apply_delta(
    conn: &Connection,
    encoded: &[u8],
    watermark: &mut DeltaWatermark,
) -> Result<DeltaStats> {
    let payload = lz4_flex::decompress_size_prepended(encoded)
        .map_err(|err| Error::new(format!("decode text op delta: {err}")))?;
    let (ops, snapshots, chunks) = decode_delta_payload(&payload)?;
    let op_depths = validate_delta_references(conn, &ops, &snapshots, &chunks)?;
    for chunk in ops.chunks(500) {
        let placeholders = (0..chunk.len())
            .map(|_| "(?, ?, ?, ?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO jazz_text_op
             (op_id, parent_op_id, start_byte, delete_bytes, insert_text, resulting_len, depth_since_snapshot)
             VALUES {placeholders}"
        );
        let mut values = Vec::with_capacity(chunk.len() * 7);
        for op in chunk {
            values.push(rusqlite::types::Value::Integer(op.op_id));
            values.push(
                op.parent_op_id
                    .map(rusqlite::types::Value::Integer)
                    .unwrap_or(rusqlite::types::Value::Null),
            );
            values.push(rusqlite::types::Value::Integer(op.start_byte));
            values.push(rusqlite::types::Value::Integer(op.delete_bytes));
            values.push(rusqlite::types::Value::Text(op.insert_text.clone()));
            values.push(rusqlite::types::Value::Integer(op.resulting_len));
            values.push(rusqlite::types::Value::Integer(
                op_depths.get(&op.op_id).copied().unwrap_or(1),
            ));
        }
        conn.prepare_cached(&sql)?
            .execute(params_from_iter(values.iter()))?;
    }
    for op in &ops {
        watermark.op_id = watermark.op_id.max(op.op_id);
    }

    for chunk_rows in chunks.chunks(500) {
        let placeholders = (0..chunk_rows.len())
            .map(|_| "(?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR IGNORE INTO jazz_text_chunk (chunk_hash, text) VALUES {placeholders}"
        );
        let mut values = Vec::with_capacity(chunk_rows.len() * 2);
        for chunk in chunk_rows {
            values.push(rusqlite::types::Value::Blob(chunk.hash.clone()));
            values.push(rusqlite::types::Value::Text(chunk.text.clone()));
        }
        conn.prepare_cached(&sql)?
            .execute(params_from_iter(values.iter()))?;
    }

    for chunk in snapshots.chunks(500) {
        let placeholders = (0..chunk.len())
            .map(|_| "(?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO jazz_text_snapshot
             (snapshot_id, op_id, byte_len, chunk_hashes)
             VALUES {placeholders}"
        );
        let mut values = Vec::with_capacity(chunk.len() * 4);
        for snapshot in chunk {
            values.push(rusqlite::types::Value::Integer(snapshot.snapshot_id));
            values.push(rusqlite::types::Value::Integer(snapshot.op_id));
            values.push(rusqlite::types::Value::Integer(snapshot.byte_len));
            values.push(rusqlite::types::Value::Blob(snapshot.chunk_hashes.clone()));
        }
        conn.prepare_cached(&sql)?
            .execute(params_from_iter(values.iter()))?;
    }
    for snapshot in &snapshots {
        watermark.snapshot_id = watermark.snapshot_id.max(snapshot.snapshot_id);
    }
    Ok(DeltaStats {
        ops: ops.len(),
        snapshots: snapshots.len(),
        chunks: chunks.len(),
        uncompressed_bytes: payload.len(),
        compressed_bytes: encoded.len(),
    })
}

fn snapshot_exists(conn: &Connection, op_id: i64) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM jazz_text_snapshot WHERE op_id = ? LIMIT 1")?
        .query_row(params![op_id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn next_depth_since_snapshot(conn: &Connection, parent: TextRoot) -> Result<i64> {
    let Some(parent_op_id) = parent else {
        return Ok(1);
    };
    if snapshot_exists(conn, parent_op_id)? {
        return Ok(1);
    }
    conn.prepare_cached("SELECT depth_since_snapshot FROM jazz_text_op WHERE op_id = ?")?
        .query_row(params![parent_op_id], |row| row.get::<_, i64>(0))
        .optional()?
        .map(|depth| depth + 1)
        .ok_or_else(|| Error::new("unknown text op root"))
}

fn validate_delta_references(
    conn: &Connection,
    ops: &[TextOpRow],
    snapshots: &[SnapshotRow],
    chunks: &[ChunkRow],
) -> Result<BTreeMap<i64, i64>> {
    let incoming_ops = ops.iter().map(|op| op.op_id).collect::<BTreeSet<_>>();
    let incoming_snapshot_ops = snapshots
        .iter()
        .map(|snapshot| snapshot.op_id)
        .collect::<BTreeSet<_>>();
    let mut op_exists_stmt = conn.prepare_cached("SELECT 1 FROM jazz_text_op WHERE op_id = ?")?;
    let mut root_len_stmt =
        conn.prepare_cached("SELECT resulting_len FROM jazz_text_op WHERE op_id = ?")?;
    let mut root_depth_stmt =
        conn.prepare_cached("SELECT depth_since_snapshot FROM jazz_text_op WHERE op_id = ?")?;
    let mut incoming_lengths = BTreeMap::new();
    let mut incoming_depths = BTreeMap::new();
    for op in ops {
        let (parent_len, depth_since_snapshot) = if let Some(parent_op_id) = op.parent_op_id {
            if parent_op_id >= op.op_id {
                return Err(Error::new(format!(
                    "text op {} parent {} is not older",
                    op.op_id, parent_op_id
                )));
            }
            if let Some(parent_len) = incoming_lengths.get(&parent_op_id).copied() {
                let parent_depth = incoming_depths.get(&parent_op_id).copied().unwrap_or(1);
                let depth = if incoming_snapshot_ops.contains(&parent_op_id) {
                    1
                } else {
                    parent_depth + 1
                };
                (parent_len, depth)
            } else if incoming_ops.contains(&parent_op_id) {
                return Err(Error::new(format!(
                    "text op parent {parent_op_id} for op {} appears after child",
                    op.op_id
                )));
            } else {
                let parent_len = root_len_stmt
                    .query_row(params![parent_op_id], |row| row.get::<_, i64>(0))
                    .optional()?
                    .ok_or_else(|| {
                        Error::new(format!(
                            "missing text op parent {parent_op_id} for op {}",
                            op.op_id
                        ))
                    })?;
                let parent_depth = root_depth_stmt
                    .query_row(params![parent_op_id], |row| row.get::<_, i64>(0))
                    .optional()?
                    .unwrap_or(1);
                let depth = if snapshot_exists(conn, parent_op_id)? {
                    1
                } else {
                    parent_depth + 1
                };
                (parent_len, depth)
            }
        } else {
            (0, 1)
        };
        if op.start_byte < 0 || op.delete_bytes < 0 {
            return Err(Error::new(format!(
                "text op {} has negative range",
                op.op_id
            )));
        }
        if op.start_byte > parent_len || op.start_byte + op.delete_bytes > parent_len {
            return Err(Error::new(format!(
                "text op {} range out of bounds",
                op.op_id
            )));
        }
        let expected_len = parent_len + op.insert_text.len() as i64 - op.delete_bytes;
        if op.resulting_len != expected_len {
            return Err(Error::new(format!(
                "text op {} resulting length mismatch",
                op.op_id
            )));
        }
        incoming_lengths.insert(op.op_id, op.resulting_len);
        incoming_depths.insert(op.op_id, depth_since_snapshot);
    }

    let mut incoming_chunks = BTreeSet::new();
    for chunk in chunks {
        let expected_hash = Sha256::digest(chunk.text.as_bytes()).to_vec();
        if chunk.hash != expected_hash {
            return Err(Error::new("text chunk hash does not match chunk text"));
        }
        incoming_chunks.insert(chunk.hash.as_slice());
    }
    let mut chunk_exists_stmt =
        conn.prepare_cached("SELECT 1 FROM jazz_text_chunk WHERE chunk_hash = ?")?;
    for snapshot in snapshots {
        if incoming_ops.contains(&snapshot.op_id) {
        } else if op_exists_stmt
            .query_row(params![snapshot.op_id], |_| Ok(()))
            .optional()?
            .is_none()
        {
            return Err(Error::new(format!(
                "missing text op {} for snapshot {}",
                snapshot.op_id, snapshot.snapshot_id
            )));
        }
        if !snapshot.chunk_hashes.len().is_multiple_of(32) {
            return Err(Error::new(format!(
                "snapshot {} has malformed chunk hash list",
                snapshot.snapshot_id
            )));
        }
        for hash in snapshot.chunk_hashes.chunks_exact(32) {
            if incoming_chunks.contains(hash) {
                continue;
            }
            let chunk_exists = chunk_exists_stmt
                .query_row(params![hash], |_| Ok(()))
                .optional()?
                .is_some();
            if !chunk_exists {
                return Err(Error::new(format!(
                    "missing text chunk for snapshot {}",
                    snapshot.snapshot_id
                )));
            }
        }
    }
    Ok(incoming_depths)
}

fn ancestor_ops_to_replay(
    conn: &Connection,
    root_op_id: i64,
) -> Result<(Option<Snapshot>, Vec<TextOpRow>)> {
    let mut ops = Vec::new();
    let mut snapshot = None;
    let mut stmt = conn.prepare_cached(
        r#"
        WITH RECURSIVE ancestors(
          depth,
          op_id,
          parent_op_id,
          start_byte,
          delete_bytes,
          insert_text,
          resulting_len,
          chunk_hashes
        ) AS (
          SELECT
            0,
            op.op_id,
            op.parent_op_id,
            op.start_byte,
            op.delete_bytes,
            op.insert_text,
            op.resulting_len,
            snapshot.chunk_hashes
          FROM jazz_text_op op
          LEFT JOIN jazz_text_snapshot snapshot ON snapshot.op_id = op.op_id
          WHERE op.op_id = ?

          UNION ALL

          SELECT
            ancestors.depth + 1,
            op.op_id,
            op.parent_op_id,
            op.start_byte,
            op.delete_bytes,
            op.insert_text,
            op.resulting_len,
            snapshot.chunk_hashes
          FROM ancestors
          JOIN jazz_text_op op ON op.op_id = ancestors.parent_op_id
          LEFT JOIN jazz_text_snapshot snapshot ON snapshot.op_id = op.op_id
          WHERE ancestors.chunk_hashes IS NULL
        )
        SELECT op_id, parent_op_id, start_byte, delete_bytes, insert_text, resulting_len, chunk_hashes
        FROM ancestors
        ORDER BY depth
        "#,
    )?;
    let mut rows = stmt.query(params![root_op_id])?;
    while let Some(row) = rows.next()? {
        let chunk_hashes: Option<Vec<u8>> = row.get(6)?;
        if let Some(chunk_hashes) = chunk_hashes {
            snapshot = Some(Snapshot { chunk_hashes });
            break;
        }
        ops.push(TextOpRow {
            op_id: row.get(0)?,
            parent_op_id: row.get(1)?,
            start_byte: row.get(2)?,
            delete_bytes: row.get(3)?,
            insert_text: row.get(4)?,
            resulting_len: row.get(5)?,
        });
    }
    if ops.is_empty() && snapshot.is_none() {
        return Err(Error::new("unknown text op root"));
    }
    Ok((snapshot, ops))
}

fn store_chunks(conn: &Connection, text: &str) -> Result<Vec<u8>> {
    let mut hashes = Vec::new();
    let mut insert = conn
        .prepare_cached("INSERT OR IGNORE INTO jazz_text_chunk (chunk_hash, text) VALUES (?, ?)")?;
    for chunk in chunk_text(text, CHUNK_BYTES) {
        let hash = Sha256::digest(chunk.as_bytes());
        insert.execute(params![hash.as_slice(), chunk])?;
        hashes.extend_from_slice(hash.as_slice());
    }
    Ok(hashes)
}

fn snapshot_text(conn: &Connection, chunk_hashes: &[u8]) -> Result<String> {
    if !chunk_hashes.len().is_multiple_of(32) {
        return Err(Error::new("text snapshot chunk hash list is truncated"));
    }
    let mut text = String::new();
    let mut stmt = conn.prepare_cached("SELECT text FROM jazz_text_chunk WHERE chunk_hash = ?")?;
    for hash in chunk_hashes.chunks_exact(32) {
        let chunk: String = stmt.query_row(params![hash], |row| row.get(0))?;
        text.push_str(&chunk);
    }
    Ok(text)
}

fn chunk_text(text: &str, max_bytes: usize) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let mut end = (start + max_bytes).min(text.len());
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(&text[start..end]);
        start = end;
    }
    chunks
}

struct Snapshot {
    chunk_hashes: Vec<u8>,
}

struct TextOpRow {
    op_id: i64,
    parent_op_id: Option<i64>,
    start_byte: i64,
    delete_bytes: i64,
    insert_text: String,
    resulting_len: i64,
}

struct SnapshotRow {
    snapshot_id: i64,
    op_id: i64,
    byte_len: i64,
    chunk_hashes: Vec<u8>,
}

struct ChunkRow {
    hash: Vec<u8>,
    text: String,
}

fn encode_delta_payload(
    out: &mut Vec<u8>,
    ops: &[TextOpRow],
    snapshots: &[SnapshotRow],
    chunks: &[ChunkRow],
) {
    out.extend_from_slice(DELTA_MAGIC);
    write_varint(out, ops.len() as u64);
    let mut previous_op_id = 0i64;
    let mut previous_parent_op_id = 0i64;
    let mut previous_start = 0i64;
    let mut previous_resulting_len = 0i64;
    for op in ops {
        write_signed_delta(out, op.op_id - previous_op_id);
        write_signed_delta(out, op.parent_op_id.unwrap_or(0) - previous_parent_op_id);
        write_signed_delta(out, op.start_byte - previous_start);
        write_varint(out, op.delete_bytes as u64);
        write_bytes(out, op.insert_text.as_bytes());
        write_signed_delta(out, op.resulting_len - previous_resulting_len);
        previous_op_id = op.op_id;
        previous_parent_op_id = op.parent_op_id.unwrap_or(0);
        previous_start = op.start_byte;
        previous_resulting_len = op.resulting_len;
    }

    write_varint(out, snapshots.len() as u64);
    let mut previous_snapshot_id = 0i64;
    let mut previous_snapshot_op_id = 0i64;
    for snapshot in snapshots {
        write_signed_delta(out, snapshot.snapshot_id - previous_snapshot_id);
        write_signed_delta(out, snapshot.op_id - previous_snapshot_op_id);
        write_varint(out, snapshot.byte_len as u64);
        write_bytes(out, &snapshot.chunk_hashes);
        previous_snapshot_id = snapshot.snapshot_id;
        previous_snapshot_op_id = snapshot.op_id;
    }

    write_varint(out, chunks.len() as u64);
    for chunk in chunks {
        write_bytes(out, &chunk.hash);
        write_bytes(out, chunk.text.as_bytes());
    }
}

fn decode_delta_payload(
    payload: &[u8],
) -> Result<(Vec<TextOpRow>, Vec<SnapshotRow>, Vec<ChunkRow>)> {
    let mut cursor = ByteCursor::new(payload);
    cursor.expect_bytes(DELTA_MAGIC)?;
    let op_count = cursor.read_varint()? as usize;
    let mut ops = Vec::with_capacity(op_count);
    let mut previous_op_id = 0i64;
    let mut previous_parent_op_id = 0i64;
    let mut previous_start = 0i64;
    let mut previous_resulting_len = 0i64;
    for _ in 0..op_count {
        let op_id = previous_op_id + cursor.read_signed_delta()?;
        let parent = previous_parent_op_id + cursor.read_signed_delta()?;
        let start_byte = previous_start + cursor.read_signed_delta()?;
        let delete_bytes = cursor.read_varint()? as i64;
        let insert_text = String::from_utf8(cursor.read_bytes()?.to_vec())
            .map_err(|err| Error::new(format!("text op delta has invalid utf8: {err}")))?;
        let resulting_len = previous_resulting_len + cursor.read_signed_delta()?;
        ops.push(TextOpRow {
            op_id,
            parent_op_id: if parent == 0 { None } else { Some(parent) },
            start_byte,
            delete_bytes,
            insert_text,
            resulting_len,
        });
        previous_op_id = op_id;
        previous_parent_op_id = parent;
        previous_start = start_byte;
        previous_resulting_len = resulting_len;
    }

    let snapshot_count = cursor.read_varint()? as usize;
    let mut snapshots = Vec::with_capacity(snapshot_count);
    let mut previous_snapshot_id = 0i64;
    let mut previous_snapshot_op_id = 0i64;
    for _ in 0..snapshot_count {
        let snapshot_id = previous_snapshot_id + cursor.read_signed_delta()?;
        let op_id = previous_snapshot_op_id + cursor.read_signed_delta()?;
        let byte_len = cursor.read_varint()? as i64;
        let chunk_hashes = cursor.read_bytes()?.to_vec();
        snapshots.push(SnapshotRow {
            snapshot_id,
            op_id,
            byte_len,
            chunk_hashes,
        });
        previous_snapshot_id = snapshot_id;
        previous_snapshot_op_id = op_id;
    }

    let chunk_count = cursor.read_varint()? as usize;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        let hash = cursor.read_bytes()?.to_vec();
        if hash.len() != 32 {
            return Err(Error::new("text op delta chunk hash has invalid length"));
        }
        let text = String::from_utf8(cursor.read_bytes()?.to_vec())
            .map_err(|err| Error::new(format!("text op delta chunk has invalid utf8: {err}")))?;
        chunks.push(ChunkRow { hash, text });
    }
    cursor.expect_end()?;
    Ok((ops, snapshots, chunks))
}

fn write_signed_delta(out: &mut Vec<u8>, value: i64) {
    write_varint(out, ((value << 1) ^ (value >> 63)) as u64);
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<()> {
        let end = self
            .index
            .checked_add(expected.len())
            .ok_or_else(|| Error::new("text op delta byte length overflowed"))?;
        if self.bytes.get(self.index..end) != Some(expected) {
            return Err(Error::new("text op delta has invalid magic"));
        }
        self.index = end;
        Ok(())
    }

    fn read_signed_delta(&mut self) -> Result<i64> {
        let value = self.read_varint()?;
        Ok(((value >> 1) as i64) ^ (-((value & 1) as i64)))
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self
                .bytes
                .get(self.index)
                .ok_or_else(|| Error::new("text op delta ended inside varint"))?;
            self.index += 1;
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Error::new("text op delta varint is too long"))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_varint()? as usize;
        let end = self
            .index
            .checked_add(len)
            .ok_or_else(|| Error::new("text op delta byte length overflowed"))?;
        let bytes = self
            .bytes
            .get(self.index..end)
            .ok_or_else(|| Error::new("text op delta ended inside byte field"))?;
        self.index = end;
        Ok(bytes)
    }

    fn expect_end(&self) -> Result<()> {
        if self.index == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::new("text op delta has trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_text_store() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        install(&conn).expect("install text op schema");
        conn
    }

    #[test]
    fn install_adds_depth_column_to_existing_text_op_table() {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch(
            r#"
            CREATE TABLE jazz_text_op (
              op_id INTEGER PRIMARY KEY,
              parent_op_id INTEGER,
              start_byte INTEGER NOT NULL,
              delete_bytes INTEGER NOT NULL,
              insert_text TEXT NOT NULL,
              resulting_len INTEGER NOT NULL
            );
            "#,
        )
        .unwrap();

        install(&conn).unwrap();

        let depth: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM pragma_table_info('jazz_text_op')
                 WHERE name = 'depth_since_snapshot'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(depth, 1);
    }

    #[test]
    fn append_ops_materialize_current_text() {
        let conn = open_text_store();

        let mut root = None;
        root = append(&conn, root, "Hello", 0).expect("append first run");
        root = append(&conn, root, ", Ada", 0).expect("append second run");

        assert_eq!(materialize(&conn, root).expect("materialize"), "Hello, Ada");
        assert_eq!(root_len(&conn, root).expect("root len"), 10);
    }

    #[test]
    fn independent_roots_do_not_replay_each_others_ops() {
        let conn = open_text_store();

        let mut ada = None;
        let mut grace = None;
        ada = append(&conn, ada, "Ada", 2).expect("append ada");
        grace = append(&conn, grace, "Grace", 2).expect("append grace");
        ada = append(&conn, ada, " Lovelace", 2).expect("append ada surname");
        grace = append(&conn, grace, " Hopper", 2).expect("append grace surname");

        assert_eq!(
            materialize(&conn, ada).expect("materialize ada"),
            "Ada Lovelace"
        );
        assert_eq!(
            materialize(&conn, grace).expect("materialize grace"),
            "Grace Hopper"
        );
    }

    #[test]
    fn snapshots_follow_per_root_depth_not_global_op_id() {
        let conn = open_text_store();

        let mut ada = None;
        let mut grace = None;
        ada = append(&conn, ada, "Ada", 2).expect("append ada");
        grace = append(&conn, grace, "Grace", 2).expect("append grace");
        ada = append(&conn, ada, " Lovelace", 2).expect("append ada surname");

        assert_eq!(
            materialize(&conn, ada).expect("materialize ada"),
            "Ada Lovelace"
        );
        assert_eq!(
            materialize(&conn, grace).expect("materialize grace"),
            "Grace"
        );
        let snapshot_op_ids = conn
            .prepare("SELECT op_id FROM jazz_text_snapshot ORDER BY op_id")
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(snapshot_op_ids, vec![3]);
    }

    #[test]
    fn replace_ops_replay_from_nearest_snapshot() {
        let conn = open_text_store();

        let mut root = None;
        root = append(&conn, root, "Hello brave world", 2).expect("append base");
        root = replace_range(&conn, root, 6, 5, "small", 2).expect("edit middle");
        root = append(&conn, root, "!", 2).expect("append ending");

        assert_eq!(
            materialize(&conn, root).expect("materialize"),
            "Hello small world!"
        );
        let snapshot_count: i64 = conn
            .query_row("SELECT count(*) FROM jazz_text_snapshot", [], |row| {
                row.get(0)
            })
            .expect("count snapshots");
        assert_eq!(snapshot_count, 1);
    }

    #[test]
    fn snapshots_reuse_content_addressed_chunks() {
        let conn = open_text_store();

        let repeated = "a".repeat(CHUNK_BYTES);
        let mut root = append(&conn, None, &repeated, 0).expect("append first chunk");
        snapshot(&conn, root).expect("snapshot first chunk");
        root = append(&conn, root, &repeated, 0).expect("append same chunk again");
        snapshot(&conn, root).expect("snapshot repeated chunks");

        let chunk_count: i64 = conn
            .query_row("SELECT count(*) FROM jazz_text_chunk", [], |row| row.get(0))
            .expect("count chunks");
        assert_eq!(chunk_count, 1);
        assert_eq!(
            materialize(&conn, root).expect("materialize"),
            repeated.repeat(2)
        );
    }

    #[test]
    fn binary_delta_round_trips_incrementally() {
        let source = open_text_store();
        let target = open_text_store();

        let mut root = None;
        root = append(&source, root, "hello", 2).expect("append first");
        root = append(&source, root, " world", 2).expect("append second");
        root = replace_range(&source, root, 6, 5, "Ada", 2).expect("replace middle");
        snapshot(&source, root).expect("snapshot current");

        let mut watermark = DeltaWatermark::default();
        let first_delta = export_delta(&source, watermark).expect("export first delta");
        let first_stats =
            apply_delta(&target, &first_delta.bytes, &mut watermark).expect("apply first delta");
        assert_eq!(first_stats.ops, 3);
        assert_eq!(
            materialize(&target, root).expect("materialize target"),
            "hello Ada"
        );
        assert_eq!(watermark.op_id, 3);

        root = append(&source, root, "!", 2).expect("append final");
        let second_delta = export_delta(&source, watermark).expect("export second delta");
        let second_stats =
            apply_delta(&target, &second_delta.bytes, &mut watermark).expect("apply second delta");
        assert_eq!(second_stats.ops, 1);
        assert_eq!(
            materialize(&target, root).expect("materialize target"),
            "hello Ada!"
        );
        assert_eq!(watermark.op_id, 4);
    }

    #[test]
    fn binary_delta_can_export_root_ranges() {
        let source = open_text_store();
        let target = open_text_store();

        let mut root = None;
        root = append(&source, root, "a", 0).expect("append 1");
        root = append(&source, root, "b", 0).expect("append 2");
        root = append(&source, root, "c", 0).expect("append 3");
        root = append(&source, root, "d", 0).expect("append 4");

        let delta = export_delta_for_roots_and_ranges(
            &source,
            std::iter::empty(),
            [(2, 4)],
            DeltaWatermark::default(),
        )
        .expect("export range");
        let stats =
            apply_delta(&target, &delta.bytes, &mut DeltaWatermark::default()).expect("apply");

        assert_eq!(stats.ops, 4);
        assert_eq!(materialize(&target, root).expect("materialize"), "abcd");
    }

    #[test]
    fn binary_delta_rejects_snapshot_with_missing_chunk() {
        let source = open_text_store();
        let target = open_text_store();

        let root = append(&source, None, "hello snapshot", 0).expect("append");
        snapshot(&source, root).expect("snapshot");
        let delta = export_delta(&source, DeltaWatermark::default()).expect("export");
        let payload = lz4_flex::decompress_size_prepended(&delta.bytes).expect("decompress");
        let (ops, snapshots, _) = decode_delta_payload(&payload).expect("decode");
        let mut corrupted = Vec::new();
        encode_delta_payload(&mut corrupted, &ops, &snapshots, &[]);
        let corrupted = lz4_flex::compress_prepend_size(&corrupted);

        let err = apply_delta(&target, &corrupted, &mut DeltaWatermark::default()).unwrap_err();

        assert!(err.to_string().contains("missing text chunk"));
    }

    #[test]
    fn binary_delta_rejects_chunk_hash_mismatch() {
        let source = open_text_store();
        let target = open_text_store();

        let root = append(&source, None, "hello snapshot", 0).expect("append");
        snapshot(&source, root).expect("snapshot");
        let delta = export_delta(&source, DeltaWatermark::default()).expect("export");
        let payload = lz4_flex::decompress_size_prepended(&delta.bytes).expect("decompress");
        let (ops, snapshots, mut chunks) = decode_delta_payload(&payload).expect("decode");
        chunks[0].text.push_str(" corrupted");
        let mut corrupted = Vec::new();
        encode_delta_payload(&mut corrupted, &ops, &snapshots, &chunks);
        let corrupted = lz4_flex::compress_prepend_size(&corrupted);

        let err = apply_delta(&target, &corrupted, &mut DeltaWatermark::default()).unwrap_err();

        assert!(err.to_string().contains("chunk hash"));
    }

    #[test]
    fn binary_delta_rejects_invalid_op_range() {
        let target = open_text_store();
        let ops = vec![TextOpRow {
            op_id: 1,
            parent_op_id: None,
            start_byte: 7,
            delete_bytes: 0,
            insert_text: "oops".to_owned(),
            resulting_len: 4,
        }];
        let mut payload = Vec::new();
        encode_delta_payload(&mut payload, &ops, &[], &[]);
        let encoded = lz4_flex::compress_prepend_size(&payload);

        let err = apply_delta(&target, &encoded, &mut DeltaWatermark::default()).unwrap_err();

        assert!(err.to_string().contains("range out of bounds"));
    }
}
