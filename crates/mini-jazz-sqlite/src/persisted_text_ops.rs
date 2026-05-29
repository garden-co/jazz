use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

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
          start_byte INTEGER NOT NULL,
          delete_bytes INTEGER NOT NULL,
          insert_text TEXT NOT NULL,
          resulting_len INTEGER NOT NULL
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
    conn.prepare_cached(
        "INSERT INTO jazz_text_op (start_byte, delete_bytes, insert_text, resulting_len)
         VALUES (?, ?, ?, ?)",
    )?
    .execute(params![
        start_byte as i64,
        delete_bytes as i64,
        insert,
        (total_len + insert.len() - delete_bytes) as i64
    ])?;
    let op_id = conn.last_insert_rowid();
    if snapshot_every > 0 && (op_id as usize).is_multiple_of(snapshot_every) {
        snapshot(conn, Some(op_id))?;
    }
    Ok(Some(op_id))
}

pub fn materialize(conn: &Connection, root: TextRoot) -> Result<String> {
    let Some(root_op_id) = root else {
        return Ok(String::new());
    };
    let snapshot = nearest_snapshot(conn, root_op_id)?;
    let (mut text, after_op) = if let Some(snapshot) = snapshot {
        (snapshot_text(conn, &snapshot.chunk_hashes)?, snapshot.op_id)
    } else {
        (String::new(), 0)
    };
    let mut stmt = conn.prepare_cached(
        "SELECT start_byte, delete_bytes, insert_text
         FROM jazz_text_op
         WHERE op_id > ? AND op_id <= ?
         ORDER BY op_id",
    )?;
    let rows = stmt.query_map(params![after_op, root_op_id], |row| {
        Ok((
            row.get::<_, i64>(0)? as usize,
            row.get::<_, i64>(1)? as usize,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (start, delete_bytes, insert) = row?;
        text.replace_range(start..start + delete_bytes, &insert);
    }
    Ok(text)
}

pub fn snapshot(conn: &Connection, root: TextRoot) -> Result<()> {
    let Some(op_id) = root else {
        return Ok(());
    };
    if snapshot_exists(conn, op_id)? {
        return Ok(());
    }
    let text = materialize(conn, Some(op_id))?;
    let chunk_hashes = store_chunks(conn, &text)?;
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

pub fn inline_op_bytes(conn: &Connection, root: TextRoot) -> Result<Vec<u8>> {
    let Some(op_id) = root else {
        return Ok(Vec::new());
    };
    let op = conn
        .prepare_cached(
            "SELECT op_id, start_byte, delete_bytes, insert_text, resulting_len
             FROM jazz_text_op
             WHERE op_id = ?",
        )?
        .query_row(params![op_id], |row| {
            Ok(TextOpRow {
                op_id: row.get(0)?,
                start_byte: row.get(1)?,
                delete_bytes: row.get(2)?,
                insert_text: row.get(3)?,
                resulting_len: row.get(4)?,
            })
        })?;
    let mut out = Vec::new();
    write_varint(&mut out, op.start_byte as u64);
    write_varint(&mut out, op.delete_bytes as u64);
    write_bytes(&mut out, op.insert_text.as_bytes());
    write_varint(&mut out, op.resulting_len as u64);
    Ok(out)
}

pub fn apply_inline_op(conn: &Connection, bytes: &[u8], snapshot_every: usize) -> Result<TextRoot> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut cursor = ByteCursor::new(bytes);
    let start_byte = cursor.read_varint()? as i64;
    let delete_bytes = cursor.read_varint()? as i64;
    let insert_text = String::from_utf8(cursor.read_bytes()?.to_vec())
        .map_err(|err| Error::new(err.to_string()))?;
    let resulting_len = cursor.read_varint()? as i64;
    cursor.expect_end()?;
    conn.prepare_cached(
        "INSERT INTO jazz_text_op (start_byte, delete_bytes, insert_text, resulting_len)
         VALUES (?, ?, ?, ?)",
    )?
    .execute(params![
        start_byte,
        delete_bytes,
        insert_text,
        resulting_len
    ])?;
    let op_id = conn.last_insert_rowid();
    if snapshot_every > 0 && (op_id as usize).is_multiple_of(snapshot_every) {
        snapshot(conn, Some(op_id))?;
    }
    Ok(Some(op_id))
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
        "SELECT op_id, start_byte, delete_bytes, insert_text, resulting_len
         FROM jazz_text_op
         WHERE op_id > ?
         ORDER BY op_id",
    )?;
    let op_rows = op_stmt.query_map(params![watermark.op_id], |row| {
        Ok(TextOpRow {
            op_id: row.get(0)?,
            start_byte: row.get(1)?,
            delete_bytes: row.get(2)?,
            insert_text: row.get(3)?,
            resulting_len: row.get(4)?,
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

pub fn apply_delta(
    conn: &Connection,
    encoded: &[u8],
    watermark: &mut DeltaWatermark,
) -> Result<DeltaStats> {
    let payload = lz4_flex::decompress_size_prepended(encoded)
        .map_err(|err| Error::new(format!("decode text op delta: {err}")))?;
    let (ops, snapshots, chunks) = decode_delta_payload(&payload)?;
    let mut insert_op = conn.prepare_cached(
        "INSERT OR REPLACE INTO jazz_text_op
         (op_id, start_byte, delete_bytes, insert_text, resulting_len)
         VALUES (?, ?, ?, ?, ?)",
    )?;
    for op in &ops {
        insert_op.execute(params![
            op.op_id,
            op.start_byte,
            op.delete_bytes,
            op.insert_text,
            op.resulting_len
        ])?;
        watermark.op_id = watermark.op_id.max(op.op_id);
    }
    drop(insert_op);

    let mut insert_chunk = conn
        .prepare_cached("INSERT OR IGNORE INTO jazz_text_chunk (chunk_hash, text) VALUES (?, ?)")?;
    for chunk in &chunks {
        insert_chunk.execute(params![&chunk.hash, &chunk.text])?;
    }
    drop(insert_chunk);

    let mut insert_snapshot = conn.prepare_cached(
        "INSERT OR REPLACE INTO jazz_text_snapshot
         (snapshot_id, op_id, byte_len, chunk_hashes)
         VALUES (?, ?, ?, ?)",
    )?;
    for snapshot in &snapshots {
        insert_snapshot.execute(params![
            snapshot.snapshot_id,
            snapshot.op_id,
            snapshot.byte_len,
            &snapshot.chunk_hashes
        ])?;
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

fn nearest_snapshot(conn: &Connection, op_id: i64) -> Result<Option<Snapshot>> {
    conn.prepare_cached(
        "SELECT op_id, chunk_hashes
         FROM jazz_text_snapshot
         WHERE op_id <= ?
         ORDER BY op_id DESC
         LIMIT 1",
    )?
    .query_row(params![op_id], |row| {
        Ok(Snapshot {
            op_id: row.get(0)?,
            chunk_hashes: row.get(1)?,
        })
    })
    .optional()
    .map_err(Into::into)
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
    op_id: i64,
    chunk_hashes: Vec<u8>,
}

struct TextOpRow {
    op_id: i64,
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
    let mut previous_start = 0i64;
    let mut previous_resulting_len = 0i64;
    for op in ops {
        write_signed_delta(out, op.op_id - previous_op_id);
        write_signed_delta(out, op.start_byte - previous_start);
        write_varint(out, op.delete_bytes as u64);
        write_bytes(out, op.insert_text.as_bytes());
        write_signed_delta(out, op.resulting_len - previous_resulting_len);
        previous_op_id = op.op_id;
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
    let mut previous_start = 0i64;
    let mut previous_resulting_len = 0i64;
    for _ in 0..op_count {
        let op_id = previous_op_id + cursor.read_signed_delta()?;
        let start_byte = previous_start + cursor.read_signed_delta()?;
        let delete_bytes = cursor.read_varint()? as i64;
        let insert_text = String::from_utf8(cursor.read_bytes()?.to_vec())
            .map_err(|err| Error::new(format!("text op delta has invalid utf8: {err}")))?;
        let resulting_len = previous_resulting_len + cursor.read_signed_delta()?;
        ops.push(TextOpRow {
            op_id,
            start_byte,
            delete_bytes,
            insert_text,
            resulting_len,
        });
        previous_op_id = op_id;
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
    fn append_ops_materialize_current_text() {
        let conn = open_text_store();

        let mut root = None;
        root = append(&conn, root, "Hello", 0).expect("append first run");
        root = append(&conn, root, ", Ada", 0).expect("append second run");

        assert_eq!(materialize(&conn, root).expect("materialize"), "Hello, Ada");
        assert_eq!(root_len(&conn, root).expect("root len"), 10);
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
}
