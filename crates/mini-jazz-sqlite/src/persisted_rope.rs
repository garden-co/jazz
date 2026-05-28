use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{btree_map::Entry, BTreeMap};

const KIND_LEAF: i64 = 1;
const KIND_CONCAT: i64 = 2;
const KIND_POSITION_LEAF: i64 = 3;
const MAX_LEAF_BYTES: usize = 4096;
const POSITION_SCALE: f64 = 1000.0;

pub type RopeRoot = Option<i64>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RopeStats {
    pub nodes: i64,
    pub leaf_nodes: i64,
    pub position_leaf_nodes: i64,
    pub concat_nodes: i64,
    pub leaf_bytes: i64,
    pub segment_bytes: i64,
    pub position_segment_bytes: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

pub fn install(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jazz_rope_node (
          node_id INTEGER PRIMARY KEY,
          kind INTEGER NOT NULL,
          byte_len INTEGER NOT NULL,
          left_node_id INTEGER,
          right_node_id INTEGER,
          segment_id INTEGER,
          segment_start INTEGER
        );

        CREATE TABLE IF NOT EXISTS jazz_rope_segment (
          segment_id INTEGER PRIMARY KEY,
          text TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_rope_position_segment (
          segment_id INTEGER PRIMARY KEY,
          base_x INTEGER NOT NULL,
          base_y INTEGER NOT NULL,
          last_x INTEGER NOT NULL,
          last_y INTEGER NOT NULL,
          sample_count INTEGER NOT NULL,
          deltas BLOB NOT NULL
        );
        "#,
    )?;
    Ok(())
}

pub fn append(conn: &Connection, root: RopeRoot, text: &str) -> Result<RopeRoot> {
    if text.is_empty() {
        return Ok(root);
    }
    concat(conn, root, Some(insert_leaf(conn, text)?))
}

pub fn replace_range(
    conn: &Connection,
    root: RopeRoot,
    start_byte: usize,
    delete_bytes: usize,
    insert: &str,
) -> Result<RopeRoot> {
    let total_len = root_len(conn, root)? as usize;
    if start_byte > total_len || start_byte + delete_bytes > total_len {
        return Err(Error::new("rope replace range out of bounds"));
    }
    let (left, rest) = split(conn, root, start_byte)?;
    let (_, right) = split(conn, rest, delete_bytes)?;
    let with_insert = concat(conn, left, build_from_text(conn, insert)?)?;
    concat(conn, with_insert, right)
}

pub fn materialize(conn: &Connection, root: RopeRoot) -> Result<String> {
    let mut out = String::new();
    let mut segments = BTreeMap::new();
    materialize_into(conn, root, &mut out, &mut segments)?;
    Ok(out)
}

pub fn compact_text_root(conn: &Connection, root: RopeRoot) -> Result<RopeRoot> {
    let text = materialize(conn, root)?;
    build_from_text(conn, &text)
}

pub fn stats(conn: &Connection) -> Result<RopeStats> {
    Ok(RopeStats {
        nodes: conn.query_row("SELECT COUNT(*) FROM jazz_rope_node", [], |row| row.get(0))?,
        leaf_nodes: conn.query_row(
            "SELECT COUNT(*) FROM jazz_rope_node WHERE kind = ?",
            params![KIND_LEAF],
            |row| row.get(0),
        )?,
        position_leaf_nodes: conn.query_row(
            "SELECT COUNT(*) FROM jazz_rope_node WHERE kind = ?",
            params![KIND_POSITION_LEAF],
            |row| row.get(0),
        )?,
        concat_nodes: conn.query_row(
            "SELECT COUNT(*) FROM jazz_rope_node WHERE kind = ?",
            params![KIND_CONCAT],
            |row| row.get(0),
        )?,
        leaf_bytes: conn.query_row(
            "SELECT COALESCE(SUM(byte_len), 0) FROM jazz_rope_node WHERE kind = ?",
            params![KIND_LEAF],
            |row| row.get(0),
        )?,
        segment_bytes: conn.query_row(
            "SELECT COALESCE(SUM(length(CAST(text AS BLOB))), 0) FROM jazz_rope_segment",
            [],
            |row| row.get(0),
        )?,
        position_segment_bytes: conn.query_row(
            "SELECT COALESCE(SUM(32 + length(deltas)), 0) FROM jazz_rope_position_segment",
            [],
            |row| row.get(0),
        )?,
    })
}

pub fn root_leaf_count(conn: &Connection, root: RopeRoot) -> Result<i64> {
    let Some(root_id) = root else {
        return Ok(0);
    };
    let root = node(conn, root_id)?;
    match root.kind {
        KIND_LEAF | KIND_POSITION_LEAF => Ok(1),
        KIND_CONCAT => {
            Ok(root_leaf_count(conn, root.left_node_id)?
                + root_leaf_count(conn, root.right_node_id)?)
        }
        _ => Err(Error::new("unknown rope node kind")),
    }
}

pub fn root_len(conn: &Connection, root: RopeRoot) -> Result<i64> {
    let Some(root_id) = root else {
        return Ok(0);
    };
    node(conn, root_id).map(|node| node.byte_len)
}

pub fn append_position(conn: &Connection, root: RopeRoot, position: Position) -> Result<RopeRoot> {
    let (segment_id, segment_start) = append_position_segment(conn, position)?;
    concat(
        conn,
        root,
        Some(insert_position_leaf_ref(
            conn,
            segment_id,
            segment_start,
            1,
        )?),
    )
}

pub fn latest_position(conn: &Connection, root: RopeRoot) -> Result<Option<Position>> {
    let Some(root_id) = root else {
        return Ok(None);
    };
    latest_position_in_node(conn, root_id)
}

fn build_from_text(conn: &Connection, text: &str) -> Result<RopeRoot> {
    let mut root = None;
    for chunk in chunk_text(text, MAX_LEAF_BYTES) {
        root = concat(conn, root, Some(insert_leaf(conn, chunk)?))?;
    }
    Ok(root)
}

fn concat(conn: &Connection, left: RopeRoot, right: RopeRoot) -> Result<RopeRoot> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(node), None) | (None, Some(node)) => Ok(Some(node)),
        (Some(left), Some(right)) => {
            let byte_len = node(conn, left)?.byte_len + node(conn, right)?.byte_len;
            conn.execute(
                "INSERT INTO jazz_rope_node
                 (kind, byte_len, left_node_id, right_node_id, segment_id, segment_start)
                 VALUES (?, ?, ?, ?, NULL, NULL)",
                params![KIND_CONCAT, byte_len, left, right],
            )?;
            Ok(Some(conn.last_insert_rowid()))
        }
    }
}

fn split(conn: &Connection, root: RopeRoot, at_byte: usize) -> Result<(RopeRoot, RopeRoot)> {
    let Some(root_id) = root else {
        if at_byte == 0 {
            return Ok((None, None));
        }
        return Err(Error::new("rope split out of bounds"));
    };
    let root_node = node(conn, root_id)?;
    if at_byte > root_node.byte_len as usize {
        return Err(Error::new("rope split out of bounds"));
    }
    if at_byte == 0 {
        return Ok((None, Some(root_id)));
    }
    if at_byte == root_node.byte_len as usize {
        return Ok((Some(root_id), None));
    }
    match root_node.kind {
        KIND_LEAF => {
            let text = leaf_text(conn, &root_node)?;
            if !text.is_char_boundary(at_byte) {
                return Err(Error::new("rope split must be on UTF-8 boundary"));
            }
            let segment_id = root_node
                .segment_id
                .ok_or_else(|| Error::new("rope leaf missing segment"))?;
            let segment_start = root_node
                .segment_start
                .ok_or_else(|| Error::new("rope leaf missing segment start"))?;
            let left = insert_leaf_ref(conn, segment_id, segment_start, at_byte)?;
            let right = insert_leaf_ref(
                conn,
                segment_id,
                segment_start + at_byte as i64,
                text.len() - at_byte,
            )?;
            Ok((Some(left), Some(right)))
        }
        KIND_POSITION_LEAF => Err(Error::new("cannot byte-split position leaf")),
        KIND_CONCAT => {
            let left_id = root_node
                .left_node_id
                .ok_or_else(|| Error::new("rope concat missing left child"))?;
            let right_id = root_node
                .right_node_id
                .ok_or_else(|| Error::new("rope concat missing right child"))?;
            let left_len = node(conn, left_id)?.byte_len as usize;
            if at_byte < left_len {
                let (left_prefix, left_suffix) = split(conn, Some(left_id), at_byte)?;
                let right = concat(conn, left_suffix, Some(right_id))?;
                Ok((left_prefix, right))
            } else if at_byte == left_len {
                Ok((Some(left_id), Some(right_id)))
            } else {
                let (right_prefix, right_suffix) = split(conn, Some(right_id), at_byte - left_len)?;
                let left = concat(conn, Some(left_id), right_prefix)?;
                Ok((left, right_suffix))
            }
        }
        _ => Err(Error::new("unknown rope node kind")),
    }
}

fn materialize_into(
    conn: &Connection,
    root: RopeRoot,
    out: &mut String,
    segments: &mut BTreeMap<i64, String>,
) -> Result<()> {
    let Some(root_id) = root else {
        return Ok(());
    };
    let root = node(conn, root_id)?;
    match root.kind {
        KIND_LEAF => {
            out.push_str(&leaf_text_cached(conn, &root, segments)?);
        }
        KIND_POSITION_LEAF => return Err(Error::new("cannot materialize position leaf as text")),
        KIND_CONCAT => {
            materialize_into(conn, root.left_node_id, out, segments)?;
            materialize_into(conn, root.right_node_id, out, segments)?;
        }
        _ => return Err(Error::new("unknown rope node kind")),
    }
    Ok(())
}

fn insert_leaf(conn: &Connection, text: &str) -> Result<i64> {
    let (segment_id, segment_start) = append_segment(conn, text)?;
    insert_leaf_ref(conn, segment_id, segment_start, text.len())
}

fn insert_leaf_ref(
    conn: &Connection,
    segment_id: i64,
    segment_start: i64,
    byte_len: usize,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO jazz_rope_node
         (kind, byte_len, left_node_id, right_node_id, segment_id, segment_start)
         VALUES (?, ?, NULL, NULL, ?, ?)",
        params![KIND_LEAF, byte_len as i64, segment_id, segment_start],
    )?;
    Ok(conn.last_insert_rowid())
}

fn insert_position_leaf_ref(
    conn: &Connection,
    segment_id: i64,
    segment_start: i64,
    sample_count: usize,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO jazz_rope_node
         (kind, byte_len, left_node_id, right_node_id, segment_id, segment_start)
         VALUES (?, ?, NULL, NULL, ?, ?)",
        params![
            KIND_POSITION_LEAF,
            sample_count as i64,
            segment_id,
            segment_start
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn node(conn: &Connection, node_id: i64) -> Result<Node> {
    conn.query_row(
        "SELECT kind, byte_len, left_node_id, right_node_id, segment_id, segment_start
         FROM jazz_rope_node
         WHERE node_id = ?",
        params![node_id],
        |row| {
            Ok(Node {
                kind: row.get(0)?,
                byte_len: row.get(1)?,
                left_node_id: row.get(2)?,
                right_node_id: row.get(3)?,
                segment_id: row.get(4)?,
                segment_start: row.get(5)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| Error::new("unknown rope node"))
}

fn append_segment(conn: &Connection, text: &str) -> Result<(i64, i64)> {
    conn.execute(
        "INSERT INTO jazz_rope_segment (text) VALUES (?)",
        params![text],
    )?;
    Ok((conn.last_insert_rowid(), 0))
}

fn append_position_segment(conn: &Connection, position: Position) -> Result<(i64, i64)> {
    let encoded = encode_position(position);
    conn.execute(
        "INSERT INTO jazz_rope_position_segment (base_x, base_y, last_x, last_y, sample_count, deltas)
         VALUES (?, ?, ?, ?, 1, zeroblob(0))",
        params![encoded.0, encoded.1, encoded.0, encoded.1],
    )?;
    Ok((conn.last_insert_rowid(), 0))
}

fn latest_position_in_node(conn: &Connection, node_id: i64) -> Result<Option<Position>> {
    let root = node(conn, node_id)?;
    match root.kind {
        KIND_POSITION_LEAF => {
            let segment_id = root
                .segment_id
                .ok_or_else(|| Error::new("position leaf missing segment"))?;
            let segment_start = root
                .segment_start
                .ok_or_else(|| Error::new("position leaf missing start"))?;
            let (base_x, base_y, deltas): (i64, i64, Vec<u8>) = conn.query_row(
                "SELECT base_x, base_y, deltas
                 FROM jazz_rope_position_segment
                 WHERE segment_id = ?",
                params![segment_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let sample = position_sample_from_segment(
                base_x,
                base_y,
                &deltas,
                segment_start + root.byte_len - 1,
            )?;
            Ok(Some(decode_position(sample)))
        }
        KIND_CONCAT => {
            if let Some(right_id) = root.right_node_id {
                if let Some(position) = latest_position_in_node(conn, right_id)? {
                    return Ok(Some(position));
                }
            }
            if let Some(left_id) = root.left_node_id {
                return latest_position_in_node(conn, left_id);
            }
            Ok(None)
        }
        KIND_LEAF => Ok(None),
        _ => Err(Error::new("unknown rope node kind")),
    }
}

fn position_sample_from_segment(
    base_x: i64,
    base_y: i64,
    deltas: &[u8],
    sample_index: i64,
) -> Result<(i64, i64)> {
    if sample_index == 0 {
        return Ok((base_x, base_y));
    }
    let mut x = base_x;
    let mut y = base_y;
    for index in 0..sample_index as usize {
        let offset = index * 8;
        if offset + 8 > deltas.len() {
            return Err(Error::new("position delta segment is truncated"));
        }
        x += i32::from_le_bytes(deltas[offset..offset + 4].try_into().unwrap()) as i64;
        y += i32::from_le_bytes(deltas[offset + 4..offset + 8].try_into().unwrap()) as i64;
    }
    Ok((x, y))
}

fn encode_position(position: Position) -> (i64, i64) {
    (
        (position.x * POSITION_SCALE).round() as i64,
        (position.y * POSITION_SCALE).round() as i64,
    )
}

fn decode_position(position: (i64, i64)) -> Position {
    Position {
        x: position.0 as f64 / POSITION_SCALE,
        y: position.1 as f64 / POSITION_SCALE,
    }
}

fn leaf_text(conn: &Connection, node: &Node) -> Result<String> {
    let mut segments = BTreeMap::new();
    leaf_text_cached(conn, node, &mut segments)
}

fn leaf_text_cached(
    conn: &Connection,
    node: &Node,
    segments: &mut BTreeMap<i64, String>,
) -> Result<String> {
    let segment_id = node
        .segment_id
        .ok_or_else(|| Error::new("rope leaf missing segment"))?;
    let segment_start =
        node.segment_start
            .ok_or_else(|| Error::new("rope leaf missing segment start"))? as usize;
    if let Entry::Vacant(entry) = segments.entry(segment_id) {
        let text: String = conn.query_row(
            "SELECT text FROM jazz_rope_segment WHERE segment_id = ?",
            params![segment_id],
            |row| row.get(0),
        )?;
        entry.insert(text);
    }
    let text = segments
        .get(&segment_id)
        .ok_or_else(|| Error::new("rope segment cache missing segment"))?;
    let end = segment_start + node.byte_len as usize;
    Ok(text[segment_start..end].to_owned())
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

struct Node {
    kind: i64,
    byte_len: i64,
    left_node_id: RopeRoot,
    right_node_id: RopeRoot,
    segment_id: Option<i64>,
    segment_start: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        install(&conn).unwrap();
        conn
    }

    #[test]
    fn appends_create_immutable_segments_and_materialize_current_root() {
        let conn = conn();
        let mut root = None;
        for _ in 0..100 {
            root = append(&conn, root, " token").unwrap();
        }

        assert_eq!(materialize(&conn, root).unwrap(), " token".repeat(100));
        assert_eq!(root_leaf_count(&conn, root).unwrap(), 100);
        let segment_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jazz_rope_segment", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(segment_count, 100);
        assert_eq!(stats(&conn).unwrap().leaf_bytes, 600);
        assert_eq!(stats(&conn).unwrap().segment_bytes, 600);
        assert_eq!(root_len(&conn, root).unwrap(), 600);
    }

    #[test]
    fn replace_range_shares_unchanged_structure_and_materializes() {
        let conn = conn();
        let mut root = build_from_text(&conn, "hello brave new world").unwrap();
        let old_nodes = stats(&conn).unwrap().nodes;

        root = replace_range(&conn, root, 6, 5, "small").unwrap();

        assert_eq!(materialize(&conn, root).unwrap(), "hello small new world");
        assert!(stats(&conn).unwrap().nodes > old_nodes);
    }

    #[test]
    fn compact_text_root_rebuilds_deep_append_chain_into_large_immutable_leaves() {
        let conn = conn();
        let mut root = None;
        for _ in 0..100 {
            root = append(&conn, root, " token").unwrap();
        }

        let compacted = compact_text_root(&conn, root).unwrap();

        assert_eq!(materialize(&conn, compacted).unwrap(), " token".repeat(100));
        assert_eq!(root_leaf_count(&conn, root).unwrap(), 100);
        assert_eq!(root_leaf_count(&conn, compacted).unwrap(), 1);
        let segment_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jazz_rope_segment", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(segment_count, 101);
    }

    #[test]
    fn positions_create_immutable_segments_and_return_latest_sample() {
        let conn = conn();
        let mut root = None;
        for index in 0..100 {
            root = append_position(
                &conn,
                root,
                Position {
                    x: 10.0 + index as f64 * 0.25,
                    y: 20.0 - index as f64 * 0.5,
                },
            )
            .unwrap();
        }

        let latest = latest_position(&conn, root).unwrap().unwrap();
        assert_eq!(latest.x, 34.75);
        assert_eq!(latest.y, -29.5);
        let stats = stats(&conn).unwrap();
        assert_eq!(stats.position_leaf_nodes, 100);
        let segment_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM jazz_rope_position_segment",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(segment_count, 100);
        assert_eq!(stats.position_segment_bytes, 100 * 32);
    }
}
