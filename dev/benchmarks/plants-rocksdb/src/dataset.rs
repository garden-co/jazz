//! The USDA plants records: schema columns, CSV parsing, loading with assigned
//! ids, id sampling, and on-disk size.

use std::path::Path;

use uuid::Uuid;

pub(crate) const TABLE: &str = "plants";
/// Schema columns in order: the assigned id plus the five CSV fields. Shared by
/// the Jazz schema and the row-cell builder so the names live in exactly one place.
pub(crate) const COLUMNS: [&str; 6] = [
    "plant_id",
    "symbol",
    "synonym_symbol",
    "scientific_name",
    "common_name",
    "family",
];
pub(crate) const FIELD_SEP: u8 = 0x1f;

pub(crate) struct Plant {
    pub(crate) id: String,
    pub(crate) fields: [String; 5],
}

impl Plant {
    /// Logical payload size: the assigned id plus every field byte.
    pub(crate) fn logical_len(&self) -> usize {
        self.id.len() + self.fields.iter().map(String::len).sum::<usize>()
    }

    /// Fields joined by `FIELD_SEP`, for the raw-RocksDB value.
    pub(crate) fn raw_value(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.fields.iter().map(String::len).sum::<usize>() + 4);
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(FIELD_SEP);
            }
            out.extend_from_slice(field.as_bytes());
        }
        out
    }
}

/// Parse one RFC-4180-ish line from the USDA file (every field double-quoted).
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

/// Load up to `limit` plant records, assigning each a stable UUID by index.
pub(crate) fn load_plants(limit: usize) -> Vec<Plant> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/data/plantlst.txt");
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}\nrun scripts/setup.sh first"));
    let mut out = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut cols = parse_csv_line(line);
        cols.truncate(5);
        cols.resize(5, String::new());
        out.push(Plant {
            id: Uuid::from_u128((out.len() + 1) as u128).to_string(),
            fields: cols.try_into().expect("exactly 5 fields"),
        });
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Pick `count` distinct plant ids (seeded xorshift, reproducible).
pub(crate) fn sample_ids(plants: &[Plant], count: usize) -> Vec<String> {
    let count = count.min(plants.len());
    let mut chosen = std::collections::BTreeSet::new();
    let mut state = 0x5eed_u64;
    while chosen.len() < count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chosen.insert((state as usize) % plants.len());
    }
    chosen.into_iter().map(|i| plants[i].id.clone()).collect()
}

pub(crate) fn logical_bytes(plants: &[Plant]) -> u64 {
    plants.iter().map(|p| p.logical_len() as u64).sum()
}

/// Recursive on-disk size of a path (directory or single file), in bytes.
pub(crate) fn dir_size(path: &Path) -> u64 {
    let Ok(md) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if md.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| dir_size(&e.path()))
            .sum()
    } else {
        md.len()
    }
}
