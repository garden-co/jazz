//! The USDA plants records: parsing, loading with assigned ids, and id sampling.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use jazz::groove::records::Value;
use uuid::Uuid;

pub(crate) const TABLE: &str = "plants";
// NB: not "id" — that name resolves to Jazz's implicit row-identity (Uuid)
// column, which would type-mismatch against our String uuid literals.
pub(crate) const ID_COL: &str = "plant_id";
pub(crate) const FIELDS: [&str; 5] = [
    "symbol",
    "synonym_symbol",
    "scientific_name",
    "common_name",
    "family",
];
pub(crate) const FIELD_SEP: u8 = 0x1f;

/// One record: a stable assigned UUID plus the five CSV fields.
pub(crate) struct Plant {
    pub(crate) id: String,
    fields: [String; 5],
}

impl Plant {
    /// The row as Jazz cells (`id` + the five columns) for `tx.insert`.
    pub(crate) fn cells(&self) -> BTreeMap<String, Value> {
        let mut cells = BTreeMap::new();
        cells.insert(ID_COL.to_owned(), Value::String(self.id.clone()));
        for (name, value) in FIELDS.iter().zip(self.fields.iter()) {
            cells.insert((*name).to_owned(), Value::String(value.clone()));
        }
        cells
    }

    /// The row encoded for the raw-RocksDB value: fields joined by `FIELD_SEP`.
    pub(crate) fn raw_value(&self) -> Vec<u8> {
        self.fields
            .iter()
            .map(String::as_bytes)
            .collect::<Vec<_>>()
            .join(&[FIELD_SEP][..])
    }
}

/// Parse one RFC-4180-ish line: comma-separated, double-quoted fields, `""` a
/// literal quote. The USDA file quotes every field, one record per line.
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

/// Load the dataset and assign each row a stable UUID derived from its index so
/// every topology and every run sees the exact same id set.
pub(crate) fn load_dataset(path: &Path, limit: Option<usize>) -> Vec<Plant> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "read dataset {}: {e}\nrun dev/benchmarks/plants-rocksdb/scripts/setup.sh first",
            path.display()
        )
    });
    let mut plants = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut cols = parse_csv_line(line);
        cols.resize(5, String::new());
        let index = plants.len() as u64;
        plants.push(Plant {
            id: Uuid::from_u128(splitmix64(index.wrapping_add(1)) as u128).to_string(),
            fields: [
                std::mem::take(&mut cols[0]),
                std::mem::take(&mut cols[1]),
                std::mem::take(&mut cols[2]),
                std::mem::take(&mut cols[3]),
                std::mem::take(&mut cols[4]),
            ],
        });
        if let Some(limit) = limit
            && plants.len() >= limit
        {
            break;
        }
    }
    plants
}

/// Pick `count` distinct plant ids at random (seeded, reproducible).
pub(crate) fn sample_ids(plants: &[Plant], count: usize, seed: u64) -> Vec<String> {
    let count = count.min(plants.len());
    let mut chosen = BTreeSet::new();
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    while chosen.len() < count {
        state = splitmix64(state);
        chosen.insert((state as usize) % plants.len());
    }
    chosen.into_iter().map(|i| plants[i].id.clone()).collect()
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
