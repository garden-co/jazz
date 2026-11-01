//! `dump-census <dump>`: one JSON line per dump with counts and lens-op
//! kinds only, never names or values, so its output can leave the cluster.
//! Run it on a `a59-export --catalogue-only` dump.

use std::collections::BTreeMap;

use migrate_alpha59_dump::{Reader, Record};

/// Runtime lens ops alpha.59 accepted and the SQLite engine refuses.
const UNSUPPORTED: [&str; 3] = ["CopyColumn", "TransformColumn", "RejectSourceDelta"];

/// The variant name of an externally tagged serde enum value.
fn kind(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(serde_json::Value::Object(map)) => map.keys().next().cloned().unwrap_or_default(),
        Ok(serde_json::Value::String(unit)) => unit,
        _ => "unparsed".to_owned(),
    }
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump-census <dump>");
    let file = std::io::BufReader::new(std::fs::File::open(path).expect("open dump"));
    let (mut reader, _) = Reader::open(file).expect("read header");
    let mut schemas = 0u64;
    let mut lenses = 0u64;
    let mut runtime_ops: BTreeMap<String, u64> = BTreeMap::new();
    let mut admin_entries: BTreeMap<String, u64> = BTreeMap::new();
    let mut admin_ops: BTreeMap<String, u64> = BTreeMap::new();
    let mut accounts = 0u64;
    while let Some(record) = reader.next_record().expect("read record") {
        match record {
            Record::Schema(_) => schemas += 1,
            Record::Lens(lens) => {
                lenses += 1;
                for table in &lens.tables {
                    for op in &table.ops_json {
                        *runtime_ops.entry(kind(op)).or_default() += 1;
                    }
                }
            }
            Record::AdminEntry(entry) => {
                if entry.object_type == "catalogue_lens"
                    && let Ok(transform) =
                        serde_json::from_str::<serde_json::Value>(&entry.payload_json)
                {
                    for op in transform["ops"].as_array().into_iter().flatten() {
                        *admin_ops.entry(kind(&op.to_string())).or_default() += 1;
                    }
                }
                *admin_entries.entry(entry.object_type).or_default() += 1;
            }
            Record::Account(_) => accounts += 1,
            _ => {}
        }
    }
    let footer = reader.footer();
    let unsupported: Vec<&str> = UNSUPPORTED
        .into_iter()
        .filter(|op| runtime_ops.contains_key(*op))
        .collect();
    println!(
        "{}",
        serde_json::json!({
            "schemas": schemas,
            "runtime_lenses": lenses,
            "runtime_lens_ops": runtime_ops,
            "admin_entries": admin_entries,
            "admin_lens_ops": admin_ops,
            "account_commands": accounts,
            "versions": footer.versions,
            "unsupported_by_sqlite": unsupported,
        })
    );
}
