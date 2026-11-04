//! Bring every row version to the active schema.
//!
//! Old migrations are not carried over: the SQLite store starts from the
//! active schema alone, as a fresh client does. Each version is translated
//! along alpha.59's own runtime lenses, forward, from the schema it was
//! written under to the active one, the way alpha.59 itself showed it to an
//! app on the active schema. Columns the active schema no longer has are
//! dropped, as those lenses already dropped them.

use std::collections::{BTreeSet, HashMap};

use migrate_alpha59_dump as dump;
use serde_json::Value as Json;

use crate::{err, uuid_string};

pub struct ToHead {
    pub head: [u8; 16],
    /// Forward lenses by source schema.
    lenses: HashMap<[u8; 16], dump::Lens>,
    /// Table names of each schema.
    tables: HashMap<[u8; 16], BTreeSet<String>>,
}

/// What became of one version.
pub enum Translated {
    Kept(dump::Version),
    /// Its table is not in the active schema.
    TableGone,
}

impl ToHead {
    pub fn new(
        head: [u8; 16],
        schemas: &[dump::Schema],
        lenses: Vec<dump::Lens>,
    ) -> Result<Self, String> {
        let mut tables = HashMap::new();
        for schema in schemas {
            let json: Json = serde_json::from_str(&schema.public_schema_json).map_err(err)?;
            let names = json["tables"]
                .as_object()
                .map(|tables| tables.keys().cloned().collect())
                .unwrap_or_default();
            tables.insert(schema.id, names);
        }
        if !tables.contains_key(&head) {
            return Err(format!(
                "the active schema {} is missing from the dump",
                uuid_string(head)
            ));
        }
        let mut forward = HashMap::new();
        for lens in lenses {
            if forward.insert(lens.source, lens).is_some() {
                return Err("several lenses lead out of one schema: not supported yet".to_owned());
            }
        }
        Ok(Self {
            head,
            lenses: forward,
            tables,
        })
    }

    pub fn translate(&self, mut version: dump::Version) -> Result<Translated, String> {
        let mut steps = 0;
        while version.schema != self.head {
            steps += 1;
            let lens = self
                .lenses
                .get(&version.schema)
                .filter(|_| steps <= self.lenses.len())
                .ok_or(format!(
                    "{} rows under schema {} have no lens path to the active schema: not supported yet",
                    version.table,
                    uuid_string(version.schema)
                ))?;
            match lens
                .tables
                .iter()
                .find(|table| table.source_table == version.table)
            {
                Some(table) => {
                    version.table = table.target_table.clone();
                    for op in &table.ops_json {
                        apply(&mut version.cells, op)?;
                    }
                }
                None => {
                    let in_target = self
                        .tables
                        .get(&lens.target)
                        .is_some_and(|tables| tables.contains(&version.table));
                    if !in_target {
                        return Ok(Translated::TableGone);
                    }
                }
            }
            version.schema = lens.target;
        }
        Ok(Translated::Kept(version))
    }
}

/// Apply one of alpha.59's runtime `LensOp`s, as its serde JSON.
fn apply(
    cells: &mut std::collections::BTreeMap<String, dump::Value>,
    op: &str,
) -> Result<(), String> {
    let op: Json = serde_json::from_str(op).map_err(err)?;
    let (kind, body) = op
        .as_object()
        .and_then(|op| op.iter().next())
        .ok_or(format!("unreadable lens op {op}"))?;
    let name = |key: &str| {
        body[key]
            .as_str()
            .map(str::to_owned)
            .ok_or(format!("lens op {kind} has no {key}"))
    };
    match kind.as_str() {
        "RenameColumn" => {
            if let Some(value) = cells.remove(&name("from")?) {
                cells.insert(name("to")?, value);
            }
        }
        "CopyColumn" => {
            if let Some(value) = cells.get(&name("from")?).cloned() {
                cells.insert(name("to")?, value);
            }
        }
        "AddColumn" => {
            let default: dump::Value = serde_json::from_value(body["default"].clone())
                .map_err(|error| format!("lens default {}: {error}", body["default"]))?;
            cells.entry(name("column")?).or_insert(default);
        }
        "DropColumn" => {
            cells.remove(&name("column")?);
        }
        // A table rename is the table lens's own source/target pair.
        "RenameTable" => {}
        other => return Err(format!("lens op {other} is not supported yet")),
    }
    Ok(())
}
