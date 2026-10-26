//! Stored names: where each schema version keeps its tables and columns.
//!
//! A table or column is stored under the name it was created with. A rename
//! lens changes only the name a schema version reads and writes it by, so
//! every version reads the same stored rows, history, indexes and policies'
//! evidence: no data is copied and no value is kept twice. Each admitted
//! version's map from its names to stored names is kept in the file
//! (`_jazz_names`), so a reopened store and its old-version views find the
//! same columns again.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rusqlite::Connection;

use crate::ids::SchemaVersionId;
use crate::options::{Error, ErrorCode};
use crate::protocol::{LensOp, MigrationLens};
use crate::records::Value;
use crate::schema::JazzSchema;
use crate::values::{Schema, StoredNames, storage, unsupported};

/// One row per (version, table, column); `col` is '' for the table itself.
/// `n` orders versions by admission.
pub(crate) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_names (version BLOB NOT NULL, \
     tbl TEXT NOT NULL, col TEXT NOT NULL, n INTEGER NOT NULL, stored_tbl TEXT NOT NULL, \
     stored_col TEXT NOT NULL, PRIMARY KEY (version, tbl, col)) WITHOUT ROWID;";

/// What a migration lens asks of the stored tables. SQLite applies a lens by
/// growing the stored tables (added columns read their default without being
/// rewritten; dropped ones stay stored) and by naming: a renamed table or
/// column keeps its stored name. Copies, transforms and rejected source
/// deltas are not supported yet.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LensEffects {
    /// Defaults for added columns, by (target table, column).
    pub defaults: BTreeMap<(String, String), Value>,
    /// Renamed tables: target name -> source name.
    pub tables: BTreeMap<String, String>,
    /// Renamed columns: (target table, target name) -> source name.
    pub columns: BTreeMap<(String, String), String>,
}

impl LensEffects {
    pub fn of(lens: &MigrationLens) -> Result<Self, Error> {
        let mut effects = Self::default();
        for table in &lens.table_lenses {
            let target = &table.target_table;
            if table.source_table != *target {
                effects
                    .tables
                    .insert(target.clone(), table.source_table.clone());
            }
            for op in &table.ops {
                match op {
                    LensOp::AddColumn { column, default } => {
                        effects
                            .defaults
                            .insert((target.clone(), column.clone()), default.clone());
                    }
                    LensOp::DropColumn { .. } => {}
                    LensOp::RenameColumn { from, to } => {
                        effects
                            .columns
                            .insert((target.clone(), to.clone()), from.clone());
                    }
                    LensOp::RenameTable { from, to } => {
                        if to != target {
                            return Err(Error::new(
                                ErrorCode::Schema,
                                format!("lens for `{target}` renames a table to `{to}`"),
                            ));
                        }
                        effects.tables.insert(to.clone(), from.clone());
                    }
                    other => return Err(unsupported(format!("lens op {other:?} on SQLite"))),
                }
            }
        }
        Ok(effects)
    }
}

/// The admitted versions' stored names, by version.
#[derive(Default)]
pub(crate) struct Names {
    versions: HashMap<SchemaVersionId, (i64, StoredNames)>,
}

fn schema_error(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Schema, message)
}

impl Names {
    pub(crate) fn load(conn: &Connection) -> Result<Self, Error> {
        conn.execute_batch(DDL).map_err(storage)?;
        let mut statement = conn
            .prepare("SELECT version, tbl, col, n, stored_tbl, stored_col FROM _jazz_names")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                ))
            })
            .map_err(storage)?;
        let mut names = Self::default();
        for row in rows {
            let (version, table, column, n, stored_table, stored_column) = row.map_err(storage)?;
            let version = SchemaVersionId::from_bytes(version.try_into().map_err(|_| {
                Error::new(ErrorCode::Storage, "bad schema version in _jazz_names")
            })?);
            let (_, names) = names
                .versions
                .entry(version)
                .or_insert_with(|| (n, StoredNames::default()));
            let (stored, columns) = names
                .tables
                .entry(table)
                .or_insert_with(|| (String::new(), BTreeMap::new()));
            if column.is_empty() {
                *stored = stored_table;
            } else {
                columns.insert(column, stored_column);
            }
        }
        Ok(names)
    }

    /// The most recently admitted version's names.
    pub(crate) fn latest(&self) -> Option<&StoredNames> {
        self.versions
            .values()
            .max_by_key(|(n, _)| *n)
            .map(|(_, names)| names)
    }

    pub(crate) fn get(&self, version: &SchemaVersionId) -> Option<&StoredNames> {
        self.versions.get(version).map(|(_, names)| names)
    }

    /// Record `schema`'s names (once per version), in memory and in the file.
    pub(crate) fn admit(&mut self, conn: &Connection, schema: &Schema) -> Result<(), Error> {
        let version = schema.jazz.version_id();
        if self.versions.contains_key(&version) {
            return Ok(());
        }
        let n = self.versions.values().map(|(n, _)| *n).max().unwrap_or(0) + 1;
        let names = schema.names();
        let id = version.as_bytes().to_vec();
        let mut insert = conn
            .prepare_cached(
                "INSERT OR REPLACE INTO _jazz_names (version, tbl, col, n, stored_tbl, stored_col) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .map_err(storage)?;
        for (table, (stored, columns)) in &names.tables {
            insert
                .execute(rusqlite::params![id, table, "", n, stored, ""])
                .map_err(storage)?;
            for (column, stored_column) in columns {
                insert
                    .execute(rusqlite::params![
                        id,
                        table,
                        column,
                        n,
                        stored,
                        stored_column
                    ])
                    .map_err(storage)?;
            }
        }
        self.versions.insert(version, (n, names));
        Ok(())
    }

    /// Where `schema` stores its tables and columns.
    ///
    /// An admitted version keeps the names it was admitted with. Otherwise a
    /// table or column takes the stored name its source has: the source of a
    /// lens into `schema` (`lenses` may name others; only lenses targeting
    /// `schema` count) where one says it was renamed, else `base`'s table or
    /// column of the same name. A new table or column is stored under its own
    /// name unless another admitted version stores a differently named one
    /// there (a rename freed the logical name, not the stored one), when it
    /// gets a fresh suffix. Where a lens from an admitted source adds it, any
    /// admitted version storing one there counts: the lens says the source
    /// lacks it, so a table or column dropped and added again is a new
    /// lineage with its own default, not the data it held before.
    pub(crate) fn resolve(
        &self,
        schema: &JazzSchema,
        lenses: &[MigrationLens],
        base: Option<&StoredNames>,
    ) -> Result<Schema, Error> {
        let version = schema.version_id();
        if let Some(names) = self.get(&version) {
            return Ok(Schema::stored_as(schema, names));
        }
        let into: Vec<&MigrationLens> = lenses.iter().filter(|l| l.target == version).collect();
        // The first lens whose source this store admitted; else the base.
        let lens = into
            .iter()
            .find(|lens| self.versions.contains_key(&lens.source))
            .or(into.first())
            .copied();
        let effects = lens.map(LensEffects::of).transpose()?.unwrap_or_default();
        let empty = StoredNames::default();
        let admitted_source = lens.and_then(|lens| self.get(&lens.source));
        let lineage = admitted_source.is_some();
        let source = admitted_source.or(base).unwrap_or(&empty);
        let mut target = Schema::new(schema);
        let mut names = StoredNames::default();
        // Tables with a source first, so a new table cannot take the stored
        // name a renamed one keeps.
        let mut order: Vec<&String> = target.tables.keys().collect();
        order.sort_by_key(|name| {
            let from = effects.tables.get(*name).unwrap_or(name);
            !source.tables.contains_key(from)
        });
        let mut taken_tables = BTreeSet::new();
        for name in order {
            let table = &target.tables[name];
            let from = effects.tables.get(name).unwrap_or(name);
            let renamed = from != name;
            let (stored, source_columns) = match source.tables.get(from) {
                Some((stored, columns)) => (stored.clone(), Some(columns)),
                None if renamed => {
                    return Err(schema_error(format!(
                        "lens renames table `{from}` to `{name}`, but the source has no `{from}`"
                    )));
                }
                None => (
                    self.fresh(name, &taken_tables, |stored, logical| {
                        self.claims_table(stored, logical, lineage)
                    }),
                    None,
                ),
            };
            taken_tables.insert(stored.clone());
            let mut columns = BTreeMap::new();
            let mut taken = BTreeSet::new();
            let mut fresh = Vec::new();
            for column in &table.columns {
                let from = effects
                    .columns
                    .get(&(name.clone(), column.name.clone()))
                    .unwrap_or(&column.name);
                if from != &column.name && table.branch_by.contains(&column.name) {
                    return Err(unsupported(format!(
                        "renaming branch column {name}.{from} on SQLite"
                    )));
                }
                match source_columns.and_then(|columns| columns.get(from)) {
                    Some(stored_column) => {
                        taken.insert(stored_column.clone());
                        columns.insert(column.name.clone(), stored_column.clone());
                    }
                    None if from != &column.name => {
                        return Err(schema_error(format!(
                            "lens renames column `{from}` of `{name}` to `{}`, but the source has no `{from}`",
                            column.name
                        )));
                    }
                    None => fresh.push(column.name.clone()),
                }
            }
            for column in fresh {
                let stored_column = self.fresh(&column, &taken, |candidate, logical| {
                    self.claims_column(&stored, candidate, logical, lineage)
                });
                taken.insert(stored_column.clone());
                columns.insert(column, stored_column);
            }
            names.tables.insert(name.clone(), (stored, columns));
        }
        target = Schema::stored_as(schema, &names);
        Ok(target)
    }

    /// `name`, or `name~2`, `name~3`, ... : the first not taken here and not
    /// claimed by another admitted version for a different logical name.
    fn fresh(
        &self,
        name: &str,
        taken: &BTreeSet<String>,
        claimed: impl Fn(&str, &str) -> bool,
    ) -> String {
        let mut candidate = name.to_owned();
        let mut n = 1;
        while taken.contains(&candidate) || claimed(&candidate, name) {
            n += 1;
            candidate = format!("{name}~{n}");
        }
        candidate
    }

    /// Whether an admitted version stores a table other than `logical` (any
    /// table, for a new `lineage`) as `stored`.
    fn claims_table(&self, stored: &str, logical: &str, lineage: bool) -> bool {
        self.versions.values().any(|(_, names)| {
            names
                .tables
                .iter()
                .any(|(name, (s, _))| s == stored && (lineage || name != logical))
        })
    }

    /// Whether an admitted version stores a column other than `logical` (any
    /// column, for a new `lineage`) as `stored` in stored table `table`.
    fn claims_column(&self, table: &str, stored: &str, logical: &str, lineage: bool) -> bool {
        self.versions.values().any(|(_, names)| {
            names.tables.values().any(|(s, columns)| {
                s == table
                    && columns
                        .iter()
                        .any(|(name, c)| c == stored && (lineage || name != logical))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TableLens;

    fn lens(ops: Vec<LensOp>) -> MigrationLens {
        MigrationLens::new(
            SchemaVersionId::from_bytes([1; 16]),
            SchemaVersionId::from_bytes([2; 16]),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops,
            }],
        )
        .unwrap()
    }

    /// SQLite applies a lens by naming and growing the stored tables. A
    /// rename only names the stored column; a copy, a transform or a
    /// rejected source delta would need rows rewritten, and is refused.
    #[test]
    fn lenses_rename_and_grow_stored_tables_and_refuse_what_would_rewrite_rows() {
        let renamed = LensEffects::of(&lens(vec![LensOp::RenameColumn {
            from: "title".to_owned(),
            to: "name".to_owned(),
        }]))
        .unwrap();
        assert_eq!(
            renamed
                .columns
                .get(&("todos".to_owned(), "name".to_owned())),
            Some(&"title".to_owned())
        );

        for op in [
            LensOp::CopyColumn {
                from: "title".to_owned(),
                to: "copy".to_owned(),
            },
            LensOp::TransformColumn {
                column: "title".to_owned(),
                transform: "jazz.identity".to_owned(),
            },
            LensOp::RejectSourceDelta {
                reason: "frozen".to_owned(),
            },
        ] {
            let error = LensEffects::of(&lens(vec![op])).unwrap_err();
            assert!(error.to_string().contains("not supported yet"), "{error}");
        }
    }
}
