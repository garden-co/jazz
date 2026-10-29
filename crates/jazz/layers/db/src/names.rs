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
use crate::records::{Value, ValueType};
use crate::schema::JazzSchema;
use crate::values::{
    Column, Schema, Sql, StoredNames, TagRegistry, in_savepoint, storage, to_sql, unsupported,
};

/// One row per (version, table, column); `col` is '' for the table itself.
/// `n` orders versions by admission.
pub(crate) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_names (version BLOB NOT NULL, \
     tbl TEXT NOT NULL, col TEXT NOT NULL, n INTEGER NOT NULL, stored_tbl TEXT NOT NULL, \
     stored_col TEXT NOT NULL, PRIMARY KEY (version, tbl, col)) WITHOUT ROWID;";

/// Each stored table's branch columns (SPEC 4, INV-BVIEW-19), with the
/// default a row written without one is keyed as (`has_dflt` 0: none); a
/// table is listed with `col` '' once it has been admitted. Every version
/// admitted to the file, in any session, must key rows the same way.
const BRANCH_COLUMNS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_branch_columns (tbl TEXT NOT NULL, \
     col TEXT NOT NULL, has_dflt INTEGER NOT NULL, dflt, PRIMARY KEY (tbl, col)) WITHOUT ROWID;";

/// Stored columns that lenses fill in rows inserted by versions that do not
/// name them, one row per (stored table, stored column, lens target version):
/// `src` names the column a copy follows, else `dflt` is a dropped
/// column's backwards default. Created with the first such lens, so stores
/// without one keep their file as it was.
const LENS_CELLS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_lens_cells (tbl TEXT NOT NULL, \
     col TEXT NOT NULL, since BLOB NOT NULL, src TEXT, dflt, PRIMARY KEY (tbl, col, since)) \
     WITHOUT ROWID;";

/// Each stored column's value type, recorded when a version storing it is
/// first admitted (a stored column never changes type). A column added again
/// under a name some earlier version stored continues that stored column
/// only if its type is the one recorded here (see [`Names::resolve`]).
const COLUMN_TYPES_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_column_types (tbl TEXT NOT NULL, \
     col TEXT NOT NULL, ty BLOB NOT NULL, PRIMARY KEY (tbl, col)) WITHOUT ROWID;";

/// Each stored enum column's tags (see [`crate::values::EnumTags`]): one
/// row per variant the authority tagged, with the name versions declare it
/// by. A raw SQL read joins its enum cells here for their names.
const ENUM_TAGS_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_enum_tags (tbl TEXT NOT NULL, \
     col TEXT NOT NULL, tag INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY (tbl, col, tag)) \
     WITHOUT ROWID;";

/// One tag: (stored table, stored column, tag, variant name).
pub(crate) type Tag = (String, String, i64, String);

/// The variant names this store may hold as TEXT cells of an enum column,
/// for want of a tag when it wrote them (see [`crate::values::EnumTags`]):
/// once the tag arrives, those cells are rewritten to it. `ddl` 1: the
/// column was added here with this name as its SQLite default, which rows
/// inserted without the column read, so those inserts write the tag
/// instead. Created with the first such cell.
const ENUM_TEXT_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_enum_text (tbl TEXT NOT NULL, \
     col TEXT NOT NULL, name TEXT NOT NULL, ddl INTEGER NOT NULL, PRIMARY KEY (tbl, col, name)) \
     WITHOUT ROWID;";

/// The versions whose names this replica's authority confirmed, each with
/// the [`names_digest`] it confirmed: a version is confirmed while its names
/// still have that digest. An authority confirms every version it admits,
/// so it keeps none.
const CONFIRMED_DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_confirmed_names (version BLOB NOT NULL \
     PRIMARY KEY, digest BLOB NOT NULL) WITHOUT ROWID;";

/// A stored column a lens into version `since` fills in rows inserted by
/// versions that do not name it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LensCell {
    /// Stored table and column.
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) since: SchemaVersionId,
    pub(crate) rule: LensCellRule,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LensCellRule {
    /// A copy of stored column `.0`: a row a version admitted before
    /// `since` inserts gives the copy the source's value.
    Copy(String),
    /// A dropped column: a row inserted by `since` or a later version gives
    /// it this backwards default.
    Dropped(Sql),
}

/// What a migration lens asks of the stored tables. SQLite applies a lens by
/// growing the stored tables (added columns read their default without being
/// rewritten; dropped ones stay stored) and by naming: a renamed table or
/// column keeps its stored name. A copy is a stored column of its own
/// (SPEC 4 §4.6), filled from its source once when the lens is activated;
/// afterwards a row an older version inserts gives it the source's value,
/// and a write of the source leaves it as it is. A row a newer version
/// inserts gives a dropped column the lens's backwards default (see
/// [`LensCell`]). The identity transform changes no value.
/// Other transforms and rejected source deltas are not supported yet.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LensEffects {
    /// Defaults for added columns, by (target table, column).
    pub defaults: BTreeMap<(String, String), Value>,
    /// Renamed tables: target name -> source name.
    pub tables: BTreeMap<String, String>,
    /// Renamed columns: (target table, target name) -> source name.
    pub columns: BTreeMap<(String, String), String>,
    /// Copied columns: (target table, copy) -> source column.
    pub copies: BTreeMap<(String, String), String>,
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
                    LensOp::CopyColumn { from, to } => {
                        effects
                            .copies
                            .insert((target.clone(), to.clone()), from.clone());
                    }
                    LensOp::TransformColumn { transform, .. } => {
                        match crate::schema::registered_column_transform(transform) {
                            None => {
                                return Err(Error::new(
                                    ErrorCode::Schema,
                                    format!(
                                        "lens for `{target}` uses unregistered transform `{transform}`"
                                    ),
                                ));
                            }
                            // The identity transform's values are the source's.
                            Some(_)
                                if matches!(transform.as_str(), "identity" | "jazz.identity") => {}
                            Some(_) => return Err(unsupported(format!("transform `{transform}`"))),
                        }
                    }
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
                    other => return Err(unsupported(format!("lens op {other:?}"))),
                }
            }
        }
        Ok(effects)
    }
}

/// The identity of stored table `table` (with `column`, of its stored
/// column) in the app whose authority namespace is `namespace`: a name-based
/// UUID (RFC 9562 version 5) of the stored names. Stored names never change
/// for the life of a table or column (a rename lens keeps them, and a column
/// added again continues its stored column only with the same type), so
/// neither does its identity, on the authority or any replica mirroring it.
pub(crate) fn identity(namespace: [u8; 16], table: &str, column: Option<&str>) -> uuid::Uuid {
    let mut name = Vec::new();
    let parts = match column {
        None => vec![b"table".as_slice(), table.as_bytes()],
        Some(column) => vec![b"column".as_slice(), table.as_bytes(), column.as_bytes()],
    };
    for part in parts {
        name.extend_from_slice(&(part.len() as u64).to_le_bytes());
        name.extend_from_slice(part);
    }
    uuid::Uuid::new_v5(&uuid::Uuid::from_bytes(namespace), &name)
}

/// A digest of where a version stores its tables and columns: equal on
/// two stores that name it alike.
pub(crate) fn names_digest(names: &StoredNames) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    for (table, (stored, columns)) in &names.tables {
        for part in [table, stored] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        for (column, stored) in columns {
            for part in [column, stored] {
                hasher.update(&(part.len() as u64).to_le_bytes());
                hasher.update(part.as_bytes());
            }
        }
        hasher.update(&[0xff]);
    }
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("16 bytes")
}

/// The admitted versions' stored names, by version.
#[derive(Clone, Default)]
pub(crate) struct Names {
    versions: HashMap<SchemaVersionId, (i64, StoredNames)>,
    lens_cells: Vec<LensCell>,
    /// By stored table: its stored branch columns and their defaults.
    branches: BTreeMap<String, BTreeMap<String, Option<Sql>>>,
    /// By (stored table, stored column): its value type.
    types: BTreeMap<(String, String), ValueType>,
    /// The digests this replica's authority confirmed, by version.
    confirmed: HashMap<SchemaVersionId, [u8; 16]>,
    /// Every stored enum column's tags.
    tags: TagRegistry,
    /// By (stored table, stored column): the name its DDL default is, as
    /// TEXT (see `_jazz_enum_text`).
    text: BTreeMap<(String, String), BTreeSet<String>>,
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
        drop(statement);
        let has_cells: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE name = '_jazz_lens_cells'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        let has_branches: bool = conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE name = '_jazz_branch_columns'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if has_branches {
            let mut statement = conn
                .prepare("SELECT tbl, col, has_dflt, dflt FROM _jazz_branch_columns")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, bool>(2)?,
                        r.get::<_, Sql>(3)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, has_default, default) = row.map_err(storage)?;
                let columns = names.branches.entry(table).or_default();
                if !column.is_empty() {
                    columns.insert(column, has_default.then_some(default));
                }
            }
        }
        if table_exists(conn, "_jazz_column_types")? {
            let mut statement = conn
                .prepare("SELECT tbl, col, ty FROM _jazz_column_types")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, ty) = row.map_err(storage)?;
                let ty = postcard::from_bytes(&ty).map_err(|_| {
                    Error::new(ErrorCode::Storage, "bad value type in _jazz_column_types")
                })?;
                names.types.insert((table, column), ty);
            }
        }
        if table_exists(conn, "_jazz_confirmed_names")? {
            let mut statement = conn
                .prepare("SELECT version, digest FROM _jazz_confirmed_names")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(storage)?;
            for row in rows {
                let (version, digest) = row.map_err(storage)?;
                let bad = || Error::new(ErrorCode::Storage, "bad row in _jazz_confirmed_names");
                let version = SchemaVersionId::from_bytes(version.try_into().map_err(|_| bad())?);
                names
                    .confirmed
                    .insert(version, digest.try_into().map_err(|_| bad())?);
            }
        }
        if table_exists(conn, "_jazz_enum_tags")? {
            let mut statement = conn
                .prepare("SELECT tbl, col, tag, name FROM _jazz_enum_tags")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, tag, name) = row.map_err(storage)?;
                names
                    .tags
                    .column(&table, &column)
                    .borrow_mut()
                    .insert(name, tag);
            }
        }
        if table_exists(conn, "_jazz_enum_text")? {
            let mut statement = conn
                .prepare("SELECT tbl, col, name, ddl FROM _jazz_enum_text WHERE ddl = 1")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, bool>(3)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, name, _) = row.map_err(storage)?;
                names.text.entry((table, column)).or_default().insert(name);
            }
        }
        if has_cells {
            let mut statement = conn
                .prepare("SELECT tbl, col, since, src, dflt FROM _jazz_lens_cells")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Sql>(4)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, since, src, default) = row.map_err(storage)?;
                let since = SchemaVersionId::from_bytes(since.try_into().map_err(|_| {
                    Error::new(ErrorCode::Storage, "bad schema version in _jazz_lens_cells")
                })?);
                let rule = match src {
                    Some(source) => LensCellRule::Copy(source),
                    None => LensCellRule::Dropped(default),
                };
                names.lens_cells.push(LensCell {
                    table,
                    column,
                    since,
                    rule,
                });
            }
        }
        Ok(names)
    }

    /// Record lens cells (once each), in memory and in the file.
    pub(crate) fn record_lens_cells(
        &mut self,
        conn: &Connection,
        cells: Vec<LensCell>,
    ) -> Result<(), Error> {
        if cells.is_empty() {
            return Ok(());
        }
        conn.execute_batch(LENS_CELLS_DDL).map_err(storage)?;
        let mut insert = conn
            .prepare_cached(
                "INSERT OR IGNORE INTO _jazz_lens_cells (tbl, col, since, src, dflt) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .map_err(storage)?;
        for cell in cells {
            if self.lens_cells.contains(&cell) {
                continue;
            }
            let (source, default) = match &cell.rule {
                LensCellRule::Copy(source) => (Some(source.as_str()), Sql::Null),
                LensCellRule::Dropped(default) => (None, default.clone()),
            };
            insert
                .execute(rusqlite::params![
                    cell.table,
                    cell.column,
                    cell.since.as_bytes().to_vec(),
                    source,
                    default
                ])
                .map_err(storage)?;
            self.lens_cells.push(cell);
        }
        Ok(())
    }

    /// Whether any lens recorded cells to fill.
    pub(crate) fn has_lens_cells(&self) -> bool {
        !self.lens_cells.is_empty()
    }

    /// The lens cells a row admitted `version` inserts into `table` fills:
    /// (column, stored copy) for copies later versions' lenses made of its
    /// columns, and (stored column, backwards default) for columns earlier
    /// lenses dropped. Looked up per write, so a handle opened before a
    /// lens was activated fills them too.
    pub(crate) fn lens_cells(
        &self,
        version: &SchemaVersionId,
        table: &crate::values::Table,
    ) -> (Vec<(String, String)>, Vec<(String, Sql)>) {
        let (mut copies, mut dropped) = (Vec::new(), Vec::new());
        let Some((n, _)) = self.versions.get(version) else {
            return (copies, dropped);
        };
        for cell in &self.lens_cells {
            let Some((since, _)) = self.versions.get(&cell.since) else {
                continue;
            };
            if table.stored != cell.table || table.columns.iter().any(|c| c.stored == cell.column) {
                continue;
            }
            match &cell.rule {
                LensCellRule::Copy(source) if n < since => {
                    if let Some(source) = table.columns.iter().find(|c| c.stored == *source) {
                        copies.push((source.name.clone(), cell.column.clone()));
                    }
                }
                LensCellRule::Dropped(default) if n >= since => {
                    dropped.push((cell.column.clone(), default.clone()));
                }
                _ => {}
            }
        }
        (copies, dropped)
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

    /// Every admitted version with its names, in admission order.
    pub(crate) fn in_order(&self) -> Vec<(SchemaVersionId, &StoredNames)> {
        let mut versions: Vec<(i64, SchemaVersionId, &StoredNames)> = self
            .versions
            .iter()
            .map(|(version, (n, names))| (*n, *version, names))
            .collect();
        versions.sort_by_key(|(n, ..)| *n);
        versions
            .into_iter()
            .map(|(_, version, names)| (version, names))
            .collect()
    }

    /// The lens cells recorded, as an upstream sends them.
    pub(crate) fn cells(&self) -> &[LensCell] {
        &self.lens_cells
    }

    /// Stored table `from` (or, with `column`, its stored column
    /// `column.0`) is stored as `to` (`column.1`) now: every version that
    /// names it, and the lens cells and branch columns kept for it, follow.
    pub(crate) fn moved(
        &mut self,
        conn: &Connection,
        from: &str,
        to: &str,
        column: Option<(&str, &str)>,
    ) -> Result<(), Error> {
        in_savepoint(conn, "jazz_names", || {
            match column {
                None => {
                    conn.execute(
                        "UPDATE _jazz_names SET stored_tbl = ?2 WHERE stored_tbl = ?1",
                        [from, to],
                    )
                    .map_err(storage)?;
                }
                Some((old, new)) => {
                    conn.execute(
                        "UPDATE _jazz_names SET stored_col = ?3 WHERE stored_tbl = ?1 AND stored_col = ?2",
                        [from, old, new],
                    )
                    .map_err(storage)?;
                }
            }
            for (ddl_table, columns) in [
                ("_jazz_lens_cells", ["col", "src"].as_slice()),
                ("_jazz_branch_columns", ["col"].as_slice()),
                ("_jazz_column_types", ["col"].as_slice()),
                ("_jazz_enum_tags", ["col"].as_slice()),
                ("_jazz_enum_text", ["col"].as_slice()),
            ] {
                let exists: bool = conn
                    .query_row(
                        "SELECT count(*) > 0 FROM sqlite_master WHERE name = ?1",
                        [ddl_table],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                if !exists {
                    continue;
                }
                match column {
                    None => {
                        conn.execute(
                            &format!("UPDATE {ddl_table} SET tbl = ?2 WHERE tbl = ?1"),
                            [from, to],
                        )
                        .map_err(storage)?;
                    }
                    Some((old, new)) => {
                        for col in columns {
                            conn.execute(
                                &format!("UPDATE {ddl_table} SET {col} = ?3 WHERE tbl = ?1 AND {col} = ?2"),
                                [from, old, new],
                            )
                            .map_err(storage)?;
                        }
                    }
                }
            }
            Ok(())
        })?;
        self.tags.moved(from, to, column);
        let table_to = if column.is_none() { to } else { from };
        let mut text = BTreeMap::<(String, String), BTreeSet<String>>::new();
        for ((table, stored_column), names) in std::mem::take(&mut self.text) {
            let key = if table != from {
                (table, stored_column)
            } else {
                let stored_column = match column {
                    Some((old, new)) if stored_column == old => new.to_owned(),
                    _ => stored_column,
                };
                (table_to.to_owned(), stored_column)
            };
            text.entry(key).or_default().extend(names);
        }
        self.text = text;
        for (_, names) in self.versions.values_mut() {
            for (stored, columns) in names.tables.values_mut() {
                if stored != from {
                    continue;
                }
                match column {
                    None => *stored = to.to_owned(),
                    Some((old, new)) => {
                        for stored_column in columns.values_mut().filter(|c| *c == old) {
                            *stored_column = new.to_owned();
                        }
                    }
                }
            }
        }
        for cell in &mut self.lens_cells {
            if cell.table != from {
                continue;
            }
            cell.table = table_to.to_owned();
            if let Some((old, new)) = column {
                if cell.column == old {
                    cell.column = new.to_owned();
                }
                if let LensCellRule::Copy(source) = &mut cell.rule
                    && source == old
                {
                    *source = new.to_owned();
                }
            }
        }
        self.types = std::mem::take(&mut self.types)
            .into_iter()
            .map(|((table, stored_column), ty)| {
                if table != from {
                    return ((table, stored_column), ty);
                }
                let stored_column = match column {
                    Some((old, new)) if stored_column == old => new.to_owned(),
                    _ => stored_column,
                };
                ((table_to.to_owned(), stored_column), ty)
            })
            .collect();
        match column {
            None => {
                if let Some(branches) = self.branches.remove(from) {
                    self.branches.insert(to.to_owned(), branches);
                }
            }
            Some((old, new)) => {
                if let Some(branches) = self.branches.get_mut(from)
                    && let Some(default) = branches.remove(old)
                {
                    branches.insert(new.to_owned(), default);
                }
            }
        }
        Ok(())
    }

    /// Store `version` under `names` instead of what this file named it
    /// (an upstream's, which a replica mirrors), keeping its place in
    /// admission order.
    pub(crate) fn rename(
        &mut self,
        conn: &Connection,
        version: SchemaVersionId,
        names: StoredNames,
    ) -> Result<(), Error> {
        let Some((n, _)) = self.versions.get(&version).cloned() else {
            return Ok(());
        };
        in_savepoint(conn, "jazz_names", || {
            let id = version.as_bytes().to_vec();
            conn.execute("DELETE FROM _jazz_names WHERE version = ?1", [&id])
                .map_err(storage)?;
            let mut insert = conn
                .prepare_cached(
                    "INSERT INTO _jazz_names (version, tbl, col, n, stored_tbl, stored_col) \
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
            Ok(())
        })?;
        self.versions.insert(version, (n, names));
        Ok(())
    }

    /// Record `schema`'s names and branch columns (once per version), in
    /// the file and then in memory. Both are written in one savepoint, so a
    /// version is never known by name without its branch columns.
    pub(crate) fn admit(&mut self, conn: &Connection, schema: &Schema) -> Result<(), Error> {
        let version = schema.jazz.version_id();
        // A version a file admitted before its column types were kept
        // records them when it is opened again.
        let types = self.new_types(schema)?;
        if self.versions.contains_key(&version) {
            if !types.is_empty() {
                in_savepoint(conn, "jazz_names", || write_types(conn, &types))?;
                self.types
                    .extend(types.into_iter().map(|(key, ty, _)| (key, ty)));
            }
            return Ok(());
        }
        let n = self.versions.values().map(|(n, _)| *n).max().unwrap_or(0) + 1;
        let names = schema.names();
        let branches = self.new_branches(schema)?;
        in_savepoint(conn, "jazz_names", || {
            write_types(conn, &types)?;
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
            write_branches(conn, &branches)
        })?;
        self.versions.insert(version, (n, names));
        self.types
            .extend(types.into_iter().map(|(key, ty, _)| (key, ty)));
        for (table, column, default) in branches {
            let columns = self.branches.entry(table).or_default();
            if let Some(default) = default {
                columns.insert(column, default);
            }
        }
        Ok(())
    }

    /// `schema`'s stored columns whose type is not recorded yet, each with
    /// its type encoded as the file keeps it.
    fn new_types(&self, schema: &Schema) -> Result<Vec<NewType>, Error> {
        let mut new = Vec::new();
        for table in schema.tables.values() {
            for column in &table.columns {
                let key = (table.stored.clone(), column.stored.clone());
                if self.types.contains_key(&key) {
                    continue;
                }
                let ty = column.stored_type();
                let bytes = postcard::to_allocvec(&ty).map_err(|error| {
                    Error::new(
                        ErrorCode::Storage,
                        format!("encoding a column type: {error}"),
                    )
                })?;
                new.push((key, ty, bytes));
            }
        }
        Ok(new)
    }

    /// Refuse `schema` if it stores a column under a stored column recorded
    /// with another type: values of one type are never read as another's.
    pub(crate) fn check_types(&self, schema: &Schema) -> Result<(), Error> {
        for table in schema.tables.values() {
            for column in &table.columns {
                if self
                    .stored_type(&table.stored, &column.stored)
                    .is_some_and(|ty| *ty != column.stored_type())
                {
                    return Err(unsupported(format!(
                        "changing the type of {}.{}",
                        table.name, column.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// The value type of stored column `column` of stored table `table`,
    /// once a version storing it was admitted.
    pub(crate) fn stored_type(&self, table: &str, column: &str) -> Option<&ValueType> {
        self.types.get(&(table.to_owned(), column.to_owned()))
    }

    /// Every stored enum column's tags.
    pub(crate) fn tags(&self) -> &TagRegistry {
        &self.tags
    }

    /// Tag every variant `schema` declares that its stored column has no
    /// tag for yet, on a store whose catalogue is its own: each column's
    /// next tags, in declaration order. Returns the new tags.
    pub(crate) fn mint(&mut self, conn: &Connection, schema: &Schema) -> Result<Vec<Tag>, Error> {
        let mut new = Vec::new();
        for table in schema.tables.values() {
            for column in &table.columns {
                let (Some(variants), Some(tags)) = (&column.variants, &column.tags) else {
                    continue;
                };
                let tags = tags.borrow();
                let mut next = tags.next();
                for variant in variants.iter() {
                    if tags.tag(variant).is_some()
                        || new.iter().any(|(t, c, _, name): &Tag| {
                            *t == table.stored && *c == column.stored && name == variant
                        })
                    {
                        continue;
                    }
                    new.push((
                        table.stored.clone(),
                        column.stored.clone(),
                        next,
                        variant.clone(),
                    ));
                    next += 1;
                }
            }
        }
        self.record_tags(conn, &new)?;
        Ok(new)
    }

    /// Take the authority's `tags`. Returns the ones new here. A tag that
    /// disagrees with one this store keeps is refused: tags never change.
    pub(crate) fn learn(&mut self, conn: &Connection, tags: &[Tag]) -> Result<Vec<Tag>, Error> {
        let mut new = Vec::new();
        for tag @ (table, column, n, name) in tags {
            let known = self.tags.column(table, column);
            let known = known.borrow();
            match (known.tag(name), known.name(*n)) {
                (Some(m), _) if m == *n => continue,
                (None, None) => new.push(tag.clone()),
                _ => {
                    return Err(Error::new(
                        ErrorCode::Schema,
                        format!(
                            "the upstream tags variant `{name}` of {table}.{column} as {n}, \
                             which this store keeps otherwise"
                        ),
                    ));
                }
            }
        }
        self.record_tags(conn, &new)?;
        Ok(new)
    }

    /// A new copy `copy` of stored column `source` of stored table `table`
    /// starts with its source's tags, removed variants' too: it sorts like
    /// its source and every value it is filled with has a tag. Only the
    /// authority calls this; replicas take the copy's tags from its
    /// catalogue, since the source may have tagged more variants since.
    pub(crate) fn copy_tags(
        &mut self,
        conn: &Connection,
        table: &str,
        source: &str,
        copy: &str,
    ) -> Result<Vec<Tag>, Error> {
        let tags: Vec<Tag> = self
            .tags
            .column(table, source)
            .borrow()
            .iter()
            .map(|(tag, name)| (table.to_owned(), copy.to_owned(), tag, name.to_owned()))
            .collect();
        self.learn(conn, &tags)
    }

    /// Record `tags`, in the file and then in memory.
    fn record_tags(&mut self, conn: &Connection, tags: &[Tag]) -> Result<(), Error> {
        if tags.is_empty() {
            return Ok(());
        }
        in_savepoint(conn, "jazz_tags", || {
            conn.execute_batch(ENUM_TAGS_DDL).map_err(storage)?;
            let mut insert = conn
                .prepare_cached(
                    "INSERT INTO _jazz_enum_tags (tbl, col, tag, name) VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(storage)?;
            for (table, column, tag, name) in tags {
                insert
                    .execute(rusqlite::params![table, column, tag, name])
                    .map_err(storage)?;
            }
            Ok(())
        })?;
        for (table, column, tag, name) in tags {
            self.tags
                .column(table, column)
                .borrow_mut()
                .insert(name.clone(), *tag);
        }
        Ok(())
    }

    /// Stored column `column` of stored table `table` now holds `name` as
    /// a TEXT cell, for want of a tag; an insert noting it many times costs
    /// one statement each, so a writer notes each name once per write.
    pub(crate) fn note_text(
        conn: &Connection,
        table: &str,
        column: &str,
        name: &str,
    ) -> Result<(), Error> {
        conn.execute_batch(ENUM_TEXT_DDL).map_err(storage)?;
        conn.prepare_cached(
            "INSERT OR IGNORE INTO _jazz_enum_text (tbl, col, name, ddl) VALUES (?1, ?2, ?3, 0)",
        )
        .and_then(|mut insert| insert.execute(rusqlite::params![table, column, name]))
        .map_err(storage)?;
        Ok(())
    }

    /// Stored column `column` of stored table `table` was added with `name`
    /// as its DDL default, for want of a tag.
    pub(crate) fn note_ddl_text(
        &mut self,
        conn: &Connection,
        table: &str,
        column: &str,
        name: &str,
    ) -> Result<(), Error> {
        conn.execute_batch(ENUM_TEXT_DDL).map_err(storage)?;
        conn.prepare_cached(
            "INSERT OR REPLACE INTO _jazz_enum_text (tbl, col, name, ddl) VALUES (?1, ?2, ?3, 1)",
        )
        .and_then(|mut insert| insert.execute(rusqlite::params![table, column, name]))
        .map_err(storage)?;
        self.text
            .entry((table.to_owned(), column.to_owned()))
            .or_default()
            .insert(name.to_owned());
        Ok(())
    }

    /// The TEXT cells `tags` (just learned) let this store rewrite: each
    /// (stored table, stored column, tag, name) noted as held as TEXT.
    /// They are no longer noted, but for DDL defaults (see
    /// [`Self::ddl_text`]).
    pub(crate) fn untagged(conn: &Connection, tags: &[Tag]) -> Result<Vec<Tag>, Error> {
        if tags.is_empty() || !table_exists(conn, "_jazz_enum_text")? {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for tag @ (table, column, _, name) in tags {
            let noted = conn
                .prepare_cached(
                    "SELECT count(*) > 0 FROM _jazz_enum_text WHERE tbl = ?1 AND col = ?2 AND name = ?3",
                )
                .and_then(|mut select| {
                    select.query_row(rusqlite::params![table, column, name], |r| r.get(0))
                })
                .map_err(storage)?;
            if noted {
                out.push(tag.clone());
                conn.execute(
                    "DELETE FROM _jazz_enum_text WHERE tbl = ?1 AND col = ?2 AND name = ?3 AND ddl = 0",
                    rusqlite::params![table, column, name],
                )
                .map_err(storage)?;
            }
        }
        Ok(out)
    }

    /// The stored enum columns of stored table `table` whose DDL default is
    /// a name, with it: an insert that does not write one writes the
    /// name's tag instead, once there is one.
    pub(crate) fn ddl_text(&self, table: &str) -> Vec<(&str, &str)> {
        self.text
            .range((table.to_owned(), String::new())..)
            .take_while(|((t, _), _)| t == table)
            .flat_map(|((_, column), names)| {
                names
                    .iter()
                    .map(move |name| (column.as_str(), name.as_str()))
            })
            .collect()
    }

    /// Every tag, as the catalogue sends them.
    pub(crate) fn all_tags(&self) -> Vec<Tag> {
        self.tags
            .all()
            .into_iter()
            .flat_map(|((table, column), tags)| {
                tags.iter()
                    .map(|(tag, name)| (table.clone(), column.clone(), tag, name.to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Whether `version`'s names are the ones this replica's authority
    /// confirmed for it.
    pub(crate) fn confirmed(&self, version: &SchemaVersionId) -> bool {
        match (self.confirmed.get(version), self.get(version)) {
            (Some(digest), Some(names)) => *digest == names_digest(names),
            _ => false,
        }
    }

    /// The versions confirmed, with their digests, in admission order.
    pub(crate) fn confirmations(&self) -> Vec<([u8; 16], [u8; 16])> {
        self.in_order()
            .into_iter()
            .filter(|(version, _)| self.confirmed(version))
            .map(|(version, names)| (*version.as_bytes(), names_digest(names)))
            .collect()
    }

    /// Record that the authority confirmed these (version, digest) pairs;
    /// whether any is new here.
    pub(crate) fn confirm(
        &mut self,
        conn: &Connection,
        confirmed: &[([u8; 16], [u8; 16])],
    ) -> Result<bool, Error> {
        let new: Vec<_> = confirmed
            .iter()
            .map(|(version, digest)| (SchemaVersionId::from_bytes(*version), *digest))
            .filter(|(version, digest)| self.confirmed.get(version) != Some(digest))
            .collect();
        if new.is_empty() {
            return Ok(false);
        }
        in_savepoint(conn, "jazz_names", || {
            conn.execute_batch(CONFIRMED_DDL).map_err(storage)?;
            let mut insert = conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO _jazz_confirmed_names (version, digest) VALUES (?1, ?2)",
                )
                .map_err(storage)?;
            for (version, digest) in &new {
                insert
                    .execute(rusqlite::params![
                        version.as_bytes().to_vec(),
                        digest.to_vec()
                    ])
                    .map_err(storage)?;
            }
            Ok(())
        })?;
        self.confirmed.extend(new);
        Ok(true)
    }

    /// Refuse `schema` if it keys a stored table's rows differently from
    /// the versions admitted before it (SPEC 4, INV-BVIEW-19): it may add a
    /// branch column only as a new column with a default, which rows
    /// written before it and selectors of versions without it read as (see
    /// [`crate::values::Table::row_branch_key`]); it may not change a
    /// branch column's default, or turn an existing column into a branch
    /// column or a branch column into an ordinary one.
    pub(crate) fn check_branches(&self, schema: &Schema) -> Result<(), Error> {
        for table in schema.tables.values() {
            let Some(recorded) = self.branches.get(&table.stored) else {
                continue;
            };
            let branch = branch_columns(table);
            for (stored, default) in recorded {
                match branch.get(stored.as_str()) {
                    Some(column) if branch_default(column)? != *default => {
                        return Err(unsupported(format!(
                            "changing the default of branch column {}.{}",
                            table.name, column.name
                        )));
                    }
                    Some(_) => {}
                    None => {
                        if let Some(column) = table.columns.iter().find(|c| c.stored == *stored) {
                            return Err(unsupported(format!(
                                "making branch column {}.{} an ordinary column",
                                table.name, column.name
                            )));
                        }
                    }
                }
            }
            for (stored, column) in &branch {
                if recorded.contains_key(*stored) {
                    continue;
                }
                if self.stores_column(&table.stored, stored) {
                    return Err(unsupported(format!(
                        "making the existing column {}.{} a branch column",
                        table.name, column.name
                    )));
                }
                if column.default.is_none() {
                    return Err(unsupported(format!(
                        "adding branch column {}.{} without a default",
                        table.name, column.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Whether an admitted version stores a column as `column` of `table`.
    fn stores_column(&self, table: &str, column: &str) -> bool {
        self.versions.values().any(|(_, names)| {
            names
                .tables
                .values()
                .any(|(stored, columns)| stored == table && columns.values().any(|c| c == column))
        })
    }

    /// `schema`'s tables and branch columns not recorded yet: (stored
    /// table, stored column, default), with no column and no default for a
    /// table itself.
    fn new_branches(&self, schema: &Schema) -> Result<Vec<NewBranch>, Error> {
        let mut new = Vec::new();
        for table in schema.tables.values() {
            let recorded = self.branches.get(&table.stored);
            if recorded.is_none() {
                new.push((table.stored.clone(), String::new(), None));
            }
            for (stored, column) in branch_columns(table) {
                if !recorded.is_some_and(|recorded| recorded.contains_key(stored)) {
                    new.push((
                        table.stored.clone(),
                        stored.to_owned(),
                        Some(branch_default(column)?),
                    ));
                }
            }
        }
        Ok(new)
    }

    /// Where `schema` stores its tables and columns.
    ///
    /// An admitted version keeps the names it was admitted with. Otherwise a
    /// table or column takes the stored name its source has: the source of a
    /// lens into `schema` (`lenses` may name others; only lenses targeting
    /// `schema` count) where one says it was renamed, else `base`'s table or
    /// column of the same name, if it has the type that stored column holds
    /// (a type changed in place makes a new column). A new table is stored
    /// under its own name
    /// unless another admitted version stores a differently named one there
    /// (a rename freed the logical name, not the stored one), when it gets a
    /// fresh suffix. A column added again without a lens, under a name an
    /// admitted version stored in its table, continues the stored column the
    /// last such version kept it in only if it has that column's type: its
    /// identity is that stored column's, and only values of its type are
    /// stored there. Any other new column gets a stored name no admitted
    /// version uses. Where a lens from an admitted source adds a table or
    /// column, any admitted version storing one there counts: the lens says
    /// the source lacks it, so one dropped and added again is a new lineage
    /// with its own default, not the data it held before.
    pub(crate) fn resolve(
        &self,
        schema: &JazzSchema,
        lenses: &[MigrationLens],
        base: Option<&StoredNames>,
    ) -> Result<Schema, Error> {
        let version = schema.version_id();
        if let Some(names) = self.get(&version) {
            return Ok(Schema::stored_as(schema, names, &self.tags));
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
        let mut target = Schema::new(schema, &self.tags);
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
                    return Err(unsupported(format!("renaming branch column {name}.{from}")));
                }
                // A column whose type differs from the one its source column
                // is stored with is a new column: values of one type are
                // never read as another's. The rows already stored have no
                // value in it, so a required one needs a default.
                let retyped = source_columns
                    .and_then(|columns| columns.get(from))
                    .and_then(|stored_column| self.stored_type(&stored, stored_column))
                    .is_some_and(|ty| *ty != column.stored_type());
                if retyped
                    && !column.nullable()
                    && column.default.is_none()
                    && !effects
                        .defaults
                        .contains_key(&(name.clone(), column.name.clone()))
                {
                    return Err(unsupported(format!(
                        "changing the type of required column {name}.{} without a default",
                        column.name
                    )));
                }
                let source_column = source_columns
                    .and_then(|columns| columns.get(from))
                    .filter(|_| !retyped);
                match source_column {
                    Some(stored_column) => {
                        taken.insert(stored_column.clone());
                        columns.insert(column.name.clone(), stored_column.clone());
                    }
                    None if from != &column.name
                        && !source_columns.is_some_and(|columns| columns.contains_key(from)) =>
                    {
                        return Err(schema_error(format!(
                            "lens renames column `{from}` of `{name}` to `{}`, but the source has no `{from}`",
                            column.name
                        )));
                    }
                    None => fresh.push(column),
                }
            }
            for column in fresh {
                // Added again without a lens: it continues the stored
                // column the version last storing one of its name used,
                // when its values fit that column's type. Otherwise (and
                // always for a column a lens adds) it starts a new one.
                let previous = (!lineage)
                    .then(|| self.last_stored_as(&stored, &column.name))
                    .flatten()
                    .filter(|previous| {
                        !taken.contains(*previous)
                            && self.stored_type(&stored, previous) == Some(&column.stored_type())
                    });
                let stored_column = match previous {
                    Some(previous) => previous.to_owned(),
                    None => self.fresh(&column.name, &taken, |candidate, _| {
                        self.claims_column(&stored, candidate, "", true)
                    }),
                };
                taken.insert(stored_column.clone());
                columns.insert(column.name.clone(), stored_column);
            }
            names.tables.insert(name.clone(), (stored, columns));
        }
        target = Schema::stored_as(schema, &names, &self.tags);
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

    /// The stored column the most recently admitted version naming a column
    /// `logical` of stored table `table` keeps it in.
    fn last_stored_as(&self, table: &str, logical: &str) -> Option<&str> {
        self.versions
            .values()
            .filter_map(|(n, names)| {
                names
                    .tables
                    .values()
                    .find(|(stored, _)| stored == table)
                    .and_then(|(_, columns)| columns.get(logical))
                    .map(|column| (*n, column.as_str()))
            })
            .max_by_key(|(n, _)| *n)
            .map(|(_, column)| column)
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

fn table_exists(conn: &Connection, name: &str) -> Result<bool, Error> {
    conn.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE name = ?1",
        [name],
        |r| r.get(0),
    )
    .map_err(storage)
}

/// A stored column's type to record: (stored table, stored column), the
/// type, and its encoding.
type NewType = ((String, String), ValueType, Vec<u8>);

fn write_types(conn: &Connection, types: &[NewType]) -> Result<(), Error> {
    if types.is_empty() {
        return Ok(());
    }
    conn.execute_batch(COLUMN_TYPES_DDL).map_err(storage)?;
    let mut insert = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO _jazz_column_types (tbl, col, ty) VALUES (?1, ?2, ?3)",
        )
        .map_err(storage)?;
    for ((table, column), _, bytes) in types {
        insert
            .execute(rusqlite::params![table, column, bytes])
            .map_err(storage)?;
    }
    Ok(())
}

/// A stored table and branch column to record, with the column's default
/// (see [`Names::new_branches`]).
type NewBranch = (String, String, Option<Option<Sql>>);

fn write_branches(conn: &Connection, branches: &[NewBranch]) -> Result<(), Error> {
    if branches.is_empty() {
        return Ok(());
    }
    conn.execute_batch(BRANCH_COLUMNS_DDL).map_err(storage)?;
    let mut insert = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO _jazz_branch_columns (tbl, col, has_dflt, dflt) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(storage)?;
    for (table, column, default) in branches {
        let default = default.clone().flatten();
        insert
            .execute(rusqlite::params![
                table,
                column,
                default.is_some(),
                default.unwrap_or(Sql::Null)
            ])
            .map_err(storage)?;
    }
    Ok(())
}

/// A table's branch columns, by stored name.
fn branch_columns(table: &crate::values::Table) -> BTreeMap<&str, &Column> {
    table
        .columns
        .iter()
        .filter(|column| table.branch_by.contains(&column.name))
        .map(|column| (column.stored.as_str(), column))
        .collect()
}

/// The default a branch column keys rows written without it as.
fn branch_default(column: &Column) -> Result<Option<Sql>, Error> {
    column
        .default
        .as_ref()
        .map(|value| Ok(to_sql(value)?.unwrap_or(Sql::Null)))
        .transpose()
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
    /// rename only names the stored column, a copy is a column of its own
    /// and the identity transform changes nothing; an unregistered
    /// transform is rejected, and a rejected source delta is not supported.
    #[test]
    fn lenses_rename_copy_and_grow_stored_tables_and_refuse_what_they_cannot_apply() {
        let effects = LensEffects::of(&lens(vec![
            LensOp::RenameColumn {
                from: "title".to_owned(),
                to: "name".to_owned(),
            },
            LensOp::CopyColumn {
                from: "name".to_owned(),
                to: "search_name".to_owned(),
            },
            LensOp::TransformColumn {
                column: "name".to_owned(),
                transform: "jazz.identity".to_owned(),
            },
        ]))
        .unwrap();
        let key = |column: &str| ("todos".to_owned(), column.to_owned());
        assert_eq!(effects.columns.get(&key("name")), Some(&"title".to_owned()));
        assert_eq!(
            effects.copies.get(&key("search_name")),
            Some(&"name".to_owned())
        );

        let unregistered = LensEffects::of(&lens(vec![LensOp::TransformColumn {
            column: "title".to_owned(),
            transform: "app.uppercase".to_owned(),
        }]))
        .unwrap_err();
        assert_eq!(unregistered.code, ErrorCode::Schema);
        assert!(
            unregistered.to_string().contains("unregistered transform"),
            "{unregistered}"
        );

        let rejected = LensEffects::of(&lens(vec![LensOp::RejectSourceDelta {
            reason: "frozen".to_owned(),
        }]))
        .unwrap_err();
        assert!(
            rejected.to_string().contains("not supported yet"),
            "{rejected}"
        );
    }
}
