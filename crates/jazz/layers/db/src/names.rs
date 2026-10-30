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

/// The stored-name catalogue, created with the store.
///
/// - `_jazz_versions`: each admitted version, numbered `n` in admission
///   order. On a replica, `confirmed` is the [`names_digest`] its authority
///   confirmed for it: a version is confirmed while its names still have
///   that digest (an authority confirms every version it admits, so it
///   keeps none).
/// - `_jazz_names`: one row per (version, table, column) with where it is
///   stored; `col` is '' for the table itself.
/// - `_jazz_stored_columns`: each stored column, recorded when a version
///   storing it is first admitted: its value type (as JSON; a stored column
///   never changes type, so a column added again under a name some earlier
///   version stored continues that stored column only with this type, see
///   [`Names::resolve`]), and whether it is a branch column (SPEC 4,
///   INV-BVIEW-19) with the default a row written without one is keyed as
///   (`has_dflt` 0: none). A table is listed with `col` '' once admitted.
///   Every version admitted to the file, in any session, must key rows the
///   same way.
/// - `_jazz_lens_cells`: stored columns that lenses fill in rows inserted
///   by versions that do not name them, one row per (stored table, stored
///   column, lens target version): `src` names the column a copy follows,
///   else `dflt` is a dropped column's backwards default.
pub(crate) const DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_versions (n INTEGER PRIMARY KEY, version BLOB NOT NULL UNIQUE, \
     confirmed BLOB);\
    CREATE TABLE IF NOT EXISTS _jazz_names (n INTEGER NOT NULL, tbl TEXT NOT NULL, col TEXT NOT NULL, \
     stored_tbl TEXT NOT NULL, stored_col TEXT NOT NULL, PRIMARY KEY (n, tbl, col)) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_stored_columns (tbl TEXT NOT NULL, col TEXT NOT NULL, ty TEXT, \
     branch INTEGER NOT NULL DEFAULT 0, has_dflt INTEGER NOT NULL DEFAULT 0, dflt, \
     PRIMARY KEY (tbl, col)) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_lens_cells (tbl TEXT NOT NULL, col TEXT NOT NULL, \
     since BLOB NOT NULL, src TEXT, dflt, PRIMARY KEY (tbl, col, since)) WITHOUT ROWID;";

/// The enum catalogue, created with the first enum column a store tags, so
/// a store without one keeps its file as it was (a read-only open of it
/// writes nothing).
///
/// - `_jazz_enum_tags`: each stored enum column's tags (see
///   [`crate::values::EnumTags`]), one row per variant this store interned,
///   with the name versions declare it by. They are this store's own: no
///   other store sees them. A raw SQL read joins enum cells here for their
///   names.
/// - `_jazz_enum_defaults`: an enum column whose `ADD COLUMN ... DEFAULT`
///   tag a renumbering ([`Names::align`]) gave another variant, with the
///   default's name. A row inserted by a version not naming the column
///   stores this name's tag, not the stale one SQLite would fill in.
const ENUM_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_enum_tags (tbl TEXT NOT NULL, col TEXT NOT NULL, \
     tag INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY (tbl, col, tag)) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_enum_defaults (tbl TEXT NOT NULL, col TEXT NOT NULL, \
     name TEXT NOT NULL, PRIMARY KEY (tbl, col)) WITHOUT ROWID;";

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

/// The stored column a column named `name` starts in when `version`
/// introduces it: `name~` and six hex digits of a hash of both, where
/// `version` is the introducing schema's compatibility id (versions that
/// differ only in column order, merge strategies or indexes share it, so
/// whichever of them a store admits first names the column alike). Every
/// store derives it alike, whatever else it admitted and in whatever order,
/// so a replica stores a version's new columns where its authority does
/// without asking (SPEC 10.7). A column keeps its stored column across
/// renames and later versions; only a new lineage (a column added or
/// retyped) starts one.
pub(crate) fn stored_column(version: &SchemaVersionId, name: &str) -> String {
    stored_name(b"column", version, name)
}

/// The stored table a table named `name` starts in when `version`, not
/// the store's first, introduces it: as [`stored_column`]. The first
/// version's tables keep their own names.
pub(crate) fn stored_table(version: &SchemaVersionId, name: &str) -> String {
    stored_name(b"table", version, name)
}

fn stored_name(kind: &[u8], version: &SchemaVersionId, name: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [kind, version.as_bytes(), name.as_bytes()] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let hash = hasher.finalize();
    let [a, b, c, ..] = *hash.as_bytes();
    format!("{name}~{a:02x}{b:02x}{c:02x}")
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
    /// By (stored table, stored column): an enum column's default by name,
    /// where its `ADD COLUMN` default tag is stale (`_jazz_enum_defaults`).
    enum_defaults: std::cell::RefCell<BTreeMap<(String, String), String>>,
}

/// Two lineages of a table whose derived stored names collide (one in
/// 2^24 per pair of same-named ones): a store that holds both refuses the
/// second, with this typed error.
fn collision(what: String, stored: &str) -> Error {
    Error::new(
        ErrorCode::Schema,
        format!(
            "{what} would be stored as `{stored}`, which another lineage already uses: stored name collisions are not supported yet"
        ),
    )
}

fn schema_error(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Schema, message)
}

impl Names {
    pub(crate) fn load(conn: &Connection) -> Result<Self, Error> {
        conn.execute_batch(DDL).map_err(storage)?;
        conn.execute_batch(crate::catalogue::DDL).map_err(storage)?;
        let mut names = Self::default();
        let bad = |what: &str| Error::new(ErrorCode::Storage, format!("bad {what}"));
        let mut by_n = HashMap::new();
        let mut statement = conn
            .prepare("SELECT n, version, confirmed FROM _jazz_versions")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                ))
            })
            .map_err(storage)?;
        for row in rows {
            let (n, version, confirmed) = row.map_err(storage)?;
            let version = SchemaVersionId::from_bytes(
                version
                    .try_into()
                    .map_err(|_| bad("schema version in _jazz_versions"))?,
            );
            if let Some(digest) = confirmed {
                let digest = digest
                    .try_into()
                    .map_err(|_| bad("digest in _jazz_versions"))?;
                names.confirmed.insert(version, digest);
            }
            by_n.insert(n, version);
            names.versions.insert(version, (n, StoredNames::default()));
        }
        drop(statement);
        let mut statement = conn
            .prepare("SELECT n, tbl, col, stored_tbl, stored_col FROM _jazz_names")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(storage)?;
        for row in rows {
            let (n, table, column, stored_table, stored_column) = row.map_err(storage)?;
            let version = by_n
                .get(&n)
                .ok_or_else(|| bad("version number in _jazz_names"))?;
            let (_, names) = names.versions.get_mut(version).expect("loaded above");
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
        let mut statement = conn
            .prepare("SELECT tbl, col, ty, branch, has_dflt, dflt FROM _jazz_stored_columns")
            .map_err(storage)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, bool>(4)?,
                    r.get::<_, Sql>(5)?,
                ))
            })
            .map_err(storage)?;
        for row in rows {
            let (table, column, ty, branch, has_default, default) = row.map_err(storage)?;
            let columns = names.branches.entry(table.clone()).or_default();
            if column.is_empty() {
                continue;
            }
            if branch {
                columns.insert(column.clone(), has_default.then_some(default));
            }
            let ty = ty.ok_or_else(|| bad("column without a type in _jazz_stored_columns"))?;
            let ty =
                serde_json::from_str(&ty).map_err(|_| bad("value type in _jazz_stored_columns"))?;
            names.types.insert((table, column), ty);
        }
        drop(statement);
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
        if table_exists(conn, "_jazz_enum_defaults")? {
            let mut statement = conn
                .prepare("SELECT tbl, col, name FROM _jazz_enum_defaults")
                .map_err(storage)?;
            let rows = statement
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (table, column, name) = row.map_err(storage)?;
                names.enum_defaults.get_mut().insert((table, column), name);
            }
        }
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
            let since = SchemaVersionId::from_bytes(
                since
                    .try_into()
                    .map_err(|_| bad("schema version in _jazz_lens_cells"))?,
            );
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
        drop(statement);
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

    /// The lens cells of a row a replicated insert (one by stored names,
    /// from whichever version) writes into stored `table`, decided by the
    /// stored columns it `given`s rather than by its writer's version: a
    /// copy it leaves out follows a source it gives, and a dropped column
    /// it leaves out takes the newest backwards default. As
    /// [`Names::lens_cells`] for an insert naming every column of its
    /// version, which a replicated one does.
    pub(crate) fn stored_lens_cells(
        &self,
        table: &str,
        given: impl Fn(&str) -> bool,
    ) -> (Vec<(String, String)>, Vec<(String, Sql)>) {
        let (mut copies, mut dropped) = (Vec::new(), Vec::new());
        let mut cells: Vec<(i64, &LensCell)> = self
            .lens_cells
            .iter()
            .filter(|cell| cell.table == table && !given(&cell.column))
            .filter_map(|cell| Some((self.versions.get(&cell.since)?.0, cell)))
            .collect();
        cells.sort_by_key(|(since, _)| std::cmp::Reverse(*since));
        let mut filled = BTreeSet::new();
        for (_, cell) in cells {
            if !filled.insert(cell.column.as_str()) {
                continue;
            }
            match &cell.rule {
                LensCellRule::Copy(source) if given(source) => {
                    copies.push((source.clone(), cell.column.clone()));
                }
                LensCellRule::Dropped(default) => {
                    dropped.push((cell.column.clone(), default.clone()));
                }
                LensCellRule::Copy(_) => {}
            }
        }
        (copies, dropped)
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

    /// The most recently admitted version.
    pub(crate) fn latest_version(&self) -> Option<SchemaVersionId> {
        self.versions
            .iter()
            .max_by_key(|(_, (n, _))| *n)
            .map(|(version, _)| *version)
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

    /// Record `schema`'s version, names, stored columns and definition
    /// (once per version), in the file and then in memory. All are written
    /// in one savepoint, so a version is never known by name without its
    /// stored columns.
    pub(crate) fn admit(&mut self, conn: &Connection, schema: &Schema) -> Result<(), Error> {
        let version = schema.jazz.version_id();
        if self.versions.contains_key(&version) {
            return Ok(());
        }
        let n = self.versions.values().map(|(n, _)| *n).max().unwrap_or(0) + 1;
        let names = schema.names();
        let columns = self.new_stored_columns(schema)?;
        in_savepoint(conn, "jazz_names", || {
            conn.execute(
                "INSERT INTO _jazz_versions (n, version) VALUES (?1, ?2)",
                rusqlite::params![n, version.as_bytes().to_vec()],
            )
            .map_err(storage)?;
            write_names(conn, n, &names)?;
            write_stored_columns(conn, &columns)?;
            crate::catalogue::record_admitted(conn, n, &schema.jazz)
        })?;
        self.versions.insert(version, (n, names));
        for column in columns {
            let branches = self.branches.entry(column.table.clone()).or_default();
            let Some(ty) = column.ty else {
                continue;
            };
            if let Some(default) = column.branch {
                branches.insert(column.column.clone(), default);
            }
            self.types.insert((column.table, column.column), ty);
        }
        Ok(())
    }

    /// `schema`'s tables and stored columns not recorded yet: each new
    /// table (as `col` ''), and each new column with its type and, for a
    /// branch column, its default.
    fn new_stored_columns(&self, schema: &Schema) -> Result<Vec<NewStoredColumn>, Error> {
        let mut new = Vec::new();
        for table in schema.tables.values() {
            if !self.branches.contains_key(&table.stored) {
                new.push(NewStoredColumn {
                    table: table.stored.clone(),
                    column: String::new(),
                    ty: None,
                    branch: None,
                });
            }
            let branch = branch_columns(table);
            for column in &table.columns {
                let key = (table.stored.clone(), column.stored.clone());
                if self.types.contains_key(&key) {
                    continue;
                }
                new.push(NewStoredColumn {
                    table: key.0,
                    column: key.1,
                    ty: Some(column.stored_type()),
                    branch: branch
                        .get(column.stored.as_str())
                        .map(|column| branch_default(column))
                        .transpose()?,
                });
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
    /// tag for yet: each column's next tags, in declaration order. Every
    /// store does this as it admits a version; [`Self::align`] then puts
    /// the current schema's in its order.
    pub(crate) fn mint(&self, conn: &Connection, schema: &Schema) -> Result<(), Error> {
        for table in schema.tables.values() {
            for column in &table.columns {
                if let Some(variants) = &column.variants {
                    let variants: Vec<&str> = variants.iter().map(String::as_str).collect();
                    self.intern(conn, &table.stored, &column.stored, &variants)?;
                }
            }
        }
        Ok(())
    }

    /// Give each of `names` that stored column `column` of stored table
    /// `table` has no tag for the column's next tag, in the file and in
    /// memory. A name no admitted version declares (a removed variant a row
    /// still holds, say) gets one too: every enum cell is a tag.
    pub(crate) fn intern(
        &self,
        conn: &Connection,
        table: &str,
        column: &str,
        names: &[&str],
    ) -> Result<(), Error> {
        let tags = self.tags.column(table, column);
        let new: Vec<(i64, &str)> = {
            let tags = tags.borrow();
            let mut next = tags.next();
            let mut new: Vec<(i64, &str)> = Vec::new();
            for name in names {
                if tags.tag(name).is_none() && !new.iter().any(|(_, n)| n == name) {
                    new.push((next, name));
                    next += 1;
                }
            }
            new
        };
        if new.is_empty() {
            return Ok(());
        }
        in_savepoint(conn, "jazz_tags", || {
            conn.execute_batch(ENUM_DDL).map_err(storage)?;
            let mut insert = conn
                .prepare_cached(
                    "INSERT INTO _jazz_enum_tags (tbl, col, tag, name) VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(storage)?;
            for (tag, name) in &new {
                insert
                    .execute(rusqlite::params![table, column, tag, name])
                    .map_err(storage)?;
            }
            Ok(())
        })?;
        crate::values::tags_changing(conn, &tags);
        let mut tags = tags.borrow_mut();
        for (tag, name) in new {
            tags.insert(name.to_owned(), tag);
        }
        Ok(())
    }

    /// Renumber stored column `column` of stored table `table`'s tags so
    /// `declared` (the current schema's variants, all tagged) is in tag
    /// order (see [`crate::values::EnumTags::in_order`]): each variant
    /// takes its rank in `declared`'s order, in the file and in memory.
    /// Returns each (old, new) tag that changed; the caller rewrites the
    /// cells.
    pub(crate) fn align(
        &self,
        conn: &Connection,
        table: &str,
        column: &str,
        declared: &[String],
    ) -> Result<Vec<(i64, i64)>, Error> {
        let tags = self.tags.column(table, column);
        let (renumbered, changed) = {
            let tags = tags.borrow();
            if tags.in_order(declared) {
                return Ok(Vec::new());
            }
            let mut renumbered = crate::values::EnumTags::default();
            let mut changed = Vec::new();
            for (rank, tag) in tags.ranked(declared).into_iter().enumerate() {
                let rank = rank as i64;
                let name = tags.name(tag).expect("a ranked tag is a tag");
                renumbered.insert(name.to_owned(), rank);
                if rank != tag {
                    changed.push((tag, rank));
                }
            }
            (renumbered, changed)
        };
        // A column added with a default keeps SQLite's `DEFAULT` tag: if
        // it now names another variant, inserts that leave the column out
        // take the default by name instead (`_jazz_enum_defaults`).
        let key = (table.to_owned(), column.to_owned());
        let stale_default = match self.enum_defaults.borrow().contains_key(&key) {
            true => None,
            false => ddl_default(conn, table, column)?
                .filter(|tag| changed.iter().any(|(old, _)| old == tag))
                .and_then(|tag| tags.borrow().name(tag).map(str::to_owned)),
        };
        in_savepoint(conn, "jazz_tags", || {
            conn.execute_batch(ENUM_DDL).map_err(storage)?;
            if let Some(name) = &stale_default {
                conn.execute(
                    "INSERT INTO _jazz_enum_defaults (tbl, col, name) VALUES (?1, ?2, ?3)",
                    rusqlite::params![table, column, name],
                )
                .map_err(storage)?;
            }
            conn.execute(
                "DELETE FROM _jazz_enum_tags WHERE tbl = ?1 AND col = ?2",
                rusqlite::params![table, column],
            )
            .map_err(storage)?;
            let mut insert = conn
                .prepare_cached(
                    "INSERT INTO _jazz_enum_tags (tbl, col, tag, name) VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(storage)?;
            for (tag, name) in renumbered.iter() {
                insert
                    .execute(rusqlite::params![table, column, tag, name])
                    .map_err(storage)?;
            }
            Ok(())
        })?;
        crate::values::tags_changing(conn, &tags);
        *tags.borrow_mut() = renumbered;
        if let Some(name) = stale_default {
            self.enum_defaults.borrow_mut().insert(key, name);
        }
        Ok(changed)
    }

    /// The enum columns of stored table `table` whose default is by name
    /// (`_jazz_enum_defaults`), with the name.
    pub(crate) fn enum_defaults(&self, table: &str) -> Vec<(String, String)> {
        self.enum_defaults
            .borrow()
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|((_, column), name)| (column.clone(), name.clone()))
            .collect()
    }

    /// A new copy `copy` of stored column `source` of stored table `table`
    /// starts with its source's tags, removed variants' too, so its cells
    /// are its source's and it sorts like its source.
    pub(crate) fn copy_tags(
        &self,
        conn: &Connection,
        table: &str,
        source: &str,
        copy: &str,
    ) -> Result<(), Error> {
        let names: Vec<String> = self
            .tags
            .column(table, source)
            .borrow()
            .iter()
            .map(|(_, name)| name.to_owned())
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        self.intern(conn, table, copy, &names)
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
        // Only a version this store admitted is kept confirmed: the
        // upstream sends the versions it lacks ahead of their confirmation.
        let new: Vec<_> = confirmed
            .iter()
            .map(|(version, digest)| (SchemaVersionId::from_bytes(*version), *digest))
            .filter(|(version, digest)| {
                self.versions.contains_key(version) && self.confirmed.get(version) != Some(digest)
            })
            .collect();
        if new.is_empty() {
            return Ok(false);
        }
        in_savepoint(conn, "jazz_names", || {
            let mut update = conn
                .prepare_cached("UPDATE _jazz_versions SET confirmed = ?2 WHERE version = ?1")
                .map_err(storage)?;
            for (version, digest) in &new {
                update
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

    /// Several lenses into `schema` from versions this store admitted (a
    /// merge of two branches) must agree on where each column comes from:
    /// a column one lens keeps from its source and another adds, or keeps
    /// from another stored column, would read as new through the lens
    /// every store takes, losing the other branch's values. That is not
    /// supported yet.
    fn lenses_agree(&self, schema: &JazzSchema, into: &[&MigrationLens]) -> Result<(), Error> {
        let sources: Vec<(&MigrationLens, &StoredNames, LensEffects)> = into
            .iter()
            .filter_map(|lens| self.get(&lens.source).map(|names| (*lens, names)))
            .map(|(lens, names)| Ok((lens, names, LensEffects::of(lens)?)))
            .collect::<Result<_, Error>>()?;
        if sources.len() < 2 {
            return Ok(());
        }
        for table in schema.tables() {
            for column in &table.columns {
                let key = (table.name.clone(), column.name.clone());
                let lineages: BTreeSet<Option<&String>> = sources
                    .iter()
                    .map(|(_, names, effects)| {
                        if effects.defaults.contains_key(&key) || effects.copies.contains_key(&key)
                        {
                            return None;
                        }
                        let from_table = effects.tables.get(&table.name).unwrap_or(&table.name);
                        let from = effects.columns.get(&key).unwrap_or(&column.name);
                        names
                            .tables
                            .get(from_table)
                            .and_then(|(_, columns)| columns.get(from))
                    })
                    .collect();
                if lineages.len() > 1 {
                    return Err(Error::new(
                        ErrorCode::Schema,
                        format!(
                            "a version whose lenses from {} versions disagree on where `{}.{}` comes from (one adds it or keeps another column) is not supported yet",
                            sources.len(),
                            table.name,
                            column.name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Where `schema` stores its tables and columns.
    ///
    /// An admitted version keeps the names it was admitted with. Otherwise a
    /// table or column takes the stored name its source has: the source of a
    /// lens into `schema` (`lenses` may name others; only lenses targeting
    /// `schema` count) where one says it was renamed, else the source's
    /// table or column of the same name, if it has the type that stored
    /// column holds (a type changed in place starts a new column). Of
    /// several lenses into `schema`, the source is the smallest admitted
    /// version, as every store picks it. Without a lens from an admitted
    /// source, the source is `base`.
    ///
    /// What starts a lineage is named after `schema`'s compatibility id
    /// (SPEC 10.7):
    /// a new column `name~<hash>` ([`stored_column`]), a table a lens adds
    /// `name~<hash>` ([`stored_table`]). Followed along the app's lenses,
    /// every store derives the same names. Without a lens (a store's first
    /// version, or one admitted without the app's lenses), a table or column
    /// added again under a name an admitted version stored continues the
    /// stored one the last such version kept it in, a column only if it has
    /// that column's type; a new table otherwise keeps its own name when no
    /// admitted version stores one there. Those names depend on what the
    /// store admitted before, so a replica agrees with its authority on them
    /// only through the app's lenses. Two lineages whose derived names
    /// collide are refused ([`collision`]).
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
        let naming = schema.compatibility_id();
        let mut into: Vec<&MigrationLens> = lenses.iter().filter(|l| l.target == version).collect();
        // Of several lenses into `schema`, the one from the smallest source
        // version this store admitted, as every store picks it (see
        // `migration_path`); else the base.
        into.sort_by_key(|lens| lens.source);
        self.lenses_agree(schema, &into)?;
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
                // A table a lens adds is named after the version it adds it
                // in, alike on every store that follows the lens. One added
                // without a lens (a store's first version, or a version
                // admitted without the app's lenses) continues the table the
                // version last naming one kept it in, else keeps its name.
                None => {
                    let previous = (!lineage)
                        .then(|| self.last_table_stored_as(name))
                        .flatten()
                        .filter(|previous| !taken_tables.contains(*previous));
                    let plain = lens.is_none()
                        && !taken_tables.contains(name)
                        && !self.claims_table(name, "", true);
                    if let Some(previous) = previous {
                        (previous.to_owned(), None)
                    } else if plain {
                        (name.clone(), None)
                    } else {
                        let stored = stored_table(&naming, name);
                        if taken_tables.contains(&stored) || self.claims_table(&stored, "", true) {
                            return Err(collision(format!("table {name}"), &stored));
                        }
                        (stored, None)
                    }
                }
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
                    None => {
                        let candidate = stored_column(&naming, &column.name);
                        // Only a hash collision (one in 2^24 per pair of
                        // same-named lineages of a table) lands here: every
                        // store refuses the version alike.
                        if taken.contains(&candidate)
                            || self.claims_column(&stored, &candidate, "", true)
                        {
                            return Err(collision(
                                format!("column {name}.{}", column.name),
                                &candidate,
                            ));
                        }
                        candidate
                    }
                };
                taken.insert(stored_column.clone());
                columns.insert(column.name.clone(), stored_column);
            }
            names.tables.insert(name.clone(), (stored, columns));
        }
        target = Schema::stored_as(schema, &names, &self.tags);
        Ok(target)
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

    /// The stored table the most recently admitted version naming a table
    /// `logical` keeps it in.
    fn last_table_stored_as(&self, logical: &str) -> Option<&str> {
        self.versions
            .values()
            .filter_map(|(n, names)| names.tables.get(logical).map(|(stored, _)| (*n, stored)))
            .max_by_key(|(n, _)| *n)
            .map(|(_, stored)| stored.as_str())
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

/// The tag stored column `column` of stored table `table` was added with
/// (`ADD COLUMN ... DEFAULT`), if any.
fn table_exists(conn: &Connection, name: &str) -> Result<bool, Error> {
    conn.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE name = ?1",
        [name],
        |r| r.get(0),
    )
    .map_err(storage)
}

fn ddl_default(conn: &Connection, table: &str, column: &str) -> Result<Option<i64>, Error> {
    let mut statement = conn
        .prepare(&format!(
            "PRAGMA table_info({})",
            crate::values::quote(table)
        ))
        .map_err(storage)?;
    let rows = statement
        .query_map([], |r| {
            Ok((r.get::<_, String>(1)?, r.get::<_, Option<String>>(4)?))
        })
        .map_err(storage)?;
    for row in rows {
        let (name, default) = row.map_err(storage)?;
        if name == column {
            return Ok(default.and_then(|default| default.trim().parse().ok()));
        }
    }
    Ok(None)
}

fn write_names(conn: &Connection, n: i64, names: &StoredNames) -> Result<(), Error> {
    let mut insert = conn
        .prepare_cached(
            "INSERT INTO _jazz_names (n, tbl, col, stored_tbl, stored_col) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .map_err(storage)?;
    for (table, (stored, columns)) in &names.tables {
        insert
            .execute(rusqlite::params![n, table, "", stored, ""])
            .map_err(storage)?;
        for (column, stored_column) in columns {
            insert
                .execute(rusqlite::params![n, table, column, stored, stored_column])
                .map_err(storage)?;
        }
    }
    Ok(())
}

/// A stored table (`column` '', no type) or column to record (see
/// [`Names::new_stored_columns`]); `branch` is a branch column's default.
struct NewStoredColumn {
    table: String,
    column: String,
    ty: Option<ValueType>,
    branch: Option<Option<Sql>>,
}

fn write_stored_columns(conn: &Connection, columns: &[NewStoredColumn]) -> Result<(), Error> {
    let mut insert = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO _jazz_stored_columns (tbl, col, ty, branch, has_dflt, dflt) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(storage)?;
    for column in columns {
        let ty = column
            .ty
            .as_ref()
            .map(|ty| {
                serde_json::to_string(ty).map_err(|error| {
                    Error::new(
                        ErrorCode::Storage,
                        format!("encoding a column type: {error}"),
                    )
                })
            })
            .transpose()?;
        let default = column.branch.clone().flatten();
        insert
            .execute(rusqlite::params![
                column.table,
                column.column,
                ty,
                column.branch.is_some(),
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

    /// A tag interned inside a savepoint that rolls back leaves memory as
    /// it leaves the file, so the next name takes that tag; one interned
    /// inside a kept savepoint stays in both.
    #[test]
    fn a_rolled_back_savepoint_takes_its_interned_tags_with_it() {
        use crate::values::{begin_savepoint, end_savepoint};
        let conn = Connection::open_in_memory().unwrap();
        let names = Names::load(&conn).unwrap();
        let file = || -> Vec<(i64, String)> {
            if !table_exists(&conn, "_jazz_enum_tags").unwrap() {
                return Vec::new();
            }
            let mut statement = conn
                .prepare("SELECT tag, name FROM _jazz_enum_tags ORDER BY tag")
                .unwrap();
            statement
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        let tag = |name: &str| names.tags().column("t", "c").borrow().tag(name);
        begin_savepoint(&conn, "outer").unwrap();
        begin_savepoint(&conn, "inner").unwrap();
        names.intern(&conn, "t", "c", &["lost"]).unwrap();
        end_savepoint(&conn, "inner", true).unwrap();
        assert_eq!(tag("lost"), Some(0));
        end_savepoint(&conn, "outer", false).unwrap();
        assert_eq!(tag("lost"), None);
        assert_eq!(file(), []);

        begin_savepoint(&conn, "outer").unwrap();
        names.intern(&conn, "t", "c", &["kept"]).unwrap();
        end_savepoint(&conn, "outer", true).unwrap();
        assert_eq!(tag("kept"), Some(0));
        assert_eq!(file(), [(0, "kept".to_owned())]);
    }
}
