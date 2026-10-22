//! Jazz values in SQLite cells, the schema as SQLite sees it, and rows.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode};
use crate::records::{Value, ValueType};
use crate::schema::JazzSchema;
use crate::tools::{PolicyExpr, TablePolicies};
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

pub(crate) fn unsupported(what: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Query, format!("{what} is not supported yet"))
}

pub(crate) fn query_error(what: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Query, what.to_string())
}

pub(crate) fn storage(error: rusqlite::Error) -> Error {
    // Only the large-value collation and functions interrupt a statement,
    // when a chunk it needs is not local (see `crate::large`).
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted) {
        return crate::large::missing();
    }
    Error::new(ErrorCode::Storage, error.to_string())
}

/// One SQLite value, as bound or read. Text holds raw bytes: large-capable
/// columns store text that is not UTF-8 (tagged bytes and chunked large-value
/// descriptors, see [`crate::large`]), which SQLite stores and compares
/// without interpreting.
#[derive(Clone, Debug, PartialEq)]
pub enum Sql {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl Sql {
    pub(crate) fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Sql::Null => ValueRef::Null,
            Sql::Integer(v) => ValueRef::Integer(*v),
            Sql::Real(v) => ValueRef::Real(*v),
            Sql::Text(v) => ValueRef::Text(v),
            Sql::Blob(v) => ValueRef::Blob(v),
        }
    }
}

impl From<ValueRef<'_>> for Sql {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Sql::Null,
            ValueRef::Integer(v) => Sql::Integer(v),
            ValueRef::Real(v) => Sql::Real(v),
            ValueRef::Text(v) => Sql::Text(v.to_vec()),
            ValueRef::Blob(v) => Sql::Blob(v.to_vec()),
        }
    }
}

impl ToSql for Sql {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.as_ref()))
    }
}

impl FromSql for Sql {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Sql::from(value))
    }
}

/// A Jazz value as a SQLite cell. `None` means "no SQL value": NaN, which
/// SQLite would silently store as NULL, so comparisons handle it before SQL.
pub(crate) fn to_sql(value: &Value) -> Result<Option<Sql>, Error> {
    Ok(Some(match value {
        Value::Nullable(None) => Sql::Null,
        Value::Nullable(Some(inner)) => return to_sql(inner),
        Value::U8(v) => Sql::Integer(*v as i64),
        Value::U16(v) => Sql::Integer(*v as i64),
        Value::U32(v) => Sql::Integer(*v as i64),
        // Timestamps are u64 ms; they fit i64 for any real date. A u64 above
        // i64::MAX would order wrongly; reject rather than wrap.
        Value::U64(v) | Value::U48(v) => {
            Sql::Integer(i64::try_from(*v).map_err(|_| unsupported("a u64 above i64::MAX"))?)
        }
        Value::I32(v) => Sql::Integer(*v as i64),
        Value::I64(v) => Sql::Integer(*v),
        Value::F64(v) if v.is_nan() => return Ok(None),
        Value::F64(v) => Sql::Real(*v),
        Value::Bool(v) => Sql::Integer(*v as i64),
        Value::String(v) => Sql::Text(v.as_bytes().to_vec()),
        Value::Bytes(v) => Sql::Text(crate::large::inline_bytes(v)),
        // A chunked cell carries its sort prefix, which needs the chunks:
        // descriptors are stored through `Inner::large_cell`.
        Value::Large(_) => return Err(unsupported("a large-value descriptor as a literal")),
        Value::Uuid(v) => Sql::Blob(v.as_bytes().to_vec()),
        Value::EnumTag(v) => Sql::Integer(*v as i64),
        other => return Err(unsupported(format!("storing {other:?} in SQLite"))),
    }))
}

/// Decode a SQLite cell by the column's declared type, reading a chunked
/// large value's chunks. `None` is a NULL in a non-nullable column, which
/// only an unwritten cell can hold.
pub(crate) fn from_sql(
    ty: &ValueType,
    cell: ValueRef<'_>,
    chunks: &crate::large::Chunks,
) -> Result<Option<Value>, Error> {
    if let ValueType::Nullable(inner) = ty {
        return Ok(Some(match cell {
            ValueRef::Null => Value::Nullable(None),
            cell => Value::Nullable(from_sql(inner, cell, chunks)?.map(Box::new)),
        }));
    }
    match (ty, cell) {
        (ValueType::String, ValueRef::Text(cell)) => {
            let text = match cell.first() {
                Some(&crate::large::CHUNKED) => chunks.materialize(cell)?,
                _ => cell.to_vec(),
            };
            return Ok(String::from_utf8(text).ok().map(Value::String));
        }
        (ValueType::Bytes, ValueRef::Text(cell)) => {
            return Ok(Some(Value::Bytes(chunks.materialize(cell)?)));
        }
        _ => {}
    }
    Ok(decode_fixed(ty, cell))
}

fn decode_fixed(ty: &ValueType, cell: ValueRef<'_>) -> Option<Value> {
    let int = || cell.as_i64().ok();
    Some(match ty {
        ValueType::U8 => Value::U8(int()? as u8),
        ValueType::U16 => Value::U16(int()? as u16),
        ValueType::U32 => Value::U32(int()? as u32),
        ValueType::U64 => Value::U64(int()? as u64),
        ValueType::U48 => Value::U48(int()? as u64),
        ValueType::I32 => Value::I32(int()? as i32),
        ValueType::I64 => Value::I64(int()?),
        ValueType::F64 => Value::F64(match cell {
            ValueRef::Real(v) => v,
            ValueRef::Integer(v) => v as f64,
            _ => return None,
        }),
        ValueType::Bool => Value::Bool(int()? != 0),
        ValueType::Uuid => Value::Uuid(uuid::Uuid::from_slice(cell.as_blob().ok()?).ok()?),
        ValueType::EnumTag(_) => Value::EnumTag(int()? as u8),
        _ => return None,
    })
}

/// One application column.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: ValueType,
    pub default: Option<Value>,
    pub json: bool,
}

impl Column {
    pub fn nullable(&self) -> bool {
        matches!(self.ty, ValueType::Nullable(_))
    }
}

/// One application table.
#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub references: BTreeMap<String, String>,
    pub policies: TablePolicies,
    pub indexes: Vec<Vec<String>>,
    /// Branch-key columns. A row's identity is `(id, branch key)`.
    pub branch_by: Vec<String>,
    /// Engine-maintained provenance columns (`$createdAt`, ...): kept on
    /// every row version, readable by name, never written by the app and
    /// not part of the default projection.
    pub provenance: Vec<Column>,
    /// How provenance is stored: authors as interned codes.
    pub stored_provenance: Vec<Column>,
}

/// Provenance as read. Timestamps are Unix ms (u64, as in Jazz); authors
/// are read by their identity parts, so policies and filters can compare
/// `$createdBy.identity.subject` and friends.
pub(crate) const PROVENANCE: [&str; 6] = [
    "$createdAt",
    "$updatedAt",
    "$createdBy.identity.issuer",
    "$createdBy.identity.subject",
    "$updatedBy.identity.issuer",
    "$updatedBy.identity.subject",
];

/// Provenance as stored: each author is one code into `_jazz_authors`
/// instead of its two strings on every row version.
pub(crate) const STORED_PROVENANCE: [&str; 4] =
    ["$createdAt", "$updatedAt", "$createdBy", "$updatedBy"];

/// The stored author column and identity part behind a readable provenance
/// column, e.g. `$createdBy.identity.subject` -> (`$createdBy`, `subject`).
pub(crate) fn author_part(column: &str) -> Option<(&'static str, &'static str)> {
    Some(match column {
        "$createdBy.identity.issuer" => ("$createdBy", "issuer"),
        "$createdBy.identity.subject" => ("$createdBy", "subject"),
        "$updatedBy.identity.issuer" => ("$updatedBy", "issuer"),
        "$updatedBy.identity.subject" => ("$updatedBy", "subject"),
        _ => return None,
    })
}

/// The create stamp an update stamp falls back to. A version written once
/// stores its update stamp as NULL instead of a copy of the create stamp.
pub(crate) fn elided_as(stored: &str) -> Option<&'static str> {
    match stored {
        "$updatedAt" => Some("$createdAt"),
        "$updatedBy" => Some("$createdBy"),
        _ => None,
    }
}

fn stored_provenance_columns() -> Vec<Column> {
    STORED_PROVENANCE
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            ty: ValueType::Nullable(Box::new(ValueType::U64)),
            default: None,
            json: false,
        })
        .collect()
}

fn provenance_columns() -> Vec<Column> {
    PROVENANCE
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            ty: if name.ends_with("At") {
                ValueType::Nullable(Box::new(ValueType::U64))
            } else {
                ValueType::Nullable(Box::new(ValueType::String))
            },
            default: None,
            json: false,
        })
        .collect()
}

impl Table {
    /// A readable column: an application or provenance column.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .chain(&self.provenance)
            .find(|column| column.name == name)
    }

    /// Every stored column: application columns, then stored provenance.
    pub fn stored(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().chain(&self.stored_provenance)
    }

    pub fn has_column(&self, name: &str) -> bool {
        name == "id" || self.column(name).is_some()
    }

    /// The policy that gates `operation`, or `None` (deny).
    pub fn policy(&self, operation: crate::tools::Operation) -> Option<&PolicyExpr> {
        use crate::tools::Operation::*;
        let policy = match operation {
            Select => &self.policies.select,
            Insert => &self.policies.insert,
            Update => &self.policies.update,
            Delete => &self.policies.delete,
        };
        policy.using.as_ref().or(policy.with_check.as_ref())
    }
}

/// The schema as the SQLite engine sees it.
#[derive(Clone, Debug)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub jazz: JazzSchema,
}

impl Schema {
    pub fn new(jazz: &JazzSchema) -> Self {
        let mut tables = BTreeMap::new();
        for table in jazz.tables() {
            let public = jazz
                .public_schema()
                .iter()
                .find(|(name, _)| name.as_str() == table.name)
                .map(|(_, public)| public);
            let columns = table
                .columns
                .iter()
                .map(|column| Column {
                    name: column.name.clone(),
                    ty: column.column_type.clone(),
                    default: column.default.clone(),
                    json: format!("{:?}", column.large_value_kind) == "Json",
                })
                .collect();
            let mut indexes: Vec<Vec<String>> = table
                .references
                .keys()
                .chain(table.indexed_columns.iter())
                .map(|column| vec![column.clone()])
                .collect();
            indexes.extend(table.composite_indexes.iter().cloned());
            indexes.sort();
            indexes.dedup();
            tables.insert(
                table.name.clone(),
                Table {
                    name: table.name.clone(),
                    columns,
                    references: table.references.clone(),
                    policies: public
                        .map(|public| public.policies.clone())
                        .unwrap_or_default(),
                    indexes,
                    branch_by: table.branch_by.clone(),
                    provenance: provenance_columns(),
                    stored_provenance: stored_provenance_columns(),
                },
            );
        }
        Self {
            tables,
            jazz: jazz.clone(),
        }
    }

    pub fn table(&self, name: &str) -> Result<&Table, Error> {
        self.tables
            .get(name)
            .ok_or_else(|| query_error(format!("unknown table {name}")))
    }

    /// DDL for a fresh store. Columns are declared without a type so SQLite
    /// keeps every value's storage class as bound (no affinity coercion).
    pub fn ddl(&self) -> String {
        self.tables.values().map(Table::ddl).collect()
    }

    /// The physical store for `self` plus `other`: tables and columns are
    /// unioned, so every schema view reads and writes one set of rows. Returns
    /// the DDL that brings an existing store up to the union: new tables, and
    /// `ADD COLUMN ... DEFAULT` for new columns (an O(1) schema change; old
    /// rows read the default without being rewritten).
    pub fn union(&mut self, other: &Schema) -> Result<String, Error> {
        let mut ddl = String::new();
        for (name, table) in &other.tables {
            match self.tables.get_mut(name) {
                None => {
                    ddl.push_str(&table.ddl());
                    self.tables.insert(name.clone(), table.clone());
                }
                Some(physical) => {
                    for column in &table.columns {
                        if physical.column(&column.name).is_some() {
                            continue;
                        }
                        let default = match &column.default {
                            Some(value) => sql_literal(value)?,
                            None => "NULL".to_owned(),
                        };
                        for target in [name.clone(), format!("{name}__h")] {
                            ddl.push_str(&format!(
                                "ALTER TABLE {} ADD COLUMN {} DEFAULT {default};\n",
                                quote(&target),
                                column_def(column)
                            ));
                        }
                        if crate::large::large_capable(column) {
                            ddl.push_str(&crate::large::triggers(physical, &[column]));
                        }
                        physical.columns.push(column.clone());
                    }
                }
            }
        }
        Ok(ddl)
    }
}

impl Table {
    /// DDL for one table. Columns are declared without a type so SQLite
    /// keeps every value's storage class as bound (no affinity coercion).
    pub fn ddl(&self) -> String {
        let table = self;
        let mut sql = String::new();
        let columns: String = table
            .stored()
            .map(|column| format!(", {}", column_def(column)))
            .collect();
        let name = quote(&table.name);
        let history = quote(&format!("{}__h", table.name));
        // `_b` is the canonical branch key ('' for unbranched tables).
        // Rows are unique per (id, branch); branch views select by `_b`
        // first, so every index leads with it.
        sql.push_str(&format!(
            "CREATE TABLE {name} (rid INTEGER PRIMARY KEY, id BLOB NOT NULL, \
             _b BLOB NOT NULL DEFAULT X'', _seq INTEGER NOT NULL, \
             _del INTEGER NOT NULL DEFAULT 0{columns}, UNIQUE (id, _b));\n\
             CREATE INDEX {} ON {name}(_seq);\n\
             CREATE TABLE {history} (id BLOB NOT NULL, _b BLOB NOT NULL, _seq INTEGER NOT NULL, \
             _del INTEGER NOT NULL{columns}, PRIMARY KEY (id, _b, _seq)) WITHOUT ROWID;\n",
            quote(&format!("{}__by_seq", table.name)),
        ));
        let lead = if table.branch_by.is_empty() {
            ""
        } else {
            "_b, "
        };
        for (n, index) in table.indexes.iter().enumerate() {
            let keys: Vec<String> = index
                .iter()
                .map(|c| match elided_as(c) {
                    Some(created) => format!("COALESCE({}, {})", quote(c), quote(created)),
                    // The key expression queries order and range by.
                    None if table.column(c).is_some_and(crate::large::large_capable) => {
                        crate::large::key_sql(&quote(c))
                    }
                    None => quote(c),
                })
                .collect();
            sql.push_str(&format!(
                "CREATE INDEX {} ON {name}({lead}{}, id);\n",
                quote(&format!("{}__ix{n}", table.name)),
                keys.join(", ")
            ));
        }
        let large: Vec<&Column> = table
            .stored()
            .filter(|c| crate::large::large_capable(c))
            .collect();
        sql.push_str(&crate::large::triggers(table, &large));
        sql
    }
}

/// A stored column's definition: just its name. Columns carry no type
/// affinity or collation, so the file opens on any SQLite.
pub(crate) fn column_def(column: &Column) -> String {
    quote(&column.name)
}

/// A value as an SQL literal, for DDL defaults.
pub(crate) fn sql_literal(value: &Value) -> Result<String, Error> {
    let hex = |v: &[u8]| v.iter().map(|b| format!("{b:02x}")).collect::<String>();
    Ok(match to_sql(value)? {
        None => return Err(unsupported("a NaN default")),
        Some(Sql::Null) => "NULL".to_owned(),
        Some(Sql::Integer(v)) => v.to_string(),
        Some(Sql::Real(v)) => format!("{v:e}"),
        // A tagged bytes cell is TEXT that no SQL literal can spell (it
        // starts with NUL), and a column default must be a literal.
        Some(Sql::Text(v)) => match std::str::from_utf8(&v) {
            Ok(text) if !text.contains('\0') => format!("'{}'", text.replace('\'', "''")),
            _ => return Err(unsupported("a bytes default on an added column")),
        },
        Some(Sql::Blob(v)) => format!("X'{}'", hex(&v)),
    })
}

/// Canonical branch-key bytes for named branch-column values.
pub(crate) fn branch_key(values: &BTreeMap<String, crate::protocol::BranchColumnValue>) -> Vec<u8> {
    let mut key = Vec::new();
    for (name, value) in values {
        key.extend((name.len() as u32).to_be_bytes());
        key.extend(name.as_bytes());
        key.extend((value.0.len() as u32).to_be_bytes());
        key.extend(&value.0);
    }
    key
}

impl Table {
    /// The branch key a row with these cells lives under.
    pub(crate) fn row_branch_key(&self, cells: &BTreeMap<String, Value>) -> Vec<u8> {
        let mut values = BTreeMap::new();
        for column in &self.branch_by {
            let mut value = cells.get(column).cloned().unwrap_or(Value::Nullable(None));
            while let Value::Nullable(Some(inner)) = value {
                value = *inner;
            }
            values.insert(
                column.clone(),
                crate::protocol::BranchColumnValue::from(value),
            );
        }
        branch_key(&values)
    }

    /// Branch-column cells named by a selector, decoded by column type.
    pub(crate) fn selector_cells(
        &self,
        selector: &crate::protocol::BranchSelector,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let mut cells = BTreeMap::new();
        for column in &self.branch_by {
            let encoded = selector.values.get(column).ok_or_else(|| {
                query_error(format!("branch selector has no `{}.{column}`", self.name))
            })?;
            let ty = &self.column(column).expect("branch column").ty;
            let value = encoded
                .decode_as(ty)
                .map_err(|e| query_error(format!("branch selector `{column}`: {e:?}")))?;
            cells.insert(column.clone(), value);
        }
        Ok(cells)
    }

    /// The branch key of a selector for this table ('' when unbranched).
    pub(crate) fn selector_key(
        &self,
        selector: &crate::protocol::BranchSelector,
    ) -> Result<Vec<u8>, Error> {
        if self.branch_by.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.row_branch_key(&self.selector_cells(selector)?))
    }
}

pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A current row, mirroring the parts of `crate::node::CurrentRow` the
/// integration tests read: identity, deletion, and projected cells.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentRow {
    pub(crate) table: Arc<str>,
    pub(crate) id: RowUuid,
    pub(crate) seq: u64,
    pub(crate) deleted: bool,
    /// Projected cells in projection order. `None` is an unwritten cell.
    pub(crate) cells: Arc<Vec<(String, Option<Value>)>>,
    /// The declared type of each projected cell, shared by a read's rows.
    pub(crate) types: Arc<[ValueType]>,
}

impl CurrentRow {
    pub fn row_uuid(&self) -> RowUuid {
        self.id
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// The commit sequence number of this row version.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// A projected cell by column name.
    pub fn cell(&self, _table: &crate::schema::TableSchema, column: &str) -> Option<Value> {
        self.cells
            .iter()
            .find(|(name, _)| name == column)
            .and_then(|(_, value)| value.clone())
    }

    /// A projected cell by projection position.
    pub fn cell_at(&self, index: usize) -> Option<Value> {
        self.cells.get(index).and_then(|(_, value)| value.clone())
    }

    /// The declared type of each projected cell, in projection order.
    pub fn cell_types(&self) -> &[ValueType] {
        &self.types
    }

    /// Projected cells in projection order.
    pub fn cells(&self) -> impl Iterator<Item = (&str, Option<&Value>)> {
        self.cells
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_ref()))
    }
}
