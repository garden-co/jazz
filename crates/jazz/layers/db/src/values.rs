//! Jazz values in SQLite cells, the schema as SQLite sees it, and rows.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::ids::RowUuid;
use crate::options::{Error, ErrorCode};
use crate::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use crate::result_tree::{ResultNode, ResultRelation};
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
        Value::Array(items) => Sql::Text(array_json(items)?.to_string().into_bytes()),
        other => return Err(unsupported(format!("storing {other:?} in SQLite"))),
    }))
}

/// An array cell as JSON text, so plain SQLite reads it with `json_each`.
/// Elements are JSON scalars (uuids as their hyphenated strings, enum
/// tags as numbers) or nested arrays; equal arrays encode to equal text.
fn array_json(items: &[Value]) -> Result<serde_json::Value, Error> {
    items
        .iter()
        .map(element_json)
        .collect::<Result<Vec<_>, _>>()
        .map(serde_json::Value::Array)
}

fn element_json(item: &Value) -> Result<serde_json::Value, Error> {
    Ok(match item {
        Value::Nullable(None) => serde_json::Value::Null,
        Value::Nullable(Some(inner)) => element_json(inner)?,
        Value::U8(v) => (*v).into(),
        Value::U16(v) => (*v).into(),
        Value::U32(v) => (*v).into(),
        Value::U64(v) | Value::U48(v) => (*v).into(),
        Value::I32(v) => (*v).into(),
        Value::I64(v) => (*v).into(),
        Value::F64(v) => serde_json::Number::from_f64(*v)
            .ok_or_else(|| unsupported("storing NaN or infinity in an array"))?
            .into(),
        Value::Bool(v) => (*v).into(),
        Value::String(v) => v.clone().into(),
        Value::Uuid(v) => v.to_string().into(),
        Value::EnumTag(v) => (*v).into(),
        Value::Array(items) => array_json(items)?,
        other => {
            return Err(unsupported(format!(
                "storing {other:?} inside an array in SQLite"
            )));
        }
    })
}

/// A JSON array element back as a value of its declared type.
fn json_element(ty: &ValueType, json: &serde_json::Value) -> Option<Value> {
    use serde_json::Value as Json;
    Some(match (ty, json) {
        (ValueType::Nullable(_), Json::Null) => Value::Nullable(None),
        (ValueType::Nullable(inner), json) => {
            Value::Nullable(Some(Box::new(json_element(inner, json)?)))
        }
        (ValueType::U8, json) => Value::U8(json.as_u64()?.try_into().ok()?),
        (ValueType::U16, json) => Value::U16(json.as_u64()?.try_into().ok()?),
        (ValueType::U32, json) => Value::U32(json.as_u64()?.try_into().ok()?),
        (ValueType::U64, json) => Value::U64(json.as_u64()?),
        (ValueType::U48, json) => Value::U48(json.as_u64()?),
        (ValueType::I32, json) => Value::I32(json.as_i64()?.try_into().ok()?),
        (ValueType::I64, json) => Value::I64(json.as_i64()?),
        (ValueType::F64, json) => Value::F64(json.as_f64()?),
        (ValueType::Bool, Json::Bool(v)) => Value::Bool(*v),
        (ValueType::String, Json::String(v)) => Value::String(v.clone()),
        (ValueType::Uuid, Json::String(v)) => Value::Uuid(v.parse().ok()?),
        (ValueType::EnumTag(_), json) => Value::EnumTag(json.as_u64()?.try_into().ok()?),
        (ValueType::Array(element), Json::Array(items)) => Value::Array(
            items
                .iter()
                .map(|item| json_element(element, item))
                .collect::<Option<_>>()?,
        ),
        _ => return None,
    })
}

/// An array element bound for comparison with what `json_each` reads back
/// as its `value`: uuids are their hyphenated text there.
pub(crate) fn array_needle(sql: Sql) -> Sql {
    match sql {
        Sql::Blob(bytes) => match uuid::Uuid::from_slice(&bytes) {
            Ok(id) => Sql::Text(id.to_string().into_bytes()),
            Err(_) => Sql::Blob(bytes),
        },
        sql => sql,
    }
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
        // An author record, read as a JSON array of its fields (see
        // `compile::column_sql`).
        (ValueType::Record(descriptor), ValueRef::Text(cell)) => {
            let json = serde_json::from_slice(cell)
                .map_err(|e| Error::new(ErrorCode::Storage, format!("record cell: {e}")))?;
            return Ok(json_record(descriptor, &json));
        }
        (ValueType::Array(_), ValueRef::Text(cell)) => {
            let json = serde_json::from_slice(cell)
                .map_err(|e| Error::new(ErrorCode::Storage, format!("array cell: {e}")))?;
            return Ok(json_element(ty, &json));
        }
        _ => {}
    }
    Ok(decode_fixed(ty, cell))
}

/// A record from a JSON array of its fields in descriptor order: strings,
/// uuids as hex, nulls, and nested records.
fn json_record(
    descriptor: &crate::records::RecordDescriptor,
    json: &serde_json::Value,
) -> Option<Value> {
    use serde_json::Value as Json;
    fn field(ty: &ValueType, json: &Json) -> Option<Value> {
        Some(match (ty, json) {
            (ValueType::Nullable(_), Json::Null) => Value::Nullable(None),
            (ValueType::Nullable(inner), json) => {
                Value::Nullable(Some(Box::new(field(inner, json)?)))
            }
            (ValueType::String, Json::String(v)) => Value::String(v.clone()),
            (ValueType::Uuid, Json::String(v)) => Value::Uuid(v.parse().ok()?),
            (ValueType::Record(descriptor), json) => json_record(descriptor, json)?,
            _ => return None,
        })
    }
    let Json::Array(items) = json else {
        return None;
    };
    let fields = descriptor.fields();
    if items.len() != fields.len() {
        return None;
    }
    let values = fields
        .iter()
        .zip(items)
        .map(|(f, item)| field(&f.value_type, item))
        .collect::<Option<Vec<_>>>()?;
    let raw = descriptor.create(&values).ok()?;
    Some(Value::Record(crate::records::OwnedRecord::new(
        raw,
        *descriptor,
    )))
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
        // A transaction id, `(time, node)`: history's `$transactionId`, read
        // as 8 big-endian bytes of time then the node's 16.
        ValueType::Tuple(members)
            if matches!(members.as_slice(), [ValueType::U64, ValueType::Uuid]) =>
        {
            let (time, node) = cell.as_blob().ok()?.split_first_chunk::<8>()?;
            Value::Tuple(vec![
                Value::U64(u64::from_be_bytes(*time)),
                Value::Uuid(uuid::Uuid::from_slice(node).ok()?),
            ])
        }
        _ => return None,
    })
}

/// One application column.
#[derive(Clone, Debug)]
pub struct Column {
    /// The name this schema reads and writes it by.
    pub name: String,
    /// The stored column's name: the name it was created under. A rename
    /// lens changes `name` only, so every schema version reads one column.
    pub stored: String,
    pub ty: ValueType,
    pub default: Option<Value>,
    pub json: bool,
    /// How concurrent writes to the column merge at the authority.
    pub merge: crate::schema::MergeStrategy,
}

impl Column {
    pub fn nullable(&self) -> bool {
        matches!(self.ty, ValueType::Nullable(_))
    }
}

/// One application table.
#[derive(Clone, Debug)]
pub struct Table {
    /// The name this schema reads and writes it by.
    pub name: String,
    /// The stored table's name: the name it was created under (see
    /// [`Column::stored`]). SQL and every table-keyed bookkeeping use it.
    pub stored: String,
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

/// Provenance as read. Timestamps are Unix ms (u64, as in Jazz). Authors
/// read whole (`$createdBy`: account and identity), by identity, or by
/// part, so policies and filters can compare `$createdBy.account`,
/// `$createdBy.identity.subject` and friends.
pub(crate) const PROVENANCE: [&str; 12] = [
    "$createdAt",
    "$updatedAt",
    "$createdBy",
    "$createdBy.account",
    "$createdBy.identity",
    "$createdBy.identity.issuer",
    "$createdBy.identity.subject",
    "$updatedBy",
    "$updatedBy.account",
    "$updatedBy.identity",
    "$updatedBy.identity.issuer",
    "$updatedBy.identity.subject",
];

/// The current deletion of a row, readable with `includeDeleted`: the
/// deleting version's update stamp, NULL for live and restored rows. Never
/// stored: a deleted row is its current version with `_del` set, and that
/// version's update stamp is the deletion's.
pub(crate) const DELETION: [&str; 6] = [
    "$deletedAt",
    "$deletedBy",
    "$deletedBy.account",
    "$deletedBy.identity",
    "$deletedBy.identity.issuer",
    "$deletedBy.identity.subject",
];

/// Whether `column` names deletion metadata (`$deletedAt`, `$deletedBy...`).
pub(crate) fn is_deletion(column: &str) -> bool {
    column == "$deletedAt" || column == "$deletedBy" || column.starts_with("$deletedBy.")
}

/// Provenance as stored: each author is one code into `_jazz_authors`
/// instead of its account and two strings on every row version.
pub(crate) const STORED_PROVENANCE: [&str; 4] =
    ["$createdAt", "$updatedAt", "$createdBy", "$updatedBy"];

/// An author as interned in `_jazz_authors`: account, issuer, subject.
pub(crate) type AuthorKey = (uuid::Uuid, String, String);

/// A writer's durable attribution. Every row author has an account (the
/// `$createdBy` record's `account` is not nullable); a writer without one
/// (the system, an accountless principal) is attributed to the reserved
/// system account, as Jazz attributes system writes.
pub(crate) fn author_key(author: &crate::ids::AuthorSubject) -> AuthorKey {
    let (issuer, subject) = author.principal_parts();
    let account = author
        .account_id()
        .unwrap_or(jazz_types::account_registry::SYSTEM_ACCOUNT_ID);
    (account.0, issuer, subject)
}

/// The stored author column and `_jazz_authors` field behind a readable
/// provenance column, e.g. `$createdBy.identity.subject` -> (`$createdBy`,
/// `subject`).
pub(crate) fn author_part(column: &str) -> Option<(&'static str, &'static str)> {
    Some(match column {
        "$createdBy.account" => ("$createdBy", "account"),
        "$createdBy.identity.issuer" => ("$createdBy", "issuer"),
        "$createdBy.identity.subject" => ("$createdBy", "subject"),
        "$updatedBy.account" => ("$updatedBy", "account"),
        "$updatedBy.identity.issuer" => ("$updatedBy", "issuer"),
        "$updatedBy.identity.subject" => ("$updatedBy", "subject"),
        "$deletedBy.account" => ("$deletedBy", "account"),
        "$deletedBy.identity.issuer" => ("$deletedBy", "issuer"),
        "$deletedBy.identity.subject" => ("$deletedBy", "subject"),
        _ => return None,
    })
}

/// The stored author column behind a readable author record, and whether
/// it is the whole author (else its identity alone).
pub(crate) fn author_record(column: &str) -> Option<(&'static str, bool)> {
    Some(match column {
        "$createdBy" => ("$createdBy", true),
        "$createdBy.identity" => ("$createdBy", false),
        "$updatedBy" => ("$updatedBy", true),
        "$updatedBy.identity" => ("$updatedBy", false),
        "$deletedBy" => ("$deletedBy", true),
        "$deletedBy.identity" => ("$deletedBy", false),
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
            stored: (*name).to_owned(),
            ty: ValueType::Nullable(Box::new(ValueType::U64)),
            default: None,
            json: false,
            merge: Default::default(),
        })
        .collect()
}

fn unwrap_nullable_type(ty: &ValueType) -> &ValueType {
    match ty {
        ValueType::Nullable(inner) => inner,
        ty => ty,
    }
}

fn provenance_columns() -> Vec<Column> {
    PROVENANCE
        .iter()
        .chain(&DELETION)
        .map(|name| Column {
            name: (*name).to_owned(),
            stored: (*name).to_owned(),
            ty: ValueType::Nullable(Box::new(if name.ends_with("At") {
                ValueType::U64
            } else {
                unwrap_nullable_type(
                    crate::ids::AuthorSubject::metadata_type(name).expect("an author path"),
                )
                .clone()
            })),
            default: None,
            json: false,
            merge: Default::default(),
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

    /// The stored name of a readable column (`id` and provenance are
    /// stored by their own names).
    pub fn stored_name<'a>(&'a self, name: &'a str) -> &'a str {
        match self.columns.iter().find(|column| column.name == name) {
            Some(column) => &column.stored,
            None => name,
        }
    }

    /// An application or stored provenance column by its stored name.
    pub fn by_stored(&self, stored: &str) -> Option<&Column> {
        self.stored().find(|column| column.stored == stored)
    }

    /// This table as the store holds it: named by stored names throughout.
    pub(crate) fn physical(&self) -> Table {
        let mut table = self.clone();
        table.name = table.stored.clone();
        table.indexes = self
            .indexes
            .iter()
            .map(|index| self.stored_index(index))
            .collect();
        for column in &mut table.columns {
            column.name = column.stored.clone();
        }
        table.branch_by = self
            .branch_by
            .iter()
            .map(|column| self.stored_name(column).to_owned())
            .collect();
        table
    }

    /// An index's key list by stored column names.
    pub(crate) fn stored_index(&self, index: &[String]) -> Vec<String> {
        index
            .iter()
            .map(|column| self.stored_name(column).to_owned())
            .collect()
    }

    pub fn has_column(&self, name: &str) -> bool {
        name == "id" || self.column(name).is_some()
    }

    /// An inherited capability concerns the accepted old row. UPDATE's CHECK
    /// validates a replacement, so a CHECK-only policy declares an unrestricted
    /// old-row capability; absence of the entire operation still denies.
    pub fn inherited_policy(&self, operation: crate::tools::Operation) -> Option<&PolicyExpr> {
        if operation == crate::tools::Operation::Update {
            return match &self.policies.update.using {
                Some(policy) => Some(policy),
                None if self.policies.update.with_check.is_some() => Some(&PolicyExpr::True),
                None => None,
            };
        }
        self.policy(operation)
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
    /// The schema whose policies govern this one's tables, when they are
    /// another's: a sync session on an older version is checked against
    /// the current schema's policies, compiled under its names (tables
    /// match by stored name; see [`crate::compile::Ctx`]).
    pub policies_from: Option<std::rc::Rc<Schema>>,
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
                    stored: column.name.clone(),
                    ty: column.column_type.clone(),
                    default: column.default.clone(),
                    json: format!("{:?}", column.large_value_kind) == "Json",
                    merge: table
                        .merge_strategies
                        .get(&column.name)
                        .copied()
                        .unwrap_or_default(),
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
                    stored: table.name.clone(),
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
            policies_from: None,
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

    /// `jazz` with each table and column stored under the name `names`
    /// gives it; ones `names` does not list are stored by their own names.
    pub fn stored_as(jazz: &JazzSchema, names: &StoredNames) -> Self {
        let mut schema = Self::new(jazz);
        for table in schema.tables.values_mut() {
            let Some((stored, columns)) = names.tables.get(&table.name) else {
                continue;
            };
            table.stored = stored.clone();
            for column in &mut table.columns {
                if let Some(stored) = columns.get(&column.name) {
                    column.stored = stored.clone();
                }
            }
        }
        schema
    }

    /// Where this schema stores each of its tables and columns.
    pub fn names(&self) -> StoredNames {
        StoredNames {
            tables: self
                .tables
                .values()
                .map(|table| {
                    let columns = table
                        .columns
                        .iter()
                        .map(|column| (column.name.clone(), column.stored.clone()))
                        .collect();
                    (table.name.clone(), (table.stored.clone(), columns))
                })
                .collect(),
        }
    }

    /// `self` governed by `current`'s policies: each table takes the
    /// policies of `current`'s table with the same stored name (none, so
    /// deny, when `current` no longer has it).
    pub fn with_policies_of(mut self, current: std::rc::Rc<Schema>) -> Self {
        for table in self.tables.values_mut() {
            table.policies = current
                .by_stored(&table.stored)
                .map(|table| table.policies.clone())
                .unwrap_or_default();
        }
        self.policies_from = Some(current);
        self
    }

    /// The schema whose names this schema's policies use.
    pub fn policy_schema(&self) -> &Schema {
        self.policies_from.as_deref().unwrap_or(self)
    }

    /// The table stored as `stored`.
    pub fn by_stored(&self, stored: &str) -> Option<&Table> {
        match self.tables.get(stored) {
            Some(table) if table.stored == stored => Some(table),
            _ => self.tables.values().find(|table| table.stored == stored),
        }
    }

    /// The stored name of table `name`.
    pub fn stored_table(&self, name: &str) -> Result<&str, Error> {
        Ok(&self.table(name)?.stored)
    }

    /// The physical store for `self` plus `other`: tables and columns are
    /// unioned by their stored names, so every schema view reads and writes
    /// one set of rows. `self` is keyed and named by stored names. Returns
    /// the DDL that brings an existing store up to the union: new tables,
    /// `ADD COLUMN ... DEFAULT` for new columns (an O(1) schema change; old
    /// rows read the default without being rewritten) and new indexes.
    pub fn union(&mut self, other: &Schema) -> Result<String, Error> {
        let mut ddl = String::new();
        for table in other.tables.values() {
            let name = &table.stored;
            match self.tables.get_mut(name) {
                None => {
                    ddl.push_str(&table.ddl());
                    self.tables.insert(name.clone(), table.physical());
                }
                Some(physical) => {
                    let mut large = Vec::new();
                    for column in &table.columns {
                        if physical.column(&column.stored).is_some() {
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
                        let mut stored = column.clone();
                        stored.name = column.stored.clone();
                        if crate::large::large_capable(column) {
                            large.push(stored.clone());
                        }
                        physical.columns.push(stored);
                    }
                    if let Some(first) = large.first() {
                        let columns: Vec<&Column> = large.iter().collect();
                        ddl.push_str(&crate::large::triggers(
                            physical,
                            &columns,
                            Some(&first.stored),
                        ));
                    }
                    for index in &table.indexes {
                        let index = table.stored_index(index);
                        if !physical.indexes.contains(&index) {
                            ddl.push_str(&physical.index_ddl(&index));
                            physical.indexes.push(index);
                        }
                    }
                    // Also install the additive derived index on stores created
                    // before scoped historical permission reads were available.
                    ddl.push_str(&table.permission_history_indexes_ddl());
                }
            }
        }
        Ok(ddl)
    }
}

/// Where one schema stores its tables and columns: logical table ->
/// (stored table, logical column -> stored column).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoredNames {
    pub tables: BTreeMap<String, (String, BTreeMap<String, String>)>,
}

impl Table {
    /// A table known only by its stored column names, as a reopened store
    /// lists it. Enough to extend it and to copy whole rows.
    pub(crate) fn stored_only(name: String, columns: Vec<String>) -> Self {
        Self {
            stored: name.clone(),
            name,
            columns: columns
                .into_iter()
                .filter(|column| !STORED_PROVENANCE.contains(&column.as_str()))
                .map(|name| Column {
                    stored: name.clone(),
                    name,
                    ty: ValueType::Nullable(Box::new(ValueType::U64)),
                    default: None,
                    json: false,
                    merge: Default::default(),
                })
                .collect(),
            references: BTreeMap::new(),
            policies: TablePolicies::default(),
            indexes: Vec::new(),
            branch_by: Vec::new(),
            provenance: provenance_columns(),
            stored_provenance: stored_provenance_columns(),
        }
    }

    /// One index. Its name is its stored key list, so re-creating it on a
    /// store that already has it (under any schema version) is a no-op.
    pub fn index_ddl(&self, index: &[String]) -> String {
        let table = self;
        let lead = if table.branch_by.is_empty() {
            ""
        } else {
            "_b, "
        };
        let index = table.stored_index(index);
        let keys: Vec<String> = index
            .iter()
            .map(|c| match elided_as(c) {
                Some(created) => format!("COALESCE({}, {})", quote(c), quote(created)),
                // The key expression queries order and range by.
                None if table.by_stored(c).is_some_and(crate::large::large_capable) => {
                    crate::large::key_sql(&quote(c))
                }
                None => quote(c),
            })
            .collect();
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}({lead}{}, id);\n",
            quote(&format!("{}__ix({})", table.stored, index.join(","))),
            quote(&table.stored),
            keys.join(", ")
        )
    }

    /// jazz.sqlite.permission-history-index.v1: a derived UUID lookup over
    /// historical images. The row bytes and primary key are unchanged.
    pub(crate) fn permission_history_index_name(&self, column: &str) -> Option<String> {
        if !self.branch_by.is_empty()
            || !self
                .indexes
                .iter()
                .any(|index| index.first().is_some_and(|first| first == column))
        {
            return None;
        }
        let field = self.column(column)?;
        let ty = match &field.ty {
            ValueType::Nullable(inner) => inner.as_ref(),
            ty => ty,
        };
        matches!(ty, ValueType::Uuid)
            .then(|| format!("{}__h_permission_v1({})", self.stored, field.stored))
    }

    fn permission_history_indexes_ddl(&self) -> String {
        self.indexes
            .iter()
            .filter_map(|index| index.first())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|column| {
                self.permission_history_index_name(column)
                    .map(|name| (column, name))
            })
            .map(|(column, name)| {
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}({},id,_b,_seq);\n",
                    quote(&name),
                    quote(&format!("{}__h", self.stored)),
                    quote(self.stored_name(column))
                )
            })
            .collect()
    }

    /// DDL for one table. Columns are declared without a type so SQLite
    /// keeps every value's storage class as bound (no affinity coercion).
    pub fn ddl(&self) -> String {
        let table = self;
        let mut sql = String::new();
        let columns: String = table
            .stored()
            .map(|column| format!(", {}", column_def(column)))
            .collect();
        let name = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        // `_b` is the canonical branch key ('' for unbranched tables).
        // Rows are unique per (id, branch); branch views select by `_b`
        // first, so every index leads with it. `_c` is 1 when the version's
        // commit wrote content (not only a delete or a restore): history
        // reads list those versions.
        sql.push_str(&format!(
            "CREATE TABLE {name} (rid INTEGER PRIMARY KEY, id BLOB NOT NULL, \
             _b BLOB NOT NULL DEFAULT X'', _seq INTEGER NOT NULL, \
             _del INTEGER NOT NULL DEFAULT 0, _c INTEGER NOT NULL DEFAULT 1{columns}, \
             UNIQUE (id, _b));\n\
             CREATE INDEX {} ON {name}(_seq);\n\
             CREATE TABLE {history} (id BLOB NOT NULL, _b BLOB NOT NULL, _seq INTEGER NOT NULL, \
             _del INTEGER NOT NULL, _c INTEGER NOT NULL DEFAULT 1{columns}, \
             PRIMARY KEY (id, _b, _seq)) WITHOUT ROWID;\n",
            quote(&format!("{}__by_seq", table.stored)),
        ));
        for index in &table.indexes {
            sql.push_str(&table.index_ddl(index));
        }
        sql.push_str(&table.permission_history_indexes_ddl());
        let large: Vec<&Column> = table
            .stored()
            .filter(|c| crate::large::large_capable(c))
            .collect();
        sql.push_str(&crate::large::triggers(table, &large, None));
        sql
    }
}

/// A stored column's definition: just its stored name. Columns carry no
/// type affinity or collation, so the file opens on any SQLite.
pub(crate) fn column_def(column: &Column) -> String {
    quote(&column.stored)
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

/// The branch columns of a branch key, as [`branch_key`] encoded them.
fn branch_key_values(key: &[u8]) -> BTreeMap<String, crate::protocol::BranchColumnValue> {
    let mut values = BTreeMap::new();
    let mut rest = key;
    let take = |rest: &mut &[u8]| -> Option<Vec<u8>> {
        let len = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?) as usize;
        let bytes = rest.get(4..4 + len)?.to_vec();
        *rest = &rest[4 + len..];
        Some(bytes)
    };
    while !rest.is_empty() {
        let (Some(name), Some(value)) = (take(&mut rest), take(&mut rest)) else {
            break;
        };
        values.insert(
            String::from_utf8_lossy(&name).into_owned(),
            crate::protocol::BranchColumnValue(value),
        );
    }
    values
}

impl Table {
    /// The branch key a row under `key` would move to if `cells` were
    /// written over it: its branch columns, those in `cells` replaced.
    pub(crate) fn branch_key_after(&self, key: &[u8], cells: &BTreeMap<String, Value>) -> Vec<u8> {
        let mut values = branch_key_values(key);
        for column in &self.branch_by {
            if let Some(value) = cells.get(column) {
                let mut value = value.clone();
                while let Value::Nullable(Some(inner)) = value {
                    value = *inner;
                }
                values.insert(
                    column.clone(),
                    crate::protocol::BranchColumnValue::from(value),
                );
            }
        }
        branch_key(&values)
    }

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
    /// A flat join's joined source rows in declared order, after the root
    /// (`id`): with it, the row's occurrence. Empty for other reads.
    pub(crate) joined: Arc<[JoinedRow]>,
    /// A flat join's root version when the root is a history source.
    pub(crate) version: Option<SourceVersion>,
    /// A retained UNION leaf at root source position zero.
    pub(crate) union_arm: Option<Arc<str>>,
}

/// A history version's transaction identity, `(tx, node)` as its
/// `$transactionId` reads.
pub type SourceVersion = (u64, crate::ids::NodeUuid);

/// A row's result key: the row itself, or a flat join's root followed by
/// its joined source rows, each with its history version if it has one.
/// Relations arrive as nested rows, not joined outputs.
pub fn occurrence_key(
    root: RowUuid,
    version: Option<SourceVersion>,
    joined: &[JoinedRow],
) -> jazz_types::object::ResultKey {
    occurrence_key_with_union_arm(root, version, joined, None)
}

/// The existing result-key v2 encoding, including a UNION leaf discriminator.
pub fn occurrence_key_with_union_arm(
    root: RowUuid,
    version: Option<SourceVersion>,
    joined: &[JoinedRow],
    union_arm: Option<&str>,
) -> jazz_types::object::ResultKey {
    use jazz_types::object::{ObjectId, OutputOccurrenceId, ResultKey, SourceOccurrenceId};
    let source = |row: RowUuid, version: Option<SourceVersion>| {
        let id = ObjectId::from_uuid(row.0);
        match version {
            Some((tx, node)) => SourceOccurrenceId::history(id, jazz_types::time::TxTime(tx), node),
            None => SourceOccurrenceId::current(id),
        }
    };
    if joined.is_empty() && version.is_none() && union_arm.is_none() {
        return ResultKey::from_occurrence(OutputOccurrenceId::single_source(ObjectId::from_uuid(
            root.0,
        )));
    }
    ResultKey::from_occurrence(
        OutputOccurrenceId::from_source_occurrences(
            source(root, version),
            joined
                .iter()
                .map(|joined| source(joined.row, joined.version)),
            union_arm.map(|label| (0, label.to_owned())),
        )
        .expect("a compiled occurrence has a validated union label"),
    )
}

/// One joined source row of a flat-join occurrence: the row, and its
/// version when the source is a history relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinedRow {
    pub row: RowUuid,
    pub version: Option<SourceVersion>,
}

impl CurrentRow {
    /// The UNION leaf label; nested labels use the relation path encoding.
    pub fn union_arm(&self) -> Option<&str> {
        self.union_arm.as_deref()
    }

    pub fn row_uuid(&self) -> RowUuid {
        self.id
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// A flat join's joined source rows in declared order (the root is
    /// [`Self::row_uuid`]); empty for other reads.
    pub fn joined_rows(&self) -> &[JoinedRow] {
        &self.joined
    }

    /// A flat join's root version, when its root is a history source.
    pub fn root_version(&self) -> Option<SourceVersion> {
        self.version
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

impl ResultNode {
    /// The node's row with each relation appended as a cell under its
    /// column name: an array of records, each the child's `row_uuid`
    /// followed by its own cells (relations nested the same way). This is
    /// the shape the bindings publish for relation reads.
    pub fn into_nested_row(self) -> Result<CurrentRow, Error> {
        let ResultNode { row, relations } = self;
        if relations.is_empty() {
            return Ok(row);
        }
        let mut cells = (*row.cells).clone();
        let mut types = row.types.to_vec();
        for (name, relation) in relations {
            let children = match relation {
                ResultRelation::Array(children) => children,
                ResultRelation::Null | ResultRelation::Hole => Vec::new(),
            };
            let children = children
                .into_iter()
                .map(ResultNode::into_nested_row)
                .collect::<Result<Vec<_>, _>>()?;
            let (element, values) = child_records(&children)?;
            cells.push((name, Some(Value::Array(values))));
            types.push(ValueType::Array(Box::new(element)));
        }
        Ok(CurrentRow {
            cells: Arc::new(cells),
            types: types.into(),
            ..row
        })
    }
}

/// One relation's children as records under a shared descriptor. A field
/// is nullable when its declared type is, or when any child left it
/// unwritten. A nested relation's type is its siblings' widest: an empty
/// relation names no fields, and a child's own unwritten cells widen only
/// its records, so each child's value is re-encoded under the shared type.
fn child_records(children: &[CurrentRow]) -> Result<(ValueType, Vec<Value>), Error> {
    let mut fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
    if let Some(first) = children.first() {
        for (index, ((name, _), ty)) in first.cells().zip(first.cell_types()).enumerate() {
            let unwritten = children
                .iter()
                .any(|child| child.cells.get(index).is_some_and(|(_, v)| v.is_none()));
            // A nested relation's records: the type its non-empty values share.
            let mut shared: Option<ValueType> = None;
            for child in children
                .iter()
                .filter(|child| !relation_is_empty(child, index))
            {
                if let (Some((_, Some(Value::Array(_)))), Some(own)) =
                    (child.cells.get(index), child.types.get(index))
                {
                    shared = Some(match shared {
                        None => own.clone(),
                        Some(ty) => widen(&ty, own)?,
                    });
                }
            }
            let ty = shared.unwrap_or_else(|| ty.clone());
            let ty = match ty {
                ValueType::Nullable(_) => ty,
                _ if unwritten => ValueType::Nullable(Box::new(ty)),
                _ => ty,
            };
            fields.push((name.to_owned(), ty));
        }
    }
    let descriptor = RecordDescriptor::new(fields.clone());
    let mut values = Vec::with_capacity(children.len());
    for child in children {
        if child.cells.len() + 1 != fields.len() {
            return Err(Error::new(
                ErrorCode::Query,
                "relation children with different projections",
            ));
        }
        let mut record = vec![Value::Uuid(child.id.0)];
        for ((_, value), (_, ty)) in child.cells.iter().zip(&fields[1..]) {
            record.push(match value {
                None => Value::Nullable(None),
                Some(value) => coerce(value, ty)?,
            });
        }
        let raw = descriptor
            .create(&record)
            .map_err(|error| Error::new(ErrorCode::Query, format!("relation record: {error}")))?;
        values.push(Value::Record(OwnedRecord::new(raw, descriptor)));
    }
    Ok((ValueType::Record(Box::new(descriptor)), values))
}

/// Whether a child's cell at `index` is an empty nested relation.
fn relation_is_empty(child: &CurrentRow, index: usize) -> bool {
    matches!(child.cells.get(index), Some((_, Some(Value::Array(items)))) if items.is_empty())
}

/// The narrowest type both relation types' values fit: nullable if either
/// is, record fields widened pairwise.
fn widen(a: &ValueType, b: &ValueType) -> Result<ValueType, Error> {
    if a == b {
        return Ok(a.clone());
    }
    Ok(match (a, b) {
        (ValueType::Nullable(x), ValueType::Nullable(y)) => {
            ValueType::Nullable(Box::new(widen(x, y)?))
        }
        (ValueType::Nullable(x), y) | (y, ValueType::Nullable(x)) => {
            ValueType::Nullable(Box::new(widen(x, y)?))
        }
        (ValueType::Array(x), ValueType::Array(y)) => ValueType::Array(Box::new(widen(x, y)?)),
        (ValueType::Record(x), ValueType::Record(y))
            if x.fields().len() == y.fields().len()
                && x.fields()
                    .iter()
                    .zip(y.fields())
                    .all(|(f, g)| f.name == g.name) =>
        {
            let fields = x
                .fields()
                .iter()
                .zip(y.fields())
                .map(|(f, g)| {
                    Ok((
                        f.name.clone().unwrap_or_default(),
                        widen(&f.value_type, &g.value_type)?,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            ValueType::Record(Box::new(RecordDescriptor::new(fields)))
        }
        // An empty relation's records name only their row id.
        (ValueType::Record(x), other @ ValueType::Record(_))
        | (other @ ValueType::Record(_), ValueType::Record(x))
            if x.fields().len() == 1 =>
        {
            other.clone()
        }
        _ => {
            return Err(Error::new(
                ErrorCode::Query,
                "relation children with different projections",
            ));
        }
    })
}

/// `value` re-encoded as `ty`, a type [`widen`] produced from its own.
fn coerce(value: &Value, ty: &ValueType) -> Result<Value, Error> {
    Ok(match (value, ty) {
        (Value::Nullable(None), _) => Value::Nullable(None),
        (Value::Nullable(Some(inner)), ValueType::Nullable(t)) => {
            Value::Nullable(Some(Box::new(coerce(inner, t)?)))
        }
        (value, ValueType::Nullable(t)) => Value::Nullable(Some(Box::new(coerce(value, t)?))),
        (Value::Array(items), ValueType::Array(t)) => Value::Array(
            items
                .iter()
                .map(|item| coerce(item, t))
                .collect::<Result<_, _>>()?,
        ),
        (Value::Record(record), ValueType::Record(descriptor))
            if record.descriptor() != descriptor.as_ref() =>
        {
            let values = record.to_values().map_err(|error| {
                Error::new(ErrorCode::Query, format!("relation record: {error}"))
            })?;
            let values = values
                .iter()
                .zip(descriptor.fields())
                .map(|(value, field)| coerce(value, &field.value_type))
                .collect::<Result<Vec<_>, _>>()?;
            let raw = descriptor.create(&values).map_err(|error| {
                Error::new(ErrorCode::Query, format!("relation record: {error}"))
            })?;
            Value::Record(OwnedRecord::new(raw, **descriptor))
        }
        (value, _) => value.clone(),
    })
}
