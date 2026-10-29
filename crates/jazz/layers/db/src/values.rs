//! Jazz values in SQLite cells, the schema as SQLite sees it, and rows.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
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

/// One-row queries through the connection's statement cache. jazz-db asks
/// the same few shapes per row (a row's state, an interned author), so
/// re-parsing each one costs more than running it.
pub(crate) trait CachedRow {
    fn cached_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>;
}

impl CachedRow for rusqlite::Connection {
    fn cached_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.prepare_cached(sql)?.query_row(params, f)
    }
}

pub(crate) fn storage(error: rusqlite::Error) -> Error {
    // Only the large-value collation and functions interrupt a statement,
    // when a chunk it needs is not local (see `crate::large`).
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted) {
        return crate::large::missing();
    }
    Error::new(ErrorCode::Storage, error.to_string())
}

/// Open `SAVEPOINT name`; [`end_savepoint`] closes it.
pub(crate) fn begin_savepoint(conn: &rusqlite::Connection, name: &str) -> Result<(), Error> {
    conn.execute_batch(&format!("SAVEPOINT {name}"))
        .map_err(storage)?;
    TAG_UNDO.with(|undo| {
        undo.borrow_mut()
            .entry(undo_key(conn))
            .or_default()
            .push(Vec::new())
    });
    Ok(())
}

thread_local! {
    /// By connection: each open savepoint's undo log, innermost last: the
    /// enum columns whose tags changed inside it, as they were before.
    static TAG_UNDO: RefCell<std::collections::HashMap<usize, Vec<Vec<(ColumnTags, EnumTags)>>>> =
        RefCell::default();
}

/// A connection's key in [`TAG_UNDO`]: its SQLite handle, which moving the
/// `Connection` keeps.
fn undo_key(conn: &rusqlite::Connection) -> usize {
    // SAFETY: the handle is only compared, never dereferenced.
    unsafe { conn.handle() as usize }
}

/// Stored enum column `tags` is about to change on `conn`: the innermost
/// open savepoint logs how it was, once, to put back if it rolls back.
pub(crate) fn tags_changing(conn: &rusqlite::Connection, tags: &ColumnTags) {
    TAG_UNDO.with(|undo| {
        if let Some(frame) = undo
            .borrow_mut()
            .get_mut(&undo_key(conn))
            .and_then(|frames| frames.last_mut())
            && !frame.iter().any(|(logged, _)| Rc::ptr_eq(logged, tags))
        {
            frame.push((tags.clone(), tags.borrow().clone()));
        }
    });
}

/// Close `conn`'s innermost savepoint's undo log: kept, its entries join
/// the enclosing savepoint's (the outermost's are committed); rolled back,
/// its columns' tags are put back. `all` closes every open log (SQLite
/// rolled the whole transaction back).
fn close_tag_undo(conn: &rusqlite::Connection, kept: bool, all: bool) {
    TAG_UNDO.with(|undo| {
        let mut undo = undo.borrow_mut();
        let key = undo_key(conn);
        let Some(frames) = undo.get_mut(&key) else {
            return;
        };
        let count = if all { frames.len() } else { 1 };
        for _ in 0..count {
            let Some(frame) = frames.pop() else {
                break;
            };
            match (kept, frames.last_mut()) {
                (true, Some(parent)) => {
                    for (tags, before) in frame {
                        if !parent.iter().any(|(logged, _)| Rc::ptr_eq(logged, &tags)) {
                            parent.push((tags, before));
                        }
                    }
                }
                (true, None) => {}
                (false, _) => {
                    for (tags, before) in frame.into_iter().rev() {
                        *tags.borrow_mut() = before;
                    }
                }
            }
        }
        if frames.is_empty() {
            undo.remove(&key);
        }
    });
}

/// Release `SAVEPOINT name` (`keep`), or roll back everything since it. SQLite
/// rolls a whole transaction back by itself after some errors (a full disk,
/// an I/O error); then the savepoint is gone and there is nothing to undo.
pub(crate) fn end_savepoint(
    conn: &rusqlite::Connection,
    name: &str,
    keep: bool,
) -> Result<(), Error> {
    let sql = if keep {
        format!("RELEASE {name}")
    } else {
        format!("ROLLBACK TO {name}; RELEASE {name}")
    };
    match conn.execute_batch(&sql) {
        Ok(()) => {
            close_tag_undo(conn, keep, false);
            Ok(())
        }
        Err(error) => {
            // In autocommit mode SQLite already rolled everything back.
            close_tag_undo(conn, false, conn.is_autocommit());
            if !keep && conn.is_autocommit() {
                Ok(())
            } else {
                Err(storage(error))
            }
        }
    }
}

/// Run `f` inside `SAVEPOINT name`: kept if it succeeds, undone if not.
pub(crate) fn in_savepoint<T>(
    conn: &rusqlite::Connection,
    name: &str,
    f: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    begin_savepoint(conn, name)?;
    let result = f();
    end_savepoint(conn, name, result.is_ok())?;
    result
}

/// One SQLite value, as bound or read. Text holds raw bytes, which SQLite
/// stores and compares without interpreting.
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
        Value::Bytes(v) => Sql::Blob(v.clone()),
        // A chunked cell names an epoch, which only a store has: descriptors
        // are stored through `Inner::large_cell`.
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

/// Decode column `column`'s cell (see [`cell_from_sql`]).
pub(crate) fn column_from_sql(
    column: &Column,
    cell: ValueRef<'_>,
    chunks: &crate::large::Chunks,
) -> Result<Option<Value>, Error> {
    cell_from_sql(&column.ty, column.tags.as_ref(), cell, chunks)
}

/// Decode a cell of type `ty`: an enum column's (with `tags`) as its
/// variant names, any other as [`from_sql`] does.
pub(crate) fn cell_from_sql(
    ty: &ValueType,
    tags: Option<&ColumnTags>,
    cell: ValueRef<'_>,
    chunks: &crate::large::Chunks,
) -> Result<Option<Value>, Error> {
    match tags {
        Some(tags) => enum_value(&tags.borrow(), ty, cell),
        None => from_sql(ty, cell, chunks),
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
        (ValueType::String, ValueRef::Text(_) | ValueRef::Integer(_)) => {
            return Ok(String::from_utf8(chunks.materialize(cell)?)
                .ok()
                .map(Value::String));
        }
        (ValueType::Bytes, ValueRef::Blob(_) | ValueRef::Integer(_)) => {
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
    /// How a column that holds JSON (a JSON column or an array of them)
    /// checks the values written to it; `None` for any other column.
    pub json_type: Option<Rc<JsonCheck>>,
    /// An enum column (or an array of enum values): the variants this
    /// schema declares. Its values are variant names everywhere above
    /// SQLite; its cells hold their tags (see [`EnumTags`]).
    pub variants: Option<Rc<[String]>>,
    /// The stored column's tags, shared by every schema storing it; set
    /// with `variants`.
    pub tags: Option<ColumnTags>,
    /// How concurrent writes to the column merge at the authority.
    pub merge: crate::schema::MergeStrategy,
}

impl Column {
    pub fn nullable(&self) -> bool {
        matches!(self.ty, ValueType::Nullable(_))
    }

    /// Whether the column holds enum values.
    pub fn is_enum(&self) -> bool {
        self.variants.is_some()
    }

    /// The type the store records for the stored column
    /// (`_jazz_stored_columns`): an enum column's names are recorded as an
    /// enum, so a stored column never changes between text and enum cells.
    pub(crate) fn stored_type(&self) -> ValueType {
        fn mark(ty: &ValueType) -> ValueType {
            match ty {
                ValueType::Nullable(inner) => ValueType::Nullable(Box::new(mark(inner))),
                ValueType::Array(inner) => ValueType::Array(Box::new(mark(inner))),
                ValueType::String => ValueType::EnumTag(
                    crate::records::ScalarEnumSchema::new(ENUM_MARK, Vec::<String>::new())
                        .expect("an empty enum schema is valid"),
                ),
                other => other.clone(),
            }
        }
        if self.is_enum() {
            mark(&self.ty)
        } else {
            self.ty.clone()
        }
    }

    /// SQL reading this enum column's `cell` as its rank in this schema's
    /// declaration order, where that is not tag order (a schema other than
    /// the current one, see [`EnumTags`]); `None` where the cell itself
    /// sorts so. An array of enum values orders as its cell does.
    pub(crate) fn rank_sql(&self, cell: &str) -> Option<String> {
        let (Some(variants), Some(tags)) = (&self.variants, &self.tags) else {
            return None;
        };
        let tags = tags.borrow();
        (!is_array_type(&self.ty) && !tags.in_order(variants))
            .then(|| tags.rank_sql(variants, cell))
    }

    /// SQL reading a rank [`Self::rank_sql`] made back as this column's tag.
    pub(crate) fn unrank_sql(&self, rank: &str) -> String {
        match (&self.variants, &self.tags) {
            (Some(variants), Some(tags)) => tags.borrow().unrank_sql(variants, rank),
            _ => rank.to_owned(),
        }
    }

    /// `value` as this column's cell sorts in this schema's order: an enum
    /// variant as its rank where [`Self::rank_sql`] ranks, else the cell.
    pub(crate) fn order_cell(&self, value: &Value) -> Result<Option<Sql>, Error> {
        let (Some(variants), Some(tags)) = (&self.variants, &self.tags) else {
            return self.cell_sql(value);
        };
        let tags = tags.borrow();
        if is_array_type(&self.ty) || tags.in_order(variants) {
            return enum_cell(&tags, value);
        }
        let mut inner = value;
        while let Value::Nullable(Some(value)) = inner {
            inner = value;
        }
        match inner {
            Value::String(name) => Ok(tags.rank(variants, name).map(Sql::Integer)),
            _ => enum_cell(&tags, value),
        }
    }

    /// Variant `name`'s rank in this schema's declaration order.
    pub(crate) fn rank(&self, name: &str) -> Option<i64> {
        let (variants, tags) = (self.variants.as_ref()?, self.tags.as_ref()?);
        tags.borrow().rank(variants, name)
    }

    /// A value as the cell this column stores: an enum variant as its tag
    /// (see [`EnumTags`]). A name with no tag (only a filter's can lack
    /// one: writes intern theirs first) stays a name and matches no cell.
    pub(crate) fn cell_sql(&self, value: &Value) -> Result<Option<Sql>, Error> {
        match &self.tags {
            Some(tags) => enum_cell(&tags.borrow(), value),
            None => to_sql(value),
        }
    }
}

/// The name an enum column's recorded type carries (see
/// [`Column::stored_type`]).
const ENUM_MARK: &str = "jazz.enum";

/// Whether a recorded type ([`Column::stored_type`]) is an enum column's.
pub(crate) fn is_enum_type(ty: &ValueType) -> bool {
    match ty {
        ValueType::Nullable(inner) | ValueType::Array(inner) => is_enum_type(inner),
        ValueType::EnumTag(schema) => schema.name == ENUM_MARK,
        _ => false,
    }
}

/// Whether `ty` (or the type a nullable wraps) is an array.
pub(crate) fn is_array_type(ty: &ValueType) -> bool {
    match ty {
        ValueType::Nullable(inner) => is_array_type(inner),
        ValueType::Array(_) => true,
        _ => false,
    }
}

/// One stored enum column's tags: each variant name a version storing the
/// column declared, with the integer its cells store it as (SPEC 10.8).
///
/// Tags are a store's own, like interned strings: values travel and are
/// compared by name everywhere above SQLite, and no other store sees a
/// tag. A store gives each new variant the next tag when it admits a
/// version declaring it, and renumbers a column's tags when a schema made
/// current declares its variants in another order, so the current
/// schema's declaration order is tag order: its `ORDER BY` and ranges
/// use the raw cell (and its index). A schema declaring another order
/// reads cells through its rank ([`Self::rank_sql`]), unindexed. A
/// removed variant keeps its tag and sorts after every declared one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnumTags {
    by_name: BTreeMap<String, i64>,
    by_tag: BTreeMap<i64, String>,
}

impl EnumTags {
    pub fn tag(&self, name: &str) -> Option<i64> {
        self.by_name.get(name).copied()
    }

    pub fn name(&self, tag: i64) -> Option<&str> {
        self.by_tag.get(&tag).map(String::as_str)
    }

    /// The tag the next new variant gets: tags are never reused.
    pub(crate) fn next(&self) -> i64 {
        self.by_tag.keys().next_back().map_or(0, |tag| tag + 1)
    }

    pub(crate) fn insert(&mut self, name: String, tag: i64) {
        self.by_tag.insert(tag, name.clone());
        self.by_name.insert(name, tag);
    }

    /// Every (tag, name), in tag order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (i64, &str)> {
        self.by_tag.iter().map(|(tag, name)| (*tag, name.as_str()))
    }

    /// Whether `declared` is in tag order: each variant's tag is its
    /// position, as the current schema's are.
    pub(crate) fn in_order(&self, declared: &[String]) -> bool {
        declared
            .iter()
            .enumerate()
            .all(|(at, name)| self.tag(name) == Some(at as i64))
    }

    /// Every tag in `declared`'s order: its declared variants', then every
    /// other tag's, in tag order. A tag's position is its rank.
    pub(crate) fn ranked(&self, declared: &[String]) -> Vec<i64> {
        let mut ranked: Vec<i64> = declared.iter().filter_map(|name| self.tag(name)).collect();
        let rest: Vec<i64> = self
            .by_tag
            .keys()
            .filter(|tag| !ranked.contains(tag))
            .copied()
            .collect();
        ranked.extend(rest);
        ranked
    }

    /// `name`'s rank in `declared`'s order (see [`Self::ranked`]).
    pub(crate) fn rank(&self, declared: &[String], name: &str) -> Option<i64> {
        let tag = self.tag(name)?;
        let ranked = self.ranked(declared);
        ranked.iter().position(|t| *t == tag).map(|at| at as i64)
    }

    /// SQL reading `cell` (this column's) as its rank in `declared`'s
    /// order. A tag interned after the SQL was built ranks after every
    /// other, by tag.
    pub(crate) fn rank_sql(&self, declared: &[String], cell: &str) -> String {
        let ranked = self.ranked(declared);
        let arms: String = ranked
            .iter()
            .enumerate()
            .map(|(rank, tag)| format!(" WHEN {tag} THEN {rank}"))
            .collect();
        format!("(CASE {cell}{arms} ELSE {cell} + {} END)", ranked.len())
    }

    /// SQL reading a rank ([`Self::rank_sql`]) back as its tag.
    pub(crate) fn unrank_sql(&self, declared: &[String], rank: &str) -> String {
        let ranked = self.ranked(declared);
        let arms: String = ranked
            .iter()
            .enumerate()
            .map(|(rank, tag)| format!(" WHEN {rank} THEN {tag}"))
            .collect();
        format!("(CASE {rank}{arms} ELSE {rank} - {} END)", ranked.len())
    }

    /// SQL reading `cell` (this column's) as its variant name: TEXT, as a
    /// string column reads.
    pub(crate) fn name_sql(&self, cell: &str) -> String {
        if self.by_tag.is_empty() {
            return cell.to_owned();
        }
        let arms: String = self
            .by_tag
            .iter()
            .map(|(tag, name)| format!(" WHEN {tag} THEN '{}'", name.replace('\'', "''")))
            .collect();
        format!("(CASE {cell}{arms} ELSE {cell} END)")
    }
}

/// A stored enum column's tags, shared by the schemas storing it.
pub type ColumnTags = Rc<RefCell<EnumTags>>;

/// Every stored enum column's tags on a store, by (stored table, stored
/// column). Schemas built for the store take their columns' entries from
/// it, so a tag the store interns reaches every schema at once.
///
/// A tag is in memory as soon as it is interned, and in the file once the
/// transaction interning it commits: every savepoint keeps an undo log of
/// the columns whose tags changed inside it ([`tags_changing`]), so rolling
/// it back puts their memory back too.
#[derive(Clone, Debug, Default)]
pub struct TagRegistry {
    map: Rc<RefCell<BTreeMap<(String, String), ColumnTags>>>,
}

impl TagRegistry {
    /// Stored column `column` of stored table `table`'s tags (none yet: an
    /// empty entry).
    pub(crate) fn column(&self, table: &str, column: &str) -> ColumnTags {
        self.map
            .borrow_mut()
            .entry((table.to_owned(), column.to_owned()))
            .or_default()
            .clone()
    }

    /// What every column's tags are now, and which entry holds them, to put
    /// back with [`Self::restore`].
    pub(crate) fn snapshot(&self) -> TagSnapshot {
        TagSnapshot(
            self.map
                .borrow()
                .iter()
                .map(|(key, tags)| (key.clone(), tags.clone(), tags.borrow().clone()))
                .collect(),
        )
    }

    /// Put the entries and their tags back as [`Self::snapshot`] took them.
    /// An entry made or moved since keeps no tags: a schema still holding
    /// it reads none it did not have.
    pub(crate) fn restore(&self, snapshot: TagSnapshot) {
        let mut map = self.map.borrow_mut();
        for tags in map.values() {
            if !snapshot.0.iter().any(|(_, kept, _)| Rc::ptr_eq(kept, tags)) {
                *tags.borrow_mut() = EnumTags::default();
            }
        }
        *map = snapshot
            .0
            .into_iter()
            .map(|(key, entry, tags)| {
                *entry.borrow_mut() = tags;
                (key, entry)
            })
            .collect();
    }

    /// Stored table `from` (with `column`, its stored column `column.0`) is
    /// stored as `to` (`column.1`) now: its tags follow it.
    pub(crate) fn moved(&self, from: &str, to: &str, column: Option<(&str, &str)>) {
        let mut map = self.map.borrow_mut();
        let keys: Vec<(String, String)> = map
            .keys()
            .filter(|(table, col)| table == from && column.is_none_or(|(old, _)| col == old))
            .cloned()
            .collect();
        for key in keys {
            let tags = map.remove(&key).expect("listed");
            let new = match column {
                None => (to.to_owned(), key.1),
                Some((_, new)) => (key.0, new.to_owned()),
            };
            map.insert(new, tags);
        }
    }
}

/// A [`TagRegistry`]'s entries and their tags at one point.
pub(crate) struct TagSnapshot(Vec<((String, String), ColumnTags, EnumTags)>);

/// The variant names an enum column's `value` holds.
pub(crate) fn enum_names(value: &Value) -> Vec<&str> {
    match value {
        Value::Nullable(Some(inner)) => enum_names(inner),
        Value::String(name) => vec![name.as_str()],
        Value::Array(items) => items.iter().flat_map(enum_names).collect(),
        _ => Vec::new(),
    }
}

/// `value` as an enum column's cell: a variant (also inside an array) as
/// its tag, or as its name where it has none (see [`Column::cell_sql`]).
pub(crate) fn enum_cell(tags: &EnumTags, value: &Value) -> Result<Option<Sql>, Error> {
    Ok(Some(match value {
        Value::Nullable(None) => Sql::Null,
        Value::Nullable(Some(inner)) => return enum_cell(tags, inner),
        Value::String(name) => match tags.tag(name) {
            Some(tag) => Sql::Integer(tag),
            None => Sql::Text(name.as_bytes().to_vec()),
        },
        Value::Array(items) => {
            let items = items
                .iter()
                .map(|item| enum_json(tags, item))
                .collect::<Result<Vec<_>, _>>()?;
            Sql::Text(serde_json::Value::Array(items).to_string().into_bytes())
        }
        other => return to_sql(other),
    }))
}

fn enum_json(tags: &EnumTags, item: &Value) -> Result<serde_json::Value, Error> {
    Ok(match item {
        Value::Nullable(None) => serde_json::Value::Null,
        Value::Nullable(Some(inner)) => enum_json(tags, inner)?,
        Value::String(name) => match tags.tag(name) {
            Some(tag) => tag.into(),
            None => name.clone().into(),
        },
        other => element_json(other)?,
    })
}

/// An enum column's cell back as its variant name (also inside an array).
fn enum_value(tags: &EnumTags, ty: &ValueType, cell: ValueRef<'_>) -> Result<Option<Value>, Error> {
    let unknown = |tag: i64| {
        Error::new(
            ErrorCode::Storage,
            format!("enum tag {tag} names no variant this store knows"),
        )
    };
    Ok(Some(match (ty, cell) {
        (ValueType::Nullable(_), ValueRef::Null) => Value::Nullable(None),
        (ValueType::Nullable(inner), cell) => {
            return Ok(enum_value(tags, inner, cell)?.map(|v| Value::Nullable(Some(Box::new(v)))));
        }
        (ValueType::String, ValueRef::Integer(tag)) => {
            Value::String(tags.name(tag).ok_or_else(|| unknown(tag))?.to_owned())
        }
        (ValueType::String, ValueRef::Text(name)) => match std::str::from_utf8(name) {
            Ok(name) => Value::String(name.to_owned()),
            Err(_) => return Ok(None),
        },
        (ValueType::Array(element), ValueRef::Text(cell)) => {
            let json: serde_json::Value = serde_json::from_slice(cell)
                .map_err(|e| Error::new(ErrorCode::Storage, format!("array cell: {e}")))?;
            let serde_json::Value::Array(items) = json else {
                return Ok(None);
            };
            fn item(
                tags: &EnumTags,
                ty: &ValueType,
                json: &serde_json::Value,
                unknown: &dyn Fn(i64) -> Error,
            ) -> Result<Option<Value>, Error> {
                Ok(Some(match (ty, json) {
                    (ValueType::Nullable(_), serde_json::Value::Null) => Value::Nullable(None),
                    (ValueType::Nullable(inner), json) => {
                        return Ok(item(tags, inner, json, unknown)?
                            .map(|v| Value::Nullable(Some(Box::new(v)))));
                    }
                    (ValueType::String, serde_json::Value::Number(tag)) => {
                        let Some(tag) = tag.as_i64() else {
                            return Ok(None);
                        };
                        Value::String(tags.name(tag).ok_or_else(|| unknown(tag))?.to_owned())
                    }
                    (ValueType::String, serde_json::Value::String(name)) => {
                        Value::String(name.clone())
                    }
                    _ => return Ok(None),
                }))
            }
            let mut values = Vec::with_capacity(items.len());
            for json in &items {
                let Some(value) = item(tags, element, json, &unknown)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Value::Array(values)
        }
        _ => return Ok(None),
    }))
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
/// stored: a deleted row is its current version with [`STATE_DELETED`] set
/// in `_st`, and that version's update stamp is the deletion's.
pub(crate) const DELETION: [&str; 6] = [
    "$deletedAt",
    "$deletedBy",
    "$deletedBy.account",
    "$deletedBy.identity",
    "$deletedBy.identity.issuer",
    "$deletedBy.identity.subject",
];

/// `_st` bit: the version's commit wrote no content, only a delete, a
/// restore or a move. Clear on an ordinary write, so a live content
/// version stores 0. History reads list versions with it clear.
pub(crate) const STATE_NO_CONTENT: i64 = 1;
/// `_st` bit: the version is a tombstone. Live versions are `_st < 2`.
pub(crate) const STATE_DELETED: i64 = 2;
/// `_st` bit: the version is an authority text merge's write as its writer
/// sent it, superseded by the merged version at the same seq (SPEC 4
/// §4.3). Kept only in `__h`, at `_seq` = minus the commit's seq, as the
/// base of its writer's next chained write; never read as a version, so
/// every read of `__h` that is not by exact seq requires this bit clear.
pub(crate) const STATE_AS_SENT: i64 = 4;

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
            json_type: None,
            variants: None,
            tags: None,
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
            json_type: None,
            variants: None,
            tags: None,
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

    /// The application column stored as `stored`.
    pub(crate) fn by_stored_column(&self, stored: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.stored == stored)
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
    /// `jazz`'s version id.
    pub version: crate::ids::SchemaVersionId,
    /// The schema whose policies govern this one's tables, when they are
    /// another's: a sync session on an older version is checked against
    /// the current schema's policies, compiled under its names (tables
    /// match by stored name; see [`crate::compile::Ctx`]).
    pub policies_from: Option<std::rc::Rc<Schema>>,
}

impl Schema {
    /// `jazz` as this store sees it, its enum columns taking their tags
    /// from `tags`.
    pub fn new(jazz: &JazzSchema, tags: &TagRegistry) -> Self {
        let mut schema = Self::unnamed(jazz);
        schema.attach_tags(tags);
        schema
    }

    fn unnamed(jazz: &JazzSchema) -> Self {
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
                .map(|column| {
                    let declared = public.and_then(|public| {
                        public
                            .columns
                            .columns
                            .iter()
                            .find(|declared| declared.name_str() == column.name)
                    });
                    Column {
                        name: column.name.clone(),
                        stored: column.name.clone(),
                        ty: column.column_type.clone(),
                        default: column.default.clone(),
                        json: column.large_value_kind
                            == crate::schema::LargeValueSemanticKind::Json,
                        json_type: declared
                            .filter(|declared| holds_json(&declared.column_type))
                            .map(|declared| Rc::new(JsonCheck::new(declared.column_type.clone()))),
                        variants: declared
                            .and_then(|declared| enum_variants(&declared.column_type))
                            .map(Rc::from),
                        tags: None,
                        merge: table
                            .merge_strategies
                            .get(&column.name)
                            .copied()
                            .unwrap_or_default(),
                    }
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
            // An index that leads another serves no read the longer one
            // cannot, and costs every write a second entry.
            let all = indexes.clone();
            indexes.retain(|index| {
                !all.iter()
                    .any(|other| other.len() > index.len() && other.starts_with(index))
            });
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
            version: jazz.version_id(),
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
    pub fn stored_as(jazz: &JazzSchema, names: &StoredNames, tags: &TagRegistry) -> Self {
        let mut schema = Self::unnamed(jazz);
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
        schema.attach_tags(tags);
        schema
    }

    /// Give each enum column its stored column's tags.
    fn attach_tags(&mut self, tags: &TagRegistry) {
        for table in self.tables.values_mut() {
            for column in &mut table.columns {
                if column.is_enum() {
                    column.tags = Some(tags.column(&table.stored, &column.stored));
                }
            }
        }
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
                        if let Some(stored) = physical
                            .columns
                            .iter_mut()
                            .find(|stored| stored.stored == column.stored)
                        {
                            // A reopened store knows a column by name only.
                            stored.ty = column.ty.clone();
                            stored.json = column.json;
                            stored.variants = column.variants.clone();
                            stored.tags = column.tags.clone();
                            continue;
                        }
                        // An enum default is its tag (see [`EnumTags`]).
                        let default = match &column.default {
                            Some(value) => sql_literal(column.cell_sql(value)?)?,
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
                            physical.indexes.push(index.clone());
                            ddl.push_str(&physical.index_ddl(&index));
                        }
                    }
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
                    json_type: None,
                    variants: None,
                    tags: None,
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
        // first, so every index leads with it. `_st` is the version's state
        // bits ([`STATE_NO_CONTENT`], [`STATE_DELETED`], and in history
        // [`STATE_AS_SENT`]): 0 for a live version whose commit wrote
        // content. `_base` and `_cols` are what the authority keeps of the
        // version an accepted upload wrote: what it was built on and the
        // columns it authored (SPEC 4 §4.6); NULL everywhere else.
        sql.push_str(&format!(
            "CREATE TABLE {name} (rid INTEGER PRIMARY KEY, id BLOB NOT NULL, \
             _b BLOB NOT NULL DEFAULT X'', _seq INTEGER NOT NULL, \
             _st INTEGER NOT NULL DEFAULT 0, _base INTEGER, _cols BLOB{columns}, \
             UNIQUE (id, _b));\n\
             CREATE INDEX {} ON {name}(_seq);\n\
             CREATE TABLE {history} (id BLOB NOT NULL, _b BLOB NOT NULL, _seq INTEGER NOT NULL, \
             _st INTEGER NOT NULL, _base INTEGER, _cols BLOB{columns}, PRIMARY KEY (id, _b, _seq)) WITHOUT ROWID;\n",
            quote(&format!("{}__by_seq", table.stored)),
        ));
        for index in &table.indexes {
            sql.push_str(&table.index_ddl(index));
        }
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

/// A cell as an SQL literal, for DDL defaults.
pub(crate) fn sql_literal(cell: Option<Sql>) -> Result<String, Error> {
    let hex = |v: &[u8]| v.iter().map(|b| format!("{b:02x}")).collect::<String>();
    Ok(match cell {
        None => return Err(unsupported("a NaN default")),
        Some(Sql::Null) => "NULL".to_owned(),
        Some(Sql::Integer(v)) => v.to_string(),
        Some(Sql::Real(v)) => format!("{v:e}"),
        // Text with a NUL has no SQL literal, and a column default must be
        // a literal.
        Some(Sql::Text(v)) => match std::str::from_utf8(&v) {
            Ok(text) if !text.contains('\0') => format!("'{}'", text.replace('\'', "''")),
            _ => return Err(unsupported("a text default with a NUL on an added column")),
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
        self.normalized_branch_key(values)
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
        self.normalized_branch_key(values)
    }

    /// The branch key of these branch-column values. A branch column at its
    /// default is left out (INV-BVIEW-19): a branch column a later schema
    /// version adds must have an immutable default, so the rows written
    /// before it, and the selectors of versions that do not name it, keep
    /// the key they had and read as that default.
    fn normalized_branch_key(
        &self,
        mut values: BTreeMap<String, crate::protocol::BranchColumnValue>,
    ) -> Vec<u8> {
        values.retain(|name, value| {
            self.branch_default(name)
                .is_none_or(|default| *value != crate::protocol::BranchColumnValue::from(default))
        });
        branch_key(&values)
    }

    /// A branch column's declared default, unwrapped from its nullability.
    fn branch_default(&self, column: &str) -> Option<Value> {
        let mut value = self.column(column)?.default.clone()?;
        while let Value::Nullable(Some(inner)) = value {
            value = *inner;
        }
        Some(value)
    }

    /// Branch-column cells named by a selector, decoded by column type.
    pub(crate) fn selector_cells(
        &self,
        selector: &crate::protocol::BranchSelector,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let mut cells = BTreeMap::new();
        for column in &self.branch_by {
            // A selector of a version before the column names its default.
            let Some(encoded) = selector.values.get(column) else {
                let default = self.branch_default(column).ok_or_else(|| {
                    query_error(format!("branch selector has no `{}.{column}`", self.name))
                })?;
                cells.insert(column.clone(), default);
                continue;
            };
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
    /// A union read's arm label: with the row, its occurrence.
    pub(crate) union_arm: Option<Arc<str>>,
}

/// A history version's transaction identity, `(tx, node)` as its
/// `$transactionId` reads.
pub type SourceVersion = (u64, crate::ids::NodeUuid);

/// A row's result key: the row itself, or a flat join's root followed by
/// its joined source rows, each with its history version if it has one,
/// and for a union read the arm that matched it (at the root's position).
/// Relations arrive as nested rows, not joined outputs.
pub fn occurrence_key(
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
        .expect("a union arm label is 1..=4096 bytes, as union_plan checks"),
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

    /// A union read's arm label (the arm that matched this occurrence of
    /// the row); `None` for other reads.
    pub fn union_arm(&self) -> Option<&str> {
        self.union_arm.as_deref()
    }

    /// The row's result key ([`occurrence_key`]).
    pub fn occurrence_key(&self) -> jazz_types::object::ResultKey {
        occurrence_key(self.id, self.version, &self.joined, self.union_arm())
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// The commit sequence number of this row version.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// A projected cell by column name.
    pub fn cell(&self, column: &str) -> Option<Value> {
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

/// The variants of an enum column, or of an array of enum values.
fn enum_variants(ty: &crate::tools::ColumnType) -> Option<Vec<String>> {
    match ty {
        crate::tools::ColumnType::Enum { variants } => Some(variants.clone()),
        crate::tools::ColumnType::Array { element } => enum_variants(element),
        _ => None,
    }
}

fn holds_json(ty: &crate::tools::ColumnType) -> bool {
    match ty {
        crate::tools::ColumnType::Json { .. } => true,
        crate::tools::ColumnType::Array { element } => holds_json(element),
        _ => false,
    }
}

/// How the values written to a column that holds JSON are checked: against
/// the column's declared public type, whose JSON Schema is compiled once, on
/// first use. Every value a write authors is checked here, local or uploaded
/// by a peer, so the schema holds for every value the store accepts, not
/// only for writers whose own validator agrees.
#[derive(Debug)]
pub struct JsonCheck {
    ty: crate::tools::ColumnType,
    compiled: std::cell::OnceCell<crate::tools::CompiledSchema>,
}

impl JsonCheck {
    pub fn new(ty: crate::tools::ColumnType) -> Self {
        Self {
            ty,
            compiled: std::cell::OnceCell::new(),
        }
    }

    /// Check a written cell: its JSON text must parse and match the schema.
    pub(crate) fn check(&self, value: &Value, path: &str) -> Result<(), String> {
        self.check_as(&self.ty, value, path)
    }

    fn check_as(
        &self,
        ty: &crate::tools::ColumnType,
        value: &Value,
        path: &str,
    ) -> Result<(), String> {
        use crate::tools::ColumnType;
        match (ty, value) {
            (_, Value::Nullable(None)) => Ok(()),
            (_, Value::Nullable(Some(inner))) => self.check_as(ty, inner, path),
            (ColumnType::Json { .. }, Value::String(text)) => self.check_text(text, path),
            (ColumnType::Array { element }, Value::Array(items)) => {
                items.iter().enumerate().try_for_each(|(index, item)| {
                    self.check_as(element, item, &format!("{path}[{index}]"))
                })
            }
            _ => Ok(()),
        }
    }

    /// Check one JSON document, given as its text.
    pub(crate) fn check_text(&self, text: &str, path: &str) -> Result<(), String> {
        let instance: serde_json::Value = serde_json::from_str(text)
            .map_err(|error| format!("invalid JSON for column `{path}`: {error}"))?;
        match self.compiled(path)? {
            Some(schema) => schema
                .validate(&instance)
                .map_err(|error| crate::tools::json_schema_failure(error, path)),
            None => Ok(()),
        }
    }

    /// The declared schema, compiled; `None` if the column declares none. A
    /// schema that fails to compile is tried again on the next check (a
    /// browser host may install its validator later).
    fn compiled(&self, path: &str) -> Result<Option<&crate::tools::CompiledSchema>, String> {
        let mut ty = &self.ty;
        while let crate::tools::ColumnType::Array { element } = ty {
            ty = element;
        }
        let crate::tools::ColumnType::Json {
            schema: Some(schema),
        } = ty
        else {
            return Ok(None);
        };
        if let Some(compiled) = self.compiled.get() {
            return Ok(Some(compiled));
        }
        let compiled = crate::tools::CompiledSchema::new(schema)
            .map_err(|error| crate::tools::json_schema_failure(error, path))?;
        Ok(Some(self.compiled.get_or_init(|| compiled)))
    }
}
