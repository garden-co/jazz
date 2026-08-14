//! Queryable JSON documents built entirely from ordinary Jazz rows.
//!
//! This is the deliberately small first representation described by
//! `SPEC/19_ordinary_json_documents.md`: immutable scalar/container parts, an
//! immutable complete root containing the ordered part references, a mutable
//! document root pointer, and ordinary declared-path projection rows.

use std::collections::HashMap;

use serde_json::{Map, Value as JsonValue};

use super::{
    BatchId, ColumnType, JazzClient, JazzError, ObjectId, Query, QueryBuilder, Result, Schema,
    TableSchema, Value, WriteContext,
};

const DOCUMENT_ROOT_COLUMN: &str = "root_id";
const ROOT_DOCUMENT_COLUMN: &str = "document_id";
const ROOT_PARTS_COLUMN: &str = "part_ids";
const PART_DOCUMENT_COLUMN: &str = "document_id";
const PART_POINTER_COLUMN: &str = "pointer";
const PART_KIND_COLUMN: &str = "kind";
const PART_SCALAR_COLUMN: &str = "scalar_json";
const PROJECTION_DOCUMENT_COLUMN: &str = "document_id";
const PROJECTION_ROOT_COLUMN: &str = "root_id";
const PROJECTION_POINTER_COLUMN: &str = "pointer";
const PROJECTION_SCALAR_COLUMN: &str = "scalar_json";

/// Ordinary table names used by one JSON-document collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDocumentNames {
    pub documents: String,
    pub roots: String,
    pub parts: String,
    pub projections: String,
}

impl JsonDocumentNames {
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            documents: format!("{prefix}_documents"),
            roots: format!("{prefix}_roots"),
            parts: format!("{prefix}_parts"),
            projections: format!("{prefix}_paths"),
        }
    }
}

/// Schema and declared JSON Pointer projections for a document collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDocumentSchema {
    pub names: JsonDocumentNames,
    projected_pointers: Vec<String>,
}

impl JsonDocumentSchema {
    pub fn new(prefix: &str) -> Self {
        Self {
            names: JsonDocumentNames::with_prefix(prefix),
            projected_pointers: Vec::new(),
        }
    }

    /// Declare an RFC 6901 JSON Pointer as an ordinary query projection.
    pub fn project(mut self, pointer: impl Into<String>) -> Result<Self> {
        let pointer = pointer.into();
        validate_pointer(&pointer)?;
        if !self.projected_pointers.contains(&pointer) {
            self.projected_pointers.push(pointer);
            self.projected_pointers.sort();
        }
        Ok(self)
    }

    pub fn projected_pointers(&self) -> &[String] {
        &self.projected_pointers
    }

    /// Add the four ordinary tables to an application schema.
    ///
    /// Callers may replace the returned tables' ordinary policies before
    /// publishing the schema. This helper intentionally grants no special
    /// document authorization semantics.
    pub fn install(&self, schema: &mut Schema) -> Result<()> {
        let tables = [
            TableSchema::builder(&self.names.documents)
                .fk_column(DOCUMENT_ROOT_COLUMN, &self.names.roots)
                .build_named(),
            TableSchema::builder(&self.names.roots)
                .fk_column(ROOT_DOCUMENT_COLUMN, &self.names.documents)
                .array_fk_column(ROOT_PARTS_COLUMN, &self.names.parts)
                .build_named(),
            TableSchema::builder(&self.names.parts)
                .fk_column(PART_DOCUMENT_COLUMN, &self.names.documents)
                .column(PART_POINTER_COLUMN, ColumnType::Text)
                .column(PART_KIND_COLUMN, ColumnType::Text)
                .column(PART_SCALAR_COLUMN, ColumnType::Text)
                .build_named(),
            TableSchema::builder(&self.names.projections)
                .fk_column(PROJECTION_DOCUMENT_COLUMN, &self.names.documents)
                .fk_column(PROJECTION_ROOT_COLUMN, &self.names.roots)
                .column(PROJECTION_POINTER_COLUMN, ColumnType::Text)
                .column(PROJECTION_SCALAR_COLUMN, ColumnType::Text)
                .index_only([
                    PROJECTION_DOCUMENT_COLUMN,
                    PROJECTION_POINTER_COLUMN,
                    PROJECTION_SCALAR_COLUMN,
                ])
                .build_named(),
        ];
        for (name, table) in tables {
            if schema.insert(name.clone(), table).is_some() {
                return Err(JazzError::Schema(format!(
                    "JSON document table already exists: {name}"
                )));
            }
        }
        Ok(())
    }

    /// Build a query over the ordinary declared-path projection table.
    pub fn query_scalar(&self, pointer: &str, value: &JsonValue) -> Result<Query> {
        validate_pointer(pointer)?;
        if !is_scalar(value) {
            return Err(JazzError::Query(
                "JSON document projection queries require a scalar value".to_owned(),
            ));
        }
        Ok(QueryBuilder::new(&self.names.projections)
            .filter_eq(PROJECTION_POINTER_COLUMN, Value::Text(pointer.to_owned()))
            .filter_eq(
                PROJECTION_SCALAR_COLUMN,
                Value::Text(serde_json::to_string(value)?),
            )
            .select(&[PROJECTION_DOCUMENT_COLUMN, PROJECTION_ROOT_COLUMN])
            .build())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDocumentCommit {
    pub document_id: ObjectId,
    pub root_id: ObjectId,
    pub batch_id: BatchId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonDocumentSnapshot {
    pub document_id: ObjectId,
    pub root_id: ObjectId,
    pub value: JsonValue,
    pub part_count: usize,
}

/// Public document operations over an existing `JazzClient`.
pub struct JsonDocumentStore<'a> {
    client: &'a JazzClient,
    schema: &'a JsonDocumentSchema,
}

impl<'a> JsonDocumentStore<'a> {
    pub fn new(client: &'a JazzClient, schema: &'a JsonDocumentSchema) -> Self {
        Self { client, schema }
    }

    /// Create one complete immutable snapshot in one ordinary transaction.
    pub fn create(&self, value: &JsonValue) -> Result<JsonDocumentCommit> {
        let document_id = ObjectId::new();
        let root_id = ObjectId::new();
        let records = flatten(value)?;
        let projections: Vec<_> = self
            .schema
            .projected_pointers
            .iter()
            .map(|pointer| {
                let projected = value.pointer(pointer).ok_or_else(|| {
                    JazzError::Write(format!("declared JSON projection is absent: {pointer}"))
                })?;
                if !is_scalar(projected) {
                    return Err(JazzError::Write(format!(
                        "declared JSON projection is not scalar: {pointer}"
                    )));
                }
                Ok((pointer, serde_json::to_string(projected)?))
            })
            .collect::<Result<_>>()?;
        let part_ids: Vec<ObjectId> = (0..records.len()).map(|_| ObjectId::new()).collect();
        let open = self.client.begin_transaction()?.open_batch_id();
        let writer = self
            .client
            .with_write_context(WriteContext::default().with_batch_id(open));

        for (id, record) in part_ids.iter().zip(&records) {
            writer.insert_with_id(
                &self.schema.names.parts,
                *id.uuid(),
                HashMap::from([
                    (PART_DOCUMENT_COLUMN.to_owned(), Value::Uuid(document_id)),
                    (
                        PART_POINTER_COLUMN.to_owned(),
                        Value::Text(record.pointer.clone()),
                    ),
                    (
                        PART_KIND_COLUMN.to_owned(),
                        Value::Text(record.kind.as_str().to_owned()),
                    ),
                    (
                        PART_SCALAR_COLUMN.to_owned(),
                        Value::Text(record.scalar_json.clone()),
                    ),
                ]),
            )?;
        }
        writer.insert_with_id(
            &self.schema.names.roots,
            *root_id.uuid(),
            HashMap::from([
                (ROOT_DOCUMENT_COLUMN.to_owned(), Value::Uuid(document_id)),
                (
                    ROOT_PARTS_COLUMN.to_owned(),
                    Value::Array(part_ids.iter().copied().map(Value::Uuid).collect()),
                ),
            ]),
        )?;
        for (pointer, projected) in projections {
            writer.insert(
                &self.schema.names.projections,
                HashMap::from([
                    (
                        PROJECTION_DOCUMENT_COLUMN.to_owned(),
                        Value::Uuid(document_id),
                    ),
                    (PROJECTION_ROOT_COLUMN.to_owned(), Value::Uuid(root_id)),
                    (
                        PROJECTION_POINTER_COLUMN.to_owned(),
                        Value::Text(pointer.clone()),
                    ),
                    (PROJECTION_SCALAR_COLUMN.to_owned(), Value::Text(projected)),
                ]),
            )?;
        }
        writer.insert_with_id(
            &self.schema.names.documents,
            *document_id.uuid(),
            HashMap::from([(DOCUMENT_ROOT_COLUMN.to_owned(), Value::Uuid(root_id))]),
        )?;
        let batch_id = self.client.commit_transaction(open)?;
        Ok(JsonDocumentCommit {
            document_id,
            root_id,
            batch_id,
        })
    }

    /// Read the current root and reconstruct its complete JSON value.
    pub async fn load(&self, document_id: ObjectId) -> Result<JsonDocumentSnapshot> {
        let row = self
            .row(
                &self.schema.names.documents,
                document_id,
                &[DOCUMENT_ROOT_COLUMN],
            )
            .await?;
        let root_id = expect_uuid(row.first(), "document root")?;
        self.load_root(document_id, root_id).await
    }

    /// Reconstruct one retained immutable root without scanning document row
    /// history. Only the root row and its explicitly referenced ordinary part
    /// rows are read.
    pub async fn load_root(
        &self,
        document_id: ObjectId,
        root_id: ObjectId,
    ) -> Result<JsonDocumentSnapshot> {
        let root = self
            .row(
                &self.schema.names.roots,
                root_id,
                &[ROOT_DOCUMENT_COLUMN, ROOT_PARTS_COLUMN],
            )
            .await?;
        if expect_uuid(root.first(), "root document")? != document_id {
            return Err(JazzError::Query(
                "JSON document root belongs to another document".to_owned(),
            ));
        }
        let part_ids = expect_uuid_array(root.get(1), "root parts")?;
        let mut records = Vec::with_capacity(part_ids.len());
        for part_id in &part_ids {
            let row = self
                .row(
                    &self.schema.names.parts,
                    *part_id,
                    &[
                        PART_DOCUMENT_COLUMN,
                        PART_POINTER_COLUMN,
                        PART_KIND_COLUMN,
                        PART_SCALAR_COLUMN,
                    ],
                )
                .await?;
            if expect_uuid(row.first(), "part document")? != document_id {
                return Err(JazzError::Query(
                    "JSON document part belongs to another document".to_owned(),
                ));
            }
            records.push(PartRecord {
                pointer: expect_text(row.get(1), "part pointer")?.to_owned(),
                kind: PartKind::parse(expect_text(row.get(2), "part kind")?)?,
                scalar_json: expect_text(row.get(3), "part scalar")?.to_owned(),
            });
        }
        Ok(JsonDocumentSnapshot {
            document_id,
            root_id,
            value: rebuild(&records)?,
            part_count: records.len(),
        })
    }

    /// Replace one existing scalar leaf, preserving all other immutable part
    /// rows. The first representation rewrites the root's reference vector;
    /// the persistent bounded-fanout root in the spec will reduce this O(n)
    /// metadata write to O(log n) replacement nodes.
    pub async fn set_scalar(
        &self,
        document_id: ObjectId,
        pointer: &str,
        value: &JsonValue,
    ) -> Result<JsonDocumentCommit> {
        validate_pointer(pointer)?;
        if !is_scalar(value) {
            return Err(JazzError::Write(
                "set_scalar requires null, boolean, number, or string".to_owned(),
            ));
        }
        let current = self
            .row(
                &self.schema.names.documents,
                document_id,
                &[DOCUMENT_ROOT_COLUMN],
            )
            .await?;
        let old_root = expect_uuid(current.first(), "document root")?;
        let root = self
            .row(
                &self.schema.names.roots,
                old_root,
                &[ROOT_DOCUMENT_COLUMN, ROOT_PARTS_COLUMN],
            )
            .await?;
        if expect_uuid(root.first(), "root document")? != document_id {
            return Err(JazzError::Query(
                "JSON document root belongs to another document".to_owned(),
            ));
        }
        let mut part_ids = expect_uuid_array(root.get(1), "root parts")?;
        let mut replacement_index = None;
        for (index, part_id) in part_ids.iter().enumerate() {
            let row = self
                .row(
                    &self.schema.names.parts,
                    *part_id,
                    &[PART_DOCUMENT_COLUMN, PART_POINTER_COLUMN, PART_KIND_COLUMN],
                )
                .await?;
            if expect_uuid(row.first(), "part document")? != document_id {
                return Err(JazzError::Query(
                    "JSON document part belongs to another document".to_owned(),
                ));
            }
            if expect_text(row.get(1), "part pointer")? == pointer {
                if PartKind::parse(expect_text(row.get(2), "part kind")?)? != PartKind::Scalar {
                    return Err(JazzError::Write(format!(
                        "JSON pointer does not name a scalar: {pointer}"
                    )));
                }
                replacement_index = Some(index);
                break;
            }
        }
        let replacement_index = replacement_index.ok_or_else(|| {
            JazzError::Write(format!("JSON pointer is absent from document: {pointer}"))
        })?;
        let replacement_id = ObjectId::new();
        part_ids[replacement_index] = replacement_id;
        let root_id = ObjectId::new();
        let open = self.client.begin_transaction()?.open_batch_id();
        let writer = self
            .client
            .with_write_context(WriteContext::default().with_batch_id(open));
        writer.insert_with_id(
            &self.schema.names.parts,
            *replacement_id.uuid(),
            HashMap::from([
                (PART_DOCUMENT_COLUMN.to_owned(), Value::Uuid(document_id)),
                (
                    PART_POINTER_COLUMN.to_owned(),
                    Value::Text(pointer.to_owned()),
                ),
                (
                    PART_KIND_COLUMN.to_owned(),
                    Value::Text("scalar".to_owned()),
                ),
                (
                    PART_SCALAR_COLUMN.to_owned(),
                    Value::Text(serde_json::to_string(value)?),
                ),
            ]),
        )?;
        writer.insert_with_id(
            &self.schema.names.roots,
            *root_id.uuid(),
            HashMap::from([
                (ROOT_DOCUMENT_COLUMN.to_owned(), Value::Uuid(document_id)),
                (
                    ROOT_PARTS_COLUMN.to_owned(),
                    Value::Array(part_ids.into_iter().map(Value::Uuid).collect()),
                ),
            ]),
        )?;
        writer.update(
            document_id,
            vec![(DOCUMENT_ROOT_COLUMN.to_owned(), Value::Uuid(root_id))],
        )?;
        for projected in &self.schema.projected_pointers {
            let query = QueryBuilder::new(&self.schema.names.projections)
                .filter_eq(PROJECTION_DOCUMENT_COLUMN, Value::Uuid(document_id))
                .filter_eq(PROJECTION_POINTER_COLUMN, Value::Text(projected.clone()))
                .build();
            let rows = self.client.query(query, None).await?;
            let (projection_id, _) = rows.first().ok_or_else(|| {
                JazzError::Write(format!("missing declared projection row: {projected}"))
            })?;
            let mut patch = vec![(PROJECTION_ROOT_COLUMN.to_owned(), Value::Uuid(root_id))];
            if projected == pointer {
                patch.push((
                    PROJECTION_SCALAR_COLUMN.to_owned(),
                    Value::Text(serde_json::to_string(value)?),
                ));
            }
            writer.update(*projection_id, patch)?;
        }
        let batch_id = self.client.commit_transaction(open)?;
        Ok(JsonDocumentCommit {
            document_id,
            root_id,
            batch_id,
        })
    }

    async fn row(&self, table: &str, id: ObjectId, columns: &[&str]) -> Result<Vec<Value>> {
        self.client
            .query(QueryBuilder::new(table).select(columns).build(), None)
            .await?
            .into_iter()
            .find(|(row_id, _)| *row_id == id)
            .map(|(_, values)| values)
            .ok_or_else(|| JazzError::Query(format!("row {id} not found in {table}")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PartKind {
    Object,
    Array,
    Scalar,
}

impl PartKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Array => "array",
            Self::Scalar => "scalar",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            "scalar" => Ok(Self::Scalar),
            _ => Err(JazzError::Query(format!("unknown JSON part kind: {value}"))),
        }
    }
}

#[derive(Clone, Debug)]
struct PartRecord {
    pointer: String,
    kind: PartKind,
    scalar_json: String,
}

fn flatten(value: &JsonValue) -> Result<Vec<PartRecord>> {
    fn visit(value: &JsonValue, pointer: &str, output: &mut Vec<PartRecord>) -> Result<()> {
        let (kind, scalar_json) = match value {
            JsonValue::Object(_) => (PartKind::Object, String::new()),
            JsonValue::Array(_) => (PartKind::Array, String::new()),
            _ => (PartKind::Scalar, serde_json::to_string(value)?),
        };
        output.push(PartRecord {
            pointer: pointer.to_owned(),
            kind,
            scalar_json,
        });
        match value {
            JsonValue::Object(values) => {
                for (key, child) in values {
                    visit(
                        child,
                        &format!("{pointer}/{}", escape_pointer_token(key)),
                        output,
                    )?;
                }
            }
            JsonValue::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    visit(child, &format!("{pointer}/{index}"), output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let mut output = Vec::new();
    visit(value, "", &mut output)?;
    Ok(output)
}

fn rebuild(records: &[PartRecord]) -> Result<JsonValue> {
    let root = records
        .iter()
        .find(|record| record.pointer.is_empty())
        .ok_or_else(|| JazzError::Query("JSON document root part is missing".to_owned()))?;
    let mut value = empty_value(root)?;
    let mut children: Vec<_> = records
        .iter()
        .filter(|record| !record.pointer.is_empty())
        .collect();
    children.sort_by_key(|record| {
        pointer_tokens(&record.pointer)
            .map(|tokens| tokens.len())
            .unwrap_or(usize::MAX)
    });
    for record in children {
        insert_pointer(&mut value, &record.pointer, empty_value(record)?)?;
    }
    Ok(value)
}

fn empty_value(record: &PartRecord) -> Result<JsonValue> {
    match record.kind {
        PartKind::Object => Ok(JsonValue::Object(Map::new())),
        PartKind::Array => Ok(JsonValue::Array(Vec::new())),
        PartKind::Scalar => Ok(serde_json::from_str(&record.scalar_json)?),
    }
}

fn insert_pointer(root: &mut JsonValue, pointer: &str, value: JsonValue) -> Result<()> {
    let mut tokens = pointer_tokens(pointer)?;
    let token = tokens
        .pop()
        .ok_or_else(|| JazzError::Query("cannot insert the root pointer".to_owned()))?;
    let parent_pointer = if tokens.is_empty() {
        String::new()
    } else {
        format!(
            "/{}",
            tokens
                .iter()
                .map(|token| escape_pointer_token(token))
                .collect::<Vec<_>>()
                .join("/")
        )
    };
    let parent = root.pointer_mut(&parent_pointer).ok_or_else(|| {
        JazzError::Query(format!("JSON document parent is missing: {parent_pointer}"))
    })?;
    match parent {
        JsonValue::Object(values) => {
            values.insert(token, value);
        }
        JsonValue::Array(values) => {
            let index: usize = token.parse().map_err(|_| {
                JazzError::Query(format!("invalid JSON array index in pointer: {pointer}"))
            })?;
            if index != values.len() {
                return Err(JazzError::Query(format!(
                    "non-contiguous JSON array index in pointer: {pointer}"
                )));
            }
            values.push(value);
        }
        _ => {
            return Err(JazzError::Query(format!(
                "JSON pointer parent is scalar: {parent_pointer}"
            )));
        }
    }
    Ok(())
}

fn pointer_tokens(pointer: &str) -> Result<Vec<String>> {
    validate_pointer(pointer)?;
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    pointer[1..]
        .split('/')
        .map(unescape_pointer_token)
        .collect()
}

fn validate_pointer(pointer: &str) -> Result<()> {
    if !pointer.is_empty() && !pointer.starts_with('/') {
        return Err(JazzError::Schema(format!(
            "JSON Pointer must be empty or begin with '/': {pointer}"
        )));
    }
    for token in pointer.split('/') {
        let bytes = token.as_bytes();
        for index in 0..bytes.len() {
            if bytes[index] == b'~'
                && (index + 1 == bytes.len() || !matches!(bytes[index + 1], b'0' | b'1'))
            {
                return Err(JazzError::Schema(format!(
                    "JSON Pointer contains an invalid '~' escape: {pointer}"
                )));
            }
        }
    }
    Ok(())
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn unescape_pointer_token(token: &str) -> Result<String> {
    let mut output = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => output.push('~'),
            Some('1') => output.push('/'),
            _ => {
                return Err(JazzError::Schema(format!(
                    "invalid JSON Pointer token: {token}"
                )));
            }
        }
    }
    Ok(output)
}

fn is_scalar(value: &JsonValue) -> bool {
    !value.is_array() && !value.is_object()
}

fn expect_uuid(value: Option<&Value>, label: &str) -> Result<ObjectId> {
    match value {
        Some(Value::Uuid(value)) => Ok(*value),
        _ => Err(JazzError::Query(format!("{label} is not a UUID"))),
    }
}

fn expect_uuid_array(value: Option<&Value>, label: &str) -> Result<Vec<ObjectId>> {
    match value {
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                Value::Uuid(value) => Ok(*value),
                _ => Err(JazzError::Query(format!("{label} contains a non-UUID"))),
            })
            .collect(),
        _ => Err(JazzError::Query(format!("{label} is not a UUID array"))),
    }
}

fn expect_text<'a>(value: Option<&'a Value>, label: &str) -> Result<&'a str> {
    match value {
        Some(Value::Text(value)) => Ok(value),
        _ => Err(JazzError::Query(format!("{label} is not text"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_and_rebuild_round_trip_nested_json_and_pointer_escapes() {
        let value = serde_json::json!({
            "a/b": {"~key": [1, true, null, {"name": "nested"}]},
            "empty": {},
        });
        let records = flatten(&value).expect("flatten");
        assert_eq!(rebuild(&records).expect("rebuild"), value);
        assert!(
            records
                .iter()
                .any(|record| record.pointer == "/a~1b/~0key/3/name")
        );
    }
}
