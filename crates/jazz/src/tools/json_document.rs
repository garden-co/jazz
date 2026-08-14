//! Queryable JSON documents built entirely from ordinary Jazz rows.
//!
//! This is the deliberately small first representation described by
//! `SPEC/19_ordinary_json_documents.md`: immutable scalar/container parts, an
//! immutable complete root containing the ordered part references, a mutable
//! document root pointer, and ordinary declared-path projection rows.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value as JsonValue};

use super::{
    BatchId, ColumnType, JazzClient, JazzError, ObjectId, Query, QueryBuilder, Result, Schema,
    TableName, TableSchema, Value,
};

const DOCUMENT_ROOT_COLUMN: &str = "root_id";
const ROOT_DOCUMENT_COLUMN: &str = "document_id";
const ROOT_PARTS_COLUMN: &str = "part_ids";
const PART_DOCUMENT_COLUMN: &str = "document_id";
const PART_POINTER_COLUMN: &str = "pointer";
const PART_KIND_COLUMN: &str = "kind";
const PART_SCALAR_COLUMN: &str = "scalar_json";
const PROJECTION_DOCUMENT_COLUMN: &str = "document_id";
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
        for name in [
            &self.names.documents,
            &self.names.roots,
            &self.names.parts,
            &self.names.projections,
        ] {
            if schema.contains_key(&TableName::new(name)) {
                return Err(JazzError::Schema(format!(
                    "JSON document table already exists: {name}"
                )));
            }
        }
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
            schema.insert(name, table);
        }
        Ok(())
    }

    /// Build a query over the ordinary declared-path projection table.
    pub fn query_scalar(&self, pointer: &str, value: &JsonValue) -> Result<Query> {
        validate_pointer(pointer)?;
        if !self
            .projected_pointers
            .iter()
            .any(|declared| declared == pointer)
        {
            return Err(JazzError::Query(format!(
                "JSON Pointer is not a declared query projection: {pointer}"
            )));
        }
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
            .select(&[PROJECTION_DOCUMENT_COLUMN])
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
        let transaction = self.client.begin_transaction()?;
        let staged = (|| -> Result<()> {
            for (id, record) in part_ids.iter().zip(&records) {
                transaction.insert_with_id(
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
            transaction.insert_with_id(
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
                transaction.insert(
                    &self.schema.names.projections,
                    HashMap::from([
                        (
                            PROJECTION_DOCUMENT_COLUMN.to_owned(),
                            Value::Uuid(document_id),
                        ),
                        (
                            PROJECTION_POINTER_COLUMN.to_owned(),
                            Value::Text(pointer.clone()),
                        ),
                        (PROJECTION_SCALAR_COLUMN.to_owned(), Value::Text(projected)),
                    ]),
                )?;
            }
            transaction.insert_with_id(
                &self.schema.names.documents,
                *document_id.uuid(),
                HashMap::from([(DOCUMENT_ROOT_COLUMN.to_owned(), Value::Uuid(root_id))]),
            )?;
            Ok(())
        })();
        if let Err(error) = staged {
            let _ = transaction.rollback();
            return Err(error);
        }
        let batch_id = transaction.commit()?;
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
    /// history. The v0 facade loads the document's immutable ordinary part rows
    /// in one filtered query, then reconstructs only the ids named by this root.
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
        let available_parts = self.parts_for_document(document_id).await?;
        let records = validate_root_records(&part_ids, &available_parts)?;
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
        let available_parts = self.parts_for_document(document_id).await?;
        let records = validate_root_records(&part_ids, &available_parts)?;
        let replacement_index = records.iter().position(|part| part.pointer == pointer);
        let replacement_index = replacement_index.ok_or_else(|| {
            JazzError::Write(format!("JSON pointer is absent from document: {pointer}"))
        })?;
        if records[replacement_index].kind != PartKind::Scalar {
            return Err(JazzError::Write(format!(
                "JSON pointer does not name a scalar: {pointer}"
            )));
        }
        let mut affected_projection = None;
        for projected in &self.schema.projected_pointers {
            let projected_part = records
                .iter()
                .find(|part| part.pointer == *projected)
                .ok_or_else(|| {
                    JazzError::Write(format!(
                        "declared projection is absent from current root: {projected}"
                    ))
                })?;
            if projected_part.kind != PartKind::Scalar {
                return Err(JazzError::Write(format!(
                    "declared projection is not scalar in current root: {projected}"
                )));
            }
            let query = QueryBuilder::new(&self.schema.names.projections)
                .filter_eq(PROJECTION_DOCUMENT_COLUMN, Value::Uuid(document_id))
                .filter_eq(PROJECTION_POINTER_COLUMN, Value::Text(projected.clone()))
                .select(&[
                    PROJECTION_DOCUMENT_COLUMN,
                    PROJECTION_POINTER_COLUMN,
                    PROJECTION_SCALAR_COLUMN,
                ])
                .build();
            let rows = self.client.query(query, None).await?;
            if rows.len() != 1 {
                return Err(JazzError::Write(format!(
                    "expected exactly one declared projection row for {projected}, found {}",
                    rows.len()
                )));
            }
            let (projection_id, projection) = &rows[0];
            if expect_uuid(projection.first(), "projection document")? != document_id
                || expect_text(projection.get(1), "projection pointer")? != projected
                || expect_text(projection.get(2), "projection scalar")?
                    != projected_part.scalar_json
            {
                return Err(JazzError::Write(format!(
                    "declared projection does not match current root: {projected}"
                )));
            }
            if projected == pointer {
                affected_projection = Some(*projection_id);
            }
        }
        let replacement_id = ObjectId::new();
        part_ids[replacement_index] = replacement_id;
        let root_id = ObjectId::new();
        let scalar_json = serde_json::to_string(value)?;
        let transaction = self.client.begin_transaction()?;
        let staged = (|| -> Result<()> {
            transaction.insert_with_id(
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
                        Value::Text(scalar_json.clone()),
                    ),
                ]),
            )?;
            transaction.insert_with_id(
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
            transaction.update(
                document_id,
                vec![(DOCUMENT_ROOT_COLUMN.to_owned(), Value::Uuid(root_id))],
            )?;
            if let Some(projection_id) = affected_projection {
                transaction.update(
                    projection_id,
                    vec![(
                        PROJECTION_SCALAR_COLUMN.to_owned(),
                        Value::Text(scalar_json.clone()),
                    )],
                )?;
            }
            Ok(())
        })();
        if let Err(error) = staged {
            let _ = transaction.rollback();
            return Err(error);
        }
        let batch_id = transaction.commit()?;
        Ok(JsonDocumentCommit {
            document_id,
            root_id,
            batch_id,
        })
    }

    async fn row(&self, table: &str, id: ObjectId, columns: &[&str]) -> Result<Vec<Value>> {
        self.client
            .query(
                QueryBuilder::new(table)
                    .filter_eq("id", Value::Uuid(id))
                    .select(columns)
                    .build(),
                None,
            )
            .await?
            .into_iter()
            .find(|(row_id, _)| *row_id == id)
            .map(|(_, values)| values)
            .ok_or_else(|| JazzError::Query(format!("row {id} not found in {table}")))
    }

    async fn parts_for_document(
        &self,
        document_id: ObjectId,
    ) -> Result<HashMap<ObjectId, PartRecord>> {
        self.client
            .query(
                QueryBuilder::new(&self.schema.names.parts)
                    .filter_eq(PART_DOCUMENT_COLUMN, Value::Uuid(document_id))
                    .select(&[
                        PART_DOCUMENT_COLUMN,
                        PART_POINTER_COLUMN,
                        PART_KIND_COLUMN,
                        PART_SCALAR_COLUMN,
                    ])
                    .build(),
                None,
            )
            .await?
            .into_iter()
            .map(|(part_id, row)| {
                if expect_uuid(row.first(), "part document")? != document_id {
                    return Err(JazzError::Query(
                        "JSON document part belongs to another document".to_owned(),
                    ));
                }
                Ok((
                    part_id,
                    PartRecord {
                        pointer: expect_text(row.get(1), "part pointer")?.to_owned(),
                        kind: PartKind::parse(expect_text(row.get(2), "part kind")?)?,
                        scalar_json: expect_text(row.get(3), "part scalar")?.to_owned(),
                    },
                ))
            })
            .collect()
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

fn validate_root_records(
    part_ids: &[ObjectId],
    available_parts: &HashMap<ObjectId, PartRecord>,
) -> Result<Vec<PartRecord>> {
    let mut ids = HashSet::with_capacity(part_ids.len());
    let mut pointers = HashSet::with_capacity(part_ids.len());
    let mut records = Vec::with_capacity(part_ids.len());
    for part_id in part_ids {
        if !ids.insert(*part_id) {
            return Err(JazzError::Query(format!(
                "JSON document root contains a duplicate part id: {part_id}"
            )));
        }
        let part = available_parts.get(part_id).ok_or_else(|| {
            JazzError::Query(format!(
                "JSON document root references an unavailable part: {part_id}"
            ))
        })?;
        let tokens = pointer_tokens(&part.pointer)?;
        let canonical = if tokens.is_empty() {
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
        if canonical != part.pointer {
            return Err(JazzError::Query(format!(
                "JSON document part pointer is not canonical: {}",
                part.pointer
            )));
        }
        if !pointers.insert(part.pointer.clone()) {
            return Err(JazzError::Query(format!(
                "JSON document root contains a duplicate pointer: {}",
                part.pointer
            )));
        }
        match part.kind {
            PartKind::Scalar => {
                let value: JsonValue = serde_json::from_str(&part.scalar_json)?;
                if !is_scalar(&value) {
                    return Err(JazzError::Query(format!(
                        "JSON scalar part contains a container payload: {}",
                        part.pointer
                    )));
                }
            }
            PartKind::Object | PartKind::Array if !part.scalar_json.is_empty() => {
                return Err(JazzError::Query(format!(
                    "JSON container part contains a scalar payload: {}",
                    part.pointer
                )));
            }
            PartKind::Object | PartKind::Array => {}
        }
        records.push(part.clone());
    }
    if !pointers.contains("") {
        return Err(JazzError::Query(
            "JSON document root part is missing".to_owned(),
        ));
    }
    let by_pointer: HashMap<_, _> = records
        .iter()
        .map(|record| (record.pointer.as_str(), record))
        .collect();
    let mut array_children: HashMap<&str, Vec<usize>> = HashMap::new();
    for record in records.iter().filter(|record| !record.pointer.is_empty()) {
        let mut tokens = pointer_tokens(&record.pointer)?;
        let child = tokens.pop().expect("non-root pointer has a token");
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
        let parent = by_pointer.get(parent_pointer.as_str()).ok_or_else(|| {
            JazzError::Query(format!("JSON document parent is missing: {parent_pointer}"))
        })?;
        match parent.kind {
            PartKind::Object => {}
            PartKind::Array => {
                let index: usize = child.parse().map_err(|_| {
                    JazzError::Query(format!(
                        "invalid JSON array index in pointer: {}",
                        record.pointer
                    ))
                })?;
                if child != index.to_string() {
                    return Err(JazzError::Query(format!(
                        "non-canonical JSON array index in pointer: {}",
                        record.pointer
                    )));
                }
                array_children
                    .entry(parent.pointer.as_str())
                    .or_default()
                    .push(index);
            }
            PartKind::Scalar => {
                return Err(JazzError::Query(format!(
                    "JSON pointer parent is scalar: {parent_pointer}"
                )));
            }
        }
    }
    for (pointer, mut indices) in array_children {
        indices.sort_unstable();
        if indices.iter().copied().ne(0..indices.len()) {
            return Err(JazzError::Query(format!(
                "JSON array children are not contiguous: {pointer}"
            )));
        }
    }
    Ok(records)
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
mod integrity_tests {
    use super::*;

    fn validate(records: Vec<PartRecord>, ids: Vec<ObjectId>) -> Result<Vec<PartRecord>> {
        let available = ids.iter().copied().zip(records).collect();
        validate_root_records(&ids, &available)
    }

    fn part(pointer: &str, kind: PartKind, scalar_json: &str) -> PartRecord {
        PartRecord {
            pointer: pointer.to_owned(),
            kind,
            scalar_json: scalar_json.to_owned(),
        }
    }

    #[test]
    fn root_validation_rejects_duplicate_ids_and_missing_or_duplicate_root_pointers() {
        let id = ObjectId::new();
        let error = validate(vec![part("", PartKind::Object, "")], vec![id, id])
            .expect_err("duplicate id must fail");
        assert!(error.to_string().contains("duplicate part id"));

        let error = validate(
            vec![part("/child", PartKind::Scalar, "1")],
            vec![ObjectId::new()],
        )
        .expect_err("missing root must fail");
        assert!(error.to_string().contains("root part is missing"));

        let error = validate(
            vec![
                part("", PartKind::Object, ""),
                part("", PartKind::Object, ""),
            ],
            vec![ObjectId::new(), ObjectId::new()],
        )
        .expect_err("duplicate root pointer must fail");
        assert!(error.to_string().contains("duplicate pointer"));
    }

    #[test]
    fn root_validation_rejects_payload_and_structure_corruption() {
        let error = validate(
            vec![part("", PartKind::Scalar, "{}")],
            vec![ObjectId::new()],
        )
        .expect_err("scalar container payload must fail");
        assert!(error.to_string().contains("container payload"));

        let error = validate(
            vec![
                part("", PartKind::Object, ""),
                part("/parent", PartKind::Scalar, "1"),
                part("/parent/child", PartKind::Scalar, "2"),
            ],
            vec![ObjectId::new(), ObjectId::new(), ObjectId::new()],
        )
        .expect_err("child beneath scalar must fail");
        assert!(error.to_string().contains("parent is scalar"));

        let error = validate(
            vec![part("", PartKind::Object, "payload")],
            vec![ObjectId::new()],
        )
        .expect_err("container scalar payload must fail");
        assert!(
            error
                .to_string()
                .contains("container part contains a scalar payload")
        );
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
