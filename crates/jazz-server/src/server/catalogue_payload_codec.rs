//! Binary encoding for active catalogue payloads.
//!
//! This module provides deterministic binary serialization for Schema and LensTransform,
//! enabling content-addressed storage in the catalogue.
//!
//! Schema and lens payloads use version 2, including an empty composite-index
//! list for tables without composite indexes. Version 1 payloads are rejected.

use std::collections::{BTreeSet, HashMap};

use jazz::protocol_limits::PolicyExpressionLimitError;
use jazz::query::policy_limits::PolicyExpressionBudget;
use jazz::tools::ObjectId;
use jazz::tools::public_schema::{CmpOp, Operation, OperationPolicy, PolicyExpr, PolicyValue};
use jazz::tools::public_schema::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnName, ColumnType, EnumCaseDescriptor,
    RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationKeyRef, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef, RowDescriptor,
    Schema, SchemaHash, TableName, TablePolicies, TableSchema, Value,
};

use jazz::tools::schema_lens::{LensOp, LensTransform};

/// Describes a struct once, as its fields' codecs in wire order, and derives
/// both its writer and its reader (a [`Wire`] impl) from that description.
/// An optional `(param: Type)` works as for [`wire_enum!`].
macro_rules! wire_struct {
    ($name:ident $(($param:ident: $param_ty:ty))?: $ty:ident {
        $($field:ident: $codec:expr),+ $(,)?
    }) => {
        struct $name$(($param_ty))?;

        impl Wire<$ty> for $name {
            fn put(&self, buf: &mut Vec<u8>, value: &$ty) {
                $(let $param: $param_ty = self.0;)?
                $(Wire::put(&$codec, buf, &value.$field);)+
            }

            fn get(&self, data: &[u8], offset: &mut usize) -> Result<$ty, CatalogueEncodingError> {
                $(let $param: $param_ty = self.0;)?
                Ok($ty { $($field: Wire::get(&$codec, data, offset)?),+ })
            }
        }
    };
}

/// Describes an enum once, as a tag byte → variant table listing each
/// variant's field codecs in wire order (`Unit`, `Tuple(binding: codec)` or
/// `Struct { field: codec, .. }`), and derives both its writer and its reader.
///
/// With a context string the table is complete: it implements [`Wire`] and an
/// unknown tag decodes to `InvalidTypeTag` with that context. With `partial`
/// it covers only the listed variants, and `get_variant` returns `None` for the
/// rest, which the caller codes by hand. The caller's encoder matches every
/// variant exhaustively and names the listed ones in the arm that calls
/// `put_variant`, so a new variant fails to build until it is placed.
/// An optional `(param: Type)` makes the codec a value carrying a parameter
/// that the field codecs can use.
macro_rules! wire_enum {
    (partial $($rest:tt)*) => {
        wire_enum!(@impl [_ => unreachable!("encoder arm names a variant missing from the table"),] $($rest)*);
    };
    (@impl [$($fallback:tt)*] $name:ident $(($param:ident: $param_ty:ty))?: $ty:ident
        $(, $context:literal)? {
        $($tag:tt => $variant:ident $(($binding:ident: $codec:expr))?
            $({ $($field:ident: $field_codec:expr),* $(,)? })?),+ $(,)?
    }) => {
        struct $name$(($param_ty))?;

        impl $name {
            fn put_variant(&self, buf: &mut Vec<u8>, value: &$ty) {
                $(let $param: $param_ty = self.0;)?
                match value {
                    $($ty::$variant $(($binding))? $({ $($field),* })? => {
                        buf.push($tag);
                        $(Wire::put(&$codec, buf, $binding);)?
                        $($(Wire::put(&$field_codec, buf, $field);)*)?
                    })+
                    $($fallback)*
                }
            }

            #[allow(unused_variables)]
            fn get_variant(
                &self,
                tag: u8,
                data: &[u8],
                offset: &mut usize,
            ) -> Result<Option<$ty>, CatalogueEncodingError> {
                $(let $param: $param_ty = self.0;)?
                Ok(Some(match tag {
                    $($tag => $ty::$variant $((Wire::get(&$codec, data, offset)?))?
                        $({ $($field: Wire::get(&$field_codec, data, offset)?),* })?,)+
                    _ => return Ok(None),
                }))
            }
        }

        $(impl Wire<$ty> for $name {
            fn put(&self, buf: &mut Vec<u8>, value: &$ty) {
                self.put_variant(buf, value);
            }

            fn get(&self, data: &[u8], offset: &mut usize) -> Result<$ty, CatalogueEncodingError> {
                let tag = read_u8(data, offset)?;
                self.get_variant(tag, data, offset)?
                    .ok_or(CatalogueEncodingError::InvalidTypeTag { tag, context: $context })
            }
        })?
    };
    ($($rest:tt)*) => { wire_enum!(@impl [] $($rest)*); };
}

/// Current catalogue payload encoding versions.
const SCHEMA_VERSION: u8 = 2;
const LENS_VERSION: u8 = 2;
const PERMISSIONS_VERSION: u8 = 1;
const PERMISSIONS_BUNDLE_VERSION: u8 = 1;
const PERMISSIONS_HEAD_VERSION: u8 = 1;

/// Encoding errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogueEncodingError {
    /// Data too short.
    TruncatedData { expected: usize, actual: usize },
    /// Unknown version byte.
    UnsupportedVersion { found: u8, expected: u8 },
    /// Invalid type tag.
    InvalidTypeTag { tag: u8, context: &'static str },
    /// Invalid UTF-8 string.
    InvalidUtf8 { context: &'static str },
    /// A named recursive-policy protocol boundary was crossed.
    ProtocolLimit(PolicyExpressionLimitError),
    /// Generic decode error.
    DecodeError { message: String },
}

impl std::fmt::Display for CatalogueEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogueEncodingError::TruncatedData { expected, actual } => {
                write!(f, "truncated data: expected {expected} bytes, got {actual}")
            }
            CatalogueEncodingError::UnsupportedVersion { found, expected } => {
                write!(f, "unsupported version: found {found}, expected {expected}")
            }
            CatalogueEncodingError::InvalidTypeTag { tag, context } => {
                write!(f, "invalid type tag {tag} in {context}")
            }
            CatalogueEncodingError::InvalidUtf8 { context } => {
                write!(f, "invalid UTF-8 in {context}")
            }
            CatalogueEncodingError::ProtocolLimit(error) => std::fmt::Display::fmt(error, f),
            CatalogueEncodingError::DecodeError { message } => {
                write!(f, "decode error: {message}")
            }
        }
    }
}

impl std::error::Error for CatalogueEncodingError {}

impl From<PolicyExpressionLimitError> for CatalogueEncodingError {
    fn from(error: PolicyExpressionLimitError) -> Self {
        Self::ProtocolLimit(error)
    }
}

// ============================================================================
// Schema Encoding
// ============================================================================

/// Encode a Schema to binary format.
///
/// Format:
/// ```text
/// [version: u8][table_count: u32][table_1]...[table_n]
/// ```
///
/// Tables are sorted by name for deterministic encoding. Column order within a
/// table is preserved exactly as declared.
pub fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut buf = vec![SCHEMA_VERSION];

    // Sort tables by name for deterministic ordering
    let mut tables: Vec<_> = schema.iter().collect();
    tables.sort_by_key(|(name, _)| name.as_str());

    write_seq(&mut buf, &tables, |buf, (name, table_schema)| {
        write_string(buf, name.as_str());
        SCHEMA_TABLE.put(buf, table_schema);
    });

    buf
}

/// Read a top-level payload's leading format-version byte. `accepted` lists
/// the versions this decoder understands, current first; a rejected version
/// is reported against that current one.
fn read_format_version(data: &[u8], accepted: &[u8]) -> Result<u8, CatalogueEncodingError> {
    let Some(&version) = data.first() else {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: 1,
            actual: 0,
        });
    };
    if !accepted.contains(&version) {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: accepted[0],
        });
    }
    Ok(version)
}

/// Decode a Schema from binary format.
pub fn decode_schema(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    read_format_version(data, &[SCHEMA_VERSION])?;
    let schema = decode_current_schema(data)?;
    ensure_canonical_payload(data, &encode_schema(&schema), "schema")?;
    Ok(schema)
}

/// A table's schema as catalogue payloads store it. Schema payloads also carry
/// the indexed-column set. Composite indexes follow, validated against the
/// columns decoded before them.
struct TableWire {
    indexed: bool,
}

/// A schema payload's table schema carries its indexed-column set.
const SCHEMA_TABLE: TableWire = TableWire { indexed: true };

impl Wire<TableSchema> for TableWire {
    fn put(&self, buf: &mut Vec<u8>, table: &TableSchema) {
        RowWire.put(buf, &table.columns);
        if self.indexed {
            encode_indexed_columns(buf, table.indexed_columns.as_deref());
        }
        BRANCH_BINDINGS.put(buf, &table.branch_by);
        encode_composite_indexes(buf, &table.composite_indexes);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<TableSchema, CatalogueEncodingError> {
        let columns = RowWire.get(data, offset)?;
        let indexed_columns = if self.indexed {
            decode_indexed_columns(data, offset)?
        } else {
            None
        };
        let branch_by = BRANCH_BINDINGS.get(data, offset)?;
        let composite_indexes = decode_composite_indexes(data, offset, &columns)?;
        Ok(TableSchema {
            columns,
            indexed_columns,
            composite_indexes,
            policies: TablePolicies::default(),
            branch_by,
        })
    }
}

const BRANCH_BINDINGS: Unique<Seq<Str>> = Unique(
    Seq("branch_bindings", Str("branch_column")),
    "branch binding",
);

fn encode_composite_indexes(buf: &mut Vec<u8>, indexes: &[Vec<ColumnName>]) {
    let mut sorted = indexes
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|column| column.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    write_seq(buf, &sorted, |buf, group| {
        write_seq(buf, group, |buf, column| write_string(buf, column))
    });
}

fn decode_composite_indexes(
    data: &[u8],
    offset: &mut usize,
    descriptor: &RowDescriptor,
) -> Result<Vec<Vec<ColumnName>>, CatalogueEncodingError> {
    let count = read_count(data, offset, "composite_indexes")?;
    let declared = descriptor
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut groups = Vec::with_capacity(count);
    let mut seen_groups = BTreeSet::new();
    for _ in 0..count {
        let width = read_count(data, offset, "composite_index_columns")?;
        if width < 2 {
            return Err(CatalogueEncodingError::DecodeError {
                message: "composite index needs at least two columns".to_owned(),
            });
        }
        let mut group = Vec::with_capacity(width);
        let mut seen_columns = BTreeSet::new();
        for _ in 0..width {
            let column = read_string(data, offset, "composite_index_column")?;
            let indexable = declared.contains(column.as_str())
                || matches!(column.as_str(), "$createdAt" | "$updatedAt");
            if !indexable || !seen_columns.insert(column.clone()) {
                return Err(CatalogueEncodingError::DecodeError {
                    message: "composite index columns must be distinct declared columns or provenance timestamps".to_owned(),
                });
            }
            group.push(ColumnName::new(column));
        }
        if !seen_groups.insert(
            group
                .iter()
                .map(|column| column.as_str().to_owned())
                .collect::<Vec<_>>(),
        ) {
            return Err(CatalogueEncodingError::DecodeError {
                message: "duplicate composite index".to_owned(),
            });
        }
        groups.push(group);
    }
    Ok(groups)
}

fn encode_indexed_columns(buf: &mut Vec<u8>, indexed_columns: Option<&[ColumnName]>) {
    match indexed_columns {
        None => write_u32(buf, u32::MAX),
        Some(columns) => {
            let mut columns: Vec<_> = columns.iter().map(|column| column.as_str()).collect();
            columns.sort_unstable();
            write_seq(buf, &columns, |buf, column| write_string(buf, column));
        }
    }
}

fn decode_indexed_columns(
    data: &[u8],
    offset: &mut usize,
) -> Result<Option<Vec<ColumnName>>, CatalogueEncodingError> {
    let encoded_count = read_u32(data, offset)?;
    if encoded_count == u32::MAX {
        return Ok(None);
    }
    let count = bound_count(data, offset, encoded_count, "indexed_columns")?;

    let mut columns = Vec::with_capacity(count);
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let column = read_string(data, offset, "indexed_column")?;
        if previous
            .as_ref()
            .is_some_and(|previous| previous.as_bytes() >= column.as_bytes())
        {
            return Err(CatalogueEncodingError::DecodeError {
                message: "indexed columns must be strictly byte-ordered".to_owned(),
            });
        }
        previous = Some(column.clone());
        columns.push(ColumnName::new(column));
    }
    Ok(Some(columns))
}

fn decode_current_schema(data: &[u8]) -> Result<Schema, CatalogueEncodingError> {
    let mut offset = 1;
    let table_count = read_count(data, &mut offset, "schema_tables")?;

    let mut schema = Schema::new();
    for _ in 0..table_count {
        let name = Str("table_name").get(data, &mut offset)?;
        schema.insert(name, SCHEMA_TABLE.get(data, &mut offset)?);
    }

    ensure_consumed(data, offset)?;
    Ok(schema)
}

wire_struct! {
    RowWire: RowDescriptor { columns: COLUMNS }
}

const COLUMNS: Unique<Seq<ColumnWire>> = Unique(
    Seq("row_descriptor_columns", ColumnWire),
    "row descriptor column",
);

wire_struct! {
    ColumnWire: ColumnDescriptor {
        name: Str("column_name"),
        column_type: ColumnTypeWire,
        nullable: Bool,
        references: Opt(Str("column_ref")),
        default: Opt(ValueWire),
        merge_strategy: Opt(MergeStrategyWire),
    }
}

wire_enum! {
    MergeStrategyWire: ColumnMergeStrategy, "column_merge_strategy" {
        1 => Counter,
        2 => GSet,
        3 => Text,
        4 => Markdown,
        5 => Html,
    }
}

wire_enum! {
    ColumnTypeWire: ColumnType, "column_type" {
        1 => Integer,
        2 => BigInt,
        3 => Boolean,
        4 => Text,
        5 => Timestamp,
        6 => Uuid,
        7 => Array { element: Boxed(ColumnTypeWire) },
        8 => Row { columns: Boxed(RowWire) },
        9 => Enum {
            variants: Unique(Seq("enum_variants", Str("enum_variant")), "enum variant"),
        },
        10 => Double,
        11 => Bytea,
        12 => Json { schema: Flag("json_schema_presence", JsonWire) },
        13 => TransactionId,
        14 => EnumPayload {
            cases: Unique(Seq("enum_payload_cases", CaseWire("enum_case")), "enum payload case"),
        },
        15 => ScalarEnum {
            name: Str("scalar_enum_name"),
            variants: Unique(
                Seq("scalar_enum_variants", Str("scalar_enum_variant")),
                "scalar enum variant",
            ),
        },
        16 => CatalogueEnumPayload {
            name: Str("catalogue_enum_payload_name"),
            cases: Unique(
                Seq("catalogue_enum_payload_cases", CaseWire("catalogue_enum_payload_case")),
                "catalogue enum payload case",
            ),
        },
    }
}

/// One payload-enum case: its name (the string names it in decode errors),
/// then its fields.
struct CaseWire(&'static str);

impl Wire<EnumCaseDescriptor> for CaseWire {
    fn put(&self, buf: &mut Vec<u8>, case: &EnumCaseDescriptor) {
        write_string(buf, &case.name);
        COLUMNS.put(buf, &case.fields);
    }

    fn get(
        &self,
        data: &[u8],
        offset: &mut usize,
    ) -> Result<EnumCaseDescriptor, CatalogueEncodingError> {
        Ok(EnumCaseDescriptor {
            name: read_string(data, offset, self.0)?,
            fields: COLUMNS.get(data, offset)?,
        })
    }
}

// ============================================================================
// LensTransform Encoding
// ============================================================================

/// Encode a LensTransform to binary format.
///
/// Format:
/// ```text
/// [version: u8][op_count: u32][ops...][draft_count: u32][draft_indices...]
/// ```
pub fn encode_lens_transform(transform: &LensTransform) -> Vec<u8> {
    let mut buf = vec![LENS_VERSION];
    LensWire.put(&mut buf, transform);
    buf
}

/// Decode a LensTransform from binary format.
pub fn decode_lens_transform(data: &[u8]) -> Result<LensTransform, CatalogueEncodingError> {
    read_format_version(data, &[LENS_VERSION])?;
    let mut offset = 1;
    let transform = LensWire.get(data, &mut offset)?;
    ensure_consumed(data, offset)?;
    ensure_canonical_payload(data, &encode_lens_transform(&transform), "lens")?;
    Ok(transform)
}

wire_struct! {
    LensWire: LensTransform {
        ops: Seq("lens_ops", LensOpWire),
        draft_ops: Seq("lens_draft_ops", U32),
    }
}

wire_enum! {
    LensOpWire: LensOp, "lens_op" {
        1 => AddColumn {
            table: Str("table"),
            column: Str("column"),
            column_type: ColumnTypeWire,
            default: ValueWire,
        },
        2 => RemoveColumn {
            table: Str("table"),
            column: Str("column"),
            column_type: ColumnTypeWire,
            default: ValueWire,
        },
        3 => RenameColumn {
            table: Str("table"),
            old_name: Str("old_name"),
            new_name: Str("new_name"),
        },
        4 => AddTable { table: Str("table"), schema: LENS_TABLE },
        5 => RemoveTable { table: Str("table"), schema: LENS_TABLE },
        6 => RenameTable { old_name: Str("old_name"), new_name: Str("new_name") },
    }
}

/// A lens op's table schema carries no indexed-column set.
const LENS_TABLE: TableWire = TableWire { indexed: false };

// ============================================================================
// Canonical nested payload codec
// ============================================================================

// JSON Schema declarations and relation-backed policies are restart-authoritative
// nested values.  They deliberately share this small, explicit v1 algebra instead
// of delegating their bytes to serde.  The outer schema/lens/permissions envelope
// supplies the storage family; this marker freezes the nested record/enum grammar.
const NESTED_CODEC_VERSION: u8 = 1;

const JSON_NULL: u8 = 1;
const JSON_FALSE: u8 = 2;
const JSON_TRUE: u8 = 3;
const JSON_NUMBER: u8 = 4;
const JSON_STRING: u8 = 5;
const JSON_ARRAY: u8 = 6;
const JSON_OBJECT: u8 = 7;

/// A JSON Schema declaration in the canonical nested grammar.
struct JsonWire;

impl Wire<serde_json::Value> for JsonWire {
    fn put(&self, buf: &mut Vec<u8>, value: &serde_json::Value) {
        buf.push(NESTED_CODEC_VERSION);
        encode_canonical_json_value_body(buf, value);
    }

    fn get(
        &self,
        data: &[u8],
        offset: &mut usize,
    ) -> Result<serde_json::Value, CatalogueEncodingError> {
        let start = *offset;
        let version = read_u8(data, offset)?;
        if version != NESTED_CODEC_VERSION {
            return Err(CatalogueEncodingError::UnsupportedVersion {
                found: version,
                expected: NESTED_CODEC_VERSION,
            });
        }
        let value = decode_canonical_json_value_body(data, offset)?;
        let mut canonical = Vec::new();
        self.put(&mut canonical, &value);
        ensure_canonical_segment(data, start, *offset, &canonical, "json schema")?;
        Ok(value)
    }
}

fn encode_canonical_json_value_body(buf: &mut Vec<u8>, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => buf.push(JSON_NULL),
        serde_json::Value::Bool(false) => buf.push(JSON_FALSE),
        serde_json::Value::Bool(true) => buf.push(JSON_TRUE),
        serde_json::Value::Number(number) => {
            buf.push(JSON_NUMBER);
            // serde_json::Number has one normalized textual representation after
            // parsing.  Store that exact primitive rather than JSON object bytes.
            write_string(buf, &number.to_string());
        }
        serde_json::Value::String(value) => {
            buf.push(JSON_STRING);
            write_string(buf, value);
        }
        serde_json::Value::Array(values) => {
            buf.push(JSON_ARRAY);
            write_seq(buf, values, encode_canonical_json_value_body);
        }
        serde_json::Value::Object(values) => {
            buf.push(JSON_OBJECT);
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            write_seq(buf, &entries, |buf, (key, value)| {
                write_string(buf, key);
                encode_canonical_json_value_body(buf, value);
            });
        }
    }
}

fn decode_canonical_json_value_body(
    data: &[u8],
    offset: &mut usize,
) -> Result<serde_json::Value, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        JSON_NULL => Ok(serde_json::Value::Null),
        JSON_FALSE => Ok(serde_json::Value::Bool(false)),
        JSON_TRUE => Ok(serde_json::Value::Bool(true)),
        JSON_NUMBER => {
            let source = read_string(data, offset, "json_number")?;
            let number = serde_json::from_str::<serde_json::Number>(&source).map_err(|error| {
                CatalogueEncodingError::DecodeError {
                    message: format!("invalid canonical JSON number: {error}"),
                }
            })?;
            if number.to_string() != source {
                return Err(CatalogueEncodingError::DecodeError {
                    message: "non-canonical JSON number spelling".to_owned(),
                });
            }
            Ok(serde_json::Value::Number(number))
        }
        JSON_STRING => Ok(serde_json::Value::String(read_string(
            data,
            offset,
            "json_string",
        )?)),
        JSON_ARRAY => Ok(serde_json::Value::Array(read_seq(
            data,
            offset,
            "json_array",
            decode_canonical_json_value_body,
        )?)),
        JSON_OBJECT => {
            let count = read_count(data, offset, "json_object")?;
            let mut values = serde_json::Map::new();
            let mut previous_key: Option<String> = None;
            for _ in 0..count {
                let key = read_string(data, offset, "json_object_key")?;
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous.as_bytes() >= key.as_bytes())
                {
                    return Err(CatalogueEncodingError::DecodeError {
                        message: "JSON object keys must be strictly byte-ordered".to_owned(),
                    });
                }
                let value = decode_canonical_json_value_body(data, offset)?;
                previous_key = Some(key.clone());
                values.insert(key, value);
            }
            Ok(serde_json::Value::Object(values))
        }
        tag => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "canonical_json_value",
        }),
    }
}

const REL_FILTER: u8 = 2;
const REL_UNION: u8 = 3;
const REL_JOIN: u8 = 4;
const REL_PROJECT: u8 = 5;
const REL_GATHER: u8 = 6;
/// Version-one labeled UNION ALL grammar: count, canonical labels, then arms.
/// The former unlabeled `REL_UNION` spelling is rejected rather than assigning
/// traversal-derived labels to persisted policy expressions.
const REL_UNION_LABELED: u8 = 7;
const MAX_RELATION_UNION_ARM_LABEL_BYTES: usize = 4096;

const REL_PREDICATE_ENUM_MATCH: u8 = 6;
const REL_PREDICATE_AND: u8 = 7;
const REL_PREDICATE_OR: u8 = 8;
const REL_PREDICATE_NOT: u8 = 9;
fn encode_canonical_relation_expr(buf: &mut Vec<u8>, rel: &RelationExpr) {
    buf.push(NESTED_CODEC_VERSION);
    encode_relation_expr_body(buf, rel);
}

/// Validate every relation-IR arm and literal before an infallible catalogue
/// encoder is allowed to hash or persist it. Policies use a subset of the query
/// relation IR: no output steps, query parameters or frontier columns, and
/// literals are typed public values. Server callers can publish `TablePolicies`
/// directly, bypassing core-schema conversion, so this is the admission
/// boundary for the durable grammar as well as a decoder defense.
pub fn validate_permissions_relations(
    permissions: &HashMap<TableName, TablePolicies>,
) -> Result<(), CatalogueEncodingError> {
    for policies in permissions.values() {
        for policy in [
            policies.select.using.as_ref(),
            policies.insert.with_check.as_ref(),
            policies.update.using.as_ref(),
            policies.update.with_check.as_ref(),
            policies.delete.using.as_ref(),
            policies.delete.with_check.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_policy_relations(policy)?;
        }
    }
    Ok(())
}

fn validate_policy_relations(policy: &PolicyExpr) -> Result<(), CatalogueEncodingError> {
    match policy {
        PolicyExpr::ExistsRel { rel } => validate_relation_policy(rel),
        PolicyExpr::Exists { condition, .. } | PolicyExpr::Not(condition) => {
            validate_policy_relations(condition)
        }
        PolicyExpr::And(expressions) | PolicyExpr::Or(expressions) => {
            for expression in expressions {
                validate_policy_relations(expression)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_relation_policy(rel: &RelationExpr) -> Result<(), CatalogueEncodingError> {
    match rel {
        RelationExpr::TableScan { .. } => Ok(()),
        RelationExpr::Filter { input, predicate } => {
            validate_relation_policy_predicate(predicate)?;
            validate_relation_policy(input)
        }
        RelationExpr::Project { input, .. } => validate_relation_policy(input),
        RelationExpr::Distinct { .. }
        | RelationExpr::OrderBy { .. }
        | RelationExpr::Offset { .. }
        | RelationExpr::Limit { .. } => Err(CatalogueEncodingError::DecodeError {
            message: "relation policies do not support Distinct, OrderBy, Offset or Limit"
                .to_owned(),
        }),
        RelationExpr::Union { inputs } => {
            let mut labels = BTreeSet::new();
            for arm in inputs {
                if arm.label.is_empty()
                    || arm.label.len() > MAX_RELATION_UNION_ARM_LABEL_BYTES
                    || arm.label.contains('\0')
                    || !labels.insert(&arm.label)
                {
                    return Err(CatalogueEncodingError::DecodeError {
                        message:
                            "relation union labels must be unique, NUL-free, and 1..=4096 bytes"
                                .to_owned(),
                    });
                }
                validate_relation_policy(&arm.input)?;
            }
            Ok(())
        }
        RelationExpr::Join { left, right, .. } => {
            validate_relation_policy(left)?;
            validate_relation_policy(right)
        }
        RelationExpr::Gather { seed, step, .. } => {
            validate_relation_policy(seed)?;
            validate_relation_policy(step)
        }
    }
}

fn validate_relation_policy_predicate(
    predicate: &RelationPredicate,
) -> Result<(), CatalogueEncodingError> {
    match predicate {
        RelationPredicate::Cmp { right, .. } | RelationPredicate::Contains { right, .. } => {
            validate_relation_policy_value(right)
        }
        RelationPredicate::In { values, .. } => {
            values.iter().try_for_each(validate_relation_policy_value)
        }
        RelationPredicate::EnumMatch { payload, .. } | RelationPredicate::Not(payload) => {
            validate_relation_policy_predicate(payload)
        }
        RelationPredicate::And(predicates) | RelationPredicate::Or(predicates) => predicates
            .iter()
            .try_for_each(validate_relation_policy_predicate),
        RelationPredicate::IsNull { .. }
        | RelationPredicate::IsNotNull { .. }
        | RelationPredicate::True
        | RelationPredicate::False => Ok(()),
    }
}

fn validate_relation_policy_value(value: &RelationValueRef) -> Result<(), CatalogueEncodingError> {
    let message = match value {
        RelationValueRef::Literal(literal) => match relation_policy_literal(literal) {
            Ok(_) => return Ok(()),
            Err(error) => format!("relation policy literals must be typed values: {error}"),
        },
        RelationValueRef::Param(_) => {
            "relation policies do not support query parameters".to_owned()
        }
        RelationValueRef::FrontierColumn(_) => {
            "relation policies do not support frontier column operands".to_owned()
        }
        RelationValueRef::SessionRef(_)
        | RelationValueRef::OuterColumn(_)
        | RelationValueRef::RowId(_) => return Ok(()),
    };
    Err(CatalogueEncodingError::DecodeError { message })
}

fn relation_policy_literal(literal: &serde_json::Value) -> Result<Value, serde_json::Error> {
    serde_json::from_value(literal.clone())
}

fn encode_relation_expr_body(buf: &mut Vec<u8>, rel: &RelationExpr) {
    let mut tasks = vec![RelationEncodeTask::Expr(rel)];
    while let Some(task) = tasks.pop() {
        match task {
            RelationEncodeTask::Expr(leaf @ RelationExpr::TableScan { .. }) => {
                RelationLeafWire.put_variant(buf, leaf)
            }
            RelationEncodeTask::Expr(RelationExpr::Filter { input, predicate }) => {
                buf.push(REL_FILTER);
                tasks.push(RelationEncodeTask::Pred(predicate));
                tasks.push(RelationEncodeTask::Expr(input));
            }
            RelationEncodeTask::Expr(RelationExpr::Union { inputs }) => {
                buf.push(REL_UNION_LABELED);
                write_seq(buf, inputs, |buf, arm| {
                    assert!(
                        !arm.label.is_empty()
                            && arm.label.len() <= MAX_RELATION_UNION_ARM_LABEL_BYTES
                            && !arm.label.contains('\0'),
                        "relation union arm labels must be 1..=4096 bytes and NUL-free"
                    );
                    write_string(buf, &arm.label);
                });
                for arm in inputs.iter().rev() {
                    tasks.push(RelationEncodeTask::Expr(&arm.input));
                }
            }
            RelationEncodeTask::Expr(RelationExpr::Join {
                left,
                right,
                on,
                join_kind,
            }) => {
                buf.push(REL_JOIN);
                tasks.push(RelationEncodeTask::JoinTail(on, join_kind));
                tasks.push(RelationEncodeTask::Expr(right));
                tasks.push(RelationEncodeTask::Expr(left));
            }
            RelationEncodeTask::Expr(RelationExpr::Project { input, columns }) => {
                buf.push(REL_PROJECT);
                tasks.push(RelationEncodeTask::ProjectTail(columns));
                tasks.push(RelationEncodeTask::Expr(input));
            }
            RelationEncodeTask::Expr(RelationExpr::Gather {
                seed,
                step,
                frontier_key,
                bound,
                dedupe_key,
            }) => {
                buf.push(REL_GATHER);
                tasks.push(RelationEncodeTask::GatherTail(
                    frontier_key,
                    bound,
                    dedupe_key,
                ));
                tasks.push(RelationEncodeTask::Expr(step));
                tasks.push(RelationEncodeTask::Expr(seed));
            }
            RelationEncodeTask::Expr(
                RelationExpr::Distinct { .. }
                | RelationExpr::OrderBy { .. }
                | RelationExpr::Offset { .. }
                | RelationExpr::Limit { .. },
            ) => unreachable!("validated relation policies have no output steps"),
            RelationEncodeTask::Pred(pred) => encode_relation_predicate_task(buf, pred, &mut tasks),
            RelationEncodeTask::JoinTail(on, kind) => {
                JOIN_CONDITIONS.put(buf, on);
                JoinKindWire.put(buf, kind);
            }
            RelationEncodeTask::ProjectTail(columns) => PROJECT_COLUMNS.put(buf, columns),
            RelationEncodeTask::GatherTail(key, bound, keys) => {
                KeyRefWire.put(buf, key);
                BoundWire.put(buf, bound);
                DEDUPE_KEYS.put(buf, keys);
            }
        }
    }
}

enum RelationEncodeTask<'a> {
    Expr(&'a RelationExpr),
    Pred(&'a RelationPredicate),
    JoinTail(&'a Vec<RelationJoinCondition>, &'a RelationJoinKind),
    ProjectTail(&'a Vec<RelationProjectColumn>),
    GatherTail(
        &'a RelationKeyRef,
        &'a RecursionBound,
        &'a Vec<RelationKeyRef>,
    ),
}

fn decode_canonical_relation_expr(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
) -> Result<RelationExpr, CatalogueEncodingError> {
    let start = *offset;
    let version = read_u8(data, offset)?;
    if version != NESTED_CODEC_VERSION {
        return Err(CatalogueEncodingError::UnsupportedVersion {
            found: version,
            expected: NESTED_CODEC_VERSION,
        });
    }
    let rel = decode_relation_expr_body_iterative(data, offset, budget)?;
    let mut canonical = Vec::new();
    encode_canonical_relation_expr(&mut canonical, &rel);
    ensure_canonical_segment(data, start, *offset, &canonical, "relation policy")?;
    Ok(rel)
}

enum RelationDecodeTask {
    Expr(usize),
    Pred(usize),
    FilterDone,
    UnionDone(Vec<String>),
    JoinTail,
    JoinDone,
    ProjectTail,
    ProjectDone,
    GatherTail,
    GatherDone,
    EnumDone(RelationColumnRef, String),
    AndDone(usize),
    OrDone(usize),
    NotDone,
}
enum RelationDecodeValue {
    Expr(RelationExpr),
    Pred(RelationPredicate),
    Join(Vec<RelationJoinCondition>, RelationJoinKind),
    Project(Vec<RelationProjectColumn>),
    Gather(RelationKeyRef, RecursionBound, Vec<RelationKeyRef>),
}

fn decode_relation_expr_body_iterative(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
) -> Result<RelationExpr, CatalogueEncodingError> {
    let mut tasks = vec![RelationDecodeTask::Expr(1)];
    let mut values = Vec::new();
    while let Some(task) = tasks.pop() {
        match task {
            RelationDecodeTask::Expr(depth) => {
                budget.enter(depth)?;
                let tag = read_u8(data, offset)?;
                if let Some(leaf) = RelationLeafWire.get_variant(tag, data, offset)? {
                    values.push(RelationDecodeValue::Expr(leaf));
                    continue;
                }
                match tag {
                    REL_FILTER => {
                        budget.reserve(1)?;
                        tasks.push(RelationDecodeTask::FilterDone);
                        tasks.push(RelationDecodeTask::Pred(depth + 1));
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                    }
                    REL_UNION => {
                        return Err(CatalogueEncodingError::DecodeError {
                            message: "unlabeled relation union grammar is unsupported".to_owned(),
                        });
                    }
                    REL_UNION_LABELED => {
                        let count = read_count(data, offset, "relation_union")?;
                        budget.reserve(count)?;
                        let mut labels = Vec::with_capacity(count);
                        let mut seen = BTreeSet::new();
                        for _ in 0..count {
                            let label = read_string(data, offset, "relation_union_label")?;
                            if label.is_empty()
                                || label.len() > MAX_RELATION_UNION_ARM_LABEL_BYTES
                                || label.contains('\0')
                                || !seen.insert(label.clone())
                            {
                                return Err(CatalogueEncodingError::DecodeError {
                                    message: "relation union labels must be unique, NUL-free, and 1..=4096 bytes".to_owned(),
                                });
                            }
                            labels.push(label);
                        }
                        tasks.push(RelationDecodeTask::UnionDone(labels));
                        for _ in 0..count {
                            tasks.push(RelationDecodeTask::Expr(depth + 1));
                        }
                    }
                    REL_JOIN => {
                        budget.reserve(2)?;
                        tasks.push(RelationDecodeTask::JoinDone);
                        tasks.push(RelationDecodeTask::JoinTail);
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                    }
                    REL_PROJECT => {
                        budget.reserve(1)?;
                        tasks.push(RelationDecodeTask::ProjectDone);
                        tasks.push(RelationDecodeTask::ProjectTail);
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                    }
                    REL_GATHER => {
                        budget.reserve(2)?;
                        tasks.push(RelationDecodeTask::GatherDone);
                        tasks.push(RelationDecodeTask::GatherTail);
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                        tasks.push(RelationDecodeTask::Expr(depth + 1));
                    }
                    tag => {
                        return Err(CatalogueEncodingError::InvalidTypeTag {
                            tag,
                            context: "relation_expr",
                        });
                    }
                }
            }
            RelationDecodeTask::Pred(depth) => decode_relation_predicate_task_iterative(
                data,
                offset,
                budget,
                depth,
                &mut tasks,
                &mut values,
            )?,
            RelationDecodeTask::FilterDone => {
                let predicate = pop_pred(&mut values)?;
                let input = pop_expr(&mut values)?;
                values.push(RelationDecodeValue::Expr(RelationExpr::Filter {
                    input: Box::new(input),
                    predicate,
                }));
            }
            RelationDecodeTask::UnionDone(labels) => {
                let mut inputs = Vec::with_capacity(labels.len());
                for label in labels.into_iter().rev() {
                    inputs.push(RelationUnionArm {
                        label,
                        input: pop_expr(&mut values)?,
                    });
                }
                inputs.reverse();
                values.push(RelationDecodeValue::Expr(RelationExpr::Union { inputs }));
            }
            RelationDecodeTask::JoinTail => values.push(RelationDecodeValue::Join(
                JOIN_CONDITIONS.get(data, offset)?,
                JoinKindWire.get(data, offset)?,
            )),
            RelationDecodeTask::JoinDone => {
                let (on, join_kind) = pop_join(&mut values)?;
                let right = pop_expr(&mut values)?;
                let left = pop_expr(&mut values)?;
                values.push(RelationDecodeValue::Expr(RelationExpr::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                    join_kind,
                }));
            }
            RelationDecodeTask::ProjectTail => values.push(RelationDecodeValue::Project(
                PROJECT_COLUMNS.get(data, offset)?,
            )),
            RelationDecodeTask::ProjectDone => {
                let columns = pop_project(&mut values)?;
                let input = pop_expr(&mut values)?;
                values.push(RelationDecodeValue::Expr(RelationExpr::Project {
                    input: Box::new(input),
                    columns,
                }));
            }
            RelationDecodeTask::GatherTail => values.push(RelationDecodeValue::Gather(
                KeyRefWire.get(data, offset)?,
                BoundWire.get(data, offset)?,
                DEDUPE_KEYS.get(data, offset)?,
            )),
            RelationDecodeTask::GatherDone => {
                let (frontier_key, bound, dedupe_key) = pop_gather(&mut values)?;
                let step = pop_expr(&mut values)?;
                let seed = pop_expr(&mut values)?;
                values.push(RelationDecodeValue::Expr(RelationExpr::Gather {
                    seed: Box::new(seed),
                    step: Box::new(step),
                    frontier_key,
                    bound,
                    dedupe_key,
                }));
            }
            RelationDecodeTask::EnumDone(column, case) => {
                let payload = pop_pred(&mut values)?;
                values.push(RelationDecodeValue::Pred(RelationPredicate::EnumMatch {
                    column,
                    case,
                    payload: Box::new(payload),
                }));
            }
            RelationDecodeTask::AndDone(count) => {
                let mut expressions = Vec::with_capacity(count);
                for _ in 0..count {
                    expressions.push(pop_pred(&mut values)?);
                }
                expressions.reverse();
                values.push(RelationDecodeValue::Pred(RelationPredicate::And(
                    expressions,
                )));
            }
            RelationDecodeTask::OrDone(count) => {
                let mut expressions = Vec::with_capacity(count);
                for _ in 0..count {
                    expressions.push(pop_pred(&mut values)?);
                }
                expressions.reverse();
                values.push(RelationDecodeValue::Pred(RelationPredicate::Or(
                    expressions,
                )));
            }
            RelationDecodeTask::NotDone => {
                let input = pop_pred(&mut values)?;
                values.push(RelationDecodeValue::Pred(RelationPredicate::Not(Box::new(
                    input,
                ))));
            }
        }
    }
    let relation = pop_expr(&mut values)?;
    if values.is_empty() {
        Ok(relation)
    } else {
        Err(relation_frame_error())
    }
}

fn decode_relation_predicate_task_iterative(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
    depth: usize,
    tasks: &mut Vec<RelationDecodeTask>,
    values: &mut Vec<RelationDecodeValue>,
) -> Result<(), CatalogueEncodingError> {
    budget.enter(depth)?;
    let tag = read_u8(data, offset)?;
    if let Some(leaf) = PredicateLeafWire.get_variant(tag, data, offset)? {
        values.push(RelationDecodeValue::Pred(leaf));
        return Ok(());
    }
    match tag {
        REL_PREDICATE_ENUM_MATCH => {
            let column = ColumnRefWire.get(data, offset)?;
            let case = read_string(data, offset, "relation_enum_case")?;
            budget.reserve(1)?;
            tasks.push(RelationDecodeTask::EnumDone(column, case));
            tasks.push(RelationDecodeTask::Pred(depth + 1));
        }
        REL_PREDICATE_AND => {
            let count = read_count(data, offset, "relation_predicate_and")?;
            budget.reserve(count)?;
            tasks.push(RelationDecodeTask::AndDone(count));
            for _ in 0..count {
                tasks.push(RelationDecodeTask::Pred(depth + 1));
            }
        }
        REL_PREDICATE_OR => {
            let count = read_count(data, offset, "relation_predicate_or")?;
            budget.reserve(count)?;
            tasks.push(RelationDecodeTask::OrDone(count));
            for _ in 0..count {
                tasks.push(RelationDecodeTask::Pred(depth + 1));
            }
        }
        REL_PREDICATE_NOT => {
            budget.reserve(1)?;
            tasks.push(RelationDecodeTask::NotDone);
            tasks.push(RelationDecodeTask::Pred(depth + 1));
        }
        tag => {
            return Err(CatalogueEncodingError::InvalidTypeTag {
                tag,
                context: "relation_predicate",
            });
        }
    }
    Ok(())
}

fn relation_frame_error() -> CatalogueEncodingError {
    CatalogueEncodingError::DecodeError {
        message: "invalid relation decoder frame order".to_owned(),
    }
}
fn pop_expr(values: &mut Vec<RelationDecodeValue>) -> Result<RelationExpr, CatalogueEncodingError> {
    match values.pop() {
        Some(RelationDecodeValue::Expr(value)) => Ok(value),
        _ => Err(relation_frame_error()),
    }
}
fn pop_pred(
    values: &mut Vec<RelationDecodeValue>,
) -> Result<RelationPredicate, CatalogueEncodingError> {
    match values.pop() {
        Some(RelationDecodeValue::Pred(value)) => Ok(value),
        _ => Err(relation_frame_error()),
    }
}
fn pop_join(
    values: &mut Vec<RelationDecodeValue>,
) -> Result<(Vec<RelationJoinCondition>, RelationJoinKind), CatalogueEncodingError> {
    match values.pop() {
        Some(RelationDecodeValue::Join(on, kind)) => Ok((on, kind)),
        _ => Err(relation_frame_error()),
    }
}
fn pop_project(
    values: &mut Vec<RelationDecodeValue>,
) -> Result<Vec<RelationProjectColumn>, CatalogueEncodingError> {
    match values.pop() {
        Some(RelationDecodeValue::Project(value)) => Ok(value),
        _ => Err(relation_frame_error()),
    }
}
fn pop_gather(
    values: &mut Vec<RelationDecodeValue>,
) -> Result<(RelationKeyRef, RecursionBound, Vec<RelationKeyRef>), CatalogueEncodingError> {
    match values.pop() {
        Some(RelationDecodeValue::Gather(key, bound, keys)) => Ok((key, bound, keys)),
        _ => Err(relation_frame_error()),
    }
}

fn encode_relation_predicate_task<'a>(
    buf: &mut Vec<u8>,
    predicate: &'a RelationPredicate,
    tasks: &mut Vec<RelationEncodeTask<'a>>,
) {
    match predicate {
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            buf.push(REL_PREDICATE_ENUM_MATCH);
            ColumnRefWire.put(buf, column);
            write_string(buf, case);
            tasks.push(RelationEncodeTask::Pred(payload));
        }
        RelationPredicate::And(expressions) => {
            buf.push(REL_PREDICATE_AND);
            write_u32(buf, expressions.len() as u32);
            for expression in expressions.iter().rev() {
                tasks.push(RelationEncodeTask::Pred(expression));
            }
        }
        RelationPredicate::Or(expressions) => {
            buf.push(REL_PREDICATE_OR);
            write_u32(buf, expressions.len() as u32);
            for expression in expressions.iter().rev() {
                tasks.push(RelationEncodeTask::Pred(expression));
            }
        }
        RelationPredicate::Not(expression) => {
            buf.push(REL_PREDICATE_NOT);
            tasks.push(RelationEncodeTask::Pred(expression));
        }
        leaf @ (RelationPredicate::Cmp { .. }
        | RelationPredicate::IsNull { .. }
        | RelationPredicate::IsNotNull { .. }
        | RelationPredicate::In { .. }
        | RelationPredicate::Contains { .. }
        | RelationPredicate::True
        | RelationPredicate::False) => PredicateLeafWire.put_variant(buf, leaf),
    }
}

wire_enum! {
    partial RelationLeafWire: RelationExpr {
        1 => TableScan {
            table: Str("relation_table"),
            alias: Flag("relation_alias", Str("relation_alias")),
        },
    }
}

wire_enum! {
    partial PredicateLeafWire: RelationPredicate {
        1 => Cmp { left: ColumnRefWire, op: RelCmpOpWire, right: ValueRefWire },
        2 => IsNull { column: ColumnRefWire },
        3 => IsNotNull { column: ColumnRefWire },
        4 => In { left: ColumnRefWire, values: Seq("relation_predicate_in", ValueRefWire) },
        5 => Contains { left: ColumnRefWire, right: ValueRefWire },
        10 => True,
        11 => False,
    }
}

wire_struct! {
    ColumnRefWire: RelationColumnRef {
        scope: Flag("relation_column_scope", Str("relation_column_scope")),
        column: Str("relation_column"),
    }
}

wire_enum! {
    partial ValueRefWire: RelationValueRef {
        1 => Literal(value: PolicyLiteralWire),
        2 => SessionRef(path: strings("relation_session_path")),
        3 => OuterColumn(column: ColumnRefWire),
        4 => RowId(row_id: RowIdWire),
    }
}

impl Wire<RelationValueRef> for ValueRefWire {
    fn put(&self, buf: &mut Vec<u8>, value: &RelationValueRef) {
        match value {
            RelationValueRef::Param(_) | RelationValueRef::FrontierColumn(_) => {
                unreachable!("validated relation policies have no parameters or frontier columns")
            }
            listed @ (RelationValueRef::Literal(_)
            | RelationValueRef::SessionRef(_)
            | RelationValueRef::OuterColumn(_)
            | RelationValueRef::RowId(_)) => self.put_variant(buf, listed),
        }
    }

    fn get(
        &self,
        data: &[u8],
        offset: &mut usize,
    ) -> Result<RelationValueRef, CatalogueEncodingError> {
        let tag = read_u8(data, offset)?;
        self.get_variant(tag, data, offset)?
            .ok_or(CatalogueEncodingError::InvalidTypeTag {
                tag,
                context: "relation_value_ref",
            })
    }
}

/// A relation policy literal: a typed public value in its JSON envelope,
/// stored as that value.
struct PolicyLiteralWire;

impl Wire<serde_json::Value> for PolicyLiteralWire {
    fn put(&self, buf: &mut Vec<u8>, literal: &serde_json::Value) {
        let value = relation_policy_literal(literal)
            .expect("validated relation policy literals are typed values");
        ValueWire.put(buf, &value);
    }

    fn get(
        &self,
        data: &[u8],
        offset: &mut usize,
    ) -> Result<serde_json::Value, CatalogueEncodingError> {
        Ok(serde_json::to_value(ValueWire.get(data, offset)?)
            .expect("public values serialize to JSON"))
    }
}

wire_enum! {
    RelCmpOpWire: RelationCmpOp, "relation_cmp_op" {
        1 => Eq,
        2 => Ne,
        3 => Lt,
        4 => Le,
        5 => Gt,
        6 => Ge,
    }
}

wire_enum! {
    RowIdWire: RelationRowIdRef, "relation_row_id" {
        1 => Current,
        2 => Outer,
        3 => Frontier,
    }
}

wire_enum! {
    JoinKindWire: RelationJoinKind, "relation_join_kind" {
        1 => Inner,
        2 => Left,
    }
}

wire_enum! {
    KeyRefWire: RelationKeyRef, "relation_key_ref" {
        1 => Column(column: ColumnRefWire),
        2 => RowId(row_id: RowIdWire),
    }
}

wire_enum! {
    ProjectExprWire: RelationProjectExpr, "relation_project_expr" {
        1 => Column(column: ColumnRefWire),
        2 => RowId(row_id: RowIdWire),
    }
}

wire_enum! {
    BoundWire: RecursionBound, "relation_recursion_bound" {
        1 => Fixpoint,
        2 => MaxDepth(depth: Depth),
    }
}

/// A recursion depth, stored as a `u64`.
struct Depth;

impl Wire<usize> for Depth {
    fn put(&self, buf: &mut Vec<u8>, value: &usize) {
        write_u64(buf, *value as u64);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<usize, CatalogueEncodingError> {
        usize::try_from(read_u64(data, offset)?).map_err(|_| CatalogueEncodingError::DecodeError {
            message: "relation recursion depth exceeds platform usize".to_owned(),
        })
    }
}

wire_struct! {
    JoinConditionWire: RelationJoinCondition { left: ColumnRefWire, right: ColumnRefWire }
}

wire_struct! {
    ProjectColumnWire: RelationProjectColumn {
        alias: Str("relation_project_alias"),
        expr: ProjectExprWire,
    }
}

const JOIN_CONDITIONS: Seq<JoinConditionWire> = Seq("relation_join_conditions", JoinConditionWire);
const PROJECT_COLUMNS: Seq<ProjectColumnWire> = Seq("relation_project_columns", ProjectColumnWire);
const DEDUPE_KEYS: Seq<KeyRefWire> = Seq("relation_dedupe_keys", KeyRefWire);

// ============================================================================
// Policy Encoding
// ============================================================================

const POLICY_EXPR_EXISTS: u8 = 5;
const POLICY_EXPR_INHERITS: u8 = 6;
const POLICY_EXPR_AND: u8 = 7;
const POLICY_EXPR_OR: u8 = 8;
const POLICY_EXPR_NOT: u8 = 9;
const POLICY_EXPR_INHERITS_WITH_DEPTH: u8 = 12;
const POLICY_EXPR_EXISTS_REL: u8 = 13;

fn encode_table_policies(buf: &mut Vec<u8>, policies: &TablePolicies) {
    for operation in [
        &policies.select,
        &policies.insert,
        &policies.update,
        &policies.delete,
    ] {
        write_opt(buf, operation.using.as_ref(), encode_policy_expr);
        write_opt(buf, operation.with_check.as_ref(), encode_policy_expr);
    }
}

fn decode_table_policies(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
) -> Result<TablePolicies, CatalogueEncodingError> {
    let mut expr = || {
        read_opt(data, offset, |data, offset| {
            decode_policy_expr(data, offset, budget)
        })
    };
    let mut operation = || -> Result<OperationPolicy, CatalogueEncodingError> {
        Ok(OperationPolicy {
            using: expr()?,
            with_check: expr()?,
        })
    };
    Ok(TablePolicies {
        select: operation()?,
        insert: operation()?,
        update: operation()?,
        delete: operation()?,
    })
}

pub fn encode_permissions(permissions: &HashMap<TableName, TablePolicies>) -> Vec<u8> {
    let mut buf = vec![PERMISSIONS_VERSION];
    let mut entries: Vec<_> = permissions.iter().collect();
    entries.sort_by_key(|(name, _)| name.as_str());
    write_seq(&mut buf, &entries, |buf, (table_name, policies)| {
        write_string(buf, table_name.as_str());
        encode_table_policies(buf, policies);
    });
    buf
}

pub fn decode_permissions(
    data: &[u8],
) -> Result<HashMap<TableName, TablePolicies>, CatalogueEncodingError> {
    read_format_version(data, &[PERMISSIONS_VERSION])?;
    let mut offset = 1;
    let table_count = read_count(data, &mut offset, "permissions_tables")?;
    let mut permissions = HashMap::new();
    let mut budget = PolicyExpressionBudget::default();

    for _ in 0..table_count {
        let table_name = Str("table_name").get(data, &mut offset)?;
        let policies = decode_table_policies(data, &mut offset, &mut budget)?;
        permissions.insert(table_name, policies);
    }

    ensure_consumed(data, offset)?;
    ensure_canonical_payload(data, &encode_permissions(&permissions), "permissions")?;
    Ok(permissions)
}

/// The schema hash, version and parent bundle that open both a permissions
/// bundle and a permissions head.
const PERMISSIONS_HEADER: (HashWire, Le, Opt<Oid>) = (
    HashWire,
    Le,
    Opt(Oid("parent permissions bundle object id")),
);

pub fn encode_permissions_bundle(
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Vec<u8> {
    let mut buf = vec![PERMISSIONS_BUNDLE_VERSION];
    PERMISSIONS_HEADER.put(&mut buf, &(schema_hash, version, parent_bundle_object_id));
    Raw.put(&mut buf, &encode_permissions(permissions));
    buf
}

type DecodedPermissionsBundle = (
    SchemaHash,
    u64,
    Option<ObjectId>,
    HashMap<TableName, TablePolicies>,
);

pub fn decode_permissions_bundle(
    data: &[u8],
) -> Result<DecodedPermissionsBundle, CatalogueEncodingError> {
    read_format_version(data, &[PERMISSIONS_BUNDLE_VERSION])?;
    let mut offset = 1;
    let (schema_hash, version, parent) = PERMISSIONS_HEADER.get(data, &mut offset)?;
    let payload: Vec<u8> = Raw.get(data, &mut offset)?;
    let permissions = decode_permissions(&payload)?;
    ensure_consumed(data, offset)?;
    ensure_canonical_payload(
        data,
        &encode_permissions_bundle(schema_hash, version, parent, &permissions),
        "permissions bundle",
    )?;
    Ok((schema_hash, version, parent, permissions))
}

const PERMISSIONS_HEAD: (HashWire, Le, Opt<Oid>, Oid) = (
    HashWire,
    Le,
    Opt(Oid("parent permissions bundle object id")),
    Oid("permissions bundle object id"),
);

pub fn encode_permissions_head(
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    bundle_object_id: ObjectId,
) -> Vec<u8> {
    let mut buf = vec![PERMISSIONS_HEAD_VERSION];
    let head = (
        schema_hash,
        version,
        parent_bundle_object_id,
        bundle_object_id,
    );
    PERMISSIONS_HEAD.put(&mut buf, &head);
    buf
}

type DecodedPermissionsHead = (SchemaHash, u64, Option<ObjectId>, ObjectId);

pub fn decode_permissions_head(
    data: &[u8],
) -> Result<DecodedPermissionsHead, CatalogueEncodingError> {
    read_format_version(data, &[PERMISSIONS_HEAD_VERSION])?;
    let mut offset = 1;
    let decoded = PERMISSIONS_HEAD.get(data, &mut offset)?;
    ensure_consumed(data, offset)?;
    ensure_canonical_payload(
        data,
        &encode_permissions_head(decoded.0, decoded.1, decoded.2, decoded.3),
        "permissions head",
    )?;
    Ok(decoded)
}

fn encode_policy_expr(buf: &mut Vec<u8>, expr: &PolicyExpr) {
    match expr {
        PolicyExpr::Exists { table, condition } => {
            buf.push(POLICY_EXPR_EXISTS);
            write_string(buf, table);
            encode_policy_expr(buf, condition);
        }
        PolicyExpr::ExistsRel { rel } => {
            buf.push(POLICY_EXPR_EXISTS_REL);
            encode_canonical_relation_expr(buf, rel);
        }
        // Two tags, chosen by whether a depth is present, so coded by hand.
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => {
            buf.push(if max_depth.is_some() {
                POLICY_EXPR_INHERITS_WITH_DEPTH
            } else {
                POLICY_EXPR_INHERITS
            });
            OperationWire.put(buf, operation);
            write_string(buf, via_column);
            if let Some(depth) = max_depth {
                U32.put(buf, depth);
            }
        }
        PolicyExpr::And(exprs) => {
            buf.push(POLICY_EXPR_AND);
            write_seq(buf, exprs, encode_policy_expr);
        }
        PolicyExpr::Or(exprs) => {
            buf.push(POLICY_EXPR_OR);
            write_seq(buf, exprs, encode_policy_expr);
        }
        PolicyExpr::Not(expr) => {
            buf.push(POLICY_EXPR_NOT);
            encode_policy_expr(buf, expr);
        }
        leaf @ (PolicyExpr::Cmp { .. }
        | PolicyExpr::IsNull { .. }
        | PolicyExpr::IsNotNull { .. }
        | PolicyExpr::In { .. }
        | PolicyExpr::True
        | PolicyExpr::False
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::Contains { .. }
        | PolicyExpr::InList { .. }
        | PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }) => PolicyLeafWire.put_variant(buf, leaf),
    }
}

wire_enum! {
    partial PolicyLeafWire: PolicyExpr {
        1 => Cmp { column: Str("policy_cmp_column"), op: CmpOpWire, value: PolicyValueWire },
        2 => IsNull { column: Str("policy_is_null_column") },
        3 => IsNotNull { column: Str("policy_is_not_null_column") },
        4 => In {
            column: Str("policy_in_column"),
            session_path: strings("policy_in_session_path"),
        },
        10 => True,
        11 => False,
        14 => InheritsReferencing {
            operation: OperationWire,
            source_table: Str("policy_inherits_referencing_source"),
            via_column: Str("policy_inherits_referencing_via_column"),
            max_depth: Opt(U32),
        },
        15 => Contains { column: Str("policy_contains_column"), value: PolicyValueWire },
        16 => InList {
            column: Str("policy_in_list_column"),
            values: Seq("policy_in_list_values", PolicyValueWire),
        },
        17 => SessionCmp {
            path: strings("policy_session_cmp_path"),
            op: CmpOpWire,
            value: ValueWire,
        },
        18 => SessionIsNull { path: strings("policy_session_is_null_path") },
        19 => SessionIsNotNull { path: strings("policy_session_is_not_null_path") },
        20 => SessionContains { path: strings("policy_session_contains_path"), value: ValueWire },
        21 => SessionInList {
            path: strings("policy_session_in_list_path"),
            values: Seq("policy_session_in_list_values", ValueWire),
        },
    }
}

fn decode_policy_expr(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
) -> Result<PolicyExpr, CatalogueEncodingError> {
    let mut frames = Vec::new();

    'decode: loop {
        let depth = frames.len() + 1;
        budget.enter(depth)?;

        let mut expression = match decode_policy_expr_node(data, offset, budget)? {
            DecodedPolicyNode::Complete(expression) => expression,
            DecodedPolicyNode::Children(frame) => {
                frames.push(frame);
                continue;
            }
        };

        while let Some(frame) = frames.pop() {
            match frame {
                PolicyDecodeFrame::Exists { table } => {
                    expression = PolicyExpr::Exists {
                        table,
                        condition: Box::new(expression),
                    };
                }
                PolicyDecodeFrame::Not => {
                    expression = PolicyExpr::Not(Box::new(expression));
                }
                PolicyDecodeFrame::And {
                    mut remaining,
                    mut expressions,
                } => {
                    expressions.push(expression);
                    remaining -= 1;
                    if remaining == 0 {
                        expression = PolicyExpr::And(expressions);
                    } else {
                        frames.push(PolicyDecodeFrame::And {
                            remaining,
                            expressions,
                        });
                        continue 'decode;
                    }
                }
                PolicyDecodeFrame::Or {
                    mut remaining,
                    mut expressions,
                } => {
                    expressions.push(expression);
                    remaining -= 1;
                    if remaining == 0 {
                        expression = PolicyExpr::Or(expressions);
                    } else {
                        frames.push(PolicyDecodeFrame::Or {
                            remaining,
                            expressions,
                        });
                        continue 'decode;
                    }
                }
            }
        }
        return Ok(expression);
    }
}

enum DecodedPolicyNode {
    Complete(PolicyExpr),
    Children(PolicyDecodeFrame),
}

enum PolicyDecodeFrame {
    Exists {
        table: String,
    },
    And {
        remaining: usize,
        expressions: Vec<PolicyExpr>,
    },
    Or {
        remaining: usize,
        expressions: Vec<PolicyExpr>,
    },
    Not,
}

fn decode_policy_expr_node(
    data: &[u8],
    offset: &mut usize,
    budget: &mut PolicyExpressionBudget,
) -> Result<DecodedPolicyNode, CatalogueEncodingError> {
    let complete = |expression| Ok(DecodedPolicyNode::Complete(expression));
    let tag = read_u8(data, offset)?;
    if let Some(leaf) = PolicyLeafWire.get_variant(tag, data, offset)? {
        return complete(leaf);
    }
    match tag {
        POLICY_EXPR_EXISTS => {
            let table = read_string(data, offset, "policy_exists_table")?;
            Ok(DecodedPolicyNode::Children(PolicyDecodeFrame::Exists {
                table,
            }))
        }
        POLICY_EXPR_EXISTS_REL => {
            let rel = decode_canonical_relation_expr(data, offset, budget)?;
            complete(PolicyExpr::ExistsRel { rel })
        }
        POLICY_EXPR_INHERITS | POLICY_EXPR_INHERITS_WITH_DEPTH => {
            let operation = OperationWire.get(data, offset)?;
            let via_column = read_string(data, offset, "policy_inherits_via_column")?;
            let max_depth = if tag == POLICY_EXPR_INHERITS_WITH_DEPTH {
                Some(U32.get(data, offset)?)
            } else {
                None
            };
            complete(PolicyExpr::Inherits {
                operation,
                via_column,
                max_depth,
            })
        }
        POLICY_EXPR_AND => {
            let count = read_count(data, offset, "policy_and")?;
            if count == 0 {
                return complete(PolicyExpr::And(Vec::new()));
            }
            budget.reserve(count)?;
            Ok(DecodedPolicyNode::Children(PolicyDecodeFrame::And {
                remaining: count,
                expressions: Vec::with_capacity(count),
            }))
        }
        POLICY_EXPR_OR => {
            let count = read_count(data, offset, "policy_or")?;
            if count == 0 {
                return complete(PolicyExpr::Or(Vec::new()));
            }
            budget.reserve(count)?;
            Ok(DecodedPolicyNode::Children(PolicyDecodeFrame::Or {
                remaining: count,
                expressions: Vec::with_capacity(count),
            }))
        }
        POLICY_EXPR_NOT => Ok(DecodedPolicyNode::Children(PolicyDecodeFrame::Not)),
        _ => Err(CatalogueEncodingError::InvalidTypeTag {
            tag,
            context: "policy_expr",
        }),
    }
}

wire_enum! {
    PolicyValueWire: PolicyValue, "policy_value" {
        1 => Literal(value: ValueWire),
        2 => SessionRef(path: strings("policy_session_ref_path")),
    }
}

wire_enum! {
    CmpOpWire: CmpOp, "policy_cmp_op" {
        1 => Eq,
        2 => Ne,
        3 => Lt,
        4 => Le,
        5 => Gt,
        6 => Ge,
    }
}

wire_enum! {
    OperationWire: Operation, "policy_operation" {
        1 => Select,
        2 => Insert,
        3 => Update,
        4 => Delete,
    }
}

// ============================================================================
// Value Encoding
// ============================================================================

// 9 is intentionally skipped: TYPE_ENUM is 9, and Values have no Enum tag
// (enum values are stored as Text). Keeping Double at 10 aligns with TYPE_DOUBLE.
wire_enum! {
    ValueWire: Value, "value" {
        0 => Null,
        1 => Integer(n: Le),
        2 => BigInt(n: Le),
        3 => Boolean(b: Bool),
        4 => Text(s: Str("value_text")),
        5 => Timestamp(t: Le),
        6 => Uuid(id: Oid("uuid")),
        7 => Array(items: Seq("value_array", ValueWire)),
        8 => Row { id: Skip, values: Seq("value_row", ValueWire) },
        10 => Double(n: Le),
        11 => Bytea(bytes: Raw),
        12 => TransactionId(id: Raw),
        13 => Enum { case: Str("enum_value_case"), values: Seq("value_enum", ValueWire) },
    }
}

// ============================================================================
// Primitive Helpers
// ============================================================================

fn write_u32(buf: &mut Vec<u8>, n: u32) {
    buf.extend_from_slice(&n.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, n: u64) {
    buf.extend_from_slice(&n.to_le_bytes());
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, CatalogueEncodingError> {
    let bytes = read_bytes(data, offset, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, CatalogueEncodingError> {
    let bytes = read_bytes(data, offset, 8)?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, CatalogueEncodingError> {
    if *offset >= data.len() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: *offset + 1,
            actual: data.len(),
        });
    }
    let val = data[*offset];
    *offset += 1;
    Ok(val)
}

fn read_bytes<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], CatalogueEncodingError> {
    if *offset + len > data.len() {
        return Err(CatalogueEncodingError::TruncatedData {
            expected: *offset + len,
            actual: data.len(),
        });
    }
    let bytes = &data[*offset..*offset + len];
    *offset += len;
    Ok(bytes)
}

fn ensure_consumed(data: &[u8], offset: usize) -> Result<(), CatalogueEncodingError> {
    if offset == data.len() {
        return Ok(());
    }
    Err(CatalogueEncodingError::DecodeError {
        message: format!(
            "trailing data after decoded payload: {} bytes remain",
            data.len() - offset
        ),
    })
}

fn ensure_canonical_payload(
    actual: &[u8],
    canonical: &[u8],
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    if actual == canonical {
        return Ok(());
    }
    Err(CatalogueEncodingError::DecodeError {
        message: format!("non-canonical {context} payload"),
    })
}

fn ensure_canonical_segment(
    data: &[u8],
    start: usize,
    end: usize,
    canonical: &[u8],
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    ensure_canonical_payload(&data[start..end], canonical, context)
}

fn read_flag(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<bool, CatalogueEncodingError> {
    match read_u8(data, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        tag => Err(CatalogueEncodingError::InvalidTypeTag { tag, context }),
    }
}

fn read_count(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<usize, CatalogueEncodingError> {
    let count = read_u32(data, offset)?;
    bound_count(data, offset, count, context)
}

fn bound_count(
    data: &[u8],
    offset: &usize,
    count: u32,
    context: &'static str,
) -> Result<usize, CatalogueEncodingError> {
    let count = count as usize;
    // Every nested record/list item has at least one tag byte.  Reject absurd
    // declared cardinalities before Vec::with_capacity can turn corruption into
    // allocation pressure during restart.
    if count > data.len().saturating_sub(*offset) {
        return Err(CatalogueEncodingError::DecodeError {
            message: format!("{context} count exceeds remaining canonical payload"),
        });
    }
    Ok(count)
}

fn ensure_unique_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
    context: &'static str,
) -> Result<(), CatalogueEncodingError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(CatalogueEncodingError::DecodeError {
                message: format!("duplicate {context}: {name}"),
            });
        }
    }
    Ok(())
}

/// Writes a `u32` item count followed by each item.
fn write_seq<T>(buf: &mut Vec<u8>, items: &[T], mut write: impl FnMut(&mut Vec<u8>, &T)) {
    write_u32(buf, items.len() as u32);
    for item in items {
        write(buf, item);
    }
}

/// Reads a bounded item count (see [`read_count`]) followed by that many items.
fn read_seq<T>(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
    mut read: impl FnMut(&[u8], &mut usize) -> Result<T, CatalogueEncodingError>,
) -> Result<Vec<T>, CatalogueEncodingError> {
    let count = read_count(data, offset, context)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(read(data, offset)?);
    }
    Ok(items)
}

/// Writes a presence byte (`1` or `0`) followed by the value when present.
fn write_opt<T: ?Sized>(
    buf: &mut Vec<u8>,
    value: Option<&T>,
    write: impl FnOnce(&mut Vec<u8>, &T),
) {
    match value {
        Some(value) => {
            buf.push(1);
            write(buf, value);
        }
        None => buf.push(0),
    }
}

/// Reads a presence byte and, when it is non-zero, the value. Payload decoders
/// re-encode and compare, so a non-canonical presence byte is still rejected.
fn read_opt<T>(
    data: &[u8],
    offset: &mut usize,
    read: impl FnOnce(&[u8], &mut usize) -> Result<T, CatalogueEncodingError>,
) -> Result<Option<T>, CatalogueEncodingError> {
    if read_u8(data, offset)? != 0 {
        read(data, offset).map(Some)
    } else {
        Ok(None)
    }
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn read_string(
    data: &[u8],
    offset: &mut usize,
    context: &'static str,
) -> Result<String, CatalogueEncodingError> {
    let len = read_u32(data, offset)? as usize;
    let bytes = read_bytes(data, offset, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| CatalogueEncodingError::InvalidUtf8 { context })
}

// ============================================================================
// Symmetric wire descriptions
// ============================================================================

/// One description of a wire shape, used for both directions so encode and
/// decode cannot drift. Codecs are small values; a `&'static str` they hold
/// names the context that decode errors report.
trait Wire<T> {
    fn put(&self, buf: &mut Vec<u8>, value: &T);
    fn get(&self, data: &[u8], offset: &mut usize) -> Result<T, CatalogueEncodingError>;
}

/// Anything stored as a name: written as a string, and checked for
/// duplicates by [`Unique`].
trait Named {
    fn name(&self) -> &str;
}

impl Named for String {
    fn name(&self) -> &str {
        self
    }
}

impl Named for TableName {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl Named for ColumnName {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl Named for ColumnDescriptor {
    fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl Named for EnumCaseDescriptor {
    fn name(&self) -> &str {
        &self.name
    }
}

/// A `u32`-length-prefixed UTF-8 string.
struct Str(&'static str);

impl<T: Named + From<String>> Wire<T> for Str {
    fn put(&self, buf: &mut Vec<u8>, value: &T) {
        write_string(buf, value.name());
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<T, CatalogueEncodingError> {
        read_string(data, offset, self.0).map(T::from)
    }
}

/// A fixed-width little-endian number.
struct Le;

macro_rules! le_wire {
    ($($ty:ty),+) => {$(
        impl Wire<$ty> for Le {
            fn put(&self, buf: &mut Vec<u8>, value: &$ty) {
                buf.extend_from_slice(&value.to_le_bytes());
            }

            fn get(&self, data: &[u8], offset: &mut usize) -> Result<$ty, CatalogueEncodingError> {
                let bytes = read_bytes(data, offset, std::mem::size_of::<$ty>())?;
                Ok(<$ty>::from_le_bytes(bytes.try_into().expect("exact width")))
            }
        }
    )+};
}

le_wire!(i32, i64, u64, f64);

/// A `usize` stored as a `u32`.
struct U32;

impl Wire<usize> for U32 {
    fn put(&self, buf: &mut Vec<u8>, value: &usize) {
        write_u32(buf, *value as u32);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<usize, CatalogueEncodingError> {
        Ok(read_u32(data, offset)? as usize)
    }
}

/// A `1`/`0` byte. Any non-zero byte reads as `true`; payload decoders
/// re-encode and compare, so a non-canonical byte is still rejected.
struct Bool;

impl Wire<bool> for Bool {
    fn put(&self, buf: &mut Vec<u8>, value: &bool) {
        buf.push(u8::from(*value));
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<bool, CatalogueEncodingError> {
        Ok(read_u8(data, offset)? != 0)
    }
}

/// Raw bytes: a fixed 16-byte id, or a `u32`-length-prefixed blob.
struct Raw;

impl Wire<[u8; 16]> for Raw {
    fn put(&self, buf: &mut Vec<u8>, value: &[u8; 16]) {
        buf.extend_from_slice(value);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<[u8; 16], CatalogueEncodingError> {
        Ok(read_bytes(data, offset, 16)?.try_into().expect("16 bytes"))
    }
}

impl Wire<Vec<u8>> for Raw {
    fn put(&self, buf: &mut Vec<u8>, value: &Vec<u8>) {
        write_u32(buf, value.len() as u32);
        buf.extend_from_slice(value);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Vec<u8>, CatalogueEncodingError> {
        let len = read_u32(data, offset)? as usize;
        Ok(read_bytes(data, offset, len)?.to_vec())
    }
}

/// An object id as its 16 UUID bytes; the string names it in decode errors.
struct Oid(&'static str);

impl Wire<ObjectId> for Oid {
    fn put(&self, buf: &mut Vec<u8>, value: &ObjectId) {
        buf.extend_from_slice(value.uuid().as_bytes());
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<ObjectId, CatalogueEncodingError> {
        let uuid = uuid::Uuid::from_slice(read_bytes(data, offset, 16)?).map_err(|err| {
            CatalogueEncodingError::DecodeError {
                message: format!("invalid {}: {err}", self.0),
            }
        })?;
        Ok(ObjectId::from_uuid(uuid))
    }
}

/// Nothing on the wire: a field the payload does not persist reads back as
/// its default.
struct Skip;

impl<T: Default> Wire<T> for Skip {
    fn put(&self, _: &mut Vec<u8>, _: &T) {}

    fn get(&self, _: &[u8], _: &mut usize) -> Result<T, CatalogueEncodingError> {
        Ok(T::default())
    }
}

/// A bounded item count followed by the items (see [`write_seq`] and
/// [`read_seq`]); the string names the count in decode errors.
struct Seq<C>(&'static str, C);

impl<T, C: Wire<T>> Wire<Vec<T>> for Seq<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Vec<T>) {
        write_seq(buf, value, |buf, item| self.1.put(buf, item));
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Vec<T>, CatalogueEncodingError> {
        read_seq(data, offset, self.0, |data, offset| {
            self.1.get(data, offset)
        })
    }
}

/// A list of strings whose count and items share one error context.
const fn strings(context: &'static str) -> Seq<Str> {
    Seq(context, Str(context))
}

/// A sequence whose items' names must be distinct; the string names the
/// items in the duplicate error.
struct Unique<C>(C, &'static str);

impl<T: Named, C: Wire<Vec<T>>> Wire<Vec<T>> for Unique<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Vec<T>) {
        self.0.put(buf, value);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Vec<T>, CatalogueEncodingError> {
        let items = self.0.get(data, offset)?;
        ensure_unique_names(items.iter().map(Named::name), self.1)?;
        Ok(items)
    }
}

/// A lenient presence byte followed by the value (see [`read_opt`]).
struct Opt<C>(C);

impl<T, C: Wire<T>> Wire<Option<T>> for Opt<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Option<T>) {
        write_opt(buf, value.as_ref(), |buf, value| self.0.put(buf, value));
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Option<T>, CatalogueEncodingError> {
        read_opt(data, offset, |data, offset| self.0.get(data, offset))
    }
}

/// A strict `0`/`1` presence byte (see [`read_flag`]) followed by the value.
struct Flag<C>(&'static str, C);

impl<T, C: Wire<T>> Wire<Option<T>> for Flag<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Option<T>) {
        write_opt(buf, value.as_ref(), |buf, value| self.1.put(buf, value));
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Option<T>, CatalogueEncodingError> {
        if read_flag(data, offset, self.0)? {
            self.1.get(data, offset).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// A boxed value, stored inline.
struct Boxed<C>(C);

impl<T, C: Wire<T>> Wire<Box<T>> for Boxed<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Box<T>) {
        self.0.put(buf, value);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Box<T>, CatalogueEncodingError> {
        self.0.get(data, offset).map(Box::new)
    }
}

/// A schema hash as its 32 raw bytes.
struct HashWire;

impl Wire<SchemaHash> for HashWire {
    fn put(&self, buf: &mut Vec<u8>, value: &SchemaHash) {
        buf.extend_from_slice(value.as_bytes());
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<SchemaHash, CatalogueEncodingError> {
        let bytes = read_bytes(data, offset, 32)?;
        Ok(SchemaHash::from_bytes(bytes.try_into().expect("32 bytes")))
    }
}

/// A tuple of codecs describes a tuple of values written in order.
macro_rules! tuple_wire {
    ($(($($codec:ident $value:ident $index:tt),+)),+) => {$(
        impl<$($value, $codec: Wire<$value>),+> Wire<($($value,)+)> for ($($codec,)+) {
            fn put(&self, buf: &mut Vec<u8>, value: &($($value,)+)) {
                $(self.$index.put(buf, &value.$index);)+
            }

            fn get(
                &self,
                data: &[u8],
                offset: &mut usize,
            ) -> Result<($($value,)+), CatalogueEncodingError> {
                Ok(($(self.$index.get(data, offset)?,)+))
            }
        }
    )+};
}

tuple_wire!((A TA 0, B TB 1, C TC 2), (A TA 0, B TB 1, C TC 2, D TD 3));

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::protocol_limits::{MAX_POLICY_EXPRESSION_DEPTH, MAX_POLICY_EXPRESSION_NODES};
    use jazz::tools::public_schema::SchemaBuilder;
    use jazz::tools::public_schema::{
        PolicyExpr, RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate,
        RelationValueRef,
    };
    use serde_json::json;

    // Wire tags these tests spell out by hand.
    const TYPE_ENUM: u8 = 9;
    const VALUE_ARRAY: u8 = 7;
    const POLICY_VALUE_SESSION_REF: u8 = 2;
    const REL_TABLE_SCAN: u8 = 1;
    const REL_PREDICATE_TRUE: u8 = 10;
    const POLICY_EXPR_IN: u8 = 4;
    const POLICY_EXPR_TRUE: u8 = 10;
    const POLICY_EXPR_IN_LIST: u8 = 16;
    const POLICY_EXPR_SESSION_CMP: u8 = 17;
    const POLICY_EXPR_SESSION_IS_NULL: u8 = 18;
    const POLICY_EXPR_SESSION_IS_NOT_NULL: u8 = 19;
    const POLICY_EXPR_SESSION_CONTAINS: u8 = 20;
    const POLICY_EXPR_SESSION_IN_LIST: u8 = 21;

    #[test]
    fn schema_roundtrip_simple() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("users").column("name", ColumnType::Text))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        // Check table exists
        let users = decoded.get(&TableName::new("users")).unwrap();
        assert_eq!(users.columns.columns.len(), 1);
    }

    #[test]
    fn schema_roundtrip_preserves_declared_column_order() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("token", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let users = decoded.get(&TableName::new("users")).unwrap();
        let column_names = users
            .columns
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(column_names, vec!["name", "token", "email"]);
    }

    #[test]
    fn schema_roundtrip_complex() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .nullable_column("email", ColumnType::Text)
                    .column("score", ColumnType::Integer)
                    .fk_column("org_id", "orgs"),
            )
            .table(TableSchema::builder("orgs").column("name", ColumnType::Text))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);

        let users = decoded.get(&TableName::new("users")).unwrap();
        assert_eq!(users.columns.columns.len(), 3);

        // Find nullable email column
        let email_col = users.columns.column("email").unwrap();
        assert!(email_col.nullable);
        assert_eq!(email_col.column_type, ColumnType::Text);

        // Find FK column
        let org_col = users.columns.column("org_id").unwrap();
        assert_eq!(org_col.references, Some(TableName::new("orgs")));
    }

    #[test]
    fn schema_roundtrip_with_arrays() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("posts").column(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        let posts = decoded.get(&TableName::new("posts")).unwrap();
        let tags_col = posts.columns.column("tags").unwrap();
        assert!(matches!(
            tags_col.column_type,
            ColumnType::Array { element: _ }
        ));
    }

    #[test]
    fn schema_roundtrip_with_bytea() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("chunks").column("payload", ColumnType::Bytea))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let chunks = decoded.get(&TableName::new("chunks")).unwrap();
        assert_eq!(
            chunks.columns.column("payload").unwrap().column_type,
            ColumnType::Bytea
        );
    }

    #[test]
    fn schema_roundtrip_with_json() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("documents")
                    .column(
                        "payload",
                        ColumnType::Json {
                            schema: Some(json!({
                                "type": "object",
                                "required": ["name"]
                            })),
                        },
                    )
                    .column("raw_payload", ColumnType::Json { schema: None }),
            )
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let docs = decoded.get(&TableName::new("documents")).unwrap();
        assert_eq!(
            docs.columns.column("payload").unwrap().column_type,
            ColumnType::Json {
                schema: Some(json!({
                    "type": "object",
                    "required": ["name"]
                }))
            }
        );
        assert_eq!(
            docs.columns.column("raw_payload").unwrap().column_type,
            ColumnType::Json { schema: None }
        );
    }

    // This stays internal because it verifies the exact restart-authoritative
    // byte contract below the public catalogue API.
    #[test]
    fn nested_catalogue_payload_goldens_are_exact() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("docs").column(
                "payload",
                ColumnType::Json {
                    schema: Some(json!({"z": null, "a": [true, 1]})),
                },
            ))
            .build();
        let schema_bytes = encode_schema(&schema);
        assert_eq!(
            hex(&schema_bytes),
            "020100000004000000646f637301000000070000007061796c6f61640c010107020000000100000061060200000003040100000031010000007a0100000000ffffffff0000000000000000"
        );
        assert_eq!(decode_schema(&schema_bytes).unwrap(), schema);

        let lens = LensTransform::new();
        assert_eq!(hex(&encode_lens_transform(&lens)), "020000000000000000");
        let decoded_lens = decode_lens_transform(&encode_lens_transform(&lens)).unwrap();
        assert!(decoded_lens.ops.is_empty());
        assert!(decoded_lens.draft_ops.is_empty());

        let rel = RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: String::from("members"),
                alias: Some("m".to_owned()),
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef::unscoped("owner_id"),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
            },
        };
        let permissions = HashMap::from([(
            TableName::new("docs"),
            TablePolicies::new().with_select(PolicyExpr::ExistsRel { rel: rel.clone() }),
        )]);
        let permission_bytes = encode_permissions(&permissions);
        assert_eq!(
            hex(&permission_bytes),
            "010100000004000000646f6373010d010201070000006d656d6265727301010000006d0100080000006f776e65725f696401020200000006000000636c61696d730300000073756200000000000000"
        );
        assert_eq!(decode_permissions(&permission_bytes).unwrap(), permissions);
    }

    #[test]
    fn nested_catalogue_payload_rejects_noncanonical_order_versions_and_suffixes() {
        // v1 JSON object with the semantic keys `b` then `a`: valid-looking
        // data, but not the required ascending UTF-8 byte order.
        let mut offset = 0;
        let unordered_json = [
            NESTED_CODEC_VERSION,
            JSON_OBJECT,
            2,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            b'b',
            JSON_NULL,
            1,
            0,
            0,
            0,
            b'a',
            JSON_NULL,
        ];
        assert!(JsonWire.get(&unordered_json, &mut offset).is_err());

        let rel = RelationExpr::TableScan {
            table: String::from("docs"),
            alias: None,
        };
        let permissions = HashMap::from([(
            TableName::new("docs"),
            TablePolicies::new().with_select(PolicyExpr::ExistsRel { rel }),
        )]);
        let mut bytes = encode_permissions(&permissions);
        bytes.push(0);
        assert!(decode_permissions(&bytes).is_err());

        // `HashMap` reconstruction would otherwise overwrite the first entry;
        // exact re-encoding makes this alternate duplicate-map spelling corrupt.
        let one_entry = encode_permissions(&permissions);
        let mut duplicate_entry = one_entry.clone();
        duplicate_entry[1..5].copy_from_slice(&2_u32.to_le_bytes());
        duplicate_entry.extend_from_slice(&one_entry[5..]);
        assert!(decode_permissions(&duplicate_entry).is_err());

        let mut unknown_nested_version = encode_permissions(&permissions);
        let nested_version = unknown_nested_version
            .windows(2)
            .position(|window| window == [POLICY_EXPR_EXISTS_REL, NESTED_CODEC_VERSION])
            .expect("ExistsRel nested version");
        unknown_nested_version[nested_version + 1] = NESTED_CODEC_VERSION + 1;
        assert!(decode_permissions(&unknown_nested_version).is_err());
    }

    // These are internal tests because the public failure boundary is the
    // catalogue scan; here we prove every binary collection decoder rejects a
    // hostile count before it can reserve memory during that scan.
    #[test]
    fn catalogue_payload_counts_are_bounded_before_collection_allocation() {
        let huge = u32::MAX.to_le_bytes();
        let with_huge = |tag: u8| [vec![tag], huge.to_vec()].concat();
        let with_empty_name_and_huge = |tag: u8| [vec![tag, 0, 0, 0, 0], huge.to_vec()].concat();

        let mut offset = 0;
        assert_count_bound(
            decode_policy_expr(
                &with_huge(POLICY_EXPR_SESSION_CMP),
                &mut offset,
                &mut PolicyExpressionBudget::default(),
            ),
            "policy_session_cmp_path",
        );
        for (tag, context) in [
            (POLICY_EXPR_SESSION_IS_NULL, "policy_session_is_null_path"),
            (
                POLICY_EXPR_SESSION_IS_NOT_NULL,
                "policy_session_is_not_null_path",
            ),
            (POLICY_EXPR_SESSION_CONTAINS, "policy_session_contains_path"),
            (POLICY_EXPR_AND, "policy_and"),
            (POLICY_EXPR_OR, "policy_or"),
        ] {
            let mut offset = 0;
            assert_count_bound(
                decode_policy_expr(
                    &with_huge(tag),
                    &mut offset,
                    &mut PolicyExpressionBudget::default(),
                ),
                context,
            );
        }
        let session_in_list_value_count = [
            vec![POLICY_EXPR_SESSION_IN_LIST],
            0_u32.to_le_bytes().to_vec(),
            huge.to_vec(),
        ]
        .concat();
        let mut offset = 0;
        assert_count_bound(
            decode_policy_expr(
                &session_in_list_value_count,
                &mut offset,
                &mut PolicyExpressionBudget::default(),
            ),
            "policy_session_in_list_values",
        );
        for (tag, context) in [
            (POLICY_EXPR_IN, "policy_in_session_path"),
            (POLICY_EXPR_IN_LIST, "policy_in_list_values"),
        ] {
            let mut offset = 0;
            assert_count_bound(
                decode_policy_expr(
                    &with_empty_name_and_huge(tag),
                    &mut offset,
                    &mut PolicyExpressionBudget::default(),
                ),
                context,
            );
        }

        assert_count_bound(
            decode_permissions(&[PERMISSIONS_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "permissions_tables",
        );
        assert_count_bound(
            decode_schema(&[SCHEMA_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "schema_tables",
        );
        assert_count_bound(
            decode_lens_transform(&[LENS_VERSION, huge[0], huge[1], huge[2], huge[3]]),
            "lens_ops",
        );

        let mut offset = 0;
        assert_count_bound(
            ColumnTypeWire.get(&with_huge(TYPE_ENUM), &mut offset),
            "enum_variants",
        );
        let mut offset = 0;
        assert_count_bound(
            ValueWire.get(&with_huge(VALUE_ARRAY), &mut offset),
            "value_array",
        );
        let mut offset = 0;
        assert_count_bound(
            PolicyValueWire.get(&with_huge(POLICY_VALUE_SESSION_REF), &mut offset),
            "policy_session_ref_path",
        );

        let mut offset = 0;
        assert!(matches!(
            decode_policy_expr(
                &[POLICY_EXPR_SESSION_CMP],
                &mut offset,
                &mut PolicyExpressionBudget::default(),
            ),
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));
        assert!(matches!(
            decode_permissions(&[PERMISSIONS_VERSION]),
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));

        // A one-item count with no item bytes is the planted-sensitivity case:
        // it must fail in `read_count`, not after a collection is allocated.
        assert_count_bound(
            decode_permissions(&[PERMISSIONS_VERSION, 1, 0, 0, 0]),
            "permissions_tables",
        );
    }

    #[test]
    fn schema_roundtrip_preserves_column_merge_strategy() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("counters"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("value", ColumnType::Integer)
                    .merge_strategy(ColumnMergeStrategy::Counter),
            ])),
        );

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let table = decoded
            .get(&TableName::new("counters"))
            .expect("decoded counters table");
        let column = table.columns.column("value").expect("counter column");

        assert_eq!(column.merge_strategy, Some(ColumnMergeStrategy::Counter));
    }

    #[test]
    fn schema_roundtrip_preserves_indexed_columns() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .index_only(["done"]),
            )
            .build();

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let todos = decoded
            .get(&TableName::new("todos"))
            .expect("decoded todos table");
        assert_eq!(todos.indexed_columns, Some(vec![ColumnName::new("done")]));
    }

    #[test]
    fn schema_roundtrip_preserves_composite_indexes() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("documents")
                    .column("owner", ColumnType::Uuid)
                    .column("updated", ColumnType::Timestamp)
                    .column("title", ColumnType::Text)
                    .composite_index(["owner", "updated"])
                    .composite_index(["owner", "updated", "title"]),
            )
            .build();

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);
        let decoded = decode_schema(&encoded).unwrap();
        assert_eq!(encode_schema(&decoded), encoded);
        assert_eq!(
            decoded[&TableName::new("documents")].composite_indexes,
            vec![
                vec![ColumnName::new("owner"), ColumnName::new("updated")],
                vec![
                    ColumnName::new("owner"),
                    ColumnName::new("updated"),
                    ColumnName::new("title"),
                ],
            ]
        );
    }

    // Internal for the same reason as the nested-value goldens: this pins the exact
    // restart-authoritative v2 bytes below the public catalogue API.
    #[test]
    fn composite_catalogue_payload_v2_goldens_are_exact() {
        let table = || {
            TableSchema::builder("docs")
                .column("owner", ColumnType::Uuid)
                .column("updated", ColumnType::Timestamp)
        };
        let schema = SchemaBuilder::new()
            .table(
                table()
                    .composite_index(["updated", "owner"])
                    .composite_index(["owner", "updated"]),
            )
            .build();
        let schema_bytes = encode_schema(&schema);
        assert_eq!(
            hex(&schema_bytes),
            "020100000004000000646f637302000000050000006f776e6572060000000007000000757064617465640500000000ffffffff000000000200000002000000050000006f776e65720700000075706461746564020000000700000075706461746564050000006f776e6572"
        );
        assert_eq!(decode_schema(&schema_bytes).unwrap(), schema);
        let redeclared = SchemaBuilder::new()
            .table(
                table()
                    .composite_index(["owner", "updated"])
                    .composite_index(["updated", "owner"]),
            )
            .build();
        assert_eq!(
            encode_schema(&redeclared),
            schema_bytes,
            "composite indexes encode in canonical order"
        );

        // Retired version 1 is rejected even when the table has no indexes.
        let mut as_v1 = schema_bytes.clone();
        as_v1[0] = 1;
        assert!(decode_schema(&as_v1).is_err());
        let plain = SchemaBuilder::new().table(table()).build();
        let plain_bytes = encode_schema(&plain);
        assert_eq!(plain_bytes[0], SCHEMA_VERSION);
        assert_eq!(decode_schema(&plain_bytes).unwrap(), plain);
        let mut plain_as_v1 = plain_bytes;
        plain_as_v1[0] = 1;
        assert!(decode_schema(&plain_as_v1).is_err());

        let mut lens = LensTransform::new();
        lens.push(
            LensOp::AddTable {
                table: "docs".to_owned(),
                schema: table().composite_index(["owner", "updated"]).build(),
            },
            false,
        );
        let lens_bytes = encode_lens_transform(&lens);
        assert_eq!(
            hex(&lens_bytes),
            "02010000000404000000646f637302000000050000006f776e6572060000000007000000757064617465640500000000000000000100000002000000050000006f776e6572070000007570646174656400000000"
        );
        assert_eq!(
            encode_lens_transform(&decode_lens_transform(&lens_bytes).unwrap()),
            lens_bytes
        );
        let mut lens_as_v1 = lens_bytes.clone();
        lens_as_v1[0] = 1;
        assert!(decode_lens_transform(&lens_as_v1).is_err());
    }

    #[test]
    fn schema_roundtrip_with_column_defaults() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column_with_default("done", ColumnType::Boolean, Value::Boolean(false))
                    .column_with_default("priority", ColumnType::Integer, Value::Integer(0))
                    .nullable_column("note", ColumnType::Text),
            )
            .build();

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let todos = decoded.get(&TableName::new("todos")).unwrap();

        assert_eq!(
            todos.columns.column("done").unwrap().default,
            Some(Value::Boolean(false))
        );
        assert_eq!(
            todos.columns.column("priority").unwrap().default,
            Some(Value::Integer(0))
        );
        assert_eq!(todos.columns.column("note").unwrap().default, None);
    }

    #[test]
    fn schema_roundtrip_with_fk_reference() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("image", ColumnType::Uuid).references("files"),
            ])),
        );
        schema.insert(
            TableName::new("files"),
            TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                "name",
                ColumnType::Text,
            )])),
        );

        let encoded = encode_schema(&schema);
        assert_eq!(encoded[0], SCHEMA_VERSION);

        let decoded = decode_schema(&encoded).unwrap();
        let image_col = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image_col.references, Some(TableName::new("files")));
        assert_eq!(image_col.default, None);
    }

    #[test]
    fn schema_roundtrip_with_enum() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("todos").column(
                "status",
                ColumnType::Enum {
                    variants: vec![
                        "done".to_string(),
                        "in_progress".to_string(),
                        "todo".to_string(),
                    ],
                },
            ))
            .build();

        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();

        let todos = decoded.get(&TableName::new("todos")).unwrap();
        let status_col = todos.columns.column("status").unwrap();
        assert_eq!(
            status_col.column_type,
            ColumnType::Enum {
                variants: vec![
                    "done".to_string(),
                    "in_progress".to_string(),
                    "todo".to_string(),
                ]
            }
        );
    }

    #[test]
    fn schema_roundtrip_with_catalogue_native_enums() {
        let scalar_enum = ColumnType::ScalarEnum {
            name: "status".to_owned(),
            variants: vec!["todo".to_owned(), "done".to_owned()],
        };
        let payload_enum = ColumnType::CatalogueEnumPayload {
            name: "event".to_owned(),
            cases: vec![EnumCaseDescriptor {
                name: "renamed".to_owned(),
                fields: vec![ColumnDescriptor::new("title", ColumnType::Text)],
            }],
        };
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("events")
                    .column("status", scalar_enum.clone())
                    .column("event", payload_enum.clone()),
            )
            .build();

        let decoded = decode_schema(&encode_schema(&schema)).unwrap();
        let events = decoded.get(&TableName::new("events")).unwrap();
        assert_eq!(
            events.columns.column("status").unwrap().column_type,
            scalar_enum
        );
        assert_eq!(
            events.columns.column("event").unwrap().column_type,
            payload_enum
        );
    }

    #[test]
    fn schema_roundtrip_strips_policies_but_preserves_hash() {
        let schema =
            SchemaBuilder::new()
                .table(
                    TableSchema::builder("todos")
                        .column("owner_id", ColumnType::Uuid)
                        .column("title", ColumnType::Text)
                        .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                            "owner_id",
                            vec!["user".to_owned()],
                        ))),
                )
                .build();

        let original_hash = jazz::tools::public_schema::SchemaHash::compute(&schema);
        let encoded = encode_schema(&schema);
        let decoded = decode_schema(&encoded).unwrap();
        let decoded_hash = jazz::tools::public_schema::SchemaHash::compute(&decoded);

        assert_eq!(
            original_hash, decoded_hash,
            "Schema hash must be stable across encode/decode when policies exist"
        );

        let decoded_todos = decoded.get(&TableName::new("todos")).unwrap();
        assert!(
            decoded_todos.policies == TablePolicies::default(),
            "Stored schema encoding should be structural-only"
        );
    }

    #[test]
    fn schema_roundtrip_preserves_branch_columns_and_hash() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("workspace_id", ColumnType::Uuid)
                    .branch_by("workspace_id"),
            )
            .build();

        let decoded = decode_schema(&encode_schema(&schema)).expect("schema decodes");
        let todos = decoded.get(&TableName::new("todos")).expect("todos table");

        assert_eq!(todos.branch_by, vec![ColumnName::new("workspace_id")]);
        assert_eq!(SchemaHash::compute(&decoded), SchemaHash::compute(&schema));
    }

    #[test]
    fn permissions_roundtrip_preserves_complex_policies() {
        let expected = PolicyExpr::And(vec![
            PolicyExpr::Contains {
                column: "owner_id".to_string(),
                value: PolicyValue::Literal(Value::Text("ali".to_string())),
            },
            PolicyExpr::InList {
                column: "status".to_string(),
                values: vec![
                    PolicyValue::Literal(Value::Text("active".to_string())),
                    PolicyValue::SessionRef(vec!["user".to_owned()]),
                ],
            },
            PolicyExpr::SessionCmp {
                path: vec!["claims".to_string(), "role".to_string()],
                op: CmpOp::Eq,
                value: Value::Text("manager".to_string()),
            },
            PolicyExpr::SessionInList {
                path: vec!["claims".to_string(), "plan".to_string()],
                values: vec![
                    Value::Text("pro".to_string()),
                    Value::Text("enterprise".to_string()),
                ],
            },
            PolicyExpr::SessionContains {
                path: vec!["claims".to_string(), "teamIds".to_string()],
                value: Value::Text("team_a".to_string()),
            },
            PolicyExpr::SessionIsNull {
                path: vec!["claims".to_string(), "deleted_at".to_string()],
            },
            PolicyExpr::SessionIsNotNull {
                path: vec!["user".to_owned()],
            },
        ]);
        let permissions = HashMap::from([(
            TableName::new("todos"),
            TablePolicies::new().with_select(expected.clone()),
        )]);

        let encoded = encode_permissions(&permissions);
        let decoded = decode_permissions(&encoded).expect("permissions should decode");

        assert_eq!(
            decoded.get(&TableName::new("todos")),
            permissions.get(&TableName::new("todos"))
        );
    }

    fn nested_policy_expression(nodes: usize) -> PolicyExpr {
        assert!(nodes > 0);
        (1..nodes).fold(PolicyExpr::True, |expression, _| {
            PolicyExpr::Not(Box::new(expression))
        })
    }

    fn wide_policy_expression(nodes: usize) -> PolicyExpr {
        assert!(nodes > 0);
        PolicyExpr::And((1..nodes).map(|_| PolicyExpr::True).collect())
    }

    fn permissions_with_policy(expression: PolicyExpr) -> HashMap<TableName, TablePolicies> {
        HashMap::from([(
            TableName::new("documents"),
            TablePolicies::new().with_select(expression),
        )])
    }

    #[test]
    fn permissions_decode_enforces_policy_depth_and_total_node_boundaries() {
        // This stays internal because the binary catalogue decoder is the
        // untrusted boundary and public policy builders already own their tree.
        let at_depth_limit =
            permissions_with_policy(nested_policy_expression(MAX_POLICY_EXPRESSION_DEPTH));
        assert_eq!(
            decode_permissions(&encode_permissions(&at_depth_limit))
                .expect("exact catalogue policy depth boundary remains valid"),
            at_depth_limit
        );

        let over_depth_limit =
            permissions_with_policy(nested_policy_expression(MAX_POLICY_EXPRESSION_DEPTH + 1));
        assert_eq!(
            decode_permissions(&encode_permissions(&over_depth_limit))
                .expect_err("catalogue policy depth must be rejected while parsing"),
            CatalogueEncodingError::ProtocolLimit(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_DEPTH",
                max: MAX_POLICY_EXPRESSION_DEPTH,
                actual: MAX_POLICY_EXPRESSION_DEPTH + 1,
            })
        );

        let at_node_limit =
            permissions_with_policy(wide_policy_expression(MAX_POLICY_EXPRESSION_NODES));
        assert_eq!(
            decode_permissions(&encode_permissions(&at_node_limit))
                .expect("exact catalogue policy node boundary remains valid"),
            at_node_limit
        );

        let over_node_limit =
            permissions_with_policy(wide_policy_expression(MAX_POLICY_EXPRESSION_NODES + 1));
        assert_eq!(
            decode_permissions(&encode_permissions(&over_node_limit))
                .expect_err("catalogue policy node count must be rejected while parsing"),
            CatalogueEncodingError::ProtocolLimit(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_NODES",
                max: MAX_POLICY_EXPRESSION_NODES,
                actual: MAX_POLICY_EXPRESSION_NODES + 1,
            })
        );
    }

    fn permissions_with_raw_relation(relation: Vec<u8>) -> Vec<u8> {
        let leaf = RelationExpr::TableScan {
            table: String::from("documents"),
            alias: None,
        };
        let permissions = permissions_with_policy(PolicyExpr::ExistsRel { rel: leaf.clone() });
        let mut encoded = encode_permissions(&permissions);
        let mut leaf_bytes = Vec::new();
        encode_canonical_relation_expr(&mut leaf_bytes, &leaf);
        let start = encoded
            .windows(leaf_bytes.len())
            .position(|window| window == leaf_bytes)
            .unwrap();
        encoded.splice(start..start + leaf_bytes.len(), relation);
        encoded
    }

    fn nested_filter_relation(nodes: usize) -> Vec<u8> {
        assert!(nodes > 0);
        let filters = nodes - 1;
        let mut relation = vec![NESTED_CODEC_VERSION];
        relation.extend(std::iter::repeat_n(REL_FILTER, filters));
        relation.push(REL_TABLE_SCAN);
        write_string(&mut relation, "documents");
        relation.push(0); // no alias
        relation.extend(std::iter::repeat_n(REL_PREDICATE_TRUE, filters));
        relation
    }

    #[test]
    fn relation_policy_decode_uses_bounded_frames_without_stack_growth() {
        let at_depth =
            permissions_with_raw_relation(nested_filter_relation(MAX_POLICY_EXPRESSION_DEPTH));
        decode_permissions(&at_depth)
            .expect("exact relation expression depth boundary must decode");
        let over_depth =
            permissions_with_raw_relation(nested_filter_relation(MAX_POLICY_EXPRESSION_DEPTH + 1));
        assert_eq!(
            decode_permissions(&over_depth).expect_err("over-depth relation must reject"),
            CatalogueEncodingError::ProtocolLimit(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_DEPTH",
                max: MAX_POLICY_EXPRESSION_DEPTH,
                actual: MAX_POLICY_EXPRESSION_DEPTH + 1,
            })
        );

        let mut wide_expr = vec![NESTED_CODEC_VERSION, REL_UNION_LABELED];
        write_u32(&mut wide_expr, MAX_POLICY_EXPRESSION_NODES as u32);
        wide_expr.extend(std::iter::repeat_n(0, MAX_POLICY_EXPRESSION_NODES));
        assert!(matches!(
            decode_permissions(&permissions_with_raw_relation(wide_expr)),
            Err(CatalogueEncodingError::ProtocolLimit(
                PolicyExpressionLimitError {
                    limit: "MAX_POLICY_EXPRESSION_NODES",
                    ..
                }
            ))
        ));

        let mut wide_predicate = vec![NESTED_CODEC_VERSION, REL_FILTER, REL_TABLE_SCAN];
        write_string(&mut wide_predicate, "documents");
        wide_predicate.push(0); // no alias
        wide_predicate.push(REL_PREDICATE_AND);
        write_u32(&mut wide_predicate, MAX_POLICY_EXPRESSION_NODES as u32);
        wide_predicate.extend(std::iter::repeat_n(0, MAX_POLICY_EXPRESSION_NODES));
        assert!(matches!(
            decode_permissions(&permissions_with_raw_relation(wide_predicate)),
            Err(CatalogueEncodingError::ProtocolLimit(
                PolicyExpressionLimitError {
                    limit: "MAX_POLICY_EXPRESSION_NODES",
                    ..
                }
            ))
        ));
    }

    #[test]
    fn catalogue_policy_budget_spans_outer_and_relation_subtree() {
        // Each side is below the 16,384-node ceiling on its own. Their sum is
        // not: do not let ExistsRel start a fresh accounting root.
        let mut relation = vec![NESTED_CODEC_VERSION, REL_UNION_LABELED];
        write_u32(&mut relation, 8_191);
        relation.extend(std::iter::repeat_n(0, 8_191));

        let mut payload = vec![PERMISSIONS_VERSION];
        write_u32(&mut payload, 1);
        write_string(&mut payload, "documents");
        payload.push(1); // select.using
        payload.push(POLICY_EXPR_AND);
        write_u32(&mut payload, 8_192);
        payload.extend(std::iter::repeat_n(POLICY_EXPR_TRUE, 8_191));
        payload.push(POLICY_EXPR_EXISTS_REL);
        payload.extend(relation);
        payload.extend([0; 7]); // select.with_check and the remaining operations

        assert!(matches!(
            decode_permissions(&payload),
            Err(CatalogueEncodingError::ProtocolLimit(
                PolicyExpressionLimitError {
                    limit: "MAX_POLICY_EXPRESSION_NODES",
                    ..
                }
            ))
        ));
    }

    #[test]
    fn relation_policy_variants_keep_exact_catalogue_roundtrip() {
        let column = RelationColumnRef::unscoped("owner");
        let predicate = RelationPredicate::And(vec![
            RelationPredicate::Cmp {
                left: column.clone(),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::SessionRef(vec!["user".to_owned()]),
            },
            RelationPredicate::Or(vec![
                RelationPredicate::Not(Box::new(RelationPredicate::IsNull {
                    column: column.clone(),
                })),
                RelationPredicate::EnumMatch {
                    column: RelationColumnRef::unscoped("kind"),
                    case: "song".to_owned(),
                    payload: Box::new(RelationPredicate::Contains {
                        left: RelationColumnRef::unscoped("title"),
                        right: RelationValueRef::literal(Value::Text("jazz".to_owned())),
                    }),
                },
            ]),
        ]);
        let relation = RelationExpr::Gather {
            seed: Box::new(RelationExpr::Project {
                input: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::Union {
                        inputs: vec![RelationUnionArm {
                            label: "documents".to_owned(),
                            input: RelationExpr::Filter {
                                input: Box::new(RelationExpr::TableScan {
                                    table: String::from("documents"),
                                    alias: Some("d".to_owned()),
                                }),
                                predicate,
                            },
                        }],
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: String::from("owners"),
                        alias: Some("o".to_owned()),
                    }),
                    on: vec![RelationJoinCondition {
                        left: RelationColumnRef::unscoped("owner"),
                        right: RelationColumnRef::unscoped("id"),
                    }],
                    join_kind: RelationJoinKind::Left,
                }),
                columns: vec![RelationProjectColumn {
                    alias: "owner".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef::unscoped("owner")),
                }],
            }),
            step: Box::new(RelationExpr::TableScan {
                table: String::from("documents"),
                alias: None,
            }),
            frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: RecursionBound::MaxDepth(3),
            dedupe_key: vec![RelationKeyRef::Column(RelationColumnRef::unscoped("owner"))],
        };
        let permissions = permissions_with_policy(PolicyExpr::ExistsRel { rel: relation });
        let encoded = encode_permissions(&permissions);
        assert_eq!(
            decode_permissions(&encoded).expect("decode variants"),
            permissions
        );
        assert_eq!(
            encode_permissions(&decode_permissions(&encoded).unwrap()),
            encoded
        );
    }

    #[test]
    fn labeled_relation_union_uses_explicit_canonical_wire_grammar() {
        let relation = RelationExpr::Union {
            inputs: vec![
                RelationUnionArm {
                    label: "a".to_owned(),
                    input: RelationExpr::TableScan {
                        table: String::from("a"),
                        alias: None,
                    },
                },
                RelationUnionArm {
                    label: "b".to_owned(),
                    input: RelationExpr::TableScan {
                        table: String::from("b"),
                        alias: None,
                    },
                },
            ],
        };
        let mut encoded = Vec::new();
        encode_canonical_relation_expr(&mut encoded, &relation);
        assert_eq!(
            hex(&encoded),
            "010702000000010000006101000000620101000000610001010000006200"
        );
        let mut offset = 0;
        assert_eq!(
            decode_canonical_relation_expr(
                &encoded,
                &mut offset,
                &mut PolicyExpressionBudget::default(),
            )
            .expect("labeled union grammar decodes"),
            relation
        );
        assert_eq!(offset, encoded.len());

        let mut legacy = vec![NESTED_CODEC_VERSION, REL_UNION];
        write_u32(&mut legacy, 0);
        let mut offset = 0;
        assert!(matches!(
            decode_canonical_relation_expr(
                &legacy,
                &mut offset,
                &mut PolicyExpressionBudget::default(),
            ),
            Err(CatalogueEncodingError::DecodeError { .. })
        ));

        let invalid = HashMap::from([(
            TableName::new("documents"),
            TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelationExpr::Union {
                    inputs: vec![
                        RelationUnionArm {
                            label: "duplicate".to_owned(),
                            input: RelationExpr::TableScan {
                                table: String::from("a"),
                                alias: None,
                            },
                        },
                        RelationUnionArm {
                            label: "duplicate".to_owned(),
                            input: RelationExpr::TableScan {
                                table: String::from("b"),
                                alias: None,
                            },
                        },
                    ],
                },
            }),
        )]);
        assert!(validate_permissions_relations(&invalid).is_err());
    }

    #[test]
    fn permissions_bundle_roundtrip_preserves_target_schema() {
        let schema_hash = SchemaHash::compute(
            &SchemaBuilder::new()
                .table(TableSchema::builder("todos").column("title", ColumnType::Text))
                .build(),
        );
        let version = 7;
        let parent_bundle_object_id = Some(ObjectId::new());
        let permissions = HashMap::from([(
            TableName::new("todos"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        let encoded =
            encode_permissions_bundle(schema_hash, version, parent_bundle_object_id, &permissions);
        let (decoded_hash, decoded_version, decoded_parent_bundle_object_id, decoded_permissions) =
            decode_permissions_bundle(&encoded).expect("bundle should decode");

        assert_eq!(decoded_hash, schema_hash);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_parent_bundle_object_id, parent_bundle_object_id);
        assert_eq!(decoded_permissions, permissions);
    }

    #[test]
    fn permissions_head_roundtrip_preserves_bundle_pointer() {
        let schema_hash = SchemaHash::compute(
            &SchemaBuilder::new()
                .table(TableSchema::builder("todos").column("title", ColumnType::Text))
                .build(),
        );
        let version = 7;
        let parent_bundle_object_id = Some(ObjectId::new());
        let bundle_object_id = ObjectId::new();

        let encoded = encode_permissions_head(
            schema_hash,
            version,
            parent_bundle_object_id,
            bundle_object_id,
        );
        let (
            decoded_hash,
            decoded_version,
            decoded_parent_bundle_object_id,
            decoded_bundle_object_id,
        ) = decode_permissions_head(&encoded).expect("head should decode");

        assert_eq!(decoded_hash, schema_hash);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_parent_bundle_object_id, parent_bundle_object_id);
        assert_eq!(decoded_bundle_object_id, bundle_object_id);
    }

    #[test]
    fn lens_roundtrip_strips_table_policies() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddTable {
                table: "todos".to_string(),
                schema: TableSchema::builder("todos")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True))
                    .build(),
            },
            false,
        );

        let decoded = decode_lens_transform(&encode_lens_transform(&transform)).unwrap();
        let LensOp::AddTable { schema, .. } = &decoded.ops[0] else {
            panic!("expected add-table op");
        };
        assert_eq!(schema.policies, TablePolicies::default());
    }

    #[test]
    fn lens_roundtrip_preserves_composite_indexes() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddTable {
                table: "documents".to_owned(),
                schema: TableSchema::builder("documents")
                    .column("owner", ColumnType::Uuid)
                    .column("updated", ColumnType::Timestamp)
                    .composite_index(["owner", "updated"])
                    .build(),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        assert_eq!(encoded[0], LENS_VERSION);
        let decoded = decode_lens_transform(&encoded).unwrap();
        assert_eq!(encode_lens_transform(&decoded), encoded);
        let LensOp::AddTable { schema, .. } = &decoded.ops[0] else {
            panic!("expected add-table op");
        };
        assert_eq!(
            schema.composite_indexes,
            vec![vec![ColumnName::new("owner"), ColumnName::new("updated")]]
        );
    }

    #[test]
    fn lens_transform_roundtrip_empty() {
        let transform = LensTransform::new();
        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert!(decoded.ops.is_empty());
        assert!(decoded.draft_ops.is_empty());
    }

    #[test]
    fn lens_transform_roundtrip_add_column() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "email".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 1);
        assert!(decoded.draft_ops.is_empty());

        if let LensOp::AddColumn {
            table,
            column,
            column_type,
            default,
        } = &decoded.ops[0]
        {
            assert_eq!(table, "users");
            assert_eq!(column, "email");
            assert_eq!(*column_type, ColumnType::Text);
            assert_eq!(*default, Value::Null);
        } else {
            panic!("Expected AddColumn");
        }
    }

    #[test]
    fn lens_transform_roundtrip_with_drafts() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "a".to_string(),
                column_type: ColumnType::Integer,
                default: Value::Integer(0),
            },
            false,
        );
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "b".to_string(),
                column_type: ColumnType::Uuid,
                default: Value::Null,
            },
            true, // draft
        );
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "x".to_string(),
                new_name: "y".to_string(),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 3);
        assert_eq!(decoded.draft_ops, vec![1]); // Second op is draft
    }

    #[test]
    fn lens_transform_roundtrip_rename_table() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 1);
        assert!(matches!(
            &decoded.ops[0],
            LensOp::RenameTable { old_name, new_name }
            if old_name == "users" && new_name == "people"
        ));
    }

    #[test]
    fn lens_transform_roundtrip_all_ops() {
        let mut transform = LensTransform::new();

        // RenameTable
        transform.push(
            LensOp::RenameTable {
                old_name: "users".to_string(),
                new_name: "people".to_string(),
            },
            false,
        );

        // AddColumn
        transform.push(
            LensOp::AddColumn {
                table: "t".to_string(),
                column: "c".to_string(),
                column_type: ColumnType::BigInt,
                default: Value::BigInt(42),
            },
            false,
        );

        // RemoveColumn
        transform.push(
            LensOp::RemoveColumn {
                table: "t".to_string(),
                column: "old".to_string(),
                column_type: ColumnType::Boolean,
                default: Value::Boolean(false),
            },
            false,
        );

        // RenameColumn
        transform.push(
            LensOp::RenameColumn {
                table: "t".to_string(),
                old_name: "a".to_string(),
                new_name: "b".to_string(),
            },
            false,
        );

        // AddTable
        transform.push(
            LensOp::AddTable {
                table: "new_table".to_string(),
                schema: TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                    "id",
                    ColumnType::Uuid,
                )])),
            },
            false,
        );

        // RemoveTable
        transform.push(
            LensOp::RemoveTable {
                table: "old_table".to_string(),
                schema: TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
                    "x",
                    ColumnType::Text,
                )])),
            },
            false,
        );

        let encoded = encode_lens_transform(&transform);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), 6);
        assert_eq!(decoded.ops, transform.ops);
    }

    #[test]
    fn value_roundtrip_all_types() {
        let values = vec![
            Value::Null,
            Value::Integer(42),
            Value::BigInt(-12345678901234i64),
            Value::Boolean(true),
            Value::Text("hello world".to_string()),
            Value::Timestamp(1234567890123456),
            Value::Uuid(ObjectId::from_uuid(uuid::Uuid::from_u128(0xDEADBEEF))),
            Value::Bytea(vec![0, 1, 2, 3, 0, 255]),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Row {
                id: None,
                values: vec![Value::Text("a".to_string()), Value::Boolean(false)],
            },
        ];

        for original in values {
            let mut buf = Vec::new();
            ValueWire.put(&mut buf, &original);

            let mut offset = 0;
            let decoded = ValueWire.get(&buf, &mut offset).unwrap();

            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn schema_encoding_deterministic() {
        // Same schema encoded multiple times should produce identical bytes
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("b_table")
                    .column("z_col", ColumnType::Integer)
                    .column("a_col", ColumnType::Text),
            )
            .table(TableSchema::builder("a_table"))
            .build();

        let encoded1 = encode_schema(&schema);
        let encoded2 = encode_schema(&schema);

        assert_eq!(encoded1, encoded2);
    }

    #[test]
    fn decode_invalid_version() {
        let data = vec![99, 0, 0, 0, 0]; // Unknown version 99
        let result = decode_schema(&data);
        assert!(matches!(
            result,
            Err(CatalogueEncodingError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn catalogue_payloads_reject_unsupported_versions() {
        assert_eq!(
            decode_schema(&[1, 0, 0, 0, 0]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 1,
                expected: 2
            })
        );
        assert_eq!(
            decode_lens_transform(&[1, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 1,
                expected: 2
            })
        );
        assert!(matches!(
            decode_schema(&[12]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 12,
                expected: 2
            })
        ));
        assert!(matches!(
            decode_lens_transform(&[5]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 5,
                expected: 2
            })
        ));

        assert!(matches!(
            decode_permissions(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
        assert!(matches!(
            decode_permissions_bundle(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
        assert!(matches!(
            decode_permissions_head(&[2]),
            Err(CatalogueEncodingError::UnsupportedVersion {
                found: 2,
                expected: 1
            })
        ));
    }

    #[test]
    fn decode_truncated_data() {
        let data = vec![SCHEMA_VERSION]; // Version only, no table count
        let result = decode_schema(&data);
        assert!(matches!(
            result,
            Err(CatalogueEncodingError::TruncatedData { .. })
        ));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn assert_count_bound<T>(result: Result<T, CatalogueEncodingError>, context: &'static str) {
        match result {
            Err(CatalogueEncodingError::DecodeError { message }) => assert!(
                message.contains(context) && message.contains("count exceeds"),
                "expected bounded count error for {context}, got {message}"
            ),
            Err(error) => panic!("expected bounded count error for {context}, got {error}"),
            Ok(_) => panic!("huge untrusted count unexpectedly decoded for {context}"),
        }
    }

    /// Every relation-IR node kind, including one literal of each value type.
    /// Policy relations are durable (catalogue JSON and these canonical bytes),
    /// so any change to the relation IR must keep both outputs identical.
    fn golden_relation_policy() -> PolicyExpr {
        use jazz::tools::public_schema::{
            RecursionBound, RelationJoinCondition, RelationJoinKind, RelationKeyRef,
            RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
        };
        let col = |scope: Option<&str>, column: &str| RelationColumnRef {
            scope: scope.map(str::to_owned),
            column: column.to_owned(),
        };
        let uuid = ObjectId::from_uuid(uuid::Uuid::from_u128(0x0123_4567_89ab_cdef));
        let literals = vec![
            Value::Integer(-7),
            Value::BigInt(9_007_199_254_740_993),
            Value::Double(1.5),
            Value::Boolean(true),
            Value::Text("viewer".to_owned()),
            Value::Timestamp(1_700_000_000_000),
            Value::Uuid(uuid),
            Value::TransactionId([7; 16]),
            Value::Bytea(vec![0, 1, 255]),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            // The catalogue codec does not carry nested-row ids.
            Value::Row {
                id: None,
                values: vec![Value::Text("a".to_owned()), Value::Null],
            },
            Value::Enum {
                case: "active".to_owned(),
                values: vec![Value::Integer(3)],
            },
            Value::Null,
        ];
        let cmp = |column: &str, op, right| RelationPredicate::Cmp {
            left: col(Some("t"), column),
            op,
            right,
        };
        let predicate = RelationPredicate::And(vec![
            cmp(
                "a",
                RelationCmpOp::Eq,
                RelationValueRef::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
            ),
            cmp(
                "b",
                RelationCmpOp::Ne,
                RelationValueRef::OuterColumn(col(None, "owner")),
            ),
            cmp(
                "c",
                RelationCmpOp::Lt,
                RelationValueRef::RowId(RelationRowIdRef::Outer),
            ),
            cmp(
                "d",
                RelationCmpOp::Le,
                RelationValueRef::RowId(RelationRowIdRef::Frontier),
            ),
            cmp(
                "e",
                RelationCmpOp::Gt,
                RelationValueRef::RowId(RelationRowIdRef::Current),
            ),
            cmp(
                "f",
                RelationCmpOp::Ge,
                RelationValueRef::literal(Value::Integer(4)),
            ),
            RelationPredicate::In {
                left: col(Some("t"), "g"),
                values: literals
                    .into_iter()
                    .map(RelationValueRef::literal)
                    .collect(),
            },
            RelationPredicate::Or(vec![
                RelationPredicate::IsNull {
                    column: col(None, "h"),
                },
                RelationPredicate::Not(Box::new(RelationPredicate::IsNotNull {
                    column: col(Some("t"), "i"),
                })),
            ]),
            RelationPredicate::Contains {
                left: col(Some("t"), "tags"),
                right: RelationValueRef::literal(Value::Text("x".to_owned())),
            },
            RelationPredicate::EnumMatch {
                column: col(Some("t"), "status"),
                case: "active".to_owned(),
                payload: Box::new(RelationPredicate::Cmp {
                    left: col(None, "since"),
                    op: RelationCmpOp::Gt,
                    right: RelationValueRef::literal(Value::Timestamp(5)),
                }),
            },
            RelationPredicate::True,
            RelationPredicate::False,
        ]);
        let scan = |table: &str, alias: Option<&str>| RelationExpr::TableScan {
            table: String::from(table),
            alias: alias.map(str::to_owned),
        };
        let gather = RelationExpr::Gather {
            seed: Box::new(RelationExpr::Filter {
                input: Box::new(scan("teams", Some("t"))),
                predicate,
            }),
            step: Box::new(RelationExpr::Project {
                input: Box::new(scan("team_edges", None)),
                columns: vec![
                    RelationProjectColumn {
                        alias: "id".to_owned(),
                        expr: RelationProjectExpr::Column(col(Some("team_edges"), "parent")),
                    },
                    RelationProjectColumn {
                        alias: "row".to_owned(),
                        expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                    },
                ],
            }),
            frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: RecursionBound::MaxDepth(6),
            dedupe_key: vec![
                RelationKeyRef::RowId(RelationRowIdRef::Current),
                RelationKeyRef::Column(col(Some("t"), "a")),
            ],
        };
        let fixpoint = RelationExpr::Gather {
            seed: Box::new(scan("teams", None)),
            step: Box::new(scan("teams", None)),
            frontier_key: RelationKeyRef::Column(col(None, "id")),
            bound: RecursionBound::Fixpoint,
            dedupe_key: Vec::new(),
        };
        PolicyExpr::ExistsRel {
            rel: RelationExpr::Union {
                inputs: vec![
                    RelationUnionArm {
                        label: "inner".to_owned(),
                        input: RelationExpr::Join {
                            left: Box::new(gather),
                            right: Box::new(scan("access", Some("access"))),
                            on: vec![RelationJoinCondition {
                                left: col(None, "id"),
                                right: col(Some("access"), "team"),
                            }],
                            join_kind: RelationJoinKind::Inner,
                        },
                    },
                    RelationUnionArm {
                        label: "left".to_owned(),
                        input: RelationExpr::Join {
                            left: Box::new(fixpoint),
                            right: Box::new(scan("access", None)),
                            on: Vec::new(),
                            join_kind: RelationJoinKind::Left,
                        },
                    },
                ],
            },
        }
    }

    #[test]
    fn relation_policy_json_and_catalogue_bytes_are_frozen() {
        let policy = golden_relation_policy();
        let json = serde_json::to_string(&policy).unwrap();
        let permissions = HashMap::from([(
            TableName::new("docs"),
            TablePolicies::new().with_select(policy.clone()),
        )]);
        let bytes = encode_permissions(&permissions);
        assert_eq!(json, GOLDEN_RELATION_POLICY_JSON);
        assert_eq!(hex(&bytes), GOLDEN_RELATION_POLICY_HEX);
        assert_eq!(serde_json::from_str::<PolicyExpr>(&json).unwrap(), policy);
        assert_eq!(decode_permissions(&bytes).unwrap(), permissions);
    }

    const GOLDEN_RELATION_POLICY_JSON: &str = r#"{"type":"ExistsRel","rel":{"Union":{"inputs":[{"label":"inner","input":{"Join":{"left":{"Gather":{"seed":{"Filter":{"input":{"TableScan":{"table":"teams","alias":"t"}},"predicate":{"And":[{"Cmp":{"left":{"scope":"t","column":"a"},"op":"Eq","right":{"SessionRef":["claims","sub"]}}},{"Cmp":{"left":{"scope":"t","column":"b"},"op":"Ne","right":{"OuterColumn":{"scope":null,"column":"owner"}}}},{"Cmp":{"left":{"scope":"t","column":"c"},"op":"Lt","right":{"RowId":"Outer"}}},{"Cmp":{"left":{"scope":"t","column":"d"},"op":"Le","right":{"RowId":"Frontier"}}},{"Cmp":{"left":{"scope":"t","column":"e"},"op":"Gt","right":{"RowId":"Current"}}},{"Cmp":{"left":{"scope":"t","column":"f"},"op":"Ge","right":{"Literal":{"type":"Integer","value":4}}}},{"In":{"left":{"scope":"t","column":"g"},"values":[{"Literal":{"type":"Integer","value":-7}},{"Literal":{"type":"BigInt","value":"9007199254740993"}},{"Literal":{"type":"Double","value":1.5}},{"Literal":{"type":"Boolean","value":true}},{"Literal":{"type":"Text","value":"viewer"}},{"Literal":{"type":"Timestamp","value":1700000000000}},{"Literal":{"type":"Uuid","value":"00000000-0000-0000-0123-456789abcdef"}},{"Literal":{"type":"TransactionId","value":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7]}},{"Literal":{"type":"Bytea","value":[0,1,255]}},{"Literal":{"type":"Array","value":[{"type":"Integer","value":1},{"type":"Integer","value":2}]}},{"Literal":{"type":"Row","value":{"values":[{"type":"Text","value":"a"},{"type":"Null"}]}}},{"Literal":{"type":"Enum","value":{"case":"active","values":[{"type":"Integer","value":3}]}}},{"Literal":{"type":"Null"}}]}},{"Or":[{"IsNull":{"column":{"scope":null,"column":"h"}}},{"Not":{"IsNotNull":{"column":{"scope":"t","column":"i"}}}}]},{"Contains":{"left":{"scope":"t","column":"tags"},"right":{"Literal":{"type":"Text","value":"x"}}}},{"EnumMatch":{"column":{"scope":"t","column":"status"},"case":"active","payload":{"Cmp":{"left":{"scope":null,"column":"since"},"op":"Gt","right":{"Literal":{"type":"Timestamp","value":5}}}}}},"True","False"]}}},"step":{"Project":{"input":{"TableScan":{"table":"team_edges"}},"columns":[{"alias":"id","expr":{"Column":{"scope":"team_edges","column":"parent"}}},{"alias":"row","expr":{"RowId":"Current"}}]}},"frontier_key":{"RowId":"Current"},"bound":{"MaxDepth":6},"dedupe_key":[{"RowId":"Current"},{"Column":{"scope":"t","column":"a"}}]}},"right":{"TableScan":{"table":"access","alias":"access"}},"on":[{"left":{"scope":null,"column":"id"},"right":{"scope":"access","column":"team"}}],"join_kind":"Inner"}}},{"label":"left","input":{"Join":{"left":{"Gather":{"seed":{"TableScan":{"table":"teams"}},"step":{"TableScan":{"table":"teams"}},"frontier_key":{"Column":{"scope":null,"column":"id"}},"bound":"Fixpoint","dedupe_key":[]}},"right":{"TableScan":{"table":"access"}},"on":[],"join_kind":"Left"}}}]}}}"#;
    const GOLDEN_RELATION_POLICY_HEX: &str = "010100000004000000646f6373010d01070200000005000000696e6e6572040000006c65667404060201050000007465616d73010100000074070c00000001010100000074010000006101020200000006000000636c61696d7303000000737562010101000000740100000062020300050000006f776e6572010101000000740100000063030402010101000000740100000064040403010101000000740100000065050401010101000000740100000066060101040000000401010000007401000000670d0000000101f9ffffff01020100000000002000010a000000000000f83f01030101040600000076696577657201050068e5cf8b010000010600000000000000000123456789abcdef010c07070707070707070707070707070707010b030000000001ff0107020000000101000000010200000001080200000004010000006100010d060000006163746976650100000001030000000100080200000002000100000068090301010000007401000000690501010000007404000000746167730104010000007806010100000074060000007374617475730600000061637469766501000500000073696e636505010505000000000000000a0b05010a0000007465616d5f6564676573000200000002000000696401010a0000007465616d5f656467657306000000706172656e7403000000726f77020102010206000000000000000200000002010101010000007401000000610106000000616363657373010600000061636365737301000000000200000069640106000000616363657373040000007465616d01040601050000007465616d730001050000007465616d730001000200000069640100000000010600000061636365737300000000000200000000000000";
}
