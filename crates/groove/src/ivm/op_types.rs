//! Operator descriptors carried by IVM graph nodes.
//!
//! This module owns serializable/equatable operator payloads only: sources,
//! stateless transformations, stateful transformations, and aggregate
//! descriptors. It does not lower queries, own graph identity, or execute
//! operators; those roles live in [`super::planner`], [`super::graph`], and
//! [`super::runtime`]. The order below mirrors the execution taxonomy: sources
//! first, then stateless operators, then stateful join/recursive operators,
//! then aggregate descriptors.

use std::collections::BTreeMap;

use crate::ivm::graph::DurableStorage;
use crate::records::{RecordDescriptor, Value, ValueType};
use crate::schema::IndexSchema;

// Operator categories:
// - Sources: TableSourceOp, InlineRecordsOp, FrontierSourceOp, BindingSourceOp.
// - Stateless transformations: PersistOp, FilterOp, MapProjectOp,
//   UnwrapNullableOp, UnnestOp, VariantProjectOp, IndexByOp.
// - Stateful transformations: JoinOp (join/semi-join/anti-join), RecursiveOp.
// - Aggregate/window: ArgMaxByOp, ArgMinByOp, TopByOp, AggregateOp.

// Sources.

/// Source node for base table deltas.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableSourceOp {
    pub table: String,
    pub scan: Option<StaticScanSpec>,
    /// Fixed-output projection applied while heterogeneous table deltas enter
    /// the graph. The target names an append-only runtime registry whose cases
    /// are deliberately outside this node's structural identity.
    pub variant_projection: Option<VariantProjectionTarget>,
}

/// Runtime registry namespace selected by a heterogeneous table source.
///
/// Named projections are caller-defined query boundaries. Schema-index
/// projections are private, derived boundaries shared by durable maintenance
/// and live index sources.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum VariantProjectionTarget {
    Named(String),
    SchemaIndex(String),
}

/// Source node for a schema-declared durable index arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexSourceOp {
    pub table: String,
    pub index: String,
    pub intersections: Vec<(String, StaticScanSpec)>,
    /// Fixed descriptor consumed by `IndexBy` after optional variant
    /// projection. For homogeneous tables this is the ordinary table
    /// descriptor.
    pub input_descriptor: RecordDescriptor,
    pub variant_projection: Option<VariantProjectionTarget>,
    /// Named projection for indexed table-row output. Absent for the
    /// traditional encoded index-entry source.
    pub row_projection: Option<VariantProjectionTarget>,
    pub key_fields: Vec<usize>,
    pub value_fields: Vec<usize>,
    pub unique: bool,
    pub append_value_to_key: bool,
    pub store_value: bool,
    pub scan: Option<StaticScanSpec>,
}

/// Static ordered-key scan supplied at graph construction.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StaticScanSpec {
    Point(Vec<LiteralValue>),
    Prefix(Vec<LiteralValue>),
    /// A prefix scan whose physical source is proven to need no more than this
    /// many entries. This is deliberately distinct from cursor batching: it is
    /// only emitted by conservative one-shot lowering after every downstream
    /// operation that could discard or reorder a candidate has been ruled out.
    PrefixLimit {
        prefix: Vec<LiteralValue>,
        max_items: usize,
    },
    Range {
        start: Vec<LiteralValue>,
        end: Vec<LiteralValue>,
    },
}

/// Source node for snapshot-only in-memory records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InlineRecordsOp {
    pub records: Vec<Vec<u8>>,
}

/// Source node for a scoped evaluation input, such as a recursive frontier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FrontierSourceOp {
    pub binding: FrontierName,
}

/// Source node for a runtime-maintained subscription-shape parameter set.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BindingSourceOp {
    pub shape: String,
}

/// Name of a value bound in an evaluation context.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FrontierName(pub String);

// Stateless transformations.

/// Durable write-through operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PersistOp {
    pub name: String,
    pub storage: DurableStorage,
    /// Resolved output field indices used to build storage keys.
    pub key_fields: Vec<usize>,
    pub unique: bool,
}

/// Predicate filter operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FilterOp {
    pub predicate: PredicateExpr,
    pub comparison: ValueComparison,
}

/// Equality semantics attached to an operator that compares user values.
///
/// Normal query and arrangement work compares encoded value types exactly.
/// Policy evaluation is the sole exception: policy claims compare integral
/// widths and signedness by their exact `i128` value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ValueComparison {
    #[default]
    Exact,
    Policy,
}

/// Projection operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MapProjectOp {
    /// Projected expressions. Literal/null projections and node identity are
    /// driven from here; `mapping` is only the raw-copy fast path for pure field
    /// projections.
    pub expressions: Vec<ProjectionExpr>,
    /// `(input_descriptor_idx, input_field_idx)` pairs for fast record copying.
    pub mapping: Vec<(usize, usize)>,
}

/// Replace one String/Bytes field with its logical BLAKE3 checksum.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamingChecksumOp {
    pub field: String,
    pub field_idx: usize,
    pub output_field: String,
    pub window_bytes: usize,
    pub max_bytes_per_turn: usize,
}

/// Per-occurrence enum tag translation at a descriptor boundary.
///
/// The paths name an enum occurrence relative to one projected field: `root`,
/// `root/nullable`, `root/array`, `root/tuple/<n>`, `root/record/<field>`,
/// and payload children beneath a stable case path.  The tag vectors map the
/// compact source tag to a target tag; `None` deliberately makes that mapping
/// non-total. `payload_children` maps each source payload tag to its semantic
/// child root.  It is distinct from the target tag because two schemas can use
/// the same local ordinal for different concurrently introduced cases.
/// This keeps a node-local physical enum registry an optimization, rather
/// than allowing its raw tags to escape as user semantics.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecursiveEnumRemaps {
    pub scalar: BTreeMap<String, Vec<Option<u8>>>,
    pub payload: BTreeMap<String, Vec<Option<u32>>>,
    pub payload_children: BTreeMap<String, Vec<Option<String>>>,
}

/// One projected expression and optional output name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionExpr {
    pub expression: PlanExpr,
    pub output_name: Option<String>,
}

/// Nullable unwrap operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnwrapNullableOp {
    /// Field to unwrap from `Nullable(T)` to `T`.
    pub field: String,
    /// Resolved logical field index.
    pub field_idx: usize,
}

/// Array element expansion operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnnestOp {
    /// Field to expand from `Array(T)` into one output row per element.
    pub array_field: String,
    /// Resolved logical array field index.
    pub array_field_idx: usize,
    /// Output field carrying the current array element.
    pub element_field: String,
}

/// Select one named case from an enum field and emit its fixed payload record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VariantProjectOp {
    /// Field containing `Enum(schema)` values.
    pub field: String,
    /// Resolved logical field index.
    pub field_idx: usize,
    /// Stable declaration-order tag of the selected case.
    pub tag: u32,
}

/// In-memory or schema-backed index construction descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexByOp {
    /// Structural identity in field-name form.
    pub key_expressions: Vec<PlanExpr>,
    pub value_expressions: Vec<PlanExpr>,
    /// Present when this IndexBy represents an explicit schema index.
    pub explicit_index: Option<IndexSchema>,
    /// Resolved input field indices used by the runtime.
    pub key_fields: Vec<usize>,
    pub value_fields: Vec<usize>,
    pub unique: bool,
    pub append_value_to_key: bool,
    pub store_value: bool,
    pub scan: Option<StaticScanSpec>,
}

// Stateful transformations.

/// Binary join operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinOp {
    pub kind: JoinOpKind,
    pub left_key: Vec<PlanExpr>,
    pub right_key: Vec<PlanExpr>,
    pub left_descriptor: RecordDescriptor,
    pub right_descriptor: RecordDescriptor,
    pub residual_predicate: Option<PlanExpr>,
    pub comparison: ValueComparison,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinOpKind {
    Inner,
    Left,
    Right,
    Full,
}

/// Fixed-point operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecursiveOp {
    /// Binding used by FrontierSource nodes inside the recursive step graph.
    pub frontier: FrontierName,
    /// Hard stop for non-settling recursive queries, especially cyclic bag
    /// semantics where multiplicities can grow forever.
    pub max_iters: usize,
    /// Tables read by the seed and step graphs, cached when the graph is compiled.
    pub read_tables: Vec<String>,
}

// Aggregate.

/// Per-group maximum operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArgMaxByOp {
    /// Grouping fields, in primary-key prefix order.
    pub group_fields: Vec<String>,
    /// Ordering fields, immediately after `group_fields` in the primary key.
    pub order_fields: Vec<String>,
    /// Resolved logical field indices for `group_fields`.
    pub group_field_indices: Vec<usize>,
    /// Resolved logical field indices for the full primary key.
    pub primary_key_field_indices: Vec<usize>,
}

/// Per-group minimum operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArgMinByOp {
    /// Grouping fields, in primary-key prefix order.
    pub group_fields: Vec<String>,
    /// Ordering fields, immediately after `group_fields` in the primary key.
    pub order_fields: Vec<String>,
    /// Resolved logical field indices for `group_fields`.
    pub group_field_indices: Vec<usize>,
    /// Resolved logical field indices for the full primary key.
    pub primary_key_field_indices: Vec<usize>,
}

/// Per-group ordered top-N window descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopByOp {
    /// Grouping fields that define independent partitions.
    pub group_fields: Vec<String>,
    /// Resolved logical field indices for `group_fields`.
    pub group_field_indices: Vec<usize>,
    /// Ordered fields, before tie fields.
    pub order_fields: Vec<TopByOrderField>,
    /// Stable tie fields appended after `order_fields`.
    pub tie_fields: Vec<String>,
    /// Resolved logical field indices for `order_fields` plus `tie_fields`.
    pub sort_field_indices: Vec<usize>,
    /// Direction for each `sort_field_indices` entry.
    pub sort_directions: Vec<TopByDirection>,
    /// Number of leading ordinals excluded from the retained window.
    pub offset: u64,
    /// Finite retained length or an explicitly unbounded suffix.
    pub limit: TopByLimit,
}

/// Retained-length bound for a [`TopByOp`] window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopByLimit {
    /// Retain at most this many ordinals after the offset.
    Finite(u64),
    /// Retain every ordinal after the offset.
    Unbounded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopByOrderField {
    pub field: String,
    pub direction: TopByDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopByDirection {
    Asc,
    Desc,
}

/// One input field copied into a rendered `CollectBy` parent or child record.
///
/// The resolved field index is kept alongside the name because the descriptor
/// is the graph sharing boundary, while the runtime must not re-resolve names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectByProjection {
    pub field: String,
    pub field_idx: usize,
    pub output_name: String,
    pub unwrap_nullable: bool,
}

/// One named, ordered child array rendered by a [`CollectByOp`].
///
/// A slot's group fields identify the record that owns this array.  They are
/// compared against the source record from which that owner was rendered;
/// nested slots therefore stay inside one terminal rather than becoming graph
/// nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectBySlot {
    pub group_fields: Vec<String>,
    pub group_field_indices: Vec<usize>,
    pub child_fields: Vec<CollectByProjection>,
    pub child_descriptor: RecordDescriptor,
    pub collection_field: String,
    pub collection_field_index: usize,
    pub slots: Vec<CollectBySlot>,
    pub order_fields: Vec<TopByOrderField>,
    pub tie_fields: Vec<String>,
    /// Optional boolean field that distinguishes a real association row from
    /// a parent anchor retained solely to render an empty collection.
    pub presence_field_index: Option<usize>,
    pub sort_field_indices: Vec<usize>,
    pub sort_directions: Vec<TopByDirection>,
    pub offset: u64,
    pub limit: TopByLimit,
}

/// Terminal collector for one rendered parent and, optionally, a tree of
/// named `Array<Record>` slots.
///
/// This descriptor intentionally contains all of the flat input projections
/// and ranking data needed to render its output. No planner-side state is
/// allowed to affect a shared collector node.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectByOp {
    pub mode: CollectByMode,
    pub group_fields: Vec<String>,
    pub group_field_indices: Vec<usize>,
    pub parent_fields: Vec<CollectByProjection>,
    pub child_fields: Vec<CollectByProjection>,
    pub child_descriptor: RecordDescriptor,
    pub collection_field: String,
    pub collection_field_index: usize,
    /// Recursive collect-mode slots.  An empty vector denotes the legacy
    /// single-slot descriptor retained for API compatibility.
    pub slots: Vec<CollectBySlot>,
    /// Flat output projection used only in [`CollectByMode::Expand`].
    pub tuple_fields: Vec<CollectByProjection>,
    /// Ordered contributing source-row ids used to address expanded tuples.
    pub occurrence_id_fields: Vec<String>,
    pub occurrence_id_field_indices: Vec<usize>,
    pub order_fields: Vec<TopByOrderField>,
    pub tie_fields: Vec<String>,
    pub sort_field_indices: Vec<usize>,
    pub sort_directions: Vec<TopByDirection>,
    pub offset: u64,
    pub limit: TopByLimit,
}

/// The rendered shape selected by the terminal [`CollectByOp`].
///
/// Both variants consume the same grouped, ordered input and window.  Collect
/// owns a single parent record, while Expand owns one output occurrence per
/// selected flat tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CollectByMode {
    Collect,
    Root,
    Expand,
}

/// Placeholder aggregate descriptor for future lowering/execution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateOp {
    pub group_key: Vec<PlanExpr>,
    pub group_field_indices: Vec<usize>,
    pub aggregates: Vec<AggregateExpr>,
}

/// One aggregate output expression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub expression: Option<PlanExpr>,
    pub distinct: bool,
    pub output_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PlanExpr {
    /// Field references are deliberately structural; Debug strings are not part
    /// of canonical node identity.
    Field(String),
    Literal(LiteralValue),
    Null(ValueType),
    Nullable(String),
    NullableFlat(String),
    EnumTagRemap {
        field: String,
        tags: Vec<Option<u8>>,
    },
    EnumRemap {
        field: String,
        tags: Vec<Option<u32>>,
    },
    RecursiveEnumRemap {
        field: String,
        remaps: RecursiveEnumRemaps,
        omit_unrepresentable: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PredicateExpr {
    Eq {
        field: String,
        value: LiteralValue,
    },
    Neq {
        field: String,
        value: LiteralValue,
    },
    Contains {
        field: String,
        value: LiteralValue,
    },
    EqField {
        field: String,
        value_field: String,
    },
    ContainsField {
        field: String,
        needle_field: String,
    },
    NeqField {
        field: String,
        value_field: String,
    },
    Gt {
        field: String,
        value: LiteralValue,
    },
    GtEq {
        field: String,
        value: LiteralValue,
    },
    Lt {
        field: String,
        value: LiteralValue,
    },
    LtEq {
        field: String,
        value: LiteralValue,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    /// Match one tagged enum case and evaluate a predicate against that
    /// case's payload record. The payload predicate field names are relative
    /// to the selected case descriptor.
    EnumMatch {
        field: String,
        case_tag: u32,
        payload: Box<PredicateExpr>,
    },
    And(Vec<PredicateExpr>),
    Or(Vec<PredicateExpr>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LiteralValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I64(i64),
    I32(i32),
    /// Stored as raw bits so predicates remain `Eq + Hash + Ord`.
    F64(u64),
    Bool(bool),
    EnumTag(u8),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Tuple(Vec<LiteralValue>),
    Array(Vec<LiteralValue>),
    Nullable(Option<Box<LiteralValue>>),
    /// Record-valued predicates are intentionally unsupported in this stage.
    Record,
}

impl From<Value> for LiteralValue {
    fn from(value: Value) -> Self {
        match value {
            Value::U8(value) => Self::U8(value),
            Value::U16(value) => Self::U16(value),
            Value::U32(value) => Self::U32(value),
            Value::U64(value) => Self::U64(value),
            Value::I64(value) => Self::I64(value),
            Value::I32(value) => Self::I32(value),
            Value::F64(value) => Self::F64(value.to_bits()),
            Value::Bool(value) => Self::Bool(value),
            Value::EnumTag(value) => Self::EnumTag(value),
            Value::String(value) => Self::String(value),
            Value::Bytes(value) => Self::Bytes(value),
            Value::Uuid(value) => Self::Uuid(value),
            Value::Tuple(values) => Self::Tuple(values.into_iter().map(Into::into).collect()),
            Value::Array(values) => Self::Array(values.into_iter().map(Into::into).collect()),
            Value::Nullable(value) => Self::Nullable(value.map(|value| Box::new((*value).into()))),
            // Neither records nor tagged payload unions are supported predicate literals.
            Value::Record(_) | Value::Enum(_) | Value::Large(_) => Self::Record,
        }
    }
}

impl LiteralValue {
    pub(crate) fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::U8(_) => Some(ValueType::U8),
            Self::U16(_) => Some(ValueType::U16),
            Self::U32(_) => Some(ValueType::U32),
            Self::U64(_) => Some(ValueType::U64),
            Self::I64(_) => Some(ValueType::I64),
            Self::I32(_) => Some(ValueType::I32),
            Self::F64(_) => Some(ValueType::F64),
            Self::Bool(_) => Some(ValueType::Bool),
            Self::EnumTag(_) => Some(ValueType::U8),
            Self::String(_) => Some(ValueType::String),
            Self::Bytes(_) => Some(ValueType::Bytes),
            Self::Uuid(_) => Some(ValueType::Uuid),
            Self::Tuple(values) => values
                .iter()
                .map(Self::value_type)
                .collect::<Option<Vec<_>>>()
                .map(ValueType::Tuple),
            Self::Array(values) => values
                .first()
                .and_then(Self::value_type)
                .map(|value_type| ValueType::Array(Box::new(value_type))),
            Self::Nullable(Some(value)) => value
                .value_type()
                .map(|value_type| ValueType::Nullable(Box::new(value_type))),
            Self::Nullable(None) => None,
            Self::Record => None,
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        match self {
            Self::U8(value) => Value::U8(*value),
            Self::U16(value) => Value::U16(*value),
            Self::U32(value) => Value::U32(*value),
            Self::U64(value) => Value::U64(*value),
            Self::I64(value) => Value::I64(*value),
            Self::I32(value) => Value::I32(*value),
            Self::F64(value) => Value::F64(f64::from_bits(*value)),
            Self::Bool(value) => Value::Bool(*value),
            Self::EnumTag(value) => Value::EnumTag(*value),
            Self::String(value) => Value::String(value.clone()),
            Self::Bytes(value) => Value::Bytes(value.clone()),
            Self::Uuid(value) => Value::Uuid(*value),
            Self::Tuple(values) => Value::Tuple(values.iter().map(Self::to_value).collect()),
            Self::Array(values) => Value::Array(values.iter().map(Self::to_value).collect()),
            Self::Nullable(value) => {
                Value::Nullable(value.as_ref().map(|value| Box::new(value.to_value())))
            }
            Self::Record => unreachable!("record literals are rejected during type validation"),
        }
    }
}

impl PredicateExpr {
    pub fn canonicalize(self) -> Self {
        match self {
            Self::And(predicates) => {
                let mut predicates = predicates
                    .into_iter()
                    .flat_map(|predicate| match predicate.canonicalize() {
                        Self::And(predicates) => predicates,
                        predicate => vec![predicate],
                    })
                    .collect::<Vec<_>>();
                predicates.sort();
                Self::And(predicates)
            }
            Self::Or(predicates) => {
                let mut predicates = predicates
                    .into_iter()
                    .flat_map(|predicate| match predicate.canonicalize() {
                        Self::Or(predicates) => predicates,
                        predicate => vec![predicate],
                    })
                    .collect::<Vec<_>>();
                predicates.sort();
                Self::Or(predicates)
            }
            predicate => predicate,
        }
    }

    pub fn eq(field: impl Into<String>, value: Value) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn gt(field: impl Into<String>, value: Value) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::IsNotNull {
            field: field.into(),
        }
    }

    pub fn from_field_literal(
        kind: PredicateKind,
        field: impl Into<String>,
        value: LiteralValue,
    ) -> Self {
        let field = field.into();
        match kind {
            PredicateKind::Eq => Self::Eq { field, value },
            PredicateKind::Neq => Self::Neq { field, value },
            PredicateKind::Gt => Self::Gt { field, value },
            PredicateKind::GtEq => Self::GtEq { field, value },
            PredicateKind::Lt => Self::Lt { field, value },
            PredicateKind::LtEq => Self::LtEq { field, value },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredicateKind {
    Eq,
    Neq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

impl PredicateKind {
    pub fn reversed(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Neq => Self::Neq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
        }
    }
}

impl PlanExpr {
    pub fn field(name: impl Into<String>) -> Self {
        Self::Field(name.into())
    }

    pub fn literal(value: impl Into<LiteralValue>) -> Self {
        Self::Literal(value.into())
    }

    pub fn null(value_type: ValueType) -> Self {
        Self::Null(value_type)
    }

    pub fn nullable(name: impl Into<String>) -> Self {
        Self::Nullable(name.into())
    }

    pub fn nullable_flat(name: impl Into<String>) -> Self {
        Self::NullableFlat(name.into())
    }
}
