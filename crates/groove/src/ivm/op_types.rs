//! Operator descriptors carried by IVM graph nodes.
//!
//! This module owns serializable/equatable operator payloads only: sources,
//! stateless transformations, stateful transformations, and aggregate
//! descriptors. It does not lower queries, own graph identity, or execute
//! operators; those roles live in [`super::planner`], [`super::graph`], and
//! [`super::runtime`]. The order below mirrors the execution taxonomy: sources
//! first, then stateless operators, then stateful join/recursive operators,
//! then aggregate descriptors.

use crate::ivm::graph::DurableStorage;
use crate::records::{RecordDescriptor, Value, ValueType};
use crate::schema::IndexSchema;

// Operator categories:
// - Sources: TableSourceOp, InlineRecordsOp, FrontierSourceOp, BindingSourceOp.
// - Stateless transformations: PersistOp, FilterOp, MapProjectOp,
//   UnwrapNullableOp, UnnestOp, IndexByOp.
// - Stateful transformations: JoinOp (join/semi-join/anti-join), RecursiveOp.
// - Aggregate/window: ArgMaxByOp, ArgMinByOp, TopByOp, AggregateOp.

// Sources.

/// Source node for base table deltas.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableSourceOp {
    pub table: String,
    pub scan: Option<StaticScanSpec>,
}

/// Source node for a schema-declared durable index arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexSourceOp {
    pub table: String,
    pub index: String,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PredicateExpr {
    Eq { field: String, value: LiteralValue },
    Neq { field: String, value: LiteralValue },
    Contains { field: String, value: LiteralValue },
    EqField { field: String, value_field: String },
    ContainsField { field: String, needle_field: String },
    NeqField { field: String, value_field: String },
    Gt { field: String, value: LiteralValue },
    GtEq { field: String, value: LiteralValue },
    Lt { field: String, value: LiteralValue },
    LtEq { field: String, value: LiteralValue },
    IsNull { field: String },
    IsNotNull { field: String },
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
    /// Stored as raw bits so predicates remain `Eq + Hash + Ord`.
    F64(u64),
    Bool(bool),
    Enum(u8),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Tuple(Vec<LiteralValue>),
    Array(Vec<LiteralValue>),
    Nullable(Option<Box<LiteralValue>>),
}

impl From<Value> for LiteralValue {
    fn from(value: Value) -> Self {
        match value {
            Value::U8(value) => Self::U8(value),
            Value::U16(value) => Self::U16(value),
            Value::U32(value) => Self::U32(value),
            Value::U64(value) => Self::U64(value),
            Value::I64(value) => Self::I64(value),
            Value::F64(value) => Self::F64(value.to_bits()),
            Value::Bool(value) => Self::Bool(value),
            Value::Enum(value) => Self::Enum(value),
            Value::String(value) => Self::String(value),
            Value::Bytes(value) => Self::Bytes(value),
            Value::Uuid(value) => Self::Uuid(value),
            Value::Tuple(values) => Self::Tuple(values.into_iter().map(Into::into).collect()),
            Value::Array(values) => Self::Array(values.into_iter().map(Into::into).collect()),
            Value::Nullable(value) => Self::Nullable(value.map(|value| Box::new((*value).into()))),
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
            Self::F64(_) => Some(ValueType::F64),
            Self::Bool(_) => Some(ValueType::Bool),
            Self::Enum(_) => Some(ValueType::U8),
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
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        match self {
            Self::U8(value) => Value::U8(*value),
            Self::U16(value) => Value::U16(*value),
            Self::U32(value) => Value::U32(*value),
            Self::U64(value) => Value::U64(*value),
            Self::I64(value) => Value::I64(*value),
            Self::F64(value) => Value::F64(f64::from_bits(*value)),
            Self::Bool(value) => Value::Bool(*value),
            Self::Enum(value) => Value::Enum(*value),
            Self::String(value) => Value::String(value.clone()),
            Self::Bytes(value) => Value::Bytes(value.clone()),
            Self::Uuid(value) => Value::Uuid(*value),
            Self::Tuple(values) => Value::Tuple(values.iter().map(Self::to_value).collect()),
            Self::Array(values) => Value::Array(values.iter().map(Self::to_value).collect()),
            Self::Nullable(value) => {
                Value::Nullable(value.as_ref().map(|value| Box::new(value.to_value())))
            }
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
