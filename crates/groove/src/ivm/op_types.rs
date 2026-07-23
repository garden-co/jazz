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
    /// The base table this source reads.
    pub table: String,
    /// Optional key restriction: only stored rows inside this scan feed the
    /// graph (hydration reads less, and irrelevant deltas are dropped early).
    pub scan: Option<StaticScanSpec>,
}

/// Source node for a schema-declared durable index arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexSourceOp {
    /// The indexed base table.
    pub table: String,
    /// The schema index name, for example `"albums_by_title"`.
    pub index: String,
    /// Resolved row field indices that form the index key, in key order.
    pub key_fields: Vec<usize>,
    /// Resolved row field indices carried as the index value.
    pub value_fields: Vec<usize>,
    /// `true` for a UNIQUE index: one live row per key.
    pub unique: bool,
    /// For non-unique indices, the value is appended to the storage key so
    /// equal-keyed rows still get distinct storage entries.
    pub append_value_to_key: bool,
    /// Whether the value bytes are stored in the entry's value slot (they
    /// are redundant when already appended to the key).
    pub store_value: bool,
    /// Optional key restriction, as on [`TableSourceOp`].
    pub scan: Option<StaticScanSpec>,
}

/// Static ordered-key scan supplied at graph construction.
///
/// The literal values are key parts, compared in the storage key encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StaticScanSpec {
    /// Exactly one key: every key part is pinned.
    Point(Vec<LiteralValue>),
    /// Every key starting with these leading parts.
    Prefix(Vec<LiteralValue>),
    /// Keys in `start..end` (end exclusive).
    Range {
        start: Vec<LiteralValue>,
        end: Vec<LiteralValue>,
    },
}

/// Source node for snapshot-only in-memory records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InlineRecordsOp {
    /// The encoded rows themselves; they never change after construction.
    pub records: Vec<Vec<u8>>,
}

/// Source node for a scoped evaluation input, such as a recursive frontier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FrontierSourceOp {
    /// Which bound input to read; matched against the enclosing
    /// [`RecursiveOp::frontier`].
    pub binding: FrontierName,
}

/// Source node for a runtime-maintained subscription-shape parameter set.
///
/// This is the "bindings as data" door of prepared shapes: the rows flowing
/// out of this source are the currently bound parameter tuples, so binding
/// or unbinding a subscription is just a delta on this input.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BindingSourceOp {
    /// The prepared shape this binding set belongs to.
    pub shape: String,
}

/// Name of a value bound in an evaluation context.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FrontierName(pub String);

// Stateless transformations.

/// Durable write-through operator descriptor.
///
/// A `Persist` node forwards its input unchanged while also writing every
/// delta to storage; this is how secondary indices stay durable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PersistOp {
    /// Name of the persisted state, for example the index name.
    pub name: String,
    /// Which storage column family the deltas are written to.
    pub storage: DurableStorage,
    /// Resolved output field indices used to build storage keys.
    pub key_fields: Vec<usize>,
    /// When `true`, two live rows with the same key are rejected as a
    /// uniqueness violation.
    pub unique: bool,
}

/// Predicate filter operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FilterOp {
    /// Rows pass through when this evaluates to true; weights are untouched.
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
    /// What the output field holds (a copied field, a literal, ...).
    pub expression: PlanExpr,
    /// The output field's name; `None` keeps the source field's name.
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
    /// Resolved input field indices carried as the index value.
    pub value_fields: Vec<usize>,
    /// `true` for a UNIQUE index: one live row per key.
    pub unique: bool,
    /// For non-unique indices, the value is appended to the storage key so
    /// equal-keyed rows still get distinct entries.
    pub append_value_to_key: bool,
    /// Whether the value bytes are also stored in the entry's value slot.
    pub store_value: bool,
    /// Optional key restriction, as on [`TableSourceOp`].
    pub scan: Option<StaticScanSpec>,
}

// Stateful transformations.

/// Binary join operator descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinOp {
    /// Which rows the join keeps; see [`JoinOpKind`].
    pub kind: JoinOpKind,
    /// Join-key expressions on the left input; matched position by position
    /// against `right_key` (`left_key[i] = right_key[i]`).
    pub left_key: Vec<PlanExpr>,
    /// Join-key expressions on the right input.
    pub right_key: Vec<PlanExpr>,
    /// Row layout of the left input.
    pub left_descriptor: RecordDescriptor,
    /// Row layout of the right input.
    pub right_descriptor: RecordDescriptor,
    /// Extra predicate evaluated on each joined row, for conditions that are
    /// not plain key equality (for example `a.x < b.y`).
    pub residual_predicate: Option<PlanExpr>,
}

/// Which rows a lowered join keeps. (Semi and anti joins are separate graph
/// node kinds that reuse the [`JoinOp`] descriptor, so they do not appear
/// here.)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinOpKind {
    /// Only matching left/right pairs.
    Inner,
    /// Every left row; unmatched ones get NULL right columns.
    Left,
    /// Every right row; unmatched ones get NULL left columns.
    Right,
    /// Every row from both sides.
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

/// One `ORDER BY` entry of a [`TopByOp`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopByOrderField {
    /// The field to order by.
    pub field: String,
    /// Ascending or descending.
    pub direction: TopByDirection,
}

/// Sort direction of one [`TopByOp`] sort field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopByDirection {
    /// Smallest first.
    Asc,
    /// Largest first.
    Desc,
}

/// Placeholder aggregate descriptor for future lowering/execution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateOp {
    /// The `GROUP BY` key expressions.
    pub group_key: Vec<PlanExpr>,
    /// Resolved logical field indices behind `group_key`.
    pub group_field_indices: Vec<usize>,
    /// The aggregate outputs, one per result column.
    pub aggregates: Vec<AggregateExpr>,
}

/// One aggregate output expression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    /// Which aggregate to compute.
    pub function: AggregateFunction,
    /// The aggregated expression; `None` for `count(*)`.
    pub expression: Option<PlanExpr>,
    /// `true` for `count(DISTINCT x)`-style calls.
    pub distinct: bool,
    /// The output column's name, if aliased.
    pub output_name: Option<String>,
}

/// The supported aggregate functions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    /// `count(...)`
    Count,
    /// `sum(...)`
    Sum,
    /// `avg(...)`
    Avg,
    /// `min(...)`
    Min,
    /// `max(...)`
    Max,
}

/// A lowered (plan-side) expression: what one output field or key part is
/// computed from. Far smaller than the query AST's `Expr` — by this point the
/// planner has reduced everything to these forms.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PlanExpr {
    /// Field references are deliberately structural; Debug strings are not part
    /// of canonical node identity.
    Field(String),
    /// A constant value.
    Literal(LiteralValue),
    /// A typed NULL constant (the type says how to encode it).
    Null(ValueType),
    /// The named field wrapped as a *present* nullable — used when a
    /// non-nullable input feeds a nullable output column (for example the
    /// inner side of a left join).
    Nullable(String),
    /// Like `Nullable`, but the field is copied unchanged when it is already
    /// nullable instead of being double-wrapped.
    NullableFlat(String),
}

/// A lowered filter predicate, evaluated per row by `Filter` nodes.
///
/// Comparisons follow SQL NULL semantics: a NULL operand makes the
/// comparison false (never true), and only `IsNull` / `IsNotNull` test NULL
/// itself.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PredicateExpr {
    /// `field = value`.
    Eq { field: String, value: LiteralValue },
    /// `field <> value`.
    Neq { field: String, value: LiteralValue },
    /// `field` contains `value`: substring match for strings, element
    /// membership for arrays.
    Contains { field: String, value: LiteralValue },
    /// `field = value_field` (two fields of the same row).
    EqField { field: String, value_field: String },
    /// `field` contains the value of `needle_field` (same row).
    ContainsField { field: String, needle_field: String },
    /// `field <> value_field` (two fields of the same row).
    NeqField { field: String, value_field: String },
    /// `field > value`.
    Gt { field: String, value: LiteralValue },
    /// `field >= value`.
    GtEq { field: String, value: LiteralValue },
    /// `field < value`.
    Lt { field: String, value: LiteralValue },
    /// `field <= value`.
    LtEq { field: String, value: LiteralValue },
    /// `field IS NULL`.
    IsNull { field: String },
    /// `field IS NOT NULL`.
    IsNotNull { field: String },
    /// Every sub-predicate must hold.
    And(Vec<PredicateExpr>),
    /// At least one sub-predicate must hold.
    Or(Vec<PredicateExpr>),
}

/// A plan-side constant.
///
/// This mirrors [`Value`] but is `Eq + Hash + Ord`, which node identity and
/// predicate canonicalization require ([`Value`] itself is not, because of
/// `f64`). Convert with `From<Value>` and `LiteralValue::to_value`.
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
    /// An enum discriminant byte.
    Enum(u8),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Tuple(Vec<LiteralValue>),
    Array(Vec<LiteralValue>),
    /// `None` is the NULL literal.
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
    /// Infers the [`ValueType`] this literal would encode as, when it can be
    /// known from the value alone. Empty arrays and bare NULLs return `None`
    /// — nothing in them says what the element/inner type is.
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

    /// Converts back to a runtime [`Value`] (the reverse of `From<Value>`).
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
    /// Rewrites the predicate into a canonical shape: nested `And`s/`Or`s
    /// are flattened and their operands sorted.
    ///
    /// Two logically identical predicates written in different orders (`a
    /// AND b` vs `b AND a`) then compare equal, so the graph can deduplicate
    /// the filter nodes built from them.
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

    /// Shorthand for `field = value`.
    pub fn eq(field: impl Into<String>, value: Value) -> Self {
        Self::Eq {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Shorthand for `field > value`.
    pub fn gt(field: impl Into<String>, value: Value) -> Self {
        Self::Gt {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Shorthand for `field IS NULL`.
    pub fn is_null(field: impl Into<String>) -> Self {
        Self::IsNull {
            field: field.into(),
        }
    }

    /// Shorthand for `field IS NOT NULL`.
    pub fn is_not_null(field: impl Into<String>) -> Self {
        Self::IsNotNull {
            field: field.into(),
        }
    }

    /// Builds the field-vs-literal comparison selected by `kind`, so callers
    /// can construct comparisons generically instead of matching on six
    /// variants.
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

/// A comparison operator by itself, without its operands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredicateKind {
    /// `=`
    Eq,
    /// `<>`
    Neq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
}

impl PredicateKind {
    /// The operator with its operands swapped: `a < b` is `b > a`. Used to
    /// normalize literal-on-the-left comparisons into field-first form.
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
    /// Shorthand for [`PlanExpr::Field`].
    pub fn field(name: impl Into<String>) -> Self {
        Self::Field(name.into())
    }

    /// Shorthand for [`PlanExpr::Literal`].
    pub fn literal(value: impl Into<LiteralValue>) -> Self {
        Self::Literal(value.into())
    }

    /// Shorthand for [`PlanExpr::Null`].
    pub fn null(value_type: ValueType) -> Self {
        Self::Null(value_type)
    }

    /// Shorthand for [`PlanExpr::Nullable`].
    pub fn nullable(name: impl Into<String>) -> Self {
        Self::Nullable(name.into())
    }

    /// Shorthand for [`PlanExpr::NullableFlat`].
    pub fn nullable_flat(name: impl Into<String>) -> Self {
        Self::NullableFlat(name.into())
    }
}
