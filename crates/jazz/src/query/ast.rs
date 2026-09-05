/// Namespace used for query shape and binding UUIDv5 ids.
pub const QUERY_NAMESPACE: uuid::Uuid = uuid::uuid!("5d39e9ed-88f3-5b58-b8db-8786b02f5d2f");

/// v0 query AST.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Query {
    /// Root table.
    pub table: String,
    /// Conjunctive filters. Flat-join filters may address a source-qualified
    /// column and are evaluated on that source before the inner join.
    pub filters: Vec<Predicate>,
    /// Junction traversals.
    pub joins: Vec<JoinVia>,
    /// Flat relational output. This is deliberately distinct from `joins`:
    /// `JoinVia` is an existential traversal, while a flat join emits every
    /// matching source tuple.
    #[serde(default)]
    pub flat_join: Option<FlatJoin>,
    /// Policy-only disjunctive branches.
    #[serde(default)]
    pub policy_branches: Vec<PolicyBranch>,
    /// Recursive reachability traversals.
    pub reachable: Vec<ReachableVia>,
    /// Parent-policy inheritance atoms.
    #[serde(default)]
    pub inherits: Vec<InheritsVia>,
    /// Included reference paths.
    pub includes: Vec<Include>,
    /// Correlated arrays assembled into recursive values by the output terminal.
    #[serde(default)]
    pub array_subqueries: Vec<ArraySubquery>,
    /// Selected application columns. Row id is always included.
    #[serde(default)]
    pub select: Option<Vec<String>>,
    /// Result-level ordering keys, applied in order before pagination.
    #[serde(default)]
    pub order_by: Vec<OrderBy>,
    /// Result-level aggregate output. Boxed so a non-aggregate `Query` (the
    /// common case) stays small — this flows into `SyncMessage`, so its size
    /// is on the sync hot path.
    #[serde(default)]
    pub aggregate: Option<Box<AggregateQuery>>,
    /// Maximum number of rows.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of rows to skip after filtering.
    #[serde(default)]
    pub offset: usize,
    /// Retained output-changing relation facade. This is normalized directly
    /// into the row-set algebra; `table` remains the real-row materialization
    /// table for the result.
    #[serde(default, with = "relation_query_wire")]
    pub relation: Option<RelationQuery>,
}

/// Human-readable query JSON retains the relation tree. Non-human serializers
/// carry the explicit bounded JRQ v1 binary grammar.
pub(crate) mod relation_query_wire {
    use super::{decode_relation_query_v1_exact, encode_relation_query_v1, RelationQuery};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<RelationQuery>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }
        value.as_ref().map(encode_relation_query_v1).transpose()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<RelationQuery>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return Option::<RelationQuery>::deserialize(deserializer);
        }
        Option::<Vec<u8>>::deserialize(deserializer)?
            .map(|encoded| decode_relation_query_v1_exact(&encoded).map_err(serde::de::Error::custom))
            .transpose()
    }

    pub fn serialize_required<S>(value: &RelationQuery, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer { if serializer.is_human_readable() { value.serialize(serializer) } else { encode_relation_query_v1(value).map_err(serde::ser::Error::custom)?.serialize(serializer) } }
    pub fn deserialize_required<'de, D>(deserializer: D) -> Result<RelationQuery, D::Error>
    where D: Deserializer<'de> { if deserializer.is_human_readable() { RelationQuery::deserialize(deserializer) } else { let bytes=Vec::<u8>::deserialize(deserializer)?; decode_relation_query_v1_exact(&bytes).map_err(serde::de::Error::custom) } }
}

/// Output-changing relational join syntax.
///
/// Sources are ordered: the query root is position zero and every `sources`
/// entry contributes one later position to the output occurrence identity.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FlatJoin {
    /// Optional public name for the root source.
    pub root_alias: Option<String>,
    /// Joined sources in declared order.
    pub sources: Vec<FlatJoinSource>,
}

/// One source introduced by a [`FlatJoin`].
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FlatJoinSource {
    /// Source table.
    pub table: String,
    /// Effective source name used by qualified fields.
    pub alias: Option<String>,
    /// Equality from the accumulated left tuple to this source.
    pub on: FlatJoinOn,
}

/// Qualified equality predicate for one flat-join source.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct FlatJoinOn {
    /// Qualified field in the accumulated left tuple.
    pub left: String,
    /// Qualified field in the right source.
    pub right: String,
}

/// Output-changing relation query used by alpha-compatible `hopTo`/`gather`.
///
/// This is facade syntax only. The compiler boundary must normalize relation
/// queries into the same row-set shape as ordinary queries before execution;
/// relation queries must not grow a separate validated/cache/runtime identity.
#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RelationQuery {
    pub rel: RelationExpr,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelationExpr {
    TableScan {
        table: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Filter {
        input: Box<RelationExpr>,
        predicate: RelationPredicate,
    },
    Union {
        inputs: Vec<RelationUnionArm>,
    },
    Join {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
        on: Vec<RelationJoinCondition>,
        join_kind: RelationJoinKind,
    },
    Project {
        input: Box<RelationExpr>,
        columns: Vec<RelationProjectColumn>,
    },
    Gather {
        seed: Box<RelationExpr>,
        step: Box<RelationExpr>,
        frontier_key: RelationKeyRef,
        #[serde(default = "RecursionBound::default_max_depth")]
        bound: RecursionBound,
        dedupe_key: Vec<RelationKeyRef>,
    },
    Distinct {
        input: Box<RelationExpr>,
        key: Vec<RelationKeyRef>,
    },
    OrderBy {
        input: Box<RelationExpr>,
        terms: Vec<RelationOrderBy>,
    },
    Offset {
        input: Box<RelationExpr>,
        offset: usize,
    },
    Limit {
        input: Box<RelationExpr>,
        limit: usize,
    },
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RelationUnionArm {
    pub label: String,
    pub input: RelationExpr,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelationPredicate {
    Cmp {
        left: RelationColumnRef,
        op: RelationCmpOp,
        right: RelationValueRef,
    },
    IsNull {
        column: RelationColumnRef,
    },
    IsNotNull {
        column: RelationColumnRef,
    },
    In {
        left: RelationColumnRef,
        values: Vec<RelationValueRef>,
    },
    Contains {
        left: RelationColumnRef,
        right: RelationValueRef,
    },
    /// Match one tagged payload-enum case. The nested predicate's column refs
    /// name fields in the selected case payload and are deliberately unscoped.
    EnumMatch {
        column: RelationColumnRef,
        case: String,
        payload: Box<RelationPredicate>,
    },
    And(Vec<RelationPredicate>),
    Or(Vec<RelationPredicate>),
    Not(Box<RelationPredicate>),
    True,
    False,
}

#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelationCmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct RelationColumnRef {
    #[serde(default)]
    pub scope: Option<String>,
    pub column: String,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelationValueRef {
    Literal(serde_json::Value),
    Param(String),
    SessionRef(Vec<String>),
    OuterColumn(RelationColumnRef),
    FrontierColumn(RelationColumnRef),
    RowId(RelationRowIdRef),
}

#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelationRowIdRef {
    Current,
    Outer,
    Frontier,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelationJoinKind {
    Inner,
    Left,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelationJoinCondition {
    pub left: RelationColumnRef,
    pub right: RelationColumnRef,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelationKeyRef {
    Column(RelationColumnRef),
    RowId(RelationRowIdRef),
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelationProjectExpr {
    Column(RelationColumnRef),
    RowId(RelationRowIdRef),
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelationProjectColumn {
    pub alias: String,
    pub expr: RelationProjectExpr,
}

#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelationOrderBy {
    pub column: RelationColumnRef,
    pub direction: OrderDirection,
}
