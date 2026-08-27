/// One policy-only alternative for authorizing a row.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PolicyBranch {
    /// Root-table filters for this policy alternative.
    pub filters: Vec<Predicate>,
    /// Junction traversals that must be satisfied for this alternative.
    pub joins: Vec<JoinVia>,
    /// Recursive reachability traversals that must be satisfied for this alternative.
    pub reachable: Vec<ReachableVia>,
    /// Parent-policy inheritance atoms that must be satisfied for this alternative.
    #[serde(default)]
    pub inherits: Vec<InheritsVia>,
}

impl PolicyBranch {
    /// Convert a policy query into all policy-only alternatives it represents,
    /// discarding query-only output options.
    ///
    /// `Predicate::Any(Vec::new())` is the schema-converter's explicit
    /// constant-false base used for pure disjunctions. Empty filters are a true
    /// base and must be retained.
    pub fn alternatives_from_query(query: Query) -> Vec<Self> {
        let Query {
            filters,
            joins,
            policy_branches,
            reachable,
            inherits,
            ..
        } = query;
        let base_is_converter_false = matches!(filters.as_slice(), [Predicate::Any(predicates)] if predicates.is_empty())
            && joins.is_empty()
            && reachable.is_empty()
            && inherits.is_empty();

        let mut alternatives = Vec::new();
        if !base_is_converter_false {
            alternatives.push(Self {
                filters,
                joins,
                reachable,
                inherits,
            });
        }
        alternatives.extend(policy_branches);
        alternatives
    }

    /// Convert a query that is expected to represent exactly one policy
    /// alternative. Panics if the query contains nested alternatives.
    pub fn single_alternative_from_query(query: Query) -> Self {
        let alternatives = Self::alternatives_from_query(query);
        assert_eq!(
            alternatives.len(),
            1,
            "expected exactly one policy alternative; use alternatives_from_query to preserve disjunctions"
        );
        alternatives
            .into_iter()
            .next()
            .expect("length checked above")
    }
}

/// Content-addressed query shape id.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ShapeId(pub uuid::Uuid);

/// Content-addressed query binding id.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct BindingId(pub uuid::Uuid);

/// Include join mode for unresolvable reference targets.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum JoinMode {
    /// Drop the parent row when the included target is not locally resolvable.
    Inner,
    /// Keep the parent row and expose a hole/null for the include.
    Holes,
}

/// Included reference path and view-side options.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct Include {
    /// Dot-separated reference path.
    pub path: String,
    /// View-side missing-target behavior.
    pub join_mode: JoinMode,
    /// Require every include target to be resolvable.
    pub require: bool,
}

/// Requirement mode for a correlated relation array.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum ArraySubqueryRequirement {
    /// Keep the parent row even when no readable child rows match.
    #[default]
    Optional,
    /// Keep only parent rows with at least one readable matching child.
    AtLeastOne,
    /// Keep only parent rows whose correlation has a complete matching child set.
    MatchCorrelationCardinality,
}

/// Correlated array assembled into its named slot by the output terminal.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ArraySubquery {
    /// Name of the output relation.
    pub column_name: String,
    /// Inner table queried for relation targets.
    pub table: String,
    /// Column on the inner table correlated with the parent scope.
    pub inner_column: String,
    /// Column on the parent scope used as the correlation value.
    pub outer_column: String,
    /// Child-local filters.
    pub filters: Vec<Predicate>,
    /// Child-local selected application columns. Row id is always included.
    #[serde(default)]
    pub select: Option<Vec<String>>,
    /// Child-local ordering keys.
    #[serde(default)]
    pub order_by: Vec<OrderBy>,
    /// Child-local row limit.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of child rows to skip after filtering and ordering.
    #[serde(default)]
    pub offset: usize,
    /// Parent membership requirement for this relation.
    #[serde(default)]
    pub requirement: ArraySubqueryRequirement,
    /// Nested correlated relation arrays rooted at child rows.
    #[serde(default)]
    pub nested_arrays: Vec<ArraySubquery>,
}

impl ArraySubquery {
    /// Construct a correlated relation array subquery.
    pub fn new(
        column_name: impl Into<String>,
        table: impl Into<String>,
        inner_column: impl Into<String>,
        outer_column: impl Into<String>,
    ) -> Self {
        fn local_column(column: impl Into<String>) -> String {
            let column = column.into();
            column
                .rsplit_once('.')
                .map_or(column.clone(), |(_, local)| local.to_owned())
        }

        Self {
            column_name: column_name.into(),
            table: table.into(),
            inner_column: local_column(inner_column),
            outer_column: local_column(outer_column),
            filters: Vec::new(),
            select: None,
            order_by: Vec::new(),
            limit: None,
            offset: 0,
            requirement: ArraySubqueryRequirement::Optional,
            nested_arrays: Vec::new(),
        }
    }

    /// Add a child-local filter.
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Select child application columns. The row id is always included.
    pub fn select(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.select = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add a child-local ordering key.
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    /// Limit child rows after filtering and ordering.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip child rows after filtering and ordering.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Set the parent membership requirement.
    pub fn requirement(mut self, requirement: ArraySubqueryRequirement) -> Self {
        self.requirement = requirement;
        self
    }

    /// Add a nested correlated relation array rooted at child rows.
    pub fn nested(mut self, subquery: ArraySubquery) -> Self {
        self.nested_arrays.push(subquery);
        self
    }
}

/// Sort direction for a query ordering key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum OrderDirection {
    /// Sort ascending.
    Asc,
    /// Sort descending.
    Desc,
}

/// Result-level ordering key.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct OrderBy {
    /// Root-table column to order by.
    pub column: String,
    /// Sort direction.
    pub direction: OrderDirection,
}

/// Result-level aggregate query.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AggregateQuery {
    /// Aggregate expressions to compute.
    pub aggregates: Vec<Aggregate>,
    /// Optional root-table grouping column.
    #[serde(default)]
    pub group_by: Option<String>,
}

impl AggregateQuery {
    /// Construct an aggregate query expression list.
    pub fn new(aggregates: impl IntoIterator<Item = Aggregate>) -> Self {
        Self {
            aggregates: aggregates.into_iter().collect(),
            group_by: None,
        }
    }
}

/// Aggregate expression.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Aggregate {
    /// Aggregate function.
    pub function: AggregateFunction,
    /// Source column, absent for COUNT(*).
    #[serde(default)]
    pub column: Option<String>,
    /// Output column name.
    pub alias: String,
}

impl Aggregate {
    /// COUNT(*).
    pub fn count() -> Self {
        Self {
            function: AggregateFunction::Count,
            column: None,
            alias: "count".to_owned(),
        }
    }

    /// SUM(column).
    pub fn sum(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            function: AggregateFunction::Sum,
            alias: format!("sum_{column}"),
            column: Some(column),
        }
    }

    /// AVG(column).
    pub fn avg(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            function: AggregateFunction::Avg,
            alias: format!("avg_{column}"),
            column: Some(column),
        }
    }

    /// MIN(column).
    pub fn min(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            function: AggregateFunction::Min,
            alias: format!("min_{column}"),
            column: Some(column),
        }
    }

    /// MAX(column).
    pub fn max(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            function: AggregateFunction::Max,
            alias: format!("max_{column}"),
            column: Some(column),
        }
    }

    /// Override the output column name.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }
}

/// Aggregate function.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
pub enum AggregateFunction {
    /// Count rows.
    Count,
    /// Sum numeric values.
    Sum,
    /// Average numeric values.
    Avg,
    /// Minimum orderable value.
    Min,
    /// Maximum orderable value.
    Max,
}

impl Include {
    /// Construct an include with the default inner join mode.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            join_mode: JoinMode::Inner,
            require: false,
        }
    }

    /// Set include join mode.
    pub fn join_mode(mut self, join_mode: JoinMode) -> Self {
        self.join_mode = join_mode;
        self
    }

    /// Require included targets to be resolvable.
    pub fn require_includes(mut self) -> Self {
        self.require = true;
        self
    }
}

/// Junction traversal.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct JoinVia {
    /// Junction table.
    pub table: String,
    /// Column on the junction/target table. For [`JoinTarget::RowId`], this is
    /// the public row-id name and execution uses the table's internal row UUID.
    pub on_column: String,
    /// Which target-table field `on_column` names.
    #[serde(default)]
    pub target: JoinTarget,
    /// Optional root-table column used for row-correlated policy joins.
    #[serde(default)]
    pub source_column: Option<String>,
    /// Optional parent-row lookup used when a policy inherited through a
    /// reference needs to correlate through a column on the referenced row.
    #[serde(default)]
    pub source_lookup: Option<JoinSourceLookup>,
    /// Additional equality correlations from joined-table columns to columns
    /// on the source row currently being checked.
    #[serde(default)]
    pub correlated_filters: Vec<JoinCorrelation>,
    /// Filters evaluated on the junction table.
    pub filters: Vec<Predicate>,
    /// Additional joins evaluated relative to the joined row.
    #[serde(default)]
    pub nested_joins: Vec<JoinVia>,
}

/// Additional row correlation required by a [`JoinVia`] traversal.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct JoinCorrelation {
    /// Column on the joined table.
    pub join_column: String,
    /// Column on the source row.
    pub source_column: String,
}

/// How a [`JoinVia`] derives its target value from a referenced source row.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct JoinSourceLookup {
    /// Referenced table to look up from the root row.
    pub table: String,
    /// Root-table column that stores the referenced row id.
    pub row_id_source_column: String,
    /// Column to read from the referenced row and use as this join's target.
    pub value_column: String,
}

/// Target-table field used by a [`JoinVia`] traversal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum JoinTarget {
    /// Join against a declared application column.
    #[default]
    Column,
    /// Join against the target table's row id.
    RowId,
}

/// Recursive reachability through a transitive edge table plus an access table.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReachableVia {
    /// Access table that relates root rows to reachable teams.
    pub access_table: String,
    /// Access-table column referencing the root row.
    pub access_row_column: String,
    /// Access-table column referencing a team.
    pub access_team_column: String,
    /// Which access-table field `access_team_column` names.
    #[serde(default)]
    pub access_team_target: JoinTarget,
    /// Seed team, usually a claim.
    pub from: Operand,
    /// Filters on access edges.
    #[serde(default)]
    pub access_filters: Vec<Predicate>,
    /// Recursive edge table.
    pub edge_table: String,
    /// Edge-table member/source column.
    pub edge_member_column: String,
    /// Edge-table parent/destination column.
    pub edge_parent_column: String,
    /// Filters on recursive edges.
    pub edge_filters: Vec<Predicate>,
    /// Recursion bound for reachable closure.
    #[serde(default = "RecursionBound::default_max_depth")]
    pub bound: RecursionBound,
    /// Optional relation that produces initial reachable team ids.
    ///
    /// When present, this replaces `from` as the initial recursive frontier.
    /// `from` remains for the single-seed form and for older call sites.
    #[serde(default)]
    pub seed: Option<ReachableSeed>,
}

/// Relation seed for recursive reachability.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReachableSeed {
    /// Table containing seed rows.
    pub table: String,
    /// Seed-table column matched against the authenticated claim.
    #[serde(default)]
    pub user_column: Option<String>,
    /// Claim path used as the seed-table user value.
    #[serde(default)]
    pub user_claim: Option<String>,
    /// Seed-table column referencing the initial team frontier.
    pub team_column: String,
    /// Filters applied to seed rows.
    pub filters: Vec<Predicate>,
}

/// Parent-policy inheritance through a root-table reference column.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct InheritsVia {
    /// Root-table column referencing the parent row.
    pub parent_column: String,
    /// Parent operation to require for the referenced row.
    #[serde(default)]
    pub operation: InheritsOperation,
    /// Optional maximum number of recursive uses of this inheritance atom.
    #[serde(default)]
    pub max_depth: Option<usize>,
}

/// Parent operation required by an inheritance atom.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum InheritsOperation {
    /// Parent row must be readable.
    #[default]
    Select,
    /// Parent row must be insertable.
    Insert,
    /// Parent row must be updateable.
    Update,
    /// Parent row must be deletable.
    Delete,
}

/// Recursion semantics for reachability and relation gather.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RecursionBound {
    /// Continue until the recursive frontier reaches a fixpoint. Unified
    /// lowering may still apply an independent safety cap that errors if hit.
    Fixpoint,
    /// Stop after at most this many recursive steps. Unified lowering must carry
    /// depth through the recursive relation and filter by it; this is not the
    /// same as groove's internal safety cap.
    MaxDepth(usize),
}

impl RecursionBound {
    /// Legacy/default recursion bound used by old v0 query helpers.
    pub fn default_max_depth() -> Self {
        Self::MaxDepth(8)
    }

    /// This bound expressed as a step count.
    ///
    /// `Fixpoint` carries no user-facing depth, so it falls back to the
    /// conservative loop cap used by evaluator paths that are not true
    /// fixpoint. Restores the behaviour of the `iteration_cap` accessor removed
    /// in c2db5a8e4, whose last caller survived the removal and left the crate
    /// unable to compile.
    pub(crate) fn depth_steps(self) -> usize {
        match self {
            Self::Fixpoint => 128,
            Self::MaxDepth(max_depth) => max_depth.max(1),
        }
    }
}

/// Query predicate.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Predicate {
    /// All child predicates must match.
    All(Vec<Predicate>),
    /// At least one child predicate must match.
    Any(Vec<Predicate>),
    /// Child predicate must not match.
    Not(Box<Predicate>),
    /// Equality.
    Eq(Operand, Operand),
    /// Inequality.
    Ne(Operand, Operand),
    /// Membership in a literal/parameter list.
    In(Operand, Vec<Operand>),
    /// Greater than.
    Gt(Operand, Operand),
    /// Greater than or equal.
    Gte(Operand, Operand),
    /// Less than.
    Lt(Operand, Operand),
    /// Less than or equal.
    Lte(Operand, Operand),
    /// String substring or array membership.
    Contains(Operand, Operand),
    /// Match one discriminated enum case, then evaluate the predicate against
    /// that case's payload record fields.
    EnumMatch {
        /// Name of the enum column in the containing table.
        column: String,
        /// Name of the enum case that must be selected.
        case: String,
        /// Predicate evaluated against the selected case's payload record.
        payload: Box<Predicate>,
    },
    /// Nullable value is null.
    IsNull(Operand),
}

/// Predicate operand.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Operand {
    /// Column in the current table context.
    Column(String),
    /// Named binding parameter.
    Param(String),
    /// Named authorization claim supplied by the caller identity.
    Claim(String),
    /// Typed literal value.
    Literal(Value),
}

/// Collision-proof internal namespace for raw identity-provider claims.
/// Public policy paths remain `session.claims[<name>]`.
pub(crate) const PROVIDER_CLAIM_PREFIX: &str = "\0claims:";

/// Collision-proof storage key for a raw provider claim exposed as
/// `session.claims[name]` in public policies.
pub fn provider_claim_key(name: &str) -> String {
    provider_claim_operand_key(name)
}

pub(crate) fn provider_claim_operand_key(name: &str) -> String {
    format!("{PROVIDER_CLAIM_PREFIX}{name}")
}

pub(crate) fn operand_claim_path(name: &str) -> Vec<String> {
    name.strip_prefix(PROVIDER_CLAIM_PREFIX)
        .map(|name| vec!["claims".to_owned(), name.to_owned()])
        .unwrap_or_else(|| vec![name.to_owned()])
}

pub(crate) fn operand_claim_storage_key(name: &str) -> String {
    name.strip_prefix(PROVIDER_CLAIM_PREFIX)
        .map(provider_claim_key)
        .unwrap_or_else(|| name.to_owned())
}
