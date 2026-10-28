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
    /// Empty for [`JoinTarget::Uncorrelated`].
    pub on_column: String,
    /// How the target relation is matched.
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

/// Matching mode used by a [`JoinVia`] traversal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum JoinTarget {
    /// Join against a declared application column.
    #[default]
    Column,
    /// Join against the target table's row id.
    RowId,
    /// Test whether the filtered relation is nonempty, independently of the source row.
    /// This target has no join columns, lookup, or outer correlations.
    Uncorrelated,
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
    /// Recursion bound for reachable closure. `MaxDepth(0)` includes only the seed.
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
    /// Optional maximum recursive uses. Zero performs no inheritance hop and denies.
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
    /// Stop after at most this many recursive steps. The seed is at depth zero,
    /// so `MaxDepth(0)` evaluates the seed without traversing an edge. Unified
    /// lowering must carry depth through the recursive relation and filter by
    /// it; this is not the same as the fixpoint safety cap.
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
    /// fixpoint. `MaxDepth(0)` remains zero: callers that request no recursive
    /// steps must never be widened to one.
    pub fn depth_steps(self) -> usize {
        match self {
            Self::Fixpoint => 128,
            Self::MaxDepth(max_depth) => max_depth,
        }
    }
}

/// Query predicate.
///
/// Decoding is bounded: see the `Deserialize` impl below.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(
    remote = "Self",
    deny_unknown_fields,
    expecting = "a bounded policy predicate"
)]
pub enum Predicate {
    /// All child predicates must match.
    All(#[serde(deserialize_with = "deserialize_predicate_children")] Vec<Predicate>),
    /// At least one child predicate must match.
    Any(#[serde(deserialize_with = "deserialize_predicate_children")] Vec<Predicate>),
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

impl Predicate {
    /// Calls `f` on every operand of this predicate, descending through
    /// `All`, `Any`, `Not` and `EnumMatch` payloads.
    pub fn for_each_operand_mut(&mut self, f: &mut impl FnMut(&mut Operand)) {
        match self {
            Self::All(predicates) | Self::Any(predicates) => {
                for predicate in predicates {
                    predicate.for_each_operand_mut(f);
                }
            }
            Self::Not(predicate) | Self::EnumMatch { payload: predicate, .. } => {
                predicate.for_each_operand_mut(f);
            }
            Self::Eq(left, right)
            | Self::Ne(left, right)
            | Self::Gt(left, right)
            | Self::Gte(left, right)
            | Self::Lt(left, right)
            | Self::Lte(left, right)
            | Self::Contains(left, right) => {
                f(left);
                f(right);
            }
            Self::In(left, values) => {
                f(left);
                values.iter_mut().for_each(f);
            }
            Self::IsNull(operand) => f(operand),
        }
    }

    /// Whether `f` holds for some operand, visiting operands in the same
    /// order as [`Predicate::for_each_operand_mut`] and stopping at the first.
    pub fn any_operand(&self, f: &mut impl FnMut(&Operand) -> bool) -> bool {
        match self {
            Self::All(predicates) | Self::Any(predicates) => {
                predicates.iter().any(|predicate| predicate.any_operand(f))
            }
            Self::Not(predicate) | Self::EnumMatch { payload: predicate, .. } => {
                predicate.any_operand(f)
            }
            Self::Eq(left, right)
            | Self::Ne(left, right)
            | Self::Gt(left, right)
            | Self::Gte(left, right)
            | Self::Lt(left, right)
            | Self::Lte(left, right)
            | Self::Contains(left, right) => f(left) || f(right),
            Self::In(left, values) => f(left) || values.iter().any(f),
            Self::IsNull(operand) => f(operand),
        }
    }
}

impl Query {
    /// Calls `f` on each top-level predicate of the query's row conditions:
    /// its filters, join filters (including nested joins), reachable
    /// access/edge/seed filters, and the same lists in every policy branch.
    /// Flat joins, array subqueries, includes and relations are not visited.
    pub fn for_each_condition_mut(&mut self, f: &mut impl FnMut(&mut Predicate)) {
        for_each_condition_in(&mut self.filters, &mut self.joins, &mut self.reachable, f);
        for branch in &mut self.policy_branches {
            for_each_condition_in(&mut branch.filters, &mut branch.joins, &mut branch.reachable, f);
        }
    }
}

fn for_each_condition_in(
    filters: &mut [Predicate],
    joins: &mut [JoinVia],
    reachable: &mut [ReachableVia],
    f: &mut impl FnMut(&mut Predicate),
) {
    filters.iter_mut().for_each(&mut *f);
    for join in joins {
        join.for_each_condition_mut(f);
    }
    for reachable in reachable {
        reachable.access_filters.iter_mut().for_each(&mut *f);
        reachable.edge_filters.iter_mut().for_each(&mut *f);
        if let Some(seed) = &mut reachable.seed {
            seed.filters.iter_mut().for_each(&mut *f);
        }
    }
}

impl JoinVia {
    /// Calls `f` on each top-level filter of this join and its nested joins.
    pub fn for_each_condition_mut(&mut self, f: &mut impl FnMut(&mut Predicate)) {
        self.filters.iter_mut().for_each(&mut *f);
        for join in &mut self.nested_joins {
            join.for_each_condition_mut(f);
        }
    }
}

impl serde::Serialize for Predicate {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Predicate::serialize(self, serializer)
    }
}

thread_local! {
    /// Nesting depth and node budget of the predicate tree being decoded on
    /// this thread; depth 0 means no decode is in progress.
    static PREDICATE_DECODE: std::cell::RefCell<(usize, policy_limits::PolicyExpressionBudget)> =
        std::cell::RefCell::default();
}

impl<'de> serde::Deserialize<'de> for Predicate {
    /// Counts every node against the budget of the outermost predicate at its
    /// nesting depth before its contents are read, so an attacker-controlled
    /// encoding is refused before the whole tree exists.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Leave;
        impl Drop for Leave {
            fn drop(&mut self) {
                PREDICATE_DECODE.with_borrow_mut(|(depth, budget)| {
                    *depth -= 1;
                    if *depth == 0 {
                        *budget = Default::default();
                    }
                });
            }
        }
        let _leave = PREDICATE_DECODE.with_borrow_mut(|(depth, _)| {
            *depth += 1;
            Leave
        });
        PREDICATE_DECODE
            .with_borrow_mut(|(depth, budget)| budget.enter(*depth))
            .map_err(serde::de::Error::custom)?;
        Predicate::deserialize(deserializer)
    }
}

/// `All`/`Any` children: a length header the node budget cannot hold is
/// refused before any child is read or allocated.
fn deserialize_predicate_children<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Predicate>, D::Error> {
    struct Children;
    impl<'de> serde::de::Visitor<'de> for Children {
        type Value = Vec<Predicate>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded sequence of policy predicates")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let capacity = seq.size_hint().unwrap_or(0);
            PREDICATE_DECODE
                .with_borrow(|(_, budget)| budget.reserve(capacity))
                .map_err(serde::de::Error::custom)?;
            let mut children = Vec::with_capacity(capacity);
            while let Some(child) = seq.next_element()? {
                children.push(child);
            }
            Ok(children)
        }
    }
    deserializer.deserialize_seq(Children)
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
pub const PROVIDER_CLAIM_PREFIX: &str = "\0claims:";

/// CatalogueTableIdentityV1: resolved from the policy schema, never session claims.
pub const TABLE_IDENTITY_OPERAND_PREFIX: &str = "\0jazz-table-identity-v1:";

/// Collision-proof storage key for a raw provider claim exposed as
/// `session.claims[name]` in public policies.
pub fn provider_claim_key(name: &str) -> String {
    provider_claim_operand_key(name)
}

#[doc(hidden)]
pub fn provider_claim_operand_key(name: &str) -> String {
    format!("{PROVIDER_CLAIM_PREFIX}{name}")
}

// ProviderClaimPathV1: UTF-8 byte-length-prefixed segments in a namespace
// disjoint from raw provider names. Existing single-key operands are unchanged.
const PROVIDER_CLAIM_PATH_V1: &str = "\0claim-path-v1:";

#[doc(hidden)]
pub fn provider_claim_path_operand_key(segments: &[String]) -> String {
    if let [name] = segments {
        return provider_claim_operand_key(name);
    }
    let mut key = PROVIDER_CLAIM_PATH_V1.to_owned();
    for segment in segments {
        key.push_str(&format!("{}:{segment}", segment.len()));
    }
    key
}

#[doc(hidden)]
pub fn operand_claim_path(name: &str) -> Vec<String> {
    if let Some(mut rest) = name.strip_prefix(PROVIDER_CLAIM_PATH_V1) {
        let mut segments = vec!["claims".to_owned()];
        while !rest.is_empty() {
            let Some((length, tail)) = rest.split_once(':') else {
                break;
            };
            let Ok(length) = length.parse::<usize>() else {
                break;
            };
            let Some(segment) = tail.get(..length) else {
                break;
            };
            segments.push(segment.to_owned());
            rest = &tail[length..];
        }
        if rest.is_empty()
            && segments.len() > 2
            && provider_claim_path_operand_key(&segments[1..]) == name
        {
            return segments;
        }
    }
    if matches!(
        name,
        "user.account" | "user.identity" | "user.identity.issuer" | "user.identity.subject"
    ) {
        return name.split('.').map(str::to_owned).collect();
    }
    name.strip_prefix(PROVIDER_CLAIM_PREFIX)
        .map(|name| vec!["claims".to_owned(), name.to_owned()])
        .unwrap_or_else(|| vec![name.to_owned()])
}

/// Recognize the finite structured author vocabulary without interpreting
/// provider claim names as paths.
pub fn author_claim_path_key(path: &[String]) -> Option<String> {
    let name = path.join(".");
    let valid = matches!(
        name.as_str(),
        "user"
            | "user.account"
            | "user.identity"
            | "user.identity.issuer"
            | "user.identity.subject"
    );
    (valid && path.iter().all(|part| !part.contains('.'))).then_some(name)
}
