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
    /// it; this is not the same as groove's internal safety cap.
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
    pub(crate) fn depth_steps(self) -> usize {
        match self {
            Self::Fixpoint => 128,
            Self::MaxDepth(max_depth) => max_depth,
        }
    }
}

/// Query predicate.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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

impl<'de> serde::Deserialize<'de> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut budget = PolicyExpressionDecodeBudget::default();
        serde::de::DeserializeSeed::deserialize(
            PredicateSeed {
                budget: &mut budget,
                depth: 1,
            },
            deserializer,
        )
    }
}

#[derive(Default)]
struct PolicyExpressionDecodeBudget {
    nodes: usize,
}

impl PolicyExpressionDecodeBudget {
    fn enter<E>(&mut self, depth: usize) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        use crate::protocol_limits::{
            MAX_POLICY_EXPRESSION_DEPTH, MAX_POLICY_EXPRESSION_NODES, PolicyExpressionLimitError,
        };

        if depth > MAX_POLICY_EXPRESSION_DEPTH {
            return Err(E::custom(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_DEPTH",
                max: MAX_POLICY_EXPRESSION_DEPTH,
                actual: depth,
            }));
        }
        if self.nodes >= MAX_POLICY_EXPRESSION_NODES {
            return Err(E::custom(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_NODES",
                max: MAX_POLICY_EXPRESSION_NODES,
                actual: self.nodes + 1,
            }));
        }
        self.nodes += 1;
        Ok(())
    }

    fn reserve_children<E>(&self, children: usize) -> Result<usize, E>
    where
        E: serde::de::Error,
    {
        use crate::protocol_limits::{
            MAX_POLICY_EXPRESSION_NODES, PolicyExpressionLimitError,
        };

        let remaining = MAX_POLICY_EXPRESSION_NODES - self.nodes;
        if children > remaining {
            return Err(E::custom(PolicyExpressionLimitError {
                limit: "MAX_POLICY_EXPRESSION_NODES",
                max: MAX_POLICY_EXPRESSION_NODES,
                actual: self.nodes.saturating_add(children),
            }));
        }
        Ok(children)
    }
}

struct PredicateSeed<'a> {
    budget: &'a mut PolicyExpressionDecodeBudget,
    depth: usize,
}

impl<'de> serde::de::DeserializeSeed<'de> for PredicateSeed<'_> {
    type Value = Predicate;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.budget.enter::<D::Error>(self.depth)?;
        deserializer.deserialize_enum(
            "Predicate",
            &[
                "All",
                "Any",
                "Not",
                "Eq",
                "Ne",
                "In",
                "Gt",
                "Gte",
                "Lt",
                "Lte",
                "Contains",
                "EnumMatch",
                "IsNull",
            ],
            PredicateVisitor {
                budget: self.budget,
                depth: self.depth,
            },
        )
    }
}

struct PredicateVisitor<'a> {
    budget: &'a mut PolicyExpressionDecodeBudget,
    depth: usize,
}

impl<'de> serde::de::Visitor<'de> for PredicateVisitor<'_> {
    type Value = Predicate;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded policy predicate")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        use serde::de::VariantAccess;

        let (variant, access) = data.variant::<PredicateVariant>()?;
        let child_depth = self.depth + 1;
        match variant {
            PredicateVariant::All => access
                .newtype_variant_seed(PredicateVecSeed {
                    budget: self.budget,
                    depth: child_depth,
                })
                .map(Predicate::All),
            PredicateVariant::Any => access
                .newtype_variant_seed(PredicateVecSeed {
                    budget: self.budget,
                    depth: child_depth,
                })
                .map(Predicate::Any),
            PredicateVariant::Not => access
                .newtype_variant_seed(PredicateSeed {
                    budget: self.budget,
                    depth: child_depth,
                })
                .map(Box::new)
                .map(Predicate::Not),
            PredicateVariant::Eq => {
                decode_binary_predicate(access, Predicate::Eq)
            }
            PredicateVariant::Ne => {
                decode_binary_predicate(access, Predicate::Ne)
            }
            PredicateVariant::In => access.tuple_variant(
                2,
                InPredicateVisitor,
            ),
            PredicateVariant::Gt => {
                decode_binary_predicate(access, Predicate::Gt)
            }
            PredicateVariant::Gte => {
                decode_binary_predicate(access, Predicate::Gte)
            }
            PredicateVariant::Lt => {
                decode_binary_predicate(access, Predicate::Lt)
            }
            PredicateVariant::Lte => {
                decode_binary_predicate(access, Predicate::Lte)
            }
            PredicateVariant::Contains => {
                decode_binary_predicate(access, Predicate::Contains)
            }
            PredicateVariant::EnumMatch => access.struct_variant(
                &["column", "case", "payload"],
                EnumMatchPredicateVisitor {
                    budget: self.budget,
                    depth: child_depth,
                },
            ),
            PredicateVariant::IsNull => access
                .newtype_variant()
                .map(Predicate::IsNull),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier)]
enum PredicateVariant {
    All,
    Any,
    Not,
    Eq,
    Ne,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    EnumMatch,
    IsNull,
}

struct PredicateVecSeed<'a> {
    budget: &'a mut PolicyExpressionDecodeBudget,
    depth: usize,
}

impl<'de> serde::de::DeserializeSeed<'de> for PredicateVecSeed<'_> {
    type Value = Vec<Predicate>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(PredicateVecVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

struct PredicateVecVisitor<'a> {
    budget: &'a mut PolicyExpressionDecodeBudget,
    depth: usize,
}

impl<'de> serde::de::Visitor<'de> for PredicateVecVisitor<'_> {
    type Value = Vec<Predicate>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded sequence of policy predicates")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let capacity = match sequence.size_hint() {
            Some(children) => self.budget.reserve_children::<A::Error>(children)?,
            None => 0,
        };
        let mut predicates = Vec::with_capacity(capacity);
        while let Some(predicate) =
            sequence.next_element_seed(PredicateSeed {
                budget: self.budget,
                depth: self.depth,
            })?
        {
            predicates.push(predicate);
        }
        Ok(predicates)
    }
}

fn decode_binary_predicate<'de, A>(
    access: A,
    constructor: fn(Operand, Operand) -> Predicate,
) -> Result<Predicate, A::Error>
where
    A: serde::de::VariantAccess<'de>,
{
    serde::de::VariantAccess::tuple_variant(
        access,
        2,
        BinaryPredicateVisitor { constructor },
    )
}

struct BinaryPredicateVisitor {
    constructor: fn(Operand, Operand) -> Predicate,
}

impl<'de> serde::de::Visitor<'de> for BinaryPredicateVisitor {
    type Value = Predicate;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("two policy operands")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let left = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        let right = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
        Ok((self.constructor)(left, right))
    }
}

struct InPredicateVisitor;

impl<'de> serde::de::Visitor<'de> for InPredicateVisitor {
    type Value = Predicate;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a policy operand and operand list")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let operand = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        let values = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
        Ok(Predicate::In(operand, values))
    }
}

struct EnumMatchPredicateVisitor<'a> {
    budget: &'a mut PolicyExpressionDecodeBudget,
    depth: usize,
}

impl<'de> serde::de::Visitor<'de> for EnumMatchPredicateVisitor<'_> {
    type Value = Predicate;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an enum-match policy predicate")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let column = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        let case = sequence
            .next_element()?
            .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
        let payload = sequence
            .next_element_seed(PredicateSeed {
                budget: self.budget,
                depth: self.depth,
            })?
            .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
        Ok(Predicate::EnumMatch {
            column,
            case,
            payload: Box::new(payload),
        })
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut column = None;
        let mut case = None;
        let mut payload = None;
        while let Some(field) = map.next_key::<EnumMatchField>()? {
            match field {
                EnumMatchField::Column => {
                    if column.is_some() {
                        return Err(serde::de::Error::duplicate_field("column"));
                    }
                    column = Some(map.next_value()?);
                }
                EnumMatchField::Case => {
                    if case.is_some() {
                        return Err(serde::de::Error::duplicate_field("case"));
                    }
                    case = Some(map.next_value()?);
                }
                EnumMatchField::Payload => {
                    if payload.is_some() {
                        return Err(serde::de::Error::duplicate_field("payload"));
                    }
                    payload = Some(map.next_value_seed(PredicateSeed {
                        budget: self.budget,
                        depth: self.depth,
                    })?);
                }
            }
        }
        Ok(Predicate::EnumMatch {
            column: column.ok_or_else(|| serde::de::Error::missing_field("column"))?,
            case: case.ok_or_else(|| serde::de::Error::missing_field("case"))?,
            payload: Box::new(
                payload.ok_or_else(|| serde::de::Error::missing_field("payload"))?,
            ),
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum EnumMatchField {
    Column,
    Case,
    Payload,
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
