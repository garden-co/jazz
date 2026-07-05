//! Pure Jazz query AST, validation, canonical form, bindings, and
//! content-addressed shape ids for the `jazz/SPEC/6_queries.md` contract. This module
//! owns syntax and schema-level validation only; execution, read-set recording,
//! and groove plan preparation live in [`crate::node::query_eval`], with emitted
//! view payloads assembled by [`crate::node::views`]. It sits above groove query
//! planning as Jazz's stable query vocabulary.

use std::collections::BTreeMap;

use groove::records::Value;
use groove::schema::ColumnType;
use thiserror::Error;

use crate::ids::SchemaVersionId;
use crate::schema::{
    ColumnSchema as JazzColumnSchema, JazzSchema, TableSchema, branch_metadata_table_schema,
};

/// Namespace used for query shape and binding UUIDv5 ids.
pub const QUERY_NAMESPACE: uuid::Uuid = uuid::uuid!("5d39e9ed-88f3-5b58-b8db-8786b02f5d2f");

/// v0 query AST.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Query {
    /// Root table.
    pub table: String,
    /// Root filters.
    pub filters: Vec<Predicate>,
    /// Junction traversals.
    pub joins: Vec<JoinVia>,
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
    /// Correlated relation arrays materialized as relation payload edges.
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
        inputs: Vec<RelationExpr>,
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

impl Query {
    /// Construct a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues");
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn from(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filters: Vec::new(),
            joins: Vec::new(),
            policy_branches: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
            includes: Vec::new(),
            array_subqueries: Vec::new(),
            select: None,
            order_by: Vec::new(),
            aggregate: None,
            limit: None,
            offset: 0,
        }
    }

    /// Add a policy-only OR branch. Runtime query evaluation ignores these;
    /// row policy checks treat the base query and every branch as alternatives.
    pub fn policy_branch(mut self, branch: PolicyBranch) -> Self {
        self.policy_branches.push(branch);
        self
    }

    /// Add a filter.
    ///
    /// ```rust
    /// # use jazz::query::{col, doctest_support, eq, param, Query};
    /// let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
    ///
    /// let validated = query.validate(&doctest_support::schema())?;
    /// assert!(validated.params().contains_key("user"));
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.filters.push(predicate);
        self
    }

    /// Add a junction traversal.
    ///
    /// ```rust
    /// # use jazz::query::{col, doctest_support, eq, param, Query};
    /// let query = Query::from("issues")
    ///     .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))]);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn join_via(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: None,
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal correlated through a root-table reference column.
    ///
    /// This expresses `exists table where table.on_column = root.source_column`.
    pub fn join_via_column(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal with extra source-row equality correlations.
    pub fn join_via_column_with_correlations(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_column: impl Into<String>,
        correlated_filters: impl IntoIterator<Item = JoinCorrelation>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: correlated_filters.into_iter().collect(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a traversal correlated through a referenced source row.
    ///
    /// This expresses `exists table where table.on_column = source.value_column`,
    /// with `source.id = root.row_id_source_column`.
    pub fn join_via_source_lookup(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        source_lookup: JoinSourceLookup,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self = self.join_via_source_lookup_with_target(
            table,
            on_column,
            JoinTarget::Column,
            source_lookup,
            filters,
        );
        self
    }

    /// Add a traversal correlated through a referenced source row with an explicit target.
    pub fn join_via_source_lookup_with_target(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        target: JoinTarget,
        source_lookup: JoinSourceLookup,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target,
            source_column: Some(source_lookup.value_column.clone()),
            source_lookup: Some(source_lookup),
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a junction traversal whose matched row must satisfy nested policy joins.
    pub fn join_via_with_nested_joins(
        mut self,
        table: impl Into<String>,
        on_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
        nested_joins: impl IntoIterator<Item = JoinVia>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: on_column.into(),
            target: JoinTarget::Column,
            source_column: None,
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: nested_joins.into_iter().collect(),
        });
        self
    }

    /// Add a row-correlated traversal to rows whose id is referenced by a root-table column.
    ///
    /// This expresses `exists table where table.id = root.source_column`.
    pub fn join_via_row_id(
        mut self,
        table: impl Into<String>,
        source_column: impl Into<String>,
        filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.joins.push(JoinVia {
            table: table.into(),
            on_column: "id".to_owned(),
            target: JoinTarget::RowId,
            source_column: Some(source_column.into()),
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: filters.into_iter().collect(),
            nested_joins: Vec::new(),
        });
        self
    }

    /// Add a recursive reachability traversal through an access table and edge table.
    #[allow(clippy::too_many_arguments)]
    pub fn reachable_via(
        mut self,
        access_table: impl Into<String>,
        access_row_column: impl Into<String>,
        access_team_column: impl Into<String>,
        from: Operand,
        edge_table: impl Into<String>,
        edge_member_column: impl Into<String>,
        edge_parent_column: impl Into<String>,
        edge_filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self = self.reachable_via_with_access_filters(
            access_table,
            access_row_column,
            access_team_column,
            from,
            [],
            edge_table,
            edge_member_column,
            edge_parent_column,
            edge_filters,
        );
        self
    }

    /// Add a recursive reachability traversal with predicates on both the
    /// access edge and recursive edge tables.
    #[allow(clippy::too_many_arguments)]
    pub fn reachable_via_with_access_filters(
        mut self,
        access_table: impl Into<String>,
        access_row_column: impl Into<String>,
        access_team_column: impl Into<String>,
        from: Operand,
        access_filters: impl IntoIterator<Item = Predicate>,
        edge_table: impl Into<String>,
        edge_member_column: impl Into<String>,
        edge_parent_column: impl Into<String>,
        edge_filters: impl IntoIterator<Item = Predicate>,
    ) -> Self {
        self.reachable.push(ReachableVia {
            access_table: access_table.into(),
            access_row_column: access_row_column.into(),
            access_team_column: access_team_column.into(),
            access_team_target: JoinTarget::Column,
            from,
            access_filters: access_filters.into_iter().collect(),
            edge_table: edge_table.into(),
            edge_member_column: edge_member_column.into(),
            edge_parent_column: edge_parent_column.into(),
            edge_filters: edge_filters.into_iter().collect(),
            bound: RecursionBound::default_max_depth(),
            seed: None,
        });
        self
    }

    /// Use a seed relation for the most recently added reachable traversal.
    ///
    /// The seed relation contributes initial teams by filtering `seed_table`
    /// rows where `user_column == claim(claim_path)`, then projecting
    /// `team_column` into the recursive frontier.
    pub fn seeded_by(
        mut self,
        seed_table: impl Into<String>,
        user_column: impl Into<String>,
        claim_path: impl Into<String>,
        team_column: impl Into<String>,
    ) -> Self {
        let Some(reachable) = self.reachable.last_mut() else {
            panic!("seeded_by requires a preceding reachable_via traversal");
        };
        let user_column = user_column.into();
        let claim_path = claim_path.into();
        reachable.seed = Some(ReachableSeed {
            table: seed_table.into(),
            user_column: Some(user_column.clone()),
            user_claim: Some(claim_path.clone()),
            team_column: team_column.into(),
            filters: Vec::new(),
        });
        self
    }

    /// Require the row referenced by `parent_column` to be readable under the
    /// parent table's composed read policy.
    pub fn inherits(mut self, parent_column: impl Into<String>) -> Self {
        self.inherits.push(InheritsVia {
            parent_column: parent_column.into(),
        });
        self
    }

    /// Add an include path such as `project.org`.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").include("project.org");
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn include(mut self, path: impl Into<String>) -> Self {
        self.includes.push(Include::new(path));
        self
    }

    /// Add an include path with options.
    pub fn include_with(mut self, include: Include) -> Self {
        self.includes.push(include);
        self
    }

    /// Add a correlated relation array subquery.
    pub fn array_subquery(mut self, subquery: ArraySubquery) -> Self {
        self.array_subqueries.push(subquery);
        self
    }

    /// Select application columns. The row id is always included.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").select(["title", "state"]);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn select(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.select = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add a result-level ordering key.
    ///
    /// Multiple calls preserve precedence: earlier keys are compared first.
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    /// Count result rows.
    pub fn count(mut self) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::count()])));
        self
    }

    /// Sum a numeric result column.
    pub fn sum(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::sum(column)])));
        self
    }

    /// Average a numeric result column.
    pub fn avg(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::avg(column)])));
        self
    }

    /// Find the minimum value for an orderable result column.
    pub fn min(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::min(column)])));
        self
    }

    /// Find the maximum value for an orderable result column.
    pub fn max(mut self, column: impl Into<String>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new([Aggregate::max(column)])));
        self
    }

    /// Replace the aggregate list for this query.
    pub fn aggregate(mut self, aggregates: impl IntoIterator<Item = Aggregate>) -> Self {
        self.aggregate = Some(Box::new(AggregateQuery::new(aggregates)));
        self
    }

    /// Group aggregate output by a root-table column.
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        let aggregate = self
            .aggregate
            .get_or_insert_with(|| Box::new(AggregateQuery::new([Aggregate::count()])));
        aggregate.group_by = Some(column.into());
        self
    }

    /// Limit result rows after filtering.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").limit(25);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip result rows after filtering.
    ///
    /// ```rust
    /// # use jazz::query::{doctest_support, Query};
    /// let query = Query::from("issues").offset(50);
    ///
    /// query.validate(&doctest_support::schema())?;
    /// # Ok::<(), jazz::query::QueryError>(())
    /// ```
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Validate and canonicalize this query against a Jazz schema.
    pub fn validate(&self, schema: &JazzSchema) -> Result<ValidatedQuery, QueryError> {
        validate_query(self, schema)
    }
}

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

    pub(crate) fn as_query(&self, table: &str) -> Query {
        Query {
            table: table.to_owned(),
            filters: self.filters.clone(),
            joins: self.joins.clone(),
            policy_branches: Vec::new(),
            reachable: self.reachable.clone(),
            inherits: self.inherits.clone(),
            includes: Vec::new(),
            array_subqueries: Vec::new(),
            select: None,
            order_by: Vec::new(),
            aggregate: None,
            limit: None,
            offset: 0,
        }
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

/// Correlated relation array materialized in the relation payload.
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
        Self {
            column_name: column_name.into(),
            table: table.into(),
            inner_column: inner_column.into(),
            outer_column: outer_column.into(),
            filters: Vec::new(),
            select: None,
            order_by: Vec::new(),
            limit: None,
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

    /// Conservative loop cap for old evaluator paths that are not true fixpoint.
    pub(crate) fn iteration_cap(self) -> usize {
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

/// Construct a column operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn col(name: impl Into<String>) -> Operand {
    Operand::Column(name.into())
}

/// Construct a parameter operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, param, Query};
/// let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn param(name: impl Into<String>) -> Operand {
    Operand::Param(name.into())
}

/// Construct a claim operand.
pub fn claim(name: impl Into<String>) -> Operand {
    Operand::Claim(name.into())
}

/// Construct a literal operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn lit(value: impl Into<Value>) -> Operand {
    Operand::Literal(value.into())
}

/// Construct an equality predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn eq(left: Operand, right: Operand) -> Predicate {
    Predicate::Eq(left, right)
}

/// Construct an inequality predicate.
pub fn ne(left: Operand, right: Operand) -> Predicate {
    Predicate::Ne(left, right)
}

/// Construct an all-of predicate.
///
/// ```rust
/// # use jazz::query::{all_of, col, doctest_support, eq, gt, lit, Query};
/// let query = Query::from("issues").filter(all_of([
///     eq(col("state"), lit("open")),
///     gt(col("priority"), lit(1_u64)),
/// ]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn all_of(predicates: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::All(predicates.into_iter().collect())
}

/// Construct an any-of predicate.
///
/// ```rust
/// # use jazz::query::{any_of, col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(any_of([
///     eq(col("state"), lit("open")),
///     eq(col("state"), lit("triage")),
/// ]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn any_of(predicates: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::Any(predicates.into_iter().collect())
}

/// Construct a negated predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, not, Query};
/// let query = Query::from("issues").filter(not(eq(col("state"), lit("closed"))));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn not(predicate: Predicate) -> Predicate {
    Predicate::Not(Box::new(predicate))
}

/// Construct an `IN` predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, in_list, lit, Query};
/// let query = Query::from("issues")
///     .filter(in_list(col("state"), [lit("open"), lit("triage")]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn in_list(left: Operand, values: impl IntoIterator<Item = Operand>) -> Predicate {
    Predicate::In(left, values.into_iter().collect())
}

/// Construct a greater-than predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, gt, lit, Query};
/// let query = Query::from("issues").filter(gt(col("priority"), lit(3_u64)));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn gt(left: Operand, right: Operand) -> Predicate {
    Predicate::Gt(left, right)
}

/// Construct a greater-than-or-equal predicate.
pub fn gte(left: Operand, right: Operand) -> Predicate {
    Predicate::Gte(left, right)
}

/// Construct a less-than predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, lit, lt, Query};
/// let query = Query::from("issues").filter(lt(col("priority"), lit(10_u64)));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn lt(left: Operand, right: Operand) -> Predicate {
    Predicate::Lt(left, right)
}

/// Construct a less-than-or-equal predicate.
pub fn lte(left: Operand, right: Operand) -> Predicate {
    Predicate::Lte(left, right)
}

/// Construct a string substring or array membership predicate.
pub fn contains(left: Operand, right: Operand) -> Predicate {
    Predicate::Contains(left, right)
}

/// Construct a nullable-is-null predicate.
pub fn is_null(operand: Operand) -> Predicate {
    Predicate::IsNull(operand)
}

/// Validated query shape with inferred parameter types.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ValidatedQuery {
    query: Query,
    schema_version: SchemaVersionId,
    params: BTreeMap<String, ColumnType>,
    canonical: Vec<u8>,
    shape_id: ShapeId,
}

impl ValidatedQuery {
    /// Shape id derived from canonical AST bytes.
    pub fn shape_id(&self) -> ShapeId {
        self.shape_id
    }

    /// Canonical AST bytes.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    /// Schema version this query was authored and validated against.
    pub fn schema_version(&self) -> SchemaVersionId {
        self.schema_version
    }

    /// Inferred parameter types by name.
    pub fn params(&self) -> &BTreeMap<String, ColumnType> {
        &self.params
    }

    /// Original AST normalized into canonical order.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Validate a binding against this shape.
    pub fn bind(&self, values: BTreeMap<String, Value>) -> Result<Binding, QueryError> {
        validate_binding_values(&self.params, values)
    }
}

fn validate_binding_values(
    params: &BTreeMap<String, ColumnType>,
    values: BTreeMap<String, Value>,
) -> Result<Binding, QueryError> {
    for required in params.keys() {
        if !values.contains_key(required) {
            return Err(QueryError::MissingParam(required.clone()));
        }
    }
    for (name, value) in &values {
        let Some(expected) = params.get(name) else {
            return Err(QueryError::UnknownParam(name.clone()));
        };
        if !value_matches_type(value, expected) {
            return Err(QueryError::ParamTypeMismatch {
                param: name.clone(),
                expected: expected.clone(),
            });
        }
    }
    let canonical = canonical_binding_bytes(&values);
    Ok(Binding { values, canonical })
}

/// Validated binding values for a query shape.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Binding {
    values: BTreeMap<String, Value>,
    canonical: Vec<u8>,
}

impl Binding {
    /// Binding id derived from canonical binding bytes.
    pub fn binding_id(&self) -> BindingId {
        BindingId(uuid::Uuid::new_v5(&QUERY_NAMESPACE, &self.canonical))
    }

    /// Canonical binding bytes.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    /// Bound values by parameter name.
    pub fn values(&self) -> &BTreeMap<String, Value> {
        &self.values
    }
}

pub(crate) fn binding_id_for_values(values: &BTreeMap<String, Value>) -> BindingId {
    BindingId(uuid::Uuid::new_v5(
        &QUERY_NAMESPACE,
        &canonical_binding_bytes(values),
    ))
}

/// Query validation error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueryError {
    /// Referenced table does not exist.
    #[error("unknown table {0}")]
    UnknownTable(String),
    /// Referenced column does not exist.
    #[error("unknown column {table}.{column}")]
    UnknownColumn {
        /// Table name.
        table: String,
        /// Column name.
        column: String,
    },
    /// Large-value columns cannot participate in query-planner predicates.
    #[error("large-value column {table}.{column} is not allowed in query predicates")]
    LargeValueColumnInQuery {
        /// Table name.
        table: String,
        /// Column name.
        column: String,
    },
    /// Operand types do not match.
    #[error("operand type mismatch")]
    OperandTypeMismatch,
    /// Claim and column operand types do not match.
    #[error(
        "claim {claim_path} has type {claim_type:?}, but column {column} has type {column_type:?}"
    )]
    ClaimTypeMismatch {
        /// Claim path.
        claim_path: String,
        /// Column name.
        column: String,
        /// Claim type.
        claim_type: String,
        /// Column type.
        column_type: String,
    },
    /// Parameter was inferred with incompatible types.
    #[error("parameter {param} inferred with incompatible type")]
    ParamTypeConflict {
        /// Parameter name.
        param: String,
    },
    /// Binding omitted a required parameter.
    #[error("missing parameter {0}")]
    MissingParam(String),
    /// Binding supplied an unknown parameter.
    #[error("unknown parameter {0}")]
    UnknownParam(String),
    /// Binding value had the wrong type.
    #[error("parameter {param} has wrong type")]
    ParamTypeMismatch {
        /// Parameter name.
        param: String,
        /// Expected type.
        expected: ColumnType,
    },
    /// Join column is not a reference to the current table.
    #[error("join column {join_table}.{column} does not reference {target_table}")]
    JoinNotRefCompatible {
        /// Junction table.
        join_table: String,
        /// Column name.
        column: String,
        /// Expected target table.
        target_table: String,
    },
    /// Include path did not resolve through reference metadata.
    #[error("bad include path {path}")]
    BadIncludePath {
        /// Include path.
        path: String,
    },
    /// Permission-introspection columns are not executable yet.
    #[error(
        "unsupported query magic column {column}: permission introspection columns are not executable yet"
    )]
    UnsupportedMagicColumn {
        /// Column name.
        column: String,
    },
}

fn validate_query(query: &Query, schema: &JazzSchema) -> Result<ValidatedQuery, QueryError> {
    let (normalized, params, canonical) = validate_query_canonical_parts(query, schema)?;
    let schema_version = schema.version_id();
    let mut shape_identity = canonical.clone();
    shape_identity.extend_from_slice(schema_version.as_bytes());
    let shape_id = ShapeId(uuid::Uuid::new_v5(&QUERY_NAMESPACE, &shape_identity));
    Ok(ValidatedQuery {
        query: normalized,
        schema_version,
        params,
        canonical,
        shape_id,
    })
}

type ValidatedQueryCanonicalParts = (Query, BTreeMap<String, ColumnType>, Vec<u8>);

fn validate_query_canonical_parts(
    query: &Query,
    schema: &JazzSchema,
) -> Result<ValidatedQueryCanonicalParts, QueryError> {
    let root = table(schema, &query.table)?;
    let mut params = BTreeMap::new();
    for predicate in &query.filters {
        validate_predicate(&root, predicate, &mut params)?;
    }
    for join in &query.joins {
        validate_join(schema, &root, &query.table, join, &mut params)?;
    }
    for reachable in &query.reachable {
        validate_reachable(schema, &root, reachable, &mut params)?;
    }
    for inherits in &query.inherits {
        validate_inherits(&root, inherits)?;
    }
    for branch in &query.policy_branches {
        for predicate in &branch.filters {
            validate_predicate(&root, predicate, &mut params)?;
        }
        for join in &branch.joins {
            validate_join(schema, &root, &query.table, join, &mut params)?;
        }
        for reachable in &branch.reachable {
            validate_reachable(schema, &root, reachable, &mut params)?;
        }
        for inherits in &branch.inherits {
            validate_inherits(&root, inherits)?;
        }
    }
    for include in &query.includes {
        validate_include(schema, &root, &include.path)?;
    }
    validate_array_subqueries(schema, &root, &query.array_subqueries, &mut params)?;
    if let Some(select) = &query.select {
        for column in select {
            validate_select_column(&root, column)?;
        }
    }
    if let Some(aggregate) = &query.aggregate {
        validate_aggregate(&root, aggregate)?;
        validate_aggregate_order_by(&query.table, aggregate, &query.order_by)?;
    } else {
        for order in &query.order_by {
            planner_column_type(&root, &order.column)?;
        }
    }
    let normalized = normalize_query(query);
    let canonical = canonical_query_bytes(&normalized);
    Ok((normalized, params, canonical))
}

fn validate_join(
    schema: &JazzSchema,
    root: &TableSchema,
    root_table: &str,
    join: &JoinVia,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let join_table = table(schema, &join.table)?;
    match join.target {
        JoinTarget::Column => {
            planner_column_type(&join_table, &join.on_column)?;
        }
        JoinTarget::RowId => {
            if join.on_column != "id" {
                return Err(QueryError::UnknownColumn {
                    table: join.table.clone(),
                    column: join.on_column.clone(),
                });
            }
        }
    }
    let target_table = if let Some(lookup) = &join.source_lookup {
        planner_column_type(root, &lookup.row_id_source_column)?;
        let lookup_table = table(schema, &lookup.table)?;
        match root.references.get(&lookup.row_id_source_column) {
            Some(target) if target == &lookup.table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: root_table.to_owned(),
                    column: lookup.row_id_source_column.clone(),
                    target_table: lookup.table.clone(),
                });
            }
        }
        if lookup.value_column != "id" {
            planner_column_type(&lookup_table, &lookup.value_column)?;
        }
        if join.source_column.as_deref() != Some(lookup.value_column.as_str()) {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: lookup.table.clone(),
                column: lookup.value_column.clone(),
                target_table: "join source column".to_owned(),
            });
        }
        if lookup.value_column == "id" {
            lookup.table.clone()
        } else {
            lookup_table
                .references
                .get(&lookup.value_column)
                .cloned()
                .ok_or_else(|| QueryError::JoinNotRefCompatible {
                    join_table: lookup.table.clone(),
                    column: lookup.value_column.clone(),
                    target_table: "referenced table".to_owned(),
                })?
        }
    } else if let Some(source_column) = &join.source_column {
        if source_column == "id" {
            root_table.to_owned()
        } else {
            planner_column_type(root, source_column)?;
            root.references.get(source_column).cloned().ok_or_else(|| {
                QueryError::JoinNotRefCompatible {
                    join_table: root_table.to_owned(),
                    column: source_column.clone(),
                    target_table: "referenced table".to_owned(),
                }
            })?
        }
    } else {
        root_table.to_owned()
    };
    for correlation in &join.correlated_filters {
        let source_type = planner_column_type(root, &correlation.source_column)?;
        let join_type = planner_column_type(&join_table, &correlation.join_column)?;
        if source_type != join_type {
            return Err(QueryError::OperandTypeMismatch);
        }
    }
    match join.target {
        JoinTarget::Column => match join_table.references.get(&join.on_column) {
            Some(target) if target == &target_table => {}
            None if join.on_column == "id" && join.table == target_table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: join.table.clone(),
                    column: join.on_column.clone(),
                    target_table: target_table.to_owned(),
                });
            }
        },
        JoinTarget::RowId => {
            if join.table != target_table {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: join.table.clone(),
                    column: join.on_column.clone(),
                    target_table: target_table.to_owned(),
                });
            }
        }
    }
    for predicate in &join.filters {
        validate_predicate(&join_table, predicate, params)?;
    }
    for nested in &join.nested_joins {
        validate_join(schema, &join_table, &join.table, nested, params)?;
    }
    Ok(())
}

fn validate_aggregate(table: &TableSchema, aggregate: &AggregateQuery) -> Result<(), QueryError> {
    if let Some(group_by) = &aggregate.group_by {
        planner_column_type(table, group_by)?;
    }
    for aggregate in &aggregate.aggregates {
        match aggregate.function {
            AggregateFunction::Count => {
                if let Some(column) = &aggregate.column {
                    column_type(table, column)?;
                }
            }
            AggregateFunction::Sum | AggregateFunction::Avg => {
                let Some(column) = &aggregate.column else {
                    return Err(QueryError::OperandTypeMismatch);
                };
                if !is_numeric(column_type(table, column)?) {
                    return Err(QueryError::OperandTypeMismatch);
                }
            }
            AggregateFunction::Min | AggregateFunction::Max => {
                let Some(column) = &aggregate.column else {
                    return Err(QueryError::OperandTypeMismatch);
                };
                if !is_orderable(column_type(table, column)?) {
                    return Err(QueryError::OperandTypeMismatch);
                }
            }
        }
    }
    Ok(())
}

fn validate_aggregate_order_by(
    table: &str,
    aggregate: &AggregateQuery,
    order_by: &[OrderBy],
) -> Result<(), QueryError> {
    for order in order_by {
        let is_group_by = aggregate.group_by.as_deref() == Some(order.column.as_str());
        let is_aggregate = aggregate
            .aggregates
            .iter()
            .any(|aggregate| aggregate.alias == order.column);
        if !is_group_by && !is_aggregate {
            return Err(QueryError::UnknownColumn {
                table: format!("{table}_aggregate"),
                column: order.column.clone(),
            });
        }
    }
    Ok(())
}

fn validate_select_column(table: &TableSchema, column: &str) -> Result<(), QueryError> {
    match column {
        "id" => Ok(()),
        name if executable_magic_column_type(name)?.is_some() => Ok(()),
        name if name.starts_with('$') => Err(QueryError::UnknownColumn {
            table: table.name.clone(),
            column: name.to_owned(),
        }),
        name => column_type(table, name).map(|_| ()),
    }
}

fn table(schema: &JazzSchema, name: &str) -> Result<TableSchema, QueryError> {
    if name == "jazz_branches" {
        return Ok(branch_metadata_table_schema());
    }
    schema
        .tables
        .iter()
        .find(|table| table.name == name)
        .cloned()
        .ok_or_else(|| QueryError::UnknownTable(name.to_owned()))
}

fn column_type<'a>(table: &'a TableSchema, column: &str) -> Result<&'a ColumnType, QueryError> {
    column_schema(table, column).map(|column| &column.column_type)
}

fn column_schema<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a JazzColumnSchema, QueryError> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| QueryError::UnknownColumn {
            table: table.name.clone(),
            column: column.to_owned(),
        })
}

fn planner_column_type<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a ColumnType, QueryError> {
    if column == "id" {
        return Ok(&ColumnType::Uuid);
    }
    if let Some(column_type) = executable_magic_column_type(column)? {
        return Ok(column_type);
    }
    let column = column_schema(table, column)?;
    if column.large_value.is_some() {
        return Err(QueryError::LargeValueColumnInQuery {
            table: table.name.clone(),
            column: column.name.clone(),
        });
    }
    Ok(&column.column_type)
}

fn executable_magic_column_type(column: &str) -> Result<Option<&'static ColumnType>, QueryError> {
    if is_permission_introspection_magic_column(column) {
        return Err(QueryError::UnsupportedMagicColumn {
            column: column.to_owned(),
        });
    }
    match column {
        "$createdBy" | "$updatedBy" => Ok(Some(&ColumnType::Uuid)),
        "$createdAt" | "$updatedAt" => Ok(Some(&ColumnType::U64)),
        _ => Ok(None),
    }
}

fn is_permission_introspection_magic_column(column: &str) -> bool {
    matches!(column, "$canRead" | "$canEdit" | "$canDelete")
}

fn validate_include(schema: &JazzSchema, root: &TableSchema, path: &str) -> Result<(), QueryError> {
    let mut current = root.clone();
    for segment in path.split('.') {
        column_type(&current, segment)?;
        let Some(target) = current.references.get(segment) else {
            return Err(QueryError::BadIncludePath {
                path: path.to_owned(),
            });
        };
        current = table(schema, target)?;
    }
    Ok(())
}

fn validate_array_subqueries(
    schema: &JazzSchema,
    parent: &TableSchema,
    subqueries: &[ArraySubquery],
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let mut names = std::collections::BTreeSet::new();
    for subquery in subqueries {
        if subquery.column_name.is_empty() || !names.insert(subquery.column_name.as_str()) {
            return Err(QueryError::BadIncludePath {
                path: subquery.column_name.clone(),
            });
        }
        validate_array_subquery(schema, parent, subquery, params)?;
    }
    Ok(())
}

fn validate_array_subquery(
    schema: &JazzSchema,
    parent: &TableSchema,
    subquery: &ArraySubquery,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let child = table(schema, &subquery.table)?;
    let parent_type = planner_column_type(parent, &subquery.outer_column)?;
    let child_type = planner_column_type(&child, &subquery.inner_column)?;
    if !in_operand_types_compatible(parent_type, child_type) {
        return Err(QueryError::OperandTypeMismatch);
    }
    for predicate in &subquery.filters {
        validate_predicate(&child, predicate, params)?;
    }
    if let Some(select) = &subquery.select {
        for column in select {
            validate_select_column(&child, column)?;
        }
    }
    for order in &subquery.order_by {
        planner_column_type(&child, &order.column)?;
    }
    validate_array_subqueries(schema, &child, &subquery.nested_arrays, params)?;
    Ok(())
}

fn validate_reachable(
    schema: &JazzSchema,
    root: &TableSchema,
    reachable: &ReachableVia,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let access = table(schema, &reachable.access_table)?;
    planner_column_type(&access, &reachable.access_row_column)?;
    planner_column_type(&access, &reachable.access_team_column)?;
    if reachable.access_row_column == "id" {
        if access.name != root.name {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: reachable.access_table.clone(),
                column: reachable.access_row_column.clone(),
                target_table: root.name.clone(),
            });
        }
    } else {
        match access.references.get(&reachable.access_row_column) {
            Some(target) if target == &root.name => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.access_table.clone(),
                    column: reachable.access_row_column.clone(),
                    target_table: root.name.clone(),
                });
            }
        }
    }
    let team_table = match reachable.access_team_target {
        JoinTarget::Column => access
            .references
            .get(&reachable.access_team_column)
            .ok_or_else(|| QueryError::JoinNotRefCompatible {
                join_table: reachable.access_table.clone(),
                column: reachable.access_team_column.clone(),
                target_table: "referenced table".to_owned(),
            })?,
        JoinTarget::RowId => {
            if reachable.access_team_column != "id" {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.access_table.clone(),
                    column: reachable.access_team_column.clone(),
                    target_table: reachable.access_table.clone(),
                });
            }
            &access.name
        }
    };
    let edge = table(schema, &reachable.edge_table)?;
    for column in [&reachable.edge_member_column, &reachable.edge_parent_column] {
        planner_column_type(&edge, column)?;
        match edge.references.get(column) {
            Some(target) if target == team_table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.edge_table.clone(),
                    column: column.clone(),
                    target_table: team_table.clone(),
                });
            }
        }
    }
    if let Some(seed) = &reachable.seed {
        let seed_table = table(schema, &seed.table)?;
        planner_column_type(&seed_table, &seed.team_column)?;
        if let Some(user_column) = &seed.user_column {
            planner_column_type(&seed_table, user_column)?;
        }
        let seed_projects_team = if seed.team_column == "id" {
            seed_table.name == *team_table
        } else {
            matches!(
                seed_table.references.get(&seed.team_column),
                Some(target) if target == team_table
            )
        };
        if !seed_projects_team {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: seed.table.clone(),
                column: seed.team_column.clone(),
                target_table: team_table.clone(),
            });
        }
        for predicate in &seed.filters {
            validate_predicate(&seed_table, predicate, params)?;
        }
    } else {
        match operand_type(root, &reachable.from, params)? {
            Some(ColumnType::Uuid) => {}
            None => infer_param(&reachable.from, ColumnType::Uuid, params)?,
            Some(_) => return Err(QueryError::OperandTypeMismatch),
        }
    }
    for predicate in &reachable.access_filters {
        validate_predicate(&access, predicate, params)?;
    }
    for predicate in &reachable.edge_filters {
        validate_predicate(&edge, predicate, params)?;
    }
    Ok(())
}

fn validate_inherits(root: &TableSchema, inherits: &InheritsVia) -> Result<(), QueryError> {
    planner_column_type(root, &inherits.parent_column)?;
    root.references
        .get(&inherits.parent_column)
        .ok_or_else(|| QueryError::JoinNotRefCompatible {
            join_table: root.name.clone(),
            column: inherits.parent_column.clone(),
            target_table: "referenced table".to_owned(),
        })?;
    Ok(())
}

fn validate_predicate(
    table: &TableSchema,
    predicate: &Predicate,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => predicates
            .iter()
            .try_for_each(|predicate| validate_predicate(table, predicate, params)),
        Predicate::Not(predicate) => validate_predicate(table, predicate, params),
        Predicate::Eq(left, right) | Predicate::Ne(left, right) => {
            validate_comparable_operands(table, left, right, params).map(|_| ())
        }
        Predicate::In(left, values) => {
            let left_type = operand_type(table, left, params)?;
            for value in values {
                let value_type = operand_type(table, value, params)?;
                match (left_type.clone(), value_type) {
                    (Some(left_type), Some(value_type))
                        if !in_operand_types_compatible(&left_type, &value_type) =>
                    {
                        return Err(QueryError::OperandTypeMismatch);
                    }
                    (Some(left_type), None) => {
                        let expected = match non_null_column_type(&left_type) {
                            ColumnType::Array(member) => *member,
                            other => other,
                        };
                        infer_param(value, expected, params)?;
                    }
                    (None, Some(value_type)) => infer_param(left, value_type, params)?,
                    (Some(_), Some(_)) => {}
                    (None, None) => return Err(QueryError::OperandTypeMismatch),
                }
            }
            Ok(())
        }
        Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right) => {
            let column_type = validate_comparable_operands(table, left, right, params)?;
            if is_orderable(&column_type) {
                Ok(())
            } else {
                Err(QueryError::OperandTypeMismatch)
            }
        }
        Predicate::Contains(left, right) => {
            let left_type = operand_type(table, left, params)?;
            let right_type = operand_type(table, right, params)?;
            match (
                left_type.map(|column_type| non_null_column_type(&column_type)),
                right_type,
            ) {
                (Some(ColumnType::String), _) => {
                    validate_operand_against_type(table, right, ColumnType::String, params)
                }
                (Some(ColumnType::Array(member)), _) => {
                    validate_operand_against_type(table, right, *member, params)
                }
                (Some(_), _) => Err(QueryError::OperandTypeMismatch),
                (None, Some(right_type)) => {
                    infer_param(left, ColumnType::Array(Box::new(right_type)), params)
                }
                (None, None) => Err(QueryError::OperandTypeMismatch),
            }
        }
        Predicate::IsNull(operand) => match operand_type(table, operand, params)? {
            Some(ColumnType::Nullable(_)) => Ok(()),
            Some(_) => Err(QueryError::OperandTypeMismatch),
            None => Err(QueryError::OperandTypeMismatch),
        },
    }
}

fn validate_comparable_operands(
    table: &TableSchema,
    left: &Operand,
    right: &Operand,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<ColumnType, QueryError> {
    let left_type = operand_type(table, left, params)?;
    let right_type = operand_type(table, right, params)?;
    match (left_type, right_type) {
        (Some(left_type), Some(right_type))
            if !column_types_comparable(&left_type, &right_type) =>
        {
            if let Some(error) = claim_type_mismatch_error(left, &left_type, right, &right_type) {
                return Err(error);
            }
            if let Some(error) = claim_type_mismatch_error(right, &right_type, left, &left_type) {
                return Err(error);
            }
            Err(QueryError::OperandTypeMismatch)
        }
        (Some(left_type), None) => {
            infer_param(right, left_type.clone(), params)?;
            Ok(left_type)
        }
        (None, Some(right_type)) => {
            infer_param(left, right_type.clone(), params)?;
            Ok(right_type)
        }
        (Some(left_type), Some(_)) => Ok(left_type),
        (None, None) => Err(QueryError::OperandTypeMismatch),
    }
}

fn validate_operand_against_type(
    table: &TableSchema,
    operand: &Operand,
    expected: ColumnType,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    match operand_type(table, operand, params)? {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(QueryError::OperandTypeMismatch),
        None => infer_param(operand, expected, params),
    }
}

fn is_orderable(column_type: &ColumnType) -> bool {
    let column_type = non_null_column_type(column_type);
    matches!(
        &column_type,
        ColumnType::U8
            | ColumnType::U16
            | ColumnType::U32
            | ColumnType::U64
            | ColumnType::F64
            | ColumnType::Uuid
            | ColumnType::String
    )
}

fn column_types_comparable(left: &ColumnType, right: &ColumnType) -> bool {
    left == right || non_null_column_type(left) == non_null_column_type(right)
}

fn in_operand_types_compatible(left: &ColumnType, right: &ColumnType) -> bool {
    if column_types_comparable(left, right) {
        return true;
    }
    match non_null_column_type(left) {
        ColumnType::Array(member) => column_types_comparable(&member, right),
        _ => false,
    }
}

fn non_null_column_type(column_type: &ColumnType) -> ColumnType {
    match column_type {
        ColumnType::Nullable(inner) => inner.as_ref().clone(),
        other => other.clone(),
    }
}

fn is_numeric(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::U8 | ColumnType::U16 | ColumnType::U32 | ColumnType::U64 | ColumnType::F64
    )
}

fn operand_type(
    table: &TableSchema,
    operand: &Operand,
    params: &BTreeMap<String, ColumnType>,
) -> Result<Option<ColumnType>, QueryError> {
    match operand {
        Operand::Column(column) => Ok(Some(planner_column_type(table, column)?.clone())),
        Operand::Literal(value) => Ok(Some(value_type(value))),
        Operand::Param(name) => Ok(params.get(name).cloned()),
        Operand::Claim(name) => claim_type(name),
    }
}

fn claim_type(name: &str) -> Result<Option<ColumnType>, QueryError> {
    match name {
        "sub" => Ok(Some(ColumnType::Uuid)),
        "team" => Ok(Some(ColumnType::Uuid)),
        "isAdmin" => Ok(Some(ColumnType::Bool)),
        _ => Ok(None),
    }
}

fn claim_type_mismatch_error(
    claim: &Operand,
    claim_type: &ColumnType,
    other: &Operand,
    other_type: &ColumnType,
) -> Option<QueryError> {
    let Operand::Claim(claim_path) = claim else {
        return None;
    };
    let Operand::Column(column) = other else {
        return None;
    };
    Some(QueryError::ClaimTypeMismatch {
        claim_path: claim_path.clone(),
        column: column.clone(),
        claim_type: column_type_name(claim_type),
        column_type: column_type_name(other_type),
    })
}

fn column_type_name(column_type: &ColumnType) -> String {
    format!("{column_type:?}")
}

fn infer_param(
    operand: &Operand,
    expected: ColumnType,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let Operand::Param(name) = operand else {
        return Ok(());
    };
    match params.get(name) {
        Some(existing) if existing != &expected => Err(QueryError::ParamTypeConflict {
            param: name.clone(),
        }),
        Some(_) => Ok(()),
        None => {
            params.insert(name.clone(), expected);
            Ok(())
        }
    }
}

fn normalize_query(query: &Query) -> Query {
    let mut query = query.clone();
    query.filters.sort_by_key(canonical_predicate_key);
    for join in &mut query.joins {
        join.filters.sort_by_key(canonical_predicate_key);
        normalize_join(join);
    }
    query.joins.sort_by_key(canonical_join_key);
    for branch in &mut query.policy_branches {
        branch.filters.sort_by_key(canonical_predicate_key);
        for join in &mut branch.joins {
            join.filters.sort_by_key(canonical_predicate_key);
            normalize_join(join);
        }
        branch.joins.sort_by_key(canonical_join_key);
        for reachable in &mut branch.reachable {
            reachable
                .access_filters
                .sort_by_key(canonical_predicate_key);
            reachable.edge_filters.sort_by_key(canonical_predicate_key);
            if let Some(seed) = &mut reachable.seed {
                seed.filters.sort_by_key(canonical_predicate_key);
            }
        }
        branch.reachable.sort_by_key(canonical_reachable_key);
        branch.inherits.sort_by_key(canonical_inherits_key);
        branch.inherits.dedup();
    }
    query
        .policy_branches
        .sort_by_key(canonical_policy_branch_key);
    for reachable in &mut query.reachable {
        reachable
            .access_filters
            .sort_by_key(canonical_predicate_key);
        reachable.edge_filters.sort_by_key(canonical_predicate_key);
        if let Some(seed) = &mut reachable.seed {
            seed.filters.sort_by_key(canonical_predicate_key);
        }
    }
    query.reachable.sort_by_key(canonical_reachable_key);
    query.inherits.sort_by_key(canonical_inherits_key);
    query.inherits.dedup();
    query.includes.sort();
    query.includes.dedup();
    for subquery in &mut query.array_subqueries {
        normalize_array_subquery(subquery);
    }
    query
        .array_subqueries
        .sort_by_key(canonical_array_subquery_key);
    query.array_subqueries.dedup();
    if let Some(select) = &mut query.select {
        select.sort();
        select.dedup();
    }
    if let Some(aggregate) = &mut query.aggregate {
        aggregate.aggregates.sort_by_key(canonical_aggregate_key);
    }
    query
}

fn normalize_array_subquery(subquery: &mut ArraySubquery) {
    subquery.filters.sort_by_key(canonical_predicate_key);
    if let Some(select) = &mut subquery.select {
        select.sort();
        select.dedup();
    }
    for nested in &mut subquery.nested_arrays {
        normalize_array_subquery(nested);
    }
    subquery
        .nested_arrays
        .sort_by_key(canonical_array_subquery_key);
    subquery.nested_arrays.dedup();
}

fn normalize_join(join: &mut JoinVia) {
    join.correlated_filters
        .sort_by_key(canonical_join_correlation_key);
    for nested in &mut join.nested_joins {
        nested.filters.sort_by_key(canonical_predicate_key);
        normalize_join(nested);
    }
    join.nested_joins.sort_by_key(canonical_join_key);
}

fn canonical_policy_branch_key(branch: &PolicyBranch) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_len(&mut bytes, branch.filters.len());
    for filter in &branch.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_len(&mut bytes, branch.joins.len());
    for join in &branch.joins {
        put_bytes(&mut bytes, &canonical_join_key(join));
    }
    put_len(&mut bytes, branch.reachable.len());
    for reachable in &branch.reachable {
        put_bytes(&mut bytes, &canonical_reachable_key(reachable));
    }
    put_len(&mut bytes, branch.inherits.len());
    for inherits in &branch.inherits {
        put_bytes(&mut bytes, &canonical_inherits_key(inherits));
    }
    bytes
}

fn canonical_aggregate_key(aggregate: &Aggregate) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(match aggregate.function {
        AggregateFunction::Count => b'c',
        AggregateFunction::Sum => b's',
        AggregateFunction::Avg => b'a',
        AggregateFunction::Min => b'n',
        AggregateFunction::Max => b'x',
    });
    if let Some(column) = &aggregate.column {
        put_str(&mut bytes, column);
    }
    put_str(&mut bytes, &aggregate.alias);
    bytes
}

fn canonical_array_subquery_key(subquery: &ArraySubquery) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &subquery.column_name);
    put_str(&mut bytes, &subquery.table);
    put_str(&mut bytes, &subquery.inner_column);
    put_str(&mut bytes, &subquery.outer_column);
    put_len(&mut bytes, subquery.filters.len());
    for filter in &subquery.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    if let Some(select) = &subquery.select {
        bytes.push(b's');
        put_len(&mut bytes, select.len());
        for column in select {
            put_str(&mut bytes, column);
        }
    }
    if !subquery.order_by.is_empty() {
        bytes.push(b'o');
        put_len(&mut bytes, subquery.order_by.len());
        for order in &subquery.order_by {
            put_str(&mut bytes, &order.column);
            bytes.push(match order.direction {
                OrderDirection::Asc => b'a',
                OrderDirection::Desc => b'd',
            });
        }
    }
    if let Some(limit) = subquery.limit {
        bytes.push(b'l');
        put_len(&mut bytes, limit);
    }
    bytes.push(match subquery.requirement {
        ArraySubqueryRequirement::Optional => b'?',
        ArraySubqueryRequirement::AtLeastOne => b'+',
        ArraySubqueryRequirement::MatchCorrelationCardinality => b'=',
    });
    if !subquery.nested_arrays.is_empty() {
        bytes.push(b'n');
        put_len(&mut bytes, subquery.nested_arrays.len());
        for nested in &subquery.nested_arrays {
            put_bytes(&mut bytes, &canonical_array_subquery_key(nested));
        }
    }
    bytes
}

fn canonical_reachable_key(reachable: &ReachableVia) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &reachable.access_table);
    put_str(&mut bytes, &reachable.access_row_column);
    put_str(&mut bytes, &reachable.access_team_column);
    match reachable.access_team_target {
        JoinTarget::Column => {}
        JoinTarget::RowId => bytes.push(b'r'),
    }
    put_bytes(&mut bytes, &canonical_operand_key(&reachable.from));
    put_len(&mut bytes, reachable.access_filters.len());
    for filter in &reachable.access_filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_str(&mut bytes, &reachable.edge_table);
    put_str(&mut bytes, &reachable.edge_member_column);
    put_str(&mut bytes, &reachable.edge_parent_column);
    match reachable.bound {
        RecursionBound::Fixpoint => bytes.push(b'f'),
        RecursionBound::MaxDepth(max_depth) => {
            bytes.push(b'd');
            put_len(&mut bytes, max_depth);
        }
    }
    for filter in &reachable.edge_filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    if let Some(seed) = &reachable.seed {
        bytes.push(b's');
        put_str(&mut bytes, &seed.table);
        if let (Some(user_column), Some(user_claim)) = (&seed.user_column, &seed.user_claim) {
            bytes.push(b'u');
            put_str(&mut bytes, user_column);
            put_str(&mut bytes, user_claim);
        }
        put_str(&mut bytes, &seed.team_column);
        put_len(&mut bytes, seed.filters.len());
        for filter in &seed.filters {
            put_bytes(&mut bytes, &canonical_predicate_key(filter));
        }
    }
    bytes
}

fn canonical_inherits_key(inherits: &InheritsVia) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &inherits.parent_column);
    bytes
}

fn canonical_join_key(join: &JoinVia) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &join.table);
    put_str(&mut bytes, &join.on_column);
    match join.target {
        JoinTarget::Column => {}
        JoinTarget::RowId => bytes.push(b'r'),
    }
    if let Some(column) = &join.source_column {
        bytes.push(b's');
        put_str(&mut bytes, column);
    }
    if let Some(lookup) = &join.source_lookup {
        bytes.push(b'l');
        put_str(&mut bytes, &lookup.table);
        put_str(&mut bytes, &lookup.row_id_source_column);
        put_str(&mut bytes, &lookup.value_column);
    }
    if !join.correlated_filters.is_empty() {
        bytes.push(b'c');
        put_len(&mut bytes, join.correlated_filters.len());
        for correlation in &join.correlated_filters {
            put_bytes(&mut bytes, &canonical_join_correlation_key(correlation));
        }
    }
    if !join.nested_joins.is_empty() {
        bytes.push(b'j');
        put_len(&mut bytes, join.nested_joins.len());
        for nested in &join.nested_joins {
            put_bytes(&mut bytes, &canonical_join_key(nested));
        }
    }
    for filter in &join.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    bytes
}

fn canonical_join_correlation_key(correlation: &JoinCorrelation) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, &correlation.join_column);
    put_str(&mut bytes, &correlation.source_column);
    bytes
}

fn canonical_predicate_key(predicate: &Predicate) -> Vec<u8> {
    let mut bytes = Vec::new();
    match predicate {
        Predicate::All(predicates) => {
            bytes.push(b'A');
            let mut predicates = predicates
                .iter()
                .map(canonical_predicate_key)
                .collect::<Vec<_>>();
            predicates.sort();
            put_len(&mut bytes, predicates.len());
            for predicate in predicates {
                put_bytes(&mut bytes, &predicate);
            }
        }
        Predicate::Any(predicates) => {
            bytes.push(b'O');
            let mut predicates = predicates
                .iter()
                .map(canonical_predicate_key)
                .collect::<Vec<_>>();
            predicates.sort();
            put_len(&mut bytes, predicates.len());
            for predicate in predicates {
                put_bytes(&mut bytes, &predicate);
            }
        }
        Predicate::Not(predicate) => {
            bytes.push(b'!');
            put_bytes(&mut bytes, &canonical_predicate_key(predicate));
        }
        Predicate::Eq(left, right) => {
            bytes.push(b'e');
            let mut operands = [canonical_operand_key(left), canonical_operand_key(right)];
            operands.sort();
            put_bytes(&mut bytes, &operands[0]);
            put_bytes(&mut bytes, &operands[1]);
        }
        Predicate::Ne(left, right) => {
            bytes.push(b'n');
            let mut operands = [canonical_operand_key(left), canonical_operand_key(right)];
            operands.sort();
            put_bytes(&mut bytes, &operands[0]);
            put_bytes(&mut bytes, &operands[1]);
        }
        Predicate::In(left, values) => {
            bytes.push(b'i');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            let mut values = values.iter().map(canonical_operand_key).collect::<Vec<_>>();
            values.sort();
            put_len(&mut bytes, values.len());
            for value in values {
                put_bytes(&mut bytes, &value);
            }
        }
        Predicate::Gt(left, right) => {
            bytes.push(b'g');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Gte(left, right) => {
            bytes.push(b'G');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Lt(left, right) => {
            bytes.push(b't');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Lte(left, right) => {
            bytes.push(b'T');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::Contains(left, right) => {
            bytes.push(b'c');
            put_bytes(&mut bytes, &canonical_operand_key(left));
            put_bytes(&mut bytes, &canonical_operand_key(right));
        }
        Predicate::IsNull(operand) => {
            bytes.push(b'0');
            put_bytes(&mut bytes, &canonical_operand_key(operand));
        }
    }
    bytes
}

fn canonical_operand_key(operand: &Operand) -> Vec<u8> {
    let mut bytes = Vec::new();
    match operand {
        Operand::Column(name) => {
            bytes.push(b'c');
            put_str(&mut bytes, name);
        }
        Operand::Param(name) => {
            bytes.push(b'p');
            put_str(&mut bytes, name);
        }
        Operand::Claim(name) => {
            bytes.push(b'a');
            put_str(&mut bytes, name);
        }
        Operand::Literal(value) => {
            bytes.push(b'l');
            put_value(&mut bytes, value);
        }
    }
    bytes
}

fn canonical_query_bytes(query: &Query) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-query-v0");
    put_str(&mut bytes, &query.table);
    put_len(&mut bytes, query.filters.len());
    for filter in &query.filters {
        put_bytes(&mut bytes, &canonical_predicate_key(filter));
    }
    put_len(&mut bytes, query.joins.len());
    for join in &query.joins {
        put_bytes(&mut bytes, &canonical_join_key(join));
    }
    if !query.policy_branches.is_empty() {
        bytes.push(b'b');
        put_len(&mut bytes, query.policy_branches.len());
        for branch in &query.policy_branches {
            put_bytes(&mut bytes, &canonical_policy_branch_key(branch));
        }
    }
    if !query.reachable.is_empty() {
        bytes.push(b'r');
        put_len(&mut bytes, query.reachable.len());
        for reachable in &query.reachable {
            put_bytes(&mut bytes, &canonical_reachable_key(reachable));
        }
    }
    if !query.inherits.is_empty() {
        bytes.push(b'i');
        put_len(&mut bytes, query.inherits.len());
        for inherits in &query.inherits {
            put_bytes(&mut bytes, &canonical_inherits_key(inherits));
        }
    }
    put_len(&mut bytes, query.includes.len());
    for include in &query.includes {
        put_str(&mut bytes, &include.path);
        bytes.push(match include.join_mode {
            JoinMode::Inner => b'i',
            JoinMode::Holes => b'h',
        });
        bytes.push(u8::from(include.require));
    }
    if !query.array_subqueries.is_empty() {
        bytes.push(b'y');
        put_len(&mut bytes, query.array_subqueries.len());
        for subquery in &query.array_subqueries {
            put_bytes(&mut bytes, &canonical_array_subquery_key(subquery));
        }
    }
    if let Some(select) = &query.select {
        bytes.push(b's');
        put_len(&mut bytes, select.len());
        for column in select {
            put_str(&mut bytes, column);
        }
    }
    if !query.order_by.is_empty() {
        bytes.push(b'o');
        put_len(&mut bytes, query.order_by.len());
        for order in &query.order_by {
            put_str(&mut bytes, &order.column);
            bytes.push(match order.direction {
                OrderDirection::Asc => b'a',
                OrderDirection::Desc => b'd',
            });
        }
    }
    if let Some(aggregate) = &query.aggregate {
        bytes.push(b'a');
        put_len(&mut bytes, aggregate.aggregates.len());
        for aggregate in &aggregate.aggregates {
            put_bytes(&mut bytes, &canonical_aggregate_key(aggregate));
        }
        if let Some(group_by) = &aggregate.group_by {
            bytes.push(1);
            put_str(&mut bytes, group_by);
        } else {
            bytes.push(0);
        }
    }
    if query.limit.is_some() || query.offset != 0 {
        bytes.push(b'p');
        match query.limit {
            Some(limit) => {
                bytes.push(1);
                put_len(&mut bytes, limit);
            }
            None => bytes.push(0),
        }
        put_len(&mut bytes, query.offset);
    }
    bytes
}

fn canonical_binding_bytes(values: &BTreeMap<String, Value>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"jazz-binding-v0");
    put_len(&mut bytes, values.len());
    for (name, value) in values {
        put_str(&mut bytes, name);
        put_value(&mut bytes, value);
    }
    bytes
}

fn value_type(value: &Value) -> ColumnType {
    match value {
        Value::U8(_) => ColumnType::U8,
        Value::U16(_) => ColumnType::U16,
        Value::U32(_) => ColumnType::U32,
        Value::U64(_) => ColumnType::U64,
        Value::F64(_) => ColumnType::F64,
        Value::Bool(_) => ColumnType::Bool,
        Value::String(_) => ColumnType::String,
        Value::Bytes(_) => ColumnType::Bytes,
        Value::Uuid(_) => ColumnType::Uuid,
        Value::Enum(_) => ColumnType::U8,
        Value::Tuple(values) => ColumnType::Tuple(values.iter().map(value_type).collect()),
        Value::Array(values) => values
            .first()
            .map(|value| ColumnType::Array(Box::new(value_type(value))))
            .unwrap_or_else(|| ColumnType::Array(Box::new(ColumnType::Bytes))),
        Value::Nullable(Some(value)) => ColumnType::Nullable(Box::new(value_type(value))),
        Value::Nullable(None) => ColumnType::Nullable(Box::new(ColumnType::Bytes)),
    }
}

fn value_matches_type(value: &Value, column_type: &ColumnType) -> bool {
    match (value, column_type) {
        (Value::U8(_), ColumnType::U8)
        | (Value::U16(_), ColumnType::U16)
        | (Value::U32(_), ColumnType::U32)
        | (Value::U64(_), ColumnType::U64)
        | (Value::F64(_), ColumnType::F64)
        | (Value::Bool(_), ColumnType::Bool)
        | (Value::String(_), ColumnType::String)
        | (Value::Bytes(_), ColumnType::Bytes)
        | (Value::Uuid(_), ColumnType::Uuid) => true,
        (Value::Enum(_), ColumnType::Enum(_)) => true,
        (Value::Tuple(values), ColumnType::Tuple(types)) => {
            values.len() == types.len()
                && values
                    .iter()
                    .zip(types)
                    .all(|(value, column_type)| value_matches_type(value, column_type))
        }
        (Value::Array(values), ColumnType::Array(item_type)) => values
            .iter()
            .all(|value| value_matches_type(value, item_type)),
        (Value::Nullable(None), ColumnType::Nullable(_)) => true,
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => {
            value_matches_type(value, inner)
        }
        _ => false,
    }
}

fn put_value(bytes: &mut Vec<u8>, value: &Value) {
    match value {
        Value::U8(value) => {
            bytes.push(1);
            bytes.push(*value);
        }
        Value::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::U32(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::U64(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        Value::F64(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::Bool(value) => {
            bytes.push(6);
            bytes.push(u8::from(*value));
        }
        Value::String(value) => {
            bytes.push(7);
            put_str(bytes, value);
        }
        Value::Bytes(value) => {
            bytes.push(8);
            put_bytes(bytes, value);
        }
        Value::Uuid(value) => {
            bytes.push(9);
            bytes.extend_from_slice(value.as_bytes());
        }
        Value::Enum(value) => {
            bytes.push(10);
            bytes.push(*value);
        }
        Value::Tuple(values) => {
            bytes.push(11);
            put_len(bytes, values.len());
            for value in values {
                put_value(bytes, value);
            }
        }
        Value::Array(values) => {
            bytes.push(12);
            put_len(bytes, values.len());
            for value in values {
                put_value(bytes, value);
            }
        }
        Value::Nullable(None) => {
            bytes.push(13);
            bytes.push(0);
        }
        Value::Nullable(Some(value)) => {
            bytes.push(13);
            bytes.push(1);
            put_value(bytes, value);
        }
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_len(bytes, value.len());
    bytes.extend_from_slice(value);
}

fn put_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u64).to_be_bytes());
}

#[doc(hidden)]
pub mod doctest_support {
    use groove::schema::{ColumnSchema, ColumnType};

    use crate::schema::{JazzSchema, TableSchema};

    /// Example schema used by query-builder doctests.
    pub fn schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("project", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                    ColumnSchema::new("labels", ColumnType::String.array_of()),
                    ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
                ],
            )
            .with_reference("assignee", "users")
            .with_reference("project", "projects"),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new(
                "projects",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("org", ColumnType::Uuid),
                ],
            )
            .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::schema::{ColumnSchema, ColumnType};

    fn schema() -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("project", ColumnType::Uuid),
                    ColumnSchema::new("priority", ColumnType::U64),
                    ColumnSchema::new("labels", ColumnType::String.array_of()),
                    ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
                ],
            )
            .with_reference("assignee", "users")
            .with_reference("project", "projects"),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new(
                "projects",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("org", ColumnType::Uuid),
                ],
            )
            .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ])
    }

    #[test]
    fn builder_validate_and_canonicalize_round_trip() {
        let query = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))])
            .include("project.org");
        let validated = query.validate(&schema()).unwrap();
        assert_eq!(validated.query().table, "issues");
        assert_eq!(validated.params().len(), 2);
        assert_eq!(validated.params()["user"], ColumnType::Uuid);
        assert_eq!(validated.params()["tag"], ColumnType::Uuid);
        assert!(!validated.canonical_bytes().is_empty());
    }

    #[test]
    fn contains_param_array_against_column_infers_array_type() {
        let validated = Query::from("issues")
            .filter(contains(param("teams"), col("assignee")))
            .validate(&schema())
            .unwrap();

        assert_eq!(
            validated.params()["teams"],
            ColumnType::Array(Box::new(ColumnType::Uuid))
        );
    }

    #[test]
    fn filter_order_does_not_change_shape_id() {
        let schema = schema();
        let left = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .validate(&schema)
            .unwrap();
        let right = Query::from("issues")
            .filter(ne(lit("done"), col("state")))
            .filter(eq(param("user"), col("assignee")))
            .validate(&schema)
            .unwrap();
        assert_eq!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn validates_boolean_operators_projection_includes_and_pagination() {
        let query = Query::from("issues")
            .filter(all_of([
                any_of([
                    eq(col("state"), lit("open")),
                    eq(col("state"), lit("blocked")),
                ]),
                in_list(col("state"), [lit("open"), lit("blocked")]),
                not(ne(col("assignee"), param("user"))),
                gt(col("priority"), lit(1_u64)),
                gte(col("priority"), lit(2_u64)),
                lt(col("priority"), lit(10_u64)),
                lte(col("priority"), lit(9_u64)),
                gt(col("title"), lit("bug")),
                gte(col("title"), lit("bug")),
                lt(col("title"), lit("z")),
                lte(col("title"), lit("z")),
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("backend")),
                is_null(col("snoozed_until")),
            ]))
            .include_with(Include::new("project.org").join_mode(JoinMode::Holes))
            .select(["title", "state", "$createdAt"])
            .offset(5)
            .limit(10);

        let validated = query.validate(&schema()).unwrap();
        assert_eq!(validated.params()["user"], ColumnType::Uuid);
        assert_eq!(validated.query().offset, 5);
        assert_eq!(validated.query().limit, Some(10));
        assert_eq!(
            validated.query().select.as_deref(),
            Some(
                [
                    "$createdAt".to_owned(),
                    "state".to_owned(),
                    "title".to_owned()
                ]
                .as_slice()
            )
        );
        assert_eq!(validated.query().includes[0].join_mode, JoinMode::Holes);
    }

    #[test]
    fn validates_array_subquery_shape_without_execution() {
        // Internal test: array-subquery execution is not implemented yet, but
        // shape validation/canonical identity are query-module responsibilities.
        let validated = Query::from("issues")
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(eq(col("tag"), param("tag")))
                    .select(["tag"])
                    .order_by("tag", OrderDirection::Asc)
                    .limit(5)
                    .requirement(ArraySubqueryRequirement::AtLeastOne)
                    .nested(ArraySubquery::new("tagRows", "tags", "id", "tag").select(["name"])),
            )
            .validate(&schema())
            .unwrap();

        assert_eq!(validated.params()["tag"], ColumnType::Uuid);
        let subquery = &validated.query().array_subqueries[0];
        assert_eq!(subquery.column_name, "tags");
        assert_eq!(subquery.nested_arrays[0].column_name, "tagRows");
        assert_eq!(
            subquery.select.as_deref(),
            Some(["tag".to_owned()].as_slice())
        );
    }

    #[test]
    fn array_subquery_order_does_not_change_shape_id() {
        // Internal test: canonicalization should be stable before execution is
        // exposed through black-box relation payload tests.
        let left = Query::from("issues")
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(eq(col("tag"), param("tag")))
                    .filter(ne(col("issue"), param("issue"))),
            )
            .array_subquery(ArraySubquery::new(
                "projectIssues",
                "issues",
                "project",
                "project",
            ))
            .validate(&schema())
            .unwrap();
        let right = Query::from("issues")
            .array_subquery(ArraySubquery::new(
                "projectIssues",
                "issues",
                "project",
                "project",
            ))
            .array_subquery(
                ArraySubquery::new("tags", "issue_tags", "issue", "id")
                    .filter(ne(col("issue"), param("issue")))
                    .filter(eq(col("tag"), param("tag"))),
            )
            .validate(&schema())
            .unwrap();

        assert_eq!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn rejects_invalid_array_subquery_shapes() {
        let schema = schema();

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "missing", "issue", "id"))
            .validate(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownTable(_)));

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "issue_tags", "missing", "id"))
            .validate(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("bad", "issue_tags", "issue", "title"))
            .validate(&schema)
            .unwrap_err();
        assert_eq!(err, QueryError::OperandTypeMismatch);

        let err = Query::from("issues")
            .array_subquery(ArraySubquery::new("dupe", "issue_tags", "issue", "id"))
            .array_subquery(ArraySubquery::new("dupe", "issues", "id", "id"))
            .validate(&schema)
            .unwrap_err();
        assert!(matches!(err, QueryError::BadIncludePath { .. }));
    }

    #[test]
    fn validates_order_by_columns_and_preserves_key_order() {
        let validated = Query::from("issues")
            .order_by("state", OrderDirection::Asc)
            .order_by("priority", OrderDirection::Desc)
            .validate(&schema())
            .unwrap();
        assert_eq!(
            validated.query().order_by,
            vec![
                OrderBy {
                    column: "state".to_owned(),
                    direction: OrderDirection::Asc,
                },
                OrderBy {
                    column: "priority".to_owned(),
                    direction: OrderDirection::Desc,
                },
            ]
        );

        let err = Query::from("issues")
            .order_by("missing", OrderDirection::Asc)
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
    }

    #[test]
    fn rejects_large_value_columns_in_filters_joins_and_ordering() {
        let schema = JazzSchema::new([
            TableSchema::new(
                "docs",
                [
                    crate::schema::ColumnSchema::new("owner", ColumnType::Uuid),
                    crate::schema::ColumnSchema::text("body"),
                    crate::schema::ColumnSchema::blob("attachment"),
                ],
            )
            .with_reference("body", "docs"),
            TableSchema::new(
                "doc_links",
                [
                    crate::schema::ColumnSchema::text("doc"),
                    crate::schema::ColumnSchema::new("team", ColumnType::Uuid),
                ],
            )
            .with_reference("doc", "docs")
            .with_reference("team", "teams"),
            TableSchema::new(
                "team_edges",
                [
                    crate::schema::ColumnSchema::new("member", ColumnType::Uuid),
                    crate::schema::ColumnSchema::blob("parent"),
                ],
            )
            .with_reference("member", "teams")
            .with_reference("parent", "teams"),
            TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        ]);

        for err in [
            Query::from("docs")
                .filter(eq(col("body"), lit(Value::Bytes(b"text".to_vec()))))
                .validate(&schema)
                .unwrap_err(),
            Query::from("docs")
                .filter(eq(col("attachment"), lit(Value::Bytes(vec![1, 2, 3]))))
                .validate(&schema)
                .unwrap_err(),
            Query::from("docs")
                .join_via("doc_links", "doc", [])
                .validate(&schema)
                .unwrap_err(),
            Query::from("docs")
                .join_via_column("doc_links", "team", "body", [])
                .validate(&schema)
                .unwrap_err(),
            Query::from("docs")
                .reachable_via(
                    "doc_links",
                    "doc",
                    "team",
                    claim("team"),
                    "team_edges",
                    "member",
                    "parent",
                    [],
                )
                .validate(&schema)
                .unwrap_err(),
            Query::from("docs")
                .order_by("body", OrderDirection::Asc)
                .validate(&schema)
                .unwrap_err(),
        ] {
            assert!(matches!(err, QueryError::LargeValueColumnInQuery { .. }));
        }
    }

    #[test]
    fn validates_aggregate_columns_types_grouping_and_ordering() {
        let validated = Query::from("issues")
            .aggregate([
                Aggregate::count(),
                Aggregate::sum("priority"),
                Aggregate::min("priority"),
                Aggregate::max("priority"),
            ])
            .group_by("state")
            .order_by("state", OrderDirection::Asc)
            .order_by("count", OrderDirection::Desc)
            .validate(&schema())
            .unwrap();
        let aggregate = validated.query().aggregate.as_ref().unwrap();
        assert_eq!(aggregate.group_by.as_deref(), Some("state"));
        assert_eq!(aggregate.aggregates.len(), 4);

        let err = Query::from("issues")
            .sum("title")
            .validate(&schema())
            .unwrap_err();
        assert_eq!(err, QueryError::OperandTypeMismatch);

        let err = Query::from("issues")
            .count()
            .group_by("missing")
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));

        let err = Query::from("issues")
            .count()
            .order_by("priority", OrderDirection::Asc)
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
    }

    #[test]
    fn semantic_difference_changes_shape_id() {
        let schema = schema();
        let left = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate(&schema)
            .unwrap();
        let right = Query::from("issues")
            .filter(ne(col("assignee"), param("user")))
            .validate(&schema)
            .unwrap();
        assert_ne!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn schema_version_context_changes_shape_id() {
        let base = schema();
        let evolved = JazzSchema::new([
            TableSchema::new(
                "issues",
                [
                    ColumnSchema::new("title", ColumnType::String),
                    ColumnSchema::new("state", ColumnType::String),
                    ColumnSchema::new("assignee", ColumnType::Uuid),
                    ColumnSchema::new("body", ColumnType::String),
                ],
            ),
            TableSchema::new(
                "issue_tags",
                [
                    ColumnSchema::new("issue", ColumnType::Uuid),
                    ColumnSchema::new("tag", ColumnType::Uuid),
                ],
            )
            .with_reference("issue", "issues")
            .with_reference("tag", "tags"),
            TableSchema::new("projects", [ColumnSchema::new("org", ColumnType::Uuid)])
                .with_reference("org", "orgs"),
            TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new("tags", [ColumnSchema::new("name", ColumnType::String)]),
        ]);
        let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
        let left = query.validate(&base).unwrap();
        let right = query.validate(&evolved).unwrap();

        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_ne!(left.schema_version(), right.schema_version());
        assert_ne!(left.shape_id(), right.shape_id());
    }

    #[test]
    fn binding_type_mismatch_errors() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate(&schema())
            .unwrap();
        let err = validated
            .bind(BTreeMap::from([(
                "user".to_owned(),
                Value::String("not-a-uuid".to_owned()),
            )]))
            .unwrap_err();
        assert!(matches!(err, QueryError::ParamTypeMismatch { .. }));
    }

    #[test]
    fn claim_column_type_mismatch_errors_loudly() {
        let err = Query::from("issues")
            .filter(eq(col("state"), claim("sub")))
            .validate(&schema())
            .unwrap_err();

        assert_eq!(
            err,
            QueryError::ClaimTypeMismatch {
                claim_path: "sub".to_owned(),
                column: "state".to_owned(),
                claim_type: "Uuid".to_owned(),
                column_type: "String".to_owned(),
            }
        );
    }

    #[test]
    fn claim_column_matched_types_still_validate() {
        Query::from("issues")
            .filter(eq(col("assignee"), claim("sub")))
            .validate(&schema())
            .unwrap();

        Query::from("issues")
            .filter(eq(col("state"), claim("user_id")))
            .validate(&schema())
            .unwrap();
    }

    #[test]
    fn include_path_resolution_errors_on_bad_path() {
        let err = Query::from("issues")
            .include("project.missing")
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::UnknownColumn { .. }));
        let err = Query::from("issues")
            .include("title.name")
            .validate(&schema())
            .unwrap_err();
        assert!(matches!(err, QueryError::BadIncludePath { .. }));
    }

    #[test]
    fn binding_id_uses_canonical_binding_values() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .validate(&schema())
            .unwrap();
        let user = uuid::uuid!("00000000-0000-0000-0000-000000000001");
        let binding = validated
            .bind(BTreeMap::from([("user".to_owned(), Value::Uuid(user))]))
            .unwrap();
        assert_eq!(
            binding.binding_id(),
            BindingId(uuid::Uuid::new_v5(
                &QUERY_NAMESPACE,
                binding.canonical_bytes()
            ))
        );
    }

    #[test]
    fn canonical_bytes_stability_golden() {
        let validated = Query::from("issues")
            .filter(eq(col("assignee"), param("user")))
            .filter(ne(col("state"), lit("done")))
            .join_via("issue_tags", "issue", [eq(col("tag"), param("tag"))])
            .include("project.org")
            .validate(&schema())
            .unwrap();
        assert_eq!(
            validated.shape_id().0.to_string(),
            "dd92ae54-eeec-57e1-be75-f3957227bed8"
        );
    }
}
