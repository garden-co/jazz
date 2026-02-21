use serde::{Deserialize, Serialize};

use crate::query_manager::encoding::encode_value_with_type;
use crate::query_manager::graph_nodes::filter::Predicate;
use crate::query_manager::graph_nodes::sort::{SortDirection, SortKey};
use crate::query_manager::types::{RowDescriptor, TableName, Value};

/// A join specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Table to join.
    pub table: TableName,
    /// Optional alias for the joined table.
    pub alias: Option<String>,
    /// Join condition: (left_column, right_column).
    /// Left refers to the accumulated result, right refers to this join's table.
    pub on: Option<(String, String)>,
}

impl JoinSpec {
    /// Get the effective name (alias if set, otherwise table name).
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(self.table.as_str())
    }
}

/// A condition in a WHERE clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    /// Column equals value.
    Eq { column: String, value: Value },
    /// Column not equals value.
    Ne { column: String, value: Value },
    /// Column less than value.
    Lt { column: String, value: Value },
    /// Column less than or equal to value.
    Le { column: String, value: Value },
    /// Column greater than value.
    Gt { column: String, value: Value },
    /// Column greater than or equal to value.
    Ge { column: String, value: Value },
    /// Column in range [min, max] inclusive.
    Between {
        column: String,
        min: Value,
        max: Value,
    },
    /// Array column contains value.
    Contains { column: String, value: Value },
    /// Column is null.
    IsNull { column: String },
    /// Column is not null.
    IsNotNull { column: String },
}

impl Condition {
    /// Get the column name this condition applies to.
    pub fn column(&self) -> &str {
        match self {
            Condition::Eq { column, .. } => column,
            Condition::Ne { column, .. } => column,
            Condition::Lt { column, .. } => column,
            Condition::Le { column, .. } => column,
            Condition::Gt { column, .. } => column,
            Condition::Ge { column, .. } => column,
            Condition::Between { column, .. } => column,
            Condition::Contains { column, .. } => column,
            Condition::IsNull { column } => column,
            Condition::IsNotNull { column } => column,
        }
    }

    /// Check if this condition can be used for an index scan.
    pub fn is_index_scannable(&self) -> bool {
        matches!(
            self,
            Condition::Eq { .. }
                | Condition::Lt { .. }
                | Condition::Le { .. }
                | Condition::Gt { .. }
                | Condition::Ge { .. }
                | Condition::Between { .. }
        )
    }

    /// Convert to a Predicate for filter evaluation.
    pub fn to_predicate(&self, descriptor: &RowDescriptor) -> Option<Predicate> {
        let col_index = descriptor.column_index(self.column())?;
        let col_type = &descriptor.columns[col_index].column_type;

        Some(match self {
            Condition::Eq { value, .. } => Predicate::Eq {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Ne { value, .. } => Predicate::Ne {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Lt { value, .. } => Predicate::Lt {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Le { value, .. } => Predicate::Le {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Gt { value, .. } => Predicate::Gt {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Ge { value, .. } => Predicate::Ge {
                col_index,
                value: encode_value_with_type(value, col_type),
            },
            Condition::Between { min, max, .. } => Predicate::And(vec![
                Predicate::Ge {
                    col_index,
                    value: encode_value_with_type(min, col_type),
                },
                Predicate::Le {
                    col_index,
                    value: encode_value_with_type(max, col_type),
                },
            ]),
            Condition::Contains { value, .. } => Predicate::Contains {
                col_index,
                value: value.clone(),
            },
            Condition::IsNull { .. } => Predicate::IsNull { col_index },
            Condition::IsNotNull { .. } => Predicate::IsNotNull { col_index },
        })
    }
}

/// A conjunction (AND group) of conditions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conjunction {
    pub conditions: Vec<Condition>,
}

impl Conjunction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, condition: Condition) {
        self.conditions.push(condition);
    }

    /// Find the best condition for index scanning.
    /// Prefers Eq over range conditions.
    pub fn best_index_condition(&self) -> Option<&Condition> {
        // First look for Eq conditions
        if let Some(cond) = self
            .conditions
            .iter()
            .find(|c| matches!(c, Condition::Eq { .. }))
        {
            return Some(cond);
        }
        // Then any other index-scannable condition
        self.conditions.iter().find(|c| c.is_index_scannable())
    }

    /// Get remaining conditions after removing the index condition.
    pub fn remaining_conditions(&self, index_column: &str) -> Vec<&Condition> {
        self.conditions
            .iter()
            .filter(|c| c.column() != index_column)
            .collect()
    }

    /// Check if all conditions are fully covered by index scan on the given column.
    /// Returns true if the only condition(s) are on the index column and are index-scannable.
    pub fn is_fully_covered_by_index(&self, index_column: &str) -> bool {
        if self.conditions.is_empty() {
            return true;
        }
        // All conditions must be on the index column and index-scannable
        self.conditions
            .iter()
            .all(|c| c.column() == index_column && c.is_index_scannable())
    }

    /// Convert remaining (non-indexed) conditions to a Predicate.
    /// Returns Predicate::True if all conditions are covered by the index.
    pub fn remaining_predicate(&self, index_column: &str, descriptor: &RowDescriptor) -> Predicate {
        let remaining: Vec<_> = self
            .remaining_conditions(index_column)
            .into_iter()
            .filter_map(|c| c.to_predicate(descriptor))
            .collect();

        if remaining.is_empty() {
            Predicate::True
        } else if remaining.len() == 1 {
            remaining.into_iter().next().unwrap()
        } else {
            Predicate::And(remaining)
        }
    }

    /// Convert to a Predicate.
    pub fn to_predicate(&self, descriptor: &RowDescriptor) -> Predicate {
        if self.conditions.is_empty() {
            return Predicate::True;
        }

        let predicates: Vec<_> = self
            .conditions
            .iter()
            .filter_map(|c| c.to_predicate(descriptor))
            .collect();

        if predicates.len() == 1 {
            predicates.into_iter().next().unwrap()
        } else {
            Predicate::And(predicates)
        }
    }
}

/// Specification for an array subquery (correlated subquery producing array column).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArraySubquerySpec {
    /// Name for the output array column.
    pub column_name: String,
    /// Inner table to query.
    pub table: TableName,
    /// Joins within the inner query.
    pub joins: Vec<JoinSpec>,
    /// Column in inner table to correlate with outer.
    pub inner_column: String,
    /// Column in outer table (or alias.column) to use as correlation value.
    pub outer_column: String,
    /// Filters to apply to inner query.
    pub filters: Vec<Condition>,
    /// Columns to select from inner query (None = all columns).
    pub select_columns: Option<Vec<String>>,
    /// Order by for inner query results.
    pub order_by: Vec<(String, SortDirection)>,
    /// Limit on inner query results.
    pub limit: Option<usize>,
    /// Nested array subqueries (for recursive structures).
    pub nested_arrays: Vec<ArraySubquerySpec>,
}

impl ArraySubquerySpec {
    /// Create a new array subquery specification.
    pub fn new(column_name: impl Into<String>, table: impl Into<TableName>) -> Self {
        Self {
            column_name: column_name.into(),
            table: table.into(),
            joins: Vec::new(),
            inner_column: String::new(),
            outer_column: String::new(),
            filters: Vec::new(),
            select_columns: None,
            order_by: Vec::new(),
            limit: None,
            nested_arrays: Vec::new(),
        }
    }
}

/// Specification for a recursive relation expansion.
///
/// The current query acts as the seed relation. Each recursive step evaluates
/// `table` with `inner_column = seed_value`, then projects `select_columns`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursiveHopSpec {
    /// Target table reached from each recursive step row.
    pub table: TableName,
    /// Column on the step table that stores the target row id.
    pub via_column: String,
}

/// Specification for a recursive relation expansion.
///
/// The current query acts as the seed relation. Each recursive step evaluates
/// `table` with `inner_column = seed_value`, then projects `select_columns`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursiveSpec {
    /// Inner table to query at each step.
    pub table: TableName,
    /// Column in inner table to correlate with previous frontier rows.
    pub inner_column: String,
    /// Column from the recursive output relation used as the next frontier value.
    pub outer_column: String,
    /// Columns selected from each step (None = all columns).
    pub select_columns: Option<Vec<String>>,
    /// Additional filters to apply to each recursive step query.
    #[serde(default)]
    pub filters: Vec<Condition>,
    /// Optional joins to apply to each recursive step query.
    #[serde(default)]
    pub joins: Vec<JoinSpec>,
    /// Optional projected tuple element index for recursive step join output.
    #[serde(default)]
    pub result_element_index: Option<usize>,
    /// Optional hop from each step row to target rows.
    #[serde(default)]
    pub hop: Option<RecursiveHopSpec>,
    /// Maximum recursion depth (levels beyond the seed level).
    pub max_depth: usize,
}

/// A query specification (DNF: disjunction of conjunctions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub table: TableName,
    /// Optional table alias (for self-joins).
    #[serde(default)]
    pub alias: Option<String>,
    /// Branches to query (required - at least one must be specified).
    /// For multi-branch queries, results are combined with LWW merge for same ObjectId.
    #[serde(default)]
    pub branches: Vec<String>,
    /// Joined tables.
    #[serde(default)]
    pub joins: Vec<JoinSpec>,
    /// OR groups (disjunction of conjunctions).
    #[serde(default = "default_disjuncts")]
    pub disjuncts: Vec<Conjunction>,
    /// Order by specification.
    #[serde(default)]
    pub order_by: Vec<(String, SortDirection)>,
    /// Limit.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Offset.
    #[serde(default)]
    pub offset: usize,
    /// If true, also scan _id_deleted to include soft-deleted rows.
    #[serde(default)]
    pub include_deleted: bool,
    /// Columns to select (None = all columns).
    #[serde(default)]
    pub select_columns: Option<Vec<String>>,
    /// Array subqueries (correlated subqueries producing array columns).
    #[serde(default)]
    pub array_subqueries: Vec<ArraySubquerySpec>,
    /// Optional recursive relation expansion.
    #[serde(default)]
    pub recursive: Option<RecursiveSpec>,
    /// Optional output tuple element index for join queries.
    ///
    /// When set, join query output is projected to this tuple element
    /// instead of returning flattened combined rows.
    #[serde(default)]
    pub result_element_index: Option<usize>,
    /// Optional relation IR payload for unified query/policy planning.
    ///
    /// Current runtime still executes the legacy query fields; this IR is
    /// carried through the boundary to enable incremental compiler cutover.
    #[serde(default)]
    pub relation_ir: Option<crate::query_manager::relation_ir::RelExpr>,
}

/// Default disjuncts - one empty conjunction (matches all rows).
fn default_disjuncts() -> Vec<Conjunction> {
    vec![Conjunction::new()]
}

impl Query {
    /// Create a new query for a table (internal use - branches not set).
    fn new_internal(table: impl Into<TableName>) -> Self {
        Self {
            table: table.into(),
            alias: None,
            branches: Vec::new(),
            joins: Vec::new(),
            disjuncts: vec![Conjunction::new()],
            order_by: Vec::new(),
            limit: None,
            offset: 0,
            include_deleted: false,
            select_columns: None,
            array_subqueries: Vec::new(),
            recursive: None,
            result_element_index: None,
            relation_ir: None,
        }
    }

    /// Create a new query for a table.
    ///
    /// Note: Branch must be set explicitly before execution.
    pub fn new(table: impl Into<TableName>) -> Self {
        Self::new_internal(table)
    }

    /// Check if this is a multi-branch query.
    pub fn is_multi_branch(&self) -> bool {
        self.branches.len() > 1
    }

    /// Check if this query has array subqueries.
    pub fn has_array_subqueries(&self) -> bool {
        !self.array_subqueries.is_empty()
    }

    /// Check if this query has a recursive expansion.
    pub fn has_recursive(&self) -> bool {
        self.recursive.is_some()
    }

    /// Check if this query carries relation IR.
    pub fn has_relation_ir(&self) -> bool {
        self.relation_ir.is_some()
    }

    /// Check if this is a join query.
    pub fn is_join(&self) -> bool {
        !self.joins.is_empty()
    }

    /// Get the effective table name (alias if set, otherwise table name).
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(self.table.as_str())
    }

    /// Get the full predicate for this query.
    pub fn to_predicate(&self, descriptor: &RowDescriptor) -> Predicate {
        if self.disjuncts.is_empty() {
            return Predicate::True;
        }

        // Filter out empty conjunctions
        let non_empty: Vec<_> = self
            .disjuncts
            .iter()
            .filter(|d| !d.conditions.is_empty())
            .collect();

        if non_empty.is_empty() {
            return Predicate::True;
        }

        if non_empty.len() == 1 {
            return non_empty[0].to_predicate(descriptor);
        }

        let predicates: Vec<_> = non_empty
            .iter()
            .map(|d| d.to_predicate(descriptor))
            .collect();

        Predicate::Or(predicates)
    }

    /// Get sort keys for this query.
    pub fn sort_keys(&self, descriptor: &RowDescriptor) -> Vec<SortKey> {
        self.order_by
            .iter()
            .filter_map(|(col, dir)| {
                descriptor.column_index(col).map(|idx| SortKey {
                    col_index: idx,
                    direction: *dir,
                })
            })
            .collect()
    }
}

/// Builder for constructing queries fluently.
pub struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    /// Start building a query for a table.
    ///
    /// Note: Branch must be explicitly specified via `.branch()` or `.branches()`.
    /// Queries without branches will error at execution time unless a SchemaContext
    /// is present (which provides branch expansion).
    pub fn new(table: impl Into<TableName>) -> Self {
        Self {
            // Use new_internal which doesn't set default branch
            // The branch will be set via .branch() or .branches()
            query: Query::new_internal(table),
        }
    }

    /// Query a single branch (required).
    ///
    /// # Example
    /// ```ignore
    /// QueryBuilder::new("users").branch("main").build()
    /// QueryBuilder::new("users").branch("draft").build()
    /// ```
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.query.branches = vec![branch.into()];
        self
    }

    /// Query multiple branches (results merged with LWW for same ObjectId).
    ///
    /// # Example
    /// ```ignore
    /// QueryBuilder::new("users").branches(&["main", "draft"]).build()
    /// ```
    pub fn branches(mut self, branches: &[&str]) -> Self {
        self.query.branches = branches.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add an equals filter condition.
    pub fn filter_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Eq {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a not equals filter condition.
    pub fn filter_ne(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Ne {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a less than filter condition.
    pub fn filter_lt(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Lt {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a less than or equal filter condition.
    pub fn filter_le(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Le {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a greater than filter condition.
    pub fn filter_gt(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Gt {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a greater than or equal filter condition.
    pub fn filter_ge(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Ge {
            column: column.into(),
            value,
        });
        self
    }

    /// Add a range filter condition.
    pub fn filter_between(mut self, column: impl Into<String>, min: Value, max: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Between {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Add an is null filter condition.
    pub fn filter_is_null(mut self, column: impl Into<String>) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::IsNull {
            column: column.into(),
        });
        self
    }

    /// Add an is not null filter condition.
    pub fn filter_is_not_null(mut self, column: impl Into<String>) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::IsNotNull {
            column: column.into(),
        });
        self
    }

    /// Add an array contains filter condition.
    pub fn filter_contains(mut self, column: impl Into<String>, value: Value) -> Self {
        let current = self.query.disjuncts.last_mut().unwrap();
        current.add(Condition::Contains {
            column: column.into(),
            value,
        });
        self
    }

    /// Start a new OR branch.
    pub fn or(mut self) -> Self {
        self.query.disjuncts.push(Conjunction::new());
        self
    }

    /// Add an order by clause (ascending).
    pub fn order_by(mut self, column: impl Into<String>) -> Self {
        self.query
            .order_by
            .push((column.into(), SortDirection::Ascending));
        self
    }

    /// Add an order by clause (descending).
    pub fn order_by_desc(mut self, column: impl Into<String>) -> Self {
        self.query
            .order_by
            .push((column.into(), SortDirection::Descending));
        self
    }

    /// Set a limit.
    pub fn limit(mut self, n: usize) -> Self {
        self.query.limit = Some(n);
        self
    }

    /// Set an offset.
    pub fn offset(mut self, n: usize) -> Self {
        self.query.offset = n;
        self
    }

    /// Include soft-deleted rows in query results.
    /// When true, the query will also scan the _id_deleted index.
    pub fn include_deleted(mut self) -> Self {
        self.query.include_deleted = true;
        self
    }

    /// Set a table alias.
    ///
    /// If called before any join(), applies to the base table.
    /// If called after join(), applies to the most recent joined table.
    ///
    /// Example: `query("users").alias("u1").join("posts").alias("p")`
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        let alias_str = alias.into();
        if let Some(last_join) = self.query.joins.last_mut() {
            // Apply to most recent join
            last_join.alias = Some(alias_str);
        } else {
            // Apply to base table
            self.query.alias = Some(alias_str);
        }
        self
    }

    /// Join another table.
    ///
    /// Example: `query("users").join("posts")` creates an inner join.
    /// Use `.on()` to specify the join condition.
    pub fn join(mut self, table: impl Into<TableName>) -> Self {
        self.query.joins.push(JoinSpec {
            table: table.into(),
            alias: None,
            on: None,
        });
        self
    }

    /// Specify the join condition for the most recent join.
    ///
    /// Format: `"left_table.column"` and `"right_table.column"`
    /// Or unqualified column names if unambiguous.
    ///
    /// Example: `query("users").alias("u").join("posts").alias("p").on("u.id", "p.author_id")`
    pub fn on(mut self, left_col: impl Into<String>, right_col: impl Into<String>) -> Self {
        if let Some(last_join) = self.query.joins.last_mut() {
            last_join.on = Some((left_col.into(), right_col.into()));
        }
        self
    }

    /// Select specific columns (projection).
    ///
    /// If not called, all columns are returned.
    /// Example: `query("users").select(&["name", "email"])`
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.query.select_columns = Some(columns.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Add an array subquery (correlated subquery producing an array column).
    ///
    /// The closure receives an `ArraySubqueryBuilder` to configure the subquery.
    ///
    /// # Example
    /// ```ignore
    /// QueryBuilder::new("users")
    ///     .with_array("posts", |sub| {
    ///         sub.from("posts")
    ///            .correlate("author_id", "users.id")
    ///            .select(&["id", "title"])
    ///            .order_by_desc("created_at")
    ///            .limit(10)
    ///     })
    ///     .build()
    /// ```
    pub fn with_array<F>(mut self, column_name: impl Into<String>, builder_fn: F) -> Self
    where
        F: FnOnce(ArraySubqueryBuilder) -> ArraySubqueryBuilder,
    {
        let builder = ArraySubqueryBuilder::new(column_name);
        let configured = builder_fn(builder);
        self.query.array_subqueries.push(configured.build());
        self
    }

    /// Add a recursive relation expansion.
    ///
    /// The current query output is used as the seed relation.
    pub fn with_recursive<F>(mut self, builder_fn: F) -> Self
    where
        F: FnOnce(RecursiveBuilder) -> RecursiveBuilder,
    {
        let builder = RecursiveBuilder::new();
        let configured = builder_fn(builder);
        self.query.recursive = Some(configured.build());
        self
    }

    /// Build the query.
    ///
    /// Branches should be specified via `.branch()` or `.branches()`.
    /// If no branches specified:
    /// - With SchemaManager: automatically expands to all live schema branches
    /// - Without SchemaManager: QueryManager returns an error
    pub fn build(self) -> Query {
        self.query
    }
}

/// Builder for configuring array subqueries.
///
/// Used with `QueryBuilder::with_array()` to define correlated subqueries
/// that produce array columns.
#[derive(Debug)]
pub struct ArraySubqueryBuilder {
    column_name: String,
    table: Option<TableName>,
    joins: Vec<JoinSpec>,
    inner_column: String,
    outer_column: String,
    filters: Vec<Condition>,
    select_columns: Option<Vec<String>>,
    order_by: Vec<(String, SortDirection)>,
    limit: Option<usize>,
    nested_arrays: Vec<ArraySubquerySpec>,
}

impl ArraySubqueryBuilder {
    /// Create a new array subquery builder with the given output column name.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            table: None,
            joins: Vec::new(),
            inner_column: String::new(),
            outer_column: String::new(),
            filters: Vec::new(),
            select_columns: None,
            order_by: Vec::new(),
            limit: None,
            nested_arrays: Vec::new(),
        }
    }

    /// Set the inner table to query.
    pub fn from(mut self, table: impl Into<TableName>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Join another table within the subquery.
    pub fn join(mut self, table: impl Into<TableName>) -> Self {
        self.joins.push(JoinSpec {
            table: table.into(),
            alias: None,
            on: None,
        });
        self
    }

    /// Specify the join condition for the most recent join.
    pub fn on(mut self, left_col: impl Into<String>, right_col: impl Into<String>) -> Self {
        if let Some(last_join) = self.joins.last_mut() {
            last_join.on = Some((left_col.into(), right_col.into()));
        }
        self
    }

    /// Set the correlation columns.
    ///
    /// # Arguments
    /// * `inner_column` - Column in the inner table to match
    /// * `outer_column` - Column in the outer table (e.g., "users.id")
    pub fn correlate(
        mut self,
        inner_column: impl Into<String>,
        outer_column: impl Into<String>,
    ) -> Self {
        self.inner_column = inner_column.into();
        self.outer_column = outer_column.into();
        self
    }

    /// Select specific columns from the inner query.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.select_columns = Some(columns.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Add an equality filter on the inner query.
    pub fn filter_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        self.filters.push(Condition::Eq {
            column: column.into(),
            value,
        });
        self
    }

    /// Add ascending order by on inner query results.
    pub fn order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by
            .push((column.into(), SortDirection::Ascending));
        self
    }

    /// Add descending order by on inner query results.
    pub fn order_by_desc(mut self, column: impl Into<String>) -> Self {
        self.order_by
            .push((column.into(), SortDirection::Descending));
        self
    }

    /// Limit the number of results from the inner query.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Add a nested array subquery.
    ///
    /// # Example
    /// ```ignore
    /// sub.from("posts")
    ///    .correlate("author_id", "users.id")
    ///    .with_array("comments", |sub2| {
    ///        sub2.from("comments")
    ///            .correlate("post_id", "posts.id")
    ///    })
    /// ```
    pub fn with_array<F>(mut self, column_name: impl Into<String>, builder_fn: F) -> Self
    where
        F: FnOnce(ArraySubqueryBuilder) -> ArraySubqueryBuilder,
    {
        let builder = ArraySubqueryBuilder::new(column_name);
        let configured = builder_fn(builder);
        self.nested_arrays.push(configured.build());
        self
    }

    /// Build the ArraySubquerySpec.
    pub fn build(self) -> ArraySubquerySpec {
        ArraySubquerySpec {
            column_name: self.column_name,
            table: self.table.unwrap_or_else(|| TableName::new("")),
            joins: self.joins,
            inner_column: self.inner_column,
            outer_column: self.outer_column,
            filters: self.filters,
            select_columns: self.select_columns,
            order_by: self.order_by,
            limit: self.limit,
            nested_arrays: self.nested_arrays,
        }
    }
}

/// Builder for configuring recursive relation expansions.
#[derive(Debug)]
pub struct RecursiveBuilder {
    table: Option<TableName>,
    inner_column: String,
    outer_column: String,
    select_columns: Option<Vec<String>>,
    filters: Vec<Condition>,
    joins: Vec<JoinSpec>,
    result_element_index: Option<usize>,
    hop: Option<RecursiveHopSpec>,
    max_depth: usize,
}

impl RecursiveBuilder {
    /// Create a new recursive builder.
    pub fn new() -> Self {
        Self {
            table: None,
            inner_column: String::new(),
            outer_column: String::new(),
            select_columns: None,
            filters: Vec::new(),
            joins: Vec::new(),
            result_element_index: None,
            hop: None,
            max_depth: 10,
        }
    }

    /// Set the inner step table.
    pub fn from(mut self, table: impl Into<TableName>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Set the recursive correlation mapping.
    ///
    /// # Arguments
    /// * `inner_column` - Column in the inner step table to filter by frontier value
    /// * `outer_column` - Column in the recursive output relation used for next frontier
    pub fn correlate(
        mut self,
        inner_column: impl Into<String>,
        outer_column: impl Into<String>,
    ) -> Self {
        self.inner_column = inner_column.into();
        self.outer_column = outer_column.into();
        self
    }

    /// Select columns projected by each recursive step.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.select_columns = Some(columns.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Add an equality filter to each recursive step query.
    pub fn filter_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        self.filters.push(Condition::Eq {
            column: column.into(),
            value,
        });
        self
    }

    /// Join another table in each recursive step query.
    pub fn join(mut self, table: impl Into<TableName>) -> Self {
        self.joins.push(JoinSpec {
            table: table.into(),
            alias: None,
            on: None,
        });
        self
    }

    /// Set alias on the most recently added recursive step join.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        if let Some(join) = self.joins.last_mut() {
            join.alias = Some(alias.into());
        }
        self
    }

    /// Set join predicate on the most recently added recursive step join.
    pub fn on(mut self, left: impl Into<String>, right: impl Into<String>) -> Self {
        if let Some(join) = self.joins.last_mut() {
            join.on = Some((left.into(), right.into()));
        }
        self
    }

    /// Project recursive step join output to a specific tuple element.
    pub fn result_element_index(mut self, index: usize) -> Self {
        self.result_element_index = Some(index);
        self
    }

    /// Configure a hop from the step query rows to target rows.
    pub fn hop(mut self, table: impl Into<TableName>, via_column: impl Into<String>) -> Self {
        self.hop = Some(RecursiveHopSpec {
            table: table.into(),
            via_column: via_column.into(),
        });
        self
    }

    /// Set maximum recursion depth.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    /// Build the recursive spec.
    pub fn build(self) -> RecursiveSpec {
        RecursiveSpec {
            table: self.table.unwrap_or_else(|| TableName::new("")),
            inner_column: self.inner_column,
            outer_column: self.outer_column,
            select_columns: self.select_columns,
            filters: self.filters,
            joins: self.joins,
            result_element_index: self.result_element_index,
            hop: self.hop,
            max_depth: self.max_depth,
        }
    }
}

impl Default for RecursiveBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
    }

    fn test_descriptor_with_array() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("tags", ColumnType::Array(Box::new(ColumnType::Text))),
        ])
    }

    #[test]
    fn query_builder_simple_eq() {
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();

        assert_eq!(query.table.as_str(), "users");
        assert_eq!(query.disjuncts.len(), 1);
        assert_eq!(query.disjuncts[0].conditions.len(), 1);
        assert!(matches!(
            &query.disjuncts[0].conditions[0],
            Condition::Eq { column, value } if column == "name" && *value == Value::Text("Alice".into())
        ));
    }

    #[test]
    fn query_builder_and_conditions() {
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .filter_ge("score", Value::Integer(50))
            .build();

        assert_eq!(query.disjuncts.len(), 1);
        assert_eq!(query.disjuncts[0].conditions.len(), 2);
    }

    #[test]
    fn query_builder_or_conditions() {
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .or()
            .filter_eq("name", Value::Text("Bob".into()))
            .build();

        assert_eq!(query.disjuncts.len(), 2);
        assert_eq!(query.disjuncts[0].conditions.len(), 1);
        assert_eq!(query.disjuncts[1].conditions.len(), 1);
    }

    #[test]
    fn query_builder_complex() {
        let query = QueryBuilder::new("users")
            .filter_eq("status", Value::Text("active".into()))
            .filter_ge("score", Value::Integer(50))
            .or()
            .filter_eq("role", Value::Text("admin".into()))
            .order_by_desc("score")
            .limit(10)
            .offset(20)
            .build();

        assert_eq!(query.disjuncts.len(), 2);
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].0, "score");
        assert_eq!(query.order_by[0].1, SortDirection::Descending);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, 20);
    }

    #[test]
    fn conjunction_best_index_condition() {
        let mut conj = Conjunction::new();
        conj.add(Condition::Ge {
            column: "score".into(),
            value: Value::Integer(50),
        });
        conj.add(Condition::Eq {
            column: "status".into(),
            value: Value::Text("active".into()),
        });

        // Should prefer Eq over Ge
        let best = conj.best_index_condition().unwrap();
        assert!(matches!(best, Condition::Eq { column, .. } if column == "status"));
    }

    #[test]
    fn query_to_predicate() {
        let descriptor = test_descriptor();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(100))
            .build();

        let predicate = query.to_predicate(&descriptor);
        assert!(matches!(predicate, Predicate::Eq { col_index: 2, .. }));
    }

    #[test]
    fn query_to_predicate_or() {
        let descriptor = test_descriptor();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(50))
            .or()
            .filter_eq("score", Value::Integer(100))
            .build();

        let predicate = query.to_predicate(&descriptor);
        assert!(matches!(predicate, Predicate::Or(_)));
    }

    #[test]
    fn query_to_predicate_contains() {
        let descriptor = test_descriptor_with_array();
        let query = QueryBuilder::new("users")
            .filter_contains("tags", Value::Text("rust".into()))
            .build();

        let predicate = query.to_predicate(&descriptor);
        assert!(matches!(
            predicate,
            Predicate::Contains { col_index: 1, .. }
        ));
    }

    #[test]
    fn query_sort_keys() {
        let descriptor = test_descriptor();
        let query = QueryBuilder::new("users")
            .order_by("name")
            .order_by_desc("score")
            .build();

        let keys = query.sort_keys(&descriptor);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].col_index, 1); // name
        assert_eq!(keys[0].direction, SortDirection::Ascending);
        assert_eq!(keys[1].col_index, 2); // score
        assert_eq!(keys[1].direction, SortDirection::Descending);
    }

    #[test]
    fn query_alias() {
        let query = QueryBuilder::new("users").alias("u1").build();

        assert_eq!(query.table.as_str(), "users");
        assert_eq!(query.alias, Some("u1".to_string()));
        assert_eq!(query.effective_name(), "u1");
    }

    #[test]
    fn query_effective_name_without_alias() {
        let query = QueryBuilder::new("users").build();

        assert_eq!(query.alias, None);
        assert_eq!(query.effective_name(), "users");
    }

    #[test]
    fn query_select_columns() {
        let query = QueryBuilder::new("users")
            .select(&["name", "score"])
            .build();

        assert_eq!(
            query.select_columns,
            Some(vec!["name".to_string(), "score".to_string()])
        );
    }

    #[test]
    fn query_select_all_by_default() {
        let query = QueryBuilder::new("users").build();

        assert_eq!(query.select_columns, None);
    }

    #[test]
    fn query_simple_join() {
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("users.id", "posts.author_id")
            .build();

        assert!(query.is_join());
        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.joins[0].table.as_str(), "posts");
        assert_eq!(
            query.joins[0].on,
            Some(("users.id".to_string(), "posts.author_id".to_string()))
        );
    }

    #[test]
    fn query_join_with_aliases() {
        let query = QueryBuilder::new("users")
            .alias("u")
            .join("posts")
            .alias("p")
            .on("u.id", "p.author_id")
            .build();

        assert_eq!(query.alias, Some("u".to_string()));
        assert_eq!(query.effective_name(), "u");

        assert_eq!(query.joins[0].alias, Some("p".to_string()));
        assert_eq!(query.joins[0].effective_name(), "p");
    }

    #[test]
    fn query_self_join() {
        let query = QueryBuilder::new("employees")
            .alias("e")
            .join("employees")
            .alias("m")
            .on("e.manager_id", "m.id")
            .build();

        assert_eq!(query.table.as_str(), "employees");
        assert_eq!(query.alias, Some("e".to_string()));

        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.joins[0].table.as_str(), "employees");
        assert_eq!(query.joins[0].alias, Some("m".to_string()));
    }

    #[test]
    fn query_multiple_joins() {
        let query = QueryBuilder::new("orders")
            .join("customers")
            .on("orders.customer_id", "customers.id")
            .join("products")
            .on("orders.product_id", "products.id")
            .build();

        assert_eq!(query.joins.len(), 2);
        assert_eq!(query.joins[0].table.as_str(), "customers");
        assert_eq!(query.joins[1].table.as_str(), "products");
    }

    #[test]
    fn query_no_join_by_default() {
        let query = QueryBuilder::new("users").build();

        assert!(!query.is_join());
        assert!(query.joins.is_empty());
    }

    // ========================================================================
    // Array subquery tests
    // ========================================================================

    #[test]
    fn query_with_array_subquery() {
        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .select(&["id", "title"])
            })
            .build();

        assert!(query.has_array_subqueries());
        assert_eq!(query.array_subqueries.len(), 1);
        assert_eq!(query.array_subqueries[0].column_name, "posts");
        assert_eq!(query.array_subqueries[0].table.as_str(), "posts");
        assert_eq!(query.array_subqueries[0].inner_column, "author_id");
        assert_eq!(query.array_subqueries[0].outer_column, "users.id");
    }

    #[test]
    fn query_with_array_subquery_filters_and_order() {
        let query = QueryBuilder::new("users")
            .with_array("recent_posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .filter_eq("published", Value::Boolean(true))
                    .order_by_desc("created_at")
                    .limit(5)
            })
            .build();

        let subquery = &query.array_subqueries[0];
        assert_eq!(subquery.filters.len(), 1);
        assert_eq!(subquery.limit, Some(5));
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.order_by[0].0, "created_at");
    }

    #[test]
    fn query_with_nested_array_subquery() {
        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .with_array("comments", |sub2| {
                        sub2.from("comments").correlate("post_id", "posts.id")
                    })
            })
            .build();

        assert_eq!(query.array_subqueries.len(), 1);
        let posts_subquery = &query.array_subqueries[0];
        assert_eq!(posts_subquery.nested_arrays.len(), 1);
        assert_eq!(posts_subquery.nested_arrays[0].column_name, "comments");
        assert_eq!(posts_subquery.nested_arrays[0].table.as_str(), "comments");
    }

    #[test]
    fn query_without_array_subqueries() {
        let query = QueryBuilder::new("users").build();

        assert!(!query.has_array_subqueries());
        assert!(query.array_subqueries.is_empty());
    }

    #[test]
    fn query_multiple_array_subqueries() {
        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .with_array("comments", |sub| {
                sub.from("comments").correlate("user_id", "users.id")
            })
            .build();

        assert_eq!(query.array_subqueries.len(), 2);
        assert_eq!(query.array_subqueries[0].column_name, "posts");
        assert_eq!(query.array_subqueries[1].column_name, "comments");
    }

    // ========================================================================
    // Recursive relation tests
    // ========================================================================

    #[test]
    fn query_with_recursive_spec() {
        let query = QueryBuilder::new("teams")
            .select(&["team_id"])
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "team_id")
                    .select(&["parent_team"])
                    .max_depth(12)
            })
            .build();

        assert!(query.has_recursive());
        let recursive = query.recursive.as_ref().expect("recursive spec");
        assert_eq!(recursive.table.as_str(), "team_edges");
        assert_eq!(recursive.inner_column, "child_team");
        assert_eq!(recursive.outer_column, "team_id");
        assert_eq!(
            recursive.select_columns,
            Some(vec!["parent_team".to_string()])
        );
        assert!(recursive.filters.is_empty());
        assert!(recursive.joins.is_empty());
        assert!(recursive.result_element_index.is_none());
        assert!(recursive.hop.is_none());
        assert_eq!(recursive.max_depth, 12);
    }

    #[test]
    fn query_without_recursive_by_default() {
        let query = QueryBuilder::new("teams").build();
        assert!(!query.has_recursive());
        assert!(query.recursive.is_none());
    }

    // ========================================================================
    // Branch tests
    // ========================================================================

    #[test]
    fn query_builder_single_branch() {
        let query = QueryBuilder::new("users").branch("draft").build();

        assert_eq!(query.branches, vec!["draft".to_string()]);
        assert!(!query.is_multi_branch());
    }

    #[test]
    fn query_builder_multiple_branches() {
        let query = QueryBuilder::new("users")
            .branches(&["main", "draft"])
            .build();

        assert_eq!(
            query.branches,
            vec!["main".to_string(), "draft".to_string()]
        );
        assert!(query.is_multi_branch());
    }

    #[test]
    fn query_builder_no_default_branch() {
        // Without calling .branch(), branches is empty
        let query = QueryBuilder::new("users").build();

        assert!(query.branches.is_empty());
        assert!(!query.is_multi_branch());
    }

    #[test]
    fn query_builder_branch_overrides_previous() {
        // Calling .branch() multiple times should override
        let query = QueryBuilder::new("users")
            .branch("draft")
            .branch("staging")
            .build();

        assert_eq!(query.branches, vec!["staging".to_string()]);
    }

    #[test]
    fn query_builder_branches_overrides_branch() {
        // Calling .branches() after .branch() should override
        let query = QueryBuilder::new("users")
            .branch("draft")
            .branches(&["main", "staging"])
            .build();

        assert_eq!(
            query.branches,
            vec!["main".to_string(), "staging".to_string()]
        );
    }

    #[test]
    fn query_new_has_no_default_branch() {
        // Query::new() does not set a default branch
        let query = Query::new("users");

        assert!(query.branches.is_empty());
    }

    // ========================================================================
    // Serialization tests
    // ========================================================================

    #[test]
    fn query_round_trip_serialization() {
        let query = QueryBuilder::new("users")
            .filter_eq("org_id", Value::Integer(42))
            .filter_ge("score", Value::Integer(50))
            .branch("main")
            .order_by_desc("score")
            .limit(10)
            .build();

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(query, decoded);
    }

    #[test]
    fn query_with_join_serialization() {
        let query = QueryBuilder::new("users")
            .alias("u")
            .join("posts")
            .alias("p")
            .on("u.id", "p.author_id")
            .branch("main")
            .build();

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(query, decoded);
    }

    #[test]
    fn query_with_array_subquery_serialization() {
        let query = QueryBuilder::new("orgs")
            .branch("main")
            .with_array("users", |b| b.from("users").correlate("id", "org_id"))
            .build();

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(query, decoded);
    }

    #[test]
    fn query_with_recursive_serialization() {
        let query = QueryBuilder::new("teams")
            .select(&["team_id"])
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "team_id")
                    .select(&["parent_team"])
                    .max_depth(10)
            })
            .branch("main")
            .build();

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(query, decoded);
    }

    #[test]
    fn query_with_relation_ir_serialization() {
        let mut query = QueryBuilder::new("users").branch("main").build();
        query.relation_ir = Some(crate::query_manager::relation_ir::RelExpr::TableScan {
            table: TableName::new("users"),
        });

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert!(decoded.has_relation_ir());
        assert_eq!(query, decoded);
    }

    #[test]
    fn query_disjunction_serialization() {
        let query = QueryBuilder::new("users")
            .filter_eq("status", Value::Text("active".into()))
            .or()
            .filter_eq("role", Value::Text("admin".into()))
            .branch("main")
            .build();

        let json = serde_json::to_string(&query).expect("serialize");
        let decoded: Query = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(query, decoded);
        assert_eq!(decoded.disjuncts.len(), 2);
    }
}
