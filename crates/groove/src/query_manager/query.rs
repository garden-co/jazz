use crate::query_manager::encoding::encode_value;
use crate::query_manager::graph_nodes::filter::Predicate;
use crate::query_manager::graph_nodes::sort::{SortDirection, SortKey};
use crate::query_manager::types::{RowDescriptor, TableName, Value};

/// A join specification.
#[derive(Debug, Clone)]
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
        self.alias.as_deref().unwrap_or(&self.table.0)
    }
}

/// A condition in a WHERE clause.
#[derive(Debug, Clone)]
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

        Some(match self {
            Condition::Eq { value, .. } => Predicate::Eq {
                col_index,
                value: encode_value(value),
            },
            Condition::Ne { value, .. } => Predicate::Ne {
                col_index,
                value: encode_value(value),
            },
            Condition::Lt { value, .. } => Predicate::Lt {
                col_index,
                value: encode_value(value),
            },
            Condition::Le { value, .. } => Predicate::Le {
                col_index,
                value: encode_value(value),
            },
            Condition::Gt { value, .. } => Predicate::Gt {
                col_index,
                value: encode_value(value),
            },
            Condition::Ge { value, .. } => Predicate::Ge {
                col_index,
                value: encode_value(value),
            },
            Condition::Between { min, max, .. } => Predicate::And(vec![
                Predicate::Ge {
                    col_index,
                    value: encode_value(min),
                },
                Predicate::Le {
                    col_index,
                    value: encode_value(max),
                },
            ]),
            Condition::IsNull { .. } => Predicate::IsNull { col_index },
            Condition::IsNotNull { .. } => Predicate::IsNotNull { col_index },
        })
    }
}

/// A conjunction (AND group) of conditions.
#[derive(Debug, Clone, Default)]
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

/// A query specification (DNF: disjunction of conjunctions).
#[derive(Debug, Clone)]
pub struct Query {
    pub table: TableName,
    /// Optional table alias (for self-joins).
    pub alias: Option<String>,
    /// Joined tables.
    pub joins: Vec<JoinSpec>,
    /// OR groups (disjunction of conjunctions).
    pub disjuncts: Vec<Conjunction>,
    /// Order by specification.
    pub order_by: Vec<(String, SortDirection)>,
    /// Limit.
    pub limit: Option<usize>,
    /// Offset.
    pub offset: usize,
    /// If true, also scan _id_deleted to include soft-deleted rows.
    pub include_deleted: bool,
    /// Columns to select (None = all columns).
    pub select_columns: Option<Vec<String>>,
}

impl Query {
    /// Create a new query for a table.
    pub fn new(table: impl Into<TableName>) -> Self {
        Self {
            table: table.into(),
            alias: None,
            joins: Vec::new(),
            disjuncts: vec![Conjunction::new()],
            order_by: Vec::new(),
            limit: None,
            offset: 0,
            include_deleted: false,
            select_columns: None,
        }
    }

    /// Check if this is a join query.
    pub fn is_join(&self) -> bool {
        !self.joins.is_empty()
    }

    /// Get the effective table name (alias if set, otherwise table name).
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table.0)
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
    pub fn new(table: impl Into<TableName>) -> Self {
        Self {
            query: Query::new(table),
        }
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

    /// Build the query.
    pub fn build(self) -> Query {
        self.query
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

    #[test]
    fn query_builder_simple_eq() {
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .build();

        assert_eq!(query.table.0, "users");
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

        assert_eq!(query.table.0, "users");
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
        assert_eq!(query.joins[0].table.0, "posts");
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

        assert_eq!(query.table.0, "employees");
        assert_eq!(query.alias, Some("e".to_string()));

        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.joins[0].table.0, "employees");
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
        assert_eq!(query.joins[0].table.0, "customers");
        assert_eq!(query.joins[1].table.0, "products");
    }

    #[test]
    fn query_no_join_by_default() {
        let query = QueryBuilder::new("users").build();

        assert!(!query.is_join());
        assert!(query.joins.is_empty());
    }
}
