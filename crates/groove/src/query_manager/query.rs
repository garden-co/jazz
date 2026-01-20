use crate::query_manager::encoding::encode_value;
use crate::query_manager::graph_nodes::filter::Predicate;
use crate::query_manager::graph_nodes::sort::{SortDirection, SortKey};
use crate::query_manager::types::{RowDescriptor, TableName, Value};

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
    /// OR groups (disjunction of conjunctions).
    pub disjuncts: Vec<Conjunction>,
    /// Order by specification.
    pub order_by: Vec<(String, SortDirection)>,
    /// Limit.
    pub limit: Option<usize>,
    /// Offset.
    pub offset: usize,
}

impl Query {
    /// Create a new query for a table.
    pub fn new(table: impl Into<TableName>) -> Self {
        Self {
            table: table.into(),
            disjuncts: vec![Conjunction::new()],
            order_by: Vec::new(),
            limit: None,
            offset: 0,
        }
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
}
