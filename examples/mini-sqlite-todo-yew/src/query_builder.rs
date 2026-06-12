use mini_jazz_sqlite::{
    BuiltQuery, QueryCondition, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use serde_json::Value;

pub struct QueryBuilder {
    query: BuiltQuery,
}

impl QueryBuilder {
    pub fn table(table: impl Into<String>) -> Self {
        Self {
            query: BuiltQuery {
                table: table.into(),
                conditions: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            },
        }
    }

    pub fn eq(mut self, column: impl Into<String>, value: Value) -> Self {
        self.query.conditions.push(QueryCondition {
            column: column.into(),
            op: QueryConditionOp::Eq,
            value,
        });
        self
    }

    pub fn ne(mut self, column: impl Into<String>, value: Value) -> Self {
        self.query.conditions.push(QueryCondition {
            column: column.into(),
            op: QueryConditionOp::Ne,
            value,
        });
        self
    }

    pub fn contains(mut self, column: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.conditions.push(QueryCondition {
            column: column.into(),
            op: QueryConditionOp::Contains,
            value: Value::String(value.into()),
        });
        self
    }

    pub fn in_values(mut self, column: impl Into<String>, value: Value) -> Self {
        self.query.conditions.push(QueryCondition {
            column: column.into(),
            op: QueryConditionOp::In,
            value,
        });
        self
    }

    pub fn order_by(mut self, column: impl Into<String>, direction: QueryDirection) -> Self {
        self.query.order_by.push(QueryOrderBy {
            column: column.into(),
            direction,
        });
        self
    }

    pub fn order_by_asc(self, column: impl Into<String>) -> Self {
        self.order_by(column, QueryDirection::Asc)
    }

    pub fn order_by_desc(self, column: impl Into<String>) -> Self {
        self.order_by(column, QueryDirection::Desc)
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = Some(offset);
        self
    }

    pub fn build(self) -> BuiltQuery {
        self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builds_query_with_filters_order_and_limit() {
        let query = QueryBuilder::table("todos")
            .eq("done", json!(false))
            .order_by_desc("$createdAt")
            .limit(10)
            .build();

        assert_eq!(query.table, "todos");
        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "done");
        assert_eq!(query.conditions[0].op, QueryConditionOp::Eq);
        assert_eq!(query.conditions[0].value, json!(false));
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].column, "$createdAt");
        assert_eq!(query.order_by[0].direction, QueryDirection::Desc);
        assert_eq!(query.limit, Some(10));
    }
}
