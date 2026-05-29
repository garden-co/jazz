use super::Runtime;
use crate::observed_query::{self, ObservedQuery};
use crate::query_api::{
    predicate_query, BuiltQuery, QueryCondition, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use crate::subscription::{RejectionSubscription, RowsSubscription, RowsSubscriptionQuery};
use crate::sync::QueryReadRecord;
use crate::types::{ReadTier, RejectionInfo, RowView};
use crate::{branch, Result};
use serde_json::Value as JsonValue;

impl Runtime {
    pub fn subscribe_rows(&self, table_name: &str) -> Result<RowsSubscription> {
        Ok(RowsSubscription::new(
            table_name,
            self.read_rows(table_name)?,
        ))
    }

    pub fn subscribe_rows_at_tier(
        &self,
        table_name: &str,
        tier: ReadTier,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::new_at_tier(
            table_name,
            tier,
            self.read_rows_at_tier(table_name, tier)?,
        ))
    }

    pub fn subscribe_rejections(&self) -> Result<RejectionSubscription> {
        Ok(RejectionSubscription::new(self.rejected_transactions()?))
    }

    pub fn subscribe_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value,
        ))
    }

    pub fn subscribe_rows_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Contains,
            JsonValue::String(needle.to_owned()),
        ))
    }

    pub fn subscribe_rows_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::In,
            JsonValue::Array(values),
        ))
    }

    pub fn subscribe_rows_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Ne,
            value,
        ))
    }

    pub fn subscribe_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(BuiltQuery {
            table: table_name.to_owned(),
            conditions: vec![QueryCondition {
                column: field_name.to_owned(),
                op: QueryConditionOp::Eq,
                value,
            }],
            order_by: vec![QueryOrderBy {
                column: "$createdAt".to_owned(),
                direction: QueryDirection::Desc,
            }],
            limit: Some(limit),
            offset: None,
        })
    }

    pub fn subscribe_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<RowsSubscription> {
        self.subscribe_query(BuiltQuery {
            table: table_name.to_owned(),
            conditions: vec![QueryCondition {
                column: field_name.to_owned(),
                op: QueryConditionOp::Eq,
                value,
            }],
            order_by: vec![QueryOrderBy {
                column: order_field_name.to_owned(),
                direction: QueryDirection::Desc,
            }],
            limit: Some(limit),
            offset: None,
        })
    }

    pub fn subscribe_observed_query(&self, read: &QueryReadRecord) -> Result<RowsSubscription> {
        if read.branch_id != branch::id_for_num(&self.conn, self.branch_num)? {
            return Err(crate::Error::new(
                "observed query branch is not checked out",
            ));
        }
        match observed_query::decode(read)? {
            ObservedQuery::RecursiveRefs { root_id } => Ok(RowsSubscription::where_recursive_refs(
                &read.table,
                &root_id,
                &read.field,
                self.read_recursive_refs(&read.table, &root_id, &read.field)?,
            )),
            ObservedQuery::Built { query, .. } => self.subscribe_query(query),
            ObservedQuery::Absent => Err(crate::Error::new(
                "absent query reads cannot be subscribed directly",
            )),
        }
    }

    pub fn poll_subscription(
        &self,
        subscription: &mut RowsSubscription,
    ) -> Result<Vec<crate::types::RowDiff>> {
        let next_rows = self.subscription_rows(subscription)?;
        Ok(subscription.replace_with_diff(next_rows))
    }

    pub(crate) fn subscription_rows(
        &self,
        subscription: &RowsSubscription,
    ) -> Result<Vec<RowView>> {
        Ok(match &subscription.query {
            RowsSubscriptionQuery::Table { table, tier } => self.read_rows_at_tier(table, *tier)?,
            RowsSubscriptionQuery::RecursiveRefs {
                table,
                root_id,
                parent_field,
            } => self.read_recursive_refs(table, root_id, parent_field)?,
            RowsSubscriptionQuery::Built { query, tier } => {
                self.query_at_tier(query.clone(), *tier)?
            }
        })
    }

    pub fn poll_rejections(
        &self,
        subscription: &mut RejectionSubscription,
    ) -> Result<Vec<RejectionInfo>> {
        Ok(subscription.replace_with_new(self.rejected_transactions()?))
    }
}
