use super::*;

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
        match read.op.as_str() {
            "eq" => self.subscribe_rows_where_eq(&read.table, &read.field, read.value.clone()),
            "ne" => self.subscribe_rows_where_ne(&read.table, &read.field, read.value.clone()),
            "contains" => {
                let Some(needle) = read.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.subscribe_rows_where_contains(&read.table, &read.field, needle)
            }
            "in" => {
                let Some(values) = read.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.subscribe_rows_where_in(&read.table, &read.field, values.clone())
            }
            "recursive_refs" => {
                let Some(root_id) = read.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                Ok(RowsSubscription::where_recursive_refs(
                    &read.table,
                    root_id,
                    &read.field,
                    self.read_recursive_refs(&read.table, root_id, &read.field)?,
                ))
            }
            "eq_top_created_at_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
                self.subscribe_rows_where_eq_top_created_at_desc(
                    &read.table,
                    &read.field,
                    value.clone(),
                    limit as usize,
                )
            }
            "eq_top_field_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
                let order_field = read
                    .value
                    .get("order_field")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
                self.subscribe_rows_where_eq_top_field_desc(
                    &read.table,
                    &read.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                )
            }
            "query" => self.subscribe_query(built_query_from_read(read)?),
            op => Err(crate::Error::new(format!(
                "unsupported observed subscription query {op}"
            ))),
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
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq" => {
                self.query(predicate_query(
                    &query.table,
                    &query.field,
                    QueryConditionOp::Eq,
                    query.value.clone(),
                ))?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "ne" => {
                self.read_rows_where_ne(&query.table, &query.field, query.value.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "contains" => {
                let Some(needle) = query.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.read_rows_where_contains(&query.table, &query.field, needle)?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "in" => {
                let Some(values) = query.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.read_rows_where_in(&query.table, &query.field, values.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "recursive_refs" => {
                let Some(root_id) = query.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                self.read_recursive_refs(&query.table, root_id, &query.field)?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq_top_created_at_desc" => {
                let value = query
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
                let limit = query
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
                self.read_rows_where_eq_top_created_at_desc(
                    &query.table,
                    &query.field,
                    value.clone(),
                    limit as usize,
                )?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq_top_field_desc" => {
                let value = query
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
                let order_field = query
                    .value
                    .get("order_field")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
                let limit = query
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
                self.read_rows_where_eq_top_field_desc(
                    &query.table,
                    &query.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                )?
            }
            RowsSubscriptionQuery::Predicate(query) => {
                return Err(crate::Error::new(format!(
                    "unsupported subscription query {}",
                    query.op
                )));
            }
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
