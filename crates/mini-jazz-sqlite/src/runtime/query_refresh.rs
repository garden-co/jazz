use super::*;

impl Runtime {
    pub fn observed_query_reads(&self) -> Result<Vec<QueryReadRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_id, table_name, field_name, op, value_json
             FROM jazz_query_read
             ORDER BY branch_id, table_name, field_name, op, value_json",
        )?;
        let rows = stmt.query_map([], |row| {
            let value_json: String = row.get(4)?;
            let value = serde_json::from_str(&value_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            Ok(QueryReadRecord {
                branch_id: row.get(0)?,
                table: row.get(1)?,
                field: row.get(2)?,
                op: row.get(3)?,
                value,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn export_observed_query_refreshes(&self) -> Result<Vec<Bundle>> {
        let reads = self.observed_query_reads()?;
        self.export_query_read_refreshes(&reads)
    }

    pub fn export_query_read_refreshes(&self, reads: &[QueryReadRecord]) -> Result<Vec<Bundle>> {
        let current_branch_id = branch::id_for_num(&self.conn, self.branch_num)?;
        let mut bundles = Vec::new();

        for plan in crate::query_refresh::plan_refreshes(&current_branch_id, reads)? {
            match plan {
                QueryRefreshPlan::Predicate {
                    table,
                    field,
                    op,
                    values,
                } => bundles
                    .push(self.export_many_predicate_query_refreshes(&table, &field, &op, values)?),
                QueryRefreshPlan::RecursiveRefs {
                    table,
                    field,
                    root_ids,
                } => bundles.push(self.export_many_recursive_refs(&table, &field, root_ids)?),
                QueryRefreshPlan::TopCreatedAt {
                    table,
                    field,
                    values,
                    limit,
                } => bundles.push(
                    self.export_many_query_where_eq_top_created_at_desc_with_previous_observed(
                        &table, &field, values, limit,
                    )?,
                ),
                QueryRefreshPlan::TopField {
                    table,
                    field,
                    values,
                    order_field,
                    limit,
                } => bundles.push(
                    self.export_many_query_where_eq_top_field_desc_with_previous_observed(
                        &table,
                        &field,
                        values,
                        &order_field,
                        limit,
                    )?,
                ),
                QueryRefreshPlan::Single(read) => {
                    bundles.push(self.export_query_read_refresh(&read)?);
                }
            }
        }
        Ok(bundles)
    }

    pub fn forget_observed_query_read(&mut self, read: &QueryReadRecord) -> Result<()> {
        self.conn.execute(
            "DELETE FROM jazz_query_read
             WHERE branch_id = ?
               AND table_name = ?
               AND field_name = ?
               AND op = ?
               AND value_json = ?",
            params![
                read.branch_id,
                read.table,
                read.field,
                read.op,
                serde_json::to_string(&read.value)
                    .map_err(|err| crate::Error::new(err.to_string()))?
            ],
        )?;
        Ok(())
    }

    fn export_query_read_refresh(&self, read: &QueryReadRecord) -> Result<Bundle> {
        if read.branch_id != branch::id_for_num(&self.conn, self.branch_num)? {
            return Err(crate::Error::new("query refresh branch is not checked out"));
        }
        match read.op.as_str() {
            "eq" => self.export_query_where_eq(&read.table, &read.field, read.value.clone()),
            "ne" => self.export_query_where_ne(&read.table, &read.field, read.value.clone()),
            "contains" => {
                let Some(needle) = read.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.export_query_where_contains(&read.table, &read.field, needle)
            }
            "in" => {
                let Some(values) = read.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.export_query_where_in(&read.table, &read.field, values.clone())
            }
            "recursive_refs" => {
                let Some(root_id) = read.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                self.export_recursive_refs(&read.table, root_id, &read.field)
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
                self.export_query_where_eq_top_created_at_desc_with_previous_observed(
                    &read.table,
                    &read.field,
                    value.clone(),
                    limit as usize,
                    observed_ids_from_query_value(&read.value)?,
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
                self.export_query_where_eq_top_field_desc_with_previous_observed(
                    &read.table,
                    &read.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                    observed_ids_from_query_value(&read.value)?,
                )
            }
            "query" => {
                let query = built_query_from_read(read)?;
                let rows = self.query(support_window_query(&query)?)?;
                self.export_built_query_scope_with_previous_observed(
                    query,
                    rows,
                    &[],
                    observed_ids_from_query_value(&read.value)?,
                )
            }
            "absent" => {
                if read.field == "id" {
                    let Some(row_id) = read.value.as_str() else {
                        return Err(crate::Error::new("absent id expects string value"));
                    };
                    if self
                        .query(predicate_query(
                            &read.table,
                            &read.field,
                            QueryConditionOp::Eq,
                            JsonValue::String(row_id.to_owned()),
                        ))?
                        .is_empty()
                    {
                        let mut branches = Vec::new();
                        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
                        let query_reads = vec![read.clone()];
                        return Ok(make_bundle(
                            &self.schema,
                            branches,
                            export_txs(&self.conn)?,
                            Vec::new(),
                            query_reads,
                            Vec::new(),
                        ));
                    }
                    return self.export_query_where_eq(
                        &read.table,
                        &read.field,
                        JsonValue::String(row_id.to_owned()),
                    );
                }
                let query_reads = vec![read.clone()];
                Ok(make_bundle(
                    &self.schema,
                    Vec::new(),
                    export_txs(&self.conn)?,
                    Vec::new(),
                    query_reads,
                    Vec::new(),
                ))
            }
            op => Err(crate::Error::new(format!(
                "unsupported observed query refresh {op}"
            ))),
        }
    }
}
