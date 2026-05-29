use super::*;

struct BatchedQueryScopeItem {
    op: String,
    value: JsonValue,
    rows: Vec<RowView>,
    extra_row_ids: Vec<String>,
}

impl Runtime {
    pub fn export_query_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        let rows = self.query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value.clone(),
        ))?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value,
            rows,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_eq_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value.clone(),
        ))?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value,
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    pub fn export_query_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "contains",
            JsonValue::String(needle.to_owned()),
            self.read_rows_where_contains(table_name, field_name, needle)?,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "in",
            JsonValue::Array(values.clone()),
            self.read_rows_where_in(table_name, field_name, values)?,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "ne",
            value.clone(),
            self.read_rows_where_ne(table_name, field_name, value)?,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query_where_eq_top_created_at_desc_with_previous_observed(
            table_name,
            field_name,
            value,
            limit,
            Vec::new(),
        )
    }

    pub fn export_query_where_eq_top_created_at_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    pub fn export_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query_where_eq_top_field_desc_with_previous_observed(
            table_name,
            field_name,
            value,
            order_field_name,
            limit,
            Vec::new(),
        )
    }

    pub fn export_query_where_eq_top_field_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_field_desc",
            json!({
                "eq": value.clone(),
                "order_field": order_field_name,
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    pub fn export_many_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values
                .into_iter()
                .map(|value| (value, Vec::new()))
                .collect(),
            order_field_name,
            limit,
            &[],
        )
    }

    pub fn export_many_query_where_eq_top_field_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
        order_field_name: &str,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values
                .into_iter()
                .map(|value| (value, Vec::new()))
                .collect(),
            order_field_name,
            limit,
            &[ref_field_name],
        )
    }

    pub fn profile_export_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<(Bundle, QueryExportProfile)> {
        let total_started = ProfileTimer::start();
        let read_started = ProfileTimer::start();
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        let read_rows_ms = read_started.elapsed_ms();

        let table = self.schema.table_def(table_name)?;

        let resolve_started = ProfileTimer::start();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let resolve_visible_row_nums_ms = resolve_started.elapsed_ms();

        let repair_started = ProfileTimer::start();
        let query_value = json!({
            "eq": value.clone(),
            "order_field": order_field_name,
            "limit": limit,
            "observed_ids": observed_row_ids(&rows),
        });
        let mut repair_row_nums = query_scope_repair_row_nums(
            &self.conn,
            table,
            field_name,
            "eq_top_field_desc",
            &query_value,
        )?;
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let repair_row_nums_ms = repair_started.elapsed_ms();

        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();

        let visible_history_started = ProfileTimer::start();
        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        let visible_history_ms = visible_history_started.elapsed_ms();

        let repair_visible_started = ProfileTimer::start();
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
        }
        let repair_visible_history_ms = repair_visible_started.elapsed_ms();

        let repair_all_started = ProfileTimer::start();
        if !repair_row_nums.is_empty() {
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        let repair_all_history_ms = repair_all_started.elapsed_ms();

        let policy_started = ProfileTimer::start();
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        let policy_dependency_history_ms = policy_started.elapsed_ms();

        let snapshot_started = ProfileTimer::start();
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        let branch_snapshot_history_ms = snapshot_started.elapsed_ms();

        let dedupe_started = ProfileTimer::start();
        dedupe_history_records(&mut history);
        let dedupe_history_ms = dedupe_started.elapsed_ms();

        let reads_started = ProfileTimer::start();
        let reads = export_reads_for_history(&self.conn, &history)?;
        let reads_ms = reads_started.elapsed_ms();

        let rejected_started = ProfileTimer::start();
        let rejected_tx_ids = query_scope_rejected_tx_ids(
            &self.conn,
            table,
            field_name,
            "eq_top_field_desc",
            &query_value,
        )?;
        let rejected_tx_ids_ms = rejected_started.elapsed_ms();

        let txs_started = ProfileTimer::start();
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let txs_ms = txs_started.elapsed_ms();

        let branches_started = ProfileTimer::start();
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let branches_ms = branches_started.elapsed_ms();

        let make_started = ProfileTimer::start();
        let query_reads = vec![QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: "eq_top_field_desc".to_owned(),
            value: query_value,
        }];
        let bundle = make_bundle(&self.schema, branches, txs, reads, query_reads, history);
        let make_bundle_ms = make_started.elapsed_ms();

        let profile = QueryExportProfile {
            total_ms: total_started.elapsed_ms(),
            read_rows_ms,
            resolve_visible_row_nums_ms,
            repair_row_nums_ms,
            visible_history_ms,
            repair_visible_history_ms,
            repair_all_history_ms,
            policy_dependency_history_ms,
            branch_snapshot_history_ms,
            dedupe_history_ms,
            reads_ms,
            rejected_tx_ids_ms,
            txs_ms,
            branches_ms,
            make_bundle_ms,
            history_rows: bundle.history.len(),
            read_rows: bundle.reads.len(),
            tx_rows: bundle.txs.len(),
            branch_rows: bundle.branches.len(),
        };
        Ok((bundle, profile))
    }
    pub(super) fn export_many_predicate_query_refreshes(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        values: Vec<(JsonValue, Vec<String>)>,
    ) -> Result<Bundle> {
        let mut items = Vec::new();
        for (value, extra_row_ids) in values {
            let rows = match op {
                "eq" => self.read_rows_where_eq(table_name, field_name, value.clone())?,
                "ne" => self.read_rows_where_ne(table_name, field_name, value.clone())?,
                "contains" => {
                    let Some(needle) = value.as_str() else {
                        return Err(crate::Error::new("contains expects a string value"));
                    };
                    self.read_rows_where_contains(table_name, field_name, needle)?
                }
                "in" => {
                    let Some(values) = value.as_array() else {
                        return Err(crate::Error::new("in predicate expects an array value"));
                    };
                    self.read_rows_where_in(table_name, field_name, values.clone())?
                }
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported batched predicate refresh {op}"
                    )));
                }
            };
            items.push(BatchedQueryScopeItem {
                op: op.to_owned(),
                value,
                rows,
                extra_row_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    pub(super) fn export_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub(super) fn export_many_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        limit: usize,
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_created_at_desc(
                table_name,
                field_name,
                &value_only,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_created_at_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    pub(super) fn export_many_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values,
            order_field_name,
            limit,
            &[],
        )
    }

    pub(super) fn export_many_query_where_eq_top_field_desc_inner(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_field_desc(
                table_name,
                field_name,
                &value_only,
                order_field_name,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_field_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "order_field": order_field_name,
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, ref_include_fields)
    }

    pub(crate) fn export_built_query_scope(
        &self,
        query: BuiltQuery,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        self.export_built_query_scope_with_previous_observed(
            query,
            rows,
            ref_include_fields,
            Vec::new(),
        )
    }

    pub(super) fn export_built_query_scope_with_previous_observed(
        &self,
        query: BuiltQuery,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: query.table.clone(),
            field: "$query".to_owned(),
            op: "query".to_owned(),
            value: built_query_read_value(&query, &rows),
        };
        let support_query = support_window_query(&query)?;
        if let Some(row_scope) = self
            .query_context()
            .lower_built_query_row_scope(&support_query)?
        {
            return self.export_built_query_read_scope_sql(
                query_read,
                &query,
                &row_scope,
                rows,
                QueryScopeOptions {
                    ref_include_fields,
                    extra_row_ids: &previous_observed_ids,
                },
            );
        }
        self.export_query_read_scope(
            query_read,
            rows,
            QueryScopeOptions {
                ref_include_fields,
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub(super) fn export_query_read_scope(
        &self,
        query_read: QueryReadRecord,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        // Query-scope exports carry more than the rows currently visible in the
        // query result. They also carry repair candidates: rows whose history
        // previously satisfied the same query scope and may need to be removed
        // or updated on a receiver.
        //
        //   +---------------------+
        //   | query result rows   |
        //   +---------------------+
        //             |
        //             v
        //   +---------------------+      +---------------------+
        //   | result row nums     | ---> | exported history    |
        //   +---------------------+      +---------------------+
        //             ^
        //             |
        //   +---------------------+
        //   | repair row nums     |
        //   +---------------------+
        //
        // Without the repair rows, query-scoped sync would only add or update
        // rows that are still in the result. A receiver would not learn that a
        // previously synced row left the predicate or page boundary.
        let table_name = &query_read.table;
        let table = self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let mut repair_row_nums = Vec::new();
        for row_id in options.extra_row_ids {
            repair_row_nums.push(row_num(&self.conn, row_id)?);
        }
        repair_row_nums.extend(query_scope_repair_row_nums_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?);
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in options.ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let rejected_tx_ids = query_scope_rejected_tx_ids_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![query_read];
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    pub(super) fn export_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_field_desc",
            json!({
                "eq": value.clone(),
                "order_field": order_field_name,
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub(crate) fn export_query_scope(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        value: JsonValue,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: op.to_owned(),
            value,
        };
        self.export_query_read_scope(query_read, rows, options)
    }

    fn export_batched_query_scopes(
        &self,
        table_name: &str,
        field_name: &str,
        items: Vec<BatchedQueryScopeItem>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let table = self.schema.table_def(table_name)?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut all_rows = Vec::new();
        let mut visible_row_nums = Vec::new();
        let mut repair_row_nums = Vec::new();
        let mut rejected_tx_ids = Vec::new();
        let mut query_reads = Vec::new();

        for item in items {
            let row_nums = item
                .rows
                .iter()
                .map(|row| row_num(&self.conn, &row.id))
                .collect::<Result<Vec<_>>>()?;
            for row_id in &item.extra_row_ids {
                repair_row_nums.push(row_num(&self.conn, row_id)?);
            }
            repair_row_nums.extend(query_scope_repair_row_nums(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            rejected_tx_ids.extend(query_scope_rejected_tx_ids(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            query_reads.push(QueryReadRecord {
                branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
                table: table_name.to_owned(),
                field: field_name.to_owned(),
                op: item.op,
                value: item.value,
            });
            visible_row_nums.extend(row_nums);
            all_rows.extend(item.rows);
        }

        visible_row_nums.sort();
        visible_row_nums.dedup();
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        rejected_tx_ids.sort();
        rejected_tx_ids.dedup();

        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &all_rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    pub(super) fn export_ref_include_history(
        &self,
        table: &crate::schema::TableDef,
        rows: &[RowView],
        ref_field_name: &str,
        branch_nums: &[i64],
    ) -> Result<Vec<HistoryRecord>> {
        let field = table
            .fields
            .iter()
            .find(|field| field.name == ref_field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown include field {ref_field_name}")))?;
        let FieldKind::Ref {
            table: ref_table_name,
        } = &field.kind
        else {
            return Err(crate::Error::new(format!(
                "include field {ref_field_name} is not a ref"
            )));
        };
        let ref_row_nums = rows
            .iter()
            .filter_map(|row| row.values.get(ref_field_name).and_then(JsonValue::as_str))
            .map(|id| row_num(&self.conn, id))
            .collect::<Result<Vec<_>>>()?;
        let mut ref_row_nums = ref_row_nums;
        ref_row_nums.sort();
        ref_row_nums.dedup();
        if ref_row_nums.is_empty() {
            return Ok(Vec::new());
        }
        let visibility = self.read_visibility();
        let mut history = export_visible_table_history(
            &visibility,
            ref_table_name,
            branch_nums,
            Some(&ref_row_nums),
        )?;
        history.extend(export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            ref_table_name,
            Some(&ref_row_nums),
            None,
            branch_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name: ref_table_name,
                policy: &self.schema.table_def(ref_table_name)?.read_policy,
                branch_nums,
                child_row_nums: Some(&ref_row_nums),
            },
        )?);
        Ok(history)
    }
}
