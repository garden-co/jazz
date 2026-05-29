use super::*;

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
}
