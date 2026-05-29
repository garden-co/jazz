use super::*;

impl Runtime {
    pub(super) fn export_built_query_read_scope_sql(
        &self,
        query_read: QueryReadRecord,
        built_query: &BuiltQuery,
        visible_scope: &query::LoweredQueryRowScope,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
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

        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history = export_history_versions_for_query_scope_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            visible_scope,
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
        history.extend(export_policy_dependency_history_for_query_scope(
            &visibility,
            PolicyDependencyQueryScopeExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_scope: visible_scope,
            },
        )?);
        if !repair_row_nums.is_empty() {
            history.extend(export_policy_dependency_history(
                &visibility,
                PolicyDependencyExport {
                    table_name,
                    policy: &table.read_policy,
                    branch_nums: &branch_nums,
                    child_row_nums: Some(&repair_row_nums),
                },
            )?);
        }
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
                history.extend(export_history_versions_for_query_scope_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    visible_scope,
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history_for_query_scope(
                    &visibility,
                    table_name,
                    base_epoch,
                    visible_scope,
                )?);
                if !repair_row_nums.is_empty() {
                    history.extend(export_history_versions_for_rows_in_branches(
                        &self.conn,
                        &self.schema,
                        table_name,
                        Some(&repair_row_nums),
                        Some(base_epoch),
                        &[1],
                    )?);
                    history.extend(export_snapshot_policy_dependency_history(
                        &visibility,
                        table_name,
                        base_epoch,
                        Some(&repair_row_nums),
                    )?);
                }
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let rejected_tx_ids = query_scope_rejected_tx_ids_for_built_query(
            &self.conn,
            &self.schema,
            table,
            built_query,
            self.branch_num,
            user,
            bypass_policy,
        )?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            vec![query_read],
            history,
        ))
    }
}
