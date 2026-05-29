use super::*;

impl Runtime {
    pub fn export_table_history(&self, table_name: &str) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let txs = export_txs(&self.conn)?;
        let history = export_table_history(
            &self.conn,
            &self.schema,
            table_name,
            user,
            bypass_policy,
            self.branch_num,
        )?;
        let reads = export_reads_for_history(&self.conn, &history)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            Vec::new(),
            history,
        ))
    }

    pub fn export_exclusive_transaction_forwarding(
        &self,
        table_name: &str,
        tx_id: &str,
        auth_user: &str,
    ) -> Result<Bundle> {
        let mut bundle = self.export_table_history(table_name)?;
        if !bundle.history.iter().any(|record| record.tx_id == tx_id) {
            let tx_num = tx::tx_num(&self.conn, tx_id)?;
            let history = history_records_for_tx(&self.conn, &self.schema, tx_num, tx_id)?
                .into_iter()
                .filter(|record| record.table == table_name)
                .collect::<Vec<_>>();
            if history.is_empty() {
                return Err(crate::Error::new(format!(
                    "transaction {tx_id} has no exported history"
                )));
            }
            let reads = export_reads_for_history(&self.conn, &history)?;
            let mut branches = export_branch_records_for_history(&self.conn, &history)?;
            include_branch_record(&self.conn, &mut branches, self.branch_num)?;
            bundle = make_bundle(
                &self.schema,
                branches,
                export_txs(&self.conn)?,
                reads,
                Vec::new(),
                history,
            );
        }
        let tx_record = bundle
            .txs
            .iter_mut()
            .find(|record| record.tx_id == tx_id)
            .ok_or_else(|| crate::Error::new(format!("transaction {tx_id} is not in bundle")))?;
        tx_record.conflict_mode = tx::MODE_EXCLUSIVE;
        tx_record.outcome = tx::OUTCOME_PENDING;
        tx_record.global_epoch = None;
        tx_record.receipt_tiers.clear();
        tx_record.auth_user = Some(auth_user.to_owned());
        Ok(bundle)
    }

    pub fn export_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let rows = self.read_recursive_refs(table_name, root_id, parent_field)?;
        let row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history =
            export_visible_table_history(&visibility, table_name, &branch_nums, Some(&row_nums))?;
        history.extend(export_deleted_recursive_descendant_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_recursive_scope_repair_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
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
        let txs = export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &[])?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![QueryReadRecord {
            branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: parent_field.to_owned(),
            op: "recursive_refs".to_owned(),
            value: JsonValue::String(root_id.to_owned()),
        }];
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    pub(super) fn export_many_recursive_refs(
        &self,
        table_name: &str,
        parent_field: &str,
        root_ids: Vec<String>,
    ) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut row_nums = Vec::new();
        let mut query_reads = Vec::new();

        for root_id in root_ids {
            let rows = self.read_recursive_refs(table_name, &root_id, parent_field)?;
            row_nums.extend(
                rows.iter()
                    .map(|row| row_num(&self.conn, &row.id))
                    .collect::<Result<Vec<_>>>()?,
            );
            query_reads.push(QueryReadRecord {
                branch_id: branch::id_for_num(&self.conn, self.branch_num)?,
                table: table_name.to_owned(),
                field: parent_field.to_owned(),
                op: "recursive_refs".to_owned(),
                value: JsonValue::String(root_id),
            });
        }

        row_nums.sort();
        row_nums.dedup();
        let mut history =
            export_visible_table_history(&visibility, table_name, &branch_nums, Some(&row_nums))?;
        history.extend(export_deleted_recursive_descendant_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_recursive_scope_repair_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
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
        let txs = export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &[])?;
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
}
