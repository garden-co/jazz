use super::history_export::{
    dedupe_history_records, export_branch_records_for_history,
    export_history_versions_for_rows_in_branches, export_policy_dependency_history,
    export_reads_for_history, export_snapshot_policy_dependency_history,
    export_txs_for_query_scope, export_visible_table_history, include_branch_record, make_bundle,
    query_scope_rejected_tx_ids_for_read, query_scope_repair_row_nums_for_read,
    PolicyDependencyExport,
};
use super::{QueryScopeOptions, Runtime};
use crate::profile::ProfileTimer;
use crate::query_api::{
    predicate_query, BuiltQuery, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use crate::query_observation::{built_query_read_value, support_window_query};
use crate::rows::row_num;
use crate::schema::FieldKind;
use crate::sync::{merge_bundles, Bundle, HistoryRecord, QueryReadRecord};
use crate::types::{QueryExportProfile, RowView};
use crate::{branch, Result};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

impl Runtime {
    pub fn export_query_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        self.export_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value,
        ))
    }

    pub fn export_query_where_eq_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        self.export_query_with_ref_includes(
            predicate_query(table_name, field_name, QueryConditionOp::Eq, value),
            &[ref_field_name],
        )
    }

    pub fn export_query_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Bundle> {
        self.export_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Contains,
            JsonValue::String(needle.to_owned()),
        ))
    }

    pub fn export_query_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Bundle> {
        self.export_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::In,
            JsonValue::Array(values),
        ))
    }

    pub fn export_query_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        self.export_query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Ne,
            value,
        ))
    }

    pub fn export_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query(top_field_query(
            table_name,
            field_name,
            value,
            order_field_name,
            limit,
        ))
    }

    pub fn export_query_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query(top_field_query(
            table_name,
            field_name,
            value,
            "$createdAt",
            limit,
        ))
    }

    pub fn export_many_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        let bundles = values
            .into_iter()
            .map(|value| {
                self.export_query(top_field_query(
                    table_name,
                    field_name,
                    value,
                    order_field_name,
                    limit,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        merge_bundles(&bundles)
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
        let bundles = values
            .into_iter()
            .map(|value| {
                self.export_query_with_ref_includes(
                    top_field_query(table_name, field_name, value, order_field_name, limit),
                    &[ref_field_name],
                )
            })
            .collect::<Result<Vec<_>>>()?;
        merge_bundles(&bundles)
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
        let query = top_field_query(table_name, field_name, value, order_field_name, limit);
        let rows = self.query(support_window_query(&query)?)?;
        let read_rows_ms = read_started.elapsed_ms();

        let export_started = ProfileTimer::start();
        let bundle = self.export_built_query_scope(query, rows, &[])?;
        let export_ms = export_started.elapsed_ms();

        let profile = QueryExportProfile {
            total_ms: total_started.elapsed_ms(),
            read_rows_ms,
            resolve_visible_row_nums_ms: 0.0,
            repair_row_nums_ms: 0.0,
            visible_history_ms: 0.0,
            repair_visible_history_ms: 0.0,
            repair_all_history_ms: 0.0,
            policy_dependency_history_ms: 0.0,
            branch_snapshot_history_ms: 0.0,
            dedupe_history_ms: 0.0,
            reads_ms: 0.0,
            rejected_tx_ids_ms: 0.0,
            txs_ms: 0.0,
            branches_ms: 0.0,
            make_bundle_ms: export_ms,
            history_rows: bundle.history.len(),
            read_rows: bundle.reads.len(),
            tx_rows: bundle.txs.len(),
            branch_rows: bundle.branches.len(),
        };
        Ok((bundle, profile))
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

fn top_field_query(
    table: &str,
    field: &str,
    value: JsonValue,
    order_field: &str,
    limit: usize,
) -> BuiltQuery {
    let mut query = predicate_query(table, field, QueryConditionOp::Eq, value);
    query.order_by = vec![QueryOrderBy {
        column: order_field.to_owned(),
        direction: QueryDirection::Desc,
    }];
    query.limit = Some(limit);
    query
}
