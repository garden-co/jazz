use super::history_export::{export_txs, include_branch_record, make_bundle};
use super::Runtime;
use crate::observed_query::{self, ObservedQuery, PredicateOp};
use crate::query_api::{predicate_query, QueryConditionOp};
use crate::query_observation::support_window_query;
use crate::query_refresh::QueryRefreshPlan;
use crate::sync::{Bundle, QueryReadRecord};
use crate::{branch, Result};
use serde_json::Value as JsonValue;

impl Runtime {
    pub fn observed_query_reads(&self) -> Result<Vec<QueryReadRecord>> {
        crate::query_descriptor::list(&self.conn)
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
        crate::query_descriptor::forget(&self.conn, read)
    }

    fn export_query_read_refresh(&self, read: &QueryReadRecord) -> Result<Bundle> {
        if read.branch_id != branch::id_for_num(&self.conn, self.branch_num)? {
            return Err(crate::Error::new("query refresh branch is not checked out"));
        }
        match observed_query::decode(read)? {
            ObservedQuery::Predicate {
                op: PredicateOp::Eq,
                value,
            } => self.export_query_where_eq(&read.table, &read.field, value),
            ObservedQuery::Predicate {
                op: PredicateOp::Ne,
                value,
            } => self.export_query_where_ne(&read.table, &read.field, value),
            ObservedQuery::Predicate {
                op: PredicateOp::Contains,
                value,
            } => {
                let Some(needle) = value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.export_query_where_contains(&read.table, &read.field, needle)
            }
            ObservedQuery::Predicate {
                op: PredicateOp::In,
                value,
            } => {
                let Some(values) = value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.export_query_where_in(&read.table, &read.field, values.clone())
            }
            ObservedQuery::RecursiveRefs { root_id } => {
                self.export_recursive_refs(&read.table, &root_id, &read.field)
            }
            ObservedQuery::TopCreatedAt {
                value,
                limit,
                observed_ids,
            } => self.export_query_where_eq_top_created_at_desc_with_previous_observed(
                &read.table,
                &read.field,
                value,
                limit,
                observed_ids,
            ),
            ObservedQuery::TopField {
                value,
                order_field,
                limit,
                observed_ids,
            } => self.export_query_where_eq_top_field_desc_with_previous_observed(
                &read.table,
                &read.field,
                value,
                &order_field,
                limit,
                observed_ids,
            ),
            ObservedQuery::Built {
                query,
                observed_ids,
            } => {
                let rows = self.query(support_window_query(&query)?)?;
                self.export_built_query_scope_with_previous_observed(query, rows, &[], observed_ids)
            }
            ObservedQuery::Absent => {
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
        }
    }
}
