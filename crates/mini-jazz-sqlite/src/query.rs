use crate::query_api::{
    predicate_query, BuiltQuery, QueryCondition, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use crate::read_visibility::ReadVisibility;
use crate::rows::{public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::types::{ReadTier, RowView};
use crate::{branch, tx, users, Result};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const RECURSIVE_VISIBLE_ROWS_TABLE_SCAN_THRESHOLD: i64 = 50_000;
const MAX_MULTI_VALUE_TOP_PAGE_VALUES: usize = 400;

pub(crate) struct QueryContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) branch_num: i64,
    pub(crate) user: &'a str,
    pub(crate) bypass_policy: bool,
    pub(crate) read_tier: ReadTier,
}

struct LoweredCondition {
    sql: String,
    params: Vec<SqlValue>,
}

pub(crate) struct LoweredQueryRowScope {
    pub(crate) ctes: Vec<String>,
    pub(crate) select_sql: String,
    pub(crate) params: Vec<SqlValue>,
}

impl LoweredQueryRowScope {
    pub(crate) fn with_scope_cte(&self, scope_name: &str) -> String {
        let mut ctes = self.ctes.clone();
        ctes.push(format!("{scope_name}(row_num) AS ({})", self.select_sql));
        format!("WITH {}", ctes.join(",\n"))
    }
}

enum QueryColumn<'a> {
    Id,
    CreatedBy,
    CreatedAt,
    UpdatedAt,
    Field(&'a FieldDef),
}

impl QueryContext<'_> {
    pub(crate) fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        if self.read_tier != ReadTier::Local {
            let rows = self.read_rows_from_history_at_tier(table_name, self.read_tier)?;
            return self.filter_rows_by_effective_branch_policy(table_name, rows);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                let mut rows = self.read_rows_from_current(table_name, false)?;
                rows.extend(self.read_main_snapshot_rows(table_name, base_epoch)?);
                return self.filter_rows_by_effective_branch_policy(table_name, rows);
            }
        }
        self.read_rows_from_current(table_name, true)
    }

    pub(crate) fn read_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        if field_name == "id" {
            let Some(id) = value.as_str() else {
                return Err(crate::Error::new("id equality expects a string value"));
            };
            return Ok(self
                .read_rows(table_name)?
                .into_iter()
                .filter(|row| row.id == id)
                .collect());
        }
        if field_name == "$createdBy" {
            let Some(created_by) = value.as_str() else {
                return Err(crate::Error::new(
                    "$createdBy equality expects a string value",
                ));
            };
            return Ok(self
                .read_rows(table_name)?
                .into_iter()
                .filter(|row| row.created_by == created_by)
                .collect());
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                let mut rows =
                    self.read_rows_from_current_where_eq(table_name, field, &value, false)?;
                rows.extend(
                    self.read_main_snapshot_rows(table_name, base_epoch)?
                        .into_iter()
                        .filter(|row| row.values.get(field_name) == Some(&value)),
                );
                return self.filter_rows_by_effective_branch_policy(table_name, rows);
            }
        }
        self.read_rows_from_current_where_eq(table_name, field, &value, true)
    }

    pub(crate) fn read_rows_for_built_query(&self, query: &BuiltQuery) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(&query.table)?;
        if self.read_tier != ReadTier::Local {
            return self.read_tiered_rows_for_built_query(query, table);
        }
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let base_epoch = branch::base_global_epoch(self.conn, self.branch_num)?;
        if self.branch_num == 1 && scope_nums == [self.branch_num] && base_epoch.is_none() {
            return self.read_current_built_query(query, table);
        }
        let (candidates_sql, mut params) = self.lower_query_candidates(query, table)?;
        let order_sql = self.lower_query_order(table, &query.order_by)?;
        let defer_window_until_effective_policy =
            self.defer_query_window_until_effective_branch_policy(table, query, base_epoch);
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["j_query_row_id".to_owned(), "j_query_tx_id".to_owned()];
        select_columns.extend(field_columns.iter().cloned());
        select_columns.push("j_query_created_by".to_owned());
        select_columns.push("j_query_created_at".to_owned());
        let mut sql = format!(
            "WITH candidates AS (
               {candidates_sql}
             ),
             ranked AS (
               SELECT *,
                      MIN(j_query_branch_depth) OVER (PARTITION BY j_query_row_id) AS j_query_min_branch_depth
               FROM candidates
             )
             SELECT {}
             FROM ranked
             WHERE j_query_branch_depth = j_query_min_branch_depth
             ORDER BY {order_sql}",
            select_columns.join(", "),
        );
        if !defer_window_until_effective_policy {
            append_query_window_sql(&mut sql, &mut params, query.limit, query.offset)?;
        }

        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 2;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, SqlValue>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let mut rows = rows
            .map(|row| row_to_view(self.conn, &query.table, table, row?))
            .collect::<Result<Vec<_>>>()?;
        if self.branch_num != 1 && base_epoch.is_some() {
            rows = self.filter_rows_by_effective_branch_policy(&query.table, rows)?;
        }
        if defer_window_until_effective_policy {
            rows = apply_query_window(rows, query.limit, query.offset);
        }
        Ok(rows)
    }

    fn read_tiered_rows_for_built_query(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
    ) -> Result<Vec<RowView>> {
        let mut rows = self
            .read_rows_from_history_at_tier(&query.table, self.read_tier)?
            .into_iter()
            .filter_map(|row| {
                self.row_matches_built_query(table, &row, &query.conditions)
                    .map(|matches| matches.then_some(row))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|left, right| self.compare_built_query_rows(table, query, left, right));
        Ok(apply_query_window(rows, query.limit, query.offset))
    }

    fn row_matches_built_query(
        &self,
        table: &crate::schema::TableDef,
        row: &RowView,
        conditions: &[QueryCondition],
    ) -> Result<bool> {
        for condition in conditions {
            if !self.row_matches_condition(table, row, condition)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn row_matches_condition(
        &self,
        table: &crate::schema::TableDef,
        row: &RowView,
        condition: &QueryCondition,
    ) -> Result<bool> {
        let value = self.row_query_value(table, row, &condition.column)?;
        Ok(match condition.op {
            QueryConditionOp::Eq => value == condition.value,
            QueryConditionOp::Ne => value != condition.value,
            QueryConditionOp::Contains => value
                .as_str()
                .zip(condition.value.as_str())
                .is_some_and(|(haystack, needle)| haystack.contains(needle)),
            QueryConditionOp::In => condition
                .value
                .as_array()
                .is_some_and(|values| values.iter().any(|candidate| candidate == &value)),
        })
    }

    fn compare_built_query_rows(
        &self,
        table: &crate::schema::TableDef,
        query: &BuiltQuery,
        left: &RowView,
        right: &RowView,
    ) -> Ordering {
        if query.order_by.is_empty() {
            return right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.id.cmp(&right.id));
        }
        for order in &query.order_by {
            let left_value = self.row_query_value(table, left, &order.column).ok();
            let right_value = self.row_query_value(table, right, &order.column).ok();
            let cmp = json_sort_key(left_value.as_ref()).cmp(&json_sort_key(right_value.as_ref()));
            let cmp = match order.direction {
                QueryDirection::Asc => cmp,
                QueryDirection::Desc => cmp.reverse(),
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        left.id.cmp(&right.id)
    }

    fn row_query_value(
        &self,
        table: &crate::schema::TableDef,
        row: &RowView,
        column: &str,
    ) -> Result<JsonValue> {
        match query_column(table, column)? {
            QueryColumn::Id => Ok(JsonValue::String(row.id.clone())),
            QueryColumn::CreatedBy => Ok(JsonValue::String(row.created_by.clone())),
            QueryColumn::CreatedAt => Ok(JsonValue::Number(row.created_at.into())),
            QueryColumn::UpdatedAt => Ok(JsonValue::Number(row.created_at.into())),
            QueryColumn::Field(field) => Ok(row
                .values
                .get(&field.name)
                .cloned()
                .unwrap_or(JsonValue::Null)),
        }
    }

    pub(crate) fn repair_row_nums_for_built_query(&self, query: &BuiltQuery) -> Result<Vec<i64>> {
        if query.limit.is_some() || query.offset.is_some() {
            let support_query = repair_support_query(query)?;
            return self
                .read_rows_for_built_query(&support_query)?
                .into_iter()
                .map(|row| row_num(self.conn, &row.id))
                .collect();
        }

        let table = self.schema.table_def(&query.table)?;
        let branch_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let mut row_nums =
            self.repair_current_row_nums_for_built_query(query, table, &branch_nums)?;
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                row_nums.extend(
                    self.repair_main_snapshot_row_nums_for_built_query(query, table, base_epoch)?,
                );
            }
        }
        row_nums.sort();
        row_nums.dedup();
        Ok(row_nums)
    }

    pub(crate) fn lower_built_query_row_scope(
        &self,
        query: &BuiltQuery,
    ) -> Result<Option<LoweredQueryRowScope>> {
        let table = self.schema.table_def(&query.table)?;
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let base_epoch = branch::base_global_epoch(self.conn, self.branch_num)?;
        if self.defer_query_window_until_effective_branch_policy(table, query, base_epoch) {
            return Ok(None);
        }

        if self.branch_num == 1 && scope_nums == [self.branch_num] && base_epoch.is_none() {
            let (condition_sql, mut condition_params) =
                self.lower_query_conditions(table, &query.conditions, "current", "ids")?;
            let order_sql =
                self.lower_source_query_order(table, &query.order_by, "current", "ids")?;
            let mut select_sql = format!(
                "SELECT current.row_num AS row_num
                 FROM {} current
                 JOIN jazz_row_id ids ON ids.row_num = current.row_num
                 JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                 WHERE current.j_branch_num = ?
                   AND current.is_deleted = 0
                   AND {tier_sql}
                   AND {condition_sql}
                   AND {policy_sql}
                 ORDER BY {order_sql}",
                crate::schema::current_table(&query.table),
                policy_sql = self.read_policy_sql(table)?,
                tier_sql = self.read_tier_sql("tx"),
            );
            let mut params = vec![SqlValue::Integer(self.branch_num)];
            params.append(&mut condition_params);
            append_query_window_sql(&mut select_sql, &mut params, query.limit, query.offset)?;
            return Ok(Some(LoweredQueryRowScope {
                ctes: Vec::new(),
                select_sql,
                params,
            }));
        }

        let (candidates_sql, mut params) = self.lower_query_candidates(query, table)?;
        let order_sql = self.lower_query_order(table, &query.order_by)?;
        let mut select_sql = format!(
            "SELECT j_query_row_num AS row_num
             FROM j_query_ranked
             WHERE j_query_branch_depth = j_query_min_branch_depth
             ORDER BY {order_sql}",
        );
        append_query_window_sql(&mut select_sql, &mut params, query.limit, query.offset)?;
        Ok(Some(LoweredQueryRowScope {
            ctes: vec![
                format!("j_query_candidates AS ({candidates_sql})"),
                "j_query_ranked AS (
                   SELECT *,
                          MIN(j_query_branch_depth) OVER (PARTITION BY j_query_row_id) AS j_query_min_branch_depth
                   FROM j_query_candidates
                 )"
                .to_owned(),
            ],
            select_sql,
            params,
        }))
    }

    fn repair_current_row_nums_for_built_query(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
        branch_nums: &[i64],
    ) -> Result<Vec<i64>> {
        let (condition_sql, condition_params) =
            self.lower_query_conditions(table, &query.conditions, "h", "ids")?;
        let policy_sql = if self.bypass_policy {
            "1 = 1".to_owned()
        } else {
            self.visibility().current_policy_sql(table, "h")?
        };
        let branch_placeholders = placeholders(branch_nums.len());
        let sql = format!(
            "SELECT DISTINCT h.row_num
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num IN ({branch_placeholders})
               AND {tier_sql}
               AND {condition_sql}
               AND {policy_sql}
            ORDER BY h.row_num",
            crate::schema::history_table(&query.table),
            tier_sql = self.read_tier_sql("tx"),
        );
        let mut query_params = branch_nums
            .iter()
            .copied()
            .map(SqlValue::Integer)
            .collect::<Vec<_>>();
        query_params.extend(condition_params);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn repair_main_snapshot_row_nums_for_built_query(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
        base_epoch: i64,
    ) -> Result<Vec<i64>> {
        let (condition_sql, condition_params) =
            self.lower_query_conditions(table, &query.conditions, "h", "ids")?;
        let policy_sql = if self.bypass_policy {
            "1 = 1".to_owned()
        } else {
            self.visibility()
                .snapshot_policy_sql(table, "h", base_epoch)?
        };
        let sql = format!(
            "SELECT DISTINCT h.row_num
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND {condition_sql}
               AND {policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
             ORDER BY h.row_num",
            crate::schema::history_table(&query.table),
            history_table = crate::schema::history_table(&query.table),
        );
        let mut query_params = vec![
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
        ];
        query_params.extend(condition_params);
        query_params.extend([
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
        ]);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn read_current_built_query(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
    ) -> Result<Vec<RowView>> {
        let (condition_sql, mut params) =
            self.lower_query_conditions(table, &query.conditions, "current", "ids")?;
        let order_sql = self.lower_source_query_order(table, &query.order_by, "current", "ids")?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        select_columns.push("current.j_created_at".to_owned());
        let mut sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.j_branch_num = ?
               AND current.is_deleted = 0
               AND {tier_sql}
               AND {condition_sql}
               AND {policy_sql}
             ORDER BY {order_sql}",
            select_columns.join(", "),
            crate::schema::current_table(&query.table),
            policy_sql = self.read_policy_sql(table)?,
            tier_sql = self.read_tier_sql("tx"),
        );
        let mut query_params = vec![SqlValue::Integer(self.branch_num)];
        query_params.append(&mut params);
        append_query_window_sql(&mut sql, &mut query_params, query.limit, query.offset)?;

        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 2;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, SqlValue>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| row_to_view(self.conn, &query.table, table, row?))
            .collect()
    }

    fn lower_query_candidates(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
    ) -> Result<(String, Vec<SqlValue>)> {
        let mut parts = Vec::new();
        let mut params = Vec::new();
        let base_epoch = if self.branch_num != 1 {
            branch::base_global_epoch(self.conn, self.branch_num)?
        } else {
            None
        };
        let (current_sql, current_params) =
            self.lower_current_query_candidate(query, table, base_epoch.is_none())?;
        parts.push(current_sql);
        params.extend(current_params);
        if let Some(base_epoch) = base_epoch {
            let (snapshot_sql, snapshot_params) =
                self.lower_main_snapshot_query_candidate(query, table, base_epoch)?;
            parts.push(snapshot_sql);
            params.extend(snapshot_params);
        }
        Ok((parts.join("\nUNION ALL\n"), params))
    }

    fn lower_current_query_candidate(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
        overlay_main: bool,
    ) -> Result<(String, Vec<SqlValue>)> {
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let (condition_sql, condition_params) =
            self.lower_query_conditions(table, &query.conditions, "current", "ids")?;
        let select_columns = query_candidate_select_columns(
            table,
            "current",
            "ids",
            "tx",
            &branch_depth_case_sql(self.conn, self.branch_num, "current")?,
        );
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num IN ({scope_placeholders})
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num IN ({scope_placeholders})
                   )
                 )
               )
	               AND {tier_sql}
               AND NOT (
                 current.j_branch_num != ?
                 AND EXISTS (
                   SELECT 1
                   FROM {current_table} active_current
                   WHERE active_current.row_num = current.row_num
                     AND active_current.j_branch_num = ?
                 )
               )
               AND {condition_sql}
               AND {policy_sql}",
            select_columns.join(", "),
            crate::schema::current_table(&query.table),
            current_table = crate::schema::current_table(&query.table),
            policy_sql = self.read_policy_sql(table)?,
            tier_sql = self.read_tier_sql("tx"),
        );
        let mut params = scope_nums
            .iter()
            .copied()
            .map(SqlValue::Integer)
            .collect::<Vec<_>>();
        params.extend([
            SqlValue::Integer(i64::from(overlay_main)),
            SqlValue::Integer(self.branch_num),
        ]);
        params.extend(scope_nums.iter().copied().map(SqlValue::Integer));
        params.extend([
            SqlValue::Integer(self.branch_num),
            SqlValue::Integer(self.branch_num),
        ]);
        params.extend(condition_params);
        Ok((sql, params))
    }

    fn lower_main_snapshot_query_candidate(
        &self,
        query: &BuiltQuery,
        table: &crate::schema::TableDef,
        base_epoch: i64,
    ) -> Result<(String, Vec<SqlValue>)> {
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let (condition_sql, condition_params) =
            self.lower_query_conditions(table, &query.conditions, "h", "ids")?;
        let policy_sql = if self.bypass_policy {
            "1 = 1".to_owned()
        } else {
            self.visibility()
                .snapshot_policy_sql(table, "h", base_epoch)?
        };
        let select_columns =
            query_candidate_select_columns(table, "h", "ids", "tx", &(i64::MAX / 4).to_string());
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND {policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM {current_table} branch_current
                 WHERE branch_current.row_num = h.row_num
                   AND branch_current.j_branch_num IN ({scope_placeholders})
               )
               AND {condition_sql}",
            select_columns.join(", "),
            crate::schema::history_table(&query.table),
            history_table = crate::schema::history_table(&query.table),
            current_table = crate::schema::current_table(&query.table),
            policy_sql = policy_sql,
        );
        let mut params = vec![
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
            SqlValue::Integer(tx::OUTCOME_REJECTED),
            SqlValue::Integer(base_epoch),
        ];
        params.extend(scope_nums.iter().copied().map(SqlValue::Integer));
        params.extend(condition_params);
        Ok((sql, params))
    }

    fn lower_query_conditions(
        &self,
        table: &crate::schema::TableDef,
        conditions: &[QueryCondition],
        row_alias: &str,
        ids_alias: &str,
    ) -> Result<(String, Vec<SqlValue>)> {
        if conditions.is_empty() {
            return Ok(("1 = 1".to_owned(), Vec::new()));
        }
        let mut sql = Vec::new();
        let mut params = Vec::new();
        for condition in conditions {
            let lowered = self.lower_query_condition(table, condition, row_alias, ids_alias)?;
            sql.push(lowered.sql);
            params.extend(lowered.params);
        }
        Ok((sql.join(" AND "), params))
    }

    fn lower_query_condition(
        &self,
        table: &crate::schema::TableDef,
        condition: &QueryCondition,
        row_alias: &str,
        ids_alias: &str,
    ) -> Result<LoweredCondition> {
        let column = query_column(table, &condition.column)?;
        let column_sql = source_query_column_sql(&column, row_alias, ids_alias);
        match condition.op {
            QueryConditionOp::Eq if condition.value.is_null() => Ok(LoweredCondition {
                sql: format!("{column_sql} IS NULL"),
                params: Vec::new(),
            }),
            QueryConditionOp::Eq => Ok(LoweredCondition {
                sql: format!("{column_sql} = ?"),
                params: vec![self.query_condition_value(&column, &condition.value)?],
            }),
            QueryConditionOp::Ne if condition.value.is_null() => Ok(LoweredCondition {
                sql: format!("{column_sql} IS NOT NULL"),
                params: Vec::new(),
            }),
            QueryConditionOp::Ne => Ok(LoweredCondition {
                sql: format!("{column_sql} IS NOT ?"),
                params: vec![self.query_condition_value(&column, &condition.value)?],
            }),
            QueryConditionOp::Contains => {
                if !query_column_is_text(&column) {
                    return Err(crate::Error::new(format!(
                        "contains only supports text fields, got {}",
                        condition.column
                    )));
                }
                let Some(needle) = condition.value.as_str() else {
                    return Err(crate::Error::new(
                        "contains condition expects a string value",
                    ));
                };
                Ok(LoweredCondition {
                    sql: format!("instr({column_sql}, ?) > 0"),
                    params: vec![SqlValue::Text(needle.to_owned())],
                })
            }
            QueryConditionOp::In => {
                let Some(values) = condition.value.as_array() else {
                    return Err(crate::Error::new("in condition expects an array value"));
                };
                self.lower_in_condition(&column, &column_sql, values)
            }
        }
    }

    fn lower_in_condition(
        &self,
        column: &QueryColumn<'_>,
        column_sql: &str,
        values: &[JsonValue],
    ) -> Result<LoweredCondition> {
        if values.is_empty() {
            return Ok(LoweredCondition {
                sql: "0 = 1".to_owned(),
                params: Vec::new(),
            });
        }
        let mut has_null = false;
        let mut params = Vec::new();
        for value in values {
            if value.is_null() {
                has_null = true;
            } else {
                params.push(self.query_condition_value(column, value)?);
            }
        }
        let sql = match (params.is_empty(), has_null) {
            (true, true) => format!("{column_sql} IS NULL"),
            (false, true) => format!(
                "({column_sql} IN ({}) OR {column_sql} IS NULL)",
                placeholders(params.len())
            ),
            (false, false) => format!("{column_sql} IN ({})", placeholders(params.len())),
            (true, false) => "0 = 1".to_owned(),
        };
        Ok(LoweredCondition { sql, params })
    }

    fn query_condition_value(
        &self,
        column: &QueryColumn<'_>,
        value: &JsonValue,
    ) -> Result<SqlValue> {
        match column {
            QueryColumn::Id | QueryColumn::CreatedBy => Ok(SqlValue::Text(
                value
                    .as_str()
                    .ok_or_else(|| crate::Error::new("query system field expects a string"))?
                    .to_owned(),
            )),
            QueryColumn::CreatedAt | QueryColumn::UpdatedAt => {
                Ok(SqlValue::Integer(value.as_i64().ok_or_else(|| {
                    crate::Error::new("query timestamp field expects an integer")
                })?))
            }
            QueryColumn::Field(field) => {
                crate::schema::field_sql_value(field, value, |ref_table, row_id| {
                    row_num(self.conn, row_id).map_err(|err| {
                        crate::Error::new(format!(
                            "failed to resolve ref {ref_table}.{row_id} for query predicate: {err}"
                        ))
                    })
                })
            }
        }
    }

    fn lower_query_order(
        &self,
        table: &crate::schema::TableDef,
        order_by: &[QueryOrderBy],
    ) -> Result<String> {
        if order_by.is_empty() {
            return Ok("j_query_created_at DESC, j_query_row_num".to_owned());
        }
        let mut parts = Vec::new();
        for order in order_by {
            let column = query_column(table, &order.column)?;
            let direction = match order.direction {
                QueryDirection::Asc => "ASC",
                QueryDirection::Desc => "DESC",
            };
            parts.push(format!(
                "{} {direction}",
                final_query_order_column_sql(&column)
            ));
        }
        parts.push("j_query_row_num".to_owned());
        Ok(parts.join(", "))
    }

    fn lower_source_query_order(
        &self,
        table: &crate::schema::TableDef,
        order_by: &[QueryOrderBy],
        row_alias: &str,
        ids_alias: &str,
    ) -> Result<String> {
        if order_by.is_empty() {
            return Ok(format!(
                "{row_alias}.j_created_at DESC, {row_alias}.row_num"
            ));
        }
        let mut parts = Vec::new();
        for order in order_by {
            let column = query_column(table, &order.column)?;
            let direction = match order.direction {
                QueryDirection::Asc => "ASC",
                QueryDirection::Desc => "DESC",
            };
            parts.push(format!(
                "{} {direction}",
                source_query_order_column_sql(&column, row_alias, ids_alias)
            ));
        }
        parts.push(format!("{row_alias}.row_num"));
        Ok(parts.join(", "))
    }

    pub(crate) fn read_rows_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Vec<RowView>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        if field_name == "id" {
            if self.branch_num != 1 {
                if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                    let ids = values
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>();
                    let mut rows =
                        self.read_rows_from_current_where_id_in(table_name, &ids, false)?;
                    rows.extend(
                        self.read_main_snapshot_rows(table_name, base_epoch)?
                            .into_iter()
                            .filter(|row| values.contains(&JsonValue::String(row.id.clone()))),
                    );
                    return self.filter_rows_by_effective_branch_policy(table_name, rows);
                }
            }
            let ids = values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            return self.read_rows_from_current_where_id_in(table_name, &ids, true);
        }
        if field_name == "$createdBy" {
            if self.branch_num != 1 {
                if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                    let user_nums = self.user_nums_for_values(&values)?;
                    let mut rows = self.read_rows_from_current_where_created_by_in(
                        table_name, &user_nums, false,
                    )?;
                    rows.extend(
                        self.read_main_snapshot_rows(table_name, base_epoch)?
                            .into_iter()
                            .filter(|row| {
                                values.contains(&JsonValue::String(row.created_by.clone()))
                            }),
                    );
                    return self.filter_rows_by_effective_branch_policy(table_name, rows);
                }
            }
            let user_nums = self.user_nums_for_values(&values)?;
            return self.read_rows_from_current_where_created_by_in(table_name, &user_nums, true);
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                let mut rows =
                    self.read_rows_from_current_where_in(table_name, field, &values, false)?;
                rows.extend(
                    self.read_main_snapshot_rows(table_name, base_epoch)?
                        .into_iter()
                        .filter(|row| {
                            row.values
                                .get(field_name)
                                .is_some_and(|value| values.contains(value))
                        }),
                );
                return self.filter_rows_by_effective_branch_policy(table_name, rows);
            }
        }
        self.read_rows_from_current_where_in(table_name, field, &values, true)
    }

    pub(crate) fn read_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        let mut rows = self.read_many_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            &[value],
            limit,
        )?;
        Ok(rows.pop().unwrap_or_default())
    }

    pub(crate) fn read_many_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        values: &[JsonValue],
        limit: usize,
    ) -> Result<Vec<Vec<RowView>>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        if values.len() > MAX_MULTI_VALUE_TOP_PAGE_VALUES {
            let mut grouped = Vec::with_capacity(values.len());
            for chunk in values.chunks(MAX_MULTI_VALUE_TOP_PAGE_VALUES) {
                grouped.extend(self.read_many_rows_where_eq_top_created_at_desc(
                    table_name, field_name, chunk, limit,
                )?);
            }
            return Ok(grouped);
        }
        if self.branch_num != 1 || matches!(field_name, "id" | "$createdBy") {
            let created_at_by_id = current_created_at_by_row_id(self.conn, table_name)?;
            return values
                .iter()
                .map(|value| {
                    let mut rows =
                        self.read_rows_where_eq(table_name, field_name, value.clone())?;
                    rows.sort_by(|left, right| {
                        created_at_by_id
                            .get(&right.id)
                            .cmp(&created_at_by_id.get(&left.id))
                            .then_with(|| left.id.cmp(&right.id))
                    });
                    rows.truncate(limit);
                    Ok(rows)
                })
                .collect();
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "value_index".to_owned(),
            "row_id".to_owned(),
            "tx_id".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| column.to_owned()));
        select_columns.push("j_created_by".to_owned());
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let value_rows = (0..values.len())
            .map(|_| "(?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH query_values(value_index, predicate_value) AS (VALUES {value_rows}),
             ranked AS (
               SELECT
                 query_values.value_index AS value_index,
                 ids.row_id AS row_id,
                 tx.tx_id AS tx_id,
                 {field_columns},
                 {created_by} AS j_created_by,
                 row_number() OVER (
                   PARTITION BY query_values.value_index
                   ORDER BY current.j_created_at DESC, ids.row_id
                 ) AS query_rank
               FROM query_values
               JOIN {current_table} current
                 ON current.{predicate_column} = query_values.predicate_value
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = 1
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND {policy_sql}
             )
             SELECT {select_columns}
             FROM ranked
             WHERE query_rank <= ?
             ORDER BY value_index, query_rank",
            field_columns = field_columns
                .iter()
                .map(|column| format!("current.{column} AS {column}"))
                .collect::<Vec<_>>()
                .join(", "),
            created_by = users::user_id_expr("current", "j_created_by"),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
            select_columns = select_columns.join(", "),
        );
        let mut params = Vec::new();
        for (idx, value) in values.iter().enumerate() {
            params.push(rusqlite::types::Value::Integer(idx as i64));
            params.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| {
                    row_num(self.conn, row_id).map_err(|err| {
                        crate::Error::new(format!(
                            "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                        ))
                    })
                },
            )?);
        }
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        params.push(rusqlite::types::Value::Integer(limit as i64));
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let mut grouped = vec![Vec::new(); values.len()];
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            let value_index = row.get::<_, i64>(0)? as usize;
            let values = (1..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((value_index, values))
        })?;
        for row in rows {
            let (value_index, row) = row?;
            let row = row_to_view(self.conn, table_name, table, row)?;
            grouped
                .get_mut(value_index)
                .ok_or_else(|| crate::Error::new("query value index out of range"))?
                .push(row);
        }
        Ok(grouped)
    }

    pub(crate) fn read_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        if self.branch_num != 1 {
            if let Some(rows) = self.read_rows_where_eq_top_field_desc_from_pinned_base_branch(
                table_name,
                field_name,
                value.clone(),
                order_field_name,
                limit,
            )? {
                return Ok(rows);
            }
            if let Some(rows) = self.read_rows_where_eq_top_field_desc_from_main_source_branch(
                table_name,
                field_name,
                value.clone(),
                order_field_name,
                limit,
            )? {
                return Ok(rows);
            }
            let query = predicate_query(table_name, field_name, QueryConditionOp::Eq, value);
            let mut rows = self.read_rows_for_built_query(&query)?;
            rows.sort_by(|left, right| {
                json_sort_key(right.values.get(order_field_name))
                    .cmp(&json_sort_key(left.values.get(order_field_name)))
                    .then_with(|| left.id.cmp(&right.id))
            });
            rows.truncate(limit);
            return Ok(rows);
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        let order_field = table
            .fields
            .iter()
            .find(|field| field.name == order_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown order field {table_name}.{order_field_name}"
                ))
            })?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let order_column = crate::schema::quote_ident(&crate::schema::storage_column(order_field));
        let predicate_value =
            crate::schema::field_sql_value(field, &value, |ref_table, row_id| {
                row_num(self.conn, row_id).map_err(|err| {
                    crate::Error::new(format!(
                        "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                    ))
                })
            })?;
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.j_branch_num = 1
               AND current.is_deleted = 0
               AND {tier_sql}
               AND current.{predicate_column} = ?
               AND {policy_sql}
             ORDER BY current.{order_column} DESC, ids.row_id
             LIMIT ?",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
            tier_sql = self.read_tier_sql("tx"),
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params![predicate_value, limit as i64], |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }

    pub(crate) fn read_many_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        values: &[JsonValue],
        order_field_name: &str,
        limit: usize,
    ) -> Result<Vec<Vec<RowView>>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        if values.len() > MAX_MULTI_VALUE_TOP_PAGE_VALUES {
            let mut grouped = Vec::with_capacity(values.len());
            for chunk in values.chunks(MAX_MULTI_VALUE_TOP_PAGE_VALUES) {
                grouped.extend(self.read_many_rows_where_eq_top_field_desc(
                    table_name,
                    field_name,
                    chunk,
                    order_field_name,
                    limit,
                )?);
            }
            return Ok(grouped);
        }
        if self.branch_num != 1 {
            return values
                .iter()
                .map(|value| {
                    self.read_rows_where_eq_top_field_desc(
                        table_name,
                        field_name,
                        value.clone(),
                        order_field_name,
                        limit,
                    )
                })
                .collect();
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        let order_field = table
            .fields
            .iter()
            .find(|field| field.name == order_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown order field {table_name}.{order_field_name}"
                ))
            })?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "value_index".to_owned(),
            "row_id".to_owned(),
            "tx_id".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| column.to_owned()));
        select_columns.push("j_created_by".to_owned());
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let order_column = crate::schema::quote_ident(&crate::schema::storage_column(order_field));
        let value_rows = (0..values.len())
            .map(|_| "(?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH query_values(value_index, predicate_value) AS (VALUES {value_rows}),
             ranked AS (
               SELECT
                 query_values.value_index AS value_index,
                 ids.row_id AS row_id,
                 tx.tx_id AS tx_id,
                 {field_columns},
                 {created_by} AS j_created_by,
                 row_number() OVER (
                   PARTITION BY query_values.value_index
                   ORDER BY current.{order_column} DESC, ids.row_id
                 ) AS query_rank
               FROM query_values
               JOIN {current_table} current
                 ON current.{predicate_column} = query_values.predicate_value
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = 1
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND {policy_sql}
             )
             SELECT {select_columns}
             FROM ranked
             WHERE query_rank <= ?
             ORDER BY value_index, query_rank",
            field_columns = field_columns
                .iter()
                .map(|column| format!("current.{column} AS {column}"))
                .collect::<Vec<_>>()
                .join(", "),
            created_by = users::user_id_expr("current", "j_created_by"),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
            select_columns = select_columns.join(", "),
        );
        let mut params = Vec::new();
        for (idx, value) in values.iter().enumerate() {
            params.push(rusqlite::types::Value::Integer(idx as i64));
            params.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| {
                    row_num(self.conn, row_id).map_err(|err| {
                        crate::Error::new(format!(
                            "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                        ))
                    })
                },
            )?);
        }
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        params.push(rusqlite::types::Value::Integer(limit as i64));
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let mut grouped = vec![Vec::new(); values.len()];
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            let value_index = row.get::<_, i64>(0)? as usize;
            let values = (1..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((value_index, values))
        })?;
        for row in rows {
            let (value_index, row) = row?;
            let row = row_to_view(self.conn, table_name, table, row)?;
            grouped
                .get_mut(value_index)
                .ok_or_else(|| crate::Error::new("query value index out of range"))?
                .push(row);
        }
        Ok(grouped)
    }

    fn read_rows_where_eq_top_field_desc_from_pinned_base_branch(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Option<Vec<RowView>>> {
        let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? else {
            return Ok(None);
        };
        if !branch::direct_source_nums(self.conn, self.branch_num)?.is_empty() {
            return Ok(None);
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        if value.is_null() {
            return Ok(None);
        }
        let order_field = table
            .fields
            .iter()
            .find(|field| field.name == order_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown order field {table_name}.{order_field_name}"
                ))
            })?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let current_field_columns = field_columns
            .iter()
            .map(|column| format!("current.{column} AS {column}"))
            .collect::<Vec<_>>();
        let history_field_columns = field_columns
            .iter()
            .map(|column| format!("h.{column} AS {column}"))
            .collect::<Vec<_>>();
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let order_column = crate::schema::quote_ident(&crate::schema::storage_column(order_field));
        let predicate_value =
            crate::schema::field_sql_value(field, &value, |ref_table, row_id| {
                row_num(self.conn, row_id).map_err(|err| {
                    crate::Error::new(format!(
                        "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                    ))
                })
            })?;
        let current_row_columns = format!(
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, current.{order_column} AS sort_value, ids.row_id AS sort_row_id",
            current_field_columns.join(", "),
            users::user_id_expr("current", "j_created_by")
        );
        let history_row_columns = format!(
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, h.{order_column} AS sort_value, ids.row_id AS sort_row_id",
            history_field_columns.join(", "),
            users::user_id_expr("h", "j_created_by")
        );
        let outer_columns = format!("row_id, tx_id, {}, j_created_by", field_columns.join(", "));
        let current_table = crate::schema::current_table(table_name);
        let history_table = crate::schema::history_table(table_name);
        let current_policy_sql = self.read_policy_sql(table)?;
        let snapshot_policy_sql = if self.bypass_policy {
            "1 = 1".to_owned()
        } else {
            self.visibility()
                .snapshot_policy_sql(table, "h", base_epoch)?
        };
        let sql = format!(
            "WITH
             overlay_rows AS (
               SELECT {current_row_columns}
               FROM {current_table} current
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = ?
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND current.{predicate_column} = ?
                 AND {current_policy_sql}
               ORDER BY current.{order_column} DESC, ids.row_id
               LIMIT ?
             ),
             base_rows AS (
               SELECT {history_row_columns}
               FROM {history_table} h
               JOIN jazz_row_id ids ON ids.row_num = h.row_num
               JOIN jazz_tx tx ON tx.tx_num = h.tx_num
               WHERE h.j_branch_num = 1
                 AND tx.outcome != ?
                 AND tx.global_epoch IS NOT NULL
                 AND tx.global_epoch <= ?
                 AND h.op != 3
                 AND h.{predicate_column} = ?
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                   WHERE newer.row_num = h.row_num
                     AND newer.j_branch_num = 1
                     AND newer_tx.outcome != ?
                     AND newer_tx.global_epoch IS NOT NULL
                     AND newer_tx.global_epoch <= ?
                     AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
                 )
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {current_table} shadow
                   WHERE shadow.row_num = h.row_num
                     AND shadow.j_branch_num = ?
                 )
                 AND {snapshot_policy_sql}
               ORDER BY h.{order_column} DESC, ids.row_id
               LIMIT ?
             ),
             merged AS (
               SELECT * FROM overlay_rows
               UNION ALL
               SELECT * FROM base_rows
               ORDER BY sort_value DESC, sort_row_id
               LIMIT ?
             )
             SELECT {outer_columns}
             FROM merged
             ORDER BY sort_value DESC, sort_row_id"
        );
        let params = vec![
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            predicate_value.clone(),
            rusqlite::types::Value::Integer(limit as i64),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Integer(base_epoch),
            predicate_value,
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Integer(base_epoch),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(limit as i64),
            rusqlite::types::Value::Integer(limit as i64),
        ];
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        Ok(Some(
            rows.map(|row| row_to_view(self.conn, table_name, table, row?))
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    fn read_rows_where_eq_top_field_desc_from_main_source_branch(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Option<Vec<RowView>>> {
        if branch::base_global_epoch(self.conn, self.branch_num)?.is_some()
            || branch::direct_source_nums(self.conn, self.branch_num)? != vec![1]
        {
            return Ok(None);
        }
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        if value.is_null() {
            return Ok(None);
        }
        let order_field = table
            .fields
            .iter()
            .find(|field| field.name == order_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown order field {table_name}.{order_field_name}"
                ))
            })?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let cte_field_columns = field_columns
            .iter()
            .map(|column| format!("current.{column} AS {column}"))
            .collect::<Vec<_>>();
        let outer_field_columns = field_columns.clone();
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let order_column = crate::schema::quote_ident(&crate::schema::storage_column(order_field));
        let predicate_value =
            crate::schema::field_sql_value(field, &value, |ref_table, row_id| {
                row_num(self.conn, row_id).map_err(|err| {
                    crate::Error::new(format!(
                        "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                    ))
                })
            })?;
        let row_columns = format!(
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, current.{order_column} AS sort_value, ids.row_id AS sort_row_id",
            cte_field_columns.join(", "),
            users::user_id_expr("current", "j_created_by")
        );
        let outer_columns = format!(
            "row_id, tx_id, {}, j_created_by",
            outer_field_columns.join(", ")
        );
        let current_table = crate::schema::current_table(table_name);
        let policy_sql = self.read_policy_sql(table)?;
        let sql = format!(
            "WITH
             overlay_rows AS (
               SELECT {row_columns}
               FROM {current_table} current
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = ?
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND current.{predicate_column} = ?
                 AND {policy_sql}
               ORDER BY current.{order_column} DESC, ids.row_id
               LIMIT ?
             ),
             main_rows AS (
               SELECT {row_columns}
               FROM {current_table} current
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = 1
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND current.{predicate_column} = ?
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {current_table} shadow
                   WHERE shadow.row_num = current.row_num
                     AND shadow.j_branch_num = ?
                 )
                 AND {policy_sql}
               ORDER BY current.{order_column} DESC, ids.row_id
               LIMIT ?
             ),
             merged AS (
               SELECT * FROM overlay_rows
               UNION ALL
               SELECT * FROM main_rows
               ORDER BY sort_value DESC, sort_row_id
               LIMIT ?
             )
             SELECT {outer_columns}
             FROM merged
             ORDER BY sort_value DESC, sort_row_id"
        );
        let params = vec![
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            predicate_value.clone(),
            rusqlite::types::Value::Integer(limit as i64),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            predicate_value,
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(limit as i64),
            rusqlite::types::Value::Integer(limit as i64),
        ];
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        Ok(Some(
            rows.map(|row| row_to_view(self.conn, table_name, table, row?))
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    pub(crate) fn read_rows_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        Ok(self
            .read_rows(table_name)?
            .into_iter()
            .filter(|row| {
                if field_name == "id" {
                    JsonValue::String(row.id.clone()) != value
                } else if field_name == "$createdBy" {
                    JsonValue::String(row.created_by.clone()) != value
                } else {
                    row.values
                        .get(field_name)
                        .is_some_and(|row_value| row_value != &value)
                }
            })
            .collect())
    }

    pub(crate) fn read_rows_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field = table
            .fields
            .iter()
            .find(|field| field.name == field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown field {table_name}.{field_name}")))?;
        if !matches!(field.kind, FieldKind::Text) {
            return Err(crate::Error::new(format!(
                "contains only supports text fields, got {table_name}.{field_name}"
            )));
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
                let mut rows =
                    self.read_rows_from_current_where_contains(table_name, field, needle, false)?;
                rows.extend(
                    self.read_main_snapshot_rows(table_name, base_epoch)?
                        .into_iter()
                        .filter(|row| {
                            row.values
                                .get(field_name)
                                .and_then(JsonValue::as_str)
                                .is_some_and(|value| value.contains(needle))
                        }),
                );
                return self.filter_rows_by_effective_branch_policy(table_name, rows);
            }
        }
        self.read_rows_from_current_where_contains(table_name, field, needle, true)
    }

    pub(crate) fn read_row_candidates(&self, table_name: &str, id: &str) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let row_num = row_num(self.conn, id)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "current.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let source_branch_nums = branch::scope_nums(self.conn, self.branch_num)?
            .into_iter()
            .filter(|branch_num| *branch_num != self.branch_num)
            .collect::<Vec<_>>();
        let rows = if source_branch_nums.is_empty() {
            Vec::new()
        } else {
            let source_placeholders = placeholders(source_branch_nums.len());
            let sql = format!(
                "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.j_branch_num IN ({source_placeholders})
               AND current.row_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY current.j_branch_num",
                select_columns.join(", "),
                crate::schema::current_table(table_name),
            );
            let mut params = source_branch_nums
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer)
                .collect::<Vec<_>>();
            params.extend([
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            ]);
            let mut stmt = self.conn.prepare(&sql)?;
            let row_width = 3 + table.fields.len() + 1;
            let mapped = stmt.query_map(params_from_iter(params.iter()), |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?;
            mapped.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut candidates = Vec::new();
        if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
            candidates.extend(
                self.read_main_snapshot_rows(table_name, base_epoch)?
                    .into_iter()
                    .filter(|row| row.id == id),
            );
        }
        let mut visible_candidates = Vec::new();
        for mut row in rows {
            let source_branch_num = branch_num_from_row(&mut row)?;
            let view = row_to_view(self.conn, table_name, table, row)?;
            if self.row_view_visible_in_branch(table_name, &view, source_branch_num)? {
                visible_candidates.push((source_branch_num, view));
            }
        }
        candidates.extend(self.collapse_shadowed_current_rows(visible_candidates)?);
        Ok(candidates)
    }

    pub(crate) fn read_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let parent_field = table
            .fields
            .iter()
            .find(|field| field.name == parent_field)
            .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
        let FieldKind::Ref { table: ref_table } = &parent_field.kind else {
            return Err(crate::Error::new(format!(
                "recursive query field {} is not a ref",
                parent_field.name
            )));
        };
        if ref_table != table_name {
            return Err(crate::Error::new(format!(
                "recursive query field {} must reference {}",
                parent_field.name, table_name
            )));
        }
        if self.branch_num != 1 && branch::base_global_epoch(self.conn, self.branch_num)?.is_some()
        {
            return self.read_recursive_refs_from_visible_rows(table_name, root_id, parent_field);
        }
        let root_num = row_num(self.conn, root_id)?;
        let parent_column =
            crate::schema::quote_ident(&crate::schema::storage_column(parent_field));
        if self.should_read_recursive_refs_from_visible_rows(
            table_name,
            &parent_column,
            root_num,
        )? {
            return self.read_recursive_refs_from_visible_rows(table_name, root_id, parent_field);
        }
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let policy_sql = self.read_policy_sql(table)?;
        let sql = format!(
            "WITH RECURSIVE subtree(row_num) AS (
               SELECT current.row_num
               FROM {current_table} current
               JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
               WHERE current.row_num = ?
                 AND current.j_branch_num = ?
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND {policy_sql}
               UNION
               SELECT child.row_num
               FROM {current_table} child
               JOIN jazz_tx child_tx ON child_tx.tx_num = child.visible_tx_num
               JOIN subtree ON child.{parent_column} = subtree.row_num
               WHERE child.j_branch_num = ?
                 AND child.is_deleted = 0
                 AND child_tx.outcome != ?
                 AND {child_policy_sql}
             )
             SELECT {}
             FROM subtree
             JOIN {current_table} current ON current.row_num = subtree.row_num
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             ORDER BY CASE WHEN current.row_num = ? THEN 0 ELSE 1 END,
                      current.j_created_at,
                      current.row_num",
            select_columns.join(", "),
            current_table = crate::schema::current_table(table_name),
            parent_column = parent_column,
            policy_sql = policy_sql,
            child_policy_sql = self.visibility().current_policy_sql(table, "child")?,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![
                root_num,
                self.branch_num,
                tx::OUTCOME_REJECTED,
                self.branch_num,
                tx::OUTCOME_REJECTED,
                root_num
            ],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }

    fn should_read_recursive_refs_from_visible_rows(
        &self,
        table_name: &str,
        parent_column: &str,
        root_num: i64,
    ) -> Result<bool> {
        // TODO: replace this broad-table heuristic with a planned recursive query
        // strategy, likely a better SQL plan or an optional derived closure index.
        let threshold = RECURSIVE_VISIBLE_ROWS_TABLE_SCAN_THRESHOLD;
        let current_table = crate::schema::current_table(table_name);
        let current_rows = self.conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {current_table} current
                 WHERE current.j_branch_num = ?
                   AND current.is_deleted = 0"
            ),
            params![self.branch_num],
            |row| row.get::<_, i64>(0),
        )?;
        if current_rows > threshold {
            return Ok(false);
        }
        let child_count = self.conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {current_table} child
                 WHERE child.j_branch_num = ?
                   AND child.is_deleted = 0
                   AND child.{parent_column} = ?
                 LIMIT 1"
            ),
            params![self.branch_num, root_num],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(child_count > 0)
    }

    fn read_recursive_refs_from_visible_rows(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &FieldDef,
    ) -> Result<Vec<RowView>> {
        let mut root = None;
        let mut children_by_parent: BTreeMap<String, Vec<RowView>> = BTreeMap::new();
        for row in self.read_rows(table_name)? {
            if row.id == root_id {
                root = Some(row);
                continue;
            }
            if let Some(parent_id) = row
                .values
                .get(&parent_field.name)
                .and_then(JsonValue::as_str)
            {
                children_by_parent
                    .entry(parent_id.to_owned())
                    .or_default()
                    .push(row);
            }
        }
        let Some(root) = root else {
            return Ok(Vec::new());
        };
        for children in children_by_parent.values_mut() {
            children.sort_by(|left, right| left.id.cmp(&right.id));
        }
        let mut ordered = vec![root];
        let mut frontier = vec![root_id.to_owned()];
        while !frontier.is_empty() {
            let mut next_ids = Vec::new();
            for parent_id in frontier {
                if let Some(children) = children_by_parent.remove(&parent_id) {
                    for row in children {
                        next_ids.push(row.id.clone());
                        ordered.push(row);
                    }
                }
            }
            frontier = next_ids;
        }
        Ok(ordered)
    }

    fn read_policy_sql(&self, table: &crate::schema::TableDef) -> Result<String> {
        self.visibility().current_policy_sql(table, "current")
    }

    fn read_tier_sql(&self, tx_alias: &str) -> String {
        tier_visibility_sql(tx_alias, self.read_tier)
    }

    fn read_rows_from_history_at_tier(
        &self,
        table_name: &str,
        tier: ReadTier,
    ) -> Result<Vec<RowView>> {
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let shadowed_main_ids =
            self.tier_visible_row_ids_for_branches(table_name, tier, &scope_nums)?;
        let mut rows = self.read_history_rows_at_tier(table_name, tier, &scope_nums, None)?;

        if self.branch_num != 1 && !scope_nums.contains(&1) {
            let main_epoch_ceiling = branch::base_global_epoch(self.conn, self.branch_num)?;
            rows.extend(
                self.read_history_rows_at_tier(table_name, tier, &[1], main_epoch_ceiling)?
                    .into_iter()
                    .filter(|(_, row)| !shadowed_main_ids.contains(&row.id)),
            );
        }

        self.collapse_shadowed_current_rows(rows)
    }

    fn read_history_rows_at_tier(
        &self,
        table_name: &str,
        tier: ReadTier,
        branch_nums: &[i64],
        max_global_epoch: Option<i64>,
    ) -> Result<Vec<(i64, RowView)>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "h.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ));
        let branch_placeholders = placeholders(branch_nums.len());
        let tx_tier_sql = tier_visibility_sql("tx", tier);
        let newer_tier_sql = tier_visibility_sql("newer_tx", tier);
        let newer_order_sql = tier_newer_order_sql("newer_tx", "tx", tier);
        let tx_epoch_sql = if max_global_epoch.is_some() {
            "AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?"
        } else {
            ""
        };
        let newer_epoch_sql = if max_global_epoch.is_some() {
            "AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num IN ({branch_placeholders})
               AND {tx_tier_sql}
               {tx_epoch_sql}
               AND h.op != 3
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = h.j_branch_num
                   AND {newer_tier_sql}
                   {newer_epoch_sql}
                   AND {newer_order_sql}
               )
             ORDER BY h.j_created_at DESC, ids.row_id",
            select_columns.join(", "),
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
        );
        let mut params = branch_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        if let Some(max_global_epoch) = max_global_epoch {
            params.push(rusqlite::types::Value::Integer(max_global_epoch));
        }
        if let Some(max_global_epoch) = max_global_epoch {
            params.push(rusqlite::types::Value::Integer(max_global_epoch));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| {
            let mut row = row?;
            let branch_num = branch_num_from_row(&mut row)?;
            Ok((branch_num, row_to_view(self.conn, table_name, table, row)?))
        })
        .collect()
    }

    fn tier_visible_row_ids_for_branches(
        &self,
        table_name: &str,
        tier: ReadTier,
        branch_nums: &[i64],
    ) -> Result<BTreeSet<String>> {
        let branch_placeholders = placeholders(branch_nums.len());
        let tx_tier_sql = tier_visibility_sql("tx", tier);
        let sql = format!(
            "SELECT DISTINCT ids.row_id
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num IN ({branch_placeholders})
               AND {tx_tier_sql}
             ORDER BY ids.row_id",
            crate::schema::history_table(table_name),
        );
        let params = branch_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            row.get::<_, String>(0)
        })?;
        rows.collect::<std::result::Result<BTreeSet<_>, _>>()
            .map_err(Into::into)
    }

    fn visibility(&self) -> ReadVisibility<'_> {
        ReadVisibility {
            conn: self.conn,
            schema: self.schema,
            branch_num: self.branch_num,
            user: self.user,
            bypass_policy: self.bypass_policy,
        }
    }

    fn filter_rows_by_effective_branch_policy(
        &self,
        table_name: &str,
        rows: Vec<RowView>,
    ) -> Result<Vec<RowView>> {
        if self.bypass_policy {
            return Ok(rows);
        }
        let table = self.schema.table_def(table_name)?;
        let PolicyDef::RefReadable { field } = &table.read_policy else {
            return Ok(rows);
        };
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == *field)
            .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
        let FieldKind::Ref {
            table: parent_table,
        } = &field.kind
        else {
            return Ok(rows);
        };
        let visible_parent_ids = self
            .read_rows(parent_table)?
            .into_iter()
            .map(|row| row.id)
            .collect::<std::collections::BTreeSet<_>>();
        Ok(rows
            .into_iter()
            .filter(|row| {
                row.values
                    .get(&field.name)
                    .and_then(JsonValue::as_str)
                    .is_some_and(|parent_id| visible_parent_ids.contains(parent_id))
            })
            .collect())
    }

    fn defer_query_window_until_effective_branch_policy(
        &self,
        table: &crate::schema::TableDef,
        query: &BuiltQuery,
        base_epoch: Option<i64>,
    ) -> bool {
        !self.bypass_policy
            && self.branch_num != 1
            && base_epoch.is_some()
            && (query.limit.is_some() || query.offset.is_some())
            && matches!(table.read_policy, PolicyDef::RefReadable { .. })
    }

    fn row_view_visible_in_branch(
        &self,
        table_name: &str,
        row: &RowView,
        branch_num: i64,
    ) -> Result<bool> {
        if self.bypass_policy {
            return Ok(true);
        }
        let table = self.schema.table_def(table_name)?;
        match &table.read_policy {
            PolicyDef::AllowAll => Ok(true),
            PolicyDef::CreatedByUser => Ok(row.created_by == self.user),
            PolicyDef::RefReadable { field } => {
                let field = table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
                let FieldKind::Ref {
                    table: parent_table,
                } = &field.kind
                else {
                    return Ok(false);
                };
                let Some(parent_id) = row.values.get(&field.name).and_then(JsonValue::as_str)
                else {
                    return Ok(false);
                };
                let parent =
                    self.read_current_row_in_branch(parent_table, parent_id, branch_num)?;
                match parent {
                    Some(parent) => {
                        self.row_view_visible_in_branch(parent_table, &parent, branch_num)
                    }
                    None => Ok(false),
                }
            }
            PolicyDef::BranchFieldEquals { .. } => Ok(false),
        }
    }

    fn read_current_row_in_branch(
        &self,
        table_name: &str,
        id: &str,
        branch_num: i64,
    ) -> Result<Option<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let row_num = row_num(self.conn, id)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.row_num = ?
               AND current.j_branch_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
        );
        let row_width = 2 + table.fields.len() + 1;
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows =
            stmt.query_map(params![row_num, branch_num, tx::OUTCOME_REJECTED], |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?;
        rows.next()
            .transpose()?
            .map(|row| row_to_view(self.conn, table_name, table, row))
            .transpose()
    }

    fn read_rows_from_current(&self, table_name: &str, overlay_main: bool) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "current.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num IN ({scope_placeholders})
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num IN ({scope_placeholders})
                   )
                 )
               )
               AND {tier_sql}
               AND NOT (
                 current.j_branch_num != ?
                 AND EXISTS (
                   SELECT 1
                   FROM {current_table} active_current
                   WHERE active_current.row_num = current.row_num
                     AND active_current.j_branch_num = ?
                 )
               )
               AND {policy_sql}
             ORDER BY ids.row_id",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
            tier_sql = self.read_tier_sql("tx"),
        );
        let mut params = scope_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.extend([
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        params.extend(
            scope_nums
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        params.extend([
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let rows = rows
            .map(|row| {
                let mut row = row?;
                let branch_num = branch_num_from_row(&mut row)?;
                Ok((branch_num, row_to_view(self.conn, table_name, table, row)?))
            })
            .collect::<Result<Vec<_>>>()?;
        self.collapse_shadowed_current_rows(rows)
    }

    fn read_rows_from_current_where_eq(
        &self,
        table_name: &str,
        field: &FieldDef,
        value: &JsonValue,
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "current.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let (predicate_sql, predicate_value) = if value.is_null() {
            if !field.nullable {
                return Err(crate::Error::new(format!(
                    "expected non-null for {}",
                    field.name
                )));
            }
            ("IS NULL".to_owned(), None)
        } else {
            (
                "= ?".to_owned(),
                Some(crate::schema::field_sql_value(
                    field,
                    value,
                    |ref_table, row_id| {
                        row_num(self.conn, row_id).map_err(|err| {
                            crate::Error::new(format!(
                                "failed to resolve ref {ref_table}.{row_id} for equality predicate: {err}"
                            ))
                        })
                    },
                )?),
            )
        };
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num IN ({scope_placeholders})
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num IN ({scope_placeholders})
                   )
                 )
               )
               AND tx.outcome != ?
               AND NOT (
                 current.j_branch_num != ?
                 AND EXISTS (
                   SELECT 1
                   FROM {current_table} active_current
                   WHERE active_current.row_num = current.row_num
                     AND active_current.j_branch_num = ?
                 )
               )
               AND current.{predicate_column} {predicate_sql}
               AND {policy_sql}
             ORDER BY ids.row_id",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut params = scope_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.extend([
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        params.extend(
            scope_nums
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        params.extend([
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        if let Some(predicate_value) = predicate_value {
            params.push(predicate_value);
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let rows = rows
            .map(|row| {
                let mut row = row?;
                let branch_num = branch_num_from_row(&mut row)?;
                Ok((branch_num, row_to_view(self.conn, table_name, table, row)?))
            })
            .collect::<Result<Vec<_>>>()?;
        self.collapse_shadowed_current_rows(rows)
    }

    fn read_rows_from_current_where_id_in(
        &self,
        table_name: &str,
        row_ids: &[String],
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        let values = row_ids
            .iter()
            .map(|row_id| rusqlite::types::Value::Text(row_id.clone()))
            .collect::<Vec<_>>();
        self.read_rows_from_current_where_in_sql(
            table_name,
            "ids.row_id",
            values,
            false,
            overlay_main,
        )
    }

    fn read_rows_from_current_where_created_by_in(
        &self,
        table_name: &str,
        user_nums: &[i64],
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        let values = user_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        self.read_rows_from_current_where_in_sql(
            table_name,
            "current.j_created_by",
            values,
            false,
            overlay_main,
        )
    }

    fn read_rows_from_current_where_in(
        &self,
        table_name: &str,
        field: &FieldDef,
        values: &[JsonValue],
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        let mut sql_values = Vec::new();
        let mut includes_null = false;
        for value in values {
            let sql_value = crate::schema::field_sql_value(field, value, |ref_table, row_id| {
                row_num(self.conn, row_id).map_err(|err| {
                    crate::Error::new(format!(
                        "failed to resolve ref {ref_table}.{row_id} for IN predicate: {err}"
                    ))
                })
            })?;
            if matches!(sql_value, rusqlite::types::Value::Null) {
                includes_null = true;
            } else {
                sql_values.push(sql_value);
            }
        }
        let predicate_column = format!(
            "current.{}",
            crate::schema::quote_ident(&crate::schema::storage_column(field))
        );
        self.read_rows_from_current_where_in_sql(
            table_name,
            &predicate_column,
            sql_values,
            includes_null,
            overlay_main,
        )
    }

    fn read_rows_from_current_where_in_sql(
        &self,
        table_name: &str,
        predicate_expr: &str,
        values: Vec<rusqlite::types::Value>,
        includes_null: bool,
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        if values.is_empty() && !includes_null {
            return Ok(Vec::new());
        }
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "current.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let predicate_sql = in_predicate_sql(predicate_expr, values.len(), includes_null);
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num IN ({scope_placeholders})
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num IN ({scope_placeholders})
                   )
                 )
               )
               AND tx.outcome != ?
               AND NOT (
                 current.j_branch_num != ?
                 AND EXISTS (
                   SELECT 1
                   FROM {current_table} active_current
                   WHERE active_current.row_num = current.row_num
                     AND active_current.j_branch_num = ?
                 )
               )
               AND {predicate_sql}
               AND {policy_sql}
             ORDER BY ids.row_id",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut params = scope_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.extend([
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        params.extend(
            scope_nums
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        params.extend([
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        params.extend(values);
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let rows = rows
            .map(|row| {
                let mut row = row?;
                let branch_num = branch_num_from_row(&mut row)?;
                Ok((branch_num, row_to_view(self.conn, table_name, table, row)?))
            })
            .collect::<Result<Vec<_>>>()?;
        self.collapse_shadowed_current_rows(rows)
    }

    fn user_nums_for_values(&self, values: &[JsonValue]) -> Result<Vec<i64>> {
        values
            .iter()
            .filter_map(JsonValue::as_str)
            .map(|user_id| {
                self.conn
                    .query_row(
                        "SELECT user_num FROM jazz_user WHERE user_id = ?",
                        params![user_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(Into::into)
            })
            .filter_map(|result| result.transpose())
            .collect()
    }
    fn read_rows_from_current_where_contains(
        &self,
        table_name: &str,
        field: &FieldDef,
        needle: &str,
        overlay_main: bool,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "current.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("current", "j_created_by")
        ));
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let scope_nums = branch::scope_nums(self.conn, self.branch_num)?;
        let scope_placeholders = placeholders(scope_nums.len());
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num IN ({scope_placeholders})
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num IN ({scope_placeholders})
                   )
                 )
               )
               AND tx.outcome != ?
               AND NOT (
                 current.j_branch_num != ?
                 AND EXISTS (
                   SELECT 1
                   FROM {current_table} active_current
                   WHERE active_current.row_num = current.row_num
                     AND active_current.j_branch_num = ?
                 )
               )
               AND instr(current.{predicate_column}, ?) > 0
               AND {policy_sql}
             ORDER BY ids.row_id",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut params = scope_nums
            .iter()
            .copied()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.extend([
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
        ]);
        params.extend(
            scope_nums
                .iter()
                .copied()
                .map(rusqlite::types::Value::Integer),
        );
        params.extend([
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Text(needle.to_owned()),
        ]);
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let rows = rows
            .map(|row| {
                let mut row = row?;
                let branch_num = branch_num_from_row(&mut row)?;
                Ok((branch_num, row_to_view(self.conn, table_name, table, row)?))
            })
            .collect::<Result<Vec<_>>>()?;
        self.collapse_shadowed_current_rows(rows)
    }

    fn collapse_shadowed_current_rows(&self, rows: Vec<(i64, RowView)>) -> Result<Vec<RowView>> {
        let depths = branch::scope_depths(self.conn, self.branch_num)?;
        let mut min_depth_by_row: BTreeMap<String, i64> = BTreeMap::new();
        for (branch_num, row) in &rows {
            let depth = depths
                .get(branch_num)
                .copied()
                .unwrap_or(if *branch_num == 1 {
                    i64::MAX / 4
                } else {
                    i64::MAX / 2
                });
            min_depth_by_row
                .entry(row.id.clone())
                .and_modify(|min_depth| *min_depth = (*min_depth).min(depth))
                .or_insert(depth);
        }
        Ok(rows
            .into_iter()
            .filter_map(|(branch_num, row)| {
                let depth = depths
                    .get(&branch_num)
                    .copied()
                    .unwrap_or(if branch_num == 1 {
                        i64::MAX / 4
                    } else {
                        i64::MAX / 2
                    });
                (min_depth_by_row.get(&row.id) == Some(&depth)).then_some(row)
            })
            .collect())
    }

    fn read_main_snapshot_rows(&self, table_name: &str, base_epoch: i64) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let policy_sql = if self.bypass_policy {
            "1 = 1".to_owned()
        } else {
            self.visibility()
                .snapshot_policy_sql(table, "h", base_epoch)?
        };
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.push(format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ));
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND {policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM {current_table} branch_current
                 WHERE branch_current.row_num = h.row_num
                   AND branch_current.j_branch_num = ?
               )
             ORDER BY h.j_created_at DESC, ids.row_id",
            select_columns.join(", "),
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = policy_sql,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![
                tx::OUTCOME_REJECTED,
                base_epoch,
                tx::OUTCOME_REJECTED,
                base_epoch,
                self.branch_num
            ],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }
}

fn query_candidate_select_columns(
    table: &crate::schema::TableDef,
    row_alias: &str,
    ids_alias: &str,
    tx_alias: &str,
    branch_depth_sql: &str,
) -> Vec<String> {
    let mut select_columns = vec![
        format!("{branch_depth_sql} AS j_query_branch_depth"),
        format!("{ids_alias}.row_id AS j_query_row_id"),
        format!("{tx_alias}.tx_id AS j_query_tx_id"),
    ];
    select_columns.extend(table.fields.iter().map(|field| {
        let column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        format!("{row_alias}.{column} AS {column}")
    }));
    select_columns.extend([
        format!(
            "{} AS j_query_created_by",
            users::user_id_expr(row_alias, "j_created_by")
        ),
        format!("{row_alias}.j_created_at AS j_query_created_at"),
        format!("{row_alias}.j_updated_at AS j_query_updated_at"),
        format!("{row_alias}.row_num AS j_query_row_num"),
    ]);
    select_columns
}

fn branch_depth_case_sql(conn: &Connection, branch_num: i64, row_alias: &str) -> Result<String> {
    let arms = branch::scope_depths(conn, branch_num)?
        .into_iter()
        .map(|(branch_num, depth)| format!("WHEN {branch_num} THEN {depth}"))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!(
        "CASE {row_alias}.j_branch_num {arms} ELSE {} END",
        i64::MAX / 2
    ))
}

fn query_column<'a>(table: &'a crate::schema::TableDef, column: &str) -> Result<QueryColumn<'a>> {
    match column {
        "id" => Ok(QueryColumn::Id),
        "$createdBy" => Ok(QueryColumn::CreatedBy),
        "$createdAt" => Ok(QueryColumn::CreatedAt),
        "$updatedAt" => Ok(QueryColumn::UpdatedAt),
        column => table
            .fields
            .iter()
            .find(|field| field.name == column)
            .map(QueryColumn::Field)
            .ok_or_else(|| crate::Error::new(format!("unknown field {}.{column}", table.name))),
    }
}

fn source_query_column_sql(column: &QueryColumn<'_>, row_alias: &str, ids_alias: &str) -> String {
    match column {
        QueryColumn::Id => format!("{ids_alias}.row_id"),
        QueryColumn::CreatedBy => users::user_id_expr(row_alias, "j_created_by"),
        QueryColumn::CreatedAt => format!("{row_alias}.j_created_at"),
        QueryColumn::UpdatedAt => format!("{row_alias}.j_updated_at"),
        QueryColumn::Field(field) => {
            format!(
                "{row_alias}.{}",
                crate::schema::quote_ident(&crate::schema::storage_column(field))
            )
        }
    }
}

fn final_query_column_sql(column: &QueryColumn<'_>) -> String {
    match column {
        QueryColumn::Id => "j_query_row_id".to_owned(),
        QueryColumn::CreatedBy => "j_query_created_by".to_owned(),
        QueryColumn::CreatedAt => "j_query_created_at".to_owned(),
        QueryColumn::UpdatedAt => "j_query_updated_at".to_owned(),
        QueryColumn::Field(field) => {
            crate::schema::quote_ident(&crate::schema::storage_column(field))
        }
    }
}

fn source_query_order_column_sql(
    column: &QueryColumn<'_>,
    row_alias: &str,
    ids_alias: &str,
) -> String {
    match column {
        QueryColumn::Field(field) if matches!(field.kind, FieldKind::Ref { .. }) => {
            let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
            format!(
                "(SELECT ref_order_ids.row_id FROM jazz_row_id ref_order_ids WHERE ref_order_ids.row_num = {row_alias}.{ref_column})"
            )
        }
        _ => source_query_column_sql(column, row_alias, ids_alias),
    }
}

fn final_query_order_column_sql(column: &QueryColumn<'_>) -> String {
    match column {
        QueryColumn::Field(field) if matches!(field.kind, FieldKind::Ref { .. }) => {
            let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
            format!(
                "(SELECT ref_order_ids.row_id FROM jazz_row_id ref_order_ids WHERE ref_order_ids.row_num = {ref_column})"
            )
        }
        _ => final_query_column_sql(column),
    }
}

fn query_column_is_text(column: &QueryColumn<'_>) -> bool {
    matches!(column, QueryColumn::Id | QueryColumn::CreatedBy)
        || matches!(column, QueryColumn::Field(field) if matches!(field.kind, FieldKind::Text))
}

fn window_value_to_i64(value: usize, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| crate::Error::new(format!("query {field} is too large")))
}

fn append_query_window_sql(
    sql: &mut String,
    params: &mut Vec<SqlValue>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<()> {
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ?");
        params.push(SqlValue::Integer(window_value_to_i64(limit, "limit")?));
    } else if offset.is_some() {
        sql.push_str(" LIMIT -1");
    }
    if let Some(offset) = offset {
        sql.push_str(" OFFSET ?");
        params.push(SqlValue::Integer(window_value_to_i64(offset, "offset")?));
    }
    Ok(())
}

fn apply_query_window(
    rows: Vec<RowView>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<RowView> {
    let mut rows = rows
        .into_iter()
        .skip(offset.unwrap_or(0))
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

fn repair_support_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut support_query = query.clone();
    support_query.offset = None;
    support_query.limit = query
        .limit
        .map(|limit| {
            offset
                .checked_add(limit)
                .ok_or_else(|| crate::Error::new("query limit plus offset is too large"))
        })
        .transpose()?;
    Ok(support_query)
}

fn row_to_view(
    conn: &Connection,
    table_name: &str,
    table: &crate::schema::TableDef,
    raw: Vec<rusqlite::types::Value>,
) -> Result<RowView> {
    let mut values = BTreeMap::new();
    for (idx, field) in table.fields.iter().enumerate() {
        values.insert(
            field.name.clone(),
            sql_value_to_json(conn, field, &raw[idx + 2])?,
        );
    }
    Ok(RowView {
        table: table_name.to_owned(),
        id: text_value(&raw[0], "row_id")?,
        tx_id: text_value(&raw[1], "tx_id")?,
        values,
        created_by: text_value(&raw[2 + table.fields.len()], "j_created_by")?,
        created_at: raw
            .get(3 + table.fields.len())
            .map(|value| integer_value(value, "j_created_at"))
            .transpose()?
            .unwrap_or(0),
        conflict_count: 0,
    })
}

fn branch_num_from_row(raw: &mut Vec<rusqlite::types::Value>) -> Result<i64> {
    match raw.remove(0) {
        rusqlite::types::Value::Integer(value) => Ok(value),
        _ => Err(crate::Error::new("expected branch num")),
    }
}

fn json_sort_key(value: Option<&JsonValue>) -> String {
    match value {
        Some(JsonValue::String(value)) => format!("s:{value}"),
        Some(JsonValue::Number(value)) => format!("n:{value:>020}"),
        Some(JsonValue::Bool(value)) => format!("b:{value}"),
        Some(value) => format!("j:{value}"),
        None => String::new(),
    }
}

fn current_created_at_by_row_id(
    conn: &Connection,
    table_name: &str,
) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT ids.row_id, MAX(current.j_created_at)
         FROM {} current
         JOIN jazz_row_id ids ON ids.row_num = current.row_num
         GROUP BY ids.row_id",
        crate::schema::current_table(table_name)
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

fn placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn tier_visibility_sql(tx_alias: &str, tier: ReadTier) -> String {
    match tier {
        ReadTier::Local => format!("{tx_alias}.outcome != {}", tx::OUTCOME_REJECTED),
        ReadTier::Edge => format!(
            "{tx_alias}.outcome != {} AND ({tx_alias}.global_epoch IS NOT NULL OR EXISTS (SELECT 1 FROM jazz_tx_receipt tier_receipt WHERE tier_receipt.tx_num = {tx_alias}.tx_num AND tier_receipt.tier >= {}))",
            tx::OUTCOME_REJECTED,
            tx::TIER_EDGE
        ),
        ReadTier::Global => format!(
            "{tx_alias}.outcome != {} AND {tx_alias}.global_epoch IS NOT NULL",
            tx::OUTCOME_REJECTED
        ),
    }
}

fn tier_newer_order_sql(newer_tx_alias: &str, current_tx_alias: &str, tier: ReadTier) -> String {
    match tier {
        ReadTier::Global => format!(
            "({newer_tx_alias}.global_epoch > {current_tx_alias}.global_epoch
              OR ({newer_tx_alias}.global_epoch = {current_tx_alias}.global_epoch
                  AND {newer_tx_alias}.tx_num > {current_tx_alias}.tx_num))"
        ),
        ReadTier::Local | ReadTier::Edge => {
            format!("{newer_tx_alias}.tx_num > {current_tx_alias}.tx_num")
        }
    }
}

fn in_predicate_sql(predicate_expr: &str, value_count: usize, includes_null: bool) -> String {
    let mut clauses = Vec::new();
    if value_count > 0 {
        clauses.push(format!(
            "{predicate_expr} IN ({})",
            placeholders(value_count)
        ));
    }
    if includes_null {
        clauses.push(format!("{predicate_expr} IS NULL"));
    }
    format!("({})", clauses.join(" OR "))
}

pub(crate) fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (_, rusqlite::types::Value::Null) if field.nullable => Ok(JsonValue::Null),
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => {
            Ok(JsonValue::String(public_row_id(conn, *row_num)?))
        }
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

pub(crate) fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}

fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema, storage, tx, Storage};
    use rusqlite::params;
    use serde_json::json;

    fn todo_app_schema() -> SchemaDef {
        SchemaDef::new()
            .table("projects", |table| {
                table.text("title");
            })
            .table("todos", |table| {
                table.text("title");
                table.bool("done");
                table.ref_("project", "projects");
                table.index("open_created", ["done", "$createdAt"]);
                table.index("created", ["$createdAt"]);
                table.index("by_title", ["title"]);
            })
    }

    #[test]
    fn top_created_at_query_returns_only_latest_page() -> Result<()> {
        let schema = todo_app_schema();
        let conn = storage::open(Storage::Memory)?;
        schema::install(&conn, &schema)?;
        seed_todos(&conn, 100)?;

        let context = QueryContext {
            conn: &conn,
            schema: &schema,
            branch_num: 1,
            user: "alice",
            bypass_policy: false,
            read_tier: ReadTier::Local,
        };

        let query = BuiltQuery::from_json_value(json!({
            "table": "todos",
            "conditions": [{"column": "done", "op": "eq", "value": false}],
            "orderBy": [["$createdAt", "desc"]],
            "limit": 10,
        }))?;
        let rows = context.read_rows_for_built_query(&query)?;

        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].id, "todo-100");
        assert_eq!(rows[9].id, "todo-91");
        Ok(())
    }

    fn seed_todos(conn: &Connection, count: i64) -> Result<()> {
        users::ensure_user(conn, "alice")?;
        conn.execute(
            "INSERT INTO jazz_node (node_num, node_id) VALUES (1, 'node-a')",
            [],
        )?;
        conn.execute(
            "INSERT INTO jazz_tx
              (tx_num, tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
             VALUES (1, 'tx-1', 1, 1, NULL, ?, ?, ?, 1, '{}')",
            params![tx::KIND_DATA, tx::MODE_MERGEABLE, tx::OUTCOME_ACCEPTED],
        )?;
        conn.execute(
            "INSERT INTO jazz_row_id (row_num, row_id)
             VALUES (1, 'project-1')",
            [],
        )?;
        conn.execute(
            "INSERT INTO projects__schema_v1_current
              (row_num, j_branch_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (1, 1, 1, 0, 'Project', 1, 1, 1, 1)",
            [],
        )?;
        for index in 1..=count {
            let row_num = index + 1;
            let row_id = format!("todo-{index}");
            conn.execute(
                "INSERT INTO jazz_row_id (row_num, row_id)
                 VALUES (?, ?)",
                params![row_num, row_id],
            )?;
            conn.execute(
                "INSERT INTO todos__schema_v1_current
                  (row_num, j_branch_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, 1, 1, 0, ?, 0, 1, ?, ?, 1, 1)",
                params![row_num, format!("Todo {index}"), index, index],
            )?;
        }
        Ok(())
    }
}
