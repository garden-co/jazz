use crate::rows::{public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::types::RowView;
use crate::value::Value as JsonValue;
use crate::{branch, policy, tx, users, Result};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use std::collections::BTreeMap;

const RECURSIVE_VISIBLE_ROWS_TABLE_SCAN_THRESHOLD: i64 = 50_000;
const MAX_MULTI_VALUE_TOP_PAGE_VALUES: usize = 400;

pub(crate) struct QueryContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) branch_num: i64,
    pub(crate) user: &'a str,
    pub(crate) bypass_policy: bool,
}

impl QueryContext<'_> {
    pub(crate) fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
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
                   ORDER BY current.j_created_at DESC, current.row_num
                 ) AS query_rank
               FROM query_values
               JOIN {current_table} current
                 ON current.{predicate_column} = query_values.predicate_value
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
            let mut rows = self.read_rows_where_eq(table_name, field_name, value)?;
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
             WHERE current.j_branch_num = 1
               AND current.is_deleted = 0
               AND tx.outcome != ?
               AND current.{predicate_column} = ?
               AND {policy_sql}
             ORDER BY current.{order_column} DESC, current.row_num
             LIMIT ?",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![tx::OUTCOME_REJECTED, predicate_value, limit as i64],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
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
                   ORDER BY current.{order_column} DESC, current.row_num
                 ) AS query_rank
               FROM query_values
               JOIN {current_table} current
                 ON current.{predicate_column} = query_values.predicate_value
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, current.{order_column} AS sort_value, current.row_num AS sort_row_num",
            current_field_columns.join(", "),
            users::user_id_expr("current", "j_created_by")
        );
        let history_row_columns = format!(
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, h.{order_column} AS sort_value, h.row_num AS sort_row_num",
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
            policy::snapshot_read_policy_sql_for_alias(
                self.schema,
                table,
                "h",
                self.user,
                base_epoch,
            )?
        };
        let sql = format!(
            "WITH
             overlay_rows AS (
               SELECT {current_row_columns}
               FROM {current_table} current
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = ?
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND current.{predicate_column} = ?
                 AND {current_policy_sql}
               ORDER BY current.{order_column} DESC, current.row_num
               LIMIT ?
             ),
             base_rows AS (
               SELECT {history_row_columns}
               FROM {history_table} h
               JOIN jazz_row_id ids ON ids.row_num = h.row_num
               JOIN jazz_tx_public tx ON tx.tx_num = h.tx_num
               WHERE h.j_branch_num = 1
                 AND tx.outcome != ?
                 AND tx.global_epoch IS NOT NULL
                 AND tx.global_epoch <= ?
                 AND h.op != 3
                 AND h.{predicate_column} = ?
                 AND NOT EXISTS (
                   SELECT 1
                   FROM {history_table} newer
                   JOIN jazz_tx_public newer_tx ON newer_tx.tx_num = newer.tx_num
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
               ORDER BY h.{order_column} DESC, h.row_num
               LIMIT ?
             ),
             merged AS (
               SELECT * FROM overlay_rows
               UNION ALL
               SELECT * FROM base_rows
               ORDER BY sort_value DESC, sort_row_num
               LIMIT ?
             )
             SELECT {outer_columns}
             FROM merged
             ORDER BY sort_value DESC, sort_row_num"
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
            "ids.row_id AS row_id, tx.tx_id AS tx_id, {}, {} AS j_created_by, current.{order_column} AS sort_value, current.row_num AS sort_row_num",
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
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
               WHERE current.j_branch_num = ?
                 AND current.is_deleted = 0
                 AND tx.outcome != ?
                 AND current.{predicate_column} = ?
                 AND {policy_sql}
               ORDER BY current.{order_column} DESC, current.row_num
               LIMIT ?
             ),
             main_rows AS (
               SELECT {row_columns}
               FROM {current_table} current
               JOIN jazz_row_id ids ON ids.row_num = current.row_num
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
               ORDER BY current.{order_column} DESC, current.row_num
               LIMIT ?
             ),
             merged AS (
               SELECT * FROM overlay_rows
               UNION ALL
               SELECT * FROM main_rows
               ORDER BY sort_value DESC, sort_row_num
               LIMIT ?
             )
             SELECT {outer_columns}
             FROM merged
             ORDER BY sort_value DESC, sort_row_num"
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
               JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
             ORDER BY CASE WHEN current.row_num = ? THEN 0 ELSE 1 END,
                      current.j_created_at,
                      current.row_num",
            select_columns.join(", "),
            current_table = crate::schema::current_table(table_name),
            parent_column = parent_column,
            policy_sql = policy_sql,
            child_policy_sql = policy::branch_read_policy_sql_for_alias(
                self.schema,
                table,
                "child",
                self.user,
                self.branch_num
            )?,
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
        if self.bypass_policy {
            Ok("1 = 1".to_owned())
        } else {
            policy::branch_read_policy_sql_for_alias(
                self.schema,
                table,
                "current",
                self.user,
                self.branch_num,
            )
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
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
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
             ORDER BY current.j_created_at DESC, current.row_num",
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
             ORDER BY current.j_created_at DESC, current.row_num",
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
             JOIN jazz_tx_public tx ON tx.tx_num = current.visible_tx_num
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
             ORDER BY current.j_created_at DESC, current.row_num",
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
            policy::snapshot_read_policy_sql_for_alias(
                self.schema,
                table,
                "h",
                self.user,
                base_epoch,
            )?
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
             JOIN jazz_tx_public tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND {policy_sql}
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx_public newer_tx ON newer_tx.tx_num = newer.tx_num
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
             ORDER BY h.j_created_at DESC, h.row_num",
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
        (FieldKind::Bytes, rusqlite::types::Value::Blob(value)) => {
            Ok(JsonValue::String(bytes_to_hex(value)))
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

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}
