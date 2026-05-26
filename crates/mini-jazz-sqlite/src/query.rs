use crate::rows::{public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::types::RowView;
use crate::{branch, policy, tx, Result};
use rusqlite::{params, params_from_iter, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) struct QueryContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) branch_num: i64,
    pub(crate) principal: &'a str,
    pub(crate) trusted: bool,
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
            "source.source_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let sql = format!(
            "SELECT {}
             FROM jazz_branch_source source
             JOIN {} current ON current.j_branch_num = source.source_branch_num
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE source.branch_num = ?
               AND current.row_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY source.source_branch_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![self.branch_num, row_num, tx::OUTCOME_REJECTED],
            |row| {
                (0..row_width)
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        let mut candidates = Vec::new();
        if let Some(base_epoch) = branch::base_global_epoch(self.conn, self.branch_num)? {
            candidates.extend(
                self.read_main_snapshot_rows(table_name, base_epoch)?
                    .into_iter()
                    .filter(|row| row.id == id),
            );
        }
        for row in rows {
            let mut row = row?;
            let source_branch_num = match row.remove(0) {
                rusqlite::types::Value::Integer(value) => value,
                _ => return Err(crate::Error::new("expected source branch num")),
            };
            let view = row_to_view(self.conn, table_name, table, row)?;
            if self.row_view_visible_in_branch(table_name, &view, source_branch_num)? {
                candidates.push(view);
            }
        }
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
        select_columns.push("current.j_created_by".to_owned());
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
            child_policy_sql = policy::branch_read_policy_sql_for_alias(
                self.schema,
                table,
                "child",
                self.principal,
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

    fn read_recursive_refs_from_visible_rows(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &FieldDef,
    ) -> Result<Vec<RowView>> {
        let mut rows_by_id = self
            .read_rows(table_name)?
            .into_iter()
            .map(|row| (row.id.clone(), row))
            .collect::<BTreeMap<_, _>>();
        let Some(root) = rows_by_id.remove(root_id) else {
            return Ok(Vec::new());
        };
        let mut ordered = vec![root];
        let mut frontier = vec![root_id.to_owned()];
        while !frontier.is_empty() {
            let mut next_ids = Vec::new();
            let ids = rows_by_id.keys().cloned().collect::<Vec<_>>();
            for id in ids {
                let Some(row) = rows_by_id.get(&id) else {
                    continue;
                };
                let parent_id = row
                    .values
                    .get(&parent_field.name)
                    .and_then(JsonValue::as_str);
                if parent_id.is_some_and(|parent_id| frontier.iter().any(|seen| seen == parent_id))
                {
                    next_ids.push(id);
                }
            }
            for id in &next_ids {
                if let Some(row) = rows_by_id.remove(id) {
                    ordered.push(row);
                }
            }
            frontier = next_ids;
        }
        Ok(ordered)
    }

    fn read_policy_sql(&self, table: &crate::schema::TableDef) -> Result<String> {
        if self.trusted {
            Ok("1 = 1".to_owned())
        } else {
            policy::branch_read_policy_sql_for_alias(
                self.schema,
                table,
                "current",
                self.principal,
                self.branch_num,
            )
        }
    }

    fn filter_rows_by_effective_branch_policy(
        &self,
        table_name: &str,
        rows: Vec<RowView>,
    ) -> Result<Vec<RowView>> {
        if self.trusted {
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
        if self.trusted {
            return Ok(true);
        }
        let table = self.schema.table_def(table_name)?;
        match &table.read_policy {
            PolicyDef::AllowAll => Ok(true),
            PolicyDef::CreatedByPrincipal => Ok(row.created_by == self.principal),
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
        select_columns.push("current.j_created_by".to_owned());
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
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num = ?
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num = ?
                   )
                 )
               )
               AND tx.outcome != ?
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(
            params![
                self.branch_num,
                if overlay_main { 1 } else { 0 },
                self.branch_num,
                self.branch_num,
                tx::OUTCOME_REJECTED
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
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let predicate_value = crate::schema::field_sql_value(field, value, |ref_table, row_id| {
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
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num = ?
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num = ?
                   )
                 )
               )
               AND tx.outcome != ?
               AND current.{predicate_column} = ?
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let params = [
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            predicate_value,
        ];
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
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
        let mut select_columns = vec!["ids.row_id".to_owned(), "tx.tx_id".to_owned()];
        select_columns.extend(
            field_columns
                .iter()
                .map(|column| format!("current.{column}")),
        );
        select_columns.push("current.j_created_by".to_owned());
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let sql = format!(
            "SELECT {}
             FROM {} current
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
            WHERE current.is_deleted = 0
               AND (
                 current.j_branch_num = ?
                 OR (
                   ? = 1
                   AND ? != 1
                   AND current.j_branch_num = 1
                   AND NOT EXISTS (
                     SELECT 1
                     FROM {current_table} branch_current
                     WHERE branch_current.row_num = current.row_num
                       AND branch_current.j_branch_num = ?
                   )
                 )
               )
               AND tx.outcome != ?
               AND instr(current.{predicate_column}, ?) > 0
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            current_table = crate::schema::current_table(table_name),
            policy_sql = self.read_policy_sql(table)?,
        );
        let params = [
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(if overlay_main { 1 } else { 0 }),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(self.branch_num),
            rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED),
            rusqlite::types::Value::Text(needle.to_owned()),
        ];
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| row_to_view(self.conn, table_name, table, row?))
            .collect()
    }

    fn read_main_snapshot_rows(&self, table_name: &str, base_epoch: i64) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let policy_sql = if self.trusted {
            "1 = 1".to_owned()
        } else {
            policy::snapshot_read_policy_sql_for_alias(
                self.schema,
                table,
                "h",
                self.principal,
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
        select_columns.push("h.j_created_by".to_owned());
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
    })
}

pub(crate) fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    match (&field.kind, value) {
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
