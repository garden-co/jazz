use crate::layout::{json_to_sql, placeholders, TablePlan};
use crate::schema::{FieldKind, Schema, TableDef};
use crate::store::{ReadSetEntry, WriteEffect};
use crate::{Error, Result};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, OptionalExtension};
use serde_json::Value as JsonValue;

struct CurrentRow {
    created_at: i64,
    visible_tx_id: String,
    values: Vec<SqlValue>,
}

pub struct WriteTx<'a> {
    pub(crate) schema: &'a Schema,
    pub(crate) conn: &'a rusqlite::Transaction<'a>,
    pub(crate) tx_id: String,
    pub(crate) branch_id: String,
    pub(crate) local_epoch: i64,
    pub(crate) now: i64,
    pub(crate) read_set: Vec<ReadSetEntry>,
    pub(crate) write_effects: Vec<WriteEffect>,
}

impl WriteTx<'_> {
    pub fn insert(&mut self, table_name: &str, value: JsonValue) -> Result<RowRef> {
        let table = self.schema.table_def(table_name)?;
        let row_id = format!(
            "{table_name}:{}:{}",
            self.local_epoch,
            self.row_count(table_name)? + 1
        );
        self.insert_with_row_id(table, &row_id, value)?;
        Ok(RowRef { id: row_id })
    }

    pub fn update(&mut self, table_name: &str, row_id: &str, patch: JsonValue) -> Result<()> {
        let table = self.schema.table_def(table_name)?;
        let object = patch
            .as_object()
            .ok_or_else(|| Error::new("update patch must be an object"))?;
        let current = self.current_values(table, row_id)?;
        self.record_row_read(table, row_id, &current.visible_tx_id);
        let mut values = current.values;
        for (idx, field) in table.fields.iter().enumerate() {
            if let Some(value) = object.get(&field.name) {
                values[idx] = json_to_sql(value, &field.kind)?;
            }
        }

        self.write_version(
            table,
            row_id,
            "update",
            values,
            current.created_at,
            self.now,
        )
    }

    pub fn delete(&mut self, table_name: &str, row_id: &str) -> Result<()> {
        let table = self.schema.table_def(table_name)?;
        let current = self.current_values(table, row_id)?;
        self.record_row_read(table, row_id, &current.visible_tx_id);
        self.write_version(
            table,
            row_id,
            "delete",
            current.values,
            current.created_at,
            self.now,
        )
    }

    fn record_row_read(&mut self, table: &TableDef, row_id: &str, visible_tx_id: &str) {
        let entry = ReadSetEntry {
            table: table.name.clone(),
            row_id: row_id.to_owned(),
            visible_tx_id: visible_tx_id.to_owned(),
        };
        if !self.read_set.iter().any(|read| {
            read.table == entry.table
                && read.row_id == entry.row_id
                && read.visible_tx_id == entry.visible_tx_id
        }) {
            self.read_set.push(entry);
        }
    }

    fn current_values(&self, table: &TableDef, row_id: &str) -> Result<CurrentRow> {
        let plan = TablePlan::new(table);

        let mut select_cols = vec!["j_created_at".to_owned(), "j_visible_tx_id".to_owned()];
        select_cols.extend(plan.user_columns.iter().cloned());
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {} FROM {} WHERE j_branch_id = 'main' AND j_row_id = ?",
            select_cols.join(", "),
            plan.current
        ))?;
        stmt.query_row(params![row_id], |row| {
            let created_at: i64 = row.get(0)?;
            let visible_tx_id: String = row.get(1)?;
            let mut values = Vec::new();
            for (idx, field) in table.fields.iter().enumerate() {
                let sql_value = match field.kind {
                    FieldKind::Text | FieldKind::Ref { .. } => SqlValue::Text(row.get(idx + 2)?),
                    FieldKind::Bool => SqlValue::Integer(row.get(idx + 2)?),
                };
                values.push(sql_value);
            }
            Ok(CurrentRow {
                created_at,
                visible_tx_id,
                values,
            })
        })
        .optional()?
        .ok_or_else(|| Error::new(format!("missing row {}:{row_id}", table.name)))
    }

    fn row_count(&self, table_name: &str) -> Result<i64> {
        let table = self.schema.table_def(table_name)?;
        let plan = TablePlan::new(table);
        self.conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", plan.history),
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn insert_with_row_id(
        &mut self,
        table: &TableDef,
        row_id: &str,
        value: JsonValue,
    ) -> Result<()> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("insert value must be an object"))?;
        let mut values = Vec::new();
        for field in &table.fields {
            let json = object.get(&field.name).ok_or_else(|| {
                Error::new(format!("missing field {}.{}", table.name, field.name))
            })?;
            values.push(json_to_sql(json, &field.kind)?);
        }

        self.write_version(table, row_id, "insert", values, self.now, self.now)
    }

    fn write_version(
        &mut self,
        table: &TableDef,
        row_id: &str,
        op: &str,
        values: Vec<SqlValue>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<()> {
        let plan = TablePlan::new(table);
        self.write_effects.push(WriteEffect {
            table: table.name.clone(),
            row_id: row_id.to_owned(),
        });

        let history_cols = std::iter::once("j_row_id".to_owned())
            .chain(std::iter::once("j_branch_id".to_owned()))
            .chain(std::iter::once("j_tx_id".to_owned()))
            .chain(std::iter::once("j_op".to_owned()))
            .chain(plan.user_columns.iter().cloned())
            .chain(std::iter::once("j_conflicts_json".to_owned()))
            .chain(std::iter::once("j_created_at".to_owned()))
            .chain(std::iter::once("j_updated_at".to_owned()))
            .collect::<Vec<_>>();
        let current_cols = std::iter::once("j_row_id".to_owned())
            .chain(std::iter::once("j_branch_id".to_owned()))
            .chain(std::iter::once("j_visible_tx_id".to_owned()))
            .chain(std::iter::once("j_is_deleted".to_owned()))
            .chain(plan.user_columns.iter().cloned())
            .chain(std::iter::once("j_conflicts_json".to_owned()))
            .chain(std::iter::once("j_created_at".to_owned()))
            .chain(std::iter::once("j_updated_at".to_owned()))
            .collect::<Vec<_>>();

        let mut history_values = vec![
            SqlValue::Text(row_id.to_owned()),
            SqlValue::Text(self.branch_id.clone()),
            SqlValue::Text(self.tx_id.clone()),
            SqlValue::Text(op.to_owned()),
        ];
        history_values.extend(values.clone());
        history_values.push(SqlValue::Text("{}".to_owned()));
        history_values.push(SqlValue::Integer(created_at));
        history_values.push(SqlValue::Integer(updated_at));

        self.conn.execute(
            &format!(
                "INSERT INTO {} ({}) VALUES ({})",
                plan.history,
                history_cols.join(", "),
                placeholders(history_cols.len())
            ),
            params_from_iter(history_values),
        )?;

        let mut current_values = vec![
            SqlValue::Text(row_id.to_owned()),
            SqlValue::Text(self.branch_id.clone()),
            SqlValue::Text(self.tx_id.clone()),
            SqlValue::Integer(i64::from(op == "delete")),
        ];
        current_values.extend(values);
        current_values.push(SqlValue::Text("{}".to_owned()));
        current_values.push(SqlValue::Integer(created_at));
        current_values.push(SqlValue::Integer(updated_at));

        self.conn.execute(
            &format!(
                "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
                plan.current,
                current_cols.join(", "),
                placeholders(current_cols.len())
            ),
            params_from_iter(current_values),
        )?;

        Ok(())
    }
}

pub struct RowRef {
    id: String,
}

impl RowRef {
    pub fn id(&self) -> &str {
        &self.id
    }
}
