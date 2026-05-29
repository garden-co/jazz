use super::write_core::{
    insert_row_in_tx, stage_delete_row_in_tx, DeleteReadSetMode, InsertRowInTx, StageDeleteInTx,
    WriteOp,
};
use super::Runtime;
use crate::rows::row_num;
use crate::time::now_ms;
use crate::{projection, query, tx, Result};
use rusqlite::params;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

impl Runtime {
    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.write_row(table_name, id, values, WriteOp::Create)
    }

    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.physical_row_num_for(id)?;
        self.write_row(table_name, id, values, WriteOp::Update)
    }

    pub fn upsert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            WriteOp::Update
        } else {
            WriteOp::Create
        };
        self.write_row(table_name, id, values, op)
    }

    pub fn resolve_row_conflict(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            WriteOp::Update
        } else {
            WriteOp::Create
        };
        self.write_row(table_name, id, values, op)
    }

    fn write_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
        op: WriteOp,
    ) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let allowed = insert_row_in_tx(InsertRowInTx {
            db: &db,
            schema: &self.schema,
            table_name,
            id,
            values: &values,
            tx_num,
            branch_num: self.branch_num,
            now,
            user: &user,
            bypass_policy,
            op,
            base_values: None,
        })?;
        let row_num = row_num(&db, id)?;
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            db.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ? AND visible_tx_num = ?",
                    crate::schema::current_table(&table.name)
                ),
                params![row_num, self.branch_num, tx_num],
            )?;
        }
        db.commit()?;
        Ok(tx_id)
    }

    pub fn delete_row(&mut self, table_name: &str, id: &str) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let visible_row = self
            .read_rows(table_name)?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))?;
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let allowed = stage_delete_row_in_tx(StageDeleteInTx {
            db: &db,
            schema: &self.schema,
            table_name: &table.name,
            id,
            visible_values: &visible_row.values,
            tx_num,
            branch_num: self.branch_num,
            now,
            user: &user,
            bypass_policy,
            read_set: DeleteReadSetMode::AlreadyCoveredByWriteCall,
        })?;
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            projection::rebuild(&db, &self.schema, self.node_num)?;
        }
        db.commit()?;
        Ok(tx_id)
    }

    pub fn restore_deleted_row(&mut self, table_name: &str, id: &str) -> Result<String> {
        let table = self.schema.table_def(table_name)?;
        let row_num = row_num(&self.conn, id)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| {
                format!(
                    "h.{}",
                    crate::schema::quote_ident(&crate::schema::storage_column(field))
                )
            })
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND h.op = 3
               AND tx.outcome != ?
             ORDER BY tx.global_epoch DESC NULLS LAST, h.tx_num DESC
             LIMIT 1",
            field_columns.join(", "),
            crate::schema::history_table(table_name)
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query_map(
            params![row_num, self.branch_num, tx::OUTCOME_REJECTED],
            |row| {
                (0..table.fields.len())
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        let row = rows
            .next()
            .transpose()?
            .ok_or_else(|| crate::Error::new(format!("row {id} has no deleted version")))?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                query::sql_value_to_json(&self.conn, field, &row[idx])?,
            );
        }
        drop(rows);
        drop(stmt);
        self.write_row(table_name, id, values, WriteOp::Create)
    }
}
