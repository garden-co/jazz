use super::write_core::{insert_row_in_tx, row_has_current_branch_value, InsertRowInTx, WriteOp};
use super::Runtime;
use crate::rows::{ensure_row_id, row_num};
use crate::time::now_ms;
use crate::tx;
use crate::Result;
use rusqlite::params;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

impl Runtime {
    pub fn insert_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
    ) -> Result<Vec<String>> {
        self.write_rows_batched(table_name, rows, BatchedWriteMode::Insert)
    }

    pub fn update_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
    ) -> Result<Vec<String>> {
        self.write_rows_batched(table_name, rows, BatchedWriteMode::Update)
    }

    pub fn upsert_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
    ) -> Result<Vec<String>> {
        self.write_rows_batched(table_name, rows, BatchedWriteMode::Upsert)
    }

    fn write_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
        mode: BatchedWriteMode,
    ) -> Result<Vec<String>> {
        let table = self.schema.table_def(table_name)?.clone();
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let mut tx_ids = Vec::with_capacity(rows.len());
        for (id, values) in rows {
            let now = now_ms();
            let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
            let op = match mode {
                BatchedWriteMode::Insert => WriteOp::Create,
                BatchedWriteMode::Update => WriteOp::Update,
                BatchedWriteMode::Upsert => {
                    let row_num = ensure_row_id(&db, table_name, &id)?;
                    if row_has_current_branch_value(&db, table_name, row_num, self.branch_num)? {
                        WriteOp::Update
                    } else {
                        WriteOp::Create
                    }
                }
            };
            let allowed = insert_row_in_tx(InsertRowInTx {
                db: &db,
                schema: &self.schema,
                table_name,
                id: &id,
                values: &values,
                tx_num,
                branch_num: self.branch_num,
                now,
                user: &user,
                bypass_policy,
                op,
                base_values: None,
            })?;
            let row_num = row_num(&db, &id)?;
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
            tx_ids.push(tx_id);
        }
        db.commit()?;
        Ok(tx_ids)
    }
}

#[derive(Clone, Copy)]
enum BatchedWriteMode {
    Insert,
    Update,
    Upsert,
}
