use super::*;

impl Runtime {
    pub fn insert_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
    ) -> Result<Vec<String>> {
        self.write_rows_batched(table_name, rows, 1)
    }

    pub fn update_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
    ) -> Result<Vec<String>> {
        self.write_rows_batched(table_name, rows, 2)
    }

    fn write_rows_batched(
        &mut self,
        table_name: &str,
        rows: Vec<(String, BTreeMap<String, JsonValue>)>,
        op: i64,
    ) -> Result<Vec<String>> {
        let table = self.schema.table_def(table_name)?.clone();
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let mut tx_ids = Vec::with_capacity(rows.len());
        for (id, values) in rows {
            let now = now_ms();
            let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
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
