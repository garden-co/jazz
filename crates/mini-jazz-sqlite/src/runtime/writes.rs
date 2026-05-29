use super::*;

impl Runtime {
    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.write_row(table_name, id, values, 1)
    }

    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.physical_row_num_for(id)?;
        self.write_row(table_name, id, values, 2)
    }

    pub fn upsert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            2
        } else {
            1
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
            2
        } else {
            1
        };
        self.write_row(table_name, id, values, op)
    }

    fn write_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
        op: i64,
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
        let row_num = row_num(&db, id)?;
        policy_read_set::record_for_write(policy_read_set::WritePolicyReadSet {
            conn: &db,
            schema: &self.schema,
            table: &table,
            policy: &table.write_policy,
            values: &visible_row.values,
            branch_num: self.branch_num,
            tx_num,
        })?;
        let allowed = bypass_policy
            || local_write_allowed(LocalWriteCheck {
                db: &db,
                schema: &self.schema,
                table: &table,
                row_num,
                branch_num: self.branch_num,
                values: &visible_row.values,
                user: &user,
                op: 3,
            })?;

        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut insert_columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        insert_columns.extend(field_columns.iter().cloned());
        insert_columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let mut select_columns = vec![
            "row_num".to_owned(),
            "?".to_owned(),
            "j_branch_num".to_owned(),
            "3".to_owned(),
        ];
        select_columns.extend(field_columns.iter().cloned());
        select_columns.extend([
            "j_created_at".to_owned(),
            "?".to_owned(),
            "j_created_by".to_owned(),
            "?".to_owned(),
        ]);
        let user_num = users::ensure_user(&db, &user)?;
        let inserted = db.execute(
            &format!(
                "INSERT OR IGNORE INTO {} ({})
                 SELECT {}
                 FROM {}
                 WHERE row_num = ? AND j_branch_num = ?",
                crate::schema::history_table(&table.name),
                insert_columns.join(", "),
                select_columns.join(", "),
                crate::schema::current_table(&table.name),
            ),
            params![tx_num, now, user_num, row_num, self.branch_num],
        )?;
        if inserted == 0 {
            let mut values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(self.branch_num),
                rusqlite::types::Value::Integer(3),
            ];
            for field in &table.fields {
                let value = visible_row
                    .values
                    .get(&field.name)
                    .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
                values.push(crate::schema::field_sql_value(
                    field,
                    value,
                    |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                )?);
            }
            values.extend([
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(user_num),
                rusqlite::types::Value::Integer(user_num),
            ]);
            insert_dynamic(
                &db,
                &crate::schema::history_table(&table.name),
                &insert_columns,
                &values,
            )?;
        }
        db.execute(
            &format!(
                "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                crate::schema::current_table(&table.name)
            ),
            params![row_num, self.branch_num],
        )?;
        if self.branch_num != 1 {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            current_columns.extend(field_columns.iter().cloned());
            current_columns.extend([
                "j_created_at".to_owned(),
                "j_updated_at".to_owned(),
                "j_created_by".to_owned(),
                "j_updated_by".to_owned(),
            ]);
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(self.branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(1),
            ];
            for field in &table.fields {
                let value = visible_row
                    .values
                    .get(&field.name)
                    .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
                current_values.push(crate::schema::field_sql_value(
                    field,
                    value,
                    |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                )?);
            }
            current_values.extend([
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(user_num),
                rusqlite::types::Value::Integer(user_num),
            ]);
            insert_dynamic(
                &db,
                &crate::schema::current_table(&table.name),
                &current_columns,
                &current_values,
            )?;
        }
        record_tx_write(&db, tx_num, &table.name, row_num, 3)?;
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
        self.write_row(table_name, id, values, 1)
    }
}
