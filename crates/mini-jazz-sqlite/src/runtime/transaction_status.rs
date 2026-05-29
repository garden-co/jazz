use super::*;

impl Runtime {
    pub fn reject_transaction(&mut self, tx_id: &str, code: &str) -> Result<()> {
        self.reject_transaction_with_optional_detail(tx_id, code, None)
    }

    pub fn reject_transaction_with_detail(
        &mut self,
        tx_id: &str,
        code: &str,
        detail: JsonValue,
    ) -> Result<()> {
        self.reject_transaction_with_optional_detail(tx_id, code, Some(detail))
    }

    fn reject_transaction_with_optional_detail(
        &mut self,
        tx_id: &str,
        code: &str,
        detail: Option<JsonValue>,
    ) -> Result<()> {
        let detail_json = tx::encode_optional_json(detail.as_ref())?;
        let db = self.conn.transaction()?;
        let tx_num = tx::reject_with_detail_json(&db, tx_id, code, &detail_json)?;
        clear_transaction_awaiting_dependency(&db, tx_num)?;
        for table in self.schema.tables() {
            db.execute(
                &format!(
                    "DELETE FROM {} WHERE visible_tx_num = ?",
                    crate::schema::current_table(&table.name)
                ),
                params![tx_num],
            )?;
        }
        db.commit()?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn accept_transaction_at_global(&mut self, tx_id: &str, global_epoch: i64) -> Result<()> {
        let tx_num = tx::accept_global(&self.conn, tx_id, global_epoch)?;
        clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn accept_transaction_at_edge(&mut self, tx_id: &str) -> Result<()> {
        let tx_num = tx::accept_edge(&self.conn, tx_id, now_ms())?;
        clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn transaction_info(&self, tx_id: &str) -> Result<TransactionInfo> {
        let (tx_id, global_epoch, conflict_mode) = self.conn.query_row(
            "SELECT tx_id, global_epoch, conflict_mode FROM jazz_tx WHERE tx_id = ?",
            params![tx_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    conflict_mode_name(row.get::<_, i64>(2)?),
                ))
            },
        )?;
        let mut stmt = self.conn.prepare(
            "SELECT tier FROM jazz_tx_receipt receipt
             JOIN jazz_tx tx ON tx.tx_num = receipt.tx_num
             WHERE tx.tx_id = ?
             ORDER BY tier",
        )?;
        let receipt_tiers = stmt
            .query_map(params![tx_id], |row| tier_name(row.get::<_, i64>(0)?))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let rejection = self
            .conn
            .query_row(
                "SELECT rejection.code, rejection.detail_json
                 FROM jazz_tx_rejection rejection
                 JOIN jazz_tx tx ON tx.tx_num = rejection.tx_num
                 WHERE tx.tx_id = ?",
                params![tx_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (rejection_code, rejection_detail) = match rejection {
            Some((code, detail_json)) => {
                let detail = parse_rejection_detail(&detail_json)?;
                (Some(code), detail)
            }
            None => (None, None),
        };
        let awaiting_dependency = self
            .conn
            .query_row(
                "SELECT awaiting.detail_json
                 FROM jazz_tx_awaiting_dependency awaiting
                 JOIN jazz_tx tx ON tx.tx_num = awaiting.tx_num
                 WHERE tx.tx_id = ?",
                params![tx_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|detail_json| parse_rejection_detail(&detail_json))
            .transpose()?
            .flatten();
        Ok(TransactionInfo {
            tx_id,
            global_epoch,
            conflict_mode,
            receipt_tiers,
            awaiting_dependency,
            rejection_code,
            rejection_detail,
        })
    }

    pub fn rejected_transactions(&self) -> Result<Vec<RejectionInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT tx.tx_id, rejection.code, rejection.detail_json
             FROM jazz_tx_rejection rejection
             JOIN jazz_tx tx ON tx.tx_num = rejection.tx_num
             ORDER BY tx.tx_num",
        )?;
        let rows = stmt.query_map([], |row| {
            let detail_json = row.get::<_, String>(2)?;
            Ok(RejectionInfo {
                tx_id: row.get(0)?,
                code: row.get(1)?,
                detail: parse_rejection_detail_for_sqlite(&detail_json, 2)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn transaction_physical_num_for(&self, tx_id: &str) -> Result<i64> {
        tx::tx_num(&self.conn, tx_id)
    }

    pub fn transaction_write_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id
             FROM jazz_tx_write writes
             JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
             JOIN jazz_table tables ON tables.table_num = writes.table_num
             JOIN jazz_row_id ids ON ids.row_num = writes.row_num
             WHERE tx.tx_id = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn transaction_policy_read_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        self.transaction_read_rows_for_reason(tx_id, 1)
    }

    pub fn transaction_previous_read_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        self.transaction_read_rows_for_reason(tx_id, 2)
    }

    pub fn transaction_observed_read_rows(
        &self,
        tx_id: &str,
    ) -> Result<Vec<(String, String, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id, observed.tx_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_table tables ON tables.table_num = reads.table_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
             WHERE tx.tx_id = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn transaction_read_rows_for_reason(
        &self,
        tx_id: &str,
        reason: i64,
    ) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_table tables ON tables.table_num = reads.table_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             WHERE tx.tx_id = ?
               AND reads.reason = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id, reason], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}
