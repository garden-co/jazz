use crate::rows::{ensure_row_id, insert_project, insert_todo, public_row_id, row_num, NewTodo};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::subscription::RowsSubscription;
use crate::sync::{BranchRecord, Bundle, HistoryRecord, TxRecord};
use crate::types::{RowView, StorageStats, TodoView, TransactionInfo};
use crate::{branch, policy, projection, query, schema, storage, tx, Result, Storage};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    principal: String,
    node_num: i64,
    branch_num: i64,
    trusted: bool,
}

impl Runtime {
    pub fn open(storage: Storage, node_id: &str, principal: &str) -> Result<Self> {
        Self::open_with_schema(storage, node_id, principal, SchemaDef::attempt3_fixture())
    }

    pub fn open_with_schema(
        storage: Storage,
        node_id: &str,
        principal: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_trust(storage, node_id, principal, schema_def, false)
    }

    pub fn open_trusted_with_schema(
        storage: Storage,
        node_id: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_trust(storage, node_id, "trusted", schema_def, true)
    }

    fn open_with_schema_and_trust(
        storage: Storage,
        node_id: &str,
        principal: &str,
        schema_def: SchemaDef,
        trusted: bool,
    ) -> Result<Self> {
        let conn = storage::open(storage)?;
        schema::install(&conn, &schema_def)?;
        let node_num = tx::ensure_node(&conn, node_id)?;
        Ok(Self {
            conn,
            schema: schema_def,
            node_id: node_id.to_owned(),
            principal: principal.to_owned(),
            node_num,
            branch_num: 1,
            trusted,
        })
    }

    pub fn create_project(&mut self, id: &str, title: &str) -> Result<String> {
        self.schema.table_def("projects")?;
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = ensure_row_id(&db, "projects", id)?;
        db.execute(
            "INSERT OR IGNORE INTO projects__schema_v1_history
             (row_num, tx_num, j_branch_num, op, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)",
            params![
                row_num,
                tx_num,
                self.branch_num,
                title,
                now,
                now,
                self.principal,
                self.principal
            ],
        )?;
        record_tx_write(&db, tx_num, "projects", row_num, 1)?;
        db.execute(
            "INSERT OR REPLACE INTO projects__schema_v1_current
             (row_num, j_branch_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
            params![row_num, self.branch_num, tx_num, title, now, now, self.principal, self.principal],
        )?;
        db.commit()?;
        Ok(tx_id)
    }

    pub fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String> {
        self.schema.table_def("todos")?;
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = ensure_row_id(&db, "todos", id)?;
        let project_row_num = ensure_row_id(&db, "projects", project_id)?;
        db.execute(
            "INSERT OR IGNORE INTO todos__schema_v1_history
             (row_num, tx_num, j_branch_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
            params![
                row_num,
                tx_num,
                self.branch_num,
                title,
                i64::from(done),
                project_row_num,
                now,
                now,
                self.principal,
                self.principal
            ],
        )?;
        record_tx_write(&db, tx_num, "todos", row_num, 1)?;
        db.execute(
            "INSERT OR REPLACE INTO todos__schema_v1_current
             (row_num, j_branch_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
            params![
                row_num,
                self.branch_num,
                tx_num,
                title,
                i64::from(done),
                project_row_num,
                now,
                now,
                self.principal,
                self.principal
            ],
        )?;
        db.commit()?;
        Ok(tx_id)
    }

    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let write_policy = table.write_policy.clone();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        insert_row_in_tx(InsertRowInTx {
            db: &db,
            schema: &self.schema,
            table_name,
            id,
            values: &values,
            tx_num,
            branch_num: self.branch_num,
            now,
            principal: &self.principal,
            trusted: self.trusted,
        })?;
        let row_num = row_num(&db, id)?;
        let allowed = self.trusted
            || policy::write_allowed(
                &db,
                &self.schema,
                &table,
                &write_policy,
                row_num,
                &values,
                &self.principal,
            )?;
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

    pub fn open_todos(&self) -> Result<Vec<TodoView>> {
        let mut stmt = self.conn.prepare(
            "SELECT todo_ids.row_id,
                    t.title,
                    t.done,
                    project_ids.row_id,
                    p.title,
                    t.j_created_by,
                    tx.tx_id
             FROM todos__schema_v1_current t
             JOIN jazz_row_id todo_ids ON todo_ids.row_num = t.row_num
             JOIN jazz_row_id project_ids ON project_ids.row_num = t.project_row_num
             LEFT JOIN projects__schema_v1_current p
               ON p.row_num = t.project_row_num AND p.is_deleted = 0
             JOIN jazz_tx tx ON tx.tx_num = t.visible_tx_num
	             WHERE t.is_deleted = 0
	               AND t.done = 0
	               AND t.j_branch_num = ?
	               AND tx.outcome != ?
             ORDER BY t.j_created_at DESC, t.row_num",
        )?;
        let rows = stmt.query_map(params![self.branch_num, tx::OUTCOME_REJECTED], |row| {
            Ok(TodoView {
                id: row.get(0)?,
                title: row.get(1)?,
                done: row.get::<_, i64>(2)? != 0,
                project_id: row.get(3)?,
                project_title: row.get(4)?,
                created_by: row.get(5)?,
                tx_id: row.get(6)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn export_query_scope_open_todos(&self) -> Result<Bundle> {
        let txs = export_txs(&self.conn)?;
        let history = export_open_todo_scope_history(&self.conn)?;
        let branches = export_branch_records_for_history(&self.conn, &history)?;
        Ok(Bundle {
            branches,
            txs,
            history,
        })
    }

    pub fn export_table_history(&self, table_name: &str) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let txs = export_txs(&self.conn)?;
        let history = export_table_history(
            &self.conn,
            &self.schema,
            table_name,
            &self.principal,
            self.trusted,
            self.branch_num,
        )?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(Bundle {
            branches,
            txs,
            history,
        })
    }

    pub fn export_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let rows = self.read_recursive_refs(table_name, root_id, parent_field)?;
        let row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let txs = export_txs(&self.conn)?;
        let history = export_visible_table_history(
            &self.conn,
            &self.schema,
            table_name,
            &self.principal,
            self.trusted,
            &branch_nums,
            Some(&row_nums),
        )?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(Bundle {
            branches,
            txs,
            history,
        })
    }

    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        let schema = self.schema.clone();
        let db = self.conn.transaction()?;
        for branch_record in &bundle.branches {
            let branch_num = branch::ensure(
                &db,
                &branch_record.branch_id,
                branch_record.base_global_epoch,
                now_ms(),
            )?;
            for source_branch_id in &branch_record.source_branch_ids {
                branch::add_source(&db, branch_num, source_branch_id)?;
            }
        }
        for tx_record in &bundle.txs {
            let node_num = tx::ensure_node(&db, &tx_record.node_id)?;
            db.execute(
                "INSERT INTO jazz_tx
                 (tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}')
                 ON CONFLICT(tx_id) DO UPDATE SET
                   outcome = MAX(jazz_tx.outcome, excluded.outcome),
                   global_epoch = COALESCE(excluded.global_epoch, jazz_tx.global_epoch),
                   conflict_mode = MAX(jazz_tx.conflict_mode, excluded.conflict_mode)",
                params![
                    tx_record.tx_id,
                    node_num,
                    tx_record.local_epoch,
                    tx_record.global_epoch,
                    tx::KIND_DATA,
                    tx_record.conflict_mode,
                    tx_record.outcome,
                    tx_record.created_at
                ],
            )?;
            if let Some(global_epoch) = tx_record.global_epoch {
                let tx_num = tx::tx_num(&db, &tx_record.tx_id)?;
                db.execute(
                    "INSERT OR REPLACE INTO jazz_tx_receipt
                     (tx_num, tier, observed_at, receipt_json)
                     VALUES (?, ?, ?, '{}')",
                    params![tx_num, tx::TIER_GLOBAL, global_epoch],
                )?;
            }
        }
        for table in schema.tables() {
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE visible_tx_num IN (
                       SELECT tx_num FROM jazz_tx WHERE outcome = ?
                     )",
                    crate::schema::current_table(&table.name)
                ),
                params![tx::OUTCOME_REJECTED],
            )?;
        }
        for record in &bundle.history {
            Self::apply_history_record(&schema, &db, record)?;
        }
        db.commit()?;
        Ok(())
    }

    fn apply_history_record(
        schema: &SchemaDef,
        db: &Connection,
        record: &HistoryRecord,
    ) -> Result<()> {
        let table = schema.table_def(&record.table)?;
        let row_num = ensure_row_id(db, &record.table, &record.row_id)?;
        let tx_num = tx::tx_num(db, &record.tx_id)?;
        let branch_num = branch::ensure(db, &record.branch_id, None, now_ms())?;

        let mut columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(branch_num),
            rusqlite::types::Value::Integer(record.op),
        ];
        for field in &table.fields {
            let value = record
                .values
                .get(&field.name)
                .or_else(|| record.values.get(&field.storage_name))
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
                field,
            )));
            values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| ensure_row_id(db, ref_table, row_id),
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        values.extend([
            rusqlite::types::Value::Integer(record.created_at),
            rusqlite::types::Value::Integer(record.updated_at),
            rusqlite::types::Value::Text(record.created_by.clone()),
            rusqlite::types::Value::Text(record.updated_by.clone()),
        ]);
        insert_dynamic(
            db,
            &crate::schema::history_table(&record.table),
            &columns,
            &values,
        )?;
        record_tx_write(db, tx_num, &record.table, row_num, record.op)?;

        if tx_outcome(db, tx_num)? != tx::OUTCOME_REJECTED && record.op != 3 {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(4).cloned());
            current_values.extend(values.iter().skip(4).cloned());
            insert_dynamic(
                db,
                &crate::schema::current_table(&record.table),
                &current_columns,
                &current_values,
            )?;
        }
        Ok(())
    }

    pub fn reject_transaction(&mut self, tx_id: &str, code: &str) -> Result<()> {
        let db = self.conn.transaction()?;
        let tx_num = tx::reject(&db, tx_id, code)?;
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
        Ok(())
    }

    pub fn accept_transaction_at_global(&mut self, tx_id: &str, global_epoch: i64) -> Result<()> {
        tx::accept_global(&self.conn, tx_id, global_epoch)?;
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
        let rejection_code = self
            .conn
            .query_row(
                "SELECT rejection.code
                 FROM jazz_tx_rejection rejection
                 JOIN jazz_tx tx ON tx.tx_num = rejection.tx_num
                 WHERE tx.tx_id = ?",
                params![tx_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(TransactionInfo {
            tx_id,
            global_epoch,
            conflict_mode,
            receipt_tiers,
            rejection_code,
        })
    }

    pub fn transaction_physical_num_for(&self, tx_id: &str) -> Result<i64> {
        tx::tx_num(&self.conn, tx_id)
    }

    pub fn transaction_write_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT writes.table_name, ids.row_id
             FROM jazz_tx_write writes
             JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
             JOIN jazz_row_id ids ON ids.row_num = writes.row_num
             WHERE tx.tx_id = ?
             ORDER BY writes.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn create_branch(&mut self, branch_id: &str, base_global_epoch: Option<i64>) -> Result<()> {
        branch::ensure(&self.conn, branch_id, base_global_epoch, now_ms())?;
        Ok(())
    }

    pub fn create_branch_from_branches(
        &mut self,
        branch_id: &str,
        source_branch_ids: &[&str],
    ) -> Result<()> {
        let branch_num = branch::ensure(&self.conn, branch_id, None, now_ms())?;
        for source_branch_id in source_branch_ids {
            branch::add_source(&self.conn, branch_num, source_branch_id)?;
        }
        Ok(())
    }

    pub fn checkout_branch(&mut self, branch_id: &str) -> Result<()> {
        self.branch_num = branch::checkout(&self.conn, branch_id)?;
        Ok(())
    }

    pub fn transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            runtime: self,
            mutations: Vec::new(),
            mode: TransactionMode::Mergeable,
        }
    }

    pub fn delete_todo(&mut self, id: &str) -> Result<String> {
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = row_num(&db, id)?;
        db.execute(
            "INSERT OR IGNORE INTO todos__schema_v1_history
	             (row_num, tx_num, j_branch_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
	             SELECT row_num, ?, j_branch_num, 3, title, done, project_row_num, j_created_at, ?, j_created_by, ?
	             FROM todos__schema_v1_current
	             WHERE row_num = ? AND j_branch_num = ?",
            params![tx_num, now, self.principal, row_num, self.branch_num],
        )?;
        record_tx_write(&db, tx_num, "todos", row_num, 3)?;
        db.execute(
            "DELETE FROM todos__schema_v1_current WHERE row_num = ? AND j_branch_num = ?",
            params![row_num, self.branch_num],
        )?;
        db.commit()?;
        Ok(tx_id)
    }

    pub fn delete_row(&mut self, table_name: &str, id: &str) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = row_num(&db, id)?;

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
        db.execute(
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
            params![tx_num, now, self.principal, row_num, self.branch_num],
        )?;
        db.execute(
            &format!(
                "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                crate::schema::current_table(&table.name)
            ),
            params![row_num, self.branch_num],
        )?;
        record_tx_write(&db, tx_num, &table.name, row_num, 3)?;
        db.commit()?;
        Ok(tx_id)
    }

    pub fn clear_current_projection_for_test(&mut self) -> Result<()> {
        projection::clear(&self.conn, &self.schema)
    }

    pub fn rebuild_current_projection(&mut self) -> Result<()> {
        projection::rebuild(&self.conn, &self.schema)
    }

    pub fn physical_row_num_for(&self, row_id: &str) -> Result<i64> {
        row_num(&self.conn, row_id)
    }

    pub fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        self.query_context().read_rows(table_name)
    }

    pub fn read_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_recursive_refs(table_name, root_id, parent_field)
    }

    pub fn subscribe_rows(&self, table_name: &str) -> Result<RowsSubscription> {
        Ok(RowsSubscription::new(
            table_name,
            self.read_rows(table_name)?,
        ))
    }

    pub fn read_row_candidates(&self, table_name: &str, id: &str) -> Result<Vec<RowView>> {
        self.query_context().read_row_candidates(table_name, id)
    }

    pub fn poll_subscription(
        &self,
        subscription: &mut RowsSubscription,
    ) -> Result<Vec<crate::types::RowDiff>> {
        let next_rows = self.read_rows(&subscription.table)?;
        Ok(subscription.replace_with_diff(next_rows))
    }

    pub fn storage_stats(&self) -> Result<StorageStats> {
        let mut history_rows = 0;
        let mut current_rows = 0;
        for table in self.schema.tables() {
            history_rows += count_rows(&self.conn, &crate::schema::history_table(&table.name))?;
            current_rows += count_rows(&self.conn, &crate::schema::current_table(&table.name))?;
        }
        let rejected_transactions: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM jazz_tx WHERE outcome = ?",
            params![tx::OUTCOME_REJECTED],
            |row| row.get(0),
        )?;
        let mut stmt = self.conn.prepare("SELECT tx_id, tx_num FROM jazz_tx")?;
        let tx_nums = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()?;
        Ok(StorageStats::new(
            history_rows,
            current_rows,
            rejected_transactions,
            tx_nums,
        ))
    }

    fn query_context(&self) -> query::QueryContext<'_> {
        query::QueryContext {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            principal: &self.principal,
            trusted: self.trusted,
        }
    }
}

fn count_rows(conn: &Connection, table: &str) -> Result<i64> {
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

struct InsertRowInTx<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    values: &'a BTreeMap<String, JsonValue>,
    tx_num: i64,
    branch_num: i64,
    now: i64,
    principal: &'a str,
    trusted: bool,
}

fn insert_row_in_tx(args: InsertRowInTx<'_>) -> Result<()> {
    let table = args.schema.table_def(args.table_name)?;
    let row_num = ensure_row_id(args.db, args.table_name, args.id)?;
    let allowed = args.trusted
        || policy::write_allowed(
            args.db,
            args.schema,
            table,
            &table.write_policy,
            row_num,
            args.values,
            args.principal,
        )?;

    let mut columns = vec![
        "row_num".to_owned(),
        "tx_num".to_owned(),
        "j_branch_num".to_owned(),
        "op".to_owned(),
    ];
    let mut sql_values = vec![
        rusqlite::types::Value::Integer(row_num),
        rusqlite::types::Value::Integer(args.tx_num),
        rusqlite::types::Value::Integer(args.branch_num),
        rusqlite::types::Value::Integer(1),
    ];

    for field in &table.fields {
        let value = args
            .values
            .get(&field.name)
            .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
        columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
            field,
        )));
        sql_values.push(crate::schema::field_sql_value(
            field,
            value,
            |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
        )?);
    }
    columns.extend([
        "j_created_at".to_owned(),
        "j_updated_at".to_owned(),
        "j_created_by".to_owned(),
        "j_updated_by".to_owned(),
    ]);
    sql_values.extend([
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Text(args.principal.to_owned()),
        rusqlite::types::Value::Text(args.principal.to_owned()),
    ]);
    insert_dynamic(
        args.db,
        &crate::schema::history_table(&table.name),
        &columns,
        &sql_values,
    )?;
    record_tx_write(args.db, args.tx_num, &table.name, row_num, 1)?;

    if allowed {
        let mut current_columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(0),
        ];
        current_columns.extend(columns.iter().skip(4).cloned());
        current_values.extend(sql_values.iter().skip(4).cloned());
        insert_dynamic(
            args.db,
            &crate::schema::current_table(&table.name),
            &current_columns,
            &current_values,
        )?;
    }
    Ok(())
}

fn record_tx_write(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    op: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_write (tx_num, table_name, row_num, op)
         VALUES (?, ?, ?, ?)",
        params![tx_num, table_name, row_num, op],
    )?;
    Ok(())
}

pub struct TransactionBuilder<'a> {
    runtime: &'a mut Runtime,
    mutations: Vec<Mutation>,
    mode: TransactionMode,
}

enum TransactionMode {
    Mergeable,
    Exclusive { global_epoch: Option<i64> },
}

enum Mutation {
    Row {
        table: String,
        id: String,
        values: BTreeMap<String, JsonValue>,
    },
    Project {
        id: String,
        title: String,
    },
    Todo {
        id: String,
        title: String,
        done: bool,
        project_id: String,
    },
}

impl<'a> TransactionBuilder<'a> {
    pub fn exclusive(mut self) -> Self {
        self.mode = TransactionMode::Exclusive { global_epoch: None };
        self
    }

    pub fn exclusive_at_global(mut self, global_epoch: i64) -> Self {
        self.mode = TransactionMode::Exclusive {
            global_epoch: Some(global_epoch),
        };
        self
    }

    pub fn insert_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
        });
        self
    }

    pub fn create_project(mut self, id: &str, title: &str) -> Self {
        self.mutations.push(Mutation::Project {
            id: id.to_owned(),
            title: title.to_owned(),
        });
        self
    }

    pub fn create_todo(mut self, id: &str, title: &str, done: bool, project_id: &str) -> Self {
        self.mutations.push(Mutation::Todo {
            id: id.to_owned(),
            title: title.to_owned(),
            done,
            project_id: project_id.to_owned(),
        });
        self
    }

    pub fn commit(self) -> Result<String> {
        let (conflict_mode, outcome, global_epoch) = match self.mode {
            TransactionMode::Mergeable => (tx::MODE_MERGEABLE, tx::OUTCOME_PENDING, None),
            TransactionMode::Exclusive {
                global_epoch: Some(global_epoch),
            } => (tx::MODE_EXCLUSIVE, tx::OUTCOME_ACCEPTED, Some(global_epoch)),
            TransactionMode::Exclusive { global_epoch: None } => {
                return Err(crate::Error::new(
                    "exclusive transactions require global acceptance",
                ));
            }
        };
        let db = self.runtime.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx_with_options(
            &db,
            self.runtime.node_num,
            &self.runtime.node_id,
            now,
            conflict_mode,
            outcome,
            global_epoch,
        )?;
        for mutation in self.mutations {
            match mutation {
                Mutation::Row { table, id, values } => insert_row_in_tx(InsertRowInTx {
                    db: &db,
                    schema: &self.runtime.schema,
                    table_name: &table,
                    id: &id,
                    values: &values,
                    tx_num,
                    branch_num: self.runtime.branch_num,
                    now,
                    principal: &self.runtime.principal,
                    trusted: self.runtime.trusted,
                })?,
                Mutation::Project { id, title } => {
                    insert_project(&db, tx_num, &id, &title, now, &self.runtime.principal)?;
                    record_tx_write(&db, tx_num, "projects", row_num(&db, &id)?, 1)?;
                }
                Mutation::Todo {
                    id,
                    title,
                    done,
                    project_id,
                } => {
                    insert_todo(
                        &db,
                        tx_num,
                        NewTodo {
                            id: &id,
                            title: &title,
                            done,
                            project_id: &project_id,
                            now,
                            principal: &self.runtime.principal,
                        },
                    )?;
                    record_tx_write(&db, tx_num, "todos", row_num(&db, &id)?, 1)?;
                }
            }
        }
        db.commit()?;
        Ok(tx_id)
    }
}

fn tx_outcome(conn: &Connection, tx_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT outcome FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

fn export_txs(conn: &Connection) -> Result<Vec<TxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, tx.created_at
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        Ok(TxRecord {
            tx_id: row.get(0)?,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            created_at: row.get(6)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_branch_records_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<BranchRecord>> {
    let mut branch_ids = history
        .iter()
        .map(|record| record.branch_id.clone())
        .collect::<Vec<_>>();
    branch_ids.sort();
    branch_ids.dedup();

    let mut records = Vec::new();
    for branch_id in branch_ids {
        let (branch_num, base_global_epoch) = conn.query_row(
            "SELECT branch_num, base_global_epoch FROM jazz_branch WHERE branch_id = ?",
            params![branch_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )?;
        let mut stmt = conn.prepare(
            "SELECT source.branch_id
             FROM jazz_branch_source branch_source
             JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
             WHERE branch_source.branch_num = ?
             ORDER BY source.branch_id",
        )?;
        let source_branch_ids = stmt
            .query_map(params![branch_num], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
        });
    }
    Ok(records)
}

fn include_branch_record(
    conn: &Connection,
    records: &mut Vec<BranchRecord>,
    branch_num: i64,
) -> Result<()> {
    let (branch_id, base_global_epoch) = conn.query_row(
        "SELECT branch_id, base_global_epoch FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
    )?;
    if records.iter().any(|record| record.branch_id == branch_id) {
        return Ok(());
    }
    let mut stmt = conn.prepare(
        "SELECT source.branch_id
         FROM jazz_branch_source branch_source
         JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
         WHERE branch_source.branch_num = ?
         ORDER BY source.branch_id",
    )?;
    let source_branch_ids = stmt
        .query_map(params![branch_num], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    records.push(BranchRecord {
        branch_id,
        base_global_epoch,
        source_branch_ids,
    });
    Ok(())
}

fn export_open_todo_scope_history(conn: &Connection) -> Result<Vec<HistoryRecord>> {
    let mut records = Vec::new();
    records.extend(export_open_todo_projects(conn)?);
    records.extend(export_open_todos(conn)?);
    Ok(records)
}

fn export_open_todo_projects(conn: &Connection) -> Result<Vec<HistoryRecord>> {
    let mut stmt = conn.prepare(
        "SELECT ids.row_id,
                tx.tx_id,
                h.op,
                h.title,
                h.j_created_at,
                h.j_updated_at,
                h.j_created_by,
                h.j_updated_by
         FROM projects__schema_v1_history h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.row_num IN (
           SELECT DISTINCT project_row_num
           FROM todos__schema_v1_current todo
           JOIN jazz_tx todo_tx ON todo_tx.tx_num = todo.visible_tx_num
           WHERE todo.is_deleted = 0
             AND todo.done = 0
             AND todo_tx.outcome != ?
         )
         ORDER BY h.row_num, h.tx_num",
    )?;
    let records = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
        let mut values = BTreeMap::new();
        values.insert("title".to_owned(), JsonValue::String(row.get(3)?));
        Ok(HistoryRecord {
            table: "projects".to_owned(),
            row_id: row.get(0)?,
            branch_id: "main".to_owned(),
            tx_id: row.get(1)?,
            op: row.get(2)?,
            values,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
            created_by: row.get(6)?,
            updated_by: row.get(7)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_open_todos(conn: &Connection) -> Result<Vec<HistoryRecord>> {
    let mut stmt = conn.prepare(
        "SELECT ids.row_id,
                tx.tx_id,
                h.op,
                h.title,
                h.done,
                project_ids.row_id,
                h.j_created_at,
                h.j_updated_at,
                h.j_created_by,
                h.j_updated_by
         FROM todos__schema_v1_history h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_row_id project_ids ON project_ids.row_num = h.project_row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         ORDER BY h.row_num, h.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let mut values = BTreeMap::new();
        values.insert("title".to_owned(), JsonValue::String(row.get(3)?));
        values.insert(
            "done".to_owned(),
            JsonValue::Bool(row.get::<_, i64>(4)? != 0),
        );
        values.insert("project".to_owned(), JsonValue::String(row.get(5)?));
        Ok(HistoryRecord {
            table: "todos".to_owned(),
            row_id: row.get(0)?,
            branch_id: "main".to_owned(),
            tx_id: row.get(1)?,
            op: row.get(2)?,
            values,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
            created_by: row.get(8)?,
            updated_by: row.get(9)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    principal: &str,
    trusted: bool,
    branch_num: i64,
) -> Result<Vec<HistoryRecord>> {
    let branch_nums = branch::scope_nums(conn, branch_num)?;
    let mut records = export_visible_table_history(
        conn,
        schema,
        table_name,
        principal,
        trusted,
        &branch_nums,
        None,
    )?;
    records.extend(export_policy_dependency_history(
        conn,
        schema,
        table_name,
        principal,
        trusted,
        &branch_nums,
        None,
    )?);
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            records.extend(export_main_base_snapshot_history(
                conn, schema, table_name, base_epoch, principal, trusted,
            )?);
        }
    }
    Ok(records)
}

fn export_main_base_snapshot_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    base_epoch: i64,
    principal: &str,
    trusted: bool,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let policy_sql = if trusted {
        "1 = 1".to_owned()
    } else {
        policy::snapshot_read_policy_sql_for_alias(schema, table, "h", principal, base_epoch)?
    };
    let sql = format!(
        "SELECT h.row_num
         FROM {} h
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
               AND newer_tx.global_epoch > tx.global_epoch
           )",
        crate::schema::history_table(table_name),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map(
            params![
                tx::OUTCOME_REJECTED,
                base_epoch,
                tx::OUTCOME_REJECTED,
                base_epoch
            ],
            |row| row.get::<_, i64>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        table_name,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        conn,
        schema,
        table_name,
        principal,
        trusted,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    principal: &str,
    trusted: bool,
    base_epoch: i64,
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
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
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = if trusted {
        "1 = 1".to_owned()
    } else {
        policy::snapshot_read_policy_sql_for_alias(schema, table, "h", principal, base_epoch)?
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT h.{ref_column}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {row_filter}
           AND h.j_branch_num = 1
           AND h.op != 3
           AND tx.outcome != {}
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= {base_epoch}
           AND {policy_sql}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = 1
               AND newer_tx.outcome != {}
               AND newer_tx.global_epoch IS NOT NULL
               AND newer_tx.global_epoch <= {base_epoch}
               AND newer_tx.global_epoch > tx.global_epoch
           )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        row_filter = history_row_filter_sql("h", child_row_nums),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        parent_table,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        conn,
        schema,
        parent_table,
        principal,
        trusted,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_policy_dependency_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    principal: &str,
    trusted: bool,
    branch_nums: &[i64],
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
        return Ok(Vec::new());
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
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = export_read_policy_sql(schema, table, principal, trusted)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT current.{ref_column}
         FROM {} current
         JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
         WHERE current.is_deleted = 0
           AND {row_filter}
           AND {}
           AND current_tx.outcome != {}
           AND {policy_sql}",
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = current_row_filter_sql("current", child_row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_visible_table_history(
        conn,
        schema,
        parent_table,
        principal,
        trusted,
        branch_nums,
        Some(&row_nums),
    )?;
    records.extend(export_policy_dependency_history(
        conn,
        schema,
        parent_table,
        principal,
        trusted,
        branch_nums,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_visible_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    principal: &str,
    trusted: bool,
    branch_nums: &[i64],
    row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let policy_sql = export_read_policy_sql(schema, table, principal, trusted)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        "h.j_created_by".to_owned(),
        "h.j_updated_by".to_owned(),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND EXISTS (
           SELECT 1
           FROM {} current
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.row_num = h.row_num
             AND current.j_branch_num = h.j_branch_num
             AND current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
             AND {policy_sql}
         )
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = row_filter_sql(row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut records = Vec::new();
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json(conn, field, &row[idx + 4])?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn export_history_versions_for_rows(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        "h.j_created_by".to_owned(),
        "h.j_updated_by".to_owned(),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        row_filter = row_filter_sql(row_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json(conn, field, &row[idx + 4])?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn history_epoch_filter_sql(max_global_epoch: Option<i64>) -> String {
    match max_global_epoch {
        Some(epoch) => format!("tx.global_epoch IS NOT NULL AND tx.global_epoch <= {epoch}"),
        None => "1 = 1".to_owned(),
    }
}

fn row_filter_sql(row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "h.row_num IN ({})",
            (0..row_nums.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn current_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn history_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn branch_filter_sql(alias: &str, branch_nums: &[i64]) -> String {
    if branch_nums.is_empty() {
        return "0 = 1".to_owned();
    }
    format!(
        "{alias}.j_branch_num IN ({})",
        branch_nums
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn export_read_policy_sql(
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    principal: &str,
    trusted: bool,
) -> Result<String> {
    if trusted {
        Ok("1 = 1".to_owned())
    } else {
        policy::read_policy_sql(schema, table, principal)
    }
}

fn sql_value_to_json(
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

fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn tier_name(tier: i64) -> rusqlite::Result<String> {
    Ok(match tier {
        tx::TIER_GLOBAL => "global",
        _ => "unknown",
    }
    .to_owned())
}

fn conflict_mode_name(mode: i64) -> String {
    match mode {
        tx::MODE_EXCLUSIVE => "exclusive",
        tx::MODE_MERGEABLE => "mergeable",
        _ => "unknown",
    }
    .to_owned()
}

fn insert_dynamic(
    conn: &Connection,
    table: &str,
    columns: &[String],
    values: &[rusqlite::types::Value],
) -> Result<()> {
    let placeholders = (0..values.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
            columns.join(", ")
        ),
        params_from_iter(values.iter()),
    )?;
    Ok(())
}
