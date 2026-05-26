use crate::rows::{ensure_row_id, insert_project, insert_todo, public_row_id, row_num, NewTodo};
use crate::schema::{FieldDef, FieldKind, SchemaDef};
use crate::sync::{Bundle, HistoryRecord, TxRecord};
use crate::types::{RowView, StorageStats, TodoView, TransactionInfo};
use crate::{policy, projection, schema, storage, tx, Result, Storage};
use rusqlite::{params, params_from_iter, Connection};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    principal: String,
    node_num: i64,
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
        let conn = storage::open(storage)?;
        schema::install(&conn, &schema_def)?;
        let node_num = tx::ensure_node(&conn, node_id)?;
        Ok(Self {
            conn,
            schema: schema_def,
            node_id: node_id.to_owned(),
            principal: principal.to_owned(),
            node_num,
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
             (row_num, tx_num, op, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
            params![
                row_num,
                tx_num,
                title,
                now,
                now,
                self.principal,
                self.principal
            ],
        )?;
        db.execute(
            "INSERT OR REPLACE INTO projects__schema_v1_current
             (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
            params![row_num, tx_num, title, now, now, self.principal, self.principal],
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
             (row_num, tx_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
            params![
                row_num,
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
        db.execute(
            "INSERT OR REPLACE INTO todos__schema_v1_current
             (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
             VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
            params![
                row_num,
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
        let row_num = ensure_row_id(&db, table_name, id)?;
        let allowed =
            policy::write_allowed(&db, &table.name, &write_policy, row_num, &self.principal)?;
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
        }

        let mut columns = vec!["row_num".to_owned(), "tx_num".to_owned(), "op".to_owned()];
        let mut sql_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(1),
        ];

        for field in &table.fields {
            let value = values
                .get(&field.name)
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
                field,
            )));
            sql_values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        sql_values.extend([
            rusqlite::types::Value::Integer(now),
            rusqlite::types::Value::Integer(now),
            rusqlite::types::Value::Text(self.principal.clone()),
            rusqlite::types::Value::Text(self.principal.clone()),
        ]);
        insert_dynamic(
            &db,
            &crate::schema::history_table(&table.name),
            &columns,
            &sql_values,
        )?;

        if allowed {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(3).cloned());
            current_values.extend(sql_values.iter().skip(3).cloned());
            insert_dynamic(
                &db,
                &crate::schema::current_table(&table.name),
                &current_columns,
                &current_values,
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
               AND tx.outcome != ?
             ORDER BY t.j_created_at DESC, t.row_num",
        )?;
        let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
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
        Ok(Bundle { txs, history })
    }

    pub fn export_table_history(&self, table_name: &str) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let txs = export_txs(&self.conn)?;
        let history = export_table_history(&self.conn, &self.schema, table_name, &self.principal)?;
        Ok(Bundle { txs, history })
    }

    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        let schema = self.schema.clone();
        let db = self.conn.transaction()?;
        for tx_record in &bundle.txs {
            let node_num = tx::ensure_node(&db, &tx_record.node_id)?;
            db.execute(
                "INSERT OR IGNORE INTO jazz_tx
                 (tx_id, node_num, local_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, '{}')",
                params![
                    tx_record.tx_id,
                    node_num,
                    tx_record.local_epoch,
                    tx::KIND_DATA,
                    tx::MODE_MERGEABLE,
                    tx_record.outcome,
                    tx_record.created_at
                ],
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

        let mut columns = vec!["row_num".to_owned(), "tx_num".to_owned(), "op".to_owned()];
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(record.op),
        ];
        for field in &table.fields {
            let value = record
                .values
                .get(&field.name)
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

        if tx_outcome(db, tx_num)? != tx::OUTCOME_REJECTED && record.op != 3 {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(3).cloned());
            current_values.extend(values.iter().skip(3).cloned());
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
        db.execute(
            "DELETE FROM todos__schema_v1_current WHERE visible_tx_num = ?",
            params![tx_num],
        )?;
        db.execute(
            "DELETE FROM projects__schema_v1_current WHERE visible_tx_num = ?",
            params![tx_num],
        )?;
        db.commit()?;
        Ok(())
    }

    pub fn accept_transaction_at_global(&mut self, tx_id: &str, global_epoch: i64) -> Result<()> {
        tx::accept_global(&self.conn, tx_id, global_epoch)?;
        Ok(())
    }

    pub fn transaction_info(&self, tx_id: &str) -> Result<TransactionInfo> {
        let (tx_id, global_epoch) = self.conn.query_row(
            "SELECT tx_id, global_epoch FROM jazz_tx WHERE tx_id = ?",
            params![tx_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
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
        Ok(TransactionInfo {
            tx_id,
            global_epoch,
            receipt_tiers,
        })
    }

    pub fn transaction_physical_num_for(&self, tx_id: &str) -> Result<i64> {
        tx::tx_num(&self.conn, tx_id)
    }

    pub fn transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            runtime: self,
            mutations: Vec::new(),
        }
    }

    pub fn delete_todo(&mut self, id: &str) -> Result<String> {
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = row_num(&db, id)?;
        db.execute(
            "INSERT OR IGNORE INTO todos__schema_v1_history
             (row_num, tx_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
             SELECT row_num, ?, 3, title, done, project_row_num, j_created_at, ?, j_created_by, ?
             FROM todos__schema_v1_current
             WHERE row_num = ?",
            params![tx_num, now, self.principal, row_num],
        )?;
        db.execute(
            "DELETE FROM todos__schema_v1_current WHERE row_num = ?",
            params![row_num],
        )?;
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
               AND tx.outcome != ?
               AND {policy_sql}
             ORDER BY current.j_created_at DESC, current.row_num",
            select_columns.join(", "),
            crate::schema::current_table(table_name),
            policy_sql = policy::read_policy_sql(&self.schema, table, &self.principal)?,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let row_width = 2 + table.fields.len() + 1;
        let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
            let raw_values = (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(raw_values)
        })?;

        let mut views = Vec::new();
        for raw in rows {
            let raw = raw?;
            let mut values = BTreeMap::new();
            for (idx, field) in table.fields.iter().enumerate() {
                values.insert(
                    field.name.clone(),
                    sql_value_to_json(&self.conn, field, &raw[idx + 2])?,
                );
            }
            views.push(RowView {
                table: table_name.to_owned(),
                id: text_value(&raw[0], "row_id")?,
                tx_id: text_value(&raw[1], "tx_id")?,
                values,
                created_by: text_value(&raw[2 + table.fields.len()], "j_created_by")?,
            });
        }
        Ok(views)
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
}

fn count_rows(conn: &Connection, table: &str) -> Result<i64> {
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

pub struct TransactionBuilder<'a> {
    runtime: &'a mut Runtime,
    mutations: Vec<Mutation>,
}

enum Mutation {
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
        let db = self.runtime.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) =
            tx::create_tx(&db, self.runtime.node_num, &self.runtime.node_id, now)?;
        for mutation in self.mutations {
            match mutation {
                Mutation::Project { id, title } => {
                    insert_project(&db, tx_num, &id, &title, now, &self.runtime.principal)?
                }
                Mutation::Todo {
                    id,
                    title,
                    done,
                    project_id,
                } => insert_todo(
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
                )?,
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
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.outcome, tx.created_at
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        Ok(TxRecord {
            tx_id: row.get(0)?,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            outcome: row.get(3)?,
            created_at: row.get(4)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
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
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let policy_sql = policy::read_policy_sql(schema, table, principal)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
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
         WHERE EXISTS (
           SELECT 1
           FROM {} current
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.row_num = h.row_num
             AND current.is_deleted = 0
             AND current_tx.outcome != {}
             AND {policy_sql}
         )
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        crate::schema::current_table(table_name),
        tx::OUTCOME_REJECTED,
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 3 + table.fields.len() + 4;
    let rows = stmt.query_map([], |row| {
        (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;

    let mut records = Vec::new();
    for row in rows {
        let row = row?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json(conn, field, &row[idx + 3])?,
            );
        }
        let sys = 3 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            tx_id: text_value(&row[1], "tx_id")?,
            op: integer_value(&row[2], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
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
