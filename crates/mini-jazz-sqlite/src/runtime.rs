use crate::sync::{Bundle, ProjectRecord, TodoRecord, TxRecord};
use crate::types::{StorageStats, TodoView};
use crate::{schema, storage, tx, Result, Storage};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Runtime {
    conn: Connection,
    node_id: String,
    principal: String,
    node_num: i64,
}

impl Runtime {
    pub fn open(storage: Storage, node_id: &str, principal: &str) -> Result<Self> {
        let conn = storage::open(storage)?;
        schema::install(&conn)?;
        let node_num = tx::ensure_node(&conn, node_id)?;
        Ok(Self {
            conn,
            node_id: node_id.to_owned(),
            principal: principal.to_owned(),
            node_num,
        })
    }

    pub fn create_project(&mut self, id: &str, title: &str) -> Result<String> {
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
        let projects = export_projects(&self.conn)?;
        let todos = export_todos(&self.conn)?;
        Ok(Bundle {
            txs,
            projects,
            todos,
        })
    }

    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
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
        for project in &bundle.projects {
            let row_num = ensure_row_id(&db, "projects", &project.row_id)?;
            let tx_num = tx::tx_num(&db, &project.tx_id)?;
            db.execute(
                "INSERT OR IGNORE INTO projects__schema_v1_history
                 (row_num, tx_num, op, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                params![
                    row_num,
                    tx_num,
                    project.title,
                    project.created_at,
                    project.updated_at,
                    project.created_by,
                    project.updated_by
                ],
            )?;
            if tx_outcome(&db, tx_num)? != tx::OUTCOME_REJECTED {
                db.execute(
                    "INSERT OR REPLACE INTO projects__schema_v1_current
                     (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
                     VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
                    params![
                        row_num,
                        tx_num,
                        project.title,
                        project.created_at,
                        project.updated_at,
                        project.created_by,
                        project.updated_by
                    ],
                )?;
            }
        }
        for todo in &bundle.todos {
            let row_num = ensure_row_id(&db, "todos", &todo.row_id)?;
            let project_row_num = ensure_row_id(&db, "projects", &todo.project_id)?;
            let tx_num = tx::tx_num(&db, &todo.tx_id)?;
            db.execute(
                "INSERT OR IGNORE INTO todos__schema_v1_history
                 (row_num, tx_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    row_num,
                    tx_num,
                    todo.title,
                    i64::from(todo.done),
                    project_row_num,
                    todo.created_at,
                    todo.updated_at,
                    todo.created_by,
                    todo.updated_by
                ],
            )?;
            if tx_outcome(&db, tx_num)? != tx::OUTCOME_REJECTED {
                db.execute(
                    "INSERT OR REPLACE INTO todos__schema_v1_current
                     (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
                     VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        row_num,
                        tx_num,
                        todo.title,
                        i64::from(todo.done),
                        project_row_num,
                        todo.created_at,
                        todo.updated_at,
                        todo.created_by,
                        todo.updated_by
                    ],
                )?;
            }
        }
        db.commit()?;
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
        self.conn
            .execute("DELETE FROM todos__schema_v1_current", [])?;
        self.conn
            .execute("DELETE FROM projects__schema_v1_current", [])?;
        Ok(())
    }

    pub fn rebuild_current_projection(&mut self) -> Result<()> {
        self.clear_current_projection_for_test()?;
        rebuild_projects(&self.conn)?;
        rebuild_todos(&self.conn)?;
        Ok(())
    }

    pub fn physical_row_num_for(&self, row_id: &str) -> Result<i64> {
        row_num(&self.conn, row_id)
    }

    pub fn storage_stats(&self) -> Result<StorageStats> {
        let history_rows: i64 = self.conn.query_row(
            "SELECT
               (SELECT COUNT(*) FROM projects__schema_v1_history) +
               (SELECT COUNT(*) FROM todos__schema_v1_history)",
            [],
            |row| row.get(0),
        )?;
        let current_rows: i64 = self.conn.query_row(
            "SELECT
               (SELECT COUNT(*) FROM projects__schema_v1_current) +
               (SELECT COUNT(*) FROM todos__schema_v1_current)",
            [],
            |row| row.get(0),
        )?;
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

fn ensure_row_id(conn: &Connection, table: &str, row_id: &str) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_row_id (table_name, row_id) VALUES (?, ?)",
        params![table, row_id],
    )?;
    Ok(conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )?)
}

fn row_num(conn: &Connection, row_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| crate::Error::new(format!("unknown row {row_id}")))
}

fn insert_project(
    conn: &Connection,
    tx_num: i64,
    id: &str,
    title: &str,
    now: i64,
    principal: &str,
) -> Result<()> {
    let row_num = ensure_row_id(conn, "projects", id)?;
    conn.execute(
        "INSERT OR IGNORE INTO projects__schema_v1_history
         (row_num, tx_num, op, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
        params![row_num, tx_num, title, now, now, principal, principal],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO projects__schema_v1_current
         (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
        params![row_num, tx_num, title, now, now, principal, principal],
    )?;
    Ok(())
}

struct NewTodo<'a> {
    id: &'a str,
    title: &'a str,
    done: bool,
    project_id: &'a str,
    now: i64,
    principal: &'a str,
}

fn insert_todo(conn: &Connection, tx_num: i64, todo: NewTodo<'_>) -> Result<()> {
    let row_num = ensure_row_id(conn, "todos", todo.id)?;
    let project_row_num = ensure_row_id(conn, "projects", todo.project_id)?;
    conn.execute(
        "INSERT OR IGNORE INTO todos__schema_v1_history
         (row_num, tx_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row_num,
            tx_num,
            todo.title,
            i64::from(todo.done),
            project_row_num,
            todo.now,
            todo.now,
            todo.principal,
            todo.principal
        ],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO todos__schema_v1_current
         (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row_num,
            tx_num,
            todo.title,
            i64::from(todo.done),
            project_row_num,
            todo.now,
            todo.now,
            todo.principal,
            todo.principal
        ],
    )?;
    Ok(())
}

fn rebuild_projects(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT h.row_num, h.tx_num, h.op, h.title, h.j_created_at, h.j_updated_at, h.j_created_by, h.j_updated_by
         FROM projects__schema_v1_history h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE tx.outcome != ?
         ORDER BY h.row_num, h.tx_num",
    )?;
    let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;
    for row in rows {
        let (row_num, tx_num, op, title, created_at, updated_at, created_by, updated_by) = row?;
        if op == 3 {
            conn.execute(
                "DELETE FROM projects__schema_v1_current WHERE row_num = ?",
                params![row_num],
            )?;
        } else {
            conn.execute(
                "INSERT OR REPLACE INTO projects__schema_v1_current
                 (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
                params![row_num, tx_num, title, created_at, updated_at, created_by, updated_by],
            )?;
        }
    }
    Ok(())
}

fn rebuild_todos(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT h.row_num, h.tx_num, h.op, h.title, h.done, h.project_row_num, h.j_created_at, h.j_updated_at, h.j_created_by, h.j_updated_by
         FROM todos__schema_v1_history h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE tx.outcome != ?
         ORDER BY h.row_num, h.tx_num",
    )?;
    let rows = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, String>(9)?,
        ))
    })?;
    for row in rows {
        let (
            row_num,
            tx_num,
            op,
            title,
            done,
            project_row_num,
            created_at,
            updated_at,
            created_by,
            updated_by,
        ) = row?;
        if op == 3 {
            conn.execute(
                "DELETE FROM todos__schema_v1_current WHERE row_num = ?",
                params![row_num],
            )?;
        } else {
            conn.execute(
                "INSERT OR REPLACE INTO todos__schema_v1_current
                 (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
                 VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    row_num,
                    tx_num,
                    title,
                    done,
                    project_row_num,
                    created_at,
                    updated_at,
                    created_by,
                    updated_by
                ],
            )?;
        }
    }
    Ok(())
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

fn export_projects(conn: &Connection) -> Result<Vec<ProjectRecord>> {
    let mut stmt = conn.prepare(
        "SELECT ids.row_id, tx.tx_id, h.title, h.j_created_at, h.j_updated_at, h.j_created_by, h.j_updated_by
         FROM projects__schema_v1_history h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         ORDER BY h.row_num, h.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        Ok(ProjectRecord {
            row_id: row.get(0)?,
            tx_id: row.get(1)?,
            title: row.get(2)?,
            created_at: row.get(3)?,
            updated_at: row.get(4)?,
            created_by: row.get(5)?,
            updated_by: row.get(6)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_todos(conn: &Connection) -> Result<Vec<TodoRecord>> {
    let mut stmt = conn.prepare(
        "SELECT ids.row_id,
                tx.tx_id,
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
        Ok(TodoRecord {
            row_id: row.get(0)?,
            tx_id: row.get(1)?,
            title: row.get(2)?,
            done: row.get::<_, i64>(3)? != 0,
            project_id: row.get(4)?,
            created_at: row.get(5)?,
            updated_at: row.get(6)?,
            created_by: row.get(7)?,
            updated_by: row.get(8)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
