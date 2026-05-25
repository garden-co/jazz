use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Todo {
    pub row_id: String,
    pub title: String,
    pub done: bool,
    pub created_at: i64,
    pub updated_at: i64,
    pub visible_tx_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertTodo {
    pub row_id: String,
    pub tx_id: String,
    pub node_id: String,
    pub title: String,
    pub done: bool,
    pub actor_id: String,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertTodos {
    pub tx_id: String,
    pub node_id: String,
    pub rows: Vec<NewTodoRow>,
    pub actor_id: String,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTodoRow {
    pub row_id: String,
    pub title: String,
    pub done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranch {
    pub branch_id: String,
    pub tx_id: String,
    pub node_id: String,
    pub name: String,
    pub head_global_epoch: i64,
    pub base_provenance_json: String,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTodo {
    pub row_id: String,
    pub tx_id: String,
    pub node_id: String,
    pub title: Option<String>,
    pub done: Option<bool>,
    pub actor_id: String,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTodo {
    pub row_id: String,
    pub tx_id: String,
    pub node_id: String,
    pub actor_id: String,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptTx {
    pub tx_id: String,
    pub global_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectTx {
    pub tx_id: String,
    pub reason_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoQuery {
    pub branch_id: String,
    pub done: Option<bool>,
    pub created_after: Option<i64>,
}

impl TodoQuery {
    pub fn open_since(created_after: i64) -> Self {
        Self {
            branch_id: "main".to_owned(),
            done: Some(false),
            created_after: Some(created_after),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVector {
    pub global_base: i64,
    pub local_bases: Vec<LocalSnapshotBase>,
    pub include_tx_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxBundle {
    pub tx: TxBundleRecord,
    pub todo_history: Vec<TodoHistoryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxBundleRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub global_epoch: Option<i64>,
    pub kind: String,
    pub base_global_epoch: i64,
    pub base_local_jsonb: String,
    pub base_include_jsonb: String,
    pub read_set_jsonb: String,
    pub write_set_jsonb: String,
    pub status: String,
    pub rejection_reason_json: Option<String>,
    pub created_at: i64,
    pub sealed_at: Option<i64>,
    pub metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TodoHistoryRecord {
    pub row_id: String,
    pub branch_id: String,
    pub tx_id: String,
    pub op: String,
    pub title: Option<String>,
    pub done: Option<i64>,
    pub conflict_tx_ids_jsonb: String,
    pub created_by: Option<String>,
    pub created_at: i64,
    pub updated_by: Option<String>,
    pub updated_at: i64,
    pub edit_metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSnapshotBase {
    pub node_id: String,
    pub local_epoch: i64,
}

impl SnapshotVector {
    pub fn new(global_base: i64) -> Self {
        Self {
            global_base,
            local_bases: Vec::new(),
            include_tx_ids: Vec::new(),
        }
    }

    pub fn with_local_base(mut self, node_id: impl Into<String>, local_epoch: i64) -> Self {
        self.local_bases.push(LocalSnapshotBase {
            node_id: node_id.into(),
            local_epoch,
        });
        self
    }

    pub fn with_include_tx_id(mut self, tx_id: impl Into<String>) -> Self {
        self.include_tx_ids.push(tx_id.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVersionLocator {
    pub table: String,
    pub schema: String,
    pub branch_id: String,
    pub row_id: String,
    pub tx_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub rows: Vec<Todo>,
    pub scope: Vec<RowVersionLocator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionChange {
    Added(Todo),
    Updated { before: Todo, after: Todo },
    Removed(Todo),
}

#[derive(Debug, Clone)]
struct Subscription {
    query: TodoQuery,
    last_rows: Vec<Todo>,
}

pub struct MiniJazzSqlite {
    conn: Connection,
    next_subscription_id: u64,
    subscriptions: BTreeMap<SubscriptionId, Subscription>,
}

impl MiniJazzSqlite {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self {
            conn,
            next_subscription_id: 0,
            subscriptions: BTreeMap::new(),
        };
        db.create_schema()?;
        Ok(db)
    }

    pub fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn,
            next_subscription_id: 0,
            subscriptions: BTreeMap::new(),
        };
        db.create_schema()?;
        Ok(db)
    }

    pub fn create_schema(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS jazz_node (
              node_num INTEGER PRIMARY KEY,
              node_id TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS jazz_tx (
              tx_id TEXT PRIMARY KEY,
              node_num INTEGER NOT NULL,
              local_epoch INTEGER NOT NULL,
              global_epoch INTEGER,
              kind TEXT NOT NULL,
              base_global_epoch INTEGER NOT NULL,
              base_local_jsonb BLOB NOT NULL,
              base_include_jsonb BLOB NOT NULL,
              read_set_jsonb BLOB NOT NULL,
              write_set_jsonb BLOB NOT NULL,
              status TEXT NOT NULL,
              rejection_reason_json TEXT,
              created_at INTEGER NOT NULL,
              sealed_at INTEGER,
              metadata_json TEXT NOT NULL,
              UNIQUE (node_num, local_epoch),
              UNIQUE (global_epoch)
            );

            CREATE TABLE IF NOT EXISTS jazz_branch (
              branch_id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              head_global_epoch INTEGER NOT NULL,
              head_local_jsonb BLOB NOT NULL,
              head_include_jsonb BLOB NOT NULL,
              base_provenance_jsonb BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jazz_branch_history (
              branch_id TEXT NOT NULL,
              tx_id TEXT NOT NULL,
              op TEXT NOT NULL,
              head_global_epoch INTEGER NOT NULL,
              head_local_jsonb BLOB NOT NULL,
              head_include_jsonb BLOB NOT NULL,
              base_provenance_jsonb BLOB NOT NULL,
              metadata_json TEXT NOT NULL,
              PRIMARY KEY (branch_id, tx_id),
              FOREIGN KEY (tx_id) REFERENCES jazz_tx(tx_id)
            );

            CREATE TABLE IF NOT EXISTS todos__schema_v1_history (
              row_id TEXT NOT NULL,
              branch_id TEXT NOT NULL,
              tx_id TEXT NOT NULL,
              op TEXT NOT NULL,
              title TEXT,
              done INTEGER,
              conflict_tx_ids_jsonb BLOB NOT NULL,
              created_by TEXT,
              created_at INTEGER NOT NULL,
              updated_by TEXT,
              updated_at INTEGER NOT NULL,
              edit_metadata_json TEXT NOT NULL,
              PRIMARY KEY (row_id, branch_id, tx_id),
              FOREIGN KEY (tx_id) REFERENCES jazz_tx(tx_id)
            );

            CREATE TABLE IF NOT EXISTS todos__schema_v1_current (
              row_id TEXT NOT NULL,
              branch_id TEXT NOT NULL,
              visible_tx_id TEXT NOT NULL,
              is_deleted INTEGER NOT NULL,
              title TEXT,
              done INTEGER,
              conflict_tx_ids_jsonb BLOB NOT NULL,
              created_by TEXT,
              created_at INTEGER NOT NULL,
              updated_by TEXT,
              updated_at INTEGER NOT NULL,
              edit_metadata_json TEXT NOT NULL,
              PRIMARY KEY (row_id, branch_id)
            );

            CREATE INDEX IF NOT EXISTS todos__schema_v1_current_done_created_at
              ON todos__schema_v1_current(branch_id, done, created_at DESC);

            CREATE INDEX IF NOT EXISTS todos__schema_v1_history_branch_row_updated
              ON todos__schema_v1_history(branch_id, row_id, updated_at DESC, tx_id);
            "#,
        )
    }

    pub fn insert_todo(&mut self, input: InsertTodo) -> rusqlite::Result<()> {
        self.insert_todo_on_branch("main", input)
    }

    pub fn insert_todo_in_branch(
        &mut self,
        branch_id: &str,
        input: InsertTodo,
    ) -> rusqlite::Result<()> {
        self.insert_todo_on_branch(branch_id, input)
    }

    fn insert_todo_on_branch(
        &mut self,
        branch_id: &str,
        input: InsertTodo,
    ) -> rusqlite::Result<()> {
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &input.node_id)?;
        let local_epoch = next_local_epoch(&sql_tx, node_num)?;
        let done = bool_to_sql(input.done);
        let conflict_tx_ids = format!(r#"["{}"]"#, input.tx_id);
        let write_set = format!(
            r#"[{{"table":"todos","rowId":"{}","op":"insert","columns":["title","done","created_at","updated_at"]}}]"#,
            input.row_id
        );

        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'data', 0, '[]', '[]', '[]', ?4, 'local_pending', ?5, ?5, '{}')
            "#,
            params![input.tx_id, node_num, local_epoch, write_set, input.now],
        )?;

        sql_tx.execute(
            r#"
            INSERT INTO todos__schema_v1_history (
              row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
              created_by, created_at, updated_by, updated_at, edit_metadata_json
            ) VALUES (?1, ?2, ?3, 'insert', ?4, ?5, ?6, ?7, ?8, ?7, ?8, '{}')
            "#,
            params![
                input.row_id,
                branch_id,
                input.tx_id,
                input.title,
                done,
                conflict_tx_ids,
                input.actor_id,
                input.now
            ],
        )?;

        if branch_id == "main" {
            sql_tx.execute(
                r#"
                INSERT INTO todos__schema_v1_current (
                  row_id, branch_id, visible_tx_id, is_deleted, title, done,
                  conflict_tx_ids_jsonb, created_by, created_at, updated_by, updated_at,
                  edit_metadata_json
                ) VALUES (?1, 'main', ?2, 0, ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
                "#,
                params![
                    input.row_id,
                    input.tx_id,
                    input.title,
                    done,
                    conflict_tx_ids,
                    input.actor_id,
                    input.now
                ],
            )?;
        }

        sql_tx.commit()
    }

    pub fn insert_todos(&mut self, input: InsertTodos) -> rusqlite::Result<()> {
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &input.node_id)?;
        let local_epoch = next_local_epoch(&sql_tx, node_num)?;
        let write_set_entries = input
            .rows
            .iter()
            .map(|row| {
                format!(
                    r#"{{"table":"todos","rowId":"{}","op":"insert","columns":["title","done","created_at","updated_at"]}}"#,
                    row.row_id
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let write_set = format!("[{write_set_entries}]");

        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'data', 0, '[]', '[]', '[]', ?4, 'local_pending', ?5, ?5, '{}')
            "#,
            params![input.tx_id, node_num, local_epoch, write_set, input.now],
        )?;

        for row in &input.rows {
            let done = bool_to_sql(row.done);
            let conflict_tx_ids = format!(r#"["{}"]"#, input.tx_id);
            sql_tx.execute(
                r#"
                INSERT INTO todos__schema_v1_history (
                  row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
                  created_by, created_at, updated_by, updated_at, edit_metadata_json
                ) VALUES (?1, 'main', ?2, 'insert', ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
                "#,
                params![
                    row.row_id,
                    input.tx_id,
                    row.title,
                    done,
                    conflict_tx_ids,
                    input.actor_id,
                    input.now
                ],
            )?;
            sql_tx.execute(
                r#"
                INSERT INTO todos__schema_v1_current (
                  row_id, branch_id, visible_tx_id, is_deleted, title, done,
                  conflict_tx_ids_jsonb, created_by, created_at, updated_by, updated_at,
                  edit_metadata_json
                ) VALUES (?1, 'main', ?2, 0, ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
                "#,
                params![
                    row.row_id,
                    input.tx_id,
                    row.title,
                    done,
                    conflict_tx_ids,
                    input.actor_id,
                    input.now
                ],
            )?;
        }

        sql_tx.commit()
    }

    pub fn create_branch(&mut self, input: CreateBranch) -> rusqlite::Result<()> {
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &input.node_id)?;
        let local_epoch = next_local_epoch(&sql_tx, node_num)?;
        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'branch_metadata', ?4, '[]', '[]', '[]', '[]',
                     'local_pending', ?5, ?5, '{}')
            "#,
            params![
                input.tx_id,
                node_num,
                local_epoch,
                input.head_global_epoch,
                input.now
            ],
        )?;
        sql_tx.execute(
            r#"
            INSERT INTO jazz_branch (
              branch_id, name, head_global_epoch, head_local_jsonb, head_include_jsonb,
              base_provenance_jsonb
            ) VALUES (?1, ?2, ?3, '[]', '[]', ?4)
            "#,
            params![
                input.branch_id,
                input.name,
                input.head_global_epoch,
                input.base_provenance_json
            ],
        )?;
        sql_tx.execute(
            r#"
            INSERT INTO jazz_branch_history (
              branch_id, tx_id, op, head_global_epoch, head_local_jsonb, head_include_jsonb,
              base_provenance_jsonb, metadata_json
            ) VALUES (?1, ?2, 'create', ?3, '[]', '[]', ?4, '{}')
            "#,
            params![
                input.branch_id,
                input.tx_id,
                input.head_global_epoch,
                input.base_provenance_json
            ],
        )?;
        sql_tx.commit()
    }

    pub fn export_tx(&self, tx_id: &str) -> rusqlite::Result<TxBundle> {
        let tx = self.conn.query_row(
            r#"
            SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.kind,
                   tx.base_global_epoch, tx.base_local_jsonb, tx.base_include_jsonb,
                   tx.read_set_jsonb, tx.write_set_jsonb, tx.status,
                   tx.rejection_reason_json, tx.created_at, tx.sealed_at, tx.metadata_json
            FROM jazz_tx tx
            JOIN jazz_node node ON node.node_num = tx.node_num
            WHERE tx.tx_id = ?1
            "#,
            params![tx_id],
            |row| {
                Ok(TxBundleRecord {
                    tx_id: row.get(0)?,
                    node_id: row.get(1)?,
                    local_epoch: row.get(2)?,
                    global_epoch: row.get(3)?,
                    kind: row.get(4)?,
                    base_global_epoch: row.get(5)?,
                    base_local_jsonb: row.get(6)?,
                    base_include_jsonb: row.get(7)?,
                    read_set_jsonb: row.get(8)?,
                    write_set_jsonb: row.get(9)?,
                    status: row.get(10)?,
                    rejection_reason_json: row.get(11)?,
                    created_at: row.get(12)?,
                    sealed_at: row.get(13)?,
                    metadata_json: row.get(14)?,
                })
            },
        )?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
                   created_by, created_at, updated_by, updated_at, edit_metadata_json
            FROM todos__schema_v1_history
            WHERE tx_id = ?1
            ORDER BY branch_id, row_id
            "#,
        )?;
        let todo_history = stmt
            .query_map(params![tx_id], |row| {
                Ok(TodoHistoryRecord {
                    row_id: row.get(0)?,
                    branch_id: row.get(1)?,
                    tx_id: row.get(2)?,
                    op: row.get(3)?,
                    title: row.get(4)?,
                    done: row.get(5)?,
                    conflict_tx_ids_jsonb: row.get(6)?,
                    created_by: row.get(7)?,
                    created_at: row.get(8)?,
                    updated_by: row.get(9)?,
                    updated_at: row.get(10)?,
                    edit_metadata_json: row.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(TxBundle { tx, todo_history })
    }

    pub fn import_tx(&mut self, bundle: &TxBundle) -> rusqlite::Result<()> {
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &bundle.tx.node_id)?;
        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, global_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, rejection_reason_json, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(tx_id) DO UPDATE SET
              global_epoch = excluded.global_epoch,
              status = excluded.status,
              rejection_reason_json = excluded.rejection_reason_json,
              sealed_at = excluded.sealed_at,
              metadata_json = excluded.metadata_json
            "#,
            params![
                bundle.tx.tx_id,
                node_num,
                bundle.tx.local_epoch,
                bundle.tx.global_epoch,
                bundle.tx.kind,
                bundle.tx.base_global_epoch,
                bundle.tx.base_local_jsonb,
                bundle.tx.base_include_jsonb,
                bundle.tx.read_set_jsonb,
                bundle.tx.write_set_jsonb,
                bundle.tx.status,
                bundle.tx.rejection_reason_json,
                bundle.tx.created_at,
                bundle.tx.sealed_at,
                bundle.tx.metadata_json
            ],
        )?;
        for history in &bundle.todo_history {
            sql_tx.execute(
                r#"
                INSERT OR IGNORE INTO todos__schema_v1_history (
                  row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
                  created_by, created_at, updated_by, updated_at, edit_metadata_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
                params![
                    history.row_id,
                    history.branch_id,
                    history.tx_id,
                    history.op,
                    history.title,
                    history.done,
                    history.conflict_tx_ids_jsonb,
                    history.created_by,
                    history.created_at,
                    history.updated_by,
                    history.updated_at,
                    history.edit_metadata_json
                ],
            )?;
        }
        sql_tx.commit()?;
        if bundle.tx.status == "rejected" {
            self.rebuild_main_current_from_history()?;
        }
        Ok(())
    }

    pub fn update_todo(&mut self, input: UpdateTodo) -> rusqlite::Result<()> {
        let previous = self
            .get_todo("main", &input.row_id)?
            .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let title = input.title.unwrap_or(previous.title);
        let done = input.done.unwrap_or(previous.done);
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &input.node_id)?;
        let local_epoch = next_local_epoch(&sql_tx, node_num)?;
        let done_sql = bool_to_sql(done);
        let conflict_tx_ids = format!(r#"["{}"]"#, input.tx_id);
        let read_set = format!(
            r#"[{{"kind":"row","table":"todos","rowId":"{}","visibleTxId":"{}","reason":"write_base"}}]"#,
            input.row_id, previous.visible_tx_id
        );
        let write_set = format!(
            r#"[{{"table":"todos","rowId":"{}","op":"update","columns":["title","done","updated_at"]}}]"#,
            input.row_id
        );

        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'data', 0, '[]', '[]', ?4, ?5, 'local_pending', ?6, ?6, '{}')
            "#,
            params![
                input.tx_id,
                node_num,
                local_epoch,
                read_set,
                write_set,
                input.now
            ],
        )?;

        sql_tx.execute(
            r#"
            INSERT INTO todos__schema_v1_history (
              row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
              created_by, created_at, updated_by, updated_at, edit_metadata_json
            ) VALUES (?1, 'main', ?2, 'update', ?3, ?4, ?5, ?6, ?7, ?6, ?8, '{}')
            "#,
            params![
                input.row_id,
                input.tx_id,
                title,
                done_sql,
                conflict_tx_ids,
                input.actor_id,
                previous.created_at,
                input.now
            ],
        )?;

        sql_tx.execute(
            r#"
            UPDATE todos__schema_v1_current
            SET visible_tx_id = ?2,
                title = ?3,
                done = ?4,
                conflict_tx_ids_jsonb = ?5,
                updated_by = ?6,
                updated_at = ?7
            WHERE branch_id = 'main' AND row_id = ?1
            "#,
            params![
                input.row_id,
                input.tx_id,
                title,
                done_sql,
                conflict_tx_ids,
                input.actor_id,
                input.now
            ],
        )?;

        sql_tx.commit()
    }

    pub fn delete_todo(&mut self, input: DeleteTodo) -> rusqlite::Result<()> {
        let previous = self
            .get_todo("main", &input.row_id)?
            .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let sql_tx = self.conn.transaction()?;
        let node_num = ensure_node(&sql_tx, &input.node_id)?;
        let local_epoch = next_local_epoch(&sql_tx, node_num)?;
        let conflict_tx_ids = format!(r#"["{}"]"#, input.tx_id);
        let read_set = format!(
            r#"[{{"kind":"row","table":"todos","rowId":"{}","visibleTxId":"{}","reason":"write_base"}}]"#,
            input.row_id, previous.visible_tx_id
        );
        let write_set = format!(
            r#"[{{"table":"todos","rowId":"{}","op":"delete","columns":["is_deleted","updated_at"]}}]"#,
            input.row_id
        );

        sql_tx.execute(
            r#"
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, base_global_epoch,
              base_local_jsonb, base_include_jsonb, read_set_jsonb, write_set_jsonb,
              status, created_at, sealed_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'data', 0, '[]', '[]', ?4, ?5, 'local_pending', ?6, ?6, '{}')
            "#,
            params![
                input.tx_id,
                node_num,
                local_epoch,
                read_set,
                write_set,
                input.now
            ],
        )?;

        sql_tx.execute(
            r#"
            INSERT INTO todos__schema_v1_history (
              row_id, branch_id, tx_id, op, title, done, conflict_tx_ids_jsonb,
              created_by, created_at, updated_by, updated_at, edit_metadata_json
            ) VALUES (?1, 'main', ?2, 'delete', ?3, ?4, ?5, ?6, ?7, ?6, ?8, '{}')
            "#,
            params![
                input.row_id,
                input.tx_id,
                previous.title,
                bool_to_sql(previous.done),
                conflict_tx_ids,
                input.actor_id,
                previous.created_at,
                input.now
            ],
        )?;

        sql_tx.execute(
            r#"
            UPDATE todos__schema_v1_current
            SET visible_tx_id = ?2,
                is_deleted = 1,
                conflict_tx_ids_jsonb = ?3,
                updated_by = ?4,
                updated_at = ?5
            WHERE branch_id = 'main' AND row_id = ?1
            "#,
            params![
                input.row_id,
                input.tx_id,
                conflict_tx_ids,
                input.actor_id,
                input.now
            ],
        )?;

        sql_tx.commit()
    }

    pub fn accept_tx(&self, input: AcceptTx) -> rusqlite::Result<()> {
        let changed = self.conn.execute(
            r#"
            UPDATE jazz_tx
            SET status = 'global_durable_accepted',
                global_epoch = ?2
            WHERE tx_id = ?1
              AND status IN ('local_pending', 'edge_durable', 'global_durable_accepted')
              AND (global_epoch IS NULL OR global_epoch = ?2)
            "#,
            params![input.tx_id, input.global_epoch],
        )?;
        if changed == 0 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        Ok(())
    }

    pub fn reject_tx(&mut self, input: RejectTx) -> rusqlite::Result<()> {
        let changed = self.conn.execute(
            r#"
            UPDATE jazz_tx
            SET status = 'rejected',
                rejection_reason_json = ?2
            WHERE tx_id = ?1
              AND status IN ('local_pending', 'edge_durable')
              AND global_epoch IS NULL
            "#,
            params![input.tx_id, input.reason_json],
        )?;
        if changed == 0 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        self.rebuild_main_current_from_history()
    }

    pub fn open_todos_since(
        &self,
        branch_id: &str,
        created_after: i64,
    ) -> rusqlite::Result<Vec<Todo>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT row_id, title, done, created_at, updated_at, visible_tx_id
            FROM todos__schema_v1_current
            WHERE branch_id = ?1
              AND is_deleted = 0
              AND done = 0
              AND created_at > ?2
            ORDER BY created_at DESC
            "#,
        )?;
        stmt.query_map(params![branch_id, created_after], todo_from_row)?
            .collect()
    }

    pub fn get_todo(&self, branch_id: &str, row_id: &str) -> rusqlite::Result<Option<Todo>> {
        self.conn
            .query_row(
                r#"
                SELECT row_id, title, done, created_at, updated_at, visible_tx_id
                FROM todos__schema_v1_current
                WHERE branch_id = ?1 AND row_id = ?2 AND is_deleted = 0
                "#,
                params![branch_id, row_id],
                todo_from_row,
            )
            .optional()
    }

    pub fn query_todos(&self, query: &TodoQuery) -> rusqlite::Result<QueryResult> {
        let done = query.done.map(bool_to_sql);
        let created_after = query.created_after.unwrap_or(i64::MIN);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT row_id, title, done, created_at, updated_at, visible_tx_id
            FROM todos__schema_v1_current
            WHERE branch_id = ?1
              AND is_deleted = 0
              AND (?2 IS NULL OR done = ?2)
              AND created_at > ?3
            ORDER BY created_at DESC, row_id ASC
            "#,
        )?;
        let rows: Vec<Todo> = stmt
            .query_map(params![query.branch_id, done, created_after], todo_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        let scope = rows
            .iter()
            .map(|row| RowVersionLocator {
                table: "todos".to_owned(),
                schema: "schema_v1".to_owned(),
                branch_id: query.branch_id.clone(),
                row_id: row.row_id.clone(),
                tx_id: row.visible_tx_id.clone(),
                reason: "result".to_owned(),
            })
            .collect();
        Ok(QueryResult { rows, scope })
    }

    pub fn query_todos_at_local_epoch(
        &self,
        query: &TodoQuery,
        node_id: &str,
        local_epoch: i64,
    ) -> rusqlite::Result<QueryResult> {
        let node_num: i64 = self.conn.query_row(
            "SELECT node_num FROM jazz_node WHERE node_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        let done = query.done.map(bool_to_sql);
        let created_after = query.created_after.unwrap_or(i64::MIN);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT h.row_id, h.title, h.done, h.created_at, h.updated_at, h.tx_id
            FROM todos__schema_v1_history h
            JOIN jazz_tx tx ON tx.tx_id = h.tx_id
            WHERE h.branch_id = ?1
              AND tx.node_num = ?2
              AND tx.local_epoch <= ?3
              AND tx.status != 'rejected'
              AND h.op != 'delete'
              AND (?4 IS NULL OR h.done = ?4)
              AND h.created_at > ?5
              AND NOT EXISTS (
                SELECT 1
                FROM todos__schema_v1_history newer_h
                JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer_h.tx_id
                WHERE newer_h.branch_id = h.branch_id
                  AND newer_h.row_id = h.row_id
                  AND newer_tx.node_num = ?2
                  AND newer_tx.local_epoch <= ?3
                  AND newer_tx.status != 'rejected'
                  AND newer_tx.local_epoch > tx.local_epoch
              )
            ORDER BY h.created_at DESC, h.row_id ASC
            "#,
        )?;
        let rows: Vec<Todo> = stmt
            .query_map(
                params![query.branch_id, node_num, local_epoch, done, created_after],
                todo_from_row,
            )?
            .collect::<rusqlite::Result<_>>()?;
        let scope = rows
            .iter()
            .map(|row| RowVersionLocator {
                table: "todos".to_owned(),
                schema: "schema_v1".to_owned(),
                branch_id: query.branch_id.clone(),
                row_id: row.row_id.clone(),
                tx_id: row.visible_tx_id.clone(),
                reason: "result".to_owned(),
            })
            .collect();
        Ok(QueryResult { rows, scope })
    }

    pub fn query_todos_at_global_epoch(
        &self,
        query: &TodoQuery,
        global_epoch: i64,
    ) -> rusqlite::Result<QueryResult> {
        let done = query.done.map(bool_to_sql);
        let created_after = query.created_after.unwrap_or(i64::MIN);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT h.row_id, h.title, h.done, h.created_at, h.updated_at, h.tx_id
            FROM todos__schema_v1_history h
            JOIN jazz_tx tx ON tx.tx_id = h.tx_id
            WHERE h.branch_id = ?1
              AND tx.status = 'global_durable_accepted'
              AND tx.global_epoch <= ?2
              AND h.op != 'delete'
              AND (?3 IS NULL OR h.done = ?3)
              AND h.created_at > ?4
              AND NOT EXISTS (
                SELECT 1
                FROM todos__schema_v1_history newer_h
                JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer_h.tx_id
                WHERE newer_h.branch_id = h.branch_id
                  AND newer_h.row_id = h.row_id
                  AND newer_tx.status = 'global_durable_accepted'
                  AND newer_tx.global_epoch <= ?2
                  AND newer_tx.global_epoch > tx.global_epoch
              )
            ORDER BY h.created_at DESC, h.row_id ASC
            "#,
        )?;
        let rows: Vec<Todo> = stmt
            .query_map(
                params![query.branch_id, global_epoch, done, created_after],
                todo_from_row,
            )?
            .collect::<rusqlite::Result<_>>()?;
        let scope = rows
            .iter()
            .map(|row| RowVersionLocator {
                table: "todos".to_owned(),
                schema: "schema_v1".to_owned(),
                branch_id: query.branch_id.clone(),
                row_id: row.row_id.clone(),
                tx_id: row.visible_tx_id.clone(),
                reason: "result".to_owned(),
            })
            .collect();
        Ok(QueryResult { rows, scope })
    }

    pub fn query_todos_at_snapshot(
        &self,
        query: &TodoQuery,
        snapshot: &SnapshotVector,
    ) -> rusqlite::Result<QueryResult> {
        let mut visible_tx_ids = self.visible_tx_ids(snapshot)?;
        visible_tx_ids.sort();
        visible_tx_ids.dedup();

        let done = query.done.map(bool_to_sql);
        let created_after = query.created_after.unwrap_or(i64::MIN);
        let mut visible_rows = Vec::new();
        let in_list = placeholders(visible_tx_ids.len());
        for tx_id in &visible_tx_ids {
            let sql = format!(
                r#"
                SELECT h.row_id, h.title, h.done, h.created_at, h.updated_at, h.tx_id
                FROM todos__schema_v1_history h
                JOIN jazz_tx tx ON tx.tx_id = h.tx_id
                WHERE h.branch_id = ?1
                  AND h.tx_id = ?2
                  AND h.op != 'delete'
                  AND (?3 IS NULL OR h.done = ?3)
                  AND h.created_at > ?4
                  AND NOT EXISTS (
                    SELECT 1
                    FROM todos__schema_v1_history newer_h
                    JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer_h.tx_id
                    WHERE newer_h.branch_id = h.branch_id
                      AND newer_h.row_id = h.row_id
                      AND newer_h.tx_id IN ({in_list})
                      AND newer_h.tx_id != h.tx_id
                      AND newer_tx.node_num = tx.node_num
                      AND newer_tx.local_epoch > tx.local_epoch
                  )
                ORDER BY h.created_at DESC, h.row_id ASC
                "#
            );
            let mut params: Vec<&dyn rusqlite::ToSql> =
                vec![&query.branch_id, &tx_id, &done, &created_after];
            for visible_tx_id in &visible_tx_ids {
                params.push(visible_tx_id);
            }
            let mut stmt = self.conn.prepare(&sql)?;
            visible_rows.extend(
                stmt.query_map(params.as_slice(), todo_from_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        visible_rows.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        let scope = visible_rows
            .iter()
            .map(|row| RowVersionLocator {
                table: "todos".to_owned(),
                schema: "schema_v1".to_owned(),
                branch_id: query.branch_id.clone(),
                row_id: row.row_id.clone(),
                tx_id: row.visible_tx_id.clone(),
                reason: "result".to_owned(),
            })
            .collect();
        Ok(QueryResult {
            rows: visible_rows,
            scope,
        })
    }

    pub fn query_todos_on_branch(
        &self,
        query: &TodoQuery,
        branch_id: &str,
        global_epoch: i64,
    ) -> rusqlite::Result<QueryResult> {
        let base_global_epoch: i64 = self.conn.query_row(
            "SELECT head_global_epoch FROM jazz_branch WHERE branch_id = ?1",
            params![branch_id],
            |row| row.get(0),
        )?;
        let base_query = TodoQuery {
            branch_id: "main".to_owned(),
            done: query.done,
            created_after: query.created_after,
        };
        let branch_query = TodoQuery {
            branch_id: branch_id.to_owned(),
            done: query.done,
            created_after: query.created_after,
        };
        let mut base = self.query_todos_at_global_epoch(&base_query, base_global_epoch)?;
        let mut branch = self.query_todos_at_global_epoch(&branch_query, global_epoch)?;

        let branch_row_ids = branch
            .rows
            .iter()
            .map(|row| row.row_id.as_str())
            .collect::<Vec<_>>();
        base.rows
            .retain(|row| !branch_row_ids.contains(&row.row_id.as_str()));
        base.scope
            .retain(|locator| !branch_row_ids.contains(&locator.row_id.as_str()));

        branch.rows.extend(base.rows);
        branch.rows.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        branch.scope.extend(base.scope);
        Ok(branch)
    }

    fn visible_tx_ids(&self, snapshot: &SnapshotVector) -> rusqlite::Result<Vec<String>> {
        let mut tx_ids = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT tx_id
                FROM jazz_tx
                WHERE status = 'global_durable_accepted'
                  AND global_epoch <= ?1
                "#,
            )?;
            tx_ids.extend(
                stmt.query_map(params![snapshot.global_base], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        for base in &snapshot.local_bases {
            let mut stmt = self.conn.prepare(
                r#"
                SELECT tx.tx_id
                FROM jazz_tx tx
                JOIN jazz_node node ON node.node_num = tx.node_num
                WHERE node.node_id = ?1
                  AND tx.local_epoch <= ?2
                  AND tx.status != 'rejected'
                "#,
            )?;
            tx_ids.extend(
                stmt.query_map(params![base.node_id, base.local_epoch], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        for tx_id in &snapshot.include_tx_ids {
            let exists: Option<String> = self
                .conn
                .query_row(
                    "SELECT tx_id FROM jazz_tx WHERE tx_id = ?1 AND status != 'rejected'",
                    params![tx_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(tx_id) = exists {
                tx_ids.push(tx_id);
            }
        }
        Ok(tx_ids)
    }

    pub fn subscribe_todos(&mut self, query: TodoQuery) -> rusqlite::Result<SubscriptionId> {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        let last_rows = self.query_todos(&query)?.rows;
        self.subscriptions
            .insert(id, Subscription { query, last_rows });
        Ok(id)
    }

    pub fn poll_subscription(
        &mut self,
        id: SubscriptionId,
    ) -> rusqlite::Result<Vec<SubscriptionChange>> {
        let Some(subscription) = self.subscriptions.get(&id).cloned() else {
            return Ok(Vec::new());
        };
        let next_rows = self.query_todos(&subscription.query)?.rows;
        let changes = diff_rows(&subscription.last_rows, &next_rows);
        self.subscriptions.insert(
            id,
            Subscription {
                query: subscription.query,
                last_rows: next_rows,
            },
        );
        Ok(changes)
    }

    pub fn rebuild_main_current_from_history(&mut self) -> rusqlite::Result<()> {
        let sql_tx = self.conn.transaction()?;
        sql_tx.execute(
            "DELETE FROM todos__schema_v1_current WHERE branch_id = 'main'",
            [],
        )?;
        {
            let mut stmt = sql_tx.prepare(
                r#"
                SELECT todos__schema_v1_history.row_id,
                       todos__schema_v1_history.tx_id,
                       todos__schema_v1_history.op,
                       todos__schema_v1_history.title,
                       todos__schema_v1_history.done,
                       todos__schema_v1_history.conflict_tx_ids_jsonb,
                       todos__schema_v1_history.created_by,
                       todos__schema_v1_history.created_at,
                       todos__schema_v1_history.updated_by,
                       todos__schema_v1_history.updated_at,
                       todos__schema_v1_history.edit_metadata_json
                FROM todos__schema_v1_history
                JOIN jazz_tx ON jazz_tx.tx_id = todos__schema_v1_history.tx_id
                WHERE todos__schema_v1_history.branch_id = 'main'
                  AND jazz_tx.status != 'rejected'
                ORDER BY todos__schema_v1_history.updated_at ASC,
                         todos__schema_v1_history.tx_id ASC
                "#,
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let row_id: String = row.get(0)?;
                let tx_id: String = row.get(1)?;
                let op: String = row.get(2)?;
                let title: String = row.get(3)?;
                let done: i64 = row.get(4)?;
                let conflict_tx_ids: String = row.get(5)?;
                let created_by: String = row.get(6)?;
                let created_at: i64 = row.get(7)?;
                let updated_by: String = row.get(8)?;
                let updated_at: i64 = row.get(9)?;
                let edit_metadata: String = row.get(10)?;
                let is_deleted = if op == "delete" { 1 } else { 0 };

                sql_tx.execute(
                    r#"
                    INSERT INTO todos__schema_v1_current (
                      row_id, branch_id, visible_tx_id, is_deleted, title, done,
                      conflict_tx_ids_jsonb, created_by, created_at, updated_by, updated_at,
                      edit_metadata_json
                    ) VALUES (?1, 'main', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                    ON CONFLICT(row_id, branch_id) DO UPDATE SET
                      visible_tx_id = excluded.visible_tx_id,
                      is_deleted = excluded.is_deleted,
                      title = excluded.title,
                      done = excluded.done,
                      conflict_tx_ids_jsonb = excluded.conflict_tx_ids_jsonb,
                      created_by = excluded.created_by,
                      created_at = excluded.created_at,
                      updated_by = excluded.updated_by,
                      updated_at = excluded.updated_at,
                      edit_metadata_json = excluded.edit_metadata_json
                    "#,
                    params![
                        row_id,
                        tx_id,
                        is_deleted,
                        title,
                        done,
                        conflict_tx_ids,
                        created_by,
                        created_at,
                        updated_by,
                        updated_at,
                        edit_metadata
                    ],
                )?;
            }
        }
        sql_tx.commit()
    }

    pub fn current_projection_fingerprint(&self) -> rusqlite::Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT row_id, branch_id, visible_tx_id, is_deleted, title, done,
                   conflict_tx_ids_jsonb, created_by, created_at, updated_by, updated_at,
                   edit_metadata_json
            FROM todos__schema_v1_current
            ORDER BY branch_id, row_id
            "#,
        )?;
        stmt.query_map([], |row| {
            let fields = [
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?.to_string(),
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?.to_string(),
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?.to_string(),
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?.to_string(),
                row.get::<_, String>(11)?,
            ];
            Ok(fields.join("|"))
        })?
        .collect()
    }
}

fn diff_rows(previous: &[Todo], next: &[Todo]) -> Vec<SubscriptionChange> {
    let previous_by_id: BTreeMap<&str, &Todo> = previous
        .iter()
        .map(|row| (row.row_id.as_str(), row))
        .collect();
    let next_by_id: BTreeMap<&str, &Todo> =
        next.iter().map(|row| (row.row_id.as_str(), row)).collect();
    let mut changes = Vec::new();

    for row in next {
        match previous_by_id.get(row.row_id.as_str()) {
            None => changes.push(SubscriptionChange::Added(row.clone())),
            Some(before) if *before != row => changes.push(SubscriptionChange::Updated {
                before: (*before).clone(),
                after: row.clone(),
            }),
            Some(_) => {}
        }
    }

    for row in previous {
        if !next_by_id.contains_key(row.row_id.as_str()) {
            changes.push(SubscriptionChange::Removed(row.clone()));
        }
    }

    changes
}

fn todo_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Todo> {
    Ok(Todo {
        row_id: row.get(0)?,
        title: row.get(1)?,
        done: sql_to_bool(row.get(2)?),
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
        visible_tx_id: row.get(5)?,
    })
}

fn ensure_node(conn: &Connection, node_id: &str) -> rusqlite::Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_node (node_id) VALUES (?1)",
        params![node_id],
    )?;
    conn.query_row(
        "SELECT node_num FROM jazz_node WHERE node_id = ?1",
        params![node_id],
        |row| row.get(0),
    )
}

fn next_local_epoch(conn: &Connection, node_num: i64) -> rusqlite::Result<i64> {
    let current: Option<i64> = conn.query_row(
        "SELECT MAX(local_epoch) FROM jazz_tx WHERE node_num = ?1",
        params![node_num],
        |row| row.get(0),
    )?;
    Ok(current.unwrap_or(0) + 1)
}

fn bool_to_sql(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn sql_to_bool(value: i64) -> bool {
    value != 0
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn insert_and_query_open_todos_by_system_column() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Write lowering".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-2".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: "Already done".into(),
            done: true,
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();

        let rows = db.open_todos_since("main", 50).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, "todo-1");
        assert_eq!(rows[0].visible_tx_id, "tx-1");
    }

    #[test]
    fn update_records_previous_visible_version_in_read_set() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Draft".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();

        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: Some("Updated".into()),
            done: None,
            actor_id: "alice".into(),
            now: 150,
        })
        .unwrap();

        let row = db.get_todo("main", "todo-1").unwrap().unwrap();
        assert_eq!(row.title, "Updated");
        assert_eq!(row.visible_tx_id, "tx-2");

        let read_set: String = db
            .conn
            .query_row(
                "SELECT read_set_jsonb FROM jazz_tx WHERE tx_id = 'tx-2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(read_set.contains(r#""visibleTxId":"tx-1""#));
    }

    #[test]
    fn delete_hides_current_row_but_keeps_history() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Delete me".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();

        db.delete_todo(DeleteTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();

        assert!(db.get_todo("main", "todo-1").unwrap().is_none());
        let history_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM todos__schema_v1_history WHERE row_id = 'todo-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(history_count, 2);
    }

    #[test]
    fn query_returns_result_scope_locators() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Older".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-2".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: "Newer".into(),
            done: false,
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();

        let result = db.query_todos(&TodoQuery::open_since(0)).unwrap();

        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>(),
            vec!["todo-2", "todo-1"]
        );
        assert_eq!(result.scope.len(), 2);
        assert_eq!(result.scope[0].row_id, "todo-2");
        assert_eq!(result.scope[0].tx_id, "tx-2");
        assert_eq!(result.scope[0].reason, "result");
    }

    #[test]
    fn subscription_reports_added_updated_and_removed_rows() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        let subscription = db.subscribe_todos(TodoQuery::open_since(0)).unwrap();
        assert!(db.poll_subscription(subscription).unwrap().is_empty());

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Appears".into(),
            done: true,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        assert!(db.poll_subscription(subscription).unwrap().is_empty());

        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: None,
            done: Some(false),
            actor_id: "alice".into(),
            now: 150,
        })
        .unwrap();
        assert!(matches!(
            db.poll_subscription(subscription).unwrap().as_slice(),
            [SubscriptionChange::Added(row)] if row.row_id == "todo-1"
        ));

        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-3".into(),
            node_id: "alice-device".into(),
            title: Some("Changed".into()),
            done: None,
            actor_id: "alice".into(),
            now: 175,
        })
        .unwrap();
        assert!(matches!(
            db.poll_subscription(subscription).unwrap().as_slice(),
            [SubscriptionChange::Updated { before, after }]
                if before.title == "Appears" && after.title == "Changed"
        ));

        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-4".into(),
            node_id: "alice-device".into(),
            title: None,
            done: Some(true),
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();
        assert!(matches!(
            db.poll_subscription(subscription).unwrap().as_slice(),
            [SubscriptionChange::Removed(row)] if row.row_id == "todo-1"
        ));
    }

    #[test]
    fn local_epoch_snapshot_query_reads_history_without_current_projection() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        let query = TodoQuery::open_since(0);

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "First title".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: Some("Second title".into()),
            done: None,
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();
        db.delete_todo(DeleteTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-3".into(),
            node_id: "alice-device".into(),
            actor_id: "alice".into(),
            now: 300,
        })
        .unwrap();

        let at_first = db
            .query_todos_at_local_epoch(&query, "alice-device", 1)
            .unwrap();
        assert_eq!(at_first.rows[0].title, "First title");
        assert_eq!(at_first.rows[0].visible_tx_id, "tx-1");

        let at_second = db
            .query_todos_at_local_epoch(&query, "alice-device", 2)
            .unwrap();
        assert_eq!(at_second.rows[0].title, "Second title");
        assert_eq!(at_second.rows[0].visible_tx_id, "tx-2");

        let after_delete = db
            .query_todos_at_local_epoch(&query, "alice-device", 3)
            .unwrap();
        assert!(after_delete.rows.is_empty());
    }

    #[test]
    fn global_epoch_snapshot_only_sees_accepted_transactions() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        let query = TodoQuery::open_since(0);

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Local only".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.update_todo(UpdateTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-2".into(),
            node_id: "alice-device".into(),
            title: Some("Accepted update".into()),
            done: None,
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();

        assert!(
            db.query_todos_at_global_epoch(&query, 1)
                .unwrap()
                .rows
                .is_empty()
        );

        db.accept_tx(AcceptTx {
            tx_id: "tx-1".into(),
            global_epoch: 1,
        })
        .unwrap();
        let at_first_global = db.query_todos_at_global_epoch(&query, 1).unwrap();
        assert_eq!(at_first_global.rows[0].title, "Local only");
        assert_eq!(at_first_global.rows[0].visible_tx_id, "tx-1");

        db.accept_tx(AcceptTx {
            tx_id: "tx-2".into(),
            global_epoch: 2,
        })
        .unwrap();
        let at_second_global = db.query_todos_at_global_epoch(&query, 2).unwrap();
        assert_eq!(at_second_global.rows[0].title, "Accepted update");
        assert_eq!(at_second_global.rows[0].visible_tx_id, "tx-2");
    }

    #[test]
    fn rejected_transaction_stays_in_history_but_out_of_snapshots() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        let query = TodoQuery::open_since(0);

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Rejected insert".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.reject_tx(RejectTx {
            tx_id: "tx-1".into(),
            reason_json: r#"{"code":"permission_denied"}"#.into(),
        })
        .unwrap();

        assert!(
            db.query_todos_at_local_epoch(&query, "alice-device", 1)
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(
            db.query_todos_at_global_epoch(&query, 1)
                .unwrap()
                .rows
                .is_empty()
        );

        let rejection_reason: String = db
            .conn
            .query_row(
                "SELECT rejection_reason_json FROM jazz_tx WHERE tx_id = 'tx-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(rejection_reason.contains("permission_denied"));

        let history_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM todos__schema_v1_history WHERE tx_id = 'tx-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(history_count, 1);
    }

    #[test]
    fn rejecting_local_insert_repairs_main_current_projection() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-1".into(),
            node_id: "alice-device".into(),
            title: "Optimistic".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        assert!(db.get_todo("main", "todo-1").unwrap().is_some());

        db.reject_tx(RejectTx {
            tx_id: "tx-1".into(),
            reason_json: r#"{"code":"permission_denied"}"#.into(),
        })
        .unwrap();

        assert!(db.get_todo("main", "todo-1").unwrap().is_none());
        assert_eq!(
            db.current_projection_fingerprint().unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn snapshot_vector_combines_global_base_local_bases_and_txid_includes() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();
        let query = TodoQuery::open_since(0);

        db.insert_todo(InsertTodo {
            row_id: "todo-1".into(),
            tx_id: "tx-global".into(),
            node_id: "alice-device".into(),
            title: "Accepted globally".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.accept_tx(AcceptTx {
            tx_id: "tx-global".into(),
            global_epoch: 7,
        })
        .unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-2".into(),
            tx_id: "tx-local".into(),
            node_id: "alice-device".into(),
            title: "Local base".into(),
            done: false,
            actor_id: "alice".into(),
            now: 200,
        })
        .unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-3".into(),
            tx_id: "tx-include".into(),
            node_id: "bob-phone".into(),
            title: "Explicit include".into(),
            done: false,
            actor_id: "bob".into(),
            now: 300,
        })
        .unwrap();
        db.insert_todo(InsertTodo {
            row_id: "todo-4".into(),
            tx_id: "tx-hidden".into(),
            node_id: "bob-phone".into(),
            title: "Hidden".into(),
            done: false,
            actor_id: "bob".into(),
            now: 400,
        })
        .unwrap();

        let snapshot = SnapshotVector::new(7)
            .with_local_base("alice-device", 2)
            .with_include_tx_id("tx-include");
        let result = db.query_todos_at_snapshot(&query, &snapshot).unwrap();

        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>(),
            vec!["todo-3", "todo-2", "todo-1"]
        );
        assert_eq!(
            result
                .scope
                .iter()
                .map(|locator| locator.tx_id.as_str())
                .collect::<Vec<_>>(),
            vec!["tx-include", "tx-local", "tx-global"]
        );
    }

    #[test]
    fn branch_local_write_is_global_history_but_not_main_visibility() {
        let mut db = MiniJazzSqlite::in_memory().unwrap();

        db.insert_todo(InsertTodo {
            row_id: "todo-main".into(),
            tx_id: "tx-main".into(),
            node_id: "alice-device".into(),
            title: "Main row".into(),
            done: false,
            actor_id: "alice".into(),
            now: 100,
        })
        .unwrap();
        db.accept_tx(AcceptTx {
            tx_id: "tx-main".into(),
            global_epoch: 1,
        })
        .unwrap();
        db.create_branch(CreateBranch {
            branch_id: "draft".into(),
            tx_id: "tx-create-draft".into(),
            node_id: "alice-device".into(),
            name: "Alice draft".into(),
            head_global_epoch: 1,
            base_provenance_json: r#"[{"branch":"main","globalBase":1}]"#.into(),
            now: 150,
        })
        .unwrap();
        db.accept_tx(AcceptTx {
            tx_id: "tx-create-draft".into(),
            global_epoch: 2,
        })
        .unwrap();
        db.insert_todo_in_branch(
            "draft",
            InsertTodo {
                row_id: "todo-draft".into(),
                tx_id: "tx-draft-row".into(),
                node_id: "alice-device".into(),
                title: "Draft row".into(),
                done: false,
                actor_id: "alice".into(),
                now: 200,
            },
        )
        .unwrap();
        db.accept_tx(AcceptTx {
            tx_id: "tx-draft-row".into(),
            global_epoch: 3,
        })
        .unwrap();

        let main_rows = db
            .query_todos_at_global_epoch(&TodoQuery::open_since(0), 3)
            .unwrap();
        assert_eq!(
            main_rows
                .rows
                .iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>(),
            vec!["todo-main"]
        );

        let draft_rows = db
            .query_todos_on_branch(
                &TodoQuery {
                    branch_id: "draft".into(),
                    done: Some(false),
                    created_after: Some(0),
                },
                "draft",
                3,
            )
            .unwrap();
        assert_eq!(
            draft_rows
                .rows
                .iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>(),
            vec!["todo-draft", "todo-main"]
        );

        let branch_history_count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM jazz_branch_history WHERE branch_id = 'draft'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(branch_history_count, 1);
    }

    #[test]
    fn transaction_bundle_import_can_be_accepted_by_an_authority_store() {
        let mut alice = MiniJazzSqlite::in_memory().unwrap();
        let mut authority = MiniJazzSqlite::in_memory().unwrap();

        alice
            .insert_todo(InsertTodo {
                row_id: "todo-1".into(),
                tx_id: "tx-alice-1".into(),
                node_id: "alice-device".into(),
                title: "Sync me".into(),
                done: false,
                actor_id: "alice".into(),
                now: 100,
            })
            .unwrap();

        let bundle = alice.export_tx("tx-alice-1").unwrap();
        authority.import_tx(&bundle).unwrap();
        authority
            .accept_tx(AcceptTx {
                tx_id: "tx-alice-1".into(),
                global_epoch: 1,
            })
            .unwrap();

        let accepted = authority
            .query_todos_at_global_epoch(&TodoQuery::open_since(0), 1)
            .unwrap();
        assert_eq!(accepted.rows[0].title, "Sync me");
        assert_eq!(accepted.rows[0].visible_tx_id, "tx-alice-1");
    }

    #[test]
    fn authority_fate_bundle_upgrades_existing_client_transaction() {
        let mut alice = MiniJazzSqlite::in_memory().unwrap();
        let mut authority = MiniJazzSqlite::in_memory().unwrap();

        alice
            .insert_todo(InsertTodo {
                row_id: "todo-1".into(),
                tx_id: "tx-alice-1".into(),
                node_id: "alice-device".into(),
                title: "Sync me back".into(),
                done: false,
                actor_id: "alice".into(),
                now: 100,
            })
            .unwrap();
        authority
            .import_tx(&alice.export_tx("tx-alice-1").unwrap())
            .unwrap();
        authority
            .accept_tx(AcceptTx {
                tx_id: "tx-alice-1".into(),
                global_epoch: 1,
            })
            .unwrap();

        alice
            .import_tx(&authority.export_tx("tx-alice-1").unwrap())
            .unwrap();

        let global_rows = alice
            .query_todos_at_global_epoch(&TodoQuery::open_since(0), 1)
            .unwrap();
        assert_eq!(global_rows.rows[0].title, "Sync me back");

        let status: (String, i64) = alice
            .conn
            .query_row(
                "SELECT status, global_epoch FROM jazz_tx WHERE tx_id = 'tx-alice-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, ("global_durable_accepted".into(), 1));
    }

    #[test]
    fn authority_rejection_bundle_repairs_existing_client_current() {
        let mut alice = MiniJazzSqlite::in_memory().unwrap();
        let mut authority = MiniJazzSqlite::in_memory().unwrap();

        alice
            .insert_todo(InsertTodo {
                row_id: "todo-1".into(),
                tx_id: "tx-alice-1".into(),
                node_id: "alice-device".into(),
                title: "Reject me remotely".into(),
                done: false,
                actor_id: "alice".into(),
                now: 100,
            })
            .unwrap();
        assert!(alice.get_todo("main", "todo-1").unwrap().is_some());

        authority
            .import_tx(&alice.export_tx("tx-alice-1").unwrap())
            .unwrap();
        authority
            .reject_tx(RejectTx {
                tx_id: "tx-alice-1".into(),
                reason_json: r#"{"code":"permission_denied"}"#.into(),
            })
            .unwrap();

        alice
            .import_tx(&authority.export_tx("tx-alice-1").unwrap())
            .unwrap();

        assert!(alice.get_todo("main", "todo-1").unwrap().is_none());
        let status: (String, String) = alice
            .conn
            .query_row(
                "SELECT status, rejection_reason_json FROM jazz_tx WHERE tx_id = 'tx-alice-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            status,
            ("rejected".into(), r#"{"code":"permission_denied"}"#.into())
        );
    }

    #[test]
    fn one_transaction_can_write_multiple_rows_and_sync_as_one_bundle() {
        let mut alice = MiniJazzSqlite::in_memory().unwrap();
        let mut authority = MiniJazzSqlite::in_memory().unwrap();

        alice
            .insert_todos(InsertTodos {
                tx_id: "tx-two-rows".into(),
                node_id: "alice-device".into(),
                rows: vec![
                    NewTodoRow {
                        row_id: "todo-1".into(),
                        title: "First".into(),
                        done: false,
                    },
                    NewTodoRow {
                        row_id: "todo-2".into(),
                        title: "Second".into(),
                        done: false,
                    },
                ],
                actor_id: "alice".into(),
                now: 100,
            })
            .unwrap();

        let bundle = alice.export_tx("tx-two-rows").unwrap();
        assert_eq!(bundle.todo_history.len(), 2);
        authority.import_tx(&bundle).unwrap();
        authority
            .accept_tx(AcceptTx {
                tx_id: "tx-two-rows".into(),
                global_epoch: 1,
            })
            .unwrap();

        let rows = authority
            .query_todos_at_global_epoch(&TodoQuery::open_since(0), 1)
            .unwrap()
            .rows;
        assert_eq!(
            rows.iter()
                .map(|row| row.row_id.as_str())
                .collect::<Vec<_>>(),
            vec!["todo-1", "todo-2"]
        );
    }

    #[test]
    fn file_database_survives_reopen_and_projection_rebuild_is_byte_identical() {
        let path = std::env::temp_dir().join(format!(
            "mini-jazz-sqlite-{}-{}.db",
            std::process::id(),
            "rebuild"
        ));
        let _ = fs::remove_file(&path);

        {
            let mut db = MiniJazzSqlite::open(&path).unwrap();
            db.insert_todo(InsertTodo {
                row_id: "todo-1".into(),
                tx_id: "tx-1".into(),
                node_id: "alice-device".into(),
                title: "Persistent".into(),
                done: false,
                actor_id: "alice".into(),
                now: 100,
            })
            .unwrap();
            db.update_todo(UpdateTodo {
                row_id: "todo-1".into(),
                tx_id: "tx-2".into(),
                node_id: "alice-device".into(),
                title: Some("Persistent updated".into()),
                done: None,
                actor_id: "alice".into(),
                now: 200,
            })
            .unwrap();
        }

        let mut reopened = MiniJazzSqlite::open(&path).unwrap();
        let row = reopened.get_todo("main", "todo-1").unwrap().unwrap();
        assert_eq!(row.title, "Persistent updated");

        let before = reopened.current_projection_fingerprint().unwrap();
        reopened.rebuild_main_current_from_history().unwrap();
        let after = reopened.current_projection_fingerprint().unwrap();
        assert_eq!(before, after);

        fs::remove_file(path).unwrap();
    }
}
