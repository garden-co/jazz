use std::collections::BTreeMap;

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
            "#,
        )
    }

    pub fn insert_todo(&mut self, input: InsertTodo) -> rusqlite::Result<()> {
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
            ) VALUES (?1, 'main', ?2, 'insert', ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
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

        sql_tx.commit()
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
            ) VALUES (?1, 'main', ?2, 'update', ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
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
            ) VALUES (?1, 'main', ?2, 'delete', ?3, ?4, ?5, ?6, ?7, ?6, ?7, '{}')
            "#,
            params![
                input.row_id,
                input.tx_id,
                previous.title,
                bool_to_sql(previous.done),
                conflict_tx_ids,
                input.actor_id,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
