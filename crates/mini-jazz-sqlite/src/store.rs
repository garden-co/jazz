use crate::layout::{json_to_sql, placeholders, quote_ident, read_field, TablePlan};
use crate::query::{filter_sql, Include, LoweredInclude, Query, SortDirection};
use crate::schema::{FieldKind, Schema, TableDef};
use crate::scope::{
    diff_rows, HistoryRecord, QueryResult, QueryScope, QueryScopeBundle, RowView, ScopeReason,
    ScopeRow, SubscriptionDiff, TxRecord,
};
use crate::write::WriteTx;
use crate::{Error, Result};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Harness;

impl Harness {
    pub fn new() -> Self {
        Self
    }

    pub fn client(self, node_id: &str, schema: Schema) -> ClientBuilder {
        ClientBuilder {
            node_id: node_id.to_owned(),
            schema,
        }
    }

    pub fn authority(self, node_id: &str, schema: Schema) -> ClientBuilder {
        self.client(node_id, schema)
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClientBuilder {
    node_id: String,
    schema: Schema,
}

impl ClientBuilder {
    pub fn durable_at(self, path: &Path) -> Result<Client> {
        let conn = Connection::open(path)?;
        Client::open(self.node_id, self.schema, conn)
    }

    pub fn durable_in_memory(self) -> Result<Client> {
        let conn = Connection::open_in_memory()?;
        Client::open(self.node_id, self.schema, conn)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SubscriptionId(u64);

struct Subscription {
    query: Query,
    previous_rows: Vec<RowView>,
}

pub struct Client {
    node_id: String,
    schema: Schema,
    conn: Connection,
    subscriptions: BTreeMap<SubscriptionId, Subscription>,
    next_subscription_id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TxMetadata {
    #[serde(default)]
    read_set: Vec<ReadSetEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReadSetEntry {
    pub(crate) table: String,
    pub(crate) row_id: String,
    pub(crate) visible_tx_id: String,
}

impl Client {
    fn open(node_id: String, schema: Schema, conn: Connection) -> Result<Self> {
        let mut client = Client {
            node_id,
            schema,
            conn,
            subscriptions: BTreeMap::new(),
            next_subscription_id: 1,
        };
        client.bootstrap()?;
        Ok(client)
    }

    fn bootstrap(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "
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
              status TEXT NOT NULL,
              rejection_reason_json TEXT,
              created_at INTEGER NOT NULL,
              metadata_json TEXT NOT NULL,
              UNIQUE (node_num, local_epoch),
              UNIQUE (global_epoch)
            );

            CREATE INDEX IF NOT EXISTS jazz_tx_status_global_epoch
              ON jazz_tx(status, global_epoch, tx_id);
            ",
        )?;

        self.ensure_node()?;
        for table in self.schema.tables.values() {
            self.create_table_storage(table)?;
        }
        Ok(())
    }

    fn ensure_node(&self) -> Result<i64> {
        self.conn.execute(
            "INSERT OR IGNORE INTO jazz_node (node_id) VALUES (?1)",
            params![self.node_id],
        )?;
        self.conn
            .query_row(
                "SELECT node_num FROM jazz_node WHERE node_id = ?1",
                params![self.node_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn create_table_storage(&self, table: &TableDef) -> Result<()> {
        let plan = TablePlan::new(table);

        self.conn.execute_batch(&format!(
            "
            CREATE TABLE IF NOT EXISTS {history} (
              j_row_id TEXT NOT NULL,
              j_branch_id TEXT NOT NULL,
              j_tx_id TEXT NOT NULL,
              j_op TEXT NOT NULL,
              {user_columns},
              j_conflicts_json TEXT NOT NULL,
              j_created_at INTEGER NOT NULL,
              j_updated_at INTEGER NOT NULL,
              PRIMARY KEY (j_row_id, j_branch_id, j_tx_id),
              FOREIGN KEY (j_tx_id) REFERENCES jazz_tx(tx_id)
            );

            CREATE INDEX IF NOT EXISTS {}
              ON {history}(j_branch_id, j_row_id, j_updated_at DESC, j_tx_id);

            CREATE INDEX IF NOT EXISTS {}
              ON {history}(j_branch_id, j_tx_id, j_row_id);

            CREATE TABLE IF NOT EXISTS {current} (
              j_row_id TEXT NOT NULL,
              j_branch_id TEXT NOT NULL,
              j_visible_tx_id TEXT NOT NULL,
              j_is_deleted INTEGER NOT NULL,
              {user_columns},
              j_conflicts_json TEXT NOT NULL,
              j_created_at INTEGER NOT NULL,
              j_updated_at INTEGER NOT NULL,
              PRIMARY KEY (j_row_id, j_branch_id)
            );
            ",
            plan.index_name("history_branch_row_updated"),
            plan.index_name("history_branch_tx"),
            history = plan.history,
            current = plan.current,
            user_columns = plan.user_column_defs()
        ))?;

        for index in &table.indexes {
            let cols = index
                .columns
                .iter()
                .map(|column| plan.physical_column(column))
                .collect::<Vec<_>>()
                .join(", ");
            self.conn.execute_batch(&format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}(j_branch_id, {cols});",
                plan.current_index_name(index),
                plan.current
            ))?;
        }

        Ok(())
    }

    pub fn write(&mut self, write: impl FnOnce(&mut WriteTx<'_>) -> Result<()>) -> Result<()> {
        let node_num = self.ensure_node()?;
        let local_epoch = self.next_local_epoch(node_num)?;
        let tx_id = format!("{}:{local_epoch}", self.node_id);
        let now = now_millis();

        let sql_tx = self.conn.transaction()?;
        sql_tx.execute(
            "
            INSERT INTO jazz_tx (
              tx_id, node_num, local_epoch, kind, status, created_at, metadata_json
            ) VALUES (?1, ?2, ?3, 'data', 'local_pending', ?4, '{}')
            ",
            params![tx_id, node_num, local_epoch, now],
        )?;

        let mut write_tx = WriteTx {
            schema: &self.schema,
            conn: &sql_tx,
            tx_id: tx_id.clone(),
            local_epoch,
            now,
            read_set: Vec::new(),
        };
        write(&mut write_tx)?;
        let metadata = TxMetadata {
            read_set: write_tx.read_set,
        };
        sql_tx.execute(
            "UPDATE jazz_tx SET metadata_json = ?2 WHERE tx_id = ?1",
            params![
                tx_id,
                serde_json::to_string(&metadata)
                    .map_err(|err| Error::new(format!("serialize tx metadata: {err}")))?
            ],
        )?;
        sql_tx.commit()?;
        Ok(())
    }

    fn next_local_epoch(&self, node_num: i64) -> Result<i64> {
        let current: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(local_epoch) FROM jazz_tx WHERE node_num = ?1",
                params![node_num],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(current.unwrap_or(0) + 1)
    }

    pub fn all(&self, query: Query) -> Result<QueryResult> {
        let table = self.schema.table_def(&query.table)?;
        let plan = TablePlan::new(table);

        let mut select_cols = vec![
            "base.j_row_id".to_owned(),
            "base.j_visible_tx_id".to_owned(),
            "base.j_created_at".to_owned(),
        ];
        for field in &table.fields {
            select_cols.push(format!("base.{}", quote_ident(&field.name)));
        }

        let include = query
            .include
            .as_ref()
            .map(|include| self.lower_include(table, include))
            .transpose()?;

        if let Some(include) = &include {
            select_cols.push("dep.j_row_id".to_owned());
            select_cols.push("dep.j_visible_tx_id".to_owned());
            select_cols.push("dep.j_created_at".to_owned());
            for field in &include.table.fields {
                select_cols.push(format!("dep.{}", quote_ident(&field.name)));
            }
        }

        let mut sql = format!(
            "SELECT {} FROM {} base",
            select_cols.join(", "),
            plan.current
        );

        if let Some(include) = &include {
            let include_plan = TablePlan::new(include.table);
            sql.push_str(&format!(
                " {} {} dep ON dep.j_branch_id = base.j_branch_id \
                 AND dep.j_row_id = base.{} AND dep.j_is_deleted = 0",
                if include.required {
                    "INNER JOIN"
                } else {
                    "LEFT JOIN"
                },
                include_plan.current,
                quote_ident(&include.fk_field.name)
            ));
        }

        let mut where_sql = vec![
            "base.j_branch_id = ?".to_owned(),
            "base.j_is_deleted = 0".to_owned(),
        ];
        let mut sql_params = vec![SqlValue::Text("main".to_owned())];
        for filter in &query.filters {
            where_sql.push(filter_sql("base", filter));
            sql_params.push(filter.value.to_sql_value());
        }
        sql.push_str(" WHERE ");
        sql.push_str(&where_sql.join(" AND "));

        if let Some(order) = &query.order {
            sql.push_str(" ORDER BY ");
            sql.push_str(&plan.aliased_column("base", &order.column));
            sql.push(' ');
            sql.push_str(match order.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            });
        }

        if let Some(limit) = query.limit {
            sql.push_str(" LIMIT ?");
            sql_params.push(SqlValue::Integer(limit as i64));
        }

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(sql_params), |row| {
            let mut idx = 0;
            let row_id: String = row.get(idx)?;
            idx += 1;
            let visible_tx_id: String = row.get(idx)?;
            idx += 1;
            let created_at: i64 = row.get(idx)?;
            idx += 1;

            let mut values = BTreeMap::new();
            values.insert("$rowId".to_owned(), JsonValue::String(row_id.clone()));
            values.insert("$txId".to_owned(), JsonValue::String(visible_tx_id.clone()));
            values.insert("$createdAt".to_owned(), JsonValue::from(created_at));
            for field in &table.fields {
                values.insert(field.name.clone(), read_field(row, idx, field)?);
                idx += 1;
            }

            let mut includes = BTreeMap::new();
            let mut dependency_scope = Vec::new();
            if let Some(include) = &include {
                let dep_row_id: Option<String> = row.get(idx)?;
                idx += 1;
                let dep_tx_id: Option<String> = row.get(idx)?;
                idx += 1;
                let dep_created_at: Option<i64> = row.get(idx)?;
                idx += 1;
                if let (Some(dep_row_id), Some(dep_tx_id), Some(dep_created_at)) =
                    (dep_row_id, dep_tx_id, dep_created_at)
                {
                    let mut dep_values = BTreeMap::new();
                    dep_values.insert("$rowId".to_owned(), JsonValue::String(dep_row_id.clone()));
                    dep_values.insert("$txId".to_owned(), JsonValue::String(dep_tx_id.clone()));
                    dep_values.insert("$createdAt".to_owned(), JsonValue::from(dep_created_at));
                    for field in &include.table.fields {
                        dep_values.insert(field.name.clone(), read_field(row, idx, field)?);
                        idx += 1;
                    }
                    includes.insert(
                        include.alias.clone(),
                        RowView {
                            values: dep_values,
                            includes: BTreeMap::new(),
                        },
                    );
                    dependency_scope.push(ScopeRow {
                        table: include.table.name.clone(),
                        row_id: dep_row_id,
                        tx_id: dep_tx_id,
                        reason: ScopeReason::Dependency,
                    });
                }
            }

            Ok((
                RowView { values, includes },
                ScopeRow {
                    table: table.name.clone(),
                    row_id,
                    tx_id: visible_tx_id,
                    reason: ScopeReason::Result,
                },
                dependency_scope,
            ))
        })?;

        let mut result_rows = Vec::new();
        let mut result_scope = Vec::new();
        let mut dependency_scope = Vec::new();
        for row in rows {
            let (view, result_locator, dependencies) = row?;
            result_rows.push(view);
            result_scope.push(result_locator);
            dependency_scope.extend(dependencies);
        }

        Ok(QueryResult {
            rows: result_rows,
            scope: QueryScope {
                result_rows: result_scope,
                dependency_rows: dependency_scope,
            },
        })
    }

    pub fn subscribe(&mut self, query: Query) -> Result<SubscriptionId> {
        let initial = self.all(query.clone())?;
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        self.subscriptions.insert(
            id,
            Subscription {
                query,
                previous_rows: initial.rows,
            },
        );
        Ok(id)
    }

    pub fn poll_subscription(&mut self, id: SubscriptionId) -> Result<SubscriptionDiff> {
        let query = self
            .subscriptions
            .get(&id)
            .ok_or_else(|| Error::new("unknown subscription"))?
            .query
            .clone();
        let next = self.all(query)?;
        let subscription = self
            .subscriptions
            .get_mut(&id)
            .ok_or_else(|| Error::new("unknown subscription"))?;
        let diff = diff_rows(&subscription.previous_rows, &next.rows);
        subscription.previous_rows = next.rows;
        Ok(diff)
    }

    pub fn export_query_scope(&self, scope: &QueryScope) -> Result<QueryScopeBundle> {
        let mut txs = BTreeMap::new();
        let mut rows = Vec::new();
        let mut scoped_rows = BTreeMap::new();

        for locator in scope.result_rows.iter().chain(&scope.dependency_rows) {
            scoped_rows.insert((locator.table.clone(), locator.row_id.clone()), ());
        }

        for ((table_name, row_id), ()) in scoped_rows {
            let table = self.schema.table_def(&table_name)?;
            let plan = TablePlan::new(table);
            let mut select_cols = vec![
                "h.j_row_id".to_owned(),
                "h.j_branch_id".to_owned(),
                "h.j_tx_id".to_owned(),
                "h.j_op".to_owned(),
                "h.j_conflicts_json".to_owned(),
                "h.j_created_at".to_owned(),
                "h.j_updated_at".to_owned(),
                "tx.node_num".to_owned(),
                "node.node_id".to_owned(),
                "tx.local_epoch".to_owned(),
                "tx.global_epoch".to_owned(),
                "tx.kind".to_owned(),
                "tx.status".to_owned(),
                "tx.rejection_reason_json".to_owned(),
                "tx.created_at".to_owned(),
                "tx.metadata_json".to_owned(),
            ];
            select_cols.extend(plan.user_columns.iter().map(|column| format!("h.{column}")));

            let mut stmt = self.conn.prepare(&format!(
                "
                SELECT {}
                FROM {} h
                JOIN jazz_tx tx ON tx.tx_id = h.j_tx_id
                JOIN jazz_node node ON node.node_num = tx.node_num
                WHERE h.j_branch_id = 'main' AND h.j_row_id = ?
                ORDER BY h.j_updated_at, h.j_tx_id
                ",
                select_cols.join(", "),
                plan.history
            ))?;
            let history_rows = stmt.query_map(params![row_id], |row| {
                let mut idx = 0;
                let row_id: String = row.get(idx)?;
                idx += 1;
                let branch_id: String = row.get(idx)?;
                idx += 1;
                let tx_id: String = row.get(idx)?;
                idx += 1;
                let op: String = row.get(idx)?;
                idx += 1;
                let conflicts_json: String = row.get(idx)?;
                idx += 1;
                let created_at: i64 = row.get(idx)?;
                idx += 1;
                let updated_at: i64 = row.get(idx)?;
                idx += 1;
                let _node_num: i64 = row.get(idx)?;
                idx += 1;
                let node_id: String = row.get(idx)?;
                idx += 1;
                let local_epoch: i64 = row.get(idx)?;
                idx += 1;
                let global_epoch: Option<i64> = row.get(idx)?;
                idx += 1;
                let kind: String = row.get(idx)?;
                idx += 1;
                let status: String = row.get(idx)?;
                idx += 1;
                let rejection_reason_json: Option<String> = row.get(idx)?;
                idx += 1;
                let tx_created_at: i64 = row.get(idx)?;
                idx += 1;
                let metadata_json: String = row.get(idx)?;
                idx += 1;

                let mut values = BTreeMap::new();
                for field in &table.fields {
                    values.insert(field.name.clone(), read_field(row, idx, field)?);
                    idx += 1;
                }

                Ok((
                    TxRecord {
                        tx_id: tx_id.clone(),
                        node_id,
                        local_epoch,
                        global_epoch,
                        kind,
                        status,
                        rejection_reason_json,
                        created_at: tx_created_at,
                        metadata_json,
                    },
                    HistoryRecord {
                        table: table.name.clone(),
                        row_id,
                        branch_id,
                        tx_id,
                        op,
                        values,
                        conflicts_json,
                        created_at,
                        updated_at,
                    },
                ))
            })?;

            for row in history_rows {
                let (tx, history) = row?;
                txs.entry(tx.tx_id.clone()).or_insert(tx);
                rows.push(history);
            }
        }

        Ok(QueryScopeBundle {
            txs: txs.into_values().collect(),
            history_rows: rows,
        })
    }

    pub fn import_query_scope(&mut self, bundle: &QueryScopeBundle) -> Result<()> {
        let sql_tx = self.conn.transaction()?;
        for tx in &bundle.txs {
            sql_tx.execute(
                "INSERT OR IGNORE INTO jazz_node (node_id) VALUES (?1)",
                params![tx.node_id],
            )?;
            let node_num: i64 = sql_tx.query_row(
                "SELECT node_num FROM jazz_node WHERE node_id = ?1",
                params![tx.node_id],
                |row| row.get(0),
            )?;
            sql_tx.execute(
                "
                INSERT INTO jazz_tx (
                  tx_id, node_num, local_epoch, global_epoch, kind, status,
                  rejection_reason_json, created_at, metadata_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(tx_id) DO UPDATE SET
                  global_epoch = excluded.global_epoch,
                  status = excluded.status,
                  rejection_reason_json = excluded.rejection_reason_json
                ",
                params![
                    tx.tx_id,
                    node_num,
                    tx.local_epoch,
                    tx.global_epoch,
                    tx.kind,
                    tx.status,
                    tx.rejection_reason_json,
                    tx.created_at,
                    tx.metadata_json
                ],
            )?;
        }

        for history in &bundle.history_rows {
            let table = self.schema.table_def(&history.table)?;
            let plan = TablePlan::new(table);
            let mut cols = vec![
                "j_row_id".to_owned(),
                "j_branch_id".to_owned(),
                "j_tx_id".to_owned(),
                "j_op".to_owned(),
            ];
            cols.extend(plan.user_columns.iter().cloned());
            cols.push("j_conflicts_json".to_owned());
            cols.push("j_created_at".to_owned());
            cols.push("j_updated_at".to_owned());

            let mut values = vec![
                SqlValue::Text(history.row_id.clone()),
                SqlValue::Text(history.branch_id.clone()),
                SqlValue::Text(history.tx_id.clone()),
                SqlValue::Text(history.op.clone()),
            ];
            for field in &table.fields {
                let value = history.values.get(&field.name).ok_or_else(|| {
                    Error::new(format!(
                        "missing bundled field {}.{}",
                        table.name, field.name
                    ))
                })?;
                values.push(json_to_sql(value, &field.kind)?);
            }
            values.push(SqlValue::Text(history.conflicts_json.clone()));
            values.push(SqlValue::Integer(history.created_at));
            values.push(SqlValue::Integer(history.updated_at));

            sql_tx.execute(
                &format!(
                    "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
                    plan.history,
                    cols.join(", "),
                    placeholders(cols.len())
                ),
                params_from_iter(values),
            )?;
        }
        sql_tx.commit()?;
        self.rebuild_current_projections()
    }

    pub fn accept_transaction(&self, tx_id: &str, global_epoch: i64) -> Result<()> {
        self.conn.execute(
            "
            UPDATE jazz_tx
            SET status = 'global_durable_accepted',
                global_epoch = ?2,
                rejection_reason_json = NULL
            WHERE tx_id = ?1
            ",
            params![tx_id, global_epoch],
        )?;
        Ok(())
    }

    pub fn accept_transaction_validating_reads(
        &mut self,
        tx_id: &str,
        global_epoch: i64,
    ) -> Result<()> {
        let metadata_json: String = self.conn.query_row(
            "SELECT metadata_json FROM jazz_tx WHERE tx_id = ?1",
            params![tx_id],
            |row| row.get(0),
        )?;
        let metadata: TxMetadata = serde_json::from_str(&metadata_json)
            .map_err(|err| Error::new(format!("parse tx metadata: {err}")))?;

        for read in metadata.read_set {
            let table = self.schema.table_def(&read.table)?;
            let plan = TablePlan::new(table);
            let latest_accepted: Option<String> = self
                .conn
                .query_row(
                    &format!(
                        "
                        SELECT h.j_tx_id
                        FROM {} h
                        JOIN jazz_tx tx ON tx.tx_id = h.j_tx_id
                        WHERE h.j_branch_id = 'main'
                          AND h.j_row_id = ?1
                          AND h.j_tx_id != ?2
                          AND tx.status = 'global_durable_accepted'
                        ORDER BY h.j_updated_at DESC, h.j_tx_id DESC
                        LIMIT 1
                        ",
                        plan.history
                    ),
                    params![read.row_id, tx_id],
                    |row| row.get(0),
                )
                .optional()?;

            if latest_accepted.as_deref() != Some(read.visible_tx_id.as_str()) {
                self.reject_transaction(
                    tx_id,
                    serde_json::json!({
                        "code": "stale_row_read",
                        "table": read.table,
                        "rowId": read.row_id,
                        "expectedTxId": read.visible_tx_id,
                        "actualTxId": latest_accepted,
                    }),
                )?;
                self.rebuild_current_projections()?;
                return Err(Error::new("stale row read"));
            }
        }

        self.accept_transaction(tx_id, global_epoch)?;
        self.rebuild_current_projections()
    }

    pub fn reject_transaction(&self, tx_id: &str, reason: JsonValue) -> Result<()> {
        self.conn.execute(
            "
            UPDATE jazz_tx
            SET status = 'rejected',
                rejection_reason_json = ?2
            WHERE tx_id = ?1
            ",
            params![tx_id, reason.to_string()],
        )?;
        Ok(())
    }

    pub fn export_transaction(&self, tx_id: &str) -> Result<QueryScopeBundle> {
        let tx = self.conn.query_row(
            "
            SELECT node.node_id, tx.local_epoch, tx.global_epoch, tx.kind, tx.status,
                   tx.rejection_reason_json, tx.created_at, tx.metadata_json
            FROM jazz_tx tx
            JOIN jazz_node node ON node.node_num = tx.node_num
            WHERE tx.tx_id = ?1
            ",
            params![tx_id],
            |row| {
                Ok(TxRecord {
                    tx_id: tx_id.to_owned(),
                    node_id: row.get(0)?,
                    local_epoch: row.get(1)?,
                    global_epoch: row.get(2)?,
                    kind: row.get(3)?,
                    status: row.get(4)?,
                    rejection_reason_json: row.get(5)?,
                    created_at: row.get(6)?,
                    metadata_json: row.get(7)?,
                })
            },
        )?;

        let mut history_rows = Vec::new();
        for table in self.schema.tables.values() {
            let plan = TablePlan::new(table);
            let mut select_cols = vec![
                "j_row_id".to_owned(),
                "j_branch_id".to_owned(),
                "j_tx_id".to_owned(),
                "j_op".to_owned(),
                "j_conflicts_json".to_owned(),
                "j_created_at".to_owned(),
                "j_updated_at".to_owned(),
            ];
            select_cols.extend(plan.user_columns.iter().cloned());
            let mut stmt = self.conn.prepare(&format!(
                "SELECT {} FROM {} WHERE j_tx_id = ? ORDER BY j_row_id",
                select_cols.join(", "),
                plan.history
            ))?;
            let rows = stmt.query_map(params![tx_id], |row| {
                let mut idx = 0;
                let row_id: String = row.get(idx)?;
                idx += 1;
                let branch_id: String = row.get(idx)?;
                idx += 1;
                let tx_id: String = row.get(idx)?;
                idx += 1;
                let op: String = row.get(idx)?;
                idx += 1;
                let conflicts_json: String = row.get(idx)?;
                idx += 1;
                let created_at: i64 = row.get(idx)?;
                idx += 1;
                let updated_at: i64 = row.get(idx)?;
                idx += 1;
                let mut values = BTreeMap::new();
                for field in &table.fields {
                    values.insert(field.name.clone(), read_field(row, idx, field)?);
                    idx += 1;
                }
                Ok(HistoryRecord {
                    table: table.name.clone(),
                    row_id,
                    branch_id,
                    tx_id,
                    op,
                    values,
                    conflicts_json,
                    created_at,
                    updated_at,
                })
            })?;
            for row in rows {
                history_rows.push(row?);
            }
        }

        Ok(QueryScopeBundle {
            txs: vec![tx],
            history_rows,
        })
    }

    pub fn transaction_status(&self, tx_id: &str) -> Result<String> {
        self.conn
            .query_row(
                "SELECT status FROM jazz_tx WHERE tx_id = ?1",
                params![tx_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn transaction_global_epoch(&self, tx_id: &str) -> Result<Option<i64>> {
        self.conn
            .query_row(
                "SELECT global_epoch FROM jazz_tx WHERE tx_id = ?1",
                params![tx_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn lower_include<'a>(
        &'a self,
        base: &'a TableDef,
        include: &Include,
    ) -> Result<LoweredInclude<'a>> {
        let fk_field = base.field(&include.fk_column)?;
        let target = match &fk_field.kind {
            FieldKind::Ref { table } => table,
            _ => {
                return Err(Error::new(format!(
                    "{}.{} is not a ref field",
                    base.name, fk_field.name
                )))
            }
        };
        Ok(LoweredInclude {
            alias: include.alias.clone(),
            required: include.required,
            fk_field,
            table: self.schema.table_def(target)?,
        })
    }

    pub fn rebuild_current_projections(&mut self) -> Result<()> {
        let sql_tx = self.conn.transaction()?;
        for table in self.schema.tables.values() {
            let plan = TablePlan::new(table);
            sql_tx.execute(&format!("DELETE FROM {}", plan.current), [])?;

            let insert_cols = std::iter::once("j_row_id".to_owned())
                .chain(std::iter::once("j_branch_id".to_owned()))
                .chain(std::iter::once("j_visible_tx_id".to_owned()))
                .chain(std::iter::once("j_is_deleted".to_owned()))
                .chain(plan.user_columns.iter().cloned())
                .chain(std::iter::once("j_conflicts_json".to_owned()))
                .chain(std::iter::once("j_created_at".to_owned()))
                .chain(std::iter::once("j_updated_at".to_owned()))
                .collect::<Vec<_>>();
            let select_user_cols = plan
                .user_columns
                .iter()
                .map(|col| format!("h.{col}"))
                .collect::<Vec<_>>();
            let select_cols = std::iter::once("h.j_row_id".to_owned())
                .chain(std::iter::once("h.j_branch_id".to_owned()))
                .chain(std::iter::once("h.j_tx_id".to_owned()))
                .chain(std::iter::once(
                    "CASE WHEN h.j_op = 'delete' THEN 1 ELSE 0 END".to_owned(),
                ))
                .chain(select_user_cols)
                .chain(std::iter::once("h.j_conflicts_json".to_owned()))
                .chain(std::iter::once("h.j_created_at".to_owned()))
                .chain(std::iter::once("h.j_updated_at".to_owned()))
                .collect::<Vec<_>>();

            sql_tx.execute(
                &format!(
                    "
                    INSERT INTO {} ({})
                    SELECT {}
                    FROM {} h
                    JOIN jazz_tx tx ON tx.tx_id = h.j_tx_id
                    WHERE tx.status != 'rejected'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM {} newer
                        JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer.j_tx_id
                        WHERE newer.j_branch_id = h.j_branch_id
                          AND newer.j_row_id = h.j_row_id
                          AND newer_tx.status != 'rejected'
                          AND (newer.j_updated_at, newer.j_tx_id) > (h.j_updated_at, h.j_tx_id)
                      )
                    ",
                    plan.current,
                    insert_cols.join(", "),
                    select_cols.join(", "),
                    plan.history,
                    plan.history
                ),
                [],
            )?;
        }
        sql_tx.commit()?;
        Ok(())
    }

    pub fn current_projection_fingerprint(&self) -> Result<Vec<String>> {
        let mut lines = Vec::new();
        for table in self.schema.tables.values() {
            let plan = TablePlan::new(table);
            let mut columns = vec![
                "j_row_id".to_owned(),
                "j_branch_id".to_owned(),
                "j_visible_tx_id".to_owned(),
                "j_is_deleted".to_owned(),
            ];
            columns.extend(plan.user_columns.iter().cloned());
            columns.push("j_conflicts_json".to_owned());
            columns.push("j_created_at".to_owned());
            columns.push("j_updated_at".to_owned());

            let mut stmt = self.conn.prepare(&format!(
                "SELECT {} FROM {} ORDER BY j_branch_id, j_row_id",
                columns.join(", "),
                plan.current
            ))?;
            let rows = stmt.query_map([], |row| {
                let mut values = Vec::new();
                for idx in 0..columns.len() {
                    let value: SqlValue = row.get(idx)?;
                    values.push(format!("{value:?}"));
                }
                Ok(format!("{}:{}", table.name, values.join("|")))
            })?;
            for row in rows {
                lines.push(row?);
            }
        }
        Ok(lines)
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
