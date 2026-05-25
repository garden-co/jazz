//! Attempt2 prototype for a SQLite-backed Jazz core.

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct Schema {
    tables: BTreeMap<String, TableDef>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn table(mut self, name: &str, build: impl FnOnce(&mut TableBuilder)) -> Self {
        let mut builder = TableBuilder::new(name);
        build(&mut builder);
        self.tables.insert(name.to_owned(), builder.finish());
        self
    }

    fn table_def(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .get(name)
            .ok_or_else(|| Error::new(format!("unknown table {name}")))
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct TableDef {
    name: String,
    fields: Vec<FieldDef>,
    indexes: Vec<IndexDef>,
}

impl TableDef {
    fn field(&self, name: &str) -> Result<&FieldDef> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .ok_or_else(|| Error::new(format!("unknown field {}.{name}", self.name)))
    }
}

#[derive(Clone, Debug)]
struct FieldDef {
    name: String,
    kind: FieldKind,
}

#[derive(Clone, Debug)]
enum FieldKind {
    Text,
    Bool,
    Ref { table: String },
}

#[derive(Clone, Debug)]
struct IndexDef {
    name: String,
    columns: Vec<String>,
}

pub struct TableBuilder {
    table: TableDef,
}

impl TableBuilder {
    fn new(name: &str) -> Self {
        Self {
            table: TableDef {
                name: name.to_owned(),
                fields: Vec::new(),
                indexes: Vec::new(),
            },
        }
    }

    pub fn text(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            kind: FieldKind::Text,
        });
    }

    pub fn bool(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            kind: FieldKind::Bool,
        });
    }

    pub fn ref_(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
        });
    }

    pub fn index<const N: usize>(&mut self, name: &str, columns: [&str; N]) {
        self.table.indexes.push(IndexDef {
            name: name.to_owned(),
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        });
    }

    fn finish(self) -> TableDef {
        self.table
    }
}

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
        let history = history_table(&table.name);
        let current = current_table(&table.name);
        let user_columns = table
            .fields
            .iter()
            .map(|field| format!("{} {}", quote_ident(&field.name), sql_type(&field.kind)))
            .collect::<Vec<_>>()
            .join(",\n  ");

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
            quote_ident(&format!(
                "{}__schema_v1_history_branch_row_updated",
                table.name
            )),
            quote_ident(&format!("{}__schema_v1_history_branch_tx", table.name))
        ))?;

        for index in &table.indexes {
            let cols = index
                .columns
                .iter()
                .map(|column| {
                    if column == "$createdAt" {
                        "j_created_at".to_owned()
                    } else if column == "$updatedAt" {
                        "j_updated_at".to_owned()
                    } else {
                        quote_ident(column)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            self.conn.execute_batch(&format!(
                "CREATE INDEX IF NOT EXISTS {} ON {current}(j_branch_id, {cols});",
                quote_ident(&format!("{}_{}", current, index.name))
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
            tx_id,
            local_epoch,
            now,
        };
        write(&mut write_tx)?;
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
        let current = current_table(&table.name);

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

        let mut sql = format!("SELECT {} FROM {current} base", select_cols.join(", "));

        if let Some(include) = &include {
            sql.push_str(&format!(
                " INNER JOIN {} dep ON dep.j_branch_id = base.j_branch_id \
                 AND dep.j_row_id = base.{} AND dep.j_is_deleted = 0",
                current_table(&include.table.name),
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
            sql.push_str(&column_sql("base", &order.column));
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
                let dep_row_id: String = row.get(idx)?;
                idx += 1;
                let dep_tx_id: String = row.get(idx)?;
                idx += 1;
                let dep_created_at: i64 = row.get(idx)?;
                idx += 1;
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
            fk_field,
            table: self.schema.table_def(target)?,
        })
    }

    pub fn rebuild_current_projections(&mut self) -> Result<()> {
        let sql_tx = self.conn.transaction()?;
        for table in self.schema.tables.values() {
            let current = current_table(&table.name);
            let history = history_table(&table.name);
            sql_tx.execute(&format!("DELETE FROM {current}"), [])?;

            let user_cols = table
                .fields
                .iter()
                .map(|field| quote_ident(&field.name))
                .collect::<Vec<_>>();
            let insert_cols = std::iter::once("j_row_id".to_owned())
                .chain(std::iter::once("j_branch_id".to_owned()))
                .chain(std::iter::once("j_visible_tx_id".to_owned()))
                .chain(std::iter::once("j_is_deleted".to_owned()))
                .chain(user_cols.iter().cloned())
                .chain(std::iter::once("j_conflicts_json".to_owned()))
                .chain(std::iter::once("j_created_at".to_owned()))
                .chain(std::iter::once("j_updated_at".to_owned()))
                .collect::<Vec<_>>();
            let select_user_cols = user_cols
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
                    INSERT INTO {current} ({})
                    SELECT {}
                    FROM {history} h
                    JOIN jazz_tx tx ON tx.tx_id = h.j_tx_id
                    WHERE tx.status != 'rejected'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM {history} newer
                        JOIN jazz_tx newer_tx ON newer_tx.tx_id = newer.j_tx_id
                        WHERE newer.j_branch_id = h.j_branch_id
                          AND newer.j_row_id = h.j_row_id
                          AND newer_tx.status != 'rejected'
                          AND (newer.j_updated_at, newer.j_tx_id) > (h.j_updated_at, h.j_tx_id)
                      )
                    ",
                    insert_cols.join(", "),
                    select_cols.join(", ")
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
            let current = current_table(&table.name);
            let mut columns = vec![
                "j_row_id".to_owned(),
                "j_branch_id".to_owned(),
                "j_visible_tx_id".to_owned(),
                "j_is_deleted".to_owned(),
            ];
            columns.extend(table.fields.iter().map(|field| quote_ident(&field.name)));
            columns.push("j_conflicts_json".to_owned());
            columns.push("j_created_at".to_owned());
            columns.push("j_updated_at".to_owned());

            let mut stmt = self.conn.prepare(&format!(
                "SELECT {} FROM {current} ORDER BY j_branch_id, j_row_id",
                columns.join(", ")
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

pub struct WriteTx<'a> {
    schema: &'a Schema,
    conn: &'a rusqlite::Transaction<'a>,
    tx_id: String,
    local_epoch: i64,
    now: i64,
}

impl WriteTx<'_> {
    pub fn insert(&mut self, table_name: &str, value: JsonValue) -> Result<RowRef> {
        let table = self.schema.table_def(table_name)?;
        let row_id = format!(
            "{table_name}:{}:{}",
            self.local_epoch,
            self.row_count(table_name)? + 1
        );
        self.insert_with_row_id(table, &row_id, value)?;
        Ok(RowRef { id: row_id })
    }

    pub fn update(&mut self, table_name: &str, row_id: &str, patch: JsonValue) -> Result<()> {
        let table = self.schema.table_def(table_name)?;
        let object = patch
            .as_object()
            .ok_or_else(|| Error::new("update patch must be an object"))?;
        let current = current_table(table_name);

        let mut select_cols = vec!["j_created_at".to_owned()];
        select_cols.extend(table.fields.iter().map(|field| quote_ident(&field.name)));
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {} FROM {current} WHERE j_branch_id = 'main' AND j_row_id = ?",
            select_cols.join(", ")
        ))?;
        let existing = stmt
            .query_row(params![row_id], |row| {
                let created_at: i64 = row.get(0)?;
                let mut values = Vec::new();
                for (idx, field) in table.fields.iter().enumerate() {
                    let sql_value = match field.kind {
                        FieldKind::Text | FieldKind::Ref { .. } => {
                            SqlValue::Text(row.get(idx + 1)?)
                        }
                        FieldKind::Bool => SqlValue::Integer(row.get(idx + 1)?),
                    };
                    values.push(sql_value);
                }
                Ok((created_at, values))
            })
            .optional()?
            .ok_or_else(|| Error::new(format!("missing row {table_name}:{row_id}")))?;

        let (created_at, mut values) = existing;
        for (idx, field) in table.fields.iter().enumerate() {
            if let Some(value) = object.get(&field.name) {
                values[idx] = json_to_sql(value, &field.kind)?;
            }
        }

        self.write_version(table, row_id, "update", values, created_at, self.now)
    }

    fn row_count(&self, table_name: &str) -> Result<i64> {
        self.conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", history_table(table_name)),
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn insert_with_row_id(
        &mut self,
        table: &TableDef,
        row_id: &str,
        value: JsonValue,
    ) -> Result<()> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("insert value must be an object"))?;
        let mut values = Vec::new();
        for field in &table.fields {
            let json = object.get(&field.name).ok_or_else(|| {
                Error::new(format!("missing field {}.{}", table.name, field.name))
            })?;
            values.push(json_to_sql(json, &field.kind)?);
        }

        self.write_version(table, row_id, "insert", values, self.now, self.now)
    }

    fn write_version(
        &mut self,
        table: &TableDef,
        row_id: &str,
        op: &str,
        values: Vec<SqlValue>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<()> {
        let history = history_table(&table.name);
        let current = current_table(&table.name);
        let user_cols = table
            .fields
            .iter()
            .map(|field| quote_ident(&field.name))
            .collect::<Vec<_>>();

        let history_cols = std::iter::once("j_row_id".to_owned())
            .chain(std::iter::once("j_branch_id".to_owned()))
            .chain(std::iter::once("j_tx_id".to_owned()))
            .chain(std::iter::once("j_op".to_owned()))
            .chain(user_cols.iter().cloned())
            .chain(std::iter::once("j_conflicts_json".to_owned()))
            .chain(std::iter::once("j_created_at".to_owned()))
            .chain(std::iter::once("j_updated_at".to_owned()))
            .collect::<Vec<_>>();
        let current_cols = std::iter::once("j_row_id".to_owned())
            .chain(std::iter::once("j_branch_id".to_owned()))
            .chain(std::iter::once("j_visible_tx_id".to_owned()))
            .chain(std::iter::once("j_is_deleted".to_owned()))
            .chain(user_cols)
            .chain(std::iter::once("j_conflicts_json".to_owned()))
            .chain(std::iter::once("j_created_at".to_owned()))
            .chain(std::iter::once("j_updated_at".to_owned()))
            .collect::<Vec<_>>();

        let mut history_values = vec![
            SqlValue::Text(row_id.to_owned()),
            SqlValue::Text("main".to_owned()),
            SqlValue::Text(self.tx_id.clone()),
            SqlValue::Text(op.to_owned()),
        ];
        history_values.extend(values.clone());
        history_values.push(SqlValue::Text("{}".to_owned()));
        history_values.push(SqlValue::Integer(created_at));
        history_values.push(SqlValue::Integer(updated_at));

        self.conn.execute(
            &format!(
                "INSERT INTO {history} ({}) VALUES ({})",
                history_cols.join(", "),
                placeholders(history_cols.len())
            ),
            params_from_iter(history_values),
        )?;

        let mut current_values = vec![
            SqlValue::Text(row_id.to_owned()),
            SqlValue::Text("main".to_owned()),
            SqlValue::Text(self.tx_id.clone()),
            SqlValue::Integer(0),
        ];
        current_values.extend(values);
        current_values.push(SqlValue::Text("{}".to_owned()));
        current_values.push(SqlValue::Integer(created_at));
        current_values.push(SqlValue::Integer(updated_at));

        self.conn.execute(
            &format!(
                "INSERT OR REPLACE INTO {current} ({}) VALUES ({})",
                current_cols.join(", "),
                placeholders(current_cols.len())
            ),
            params_from_iter(current_values),
        )?;

        Ok(())
    }
}

pub struct RowRef {
    id: String,
}

impl RowRef {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    table: String,
    filters: Vec<Filter>,
    include: Option<Include>,
    order: Option<Order>,
    limit: Option<usize>,
}

pub fn query(table: &str) -> Query {
    Query {
        table: table.to_owned(),
        filters: Vec::new(),
        include: None,
        order: None,
        limit: None,
    }
}

impl Query {
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn include_required(mut self, alias: &str, fk_column: &str) -> Self {
        self.include = Some(Include {
            alias: alias.to_owned(),
            fk_column: fk_column.to_owned(),
        });
        self
    }

    pub fn order_by(mut self, column: &str, direction: SortDirection) -> Self {
        self.order = Some(Order {
            column: column.to_owned(),
            direction,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Clone, Debug)]
struct Include {
    alias: String,
    fk_column: String,
}

struct LoweredInclude<'a> {
    alias: String,
    fk_field: &'a FieldDef,
    table: &'a TableDef,
}

#[derive(Clone, Debug)]
pub struct Filter {
    column: String,
    op: FilterOp,
    value: FilterValue,
}

pub fn eq(column: &str, value: impl Into<FilterValue>) -> Filter {
    Filter {
        column: column.to_owned(),
        op: FilterOp::Eq,
        value: value.into(),
    }
}

pub fn gt(column: &str, value: impl Into<FilterValue>) -> Filter {
    Filter {
        column: column.to_owned(),
        op: FilterOp::Gt,
        value: value.into(),
    }
}

#[derive(Clone, Debug)]
enum FilterOp {
    Eq,
    Gt,
}

#[derive(Clone, Debug)]
pub enum FilterValue {
    Bool(bool),
    Int(i64),
    Text(String),
}

impl FilterValue {
    fn to_sql_value(&self) -> SqlValue {
        match self {
            Self::Bool(value) => SqlValue::Integer(i64::from(*value)),
            Self::Int(value) => SqlValue::Integer(*value),
            Self::Text(value) => SqlValue::Text(value.clone()),
        }
    }
}

impl From<bool> for FilterValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for FilterValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for FilterValue {
    fn from(value: i32) -> Self {
        Self::Int(value.into())
    }
}

impl From<&str> for FilterValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

#[derive(Clone, Debug)]
struct Order {
    column: String,
    direction: SortDirection,
}

#[derive(Clone, Copy, Debug)]
pub enum SortDirection {
    Asc,
    Desc,
}

pub use SortDirection::Desc;

pub struct QueryResult {
    pub rows: Vec<RowView>,
    pub scope: QueryScope,
}

pub struct SubscriptionDiff {
    pub added: Vec<RowView>,
    pub updated: Vec<RowView>,
    pub removed: Vec<RowView>,
}

pub struct QueryScope {
    pub result_rows: Vec<ScopeRow>,
    pub dependency_rows: Vec<ScopeRow>,
}

pub struct ScopeRow {
    pub table: String,
    pub row_id: String,
    pub tx_id: String,
    pub reason: ScopeReason,
}

pub enum ScopeReason {
    Result,
    Dependency,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowView {
    values: BTreeMap<String, JsonValue>,
    includes: BTreeMap<String, RowView>,
}

impl RowView {
    pub fn get(&self, column: &str) -> Option<&str> {
        self.values.get(column)?.as_str()
    }

    pub fn include(&self, alias: &str) -> Option<&RowView> {
        self.includes.get(alias)
    }
}

fn diff_rows(previous: &[RowView], next: &[RowView]) -> SubscriptionDiff {
    let previous_by_id = previous
        .iter()
        .map(|row| (row.row_id().to_owned(), row))
        .collect::<BTreeMap<_, _>>();
    let next_by_id = next
        .iter()
        .map(|row| (row.row_id().to_owned(), row))
        .collect::<BTreeMap<_, _>>();

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();

    for (row_id, next_row) in &next_by_id {
        match previous_by_id.get(row_id) {
            Some(previous_row) if *previous_row != *next_row => updated.push((*next_row).clone()),
            Some(_) => {}
            None => added.push((*next_row).clone()),
        }
    }

    for (row_id, previous_row) in &previous_by_id {
        if !next_by_id.contains_key(row_id) {
            removed.push((*previous_row).clone());
        }
    }

    SubscriptionDiff {
        added,
        updated,
        removed,
    }
}

impl RowView {
    fn row_id(&self) -> &str {
        self.get("$rowId").unwrap_or("")
    }
}

fn filter_sql(alias: &str, filter: &Filter) -> String {
    let op = match filter.op {
        FilterOp::Eq => "=",
        FilterOp::Gt => ">",
    };
    format!("{} {op} ?", column_sql(alias, &filter.column))
}

fn column_sql(alias: &str, column: &str) -> String {
    let col = match column {
        "$createdAt" => "j_created_at".to_owned(),
        "$updatedAt" => "j_updated_at".to_owned(),
        "$rowId" => "j_row_id".to_owned(),
        "$txId" => "j_visible_tx_id".to_owned(),
        other => quote_ident(other),
    };
    format!("{alias}.{col}")
}

fn read_field(
    row: &rusqlite::Row<'_>,
    idx: usize,
    field: &FieldDef,
) -> rusqlite::Result<JsonValue> {
    match field.kind {
        FieldKind::Text | FieldKind::Ref { .. } => {
            let value: String = row.get(idx)?;
            Ok(JsonValue::String(value))
        }
        FieldKind::Bool => {
            let value: i64 = row.get(idx)?;
            Ok(JsonValue::Bool(value != 0))
        }
    }
}

fn json_to_sql(value: &JsonValue, kind: &FieldKind) -> Result<SqlValue> {
    match kind {
        FieldKind::Text | FieldKind::Ref { .. } => value
            .as_str()
            .map(|value| SqlValue::Text(value.to_owned()))
            .ok_or_else(|| Error::new("expected string value")),
        FieldKind::Bool => value
            .as_bool()
            .map(|value| SqlValue::Integer(i64::from(value)))
            .ok_or_else(|| Error::new("expected bool value")),
    }
}

fn sql_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text | FieldKind::Ref { .. } => "TEXT",
        FieldKind::Bool => "INTEGER",
    }
}

fn history_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_history"))
}

fn current_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_current"))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
