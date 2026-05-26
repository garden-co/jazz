use crate::Result;
use rusqlite::Connection;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct SchemaDef {
    tables: BTreeMap<String, TableDef>,
}

impl SchemaDef {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn attempt3_fixture() -> Self {
        Self::new()
            .table("projects", |table| {
                table.text("title");
            })
            .table("todos", |table| {
                table.text("title");
                table.bool("done");
                table.ref_("project", "projects");
                table.index("open_created", ["done", "$createdAt"]);
            })
    }

    pub fn table(mut self, name: &str, build: impl FnOnce(&mut TableBuilder)) -> Self {
        let mut builder = TableBuilder::new(name);
        build(&mut builder);
        self.tables.insert(name.to_owned(), builder.finish());
        self
    }

    pub(crate) fn tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    pub(crate) fn table_def(&self, name: &str) -> crate::Result<&TableDef> {
        self.tables
            .get(name)
            .ok_or_else(|| crate::Error::new(format!("unknown table {name}")))
    }
}

impl Default for SchemaDef {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TableDef {
    pub(crate) name: String,
    pub(crate) fields: Vec<FieldDef>,
    pub(crate) indexes: Vec<IndexDef>,
    pub(crate) read_policy: PolicyDef,
    pub(crate) write_policy: PolicyDef,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldDef {
    pub(crate) name: String,
    pub(crate) storage_name: String,
    pub(crate) kind: FieldKind,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldKind {
    Text,
    Bool,
    Ref { table: String },
}

#[derive(Clone, Debug)]
pub(crate) struct IndexDef {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) enum PolicyDef {
    #[default]
    AllowAll,
    CreatedByPrincipal,
    RefReadable {
        field: String,
    },
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
                read_policy: PolicyDef::AllowAll,
                write_policy: PolicyDef::AllowAll,
            },
        }
    }

    pub fn text(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: name.to_owned(),
            kind: FieldKind::Text,
        });
    }

    pub fn text_lens(&mut self, name: &str, stored_as: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: stored_as.to_owned(),
            kind: FieldKind::Text,
        });
    }

    pub fn bool(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: name.to_owned(),
            kind: FieldKind::Bool,
        });
    }

    pub fn ref_(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: name.to_owned(),
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

    pub fn read_if_created_by_principal(&mut self) {
        self.table.read_policy = PolicyDef::CreatedByPrincipal;
    }

    pub fn write_if_created_by_principal(&mut self) {
        self.table.write_policy = PolicyDef::CreatedByPrincipal;
    }

    pub fn write_if_ref_readable(&mut self, field: &str) {
        self.table.write_policy = PolicyDef::RefReadable {
            field: field.to_owned(),
        };
    }

    pub fn read_if_ref_readable(&mut self, field: &str) {
        self.table.read_policy = PolicyDef::RefReadable {
            field: field.to_owned(),
        };
    }

    fn finish(self) -> TableDef {
        self.table
    }
}

pub(crate) fn install(conn: &Connection, schema: &SchemaDef) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jazz_node (
          node_num INTEGER PRIMARY KEY,
          node_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_tx (
          tx_num INTEGER PRIMARY KEY,
          tx_id TEXT NOT NULL UNIQUE,
          node_num INTEGER NOT NULL,
          local_epoch INTEGER NOT NULL,
          global_epoch INTEGER,
          kind INTEGER NOT NULL,
          conflict_mode INTEGER NOT NULL,
          outcome INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          metadata_json TEXT NOT NULL,
          UNIQUE (node_num, local_epoch),
          UNIQUE (global_epoch)
        );

        CREATE TABLE IF NOT EXISTS jazz_tx_receipt (
          tx_num INTEGER NOT NULL,
          tier INTEGER NOT NULL,
          observed_at INTEGER NOT NULL,
          authority_node_num INTEGER,
          receipt_json TEXT,
          PRIMARY KEY (tx_num, tier)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_tx_rejection (
          tx_num INTEGER PRIMARY KEY,
          code TEXT NOT NULL,
          detail_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_row_id (
          row_num INTEGER PRIMARY KEY,
          table_name TEXT NOT NULL,
          row_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_branch (
          branch_num INTEGER PRIMARY KEY,
          branch_id TEXT NOT NULL UNIQUE,
          base_global_epoch INTEGER,
          created_at INTEGER NOT NULL
        );

        INSERT OR IGNORE INTO jazz_branch
          (branch_num, branch_id, base_global_epoch, created_at)
          VALUES (1, 'main', NULL, 0);
        "#,
    )?;

    for table in schema.tables() {
        install_table(conn, table)?;
    }
    Ok(())
}

fn install_table(conn: &Connection, table: &TableDef) -> Result<()> {
    let user_columns = table
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} {}",
                quote_ident(&storage_column(field)),
                sql_type(&field.kind)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n          ");
    conn.execute_batch(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {history} (
          row_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          j_branch_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          {user_columns},
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL,
          PRIMARY KEY (row_num, tx_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS {current} (
          row_num INTEGER NOT NULL,
          j_branch_num INTEGER NOT NULL,
          visible_tx_num INTEGER NOT NULL,
          is_deleted INTEGER NOT NULL,
          {user_columns},
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL,
          PRIMARY KEY (row_num, j_branch_num)
        );
        "#,
        history = history_table(&table.name),
        current = current_table(&table.name),
    ))?;

    for index in &table.indexes {
        let columns = index
            .columns
            .iter()
            .map(|column| storage_column_name(column))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(is_deleted, {});",
            quote_ident(&format!("{}_current_{}", table.name, index.name)),
            current_table(&table.name),
            columns
        ))?;
    }
    Ok(())
}

pub(crate) fn history_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_history"))
}

pub(crate) fn current_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_current"))
}

pub(crate) fn storage_column(field: &FieldDef) -> String {
    match field.kind {
        FieldKind::Ref { .. } => format!("{}_row_num", field.storage_name),
        _ => field.storage_name.clone(),
    }
}

pub(crate) fn field_sql_value(
    field: &FieldDef,
    value: &serde_json::Value,
    resolve_ref: impl FnOnce(&str, &str) -> crate::Result<i64>,
) -> crate::Result<rusqlite::types::Value> {
    Ok(match &field.kind {
        FieldKind::Text => rusqlite::types::Value::Text(
            value
                .as_str()
                .ok_or_else(|| crate::Error::new(format!("expected text for {}", field.name)))?
                .to_owned(),
        ),
        FieldKind::Bool => rusqlite::types::Value::Integer(i64::from(
            value
                .as_bool()
                .ok_or_else(|| crate::Error::new(format!("expected bool for {}", field.name)))?,
        )),
        FieldKind::Ref { table } => rusqlite::types::Value::Integer(resolve_ref(
            table,
            value
                .as_str()
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {}", field.name)))?,
        )?),
    })
}

pub(crate) fn storage_column_name(column: &str) -> String {
    quote_ident(match column {
        "$createdAt" => "j_created_at",
        "$updatedAt" => "j_updated_at",
        other => other,
    })
}

fn sql_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text => "TEXT",
        FieldKind::Ref { table } => {
            let _ = table;
            "INTEGER"
        }
        FieldKind::Bool => "INTEGER",
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
