use crate::value::Value as JsonValue;
use crate::Result;
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};

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

    pub(crate) fn compatibility_fingerprint(&self) -> String {
        let mut parts = Vec::new();
        for table in self.tables.values() {
            parts.push(format!("table:{}", table.name));
            for field in &table.fields {
                parts.push(format!(
                    "field:{}:{}",
                    field.storage_name,
                    field.kind.fingerprint()
                ));
            }
        }
        parts.join("|")
    }

    pub(crate) fn policy_fingerprint(&self) -> String {
        self.policy_fingerprint_for_tables(self.tables.keys())
    }

    pub(crate) fn policy_fingerprint_for_tables<'a>(
        &self,
        table_names: impl IntoIterator<Item = &'a String>,
    ) -> String {
        let mut parts = Vec::new();
        for table_name in table_names {
            let Some(table) = self.tables.get(table_name) else {
                continue;
            };
            parts.push(format!(
                "{}:write:{}",
                table.name,
                table.write_policy.fingerprint_for_table(table)
            ));
        }
        parts.join("|")
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
    pub(crate) nullable: bool,
    pub(crate) default_value: Option<JsonValue>,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldKind {
    Text,
    Bytes,
    Bool,
    Ref { table: String },
}

impl FieldKind {
    fn fingerprint(&self) -> String {
        match self {
            FieldKind::Text => "text".to_owned(),
            FieldKind::Bytes => "bytes".to_owned(),
            FieldKind::Bool => "bool".to_owned(),
            FieldKind::Ref { table } => format!("ref:{table}"),
        }
    }
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
    CreatedByUser,
    RefReadable {
        field: String,
    },
}

impl PolicyDef {
    fn fingerprint_for_table(&self, table: &TableDef) -> String {
        match self {
            PolicyDef::AllowAll => "allow_all".to_owned(),
            PolicyDef::CreatedByUser => "created_by_user".to_owned(),
            PolicyDef::RefReadable { field } => {
                let storage_field = table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .map(|field| field.storage_name.as_str())
                    .unwrap_or(field);
                format!("ref_readable:{storage_field}")
            }
        }
    }
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
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: false,
            default_value: None,
        });
    }

    pub fn bytes(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Bytes,
            nullable: false,
            default_value: None,
        });
    }

    pub fn text_default(&mut self, name: &str, value: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: false,
            default_value: Some(JsonValue::String(value.to_owned())),
        });
    }

    pub fn optional_text(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: true,
            default_value: None,
        });
    }

    pub fn text_lens(&mut self, name: &str, stored_as: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(stored_as),
            kind: FieldKind::Text,
            nullable: false,
            default_value: None,
        });
    }

    pub fn bool(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Bool,
            nullable: false,
            default_value: None,
        });
    }

    pub fn bool_default(&mut self, name: &str, value: bool) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Bool,
            nullable: false,
            default_value: Some(JsonValue::Bool(value)),
        });
    }

    pub fn ref_(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: false,
            default_value: None,
        });
    }

    pub fn optional_ref(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: true,
            default_value: None,
        });
    }

    pub fn ref_lens(&mut self, name: &str, stored_as: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(stored_as),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: false,
            default_value: None,
        });
    }

    pub fn index<const N: usize>(&mut self, name: &str, columns: [&str; N]) {
        self.table.indexes.push(IndexDef {
            name: name.to_owned(),
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        });
    }

    pub fn read_if_created_by_user(&mut self) {
        self.table.read_policy = PolicyDef::CreatedByUser;
    }

    pub fn write_if_created_by_user(&mut self) {
        self.table.write_policy = PolicyDef::CreatedByUser;
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
    validate_schema_shape(schema)?;
    validate_policy_cycles(schema)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jazz_node (
          node_num INTEGER PRIMARY KEY,
          node_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_user (
          user_num INTEGER PRIMARY KEY,
          user_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_tx (
          tx_num INTEGER PRIMARY KEY,
          node_num INTEGER NOT NULL,
          local_epoch INTEGER NOT NULL,
          global_epoch INTEGER,
          kind INTEGER NOT NULL,
          conflict_mode INTEGER NOT NULL,
          outcome INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          metadata TEXT,
          writes_json BLOB NOT NULL DEFAULT X'0B',
          reads_json BLOB,
          UNIQUE (node_num, local_epoch)
        );

        CREATE VIEW IF NOT EXISTS jazz_tx_public AS
        SELECT tx.tx_num,
               'tx-' || node.node_id || '-' || tx.local_epoch AS tx_id,
               node.node_id,
               tx.node_num,
               tx.local_epoch,
               tx.global_epoch,
               tx.kind,
               tx.conflict_mode,
               tx.outcome,
               tx.created_at,
               tx.metadata
          FROM jazz_tx tx
          JOIN jazz_node node ON node.node_num = tx.node_num;

        CREATE TABLE IF NOT EXISTS jazz_tx_receipt (
          tx_num INTEGER NOT NULL,
          tier INTEGER NOT NULL,
          observed_at INTEGER NOT NULL,
          authority_node_num INTEGER,
          receipt TEXT,
          PRIMARY KEY (tx_num, tier)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_tx_rejection (
          tx_num INTEGER PRIMARY KEY,
          code TEXT NOT NULL,
          detail TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_tx_awaiting_dependency (
          tx_num INTEGER PRIMARY KEY,
          auth_user TEXT NOT NULL,
          detail TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_table (
          table_num INTEGER PRIMARY KEY,
          table_name TEXT NOT NULL UNIQUE
        );

        CREATE VIEW IF NOT EXISTS jazz_tx_write AS
        SELECT tx.tx_num,
               CAST(json_extract(write.value, '$[0]') AS INTEGER) AS table_num,
               CAST(json_extract(write.value, '$[1]') AS INTEGER) AS row_num,
               CAST(json_extract(write.value, '$[2]') AS INTEGER) AS op
          FROM jazz_tx tx, json_each(tx.writes_json) write;

        CREATE VIEW IF NOT EXISTS jazz_tx_read AS
        SELECT tx.tx_num,
               CAST(json_extract(read.value, '$[0]') AS INTEGER) AS table_num,
               CAST(json_extract(read.value, '$[1]') AS INTEGER) AS row_num,
               CAST(json_extract(read.value, '$[2]') AS INTEGER) AS reason,
               CAST(json_extract(read.value, '$[3]') AS INTEGER) AS observed_tx_num
          FROM jazz_tx tx, json_each(tx.reads_json) read
         WHERE tx.reads_json IS NOT NULL
        UNION ALL
        SELECT tx.tx_num,
               CAST(json_extract(write.value, '$[0]') AS INTEGER) AS table_num,
               CAST(json_extract(write.value, '$[1]') AS INTEGER) AS row_num,
               2 AS reason,
               previous.tx_num AS observed_tx_num
          FROM jazz_tx tx
          JOIN jazz_tx previous
            ON previous.node_num = tx.node_num
           AND previous.local_epoch = tx.local_epoch - 1,
               json_each(tx.writes_json) write
         WHERE tx.reads_json IS NULL;

        CREATE TABLE IF NOT EXISTS jazz_query_read (
          branch_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          field_name TEXT NOT NULL,
          op TEXT NOT NULL,
          value TEXT NOT NULL,
          observed_at INTEGER NOT NULL,
          PRIMARY KEY (branch_id, table_name, field_name, op, value)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS history_blocks (
          block_id INTEGER PRIMARY KEY,
          block_kind INTEGER NOT NULL,
          table_num INTEGER NOT NULL,
          row_num INTEGER NOT NULL,
          min_global_epoch INTEGER NOT NULL,
          max_global_epoch INTEGER NOT NULL,
          row_count INTEGER NOT NULL,
          tx_count INTEGER NOT NULL,
          codec TEXT NOT NULL,
          format_version INTEGER NOT NULL,
          uncompressed_bytes INTEGER NOT NULL,
          compressed_bytes INTEGER NOT NULL,
          payload_sha256 TEXT NOT NULL,
          payload BLOB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS history_blocks_row_epoch
          ON history_blocks(block_kind, table_num, row_num, max_global_epoch DESC, min_global_epoch);

        CREATE INDEX IF NOT EXISTS history_blocks_inventory
          ON history_blocks(block_kind, table_num, row_num, min_global_epoch, max_global_epoch, payload_sha256);

        CREATE TABLE IF NOT EXISTS history_block_tx_index (
          node_num INTEGER NOT NULL,
          min_local_epoch INTEGER NOT NULL,
          max_local_epoch INTEGER NOT NULL,
          block_id INTEGER NOT NULL,
          PRIMARY KEY (node_num, max_local_epoch, min_local_epoch, block_id)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_row_id (
          row_num INTEGER PRIMARY KEY,
          row_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_branch (
          branch_num INTEGER PRIMARY KEY,
          branch_id TEXT NOT NULL UNIQUE,
          base_global_epoch INTEGER,
          created_at INTEGER NOT NULL,
          source_version INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS jazz_branch_source (
          branch_num INTEGER NOT NULL,
          source_branch_num INTEGER NOT NULL,
          PRIMARY KEY (branch_num, source_branch_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_branch_backing (
          branch_id TEXT PRIMARY KEY,
          base_global_epoch INTEGER,
          source_branch_ids BLOB NOT NULL,
          created_at INTEGER NOT NULL
        ) WITHOUT ROWID;

        INSERT OR IGNORE INTO jazz_branch
          (branch_num, branch_id, base_global_epoch, created_at, source_version)
          VALUES (1, 'main', NULL, 0, 0);

        INSERT OR IGNORE INTO jazz_branch_backing
          (branch_id, base_global_epoch, source_branch_ids, created_at)
          VALUES ('main', NULL, X'0000000000000000', 0);
        "#,
    )?;

    for (idx, table) in schema.tables().enumerate() {
        conn.execute(
            "INSERT OR IGNORE INTO jazz_table (table_num, table_name) VALUES (?, ?)",
            rusqlite::params![idx as i64 + 1, table.name],
        )?;
        install_table(conn, table)?;
    }
    Ok(())
}

fn validate_schema_shape(schema: &SchemaDef) -> Result<()> {
    for table in schema.tables() {
        let mut fields = BTreeSet::new();
        let mut storage_fields = BTreeSet::new();
        for field in &table.fields {
            if !fields.insert(field.name.clone()) {
                return Err(crate::Error::new(format!(
                    "duplicate field {}.{}",
                    table.name, field.name
                )));
            }
            if !storage_fields.insert(storage_column(field)) {
                return Err(crate::Error::new(format!(
                    "duplicate storage field {}.{}",
                    table.name, field.storage_name
                )));
            }
            if let FieldKind::Ref { table: ref_table } = &field.kind {
                schema.table_def(ref_table)?;
            }
        }
        for index in &table.indexes {
            for column in &index.columns {
                if column == "$createdAt" || column == "$updatedAt" {
                    continue;
                }
                if !table.fields.iter().any(|field| field.name == *column) {
                    return Err(crate::Error::new(format!(
                        "index {}.{} references unknown field {}",
                        table.name, index.name, column
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_policy_cycles(schema: &SchemaDef) -> Result<()> {
    for table in schema.tables() {
        validate_policy_cycle(schema, table, &table.read_policy, &mut BTreeSet::new())?;
        validate_policy_cycle(schema, table, &table.write_policy, &mut BTreeSet::new())?;
    }
    Ok(())
}

fn validate_policy_cycle(
    schema: &SchemaDef,
    table: &TableDef,
    policy: &PolicyDef,
    seen: &mut BTreeSet<String>,
) -> Result<()> {
    let PolicyDef::RefReadable { field } = policy else {
        return Ok(());
    };
    if !seen.insert(table.name.clone()) {
        return Err(crate::Error::new(format!(
            "policy cycle detected at {}",
            table.name
        )));
    }
    let Some(field) = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
    else {
        seen.remove(&table.name);
        return Err(crate::Error::new(format!(
            "policy on {} references unknown field {}",
            table.name, field
        )));
    };
    let FieldKind::Ref { table: parent } = &field.kind else {
        seen.remove(&table.name);
        return Err(crate::Error::new(format!(
            "policy on {} references non-ref field {}",
            table.name, field.name
        )));
    };
    let parent_table = schema.table_def(parent)?;
    let result = validate_policy_cycle(schema, parent_table, &parent_table.read_policy, seen);
    seen.remove(&table.name);
    result
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
          j_created_by INTEGER NOT NULL,
          j_updated_by INTEGER NOT NULL,
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
          j_created_by INTEGER NOT NULL,
          j_updated_by INTEGER NOT NULL,
          PRIMARY KEY (row_num, j_branch_num)
        ) WITHOUT ROWID;
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

pub(crate) fn table_num(conn: &Connection, table_name: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT table_num FROM jazz_table WHERE table_name = ?",
        rusqlite::params![table_name],
        |row| row.get(0),
    )?)
}

pub(crate) fn table_nums(conn: &Connection) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare("SELECT table_name, table_num FROM jazz_table")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

pub(crate) fn storage_column(field: &FieldDef) -> String {
    match field.kind {
        FieldKind::Ref { .. } => format!("{}_row_num", field.storage_name),
        _ => field.storage_name.clone(),
    }
}

pub(crate) fn field_sql_value(
    field: &FieldDef,
    value: &JsonValue,
    resolve_ref: impl FnOnce(&str, &str) -> crate::Result<i64>,
) -> crate::Result<rusqlite::types::Value> {
    if value.is_null() {
        if field.nullable {
            return Ok(rusqlite::types::Value::Null);
        }
        return Err(crate::Error::new(format!(
            "expected non-null for {}",
            field.name
        )));
    }
    Ok(match &field.kind {
        FieldKind::Text => rusqlite::types::Value::Text(
            value
                .as_str()
                .ok_or_else(|| crate::Error::new(format!("expected text for {}", field.name)))?
                .to_owned(),
        ),
        FieldKind::Bytes => {
            rusqlite::types::Value::Blob(if let Some(bytes) = value.as_bytes() {
                bytes.to_vec()
            } else {
                hex_to_bytes(value.as_str().ok_or_else(|| {
                    crate::Error::new(format!("expected bytes for {}", field.name))
                })?)?
            })
        }
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
    let storage = match column {
        "$createdAt" => "j_created_at".to_owned(),
        "$updatedAt" => "j_updated_at".to_owned(),
        other => user_storage_name(other),
    };
    quote_ident(&storage)
}

fn user_storage_name(name: &str) -> String {
    if name.starts_with("j_") {
        format!("u_{name}")
    } else {
        name.to_owned()
    }
}

fn sql_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text => "TEXT",
        FieldKind::Bytes => "BLOB",
        FieldKind::Ref { table } => {
            let _ = table;
            "INTEGER"
        }
        FieldKind::Bool => "INTEGER",
    }
}

fn hex_to_bytes(value: &str) -> crate::Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(crate::Error::new("hex bytes value has odd length"));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let raw = value.as_bytes();
    for idx in (0..raw.len()).step_by(2) {
        let high = hex_nibble(raw[idx])?;
        let low = hex_nibble(raw[idx + 1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> crate::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crate::Error::new("invalid hex bytes value")),
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
