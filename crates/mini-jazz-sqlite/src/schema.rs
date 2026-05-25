use crate::Result;
use rusqlite::Connection;

pub(crate) fn install(conn: &Connection) -> Result<()> {
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

        CREATE TABLE IF NOT EXISTS projects__schema_v1_history (
          row_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          title TEXT,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL,
          PRIMARY KEY (row_num, tx_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS projects__schema_v1_current (
          row_num INTEGER PRIMARY KEY,
          visible_tx_num INTEGER NOT NULL,
          is_deleted INTEGER NOT NULL,
          title TEXT,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS todos__schema_v1_history (
          row_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          title TEXT,
          done INTEGER,
          project_row_num INTEGER,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL,
          PRIMARY KEY (row_num, tx_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS todos__schema_v1_current (
          row_num INTEGER PRIMARY KEY,
          visible_tx_num INTEGER NOT NULL,
          is_deleted INTEGER NOT NULL,
          title TEXT,
          done INTEGER,
          project_row_num INTEGER,
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by TEXT NOT NULL,
          j_updated_by TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS todos_current_open_created
          ON todos__schema_v1_current(is_deleted, done, j_created_at DESC, row_num);
        "#,
    )?;
    Ok(())
}
