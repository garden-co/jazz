use crate::auth::RuntimeAuth;
use crate::schema::SchemaDef;
use crate::{tx, Result};
use rusqlite::{params_from_iter, Connection};

mod branches;
mod history_export;
mod query_export;
mod query_refresh;
mod query_scope_export;
mod reads;
mod session;
mod storage_admin;
mod subscriptions;
mod sync_apply;
mod sync_export;
mod transaction_builder;
mod transaction_status;
mod write_batch;
mod write_core;
mod writes;
use history_export::*;
use sync_apply::*;
#[allow(unused_imports)]
pub use transaction_builder::TransactionBuilder;

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    auth: RuntimeAuth,
    node_num: i64,
    branch_num: i64,
}
pub(crate) struct QueryScopeOptions<'a> {
    ref_include_fields: &'a [&'a str],
    extra_row_ids: &'a [String],
}
impl QueryScopeOptions<'_> {
    fn empty() -> Self {
        Self {
            ref_include_fields: &[],
            extra_row_ids: &[],
        }
    }
}

fn tier_name(tier: i64) -> rusqlite::Result<String> {
    Ok(match tier {
        tx::TIER_EDGE => "edge",
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
    let mut stmt = conn.prepare(&format!(
        "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
        columns.join(", ")
    ))?;
    stmt.execute(params_from_iter(values.iter()))?;
    Ok(())
}
