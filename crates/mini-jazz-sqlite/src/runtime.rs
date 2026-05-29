use crate::apply::{
    apply_branch_records, apply_query_read_records, apply_read_records, apply_tx_records,
    tx_apply_info, ApplyCaches, ApplyTxInfo, BundleApplyPlan,
};
use crate::auth::RuntimeAuth;
use crate::profile::ProfileTimer;
use crate::query_api::{
    predicate_query, BuiltQuery, QueryCondition, QueryConditionOp, QueryDirection, QueryOrderBy,
};
use crate::query_observation::{
    built_query_from_read, built_query_read_value, observed_ids_from_query_value, observed_row_ids,
    support_window_query,
};
use crate::query_refresh::QueryRefreshPlan;
use crate::read_visibility::ReadVisibility;
use crate::rows::{ensure_row_id, ensure_row_id_with_status, public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::subscription::{RejectionSubscription, RowsSubscription, RowsSubscriptionQuery};
use crate::sync::{
    BranchRecord, Bundle, HistoryRecord, QueryReadRecord, ReadRecord, TxRecord,
    BUNDLE_PROTOCOL_VERSION,
};
use crate::time::now_ms;
use crate::types::{
    ApplyBundleProfile, QueryExportProfile, ReadTier, RejectionInfo, RowView, TransactionInfo,
};
use crate::{
    branch, effective, policy, policy_read_set, projection, query, query_predicate, read_set, tx,
    users, Result,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};

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
use write_core::{record_tx_write_num, row_id_used_by_other_table};

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
