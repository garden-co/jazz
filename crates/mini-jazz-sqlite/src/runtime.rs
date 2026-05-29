use crate::query_api::{predicate_query, BuiltQuery, QueryCondition, QueryConditionOp};
use crate::read_visibility::ReadVisibility;
use crate::rows::{ensure_row_id, ensure_row_id_with_status, public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, Operation, PolicyDef, SchemaDef};
use crate::subscription::{RejectionSubscription, RowsSubscription, RowsSubscriptionQuery};
use crate::sync::{
    history_op, BranchRecord, Bundle, HistoryRecord, QueryReadRecord, ReadRecord, TxRecord,
    BUNDLE_PROTOCOL_VERSION,
};
use crate::time::now_ms;
use crate::types::{
    ApplyBundleProfile, BranchInfo, QueryExportProfile, RejectionInfo, RowView, StorageStats,
    TransactionInfo,
};
use crate::{
    branch, effective, policy, projection, query, query_predicate, read_set, schema, stats,
    storage, tx, users, Result, Storage,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    auth: RuntimeAuth,
    node_num: i64,
    branch_num: i64,
}

#[cfg(not(target_arch = "wasm32"))]
struct ProfileTimer {
    started_at: Instant,
}

#[cfg(target_arch = "wasm32")]
struct ProfileTimer {
    started_at_ms: f64,
}

impl ProfileTimer {
    fn start() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self {
                started_at: Instant::now(),
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self {
                started_at_ms: js_sys::Date::now(),
            }
        }
    }

    fn elapsed_ms(&self) -> f64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started_at.elapsed().as_secs_f64() * 1000.0
        }
        #[cfg(target_arch = "wasm32")]
        {
            js_sys::Date::now() - self.started_at_ms
        }
    }
}

struct AwaitingDependencyTx {
    tx_num: i64,
    tx_id: String,
    auth_user: String,
}

pub(crate) struct QueryScopeOptions<'a> {
    ref_include_fields: &'a [&'a str],
    extra_row_ids: &'a [String],
}

struct BatchedQueryScopeItem {
    op: String,
    value: JsonValue,
    rows: Vec<RowView>,
    extra_row_ids: Vec<String>,
}

type PredicateRefreshValue = (JsonValue, Vec<String>);
type TopFieldRefreshValue = (JsonValue, Vec<String>);
type TopCreatedAtRefreshValue = (JsonValue, Vec<String>);

enum QueryRefreshPlan {
    Predicate {
        table: String,
        field: String,
        op: String,
        values: Vec<PredicateRefreshValue>,
    },
    RecursiveRefs {
        table: String,
        field: String,
        root_ids: Vec<String>,
    },
    TopCreatedAt {
        table: String,
        field: String,
        values: Vec<TopCreatedAtRefreshValue>,
        limit: usize,
    },
    TopField {
        table: String,
        field: String,
        values: Vec<TopFieldRefreshValue>,
        order_field: String,
        limit: usize,
    },
    Single(QueryReadRecord),
}

impl QueryScopeOptions<'_> {
    fn empty() -> Self {
        Self {
            ref_include_fields: &[],
            extra_row_ids: &[],
        }
    }
}

pub const ADMIN_SYSTEM_USER: &str = "@system/admin";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeAuth {
    Client(User),
    TrustedPeer { session: TrustedSession },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustedSession {
    Admin,
    AsUser(User),
    AttributingToUser(User),
}

impl Runtime {
    pub fn open(storage: Storage, node_id: &str, user: &str) -> Result<Self> {
        Self::open_with_schema(storage, node_id, user, SchemaDef::attempt3_fixture())
    }

    pub fn open_with_schema(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::Client(User(user.to_owned())),
            schema_def,
        )
    }

    pub fn open_trusted_with_schema(
        storage: Storage,
        node_id: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::Admin,
            },
            schema_def,
        )
    }

    pub fn open_trusted_with_session_user(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::AsUser(User(user.to_owned())),
            },
            schema_def,
        )
    }

    pub fn open_trusted_attributing_to_user(
        storage: Storage,
        node_id: &str,
        user: &str,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        Self::open_with_schema_and_auth(
            storage,
            node_id,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::AttributingToUser(User(user.to_owned())),
            },
            schema_def,
        )
    }

    fn open_with_schema_and_auth(
        storage: Storage,
        node_id: &str,
        auth: RuntimeAuth,
        schema_def: SchemaDef,
    ) -> Result<Self> {
        let conn = storage::open(storage)?;
        schema::install(&conn, &schema_def)?;
        let node_num = tx::ensure_node(&conn, node_id)?;
        Ok(Self {
            conn,
            schema: schema_def,
            node_id: node_id.to_owned(),
            auth,
            node_num,
            branch_num: 1,
        })
    }

    pub fn is_trusted(&self) -> bool {
        matches!(self.auth, RuntimeAuth::TrustedPeer { .. })
    }

    pub fn session_user(&self) -> &str {
        self.policy_user()
    }

    fn policy_user(&self) -> &str {
        match &self.auth {
            RuntimeAuth::Client(User(user)) => user,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::AsUser(User(user)),
            } => user,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::AttributingToUser(User(user)),
            } => user,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::Admin,
            } => ADMIN_SYSTEM_USER,
        }
    }

    fn attribution_user(&self) -> &str {
        match &self.auth {
            RuntimeAuth::Client(User(user)) => user,
            RuntimeAuth::TrustedPeer {
                session:
                    TrustedSession::AsUser(User(user)) | TrustedSession::AttributingToUser(User(user)),
            } => user,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::Admin,
            } => ADMIN_SYSTEM_USER,
        }
    }

    fn bypasses_policy(&self) -> bool {
        matches!(
            &self.auth,
            RuntimeAuth::TrustedPeer {
                session: TrustedSession::Admin | TrustedSession::AttributingToUser(_)
            }
        )
    }

    pub fn run_as_user<T>(&mut self, user: &str, f: impl FnOnce(&mut Runtime) -> T) -> T {
        assert!(
            self.is_trusted(),
            "run_as_user is only valid for trusted peers"
        );
        let previous = self.auth.clone();
        self.auth = RuntimeAuth::TrustedPeer {
            session: TrustedSession::AsUser(User(user.to_owned())),
        };
        let result = f(self);
        self.auth = previous;
        result
    }

    pub fn run_attributing_to_user<T>(
        &mut self,
        user: &str,
        f: impl FnOnce(&mut Runtime) -> T,
    ) -> T {
        assert!(
            self.is_trusted(),
            "run_attributing_to_user is only valid for trusted peers"
        );
        let previous = self.auth.clone();
        self.auth = RuntimeAuth::TrustedPeer {
            session: TrustedSession::AttributingToUser(User(user.to_owned())),
        };
        let result = f(self);
        self.auth = previous;
        result
    }

    pub fn insert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.write_row(table_name, id, values, history_op::INSERT)
    }

    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.physical_row_num_for(id)?;
        self.write_row(table_name, id, values, history_op::UPDATE)
    }

    pub fn upsert_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            history_op::UPDATE
        } else {
            history_op::INSERT
        };
        self.write_row(table_name, id, values, op)
    }

    pub fn resolve_row_conflict(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            history_op::UPDATE
        } else {
            history_op::INSERT
        };
        self.write_row(table_name, id, values, op)
    }

    fn write_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
        op: i64,
    ) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let allowed = insert_row_in_tx(InsertRowInTx {
            db: &db,
            schema: &self.schema,
            table_name,
            id,
            values: &values,
            tx_num,
            branch_num: self.branch_num,
            now,
            user: &user,
            bypass_policy,
            op,
        })?;
        let row_num = row_num(&db, id)?;
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            db.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ? AND visible_tx_num = ?",
                    crate::schema::current_table(&table.name)
                ),
                params![row_num, self.branch_num, tx_num],
            )?;
        }
        db.commit()?;
        Ok(tx_id)
    }

    pub fn read_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)
    }

    pub fn read_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Vec<RowView>> {
        self.query_context().read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value,
            order_field_name,
            limit,
        )
    }

    pub fn export_table_history(&self, table_name: &str) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let txs = export_txs(&self.conn)?;
        let history = export_table_history(
            &self.conn,
            &self.schema,
            table_name,
            user,
            bypass_policy,
            self.branch_num,
        )?;
        let reads = export_reads_for_history(&self.conn, &history)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            Vec::new(),
            history,
        ))
    }

    pub fn export_exclusive_transaction_forwarding(
        &self,
        table_name: &str,
        tx_id: &str,
        auth_user: &str,
    ) -> Result<Bundle> {
        let mut bundle = self.export_table_history(table_name)?;
        if !bundle.history.iter().any(|record| record.tx_id == tx_id) {
            let tx_num = tx::tx_num(&self.conn, tx_id)?;
            let history = history_records_for_tx(&self.conn, &self.schema, tx_num, tx_id)?
                .into_iter()
                .filter(|record| record.table == table_name)
                .collect::<Vec<_>>();
            if history.is_empty() {
                return Err(crate::Error::new(format!(
                    "transaction {tx_id} has no exported history"
                )));
            }
            let reads = export_reads_for_history(&self.conn, &history)?;
            let mut branches = export_branch_records_for_history(&self.conn, &history)?;
            include_branch_record(&self.conn, &mut branches, self.branch_num)?;
            bundle = make_bundle(
                &self.schema,
                branches,
                export_txs(&self.conn)?,
                reads,
                Vec::new(),
                history,
            );
        }
        let tx_record = bundle
            .txs
            .iter_mut()
            .find(|record| record.tx_id == tx_id)
            .ok_or_else(|| crate::Error::new(format!("transaction {tx_id} is not in bundle")))?;
        tx_record.conflict_mode = tx::MODE_EXCLUSIVE;
        tx_record.outcome = tx::OUTCOME_PENDING;
        tx_record.global_epoch = None;
        tx_record.receipt_tiers.clear();
        tx_record.auth_user = Some(auth_user.to_owned());
        Ok(bundle)
    }

    pub fn export_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let rows = self.read_recursive_refs(table_name, root_id, parent_field)?;
        let row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history =
            export_visible_table_history(&visibility, table_name, &branch_nums, Some(&row_nums))?;
        history.extend(export_deleted_recursive_descendant_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_recursive_scope_repair_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let txs = export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &[])?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![QueryReadRecord {
            branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: parent_field.to_owned(),
            op: "recursive_refs".to_owned(),
            value: JsonValue::String(root_id.to_owned()),
        }];
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    fn export_many_recursive_refs(
        &self,
        table_name: &str,
        parent_field: &str,
        root_ids: Vec<String>,
    ) -> Result<Bundle> {
        self.schema.table_def(table_name)?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut row_nums = Vec::new();
        let mut query_reads = Vec::new();

        for root_id in root_ids {
            let rows = self.read_recursive_refs(table_name, &root_id, parent_field)?;
            row_nums.extend(
                rows.iter()
                    .map(|row| row_num(&self.conn, &row.id))
                    .collect::<Result<Vec<_>>>()?,
            );
            query_reads.push(QueryReadRecord {
                branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
                table: table_name.to_owned(),
                field: parent_field.to_owned(),
                op: "recursive_refs".to_owned(),
                value: JsonValue::String(root_id),
            });
        }

        row_nums.sort();
        row_nums.dedup();
        let mut history =
            export_visible_table_history(&visibility, table_name, &branch_nums, Some(&row_nums))?;
        history.extend(export_deleted_recursive_descendant_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_recursive_scope_repair_history(
            &self.conn,
            &self.schema,
            table_name,
            parent_field,
            &branch_nums,
            &row_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let txs = export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &[])?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        self.apply_bundle_inner(bundle, true).map(|_| ())
    }

    pub fn profile_apply_bundle(&mut self, bundle: &Bundle) -> Result<ApplyBundleProfile> {
        self.apply_bundle_inner(bundle, true)
    }

    fn apply_bundle_inner(
        &mut self,
        bundle: &Bundle,
        check_policy_fingerprint: bool,
    ) -> Result<ApplyBundleProfile> {
        let total_started = ProfileTimer::start();
        let validation_started = ProfileTimer::start();
        if bundle.protocol_version != BUNDLE_PROTOCOL_VERSION {
            return Err(crate::Error::new(format!(
                "unsupported bundle protocol version {}",
                bundle.protocol_version
            )));
        }
        let local_schema_fingerprint = self.schema.compatibility_fingerprint();
        if bundle.schema_fingerprint != "legacy"
            && bundle.schema_fingerprint != local_schema_fingerprint
        {
            return Err(crate::Error::new("incompatible schema fingerprint"));
        }
        if check_policy_fingerprint {
            let policy_tables = bundle_policy_tables(bundle);
            for table_name in &policy_tables {
                self.schema.table_def(table_name)?;
            }
            if !policy_tables.is_empty() {
                let local_policy_fingerprint = self
                    .schema
                    .policy_fingerprint_for_tables(policy_tables.iter());
                if bundle.policy_fingerprint != "legacy"
                    && bundle.policy_fingerprint != local_policy_fingerprint
                {
                    return Err(crate::Error::new("incompatible policy fingerprint"));
                }
            }
        }
        let validation_ms = validation_started.elapsed_ms();
        let schema = self.schema.clone();
        let repair_user = self.policy_user().to_owned();
        let repair_bypass_policy = self.bypasses_policy();
        let begin_tx_started = ProfileTimer::start();
        let db = self.conn.transaction()?;
        let begin_tx_ms = begin_tx_started.elapsed_ms();

        let branches_started = ProfileTimer::start();
        for branch_record in &bundle.branches {
            let branch_num = branch::ensure(
                &db,
                &branch_record.branch_id,
                branch_record.base_global_epoch,
                now_ms(),
            )?;
            branch::set_sources_from_sync(
                &db,
                branch_num,
                &branch_record.source_branch_ids,
                branch_record.source_version,
            )?;
        }
        let mut branch_nums_by_id = BTreeMap::new();
        for branch_record in &bundle.branches {
            let branch_num = branch::checkout(&db, &branch_record.branch_id)?;
            branch_nums_by_id.insert(branch_record.branch_id.clone(), branch_num);
        }
        let branches_ms = branches_started.elapsed_ms();

        let table_nums_by_name = crate::schema::table_nums(&db)?;

        let txs_started = ProfileTimer::start();
        let mut tx_nums_by_id = BTreeMap::new();
        let mut tx_info_by_num = BTreeMap::new();
        for tx_record in &bundle.txs {
            let node_num = tx::ensure_node(&db, &tx_record.node_id)?;
            let metadata_json = tx_metadata_json(tx_record.auth_user.as_deref())?;
            db.execute(
                "INSERT INTO jazz_tx
                 (tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(tx_id) DO UPDATE SET
                   outcome = MAX(jazz_tx.outcome, excluded.outcome),
                   global_epoch = CASE
                     WHEN jazz_tx.global_epoch IS NULL THEN excluded.global_epoch
                     WHEN excluded.global_epoch IS NULL THEN jazz_tx.global_epoch
                     ELSE MAX(jazz_tx.global_epoch, excluded.global_epoch)
                   END,
                   conflict_mode = MAX(jazz_tx.conflict_mode, excluded.conflict_mode),
                   metadata_json = CASE
                     WHEN excluded.metadata_json != '{}' THEN excluded.metadata_json
                     ELSE jazz_tx.metadata_json
                   END",
                params![
                    tx_record.tx_id,
                    node_num,
                    tx_record.local_epoch,
                    tx_record.global_epoch,
                    tx::KIND_DATA,
                    tx_record.conflict_mode,
                    tx_record.outcome,
                    tx_record.created_at,
                    metadata_json
                ],
            )?;
            let tx_num = tx::tx_num(&db, &tx_record.tx_id)?;
            tx_nums_by_id.insert(tx_record.tx_id.clone(), tx_num);
            tx_info_by_num.insert(tx_num, tx_apply_info(&db, tx_num)?);
            if tx_record.outcome == tx::OUTCOME_REJECTED {
                if let Some(code) = &tx_record.rejection_code {
                    let detail_json = encode_optional_json(tx_record.rejection_detail.as_ref())?;
                    db.execute(
                        "INSERT INTO jazz_tx_rejection (tx_num, code, detail_json)
                         VALUES (?, ?, ?)
                         ON CONFLICT(tx_num) DO UPDATE SET
                           code = excluded.code,
                           detail_json = CASE
                             WHEN excluded.detail_json = 'null' AND jazz_tx_rejection.detail_json != 'null' THEN jazz_tx_rejection.detail_json
                             ELSE excluded.detail_json
                           END",
                        params![tx_num, code, detail_json],
                    )?;
                }
            }
            if let Some(global_epoch) = tx_record.global_epoch {
                db.execute(
                    "INSERT OR REPLACE INTO jazz_tx_receipt
                     (tx_num, tier, observed_at, receipt_json)
                     VALUES (?, ?, ?, '{}')",
                    params![tx_num, tx::TIER_GLOBAL, global_epoch],
                )?;
            }
            for tier in &tx_record.receipt_tiers {
                let observed_at = if *tier == tx::TIER_GLOBAL {
                    tx_record.global_epoch.unwrap_or(tx_record.created_at)
                } else {
                    tx_record.created_at
                };
                db.execute(
                    "INSERT OR REPLACE INTO jazz_tx_receipt
                     (tx_num, tier, observed_at, receipt_json)
                     VALUES (?, ?, ?, '{}')",
                    params![tx_num, tier, observed_at],
                )?;
            }
        }
        let txs_ms = txs_started.elapsed_ms();

        let reads_started = ProfileTimer::start();
        let mut row_nums_by_id = BTreeMap::new();
        let mut row_nums_created_in_apply = BTreeSet::new();
        let mut user_nums_by_id = BTreeMap::new();
        let mut insert_read_stmt = db.prepare(
            "INSERT OR REPLACE INTO jazz_tx_read
             (tx_num, table_num, row_num, reason, observed_tx_num)
             VALUES (?, ?, ?, ?, ?)",
        )?;
        for read_record in &bundle.reads {
            let tx_num = tx_nums_by_id
                .get(&read_record.tx_id)
                .copied()
                .ok_or_else(|| crate::Error::new("bundle read references missing tx"))?;
            let row_num = cached_ensure_row_id_with_status(
                &db,
                &mut row_nums_by_id,
                &mut row_nums_created_in_apply,
                &read_record.table,
                &read_record.row_id,
            )?;
            let table_num = table_nums_by_name
                .get(&read_record.table)
                .copied()
                .ok_or_else(|| crate::Error::new("bundle read references missing table"))?;
            let observed_tx_num = read_record
                .observed_tx_id
                .as_deref()
                .map(|observed_tx_id| {
                    tx_nums_by_id.get(observed_tx_id).copied().ok_or_else(|| {
                        crate::Error::new("bundle read references missing observed tx")
                    })
                })
                .transpose()?;
            insert_read_stmt.execute(params![
                tx_num,
                table_num,
                row_num,
                read_record.reason,
                observed_tx_num
            ])?;
        }
        drop(insert_read_stmt);
        let reads_ms = reads_started.elapsed_ms();

        let rejected_cleanup_started = ProfileTimer::start();
        if bundle
            .txs
            .iter()
            .any(|tx| tx.outcome == tx::OUTCOME_REJECTED)
        {
            for table_name in bundle_touched_tables(bundle) {
                schema.table_def(&table_name)?;
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE visible_tx_num IN (
                           SELECT tx_num FROM jazz_tx WHERE outcome = ?
                         )",
                        crate::schema::current_table(&table_name)
                    ),
                    params![tx::OUTCOME_REJECTED],
                )?;
            }
        }
        let rejected_cleanup_ms = rejected_cleanup_started.elapsed_ms();

        let query_reads_started = ProfileTimer::start();
        for query_read in &bundle.query_reads {
            Self::record_query_read(&db, query_read)?;
        }
        let query_reads_ms = query_reads_started.elapsed_ms();

        let history_started = ProfileTimer::start();
        let mut history_context = ApplyHistoryContext {
            schema: &schema,
            db: &db,
            local_node_num: self.node_num,
            tx_nums_by_id: &tx_nums_by_id,
            tx_info_by_num: &tx_info_by_num,
            branch_nums_by_id: &branch_nums_by_id,
            table_nums_by_name: &table_nums_by_name,
            row_nums_by_id: &mut row_nums_by_id,
            row_nums_created_in_apply: &mut row_nums_created_in_apply,
            user_nums_by_id: &mut user_nums_by_id,
        };
        for record in &bundle.history {
            Self::apply_history_record(&mut history_context, record)?;
        }
        let history_ms = history_started.elapsed_ms();

        let query_scope_repair_started = ProfileTimer::start();
        for query_read in &bundle.query_reads {
            Self::apply_query_scope_repair(
                &schema,
                &db,
                query_read,
                &repair_user,
                repair_bypass_policy,
            )?;
        }
        let query_scope_repair_ms = query_scope_repair_started.elapsed_ms();

        let commit_started = ProfileTimer::start();
        db.commit()?;
        let commit_ms = commit_started.elapsed_ms();

        let revalidate_started = ProfileTimer::start();
        self.revalidate_awaiting_dependencies()?;
        let revalidate_awaiting_ms = revalidate_started.elapsed_ms();

        Ok(ApplyBundleProfile {
            total_ms: total_started.elapsed_ms(),
            validation_ms,
            begin_tx_ms,
            branches_ms,
            txs_ms,
            reads_ms,
            rejected_cleanup_ms,
            query_reads_ms,
            history_ms,
            query_scope_repair_ms,
            commit_ms,
            revalidate_awaiting_ms,
            branch_rows: bundle.branches.len(),
            tx_rows: bundle.txs.len(),
            read_rows: bundle.reads.len(),
            query_read_rows: bundle.query_reads.len(),
            history_rows: bundle.history.len(),
        })
    }

    pub fn observed_query_reads(&self) -> Result<Vec<QueryReadRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_id, table_name, field_name, op, value_json
             FROM jazz_query_read
             ORDER BY branch_id, table_name, field_name, op, value_json",
        )?;
        let rows = stmt.query_map([], |row| {
            let value_json: String = row.get(4)?;
            let value = serde_json::from_str(&value_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            Ok(QueryReadRecord {
                branch_id: row.get(0)?,
                table: row.get(1)?,
                field: row.get(2)?,
                op: row.get(3)?,
                value,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn export_observed_query_refreshes(&self) -> Result<Vec<Bundle>> {
        let reads = self.observed_query_reads()?;
        self.export_query_read_refreshes(&reads)
    }

    pub fn export_query_read_refreshes(&self, reads: &[QueryReadRecord]) -> Result<Vec<Bundle>> {
        let current_branch_id = branch_id_for_num(&self.conn, self.branch_num)?;
        let mut bundles = Vec::new();

        for plan in plan_query_read_refreshes(&current_branch_id, reads)? {
            match plan {
                QueryRefreshPlan::Predicate {
                    table,
                    field,
                    op,
                    values,
                } => bundles
                    .push(self.export_many_predicate_query_refreshes(&table, &field, &op, values)?),
                QueryRefreshPlan::RecursiveRefs {
                    table,
                    field,
                    root_ids,
                } => bundles.push(self.export_many_recursive_refs(&table, &field, root_ids)?),
                QueryRefreshPlan::TopCreatedAt {
                    table,
                    field,
                    values,
                    limit,
                } => bundles.push(
                    self.export_many_query_where_eq_top_created_at_desc_with_previous_observed(
                        &table, &field, values, limit,
                    )?,
                ),
                QueryRefreshPlan::TopField {
                    table,
                    field,
                    values,
                    order_field,
                    limit,
                } => bundles.push(
                    self.export_many_query_where_eq_top_field_desc_with_previous_observed(
                        &table,
                        &field,
                        values,
                        &order_field,
                        limit,
                    )?,
                ),
                QueryRefreshPlan::Single(read) => {
                    bundles.push(self.export_query_read_refresh(&read)?);
                }
            }
        }
        Ok(bundles)
    }

    pub fn forget_observed_query_read(&mut self, read: &QueryReadRecord) -> Result<()> {
        self.conn.execute(
            "DELETE FROM jazz_query_read
             WHERE branch_id = ?
               AND table_name = ?
               AND field_name = ?
               AND op = ?
               AND value_json = ?",
            params![
                read.branch_id,
                read.table,
                read.field,
                read.op,
                serde_json::to_string(&read.value)
                    .map_err(|err| crate::Error::new(err.to_string()))?
            ],
        )?;
        Ok(())
    }

    fn export_query_read_refresh(&self, read: &QueryReadRecord) -> Result<Bundle> {
        if read.branch_id != branch_id_for_num(&self.conn, self.branch_num)? {
            return Err(crate::Error::new("query refresh branch is not checked out"));
        }
        match read.op.as_str() {
            "eq" => self.export_query_where_eq(&read.table, &read.field, read.value.clone()),
            "ne" => self.export_query_where_ne(&read.table, &read.field, read.value.clone()),
            "contains" => {
                let Some(needle) = read.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.export_query_where_contains(&read.table, &read.field, needle)
            }
            "in" => {
                let Some(values) = read.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.export_query_where_in(&read.table, &read.field, values.clone())
            }
            "recursive_refs" => {
                let Some(root_id) = read.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                self.export_recursive_refs(&read.table, root_id, &read.field)
            }
            "eq_top_created_at_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
                self.export_query_where_eq_top_created_at_desc_with_previous_observed(
                    &read.table,
                    &read.field,
                    value.clone(),
                    limit as usize,
                    observed_ids_from_query_value(&read.value)?,
                )
            }
            "eq_top_field_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
                let order_field = read
                    .value
                    .get("order_field")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
                self.export_query_where_eq_top_field_desc_with_previous_observed(
                    &read.table,
                    &read.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                    observed_ids_from_query_value(&read.value)?,
                )
            }
            "query" => self.export_query(built_query_from_read(read)?),
            "absent" => {
                if read.field == "id" {
                    let Some(row_id) = read.value.as_str() else {
                        return Err(crate::Error::new("absent id expects string value"));
                    };
                    if self
                        .query(predicate_query(
                            &read.table,
                            &read.field,
                            QueryConditionOp::Eq,
                            JsonValue::String(row_id.to_owned()),
                        ))?
                        .is_empty()
                    {
                        let mut branches = Vec::new();
                        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
                        let query_reads = vec![read.clone()];
                        return Ok(make_bundle(
                            &self.schema,
                            branches,
                            export_txs(&self.conn)?,
                            Vec::new(),
                            query_reads,
                            Vec::new(),
                        ));
                    }
                    return self.export_query_where_eq(
                        &read.table,
                        &read.field,
                        JsonValue::String(row_id.to_owned()),
                    );
                }
                let query_reads = vec![read.clone()];
                Ok(make_bundle(
                    &self.schema,
                    Vec::new(),
                    export_txs(&self.conn)?,
                    Vec::new(),
                    query_reads,
                    Vec::new(),
                ))
            }
            op => Err(crate::Error::new(format!(
                "unsupported observed query refresh {op}"
            ))),
        }
    }

    fn record_query_read(db: &Connection, query_read: &QueryReadRecord) -> Result<()> {
        db.execute(
            "INSERT OR REPLACE INTO jazz_query_read
             (branch_id, table_name, field_name, op, value_json, observed_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                query_read.branch_id,
                query_read.table,
                query_read.field,
                query_read.op,
                serde_json::to_string(&query_read.value)
                    .map_err(|err| crate::Error::new(err.to_string()))?,
                now_ms()
            ],
        )?;
        Ok(())
    }

    pub fn apply_untrusted_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        self.apply_untrusted_bundle_with_auth_user(bundle, None)
    }

    pub fn apply_untrusted_bundle_as_user(&mut self, bundle: &Bundle, user: &str) -> Result<()> {
        self.apply_untrusted_bundle_with_auth_user(bundle, Some(user))
    }

    pub fn stage_exclusive_bundle_for_forwarding(&mut self, bundle: &Bundle) -> Result<()> {
        for tx_record in &bundle.txs {
            if tx_record.conflict_mode == tx::MODE_EXCLUSIVE
                && tx_record.outcome == tx::OUTCOME_PENDING
                && tx_record.auth_user.is_none()
            {
                return Err(crate::Error::new(format!(
                    "exclusive transaction {} is missing forwarded auth user",
                    tx_record.tx_id
                )));
            }
        }
        self.apply_bundle_inner(bundle, false)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    fn apply_untrusted_bundle_with_auth_user(
        &mut self,
        bundle: &Bundle,
        connection_auth_user: Option<&str>,
    ) -> Result<()> {
        let stale_exclusive_tx_ids =
            read_set::stale_exclusive_tx_ids_in_bundle(&self.conn, bundle)?;
        let forwarded_auth_users = bundle
            .txs
            .iter()
            .filter(|tx| tx.conflict_mode == tx::MODE_EXCLUSIVE)
            .filter_map(|tx| {
                tx.auth_user
                    .as_deref()
                    .map(|user| (tx.tx_id.as_str(), user))
            })
            .collect::<BTreeMap<_, _>>();
        self.apply_bundle_inner(bundle, false)?;
        let mut rejected = BTreeSet::new();
        let mut exclusive_to_accept = BTreeSet::new();
        for tx_id in stale_exclusive_tx_ids {
            self.reject_transaction_with_detail(
                &tx_id,
                "stale_read_set",
                json!({
                    "reason": "exclusive_read_dependency_changed",
                }),
            )?;
            rejected.insert(tx_id);
        }
        for record in &bundle.history {
            if rejected.contains(&record.tx_id) {
                continue;
            }
            let tx_num = tx::tx_num(&self.conn, &record.tx_id)?;
            if tx_outcome(&self.conn, tx_num)? != tx::OUTCOME_PENDING {
                continue;
            }
            let conflict_mode = tx_conflict_mode(&self.conn, tx_num)?;
            if conflict_mode == tx::MODE_EXCLUSIVE {
                if !forwarded_auth_users.contains_key(record.tx_id.as_str()) {
                    self.reject_transaction_with_detail(
                        &record.tx_id,
                        "policy_denied",
                        json!({
                            "reason": "missing_auth_user",
                        }),
                    )?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
                if read_set::tx_read_set_is_stale(&self.conn, tx_num, &record.branch_id)? {
                    self.reject_transaction_with_detail(
                        &record.tx_id,
                        "stale_read_set",
                        json!({
                            "reason": "exclusive_read_dependency_changed",
                        }),
                    )?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
            }
            let table = self.schema.table_def(&record.table)?;
            let row_num = ensure_row_id(&self.conn, &record.table, &record.row_id)?;
            let auth_user = if conflict_mode == tx::MODE_EXCLUSIVE {
                forwarded_auth_users.get(record.tx_id.as_str()).copied()
            } else {
                connection_auth_user
            };
            if auth_user.is_none() {
                self.reject_transaction_with_detail(
                    &record.tx_id,
                    "policy_denied",
                    json!({
                        "reason": "missing_auth_user",
                    }),
                )?;
                rejected.insert(record.tx_id.clone());
                continue;
            }
            let auth_user = auth_user.expect("auth user checked above");
            let allowed = write_allowed_for_history_record(
                &self.conn,
                &self.schema,
                table,
                row_num,
                record,
                tx_num,
                Some(auth_user),
            )?;
            if !allowed {
                let detail =
                    policy_denial_detail_for_history_record(&self.conn, table, record, tx_num)?;
                if is_policy_dependency_unavailable(&detail) {
                    if conflict_mode == tx::MODE_EXCLUSIVE {
                        self.reject_transaction_with_detail(
                            &record.tx_id,
                            "policy_denied",
                            detail,
                        )?;
                        rejected.insert(record.tx_id.clone());
                        continue;
                    }
                    mark_transaction_awaiting_dependency(&self.conn, tx_num, auth_user, &detail)?;
                    remove_current_for_awaiting_dependency(&self.conn, record, row_num)?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
                self.reject_transaction_with_detail(&record.tx_id, "policy_denied", detail)?;
                rejected.insert(record.tx_id.clone());
            } else {
                clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
                if conflict_mode == tx::MODE_EXCLUSIVE {
                    exclusive_to_accept.insert(record.tx_id.clone());
                }
            }
        }
        let mut accepted_exclusive = false;
        for tx_id in exclusive_to_accept {
            let tx_num = tx::tx_num(&self.conn, &tx_id)?;
            if !rejected.contains(&tx_id) && tx_outcome(&self.conn, tx_num)? == tx::OUTCOME_PENDING
            {
                tx::accept_global(&self.conn, &tx_id, next_global_epoch(&self.conn)?)?;
                accepted_exclusive = true;
            }
        }
        if !rejected.is_empty() || accepted_exclusive {
            projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        }
        self.revalidate_awaiting_dependencies()?;
        Ok(())
    }

    fn revalidate_awaiting_dependencies(&mut self) -> Result<()> {
        let awaiting = awaiting_dependency_transactions(&self.conn)?;
        let mut changed = false;
        for awaiting in awaiting {
            if tx_outcome(&self.conn, awaiting.tx_num)? != tx::OUTCOME_PENDING {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                changed = true;
                continue;
            }
            let records =
                history_records_for_tx(&self.conn, &self.schema, awaiting.tx_num, &awaiting.tx_id)?;
            if records.is_empty() {
                continue;
            }
            let mut still_waiting = None;
            let mut denied = None;
            for record in records {
                let table = self.schema.table_def(&record.table)?;
                let row_num = row_num(&self.conn, &record.row_id)?;
                let allowed = write_allowed_for_history_record(
                    &self.conn,
                    &self.schema,
                    table,
                    row_num,
                    &record,
                    awaiting.tx_num,
                    Some(awaiting.auth_user.as_str()),
                )?;
                if !allowed {
                    let detail = policy_denial_detail_for_history_record(
                        &self.conn,
                        table,
                        &record,
                        awaiting.tx_num,
                    )?;
                    if is_policy_dependency_unavailable(&detail) {
                        still_waiting = Some(detail);
                    } else {
                        denied = Some(detail);
                    }
                    break;
                }
            }
            if let Some(detail) = denied {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                tx::reject_with_detail_json(
                    &self.conn,
                    &awaiting.tx_id,
                    "policy_denied",
                    &serde_json::to_string(&detail)
                        .map_err(|err| crate::Error::new(err.to_string()))?,
                )?;
                changed = true;
            } else if let Some(detail) = still_waiting {
                mark_transaction_awaiting_dependency(
                    &self.conn,
                    awaiting.tx_num,
                    &awaiting.auth_user,
                    &detail,
                )?;
            } else {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                if tx_conflict_mode(&self.conn, awaiting.tx_num)? == tx::MODE_MERGEABLE {
                    tx::accept_edge(&self.conn, &awaiting.tx_id, now_ms())?;
                }
                changed = true;
            }
        }
        if changed {
            projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        }
        Ok(())
    }

    fn apply_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Query-scope repair keeps a receiver's current projection from retaining
        // rows that used to satisfy an observed query but are no longer covered by
        // the refresh bundle.
        //
        // Export side:
        //
        //   +------------------+      +---------------------+
        //   | current results  | ---> | visible row history |
        //   +------------------+      +---------------------+
        //            |                         ^
        //            v                         |
        //   +------------------+      +---------------------+
        //   | repair row nums  | ---> | old matching rows   |
        //   +------------------+      +---------------------+
        //
        // Apply side:
        //
        //   +-------------------+      +----------------------+
        //   | local current row | ---> | still has matching   |
        //   | matches read      |      | history in bundle?   |
        //   +-------------------+      +----------------------+
        //             | yes                     | no
        //             v                         v
        //        keep current             delete current
        //
        // `apply_bundle` runs this both before and after applying incoming
        // history. The first pass can remove stale rows using repair history
        // already present locally; the second pass observes repair history that
        // arrived in the bundle and catches page-boundary changes after current
        // projection updates.
        if query_read.op == "absent" {
            let table = schema.table_def(&query_read.table)?;
            if query_read.field != "id"
                && !table
                    .fields
                    .iter()
                    .any(|field| field.name == query_read.field)
            {
                return Err(crate::Error::new(format!(
                    "unknown query field {}",
                    query_read.field
                )));
            }
            return Ok(());
        }
        if query_read.op == "recursive_refs" {
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            if !matches!(field.kind, FieldKind::Ref { .. }) {
                return Err(crate::Error::new(format!(
                    "recursive refs expects ref field {}",
                    query_read.field
                )));
            }
            if !query_read.value.is_string() {
                return Err(crate::Error::new("recursive refs expects root id string"));
            }
            return Ok(());
        }
        if query_read.op == "query" {
            let query = built_query_from_read(query_read)?;
            return Self::apply_built_query_scope_repair(
                schema,
                db,
                query_read,
                &query,
                user,
                bypass_policy,
            );
        }
        if query_read.op == "eq_top_created_at_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            if matches!(query_read.field.as_str(), "id" | "$createdBy") {
                let branch_num = branch::checkout(db, &query_read.branch_id)?;
                let observed_row_nums = observed_ids_from_query_value(&query_read.value)?
                    .into_iter()
                    .map(|row_id| row_num(db, &row_id))
                    .collect::<Result<Vec<_>>>()?;
                let observed_filter = if observed_row_nums.is_empty() {
                    String::new()
                } else {
                    format!(
                        "AND row_num NOT IN ({})",
                        sql_placeholders(observed_row_nums.len())
                    )
                };
                let mut params = vec![rusqlite::types::Value::Integer(branch_num)];
                let predicate_sql = if query_read.field == "id" {
                    let row_id = value
                        .as_str()
                        .ok_or_else(|| crate::Error::new("id equality expects a string value"))?;
                    params.push(rusqlite::types::Value::Integer(ensure_row_id(
                        db,
                        &query_read.table,
                        row_id,
                    )?));
                    "row_num = ?".to_owned()
                } else {
                    let user_id = value.as_str().ok_or_else(|| {
                        crate::Error::new("$createdBy equality expects a string value")
                    })?;
                    let Ok(user_num) = users::user_num(db, user_id) else {
                        return Ok(());
                    };
                    params.push(rusqlite::types::Value::Integer(user_num));
                    "j_created_by = ?".to_owned()
                };
                params.extend(
                    observed_row_nums
                        .into_iter()
                        .map(rusqlite::types::Value::Integer),
                );
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND is_deleted = 0
                           AND {predicate_sql}
                           {observed_filter}",
                        crate::schema::current_table(&query_read.table),
                    ),
                    params_from_iter(params.iter()),
                )?;
                return Ok(());
            }
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.j_created_at DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "eq_top_field_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
            let order_field_name = query_read
                .value
                .get("order_field")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let order_field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == order_field_name)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown order field {order_field_name}"))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let order_column =
                crate::schema::quote_ident(&crate::schema::storage_column(order_field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.{order_column} DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "in" && query_read.field != "id" {
            for value in query_read
                .value
                .as_array()
                .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
            {
                let eq_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: query_read.table.clone(),
                    field: query_read.field.clone(),
                    op: "eq".to_owned(),
                    value: value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &eq_read, user, bypass_policy)?;
            }
            return Ok(());
        }
        if query_read.field == "id" {
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            if query_read.op == "ne" {
                let excluded_id = query_read
                    .value
                    .as_str()
                    .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
                db.execute(
                    &format!(
                        "DELETE FROM {current_table}
                         WHERE j_branch_num = ?
                           AND row_num != (SELECT row_num FROM jazz_row_id WHERE row_id = ?)
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_row_id ids ON ids.row_num = h.row_num
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE ids.row_id != ?
                               AND h.j_branch_num = ?
                               AND h.op != {delete_op}
                               AND tx.outcome != ?
                           )",
                        current_table = crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                        delete_op = history_op::DELETE,
                    ),
                    params![
                        branch_num,
                        excluded_id,
                        excluded_id,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
                return Ok(());
            }
            let row_ids = id_predicate_values(&query_read.op, &query_read.value)?;
            for row_id in row_ids {
                let row_num = ensure_row_id(db, &query_read.table, &row_id)?;
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND row_num = ?
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE h.row_num = ?
                               AND h.j_branch_num = ?
                               AND h.op != {delete_op}
                               AND tx.outcome != ?
                           )",
                        crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                        delete_op = history_op::DELETE,
                    ),
                    params![
                        branch_num,
                        row_num,
                        row_num,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
            }
            return Ok(());
        }
        if query_read.field == "$createdBy" {
            let Some(created_by) = query_read.value.as_str() else {
                return Err(crate::Error::new(
                    "$createdBy predicate expects a string value",
                ));
            };
            let created_by_num = users::ensure_user(db, created_by)?;
            let created_by_sql = match query_read.op.as_str() {
                "eq" => "j_created_by = ?",
                "ne" => "j_created_by != ?",
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported $createdBy predicate op {op}"
                    )));
                }
            };
            let history_created_by_sql = match query_read.op.as_str() {
                "eq" => "h.j_created_by = ?",
                "ne" => "h.j_created_by != ?",
                _ => unreachable!("validated above"),
            };
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND {created_by_sql}
                       AND row_num NOT IN (
                         SELECT h.row_num
                         FROM {history_table} h
                         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                         WHERE h.j_branch_num = ?
                           AND {history_created_by_sql}
                           AND h.op != {delete_op}
                           AND tx.outcome != ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    history_table = crate::schema::history_table(&query_read.table),
                    delete_op = history_op::DELETE,
                ),
                params![
                    branch_num,
                    created_by_num,
                    branch_num,
                    created_by_num,
                    tx::OUTCOME_REJECTED
                ],
            )?;
            return Ok(());
        }
        let table = schema.table_def(&query_read.table)?;
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == query_read.field)
            .ok_or_else(|| {
                crate::Error::new(format!("unknown query field {}", query_read.field))
            })?;
        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let predicate_sql = query_predicate::sql(field, &predicate_column, &query_read.op)?;
        let predicate_value = query_predicate::value(field, &query_read.op, &query_read.value, db)?;
        db.execute(
            &format!(
                "DELETE FROM {}
                 WHERE j_branch_num = ?
                   AND is_deleted = 0
                   AND {predicate_sql}
                   AND row_num NOT IN (
                     SELECT ids.row_num
                     FROM jazz_row_id ids
                     JOIN {history_table} h ON h.row_num = ids.row_num
                     JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                     WHERE h.j_branch_num = ?
                       AND h.op != {delete_op}
                       AND tx.outcome != ?
                       AND {history_predicate_sql}
                   )",
                crate::schema::current_table(&query_read.table),
                history_table = crate::schema::history_table(&query_read.table),
                delete_op = history_op::DELETE,
                history_predicate_sql =
                    query_predicate::sql(field, &format!("h.{predicate_column}"), &query_read.op)?,
            ),
            params![
                branch_num,
                predicate_value.clone(),
                branch_num,
                tx::OUTCOME_REJECTED,
                predicate_value
            ],
        )?;
        Ok(())
    }

    fn apply_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Built queries are recorded as one opaque query-read value so they can
        // be replayed exactly after reconnect:
        //
        //   jazz_query_read
        //   field="$query", op="query", value=<BuiltQuery JSON>
        //
        // Repair keeps the old narrow fast path for simple predicates, then
        // falls back to a generic SQL-lowered pass for the wider built-query
        // language:
        //
        //   +-----------------------------+
        //   | BuiltQuery descriptor       |
        //   +-----------------------------+
        //        | one predicate only
        //        v
        //   +-----------------------------+      +--------------------------+
        //   | QueryReadRecord predicate   | ---> | apply_query_scope_repair |
        //   +-----------------------------+      +--------------------------+
        //
        //        | every other SQL-lowered shape
        //        v
        //   +-----------------------------+
        //   | generic SQL-lowered repair  |
        //   +-----------------------------+
        match built_query_repair_scope(built_query)? {
            BuiltQueryRepairScope::Predicate(condition) => {
                let predicate_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: built_query.table.clone(),
                    field: condition.column.clone(),
                    op: condition.op.as_str().to_owned(),
                    value: condition.value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &predicate_read, user, bypass_policy)
            }
            BuiltQueryRepairScope::Generic => Self::apply_generic_built_query_scope_repair(
                schema,
                db,
                query_read,
                built_query,
                user,
                bypass_policy,
            ),
        }
    }

    fn apply_generic_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        if built_query.limit.is_none() && built_query.offset.unwrap_or(0) == 0 {
            return Ok(());
        }

        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let context = query::QueryContext {
            conn: db,
            schema,
            branch_num,
            user,
            bypass_policy,
        };
        let mut scope_query = built_query.clone();
        scope_query.limit = None;
        scope_query.offset = None;
        let scope_row_nums = context
            .read_rows_for_built_query(&scope_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        if scope_row_nums.is_empty() {
            return Ok(());
        }

        let keep_query = built_query_repair_keep_query(built_query)?;
        let keep_row_nums = context
            .read_rows_for_built_query(&keep_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        delete_current_rows_outside_keep_set(
            db,
            &built_query.table,
            branch_num,
            &scope_row_nums,
            &keep_row_nums,
        )
    }

    fn apply_history_record(
        context: &mut ApplyHistoryContext<'_>,
        record: &HistoryRecord,
    ) -> Result<()> {
        let table = context.schema.table_def(&record.table)?;
        let row_num = cached_ensure_row_id_with_status(
            context.db,
            context.row_nums_by_id,
            context.row_nums_created_in_apply,
            &record.table,
            &record.row_id,
        )?;
        if row_id_used_by_other_table(context.db, context.schema, &record.table, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                record.row_id
            )));
        }
        let tx_num = context
            .tx_nums_by_id
            .get(&record.tx_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx::tx_num(context.db, &record.tx_id))?;
        let branch_num = context
            .branch_nums_by_id
            .get(&record.branch_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| branch::ensure(context.db, &record.branch_id, None, now_ms()))?;
        let tx_info = context
            .tx_info_by_num
            .get(&tx_num)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx_apply_info(context.db, tx_num))?;
        let outcome = tx_info.outcome;
        let history_exists = history_record_exists(context.db, &record.table, row_num, tx_num)?;
        if history_exists
            && current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
                .is_some_and(|current_tx_num| current_tx_num == tx_num)
        {
            return Ok(());
        }

        let mut columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(branch_num),
            rusqlite::types::Value::Integer(record.op),
        ];
        for field in &table.fields {
            let value = record
                .values
                .get(&field.name)
                .or_else(|| record.values.get(&field.storage_name))
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
                field,
            )));
            values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| {
                    cached_ensure_row_id(context.db, context.row_nums_by_id, ref_table, row_id)
                },
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let created_by_num =
            cached_ensure_user(context.db, context.user_nums_by_id, &record.created_by)?;
        let updated_by_num =
            cached_ensure_user(context.db, context.user_nums_by_id, &record.updated_by)?;
        values.extend([
            rusqlite::types::Value::Integer(record.created_at),
            rusqlite::types::Value::Integer(record.updated_at),
            rusqlite::types::Value::Integer(created_by_num),
            rusqlite::types::Value::Integer(updated_by_num),
        ]);
        if !history_exists {
            insert_dynamic(
                context.db,
                &crate::schema::history_table(&record.table),
                &columns,
                &values,
            )?;
        }
        let table_num = context
            .table_nums_by_name
            .get(&record.table)
            .copied()
            .ok_or_else(|| crate::Error::new("history record references missing table"))?;
        if !history_exists {
            record_tx_write_num(context.db, tx_num, table_num, row_num, record.op)?;
        }
        if outcome == tx::OUTCOME_PENDING && tx_info.conflict_mode == tx::MODE_EXCLUSIVE {
            return Ok(());
        }
        if tx_info.outcome == tx::OUTCOME_PENDING
            && tx_info.node_num != context.local_node_num
            && durable_version_exists_for_row(context.db, &record.table, row_num, branch_num)?
        {
            return Ok(());
        }
        if outcome != tx::OUTCOME_REJECTED {
            if let Some(current_tx_num) =
                current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
            {
                if let Some(is_newer) =
                    tx_is_newer_than_current_fast_path(context.db, tx_num, current_tx_num)?
                {
                    if !is_newer {
                        return Ok(());
                    }
                } else if !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )? {
                    return Ok(());
                }
            } else if !context.row_nums_created_in_apply.contains(&row_num)
                && !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )?
            {
                return Ok(());
            }
        }
        if outcome != tx::OUTCOME_REJECTED && record.op == history_op::DELETE {
            context.db.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                    crate::schema::current_table(&record.table)
                ),
                params![row_num, branch_num],
            )?;
            if branch_num != 1 {
                let mut current_columns = vec![
                    "row_num".to_owned(),
                    "j_branch_num".to_owned(),
                    "visible_tx_num".to_owned(),
                    "is_deleted".to_owned(),
                ];
                let mut current_values = vec![
                    rusqlite::types::Value::Integer(row_num),
                    rusqlite::types::Value::Integer(branch_num),
                    rusqlite::types::Value::Integer(tx_num),
                    rusqlite::types::Value::Integer(1),
                ];
                current_columns.extend(columns.iter().skip(4).cloned());
                current_values.extend(values.iter().skip(4).cloned());
                insert_dynamic(
                    context.db,
                    &crate::schema::current_table(&record.table),
                    &current_columns,
                    &current_values,
                )?;
            }
        } else if outcome != tx::OUTCOME_REJECTED {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(4).cloned());
            current_values.extend(values.iter().skip(4).cloned());
            insert_dynamic(
                context.db,
                &crate::schema::current_table(&record.table),
                &current_columns,
                &current_values,
            )?;
        }
        Ok(())
    }

    pub fn reject_transaction(&mut self, tx_id: &str, code: &str) -> Result<()> {
        self.reject_transaction_with_optional_detail(tx_id, code, None)
    }

    pub fn reject_transaction_with_detail(
        &mut self,
        tx_id: &str,
        code: &str,
        detail: JsonValue,
    ) -> Result<()> {
        self.reject_transaction_with_optional_detail(tx_id, code, Some(detail))
    }

    fn reject_transaction_with_optional_detail(
        &mut self,
        tx_id: &str,
        code: &str,
        detail: Option<JsonValue>,
    ) -> Result<()> {
        let detail_json = encode_optional_json(detail.as_ref())?;
        let db = self.conn.transaction()?;
        let tx_num = tx::reject_with_detail_json(&db, tx_id, code, &detail_json)?;
        clear_transaction_awaiting_dependency(&db, tx_num)?;
        for table in self.schema.tables() {
            db.execute(
                &format!(
                    "DELETE FROM {} WHERE visible_tx_num = ?",
                    crate::schema::current_table(&table.name)
                ),
                params![tx_num],
            )?;
        }
        db.commit()?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn accept_transaction_at_global(&mut self, tx_id: &str, global_epoch: i64) -> Result<()> {
        let tx_num = tx::accept_global(&self.conn, tx_id, global_epoch)?;
        clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn accept_transaction_at_edge(&mut self, tx_id: &str) -> Result<()> {
        let tx_num = tx::accept_edge(&self.conn, tx_id, now_ms())?;
        clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    pub fn transaction_info(&self, tx_id: &str) -> Result<TransactionInfo> {
        let (tx_id, global_epoch, conflict_mode) = self.conn.query_row(
            "SELECT tx_id, global_epoch, conflict_mode FROM jazz_tx WHERE tx_id = ?",
            params![tx_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    conflict_mode_name(row.get::<_, i64>(2)?),
                ))
            },
        )?;
        let mut stmt = self.conn.prepare(
            "SELECT tier FROM jazz_tx_receipt receipt
             JOIN jazz_tx tx ON tx.tx_num = receipt.tx_num
             WHERE tx.tx_id = ?
             ORDER BY tier",
        )?;
        let receipt_tiers = stmt
            .query_map(params![tx_id], |row| tier_name(row.get::<_, i64>(0)?))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let rejection = self
            .conn
            .query_row(
                "SELECT rejection.code, rejection.detail_json
                 FROM jazz_tx_rejection rejection
                 JOIN jazz_tx tx ON tx.tx_num = rejection.tx_num
                 WHERE tx.tx_id = ?",
                params![tx_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (rejection_code, rejection_detail) = match rejection {
            Some((code, detail_json)) => {
                let detail = parse_rejection_detail(&detail_json)?;
                (Some(code), detail)
            }
            None => (None, None),
        };
        let awaiting_dependency = self
            .conn
            .query_row(
                "SELECT awaiting.detail_json
                 FROM jazz_tx_awaiting_dependency awaiting
                 JOIN jazz_tx tx ON tx.tx_num = awaiting.tx_num
                 WHERE tx.tx_id = ?",
                params![tx_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|detail_json| parse_rejection_detail(&detail_json))
            .transpose()?
            .flatten();
        Ok(TransactionInfo {
            tx_id,
            global_epoch,
            conflict_mode,
            receipt_tiers,
            awaiting_dependency,
            rejection_code,
            rejection_detail,
        })
    }

    pub fn rejected_transactions(&self) -> Result<Vec<RejectionInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT tx.tx_id, rejection.code, rejection.detail_json
             FROM jazz_tx_rejection rejection
             JOIN jazz_tx tx ON tx.tx_num = rejection.tx_num
             ORDER BY tx.tx_num",
        )?;
        let rows = stmt.query_map([], |row| {
            let detail_json = row.get::<_, String>(2)?;
            Ok(RejectionInfo {
                tx_id: row.get(0)?,
                code: row.get(1)?,
                detail: parse_rejection_detail_for_sqlite(&detail_json, 2)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn transaction_physical_num_for(&self, tx_id: &str) -> Result<i64> {
        tx::tx_num(&self.conn, tx_id)
    }

    pub fn transaction_write_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id
             FROM jazz_tx_write writes
             JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
             JOIN jazz_table tables ON tables.table_num = writes.table_num
             JOIN jazz_row_id ids ON ids.row_num = writes.row_num
             WHERE tx.tx_id = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn transaction_policy_read_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        self.transaction_read_rows_for_reason(tx_id, 1)
    }

    pub fn transaction_previous_read_rows(&self, tx_id: &str) -> Result<Vec<(String, String)>> {
        self.transaction_read_rows_for_reason(tx_id, 2)
    }

    pub fn transaction_observed_read_rows(
        &self,
        tx_id: &str,
    ) -> Result<Vec<(String, String, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id, observed.tx_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_table tables ON tables.table_num = reads.table_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
             WHERE tx.tx_id = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn transaction_read_rows_for_reason(
        &self,
        tx_id: &str,
        reason: i64,
    ) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT tables.table_name, ids.row_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_table tables ON tables.table_num = reads.table_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             WHERE tx.tx_id = ?
               AND reads.reason = ?
             ORDER BY tables.table_name, ids.row_id",
        )?;
        let rows = stmt.query_map(params![tx_id, reason], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn create_branch(&mut self, branch_id: &str, base_global_epoch: Option<i64>) -> Result<()> {
        branch::ensure(&self.conn, branch_id, base_global_epoch, now_ms())?;
        Ok(())
    }

    pub fn create_branch_from_branches(
        &mut self,
        branch_id: &str,
        source_branch_ids: &[&str],
    ) -> Result<()> {
        self.create_branch_from_branches_at_base(branch_id, None, source_branch_ids)
    }

    pub fn create_branch_from_branches_at_base(
        &mut self,
        branch_id: &str,
        base_global_epoch: Option<i64>,
        source_branch_ids: &[&str],
    ) -> Result<()> {
        let branch_num = branch::ensure(&self.conn, branch_id, base_global_epoch, now_ms())?;
        for source_branch_id in source_branch_ids {
            branch::add_source(&self.conn, branch_num, source_branch_id)?;
        }
        Ok(())
    }

    pub fn add_branch_source(&mut self, branch_id: &str, source_branch_id: &str) -> Result<()> {
        let branch_num = branch::checkout(&self.conn, branch_id)?;
        branch::add_source(&self.conn, branch_num, source_branch_id)
    }

    pub fn remove_branch_source(&mut self, branch_id: &str, source_branch_id: &str) -> Result<()> {
        let branch_num = branch::checkout(&self.conn, branch_id)?;
        branch::remove_source(&self.conn, branch_num, source_branch_id)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)
    }

    pub fn checkout_branch(&mut self, branch_id: &str) -> Result<()> {
        self.branch_num = branch::checkout(&self.conn, branch_id)?;
        Ok(())
    }

    pub fn branches(&self) -> Result<Vec<BranchInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_num, branch_id, base_global_epoch
             FROM jazz_branch
             ORDER BY branch_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        let mut branches = Vec::new();
        for row in rows {
            let (branch_num, id, base_global_epoch) = row?;
            let mut source_stmt = self.conn.prepare(
                "SELECT source.branch_id
                 FROM jazz_branch_source branch_source
                 JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
                 WHERE branch_source.branch_num = ?
                 ORDER BY source.branch_id",
            )?;
            let source_branch_ids = source_stmt
                .query_map(params![branch_num], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            branches.push(BranchInfo {
                id,
                base_global_epoch,
                source_branch_ids,
            });
        }
        Ok(branches)
    }

    pub fn branch_backing_rows(&self) -> Result<Vec<BranchInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT branch_id, base_global_epoch, source_branch_ids_json
             FROM jazz_branch_backing
             ORDER BY branch_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut branches = Vec::new();
        for row in rows {
            let (id, base_global_epoch, source_branch_ids_json) = row?;
            let source_branch_ids = serde_json::from_str::<Vec<String>>(&source_branch_ids_json)
                .map_err(|err| crate::Error::new(err.to_string()))?;
            branches.push(BranchInfo {
                id,
                base_global_epoch,
                source_branch_ids,
            });
        }
        Ok(branches)
    }

    pub fn session_user_for_test(&mut self, user: &str) {
        match &mut self.auth {
            RuntimeAuth::Client(User(current)) => *current = user.to_owned(),
            RuntimeAuth::TrustedPeer { session } => {
                *session = TrustedSession::AsUser(User(user.to_owned()));
            }
        }
    }

    pub fn transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            runtime: self,
            mutations: Vec::new(),
            mode: TransactionMode::Mergeable,
        }
    }

    pub fn delete_row(&mut self, table_name: &str, id: &str) -> Result<String> {
        let table = self.schema.table_def(table_name)?.clone();
        let visible_row = self
            .read_rows(table_name)?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))?;
        let user = self.attribution_user().to_owned();
        let bypass_policy = self.bypasses_policy();
        let db = self.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx(&db, self.node_num, &self.node_id, now)?;
        let row_num = row_num(&db, id)?;
        if let Some(policy) = table.effective_delete_using() {
            record_policy_read_set_for_write(
                &db,
                &self.schema,
                &table,
                policy,
                &visible_row.values,
                self.branch_num,
                tx_num,
            )?;
        }
        let allowed = bypass_policy
            || local_write_allowed(LocalWriteCheck {
                db: &db,
                schema: &self.schema,
                table: &table,
                row_num,
                branch_num: self.branch_num,
                old_values: Some(&visible_row.values),
                new_values: &visible_row.values,
                user: &user,
                op: history_op::DELETE,
                created_by: Some(&visible_row.created_by),
            })?;

        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut insert_columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        insert_columns.extend(field_columns.iter().cloned());
        insert_columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let mut select_columns = vec![
            "row_num".to_owned(),
            "?".to_owned(),
            "j_branch_num".to_owned(),
            history_op::DELETE.to_string(),
        ];
        select_columns.extend(field_columns.iter().cloned());
        select_columns.extend([
            "j_created_at".to_owned(),
            "?".to_owned(),
            "j_created_by".to_owned(),
            "?".to_owned(),
        ]);
        let user_num = users::ensure_user(&db, &user)?;
        let inserted = db.execute(
            &format!(
                "INSERT OR IGNORE INTO {} ({})
                 SELECT {}
                 FROM {}
                 WHERE row_num = ? AND j_branch_num = ?",
                crate::schema::history_table(&table.name),
                insert_columns.join(", "),
                select_columns.join(", "),
                crate::schema::current_table(&table.name),
            ),
            params![tx_num, now, user_num, row_num, self.branch_num],
        )?;
        if inserted == 0 {
            let mut values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(self.branch_num),
                rusqlite::types::Value::Integer(history_op::DELETE),
            ];
            for field in &table.fields {
                let value = visible_row
                    .values
                    .get(&field.name)
                    .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
                values.push(crate::schema::field_sql_value(
                    field,
                    value,
                    |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                )?);
            }
            values.extend([
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(user_num),
                rusqlite::types::Value::Integer(user_num),
            ]);
            insert_dynamic(
                &db,
                &crate::schema::history_table(&table.name),
                &insert_columns,
                &values,
            )?;
        }
        db.execute(
            &format!(
                "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                crate::schema::current_table(&table.name)
            ),
            params![row_num, self.branch_num],
        )?;
        if self.branch_num != 1 {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            current_columns.extend(field_columns.iter().cloned());
            current_columns.extend([
                "j_created_at".to_owned(),
                "j_updated_at".to_owned(),
                "j_created_by".to_owned(),
                "j_updated_by".to_owned(),
            ]);
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(self.branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(1),
            ];
            for field in &table.fields {
                let value = visible_row
                    .values
                    .get(&field.name)
                    .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
                current_values.push(crate::schema::field_sql_value(
                    field,
                    value,
                    |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                )?);
            }
            current_values.extend([
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(now),
                rusqlite::types::Value::Integer(user_num),
                rusqlite::types::Value::Integer(user_num),
            ]);
            insert_dynamic(
                &db,
                &crate::schema::current_table(&table.name),
                &current_columns,
                &current_values,
            )?;
        }
        record_tx_write(&db, tx_num, &table.name, row_num, history_op::DELETE)?;
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            projection::rebuild(&db, &self.schema, self.node_num)?;
        }
        db.commit()?;
        Ok(tx_id)
    }

    pub fn restore_deleted_row(&mut self, table_name: &str, id: &str) -> Result<String> {
        let table = self.schema.table_def(table_name)?;
        let row_num = row_num(&self.conn, id)?;
        let field_columns = table
            .fields
            .iter()
            .map(|field| {
                format!(
                    "h.{}",
                    crate::schema::quote_ident(&crate::schema::storage_column(field))
                )
            })
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND h.op = {delete_op}
               AND tx.outcome != ?
             ORDER BY tx.global_epoch DESC NULLS LAST, h.tx_num DESC
             LIMIT 1",
            field_columns.join(", "),
            crate::schema::history_table(table_name),
            delete_op = history_op::DELETE,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query_map(
            params![row_num, self.branch_num, tx::OUTCOME_REJECTED],
            |row| {
                (0..table.fields.len())
                    .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )?;
        let row = rows
            .next()
            .transpose()?
            .ok_or_else(|| crate::Error::new(format!("row {id} has no deleted version")))?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                query::sql_value_to_json(&self.conn, field, &row[idx])?,
            );
        }
        drop(rows);
        drop(stmt);
        self.write_row(table_name, id, values, history_op::INSERT)
    }

    pub fn clear_current_projection_for_test(&mut self) -> Result<()> {
        projection::clear(&self.conn, &self.schema)
    }

    pub fn rebuild_current_projection(&mut self) -> Result<()> {
        projection::rebuild(&self.conn, &self.schema, self.node_num)
    }

    pub fn physical_row_num_for(&self, row_id: &str) -> Result<i64> {
        row_num(&self.conn, row_id)
    }

    pub fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        self.query_context().read_rows(table_name)
    }

    pub fn read_rows_require_ref(
        &self,
        table_name: &str,
        ref_field_name: &str,
    ) -> Result<Vec<RowView>> {
        let table = self.schema.table_def(table_name)?;
        let ref_field = table
            .fields
            .iter()
            .find(|field| field.name == ref_field_name)
            .ok_or_else(|| {
                crate::Error::new(format!(
                    "unknown field {ref_field_name} on table {table_name}"
                ))
            })?;
        let FieldKind::Ref {
            table: target_table,
        } = &ref_field.kind
        else {
            return Err(crate::Error::new(format!(
                "field {ref_field_name} on table {table_name} is not a ref"
            )));
        };
        let visible_targets = self
            .query_context()
            .read_rows(target_table)?
            .into_iter()
            .map(|row| row.id)
            .collect::<BTreeSet<_>>();
        Ok(self
            .query_context()
            .read_rows(table_name)?
            .into_iter()
            .filter(|row| {
                row.values
                    .get(ref_field_name)
                    .and_then(JsonValue::as_str)
                    .is_some_and(|id| visible_targets.contains(id))
            })
            .collect())
    }

    pub(crate) fn read_rows_for_built_query(&self, query: &BuiltQuery) -> Result<Vec<RowView>> {
        self.query_context().read_rows_for_built_query(query)
    }

    pub fn read_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_eq(table_name, field_name, value)
    }

    pub(crate) fn debug_sql_for_built_query(
        &self,
        query: &BuiltQuery,
    ) -> Result<query::SqliteQueryDebug> {
        self.query_context().debug_sql_for_built_query(query)
    }

    pub(crate) fn explain_built_query(&self, query: &BuiltQuery) -> Result<query::SqliteQueryPlan> {
        self.query_context().explain_built_query(query)
    }

    pub fn read_rows_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_contains(table_name, field_name, needle)
    }

    pub fn read_rows_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_in(table_name, field_name, values)
    }

    pub fn read_rows_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_ne(table_name, field_name, value)
    }

    pub fn export_query_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        let rows = self.query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value.clone(),
        ))?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value,
            rows,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_eq_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value.clone(),
        ))?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value,
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    pub fn export_query_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "contains",
            JsonValue::String(needle.to_owned()),
            self.read_rows_where_contains(table_name, field_name, needle)?,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "in",
            JsonValue::Array(values.clone()),
            self.read_rows_where_in(table_name, field_name, values)?,
            QueryScopeOptions::empty(),
        )
    }

    pub fn export_query_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "ne",
            value.clone(),
            self.read_rows_where_ne(table_name, field_name, value)?,
            QueryScopeOptions::empty(),
        )
    }

    fn export_many_predicate_query_refreshes(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        values: Vec<(JsonValue, Vec<String>)>,
    ) -> Result<Bundle> {
        let mut items = Vec::new();
        for (value, extra_row_ids) in values {
            let rows = match op {
                "eq" => self.read_rows_where_eq(table_name, field_name, value.clone())?,
                "ne" => self.read_rows_where_ne(table_name, field_name, value.clone())?,
                "contains" => {
                    let Some(needle) = value.as_str() else {
                        return Err(crate::Error::new("contains expects a string value"));
                    };
                    self.read_rows_where_contains(table_name, field_name, needle)?
                }
                "in" => {
                    let Some(values) = value.as_array() else {
                        return Err(crate::Error::new("in predicate expects an array value"));
                    };
                    self.read_rows_where_in(table_name, field_name, values.clone())?
                }
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported batched predicate refresh {op}"
                    )));
                }
            };
            items.push(BatchedQueryScopeItem {
                op: op.to_owned(),
                value,
                rows,
                extra_row_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    pub fn export_query_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query_where_eq_top_created_at_desc_with_previous_observed(
            table_name,
            field_name,
            value,
            limit,
            Vec::new(),
        )
    }

    fn export_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub fn export_query_where_eq_top_created_at_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    fn export_many_query_where_eq_top_created_at_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        limit: usize,
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_created_at_desc(
                table_name,
                field_name,
                &value_only,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_created_at_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, &[])
    }

    pub fn export_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query_where_eq_top_field_desc_with_previous_observed(
            table_name,
            field_name,
            value,
            order_field_name,
            limit,
            Vec::new(),
        )
    }

    pub fn export_query_where_eq_top_field_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_field_desc",
            json!({
                "eq": value.clone(),
                "order_field": order_field_name,
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[ref_field_name],
                extra_row_ids: &[],
            },
        )
    }

    pub fn export_many_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values
                .into_iter()
                .map(|value| (value, Vec::new()))
                .collect(),
            order_field_name,
            limit,
            &[],
        )
    }

    pub fn export_many_query_where_eq_top_field_desc_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
        order_field_name: &str,
        limit: usize,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values
                .into_iter()
                .map(|value| (value, Vec::new()))
                .collect(),
            order_field_name,
            limit,
            &[ref_field_name],
        )
    }

    fn export_many_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_many_query_where_eq_top_field_desc_inner(
            table_name,
            field_name,
            values,
            order_field_name,
            limit,
            &[],
        )
    }

    fn export_many_query_where_eq_top_field_desc_inner(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<(JsonValue, Vec<String>)>,
        order_field_name: &str,
        limit: usize,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let value_only = values
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let rows_by_value = self
            .query_context()
            .read_many_rows_where_eq_top_field_desc(
                table_name,
                field_name,
                &value_only,
                order_field_name,
                limit,
            )?;
        let mut items = Vec::new();
        for ((value, previous_observed_ids), rows) in values.into_iter().zip(rows_by_value) {
            items.push(BatchedQueryScopeItem {
                op: "eq_top_field_desc".to_owned(),
                value: json!({
                    "eq": value.clone(),
                    "order_field": order_field_name,
                    "limit": limit,
                    "observed_ids": observed_row_ids(&rows),
                }),
                rows,
                extra_row_ids: previous_observed_ids,
            });
        }
        self.export_batched_query_scopes(table_name, field_name, items, ref_include_fields)
    }

    pub(crate) fn export_built_query_scope(
        &self,
        query: BuiltQuery,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
            table: query.table.clone(),
            field: "$query".to_owned(),
            op: "query".to_owned(),
            value: query.to_json_value(),
        };
        let support_query = built_query_repair_keep_query(&query)?;
        if let Some(row_scope) = self
            .query_context()
            .lower_built_query_row_scope(&support_query)?
        {
            return self.export_built_query_read_scope_sql(
                query_read,
                &query,
                &row_scope,
                rows,
                QueryScopeOptions {
                    ref_include_fields,
                    extra_row_ids: &[],
                },
            );
        }
        self.export_query_read_scope(
            query_read,
            rows,
            QueryScopeOptions {
                ref_include_fields,
                extra_row_ids: &[],
            },
        )
    }

    fn export_query_read_scope(
        &self,
        query_read: QueryReadRecord,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        // Query-scope exports carry more than the rows currently visible in the
        // query result. They also carry repair candidates: rows whose history
        // previously satisfied the same query scope and may need to be removed
        // or updated on a receiver.
        //
        //   +---------------------+
        //   | query result rows   |
        //   +---------------------+
        //             |
        //             v
        //   +---------------------+      +---------------------+
        //   | result row nums     | ---> | exported history    |
        //   +---------------------+      +---------------------+
        //             ^
        //             |
        //   +---------------------+
        //   | repair row nums     |
        //   +---------------------+
        //
        // Without the repair rows, query-scoped sync would only add or update
        // rows that are still in the result. A receiver would not learn that a
        // previously synced row left the predicate or page boundary.
        let table_name = &query_read.table;
        let table = self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let mut repair_row_nums = Vec::new();
        for row_id in options.extra_row_ids {
            repair_row_nums.push(row_num(&self.conn, row_id)?);
        }
        repair_row_nums.extend(query_scope_repair_row_nums_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?);
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in options.ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let rejected_tx_ids = query_scope_rejected_tx_ids_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![query_read];
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    fn export_built_query_read_scope_sql(
        &self,
        query_read: QueryReadRecord,
        built_query: &BuiltQuery,
        visible_scope: &query::LoweredQueryRowScope,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        let table_name = &query_read.table;
        let table = self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let mut repair_row_nums = Vec::new();
        for row_id in options.extra_row_ids {
            repair_row_nums.push(row_num(&self.conn, row_id)?);
        }
        repair_row_nums.extend(query_scope_repair_row_nums_for_read(
            &self.conn,
            &self.schema,
            table,
            &query_read,
            self.branch_num,
            user,
            bypass_policy,
        )?);
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();

        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut history = export_history_versions_for_query_scope_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            visible_scope,
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history_for_query_scope(
            &visibility,
            PolicyDependencyQueryScopeExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_scope: visible_scope,
            },
        )?);
        if !repair_row_nums.is_empty() {
            history.extend(export_policy_dependency_history(
                &visibility,
                PolicyDependencyExport {
                    table_name,
                    policy: &table.read_policy,
                    branch_nums: &branch_nums,
                    child_row_nums: Some(&repair_row_nums),
                },
            )?);
        }
        for ref_field_name in options.ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_query_scope_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    visible_scope,
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history_for_query_scope(
                    &visibility,
                    table_name,
                    base_epoch,
                    visible_scope,
                )?);
                if !repair_row_nums.is_empty() {
                    history.extend(export_history_versions_for_rows_in_branches(
                        &self.conn,
                        &self.schema,
                        table_name,
                        Some(&repair_row_nums),
                        Some(base_epoch),
                        &[1],
                    )?);
                    history.extend(export_snapshot_policy_dependency_history(
                        &visibility,
                        table_name,
                        base_epoch,
                        Some(&repair_row_nums),
                    )?);
                }
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let rejected_tx_ids = query_scope_rejected_tx_ids_for_built_query(
            &self.conn,
            &self.schema,
            table,
            built_query,
            self.branch_num,
            user,
            bypass_policy,
        )?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            vec![query_read],
            history,
        ))
    }

    fn export_query_where_eq_top_field_desc_with_previous_observed(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
        previous_observed_ids: Vec<String>,
    ) -> Result<Bundle> {
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_field_desc",
            json!({
                "eq": value.clone(),
                "order_field": order_field_name,
                "limit": limit,
                "observed_ids": observed_row_ids(&rows),
            }),
            rows,
            QueryScopeOptions {
                ref_include_fields: &[],
                extra_row_ids: &previous_observed_ids,
            },
        )
    }

    pub fn profile_export_query_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<(Bundle, QueryExportProfile)> {
        let total_started = ProfileTimer::start();
        let read_started = ProfileTimer::start();
        let rows = self.read_rows_where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
        )?;
        let read_rows_ms = read_started.elapsed_ms();

        let table = self.schema.table_def(table_name)?;

        let resolve_started = ProfileTimer::start();
        let visible_row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let resolve_visible_row_nums_ms = resolve_started.elapsed_ms();

        let repair_started = ProfileTimer::start();
        let query_value = json!({
            "eq": value.clone(),
            "order_field": order_field_name,
            "limit": limit,
            "observed_ids": observed_row_ids(&rows),
        });
        let mut repair_row_nums = query_scope_repair_row_nums(
            &self.conn,
            table,
            field_name,
            "eq_top_field_desc",
            &query_value,
        )?;
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let repair_row_nums_ms = repair_started.elapsed_ms();

        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();

        let visible_history_started = ProfileTimer::start();
        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        let visible_history_ms = visible_history_started.elapsed_ms();

        let repair_visible_started = ProfileTimer::start();
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
        }
        let repair_visible_history_ms = repair_visible_started.elapsed_ms();

        let repair_all_started = ProfileTimer::start();
        if !repair_row_nums.is_empty() {
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        let repair_all_history_ms = repair_all_started.elapsed_ms();

        let policy_started = ProfileTimer::start();
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        let policy_dependency_history_ms = policy_started.elapsed_ms();

        let snapshot_started = ProfileTimer::start();
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        let branch_snapshot_history_ms = snapshot_started.elapsed_ms();

        let dedupe_started = ProfileTimer::start();
        dedupe_history_records(&mut history);
        let dedupe_history_ms = dedupe_started.elapsed_ms();

        let reads_started = ProfileTimer::start();
        let reads = export_reads_for_history(&self.conn, &history)?;
        let reads_ms = reads_started.elapsed_ms();

        let rejected_started = ProfileTimer::start();
        let rejected_tx_ids = query_scope_rejected_tx_ids(
            &self.conn,
            table,
            field_name,
            "eq_top_field_desc",
            &query_value,
        )?;
        let rejected_tx_ids_ms = rejected_started.elapsed_ms();

        let txs_started = ProfileTimer::start();
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let txs_ms = txs_started.elapsed_ms();

        let branches_started = ProfileTimer::start();
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let branches_ms = branches_started.elapsed_ms();

        let make_started = ProfileTimer::start();
        let query_reads = vec![QueryReadRecord {
            branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: "eq_top_field_desc".to_owned(),
            value: query_value,
        }];
        let bundle = make_bundle(&self.schema, branches, txs, reads, query_reads, history);
        let make_bundle_ms = make_started.elapsed_ms();

        let profile = QueryExportProfile {
            total_ms: total_started.elapsed_ms(),
            read_rows_ms,
            resolve_visible_row_nums_ms,
            repair_row_nums_ms,
            visible_history_ms,
            repair_visible_history_ms,
            repair_all_history_ms,
            policy_dependency_history_ms,
            branch_snapshot_history_ms,
            dedupe_history_ms,
            reads_ms,
            rejected_tx_ids_ms,
            txs_ms,
            branches_ms,
            make_bundle_ms,
            history_rows: bundle.history.len(),
            read_rows: bundle.reads.len(),
            tx_rows: bundle.txs.len(),
            branch_rows: bundle.branches.len(),
        };
        Ok((bundle, profile))
    }

    pub(crate) fn export_query_scope(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        value: JsonValue,
        rows: Vec<RowView>,
        options: QueryScopeOptions<'_>,
    ) -> Result<Bundle> {
        let query_read = QueryReadRecord {
            branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: op.to_owned(),
            value,
        };
        self.export_query_read_scope(query_read, rows, options)
    }

    fn export_batched_query_scopes(
        &self,
        table_name: &str,
        field_name: &str,
        items: Vec<BatchedQueryScopeItem>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let table = self.schema.table_def(table_name)?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let visibility = self.read_visibility();
        let mut all_rows = Vec::new();
        let mut visible_row_nums = Vec::new();
        let mut repair_row_nums = Vec::new();
        let mut rejected_tx_ids = Vec::new();
        let mut query_reads = Vec::new();

        for item in items {
            let row_nums = item
                .rows
                .iter()
                .map(|row| row_num(&self.conn, &row.id))
                .collect::<Result<Vec<_>>>()?;
            for row_id in &item.extra_row_ids {
                repair_row_nums.push(row_num(&self.conn, row_id)?);
            }
            repair_row_nums.extend(query_scope_repair_row_nums(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            rejected_tx_ids.extend(query_scope_rejected_tx_ids(
                &self.conn,
                table,
                field_name,
                &item.op,
                &item.value,
            )?);
            query_reads.push(QueryReadRecord {
                branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
                table: table_name.to_owned(),
                field: field_name.to_owned(),
                op: item.op,
                value: item.value,
            });
            visible_row_nums.extend(row_nums);
            all_rows.extend(item.rows);
        }

        visible_row_nums.sort();
        visible_row_nums.dedup();
        let visible_row_num_set = visible_row_nums.iter().copied().collect::<BTreeSet<_>>();
        repair_row_nums.retain(|row_num| !visible_row_num_set.contains(row_num));
        repair_row_nums.sort();
        repair_row_nums.dedup();
        let mut row_nums = visible_row_nums.clone();
        row_nums.extend(repair_row_nums.iter());
        row_nums.sort();
        row_nums.dedup();
        rejected_tx_ids.sort();
        rejected_tx_ids.dedup();

        let mut history = export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            table_name,
            Some(&visible_row_nums),
            None,
            &branch_nums,
        )?;
        if !repair_row_nums.is_empty() {
            history.extend(export_visible_table_history(
                &visibility,
                table_name,
                &branch_nums,
                Some(&repair_row_nums),
            )?);
            history.extend(export_history_versions_for_rows_in_branches(
                &self.conn,
                &self.schema,
                table_name,
                Some(&repair_row_nums),
                None,
                &branch_nums,
            )?);
        }
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy: &table.read_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &all_rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
        if self.branch_num != 1 {
            if let Some(base_epoch) = branch::base_global_epoch(&self.conn, self.branch_num)? {
                history.extend(export_history_versions_for_rows_in_branches(
                    &self.conn,
                    &self.schema,
                    table_name,
                    Some(&row_nums),
                    Some(base_epoch),
                    &[1],
                )?);
                history.extend(export_snapshot_policy_dependency_history(
                    &visibility,
                    table_name,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let txs =
            export_txs_for_query_scope(&self.conn, table_name, &history, &reads, &rejected_tx_ids)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        Ok(make_bundle(
            &self.schema,
            branches,
            txs,
            reads,
            query_reads,
            history,
        ))
    }

    fn export_ref_include_history(
        &self,
        table: &crate::schema::TableDef,
        rows: &[RowView],
        ref_field_name: &str,
        branch_nums: &[i64],
    ) -> Result<Vec<HistoryRecord>> {
        let field = table
            .fields
            .iter()
            .find(|field| field.name == ref_field_name)
            .ok_or_else(|| crate::Error::new(format!("unknown include field {ref_field_name}")))?;
        let FieldKind::Ref {
            table: ref_table_name,
        } = &field.kind
        else {
            return Err(crate::Error::new(format!(
                "include field {ref_field_name} is not a ref"
            )));
        };
        let ref_row_nums = rows
            .iter()
            .filter_map(|row| row.values.get(ref_field_name).and_then(JsonValue::as_str))
            .map(|id| row_num(&self.conn, id))
            .collect::<Result<Vec<_>>>()?;
        let mut ref_row_nums = ref_row_nums;
        ref_row_nums.sort();
        ref_row_nums.dedup();
        if ref_row_nums.is_empty() {
            return Ok(Vec::new());
        }
        let visibility = self.read_visibility();
        let mut history = export_visible_table_history(
            &visibility,
            ref_table_name,
            branch_nums,
            Some(&ref_row_nums),
        )?;
        history.extend(export_history_versions_for_rows_in_branches(
            &self.conn,
            &self.schema,
            ref_table_name,
            Some(&ref_row_nums),
            None,
            branch_nums,
        )?);
        history.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name: ref_table_name,
                policy: &self.schema.table_def(ref_table_name)?.read_policy,
                branch_nums,
                child_row_nums: Some(&ref_row_nums),
            },
        )?);
        Ok(history)
    }

    pub fn read_recursive_refs(
        &self,
        table_name: &str,
        root_id: &str,
        parent_field: &str,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_recursive_refs(table_name, root_id, parent_field)
    }

    pub fn subscribe_rows(&self, table_name: &str) -> Result<RowsSubscription> {
        Ok(RowsSubscription::new(
            table_name,
            self.read_rows(table_name)?,
        ))
    }

    pub fn subscribe_rejections(&self) -> Result<RejectionSubscription> {
        Ok(RejectionSubscription::new(self.rejected_transactions()?))
    }

    pub fn subscribe_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<RowsSubscription> {
        let rows = self.query(predicate_query(
            table_name,
            field_name,
            QueryConditionOp::Eq,
            value.clone(),
        ))?;
        Ok(RowsSubscription::where_eq(
            table_name, field_name, value, rows,
        ))
    }

    pub fn subscribe_rows_where_contains(
        &self,
        table_name: &str,
        field_name: &str,
        needle: &str,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::where_contains(
            table_name,
            field_name,
            needle,
            self.read_rows_where_contains(table_name, field_name, needle)?,
        ))
    }

    pub fn subscribe_rows_where_in(
        &self,
        table_name: &str,
        field_name: &str,
        values: Vec<JsonValue>,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::where_in(
            table_name,
            field_name,
            values.clone(),
            self.read_rows_where_in(table_name, field_name, values)?,
        ))
    }

    pub fn subscribe_rows_where_ne(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::where_ne(
            table_name,
            field_name,
            value.clone(),
            self.read_rows_where_ne(table_name, field_name, value)?,
        ))
    }

    pub fn subscribe_rows_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::where_eq_top_created_at_desc(
            table_name,
            field_name,
            value.clone(),
            limit,
            self.read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)?,
        ))
    }

    pub fn subscribe_rows_where_eq_top_field_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        order_field_name: &str,
        limit: usize,
    ) -> Result<RowsSubscription> {
        Ok(RowsSubscription::where_eq_top_field_desc(
            table_name,
            field_name,
            value.clone(),
            order_field_name,
            limit,
            self.read_rows_where_eq_top_field_desc(
                table_name,
                field_name,
                value,
                order_field_name,
                limit,
            )?,
        ))
    }

    pub fn subscribe_observed_query(&self, read: &QueryReadRecord) -> Result<RowsSubscription> {
        if read.branch_id != branch_id_for_num(&self.conn, self.branch_num)? {
            return Err(crate::Error::new(
                "observed query branch is not checked out",
            ));
        }
        match read.op.as_str() {
            "eq" => self.subscribe_rows_where_eq(&read.table, &read.field, read.value.clone()),
            "ne" => self.subscribe_rows_where_ne(&read.table, &read.field, read.value.clone()),
            "contains" => {
                let Some(needle) = read.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.subscribe_rows_where_contains(&read.table, &read.field, needle)
            }
            "in" => {
                let Some(values) = read.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.subscribe_rows_where_in(&read.table, &read.field, values.clone())
            }
            "recursive_refs" => {
                let Some(root_id) = read.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                Ok(RowsSubscription::where_recursive_refs(
                    &read.table,
                    root_id,
                    &read.field,
                    self.read_recursive_refs(&read.table, root_id, &read.field)?,
                ))
            }
            "eq_top_created_at_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
                self.subscribe_rows_where_eq_top_created_at_desc(
                    &read.table,
                    &read.field,
                    value.clone(),
                    limit as usize,
                )
            }
            "eq_top_field_desc" => {
                let value = read
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
                let order_field = read
                    .value
                    .get("order_field")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
                let limit = read
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
                self.subscribe_rows_where_eq_top_field_desc(
                    &read.table,
                    &read.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                )
            }
            "query" => self.subscribe_query(built_query_from_read(read)?),
            op => Err(crate::Error::new(format!(
                "unsupported observed subscription query {op}"
            ))),
        }
    }

    pub fn read_row_candidates(&self, table_name: &str, id: &str) -> Result<Vec<RowView>> {
        self.query_context().read_row_candidates(table_name, id)
    }

    pub fn read_rows_with_conflict_meta(&self, table_name: &str) -> Result<Vec<RowView>> {
        let mut rows = self.read_rows(table_name)?;
        if rows.is_empty() {
            let mut candidate_ids = self.conflict_candidate_row_ids(table_name)?;
            candidate_ids.sort();
            candidate_ids.dedup();
            for row_id in candidate_ids {
                let candidates = self.read_row_candidates(table_name, &row_id)?;
                if candidates.len() > 1 {
                    rows.extend(candidates);
                }
            }
        }
        for row in &mut rows {
            if self.row_has_current_branch_value(table_name, &row.id)? {
                continue;
            }
            let candidate_count = self.read_row_candidates(table_name, &row.id)?.len();
            if candidate_count > 1 {
                row.conflict_count = candidate_count;
            }
        }
        Ok(rows)
    }

    fn row_has_current_branch_value(&self, table_name: &str, id: &str) -> Result<bool> {
        self.schema.table_def(table_name)?;
        let Ok(row_num) = row_num(&self.conn, id) else {
            return Ok(false);
        };
        let count: i64 = self.conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {}
                 WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, self.branch_num],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn conflict_candidate_row_ids(&self, table_name: &str) -> Result<Vec<String>> {
        self.schema.table_def(table_name)?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT DISTINCT ids.row_id
             FROM jazz_branch_source source
             JOIN {} current ON current.j_branch_num = source.source_branch_num
             JOIN jazz_row_id ids ON ids.row_num = current.row_num
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE source.branch_num = ?
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY ids.row_id",
            crate::schema::current_table(table_name)
        ))?;
        let rows = stmt.query_map(params![self.branch_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, String>(0)
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn poll_subscription(
        &self,
        subscription: &mut RowsSubscription,
    ) -> Result<Vec<crate::types::RowDiff>> {
        let next_rows = match &subscription.query {
            RowsSubscriptionQuery::Table { table } => self.read_rows(table)?,
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq" => {
                self.query(predicate_query(
                    &query.table,
                    &query.field,
                    QueryConditionOp::Eq,
                    query.value.clone(),
                ))?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "ne" => {
                self.read_rows_where_ne(&query.table, &query.field, query.value.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "contains" => {
                let Some(needle) = query.value.as_str() else {
                    return Err(crate::Error::new("contains expects a string value"));
                };
                self.read_rows_where_contains(&query.table, &query.field, needle)?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "in" => {
                let Some(values) = query.value.as_array() else {
                    return Err(crate::Error::new("in predicate expects an array value"));
                };
                self.read_rows_where_in(&query.table, &query.field, values.clone())?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "recursive_refs" => {
                let Some(root_id) = query.value.as_str() else {
                    return Err(crate::Error::new("recursive refs expects root id string"));
                };
                self.read_recursive_refs(&query.table, root_id, &query.field)?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq_top_created_at_desc" => {
                let value = query
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
                let limit = query
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
                self.read_rows_where_eq_top_created_at_desc(
                    &query.table,
                    &query.field,
                    value.clone(),
                    limit as usize,
                )?
            }
            RowsSubscriptionQuery::Predicate(query) if query.op == "eq_top_field_desc" => {
                let value = query
                    .value
                    .get("eq")
                    .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
                let order_field = query
                    .value
                    .get("order_field")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
                let limit = query
                    .value
                    .get("limit")
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
                self.read_rows_where_eq_top_field_desc(
                    &query.table,
                    &query.field,
                    value.clone(),
                    order_field,
                    limit as usize,
                )?
            }
            RowsSubscriptionQuery::Predicate(query) => {
                return Err(crate::Error::new(format!(
                    "unsupported subscription query {}",
                    query.op
                )));
            }
            RowsSubscriptionQuery::Built(query) => self.query(query.clone())?,
        };
        Ok(subscription.replace_with_diff(next_rows))
    }

    pub fn poll_rejections(
        &self,
        subscription: &mut RejectionSubscription,
    ) -> Result<Vec<RejectionInfo>> {
        Ok(subscription.replace_with_new(self.rejected_transactions()?))
    }

    pub fn storage_stats(&self) -> Result<StorageStats> {
        stats::collect(&self.conn, &self.schema)
    }

    pub fn storage_format_version(&self) -> Result<i64> {
        storage::storage_version(&self.conn)
    }

    pub fn local_policy_fingerprint(&self) -> String {
        self.schema.policy_fingerprint()
    }

    pub fn local_schema_fingerprint(&self) -> String {
        self.schema.compatibility_fingerprint()
    }

    fn query_context(&self) -> query::QueryContext<'_> {
        query::QueryContext {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
        }
    }

    fn read_visibility(&self) -> ReadVisibility<'_> {
        ReadVisibility {
            conn: &self.conn,
            schema: &self.schema,
            branch_num: self.branch_num,
            user: self.policy_user(),
            bypass_policy: self.bypasses_policy(),
        }
    }
}

struct InsertRowInTx<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    values: &'a BTreeMap<String, JsonValue>,
    tx_num: i64,
    branch_num: i64,
    now: i64,
    user: &'a str,
    bypass_policy: bool,
    op: i64,
}

struct EffectiveWriteValues<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    row_num: i64,
    branch_num: i64,
    patch_values: &'a BTreeMap<String, JsonValue>,
    op: i64,
}

fn effective_write_values(args: EffectiveWriteValues<'_>) -> Result<BTreeMap<String, JsonValue>> {
    let table = args.schema.table_def(args.table_name)?;
    if args.op == history_op::INSERT {
        let mut values = args.patch_values.clone();
        for field in &table.fields {
            if !values.contains_key(&field.name) {
                if let Some(default_value) = &field.default_value {
                    values.insert(field.name.clone(), default_value.clone());
                }
            }
        }
        return Ok(values);
    }
    let mut current = effective::row_values(
        args.db,
        args.schema,
        args.table_name,
        args.row_num,
        args.branch_num,
    )?
    .ok_or_else(|| crate::Error::new(format!("row {} is not visible", args.id)))?;
    current.extend(args.patch_values.clone());
    Ok(current)
}

fn insert_row_in_tx(args: InsertRowInTx<'_>) -> Result<bool> {
    let table = args.schema.table_def(args.table_name)?;
    validate_write_fields(table, args.values)?;
    let (row_num, row_id_created) = ensure_row_id_with_status(args.db, args.id)?;
    if args.op == history_op::INSERT && !row_id_created {
        if row_id_used_by_other_table(args.db, args.schema, args.table_name, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                args.id
            )));
        }
        if row_has_current_branch_value(args.db, args.table_name, row_num, args.branch_num)? {
            return Err(crate::Error::new(format!(
                "row id {} already exists in table {}",
                args.id, args.table_name
            )));
        }
    }
    let effective_values = effective_write_values(EffectiveWriteValues {
        db: args.db,
        schema: args.schema,
        table_name: args.table_name,
        id: args.id,
        row_num,
        branch_num: args.branch_num,
        patch_values: args.values,
        op: args.op,
    })?;
    let previous_values = if args.op == history_op::UPDATE {
        Some(
            effective::row_values(
                args.db,
                args.schema,
                args.table_name,
                row_num,
                args.branch_num,
            )?
            .ok_or_else(|| crate::Error::new(format!("row {} is not visible", args.id)))?,
        )
    } else {
        None
    };
    if args.op == history_op::INSERT {
        if row_id_created {
            read_set::record_tx_absent_read(args.db, args.tx_num, args.table_name, row_num)?;
        } else {
            read_set::record_tx_create_read(
                args.db,
                args.tx_num,
                args.table_name,
                row_num,
                args.branch_num,
            )?;
        }
    } else {
        read_set::record_tx_read(
            args.db,
            args.tx_num,
            args.table_name,
            row_num,
            args.branch_num,
            2,
        )?;
    }
    record_policy_read_sets_for_write(
        args.db,
        args.schema,
        table,
        args.op,
        previous_values.as_ref(),
        &effective_values,
        args.branch_num,
        args.tx_num,
    )?;
    let allowed = args.bypass_policy
        || local_write_allowed(LocalWriteCheck {
            db: args.db,
            schema: args.schema,
            table,
            row_num,
            branch_num: args.branch_num,
            old_values: previous_values.as_ref(),
            new_values: &effective_values,
            user: args.user,
            op: args.op,
            created_by: (args.op == history_op::INSERT).then_some(args.user),
        })?;

    let mut columns = vec![
        "row_num".to_owned(),
        "tx_num".to_owned(),
        "j_branch_num".to_owned(),
        "op".to_owned(),
    ];
    let mut sql_values = vec![
        rusqlite::types::Value::Integer(row_num),
        rusqlite::types::Value::Integer(args.tx_num),
        rusqlite::types::Value::Integer(args.branch_num),
        rusqlite::types::Value::Integer(args.op),
    ];

    for field in &table.fields {
        let value = effective_values
            .get(&field.name)
            .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
        columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
            field,
        )));
        sql_values.push(crate::schema::field_sql_value(
            field,
            value,
            |ref_table, row_id| ensure_row_id(args.db, ref_table, row_id),
        )?);
    }
    columns.extend([
        "j_created_at".to_owned(),
        "j_updated_at".to_owned(),
        "j_created_by".to_owned(),
        "j_updated_by".to_owned(),
    ]);
    let (created_at, created_by) = if args.op == history_op::INSERT {
        (args.now, args.user.to_owned())
    } else {
        current_creation_metadata(args.db, &table.name, row_num, args.branch_num)?
            .unwrap_or((args.now, args.user.to_owned()))
    };
    let created_by_num = users::ensure_user(args.db, &created_by)?;
    let updated_by_num = users::ensure_user(args.db, args.user)?;
    sql_values.extend([
        rusqlite::types::Value::Integer(created_at),
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Integer(created_by_num),
        rusqlite::types::Value::Integer(updated_by_num),
    ]);
    insert_dynamic(
        args.db,
        &crate::schema::history_table(&table.name),
        &columns,
        &sql_values,
    )?;
    record_tx_write(args.db, args.tx_num, &table.name, row_num, args.op)?;

    if allowed {
        let mut current_columns = vec![
            "row_num".to_owned(),
            "j_branch_num".to_owned(),
            "visible_tx_num".to_owned(),
            "is_deleted".to_owned(),
        ];
        let mut current_values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(args.branch_num),
            rusqlite::types::Value::Integer(args.tx_num),
            rusqlite::types::Value::Integer(0),
        ];
        current_columns.extend(columns.iter().skip(4).cloned());
        current_values.extend(sql_values.iter().skip(4).cloned());
        insert_dynamic(
            args.db,
            &crate::schema::current_table(&table.name),
            &current_columns,
            &current_values,
        )?;
    }
    Ok(allowed)
}

fn row_has_current_branch_value(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn row_id_used_by_other_table(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    for table in schema.tables() {
        if table.name == table_name {
            continue;
        }
        let history_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::history_table(&table.name)
        );
        if conn
            .query_row(&history_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
        let current_sql = format!(
            "SELECT 1 FROM {} WHERE row_num = ? LIMIT 1",
            crate::schema::current_table(&table.name)
        );
        if conn
            .query_row(&current_sql, params![row_num], |_| Ok(()))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_write_fields(
    table: &crate::schema::TableDef,
    values: &BTreeMap<String, JsonValue>,
) -> Result<()> {
    let schema_fields = table
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for field_name in values.keys() {
        if !schema_fields.contains(field_name.as_str()) {
            return Err(crate::Error::new(format!(
                "unknown field {} on table {}",
                field_name, table.name
            )));
        }
    }
    Ok(())
}

struct LocalWriteCheck<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table: &'a crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
    old_values: Option<&'a BTreeMap<String, JsonValue>>,
    new_values: &'a BTreeMap<String, JsonValue>,
    user: &'a str,
    op: i64,
    created_by: Option<&'a str>,
}

fn local_write_allowed(check: LocalWriteCheck<'_>) -> Result<bool> {
    operation_policy_allowed_for_op(&check)
}

fn operation_policy_allowed_for_op(check: &LocalWriteCheck<'_>) -> Result<bool> {
    if check.op == history_op::DELETE {
        let Some(policy) = check.table.effective_delete_using() else {
            return Ok(true);
        };
        let Some(old_values) = check.old_values else {
            return Ok(false);
        };
        return policy_allowed_for_values(PolicyValueCheck {
            db: check.db,
            schema: check.schema,
            table: check.table,
            policy,
            row_num: check.row_num,
            branch_num: check.branch_num,
            values: old_values,
            user: check.user,
            created_by: check.created_by,
        });
    }
    operation_policy_allowed(OperationPolicyCheck {
        db: check.db,
        schema: check.schema,
        table: check.table,
        row_num: check.row_num,
        branch_num: check.branch_num,
        policy: operation_policy_for_op(check.table, check.op),
        old_values: check.old_values,
        new_values: check.new_values,
        user: check.user,
        created_by: check.created_by,
    })
}

struct OperationPolicyCheck<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table: &'a crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
    policy: &'a crate::schema::OperationPolicy,
    old_values: Option<&'a BTreeMap<String, JsonValue>>,
    new_values: &'a BTreeMap<String, JsonValue>,
    user: &'a str,
    created_by: Option<&'a str>,
}

fn operation_policy_allowed(check: OperationPolicyCheck<'_>) -> Result<bool> {
    if let Some(using) = &check.policy.using {
        let Some(old_values) = check.old_values else {
            return Ok(false);
        };
        if !policy_allowed_for_values(PolicyValueCheck {
            db: check.db,
            schema: check.schema,
            table: check.table,
            policy: using,
            row_num: check.row_num,
            branch_num: check.branch_num,
            values: old_values,
            user: check.user,
            created_by: check.created_by,
        })? {
            return Ok(false);
        }
    }
    if let Some(with_check) = &check.policy.with_check {
        if !policy_allowed_for_values(PolicyValueCheck {
            db: check.db,
            schema: check.schema,
            table: check.table,
            policy: with_check,
            row_num: check.row_num,
            branch_num: check.branch_num,
            values: check.new_values,
            user: check.user,
            created_by: check.created_by,
        })? {
            return Ok(false);
        }
    }
    Ok(true)
}

struct PolicyValueCheck<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table: &'a crate::schema::TableDef,
    policy: &'a PolicyDef,
    row_num: i64,
    branch_num: i64,
    values: &'a BTreeMap<String, JsonValue>,
    user: &'a str,
    created_by: Option<&'a str>,
}

fn policy_allowed_for_values(check: PolicyValueCheck<'_>) -> Result<bool> {
    policy::write_allowed(policy::WriteCheck {
        db: check.db,
        schema: check.schema,
        table: check.table,
        policy: check.policy,
        row_num: check.row_num,
        branch_num: check.branch_num,
        values: check.values,
        user: check.user,
        created_by: check.created_by,
    })
}

fn operation_policy_for_op(
    table: &crate::schema::TableDef,
    op: i64,
) -> &crate::schema::OperationPolicy {
    match op {
        history_op::INSERT => &table.insert_policy,
        history_op::UPDATE => &table.update_policy,
        history_op::DELETE => &table.delete_policy,
        _ => &table.update_policy,
    }
}

fn inherited_ref_policy_field(policy: &PolicyDef) -> Option<&str> {
    match policy {
        PolicyDef::Inherits {
            operation: Operation::Select,
            via_column,
            ..
        } => Some(via_column),
        _ => None,
    }
}

fn inherited_ref_policy_fields<'a>(policy: &'a PolicyDef, fields: &mut Vec<&'a str>) {
    match policy {
        PolicyDef::Inherits {
            operation: Operation::Select,
            via_column,
            ..
        } => fields.push(via_column),
        PolicyDef::And(children) | PolicyDef::Or(children) => {
            for child in children {
                inherited_ref_policy_fields(child, fields);
            }
        }
        PolicyDef::Not(child) => inherited_ref_policy_fields(child, fields),
        _ => {}
    }
}

fn policy_denial_detail_for_history_record(
    conn: &Connection,
    table: &crate::schema::TableDef,
    record: &HistoryRecord,
    tx_num: i64,
) -> Result<JsonValue> {
    let branch_num = branch::checkout(conn, &record.branch_id)?;
    if let Some(dependency) = unavailable_recorded_policy_dependency(conn, tx_num, branch_num)? {
        return Ok(json!({
            "reason": "policy_dependency_unavailable",
            "table": record.table,
            "row_id": record.row_id,
            "dependency_table": dependency.0,
            "dependency_row_id": dependency.1,
        }));
    }
    let policies = if record.op == history_op::DELETE {
        vec![table.effective_delete_using()]
    } else {
        let policy = operation_policy_for_op(table, record.op);
        vec![policy.using.as_ref(), policy.with_check.as_ref()]
    };
    for policy in policies.into_iter().flatten() {
        let mut fields = Vec::new();
        inherited_ref_policy_fields(policy, &mut fields);
        for field in fields {
            if let Some(dependency) =
                unavailable_policy_dependency(conn, table, record, tx_num, field)?
            {
                return Ok(json!({
                    "reason": "policy_dependency_unavailable",
                    "table": record.table,
                    "row_id": record.row_id,
                    "dependency_table": dependency.0,
                    "dependency_row_id": dependency.1,
                }));
            }
        }
    }
    Ok(json!({
        "reason": "write_policy_denied",
        "table": record.table,
        "row_id": record.row_id,
    }))
}

fn is_policy_dependency_unavailable(detail: &JsonValue) -> bool {
    detail.get("reason").and_then(JsonValue::as_str) == Some("policy_dependency_unavailable")
}

fn mark_transaction_awaiting_dependency(
    conn: &Connection,
    tx_num: i64,
    auth_user: &str,
    detail: &JsonValue,
) -> Result<()> {
    let detail_json =
        serde_json::to_string(detail).map_err(|err| crate::Error::new(err.to_string()))?;
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_awaiting_dependency
         (tx_num, auth_user, detail_json, updated_at)
         VALUES (?, ?, ?, ?)",
        params![tx_num, auth_user, detail_json, now_ms()],
    )?;
    Ok(())
}

fn remove_current_for_awaiting_dependency(
    conn: &Connection,
    record: &HistoryRecord,
    row_num: i64,
) -> Result<()> {
    let branch_num = branch::ensure(conn, &record.branch_id, None, now_ms())?;
    conn.execute(
        &format!(
            "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
            crate::schema::current_table(&record.table)
        ),
        params![row_num, branch_num],
    )?;
    Ok(())
}

fn clear_transaction_awaiting_dependency(conn: &Connection, tx_num: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM jazz_tx_awaiting_dependency WHERE tx_num = ?",
        params![tx_num],
    )?;
    Ok(())
}

fn awaiting_dependency_transactions(conn: &Connection) -> Result<Vec<AwaitingDependencyTx>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_num, tx.tx_id, awaiting.auth_user
         FROM jazz_tx_awaiting_dependency awaiting
         JOIN jazz_tx tx ON tx.tx_num = awaiting.tx_num
         ORDER BY tx.tx_num",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(AwaitingDependencyTx {
            tx_num: row.get(0)?,
            tx_id: row.get(1)?,
            auth_user: row.get(2)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn history_records_for_tx(
    conn: &Connection,
    schema: &SchemaDef,
    tx_num: i64,
    tx_id: &str,
) -> Result<Vec<HistoryRecord>> {
    let mut records = Vec::new();
    for table in schema.tables() {
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "ids.row_id".to_owned(),
            "branch.branch_id".to_owned(),
            "h.op".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.extend([
            "h.j_created_at".to_owned(),
            "h.j_updated_at".to_owned(),
            format!(
                "{} AS j_created_by",
                users::user_id_expr("h", "j_created_by")
            ),
            format!(
                "{} AS j_updated_by",
                users::user_id_expr("h", "j_updated_by")
            ),
        ]);
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
             WHERE h.tx_num = ?
             ORDER BY h.row_num",
            select_columns.join(", "),
            crate::schema::history_table(&table.name)
        );
        let mut stmt = conn.prepare(&sql)?;
        let row_width = 3 + table.fields.len() + 4;
        let mut rows = stmt.query(params![tx_num])?;
        while let Some(row) = rows.next()? {
            let raw = (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut values = BTreeMap::new();
            for (idx, field) in table.fields.iter().enumerate() {
                values.insert(
                    field.name.clone(),
                    sql_value_to_json(conn, field, &raw[idx + 3])?,
                );
            }
            let sys = 3 + table.fields.len();
            records.push(HistoryRecord {
                table: table.name.clone(),
                row_id: text_value(&raw[0], "row_id")?,
                branch_id: text_value(&raw[1], "branch_id")?,
                tx_id: tx_id.to_owned(),
                op: integer_value(&raw[2], "op")?,
                values,
                created_at: integer_value(&raw[sys], "j_created_at")?,
                updated_at: integer_value(&raw[sys + 1], "j_updated_at")?,
                created_by: text_value(&raw[sys + 2], "j_created_by")?,
                updated_by: text_value(&raw[sys + 3], "j_updated_by")?,
            });
        }
    }
    Ok(records)
}

fn unavailable_recorded_policy_dependency(
    conn: &Connection,
    tx_num: i64,
    branch_num: i64,
) -> Result<Option<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT tables.table_name, ids.row_id, reads.row_num, reads.observed_tx_num
         FROM jazz_tx_read reads
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE reads.tx_num = ?
           AND reads.reason = ?
         ORDER BY tables.table_name, ids.row_id",
    )?;
    let rows = stmt.query_map(params![tx_num, 1], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })?;
    for row in rows {
        let (table_name, row_id, row_num, observed_tx_num) = row?;
        let visible_count: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {}
                 WHERE row_num = ?
                   AND j_branch_num = ?
                   AND is_deleted = 0",
                crate::schema::current_table(&table_name)
            ),
            params![row_num, branch_num],
            |row| row.get(0),
        )?;
        if visible_count == 0 {
            return Ok(Some((table_name, row_id)));
        }
        if observed_tx_num.is_none() {
            repair_missing_observed_policy_read(conn, tx_num, &table_name, row_num, branch_num)?;
        }
    }
    Ok(None)
}

fn unavailable_policy_dependency(
    conn: &Connection,
    table: &crate::schema::TableDef,
    record: &HistoryRecord,
    tx_num: i64,
    field_name: &str,
) -> Result<Option<(String, String)>> {
    let Some(field) = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
    else {
        return Ok(None);
    };
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(None);
    };
    let Some(row_id) = record.values.get(&field.name).and_then(JsonValue::as_str) else {
        return Ok(None);
    };
    let dependency_row_num = ensure_row_id(conn, ref_table_name, row_id)?;
    let branch_num = branch::checkout(conn, &record.branch_id)?;
    let visible_count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ?
               AND j_branch_num = ?
               AND is_deleted = 0",
            crate::schema::current_table(ref_table_name)
        ),
        params![dependency_row_num, branch_num],
        |row| row.get(0),
    )?;
    if visible_count == 0 {
        return Ok(Some((ref_table_name.clone(), row_id.to_owned())));
    }
    let missing_observed_read_count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM jazz_tx_read
         WHERE tx_num = ?
           AND table_num = ?
           AND row_num = ?
           AND observed_tx_num IS NULL",
        params![
            tx_num,
            crate::schema::table_num(conn, ref_table_name)?,
            dependency_row_num
        ],
        |row| row.get(0),
    )?;
    if missing_observed_read_count > 0 {
        repair_missing_observed_policy_read(
            conn,
            tx_num,
            ref_table_name,
            dependency_row_num,
            branch_num,
        )?;
    }
    Ok(None)
}

fn repair_missing_observed_policy_read(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<()> {
    let observed_tx_num: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT visible_tx_num
                 FROM {}
                 WHERE row_num = ?
                   AND j_branch_num = ?
                   AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, branch_num],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(observed_tx_num) = observed_tx_num {
        conn.execute(
            "UPDATE jazz_tx_read
             SET observed_tx_num = ?
             WHERE tx_num = ?
               AND table_num = ?
               AND row_num = ?
               AND observed_tx_num IS NULL",
            params![
                observed_tx_num,
                tx_num,
                crate::schema::table_num(conn, table_name)?,
                row_num
            ],
        )?;
    }
    Ok(())
}

fn current_creation_metadata(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<(i64, String)>> {
    let metadata = conn
        .query_row(
            &format!(
                "SELECT j_created_at, j_created_by
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
                crate::schema::current_table(table_name)
            ),
            params![row_num, branch_num],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    metadata
        .map(|(created_at, created_by_num)| {
            users::user_id(conn, created_by_num).map(|created_by| (created_at, created_by))
        })
        .transpose()
}

fn exclusive_write_conflict_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM jazz_tx_write writes
         JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
         WHERE writes.table_num = ?
           AND writes.row_num = ?
           AND tx.conflict_mode = ?
           AND tx.outcome = ?",
        params![
            crate::schema::table_num(conn, table_name)?,
            row_num,
            tx::MODE_EXCLUSIVE,
            tx::OUTCOME_ACCEPTED
        ],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn record_policy_read_set_for_write(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    policy: &PolicyDef,
    values: &BTreeMap<String, JsonValue>,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    let mut fields = Vec::new();
    inherited_ref_policy_fields(policy, &mut fields);
    if fields.is_empty() {
        return Ok(());
    };
    for field in fields {
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == *field)
            .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
        let FieldKind::Ref {
            table: ref_table_name,
        } = &field.kind
        else {
            continue;
        };
        let Some(row_id) = values.get(&field.name).and_then(JsonValue::as_str) else {
            continue;
        };
        let row_num = ensure_row_id(conn, ref_table_name, row_id)?;
        read_set::record_tx_read(conn, tx_num, ref_table_name, row_num, branch_num, 1)?;
        let ref_table = schema.table_def(ref_table_name)?;
        record_policy_read_dependencies_for_row(
            conn,
            schema,
            ref_table,
            &ref_table.read_policy,
            row_num,
            branch_num,
            tx_num,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn record_policy_read_sets_for_write(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    op: i64,
    old_values: Option<&BTreeMap<String, JsonValue>>,
    new_values: &BTreeMap<String, JsonValue>,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    if op == history_op::DELETE {
        if let (Some(policy), Some(old_values)) = (table.effective_delete_using(), old_values) {
            record_policy_read_set_for_write(
                conn, schema, table, policy, old_values, branch_num, tx_num,
            )?;
        }
        return Ok(());
    }
    record_operation_policy_read_set_for_write(
        conn,
        schema,
        table,
        operation_policy_for_op(table, op),
        old_values,
        new_values,
        branch_num,
        tx_num,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn record_operation_policy_read_set_for_write(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    policy: &crate::schema::OperationPolicy,
    old_values: Option<&BTreeMap<String, JsonValue>>,
    new_values: &BTreeMap<String, JsonValue>,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    if let (Some(using), Some(old_values)) = (&policy.using, old_values) {
        record_policy_read_set_for_write(
            conn, schema, table, using, old_values, branch_num, tx_num,
        )?;
    }
    if let Some(with_check) = &policy.with_check {
        record_policy_read_set_for_write(
            conn, schema, table, with_check, new_values, branch_num, tx_num,
        )?;
    }
    Ok(())
}

fn record_policy_read_dependencies_for_row(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    policy: &PolicyDef,
    row_num: i64,
    branch_num: i64,
    tx_num: i64,
) -> Result<()> {
    let mut fields = Vec::new();
    inherited_ref_policy_fields(policy, &mut fields);
    if fields.is_empty() {
        return Ok(());
    };
    for field in fields {
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == *field)
            .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
        let FieldKind::Ref {
            table: ref_table_name,
        } = &field.kind
        else {
            continue;
        };
        let Some(parent_row_num) =
            current_ref_field_row_num(conn, &table.name, field, row_num, branch_num)?
        else {
            continue;
        };
        read_set::record_tx_read(conn, tx_num, ref_table_name, parent_row_num, branch_num, 1)?;
        let parent_table = schema.table_def(ref_table_name)?;
        record_policy_read_dependencies_for_row(
            conn,
            schema,
            parent_table,
            &parent_table.read_policy,
            parent_row_num,
            branch_num,
            tx_num,
        )?;
    }
    Ok(())
}

fn current_ref_field_row_num(
    conn: &Connection,
    table_name: &str,
    field: &FieldDef,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<i64>> {
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            if !current_row_exists_on_branch(conn, table_name, row_num, branch_num)? {
                return snapshot_ref_field_row_num(conn, table_name, field, row_num, base_epoch);
            }
        }
    }
    let column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    conn.query_row(
        &format!(
            "SELECT current.{column}
             FROM {} current
             JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
             WHERE current.row_num = ?
               AND {}
               AND current.is_deleted = 0
               AND tx.outcome != ?
             ORDER BY CASE WHEN current.j_branch_num = ? THEN 0 ELSE 1 END
             LIMIT 1",
            crate::schema::current_table(table_name),
            current_effective_branch_sql("current", table_name, branch_num)
        ),
        params![row_num, tx::OUTCOME_REJECTED, branch_num],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn current_row_exists_on_branch(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {}
             WHERE row_num = ?
               AND j_branch_num = ?",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn snapshot_ref_field_row_num(
    conn: &Connection,
    table_name: &str,
    field: &FieldDef,
    row_num: i64,
    base_epoch: i64,
) -> Result<Option<i64>> {
    let column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    conn.query_row(
        &format!(
            "SELECT h.{column}
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = 1
               AND h.op != ?
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
             LIMIT 1",
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
        ),
        params![
            row_num,
            history_op::DELETE,
            tx::OUTCOME_REJECTED,
            base_epoch,
            tx::OUTCOME_REJECTED,
            base_epoch
        ],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Into::into)
}

fn current_effective_branch_sql(alias: &str, table_name: &str, branch_num: i64) -> String {
    if branch_num == 1 {
        return format!("{alias}.j_branch_num = 1");
    }
    format!(
        "({alias}.j_branch_num = {branch_num}
          OR (
            {alias}.j_branch_num = 1
            AND NOT EXISTS (
              SELECT 1
              FROM {} branch_shadow
              WHERE branch_shadow.row_num = {alias}.row_num
                AND branch_shadow.j_branch_num = {branch_num}
            )
          ))",
        crate::schema::current_table(table_name)
    )
}

fn record_tx_write(
    conn: &Connection,
    tx_num: i64,
    table_name: &str,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let table_num = crate::schema::table_num(conn, table_name)?;
    record_tx_write_num(conn, tx_num, table_num, row_num, op)
}

fn record_tx_write_num(
    conn: &Connection,
    tx_num: i64,
    table_num: i64,
    row_num: i64,
    op: i64,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO jazz_tx_write (tx_num, table_num, row_num, op)
         VALUES (?, ?, ?, ?)",
    )?;
    stmt.execute(params![tx_num, table_num, row_num, op])?;
    Ok(())
}

pub struct TransactionBuilder<'a> {
    runtime: &'a mut Runtime,
    mutations: Vec<Mutation>,
    mode: TransactionMode,
}

enum TransactionMode {
    Mergeable,
    Exclusive { global_epoch: Option<i64> },
}

enum Mutation {
    Row {
        table: String,
        id: String,
        values: BTreeMap<String, JsonValue>,
        op: i64,
    },
    DeleteRow {
        table: String,
        id: String,
    },
}

fn normalize_mutations(mutations: Vec<Mutation>) -> Vec<Mutation> {
    let mut normalized: Vec<Mutation> = Vec::new();
    for mutation in mutations {
        let (table, id) = match &mutation {
            Mutation::Row { table, id, .. } | Mutation::DeleteRow { table, id } => {
                (table.as_str(), id.as_str())
            }
        };
        let Some(existing) = normalized.iter_mut().find(|existing| match existing {
            Mutation::Row {
                table: existing_table,
                id: existing_id,
                ..
            }
            | Mutation::DeleteRow {
                table: existing_table,
                id: existing_id,
            } => existing_table == table && existing_id == id,
        }) else {
            normalized.push(mutation);
            continue;
        };
        match (existing, mutation) {
            (
                Mutation::Row {
                    values: existing_values,
                    op: existing_op,
                    ..
                },
                Mutation::Row { values, op, .. },
            ) => {
                existing_values.extend(values);
                if *existing_op != history_op::INSERT {
                    *existing_op = op;
                }
            }
            (existing_slot, later) => {
                *existing_slot = later;
            }
        }
    }
    normalized
}

impl<'a> TransactionBuilder<'a> {
    pub fn exclusive(mut self) -> Self {
        self.mode = TransactionMode::Exclusive { global_epoch: None };
        self
    }

    pub fn exclusive_at_global(mut self, global_epoch: i64) -> Self {
        self.mode = TransactionMode::Exclusive {
            global_epoch: Some(global_epoch),
        };
        self
    }

    pub fn insert_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op: history_op::INSERT,
        });
        self
    }

    pub fn update_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op: history_op::UPDATE,
        });
        self
    }

    pub fn upsert_row(
        mut self,
        table: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Self {
        let op = match self.runtime.row_has_current_branch_value(table, id) {
            Ok(true) => history_op::UPDATE,
            Ok(false) | Err(_) => history_op::INSERT,
        };
        self.mutations.push(Mutation::Row {
            table: table.to_owned(),
            id: id.to_owned(),
            values,
            op,
        });
        self
    }

    pub fn delete_row(mut self, table: &str, id: &str) -> Self {
        self.mutations.push(Mutation::DeleteRow {
            table: table.to_owned(),
            id: id.to_owned(),
        });
        self
    }

    pub fn commit(self) -> Result<String> {
        let mutations = normalize_mutations(self.mutations);
        if mutations.is_empty() {
            return Ok(String::new());
        }
        let user = self.runtime.attribution_user().to_owned();
        let bypass_policy = self.runtime.bypasses_policy();
        let mut delete_snapshots = BTreeMap::new();
        for mutation in &mutations {
            let Mutation::DeleteRow { table, id } = mutation else {
                continue;
            };
            let visible_row = self
                .runtime
                .read_rows(table)?
                .into_iter()
                .find(|row| row.id == *id)
                .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))?;
            delete_snapshots.insert((table.clone(), id.clone()), visible_row);
        }
        let (conflict_mode, outcome, global_epoch) = match self.mode {
            TransactionMode::Mergeable => (tx::MODE_MERGEABLE, tx::OUTCOME_PENDING, None),
            TransactionMode::Exclusive {
                global_epoch: Some(global_epoch),
            } => (tx::MODE_EXCLUSIVE, tx::OUTCOME_ACCEPTED, Some(global_epoch)),
            TransactionMode::Exclusive { global_epoch: None } => {
                return Err(crate::Error::new(
                    "exclusive transactions require global acceptance",
                ));
            }
        };
        if conflict_mode == tx::MODE_EXCLUSIVE {
            for mutation in &mutations {
                let (table, id): (&str, &str) = match mutation {
                    Mutation::Row { table, id, .. } | Mutation::DeleteRow { table, id } => {
                        (table.as_str(), id.as_str())
                    }
                };
                let row_num = ensure_row_id(&self.runtime.conn, table, id)?;
                if exclusive_write_conflict_exists(&self.runtime.conn, table, row_num)? {
                    return Err(crate::Error::new("exclusive conflict"));
                }
            }
        }
        let db = self.runtime.conn.transaction()?;
        let now = now_ms();
        let (tx_num, tx_id) = tx::create_tx_with_options(
            &db,
            self.runtime.node_num,
            &self.runtime.node_id,
            now,
            conflict_mode,
            outcome,
            global_epoch,
        )?;
        let mut allowed = true;
        for mutation in mutations {
            match mutation {
                Mutation::Row {
                    table,
                    id,
                    values,
                    op,
                } => {
                    allowed &= insert_row_in_tx(InsertRowInTx {
                        db: &db,
                        schema: &self.runtime.schema,
                        table_name: &table,
                        id: &id,
                        values: &values,
                        tx_num,
                        branch_num: self.runtime.branch_num,
                        now,
                        user: &user,
                        bypass_policy,
                        op,
                    })?;
                }
                Mutation::DeleteRow { table, id } => {
                    let table_def = self.runtime.schema.table_def(&table)?;
                    let row_num = row_num(&db, &id)?;
                    read_set::record_tx_read(
                        &db,
                        tx_num,
                        &table,
                        row_num,
                        self.runtime.branch_num,
                        2,
                    )?;
                    let visible_row = delete_snapshots
                        .get(&(table.clone(), id.clone()))
                        .ok_or_else(|| {
                            crate::Error::new(format!("missing delete snapshot {id}"))
                        })?;
                    if let Some(policy) = table_def.effective_delete_using() {
                        record_policy_read_set_for_write(
                            &db,
                            &self.runtime.schema,
                            table_def,
                            policy,
                            &visible_row.values,
                            self.runtime.branch_num,
                            tx_num,
                        )?;
                    }
                    allowed &= bypass_policy
                        || local_write_allowed(LocalWriteCheck {
                            db: &db,
                            schema: &self.runtime.schema,
                            table: table_def,
                            row_num,
                            branch_num: self.runtime.branch_num,
                            old_values: Some(&visible_row.values),
                            new_values: &visible_row.values,
                            user: &user,
                            op: history_op::DELETE,
                            created_by: Some(&visible_row.created_by),
                        })?;
                    let field_columns = table_def
                        .fields
                        .iter()
                        .map(|field| {
                            crate::schema::quote_ident(&crate::schema::storage_column(field))
                        })
                        .collect::<Vec<_>>();
                    let mut insert_columns = vec![
                        "row_num".to_owned(),
                        "tx_num".to_owned(),
                        "j_branch_num".to_owned(),
                        "op".to_owned(),
                    ];
                    insert_columns.extend(field_columns.iter().cloned());
                    insert_columns.extend([
                        "j_created_at".to_owned(),
                        "j_updated_at".to_owned(),
                        "j_created_by".to_owned(),
                        "j_updated_by".to_owned(),
                    ]);
                    let mut select_columns = vec![
                        "row_num".to_owned(),
                        "?".to_owned(),
                        "j_branch_num".to_owned(),
                        history_op::DELETE.to_string(),
                    ];
                    select_columns.extend(field_columns.iter().cloned());
                    select_columns.extend([
                        "j_created_at".to_owned(),
                        "?".to_owned(),
                        "j_created_by".to_owned(),
                        "?".to_owned(),
                    ]);
                    let user_num = users::ensure_user(&db, &user)?;
                    let inserted = db.execute(
                        &format!(
                            "INSERT OR IGNORE INTO {} ({})
                             SELECT {}
                             FROM {}
                             WHERE row_num = ? AND j_branch_num = ?",
                            crate::schema::history_table(&table),
                            insert_columns.join(", "),
                            select_columns.join(", "),
                            crate::schema::current_table(&table),
                        ),
                        params![tx_num, now, user_num, row_num, self.runtime.branch_num],
                    )?;
                    if inserted == 0 {
                        let mut values = vec![
                            rusqlite::types::Value::Integer(row_num),
                            rusqlite::types::Value::Integer(tx_num),
                            rusqlite::types::Value::Integer(self.runtime.branch_num),
                            rusqlite::types::Value::Integer(history_op::DELETE),
                        ];
                        for field in &table_def.fields {
                            let value = visible_row.values.get(&field.name).ok_or_else(|| {
                                crate::Error::new(format!("missing field {}", field.name))
                            })?;
                            values.push(crate::schema::field_sql_value(
                                field,
                                value,
                                |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                            )?);
                        }
                        values.extend([
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(user_num),
                            rusqlite::types::Value::Integer(user_num),
                        ]);
                        insert_dynamic(
                            &db,
                            &crate::schema::history_table(&table),
                            &insert_columns,
                            &values,
                        )?;
                    }
                    db.execute(
                        &format!(
                            "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                            crate::schema::current_table(&table)
                        ),
                        params![row_num, self.runtime.branch_num],
                    )?;
                    if self.runtime.branch_num != 1 {
                        let mut current_columns = vec![
                            "row_num".to_owned(),
                            "j_branch_num".to_owned(),
                            "visible_tx_num".to_owned(),
                            "is_deleted".to_owned(),
                        ];
                        current_columns.extend(field_columns.iter().cloned());
                        current_columns.extend([
                            "j_created_at".to_owned(),
                            "j_updated_at".to_owned(),
                            "j_created_by".to_owned(),
                            "j_updated_by".to_owned(),
                        ]);
                        let mut current_values = vec![
                            rusqlite::types::Value::Integer(row_num),
                            rusqlite::types::Value::Integer(self.runtime.branch_num),
                            rusqlite::types::Value::Integer(tx_num),
                            rusqlite::types::Value::Integer(1),
                        ];
                        for field in &table_def.fields {
                            let value = visible_row.values.get(&field.name).ok_or_else(|| {
                                crate::Error::new(format!("missing field {}", field.name))
                            })?;
                            current_values.push(crate::schema::field_sql_value(
                                field,
                                value,
                                |ref_table, row_id| ensure_row_id(&db, ref_table, row_id),
                            )?);
                        }
                        current_values.extend([
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(now),
                            rusqlite::types::Value::Integer(user_num),
                            rusqlite::types::Value::Integer(user_num),
                        ]);
                        insert_dynamic(
                            &db,
                            &crate::schema::current_table(&table),
                            &current_columns,
                            &current_values,
                        )?;
                    }
                    record_tx_write(&db, tx_num, &table, row_num, history_op::DELETE)?;
                }
            }
        }
        if !allowed {
            tx::reject(&db, &tx_id, "policy_denied")?;
            projection::rebuild(&db, &self.runtime.schema, self.runtime.node_num)?;
        }
        db.commit()?;
        Ok(tx_id)
    }
}

fn tx_outcome(conn: &Connection, tx_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT outcome FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

fn tx_conflict_mode(conn: &Connection, tx_num: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT conflict_mode FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| row.get(0),
    )?)
}

#[derive(Clone, Copy)]
struct ApplyTxInfo {
    node_num: i64,
    outcome: i64,
    conflict_mode: i64,
}

struct ApplyHistoryContext<'a> {
    schema: &'a SchemaDef,
    db: &'a Connection,
    local_node_num: i64,
    tx_nums_by_id: &'a BTreeMap<String, i64>,
    tx_info_by_num: &'a BTreeMap<i64, ApplyTxInfo>,
    branch_nums_by_id: &'a BTreeMap<String, i64>,
    table_nums_by_name: &'a BTreeMap<String, i64>,
    row_nums_by_id: &'a mut BTreeMap<(String, String), i64>,
    row_nums_created_in_apply: &'a mut BTreeSet<i64>,
    user_nums_by_id: &'a mut BTreeMap<String, i64>,
}

fn tx_apply_info(conn: &Connection, tx_num: i64) -> Result<ApplyTxInfo> {
    Ok(conn.query_row(
        "SELECT node_num, outcome, conflict_mode FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| {
            Ok(ApplyTxInfo {
                node_num: row.get(0)?,
                outcome: row.get(1)?,
                conflict_mode: row.get(2)?,
            })
        },
    )?)
}

fn next_global_epoch(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(global_epoch), 0) + 1 FROM jazz_tx",
        [],
        |row| row.get(0),
    )?)
}

fn durable_version_exists_for_row(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND tx.outcome != ?
               AND (tx.outcome = ? OR tx.global_epoch IS NOT NULL)",
            crate::schema::history_table(table_name)
        ),
        params![
            row_num,
            branch_num,
            tx::OUTCOME_REJECTED,
            tx::OUTCOME_ACCEPTED
        ],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn write_allowed_for_history_record(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    row_num: i64,
    record: &HistoryRecord,
    tx_num: i64,
    auth_user: Option<&str>,
) -> Result<bool> {
    let user = auth_user
        .ok_or_else(|| crate::Error::new("untrusted policy validation requires auth user"))?;
    let branch_num = branch::ensure(conn, &record.branch_id, None, now_ms())?;
    let previous_values = match record.op {
        history_op::UPDATE => {
            previous_history_values_before_tx(conn, table, row_num, branch_num, tx_num)?
        }
        history_op::DELETE => Some(record.values.clone()),
        _ => None,
    };
    let row_policy_allowed = if record.op == history_op::DELETE {
        if let Some(policy) = table.effective_delete_using() {
            let Some(old_values) = previous_values.as_ref() else {
                return Ok(false);
            };
            policy_allowed_for_values(PolicyValueCheck {
                db: conn,
                schema,
                table,
                policy,
                row_num,
                branch_num,
                values: old_values,
                user,
                created_by: Some(&record.created_by),
            })?
        } else {
            true
        }
    } else {
        operation_policy_allowed(OperationPolicyCheck {
            db: conn,
            schema,
            table,
            row_num,
            branch_num,
            policy: operation_policy_for_op(table, record.op),
            old_values: previous_values.as_ref(),
            new_values: &record.values,
            user,
            created_by: Some(&record.created_by),
        })?
    };
    if !row_policy_allowed {
        return Ok(false);
    }
    Ok(true)
}

fn previous_history_values_before_tx(
    conn: &Connection,
    table: &crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
    tx_num: i64,
) -> Result<Option<BTreeMap<String, JsonValue>>> {
    let field_columns = table
        .fields
        .iter()
        .map(|field| {
            format!(
                "h.{}",
                crate::schema::quote_ident(&crate::schema::storage_column(field))
            )
        })
        .collect::<Vec<_>>();
    let mut stmt = conn.prepare(&format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.row_num = ?
           AND h.j_branch_num = ?
           AND h.tx_num < ?
           AND h.op != ?
           AND tx.outcome != ?
         ORDER BY h.tx_num DESC
         LIMIT 1",
        field_columns.join(", "),
        crate::schema::history_table(&table.name)
    ))?;
    let mut rows = stmt.query_map(
        params![
            row_num,
            branch_num,
            tx_num,
            history_op::DELETE,
            tx::OUTCOME_REJECTED
        ],
        |row| {
            (0..table.fields.len())
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        },
    )?;
    let Some(row) = rows.next().transpose()? else {
        return Ok(None);
    };
    let mut values = BTreeMap::new();
    for (idx, field) in table.fields.iter().enumerate() {
        values.insert(
            field.name.clone(),
            query::sql_value_to_json(conn, field, &row[idx])?,
        );
    }
    Ok(Some(values))
}

fn is_newest_version_for_current(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
    tx_num: i64,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             JOIN jazz_tx current_tx ON current_tx.tx_num = ?
             WHERE h.row_num = ?
               AND h.j_branch_num = ?
               AND tx.outcome != ?
               AND (
                 (tx.global_epoch IS NOT NULL AND current_tx.global_epoch IS NOT NULL
                  AND (tx.global_epoch > current_tx.global_epoch
                       OR (tx.global_epoch = current_tx.global_epoch AND tx.tx_num > current_tx.tx_num)))
                 OR ((tx.global_epoch IS NOT NULL) = (current_tx.global_epoch IS NOT NULL)
                     AND tx.global_epoch IS NULL
                     AND tx.tx_num > current_tx.tx_num)
               )",
            crate::schema::history_table(table_name)
        ),
        params![tx_num, row_num, branch_num, tx::OUTCOME_REJECTED],
        |row| row.get(0),
    )?;
    Ok(count == 0)
}

fn current_visible_tx_num(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    branch_num: i64,
) -> Result<Option<i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT visible_tx_num
         FROM {}
         WHERE row_num = ?
           AND j_branch_num = ?",
        crate::schema::current_table(table_name)
    ))?;
    let mut rows = stmt.query(params![row_num, branch_num])?;
    rows.next()?
        .map(|row| row.get(0))
        .transpose()
        .map_err(Into::into)
}

fn history_record_exists(
    conn: &Connection,
    table_name: &str,
    row_num: i64,
    tx_num: i64,
) -> Result<bool> {
    let mut stmt = conn.prepare(&format!(
        "SELECT 1
         FROM {}
         WHERE row_num = ?
           AND tx_num = ?
         LIMIT 1",
        crate::schema::history_table(table_name)
    ))?;
    let mut rows = stmt.query(params![row_num, tx_num])?;
    Ok(rows.next()?.is_some())
}

fn tx_is_newer_than_current_fast_path(
    conn: &Connection,
    candidate_tx_num: i64,
    current_tx_num: i64,
) -> Result<Option<bool>> {
    let comparison: Option<i64> = conn.query_row(
        "SELECT CASE
           WHEN (candidate.global_epoch IS NULL) != (current.global_epoch IS NULL)
             THEN NULL
           WHEN candidate.global_epoch IS NOT NULL
             THEN candidate.global_epoch > current.global_epoch
               OR (candidate.global_epoch = current.global_epoch AND candidate.tx_num > current.tx_num)
           ELSE candidate.tx_num > current.tx_num
         END
         FROM jazz_tx candidate
         JOIN jazz_tx current ON current.tx_num = ?
         WHERE candidate.tx_num = ?",
        params![current_tx_num, candidate_tx_num],
        |row| row.get(0),
    )?;
    Ok(comparison.map(|is_newer| is_newer != 0))
}

fn export_txs(conn: &Connection) -> Result<Vec<TxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let mut receipt_stmt = conn.prepare(
            "SELECT receipt.tier
             FROM jazz_tx_receipt receipt
             JOIN jazz_tx tx ON tx.tx_num = receipt.tx_num
             WHERE tx.tx_id = ?
             ORDER BY receipt.tier",
        )?;
        let receipt_tiers = receipt_stmt
            .query_map(params![tx_id], |row| row.get::<_, i64>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn export_txs_for_query_scope(
    conn: &Connection,
    _table_name: &str,
    history: &[HistoryRecord],
    reads: &[ReadRecord],
    extra_tx_ids: &[String],
) -> Result<Vec<TxRecord>> {
    let mut needed_tx_ids = history
        .iter()
        .map(|record| record.tx_id.as_str())
        .collect::<BTreeSet<_>>();
    for tx_id in extra_tx_ids {
        needed_tx_ids.insert(tx_id.as_str());
    }
    for record in reads {
        needed_tx_ids.insert(record.tx_id.as_str());
        if let Some(observed_tx_id) = &record.observed_tx_id {
            needed_tx_ids.insert(observed_tx_id.as_str());
        }
    }
    export_txs_by_ids(conn, needed_tx_ids)
}

fn export_txs_by_ids(conn: &Connection, tx_ids: BTreeSet<&str>) -> Result<Vec<TxRecord>> {
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut receipt_stmt = conn.prepare(
        "SELECT tx.tx_id, receipt.tier
         FROM jazz_tx tx
         JOIN jazz_tx_receipt receipt ON receipt.tx_num = tx.tx_num
         ORDER BY tx.tx_num, receipt.tier",
    )?;
    let receipt_rows = receipt_stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut receipt_tiers_by_tx = BTreeMap::<String, Vec<i64>>::new();
    for receipt_row in receipt_rows {
        let (tx_id, tier) = receipt_row?;
        if tx_ids.contains(tx_id.as_str()) {
            receipt_tiers_by_tx.entry(tx_id).or_default().push(tier);
        }
    }

    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, node.node_id, tx.local_epoch, tx.global_epoch, tx.conflict_mode, tx.outcome, rejection.code, rejection.detail_json, tx.created_at, tx.metadata_json
         FROM jazz_tx tx
         JOIN jazz_node node ON node.node_num = tx.node_num
         LEFT JOIN jazz_tx_rejection rejection ON rejection.tx_num = tx.tx_num
         ORDER BY tx.tx_num",
    )?;
    let records = stmt.query_map([], |row| {
        let tx_id = row.get::<_, String>(0)?;
        let receipt_tiers = receipt_tiers_by_tx.get(&tx_id).cloned().unwrap_or_default();
        Ok(TxRecord {
            tx_id,
            node_id: row.get(1)?,
            local_epoch: row.get(2)?,
            global_epoch: row.get(3)?,
            conflict_mode: row.get(4)?,
            outcome: row.get(5)?,
            auth_user: parse_tx_auth_user_for_sqlite(&row.get::<_, String>(9)?, 9)?,
            rejection_code: row.get(6)?,
            rejection_detail: row
                .get::<_, Option<String>>(7)?
                .map(|detail_json| parse_rejection_detail_for_sqlite(&detail_json, 7))
                .transpose()?
                .flatten(),
            receipt_tiers,
            created_at: row.get(8)?,
        })
    })?;
    let mut tx_records = Vec::new();
    for record in records {
        let record = record?;
        if tx_ids.contains(record.tx_id.as_str()) {
            tx_records.push(record);
        }
    }
    Ok(tx_records)
}

fn parse_rejection_detail(detail_json: &str) -> Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json)
        .map_err(|err| crate::Error::new(format!("invalid rejection detail JSON: {err}")))?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

fn tx_metadata_json(auth_user: Option<&str>) -> Result<String> {
    let metadata = match auth_user {
        Some(user) => json!({ "auth_user": user }),
        None => json!({}),
    };
    serde_json::to_string(&metadata).map_err(|err| crate::Error::new(err.to_string()))
}

fn parse_tx_auth_user_for_sqlite(
    metadata_json: &str,
    column: usize,
) -> rusqlite::Result<Option<String>> {
    let metadata = serde_json::from_str::<JsonValue>(metadata_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    Ok(metadata
        .get("auth_user")
        .and_then(JsonValue::as_str)
        .map(str::to_owned))
}

fn parse_rejection_detail_for_sqlite(
    detail_json: &str,
    column: usize,
) -> rusqlite::Result<Option<JsonValue>> {
    let detail = serde_json::from_str::<JsonValue>(detail_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })?;
    if detail.is_null() {
        Ok(None)
    } else {
        Ok(Some(detail))
    }
}

fn encode_optional_json(value: Option<&JsonValue>) -> Result<String> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(|err| crate::Error::new(format!("invalid JSON detail: {err}")))
        .map(|value| value.unwrap_or_else(|| "null".to_owned()))
}

fn export_reads_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<ReadRecord>> {
    let tx_ids = history
        .iter()
        .map(|record| record.tx_id.as_str())
        .collect::<BTreeSet<_>>();
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    let history_keys = history
        .iter()
        .map(|record| {
            (
                record.tx_id.as_str(),
                record.table.as_str(),
                record.row_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut stmt = conn.prepare(
        "SELECT tx.tx_id, tables.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_tx_read reads
         JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
         JOIN jazz_table tables ON tables.table_num = reads.table_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         ORDER BY tx.tx_num, tables.table_name, ids.row_id, reads.reason",
    )?;
    let records = stmt.query_map([], |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    let records = records
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            tx_ids.contains(record.tx_id.as_str())
                && (record.reason != read_set::REASON_ABSENT
                    || history_keys.contains(&(
                        record.tx_id.as_str(),
                        record.table.as_str(),
                        record.row_id.as_str(),
                    )))
        })
        .collect();
    Ok(records)
}

fn export_branch_records_for_history(
    conn: &Connection,
    history: &[HistoryRecord],
) -> Result<Vec<BranchRecord>> {
    let mut branch_ids = history
        .iter()
        .map(|record| record.branch_id.clone())
        .collect::<Vec<_>>();
    branch_ids.sort();
    branch_ids.dedup();

    let mut records = Vec::new();
    for branch_id in branch_ids {
        let (branch_num, base_global_epoch, source_version) = conn.query_row(
            "SELECT branch_num, base_global_epoch, source_version FROM jazz_branch WHERE branch_id = ?",
            params![branch_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, i64>(2)?)),
        )?;
        let mut stmt = conn.prepare(
            "SELECT source.branch_id
             FROM jazz_branch_source branch_source
             JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
             WHERE branch_source.branch_num = ?
             ORDER BY source.branch_id",
        )?;
        let source_branch_ids = stmt
            .query_map(params![branch_num], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    Ok(records)
}

fn include_branch_record(
    conn: &Connection,
    records: &mut Vec<BranchRecord>,
    branch_num: i64,
) -> Result<()> {
    let (branch_id, base_global_epoch, source_version) = conn.query_row(
        "SELECT branch_id, base_global_epoch, source_version FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let mut stmt = conn.prepare(
        "SELECT source.branch_num, source.branch_id
         FROM jazz_branch_source branch_source
         JOIN jazz_branch source ON source.branch_num = branch_source.source_branch_num
         WHERE branch_source.branch_num = ?
         ORDER BY source.branch_id",
    )?;
    let source_branches = stmt
        .query_map(params![branch_num], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let source_branch_ids = source_branches
        .iter()
        .map(|(_, branch_id)| branch_id.clone())
        .collect();
    if !records.iter().any(|record| record.branch_id == branch_id) {
        records.push(BranchRecord {
            branch_id,
            base_global_epoch,
            source_branch_ids,
            source_version,
        });
    }
    for (source_branch_num, _) in source_branches {
        include_branch_record(conn, records, source_branch_num)?;
    }
    Ok(())
}

fn branch_id_for_num(conn: &Connection, branch_num: i64) -> Result<String> {
    conn.query_row(
        "SELECT branch_id FROM jazz_branch WHERE branch_num = ?",
        params![branch_num],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn export_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    user: &str,
    bypass_policy: bool,
    branch_num: i64,
) -> Result<Vec<HistoryRecord>> {
    let branch_nums = branch::scope_nums(conn, branch_num)?;
    let visibility = ReadVisibility {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
    };
    let mut records = export_visible_table_history(&visibility, table_name, &branch_nums, None)?;
    records.extend(export_deleted_table_history(
        conn,
        schema,
        table_name,
        &branch_nums,
    )?);
    records.extend(export_policy_dependency_history(
        &visibility,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.read_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    let table = schema.table_def(table_name)?;
    for policy in [
        table.insert_policy.with_check.as_ref(),
        table.update_policy.using.as_ref(),
        table.update_policy.with_check.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        records.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy,
                branch_nums: &branch_nums,
                child_row_nums: None,
            },
        )?);
    }
    if let Some(policy) = table.effective_delete_using() {
        records.extend(export_policy_dependency_history(
            &visibility,
            PolicyDependencyExport {
                table_name,
                policy,
                branch_nums: &branch_nums,
                child_row_nums: None,
            },
        )?);
    }
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            records.extend(export_main_base_snapshot_history(
                &visibility,
                table_name,
                base_epoch,
            )?);
        }
    }
    Ok(records)
}

fn export_main_base_snapshot_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let row_nums =
        visibility.base_snapshot_row_nums_visible_in_branch(table_name, base_epoch, None)?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        table_name,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        table_name,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let Some(field) = inherited_ref_policy_field(&table.read_policy) else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT h.{ref_column}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {row_filter}
           AND h.j_branch_num = 1
           AND h.op != {delete_op}
           AND tx.outcome != {}
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= {base_epoch}
           AND {policy_sql}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = 1
               AND newer_tx.outcome != {}
               AND newer_tx.global_epoch IS NOT NULL
               AND newer_tx.global_epoch <= {base_epoch}
               AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
        )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        delete_op = history_op::DELETE,
        row_filter = history_row_filter_sql("h", child_row_nums),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        parent_table,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        visibility,
        parent_table,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
) -> Result<Vec<HistoryRecord>> {
    export_snapshot_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        table_name,
        base_epoch,
        child_scope,
        0,
    )
}

fn export_snapshot_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    base_epoch: i64,
    child_scope: &query::LoweredQueryRowScope,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let Some(field) = inherited_ref_policy_field(&table.read_policy) else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let policy_sql = visibility.snapshot_policy_sql(table, "h", base_epoch)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("snapshot_policy_child_scope_{depth}");
    let parent_scope_name = format!("snapshot_policy_parent_scope_{depth}");
    let mut ctes = child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT h.{ref_column}
           FROM {} h
           JOIN {child_scope_name} child_scope ON child_scope.row_num = h.row_num
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE h.j_branch_num = 1
             AND h.op != {delete_op}
             AND tx.outcome != {}
             AND tx.global_epoch IS NOT NULL
             AND tx.global_epoch <= {base_epoch}
             AND {policy_sql}
             AND NOT EXISTS (
               SELECT 1
               FROM {history_table} newer
               JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
               WHERE newer.row_num = h.row_num
                 AND newer.j_branch_num = 1
                 AND newer_tx.outcome != {}
                 AND newer_tx.global_epoch IS NOT NULL
                 AND newer_tx.global_epoch <= {base_epoch}
                 AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
             )
        )",
        crate::schema::history_table(table_name),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        delete_op = history_op::DELETE,
        history_table = crate::schema::history_table(table_name),
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: child_scope.params.clone(),
    };
    let mut records = export_history_versions_for_query_scope(
        conn,
        schema,
        parent_table,
        &parent_scope,
        Some(base_epoch),
    )?;
    records.extend(
        export_snapshot_policy_dependency_history_for_query_scope_at_depth(
            visibility,
            parent_table,
            base_epoch,
            &parent_scope,
            depth + 1,
        )?,
    );
    Ok(records)
}

struct PolicyDependencyExport<'a> {
    table_name: &'a str,
    policy: &'a PolicyDef,
    branch_nums: &'a [i64],
    child_row_nums: Option<&'a [i64]>,
}

struct PolicyDependencyQueryScopeExport<'a> {
    table_name: &'a str,
    policy: &'a PolicyDef,
    branch_nums: &'a [i64],
    child_scope: &'a query::LoweredQueryRowScope,
}

fn export_policy_dependency_history(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    match args.policy {
        PolicyDef::True
        | PolicyDef::False
        | PolicyDef::Cmp { .. }
        | PolicyDef::SessionCmp { .. }
        | PolicyDef::IsNull { .. }
        | PolicyDef::SessionIsNull { .. }
        | PolicyDef::IsNotNull { .. }
        | PolicyDef::SessionIsNotNull { .. }
        | PolicyDef::Contains { .. }
        | PolicyDef::SessionContains { .. }
        | PolicyDef::In { .. }
        | PolicyDef::InList { .. }
        | PolicyDef::SessionInList { .. }
        | PolicyDef::Exists { .. }
        | PolicyDef::ExistsRel { .. } => Ok(Vec::new()),
        PolicyDef::InheritsReferencing {
            operation,
            source_table,
            via_column,
            ..
        } => {
            if *operation != Operation::Select {
                return Err(crate::Error::new(
                    "mini-sqlite policies only lower SELECT inheritance today",
                ));
            }
            let source_def = schema.table_def(source_table)?;
            if source_table == "group_members"
                && via_column == "group"
                && source_def
                    .read_policy
                    .is_user_or_ref_readable("user", "member_group")
            {
                export_group_membership_policy_dependencies(visibility, args)
            } else if source_table == "project_members"
                && via_column == "project"
                && source_def
                    .read_policy
                    .is_user_or_ref_readable("user", "group")
            {
                let project_row_nums = args.child_row_nums.map(|rows| rows.to_vec());
                export_project_membership_policy_dependencies(visibility, args, project_row_nums)
            } else {
                Ok(Vec::new())
            }
        }
        PolicyDef::And(children) | PolicyDef::Or(children) => {
            let mut records = Vec::new();
            for child in children {
                records.extend(export_policy_dependency_history(
                    visibility,
                    PolicyDependencyExport {
                        table_name: args.table_name,
                        policy: child,
                        branch_nums: args.branch_nums,
                        child_row_nums: args.child_row_nums,
                    },
                )?);
            }
            Ok(records)
        }
        PolicyDef::Not(child) => export_policy_dependency_history(
            visibility,
            PolicyDependencyExport {
                table_name: args.table_name,
                policy: child,
                branch_nums: args.branch_nums,
                child_row_nums: args.child_row_nums,
            },
        ),
        PolicyDef::Inherits {
            operation,
            via_column,
            ..
        } => {
            if *operation != Operation::Select {
                return Err(crate::Error::new(
                    "mini-sqlite policies only lower SELECT inheritance today",
                ));
            }
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == *via_column)
                .ok_or_else(|| crate::Error::new(format!("unknown policy ref {via_column}")))?;
            let FieldKind::Ref {
                table: parent_table,
            } = &field.kind
            else {
                return Err(crate::Error::new(format!(
                    "policy field {} is not a ref",
                    field.name
                )));
            };
            let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
            let policy_sql = if args.child_row_nums.is_some() {
                "1 = 1".to_owned()
            } else {
                visibility.current_policy_sql(table, "current")?
            };
            let row_nums = if let Some(child_row_nums) = args.child_row_nums {
                scoped_policy_parent_row_nums(
                    conn,
                    args.table_name,
                    &ref_column,
                    args.branch_nums,
                    child_row_nums,
                )?
            } else {
                let sql = format!(
                    "SELECT DISTINCT current.{ref_column}
                     FROM {} current
                     JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
                     WHERE current.is_deleted = 0
                       AND {}
                       AND current.{ref_column} IS NOT NULL
                       AND current_tx.outcome != {}
                       AND {policy_sql}",
                    crate::schema::current_table(args.table_name),
                    branch_filter_sql("current", args.branch_nums),
                    tx::OUTCOME_REJECTED,
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, i64>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            };
            let mut records = if args.child_row_nums.is_some() {
                export_history_versions_for_rows(conn, schema, parent_table, Some(&row_nums), None)?
            } else {
                export_visible_table_history(
                    visibility,
                    parent_table,
                    args.branch_nums,
                    Some(&row_nums),
                )?
            };
            records.extend(export_policy_dependency_history(
                visibility,
                PolicyDependencyExport {
                    table_name: parent_table,
                    policy: &schema.table_def(parent_table)?.read_policy,
                    branch_nums: args.branch_nums,
                    child_row_nums: Some(&row_nums),
                },
            )?);
            Ok(records)
        }
    }
}

fn export_group_membership_policy_dependencies(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    let member_row_nums = current_group_member_row_nums_for_groups(
        visibility.conn,
        visibility.user,
        args.branch_nums,
        args.child_row_nums,
    )?;
    export_visible_table_history(
        visibility,
        "group_members",
        args.branch_nums,
        Some(&member_row_nums),
    )
}

fn export_project_membership_policy_dependencies(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyExport<'_>,
    project_row_nums: Option<Vec<i64>>,
) -> Result<Vec<HistoryRecord>> {
    let project_row_nums_ref = project_row_nums.as_deref();
    let project_member_row_nums = current_project_member_row_nums_for_projects(
        visibility.conn,
        args.branch_nums,
        project_row_nums_ref,
    )?;
    let mut records = export_visible_table_history(
        visibility,
        "project_members",
        args.branch_nums,
        Some(&project_member_row_nums),
    )?;
    let group_member_row_nums =
        current_group_member_row_nums_for_user(visibility.conn, visibility.user, args.branch_nums)?;
    records.extend(export_visible_table_history(
        visibility,
        "group_members",
        args.branch_nums,
        Some(&group_member_row_nums),
    )?);
    Ok(records)
}

fn current_group_member_row_nums_for_groups(
    conn: &Connection,
    user: &str,
    branch_nums: &[i64],
    group_row_nums: Option<&[i64]>,
) -> Result<Vec<i64>> {
    if matches!(group_row_nums, Some([])) {
        return Ok(Vec::new());
    }
    let direct_branch_filter = branch_filter_sql("direct", branch_nums);
    let nested_branch_filter = branch_filter_sql("nested", branch_nums);
    let sql = format!(
        "WITH RECURSIVE reachable(group_row_num, member_row_num) AS (
           SELECT direct.group_row_num, direct.row_num
           FROM {} direct
           JOIN jazz_tx direct_tx ON direct_tx.tx_num = direct.visible_tx_num
           WHERE direct.is_deleted = 0
             AND {direct_branch_filter}
             AND direct.user_row_num = (SELECT row_num FROM jazz_row_id WHERE row_id = ?)
             AND direct_tx.outcome != {}
           UNION
           SELECT nested.group_row_num, nested.row_num
           FROM {} nested
           JOIN jazz_tx nested_tx ON nested_tx.tx_num = nested.visible_tx_num
           JOIN reachable parent ON 1 = 1
           WHERE nested.is_deleted = 0
             AND {nested_branch_filter}
             AND nested.member_group_row_num = parent.group_row_num
             AND nested_tx.outcome != {}
         )
         SELECT DISTINCT member_row_num
         FROM reachable",
        crate::schema::current_table("group_members"),
        tx::OUTCOME_REJECTED,
        crate::schema::current_table("group_members"),
        tx::OUTCOME_REJECTED,
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map(params![user], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(row_nums)
}

fn current_group_member_row_nums_for_user(
    conn: &Connection,
    user: &str,
    branch_nums: &[i64],
) -> Result<Vec<i64>> {
    current_group_member_row_nums_for_groups(conn, user, branch_nums, None)
}

fn current_project_member_row_nums_for_projects(
    conn: &Connection,
    branch_nums: &[i64],
    project_row_nums: Option<&[i64]>,
) -> Result<Vec<i64>> {
    if matches!(project_row_nums, Some([])) {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT DISTINCT current.row_num
         FROM {} current
         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
         WHERE current.is_deleted = 0
           AND {}
           AND {}
           AND tx.outcome != {}",
        crate::schema::current_table("project_members"),
        branch_filter_sql("current", branch_nums),
        project_row_nums
            .map(|row_nums| {
                format!(
                    "current.project_row_num IN ({})",
                    row_nums
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_else(|| "1 = 1".to_owned()),
        tx::OUTCOME_REJECTED,
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(row_nums)
}

fn export_policy_dependency_history_for_query_scope(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    export_policy_dependency_history_for_query_scope_at_depth(visibility, args, 0)
}

fn export_policy_dependency_history_for_query_scope_at_depth(
    visibility: &ReadVisibility<'_>,
    args: PolicyDependencyQueryScopeExport<'_>,
    depth: usize,
) -> Result<Vec<HistoryRecord>> {
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(args.table_name)?;
    match args.policy {
        PolicyDef::And(children) | PolicyDef::Or(children) => {
            let mut records = Vec::new();
            for child in children {
                records.extend(export_policy_dependency_history_for_query_scope_at_depth(
                    visibility,
                    PolicyDependencyQueryScopeExport {
                        table_name: args.table_name,
                        policy: child,
                        branch_nums: args.branch_nums,
                        child_scope: args.child_scope,
                    },
                    depth,
                )?);
            }
            return Ok(records);
        }
        PolicyDef::Not(child) => {
            return export_policy_dependency_history_for_query_scope_at_depth(
                visibility,
                PolicyDependencyQueryScopeExport {
                    table_name: args.table_name,
                    policy: child,
                    branch_nums: args.branch_nums,
                    child_scope: args.child_scope,
                },
                depth,
            );
        }
        _ => {}
    }
    let Some(field) = inherited_ref_policy_field(args.policy) else {
        return Ok(Vec::new());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: parent_table,
    } = &field.kind
    else {
        return Err(crate::Error::new(format!(
            "policy field {} is not a ref",
            field.name
        )));
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let child_scope_name = format!("policy_child_scope_{depth}");
    let parent_scope_name = format!("policy_parent_scope_{depth}");
    let mut ctes = args.child_scope.ctes.clone();
    ctes.push(format!(
        "{child_scope_name}(row_num) AS ({})",
        args.child_scope.select_sql
    ));
    ctes.push(format!(
        "{parent_scope_name}(row_num) AS (
           SELECT DISTINCT current.{ref_column}
           FROM {} current
           JOIN {child_scope_name} child_scope ON child_scope.row_num = current.row_num
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
         )",
        crate::schema::current_table(args.table_name),
        branch_filter_sql("current", args.branch_nums),
        tx::OUTCOME_REJECTED,
    ));
    let parent_scope = query::LoweredQueryRowScope {
        ctes,
        select_sql: format!("SELECT row_num FROM {parent_scope_name} WHERE row_num IS NOT NULL"),
        params: args.child_scope.params.clone(),
    };
    let mut records =
        export_history_versions_for_query_scope(conn, schema, parent_table, &parent_scope, None)?;
    records.extend(export_policy_dependency_history_for_query_scope_at_depth(
        visibility,
        PolicyDependencyQueryScopeExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            branch_nums: args.branch_nums,
            child_scope: &parent_scope,
        },
        depth + 1,
    )?);
    Ok(records)
}

fn scoped_policy_parent_row_nums(
    conn: &Connection,
    table_name: &str,
    ref_column: &str,
    branch_nums: &[i64],
    child_row_nums: &[i64],
) -> Result<Vec<i64>> {
    if child_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let mut parent_row_nums = BTreeSet::new();
    let child_row_nums = sorted_unique_row_nums(child_row_nums);
    let mut stmt = conn.prepare(&format!(
        "SELECT current.{ref_column}
         FROM {} current
         JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
         WHERE current.row_num IN ({child_row_nums})
           AND current.is_deleted = 0
           AND {}
           AND current.{ref_column} IS NOT NULL
           AND current_tx.outcome != {}",
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        child_row_nums = integer_list_sql(&child_row_nums),
    ))?;
    let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
    for row in rows {
        parent_row_nums.insert(row?);
    }
    Ok(parent_row_nums.into_iter().collect())
}

fn export_deleted_table_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    let sql = format!(
        "SELECT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.op = {delete_op}
           AND {}
           AND tx.outcome != {}
           AND NOT EXISTS (
             SELECT 1
             FROM {history_table} newer
             JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
             WHERE newer.row_num = h.row_num
               AND newer.j_branch_num = h.j_branch_num
               AND newer_tx.outcome != {}
               AND newer.tx_num > h.tx_num
           )",
        crate::schema::history_table(table_name),
        branch_filter_sql("h", branch_nums),
        tx::OUTCOME_REJECTED,
        tx::OUTCOME_REJECTED,
        delete_op = history_op::DELETE,
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn export_deleted_recursive_descendant_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    let sql = format!(
        "WITH RECURSIVE deleted_tree(row_num) AS (
           SELECT h.row_num
           FROM {history_table} h
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE h.op = {delete_op}
             AND {branch_filter}
             AND h.{parent_column} IN ({parent_row_nums})
             AND tx.outcome != {rejected}
             AND NOT EXISTS (
               SELECT 1
               FROM {history_table} newer
               JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
               WHERE newer.row_num = h.row_num
                 AND newer.j_branch_num = h.j_branch_num
                 AND newer_tx.outcome != {rejected}
                 AND newer.tx_num > h.tx_num
             )
           UNION
           SELECT child.row_num
           FROM {history_table} child
           JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
           JOIN deleted_tree parent ON child.{parent_column} = parent.row_num
           WHERE child.op = {delete_op}
             AND {child_branch_filter}
             AND child_tx.outcome != {rejected}
             AND NOT EXISTS (
               SELECT 1
               FROM {history_table} newer
               JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
               WHERE newer.row_num = child.row_num
                 AND newer.j_branch_num = child.j_branch_num
                 AND newer_tx.outcome != {rejected}
                 AND newer.tx_num > child.tx_num
             )
         )
         SELECT row_num FROM deleted_tree",
        history_table = crate::schema::history_table(table_name),
        branch_filter = branch_filter_sql("h", branch_nums),
        child_branch_filter = branch_filter_sql("child", branch_nums),
        rejected = tx::OUTCOME_REJECTED,
        delete_op = history_op::DELETE,
        parent_row_nums = integer_list_sql(&parent_row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
    for row in rows {
        row_nums.insert(row?);
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn export_recursive_scope_repair_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    parent_field: &str,
    branch_nums: &[i64],
    parent_row_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    if parent_row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let parent_row_nums = sorted_unique_row_nums(parent_row_nums);
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let mut row_nums = BTreeSet::new();
    let sql = format!(
        "WITH RECURSIVE historical_tree(row_num) AS (
           SELECT h.row_num
           FROM {history_table} h
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE {branch_filter}
             AND h.{parent_column} IN ({parent_row_nums})
             AND tx.outcome != {rejected}
           UNION
           SELECT child.row_num
           FROM {history_table} child
           JOIN jazz_tx child_tx ON child_tx.tx_num = child.tx_num
           JOIN historical_tree parent ON child.{parent_column} = parent.row_num
           WHERE {child_branch_filter}
             AND child_tx.outcome != {rejected}
         )
         SELECT DISTINCT row_num FROM historical_tree",
        history_table = crate::schema::history_table(table_name),
        branch_filter = branch_filter_sql("h", branch_nums),
        child_branch_filter = branch_filter_sql("child", branch_nums),
        rejected = tx::OUTCOME_REJECTED,
        parent_row_nums = integer_list_sql(&parent_row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
    for row in rows {
        row_nums.insert(row?);
    }
    let row_nums = row_nums.into_iter().collect::<Vec<_>>();
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn query_scope_repair_row_nums(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<i64>> {
    // Return the physical rows whose history can affect a query-scope refresh.
    // These are not necessarily current result rows.
    //
    // Predicate repair:
    //
    //   history rows ever matching predicate
    //        |
    //        v
    //   exported repair history
    //        |
    //        v
    //   receiver deletes stale current rows no longer justified by history
    //
    // `id` is special because the row id lives in `jazz_row_id`, not the user
    // table history. `$createdBy` is special because it is a system column on
    // history/current tables. User fields lower through `query_predicate`.
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_repair_row_nums(conn, table, field_name, "eq", value);
        }
        return observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect();
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let mut stmt = conn.prepare(&format!(
                "SELECT DISTINCT h.row_num
                 FROM {} h
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                 ORDER BY h.row_num",
                crate::schema::history_table(&table.name)
            ))?;
            let rows = stmt.query_map(params![excluded_id], |row| row.get(0))?;
            return rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        return id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect();
    }
    if field_name == "$createdBy" {
        let Some(created_by) = value.as_str() else {
            return Err(crate::Error::new(
                "$createdBy predicate expects a string value",
            ));
        };
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT h.row_num
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome != ?",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, i64>(0)
        })?;
        return rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    if op == "in" {
        let mut row_nums = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            row_nums.extend(query_scope_repair_row_nums(
                conn, table, field_name, "eq", value,
            )?);
        }
        row_nums.sort();
        row_nums.dedup();
        return Ok(row_nums);
    }
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome != ?",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, i64>(0)
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_scope_rejected_tx_ids(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<String>> {
    if op == "eq_top_created_at_desc" || op == "eq_top_field_desc" {
        let observed_ids = observed_ids_from_query_value(value)?;
        if observed_ids.is_empty() {
            let value = value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top query expects eq value"))?;
            return query_scope_rejected_tx_ids(conn, table, field_name, "eq", value);
        }
        let row_nums = observed_ids
            .into_iter()
            .map(|row_id| row_num(conn, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if op == "in" {
        let mut tx_ids = Vec::new();
        for value in value
            .as_array()
            .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
        {
            tx_ids.extend(query_scope_rejected_tx_ids(
                conn, table, field_name, "eq", value,
            )?);
        }
        tx_ids.sort();
        tx_ids.dedup();
        return Ok(tx_ids);
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let sql = format!(
                "SELECT DISTINCT tx.tx_id
                 FROM {} h
                 JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                 JOIN jazz_row_id ids ON ids.row_num = h.row_num
                 WHERE ids.row_id != ?
                   AND tx.outcome = ?
                 ORDER BY tx.tx_num",
                crate::schema::history_table(&table.name),
            );
            let mut stmt = conn.prepare(&sql)?;
            let tx_ids = stmt.query_map(params![excluded_id, tx::OUTCOME_REJECTED], |row| {
                row.get::<_, String>(0)
            })?;
            return tx_ids
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into);
        }
        let row_nums = id_predicate_values(op, value)?
            .into_iter()
            .map(|row_id| ensure_row_id(conn, &table.name, &row_id))
            .collect::<Result<Vec<_>>>()?;
        return rejected_tx_ids_for_row_nums(conn, &table.name, &row_nums);
    }
    if field_name == "$createdBy" {
        let created_by = value
            .as_str()
            .ok_or_else(|| crate::Error::new("$createdBy predicate expects a string value"))?;
        let created_by_num = match users::user_num(conn, created_by) {
            Ok(user_num) => user_num,
            Err(_) if op == "eq" => return Ok(Vec::new()),
            Err(_) => -1,
        };
        let created_by_sql = match op {
            "eq" => "h.j_created_by = ?",
            "ne" => "h.j_created_by != ?",
            op => {
                return Err(crate::Error::new(format!(
                    "unsupported $createdBy predicate op {op}"
                )));
            }
        };
        let sql = format!(
            "SELECT DISTINCT tx.tx_id
             FROM {} h
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE {created_by_sql}
               AND tx.outcome = ?
             ORDER BY tx.tx_num",
            crate::schema::history_table(&table.name),
        );
        let mut stmt = conn.prepare(&sql)?;
        let tx_ids = stmt.query_map(params![created_by_num, tx::OUTCOME_REJECTED], |row| {
            row.get::<_, String>(0)
        })?;
        return tx_ids
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
        .ok_or_else(|| crate::Error::new(format!("unknown query field {field_name}")))?;
    let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let predicate_sql = query_predicate::sql(field, &format!("h.{predicate_column}"), op)?;
    let predicate_value = query_predicate::value(field, op, value, conn)?;
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {predicate_sql}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(&table.name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![predicate_value, tx::OUTCOME_REJECTED], |row| {
        row.get::<_, String>(0)
    })?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn rejected_tx_ids_for_row_nums(
    conn: &Connection,
    table_name: &str,
    row_nums: &[i64],
) -> Result<Vec<String>> {
    if row_nums.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT DISTINCT tx.tx_id
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {}
           AND tx.outcome = ?
         ORDER BY tx.tx_num",
        crate::schema::history_table(table_name),
        history_row_filter_sql("h", Some(row_nums)),
    );
    let mut stmt = conn.prepare(&sql)?;
    let tx_ids = stmt.query_map(params![tx::OUTCOME_REJECTED], |row| row.get::<_, String>(0))?;
    tx_ids
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn query_scope_repair_row_nums_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Dispatch from the serialized query-read shape used in bundles to the row
    // collection shape used by export. Built queries are stored opaquely, so
    // they need a small adapter before repair candidates can be collected.
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_repair_row_nums_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_repair_row_nums(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

fn query_scope_repair_row_nums_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<i64>> {
    // Built-query repair row collection mirrors `apply_built_query_scope_repair`:
    //
    //   built query
    //       |
    //       +-- one predicate ------------------------+
    //       |                                         v
    //       +-- eq + createdAt desc + limit --> predicate repair rows
    //       |
    //       +-- other SQL-lowered shape ------> SQL-lowered history scope
    //
    // Generic built-query repair asks SQLite for rows whose history matched the
    // query conditions. Export then sends those row histories so peers learn
    // about rows that left a multi-filter or custom-ordered query.
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
    };
    context.repair_row_nums_for_built_query(built_query)
}

fn query_scope_rejected_tx_ids_for_read(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    query_read: &QueryReadRecord,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if query_read.op == "query" {
        let query = built_query_from_read(query_read)?;
        return query_scope_rejected_tx_ids_for_built_query(
            conn,
            schema,
            table,
            &query,
            branch_num,
            user,
            bypass_policy,
        );
    }
    query_scope_rejected_tx_ids(
        conn,
        table,
        &query_read.field,
        &query_read.op,
        &query_read.value,
    )
}

fn query_scope_rejected_tx_ids_for_built_query(
    conn: &Connection,
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    built_query: &BuiltQuery,
    branch_num: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<String>> {
    if built_query.table != table.name {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    let context = query::QueryContext {
        conn,
        schema,
        branch_num,
        user,
        bypass_policy,
    };
    let row_nums = context.repair_row_nums_for_built_query(built_query)?;
    rejected_tx_ids_for_row_nums(conn, &built_query.table, &row_nums)
}

enum BuiltQueryRepairScope<'a> {
    Predicate(&'a QueryCondition),
    Generic,
}

fn built_query_repair_scope(query: &BuiltQuery) -> Result<BuiltQueryRepairScope<'_>> {
    if query.conditions.len() == 1 && query.offset.unwrap_or(0) == 0 {
        let condition = &query.conditions[0];
        match (query.order_by.as_slice(), query.limit) {
            ([], None) if legacy_predicate_repair_supports(condition) => {
                return Ok(BuiltQueryRepairScope::Predicate(condition));
            }
            _ => {}
        }
    }
    Ok(BuiltQueryRepairScope::Generic)
}

fn legacy_predicate_repair_supports(condition: &QueryCondition) -> bool {
    match condition.column.as_str() {
        "id" => matches!(
            condition.op,
            QueryConditionOp::Eq | QueryConditionOp::Ne | QueryConditionOp::In
        ),
        "$createdBy" => matches!(condition.op, QueryConditionOp::Eq | QueryConditionOp::Ne),
        "$createdAt" | "$updatedAt" => false,
        _ => !query_condition_value_contains_null(&condition.value),
    }
}

fn query_condition_value_contains_null(value: &JsonValue) -> bool {
    value.is_null()
        || value
            .as_array()
            .is_some_and(|values| values.iter().any(JsonValue::is_null))
}

fn built_query_repair_keep_query(query: &BuiltQuery) -> Result<BuiltQuery> {
    let offset = query.offset.unwrap_or(0);
    if offset == 0 {
        return Ok(query.clone());
    }

    let mut keep_query = query.clone();
    keep_query.offset = None;
    keep_query.limit = query
        .limit
        .map(|limit| {
            offset
                .checked_add(limit)
                .ok_or_else(|| crate::Error::new("query limit plus offset is too large"))
        })
        .transpose()?;
    Ok(keep_query)
}

fn delete_current_rows_outside_keep_set(
    db: &Connection,
    table_name: &str,
    branch_num: i64,
    scope_row_nums: &[i64],
    keep_row_nums: &[i64],
) -> Result<()> {
    if scope_row_nums.is_empty() {
        return Ok(());
    }

    // Generic window repair is a contraction pass:
    //
    //   rows matching query filters, without LIMIT/OFFSET
    //             |
    //             v
    //   +------------------+        +----------------------+
    //   | scope row nums   |  minus | rows to keep locally |
    //   +------------------+        +----------------------+
    //             |
    //             v
    //   DELETE stale current rows from the observed branch
    //
    // For offset queries, "keep" is the exported support window
    // [0, offset + limit). Those prefix rows must stay local so SQLite can
    // still evaluate the original OFFSET query correctly after the refresh.
    let keep_row_nums = keep_row_nums.iter().copied().collect::<BTreeSet<_>>();
    let delete_row_nums = scope_row_nums
        .iter()
        .copied()
        .filter(|row_num| !keep_row_nums.contains(row_num))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if !delete_row_nums.is_empty() {
        let sql = format!(
            "DELETE FROM {}
             WHERE j_branch_num = ?
               AND is_deleted = 0
               AND row_num IN ({})",
            crate::schema::current_table(table_name),
            integer_list_sql(&delete_row_nums),
        );
        db.execute(&sql, params![branch_num])?;
    }
    Ok(())
}

fn built_query_from_read(read: &QueryReadRecord) -> Result<BuiltQuery> {
    let query = BuiltQuery::from_json_value(read.value.clone())?;
    if read.table != query.table {
        return Err(crate::Error::new(
            "query read table does not match descriptor",
        ));
    }
    Ok(query)
}

fn id_predicate_values(op: &str, value: &JsonValue) -> Result<Vec<String>> {
    match op {
        "eq" => value
            .as_str()
            .map(|row_id| vec![row_id.to_owned()])
            .ok_or_else(|| crate::Error::new("id equality expects a string value")),
        "in" => value
            .as_array()
            .ok_or_else(|| crate::Error::new("id in expects an array value"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| crate::Error::new("id in expects string values"))
            })
            .collect(),
        _ => Err(crate::Error::new(format!("unsupported id predicate {op}"))),
    }
}

fn dedupe_history_records(records: &mut Vec<HistoryRecord>) {
    let mut seen = BTreeSet::new();
    records.retain(|record| {
        seen.insert((
            record.table.clone(),
            record.row_id.clone(),
            record.branch_id.clone(),
            record.tx_id.clone(),
            record.op,
        ))
    });
}

fn export_visible_table_history(
    visibility: &ReadVisibility<'_>,
    table_name: &str,
    branch_nums: &[i64],
    row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
    }
    let conn = visibility.conn;
    let schema = visibility.schema;
    let table = schema.table_def(table_name)?;
    let policy_sql = visibility.current_policy_sql(table, "current")?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND EXISTS (
           SELECT 1
           FROM {} current
           JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
           WHERE current.row_num = h.row_num
             AND current.j_branch_num = h.j_branch_num
             AND current.is_deleted = 0
             AND {}
             AND current_tx.outcome != {}
             AND {policy_sql}
         )
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        crate::schema::current_table(table_name),
        branch_filter_sql("current", branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = row_filter_sql(row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut records = Vec::new();
    let mut rows = stmt.query([])?;
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn export_history_versions_for_rows(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        None,
    )
}

fn export_history_versions_for_rows_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_rows_with_branch_filter(
        conn,
        schema,
        table_name,
        row_nums,
        max_global_epoch,
        Some(branch_nums),
    )
}

fn export_history_versions_for_query_scope(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        None,
    )
}

fn export_history_versions_for_query_scope_in_branches(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: &[i64],
) -> Result<Vec<HistoryRecord>> {
    export_history_versions_for_query_scope_with_branch_filter(
        conn,
        schema,
        table_name,
        row_scope,
        max_global_epoch,
        Some(branch_nums),
    )
}

fn export_history_versions_for_query_scope_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_scope: &query::LoweredQueryRowScope,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "{}
         SELECT {}
         FROM {} h
         JOIN query_scope scope ON scope.row_num = h.row_num
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        row_scope.with_scope_cte("query_scope"),
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = stmt.query(params_from_iter(row_scope.params.iter()))?;
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn export_history_versions_for_rows_with_branch_filter(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    row_nums: Option<&[i64]>,
    max_global_epoch: Option<i64>,
    branch_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    if let Some(row_nums) = row_nums {
        if row_nums.is_empty() {
            return Ok(Vec::new());
        }
    }
    let table = schema.table_def(table_name)?;
    let field_columns = table
        .fields
        .iter()
        .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
        .collect::<Vec<_>>();
    let mut select_columns = vec![
        "ids.row_id".to_owned(),
        "branch.branch_id".to_owned(),
        "tx.tx_id".to_owned(),
        "h.op".to_owned(),
    ];
    select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
    select_columns.extend([
        "h.j_created_at".to_owned(),
        "h.j_updated_at".to_owned(),
        format!(
            "{} AS j_created_by",
            users::user_id_expr("h", "j_created_by")
        ),
        format!(
            "{} AS j_updated_by",
            users::user_id_expr("h", "j_updated_by")
        ),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND {branch_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        row_filter = row_filter_sql(row_nums),
        branch_filter = history_branch_filter_sql("h", branch_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = stmt.query([])?;
    let mut records = Vec::new();
    let mut public_row_id_cache = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json_cached(conn, field, &row[idx + 4], &mut public_row_id_cache)?,
            );
        }
        let sys = 4 + table.fields.len();
        records.push(HistoryRecord {
            table: table_name.to_owned(),
            row_id: text_value(&row[0], "row_id")?,
            branch_id: text_value(&row[1], "branch_id")?,
            tx_id: text_value(&row[2], "tx_id")?,
            op: integer_value(&row[3], "op")?,
            values,
            created_at: integer_value(&row[sys], "j_created_at")?,
            updated_at: integer_value(&row[sys + 1], "j_updated_at")?,
            created_by: text_value(&row[sys + 2], "j_created_by")?,
            updated_by: text_value(&row[sys + 3], "j_updated_by")?,
        });
    }
    Ok(records)
}

fn history_epoch_filter_sql(max_global_epoch: Option<i64>) -> String {
    match max_global_epoch {
        Some(epoch) => format!("tx.global_epoch IS NOT NULL AND tx.global_epoch <= {epoch}"),
        None => "1 = 1".to_owned(),
    }
}

fn row_filter_sql(row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!("h.row_num IN ({})", integer_list_sql(row_nums)),
        None => "1 = 1".to_owned(),
    }
}

fn history_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!("{alias}.row_num IN ({})", integer_list_sql(row_nums)),
        None => "1 = 1".to_owned(),
    }
}

fn history_branch_filter_sql(alias: &str, branch_nums: Option<&[i64]>) -> String {
    match branch_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(branch_nums) => format!(
            "{alias}.j_branch_num IN ({})",
            integer_list_sql(branch_nums)
        ),
        None => "1 = 1".to_owned(),
    }
}

fn branch_filter_sql(alias: &str, branch_nums: &[i64]) -> String {
    if branch_nums.is_empty() {
        return "0 = 1".to_owned();
    }
    format!(
        "{alias}.j_branch_num IN ({})",
        integer_list_sql(branch_nums)
    )
}

fn integer_list_sql(values: &[i64]) -> String {
    values
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn sorted_unique_row_nums(row_nums: &[i64]) -> Vec<i64> {
    let mut row_nums = row_nums.to_vec();
    row_nums.sort();
    row_nums.dedup();
    row_nums
}

fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    let mut public_row_id_cache = BTreeMap::new();
    sql_value_to_json_cached(conn, field, value, &mut public_row_id_cache)
}

fn sql_value_to_json_cached(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
    public_row_id_cache: &mut BTreeMap<i64, String>,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (_, rusqlite::types::Value::Null) if field.nullable => Ok(JsonValue::Null),
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => Ok(JsonValue::String(
            cached_public_row_id(conn, public_row_id_cache, *row_num)?,
        )),
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

fn cached_public_row_id(
    conn: &Connection,
    cache: &mut BTreeMap<i64, String>,
    row_num: i64,
) -> Result<String> {
    if let Some(row_id) = cache.get(&row_num) {
        return Ok(row_id.clone());
    }
    let row_id = public_row_id(conn, row_num)?;
    cache.insert(row_num, row_id.clone());
    Ok(row_id)
}

fn cached_ensure_row_id(
    conn: &Connection,
    cache: &mut BTreeMap<(String, String), i64>,
    table: &str,
    row_id: &str,
) -> Result<i64> {
    let key = (table.to_owned(), row_id.to_owned());
    if let Some(row_num) = cache.get(&key) {
        return Ok(*row_num);
    }
    let row_num = ensure_row_id(conn, table, row_id)?;
    cache.insert(key, row_num);
    Ok(row_num)
}

fn cached_ensure_row_id_with_status(
    conn: &Connection,
    cache: &mut BTreeMap<(String, String), i64>,
    created_in_apply: &mut BTreeSet<i64>,
    table: &str,
    row_id: &str,
) -> Result<i64> {
    let key = (table.to_owned(), row_id.to_owned());
    if let Some(row_num) = cache.get(&key) {
        return Ok(*row_num);
    }
    let (row_num, created) = ensure_row_id_with_status(conn, row_id)?;
    if created {
        created_in_apply.insert(row_num);
    }
    cache.insert(key, row_num);
    Ok(row_num)
}

fn cached_ensure_user(
    conn: &Connection,
    cache: &mut BTreeMap<String, i64>,
    user_id: &str,
) -> Result<i64> {
    if let Some(user_num) = cache.get(user_id) {
        return Ok(*user_num);
    }
    let user_num = users::ensure_user(conn, user_id)?;
    cache.insert(user_id.to_owned(), user_num);
    Ok(user_num)
}

fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}

fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

fn observed_row_ids(rows: &[RowView]) -> Vec<String> {
    rows.iter().map(|row| row.id.clone()).collect()
}

fn observed_ids_from_query_value(value: &JsonValue) -> Result<Vec<String>> {
    let Some(observed_ids) = value.get("observed_ids") else {
        return Ok(Vec::new());
    };
    observed_ids
        .as_array()
        .ok_or_else(|| crate::Error::new("observed_ids expects an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| crate::Error::new("observed_ids expects string row ids"))
        })
        .collect()
}

fn plan_query_read_refreshes(
    current_branch_id: &str,
    reads: &[QueryReadRecord],
) -> Result<Vec<QueryRefreshPlan>> {
    let mut plans = Vec::new();

    for read in reads {
        if read.branch_id == current_branch_id
            && matches!(read.op.as_str(), "eq" | "ne" | "contains" | "in")
        {
            if let Some(values) = plans.iter_mut().find_map(|plan| match plan {
                QueryRefreshPlan::Predicate {
                    table,
                    field,
                    op,
                    values,
                } if table == &read.table && field == &read.field && op == &read.op => Some(values),
                _ => None,
            }) {
                values.push((read.value.clone(), Vec::new()));
            } else {
                plans.push(QueryRefreshPlan::Predicate {
                    table: read.table.clone(),
                    field: read.field.clone(),
                    op: read.op.clone(),
                    values: vec![(read.value.clone(), Vec::new())],
                });
            }
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "recursive_refs" {
            let Some(root_id) = read.value.as_str() else {
                return Err(crate::Error::new("recursive refs expects root id string"));
            };
            if let Some(root_ids) = plans.iter_mut().find_map(|plan| match plan {
                QueryRefreshPlan::RecursiveRefs {
                    table,
                    field,
                    root_ids,
                } if table == &read.table && field == &read.field => Some(root_ids),
                _ => None,
            }) {
                root_ids.push(root_id.to_owned());
            } else {
                plans.push(QueryRefreshPlan::RecursiveRefs {
                    table: read.table.clone(),
                    field: read.field.clone(),
                    root_ids: vec![root_id.to_owned()],
                });
            }
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "eq_top_created_at_desc" {
            let value = read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            let limit = limit as usize;
            if let Some(values) = plans.iter_mut().find_map(|plan| match plan {
                QueryRefreshPlan::TopCreatedAt {
                    table,
                    field,
                    values,
                    limit: plan_limit,
                } if table == &read.table && field == &read.field && *plan_limit == limit => {
                    Some(values)
                }
                _ => None,
            }) {
                values.push((value.clone(), observed_ids_from_query_value(&read.value)?));
            } else {
                plans.push(QueryRefreshPlan::TopCreatedAt {
                    table: read.table.clone(),
                    field: read.field.clone(),
                    values: vec![(value.clone(), observed_ids_from_query_value(&read.value)?)],
                    limit,
                });
            }
            continue;
        }
        if read.branch_id == current_branch_id && read.op == "eq_top_field_desc" {
            let value = read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
            let order_field = read
                .value
                .get("order_field")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
            let limit = read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
            let limit = limit as usize;
            if let Some(values) = plans.iter_mut().find_map(|plan| match plan {
                QueryRefreshPlan::TopField {
                    table,
                    field,
                    values,
                    order_field: plan_order_field,
                    limit: plan_limit,
                } if table == &read.table
                    && field == &read.field
                    && plan_order_field == order_field
                    && *plan_limit == limit =>
                {
                    Some(values)
                }
                _ => None,
            }) {
                values.push((value.clone(), observed_ids_from_query_value(&read.value)?));
            } else {
                plans.push(QueryRefreshPlan::TopField {
                    table: read.table.clone(),
                    field: read.field.clone(),
                    values: vec![(value.clone(), observed_ids_from_query_value(&read.value)?)],
                    order_field: order_field.to_owned(),
                    limit,
                });
            }
            continue;
        }
        plans.push(QueryRefreshPlan::Single(read.clone()));
    }

    Ok(plans)
}

fn bundle_policy_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}

fn bundle_touched_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for record in &bundle.reads {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}

fn scoped_policy_fingerprint(
    schema: &SchemaDef,
    history: &[HistoryRecord],
    query_reads: &[QueryReadRecord],
) -> String {
    let mut tables = BTreeSet::new();
    for record in history {
        tables.insert(record.table.clone());
    }
    for query_read in query_reads {
        tables.insert(query_read.table.clone());
    }
    schema.policy_fingerprint_for_tables(tables.iter())
}

fn make_bundle(
    schema: &SchemaDef,
    branches: Vec<BranchRecord>,
    txs: Vec<TxRecord>,
    reads: Vec<ReadRecord>,
    query_reads: Vec<QueryReadRecord>,
    history: Vec<HistoryRecord>,
) -> Bundle {
    Bundle {
        protocol_version: BUNDLE_PROTOCOL_VERSION,
        schema_fingerprint: schema.compatibility_fingerprint(),
        policy_fingerprint: scoped_policy_fingerprint(schema, &history, &query_reads),
        branches,
        txs,
        reads,
        query_reads,
        history,
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::limits::Limit;

    #[test]
    fn generic_built_query_export_uses_sql_scope_under_low_variable_limit() -> Result<()> {
        const ROW_COUNT: usize = 120;

        let schema = SchemaDef::new().table("tasks", |table| {
            table.text("title");
            table.bool("done");
        });
        let mut upstream = Runtime::open_trusted_with_schema(
            Storage::Memory,
            "low-limit-upstream",
            schema.clone(),
        )?;
        let mut peer =
            Runtime::open_with_schema(Storage::Memory, "low-limit-peer", "alice", schema)?;

        upstream
            .conn
            .set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 32)?;

        for index in 0..ROW_COUNT {
            upstream.insert_row(
                "tasks",
                &format!("task-{index:03}"),
                BTreeMap::from([
                    ("title".to_owned(), json!(format!("Task {index:03}"))),
                    ("done".to_owned(), json!(false)),
                ]),
            )?;
        }

        let query = BuiltQuery::from_json_value(json!({
            "table": "tasks",
            "conditions": [{"column": "done", "op": "eq", "value": false}],
            "orderBy": [["$createdAt", "desc"]]
        }))?;

        peer.apply_bundle(&upstream.export_query(query.clone())?)?;

        assert_eq!(peer.query(query)?.len(), ROW_COUNT);
        Ok(())
    }
}
