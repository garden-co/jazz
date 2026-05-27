use crate::rows::{ensure_row_id, public_row_id, row_num};
use crate::schema::{FieldDef, FieldKind, PolicyDef, SchemaDef};
use crate::subscription::{RejectionSubscription, RowsSubscription, RowsSubscriptionQuery};
use crate::sync::{
    BranchRecord, Bundle, HistoryRecord, QueryReadRecord, ReadRecord, TxRecord,
    BUNDLE_PROTOCOL_VERSION,
};
use crate::types::{BranchInfo, RejectionInfo, RowView, StorageStats, TransactionInfo};
use crate::{
    branch, effective, policy, projection, query, query_predicate, read_set, schema, stats,
    storage, tx, Result, Storage,
};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Runtime {
    conn: Connection,
    schema: SchemaDef,
    node_id: String,
    auth: RuntimeAuth,
    node_num: i64,
    branch_num: i64,
}

struct AwaitingDependencyTx {
    tx_num: i64,
    tx_id: String,
    auth_user: String,
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
        self.write_row(table_name, id, values, 1)
    }

    pub fn update_row(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        self.physical_row_num_for(id)?;
        self.write_row(table_name, id, values, 2)
    }

    pub fn resolve_row_conflict(
        &mut self,
        table_name: &str,
        id: &str,
        values: BTreeMap<String, JsonValue>,
    ) -> Result<String> {
        let op = if self.row_has_current_branch_value(table_name, id)? {
            2
        } else {
            1
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
            values_are_effective: false,
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
        let mut rows = self.read_rows_where_eq(table_name, field_name, value)?;
        let created_at_by_id = current_created_at_by_row_id(&self.conn, table_name)?;
        rows.sort_by(|left, right| {
            created_at_by_id
                .get(&right.id)
                .cmp(&created_at_by_id.get(&left.id))
                .then_with(|| left.id.cmp(&right.id))
        });
        rows.truncate(limit);
        Ok(rows)
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
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let rows = self.read_recursive_refs(table_name, root_id, parent_field)?;
        let row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let txs = export_txs(&self.conn)?;
        let mut history = export_visible_table_history(
            &self.conn,
            &self.schema,
            table_name,
            user,
            bypass_policy,
            &branch_nums,
            Some(&row_nums),
        )?;
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
            &self.conn,
            &self.schema,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                user,
                bypass_policy,
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
                    &self.conn,
                    &self.schema,
                    table_name,
                    user,
                    bypass_policy,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
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

    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        self.apply_bundle_inner(bundle, true)
    }

    fn apply_bundle_inner(
        &mut self,
        bundle: &Bundle,
        check_policy_fingerprint: bool,
    ) -> Result<()> {
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
        let schema = self.schema.clone();
        let db = self.conn.transaction()?;
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
        for tx_record in &bundle.txs {
            let node_num = tx::ensure_node(&db, &tx_record.node_id)?;
            let metadata_json = tx_metadata_json(tx_record.auth_user.as_deref())?;
            db.execute(
                "INSERT INTO jazz_tx
                 (tx_id, node_num, local_epoch, global_epoch, kind, conflict_mode, outcome, created_at, metadata_json)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(tx_id) DO UPDATE SET
                   outcome = MAX(jazz_tx.outcome, excluded.outcome),
                   global_epoch = COALESCE(excluded.global_epoch, jazz_tx.global_epoch),
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
            if tx_record.outcome == tx::OUTCOME_REJECTED {
                if let Some(code) = &tx_record.rejection_code {
                    let tx_num = tx::tx_num(&db, &tx_record.tx_id)?;
                    let detail_json = encode_optional_json(tx_record.rejection_detail.as_ref())?;
                    db.execute(
                        "INSERT OR REPLACE INTO jazz_tx_rejection (tx_num, code, detail_json)
                         VALUES (?, ?, ?)",
                        params![tx_num, code, detail_json],
                    )?;
                }
            }
            if let Some(global_epoch) = tx_record.global_epoch {
                let tx_num = tx::tx_num(&db, &tx_record.tx_id)?;
                db.execute(
                    "INSERT OR REPLACE INTO jazz_tx_receipt
                     (tx_num, tier, observed_at, receipt_json)
                     VALUES (?, ?, ?, '{}')",
                    params![tx_num, tx::TIER_GLOBAL, global_epoch],
                )?;
            }
            for tier in &tx_record.receipt_tiers {
                let tx_num = tx::tx_num(&db, &tx_record.tx_id)?;
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
        for read_record in &bundle.reads {
            let tx_num = tx::tx_num(&db, &read_record.tx_id)?;
            let row_num = ensure_row_id(&db, &read_record.table, &read_record.row_id)?;
            let observed_tx_num = read_record
                .observed_tx_id
                .as_deref()
                .map(|observed_tx_id| tx::tx_num(&db, observed_tx_id))
                .transpose()?;
            read_set::record_tx_read_with_observed(
                &db,
                tx_num,
                &read_record.table,
                row_num,
                read_record.reason,
                observed_tx_num,
            )?;
        }
        for table in schema.tables() {
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE visible_tx_num IN (
                       SELECT tx_num FROM jazz_tx WHERE outcome = ?
                     )",
                    crate::schema::current_table(&table.name)
                ),
                params![tx::OUTCOME_REJECTED],
            )?;
        }
        for query_read in &bundle.query_reads {
            Self::record_query_read(&db, query_read)?;
            Self::apply_query_scope_repair(&schema, &db, query_read)?;
        }
        for record in &bundle.history {
            Self::apply_history_record(&schema, &db, self.node_num, record)?;
        }
        for query_read in &bundle.query_reads {
            Self::apply_query_scope_repair(&schema, &db, query_read)?;
        }
        db.commit()?;
        self.revalidate_awaiting_dependencies()?;
        Ok(())
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
        reads
            .iter()
            .map(|read| self.export_query_read_refresh(read))
            .collect()
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
                self.export_query_where_eq_top_created_at_desc(
                    &read.table,
                    &read.field,
                    value.clone(),
                    limit as usize,
                )
            }
            "absent" => {
                if read.field == "id" {
                    let Some(row_id) = read.value.as_str() else {
                        return Err(crate::Error::new("absent id expects string value"));
                    };
                    if self
                        .read_rows_where_eq(
                            &read.table,
                            &read.field,
                            JsonValue::String(row_id.to_owned()),
                        )?
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
    ) -> Result<()> {
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
                Self::apply_query_scope_repair(schema, db, &eq_read)?;
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
                           AND row_num IN (
                             SELECT row_num FROM jazz_row_id
                             WHERE table_name = ? AND row_id != ?
                           )
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_row_id ids ON ids.row_num = h.row_num
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE ids.table_name = ?
                               AND ids.row_id != ?
                               AND h.j_branch_num = ?
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        current_table = crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                    ),
                    params![
                        branch_num,
                        query_read.table,
                        excluded_id,
                        query_read.table,
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
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
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
                           AND h.op != 3
                           AND tx.outcome != ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    history_table = crate::schema::history_table(&query_read.table),
                ),
                params![
                    branch_num,
                    created_by,
                    branch_num,
                    created_by,
                    tx::OUTCOME_REJECTED
                ],
            )?;
            return Ok(());
        }
        if query_read.table == "todos"
            && query_read.field == "done"
            && query_read.op == "top_created_at_desc"
        {
            let Some(limit) = query_read.value.as_u64() else {
                return Err(crate::Error::new(
                    "top_created_at_desc expects numeric limit",
                ));
            };
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND row_num NOT IN (
                         SELECT todo.row_num
                         FROM {current_table} todo
                         JOIN jazz_tx todo_tx ON todo_tx.tx_num = todo.visible_tx_num
                         WHERE todo.j_branch_num = ?
                           AND todo.is_deleted = 0
                           AND todo.done = 0
                           AND todo_tx.outcome != ?
                         ORDER BY todo.j_created_at DESC, todo.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table("todos"),
                    current_table = crate::schema::current_table("todos"),
                ),
                params![branch_num, branch_num, tx::OUTCOME_REJECTED, limit as i64],
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
                     WHERE ids.table_name = ?
                       AND h.j_branch_num = ?
                       AND h.op != 3
                       AND tx.outcome != ?
                       AND {history_predicate_sql}
                   )",
                crate::schema::current_table(&query_read.table),
                history_table = crate::schema::history_table(&query_read.table),
                history_predicate_sql =
                    query_predicate::sql(field, &format!("h.{predicate_column}"), &query_read.op)?,
            ),
            params![
                branch_num,
                predicate_value.clone(),
                query_read.table,
                branch_num,
                tx::OUTCOME_REJECTED,
                predicate_value
            ],
        )?;
        Ok(())
    }

    fn apply_history_record(
        schema: &SchemaDef,
        db: &Connection,
        local_node_num: i64,
        record: &HistoryRecord,
    ) -> Result<()> {
        let table = schema.table_def(&record.table)?;
        let row_num = ensure_row_id(db, &record.table, &record.row_id)?;
        let tx_num = tx::tx_num(db, &record.tx_id)?;
        let branch_num = branch::ensure(db, &record.branch_id, None, now_ms())?;

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
                |ref_table, row_id| ensure_row_id(db, ref_table, row_id),
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        values.extend([
            rusqlite::types::Value::Integer(record.created_at),
            rusqlite::types::Value::Integer(record.updated_at),
            rusqlite::types::Value::Text(record.created_by.clone()),
            rusqlite::types::Value::Text(record.updated_by.clone()),
        ]);
        insert_dynamic(
            db,
            &crate::schema::history_table(&record.table),
            &columns,
            &values,
        )?;
        record_tx_write(db, tx_num, &record.table, row_num, record.op)?;

        let outcome = tx_outcome(db, tx_num)?;
        if outcome == tx::OUTCOME_PENDING && tx_conflict_mode(db, tx_num)? == tx::MODE_EXCLUSIVE {
            return Ok(());
        }
        if tx_is_remote_pending(db, tx_num, local_node_num)?
            && durable_version_exists_for_row(db, &record.table, row_num, branch_num)?
        {
            return Ok(());
        }
        if outcome != tx::OUTCOME_REJECTED
            && !is_newest_version_for_current(db, &record.table, row_num, branch_num, tx_num)?
        {
            return Ok(());
        }
        if outcome != tx::OUTCOME_REJECTED && record.op == 3 {
            db.execute(
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
                    db,
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
                db,
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
            "SELECT writes.table_name, ids.row_id
             FROM jazz_tx_write writes
             JOIN jazz_tx tx ON tx.tx_num = writes.tx_num
             JOIN jazz_row_id ids ON ids.row_num = writes.row_num
             WHERE tx.tx_id = ?
             ORDER BY writes.table_name, ids.row_id",
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
            "SELECT reads.table_name, ids.row_id, observed.tx_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
             WHERE tx.tx_id = ?
             ORDER BY reads.table_name, ids.row_id",
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
            "SELECT reads.table_name, ids.row_id
             FROM jazz_tx_read reads
             JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
             JOIN jazz_row_id ids ON ids.row_num = reads.row_num
             WHERE tx.tx_id = ?
               AND reads.reason = ?
             ORDER BY reads.table_name, ids.row_id",
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
        let start = self.transaction_start().map_err(|err| err.to_string());
        TransactionBuilder {
            runtime: self,
            mutations: Vec::new(),
            mode: TransactionMode::Mergeable,
            start,
        }
    }

    fn transaction_start(&self) -> Result<TransactionStart> {
        Ok(TransactionStart {
            vector: transaction_dotted_vector(&self.conn)?,
            branch_num: self.branch_num,
            scope_depths: branch::scope_depths(&self.conn, self.branch_num)?,
        })
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
        record_policy_read_set_for_write(
            &db,
            &self.schema,
            &table,
            &table.write_policy,
            &visible_row.values,
            self.branch_num,
            tx_num,
        )?;
        let allowed = bypass_policy
            || local_write_allowed(LocalWriteCheck {
                db: &db,
                schema: &self.schema,
                table: &table,
                row_num,
                branch_num: self.branch_num,
                values: &visible_row.values,
                user: &user,
                op: 3,
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
            "3".to_owned(),
        ];
        select_columns.extend(field_columns.iter().cloned());
        select_columns.extend([
            "j_created_at".to_owned(),
            "?".to_owned(),
            "j_created_by".to_owned(),
            "?".to_owned(),
        ]);
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
            params![tx_num, now, user, row_num, self.branch_num],
        )?;
        if inserted == 0 {
            let mut values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(self.branch_num),
                rusqlite::types::Value::Integer(3),
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
                rusqlite::types::Value::Text(user.to_owned()),
                rusqlite::types::Value::Text(user.to_owned()),
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
                rusqlite::types::Value::Text(user.to_owned()),
                rusqlite::types::Value::Text(user.to_owned()),
            ]);
            insert_dynamic(
                &db,
                &crate::schema::current_table(&table.name),
                &current_columns,
                &current_values,
            )?;
        }
        record_tx_write(&db, tx_num, &table.name, row_num, 3)?;
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
               AND h.op = 3
               AND tx.outcome != ?
             ORDER BY tx.global_epoch DESC NULLS LAST, h.tx_num DESC
             LIMIT 1",
            field_columns.join(", "),
            crate::schema::history_table(table_name)
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
        self.write_row(table_name, id, values, 1)
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

    pub fn read_rows_where_eq(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        self.query_context()
            .read_rows_where_eq(table_name, field_name, value)
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
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value.clone(),
            self.read_rows_where_eq(table_name, field_name, value)?,
            &[],
        )
    }

    pub fn export_query_where_eq_with_ref_include(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        ref_field_name: &str,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "eq",
            value.clone(),
            self.read_rows_where_eq(table_name, field_name, value)?,
            &[ref_field_name],
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
            &[],
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
            &[],
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
            &[],
        )
    }

    pub fn export_query_where_eq_top_created_at_desc(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
        limit: usize,
    ) -> Result<Bundle> {
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
            }),
            self.read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)?,
            &[],
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
        self.export_query_scope(
            table_name,
            field_name,
            "eq_top_created_at_desc",
            json!({
                "eq": value.clone(),
                "limit": limit,
            }),
            self.read_rows_where_eq_top_created_at_desc(table_name, field_name, value, limit)?,
            &[ref_field_name],
        )
    }

    fn export_query_scope(
        &self,
        table_name: &str,
        field_name: &str,
        op: &str,
        value: JsonValue,
        rows: Vec<RowView>,
        ref_include_fields: &[&str],
    ) -> Result<Bundle> {
        let table = self.schema.table_def(table_name)?;
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
        let mut row_nums = rows
            .iter()
            .map(|row| row_num(&self.conn, &row.id))
            .collect::<Result<Vec<_>>>()?;
        row_nums.extend(query_scope_repair_row_nums(
            &self.conn, table, field_name, op, &value,
        )?);
        row_nums.sort();
        row_nums.dedup();
        let branch_nums = branch::scope_nums(&self.conn, self.branch_num)?;
        let txs = export_txs(&self.conn)?;
        let mut history = export_visible_table_history(
            &self.conn,
            &self.schema,
            table_name,
            user,
            bypass_policy,
            &branch_nums,
            Some(&row_nums),
        )?;
        history.extend(export_history_versions_for_rows(
            &self.conn,
            &self.schema,
            table_name,
            Some(&row_nums),
            None,
        )?);
        history.extend(export_policy_dependency_history(
            &self.conn,
            &self.schema,
            PolicyDependencyExport {
                table_name,
                policy: &self.schema.table_def(table_name)?.read_policy,
                user,
                bypass_policy,
                branch_nums: &branch_nums,
                child_row_nums: Some(&row_nums),
            },
        )?);
        for ref_field_name in ref_include_fields {
            history.extend(self.export_ref_include_history(
                table,
                &rows,
                ref_field_name,
                &branch_nums,
            )?);
        }
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
                    &self.conn,
                    &self.schema,
                    table_name,
                    user,
                    bypass_policy,
                    base_epoch,
                    Some(&row_nums),
                )?);
            }
        }
        dedupe_history_records(&mut history);
        let reads = export_reads_for_history(&self.conn, &history)?;
        let mut branches = export_branch_records_for_history(&self.conn, &history)?;
        include_branch_record(&self.conn, &mut branches, self.branch_num)?;
        let query_reads = vec![QueryReadRecord {
            branch_id: branch_id_for_num(&self.conn, self.branch_num)?,
            table: table_name.to_owned(),
            field: field_name.to_owned(),
            op: op.to_owned(),
            value,
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

    fn export_ref_include_history(
        &self,
        table: &crate::schema::TableDef,
        rows: &[RowView],
        ref_field_name: &str,
        branch_nums: &[i64],
    ) -> Result<Vec<HistoryRecord>> {
        let user = self.policy_user();
        let bypass_policy = self.bypasses_policy();
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
        let mut history = export_visible_table_history(
            &self.conn,
            &self.schema,
            ref_table_name,
            user,
            bypass_policy,
            branch_nums,
            Some(&ref_row_nums),
        )?;
        history.extend(export_history_versions_for_rows(
            &self.conn,
            &self.schema,
            ref_table_name,
            Some(&ref_row_nums),
            None,
        )?);
        history.extend(export_policy_dependency_history(
            &self.conn,
            &self.schema,
            PolicyDependencyExport {
                table_name: ref_table_name,
                policy: &self.schema.table_def(ref_table_name)?.read_policy,
                user,
                bypass_policy,
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
        Ok(RowsSubscription::where_eq(
            table_name,
            field_name,
            value.clone(),
            self.read_rows_where_eq(table_name, field_name, value)?,
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

    pub fn read_rows_where_eq_with_conflict_meta(
        &self,
        table_name: &str,
        field_name: &str,
        value: JsonValue,
    ) -> Result<Vec<RowView>> {
        if field_name == "id" {
            let Some(id) = value.as_str() else {
                return Err(crate::Error::new("id equality expects a string"));
            };
            return Ok(self
                .read_rows_with_conflict_meta(table_name)?
                .into_iter()
                .filter(|row| row.id == id)
                .collect());
        }
        self.schema.table_def(table_name)?;
        Ok(self
            .read_rows_with_conflict_meta(table_name)?
            .into_iter()
            .filter(|row| row.values.get(field_name) == Some(&value))
            .collect())
    }

    fn row_has_current_branch_value(&self, table_name: &str, id: &str) -> Result<bool> {
        self.schema.table_def(table_name)?;
        let row_num = row_num(&self.conn, id)?;
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
                self.read_rows_where_eq(&query.table, &query.field, query.value.clone())?
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
            RowsSubscriptionQuery::Predicate(query) => {
                return Err(crate::Error::new(format!(
                    "unsupported subscription query {}",
                    query.op
                )));
            }
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

    fn query_context(&self) -> query::QueryContext<'_> {
        query::QueryContext {
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
    values_are_effective: bool,
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
    if args.op == 1 {
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
    let row_num = ensure_row_id(args.db, args.table_name, args.id)?;
    let effective_values = if args.values_are_effective {
        args.values.clone()
    } else {
        effective_write_values(EffectiveWriteValues {
            db: args.db,
            schema: args.schema,
            table_name: args.table_name,
            id: args.id,
            row_num,
            branch_num: args.branch_num,
            patch_values: args.values,
            op: args.op,
        })?
    };
    if args.op == 1 {
        read_set::record_tx_create_read(
            args.db,
            args.tx_num,
            args.table_name,
            row_num,
            args.branch_num,
        )?;
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
    record_policy_read_set_for_write(
        args.db,
        args.schema,
        table,
        &table.write_policy,
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
            values: &effective_values,
            user: args.user,
            op: args.op,
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
    let (created_at, created_by) = if args.op == 1 {
        (args.now, args.user.to_owned())
    } else {
        current_creation_metadata(args.db, &table.name, row_num, args.branch_num)?
            .unwrap_or((args.now, args.user.to_owned()))
    };
    sql_values.extend([
        rusqlite::types::Value::Integer(created_at),
        rusqlite::types::Value::Integer(args.now),
        rusqlite::types::Value::Text(created_by),
        rusqlite::types::Value::Text(args.user.to_owned()),
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

fn transaction_effective_values(
    table: &crate::schema::TableDef,
    base: Option<&RowView>,
    patch_values: &BTreeMap<String, JsonValue>,
    op: i64,
    id: &str,
) -> Result<BTreeMap<String, JsonValue>> {
    if op == 1 {
        let mut values = patch_values.clone();
        for field in &table.fields {
            if !values.contains_key(&field.name) {
                if let Some(default_value) = &field.default_value {
                    values.insert(field.name.clone(), default_value.clone());
                }
            }
        }
        return Ok(values);
    }
    let mut values = base
        .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))?
        .values
        .clone();
    values.extend(patch_values.clone());
    Ok(values)
}

fn transaction_rows_by_id(rows: Vec<RowView>) -> BTreeMap<String, Vec<RowView>> {
    let mut rows_by_id = BTreeMap::<String, Vec<RowView>>::new();
    for row in rows {
        rows_by_id.entry(row.id.clone()).or_default().push(row);
    }
    rows_by_id
}

fn single_transaction_row<'a>(
    rows: Option<&'a Vec<RowView>>,
    id: &str,
) -> Result<Option<&'a RowView>> {
    let Some(rows) = rows else {
        return Ok(None);
    };
    if rows.len() > 1 {
        return Err(crate::Error::new("ambiguous branch row source candidates"));
    }
    rows.first()
        .map(Some)
        .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))
}

fn take_single_transaction_row(rows: Option<Vec<RowView>>, id: &str) -> Result<RowView> {
    let Some(mut rows) = rows else {
        return Err(crate::Error::new(format!("row {id} is not visible")));
    };
    if rows.len() > 1 {
        return Err(crate::Error::new("ambiguous branch row source candidates"));
    }
    rows.pop()
        .ok_or_else(|| crate::Error::new(format!("row {id} is not visible")))
}

struct LocalWriteCheck<'a> {
    db: &'a Connection,
    schema: &'a SchemaDef,
    table: &'a crate::schema::TableDef,
    row_num: i64,
    branch_num: i64,
    values: &'a BTreeMap<String, JsonValue>,
    user: &'a str,
    op: i64,
}

fn local_write_allowed(check: LocalWriteCheck<'_>) -> Result<bool> {
    if check.op == 1 && matches!(check.table.write_policy, PolicyDef::CreatedByUser) {
        return Ok(true);
    }
    policy::write_allowed(policy::WriteCheck {
        db: check.db,
        schema: check.schema,
        table: check.table,
        row_num: check.row_num,
        branch_num: check.branch_num,
        values: check.values,
        user: check.user,
    })
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
    if let PolicyDef::RefReadable { field } = &table.write_policy {
        if let Some(dependency) = unavailable_policy_dependency(conn, table, record, tx_num, field)?
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
            "h.j_created_by".to_owned(),
            "h.j_updated_by".to_owned(),
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
        "SELECT reads.table_name, ids.row_id, reads.row_num, reads.observed_tx_num
         FROM jazz_tx_read reads
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE reads.tx_num = ?
           AND reads.reason = ?
         ORDER BY reads.table_name, ids.row_id",
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
           AND table_name = ?
           AND row_num = ?
           AND observed_tx_num IS NULL",
        params![tx_num, ref_table_name, dependency_row_num],
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
               AND table_name = ?
               AND row_num = ?
               AND observed_tx_num IS NULL",
            params![observed_tx_num, tx_num, table_name, row_num],
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
    conn.query_row(
        &format!(
            "SELECT j_created_at, j_created_by
             FROM {}
             WHERE row_num = ? AND j_branch_num = ? AND is_deleted = 0",
            crate::schema::current_table(table_name)
        ),
        params![row_num, branch_num],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Into::into)
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
         WHERE writes.table_name = ?
           AND writes.row_num = ?
           AND tx.conflict_mode = ?
           AND tx.outcome = ?",
        params![
            table_name,
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
    let PolicyDef::RefReadable { field } = policy else {
        return Ok(());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(());
    };
    let Some(row_id) = values.get(&field.name).and_then(JsonValue::as_str) else {
        return Ok(());
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
    )
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
    let PolicyDef::RefReadable { field } = policy else {
        return Ok(());
    };
    let field = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
        .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
    let FieldKind::Ref {
        table: ref_table_name,
    } = &field.kind
    else {
        return Ok(());
    };
    let Some(parent_row_num) =
        current_ref_field_row_num(conn, &table.name, field, row_num, branch_num)?
    else {
        return Ok(());
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
    )
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
               AND h.op != 3
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
    conn.execute(
        "INSERT OR REPLACE INTO jazz_tx_write (tx_num, table_name, row_num, op)
         VALUES (?, ?, ?, ?)",
        params![tx_num, table_name, row_num, op],
    )?;
    Ok(())
}

fn transaction_dotted_vector(conn: &Connection) -> Result<BTreeMap<i64, i64>> {
    let mut stmt = conn.prepare(
        "SELECT node_num, MAX(local_epoch)
         FROM jazz_tx
         GROUP BY node_num
         ORDER BY node_num",
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

pub struct TransactionBuilder<'a> {
    runtime: &'a mut Runtime,
    mutations: Vec<Mutation>,
    mode: TransactionMode,
    start: std::result::Result<TransactionStart, String>,
}

struct TransactionStart {
    vector: BTreeMap<i64, i64>,
    branch_num: i64,
    scope_depths: BTreeMap<i64, i64>,
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

enum CommitMutation {
    Row {
        table: String,
        id: String,
        values: BTreeMap<String, JsonValue>,
        op: i64,
    },
    DeleteRow {
        table: String,
        id: String,
        visible_row: RowView,
    },
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
            op: 1,
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
            op: 2,
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

    pub fn read_rows(&self, table_name: &str) -> Result<Vec<RowView>> {
        let table = self.runtime.schema.table_def(table_name)?;
        let mut rows_by_id = self.read_rows_at_start(table_name)?.into_iter().fold(
            BTreeMap::<String, Vec<RowView>>::new(),
            |mut rows, row| {
                rows.entry(row.id.clone()).or_default().push(row);
                rows
            },
        );

        for mutation in &self.mutations {
            match mutation {
                Mutation::Row {
                    table: mutation_table,
                    id,
                    values,
                    op,
                } if mutation_table == table_name => {
                    validate_write_fields(table, values)?;
                    let base = single_transaction_row(rows_by_id.get(id), id)?;
                    let effective_values =
                        transaction_effective_values(table, base, values, *op, id)?;
                    let created_by = if *op == 1 {
                        self.runtime.attribution_user().to_owned()
                    } else {
                        base.map(|row| row.created_by.clone())
                            .unwrap_or_else(|| self.runtime.attribution_user().to_owned())
                    };
                    rows_by_id.insert(
                        id.clone(),
                        vec![RowView {
                            table: table_name.to_owned(),
                            id: id.clone(),
                            values: effective_values,
                            created_by,
                            tx_id: "staged".to_owned(),
                            conflict_count: 0,
                        }],
                    );
                }
                Mutation::DeleteRow {
                    table: mutation_table,
                    id,
                } if mutation_table == table_name => {
                    rows_by_id.remove(id);
                }
                _ => {}
            }
        }

        let mut rows = self.filter_transaction_rows_by_policy(
            table_name,
            rows_by_id.into_values().flatten().collect::<Vec<_>>(),
        )?;
        rows.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(rows)
    }

    fn start(&self) -> Result<&TransactionStart> {
        self.start
            .as_ref()
            .map_err(|err| crate::Error::new(err.clone()))
    }

    fn read_rows_at_start(&self, table_name: &str) -> Result<Vec<RowView>> {
        let mut rows = self.read_history_rows_at_start(table_name)?;
        let start = self.start()?;
        if start.branch_num != 1 {
            if let Some(base_epoch) =
                branch::base_global_epoch(&self.runtime.conn, start.branch_num)?
            {
                rows.extend(self.read_main_base_rows_at_start(table_name, base_epoch)?);
            }
        }
        self.collapse_transaction_rows(rows)
    }

    fn read_history_rows_at_start(&self, table_name: &str) -> Result<Vec<(i64, RowView)>> {
        let start = self.start()?;
        let table = self.runtime.schema.table_def(table_name)?;
        let scope_nums = start.scope_depths.keys().copied().collect::<Vec<_>>();
        if scope_nums.is_empty() || start.vector.is_empty() {
            return Ok(Vec::new());
        }
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "h.j_branch_num".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.push("h.j_created_by".to_owned());
        let scope_placeholders = local_placeholders(scope_nums.len());
        let (tx_visible_sql, tx_visible_params) = dotted_vector_sql("tx", &start.vector);
        let (newer_visible_sql, newer_visible_params) =
            dotted_vector_sql("newer_tx", &start.vector);
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num IN ({scope_placeholders})
               AND {tx_visible_sql}
               AND tx.outcome != ?
               AND h.op != 3
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = h.j_branch_num
                   AND {newer_visible_sql}
                   AND newer_tx.outcome != ?
                   AND newer_tx.tx_num > tx.tx_num
               )
             ORDER BY h.j_created_at DESC, h.row_num",
            select_columns.join(", "),
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
        );
        let mut params = scope_nums
            .into_iter()
            .map(rusqlite::types::Value::Integer)
            .collect::<Vec<_>>();
        params.extend(tx_visible_params);
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        params.extend(newer_visible_params);
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        self.query_transaction_rows(table_name, table, &sql, params)
    }

    fn read_main_base_rows_at_start(
        &self,
        table_name: &str,
        base_epoch: i64,
    ) -> Result<Vec<(i64, RowView)>> {
        let start = self.start()?;
        if start.vector.is_empty() {
            return Ok(Vec::new());
        }
        let table = self.runtime.schema.table_def(table_name)?;
        let scope_nums = start.scope_depths.keys().copied().collect::<Vec<_>>();
        let scope_placeholders = local_placeholders(scope_nums.len());
        let field_columns = table
            .fields
            .iter()
            .map(|field| crate::schema::quote_ident(&crate::schema::storage_column(field)))
            .collect::<Vec<_>>();
        let mut select_columns = vec![
            "1".to_owned(),
            "ids.row_id".to_owned(),
            "tx.tx_id".to_owned(),
        ];
        select_columns.extend(field_columns.iter().map(|column| format!("h.{column}")));
        select_columns.push("h.j_created_by".to_owned());
        let (tx_visible_sql, tx_visible_params) = dotted_vector_sql("tx", &start.vector);
        let (newer_visible_sql, newer_visible_params) =
            dotted_vector_sql("newer_tx", &start.vector);
        let (shadow_visible_sql, shadow_visible_params) =
            dotted_vector_sql("shadow_tx", &start.vector);
        let sql = format!(
            "SELECT {}
             FROM {} h
             JOIN jazz_row_id ids ON ids.row_num = h.row_num
             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
             WHERE h.j_branch_num = 1
               AND {tx_visible_sql}
               AND tx.outcome != ?
               AND tx.global_epoch IS NOT NULL
               AND tx.global_epoch <= ?
               AND h.op != 3
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} newer
                 JOIN jazz_tx newer_tx ON newer_tx.tx_num = newer.tx_num
                 WHERE newer.row_num = h.row_num
                   AND newer.j_branch_num = 1
                   AND {newer_visible_sql}
                   AND newer_tx.outcome != ?
                   AND newer_tx.global_epoch IS NOT NULL
                   AND newer_tx.global_epoch <= ?
                   AND (newer_tx.global_epoch > tx.global_epoch OR (newer_tx.global_epoch = tx.global_epoch AND newer_tx.tx_num > tx.tx_num))
               )
               AND NOT EXISTS (
                 SELECT 1
                 FROM {history_table} branch_shadow
                 JOIN jazz_tx shadow_tx ON shadow_tx.tx_num = branch_shadow.tx_num
                 WHERE branch_shadow.row_num = h.row_num
                   AND branch_shadow.j_branch_num IN ({scope_placeholders})
                   AND {shadow_visible_sql}
                   AND shadow_tx.outcome != ?
               )
             ORDER BY h.j_created_at DESC, h.row_num",
            select_columns.join(", "),
            crate::schema::history_table(table_name),
            history_table = crate::schema::history_table(table_name),
        );
        let mut params = tx_visible_params;
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        params.push(rusqlite::types::Value::Integer(base_epoch));
        params.extend(newer_visible_params);
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        params.push(rusqlite::types::Value::Integer(base_epoch));
        params.extend(scope_nums.into_iter().map(rusqlite::types::Value::Integer));
        params.extend(shadow_visible_params);
        params.push(rusqlite::types::Value::Integer(tx::OUTCOME_REJECTED));
        self.query_transaction_rows(table_name, table, &sql, params)
    }

    fn query_transaction_rows(
        &self,
        table_name: &str,
        table: &crate::schema::TableDef,
        sql: &str,
        params: Vec<rusqlite::types::Value>,
    ) -> Result<Vec<(i64, RowView)>> {
        let mut stmt = self.runtime.conn.prepare(sql)?;
        let row_width = 3 + table.fields.len() + 1;
        let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
            (0..row_width)
                .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        rows.map(|row| {
            let mut row = row?;
            let branch_num = integer_value(&row.remove(0), "j_branch_num")?;
            Ok((
                branch_num,
                runtime_row_to_view(&self.runtime.conn, table_name, table, row)?,
            ))
        })
        .collect()
    }

    fn collapse_transaction_rows(&self, rows: Vec<(i64, RowView)>) -> Result<Vec<RowView>> {
        let start = self.start()?;
        let mut min_depth_by_row: BTreeMap<String, i64> = BTreeMap::new();
        for (branch_num, row) in &rows {
            let depth =
                start
                    .scope_depths
                    .get(branch_num)
                    .copied()
                    .unwrap_or(if *branch_num == 1 {
                        i64::MAX / 4
                    } else {
                        i64::MAX / 2
                    });
            min_depth_by_row
                .entry(row.id.clone())
                .and_modify(|min_depth| *min_depth = (*min_depth).min(depth))
                .or_insert(depth);
        }
        Ok(rows
            .into_iter()
            .filter_map(|(branch_num, row)| {
                let depth =
                    start
                        .scope_depths
                        .get(&branch_num)
                        .copied()
                        .unwrap_or(if branch_num == 1 {
                            i64::MAX / 4
                        } else {
                            i64::MAX / 2
                        });
                (min_depth_by_row.get(&row.id) == Some(&depth)).then_some(row)
            })
            .collect())
    }

    fn filter_transaction_rows_by_policy(
        &self,
        table_name: &str,
        rows: Vec<RowView>,
    ) -> Result<Vec<RowView>> {
        if self.runtime.bypasses_policy() {
            return Ok(rows);
        }
        let table = self.runtime.schema.table_def(table_name)?;
        match &table.read_policy {
            PolicyDef::AllowAll => Ok(rows),
            PolicyDef::CreatedByUser => Ok(rows
                .into_iter()
                .filter(|row| row.created_by == self.runtime.policy_user())
                .collect()),
            PolicyDef::RefReadable { field } => {
                let field = table
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .ok_or_else(|| crate::Error::new(format!("unknown policy ref {field}")))?;
                let FieldKind::Ref {
                    table: parent_table,
                } = &field.kind
                else {
                    return Ok(Vec::new());
                };
                let visible_parent_ids = self
                    .read_rows(parent_table)?
                    .into_iter()
                    .map(|row| row.id)
                    .collect::<BTreeSet<_>>();
                Ok(rows
                    .into_iter()
                    .filter(|row| {
                        row.values
                            .get(&field.name)
                            .and_then(JsonValue::as_str)
                            .is_some_and(|parent_id| visible_parent_ids.contains(parent_id))
                    })
                    .collect())
            }
        }
    }

    fn materialize_commit_mutations(&self) -> Result<Vec<CommitMutation>> {
        let mut rows_by_table = BTreeMap::<String, BTreeMap<String, Vec<RowView>>>::new();
        let mut commit_mutations = Vec::new();
        let user = self.runtime.attribution_user().to_owned();

        for mutation in &self.mutations {
            match mutation {
                Mutation::Row {
                    table,
                    id,
                    values,
                    op,
                } => {
                    let table_def = self.runtime.schema.table_def(table)?;
                    validate_write_fields(table_def, values)?;
                    if !rows_by_table.contains_key(table) {
                        rows_by_table.insert(
                            table.clone(),
                            transaction_rows_by_id(self.read_rows_at_start(table)?),
                        );
                    }
                    let rows_by_id = rows_by_table.get_mut(table).expect("table rows inserted");
                    let base = single_transaction_row(rows_by_id.get(id), id)?;
                    let effective_values =
                        transaction_effective_values(table_def, base, values, *op, id)?;
                    let created_by = if *op == 1 {
                        user.clone()
                    } else {
                        base.map(|row| row.created_by.clone())
                            .unwrap_or_else(|| user.clone())
                    };
                    rows_by_id.insert(
                        id.clone(),
                        vec![RowView {
                            table: table.clone(),
                            id: id.clone(),
                            values: effective_values.clone(),
                            created_by,
                            tx_id: "staged".to_owned(),
                            conflict_count: 0,
                        }],
                    );
                    commit_mutations.push(CommitMutation::Row {
                        table: table.clone(),
                        id: id.clone(),
                        values: effective_values,
                        op: *op,
                    });
                }
                Mutation::DeleteRow { table, id } => {
                    if !rows_by_table.contains_key(table) {
                        rows_by_table.insert(
                            table.clone(),
                            transaction_rows_by_id(self.read_rows_at_start(table)?),
                        );
                    }
                    let rows_by_id = rows_by_table.get_mut(table).expect("table rows inserted");
                    let visible_row = take_single_transaction_row(rows_by_id.remove(id), id)?;
                    commit_mutations.push(CommitMutation::DeleteRow {
                        table: table.clone(),
                        id: id.clone(),
                        visible_row,
                    });
                }
            }
        }

        Ok(commit_mutations)
    }

    pub fn commit(self) -> Result<String> {
        let user = self.runtime.attribution_user().to_owned();
        let bypass_policy = self.runtime.bypasses_policy();
        let mutations = self.materialize_commit_mutations()?;
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
                    CommitMutation::Row { table, id, .. }
                    | CommitMutation::DeleteRow { table, id, .. } => (table.as_str(), id.as_str()),
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
                CommitMutation::Row {
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
                        values_are_effective: true,
                        tx_num,
                        branch_num: self.runtime.branch_num,
                        now,
                        user: &user,
                        bypass_policy,
                        op,
                    })?;
                }
                CommitMutation::DeleteRow {
                    table,
                    id,
                    visible_row,
                } => {
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
                    record_policy_read_set_for_write(
                        &db,
                        &self.runtime.schema,
                        table_def,
                        &table_def.write_policy,
                        &visible_row.values,
                        self.runtime.branch_num,
                        tx_num,
                    )?;
                    allowed &= bypass_policy
                        || local_write_allowed(LocalWriteCheck {
                            db: &db,
                            schema: &self.runtime.schema,
                            table: table_def,
                            row_num,
                            branch_num: self.runtime.branch_num,
                            values: &visible_row.values,
                            user: &user,
                            op: 3,
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
                    let mut values = vec![
                        rusqlite::types::Value::Integer(row_num),
                        rusqlite::types::Value::Integer(tx_num),
                        rusqlite::types::Value::Integer(self.runtime.branch_num),
                        rusqlite::types::Value::Integer(3),
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
                    let (created_at, created_by) =
                        current_creation_metadata(&db, &table, row_num, self.runtime.branch_num)?
                            .unwrap_or((now, visible_row.created_by.clone()));
                    values.extend([
                        rusqlite::types::Value::Integer(created_at),
                        rusqlite::types::Value::Integer(now),
                        rusqlite::types::Value::Text(created_by),
                        rusqlite::types::Value::Text(user.to_owned()),
                    ]);
                    insert_dynamic(
                        &db,
                        &crate::schema::history_table(&table),
                        &insert_columns,
                        &values,
                    )?;
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
                            rusqlite::types::Value::Text(user.to_owned()),
                            rusqlite::types::Value::Text(user.to_owned()),
                        ]);
                        insert_dynamic(
                            &db,
                            &crate::schema::current_table(&table),
                            &current_columns,
                            &current_values,
                        )?;
                    }
                    record_tx_write(&db, tx_num, &table, row_num, 3)?;
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

fn next_global_epoch(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(global_epoch), 0) + 1 FROM jazz_tx",
        [],
        |row| row.get(0),
    )?)
}

fn tx_is_remote_pending(conn: &Connection, tx_num: i64, local_node_num: i64) -> Result<bool> {
    let (node_num, outcome): (i64, i64) = conn.query_row(
        "SELECT node_num, outcome FROM jazz_tx WHERE tx_num = ?",
        params![tx_num],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(outcome == tx::OUTCOME_PENDING && node_num != local_node_num)
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
    auth_user: Option<&str>,
) -> Result<bool> {
    let user = auth_user
        .ok_or_else(|| crate::Error::new("untrusted policy validation requires auth user"))?;
    let branch_num = branch::ensure(conn, &record.branch_id, None, now_ms())?;
    if record.op == 3 && matches!(table.write_policy, PolicyDef::CreatedByUser) {
        return Ok(record.created_by == user);
    }
    policy::write_allowed(policy::WriteCheck {
        db: conn,
        schema,
        table,
        row_num,
        branch_num,
        values: &record.values,
        user,
    })
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
    let mut tx_ids = history
        .iter()
        .map(|record| record.tx_id.clone())
        .collect::<Vec<_>>();
    tx_ids.sort();
    tx_ids.dedup();
    if tx_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT tx.tx_id, reads.table_name, ids.row_id, reads.reason, observed.tx_id
         FROM jazz_tx_read reads
         JOIN jazz_tx tx ON tx.tx_num = reads.tx_num
         LEFT JOIN jazz_tx observed ON observed.tx_num = reads.observed_tx_num
         JOIN jazz_row_id ids ON ids.row_num = reads.row_num
         WHERE tx.tx_id IN ({placeholders})
         ORDER BY tx.tx_num, reads.table_name, ids.row_id, reads.reason",
        placeholders = (0..tx_ids.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", "),
    ))?;
    let records = stmt.query_map(params_from_iter(tx_ids.iter()), |row| {
        Ok(ReadRecord {
            tx_id: row.get(0)?,
            table: row.get(1)?,
            row_id: row.get(2)?,
            reason: row.get(3)?,
            observed_tx_id: row.get(4)?,
        })
    })?;
    records
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
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

fn current_created_at_by_row_id(
    conn: &Connection,
    table_name: &str,
) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT ids.row_id, current.j_created_at
         FROM {} current
         JOIN jazz_row_id ids ON ids.row_num = current.row_num",
        crate::schema::current_table(table_name)
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
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
    let mut records = export_visible_table_history(
        conn,
        schema,
        table_name,
        user,
        bypass_policy,
        &branch_nums,
        None,
    )?;
    records.extend(export_deleted_table_history(
        conn,
        schema,
        table_name,
        &branch_nums,
    )?);
    records.extend(export_policy_dependency_history(
        conn,
        schema,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.read_policy,
            user,
            bypass_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    records.extend(export_policy_dependency_history(
        conn,
        schema,
        PolicyDependencyExport {
            table_name,
            policy: &schema.table_def(table_name)?.write_policy,
            user,
            bypass_policy,
            branch_nums: &branch_nums,
            child_row_nums: None,
        },
    )?);
    if branch_num != 1 {
        if let Some(base_epoch) = branch::base_global_epoch(conn, branch_num)? {
            records.extend(export_main_base_snapshot_history(
                conn,
                schema,
                table_name,
                base_epoch,
                user,
                bypass_policy,
            )?);
        }
    }
    Ok(records)
}

fn export_main_base_snapshot_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    base_epoch: i64,
    user: &str,
    bypass_policy: bool,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let policy_sql = if bypass_policy {
        "1 = 1".to_owned()
    } else {
        policy::snapshot_read_policy_sql_for_alias(schema, table, "h", user, base_epoch)?
    };
    let sql = format!(
        "SELECT h.row_num
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE h.j_branch_num = 1
           AND tx.outcome != ?
           AND tx.global_epoch IS NOT NULL
           AND tx.global_epoch <= ?
           AND h.op != 3
           AND {policy_sql}
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
           )",
        crate::schema::history_table(table_name),
        history_table = crate::schema::history_table(table_name),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map(
            params![
                tx::OUTCOME_REJECTED,
                base_epoch,
                tx::OUTCOME_REJECTED,
                base_epoch
            ],
            |row| row.get::<_, i64>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_history_versions_for_rows(
        conn,
        schema,
        table_name,
        Some(&row_nums),
        Some(base_epoch),
    )?;
    records.extend(export_snapshot_policy_dependency_history(
        conn,
        schema,
        table_name,
        user,
        bypass_policy,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

fn export_snapshot_policy_dependency_history(
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    user: &str,
    bypass_policy: bool,
    base_epoch: i64,
    child_row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let PolicyDef::RefReadable { field } = &table.read_policy else {
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
    let policy_sql = if bypass_policy {
        "1 = 1".to_owned()
    } else {
        policy::snapshot_read_policy_sql_for_alias(schema, table, "h", user, base_epoch)?
    };
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT h.{ref_column}
         FROM {} h
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         WHERE {row_filter}
           AND h.j_branch_num = 1
           AND h.op != 3
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
        conn,
        schema,
        parent_table,
        user,
        bypass_policy,
        base_epoch,
        Some(&row_nums),
    )?);
    Ok(records)
}

struct PolicyDependencyExport<'a> {
    table_name: &'a str,
    policy: &'a PolicyDef,
    user: &'a str,
    bypass_policy: bool,
    branch_nums: &'a [i64],
    child_row_nums: Option<&'a [i64]>,
}

fn export_policy_dependency_history(
    conn: &Connection,
    schema: &SchemaDef,
    args: PolicyDependencyExport<'_>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(args.table_name)?;
    let PolicyDef::RefReadable { field } = args.policy else {
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
    let policy_sql = export_read_policy_sql(schema, table, args.user, args.bypass_policy)?;
    let ref_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "SELECT DISTINCT current.{ref_column}
         FROM {} current
         JOIN jazz_tx current_tx ON current_tx.tx_num = current.visible_tx_num
         WHERE current.is_deleted = 0
           AND {row_filter}
           AND {}
           AND current_tx.outcome != {}
           AND {policy_sql}",
        crate::schema::current_table(args.table_name),
        branch_filter_sql("current", args.branch_nums),
        tx::OUTCOME_REJECTED,
        row_filter = current_row_filter_sql("current", args.child_row_nums),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut records = export_visible_table_history(
        conn,
        schema,
        parent_table,
        args.user,
        args.bypass_policy,
        args.branch_nums,
        Some(&row_nums),
    )?;
    records.extend(export_policy_dependency_history(
        conn,
        schema,
        PolicyDependencyExport {
            table_name: parent_table,
            policy: &schema.table_def(parent_table)?.read_policy,
            user: args.user,
            bypass_policy: args.bypass_policy,
            branch_nums: args.branch_nums,
            child_row_nums: Some(&row_nums),
        },
    )?);
    Ok(records)
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
         WHERE h.op = 3
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
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "WITH RECURSIVE deleted_tree(row_num) AS (
           SELECT h.row_num
           FROM {history_table} h
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE h.op = 3
             AND {branch_filter}
             AND h.{parent_column} IN ({parent_placeholders})
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
           WHERE child.op = 3
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
        parent_placeholders = (0..parent_row_nums.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map(params_from_iter(parent_row_nums.iter()), |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
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
    let table = schema.table_def(table_name)?;
    let field = table
        .fields
        .iter()
        .find(|field| field.name == parent_field)
        .ok_or_else(|| crate::Error::new(format!("unknown ref field {parent_field}")))?;
    let parent_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
    let sql = format!(
        "WITH RECURSIVE historical_tree(row_num) AS (
           SELECT h.row_num
           FROM {history_table} h
           JOIN jazz_tx tx ON tx.tx_num = h.tx_num
           WHERE {branch_filter}
             AND h.{parent_column} IN ({parent_placeholders})
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
        parent_placeholders = (0..parent_row_nums.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_nums = stmt
        .query_map(params_from_iter(parent_row_nums.iter()), |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    export_history_versions_for_rows(conn, schema, table_name, Some(&row_nums), None)
}

fn query_scope_repair_row_nums(
    conn: &Connection,
    table: &crate::schema::TableDef,
    field_name: &str,
    op: &str,
    value: &JsonValue,
) -> Result<Vec<i64>> {
    if op == "eq_top_created_at_desc" {
        let value = value
            .get("eq")
            .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
        return query_scope_repair_row_nums(conn, table, field_name, "eq", value);
    }
    if field_name == "id" {
        if op == "ne" {
            let excluded_id = value
                .as_str()
                .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
            let mut stmt = conn.prepare(
                "SELECT row_num
                 FROM jazz_row_id
                 WHERE table_name = ? AND row_id != ?
                 ORDER BY row_num",
            )?;
            let rows = stmt.query_map(params![table.name, excluded_id], |row| row.get(0))?;
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
        let rows = stmt.query_map(params![created_by, tx::OUTCOME_REJECTED], |row| {
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
    conn: &Connection,
    schema: &SchemaDef,
    table_name: &str,
    user: &str,
    bypass_policy: bool,
    branch_nums: &[i64],
    row_nums: Option<&[i64]>,
) -> Result<Vec<HistoryRecord>> {
    let table = schema.table_def(table_name)?;
    let policy_sql = export_read_policy_sql(schema, table, user, bypass_policy)?;
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
        "h.j_created_by".to_owned(),
        "h.j_updated_by".to_owned(),
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
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json(conn, field, &row[idx + 4])?,
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
        "h.j_created_by".to_owned(),
        "h.j_updated_by".to_owned(),
    ]);
    let sql = format!(
        "SELECT {}
         FROM {} h
         JOIN jazz_row_id ids ON ids.row_num = h.row_num
         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
         JOIN jazz_branch branch ON branch.branch_num = h.j_branch_num
         WHERE {row_filter}
           AND {epoch_filter}
         ORDER BY h.row_num, h.tx_num",
        select_columns.join(", "),
        crate::schema::history_table(table_name),
        row_filter = row_filter_sql(row_nums),
        epoch_filter = history_epoch_filter_sql(max_global_epoch),
    );
    let mut stmt = conn.prepare(&sql)?;
    let row_width = 4 + table.fields.len() + 4;
    let mut rows = match row_nums {
        Some(row_nums) => stmt.query(params_from_iter(row_nums.iter()))?,
        None => stmt.query([])?,
    };
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let row = (0..row_width)
            .map(|idx| row.get::<_, rusqlite::types::Value>(idx))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut values = BTreeMap::new();
        for (idx, field) in table.fields.iter().enumerate() {
            values.insert(
                field.name.clone(),
                sql_value_to_json(conn, field, &row[idx + 4])?,
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
        Some(row_nums) => format!(
            "h.row_num IN ({})",
            (0..row_nums.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn current_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None => "1 = 1".to_owned(),
    }
}

fn history_row_filter_sql(alias: &str, row_nums: Option<&[i64]>) -> String {
    match row_nums {
        Some([]) => "0 = 1".to_owned(),
        Some(row_nums) => format!(
            "{alias}.row_num IN ({})",
            row_nums
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
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
        branch_nums
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn export_read_policy_sql(
    schema: &SchemaDef,
    table: &crate::schema::TableDef,
    user: &str,
    bypass_policy: bool,
) -> Result<String> {
    if bypass_policy {
        Ok("1 = 1".to_owned())
    } else {
        policy::read_policy_sql(schema, table, user)
    }
}

fn sql_value_to_json(
    conn: &Connection,
    field: &FieldDef,
    value: &rusqlite::types::Value,
) -> Result<JsonValue> {
    match (&field.kind, value) {
        (_, rusqlite::types::Value::Null) if field.nullable => Ok(JsonValue::Null),
        (FieldKind::Text, rusqlite::types::Value::Text(value)) => {
            Ok(JsonValue::String(value.clone()))
        }
        (FieldKind::Bool, rusqlite::types::Value::Integer(value)) => {
            Ok(JsonValue::Bool(*value != 0))
        }
        (FieldKind::Ref { .. }, rusqlite::types::Value::Integer(row_num)) => {
            Ok(JsonValue::String(public_row_id(conn, *row_num)?))
        }
        _ => Err(crate::Error::new(format!(
            "unexpected SQL value for field {}",
            field.name
        ))),
    }
}

fn text_value(value: &rusqlite::types::Value, name: &str) -> Result<String> {
    match value {
        rusqlite::types::Value::Text(value) => Ok(value.clone()),
        _ => Err(crate::Error::new(format!("expected text {name}"))),
    }
}

fn runtime_row_to_view(
    conn: &Connection,
    table_name: &str,
    table: &crate::schema::TableDef,
    raw: Vec<rusqlite::types::Value>,
) -> Result<RowView> {
    let mut values = BTreeMap::new();
    for (idx, field) in table.fields.iter().enumerate() {
        values.insert(
            field.name.clone(),
            sql_value_to_json(conn, field, &raw[idx + 2])?,
        );
    }
    Ok(RowView {
        table: table_name.to_owned(),
        id: text_value(&raw[0], "row_id")?,
        tx_id: text_value(&raw[1], "tx_id")?,
        values,
        created_by: text_value(&raw[2 + table.fields.len()], "j_created_by")?,
        conflict_count: 0,
    })
}

fn integer_value(value: &rusqlite::types::Value, name: &str) -> Result<i64> {
    match value {
        rusqlite::types::Value::Integer(value) => Ok(*value),
        _ => Err(crate::Error::new(format!("expected integer {name}"))),
    }
}

fn local_placeholders(count: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn dotted_vector_sql(
    alias: &str,
    vector: &BTreeMap<i64, i64>,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut params = Vec::new();
    let clauses = vector
        .iter()
        .map(|(node_num, local_epoch)| {
            params.push(rusqlite::types::Value::Integer(*node_num));
            params.push(rusqlite::types::Value::Integer(*local_epoch));
            format!("({alias}.node_num = ? AND {alias}.local_epoch <= ?)")
        })
        .collect::<Vec<_>>();
    (format!("({})", clauses.join(" OR ")), params)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {table} ({}) VALUES ({placeholders})",
            columns.join(", ")
        ),
        params_from_iter(values.iter()),
    )?;
    Ok(())
}
