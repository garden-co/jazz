//! Owner-thread SQLite lifetime and native relay file admission.
use super::*;
use jazz::db::{Error, ErrorCode, FileOptions};
use rusqlite::{Connection, OptionalExtension};
use std::fs::{File, OpenOptions};

pub(super) struct ClientRelayScope {
    owner: String,
    author: jazz::ids::AuthorSubject,
}
impl ClientRelayScope {
    /// Caller has authenticated the exact host-owned storage scope.
    pub(super) unsafe fn from_admitted_storage_owner(
        owner: String,
        author: jazz::ids::AuthorSubject,
    ) -> Self {
        Self { owner, author }
    }
}

pub(super) struct RelayDb {
    pub(super) binding: jazz::binding_host::Binding,
    identity: DbIdentity,
    _lock: Option<File>,
    reports: RefCell<BTreeMap<TransactionId, jazz::binding_host::Write>>,
    pub(super) mutation_errors: Rc<RefCell<Vec<serde_json::Value>>>,
}
impl std::ops::Deref for RelayDb {
    type Target = jazz::db::Db;
    fn deref(&self) -> &Self::Target {
        &self.binding.db
    }
}
fn storage(error: impl std::fmt::Display) -> RelayError {
    RelayError::Db(Error::new(ErrorCode::Storage, error.to_string()))
}
fn unsupported() -> RelayError {
    RelayError::Db(Error::new(
        ErrorCode::UnsupportedStorageCodecs,
        "native relay requires jazz.sqlite.native-relay-file.v1; legacy or foreign layouts cannot be opened",
    ))
}
impl RelayDb {
    pub(super) fn identity(&self) -> DbIdentity {
        self.identity
    }
    pub(super) fn open_memory(
        schema: JazzSchema,
        identity: DbIdentity,
        cached: Option<&[u8]>,
    ) -> Result<Self, RelayError> {
        let core = block_on(jazz::db::Db::open(DbConfig::new(schema, identity)))
            .map_err(RelayError::Db)?;
        if let Some(bytes) = cached {
            // SAFETY: the caller loads captures from its admitted host scope.
            unsafe { core.install_cached_catalogue(bytes) }.map_err(RelayError::Db)?;
        }
        Ok(Self {
            binding: jazz::binding_host::Binding::new(core, identity, false),
            identity,
            _lock: None,
            reports: RefCell::default(),
            mutation_errors: Rc::default(),
        })
    }
    pub(super) fn open_persistent(
        config: &RelayOpenConfig,
        admitted: Option<&ClientRelayScope>,
        cached: Option<&[u8]>,
    ) -> Result<Self, RelayError> {
        let path = &config.sqlite_path;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(storage)?;
        }
        let lock_path = path.with_extension("owner.lock");
        for file in [path, &lock_path] {
            if let Ok(meta) = std::fs::symlink_metadata(file)
                && !meta.file_type().is_file()
            {
                return Err(unsupported());
            }
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(storage)?;
        lock.try_lock()
            .map_err(|_| RelayError::StorageAlreadyOwned)?;
        let owner = admitted.map(|scope| scope.owner.as_str());
        if admitted.is_some_and(|scope| scope.author != config.identity.author) {
            return Err(RelayError::ScopeConfigurationMismatch);
        }
        // Format and scope admission precede core schema or journal installation.
        let mut conn = Connection::open(path).map_err(storage)?;
        conn.execute_batch("PRAGMA synchronous=FULL")
            .map_err(storage)?;
        let tx = conn.transaction().map_err(storage)?;
        let marker:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='_jazz_native_relay_v1')",[],|r|r.get(0)).map_err(storage)?;
        let mut promote = false;
        if marker {
            let stored:(i64,String,Option<String>,Vec<u8>,String)=tx.query_row(
                "SELECT version,scope,owner,node,author FROM _jazz_native_relay_v1 WHERE slot=1",[],
                |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(storage)?.ok_or_else(unsupported)?;
            if stored.0 != 1 {
                return Err(unsupported());
            }
            // A trusted admission can promote a closed generic root, but it
            // cannot change its scope or identity, or downgrade a private one.
            promote = stored.2.is_none() && owner.is_some();
            if stored.1 != config.scope.ledger_owner_encoding()
                || (stored.2.as_deref() != owner && !promote)
                || stored.3.as_slice() != config.identity.node.as_bytes()
                || stored.4 != config.identity.author.canonical()
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            // Core admission precedes the native marker update. A crash in
            // between must not reopen a private projection as a generic relay.
            if owner.is_none() {
                let has_scope: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='_jazz_owner_scope_v1')",
                    [], |r| r.get(0),
                ).map_err(storage)?;
                if has_scope
                    && tx
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM _jazz_owner_scope_v1 WHERE singleton=1)",
                            [],
                            |r| r.get::<_, bool>(0),
                        )
                        .map_err(storage)?
                {
                    return Err(RelayError::ScopeConfigurationMismatch);
                }
            }
        } else {
            let occupied: bool = tx
                .query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master)", [], |r| {
                    r.get(0)
                })
                .map_err(storage)?;
            if occupied {
                return Err(unsupported());
            }
            tx.execute_batch("CREATE TABLE _jazz_native_relay_v1(slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL CHECK(version=1),scope TEXT NOT NULL,owner TEXT,node BLOB NOT NULL CHECK(length(node)=16),author TEXT NOT NULL)").map_err(storage)?;
            tx.execute(
                "INSERT INTO _jazz_native_relay_v1 VALUES(1,1,?1,?2,?3,?4)",
                rusqlite::params![
                    config.scope.ledger_owner_encoding(),
                    owner,
                    config.identity.node.as_bytes().as_slice(),
                    config.identity.author.canonical()
                ],
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        drop(conn);
        sync_parent(path)?;
        let core = block_on(jazz::db::Db::open_at(
            DbConfig::new(config.schema.clone(), config.identity),
            path,
            FileOptions {
                synchronous_full: true,
                ..Default::default()
            },
        ))
        .map_err(RelayError::Db)?;
        if let Some(bytes) = cached {
            // SAFETY: the cache is read in this exact admitted host scope.
            unsafe { core.install_cached_catalogue(bytes) }.map_err(RelayError::Db)?;
        }
        if let Some(owner) = owner {
            core.set_scope_isolated_storage_owner()
                .map_err(RelayError::Db)?;
            if promote {
                // The core validates every pending source before granting its
                // private projection. Record the native mode only afterwards;
                // an interrupted promotion can resume under the same admission.
                let conn = Connection::open(path).map_err(storage)?;
                conn.execute_batch("PRAGMA synchronous=FULL")
                    .map_err(storage)?;
                let changed = conn.execute(
                    "UPDATE _jazz_native_relay_v1 SET owner=?1 WHERE slot=1 AND version=1 AND owner IS NULL AND scope=?1 AND node=?2 AND author=?3",
                    rusqlite::params![owner, config.identity.node.as_bytes().as_slice(), config.identity.author.canonical()],
                ).map_err(storage)?;
                if changed != 1 {
                    return Err(RelayError::ScopeConfigurationMismatch);
                }
            }
        }
        Ok(Self {
            binding: jazz::binding_host::Binding::new(core, config.identity, false),
            identity: config.identity,
            _lock: Some(lock),
            reports: RefCell::default(),
            mutation_errors: Rc::default(),
        })
    }
}

pub struct WriteWaitOptions {
    pub tier: CoreDurabilityTier,
    pub observe_only: bool,
}
impl From<CoreDurabilityTier> for WriteWaitOptions {
    fn from(tier: CoreDurabilityTier) -> Self {
        Self {
            tier,
            observe_only: false,
        }
    }
}
pub(super) fn host_error(error: jazz::binding_host::HostError) -> RelayError {
    match error {
        jazz::binding_host::HostError::Engine(e) => RelayError::Db(e),
        jazz::binding_host::HostError::Argument(message) => RelayError::ForegroundCommand(message),
    }
}

impl RelayDb {
    pub(super) fn set_binding_scheduler(&self, wake: Arc<RelayWake>) {
        // Binding writes and read cleanup request work through this hook;
        // installing only Db's scheduler leaves streamed writes parked until
        // another unrelated command happens to drive the owner.
        *self.binding.hooks.schedule_tick.borrow_mut() = Some(Box::new(move || wake.signal(0)));
    }

    pub(super) fn report_write(&self, write: jazz::binding_host::Write) {
        self.reports.borrow_mut().insert(write.tx_id, write);
        self.binding.hooks.schedule_tick();
    }
    pub(super) fn claim_write(&self, write: &jazz::binding_host::Write) {
        self.reports.borrow_mut().remove(&write.tx_id);
        let id = write.tx_id.to_string();
        self.mutation_errors
            .borrow_mut()
            .retain(|event| event["transaction"]["transactionId"].as_str() != Some(&id));
    }
    pub(super) fn clear_mutation_reports(&self) {
        self.reports.borrow_mut().clear();
    }
    pub(super) async fn tick(&self) -> Result<(), Error> {
        self.binding.tick().await.map_err(|e| match e {
            jazz::binding_host::HostError::Engine(e) => e,
            jazz::binding_host::HostError::Argument(s) => Error::new(ErrorCode::Protocol, s),
        })?;
        let reports = std::mem::take(&mut *self.reports.borrow_mut());
        for (id, write) in reports {
            let state = match self.binding.write_state(&write) {
                Ok(state) => state,
                Err(_) => {
                    // An unreadable receipt is not an authority rejection.
                    // Retain it for a later turn without discarding other reports.
                    self.reports.borrow_mut().insert(id, write);
                    continue;
                }
            };
            match state.fate {
                jazz::tx::Fate::Pending => {
                    self.reports.borrow_mut().insert(id, write);
                }
                jazz::tx::Fate::Accepted => {}
                jazz::tx::Fate::Rejected(reason) => {
                    let code = match &reason {
                        jazz::tx::RejectionReason::AuthorizationDenied => "permission_denied",
                        jazz::tx::RejectionReason::SerializableConflict => "serialization_conflict",
                        jazz::tx::RejectionReason::ClientClockTooFarAhead => {
                            "client_clock_too_far_ahead"
                        }
                        jazz::tx::RejectionReason::CausalityViolation => "causality_violation",
                        jazz::tx::RejectionReason::Cascade { .. } => "cascade_rejected",
                        jazz::tx::RejectionReason::MalformedCommit(_) => "write_rejected",
                    };
                    let tx = id.to_string();
                    let reason = format!("{reason:?}");
                    self.mutation_errors.borrow_mut().push(serde_json::json!({"code":code,"reason":reason,
                        "transaction":{"transactionId":tx,"kind":"mergeable","sealed":true,
                            "latestSettlement":{"kind":"rejected","transactionId":tx,"code":code,"reason":reason}}}));
                }
            }
        }
        Ok(())
    }
}

impl RelayDb {
    pub(super) fn apply_insert(
        &self,
        table: String,
        cells: BTreeMap<String, Value>,
        options: jazz::db::InsertOptions,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.insert(&table, cells, options)))
            .map_err(core_error)
    }
    pub(super) fn apply_transaction_insert(
        &self,
        id: OpenTransactionId,
        table: String,
        cells: BTreeMap<String, Value>,
        mut options: jazz::db::InsertOptions,
    ) -> Result<RowUuid, Error> {
        let row = *options.row_id.get_or_insert_with(|| self.next_row_id());
        let staged = block_on(self.serializable_tx_ref(id).insert(&table, cells, options));
        self.binding.staged(id, staged).map_err(core_error)?;
        Ok(row)
    }
    pub(super) fn apply_update(
        &self,
        table: String,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        options: jazz::db::UpdateOptions,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.update(&table, row, cells, options)))
            .map_err(core_error)
    }
    pub(super) fn apply_transaction_update(
        &self,
        id: OpenTransactionId,
        table: String,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        options: jazz::db::UpdateOptions,
    ) -> Result<(), Error> {
        let staged = block_on(
            self.serializable_tx_ref(id)
                .update(&table, row, cells, options),
        );
        self.binding.staged(id, staged).map_err(core_error)
    }
    pub(super) fn apply_upsert(
        &self,
        table: String,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        options: jazz::db::UpsertOptions,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.upsert(&table, row, cells, options)))
            .map_err(core_error)
    }
    pub(super) fn apply_transaction_upsert(
        &self,
        id: OpenTransactionId,
        table: String,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        options: jazz::db::UpsertOptions,
    ) -> Result<(), Error> {
        let staged = block_on(
            self.serializable_tx_ref(id)
                .upsert(&table, row, cells, options),
        );
        self.binding.staged(id, staged).map_err(core_error)
    }
    pub(super) fn apply_delete(
        &self,
        table: String,
        row: RowUuid,
        options: jazz::db::DeleteOptions,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.delete(&table, row, options)))
            .map_err(core_error)
    }
    pub(super) fn apply_transaction_delete(
        &self,
        id: OpenTransactionId,
        table: String,
        row: RowUuid,
        options: jazz::db::DeleteOptions,
    ) -> Result<(), Error> {
        let staged = block_on(self.serializable_tx_ref(id).delete(&table, row, options));
        self.binding.staged(id, staged).map_err(core_error)
    }
    pub(super) fn apply_restore(
        &self,
        table: String,
        row: RowUuid,
        cells: Option<BTreeMap<String, Value>>,
        options: jazz::db::RestoreOptions,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.restore(&table, row, cells, options)))
            .map_err(core_error)
    }
    pub(super) fn apply_transaction_restore(
        &self,
        id: OpenTransactionId,
        table: String,
        row: RowUuid,
        cells: Option<BTreeMap<String, Value>>,
        options: jazz::db::RestoreOptions,
    ) -> Result<(), Error> {
        let staged = block_on(
            self.serializable_tx_ref(id)
                .restore(&table, row, cells, options),
        );
        self.binding.staged(id, staged).map_err(core_error)
    }
    pub(super) fn apply_large_value_update(
        &self,
        table: String,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        updates: Vec<jazz::db::LargeValueUpdate>,
        updated_at_ms: Option<u64>,
    ) -> Result<jazz::binding_host::Write, Error> {
        self.binding
            .write(block_on(self.update_with_large_value_mutations(
                &table,
                row,
                cells,
                updates,
                UpdateOptions {
                    updated_at_ms,
                    ..Default::default()
                },
            )))
            .map_err(core_error)
    }
}

fn core_error(error: jazz::binding_host::HostError) -> Error {
    match error {
        jazz::binding_host::HostError::Engine(e) => e,
        jazz::binding_host::HostError::Argument(s) => Error::new(ErrorCode::Protocol, s),
    }
}
impl RelayDb {
    pub(super) async fn close(&self) -> Result<(), Error> {
        self.binding.close().await.map_err(core_error)
    }
}

/// Private session setup starts with a fresh candidate node. Reopening an
/// already admitted root must recover its durable node instead. This read is
/// only a hint: `open_persistent` checks every marker field again while holding
/// the exclusive owner lock before installing core state.
pub(super) fn restore_private_node(
    path: &std::path::Path,
    scope: &RelayScope,
    identity: DbIdentity,
) -> Result<DbIdentity, RelayError> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(identity),
        Err(error) => return Err(storage(error)),
        Ok(meta) if !meta.file_type().is_file() => return Err(unsupported()),
        Ok(meta) if meta.len() == 0 => return Ok(identity),
        Ok(_) => {}
    }
    scope.validate()?;
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(storage)?;
    let marker:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='_jazz_native_relay_v1')",[],|r|r.get(0)).map_err(storage)?;
    if !marker {
        return Err(unsupported());
    }
    let (version, stored_scope, owner, node, author): (
        i64,
        String,
        Option<String>,
        Vec<u8>,
        String,
    ) = conn
        .query_row(
            "SELECT version,scope,owner,node,author FROM _jazz_native_relay_v1 WHERE slot=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(storage)?
        .ok_or_else(unsupported)?;
    if version != 1 {
        return Err(unsupported());
    }
    let expected = scope.ledger_owner_encoding();
    if stored_scope != expected
        || owner.as_deref() != Some(expected.as_str())
        || author != identity.author.canonical()
    {
        return Err(RelayError::ScopeConfigurationMismatch);
    }
    let node = node.try_into().map_err(|_| unsupported())?;
    Ok(DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes(node),
        author: identity.author,
    })
}

pub(super) fn sync_parent(path: &std::path::Path) -> Result<(), RelayError> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(storage)?;
    }
    Ok(())
}
