//! Owner-thread SQLite lifetime and native relay file admission.
use super::*;
use jazz::db::{Error, ErrorCode, FileOptions};
use rusqlite::{Connection, OptionalExtension};
use std::fs::{File, OpenOptions};

pub(super) struct ClientRelayScope {
    owner: String,
    author: jazz::ids::AuthorSubject,
    pub(super) account_host: bool,
    pub(super) account_root: Option<PathBuf>,
}
impl ClientRelayScope {
    /// Caller has authenticated the exact host-owned storage scope.
    pub(super) unsafe fn from_admitted_storage_owner(
        owner: String,
        author: jazz::ids::AuthorSubject,
    ) -> Self {
        Self {
            owner,
            author,
            account_host: false,
            account_root: None,
        }
    }

    /// Caller resolved the native account-session capability, including its
    /// registry, app, environment and account-derived durable root.
    pub(super) unsafe fn from_admitted_account_owner(
        owner: String,
        author: jazz::ids::AuthorSubject,
        account_root: PathBuf,
    ) -> Self {
        Self {
            owner,
            author,
            account_host: true,
            account_root: Some(account_root),
        }
    }
}

/// Account admission permits a different login in the same stable namespace.
/// Canonical scope bytes must bind both the old and new exact login; matching
/// account IDs alone never admits a generic private-scope caller.
fn linked_account_scope_matches(
    stored_scope: &str,
    stored_author: &str,
    scope: &RelayScope,
    author: jazz::ids::AuthorSubject,
) -> bool {
    let Ok(previous) = jazz::ids::AuthorSubject::from_canonical(stored_author) else {
        return false;
    };
    if !matches!(previous, jazz::ids::AuthorSubject::Authenticated(_))
        || !matches!(author, jazz::ids::AuthorSubject::Authenticated(_))
        || previous.account_id().is_none()
        || previous.account_id() != author.account_id()
        || scope.auth_scope.as_deref() != Some(author.canonical())
    {
        return false;
    }
    let previous_scope = RelayScope {
        app_namespace: scope.app_namespace.clone(),
        storage_namespace: scope.storage_namespace.clone(),
        auth_scope: Some(previous.canonical().to_owned()),
    };
    previous_scope.ledger_owner_encoding() == stored_scope
}

pub(super) struct RelayDb {
    pub(super) binding: jazz::binding_host::Binding,
    identity: DbIdentity,
    _lock: Option<File>,
    _account_lock: Option<File>,
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
/// A closed generic root may move between already linked host identities only
/// after all original uploads have settled. This is deliberately not a private
/// owner admission: matching account IDs never grants that capability.
fn generic_handoff_is_admissible(
    tx: &rusqlite::Transaction<'_>,
    stored_scope: &str,
    stored_author: &str,
    config: &RelayOpenConfig,
) -> Result<bool, RelayError> {
    let previous =
        jazz::ids::AuthorSubject::from_canonical(stored_author).map_err(|_| unsupported())?;
    if !matches!(previous, jazz::ids::AuthorSubject::Authenticated(_))
        || !matches!(
            config.identity.author,
            jazz::ids::AuthorSubject::Authenticated(_)
        )
        || previous == config.identity.author
        || previous.account_id().is_none()
        || previous.account_id() != config.identity.author.account_id()
    {
        return Ok(false);
    }
    let Some(mut fields) = stored_scope.strip_prefix("jazz-native-relay-owner-v1:") else {
        return Ok(false);
    };
    let mut decoded = Vec::with_capacity(3);
    for _ in 0..3 {
        let Some((length, remaining)) = fields.split_once(':') else {
            return Ok(false);
        };
        let Ok(length) = length.parse::<usize>() else {
            return Ok(false);
        };
        let Some((field, remaining)) = remaining.split_at_checked(length) else {
            return Ok(false);
        };
        decoded.push(field);
        fields = remaining;
    }
    let previous_scope = RelayScope {
        app_namespace: decoded[0].to_owned(),
        storage_namespace: decoded[1].to_owned(),
        auth_scope: Some(decoded[2].to_owned()),
    };
    if !fields.is_empty()
        || previous_scope.validate().is_err()
        || previous_scope.ledger_owner_encoding() != stored_scope
        || previous_scope.app_namespace != config.scope.app_namespace
        || previous_scope.storage_namespace != config.scope.storage_namespace
    {
        return Ok(false);
    }
    // Existence checks use the singleton/transaction keys and the existing
    // partial pending-upload index; settled history is never scanned.
    let initialized: i64 = tx.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('_jazz_pending','_jazz_relay_upload_v1','_jazz_owner_scope_v1')",
        [], |row| row.get(0),
    ).map_err(storage)?;
    if initialized != 3 {
        return Ok(false);
    }
    let held: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM _jazz_owner_scope_v1 WHERE singleton=1) OR EXISTS(SELECT 1 FROM _jazz_pending LIMIT 1) OR EXISTS(SELECT 1 FROM _jazz_relay_upload_v1 WHERE pending=1 LIMIT 1)",
        [], |row| row.get(0),
    ).map_err(storage)?;
    Ok(!held)
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
            _account_lock: None,
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
        // Every login in an account shares the old account-root lock inode.
        // A separate login store also keeps its own file lock, so neither an
        // old account-root opener nor an exact generic file opener overlaps it.
        let account_root = admitted.and_then(|scope| scope.account_root.as_ref());
        let account_lock = account_root
            .filter(|root| *root != path)
            .map(|root| lock_file(&root.with_extension("owner.lock")))
            .transpose()?;
        let lock_path = path.with_extension("owner.lock");
        for file in [path, &lock_path] {
            if let Ok(meta) = std::fs::symlink_metadata(file)
                && !meta.file_type().is_file()
            {
                return Err(unsupported());
            }
        }
        let lock = lock_file(&lock_path)?;
        if let Some(root) = account_root
            && select_account_storage(root, &config.scope, config.identity)? != *path
        {
            return Err(RelayError::ScopeConfigurationMismatch);
        }
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
        let mut generic_handoff = None;
        if marker {
            let stored:(i64,String,Option<String>,Vec<u8>,String)=tx.query_row(
                "SELECT version,scope,owner,node,author FROM _jazz_native_relay_v1 WHERE slot=1",[],
                |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(storage)?.ok_or_else(unsupported)?;
            if stored.0 != 1 {
                return Err(unsupported());
            }
            // Trusted promotion keeps the exact scope and identity. A generic
            // linked-identity handoff is separate and never promotes a root.
            promote = stored.2.is_none() && owner.is_some();
            if stored.3.as_slice() != config.identity.node.as_bytes()
                || (stored.2.as_deref() != owner && !promote)
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            if stored.1 != config.scope.ledger_owner_encoding()
                || stored.4 != config.identity.author.canonical()
            {
                if stored.2.is_none()
                    && owner.is_none()
                    && generic_handoff_is_admissible(&tx, &stored.1, &stored.4, config)?
                {
                    generic_handoff = Some((stored.1, stored.4));
                } else {
                    return Err(RelayError::ScopeConfigurationMismatch);
                }
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
        let db_config = DbConfig::new(config.schema.clone(), config.identity);
        let options = FileOptions {
            synchronous_full: true,
            ..Default::default()
        };
        let core = if admitted.is_some_and(|scope| scope.account_host) {
            // SAFETY: the private account-session route authenticates the
            // stable root. This exclusive lock outlives both marker updates
            // and every runtime handle; generic admissions lack this flag.
            block_on(unsafe { jazz::db::Db::open_at_as_account_owner(db_config, path, options) })
        } else {
            block_on(jazz::db::Db::open_at(db_config, path, options))
        }
        .map_err(RelayError::Db)?;
        if let Some(bytes) = cached {
            // SAFETY: the cache is read in this exact admitted host scope.
            unsafe { core.install_cached_catalogue(bytes) }.map_err(RelayError::Db)?;
        }
        if let Some((previous_scope, previous_author)) = generic_handoff {
            // jazz.sqlite.native-generic-handoff.v1: retain the v1 file layout,
            // replacing only scope/author after core open succeeds. The owner
            // lock spans both operations, including stale-handle invalidation.
            let conn = Connection::open(path).map_err(storage)?;
            conn.execute_batch("PRAGMA synchronous=FULL")
                .map_err(storage)?;
            let changed = conn.execute(
                "UPDATE _jazz_native_relay_v1 SET scope=?1,author=?2 WHERE slot=1 AND version=1 AND owner IS NULL AND node=?3 AND scope=?4 AND author=?5",
                rusqlite::params![config.scope.ledger_owner_encoding(), config.identity.author.canonical(), config.identity.node.as_bytes().as_slice(), previous_scope, previous_author],
            ).map_err(storage)?;
            if changed != 1 {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
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
            _account_lock: account_lock,
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
        let staged = block_on(self.tx_ref(id).insert(&table, cells, options));
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
            self.tx_ref(id)
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
            self.tx_ref(id)
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
        let staged = block_on(self.tx_ref(id).delete(&table, row, options));
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
            self.tx_ref(id)
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
    account_host: bool,
) -> Result<DbIdentity, RelayError> {
    Ok(read_private_node(path, scope, identity, account_host)?
        .map(|stored| DbIdentity {
            node: stored.node,
            ..identity
        })
        .unwrap_or(identity))
}

fn read_private_node(
    path: &std::path::Path,
    scope: &RelayScope,
    identity: DbIdentity,
    account_host: bool,
) -> Result<Option<DbIdentity>, RelayError> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
        Ok(meta) if !meta.file_type().is_file() => return Err(unsupported()),
        Ok(meta) if meta.len() == 0 => return Ok(None),
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
    let linked = account_host
        && owner.as_deref() == Some(stored_scope.as_str())
        && linked_account_scope_matches(&stored_scope, &author, scope, identity.author);
    if !linked
        && (stored_scope != expected
            || owner.as_deref() != Some(expected.as_str())
            || author != identity.author.canonical())
    {
        return Err(RelayError::ScopeConfigurationMismatch);
    }
    let node = node.try_into().map_err(|_| unsupported())?;
    Ok(Some(DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes(node),
        author: jazz::ids::AuthorSubject::from_canonical(&author).map_err(storage)?,
    }))
}

/// jazz.native.account-login-root.v1: a login-specific sibling of the existing
/// account root, with an explicit domain and BE-u32 UTF-8 field lengths. The
/// root basename is the unchanged account-root-v1 digest, never an OS path.
fn login_storage_path(
    root: &std::path::Path,
    author: jazz::ids::AuthorSubject,
) -> Result<PathBuf, RelayError> {
    let stem = root
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(unsupported)?;
    if stem.len() != 64
        || !stem
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(unsupported());
    }
    let mut bytes = b"jazz-native-account-login-root-v1\0".to_vec();
    for field in [stem, author.canonical()] {
        let length = u32::try_from(field.len()).map_err(storage)?;
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    Ok(root.with_file_name(format!(
        "{stem}.login-{}.sqlite",
        blake3::hash(&bytes).to_hex()
    )))
}

/// Select only this login's existing store. The first login retains the old
/// account file; another login gets its own journal, without copying, relabelling
/// or exposing the previous login's pending projection. No directory scan.
pub(super) fn select_account_storage(
    root: &std::path::Path,
    scope: &RelayScope,
    identity: DbIdentity,
) -> Result<PathBuf, RelayError> {
    let login = login_storage_path(root, identity.author)?;
    match std::fs::symlink_metadata(&login) {
        Ok(_) => {
            read_private_node(&login, scope, identity, false)?;
            return Ok(login);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage(error)),
    }
    match read_private_node(root, scope, identity, true)? {
        None => Ok(root.to_path_buf()),
        Some(previous) if previous.author == identity.author => Ok(root.to_path_buf()),
        Some(_) => Ok(login),
    }
}

fn lock_file(path: &std::path::Path) -> Result<File, RelayError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => return Err(unsupported()),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(storage(error)),
        _ => {}
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(storage)?;
    file.try_lock()
        .map_err(|_| RelayError::StorageAlreadyOwned)?;
    Ok(file)
}

pub(super) fn sync_parent(path: &std::path::Path) -> Result<(), RelayError> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(storage)?;
    }
    Ok(())
}
