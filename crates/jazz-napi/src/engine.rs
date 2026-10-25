//! Node ownership of a SQLite runtime. The shared binding host owns semantics;
//! this module owns the file lock and keeps it alive across every schema view.
use super::*;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

pub(super) enum NativeStorage {
    Memory,
    File { path: PathBuf, lock: Rc<File> },
}

/// `jazz.sqlite.native-root.v1`: one engine file under an exclusively owned
/// root. Existing non-SQLite roots are refused, never opened as empty stores.
pub(super) fn persistent_storage(
    root: &str,
) -> std::result::Result<NativeStorage, PersistentOpenError> {
    let root = std::path::Path::new(root);
    std::fs::create_dir_all(root).map_err(napi_error)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".jazz-native-owner.lock"))
        .map_err(napi_error)?;
    lock.try_lock().map_err(|error| {
        napi::Error::from_reason(format!(
            "native database root is already open or cannot be locked: {error}"
        ))
    })?;
    // This is a dedicated root, not an arbitrary directory containing user
    // files. Stop at the first foreign entry (including a RocksDB CURRENT or
    // MANIFEST) before opening any SQLite database or installing owner metadata.
    for entry in std::fs::read_dir(root).map_err(napi_error)? {
        let entry = entry.map_err(napi_error)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            napi::Error::from_reason("native database root contains an unknown storage entry")
        })?;
        let known_name = matches!(
            name,
            ".jazz-native-owner.lock"
                | ".jazz-account-owner"
                | "jazz.sqlite"
                | "jazz.sqlite-wal"
                | "jazz.sqlite-shm"
                | "jazz.sqlite-journal"
        ) || account_owner::is_temporary_marker_name(name);
        // A recognized basename must still be a regular file. Do not follow
        // symlinks into another root or mistake a directory for an engine file.
        if !known_name || !entry.file_type().map_err(napi_error)?.is_file() {
            return Err(PersistentOpenError::UnsupportedStorageCodecs(
                jazz::db::Error::new(
                    jazz::db::ErrorCode::UnsupportedStorageCodecs,
                    "native database root uses an unsupported storage layout; expected jazz.sqlite.native-root.v1",
                ),
            ));
        }
    }
    Ok(NativeStorage::File {
        path: root.join("jazz.sqlite"),
        lock: Rc::new(lock),
    })
}

#[derive(Clone)]
pub(super) struct NativeDb {
    pub(super) binding: jazz::binding_host::Binding,
    identity: CoreDbIdentity,
    pub(super) host: Rc<NativeHost>,
}

pub(super) struct NativeWait {
    pub(super) env: sys::napi_env,
    pub(super) deferred: sys::napi_deferred,
    pub(super) future: jazz::binding_host::HostFuture<()>,
}

pub(super) struct NativeReport {
    pub(super) write: jazz::binding_host::Write,
    pub(super) kind: &'static str,
    pub(super) claimed: Rc<Cell<bool>>,
}

/// Public rejection fields are derived from the engine's real write fate,
/// rather than guessed from a storage or transport diagnostic.
pub(super) fn rejection_details(reason: &jazz::tx::RejectionReason) -> (&'static str, String) {
    use jazz::tx::RejectionReason::*;
    let (code, reason) = match reason {
        AuthorizationDenied => (
            "permission_denied",
            "Write rejected by server authorization",
        ),
        SerializableConflict => (
            "serialization_conflict",
            "Serializable transaction conflicted with another write",
        ),
        ClientClockTooFarAhead => (
            "client_clock_too_far_ahead",
            "Client clock is too far ahead",
        ),
        CausalityViolation => (
            "causality_violation",
            "Transaction violated causal ordering",
        ),
        Cascade { .. } => (
            "cascade_rejected",
            "Transaction was rejected because an ancestor transaction was rejected",
        ),
        MalformedCommit(reason) => return ("write_rejected", reason.clone()),
    };
    (code, reason.to_owned())
}

type MutationErrorHandler = Rc<dyn Fn(JsonValue)>;

#[derive(Default)]
pub(super) struct NativeHost {
    lock: RefCell<Option<Rc<File>>>,
    closed: Cell<bool>,
    pub(super) waits: RefCell<Vec<NativeWait>>,
    pub(super) reports: RefCell<Vec<NativeReport>>,
    pub(super) mutation_errors: RefCell<Option<MutationErrorHandler>>,
}

fn engine_error(error: jazz::binding_host::HostError) -> jazz::db::Error {
    match error {
        jazz::binding_host::HostError::Engine(error) => error,
        jazz::binding_host::HostError::Argument(message) => {
            jazz::db::Error::new(jazz::db::ErrorCode::Protocol, message)
        }
    }
}
impl std::ops::Deref for NativeDb {
    type Target = jazz::db::Db;
    fn deref(&self) -> &Self::Target {
        &self.binding.db
    }
}
impl NativeDb {
    pub(super) fn open(
        schema: JazzSchema,
        storage: NativeStorage,
        config: CoreOpenDbConfig,
        identity: CoreDbIdentity,
        trusted_backend: bool,
    ) -> napi::Result<Self> {
        let mut db_config = jazz::db::DbConfig::new(schema, identity);
        if let Some(seed) = config.row_id_seed {
            db_config = db_config.with_id_source(CoreSeededRowIdSource::new(seed));
        }
        let (db, lock) = match storage {
            NativeStorage::Memory => (
                core_block_on(jazz::db::Db::open(db_config)).map_err(napi_error)?,
                None,
            ),
            NativeStorage::File { path, lock } => (
                core_block_on(jazz::db::Db::open_at(
                    db_config,
                    &path,
                    jazz::db::FileOptions {
                        synchronous_full: true,
                        ..Default::default()
                    },
                ))
                .map_err(napi_error)?,
                Some(lock),
            ),
        };
        if config.history_complete {
            db.set_authority();
        }
        Ok(Self {
            binding: jazz::binding_host::Binding::new(db, identity, trusted_backend),
            identity,
            host: Rc::new(NativeHost {
                lock: RefCell::new(lock),
                ..Default::default()
            }),
        })
    }

    pub(super) fn is_persistent(&self) -> bool {
        self.host.lock.borrow().is_some()
    }

    pub(super) fn identity(&self) -> CoreDbIdentity {
        self.identity
    }

    pub(super) fn register_schema(&self, schema: &[u8]) -> napi::Result<Self> {
        let binding = self.binding.register_schema(schema).map_err(napi_error)?;
        Ok(Self {
            binding,
            identity: self.identity,
            host: self.host.clone(),
        })
    }

    // Every poll and N-API completion stays on the owning JavaScript thread.
    // Futures own shared core bindings, not NativeDb, so the wait list cannot
    // retain itself or call into a torn-down environment from a worker thread.
    pub(super) fn poll_waits(&self) {
        let waits = std::mem::take(&mut *self.host.waits.borrow_mut());
        let mut pending = Vec::new();
        for mut wait in waits {
            let mut context = Context::from_waker(Waker::noop());
            match wait.future.as_mut().poll(&mut context) {
                Poll::Ready(result) => {
                    finish_immediate_promise(wait.env, wait.deferred, result.map_err(engine_error))
                }
                Poll::Pending => pending.push(wait),
            }
        }
        self.host.waits.borrow_mut().extend(pending);
    }

    pub(super) async fn tick(&self) -> std::result::Result<(), jazz::db::Error> {
        if self.host.closed.get() {
            return Err(jazz::db::Error::new(
                jazz::db::ErrorCode::Storage,
                "database is closed",
            ));
        }
        let result = self.binding.tick().await.map_err(engine_error);
        self.poll_waits();
        let reports = std::mem::take(&mut *self.host.reports.borrow_mut());
        let callback = self.host.mutation_errors.borrow().clone();
        let mut pending = Vec::new();
        for report in reports {
            if report.claimed.get() {
                continue;
            }
            match self.binding.write_state(&report.write) {
                Ok(state) => match state.fate {
                    jazz::tx::Fate::Pending => pending.push(report),
                    jazz::tx::Fate::Accepted => {}
                    jazz::tx::Fate::Rejected(reason) => {
                        if let Some(callback) = &callback {
                            let (code, reason) = rejection_details(&reason);
                            let tx = report.write.tx_id.to_string();
                            callback(serde_json::json!({
                                "code": code, "reason": reason,
                                "transaction": { "transactionId": tx, "kind": report.kind, "sealed": true,
                                    "latestSettlement": { "kind": "rejected", "transactionId": tx,
                                        "code": code, "reason": reason } }
                            }));
                        }
                    }
                },
                // An unreadable receipt is not an authority rejection. A
                // later owner tick can retry it without inventing a fate.
                Err(_) => pending.push(report),
            }
        }
        self.host.reports.borrow_mut().extend(pending);
        result
    }

    pub(super) async fn close(&self) -> std::result::Result<(), jazz::db::Error> {
        if self.host.closed.get() {
            return Ok(());
        }
        self.stop_mutation_admission();
        self.tick().await?;
        self.binding.close_initializations();
        // Retained schema views and transports must lose access before the
        // exclusive root lock is released to another runtime.
        self.binding.db.retire_storage_owner()?;
        self.host.closed.set(true);
        self.set_tick_scheduler(None);
        self.binding.hooks.schedule_tick.borrow_mut().take();
        self.host.mutation_errors.borrow_mut().take();
        self.host.reports.borrow_mut().clear();
        for wait in std::mem::take(&mut *self.host.waits.borrow_mut()) {
            finish_immediate_promise(
                wait.env,
                wait.deferred,
                Err(jazz::db::Error::new(
                    jazz::db::ErrorCode::Storage,
                    "database closed while waiting for settlement",
                )),
            );
        }
        self.host.lock.borrow_mut().take();
        Ok(())
    }
}
