//! React Native bindings for the current Jazz database facade.
//!
//! Jazz's core is intentionally thread-affine. Every database therefore owns
//! one Rust actor thread; UniFFI objects are sendable ids that marshal work to
//! that actor rather than moving `Rc`/`RefCell` core values across threads.

uniffi::setup_scaffolding!();

mod actor;
mod mutation_errors;
mod scheduler;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine;
use futures::FutureExt;
use jazz::binding_support::BindingError;
use jazz::db::{Error as DbError, ErrorCode};

use actor::{ActorHandle, TransactionKind, WaiterSignal};

/// Stable error boundary for the React Native runtime.
#[derive(Clone, Debug, thiserror::Error, uniffi::Error)]
pub enum JazzRnError {
    /// A postcard, JSON, identifier, or scalar argument was invalid.
    #[error("InvalidPayload: {message}")]
    InvalidPayload { message: String },
    /// Schema validation or lowering failed.
    #[error("Schema: {message}")]
    Schema { message: String },
    /// Jazz rejected or could not currently observe the requested operation.
    #[error("Runtime: {message}")]
    Runtime { message: String },
    /// Binding infrastructure failed.
    #[error("Internal: {message}")]
    Internal { message: String },
    /// The database has crossed its close barrier.
    #[error("Closed: {message}")]
    Closed { message: String },
    /// A core-thread panic made this database unusable.
    #[error("Poisoned: {message}")]
    Poisoned { message: String },
}

impl From<BindingError> for JazzRnError {
    fn from(error: BindingError) -> Self {
        match error {
            BindingError::InvalidPayload(message) => Self::InvalidPayload { message },
            BindingError::Core(error) => core_error(error),
            BindingError::Encode(message) => Self::Internal { message },
            BindingError::WaitState { code, detail } => Self::Runtime {
                message: format!("{code:?}: {detail}"),
            },
        }
    }
}

/// Pull-based subscription event crossing the UniFFI boundary.
///
/// Delta payloads remain byte buffers so Hermes does not need to materialize
/// JSON arrays with one number per postcard byte.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RnSubscriptionEvent {
    /// `delta`, `rejected`, or `closed`.
    pub event_type: String,
    /// Reset marker for delta events.
    pub reset: Option<bool>,
    /// Postcard-encoded row delta. Carries no rows when
    /// `terminal_operations_json` is present.
    pub delta: Option<Vec<u8>>,
    /// Typed structural edits to already hydrated rows, as a JSON array.
    ///
    /// UniFFI has no mapping for `serde_json::Value`, so this crosses as a
    /// string and the TypeScript shim parses it — the same treatment
    /// `reason_json` gets.
    pub terminal_operations_json: Option<String>,
    /// Producer-owned terminal root layouts referenced by operations, as JSON.
    pub terminal_layouts_json: Option<String>,
    /// Read-tier settlement marker for delta events.
    pub settled: Option<bool>,
    /// Durability tier for delta events.
    pub tier: Option<String>,
    /// Structured rejection metadata for rejected events.
    pub reason_json: Option<String>,
}

pub(crate) fn core_error(error: DbError) -> JazzRnError {
    if error.code == ErrorCode::Schema {
        JazzRnError::Schema {
            message: error.to_string(),
        }
    } else {
        JazzRnError::Runtime {
            message: error.to_string(),
        }
    }
}

pub(crate) fn closed_error() -> JazzRnError {
    JazzRnError::Closed {
        message: "database is closed".to_owned(),
    }
}

pub(crate) fn poisoned_error(reason: String) -> JazzRnError {
    JazzRnError::Poisoned { message: reason }
}

fn panic_payload_to_string(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    "non-string panic payload".to_owned()
}

pub(crate) fn panic_to_jazz_error(
    context: &'static str,
    payload: Box<dyn std::any::Any + Send>,
) -> JazzRnError {
    let panic_message = panic_payload_to_string(&payload);
    let backtrace = std::backtrace::Backtrace::force_capture();
    JazzRnError::Internal {
        message: format!("panic in {context}: {panic_message}\n{backtrace}"),
    }
}

fn with_panic_boundary<T, F>(context: &'static str, call: F) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Result<T, JazzRnError>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call))
        .unwrap_or_else(|payload| Err(panic_to_jazz_error(context, payload)))
}

async fn with_async_panic_boundary<T, F, Fut>(
    context: &'static str,
    call: F,
) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, JazzRnError>>,
{
    std::panic::AssertUnwindSafe(call())
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| Err(panic_to_jazz_error(context, payload)))
}

fn updated_at_ms(value: Option<f64>) -> Result<Option<u64>, JazzRnError> {
    value
        .map(|value| {
            if !value.is_finite() || value < 0.0 || value > u64::MAX as f64 {
                return Err(JazzRnError::InvalidPayload {
                    message: "updated_at_ms must be a finite non-negative integer".to_owned(),
                });
            }
            Ok(value as u64)
        })
        .transpose()
}

fn ensure_same_actor(
    expected: &Arc<ActorHandle>,
    actual: &Arc<ActorHandle>,
    kind: &'static str,
) -> Result<(), JazzRnError> {
    if Arc::ptr_eq(expected, actual) {
        Ok(())
    } else {
        Err(JazzRnError::InvalidPayload {
            message: format!("{kind} belongs to another database"),
        })
    }
}

fn ensure_same_view(expected: u64, actual: u64, kind: &'static str) -> Result<(), JazzRnError> {
    if expected == actual {
        Ok(())
    } else {
        Err(JazzRnError::InvalidPayload {
            message: format!("{kind} belongs to another schema view"),
        })
    }
}

/// Rejects use of a handle whose owner already closed it.
///
/// Every closable handle guards its methods this way; `kind` names the handle
/// in the message the TypeScript shim surfaces.
fn ensure_open(closed: &AtomicBool, kind: &str) -> Result<(), JazzRnError> {
    if closed.load(Ordering::SeqCst) {
        Err(JazzRnError::Runtime {
            message: format!("{kind} is closed"),
        })
    } else {
        Ok(())
    }
}

/// Runs `release` only on the first close, whether that is an explicit
/// `close()` or the handle being dropped. Without the flag the actor would see
/// a second release for an id it may already have reissued.
fn release_once(closed: &AtomicBool, release: impl FnOnce()) {
    if !closed.swap(true, Ordering::SeqCst) {
        release();
    }
}

fn write_object(actor: Arc<ActorHandle>, id: u64) -> Result<Arc<RnWrite>, JazzRnError> {
    let batch_id = actor.write_batch_id(id)?;
    Ok(Arc::new(RnWrite {
        actor,
        id,
        batch_id,
        closed: AtomicBool::new(false),
    }))
}

/// Called from the detached notifier when Jazz needs another core tick.
#[uniffi::export(callback_interface)]
pub trait TickSchedulerCallback: Send + Sync {
    /// Request a tick with `"immediate"` or `"deferred"` urgency.
    fn on_tick_needed(&self, urgency: String);
}

/// Called from the detached notifier for an unhandled rejected write.
#[uniffi::export(callback_interface)]
pub trait MutationErrorCallback: Send + Sync {
    /// Deliver one camelCase `MutationErrorEvent` JSON object.
    fn on_mutation_error(&self, event_json: String);
}

/// A React Native database backed by one dedicated Jazz actor thread.
#[derive(uniffi::Object)]
pub struct RnDb {
    actor: Arc<ActorHandle>,
    view: u64,
    released: AtomicBool,
}

#[uniffi::export]
impl RnDb {
    /// Open an ephemeral in-memory database.
    #[uniffi::constructor]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("RnDb.open_memory", || {
            Ok(Arc::new(Self {
                actor: ActorHandle::open_memory(schema, config)?,
                view: 0,
                released: AtomicBool::new(false),
            }))
        })
    }

    /// Open a SQLite-backed persistent database.
    #[uniffi::constructor]
    pub fn open_persistent(
        data_path: String,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("RnDb.open_persistent", || {
            Ok(Arc::new(Self {
                actor: ActorHandle::open_persistent(data_path, schema, config)?,
                view: 0,
                released: AtomicBool::new(false),
            }))
        })
    }

    /// Register and return a schema view over the same runtime owner.
    pub fn register_schema(&self, schema: Vec<u8>) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("RnDb.register_schema", || {
            let view = self.actor.register_schema(self.view, schema)?;
            Ok(Arc::new(Self {
                actor: Arc::clone(&self.actor),
                view,
                released: AtomicBool::new(false),
            }))
        })
    }

    /// Install the foreign tick callback through the detached notifier.
    pub fn set_tick_scheduler(
        &self,
        callback: Box<dyn TickSchedulerCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.set_tick_scheduler", || {
            self.actor.set_tick_scheduler(callback)
        })
    }

    /// Register the callback for rejected writes not consumed by an active wait.
    pub fn on_mutation_error(
        &self,
        callback: Box<dyn MutationErrorCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.on_mutation_error", || {
            self.actor.on_mutation_error(self.view, callback)
        })
    }

    /// Drive every attached peer connection once.
    pub fn tick(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.tick", || self.actor.tick())
    }

    /// Close the root runtime, or release this non-root schema view.
    pub fn close(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.close", || {
            if self.view == 0 {
                self.actor.close()
            } else {
                self.free()
            }
        })
    }

    /// Release a non-root schema view without closing the shared runtime.
    pub fn free(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.free", || {
            if self.view == 0 || self.released.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            self.actor.release_view(self.view)
        })
    }

    /// Validate and retain a postcard query.
    pub fn prepare_query(&self, query: Vec<u8>) -> Result<Arc<RnPreparedQuery>, JazzRnError> {
        with_panic_boundary("RnDb.prepare_query", || {
            Ok(Arc::new(RnPreparedQuery {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.prepare_query(self.view, query)?,
            }))
        })
    }

    /// Read all matching rows as the database identity.
    pub fn all(
        &self,
        query: Arc<RnPreparedQuery>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            self.actor.all(self.view, query.id, None, opts_json)
        })
    }

    /// Read all matching rows as an explicit identity.
    pub fn all_for_identity(
        &self,
        query: Arc<RnPreparedQuery>,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_for_identity", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            self.actor.all(self.view, query.id, Some(author), opts_json)
        })
    }

    /// Read an output-changing relation query as rows.
    pub fn all_relation_query(
        &self,
        query_json: String,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_relation_query", || {
            self.actor
                .all_relation_query(self.view, query_json, None, opts_json)
        })
    }

    /// Read an output-changing relation query as an explicit identity.
    pub fn all_relation_query_for_identity(
        &self,
        query_json: String,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_relation_query_for_identity", || {
            self.actor
                .all_relation_query(self.view, query_json, Some(author), opts_json)
        })
    }

    /// Read a correlated relation snapshot.
    pub fn all_relation_snapshot(
        &self,
        query: Arc<RnPreparedQuery>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_relation_snapshot", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            self.actor
                .all_relation_snapshot(self.view, query.id, None, opts_json)
        })
    }

    /// Read a correlated relation snapshot as an explicit identity.
    pub fn all_relation_snapshot_for_identity(
        &self,
        query: Arc<RnPreparedQuery>,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_relation_snapshot_for_identity", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            self.actor
                .all_relation_snapshot(self.view, query.id, Some(author), opts_json)
        })
    }

    /// Read through a mergeable or exclusive transaction's overlay.
    pub fn all_in_transaction(
        &self,
        query: Arc<RnPreparedQuery>,
        transaction: Arc<RnTx>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_in_transaction", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_actor(&self.actor, &transaction.actor, "transaction")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            ensure_same_view(self.view, transaction.view, "transaction")?;
            self.actor
                .all_in_transaction(query.id, transaction.id, None, opts_json)
        })
    }

    /// Read through a mergeable or exclusive transaction's overlay as an explicit identity.
    pub fn all_in_transaction_for_identity(
        &self,
        query: Arc<RnPreparedQuery>,
        transaction: Arc<RnTx>,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_in_transaction_for_identity", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_actor(&self.actor, &transaction.actor, "transaction")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            ensure_same_view(self.view, transaction.view, "transaction")?;
            self.actor
                .all_in_transaction(query.id, transaction.id, Some(author), opts_json)
        })
    }

    /// Read one locally-current row by primary key.
    pub fn local_current_row(
        &self,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.local_current_row", || {
            self.actor.local_current_row(self.view, table, row_id)
        })
    }

    /// Set policy claims for an author from a JSON object.
    pub fn set_identity_claims(
        &self,
        author: Vec<u8>,
        claims_json: Option<String>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.set_identity_claims", || {
            self.actor
                .set_identity_claims(self.view, author, claims_json)
        })
    }

    /// Attach a one-shot query coverage request.
    pub fn attach_query(
        &self,
        query: Arc<RnPreparedQuery>,
        opts_json: Option<String>,
    ) -> Result<Arc<RnQueryAttachment>, JazzRnError> {
        with_panic_boundary("RnDb.attach_query", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            Ok(Arc::new(RnQueryAttachment {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .attach_query(self.view, query.id, None, opts_json)?,
                detached: AtomicBool::new(false),
            }))
        })
    }

    /// Attach a one-shot coverage request as an explicit identity.
    pub fn attach_query_for_identity(
        &self,
        query: Arc<RnPreparedQuery>,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Arc<RnQueryAttachment>, JazzRnError> {
        with_panic_boundary("RnDb.attach_query_for_identity", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            Ok(Arc::new(RnQueryAttachment {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .attach_query(self.view, query.id, Some(author), opts_json)?,
                detached: AtomicBool::new(false),
            }))
        })
    }

    /// Return whether an attachment has received upstream coverage.
    pub fn query_attachment_is_covered(
        &self,
        attachment: Arc<RnQueryAttachment>,
    ) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.query_attachment_is_covered", || {
            ensure_same_actor(&self.actor, &attachment.actor, "query attachment")?;
            self.actor.attachment_is_covered(attachment.id)
        })
    }

    /// Detach a query coverage request.
    pub fn detach_query(&self, attachment: Arc<RnQueryAttachment>) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.detach_query", || {
            ensure_same_actor(&self.actor, &attachment.actor, "query attachment")?;
            self.actor.detach_query(attachment.id)?;
            attachment.detached.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    /// Subscribe to a prepared query.
    pub fn subscribe(
        &self,
        query: Arc<RnPreparedQuery>,
        opts_json: Option<String>,
    ) -> Result<Arc<RnSubscription>, JazzRnError> {
        with_panic_boundary("RnDb.subscribe", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self.actor.subscribe(self.view, query.id, None, opts_json)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Subscribe to a prepared query as an explicit identity.
    pub fn subscribe_for_identity(
        &self,
        query: Arc<RnPreparedQuery>,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Arc<RnSubscription>, JazzRnError> {
        with_panic_boundary("RnDb.subscribe_for_identity", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_view(self.view, query.view, "prepared query")?;
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .subscribe(self.view, query.id, Some(author), opts_json)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Subscribe to an output-changing relation query.
    pub fn subscribe_relation_query(
        &self,
        query_json: String,
        opts_json: Option<String>,
    ) -> Result<Arc<RnSubscription>, JazzRnError> {
        with_panic_boundary("RnDb.subscribe_relation_query", || {
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .subscribe_relation_query(self.view, query_json, None, opts_json)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Subscribe to a relation query as an explicit identity.
    pub fn subscribe_relation_query_for_identity(
        &self,
        query_json: String,
        author: Vec<u8>,
        opts_json: Option<String>,
    ) -> Result<Arc<RnSubscription>, JazzRnError> {
        with_panic_boundary("RnDb.subscribe_relation_query_for_identity", || {
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self.actor.subscribe_relation_query(
                    self.view,
                    query_json,
                    Some(author),
                    opts_json,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Insert a caller-supplied row id.
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.insert_with_id_encoded", || {
            let write = self.actor.insert_with_id(
                self.view,
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Insert while evaluating policy as an explicit identity.
    pub fn insert_with_id_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.insert_with_id_encoded_for_identity", || {
            let write = self.actor.insert_with_id(
                self.view,
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Update a row.
    pub fn update_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.update_encoded", || {
            let write = self.actor.update(
                self.view,
                table,
                row_id,
                patch,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Update while evaluating policy as an explicit identity.
    pub fn update_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.update_encoded_for_identity", || {
            let write = self.actor.update(
                self.view,
                table,
                row_id,
                patch,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Insert or update a row.
    pub fn upsert_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.upsert_encoded", || {
            let write = self.actor.upsert(
                self.view,
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Upsert while evaluating policy as an explicit identity.
    pub fn upsert_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.upsert_encoded_for_identity", || {
            let write = self.actor.upsert(
                self.view,
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Soft-delete a row.
    pub fn delete(
        &self,
        table: String,
        row_id: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.delete", || {
            let write = self.actor.delete(
                self.view,
                table,
                row_id,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Delete while evaluating policy as an explicit identity.
    pub fn delete_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.delete_for_identity", || {
            let write = self.actor.delete(
                self.view,
                table,
                row_id,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Restore a deleted row.
    pub fn restore_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.restore_encoded", || {
            let write = self.actor.restore(
                self.view,
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Restore while evaluating policy as an explicit identity.
    pub fn restore_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.restore_encoded_for_identity", || {
            let write = self.actor.restore(
                self.view,
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Begin one owner-wide batch without creating a transaction attachment.
    pub fn begin_transaction(
        &self,
        open_batch_id: String,
        kind: String,
        author: Option<Vec<u8>>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.begin_transaction", || {
            self.actor
                .begin_batch(open_batch_id, TransactionKind::from_str(&kind)?, author)
        })
    }

    /// Commit an owner-wide batch by caller-minted id.
    pub fn commit_transaction(
        &self,
        open_batch_id: String,
        kind: Option<String>,
    ) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnDb.commit_transaction", || {
            let kind = kind.as_deref().map(TransactionKind::from_str).transpose()?;
            let write = self.actor.commit_batch(open_batch_id, kind)?;
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Roll back an owner-wide batch by caller-minted id.
    pub fn rollback_transaction(&self, open_batch_id: String) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.rollback_transaction", || {
            self.actor.rollback_batch(open_batch_id)
        })
    }

    /// Attach this schema view to an existing mergeable batch.
    pub fn attach_mergeable_tx(&self, open_batch_id: String) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.attach_mergeable_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.attach_transaction(
                    self.view,
                    open_batch_id,
                    TransactionKind::Mergeable,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Attach this schema view to an existing exclusive batch.
    pub fn attach_exclusive_tx(&self, open_batch_id: String) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.attach_exclusive_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.attach_transaction(
                    self.view,
                    open_batch_id,
                    TransactionKind::Exclusive,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Begin a mergeable batch and return its owning attachment.
    pub fn mergeable_tx(&self, open_batch_id: String) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.mergeable_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.open_owning_transaction(
                    self.view,
                    open_batch_id,
                    TransactionKind::Mergeable,
                    None,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Begin a mergeable batch as an explicit identity and return its owner.
    pub fn mergeable_tx_for_identity(
        &self,
        open_batch_id: String,
        author: Vec<u8>,
    ) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.mergeable_tx_for_identity", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.open_owning_transaction(
                    self.view,
                    open_batch_id,
                    TransactionKind::Mergeable,
                    Some(author),
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Begin an exclusive batch and return its owning attachment.
    pub fn exclusive_tx(&self, open_batch_id: String) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.exclusive_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                view: self.view,
                id: self.actor.open_owning_transaction(
                    self.view,
                    open_batch_id,
                    TransactionKind::Exclusive,
                    None,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Attach a binding-owned upstream transport.
    pub fn connect_upstream(&self) -> Result<Arc<RnTransport>, JazzRnError> {
        with_panic_boundary("RnDb.connect_upstream", || {
            Ok(Arc::new(RnTransport {
                actor: Arc::clone(&self.actor),
                id: self.actor.connect_upstream(self.view)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Connect using the protocol and authority facts negotiated by the carrier.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_upstream_with_session(
        &self,
        protocol_version: u16,
        features: u32,
        remote_node: Vec<u8>,
        remote_epoch: u64,
        local_node: Vec<u8>,
        local_epoch: u64,
    ) -> Result<Arc<RnTransport>, JazzRnError> {
        with_panic_boundary("RnDb.connect_upstream_with_session", || {
            Ok(Arc::new(RnTransport {
                actor: Arc::clone(&self.actor),
                id: self.actor.connect_upstream_with_session(
                    self.view,
                    protocol_version,
                    features,
                    remote_node,
                    remote_epoch,
                    local_node,
                    local_epoch,
                )?,
                closed: AtomicBool::new(false),
            }))
        })
    }
}

impl Drop for RnDb {
    fn drop(&mut self) {
        if self.view != 0 {
            release_once(&self.released, || {
                self.actor.release_view_if_present(self.view)
            });
        }
    }
}

/// Opaque prepared-query handle.
#[derive(uniffi::Object)]
pub struct RnPreparedQuery {
    actor: Arc<ActorHandle>,
    view: u64,
    id: u64,
}

impl Drop for RnPreparedQuery {
    fn drop(&mut self) {
        self.actor.release_query(self.id);
    }
}

/// Opaque query-coverage attachment.
#[derive(uniffi::Object)]
pub struct RnQueryAttachment {
    actor: Arc<ActorHandle>,
    id: u64,
    detached: AtomicBool,
}

impl Drop for RnQueryAttachment {
    fn drop(&mut self) {
        release_once(&self.detached, || self.actor.release_attachment(self.id));
    }
}

/// Open transaction handle.
#[derive(uniffi::Object)]
pub struct RnTx {
    actor: Arc<ActorHandle>,
    view: u64,
    id: u64,
    closed: AtomicBool,
}

impl RnTx {
    fn ensure_open(&self) -> Result<(), JazzRnError> {
        ensure_open(&self.closed, "transaction")
    }
}

#[uniffi::export]
impl RnTx {
    /// Stage an insert.
    pub fn insert_with_id_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.insert_with_id_encoded", || {
            self.ensure_open()?;
            self.actor.tx_insert(
                self.id,
                table,
                row_id,
                cells,
                updated_at_ms(updated_at_ms_value)?,
            )
        })
    }

    /// Stage an update.
    pub fn update_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.update_encoded", || {
            self.ensure_open()?;
            self.actor.tx_update(
                self.id,
                table,
                row_id,
                patch,
                updated_at_ms(updated_at_ms_value)?,
            )
        })
    }

    /// Stage an upsert.
    pub fn upsert_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.upsert_encoded", || {
            self.ensure_open()?;
            self.actor.tx_upsert(
                self.id,
                table,
                row_id,
                cells,
                updated_at_ms(updated_at_ms_value)?,
            )
        })
    }

    /// Stage a soft deletion.
    pub fn delete(
        &self,
        table: String,
        row_id: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.delete", || {
            self.ensure_open()?;
            self.actor
                .tx_delete(self.id, table, row_id, updated_at_ms(updated_at_ms_value)?)
        })
    }

    /// Stage a restore.
    pub fn restore_encoded(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms_value: Option<f64>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.restore_encoded", || {
            self.ensure_open()?;
            self.actor.tx_restore(
                self.id,
                table,
                row_id,
                cells,
                updated_at_ms(updated_at_ms_value)?,
            )
        })
    }

    /// Commit and return a write-state handle.
    pub fn commit(&self) -> Result<Arc<RnWrite>, JazzRnError> {
        with_panic_boundary("RnTx.commit", || {
            self.ensure_open()?;
            let write = self.actor.commit_transaction(self.id)?;
            self.closed.store(true, Ordering::SeqCst);
            write_object(Arc::clone(&self.actor), write)
        })
    }

    /// Abandon the transaction.
    pub fn rollback(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTx.rollback", || {
            self.ensure_open()?;
            self.actor.rollback_transaction(self.id)?;
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

impl Drop for RnTx {
    fn drop(&mut self) {
        release_once(&self.closed, || self.actor.release_transaction(self.id));
    }
}

/// Committed write and its observable durability state.
#[derive(uniffi::Object)]
pub struct RnWrite {
    actor: Arc<ActorHandle>,
    id: u64,
    batch_id: String,
    closed: AtomicBool,
}

impl RnWrite {
    fn ensure_open(&self) -> Result<(), JazzRnError> {
        ensure_open(&self.closed, "write")
    }
}

#[uniffi::export]
impl RnWrite {
    /// Stable public id of the committed batch.
    pub fn batch_id(&self) -> String {
        self.batch_id.clone()
    }

    /// Postcard `(row_id, tx_id)` payload consumed by the adapter.
    pub fn payload(&self) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnWrite.payload", || {
            self.ensure_open()?;
            self.actor.write_payload(self.id)
        })
    }

    /// Wait until the write reaches the requested durability tier or is rejected.
    pub async fn wait(&self, tier: String) -> Result<(), JazzRnError> {
        with_async_panic_boundary("RnWrite.wait", || async {
            self.ensure_open()?;
            let (id, receiver) = self.actor.wait_for_write(self.id, tier)?;
            let registration = WriteWaitRegistration {
                actor: Arc::clone(&self.actor),
                id,
            };
            let signal = receiver.await.map_err(|_| closed_error())?;
            drop(registration);
            match signal {
                WaiterSignal::Completed(result) => result,
                WaiterSignal::Closed => Err(closed_error()),
                WaiterSignal::Poisoned(reason) => Err(poisoned_error(reason)),
            }
        })
        .await
    }

    /// Return the current write state as JSON.
    pub fn write_state(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("RnWrite.write_state", || {
            self.ensure_open()?;
            self.actor.write_state(self.id)
        })
    }

    /// Release this write handle.
    pub fn close(&self) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnWrite.close", || {
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(false);
            }
            self.actor.close_write(self.id)
        })
    }
}

impl Drop for RnWrite {
    fn drop(&mut self) {
        release_once(&self.closed, || self.actor.release_write(self.id));
    }
}

struct WriteWaitRegistration {
    actor: Arc<ActorHandle>,
    id: u64,
}

impl Drop for WriteWaitRegistration {
    fn drop(&mut self) {
        self.actor.release_waiter(self.id);
    }
}

/// Pull-based subscription stream used by the TypeScript shim.
#[derive(uniffi::Object)]
pub struct RnSubscription {
    actor: Arc<ActorHandle>,
    id: u64,
    closed: AtomicBool,
}

#[uniffi::export]
impl RnSubscription {
    /// Drain all currently queued events with binary deltas kept as bytes.
    pub fn read_all(&self) -> Result<Vec<RnSubscriptionEvent>, JazzRnError> {
        with_panic_boundary("RnSubscription.read_all", || {
            if self.closed.load(Ordering::SeqCst) {
                return Err(JazzRnError::Runtime {
                    message: "subscription is closed".to_owned(),
                });
            }
            self.actor.subscription_read_all(self.id)
        })
    }

    /// Alias for `read_all`.
    pub fn drain(&self) -> Result<Vec<RnSubscriptionEvent>, JazzRnError> {
        self.read_all()
    }

    /// Close the subscription.
    pub fn close(&self) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnSubscription.close", || {
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(false);
            }
            self.actor.close_subscription(self.id)
        })
    }
}

impl Drop for RnSubscription {
    fn drop(&mut self) {
        release_once(&self.closed, || self.actor.release_subscription(self.id));
    }
}

/// Binding-owned wire transport connected to an upstream peer.
#[derive(uniffi::Object)]
pub struct RnTransport {
    actor: Arc<ActorHandle>,
    id: u64,
    closed: AtomicBool,
}

impl RnTransport {
    fn ensure_open(&self) -> Result<(), JazzRnError> {
        ensure_open(&self.closed, "transport")
    }
}

#[uniffi::export]
impl RnTransport {
    /// Enqueue one inbound wire frame.
    pub fn send_wire_frame(&self, frame: Vec<u8>) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTransport.send_wire_frame", || {
            self.ensure_open()?;
            self.actor.transport_send(self.id, vec![frame])
        })
    }

    /// Enqueue a batch of inbound wire frames.
    pub fn send_wire_frames(&self, frames: Vec<Vec<u8>>) -> Result<(), JazzRnError> {
        with_panic_boundary("RnTransport.send_wire_frames", || {
            self.ensure_open()?;
            self.actor.transport_send(self.id, frames)
        })
    }

    /// Drain outbound wire frames.
    pub fn recv_wire_frames(&self) -> Result<Vec<Vec<u8>>, JazzRnError> {
        with_panic_boundary("RnTransport.recv_wire_frames", || {
            self.ensure_open()?;
            self.actor.transport_recv(self.id)
        })
    }

    /// Pump this peer connection and return emitted subscription-event count.
    pub fn tick(&self) -> Result<u32, JazzRnError> {
        with_panic_boundary("RnTransport.tick", || {
            self.ensure_open()?;
            self.actor.transport_tick(self.id)
        })
    }

    /// Detach the peer connection.
    pub fn close(&self) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnTransport.close", || {
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(false);
            }
            self.actor.close_transport(self.id)
        })
    }
}

impl Drop for RnTransport {
    fn drop(&mut self) {
        release_once(&self.closed, || self.actor.release_transport(self.id));
    }
}

/// Mint a local-first JWT at a caller-supplied timestamp.
#[uniffi::export]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
    now_seconds: u64,
) -> Result<String, JazzRnError> {
    with_panic_boundary("mint_local_first_token", || {
        mint_token(
            seed_b64,
            audience,
            ttl_seconds,
            now_seconds,
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
        )
    })
}

/// Mint an anonymous JWT at a caller-supplied timestamp.
#[uniffi::export]
pub fn mint_anonymous_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
    now_seconds: u64,
) -> Result<String, JazzRnError> {
    with_panic_boundary("mint_anonymous_token", || {
        mint_token(
            seed_b64,
            audience,
            ttl_seconds,
            now_seconds,
            jazz::tools::identity::ANONYMOUS_ISSUER,
        )
    })
}

fn mint_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
    now_seconds: u64,
    issuer: &'static str,
) -> Result<String, JazzRnError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|error| JazzRnError::InvalidPayload {
            message: format!("invalid base64 seed: {error}"),
        })?;
    let seed: [u8; 32] = bytes.try_into().map_err(|_| JazzRnError::InvalidPayload {
        message: "seed must be exactly 32 bytes".to_owned(),
    })?;
    jazz::tools::identity::mint_jazz_self_signed_token_at(
        &seed,
        issuer,
        &audience,
        ttl_seconds as u64,
        now_seconds,
    )
    .map_err(|message| JazzRnError::Internal { message })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use jazz::binding_support::{OpenDbConfig, OpenDbIdentity};
    use jazz::db::{CommitUnitTrust, ConnectionSessionContext, DbIdentity};
    use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value, ValueType};
    use jazz::groove::schema::{ColumnSchema, ColumnType};
    use jazz::ids::{AuthorId, NodeUuid, RowUuid};
    use jazz::protocol::SyncMessage;
    use jazz::query::Query;
    use jazz::schema::{JazzSchema, Policy, TableSchema};
    use jazz::serving::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};
    use jazz::tools::OpenBatchId;
    use jazz::tx::TxId;
    use jazz::wire::{
        WIRE_PROTOCOL_VERSION, WireAuthorityEndpoint, WireFrame, WireStreamDecoder,
        current_wire_features, decode_frame, decode_sync_message,
    };

    use super::RnDb;

    type DecodedRows = Vec<(String, RecordDescriptor, Vec<(RowUuid, bool, Vec<u8>)>)>;

    fn schema(table: &str) -> JazzSchema {
        JazzSchema::new([
            TableSchema::new(table, [ColumnSchema::new("title", ColumnType::String)])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ])
    }

    fn encoded_schema() -> Vec<u8> {
        postcard::to_allocvec(&schema("todos")).expect("encode test schema")
    }

    fn encoded_schema_for(table: &str) -> Vec<u8> {
        postcard::to_allocvec(&schema(table)).expect("encode named test schema")
    }

    fn encoded_empty_schema() -> Vec<u8> {
        postcard::to_allocvec(&JazzSchema::new([])).expect("encode empty schema")
    }

    fn encoded_config(node: [u8; 16], author: [u8; 16]) -> Vec<u8> {
        postcard::to_allocvec(&OpenDbConfig {
            identity: OpenDbIdentity {
                node: NodeUuid::from_bytes(node),
                author: AuthorId::from_bytes(author),
            },
            row_id_seed: Some(1),
            history_complete: false,
            initial_sync_flush_every: None,
        })
        .expect("encode test config")
    }

    fn encoded_cells_with_title(title: &str) -> Vec<u8> {
        let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
        let record = descriptor
            .create(&[Value::String(title.to_owned())])
            .expect("encode test row");
        postcard::to_allocvec(&(descriptor, record)).expect("encode test cells")
    }

    fn encoded_cells() -> Vec<u8> {
        encoded_cells_with_title("survives restart")
    }

    fn open_memory_with(schema: Vec<u8>, node: u8) -> Arc<RnDb> {
        RnDb::open_memory(schema, encoded_config([node; 16], [node; 16]))
            .expect("open memory binding")
    }

    fn prepared_table(db: &Arc<RnDb>, table: &str) -> Arc<super::RnPreparedQuery> {
        db.prepare_query(postcard::to_allocvec(&Query::from(table)).expect("encode test query"))
            .expect("prepare test query")
    }

    fn decode_rows(bytes: &[u8]) -> DecodedRows {
        postcard::from_bytes(bytes).expect("decode binding rows")
    }

    fn decoded_row_count(bytes: &[u8]) -> usize {
        decode_rows(bytes)
            .iter()
            .map(|(_, _, rows)| rows.len())
            .sum()
    }

    fn decoded_title(bytes: &[u8], wanted: RowUuid) -> Option<String> {
        for (_, descriptor, rows) in decode_rows(bytes) {
            let title = descriptor
                .field_index("user_title")
                .or_else(|| descriptor.field_index("title"))
                .expect("title field in native row");
            for (row_id, _, raw) in rows {
                if row_id != wanted {
                    continue;
                }
                let title = match BorrowedRecord::new(&raw, &descriptor)
                    .get_idx(title)
                    .expect("decode row title")
                {
                    Value::String(title) => title,
                    Value::Nullable(Some(value)) => match *value {
                        Value::String(title) => title,
                        other => panic!("title must be a string, got {other:?}"),
                    },
                    other => panic!("title must be a string, got {other:?}"),
                };
                return Some(title);
            }
        }
        None
    }

    fn pump_session(
        client: &Arc<RnDb>,
        transport: &Arc<super::RnTransport>,
        server: &mut InMemoryServerShell,
        session: ServerSession,
    ) {
        client.tick().expect("tick client");
        transport.tick().expect("tick client transport");
        server
            .receive_frames(
                session,
                transport.recv_wire_frames().expect("drain client frames"),
            )
            .expect("deliver client frames");
        server.tick().expect("tick server");
        transport
            .send_wire_frames(server.take_frames(session).expect("drain server frames"))
            .expect("deliver server frames");
        transport.tick().expect("consume server frames");
    }

    fn outbound_messages(db: &Arc<RnDb>) -> Vec<SyncMessage> {
        let transport = db.connect_upstream().expect("attach upstream transport");
        let mut frames = Vec::new();
        for _ in 0..4 {
            transport.tick().expect("tick reopened transport");
            frames.extend(
                transport
                    .recv_wire_frames()
                    .expect("drain reopened transport"),
            );
        }

        let mut decoder = WireStreamDecoder::new(current_wire_features())
            .expect("construct outbound stream decoder");
        frames
            .into_iter()
            .filter_map(
                |frame| match decode_frame(&frame).expect("decode outbound frame") {
                    WireFrame::Message(envelope) => {
                        let payload = decoder
                            .decode_message(&envelope.payload, envelope.features)
                            .expect("decode outbound stream payload");
                        Some(decode_sync_message(&payload).expect("decode outbound sync message"))
                    }
                    WireFrame::Hello(_) | WireFrame::Error(_) | WireFrame::MessageFragment(_) => {
                        None
                    }
                },
            )
            .collect()
    }

    #[test]
    fn persistent_reopen_with_same_identity_reschedules_pending_write() {
        // Pending-upload restoration is observable only through the binding's
        // raw transport contract, so this black-boxes RnDb and decodes the
        // emitted public wire message rather than inspecting actor state.
        let directory = tempfile::tempdir().expect("create test directory");
        let path = directory.path().join("nested").join("jazz.db");
        let schema = encoded_schema();
        let config = encoded_config([0x11; 16], [0x22; 16]);
        let db = RnDb::open_persistent(
            path.to_string_lossy().into_owned(),
            schema.clone(),
            config.clone(),
        )
        .expect("open persistent binding");
        let write = db
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x33; 16],
                encoded_cells(),
                Some(1_000.0),
            )
            .expect("insert offline row");
        futures::executor::block_on(write.wait("local".to_owned()))
            .expect("write reaches local durability");
        let (_, expected_tx): (RowUuid, TxId) =
            postcard::from_bytes(&write.payload().expect("read write payload"))
                .expect("decode write payload");
        write.close().expect("close write handle");
        db.close().expect("close first persistent binding");

        let wrong_identity = RnDb::open_persistent(
            path.to_string_lossy().into_owned(),
            schema.clone(),
            encoded_config([0x44; 16], [0x22; 16]),
        )
        .expect("reopen with a different node identity");
        assert!(outbound_messages(&wrong_identity).into_iter().all(|message| {
            !matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == expected_tx)
        }));
        wrong_identity
            .close()
            .expect("close wrong-identity binding");

        let reopened = RnDb::open_persistent(path.to_string_lossy().into_owned(), schema, config)
            .expect("reopen with the original identity");
        assert!(outbound_messages(&reopened).into_iter().any(|message| {
            matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == expected_tx)
        }));
        reopened.close().expect("close reopened binding");
    }

    #[test]
    fn transaction_reads_own_writes_for_both_kinds_and_honor_read_opts() {
        for (kind, node) in [("mergeable", 0x61), ("exclusive", 0x62)] {
            let db = open_memory_with(encoded_schema(), node);
            let existing_id = RowUuid::from_bytes([0x11; 16]);
            let existing_write = db
                .insert_with_id_encoded(
                    "todos".to_owned(),
                    existing_id.to_bytes(),
                    encoded_cells_with_title("existing"),
                    None,
                )
                .expect("insert existing row");
            futures::executor::block_on(existing_write.wait("local".to_owned()))
                .expect("existing row reaches local durability");

            let batch = OpenBatchId::new().to_string();
            db.begin_transaction(batch.clone(), kind.to_owned(), None)
                .expect("begin batch");
            let tx = match kind {
                "mergeable" => db.attach_mergeable_tx(batch.clone()),
                "exclusive" => db.attach_exclusive_tx(batch.clone()),
                _ => unreachable!(),
            }
            .expect("attach transaction");
            tx.insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x12; 16],
                encoded_cells_with_title("staged"),
                None,
            )
            .expect("stage insert");
            tx.delete("todos".to_owned(), existing_id.to_bytes(), None)
                .expect("stage delete");

            let query = prepared_table(&db, "todos");
            let ordinary = db
                .all_in_transaction(Arc::clone(&query), Arc::clone(&tx), None)
                .expect("read transaction without deleted rows");
            assert_eq!(decoded_row_count(&ordinary), 1, "{kind}");

            let including_deleted = db
                .all_in_transaction(
                    query,
                    tx,
                    Some(r#"{"include_deleted":true,"propagation":"local-only"}"#.to_owned()),
                )
                .expect("read transaction with non-default options");
            let rows = decode_rows(&including_deleted);
            assert_eq!(
                rows.iter().map(|(_, _, rows)| rows.len()).sum::<usize>(),
                2,
                "{kind}"
            );
            assert!(
                rows.iter()
                    .flat_map(|(_, _, rows)| rows)
                    .any(|(row_id, deleted, _)| *row_id == existing_id && *deleted),
                "{kind} must honor include_deleted; decoded rows: {rows:?}"
            );
            db.rollback_transaction(batch).expect("roll back batch");
            db.close().expect("close transaction test db");
        }
    }

    #[test]
    fn dropping_an_attached_transaction_preserves_the_owner_batch() {
        let db = open_memory_with(encoded_schema(), 0x63);
        let batch = OpenBatchId::new().to_string();
        db.begin_transaction(batch.clone(), "mergeable".to_owned(), None)
            .expect("begin owner batch");
        drop(
            db.attach_mergeable_tx(batch.clone())
                .expect("attach first view"),
        );

        let tx = db
            .attach_mergeable_tx(batch.clone())
            .expect("reattach after drop");
        tx.insert_with_id_encoded(
            "todos".to_owned(),
            vec![0x21; 16],
            encoded_cells_with_title("survived"),
            None,
        )
        .expect("stage after attachment drop");
        let write = db
            .commit_transaction(batch, Some("mergeable".to_owned()))
            .expect("commit owner batch");
        assert_eq!(write.batch_id().len(), 32);
        assert_eq!(
            decoded_row_count(
                &db.all(prepared_table(&db, "todos"), None)
                    .expect("read committed row")
            ),
            1
        );
        db.close().expect("close db");
    }

    #[test]
    fn one_batch_has_independent_attachments_in_two_schema_views() {
        let db = open_memory_with(encoded_empty_schema(), 0x64);
        let first = db
            .register_schema(encoded_schema())
            .expect("register first view");
        let second = db
            .register_schema(encoded_schema())
            .expect("register second view");
        let batch = OpenBatchId::new().to_string();
        db.begin_transaction(batch.clone(), "mergeable".to_owned(), None)
            .expect("begin shared batch");
        let first_tx = first
            .attach_mergeable_tx(batch.clone())
            .expect("attach first schema view");
        let second_tx = second
            .attach_mergeable_tx(batch.clone())
            .expect("attach second schema view");
        first_tx
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x31; 16],
                encoded_cells_with_title("first"),
                None,
            )
            .expect("stage through first view");
        second_tx
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x32; 16],
                encoded_cells_with_title("second"),
                None,
            )
            .expect("stage through second view");
        db.commit_transaction(batch, None)
            .expect("commit shared batch");

        for view in [&first, &second] {
            assert_eq!(
                decoded_row_count(
                    &view
                        .all(prepared_table(view, "todos"), None)
                        .expect("read shared rows")
                ),
                2
            );
        }
        first.free().expect("release first view");
        second.free().expect("release second view");
        db.close().expect("close owner");
    }

    #[test]
    fn staged_upsert_updates_an_existing_row() {
        let db = open_memory_with(encoded_schema(), 0x65);
        let row_id = RowUuid::from_bytes([0x41; 16]);
        db.insert_with_id_encoded(
            "todos".to_owned(),
            row_id.to_bytes(),
            encoded_cells_with_title("before"),
            None,
        )
        .expect("insert base row");
        let batch = OpenBatchId::new().to_string();
        db.begin_transaction(batch.clone(), "mergeable".to_owned(), None)
            .expect("begin upsert batch");
        db.attach_mergeable_tx(batch.clone())
            .expect("attach upsert batch")
            .upsert_encoded(
                "todos".to_owned(),
                row_id.to_bytes(),
                encoded_cells_with_title("after"),
                None,
            )
            .expect("stage upsert");
        db.commit_transaction(batch, None)
            .expect("commit upsert batch");

        let row = db
            .local_current_row("todos".to_owned(), row_id.to_bytes())
            .expect("read upserted row");
        assert_eq!(decoded_row_count(&row), 1);
        assert_eq!(decoded_title(&row, row_id).as_deref(), Some("after"));
        db.close().expect("close db");
    }

    #[test]
    fn batch_lifecycle_validates_kind_identity_and_terminal_operations() {
        let db = open_memory_with(encoded_schema(), 0x66);
        assert!(
            db.begin_transaction(OpenBatchId::new().to_string(), "unknown".to_owned(), None)
                .is_err()
        );
        assert!(
            db.begin_transaction(
                OpenBatchId::new().to_string(),
                "exclusive".to_owned(),
                Some(vec![0x66; 16])
            )
            .is_err()
        );

        let committed = OpenBatchId::new().to_string();
        db.begin_transaction(committed.clone(), "exclusive".to_owned(), None)
            .expect("begin exclusive batch");
        db.attach_exclusive_tx(committed.clone())
            .expect("attach exclusive batch")
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x51; 16],
                encoded_cells_with_title("commit"),
                None,
            )
            .expect("stage exclusive insert");
        db.commit_transaction(committed, Some("exclusive".to_owned()))
            .expect("commit exclusive batch");

        let rolled_back = OpenBatchId::new().to_string();
        db.begin_transaction(rolled_back.clone(), "mergeable".to_owned(), None)
            .expect("begin rollback batch");
        db.attach_mergeable_tx(rolled_back.clone())
            .expect("attach rollback batch")
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x52; 16],
                encoded_cells_with_title("rollback"),
                None,
            )
            .expect("stage rolled-back insert");
        db.rollback_transaction(rolled_back)
            .expect("roll back mergeable batch");
        assert_eq!(
            decoded_row_count(
                &db.all(prepared_table(&db, "todos"), None)
                    .expect("read terminal batch result")
            ),
            1
        );
        db.close().expect("close db");
    }

    #[test]
    fn registered_schema_views_are_isolated_and_free_keeps_root_alive() {
        let db = open_memory_with(encoded_empty_schema(), 0x67);
        let todos = db
            .register_schema(encoded_schema_for("todos"))
            .expect("register todos view");
        let notes = db
            .register_schema(encoded_schema_for("notes"))
            .expect("register notes view");
        prepared_table(&todos, "todos");
        let scoped_transport = todos
            .connect_upstream()
            .expect("attach transport to todos view");
        assert!(
            notes
                .prepare_query(
                    postcard::to_allocvec(&Query::from("todos")).expect("encode wrong-view query")
                )
                .is_err()
        );

        todos.free().expect("free todos view");
        assert!(
            todos
                .prepare_query(
                    postcard::to_allocvec(&Query::from("todos")).expect("encode freed-view query")
                )
                .is_err()
        );
        assert!(scoped_transport.recv_wire_frames().is_err());
        prepared_table(&notes, "notes");
        db.tick().expect("root remains usable after view free");
        notes.free().expect("free notes view");
        db.close().expect("close root");
    }

    #[test]
    fn session_bound_transport_round_trips_with_an_in_process_server() {
        let client_node = NodeUuid::from_bytes([0x71; 16]);
        let server_node = NodeUuid::from_bytes([0x72; 16]);
        let identity = AuthorId::from_bytes([0x71; 16]);
        let client = RnDb::open_memory(encoded_schema(), encoded_config([0x71; 16], [0x71; 16]))
            .expect("open session client");
        let mut server = InMemoryServerShell::start(
            InMemoryServerShellConfig::new(
                schema("todos"),
                DbIdentity {
                    node: server_node,
                    author: AuthorId::SYSTEM,
                },
            )
            .with_role(NodeRole::Core),
        )
        .expect("start in-process server");
        let features = current_wire_features() as u32;
        let client_epoch = 17;
        let server_epoch = 29;
        let transport = client
            .connect_upstream_with_session(
                WIRE_PROTOCOL_VERSION,
                features,
                server_node.to_bytes(),
                server_epoch,
                client_node.to_bytes(),
                client_epoch,
            )
            .expect("connect session-bound client transport");
        let session = server
            .accept_subscriber_session_with_claims_and_trust_and_context(
                identity,
                BTreeMap::new(),
                CommitUnitTrust::Session,
                u64::from(features),
                Some(ConnectionSessionContext {
                    local: WireAuthorityEndpoint {
                        node: server_node,
                        epoch: server_epoch,
                    },
                    remote: WireAuthorityEndpoint {
                        node: client_node,
                        epoch: client_epoch,
                    },
                    link_identity: identity,
                    negotiated_features: u64::from(features),
                }),
            )
            .expect("accept session-bound server transport");

        let write = client
            .insert_with_id_encoded(
                "todos".to_owned(),
                vec![0x73; 16],
                encoded_cells_with_title("session round trip"),
                None,
            )
            .expect("insert session row");
        for _ in 0..8 {
            pump_session(&client, &transport, &mut server, session);
        }
        futures::executor::block_on(write.wait("global".to_owned()))
            .expect("session write reaches core authority");
        assert!(server.metrics_snapshot().frames_received > 0);
        transport.close().expect("close client transport");
        server.close_session(session).expect("close server session");
        client.close().expect("close client");
    }
}
