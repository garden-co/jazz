//! React Native bindings for the current Jazz database facade.
//!
//! Jazz's core is intentionally thread-affine. Every database therefore owns
//! one Rust actor thread; UniFFI objects are sendable ids that marshal work to
//! that actor rather than moving `Rc`/`RefCell` core values across threads.

uniffi::setup_scaffolding!();

mod actor;
mod scheduler;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine;
use futures::FutureExt;
use jazz::binding_support::BindingError;
use jazz::db::{Error as DbError, ErrorCode};

use actor::{ActorHandle, TransactionKind, WaiterSignal, WriteOpened};

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

fn write_object(actor: Arc<ActorHandle>, opened: WriteOpened) -> Arc<RnWrite> {
    Arc::new(RnWrite {
        actor,
        id: opened.id,
        closed: AtomicBool::new(false),
    })
}

/// Called from the detached notifier when Jazz needs another core tick.
#[uniffi::export(callback_interface)]
pub trait TickSchedulerCallback: Send + Sync {
    /// Request a tick with `"immediate"` or `"deferred"` urgency.
    fn on_tick_needed(&self, urgency: String);
}

/// A React Native database backed by one dedicated Jazz actor thread.
#[derive(uniffi::Object)]
pub struct RnDb {
    actor: Arc<ActorHandle>,
}

#[uniffi::export]
impl RnDb {
    /// Open an ephemeral in-memory database.
    #[uniffi::constructor]
    pub fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("RnDb.open_memory", || {
            Ok(Arc::new(Self {
                actor: ActorHandle::open_memory(schema, config)?,
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

    /// Drive every attached peer connection once.
    pub fn tick(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.tick", || self.actor.tick())
    }

    /// Close storage, cancel waiters, and join the core actor.
    pub fn close(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.close", || self.actor.close())
    }

    /// Validate and retain a postcard query.
    pub fn prepare_query(&self, query: Vec<u8>) -> Result<Arc<RnPreparedQuery>, JazzRnError> {
        with_panic_boundary("RnDb.prepare_query", || {
            Ok(Arc::new(RnPreparedQuery {
                actor: Arc::clone(&self.actor),
                id: self.actor.prepare_query(query)?,
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
            self.actor.all(query.id, None, opts_json)
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
            self.actor.all(query.id, Some(author), opts_json)
        })
    }

    /// Read an output-changing relation query as rows.
    pub fn all_relation_query(
        &self,
        query_json: String,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_relation_query", || {
            self.actor.all_relation_query(query_json, None, opts_json)
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
                .all_relation_query(query_json, Some(author), opts_json)
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
            self.actor.all_relation_snapshot(query.id, None, opts_json)
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
            self.actor
                .all_relation_snapshot(query.id, Some(author), opts_json)
        })
    }

    /// Read through an exclusive transaction's overlay.
    pub fn all_in_transaction(
        &self,
        query: Arc<RnPreparedQuery>,
        transaction: Arc<RnTx>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnDb.all_in_transaction", || {
            ensure_same_actor(&self.actor, &query.actor, "prepared query")?;
            ensure_same_actor(&self.actor, &transaction.actor, "transaction")?;
            self.actor
                .all_in_transaction(query.id, transaction.id, None, opts_json)
        })
    }

    /// Read through an exclusive transaction's overlay as an explicit identity.
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
            self.actor.local_current_row(table, row_id)
        })
    }

    /// Set policy claims for an author from a JSON object.
    pub fn set_identity_claims(
        &self,
        author: Vec<u8>,
        claims_json: Option<String>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("RnDb.set_identity_claims", || {
            self.actor.set_identity_claims(author, claims_json)
        })
    }

    /// Probe insert permission as the database identity.
    pub fn can_insert_encoded(&self, table: String, cells: Vec<u8>) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.can_insert_encoded", || {
            self.actor.can_insert(table, cells, None)
        })
    }

    /// Probe insert permission as an explicit identity.
    pub fn can_insert_encoded_for_identity(
        &self,
        table: String,
        cells: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.can_insert_encoded_for_identity", || {
            self.actor.can_insert(table, cells, Some(author))
        })
    }

    /// Probe read permission as an explicit identity.
    pub fn can_read_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.can_read_for_identity", || {
            self.actor.can_read(table, row_id, author)
        })
    }

    /// Probe update permission as an explicit identity.
    pub fn can_update_encoded_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        _patch: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.can_update_encoded_for_identity", || {
            self.actor.can_update(table, row_id, author)
        })
    }

    /// Probe delete permission as an explicit identity.
    pub fn can_delete_for_identity(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        with_panic_boundary("RnDb.can_delete_for_identity", || {
            self.actor.can_delete(table, row_id, author)
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
            Ok(Arc::new(RnQueryAttachment {
                actor: Arc::clone(&self.actor),
                id: self.actor.attach_query(query.id, None, opts_json)?,
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
            Ok(Arc::new(RnQueryAttachment {
                actor: Arc::clone(&self.actor),
                id: self.actor.attach_query(query.id, Some(author), opts_json)?,
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
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self.actor.subscribe(query.id, None, opts_json)?,
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
            Ok(Arc::new(RnSubscription {
                actor: Arc::clone(&self.actor),
                id: self.actor.subscribe(query.id, Some(author), opts_json)?,
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
                    .subscribe_relation_query(query_json, None, opts_json)?,
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
                id: self
                    .actor
                    .subscribe_relation_query(query_json, Some(author), opts_json)?,
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
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                patch,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                patch,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
            let write =
                self.actor
                    .delete(table, row_id, None, updated_at_ms(updated_at_ms_value)?)?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                cells,
                None,
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
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
                table,
                row_id,
                cells,
                Some(author),
                updated_at_ms(updated_at_ms_value)?,
            )?;
            Ok(write_object(Arc::clone(&self.actor), write))
        })
    }

    /// Begin a mergeable transaction.
    pub fn mergeable_tx(&self) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.mergeable_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .begin_transaction(TransactionKind::Mergeable, None)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Begin a mergeable transaction as an explicit identity.
    pub fn mergeable_tx_for_identity(&self, author: Vec<u8>) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.mergeable_tx_for_identity", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .begin_transaction(TransactionKind::Mergeable, Some(author))?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Begin an exclusive transaction.
    pub fn exclusive_tx(&self) -> Result<Arc<RnTx>, JazzRnError> {
        with_panic_boundary("RnDb.exclusive_tx", || {
            Ok(Arc::new(RnTx {
                actor: Arc::clone(&self.actor),
                id: self
                    .actor
                    .begin_transaction(TransactionKind::Exclusive, None)?,
                closed: AtomicBool::new(false),
            }))
        })
    }

    /// Attach a binding-owned upstream transport.
    pub fn connect_upstream(&self) -> Result<Arc<RnTransport>, JazzRnError> {
        with_panic_boundary("RnDb.connect_upstream", || {
            Ok(Arc::new(RnTransport {
                actor: Arc::clone(&self.actor),
                id: self.actor.connect_upstream()?,
                closed: AtomicBool::new(false),
            }))
        })
    }
}

/// Opaque prepared-query handle.
#[derive(uniffi::Object)]
pub struct RnPreparedQuery {
    actor: Arc<ActorHandle>,
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
        if !self.detached.swap(true, Ordering::SeqCst) {
            self.actor.release_attachment(self.id);
        }
    }
}

/// Open transaction handle.
#[derive(uniffi::Object)]
pub struct RnTx {
    actor: Arc<ActorHandle>,
    id: u64,
    closed: AtomicBool,
}

impl RnTx {
    fn ensure_open(&self) -> Result<(), JazzRnError> {
        if self.closed.load(Ordering::SeqCst) {
            Err(JazzRnError::Runtime {
                message: "transaction is already closed".to_owned(),
            })
        } else {
            Ok(())
        }
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
            // Matches the current WASM transaction boundary: the adapter
            // selects insert-vs-patch cells before calling this method.
            self.actor.tx_insert(
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
            Ok(write_object(Arc::clone(&self.actor), write))
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
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.actor.release_transaction(self.id);
        }
    }
}

/// Committed write and its observable durability state.
#[derive(uniffi::Object)]
pub struct RnWrite {
    actor: Arc<ActorHandle>,
    id: u64,
    closed: AtomicBool,
}

impl RnWrite {
    fn ensure_open(&self) -> Result<(), JazzRnError> {
        if self.closed.load(Ordering::SeqCst) {
            Err(JazzRnError::Runtime {
                message: "write is closed".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

#[uniffi::export]
impl RnWrite {
    /// Postcard `(row_id, tx_id)` payload consumed by the adapter.
    pub fn payload(&self) -> Result<Vec<u8>, JazzRnError> {
        with_panic_boundary("RnWrite.payload", || {
            self.ensure_open()?;
            self.actor.write_payload(self.id)
        })
    }

    /// Check whether the write has reached the requested durability tier.
    pub fn wait(&self, tier: String) -> Result<(), JazzRnError> {
        with_panic_boundary("RnWrite.wait", || {
            self.ensure_open()?;
            self.actor.wait_for_write(self.id, tier)
        })
    }

    /// Return the current write state as JSON.
    pub fn write_state(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("RnWrite.write_state", || {
            self.ensure_open()?;
            self.actor.write_state(self.id)
        })
    }

    /// Synchronously register a one-shot state-transition waiter.
    pub fn register_write_state_waiter(&self) -> Result<Arc<RnWriteStateWaiter>, JazzRnError> {
        with_panic_boundary("RnWrite.register_write_state_waiter", || {
            self.ensure_open()?;
            let (id, receiver) = self.actor.register_write_state_waiter(self.id)?;
            Ok(Arc::new(RnWriteStateWaiter {
                actor: Arc::clone(&self.actor),
                id,
                receiver: Mutex::new(Some(receiver)),
            }))
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
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.actor.release_write(self.id);
        }
    }
}

/// An eagerly registered write-state wake primitive.
#[derive(uniffi::Object)]
pub struct RnWriteStateWaiter {
    actor: Arc<ActorHandle>,
    id: u64,
    receiver: Mutex<Option<futures::channel::oneshot::Receiver<WaiterSignal>>>,
}

#[uniffi::export]
impl RnWriteStateWaiter {
    /// Await the already-registered state transition.
    pub async fn wait(&self) -> Result<(), JazzRnError> {
        with_async_panic_boundary("RnWriteStateWaiter.wait", || async {
            let receiver = self
                .receiver
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "write-state waiter lock poisoned".to_owned(),
                })?
                .take()
                .ok_or_else(|| JazzRnError::Runtime {
                    message: "write-state waiter was already awaited".to_owned(),
                })?;
            let signal = receiver.await.map_err(|_| closed_error())?;
            self.actor.release_waiter(self.id);
            match signal {
                WaiterSignal::Changed => Ok(()),
                WaiterSignal::Closed => Err(closed_error()),
                WaiterSignal::Poisoned(reason) => Err(poisoned_error(reason)),
            }
        })
        .await
    }
}

impl Drop for RnWriteStateWaiter {
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
    /// Drain all currently queued events as JSON strings.
    pub fn read_all(&self) -> Result<Vec<String>, JazzRnError> {
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
    pub fn drain(&self) -> Result<Vec<String>, JazzRnError> {
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
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.actor.release_subscription(self.id);
        }
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
        if self.closed.load(Ordering::SeqCst) {
            Err(JazzRnError::Runtime {
                message: "transport is closed".to_owned(),
            })
        } else {
            Ok(())
        }
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
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.actor.release_transport(self.id);
        }
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
