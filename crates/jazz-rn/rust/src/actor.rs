use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread::JoinHandle;

use futures::channel::oneshot;
use jazz::binding_support::{self as binding, WireQueues};
use jazz::db::{
    ConnectionSessionContext, Db, Error as DbError, ExclusiveTxOps, MergeableTxOps, PeerConnection,
    PreparedQuery, QueryAttachment, SubscriptionStream, WireTransportAdapter, WriteHandle,
    block_on,
};
use jazz::groove::storage::{MemoryStorage, SqliteStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::tools::{BatchId, OpenBatchId};
use jazz::tx::TxId;
use jazz::wire::{
    FEATURE_AUTHORIZATION_SCOPE_RECEIPTS, FEATURE_AUTHORIZATION_SCOPE_VIEWS, WIRE_PROTOCOL_VERSION,
    WireAuthorityEndpoint, current_wire_features,
};

use crate::scheduler::RnScheduler;
use crate::{
    JazzRnError, RnSubscriptionEvent, TickSchedulerCallback, closed_error, core_error,
    panic_to_jazz_error, poisoned_error,
};

/// Runs `body` against whichever storage backend the core was opened with.
///
/// `Db<MemoryStorage>` and `Db<SqliteStorage>` share one generic `impl`, so a
/// body written once type-checks in both arms. Without this every operation
/// would be spelled twice and the two copies could silently drift apart.
macro_rules! with_db {
    ($state:expr, $view:expr, |$db:ident| $body:expr) => {{
        match $state.view($view)? {
            CoreDb::Memory($db) => $body,
            CoreDb::Persistent($db) => $body,
        }
    }};
}

const ROOT_VIEW: u64 = 0;

type Job = Box<dyn ActorJob>;

trait ActorJob: Send {
    fn run(&mut self, state: &mut CoreState);
    fn fail(&mut self, error: JazzRnError);
}

struct CallJob<F, T>
where
    F: FnOnce(&mut CoreState) -> Result<T, JazzRnError> + Send + 'static,
    T: Send + 'static,
{
    call: Option<F>,
    reply: Option<mpsc::SyncSender<Result<T, JazzRnError>>>,
}

impl<F, T> ActorJob for CallJob<F, T>
where
    F: FnOnce(&mut CoreState) -> Result<T, JazzRnError> + Send + 'static,
    T: Send + 'static,
{
    fn run(&mut self, state: &mut CoreState) {
        let result = self.call.take().expect("actor job runs once")(state);
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }

    fn fail(&mut self, error: JazzRnError) {
        self.call.take();
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(error));
        }
    }
}

struct FireJob<F>
where
    F: FnOnce(&mut CoreState) -> Result<(), JazzRnError> + Send + 'static,
{
    call: Option<F>,
}

impl<F> ActorJob for FireJob<F>
where
    F: FnOnce(&mut CoreState) -> Result<(), JazzRnError> + Send + 'static,
{
    fn run(&mut self, state: &mut CoreState) {
        let _ = self.call.take().expect("actor job runs once")(state);
    }

    fn fail(&mut self, _error: JazzRnError) {
        self.call.take();
    }
}

#[derive(Clone, Debug)]
enum Lifecycle {
    Open,
    Closing,
    Closed,
    Poisoned(String),
}

struct Control {
    lifecycle: Lifecycle,
    sender: Option<mpsc::Sender<Job>>,
    join: Option<JoinHandle<()>>,
}

enum OpenRequest {
    Memory {
        schema: Vec<u8>,
        config: Vec<u8>,
    },
    Persistent {
        path: PathBuf,
        schema: Vec<u8>,
        config: Vec<u8>,
    },
}

/// Sendable client for the one-thread Jazz core actor.
pub(crate) struct ActorHandle {
    control: Arc<Mutex<Control>>,
    next_id: AtomicU64,
}

impl ActorHandle {
    pub(crate) fn open_memory(schema: Vec<u8>, config: Vec<u8>) -> Result<Arc<Self>, JazzRnError> {
        Self::spawn(OpenRequest::Memory { schema, config })
    }

    pub(crate) fn open_persistent(
        path: String,
        schema: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<Arc<Self>, JazzRnError> {
        Self::spawn(OpenRequest::Persistent {
            path: PathBuf::from(path),
            schema,
            config,
        })
    }

    fn spawn(request: OpenRequest) -> Result<Arc<Self>, JazzRnError> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let control = Arc::new(Mutex::new(Control {
            lifecycle: Lifecycle::Open,
            sender: Some(sender),
            join: None,
        }));
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let weak_control = Arc::downgrade(&control);
        let join = std::thread::Builder::new()
            .name("jazz-rn-core".to_owned())
            .spawn(move || actor_main(request, receiver, startup_sender, weak_control))
            .map_err(|error| JazzRnError::Internal {
                message: format!("failed to spawn Jazz core thread: {error}"),
            })?;
        control
            .lock()
            .map_err(|_| JazzRnError::Internal {
                message: "actor lifecycle lock poisoned during startup".to_owned(),
            })?
            .join = Some(join);

        match startup_receiver.recv() {
            Ok(Ok(())) => Ok(Arc::new(Self {
                control,
                next_id: AtomicU64::new(1),
            })),
            Ok(Err(error)) => {
                join_from_control(&control);
                Err(error)
            }
            Err(_) => {
                join_from_control(&control);
                Err(JazzRnError::Internal {
                    message: "Jazz core thread exited during startup".to_owned(),
                })
            }
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn call<T, F>(&self, context: &'static str, call: F) -> Result<T, JazzRnError>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreState) -> Result<T, JazzRnError> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        let job: Job = Box::new(CallJob {
            call: Some(call),
            reply: Some(reply_sender),
        });
        {
            let control = self.control.lock().map_err(|_| JazzRnError::Internal {
                message: format!("actor lifecycle lock poisoned in {context}"),
            })?;
            match &control.lifecycle {
                Lifecycle::Open => control
                    .sender
                    .as_ref()
                    .ok_or_else(closed_error)?
                    .send(job)
                    .map_err(|_| closed_error())?,
                Lifecycle::Closing | Lifecycle::Closed => return Err(closed_error()),
                Lifecycle::Poisoned(reason) => return Err(poisoned_error(reason.clone())),
            }
        }
        reply_receiver.recv().map_err(|_| JazzRnError::Internal {
            message: format!("Jazz core thread dropped the reply for {context}"),
        })?
    }

    fn cast<F>(&self, call: F)
    where
        F: FnOnce(&mut CoreState) -> Result<(), JazzRnError> + Send + 'static,
    {
        let job: Job = Box::new(FireJob { call: Some(call) });
        if let Ok(control) = self.control.lock()
            && matches!(control.lifecycle, Lifecycle::Open)
            && let Some(sender) = &control.sender
        {
            let _ = sender.send(job);
        }
    }

    pub(crate) fn close(&self) -> Result<(), JazzRnError> {
        close_control(&self.control)
    }

    pub(crate) fn set_tick_scheduler(
        &self,
        callback: Box<dyn TickSchedulerCallback>,
    ) -> Result<(), JazzRnError> {
        self.call("set_tick_scheduler", move |state| {
            state.set_tick_scheduler(callback)
        })
    }

    pub(crate) fn tick(&self) -> Result<(), JazzRnError> {
        self.call("tick", CoreState::tick)
    }

    pub(crate) fn register_schema(&self, view: u64, schema: Vec<u8>) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("register_schema", move |state| {
            state.register_schema(view, id, &schema)?;
            Ok(id)
        })
    }

    pub(crate) fn release_view(&self, view: u64) -> Result<(), JazzRnError> {
        self.call("release_view", move |state| state.release_view(view))
    }

    pub(crate) fn release_view_if_present(&self, view: u64) {
        self.cast(move |state| state.release_view_if_present(view));
    }

    pub(crate) fn prepare_query(&self, view: u64, query: Vec<u8>) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("prepare_query", move |state| {
            state.prepare_query(view, id, &query)?;
            Ok(id)
        })
    }

    pub(crate) fn release_query(&self, id: u64) {
        self.cast(move |state| {
            state.queries.remove(&id);
            Ok(())
        });
    }

    pub(crate) fn all(
        &self,
        view: u64,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all", move |state| {
            state.all(view, query, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn all_relation_snapshot(
        &self,
        view: u64,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all_relation_snapshot", move |state| {
            state.all_relation_snapshot(view, query, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn all_relation_query(
        &self,
        view: u64,
        query_json: String,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all_relation_query", move |state| {
            state.all_relation_query(view, &query_json, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn all_in_transaction(
        &self,
        query: u64,
        transaction: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all_in_transaction", move |state| {
            state.all_in_transaction(query, transaction, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn local_current_row(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("local_current_row", move |state| {
            state.local_current_row(view, &table, &row_id)
        })
    }

    pub(crate) fn set_identity_claims(
        &self,
        view: u64,
        author: Vec<u8>,
        claims_json: Option<String>,
    ) -> Result<(), JazzRnError> {
        self.call("set_identity_claims", move |state| {
            state.set_identity_claims(view, &author, claims_json.as_deref())
        })
    }

    pub(crate) fn attach_query(
        &self,
        view: u64,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("attach_query", move |state| {
            state.attach_query(view, id, query, author.as_deref(), opts_json.as_deref())?;
            Ok(id)
        })
    }

    pub(crate) fn attachment_is_covered(&self, id: u64) -> Result<bool, JazzRnError> {
        self.call("query_attachment_is_covered", move |state| {
            state.attachment_is_covered(id)
        })
    }

    pub(crate) fn detach_query(&self, id: u64) -> Result<(), JazzRnError> {
        self.call("detach_query", move |state| state.detach_query(id))
    }

    pub(crate) fn release_attachment(&self, id: u64) {
        self.cast(move |state| state.detach_query_if_present(id));
    }

    pub(crate) fn subscribe(
        &self,
        view: u64,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("subscribe", move |state| {
            state.subscribe(view, id, query, author.as_deref(), opts_json.as_deref())?;
            Ok(id)
        })
    }

    pub(crate) fn subscribe_relation_query(
        &self,
        view: u64,
        query_json: String,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("subscribe_relation_query", move |state| {
            state.subscribe_relation_query(
                view,
                id,
                &query_json,
                author.as_deref(),
                opts_json.as_deref(),
            )?;
            Ok(id)
        })
    }

    pub(crate) fn subscription_read_all(
        &self,
        id: u64,
    ) -> Result<Vec<RnSubscriptionEvent>, JazzRnError> {
        self.call("subscription_read_all", move |state| {
            state.subscription_read_all(id)
        })
    }

    pub(crate) fn close_subscription(&self, id: u64) -> Result<bool, JazzRnError> {
        self.call("close_subscription", move |state| {
            Ok(state.subscriptions.remove(&id).is_some())
        })
    }

    pub(crate) fn release_subscription(&self, id: u64) {
        self.cast(move |state| {
            state.subscriptions.remove(&id);
            Ok(())
        });
    }

    pub(crate) fn insert_with_id(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("insert_with_id", move |state| {
            state.insert_with_id(WriteArgs {
                view,
                id,
                table: &table,
                row_id: &row_id,
                cells: &cells,
                author: author.as_deref(),
                updated_at_ms,
            })
        })
    }

    pub(crate) fn update(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("update", move |state| {
            state.update(WriteArgs {
                view,
                id,
                table: &table,
                row_id: &row_id,
                cells: &patch,
                author: author.as_deref(),
                updated_at_ms,
            })
        })
    }

    pub(crate) fn upsert(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("upsert", move |state| {
            state.upsert(WriteArgs {
                view,
                id,
                table: &table,
                row_id: &row_id,
                cells: &cells,
                author: author.as_deref(),
                updated_at_ms,
            })
        })
    }

    pub(crate) fn delete(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("delete", move |state| {
            state.delete(view, id, &table, &row_id, author.as_deref(), updated_at_ms)
        })
    }

    pub(crate) fn restore(
        &self,
        view: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("restore", move |state| {
            state.restore(WriteArgs {
                view,
                id,
                table: &table,
                row_id: &row_id,
                cells: &cells,
                author: author.as_deref(),
                updated_at_ms,
            })
        })
    }

    pub(crate) fn begin_batch(
        &self,
        open_batch_id: String,
        kind: TransactionKind,
        author: Option<Vec<u8>>,
    ) -> Result<(), JazzRnError> {
        self.call("begin_transaction", move |state| {
            state.begin_batch(&open_batch_id, kind, author.as_deref())
        })
    }

    pub(crate) fn attach_transaction(
        &self,
        view: u64,
        open_batch_id: String,
        kind: TransactionKind,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("attach_transaction", move |state| {
            state.attach_transaction(id, view, &open_batch_id, kind, false)?;
            Ok(id)
        })
    }

    pub(crate) fn open_owning_transaction(
        &self,
        view: u64,
        open_batch_id: String,
        kind: TransactionKind,
        author: Option<Vec<u8>>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("open_owning_transaction", move |state| {
            state.begin_batch(&open_batch_id, kind, author.as_deref())?;
            if let Err(error) = state.attach_transaction(id, view, &open_batch_id, kind, true) {
                let _ = state.rollback_batch(&open_batch_id);
                return Err(error);
            }
            Ok(id)
        })
    }

    pub(crate) fn tx_insert(
        &self,
        transaction: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        self.call("tx_insert", move |state| {
            state.tx_insert(transaction, &table, &row_id, &cells, updated_at_ms)
        })
    }

    pub(crate) fn tx_update(
        &self,
        transaction: u64,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        self.call("tx_update", move |state| {
            state.tx_update(transaction, &table, &row_id, &patch, updated_at_ms)
        })
    }

    pub(crate) fn tx_upsert(
        &self,
        transaction: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        self.call("tx_upsert", move |state| {
            state.tx_upsert(transaction, &table, &row_id, &cells, updated_at_ms)
        })
    }

    pub(crate) fn tx_delete(
        &self,
        transaction: u64,
        table: String,
        row_id: Vec<u8>,
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        self.call("tx_delete", move |state| {
            state.tx_delete(transaction, &table, &row_id, updated_at_ms)
        })
    }

    pub(crate) fn tx_restore(
        &self,
        transaction: u64,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        self.call("tx_restore", move |state| {
            state.tx_restore(transaction, &table, &row_id, &cells, updated_at_ms)
        })
    }

    pub(crate) fn commit_batch(
        &self,
        open_batch_id: String,
        kind: Option<TransactionKind>,
    ) -> Result<u64, JazzRnError> {
        let write_id = self.next_id();
        self.call("commit_transaction", move |state| {
            state.commit_batch(&open_batch_id, kind, write_id)
        })
    }

    pub(crate) fn rollback_batch(&self, open_batch_id: String) -> Result<(), JazzRnError> {
        self.call("rollback_transaction", move |state| {
            state.rollback_batch(&open_batch_id)
        })
    }

    pub(crate) fn commit_transaction(&self, transaction: u64) -> Result<u64, JazzRnError> {
        let write_id = self.next_id();
        self.call("commit_transaction_handle", move |state| {
            state.commit_transaction(transaction, write_id)
        })
    }

    pub(crate) fn rollback_transaction(&self, transaction: u64) -> Result<(), JazzRnError> {
        self.call("rollback_transaction_handle", move |state| {
            state.rollback_transaction(transaction)
        })
    }

    pub(crate) fn release_transaction(&self, transaction: u64) {
        self.cast(move |state| state.release_transaction_if_present(transaction));
    }

    pub(crate) fn write_payload(&self, write: u64) -> Result<Vec<u8>, JazzRnError> {
        self.call("write_payload", move |state| {
            let entry = *state.write(write)?;
            binding::encode_write_result(entry.row_id, entry.tx_id).map_err(Into::into)
        })
    }

    pub(crate) fn write_batch_id(&self, write: u64) -> Result<String, JazzRnError> {
        self.call("write_batch_id", move |state| {
            Ok(state.write(write)?.batch_id.to_string())
        })
    }

    pub(crate) fn wait_for_write(&self, write: u64, tier: String) -> Result<(), JazzRnError> {
        self.call("wait_for_write", move |state| {
            state.wait_for_write(write, &tier)
        })
    }

    pub(crate) fn write_state(&self, write: u64) -> Result<String, JazzRnError> {
        self.call("write_state", move |state| state.write_state(write))
    }

    pub(crate) fn register_write_state_waiter(
        &self,
        write: u64,
    ) -> Result<(u64, oneshot::Receiver<WaiterSignal>), JazzRnError> {
        let waiter = self.next_id();
        self.call("register_write_state_waiter", move |state| {
            let receiver = state.register_write_state_waiter(waiter, write)?;
            Ok((waiter, receiver))
        })
    }

    pub(crate) fn close_write(&self, write: u64) -> Result<bool, JazzRnError> {
        self.call("close_write", move |state| {
            Ok(state.writes.remove(&write).is_some())
        })
    }

    pub(crate) fn release_write(&self, write: u64) {
        self.cast(move |state| {
            state.writes.remove(&write);
            Ok(())
        });
    }

    pub(crate) fn release_waiter(&self, waiter: u64) {
        self.cast(move |state| {
            state.waiters.remove(&waiter);
            Ok(())
        });
    }

    pub(crate) fn connect_upstream(&self, view: u64) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("connect_upstream", move |state| {
            state.connect_upstream(view, id, None)?;
            Ok(id)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn connect_upstream_with_session(
        &self,
        view: u64,
        protocol_version: u16,
        features: u32,
        remote_node: Vec<u8>,
        remote_epoch: u64,
        local_node: Vec<u8>,
        local_epoch: u64,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("connect_upstream_with_session", move |state| {
            let session = connection_session_context(
                features,
                &remote_node,
                remote_epoch,
                &local_node,
                local_epoch,
            )?;
            state.connect_upstream(view, id, Some((protocol_version, features as u64, session)))?;
            Ok(id)
        })
    }

    pub(crate) fn transport_send(
        &self,
        transport: u64,
        frames: Vec<Vec<u8>>,
    ) -> Result<(), JazzRnError> {
        self.call("transport_send", move |state| {
            state.transport_send(transport, frames)
        })
    }

    pub(crate) fn transport_recv(&self, transport: u64) -> Result<Vec<Vec<u8>>, JazzRnError> {
        self.call("transport_recv", move |state| {
            state.transport_recv(transport)
        })
    }

    pub(crate) fn transport_tick(&self, transport: u64) -> Result<u32, JazzRnError> {
        self.call("transport_tick", move |state| {
            state.transport_tick(transport)
        })
    }

    pub(crate) fn close_transport(&self, transport: u64) -> Result<bool, JazzRnError> {
        self.call("close_transport", move |state| {
            state.close_transport(transport)
        })
    }

    pub(crate) fn release_transport(&self, transport: u64) {
        self.cast(move |state| {
            let _ = state.close_transport(transport)?;
            Ok(())
        });
    }

    #[cfg(test)]
    fn force_job_panic(&self) -> Result<(), JazzRnError> {
        self.call("force_job_panic", |_state| -> Result<(), JazzRnError> {
            panic!("injected actor panic")
        })
    }

    #[cfg(test)]
    fn register_test_waiter(&self) -> Result<(u64, oneshot::Receiver<WaiterSignal>), JazzRnError> {
        let id = self.next_id();
        self.call("register_test_waiter", move |state| {
            let (sender, receiver) = oneshot::channel();
            state.waiters.insert(
                id,
                WaiterEntry {
                    completion: Rc::new(RefCell::new(Some(sender))),
                },
            );
            Ok((id, receiver))
        })
    }

    #[cfg(test)]
    fn fire_test_waiter(&self, id: u64) -> Result<(), JazzRnError> {
        self.call("fire_test_waiter", move |state| {
            if let Some(waiter) = state.waiters.get(&id)
                && let Some(sender) = waiter.completion.borrow_mut().take()
            {
                let _ = sender.send(WaiterSignal::Changed);
            }
            Ok(())
        })
    }
}

impl Drop for ActorHandle {
    fn drop(&mut self) {
        let _ = close_control(&self.control);
    }
}

fn close_control(control: &Arc<Mutex<Control>>) -> Result<(), JazzRnError> {
    let (reply_receiver, join, early_error) = {
        let mut control = control.lock().map_err(|_| JazzRnError::Internal {
            message: "actor lifecycle lock poisoned during close".to_owned(),
        })?;
        match &control.lifecycle {
            Lifecycle::Closed => return Ok(()),
            Lifecycle::Closing => return Err(closed_error()),
            Lifecycle::Poisoned(reason) => {
                let reason = reason.clone();
                let join = control.join.take();
                (None, join, Some(poisoned_error(reason)))
            }
            Lifecycle::Open => {
                control.lifecycle = Lifecycle::Closing;
                match control.sender.take() {
                    None => {
                        control.lifecycle = Lifecycle::Closed;
                        (None, control.join.take(), Some(closed_error()))
                    }
                    Some(sender) => {
                        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
                        let job: Job = Box::new(CallJob {
                            call: Some(|state: &mut CoreState| {
                                state.shutdown(WaiterSignal::Closed)
                            }),
                            reply: Some(reply_sender),
                        });
                        if sender.send(job).is_err() {
                            control.lifecycle = Lifecycle::Closed;
                            (None, control.join.take(), Some(closed_error()))
                        } else {
                            drop(sender);
                            (Some(reply_receiver), control.join.take(), None)
                        }
                    }
                }
            }
        }
    };

    let result = if let Some(error) = early_error {
        Err(error)
    } else {
        reply_receiver
            .expect("open close has a reply")
            .recv()
            .map_err(|_| JazzRnError::Internal {
                message: "Jazz core thread exited before close completed".to_owned(),
            })
            .and_then(|result| result)
    };
    let join_error = join
        .and_then(|join| join.join().err())
        .map(|payload| panic_to_jazz_error("joining Jazz core thread", payload));
    if let Ok(mut control) = control.lock() {
        control.lifecycle = Lifecycle::Closed;
        control.sender = None;
    }
    match join_error {
        Some(error) => Err(error),
        None => result,
    }
}

fn join_from_control(control: &Arc<Mutex<Control>>) {
    let join = control
        .lock()
        .ok()
        .and_then(|mut control| control.join.take());
    if let Some(join) = join {
        let _ = join.join();
    }
}

fn actor_main(
    request: OpenRequest,
    receiver: mpsc::Receiver<Job>,
    startup: mpsc::SyncSender<Result<(), JazzRnError>>,
    control: Weak<Mutex<Control>>,
) {
    let opened =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| CoreState::open(request)));
    let mut state = match opened {
        Ok(Ok(state)) => {
            let _ = startup.send(Ok(()));
            state
        }
        Ok(Err(error)) => {
            let _ = startup.send(Err(error));
            mark_closed(&control);
            return;
        }
        Err(payload) => {
            let error = panic_to_jazz_error("opening Jazz core", payload);
            let _ = startup.send(Err(error));
            mark_closed(&control);
            return;
        }
    };

    while let Ok(mut job) = receiver.recv() {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            job.run(&mut state);
        }));
        if let Err(payload) = outcome {
            let internal = panic_to_jazz_error("Jazz core actor job", payload);
            let reason = internal.to_string();
            job.fail(internal);
            state.cancel_waiters(WaiterSignal::Poisoned(reason.clone()));
            mark_poisoned(&control, reason.clone());
            while let Ok(mut pending) = receiver.try_recv() {
                pending.fail(poisoned_error(reason.clone()));
            }
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = state.shutdown(WaiterSignal::Poisoned(reason));
            }));
            return;
        }
    }

    if !state.closed {
        let _ = state.shutdown(WaiterSignal::Closed);
    }
    mark_closed(&control);
}

fn mark_closed(control: &Weak<Mutex<Control>>) {
    if let Some(control) = control.upgrade()
        && let Ok(mut control) = control.lock()
    {
        control.lifecycle = Lifecycle::Closed;
        control.sender = None;
    }
}

fn mark_poisoned(control: &Weak<Mutex<Control>>, reason: String) {
    if let Some(control) = control.upgrade()
        && let Ok(mut control) = control.lock()
    {
        control.lifecycle = Lifecycle::Poisoned(reason);
        control.sender = None;
    }
}

#[derive(Clone)]
enum CoreDb {
    Memory(Rc<Db<MemoryStorage>>),
    Persistent(Rc<Db<SqliteStorage>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionKind {
    Mergeable,
    Exclusive,
}

impl TransactionKind {
    pub(crate) fn from_str(kind: &str) -> Result<Self, JazzRnError> {
        match kind {
            "mergeable" => Ok(Self::Mergeable),
            "exclusive" => Ok(Self::Exclusive),
            other => Err(JazzRnError::InvalidPayload {
                message: format!("unknown batch kind {other}"),
            }),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Mergeable => "mergeable",
            Self::Exclusive => "exclusive",
        }
    }
}

#[derive(Clone, Copy)]
struct BatchEntry {
    kind: TransactionKind,
}

#[derive(Clone, Copy)]
struct TxAttachment {
    batch: OpenBatchId,
    view: u64,
    owns_lifetime: bool,
}

struct PreparedQueryEntry {
    view: u64,
    query: PreparedQuery,
}

struct QueryAttachmentEntry {
    view: u64,
    attachment: QueryAttachment,
}

struct SubscriptionEntry {
    view: u64,
    stream: SubscriptionStream,
}

/// The ids a completed write exposes. Both are `Copy`, and the postcard
/// payload the runtime reads is encoded on demand rather than retained per
/// write — most writes are never asked for it.
#[derive(Clone, Copy)]
struct WriteEntry {
    row_id: RowUuid,
    tx_id: TxId,
    batch_id: BatchId,
}

struct WriteArgs<'a> {
    view: u64,
    id: u64,
    table: &'a str,
    row_id: &'a [u8],
    cells: &'a [u8],
    author: Option<&'a [u8]>,
    updated_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WaiterSignal {
    Changed,
    Closed,
    Poisoned(String),
}

struct WaiterEntry {
    completion: Rc<RefCell<Option<oneshot::Sender<WaiterSignal>>>>,
}

enum TransportEntry {
    Memory {
        view: u64,
        db: Rc<Db<MemoryStorage>>,
        connection: Rc<RefCell<PeerConnection<MemoryStorage>>>,
        queues: WireQueues,
    },
    Persistent {
        view: u64,
        db: Rc<Db<SqliteStorage>>,
        connection: Rc<RefCell<PeerConnection<SqliteStorage>>>,
        queues: WireQueues,
    },
}

impl TransportEntry {
    fn view(&self) -> u64 {
        match self {
            Self::Memory { view, .. } | Self::Persistent { view, .. } => *view,
        }
    }

    fn queues(&self) -> &WireQueues {
        match self {
            Self::Memory { queues, .. } | Self::Persistent { queues, .. } => queues,
        }
    }

    fn send(&self, frames: Vec<Vec<u8>>) {
        self.queues().push_inbound(frames);
    }

    fn recv(&self) -> Vec<Vec<u8>> {
        self.queues().drain_outbound()
    }

    fn tick(&self) -> Result<u32, JazzRnError> {
        let stats = match self {
            Self::Memory { connection, .. } => connection.borrow_mut().tick(),
            Self::Persistent { connection, .. } => connection.borrow_mut().tick(),
        }
        .map_err(core_error)?;
        Ok(stats.subscription_events as u32)
    }

    fn close(self) -> bool {
        match self {
            Self::Memory { db, connection, .. } => db.detach_connection(&connection),
            Self::Persistent { db, connection, .. } => db.detach_connection(&connection),
        }
    }
}

struct CoreState {
    views: HashMap<u64, CoreDb>,
    scheduler: RnScheduler,
    queries: HashMap<u64, PreparedQueryEntry>,
    query_attachments: HashMap<u64, QueryAttachmentEntry>,
    open_batches: HashMap<OpenBatchId, BatchEntry>,
    tx_attachments: HashMap<u64, TxAttachment>,
    writes: HashMap<u64, WriteEntry>,
    waiters: HashMap<u64, WaiterEntry>,
    subscriptions: HashMap<u64, SubscriptionEntry>,
    transports: HashMap<u64, TransportEntry>,
    closed: bool,
}

impl CoreState {
    fn open(request: OpenRequest) -> Result<Self, JazzRnError> {
        let db = match request {
            OpenRequest::Memory { schema, config } => {
                let (schema, config) = binding::decode_open_args(&schema, &config)?;
                let column_families = schema.column_families();
                let refs = column_families
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let db = binding::open_db(schema, MemoryStorage::new(&refs), config)
                    .map_err(core_error)?;
                CoreDb::Memory(Rc::new(db))
            }
            OpenRequest::Persistent {
                path,
                schema,
                config,
            } => {
                if let Some(parent) = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    std::fs::create_dir_all(parent).map_err(|error| JazzRnError::Runtime {
                        message: format!(
                            "failed to create SQLite storage directory {}: {error}",
                            parent.display()
                        ),
                    })?;
                }
                let (schema, config) = binding::decode_open_args(&schema, &config)?;
                let column_families = schema.column_families();
                let refs = column_families
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let storage =
                    SqliteStorage::open(&path, &refs).map_err(|error| JazzRnError::Runtime {
                        message: format!(
                            "failed to open SQLite storage at {}: {error}",
                            path.display()
                        ),
                    })?;
                let db = binding::open_db(schema, storage, config).map_err(core_error)?;
                CoreDb::Persistent(Rc::new(db))
            }
        };
        Ok(Self {
            views: HashMap::from([(ROOT_VIEW, db)]),
            scheduler: RnScheduler::default(),
            queries: HashMap::new(),
            query_attachments: HashMap::new(),
            open_batches: HashMap::new(),
            tx_attachments: HashMap::new(),
            writes: HashMap::new(),
            waiters: HashMap::new(),
            subscriptions: HashMap::new(),
            transports: HashMap::new(),
            closed: false,
        })
    }

    fn view(&self, id: u64) -> Result<&CoreDb, JazzRnError> {
        self.views.get(&id).ok_or_else(|| invalid_view(id))
    }

    fn set_tick_scheduler(
        &mut self,
        callback: Box<dyn TickSchedulerCallback>,
    ) -> Result<(), JazzRnError> {
        self.scheduler.set_callback(Some(callback));
        let scheduler = Rc::new(self.scheduler.clone());
        with_db!(self, ROOT_VIEW, |db| db.set_tick_scheduler(Some(scheduler)));
        Ok(())
    }

    fn tick(&mut self) -> Result<(), JazzRnError> {
        with_db!(self, ROOT_VIEW, |db| db.tick()).map_err(core_error)
    }

    fn register_schema(
        &mut self,
        source_view: u64,
        id: u64,
        bytes: &[u8],
    ) -> Result<(), JazzRnError> {
        let schema = postcard::from_bytes(bytes).map_err(|error| JazzRnError::InvalidPayload {
            message: format!("decode schema: {error}"),
        })?;
        let view = match self.view(source_view)? {
            CoreDb::Memory(db) => CoreDb::Memory(Rc::new(
                db.register_schema_view(schema).map_err(core_error)?,
            )),
            CoreDb::Persistent(db) => CoreDb::Persistent(Rc::new(
                db.register_schema_view(schema).map_err(core_error)?,
            )),
        };
        self.views.insert(id, view);
        Ok(())
    }

    fn release_view(&mut self, view: u64) -> Result<(), JazzRnError> {
        if view == ROOT_VIEW {
            return Ok(());
        }
        if !self.views.contains_key(&view) {
            return Err(invalid_view(view));
        }
        self.release_view_resources(view);
        self.views.remove(&view);
        Ok(())
    }

    fn release_view_if_present(&mut self, view: u64) -> Result<(), JazzRnError> {
        if view != ROOT_VIEW && self.views.contains_key(&view) {
            self.release_view_resources(view);
            self.views.remove(&view);
        }
        Ok(())
    }

    fn release_view_resources(&mut self, view: u64) {
        self.queries.retain(|_, query| query.view != view);
        self.subscriptions.retain(|_, stream| stream.view != view);

        let query_attachment_ids = self
            .query_attachments
            .iter()
            .filter_map(|(id, entry)| (entry.view == view).then_some(*id))
            .collect::<Vec<_>>();
        for id in query_attachment_ids {
            let _ = self.detach_query_if_present(id);
        }

        let transaction_ids = self
            .tx_attachments
            .iter()
            .filter_map(|(id, attachment)| (attachment.view == view).then_some(*id))
            .collect::<Vec<_>>();
        for id in transaction_ids {
            let _ = self.release_transaction_if_present(id);
        }

        let transport_ids = self
            .transports
            .iter()
            .filter_map(|(id, transport)| (transport.view() == view).then_some(*id))
            .collect::<Vec<_>>();
        for id in transport_ids {
            if let Some(transport) = self.transports.remove(&id) {
                transport.close();
            }
        }
    }

    fn prepare_query(&mut self, view: u64, id: u64, bytes: &[u8]) -> Result<(), JazzRnError> {
        let query = binding::decode_query(bytes)?;
        let prepared = with_db!(self, view, |db| db.prepare_query(&query)).map_err(core_error)?;
        self.queries.insert(
            id,
            PreparedQueryEntry {
                view,
                query: prepared,
            },
        );
        Ok(())
    }

    fn query(&self, id: u64) -> Result<PreparedQuery, JazzRnError> {
        self.queries
            .get(&id)
            .map(|entry| entry.query.clone())
            .ok_or_else(|| invalid_handle("prepared query", id))
    }

    fn transaction(&self, id: u64) -> Result<TxAttachment, JazzRnError> {
        self.tx_attachments
            .get(&id)
            .copied()
            .ok_or_else(|| invalid_handle("transaction", id))
    }

    fn batch(&self, id: OpenBatchId) -> Result<BatchEntry, JazzRnError> {
        self.open_batches
            .get(&id)
            .copied()
            .ok_or_else(|| invalid_batch(id))
    }

    fn write(&self, id: u64) -> Result<&WriteEntry, JazzRnError> {
        self.writes
            .get(&id)
            .ok_or_else(|| invalid_handle("write", id))
    }

    fn all(
        &mut self,
        view: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let rows = with_db!(self, view, |db| match author {
            Some(author) => {
                block_on(db.all_for_identity(&query, opts, author))
            }
            None => block_on(db.all(&query, opts)),
        })
        .map_err(core_error)?;
        binding::encode_rows(&rows).map_err(Into::into)
    }

    fn all_relation_snapshot(
        &mut self,
        view: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let snapshot = with_db!(self, view, |db| match author {
            Some(author) => {
                block_on(db.all_relation_snapshot_for_identity(&query, opts, author))
            }
            None => block_on(db.all_relation_snapshot(&query, opts)),
        })
        .map_err(core_error)?;
        binding::encode_relation_snapshot(&snapshot).map_err(Into::into)
    }

    fn all_relation_query(
        &mut self,
        view: u64,
        query_json: &str,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = binding::relation_query_from_json(query_json)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let snapshot = with_db!(self, view, |db| match author {
            Some(author) => {
                block_on(db.all_relation_query_for_identity(&query, opts, author))
            }
            None => block_on(db.all_relation_query(&query, opts)),
        })
        .map_err(core_error)?;
        binding::encode_rows(&snapshot.rows).map_err(Into::into)
    }

    fn all_in_transaction(
        &mut self,
        query: u64,
        transaction: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let query = self.query(query)?;
        let transaction = self.transaction(transaction)?;
        let batch = self.batch(transaction.batch)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let rows = with_db!(self, transaction.view, |db| match (batch.kind, author) {
            (TransactionKind::Mergeable, Some(author)) => db
                .mergeable_tx_ref(transaction.batch)
                .all_prepared_for_identity_with_opts(&query, author, opts),
            (TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.batch)
                .all_prepared_with_opts(&query, opts),
            (TransactionKind::Exclusive, Some(author)) => db
                .exclusive_tx_ref(transaction.batch)
                .all_prepared_for_identity_with_opts(&query, author, opts),
            (TransactionKind::Exclusive, None) => db
                .exclusive_tx_ref(transaction.batch)
                .all_prepared_with_opts(&query, opts),
        })
        .map_err(core_error)?;
        binding::encode_rows(&rows).map_err(Into::into)
    }

    fn local_current_row(
        &mut self,
        view: u64,
        table: &str,
        row_id: &[u8],
    ) -> Result<Vec<u8>, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let row =
            with_db!(self, view, |db| db.local_current_row(table, row_id)).map_err(core_error)?;
        binding::encode_rows(&row.into_iter().collect::<Vec<_>>()).map_err(Into::into)
    }

    fn set_identity_claims(
        &mut self,
        view: u64,
        author: &[u8],
        claims_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let author = binding::author_id_from_bytes(author)?;
        let claims = claims_json
            .map(|claims| {
                serde_json::from_str(claims).map_err(|error| JazzRnError::InvalidPayload {
                    message: format!("decode identity claims json: {error}"),
                })
            })
            .transpose()?;
        let claims = binding::claims_from_json(author, claims)?;
        with_db!(self, view, |db| db.set_identity_claims(author, claims));
        Ok(())
    }

    fn attach_query(
        &mut self,
        view: u64,
        id: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let attachment = with_db!(self, view, |db| match author {
            Some(author) => {
                db.attach_query_with_opts_for_identity(&query, opts, author)
            }
            None => db.attach_query_with_opts(&query, opts),
        })
        .map_err(core_error)?;
        self.query_attachments
            .insert(id, QueryAttachmentEntry { view, attachment });
        Ok(())
    }

    fn attachment_is_covered(&self, id: u64) -> Result<bool, JazzRnError> {
        let attachment = self
            .query_attachments
            .get(&id)
            .ok_or_else(|| invalid_handle("query attachment", id))?;
        Ok(with_db!(self, attachment.view, |db| db
            .query_attachment_is_covered(&attachment.attachment)))
    }

    fn detach_query(&mut self, id: u64) -> Result<(), JazzRnError> {
        let attachment = self
            .query_attachments
            .remove(&id)
            .ok_or_else(|| invalid_handle("query attachment", id))?;
        with_db!(self, attachment.view, |db| db
            .detach_query(attachment.attachment));
        Ok(())
    }

    fn detach_query_if_present(&mut self, id: u64) -> Result<(), JazzRnError> {
        if let Some(attachment) = self.query_attachments.remove(&id) {
            with_db!(self, attachment.view, |db| db
                .detach_query(attachment.attachment))
        }
        Ok(())
    }

    fn subscribe(
        &mut self,
        view: u64,
        id: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let stream = with_db!(self, view, |db| match author {
            Some(author) => {
                block_on(db.subscribe_for_identity(&query, opts, author))
            }
            None => block_on(db.subscribe(&query, opts)),
        })
        .map_err(core_error)?;
        self.subscriptions
            .insert(id, SubscriptionEntry { view, stream });
        Ok(())
    }

    fn subscribe_relation_query(
        &mut self,
        view: u64,
        id: u64,
        query_json: &str,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = binding::relation_query_from_json(query_json)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let stream = with_db!(self, view, |db| match author {
            Some(author) => {
                block_on(db.subscribe_relation_query_for_identity(&query, opts, author))
            }
            None => block_on(db.subscribe_relation_query(&query, opts)),
        })
        .map_err(core_error)?;
        self.subscriptions
            .insert(id, SubscriptionEntry { view, stream });
        Ok(())
    }

    fn subscription_read_all(&mut self, id: u64) -> Result<Vec<RnSubscriptionEvent>, JazzRnError> {
        let stream = self
            .subscriptions
            .get_mut(&id)
            .ok_or_else(|| invalid_handle("subscription", id))?;
        let mut events = Vec::new();
        while let Some(event) = stream.stream.try_next_event() {
            let event = match binding::encode_subscription_event(&event)? {
                binding::EncodedSubscriptionEvent::Delta {
                    reset,
                    delta,
                    terminal_operations,
                    settled,
                    tier,
                } => RnSubscriptionEvent {
                    event_type: "delta".to_owned(),
                    reset: Some(reset),
                    delta: Some(delta),
                    terminal_operations_json: Some(
                        serde_json::to_string(&terminal_operations).map_err(|error| {
                            JazzRnError::Internal {
                                message: format!("encode terminal operations: {error}"),
                            }
                        })?,
                    ),
                    settled: Some(settled),
                    tier: Some(tier),
                    reason_json: None,
                },
                binding::EncodedSubscriptionEvent::Rejected { reason } => RnSubscriptionEvent {
                    event_type: "rejected".to_owned(),
                    reset: None,
                    delta: None,
                    terminal_operations_json: None,
                    settled: None,
                    tier: None,
                    reason_json: Some(serde_json::to_string(&reason).map_err(|error| {
                        JazzRnError::Internal {
                            message: format!("encode subscription rejection: {error}"),
                        }
                    })?),
                },
                binding::EncodedSubscriptionEvent::Closed => RnSubscriptionEvent {
                    event_type: "closed".to_owned(),
                    reset: None,
                    delta: None,
                    terminal_operations_json: None,
                    settled: None,
                    tier: None,
                    reason_json: None,
                },
            };
            events.push(event);
        }
        Ok(events)
    }

    fn insert_with_id(&mut self, args: WriteArgs<'_>) -> Result<u64, JazzRnError> {
        let WriteArgs {
            view,
            id,
            table,
            row_id,
            cells,
            author,
            updated_at_ms,
        } = args;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = with_db!(self, view, |db| match (author, updated_at_ms) {
            (Some(author), Some(now)) => write_parts(
                db.insert_with_id_for_identity_at_ms(author, table, row_id, cells, now)
            )?,
            (Some(author), None) =>
                write_parts(db.insert_with_id_for_identity(author, table, row_id, cells))?,
            (None, Some(now)) => write_parts(db.insert_with_id_at_ms(table, row_id, cells, now))?,
            (None, None) => write_parts(db.insert_with_id(table, row_id, cells))?,
        });
        self.register_write(id, row_id, tx_id)
    }

    fn update(&mut self, args: WriteArgs<'_>) -> Result<u64, JazzRnError> {
        let WriteArgs {
            view,
            id,
            table,
            row_id,
            cells: patch,
            author,
            updated_at_ms,
        } = args;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let patch = binding::decode_cells(patch)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = with_db!(self, view, |db| match (author, updated_at_ms) {
            (Some(author), Some(now)) =>
                write_parts(db.update_for_identity_at_ms(author, table, row_id, patch, now))?,
            (Some(author), None) =>
                write_parts(db.update_for_identity(author, table, row_id, patch))?,
            (None, Some(now)) => write_parts(db.update_at_ms(table, row_id, patch, now))?,
            (None, None) => {
                write_parts(db.update(table, row_id, patch))?
            }
        });
        self.register_write(id, row_id, tx_id)
    }

    fn upsert(&mut self, args: WriteArgs<'_>) -> Result<u64, JazzRnError> {
        let WriteArgs {
            view,
            id,
            table,
            row_id,
            cells,
            author,
            updated_at_ms,
        } = args;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = with_db!(self, view, |db| match (author, updated_at_ms) {
            (Some(author), Some(now)) =>
                write_parts(db.upsert_for_identity_at_ms(author, table, row_id, cells, now))?,
            (Some(author), None) =>
                write_parts(db.upsert_for_identity(author, table, row_id, cells))?,
            (None, Some(now)) => write_parts(db.upsert_at_ms(table, row_id, cells, now))?,
            (None, None) => {
                write_parts(db.upsert(table, row_id, cells))?
            }
        });
        self.register_write(id, row_id, tx_id)
    }

    fn delete(
        &mut self,
        view: u64,
        id: u64,
        table: &str,
        row_id: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<u64, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = with_db!(self, view, |db| match (author, updated_at_ms) {
            (Some(author), Some(now)) =>
                write_parts(db.delete_for_identity_at_ms(author, table, row_id, now))?,
            (Some(author), None) => write_parts(db.delete_for_identity(author, table, row_id))?,
            (None, Some(now)) => {
                write_parts(db.delete_at_ms(table, row_id, now))?
            }
            (None, None) => {
                write_parts(db.delete(table, row_id))?
            }
        });
        self.register_write(id, row_id, tx_id)
    }

    fn restore(&mut self, args: WriteArgs<'_>) -> Result<u64, JazzRnError> {
        let WriteArgs {
            view,
            id,
            table,
            row_id,
            cells,
            author,
            updated_at_ms,
        } = args;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = with_db!(self, view, |db| match (author, updated_at_ms) {
            (Some(author), Some(now)) =>
                write_parts(db.restore_for_identity_at_ms(author, table, row_id, cells, now))?,
            (Some(author), None) =>
                write_parts(db.restore_for_identity(author, table, row_id, cells))?,
            (None, Some(now)) => write_parts(db.restore_at_ms(table, row_id, cells, now))?,
            (None, None) => {
                write_parts(db.restore(table, row_id, cells))?
            }
        });
        self.register_write(id, row_id, tx_id)
    }

    fn register_write(
        &mut self,
        id: u64,
        row_id: RowUuid,
        tx_id: TxId,
    ) -> Result<u64, JazzRnError> {
        self.writes.insert(
            id,
            WriteEntry {
                row_id,
                tx_id,
                batch_id: BatchId::from_committed_tx(tx_id),
            },
        );
        Ok(id)
    }

    fn begin_batch(
        &mut self,
        raw_batch: &str,
        kind: TransactionKind,
        author: Option<&[u8]>,
    ) -> Result<(), JazzRnError> {
        let batch = parse_open_batch_id(raw_batch)?;
        if self.open_batches.contains_key(&batch) {
            return Err(JazzRnError::Runtime {
                message: format!("batch {batch} has already been opened"),
            });
        }
        if kind == TransactionKind::Exclusive && author.is_some() {
            return Err(JazzRnError::InvalidPayload {
                message: "exclusive batches do not accept an identity override".to_owned(),
            });
        }
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        with_db!(self, ROOT_VIEW, |db| match (kind, author) {
            (TransactionKind::Mergeable, Some(author)) => {
                db.begin_mergeable_for_identity(batch, author)
            }
            (TransactionKind::Mergeable, None) => db.begin_mergeable(batch),
            (TransactionKind::Exclusive, None) => db.begin_exclusive(batch),
            (TransactionKind::Exclusive, Some(_)) => unreachable!("validated above"),
        })
        .map_err(core_error)?;
        self.open_batches.insert(batch, BatchEntry { kind });
        Ok(())
    }

    fn attach_transaction(
        &mut self,
        id: u64,
        view: u64,
        raw_batch: &str,
        kind: TransactionKind,
        owns_lifetime: bool,
    ) -> Result<(), JazzRnError> {
        self.view(view)?;
        let batch = parse_open_batch_id(raw_batch)?;
        let actual = self.batch(batch)?;
        if actual.kind != kind {
            return Err(JazzRnError::InvalidPayload {
                message: format!(
                    "batch {batch} is {}, not {}",
                    actual.kind.as_str(),
                    kind.as_str()
                ),
            });
        }
        self.tx_attachments.insert(
            id,
            TxAttachment {
                batch,
                view,
                owns_lifetime,
            },
        );
        Ok(())
    }

    fn tx_insert(
        &mut self,
        transaction: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction)?;
        let kind = self.batch(transaction.batch)?.kind;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        with_db!(self, transaction.view, |db| match (kind, updated_at_ms) {
            (TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.batch)
                .insert_with_id_at_ms(table, row_id, cells, now),
            (TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.batch)
                .insert_with_id(table, row_id, cells),
            (TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.batch)
                .insert_with_id(table, row_id, cells),
        })
        .map_err(core_error)
    }

    fn tx_update(
        &mut self,
        transaction: u64,
        table: &str,
        row_id: &[u8],
        patch: &[u8],
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction)?;
        let kind = self.batch(transaction.batch)?.kind;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let patch = binding::decode_cells(patch)?;
        with_db!(self, transaction.view, |db| match (kind, updated_at_ms) {
            (TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.batch)
                .update_at_ms(table, row_id, patch, now),
            (TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.batch)
                .update(table, row_id, patch),
            (TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.batch)
                .update(table, row_id, patch),
        })
        .map_err(core_error)
    }

    fn tx_upsert(
        &mut self,
        transaction: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction)?;
        let kind = self.batch(transaction.batch)?.kind;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        with_db!(self, transaction.view, |db| match kind {
            TransactionKind::Mergeable => {
                let tx = db.mergeable_tx_ref(transaction.batch);
                tx.read(table, row_id).and_then(|existing| {
                    if existing.is_some() {
                        match updated_at_ms {
                            Some(now) => tx.update_at_ms(table, row_id, cells, now),
                            None => tx.update(table, row_id, cells),
                        }
                    } else {
                        match updated_at_ms {
                            Some(now) => tx.insert_with_id_at_ms(table, row_id, cells, now),
                            None => tx.insert_with_id(table, row_id, cells),
                        }
                    }
                })
            }
            TransactionKind::Exclusive => {
                let tx = db.exclusive_tx_ref(transaction.batch);
                tx.read(table, row_id).and_then(|existing| {
                    if existing.is_some() {
                        tx.update(table, row_id, cells)
                    } else {
                        tx.insert_with_id(table, row_id, cells)
                    }
                })
            }
        })
        .map_err(core_error)
    }

    fn tx_delete(
        &mut self,
        transaction: u64,
        table: &str,
        row_id: &[u8],
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction)?;
        let kind = self.batch(transaction.batch)?.kind;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        with_db!(self, transaction.view, |db| match (kind, updated_at_ms) {
            (TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.batch)
                .delete_at_ms(table, row_id, now),
            (TransactionKind::Mergeable, None) =>
                db.mergeable_tx_ref(transaction.batch).delete(table, row_id),
            (TransactionKind::Exclusive, _) =>
                db.exclusive_tx_ref(transaction.batch).delete(table, row_id),
        })
        .map_err(core_error)
    }

    fn tx_restore(
        &mut self,
        transaction: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        updated_at_ms: Option<u64>,
    ) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction)?;
        let kind = self.batch(transaction.batch)?.kind;
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        with_db!(self, transaction.view, |db| match (kind, updated_at_ms) {
            (TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.batch)
                .restore_at_ms(table, row_id, cells, now),
            (TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.batch)
                .restore(table, row_id, cells),
            (TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.batch)
                .restore(table, row_id, cells),
        })
        .map_err(core_error)
    }

    fn commit_batch(
        &mut self,
        raw_batch: &str,
        requested_kind: Option<TransactionKind>,
        write_id: u64,
    ) -> Result<u64, JazzRnError> {
        let batch = parse_open_batch_id(raw_batch)?;
        let actual_kind = self.batch(batch)?.kind;
        let requested_kind = requested_kind.unwrap_or(TransactionKind::Mergeable);
        if requested_kind != actual_kind {
            return Err(JazzRnError::InvalidPayload {
                message: format!(
                    "batch {batch} is {}, not {}",
                    actual_kind.as_str(),
                    requested_kind.as_str()
                ),
            });
        }
        self.commit_open_batch(batch, actual_kind, write_id)
    }

    fn commit_open_batch(
        &mut self,
        batch: OpenBatchId,
        kind: TransactionKind,
        write_id: u64,
    ) -> Result<u64, JazzRnError> {
        let tx_id = with_db!(self, ROOT_VIEW, |db| match kind {
            TransactionKind::Mergeable => db.commit_mergeable_handle(batch),
            TransactionKind::Exclusive => db.commit_exclusive_handle(batch),
        })
        .map_err(core_error)?;
        self.finish_batch(batch);
        self.register_write(write_id, RowUuid::from_bytes([0; 16]), tx_id)
    }

    fn rollback_batch(&mut self, raw_batch: &str) -> Result<(), JazzRnError> {
        let batch = parse_open_batch_id(raw_batch)?;
        self.batch(batch)?;
        self.abandon_open_batch(batch)
    }

    fn abandon_open_batch(&mut self, batch: OpenBatchId) -> Result<(), JazzRnError> {
        with_db!(self, ROOT_VIEW, |db| db.abandon_transaction_handle(batch)).map_err(core_error)?;
        self.finish_batch(batch);
        Ok(())
    }

    fn finish_batch(&mut self, batch: OpenBatchId) {
        self.open_batches.remove(&batch);
        self.tx_attachments
            .retain(|_, attachment| attachment.batch != batch);
    }

    fn commit_transaction(
        &mut self,
        transaction_id: u64,
        write_id: u64,
    ) -> Result<u64, JazzRnError> {
        let transaction = self.transaction(transaction_id)?;
        if !transaction.owns_lifetime {
            return Err(JazzRnError::Runtime {
                message: "attached transaction views cannot commit the owner-wide batch".to_owned(),
            });
        }
        let kind = self.batch(transaction.batch)?.kind;
        self.commit_open_batch(transaction.batch, kind, write_id)
    }

    fn rollback_transaction(&mut self, transaction_id: u64) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction_id)?;
        if !transaction.owns_lifetime {
            return Err(JazzRnError::Runtime {
                message: "attached transaction views cannot roll back the owner-wide batch"
                    .to_owned(),
            });
        }
        self.abandon_open_batch(transaction.batch)
    }

    fn release_transaction_if_present(&mut self, transaction_id: u64) -> Result<(), JazzRnError> {
        let Some(transaction) = self.tx_attachments.remove(&transaction_id) else {
            return Ok(());
        };
        if transaction.owns_lifetime && self.open_batches.contains_key(&transaction.batch) {
            self.abandon_open_batch(transaction.batch)?;
        }
        Ok(())
    }

    fn wait_for_write(&mut self, write: u64, tier: &str) -> Result<(), JazzRnError> {
        let tx_id = self.write(write)?.tx_id;
        let tier = binding::durability_tier_from_str(tier)?;
        with_db!(self, ROOT_VIEW, |db| binding::wait_for_tx(db, tx_id, tier)).map_err(Into::into)
    }

    fn write_state(&mut self, write: u64) -> Result<String, JazzRnError> {
        let tx_id = self.write(write)?.tx_id;
        let state = with_db!(self, ROOT_VIEW, |db| db.write_state(tx_id)).map_err(core_error)?;
        serde_json::to_string(&binding::write_state_to_json(&state)).map_err(|error| {
            JazzRnError::Internal {
                message: format!("encode write state json: {error}"),
            }
        })
    }

    fn register_write_state_waiter(
        &mut self,
        waiter: u64,
        write: u64,
    ) -> Result<oneshot::Receiver<WaiterSignal>, JazzRnError> {
        let tx_id = self.write(write)?.tx_id;
        let (sender, receiver) = oneshot::channel();
        let completion = Rc::new(RefCell::new(Some(sender)));
        let callback_completion = Rc::clone(&completion);
        with_db!(self, ROOT_VIEW, |db| db.on_next_write_state_change(
            tx_id,
            move || {
                if let Some(sender) = callback_completion.borrow_mut().take() {
                    let _ = sender.send(WaiterSignal::Changed);
                }
            }
        ));
        self.waiters.insert(waiter, WaiterEntry { completion });
        Ok(receiver)
    }

    fn connect_upstream(
        &mut self,
        view: u64,
        id: u64,
        session: Option<(u16, u64, ConnectionSessionContext)>,
    ) -> Result<(), JazzRnError> {
        let queues = WireQueues::default();
        let transport = match session {
            Some((protocol_version, features, context)) => {
                Box::new(WireTransportAdapter::new_with_session_context(
                    queues.transport(),
                    protocol_version,
                    features,
                    None,
                    Some(context),
                ))
            }
            None => Box::new(WireTransportAdapter::new(
                queues.transport(),
                WIRE_PROTOCOL_VERSION,
                current_wire_features()
                    & !(FEATURE_AUTHORIZATION_SCOPE_RECEIPTS | FEATURE_AUTHORIZATION_SCOPE_VIEWS),
                None,
            )),
        };
        let entry = match self.view(view)? {
            CoreDb::Memory(db) => TransportEntry::Memory {
                view,
                db: Rc::clone(db),
                connection: db.connect_upstream(transport),
                queues,
            },
            CoreDb::Persistent(db) => TransportEntry::Persistent {
                view,
                db: Rc::clone(db),
                connection: db.connect_upstream(transport),
                queues,
            },
        };
        self.transports.insert(id, entry);
        // Opening persistent storage can restore transaction IDs into the
        // outbox before a foreign tick callback exists. Bootstrap the newly
        // attached peer synchronously so replay never depends on that wakeup.
        self.tick()
    }

    fn transport_send(&mut self, id: u64, frames: Vec<Vec<u8>>) -> Result<(), JazzRnError> {
        let transport = self
            .transports
            .get(&id)
            .ok_or_else(|| invalid_handle("transport", id))?;
        transport.send(frames);
        Ok(())
    }

    fn transport_recv(&mut self, id: u64) -> Result<Vec<Vec<u8>>, JazzRnError> {
        let transport = self
            .transports
            .get(&id)
            .ok_or_else(|| invalid_handle("transport", id))?;
        Ok(transport.recv())
    }

    fn transport_tick(&mut self, id: u64) -> Result<u32, JazzRnError> {
        self.transports
            .get(&id)
            .ok_or_else(|| invalid_handle("transport", id))?
            .tick()
    }

    fn close_transport(&mut self, id: u64) -> Result<bool, JazzRnError> {
        Ok(self
            .transports
            .remove(&id)
            .is_some_and(TransportEntry::close))
    }

    fn cancel_waiters(&mut self, signal: WaiterSignal) {
        for waiter in self.waiters.values() {
            if let Some(sender) = waiter.completion.borrow_mut().take() {
                let _ = sender.send(signal.clone());
            }
        }
        self.waiters.clear();
    }

    fn shutdown(&mut self, waiter_signal: WaiterSignal) -> Result<(), JazzRnError> {
        if self.closed {
            return Ok(());
        }
        self.cancel_waiters(waiter_signal);
        with_db!(self, ROOT_VIEW, |db| db.set_tick_scheduler(None));
        self.scheduler.shutdown();
        for (_, transport) in self.transports.drain() {
            transport.close();
        }
        self.subscriptions.clear();
        let query_attachments = std::mem::take(&mut self.query_attachments);
        for (_, attachment) in query_attachments {
            with_db!(self, attachment.view, |db| db
                .detach_query(attachment.attachment));
        }
        let open_batches = self.open_batches.keys().copied().collect::<Vec<_>>();
        for batch in open_batches {
            let _ = with_db!(self, ROOT_VIEW, |db| db.abandon_transaction_handle(batch));
        }
        self.open_batches.clear();
        self.tx_attachments.clear();
        self.writes.clear();
        self.queries.clear();
        let result = with_db!(self, ROOT_VIEW, |db| db.close()).map_err(core_error);
        self.views.clear();
        self.closed = true;
        result
    }
}

/// Erases a storage-typed write handle down to the ids the bindings return.
///
/// Write calls are dispatched per storage backend, so their handle types
/// differ per `with_db!` arm; this collapses both to one type at the arm
/// boundary and maps the core error while it is there.
fn write_parts<S>(write: Result<WriteHandle<S>, DbError>) -> Result<(RowUuid, TxId), JazzRnError>
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let write = write.map_err(core_error)?;
    Ok((write.row_uuid(), write.mergeable_tx_id()))
}

fn invalid_handle(kind: &str, id: u64) -> JazzRnError {
    JazzRnError::Runtime {
        message: format!("{kind} handle {id} is closed or belongs to another database"),
    }
}

fn invalid_view(id: u64) -> JazzRnError {
    JazzRnError::Runtime {
        message: format!("schema view {id} is closed or belongs to another database"),
    }
}

fn invalid_batch(id: OpenBatchId) -> JazzRnError {
    JazzRnError::Runtime {
        message: format!("batch {id} is closed or belongs to another database"),
    }
}

fn parse_open_batch_id(raw: &str) -> Result<OpenBatchId, JazzRnError> {
    raw.parse()
        .map_err(|message| JazzRnError::InvalidPayload { message })
}

fn endpoint_node(bytes: &[u8], label: &str) -> Result<NodeUuid, JazzRnError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| JazzRnError::InvalidPayload {
        message: format!("{label} must be 16 bytes"),
    })?;
    Ok(NodeUuid::from_bytes(bytes))
}

fn connection_session_context(
    features: u32,
    remote_node: &[u8],
    remote_epoch: u64,
    local_node: &[u8],
    local_epoch: u64,
) -> Result<ConnectionSessionContext, JazzRnError> {
    let remote_node = endpoint_node(remote_node, "server hello authority node")?;
    let local_node = endpoint_node(local_node, "local peer identity")?;
    Ok(ConnectionSessionContext {
        local: WireAuthorityEndpoint {
            node: local_node,
            epoch: local_epoch,
        },
        remote: WireAuthorityEndpoint {
            node: remote_node,
            epoch: remote_epoch,
        },
        link_identity: AuthorId::from_bytes(*local_node.0.as_bytes()),
        negotiated_features: features as u64,
    })
}

#[cfg(test)]
mod tests {
    use jazz::binding_support::{OpenDbConfig, OpenDbIdentity};
    use jazz::ids::{AuthorId, NodeUuid};
    use jazz::schema::JazzSchema;

    use super::*;

    fn open_actor() -> Arc<ActorHandle> {
        let schema = JazzSchema::new([]);
        let config = OpenDbConfig {
            identity: OpenDbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorId::from_bytes([2; 16]),
            },
            row_id_seed: Some(1),
            history_complete: false,
            initial_sync_flush_every: None,
        };
        ActorHandle::open_memory(
            postcard::to_allocvec(&schema).unwrap(),
            postcard::to_allocvec(&config).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn close_is_a_submission_barrier() {
        // The binding's cross-thread close barrier is not observable through
        // Jazz's public database API, so this tests the actor mechanism.
        let actor = open_actor();
        actor.close().unwrap();
        assert!(matches!(actor.tick(), Err(JazzRnError::Closed { .. })));
    }

    #[test]
    fn failed_shutdown_send_finishes_the_close_transition() {
        // A dead actor receiver is a binding-thread failure that cannot be
        // induced through Jazz's public database API. The close controller
        // must nevertheless leave its lifecycle terminal and idempotent.
        let (sender, receiver) = mpsc::channel::<Job>();
        drop(receiver);
        let control = Arc::new(Mutex::new(Control {
            lifecycle: Lifecycle::Open,
            sender: Some(sender),
            join: None,
        }));

        assert!(matches!(
            close_control(&control),
            Err(JazzRnError::Closed { .. })
        ));
        assert!(matches!(
            control.lock().unwrap().lifecycle,
            Lifecycle::Closed
        ));
        assert!(close_control(&control).is_ok());
    }

    #[test]
    fn failed_shutdown_reply_finishes_the_close_transition() {
        // The actor may disappear after accepting the shutdown job but before
        // completing its reply. Closing must still join it and become
        // idempotent instead of leaving the handle stuck in `Closing`.
        let (sender, receiver) = mpsc::channel::<Job>();
        let join = std::thread::spawn(move || drop(receiver.recv().unwrap()));
        let control = Arc::new(Mutex::new(Control {
            lifecycle: Lifecycle::Open,
            sender: Some(sender),
            join: Some(join),
        }));

        assert!(matches!(
            close_control(&control),
            Err(JazzRnError::Internal { .. })
        ));
        assert!(matches!(
            control.lock().unwrap().lifecycle,
            Lifecycle::Closed
        ));
        assert!(close_control(&control).is_ok());
    }

    #[test]
    fn panicking_job_poisoned_actor_and_completes_reply() {
        // Actor panic containment is a binding-only safety property.
        let actor = open_actor();
        let error = actor.force_job_panic().unwrap_err();
        assert!(matches!(error, JazzRnError::Internal { .. }));
        let error = actor.tick().unwrap_err();
        assert!(matches!(error, JazzRnError::Poisoned { .. }));
    }

    #[test]
    fn waiter_registration_has_no_lazy_poll_window() {
        // The eager registration handshake is a binding-only mechanism. These
        // three cases cover transitions before registration, after registration
        // but before await, and while await is pending.
        let actor = open_actor();

        actor.fire_test_waiter(u64::MAX).unwrap();
        let (before_id, mut before) = actor.register_test_waiter().unwrap();
        assert_eq!(before.try_recv().unwrap(), None);
        actor.fire_test_waiter(before_id).unwrap();
        assert!(matches!(
            futures::executor::block_on(before).unwrap(),
            WaiterSignal::Changed
        ));

        let (between_id, between) = actor.register_test_waiter().unwrap();
        actor.fire_test_waiter(between_id).unwrap();
        assert!(matches!(
            futures::executor::block_on(between).unwrap(),
            WaiterSignal::Changed
        ));

        let (after_id, after) = actor.register_test_waiter().unwrap();
        let awaiting = std::thread::spawn(move || futures::executor::block_on(after));
        actor.fire_test_waiter(after_id).unwrap();
        assert!(matches!(
            awaiting.join().unwrap().unwrap(),
            WaiterSignal::Changed
        ));
        actor.close().unwrap();
    }
}
