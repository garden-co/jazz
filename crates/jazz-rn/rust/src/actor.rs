use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread::JoinHandle;

use futures::channel::oneshot;
use jazz::binding_support as binding;
use jazz::db::{
    Db, ExclusiveTxOps, MergeableTxOps, PeerConnection, PreparedQuery, QueryAttachment,
    SubscriptionStream, WireTransportAdapter, WriteHandle, block_on,
};
use jazz::groove::storage::{MemoryStorage, SqliteStorage};
use jazz::ids::RowUuid;
use jazz::node::OpenTxId;
use jazz::tx::TxId;
use jazz::wire::{TransportError, WireTransport};

use crate::scheduler::RnScheduler;
use crate::{
    JazzRnError, RnSubscriptionEvent, TickSchedulerCallback, closed_error, core_error,
    panic_to_jazz_error, poisoned_error,
};

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
            state.set_tick_scheduler(callback);
            Ok(())
        })
    }

    pub(crate) fn tick(&self) -> Result<(), JazzRnError> {
        self.call("tick", CoreState::tick)
    }

    pub(crate) fn prepare_query(&self, query: Vec<u8>) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("prepare_query", move |state| {
            state.prepare_query(id, &query)?;
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
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all", move |state| {
            state.all(query, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn all_relation_snapshot(
        &self,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all_relation_snapshot", move |state| {
            state.all_relation_snapshot(query, author.as_deref(), opts_json.as_deref())
        })
    }

    pub(crate) fn all_relation_query(
        &self,
        query_json: String,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("all_relation_query", move |state| {
            state.all_relation_query(&query_json, author.as_deref(), opts_json.as_deref())
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
        table: String,
        row_id: Vec<u8>,
    ) -> Result<Vec<u8>, JazzRnError> {
        self.call("local_current_row", move |state| {
            state.local_current_row(&table, &row_id)
        })
    }

    pub(crate) fn set_identity_claims(
        &self,
        author: Vec<u8>,
        claims_json: Option<String>,
    ) -> Result<(), JazzRnError> {
        self.call("set_identity_claims", move |state| {
            state.set_identity_claims(&author, claims_json.as_deref())
        })
    }

    pub(crate) fn can_insert(
        &self,
        table: String,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
    ) -> Result<bool, JazzRnError> {
        self.call("can_insert", move |state| {
            state.can_insert(&table, &cells, author.as_deref())
        })
    }

    pub(crate) fn can_read(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        self.call("can_read", move |state| {
            state.can_read(&table, &row_id, &author)
        })
    }

    pub(crate) fn can_update(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        self.call("can_update", move |state| {
            state.can_update(&table, &row_id, &author)
        })
    }

    pub(crate) fn can_delete(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Vec<u8>,
    ) -> Result<bool, JazzRnError> {
        self.call("can_delete", move |state| {
            state.can_delete(&table, &row_id, &author)
        })
    }

    pub(crate) fn attach_query(
        &self,
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("attach_query", move |state| {
            state.attach_query(id, query, author.as_deref(), opts_json.as_deref())?;
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
        query: u64,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("subscribe", move |state| {
            state.subscribe(id, query, author.as_deref(), opts_json.as_deref())?;
            Ok(id)
        })
    }

    pub(crate) fn subscribe_relation_query(
        &self,
        query_json: String,
        author: Option<Vec<u8>>,
        opts_json: Option<String>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("subscribe_relation_query", move |state| {
            state.subscribe_relation_query(
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
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let id = self.next_id();
        self.call("insert_with_id", move |state| {
            state.insert_with_id(
                id,
                &table,
                &row_id,
                &cells,
                author.as_deref(),
                updated_at_ms,
            )
        })
    }

    pub(crate) fn update(
        &self,
        table: String,
        row_id: Vec<u8>,
        patch: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let id = self.next_id();
        self.call("update", move |state| {
            state.update(
                id,
                &table,
                &row_id,
                &patch,
                author.as_deref(),
                updated_at_ms,
            )
        })
    }

    pub(crate) fn upsert(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let id = self.next_id();
        self.call("upsert", move |state| {
            state.upsert(
                id,
                &table,
                &row_id,
                &cells,
                author.as_deref(),
                updated_at_ms,
            )
        })
    }

    pub(crate) fn delete(
        &self,
        table: String,
        row_id: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let id = self.next_id();
        self.call("delete", move |state| {
            state.delete(id, &table, &row_id, author.as_deref(), updated_at_ms)
        })
    }

    pub(crate) fn restore(
        &self,
        table: String,
        row_id: Vec<u8>,
        cells: Vec<u8>,
        author: Option<Vec<u8>>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let id = self.next_id();
        self.call("restore", move |state| {
            state.restore(
                id,
                &table,
                &row_id,
                &cells,
                author.as_deref(),
                updated_at_ms,
            )
        })
    }

    pub(crate) fn begin_transaction(
        &self,
        kind: TransactionKind,
        author: Option<Vec<u8>>,
    ) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("begin_transaction", move |state| {
            state.begin_transaction(id, kind, author.as_deref())?;
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

    pub(crate) fn commit_transaction(&self, transaction: u64) -> Result<WriteOpened, JazzRnError> {
        let write_id = self.next_id();
        self.call("commit_transaction", move |state| {
            state.commit_transaction(transaction, write_id)
        })
    }

    pub(crate) fn rollback_transaction(&self, transaction: u64) -> Result<(), JazzRnError> {
        self.call("rollback_transaction", move |state| {
            state.rollback_transaction(transaction)
        })
    }

    pub(crate) fn release_transaction(&self, transaction: u64) {
        self.cast(move |state| state.rollback_transaction_if_present(transaction));
    }

    pub(crate) fn write_payload(&self, write: u64) -> Result<Vec<u8>, JazzRnError> {
        self.call("write_payload", move |state| {
            Ok(state.write(write)?.payload.clone())
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

    pub(crate) fn connect_upstream(&self) -> Result<u64, JazzRnError> {
        let id = self.next_id();
        self.call("connect_upstream", move |state| {
            state.connect_upstream(id)?;
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

enum CoreDb {
    Memory(Rc<Db<MemoryStorage>>),
    Persistent(Rc<Db<SqliteStorage>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionKind {
    Mergeable,
    Exclusive,
}

#[derive(Clone, Copy)]
struct TransactionEntry {
    kind: TransactionKind,
    open_tx: OpenTxId,
}

struct WriteEntry {
    tx_id: TxId,
    payload: Vec<u8>,
}

pub(crate) struct WriteOpened {
    pub(crate) id: u64,
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

#[derive(Clone, Default)]
struct WireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

struct RnWireTransport {
    queues: WireQueues,
}

impl WireTransport for RnWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

enum TransportEntry {
    Memory {
        db: Rc<Db<MemoryStorage>>,
        connection: Rc<RefCell<PeerConnection<MemoryStorage>>>,
        queues: WireQueues,
    },
    Persistent {
        db: Rc<Db<SqliteStorage>>,
        connection: Rc<RefCell<PeerConnection<SqliteStorage>>>,
        queues: WireQueues,
    },
}

impl TransportEntry {
    fn send(&self, frames: Vec<Vec<u8>>) {
        let queues = match self {
            Self::Memory { queues, .. } | Self::Persistent { queues, .. } => queues,
        };
        queues.inbound.borrow_mut().extend(frames);
    }

    fn recv(&self) -> Vec<Vec<u8>> {
        let queues = match self {
            Self::Memory { queues, .. } | Self::Persistent { queues, .. } => queues,
        };
        queues.outbound.borrow_mut().drain(..).collect()
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
    db: CoreDb,
    scheduler: RnScheduler,
    queries: HashMap<u64, PreparedQuery>,
    attachments: HashMap<u64, QueryAttachment>,
    transactions: HashMap<u64, TransactionEntry>,
    writes: HashMap<u64, WriteEntry>,
    waiters: HashMap<u64, WaiterEntry>,
    subscriptions: HashMap<u64, SubscriptionStream>,
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
            db,
            scheduler: RnScheduler::default(),
            queries: HashMap::new(),
            attachments: HashMap::new(),
            transactions: HashMap::new(),
            writes: HashMap::new(),
            waiters: HashMap::new(),
            subscriptions: HashMap::new(),
            transports: HashMap::new(),
            closed: false,
        })
    }

    fn set_tick_scheduler(&mut self, callback: Box<dyn TickSchedulerCallback>) {
        self.scheduler.set_callback(Some(callback));
        let scheduler = Rc::new(self.scheduler.clone());
        match &self.db {
            CoreDb::Memory(db) => db.set_tick_scheduler(Some(scheduler)),
            CoreDb::Persistent(db) => db.set_tick_scheduler(Some(scheduler)),
        }
    }

    fn tick(&mut self) -> Result<(), JazzRnError> {
        match &self.db {
            CoreDb::Memory(db) => db.tick(),
            CoreDb::Persistent(db) => db.tick(),
        }
        .map_err(core_error)
    }

    fn prepare_query(&mut self, id: u64, bytes: &[u8]) -> Result<(), JazzRnError> {
        let query = binding::decode_query(bytes)?;
        let prepared = match &self.db {
            CoreDb::Memory(db) => db.prepare_query(&query),
            CoreDb::Persistent(db) => db.prepare_query(&query),
        }
        .map_err(core_error)?;
        self.queries.insert(id, prepared);
        Ok(())
    }

    fn query(&self, id: u64) -> Result<PreparedQuery, JazzRnError> {
        self.queries
            .get(&id)
            .cloned()
            .ok_or_else(|| invalid_handle("prepared query", id))
    }

    fn transaction(&self, id: u64) -> Result<TransactionEntry, JazzRnError> {
        self.transactions
            .get(&id)
            .copied()
            .ok_or_else(|| invalid_handle("transaction", id))
    }

    fn write(&self, id: u64) -> Result<&WriteEntry, JazzRnError> {
        self.writes
            .get(&id)
            .ok_or_else(|| invalid_handle("write", id))
    }

    fn all(
        &mut self,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let rows = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                block_on(db.all_for_identity(&query, opts, author))
            }
            (CoreDb::Persistent(db), Some(author)) => {
                block_on(db.all_for_identity(&query, opts, author))
            }
            (CoreDb::Memory(db), None) => block_on(db.all(&query, opts)),
            (CoreDb::Persistent(db), None) => block_on(db.all(&query, opts)),
        }
        .map_err(core_error)?;
        binding::encode_rows(&rows).map_err(Into::into)
    }

    fn all_relation_snapshot(
        &mut self,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let snapshot = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                block_on(db.all_relation_snapshot_for_identity(&query, opts, author))
            }
            (CoreDb::Persistent(db), Some(author)) => {
                block_on(db.all_relation_snapshot_for_identity(&query, opts, author))
            }
            (CoreDb::Memory(db), None) => block_on(db.all_relation_snapshot(&query, opts)),
            (CoreDb::Persistent(db), None) => block_on(db.all_relation_snapshot(&query, opts)),
        }
        .map_err(core_error)?;
        binding::encode_relation_snapshot(&snapshot).map_err(Into::into)
    }

    fn all_relation_query(
        &mut self,
        query_json: &str,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<Vec<u8>, JazzRnError> {
        let query = binding::relation_query_from_json(query_json)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let snapshot = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                block_on(db.all_relation_query_for_identity(&query, opts, author))
            }
            (CoreDb::Persistent(db), Some(author)) => {
                block_on(db.all_relation_query_for_identity(&query, opts, author))
            }
            (CoreDb::Memory(db), None) => block_on(db.all_relation_query(&query, opts)),
            (CoreDb::Persistent(db), None) => block_on(db.all_relation_query(&query, opts)),
        }
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
        let _opts = binding::read_opts_from_json_str(opts_json)?;
        let query = self.query(query)?;
        let transaction = self.transaction(transaction)?;
        if transaction.kind != TransactionKind::Exclusive {
            return Err(JazzRnError::Runtime {
                message: "transaction reads require an exclusive transaction".to_owned(),
            });
        }
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let rows = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => db
                .exclusive_tx_ref(transaction.open_tx)
                .all_prepared_for_identity(&query, author),
            (CoreDb::Persistent(db), Some(author)) => db
                .exclusive_tx_ref(transaction.open_tx)
                .all_prepared_for_identity(&query, author),
            (CoreDb::Memory(db), None) => db
                .exclusive_tx_ref(transaction.open_tx)
                .all_prepared(&query),
            (CoreDb::Persistent(db), None) => db
                .exclusive_tx_ref(transaction.open_tx)
                .all_prepared(&query),
        }
        .map_err(core_error)?;
        binding::encode_rows(&rows).map_err(Into::into)
    }

    fn local_current_row(&mut self, table: &str, row_id: &[u8]) -> Result<Vec<u8>, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let row = match &self.db {
            CoreDb::Memory(db) => db.local_current_row(table, row_id),
            CoreDb::Persistent(db) => db.local_current_row(table, row_id),
        }
        .map_err(core_error)?;
        binding::encode_rows(&row.into_iter().collect::<Vec<_>>()).map_err(Into::into)
    }

    fn set_identity_claims(
        &mut self,
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
        match &self.db {
            CoreDb::Memory(db) => db.set_identity_claims(author, claims),
            CoreDb::Persistent(db) => db.set_identity_claims(author, claims),
        }
        Ok(())
    }

    fn can_insert(
        &mut self,
        table: &str,
        cells: &[u8],
        author: Option<&[u8]>,
    ) -> Result<bool, JazzRnError> {
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => db.can_insert_for_identity(table, cells, author),
            (CoreDb::Persistent(db), Some(author)) => {
                db.can_insert_for_identity(table, cells, author)
            }
            (CoreDb::Memory(db), None) => db.can_insert(table, cells),
            (CoreDb::Persistent(db), None) => db.can_insert(table, cells),
        }
        .map_err(core_error)
    }

    fn can_read(&mut self, table: &str, row_id: &[u8], author: &[u8]) -> Result<bool, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let author = binding::author_id_from_bytes(author)?;
        match &self.db {
            CoreDb::Memory(db) => db.can_read_for_identity(table, row_id, author),
            CoreDb::Persistent(db) => db.can_read_for_identity(table, row_id, author),
        }
        .map_err(core_error)
    }

    fn can_update(
        &mut self,
        table: &str,
        row_id: &[u8],
        author: &[u8],
    ) -> Result<bool, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let author = binding::author_id_from_bytes(author)?;
        match &self.db {
            CoreDb::Memory(db) => db.can_update_for_identity(table, row_id, author),
            CoreDb::Persistent(db) => db.can_update_for_identity(table, row_id, author),
        }
        .map_err(core_error)
    }

    fn can_delete(
        &mut self,
        table: &str,
        row_id: &[u8],
        author: &[u8],
    ) -> Result<bool, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let author = binding::author_id_from_bytes(author)?;
        match &self.db {
            CoreDb::Memory(db) => db.can_delete_for_identity(table, row_id, author),
            CoreDb::Persistent(db) => db.can_delete_for_identity(table, row_id, author),
        }
        .map_err(core_error)
    }

    fn attach_query(
        &mut self,
        id: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let attachment = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                db.attach_query_with_opts_for_identity(&query, opts, author)
            }
            (CoreDb::Persistent(db), Some(author)) => {
                db.attach_query_with_opts_for_identity(&query, opts, author)
            }
            (CoreDb::Memory(db), None) => db.attach_query_with_opts(&query, opts),
            (CoreDb::Persistent(db), None) => db.attach_query_with_opts(&query, opts),
        }
        .map_err(core_error)?;
        self.attachments.insert(id, attachment);
        Ok(())
    }

    fn attachment_is_covered(&self, id: u64) -> Result<bool, JazzRnError> {
        let attachment = self
            .attachments
            .get(&id)
            .ok_or_else(|| invalid_handle("query attachment", id))?;
        Ok(match &self.db {
            CoreDb::Memory(db) => db.query_attachment_is_covered(attachment),
            CoreDb::Persistent(db) => db.query_attachment_is_covered(attachment),
        })
    }

    fn detach_query(&mut self, id: u64) -> Result<(), JazzRnError> {
        let attachment = self
            .attachments
            .remove(&id)
            .ok_or_else(|| invalid_handle("query attachment", id))?;
        match &self.db {
            CoreDb::Memory(db) => db.detach_query(attachment),
            CoreDb::Persistent(db) => db.detach_query(attachment),
        }
        Ok(())
    }

    fn detach_query_if_present(&mut self, id: u64) -> Result<(), JazzRnError> {
        if let Some(attachment) = self.attachments.remove(&id) {
            match &self.db {
                CoreDb::Memory(db) => db.detach_query(attachment),
                CoreDb::Persistent(db) => db.detach_query(attachment),
            }
        }
        Ok(())
    }

    fn subscribe(
        &mut self,
        id: u64,
        query: u64,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = self.query(query)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let stream = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                block_on(db.subscribe_for_identity(&query, opts, author))
            }
            (CoreDb::Persistent(db), Some(author)) => {
                block_on(db.subscribe_for_identity(&query, opts, author))
            }
            (CoreDb::Memory(db), None) => block_on(db.subscribe(&query, opts)),
            (CoreDb::Persistent(db), None) => block_on(db.subscribe(&query, opts)),
        }
        .map_err(core_error)?;
        self.subscriptions.insert(id, stream);
        Ok(())
    }

    fn subscribe_relation_query(
        &mut self,
        id: u64,
        query_json: &str,
        author: Option<&[u8]>,
        opts_json: Option<&str>,
    ) -> Result<(), JazzRnError> {
        let query = binding::relation_query_from_json(query_json)?;
        let opts = binding::read_opts_from_json_str(opts_json)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let stream = match (&self.db, author) {
            (CoreDb::Memory(db), Some(author)) => {
                block_on(db.subscribe_relation_query_for_identity(&query, opts, author))
            }
            (CoreDb::Persistent(db), Some(author)) => {
                block_on(db.subscribe_relation_query_for_identity(&query, opts, author))
            }
            (CoreDb::Memory(db), None) => block_on(db.subscribe_relation_query(&query, opts)),
            (CoreDb::Persistent(db), None) => block_on(db.subscribe_relation_query(&query, opts)),
        }
        .map_err(core_error)?;
        self.subscriptions.insert(id, stream);
        Ok(())
    }

    fn subscription_read_all(&mut self, id: u64) -> Result<Vec<RnSubscriptionEvent>, JazzRnError> {
        let stream = self
            .subscriptions
            .get_mut(&id)
            .ok_or_else(|| invalid_handle("subscription", id))?;
        let mut events = Vec::new();
        while let Some(event) = stream.try_next_event() {
            let event = match binding::encode_subscription_event(&event)? {
                binding::EncodedSubscriptionEvent::Delta {
                    reset,
                    delta,
                    relation_delta,
                    settled,
                    tier,
                } => RnSubscriptionEvent {
                    event_type: "delta".to_owned(),
                    reset: Some(reset),
                    delta: Some(delta),
                    relation_delta: Some(relation_delta),
                    settled: Some(settled),
                    tier: Some(tier),
                    reason_json: None,
                },
                binding::EncodedSubscriptionEvent::Rejected { reason } => RnSubscriptionEvent {
                    event_type: "rejected".to_owned(),
                    reset: None,
                    delta: None,
                    relation_delta: None,
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
                    relation_delta: None,
                    settled: None,
                    tier: None,
                    reason_json: None,
                },
            };
            events.push(event);
        }
        Ok(events)
    }

    fn insert_with_id(
        &mut self,
        id: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = match (&self.db, author, updated_at_ms) {
            (CoreDb::Memory(db), Some(author), Some(now)) => write_parts(
                db.insert_with_id_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), Some(now)) => write_parts(
                db.insert_with_id_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), Some(author), None) => write_parts(
                db.insert_with_id_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), None) => write_parts(
                db.insert_with_id_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, Some(now)) => write_parts(
                db.insert_with_id_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), None, Some(now)) => write_parts(
                db.insert_with_id_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, None) => write_parts(
                db.insert_with_id(table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), None, None) => write_parts(
                db.insert_with_id(table, row_id, cells)
                    .map_err(core_error)?,
            ),
        };
        self.register_write(id, row_id, tx_id)
    }

    fn update(
        &mut self,
        id: u64,
        table: &str,
        row_id: &[u8],
        patch: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let patch = binding::decode_cells(patch)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = match (&self.db, author, updated_at_ms) {
            (CoreDb::Memory(db), Some(author), Some(now)) => write_parts(
                db.update_for_identity_at_ms(author, table, row_id, patch, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), Some(now)) => write_parts(
                db.update_for_identity_at_ms(author, table, row_id, patch, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), Some(author), None) => write_parts(
                db.update_for_identity(author, table, row_id, patch)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), None) => write_parts(
                db.update_for_identity(author, table, row_id, patch)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, Some(now)) => write_parts(
                db.update_at_ms(table, row_id, patch, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), None, Some(now)) => write_parts(
                db.update_at_ms(table, row_id, patch, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, None) => {
                write_parts(db.update(table, row_id, patch).map_err(core_error)?)
            }
            (CoreDb::Persistent(db), None, None) => {
                write_parts(db.update(table, row_id, patch).map_err(core_error)?)
            }
        };
        self.register_write(id, row_id, tx_id)
    }

    fn upsert(
        &mut self,
        id: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = match (&self.db, author, updated_at_ms) {
            (CoreDb::Memory(db), Some(author), Some(now)) => write_parts(
                db.upsert_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), Some(now)) => write_parts(
                db.upsert_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), Some(author), None) => write_parts(
                db.upsert_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), None) => write_parts(
                db.upsert_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, Some(now)) => write_parts(
                db.upsert_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), None, Some(now)) => write_parts(
                db.upsert_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, None) => {
                write_parts(db.upsert(table, row_id, cells).map_err(core_error)?)
            }
            (CoreDb::Persistent(db), None, None) => {
                write_parts(db.upsert(table, row_id, cells).map_err(core_error)?)
            }
        };
        self.register_write(id, row_id, tx_id)
    }

    fn delete(
        &mut self,
        id: u64,
        table: &str,
        row_id: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = match (&self.db, author, updated_at_ms) {
            (CoreDb::Memory(db), Some(author), Some(now)) => write_parts(
                db.delete_for_identity_at_ms(author, table, row_id, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), Some(now)) => write_parts(
                db.delete_for_identity_at_ms(author, table, row_id, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), Some(author), None) => write_parts(
                db.delete_for_identity(author, table, row_id)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), None) => write_parts(
                db.delete_for_identity(author, table, row_id)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, Some(now)) => {
                write_parts(db.delete_at_ms(table, row_id, now).map_err(core_error)?)
            }
            (CoreDb::Persistent(db), None, Some(now)) => {
                write_parts(db.delete_at_ms(table, row_id, now).map_err(core_error)?)
            }
            (CoreDb::Memory(db), None, None) => {
                write_parts(db.delete(table, row_id).map_err(core_error)?)
            }
            (CoreDb::Persistent(db), None, None) => {
                write_parts(db.delete(table, row_id).map_err(core_error)?)
            }
        };
        self.register_write(id, row_id, tx_id)
    }

    fn restore(
        &mut self,
        id: u64,
        table: &str,
        row_id: &[u8],
        cells: &[u8],
        author: Option<&[u8]>,
        updated_at_ms: Option<u64>,
    ) -> Result<WriteOpened, JazzRnError> {
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let (row_id, tx_id) = match (&self.db, author, updated_at_ms) {
            (CoreDb::Memory(db), Some(author), Some(now)) => write_parts(
                db.restore_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), Some(now)) => write_parts(
                db.restore_for_identity_at_ms(author, table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), Some(author), None) => write_parts(
                db.restore_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), Some(author), None) => write_parts(
                db.restore_for_identity(author, table, row_id, cells)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, Some(now)) => write_parts(
                db.restore_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Persistent(db), None, Some(now)) => write_parts(
                db.restore_at_ms(table, row_id, cells, now)
                    .map_err(core_error)?,
            ),
            (CoreDb::Memory(db), None, None) => {
                write_parts(db.restore(table, row_id, cells).map_err(core_error)?)
            }
            (CoreDb::Persistent(db), None, None) => {
                write_parts(db.restore(table, row_id, cells).map_err(core_error)?)
            }
        };
        self.register_write(id, row_id, tx_id)
    }

    fn register_write(
        &mut self,
        id: u64,
        row_id: RowUuid,
        tx_id: TxId,
    ) -> Result<WriteOpened, JazzRnError> {
        let payload = binding::encode_write_result(row_id, tx_id)?;
        self.writes.insert(
            id,
            WriteEntry {
                tx_id,
                payload: payload.clone(),
            },
        );
        Ok(WriteOpened { id })
    }

    fn begin_transaction(
        &mut self,
        id: u64,
        kind: TransactionKind,
        author: Option<&[u8]>,
    ) -> Result<(), JazzRnError> {
        let author = author.map(binding::author_id_from_bytes).transpose()?;
        let open_tx = match (&self.db, kind, author) {
            (CoreDb::Memory(db), TransactionKind::Mergeable, Some(author)) => {
                db.begin_mergeable_for_identity(author)
            }
            (CoreDb::Persistent(db), TransactionKind::Mergeable, Some(author)) => {
                db.begin_mergeable_for_identity(author)
            }
            (CoreDb::Memory(db), TransactionKind::Mergeable, None) => db.begin_mergeable(),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, None) => db.begin_mergeable(),
            (CoreDb::Memory(db), TransactionKind::Exclusive, _) => db.begin_exclusive(),
            (CoreDb::Persistent(db), TransactionKind::Exclusive, _) => db.begin_exclusive(),
        }
        .map_err(core_error)?;
        self.transactions
            .insert(id, TransactionEntry { kind, open_tx });
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
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        match (&self.db, transaction.kind, updated_at_ms) {
            (CoreDb::Memory(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .insert_with_id_at_ms(table, row_id, cells, now),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .insert_with_id_at_ms(table, row_id, cells, now),
            (CoreDb::Memory(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .insert_with_id(table, row_id, cells),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .insert_with_id(table, row_id, cells),
            (CoreDb::Memory(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .insert_with_id(table, row_id, cells),
            (CoreDb::Persistent(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .insert_with_id(table, row_id, cells),
        }
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
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let patch = binding::decode_cells(patch)?;
        match (&self.db, transaction.kind, updated_at_ms) {
            (CoreDb::Memory(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .update_at_ms(table, row_id, patch, now),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .update_at_ms(table, row_id, patch, now),
            (CoreDb::Memory(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .update(table, row_id, patch),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .update(table, row_id, patch),
            (CoreDb::Memory(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .update(table, row_id, patch),
            (CoreDb::Persistent(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .update(table, row_id, patch),
        }
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
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        match (&self.db, transaction.kind, updated_at_ms) {
            (CoreDb::Memory(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .delete_at_ms(table, row_id, now),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .delete_at_ms(table, row_id, now),
            (CoreDb::Memory(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .delete(table, row_id),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .delete(table, row_id),
            (CoreDb::Memory(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .delete(table, row_id),
            (CoreDb::Persistent(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .delete(table, row_id),
        }
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
        let row_id = binding::row_uuid_from_bytes(row_id)?;
        let cells = binding::decode_cells(cells)?;
        match (&self.db, transaction.kind, updated_at_ms) {
            (CoreDb::Memory(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .restore_at_ms(table, row_id, cells, now),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, Some(now)) => db
                .mergeable_tx_ref(transaction.open_tx)
                .restore_at_ms(table, row_id, cells, now),
            (CoreDb::Memory(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .restore(table, row_id, cells),
            (CoreDb::Persistent(db), TransactionKind::Mergeable, None) => db
                .mergeable_tx_ref(transaction.open_tx)
                .restore(table, row_id, cells),
            (CoreDb::Memory(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .restore(table, row_id, cells),
            (CoreDb::Persistent(db), TransactionKind::Exclusive, _) => db
                .exclusive_tx_ref(transaction.open_tx)
                .restore(table, row_id, cells),
        }
        .map_err(core_error)
    }

    fn commit_transaction(
        &mut self,
        transaction_id: u64,
        write_id: u64,
    ) -> Result<WriteOpened, JazzRnError> {
        let transaction = self.transaction(transaction_id)?;
        let tx_id = match (&self.db, transaction.kind) {
            (CoreDb::Memory(db), TransactionKind::Mergeable) => {
                db.commit_mergeable_handle(transaction.open_tx)
            }
            (CoreDb::Persistent(db), TransactionKind::Mergeable) => {
                db.commit_mergeable_handle(transaction.open_tx)
            }
            (CoreDb::Memory(db), TransactionKind::Exclusive) => {
                db.commit_exclusive_handle(transaction.open_tx)
            }
            (CoreDb::Persistent(db), TransactionKind::Exclusive) => {
                db.commit_exclusive_handle(transaction.open_tx)
            }
        }
        .map_err(core_error)?;
        self.transactions.remove(&transaction_id);
        self.register_write(write_id, RowUuid::from_bytes([0; 16]), tx_id)
    }

    fn rollback_transaction(&mut self, transaction_id: u64) -> Result<(), JazzRnError> {
        let transaction = self.transaction(transaction_id)?;
        match &self.db {
            CoreDb::Memory(db) => db.abandon_transaction_handle(transaction.open_tx),
            CoreDb::Persistent(db) => db.abandon_transaction_handle(transaction.open_tx),
        }
        .map_err(core_error)?;
        self.transactions.remove(&transaction_id);
        Ok(())
    }

    fn rollback_transaction_if_present(&mut self, transaction_id: u64) -> Result<(), JazzRnError> {
        let Some(transaction) = self.transactions.remove(&transaction_id) else {
            return Ok(());
        };
        match &self.db {
            CoreDb::Memory(db) => db.abandon_transaction_handle(transaction.open_tx),
            CoreDb::Persistent(db) => db.abandon_transaction_handle(transaction.open_tx),
        }
        .map_err(core_error)
    }

    fn wait_for_write(&mut self, write: u64, tier: &str) -> Result<(), JazzRnError> {
        let tx_id = self.write(write)?.tx_id;
        let tier = binding::durability_tier_from_str(tier)?;
        match &self.db {
            CoreDb::Memory(db) => binding::wait_for_tx(db, tx_id, tier),
            CoreDb::Persistent(db) => binding::wait_for_tx(db, tx_id, tier),
        }
        .map_err(Into::into)
    }

    fn write_state(&mut self, write: u64) -> Result<String, JazzRnError> {
        let tx_id = self.write(write)?.tx_id;
        let state = match &self.db {
            CoreDb::Memory(db) => db.write_state(tx_id),
            CoreDb::Persistent(db) => db.write_state(tx_id),
        }
        .map_err(core_error)?;
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
        match &self.db {
            CoreDb::Memory(db) => db.on_next_write_state_change(tx_id, move || {
                if let Some(sender) = callback_completion.borrow_mut().take() {
                    let _ = sender.send(WaiterSignal::Changed);
                }
            }),
            CoreDb::Persistent(db) => db.on_next_write_state_change(tx_id, move || {
                if let Some(sender) = callback_completion.borrow_mut().take() {
                    let _ = sender.send(WaiterSignal::Changed);
                }
            }),
        }
        self.waiters.insert(waiter, WaiterEntry { completion });
        Ok(receiver)
    }

    fn connect_upstream(&mut self, id: u64) -> Result<(), JazzRnError> {
        let queues = WireQueues::default();
        let transport = Box::new(WireTransportAdapter::current(RnWireTransport {
            queues: queues.clone(),
        }));
        let entry = match &self.db {
            CoreDb::Memory(db) => TransportEntry::Memory {
                db: Rc::clone(db),
                connection: db.connect_upstream(transport),
                queues,
            },
            CoreDb::Persistent(db) => TransportEntry::Persistent {
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
        match &self.db {
            CoreDb::Memory(db) => db.set_tick_scheduler(None),
            CoreDb::Persistent(db) => db.set_tick_scheduler(None),
        }
        self.scheduler.shutdown();
        for (_, transport) in self.transports.drain() {
            transport.close();
        }
        self.subscriptions.clear();
        let attachments = self
            .attachments
            .drain()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        for attachment in attachments {
            match &self.db {
                CoreDb::Memory(db) => db.detach_query(attachment),
                CoreDb::Persistent(db) => db.detach_query(attachment),
            }
        }
        let transactions = self
            .transactions
            .drain()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        for transaction in transactions {
            let _ = match &self.db {
                CoreDb::Memory(db) => db.abandon_transaction_handle(transaction.open_tx),
                CoreDb::Persistent(db) => db.abandon_transaction_handle(transaction.open_tx),
            };
        }
        self.writes.clear();
        self.queries.clear();
        let result = match &self.db {
            CoreDb::Memory(db) => db.close(),
            CoreDb::Persistent(db) => db.close(),
        }
        .map_err(core_error);
        self.closed = true;
        result
    }
}

fn write_parts<S>(write: WriteHandle<S>) -> (RowUuid, TxId)
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    (write.row_uuid(), write.mergeable_tx_id())
}

fn invalid_handle(kind: &str, id: u64) -> JazzRnError {
    JazzRnError::Runtime {
        message: format!("{kind} handle {id} is closed or belongs to another database"),
    }
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
