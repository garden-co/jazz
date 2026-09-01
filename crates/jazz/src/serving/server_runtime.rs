use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::thread;

use crate::db::{
    CommitUnitTrust, ConnectionSessionContext, DbIdentity, TickScheduler, TickUrgency, Transport,
};
use crate::groove::records::Value;
use crate::groove::storage::StorageFactory;
use crate::ids::{AuthorSubject, NodeUuid, SchemaVersionId};
use crate::node::EdgeCacheBudget;
use crate::protocol::{MigrationLens, SyncMessage};
use crate::schema::JazzSchema;
use crate::serving::{
    AbiBytes, InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession,
    StorageConfig,
};
use crate::tools::native_transport_connector::{
    NativeTransportTerminal, NativeTransportTerminalFuture,
};
use crate::wire::{TransportError, WireFrame, WireTransport, decode_frame, decode_sync_message};
use futures::channel::mpsc;
use futures::future::LocalBoxFuture;
use futures::task::LocalSpawnExt;
use futures::task::{ArcWake, waker};
use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};

/// Sendable handle for the thread that owns the in-memory server shell.
///
/// The underlying `InMemoryServerShell` is intentionally kept on one OS thread
/// because it currently stores its DB, sessions, and transports behind
/// `Rc<RefCell<...>>`. Axum request/websocket tasks can clone this handle, but
/// all shell access is serialized onto that owner thread. Shutdown explicitly
/// joins the owner before its surrounding server storage lifecycle completes.
/// This ensures native RocksDB resources are destroyed on their owner thread.
#[derive(Clone)]
pub struct ServerRuntimeHandle {
    inner: Arc<ServerShellInner>,
}

/// Opaque activity subscription for a server runtime.
pub struct ServerRuntimeActivity {
    receiver: watch::Receiver<u64>,
}

impl ServerRuntimeActivity {
    /// Wait until runtime work may have produced outbound frames.
    pub async fn changed(&mut self) -> Result<(), String> {
        self.receiver
            .changed()
            .await
            .map_err(|_| "server runtime activity channel closed".to_owned())
    }
}

/// Opaque stream of per-tick outbound frame batches.
pub struct ServerRuntimeFrameStream {
    receiver: tokio_mpsc::UnboundedReceiver<Result<Vec<AbiBytes>, String>>,
}

impl ServerRuntimeFrameStream {
    /// Receive the next completed tick's outbound frames.
    pub async fn recv(&mut self) -> Option<Result<Vec<AbiBytes>, String>> {
        self.receiver.recv().await
    }
}

/// Terminal reason for an attached edge-to-authority wire transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerUpstreamTerminalReason {
    /// The target-owned native socket pump reported its terminal outcome.
    NativeTransport(NativeTransportTerminal),
    /// The adapter rejected an outbound frame because its pump had failed.
    TransportFailed(String),
    /// Wire decoding or semantic auxiliary routing failed.
    ProtocolFailed(String),
    /// The owner dropped the connection to cancel its local wire driver.
    Cancelled,
    /// The shell owner stopped without returning a more specific reason.
    RuntimeStopped,
}

/// Owned lifetime signal for an attached edge-to-authority wire transport.
pub struct ServerUpstreamConnection {
    terminal: Option<oneshot::Receiver<ServerUpstreamTerminalReason>>,
    cancel: Option<oneshot::Sender<()>>,
}

impl ServerUpstreamConnection {
    /// Resolve with the reason the socket/semantic pump stopped.
    pub async fn terminal(mut self) -> ServerUpstreamTerminalReason {
        let terminal = self
            .terminal
            .take()
            .expect("upstream terminal receiver is consumed exactly once");
        let reason = terminal
            .await
            .unwrap_or(ServerUpstreamTerminalReason::RuntimeStopped);
        self.cancel.take();
        reason
    }
}

impl Drop for ServerUpstreamConnection {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

struct ServerShellInner {
    jobs: Mutex<Option<mpsc::UnboundedSender<ServerShellCommand>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown: Mutex<ShutdownState>,
    shutdown_changed: Condvar,
    activity_tx: watch::Sender<u64>,
    io_wakers: Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>>,
}

#[derive(Clone)]
enum ShutdownState {
    Running,
    InProgress,
    Finished(Result<(), String>),
}

impl Drop for ServerShellInner {
    fn drop(&mut self) {
        // `BuiltServer::shutdown` is the public, async lifecycle boundary and
        // drains request work before reaching this point. This fallback covers
        // direct builder use that is simply dropped: without it, dropping the
        // last sender merely asks the owner thread to exit and a caller can
        // race a reopen of the same RocksDB path before that exit completes.
        // There can be no remaining `ServerRuntimeHandle` when this runs, so no
        // public operation can be waiting for an owner-thread reply.
        let _ = shutdown_blocking(self);
    }
}

type ServerShellJob = Box<dyn FnOnce(&mut InMemoryServerShell) + Send + 'static>;
type AsyncServerShellJob =
    Box<dyn for<'a> FnOnce(&'a mut InMemoryServerShell) -> LocalBoxFuture<'a, ()> + Send + 'static>;

enum ServerShellCommand {
    Run(ServerShellJob),
    RunAsync(AsyncServerShellJob),
    AttachUpstreamWire {
        transport: Box<dyn WireTransport + Send>,
        transport_terminal: NativeTransportTerminalFuture,
        protocol_version: u16,
        features: crate::wire::WireFeatures,
        session_context: Option<ConnectionSessionContext>,
        reply: oneshot::Sender<Result<ServerUpstreamConnection, String>>,
    },
    Shutdown(std_mpsc::Sender<()>),
}

/// Bridges database-local progress requests back into the shell owner queue.
///
/// The database is intentionally thread-affine, so this scheduler is only
/// installed and invoked on the owner thread. The command queue preserves that
/// affinity while ensuring an Immediate request becomes a following shell turn
/// instead of waiting for unrelated socket activity.
#[derive(Clone)]
struct ServerShellTickScheduler {
    jobs: mpsc::UnboundedSender<ServerShellCommand>,
    activity_tx: watch::Sender<u64>,
    io_wakers: Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>>,
    state: Arc<ServerShellTickState>,
}

#[derive(Default)]
struct ServerShellTickState {
    queued: AtomicBool,
    delayed: AtomicBool,
}

/// A storage-future wake is translated back into one serialized shell turn.
/// The callback contains no database state, so it remains safe for a backend
/// to invoke through a normal cross-thread `Waker`.
struct ServerShellQueryRuntimeWake {
    scheduler: ServerShellTickScheduler,
}

impl ArcWake for ServerShellQueryRuntimeWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.scheduler.schedule_tick(TickUrgency::Immediate);
    }
}

impl ServerShellTickScheduler {
    fn enqueue_tick(
        jobs: &mpsc::UnboundedSender<ServerShellCommand>,
        activity_tx: &watch::Sender<u64>,
        io_wakers: &Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>>,
        state: &Arc<ServerShellTickState>,
    ) {
        if state.queued.swap(true, Ordering::AcqRel) {
            return;
        }

        let activity_tx = activity_tx.clone();
        let io_wakers = Arc::clone(io_wakers);
        let tick_state = Arc::clone(state);
        if jobs
            .unbounded_send(ServerShellCommand::RunAsync(Box::new(move |shell| {
                Box::pin(async move {
                    // Mark this turn consumed before ticking. Work discovered by
                    // the tick itself queues one follow-up turn instead of being
                    // lost behind the currently running command.
                    tick_state.queued.store(false, Ordering::Release);
                    if shell.tick_async().await.is_ok() {
                        notify_shell_activity(&activity_tx);
                        if let Ok(mut wakers) = io_wakers.lock() {
                            wakers.retain(|wake| wake.unbounded_send(()).is_ok());
                        }
                    }
                })
            })))
            .is_err()
        {
            state.queued.store(false, Ordering::Release);
        }
    }
}

impl TickScheduler for ServerShellTickScheduler {
    fn schedule_tick(&self, _urgency: TickUrgency) {
        Self::enqueue_tick(&self.jobs, &self.activity_tx, &self.io_wakers, &self.state);
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        // The shell owner must not sleep: it owns the thread-affine database
        // and needs to keep accepting transport work while an upload waits for
        // its admission window. Coalesce same-window retry wakes, then return
        // to the owner queue from a tiny timer thread.
        if self.state.delayed.swap(true, Ordering::AcqRel) {
            return;
        }
        let jobs = self.jobs.clone();
        let activity_tx = self.activity_tx.clone();
        let io_wakers = Arc::clone(&self.io_wakers);
        let state = Arc::clone(&self.state);
        thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(delay_ms));
            state.delayed.store(false, Ordering::Release);
            Self::enqueue_tick(&jobs, &activity_tx, &io_wakers, &state);
        });
    }

    fn query_runtime_waker(&self) -> Option<std::task::Waker> {
        Some(waker(Arc::new(ServerShellQueryRuntimeWake {
            scheduler: self.clone(),
        })))
    }
}

fn run_server_shell_owner(
    mut shell: InMemoryServerShell,
    mut receiver: mpsc::UnboundedReceiver<ServerShellCommand>,
    jobs: mpsc::UnboundedSender<ServerShellCommand>,
    activity_tx: watch::Sender<u64>,
    io_wakers: Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>>,
) {
    let scheduler = ServerShellTickScheduler {
        jobs,
        activity_tx,
        io_wakers: Arc::clone(&io_wakers),
        state: Arc::new(ServerShellTickState::default()),
    };
    shell.set_tick_scheduler(Some(Rc::new(scheduler.clone())));
    let mut executor = futures::executor::LocalPool::new();
    let spawner = executor.spawner();
    executor.run_until(async move {
        let mut pending = VecDeque::new();
        loop {
            let command = match pending.pop_front() {
                Some(command) => command,
                None => match receiver.next().await {
                    Some(command) => command,
                    None => return,
                },
            };
            match command {
                ServerShellCommand::Run(job) => job(&mut shell),
                ServerShellCommand::RunAsync(job) => {
                    let mut operation = job(&mut shell);
                    loop {
                        match futures::future::select(operation.as_mut(), receiver.next()).await {
                            futures::future::Either::Left(((), _)) => break,
                            futures::future::Either::Right((
                                Some(ServerShellCommand::Shutdown(stopped)),
                                _,
                            )) => {
                                // Dropping the suspended operation releases all
                                // evaluator leases before storage destruction.
                                drop(operation);
                                drop(shell);
                                let _ = stopped.send(());
                                return;
                            }
                            futures::future::Either::Right((Some(command), _)) => {
                                pending.push_back(command);
                            }
                            futures::future::Either::Right((None, _)) => return,
                        }
                    }
                }
                ServerShellCommand::AttachUpstreamWire {
                    transport,
                    transport_terminal,
                    protocol_version,
                    features,
                    session_context,
                    reply,
                } => {
                    let io =
                        shell.connect_upstream_wire_io(protocol_version, features, session_context);
                    let connection_id = io.connection_id;
                    let (wake_tx, wake_rx) = mpsc::unbounded();
                    if let Ok(mut wakers) = io_wakers.lock() {
                        wakers.push(wake_tx);
                    }
                    let (terminal_tx, terminal) = oneshot::channel();
                    let (cancel, cancel_rx) = oneshot::channel();
                    let wire_scheduler = scheduler.clone();
                    let spawn_result = spawner.spawn_local(async move {
                        let reason = drive_upstream_wire(
                            transport,
                            transport_terminal,
                            io,
                            wake_rx,
                            wire_scheduler,
                            cancel_rx,
                        )
                        .await;
                        let _ = terminal_tx.send(reason);
                    });
                    let connection = spawn_result
                        .map(|()| ServerUpstreamConnection {
                            terminal: Some(terminal),
                            cancel: Some(cancel),
                        })
                        .map_err(|error| {
                            format!("failed to spawn local upstream wire pump: {error}")
                        });
                    if connection.is_ok() {
                        // Creating the semantic upstream queues its initial
                        // claims/frontier work, but unlike the in-memory attach
                        // path it has no caller-owned tick. Queue that first
                        // shell turn only after the wire pump exists so initial
                        // bootstrap and reconnect both flush without waiting
                        // for unrelated downstream activity.
                        scheduler.schedule_tick(TickUrgency::Immediate);
                    } else {
                        shell.disconnect_upstream_wire_io(connection_id);
                    }
                    let _ = reply.send(connection);
                }
                ServerShellCommand::Shutdown(stopped) => {
                    // Native storage is destroyed on its owner thread before
                    // shutdown is acknowledged.
                    drop(shell);
                    let _ = stopped.send(());
                    return;
                }
            }

            // A shell tick can discover and enqueue another immediate tick.
            // The owner also hosts upstream wire pumps in this LocalPool, so
            // consuming a run of already-ready commands without yielding can
            // indefinitely delay an edge's inbound/outbound wire progress.
            // Cooperate once per command: queued shell work remains ordered,
            // while a ready wire pump gets an opportunity to transfer the
            // corresponding core update.
            yield_to_local_tasks().await;
        }
    });
}

/// Yield exactly once through the current local executor without introducing a
/// host timer or moving the thread-affine shell to another thread.
async fn yield_to_local_tasks() {
    let mut yielded = false;
    futures::future::poll_fn(move |context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

async fn drive_upstream_wire(
    mut wire: Box<dyn WireTransport + Send>,
    mut transport_terminal: NativeTransportTerminalFuture,
    io: super::ServerUpstreamIo,
    mut wake_rx: mpsc::UnboundedReceiver<()>,
    scheduler: ServerShellTickScheduler,
    mut cancel_rx: oneshot::Receiver<()>,
) -> ServerUpstreamTerminalReason {
    let connection_id = io.connection_id;
    let reason = async {
        loop {
            let mut staged_semantic_input = false;
            while let Some(frame) = wire.try_recv_frame() {
                match io.pump.route_incoming_wire_frame(frame, io.features).await {
                    Ok(Some(canonical)) => {
                        io.transport
                            .queues
                            .borrow_mut()
                            .inbound
                            .push_back(canonical);
                        staged_semantic_input = true;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return ServerUpstreamTerminalReason::ProtocolFailed(error);
                    }
                }
            }
            // The socket callback can notify the host before this local pump gets
            // a turn to stage its frame. Schedule only after canonical input is
            // actually visible to the shell, otherwise a downstream activity tick
            // may observe an empty queue and leave the edge dormant indefinitely.
            if staged_semantic_input {
                scheduler.schedule_tick(TickUrgency::Immediate);
            }
            let mut transport_backpressured = false;
            loop {
                let frame = io.transport.queues.borrow_mut().outbound.pop_front();
                let Some(frame) = frame else { break };
                match wire.send_frame(frame.clone()) {
                    Ok(()) => {}
                    Err(TransportError::Backpressure) => {
                        // Backpressure is a retry signal from this live
                        // transport, not a disconnect. Put the exact frame
                        // back ahead of later semantic work so its FIFO
                        // obligation cannot be lost or overtaken.
                        io.transport.queues.borrow_mut().outbound.push_front(frame);
                        transport_backpressured = true;
                        break;
                    }
                    Err(TransportError::Failed(error)) => {
                        return ServerUpstreamTerminalReason::TransportFailed(error);
                    }
                }
            }
            if !transport_backpressured {
                loop {
                    let reservation = match io.pump.reserve_outbound_wire_frame(
                        io.protocol_version,
                        io.features,
                        None,
                    ) {
                        Ok(reservation) => reservation,
                        Err(error) => {
                            return ServerUpstreamTerminalReason::ProtocolFailed(error);
                        }
                    };
                    let Some(mut reservation) = reservation else {
                        break;
                    };
                    match wire.send_frame(reservation.take_frame()) {
                        Ok(()) => reservation.commit(),
                        Err(TransportError::Backpressure) => {
                            // The reservation restores requests and relay
                            // responses (including their capacity claim) to
                            // their exact FIFO lane before we wait for this
                            // same transport to become writable again.
                            drop(reservation);
                            break;
                        }
                        Err(TransportError::Failed(error)) => {
                            // Do not let a failed connection consume the
                            // auxiliary obligation while the driver reports
                            // its terminal outcome to the reconnect owner.
                            drop(reservation);
                            return ServerUpstreamTerminalReason::TransportFailed(error);
                        }
                    }
                }
            }

            let external_wake = wake_rx.next().fuse();
            let auxiliary_wake = io.pump.outbound_ready().fuse();
            let transport_stopped = transport_terminal.as_mut().fuse();
            let cancelled = (&mut cancel_rx).fuse();
            futures::pin_mut!(external_wake, auxiliary_wake, transport_stopped, cancelled);
            futures::select_biased! {
                _ = cancelled => return ServerUpstreamTerminalReason::Cancelled,
                terminal = transport_stopped => {
                    return ServerUpstreamTerminalReason::NativeTransport(terminal);
                }
                wake = external_wake => {
                    if wake.is_none() {
                        return ServerUpstreamTerminalReason::RuntimeStopped;
                    }
                }
                _ = auxiliary_wake => {
                    if io.pump.is_disconnected() {
                        return ServerUpstreamTerminalReason::TransportFailed(
                            "upstream semantic transport disconnected".to_owned(),
                        );
                    }
                }
            }
        }
    }
    .await;
    io.pump.disconnect();
    let (detached_tx, detached_rx) = oneshot::channel();
    if scheduler
        .jobs
        .unbounded_send(ServerShellCommand::Run(Box::new(move |shell| {
            shell.disconnect_upstream_wire_io(connection_id);
            let _ = detached_tx.send(());
        })))
        .is_ok()
    {
        // Do not expose the terminal event to the reconnect loop until the
        // dead semantic link has relinquished authority. Otherwise the
        // replacement wire can attach as a parallel non-owner and strand
        // writes retained during the outage.
        let _ = detached_rx.await;
    }
    reason
}

impl ServerRuntimeHandle {
    /// Reopen an already bootstrapped dynamic edge. A blank store returns
    /// `None` so its owner can run the authenticated bootstrap exchange.
    pub fn try_start_dynamic_edge_from_storage(
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        edge_cache_budget: Option<EdgeCacheBudget>,
    ) -> Result<Option<Self>, String> {
        let (jobs, receiver) = mpsc::unbounded::<ServerShellCommand>();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let io_wakers = Arc::new(Mutex::new(Vec::new()));
        let owner_io_wakers = Arc::clone(&io_wakers);
        let owner_jobs = jobs.clone();
        let owner_activity_tx = activity_tx.clone();
        let join = thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let shell = match InMemoryServerShell::try_start_dynamic_edge_from_storage(
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorSubject::SYSTEM,
                    },
                    storage_config,
                    storage_factory,
                    edge_cache_budget,
                ) {
                    Ok(Some(shell)) => {
                        let _ = started_tx.send(Ok(true));
                        shell
                    }
                    Ok(None) => {
                        let _ = started_tx.send(Ok(false));
                        return;
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                run_server_shell_owner(
                    shell,
                    receiver,
                    owner_jobs,
                    owner_activity_tx,
                    owner_io_wakers,
                );
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;
        let started = started_rx
            .recv()
            .map_err(|_| "server shell thread exited before dynamic reopen".to_owned())??;
        Ok(started.then_some(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                shutdown: Mutex::new(ShutdownState::Running),
                shutdown_changed: Condvar::new(),
                activity_tx,
                io_wakers,
            }),
        }))
    }

    /// Construct a ready edge shell only after an authenticated bootstrap
    /// snapshot has been durably adopted. The owner thread is not published to
    /// downstream routes until this returns successfully.
    pub fn start_dynamic_edge_with_catalogue_snapshot(
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        edge_cache_budget: Option<EdgeCacheBudget>,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::unbounded::<ServerShellCommand>();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let io_wakers = Arc::new(Mutex::new(Vec::new()));
        let owner_io_wakers = Arc::clone(&io_wakers);
        let owner_jobs = jobs.clone();
        let owner_activity_tx = activity_tx.clone();

        let join = thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let shell = match InMemoryServerShell::start_dynamic_edge_with_catalogue_snapshot(
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorSubject::SYSTEM,
                    },
                    storage_config,
                    storage_factory,
                    edge_cache_budget,
                    snapshot,
                ) {
                    Ok(shell) => {
                        let _ = started_tx.send(Ok(()));
                        shell
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                run_server_shell_owner(
                    shell,
                    receiver,
                    owner_jobs,
                    owner_activity_tx,
                    owner_io_wakers,
                );
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "server shell thread exited before dynamic bootstrap".to_owned())??;
        Ok(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                shutdown: Mutex::new(ShutdownState::Running),
                shutdown_changed: Condvar::new(),
                activity_tx,
                io_wakers,
            }),
        })
    }

    /// Encode the trusted catalogue through the negotiated wire format.
    pub async fn encoded_trusted_catalogue_snapshot(
        &self,
        protocol_version: u16,
        features: crate::wire::WireFeatures,
    ) -> Result<Vec<AbiBytes>, String> {
        self.run(move |shell| {
            shell
                .encoded_trusted_catalogue_snapshot(protocol_version, features)
                .map_err(|error| error.to_string())
        })
        .await
    }

    /// Replace an already persisted edge's authority catalogue through the
    /// same authenticated snapshot path used at first bootstrap. The snapshot
    /// adoption rebuilds the local physical projection registry before this
    /// call returns, so callers may safely make the edge externally ready.
    pub async fn apply_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), String> {
        self.run(move |shell| {
            shell
                .apply_trusted_catalogue_snapshot(snapshot)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) -> Result<(), String> {
        self.run(move |shell| {
            shell.set_catalogue_activation_failpoint(failpoint);
            Ok(())
        })
        .await
    }

    #[doc(hidden)]
    pub async fn trusted_catalogue_snapshot_for_test(
        &self,
    ) -> Result<crate::protocol::CatalogueSnapshot, String> {
        self.run(move |shell| {
            shell
                .trusted_catalogue_snapshot()
                .map_err(|error| error.to_string())
        })
        .await
    }
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn runtime_catalogue_contains(
        &self,
        schema: SchemaVersionId,
        lens: crate::ids::MigrationLensId,
    ) -> Result<(bool, bool), String> {
        self.run(move |shell| Ok(shell.runtime_catalogue_contains(schema, lens)))
            .await
    }

    /// Start a core runtime over the selected storage configuration.
    pub fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            storage_factory,
            NodeRole::Core,
            None,
            false,
        )
    }

    /// Start a runtime with an explicit role and optional Edge cache budget.
    pub fn start_with_storage_config(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            storage_factory,
            role,
            edge_cache_budget,
            true,
        )
    }

    fn start_with_storage_config_and_permissions(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
        permissions_ready: bool,
    ) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::unbounded::<ServerShellCommand>();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let io_wakers = Arc::new(Mutex::new(Vec::new()));
        let owner_io_wakers = Arc::clone(&io_wakers);
        let owner_jobs = jobs.clone();
        let owner_activity_tx = activity_tx.clone();

        let join = thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let config = InMemoryServerShellConfig::new(
                    schema,
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorSubject::SYSTEM,
                    },
                )
                .with_row_id_seed(0x5e)
                .with_runtime_schema_bootstrap()
                .with_role(role);
                let config = match storage_factory {
                    Some(factory) => config.with_storage_factory(factory),
                    None => config,
                };
                let config = match edge_cache_budget {
                    Some(budget) => config.with_edge_cache_budget(budget),
                    None => config,
                };
                let shell = match InMemoryServerShell::start_with_storage(config, storage_config) {
                    Ok(mut shell) => {
                        if !permissions_ready && let Err(error) = shell.set_permissions_ready(false)
                        {
                            let _ = started_tx.send(Err(error.to_string()));
                            return;
                        }
                        let _ = started_tx.send(Ok(()));
                        shell
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };

                run_server_shell_owner(
                    shell,
                    receiver,
                    owner_jobs,
                    owner_activity_tx,
                    owner_io_wakers,
                );
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "server shell thread exited before startup".to_owned())??;
        Ok(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                shutdown: Mutex::new(ShutdownState::Running),
                shutdown_changed: Condvar::new(),
                activity_tx,
                io_wakers,
            }),
        })
    }

    /// Subscribe to runtime activity that may make outbound session frames available.
    pub fn subscribe_activity(&self) -> ServerRuntimeActivity {
        ServerRuntimeActivity {
            receiver: self.inner.activity_tx.subscribe(),
        }
    }

    /// Admit an authenticated session into the semantic runtime.
    pub async fn open_with_session_context(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
        negotiated_features: crate::wire::WireFeatures,
        session_context: Option<ConnectionSessionContext>,
        link_admission: crate::serving::ServerLinkAdmission,
    ) -> Result<ServerSession, String> {
        self.run(move |shell| {
            shell
                .accept_subscriber_session_with_claims_and_trust_and_context(
                    identity,
                    claims,
                    trust,
                    negotiated_features,
                    session_context,
                    link_admission,
                )
                .map_err(|error| error.to_string())
        })
        .await
    }

    /// Publish a validated schema and optional migration lens to the runtime.
    pub async fn publish_schema_with_lens(
        &self,
        schema: JazzSchema,
        lens: MigrationLens,
        new_tables: Vec<String>,
        dropped_tables: Vec<String>,
    ) -> Result<SchemaVersionId, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let result = self
            .run(move |shell| {
                shell
                    .publish_runtime_schema_with_lens(schema, lens, new_tables, dropped_tables)
                    .map_err(|error| error.to_string())
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    /// Compile and publish the permissions source selected by the catalogue
    /// shell.
    pub async fn publish_permissions_source(
        &self,
        schema: crate::tools::Schema,
        lineage_source: SchemaVersionId,
    ) -> Result<SchemaVersionId, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let result = self
            .run(move |shell| {
                let schema =
                    crate::schema::JazzSchema::new(&schema).map_err(|error| error.to_string())?;
                shell
                    .publish_permissions_schema(schema, lineage_source)
                    .map_err(|error| error.to_string())
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    /// Service one WebSocket message as ordered frame-sized shell ticks.
    ///
    /// Results become available as each frame has completed its tick while the
    /// shell thread keeps ingesting later frames. This is intentionally a
    /// streaming operation rather than one large `run` job so a route can
    /// publish a durability fate as soon as it is true.
    /// Apply an inbound frame batch and stream every immediately available response.
    pub fn receive_tick_stream(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<ServerRuntimeFrameStream, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let (outbound_tx, outbound_rx) = tokio_mpsc::unbounded_channel();
        self.send(ServerShellCommand::RunAsync(Box::new(move |shell| {
            Box::pin(async move {
                for frame in frames {
                    let phase = inbound_frame_phase(&frame);
                    let result = match shell.receive_frames_async(session, [frame]).await {
                        Err(error) => Err(format!("server receive {phase}: {error}")),
                        Ok(()) => match shell.tick_async().await {
                            Err(error) => Err(format!("server tick after {phase}: {error}")),
                            Ok(()) => shell
                                .take_frames(session)
                                .map_err(|error| format!("server drain after {phase}: {error}")),
                        },
                    };
                    let keep_streaming = result.is_ok();
                    if outbound_tx.send(result).is_err() || !keep_streaming {
                        return;
                    }
                    notify_shell_activity(&activity_tx);
                }
            })
        })))?;
        Ok(ServerRuntimeFrameStream {
            receiver: outbound_rx,
        })
    }

    /// Tick the runtime once and return pending frames for one session.
    pub async fn tick_take(&self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        let activity_tx = self.inner.activity_tx.clone();
        self.run_async(move |shell| {
            Box::pin(async move {
                let result = match shell.tick_async().await {
                    Ok(()) => shell
                        .take_frames(session)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.to_string()),
                };
                // Progress-based re-arm: a tick that yielded frames may have more
                // behind it (large resets span many ticks), so schedule another.
                // Empty ticks do NOT re-arm — that unconditional re-arm was the
                // consolidation-spin feeder. One notification must never buy an
                // unbounded loop, and delivery must never stall mid-reset; frames
                // produced is exactly the signal that separates the two.
                if let Ok(frames) = &result
                    && !frames.is_empty()
                {
                    notify_shell_activity(&activity_tx);
                }
                result
            })
        })
        .await
    }

    /// Attach a negotiated upstream transport to an edge runtime.
    pub async fn connect_upstream(
        &self,
        transport: Box<dyn Transport + Send>,
    ) -> Result<(), String> {
        let activity_tx = self.inner.activity_tx.clone();
        self.run(move |shell| {
            let result = shell
                .connect_upstream(transport)
                .map_err(|error| error.to_string());
            if result.is_ok() {
                notify_shell_activity(&activity_tx);
            }
            result
        })
        .await
    }

    /// Attach a negotiated native wire transport and drive its semantic and
    /// auxiliary channels on the shell's local executor.
    pub async fn connect_upstream_wire(
        &self,
        transport: Box<dyn WireTransport + Send>,
        transport_terminal: NativeTransportTerminalFuture,
        protocol_version: u16,
        features: crate::wire::WireFeatures,
        session_context: Option<ConnectionSessionContext>,
    ) -> Result<ServerUpstreamConnection, String> {
        let (reply, response) = oneshot::channel();
        self.send(ServerShellCommand::AttachUpstreamWire {
            transport,
            transport_terminal,
            protocol_version,
            features,
            session_context,
            reply,
        })?;
        let result = response
            .await
            .map_err(|_| "server shell dropped upstream wire attachment".to_owned())?;
        if result.is_ok() {
            self.notify_activity();
        }
        result
    }

    /// Wake shell tasks after an adapter stages inbound work.
    pub fn notify_activity(&self) {
        notify_shell_activity(&self.inner.activity_tx);
        if let Ok(mut wakers) = self.inner.io_wakers.lock() {
            wakers.retain(|wake| wake.unbounded_send(()).is_ok());
        }
    }

    /// Close a semantic session without exposing runtime storage or peer state.
    pub fn close(&self, session: ServerSession) {
        let _ = self.send(ServerShellCommand::Run(Box::new(move |shell| {
            let _ = shell.close_session(session);
        })));
    }

    /// Retire the job sender, then wait until the owner has dropped the shell
    /// and its storage. It is safe for multiple shutdown paths to call this.
    /// Stop and join the runtime owner thread.
    pub async fn shutdown(&self) -> Result<(), String> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || shutdown_blocking(&inner))
            .await
            .map_err(|error| format!("server shell shutdown task failed: {error}"))?
    }

    fn send(&self, command: ServerShellCommand) -> Result<(), String> {
        self.inner
            .jobs
            .lock()
            .map_err(|_| "server shell jobs mutex poisoned".to_owned())?
            .as_ref()
            .ok_or_else(|| "server shell is shut down".to_owned())?
            .unbounded_send(command)
            .map_err(|_| "server shell thread is not running".to_owned())
    }

    async fn run<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.send(ServerShellCommand::Run(Box::new(move |shell| {
            let _ = reply.send(run_on_shell(shell));
        })))?;
        response
            .await
            .map_err(|_| "server shell thread dropped response".to_owned())?
    }

    async fn run_async<T>(
        &self,
        run_on_shell: impl for<'a> FnOnce(
            &'a mut InMemoryServerShell,
        ) -> LocalBoxFuture<'a, Result<T, String>>
        + Send
        + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.send(ServerShellCommand::RunAsync(Box::new(move |shell| {
            Box::pin(async move {
                let result = run_on_shell(shell).await;
                let _ = reply.send(result);
            })
        })))?;
        response
            .await
            .map_err(|_| "server shell thread dropped async response".to_owned())?
    }
}

fn shutdown_blocking(inner: &ServerShellInner) -> Result<(), String> {
    shutdown_blocking_with(inner, perform_shutdown_blocking, || {})
}

fn shutdown_blocking_with(
    inner: &ServerShellInner,
    perform: impl FnOnce(&ServerShellInner) -> Result<(), String>,
    on_waiting: impl FnOnce(),
) -> Result<(), String> {
    let mut on_waiting = Some(on_waiting);
    {
        let mut state = inner
            .shutdown
            .lock()
            .map_err(|_| "server shell shutdown mutex poisoned".to_owned())?;
        loop {
            match &*state {
                ShutdownState::Running => {
                    *state = ShutdownState::InProgress;
                    break;
                }
                ShutdownState::InProgress => {
                    if let Some(on_waiting) = on_waiting.take() {
                        on_waiting();
                    }
                    state = inner
                        .shutdown_changed
                        .wait(state)
                        .map_err(|_| "server shell shutdown mutex poisoned".to_owned())?;
                }
                ShutdownState::Finished(result) => return result.clone(),
            }
        }
    }

    let result = catch_unwind(AssertUnwindSafe(|| perform(inner))).unwrap_or_else(|payload| {
        Err(format!(
            "server shell shutdown panicked: {}",
            panic_payload_message(payload.as_ref())
        ))
    });
    let mut state = inner
        .shutdown
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *state = ShutdownState::Finished(result.clone());
    inner.shutdown_changed.notify_all();
    result
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

fn perform_shutdown_blocking(inner: &ServerShellInner) -> Result<(), String> {
    let sender = inner
        .jobs
        .lock()
        .map_err(|_| "server shell jobs mutex poisoned".to_owned())?
        .take();
    let Some(sender) = sender else {
        return Ok(());
    };

    let (stopped_tx, stopped_rx) = std_mpsc::channel();
    sender
        .unbounded_send(ServerShellCommand::Shutdown(stopped_tx))
        .map_err(|_| "server shell thread is not running".to_owned())?;
    drop(sender);
    stopped_rx
        .recv()
        .map_err(|_| "server shell thread exited before storage shutdown".to_owned())?;

    if let Some(join) = inner
        .join
        .lock()
        .map_err(|_| "server shell join mutex poisoned".to_owned())?
        .take()
    {
        join.join()
            .map_err(|_| "server shell thread panicked during shutdown".to_owned())?;
    }
    Ok(())
}

fn inbound_frame_phase(frame: &[u8]) -> String {
    let Ok(frame) = decode_frame(frame) else {
        return "malformed wire frame".to_owned();
    };
    let WireFrame::Message(envelope) = frame else {
        return match frame {
            WireFrame::Hello(_) => "wire hello".to_owned(),
            WireFrame::Error(_) => "wire error".to_owned(),
            WireFrame::MessageFragment(_) => "wire message fragment".to_owned(),
            WireFrame::Message(_) => unreachable!("message handled above"),
        };
    };
    match decode_sync_message(&envelope.payload) {
        Ok(message) => sync_message_name(&message).to_owned(),
        Err(_) => "malformed SyncMessage".to_owned(),
    }
}

fn sync_message_name(message: &SyncMessage) -> &'static str {
    // This deliberately names only the protocol variant. Never format the
    // message itself here: claims and row payloads must not escape through a
    // transport diagnostic.
    match message {
        SyncMessage::ChunkRequestBatch(_) => "ChunkRequestBatch",
        SyncMessage::ChunkResponseBatch(_) => "ChunkResponseBatch",
        SyncMessage::ChunkUploadStart(_) => "ChunkUploadStart",
        SyncMessage::ChunkUploadNodes(_) => "ChunkUploadNodes",
        SyncMessage::ChunkUploadResult(_) => "ChunkUploadResult",
        SyncMessage::SessionClaims { .. } => "SessionClaims",
        SyncMessage::CommitUnit { .. } => "CommitUnit",
        SyncMessage::FateUpdate { .. } => "FateUpdate",
        SyncMessage::RegisterShape { .. } => "RegisterShape",
        SyncMessage::Subscribe(_) => "Subscribe",
        SyncMessage::SubscribeRejected { .. } => "SubscribeRejected",
        SyncMessage::Unsubscribe { .. } => "Unsubscribe",
        SyncMessage::PublishSchema { .. } => "PublishSchema",
        SyncMessage::PublishSchemaWithLens { .. } => "PublishSchemaWithLens",
        SyncMessage::PublishLens { .. } => "PublishLens",
        SyncMessage::SetCurrentWriteSchema { .. } => "SetCurrentWriteSchema",
        SyncMessage::CatalogueAck(_) => "CatalogueAck",
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. }) => "ViewUpdate",
        SyncMessage::FetchRowVersions { .. } => "FetchRowVersions",
        SyncMessage::RowVersionPayloads { .. } => "RowVersionPayloads",
        SyncMessage::CatalogueSnapshot(_) => "CatalogueSnapshot",
        SyncMessage::PermissionAdviceRequest { .. } => "PermissionAdviceRequest",
        SyncMessage::PermissionAdviceResponse { .. } => "PermissionAdviceResponse",
        SyncMessage::AuthorizationScopeSubscribe { .. } => "AuthorizationScopeSubscribe",
        SyncMessage::AuthorizationScopeReceipt { .. } => "AuthorizationScopeReceipt",
        SyncMessage::AuthorizationScopeIntent { .. } => "AuthorizationScopeIntent",
        SyncMessage::AuthorizationScopeView { .. } => "AuthorizationScopeView",
        SyncMessage::AuthorizationScopeAggregateReceipt { .. } => {
            "AuthorizationScopeAggregateReceipt"
        }
        SyncMessage::AuthorizationScopeUnavailable { .. } => "AuthorizationScopeUnavailable",
        SyncMessage::AuthorizationScopeDecision { .. } => "AuthorizationScopeDecision",
    }
}

fn notify_shell_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ReadViewKey, Subscribe, SubscriptionKey};
    use crate::query::{BindingId, ShapeId};
    use crate::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use crate::wire::{WireEnvelope, encode_frame, encode_sync_message};
    use futures::FutureExt;
    use std::collections::VecDeque;
    use std::time::Duration;

    /// A host-facing wire queue whose canonical input can be withheld until
    /// after the shell has consumed an earlier, empty activity tick.
    #[derive(Default)]
    struct QueuedWireTransport {
        inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }

    impl QueuedWireTransport {
        fn push_inbound(&self, frame: Vec<u8>) {
            self.inbound.lock().unwrap().push_back(frame);
        }
    }

    impl WireTransport for QueuedWireTransport {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), crate::wire::TransportError> {
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.lock().unwrap().pop_front()
        }
    }

    #[derive(Default)]
    struct FirstSendBackpressureState {
        rejected: bool,
        sent: Vec<Vec<u8>>,
    }

    struct FirstSendBackpressureWire {
        state: Arc<Mutex<FirstSendBackpressureState>>,
    }

    impl WireTransport for FirstSendBackpressureWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), crate::wire::TransportError> {
            let mut state = self.state.lock().unwrap();
            if !state.rejected {
                state.rejected = true;
                return Err(crate::wire::TransportError::Backpressure);
            }
            state.sent.push(frame);
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    fn encode_message(message: SyncMessage) -> Vec<u8> {
        let payload = encode_sync_message(&message).unwrap();
        encode_frame(&WireFrame::Message(WireEnvelope::new(0, 0, payload))).unwrap()
    }

    // This internal test is necessary because the terminal reason crosses the
    // shell's thread-affine local executor; no public API exposes that channel.
    #[test]
    fn upstream_connection_returns_the_driver_terminal_reason() {
        let (terminal_tx, terminal) = oneshot::channel();
        let (cancel, _cancelled) = oneshot::channel();
        let connection = ServerUpstreamConnection {
            terminal: Some(terminal),
            cancel: Some(cancel),
        };
        terminal_tx
            .send(ServerUpstreamTerminalReason::TransportFailed(
                "socket closed".to_owned(),
            ))
            .unwrap();

        assert_eq!(
            futures::executor::block_on(connection.terminal()),
            ServerUpstreamTerminalReason::TransportFailed("socket closed".to_owned())
        );
    }

    // This internal test is necessary because cancellation owns a local wire
    // pump rather than an application-facing session.
    #[test]
    fn dropping_upstream_connection_cancels_its_wire_driver() {
        let (_terminal_tx, terminal) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        let connection = ServerUpstreamConnection {
            terminal: Some(terminal),
            cancel: Some(cancel),
        };

        drop(connection);

        assert!(futures::executor::block_on(cancelled).is_ok());
    }

    // This internal receipt is necessary because retrying the native
    // binding-owned wire driver is below the public server API. It proves a
    // full bounded queue retains its canonical frame and retries that same
    // live transport rather than reconnecting and losing the obligation.
    #[test]
    fn semantic_backpressure_retries_the_same_queued_frame_once() {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let mut shell = InMemoryServerShell::start(InMemoryServerShellConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x75; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ))
        .unwrap();
        let (jobs, _pending_jobs) = mpsc::unbounded();
        let (activity_tx, _) = watch::channel(0_u64);
        let scheduler = ServerShellTickScheduler {
            jobs,
            activity_tx,
            io_wakers: Arc::new(Mutex::new(Vec::new())),
            state: Arc::new(ServerShellTickState::default()),
        };
        shell.set_tick_scheduler(Some(Rc::new(scheduler.clone())));
        let io = shell.connect_upstream_wire_io(
            crate::wire::WIRE_PROTOCOL_VERSION,
            crate::wire::FEATURE_NONE,
            None,
        );
        let expected = encode_message(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0x75; 16]),
            claims: BTreeMap::new(),
        });
        io.transport
            .queues
            .borrow_mut()
            .outbound
            .push_back(expected.clone());

        let state = Arc::new(Mutex::new(FirstSendBackpressureState::default()));
        let wire = FirstSendBackpressureWire {
            state: Arc::clone(&state),
        };
        let (wake_tx, wake_rx) = mpsc::unbounded();
        let (_cancel_tx, cancel_rx) = oneshot::channel();
        let mut executor = futures::executor::LocalPool::new();
        executor
            .spawner()
            .spawn_local(async move {
                let _ = drive_upstream_wire(
                    Box::new(wire),
                    Box::pin(futures::future::pending()),
                    io,
                    wake_rx,
                    scheduler,
                    cancel_rx,
                )
                .await;
            })
            .unwrap();

        executor.run_until_stalled();
        assert!(state.lock().unwrap().sent.is_empty());
        let connection_id = *shell
            .wire_upstream_connections
            .keys()
            .next()
            .expect("the original upstream owner remains attached");
        assert_eq!(
            shell.wire_upstream_connections.len(),
            1,
            "backpressure does not replace the live semantic connection"
        );

        wake_tx.unbounded_send(()).unwrap();
        executor.run_until_stalled();
        assert_eq!(
            state.lock().unwrap().sent,
            vec![expected],
            "the front-of-queue semantic frame is delivered exactly once after retry"
        );
        assert!(
            shell.wire_upstream_connections.contains_key(&connection_id),
            "the retry uses the original live transport owner"
        );
    }

    #[test]
    fn inbound_frame_phase_labels_semantic_transport_work() {
        let encoded = encode_message(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([7; 16]),
            claims: BTreeMap::new(),
        });

        assert_eq!(inbound_frame_phase(&encoded), "SessionClaims");
        let shape_id = ShapeId(uuid::Uuid::from_bytes([3; 16]));
        let subscribe = encode_message(SyncMessage::Subscribe(Subscribe {
            shape_id,
            subscription: SubscriptionKey {
                shape_id,
                binding_id: BindingId(uuid::Uuid::from_bytes([4; 16])),
                read_view: ReadViewKey::default(),
            },
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }));
        assert_eq!(inbound_frame_phase(&subscribe), "Subscribe");
        assert_eq!(inbound_frame_phase(&[0xff]), "malformed wire frame");
    }

    #[test]
    fn delayed_tick_keeps_the_native_shell_owner_live_and_does_not_replace_deferred_work() {
        let (jobs, mut receiver) = mpsc::unbounded();
        let (activity_tx, _) = watch::channel(0_u64);
        let io_wakers = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(ServerShellTickState::default());
        let scheduler = ServerShellTickScheduler {
            jobs,
            activity_tx,
            io_wakers,
            state: Arc::clone(&state),
        };

        scheduler.schedule_tick_after(20);
        assert!(
            receiver.next().now_or_never().is_none(),
            "an admission deadline must not enqueue an early shell tick"
        );

        // Ordinary deferred work remains immediately serviceable while the
        // separate timer waits. This is the native owner-liveness guarantee:
        // the owner queue never sleeps for an upload admission window.
        scheduler.schedule_tick(TickUrgency::Deferred);
        assert!(matches!(
            receiver.next().now_or_never().flatten(),
            Some(ServerShellCommand::RunAsync(_))
        ));
        state.queued.store(false, Ordering::Release);

        std::thread::sleep(Duration::from_millis(40));
        assert!(matches!(
            receiver.next().now_or_never().flatten(),
            Some(ServerShellCommand::RunAsync(_))
        ));
    }

    #[test]
    fn native_wire_input_rearms_after_an_earlier_empty_activity_tick() {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let mut shell = InMemoryServerShell::start(InMemoryServerShellConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x73; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ))
        .unwrap();
        let (jobs, mut pending_ticks) = mpsc::unbounded();
        let (activity_tx, _) = watch::channel(0_u64);
        let scheduler = ServerShellTickScheduler {
            jobs,
            activity_tx,
            io_wakers: Arc::new(Mutex::new(Vec::new())),
            state: Arc::new(ServerShellTickState::default()),
        };
        shell.set_tick_scheduler(Some(Rc::new(scheduler.clone())));

        let io = shell.connect_upstream_wire_io(
            crate::wire::WIRE_PROTOCOL_VERSION,
            crate::wire::FEATURE_NONE,
            None,
        );

        // Connecting the upstream may have queued setup work. Service all of
        // it first: the only outstanding turn below is the host activity tick
        // whose queue we deliberately make empty.
        while let Some(ServerShellCommand::RunAsync(setup_tick)) =
            pending_ticks.next().now_or_never().flatten()
        {
            futures::executor::block_on(setup_tick(&mut shell));
        }

        // A host activity notification can run before the local wire pump has
        // translated bytes into canonical input. Consume that tick while the
        // upstream queue is still empty.
        scheduler.schedule_tick(TickUrgency::Immediate);
        let Some(ServerShellCommand::RunAsync(empty_tick)) =
            pending_ticks.next().now_or_never().flatten()
        else {
            panic!("the pre-stage activity wake queues one shell tick");
        };
        futures::executor::block_on(empty_tick(&mut shell));

        let wire = QueuedWireTransport::default();
        wire.push_inbound(encode_message(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0x74; 16]),
            claims: BTreeMap::new(),
        }));
        let (_wake_tx, wake_rx) = mpsc::unbounded();
        let (_cancel_tx, cancel_rx) = oneshot::channel();
        let mut executor = futures::executor::LocalPool::new();
        executor
            .spawner()
            .spawn_local(async move {
                let _ = drive_upstream_wire(
                    Box::new(wire),
                    Box::pin(futures::future::pending()),
                    io,
                    wake_rx,
                    scheduler,
                    cancel_rx,
                )
                .await;
            })
            .unwrap();
        executor.run_until_stalled();

        assert!(
            matches!(
                pending_ticks.next().now_or_never().flatten(),
                Some(ServerShellCommand::RunAsync(_))
            ),
            "canonical input staged after an empty activity tick must queue a post-stage shell tick"
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_a_suspended_local_shell_operation() {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let runtime =
            ServerRuntimeHandle::start_with_storage(schema, StorageConfig::InMemory, None).unwrap();
        let suspended_runtime = runtime.clone();
        let suspended = tokio::spawn(async move {
            suspended_runtime
                .run_async(|_| Box::pin(futures::future::pending::<Result<(), String>>()))
                .await
        });
        tokio::task::yield_now().await;

        tokio::time::timeout(Duration::from_secs(1), runtime.shutdown())
            .await
            .expect("shutdown must not wait for suspended Groove work")
            .unwrap();
        assert!(suspended.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn concurrent_shutdown_callers_wait_for_the_owner_thread() {
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let runtime =
            ServerRuntimeHandle::start_with_storage(schema, StorageConfig::InMemory, None).unwrap();
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let blocked_runtime = runtime.clone();
        let blocked_entered = Arc::clone(&entered);
        let blocked_release = Arc::clone(&release);
        let blocked = tokio::spawn(async move {
            blocked_runtime
                .run(move |_| {
                    blocked_entered.wait();
                    blocked_release.wait();
                    Ok(())
                })
                .await
        });
        tokio::task::yield_now().await;
        entered.wait();

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.shutdown().await });
        loop {
            let jobs_retired = runtime.inner.jobs.lock().is_ok_and(|jobs| jobs.is_none());
            if jobs_retired {
                break;
            }
            tokio::task::yield_now().await;
        }

        let second_runtime = runtime.clone();
        let (second_waiting_tx, second_waiting_rx) = oneshot::channel();
        let second = tokio::task::spawn_blocking(move || {
            shutdown_blocking_with(
                &second_runtime.inner,
                perform_shutdown_blocking,
                move || {
                    let _ = second_waiting_tx.send(());
                },
            )
        });
        let second_reached_wait = tokio::time::timeout(Duration::from_secs(1), second_waiting_rx)
            .await
            .is_ok_and(|result| result.is_ok());
        let second_returned_early = second.is_finished();

        release.wait();
        blocked.await.unwrap().unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert!(
            second_reached_wait,
            "the concurrent shutdown caller must reach the wait path"
        );
        assert!(
            !second_returned_early,
            "a concurrent shutdown must not return before the owner thread exits"
        );
    }

    // This state-machine test is intentionally internal: a panic in the
    // shutdown implementation cannot be injected through the public API.
    #[tokio::test]
    async fn shutdown_panic_is_published_to_waiters_and_late_callers() {
        let (activity_tx, _) = watch::channel(0_u64);
        let inner = Arc::new(ServerShellInner {
            jobs: Mutex::new(None),
            join: Mutex::new(None),
            shutdown: Mutex::new(ShutdownState::Running),
            shutdown_changed: Condvar::new(),
            activity_tx,
            io_wakers: Arc::new(Mutex::new(Vec::new())),
        });
        let (perform_entered_tx, perform_entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let owner_inner = Arc::clone(&inner);
        let owner = tokio::task::spawn_blocking(move || {
            shutdown_blocking_with(
                &owner_inner,
                move |_| {
                    let _ = perform_entered_tx.send(());
                    release_rx.recv().unwrap();
                    panic!("planted shutdown panic")
                },
                || {},
            )
        });
        perform_entered_rx.await.unwrap();

        let (waiter_entered_tx, waiter_entered_rx) = oneshot::channel();
        let waiter_inner = Arc::clone(&inner);
        let waiter = tokio::task::spawn_blocking(move || {
            shutdown_blocking_with(&waiter_inner, perform_shutdown_blocking, move || {
                let _ = waiter_entered_tx.send(());
            })
        });
        tokio::time::timeout(Duration::from_secs(1), waiter_entered_rx)
            .await
            .expect("the waiter must enter shutdown")
            .expect("the waiter must reach the wait path");

        release_tx.send(()).unwrap();
        let owner_result = owner.await.unwrap();
        let waiter_result = waiter.await.unwrap();
        assert_eq!(owner_result, waiter_result);
        assert_eq!(
            owner_result.unwrap_err(),
            "server shell shutdown panicked: planted shutdown panic"
        );
        assert_eq!(shutdown_blocking(&inner), waiter_result);
    }
}
