//! The relay engine: one owner thread per durable scope.
//!
//! A `jazz-db` database is `Rc`/thread-affine, so every database of a scope
//! lives on that scope's owner thread: the persistent relay `Db` over its
//! SQLite file and one in-memory, non-durable `Db` per attached foreground.
//! Callers on other threads (the platform host, the socket worker) send it
//! closures and wait for their result.
//!
//! The topology is the browser worker's: each foreground connects upstream
//! to the relay through an ordinary sync link and the relay serves it as a
//! subscriber; the relay itself connects upstream to Core when a socket
//! worker installs a connection. Both halves use [`jazz::binding_host`], the
//! layer NAPI and WASM share, so a foreground command means exactly what the
//! same `NativeDb` call means there.
//!
//! The owner drives all of it: after every command, and whenever frames
//! arrive, it ticks the databases that asked for a tick and moves frames
//! between them until nothing moves. It buffers each subscription's events
//! as they are produced, so it knows when a foreground has something new and
//! wakes it through the platform callback.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use jazz::binding_host::{self, Binding, EventChunk, HostError, Link};
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, LargeValueUpdate, Migrations, RemoteLinkHint,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::link::{
    AuthFailureReason, LinkAction, LinkEvent, LinkFailure, LinkInput, LinkPolicy, UpstreamLink,
};
use jazz::protocol_limits::validate_logical_message_len;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportError, NativeTransportLink, NativeTransportRequest,
    NativeTransportTerminal,
};
use jazz::tools::transaction::TransactionId;
use jazz::tools::websocket_prelude_auth::AuthConfig;
use jazz::tx::DurabilityTier;
use jazz::wire::{TransportError, WireTransport};
use jazz_native_transport::NativeWebSocketConnector;
use thiserror::Error;

use crate::node_lease::ForegroundNodeLease;
use crate::{
    FOREGROUND_WAKE_IMMEDIATE, ForegroundDbCommandRequest, ForegroundDbCommandResponse,
    ForegroundMutationKind, ForegroundPermissionAdvice, ForegroundPermissionAdviceAction,
    ForegroundSubscriptionEvent, ForegroundTransactionKind, ForegroundWakeState,
    NATIVE_RELAY_ABI_V1, NativeRelayAbiRange, ensure_native_relay_abi_compatible,
};

/// Frames one host-carried queue holds before a producer sees backpressure.
const NATIVE_RELAY_QUEUE_MAX_MESSAGES: usize = 1024;
/// Commands which cross from a platform call into a relay owner must not
/// accumulate without bound.
const NATIVE_RELAY_OWNER_COMMAND_MAX: usize = 1_024;
/// Rounds of one drive before the owner yields to queued commands. A drive
/// that hits it continues on the next owner turn.
const NATIVE_RELAY_DRIVE_ROUNDS: usize = 64;
/// How often the owner re-checks time-bound work (a subscription waiting for
/// its first answer, a read waiting for the server) while some is pending.
const NATIVE_RELAY_POLL_INTERVAL: Duration = Duration::from_millis(25);
/// Bounds on the per-foreground handles a JavaScript runtime may hold open.
const NATIVE_RELAY_FOREGROUND_PENDING_MAX: usize = 64;
const NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX: usize = 64;
/// Committed writes a foreground remembers for later waits by transaction id.
const NATIVE_RELAY_FOREGROUND_WRITES_MAX: usize = 16 * 1024;

// ---- scope and configuration --------------------------------------------------------

/// Explicit process-local persistence/synchronization scope.
///
/// Authentication material is intentionally absent. `auth_scope` is an opaque,
/// required stable subject/tenant discriminator supplied by the host after
/// validation; tokens are sent to an upstream connection, never used as storage
/// names. The `Option` preserves the strict JSON boundary's ability to reject
/// an omitted or `null` field rather than silently manufacturing an anonymous
/// persistent scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelayScope {
    pub app_namespace: String,
    pub storage_namespace: String,
    pub auth_scope: Option<String>,
}

impl RelayScope {
    pub fn validate(&self) -> Result<(), RelayError> {
        for (field, value) in [
            ("app namespace", self.app_namespace.as_str()),
            ("storage namespace", self.storage_namespace.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(RelayError::InvalidScope(format!(
                    "{field} must not be empty"
                )));
            }
        }
        match self.auth_scope.as_deref() {
            Some(auth_scope) if !auth_scope.trim().is_empty() => Ok(()),
            Some(_) => Err(RelayError::InvalidScope(
                "auth scope must not be empty".to_owned(),
            )),
            None => Err(RelayError::InvalidScope(
                "auth scope is required for a persistent relay".to_owned(),
            )),
        }
    }
}

pub(crate) type MigrationsKey = (
    Vec<jazz::ids::SchemaVersionId>,
    Vec<jazz::ids::MigrationLensId>,
);

/// An upstream the relay's database refused on its own (see
/// [`Link::upstream_refusal`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UpstreamRefusal {
    /// The core's stable error code (`ErrorCode::as_str`).
    pub code: String,
    /// The reason the relay reported the refusal with.
    pub reason: String,
}

/// Immutable configuration used to start a relay owner thread.
#[derive(Clone, Debug)]
pub struct RelayOpenConfig {
    /// Inclusive ABI range implemented by the calling platform wrapper. This
    /// is mandatory: opening a scope must reject an OTA/native mismatch before
    /// allocating an owner thread or touching SQLite.
    pub supported_abi: NativeRelayAbiRange,
    pub scope: RelayScope,
    /// Exact owned path chosen by the platform wrapper. The native relay never
    /// interpolates auth tokens or untrusted strings into a filesystem path.
    pub sqlite_path: PathBuf,
    pub schema: JazzSchema,
    /// The app's earlier schema versions and the lenses between them, as it
    /// ships them: the store keeps each column where the authority does.
    pub migrations: Migrations,
    pub identity: DbIdentity,
}

impl RelayOpenConfig {
    /// What identifies this config's schema history: its earlier versions
    /// and its content-addressed lenses, in the order shipped. A scope
    /// reopened with another history is another configuration.
    pub(crate) fn migrations_key(&self) -> MigrationsKey {
        (
            self.migrations
                .schemas
                .iter()
                .map(|schema| schema.version_id())
                .collect(),
            self.migrations.lenses.iter().map(|lens| lens.id).collect(),
        )
    }

    fn validate(&self) -> Result<(), RelayError> {
        ensure_native_relay_abi_compatible(self.supported_abi)?;
        self.scope.validate()
    }
}

// ---- host-carried frame queues ------------------------------------------------------

#[derive(Default)]
struct FrameQueue {
    frames: VecDeque<Vec<u8>>,
}

/// Thread-safe frame queues between the relay's upstream link and whoever
/// carries it: the socket worker, or a host driving frames itself through
/// `SendRelayFrame`/`ReceiveRelayFrames`. Each frame is one postcard
/// `SyncMessage`, exactly as the socket carries it.
#[derive(Clone, Default)]
pub struct NativeRelayWire {
    /// Frames from the server, applied on the owner's next drive.
    inbound: Arc<Mutex<FrameQueue>>,
    /// Frames for the server.
    outbound: Arc<Mutex<FrameQueue>>,
    /// Signalled when outbound frames are queued.
    outbound_ready: Arc<tokio::sync::Notify>,
    /// Set when the owner retired this connection: no more frames cross it.
    retired: Arc<AtomicBool>,
    /// Why the relay's database dropped this connection on its own (see
    /// [`Link::upstream_refusal`]): whoever carries it stops instead of
    /// keeping a socket nothing syncs over, and reports why.
    refusal: Arc<Mutex<Option<String>>>,
}

impl NativeRelayWire {
    fn lock(queue: &Mutex<FrameQueue>) -> std::sync::MutexGuard<'_, FrameQueue> {
        queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Admit one frame from the server.
    pub fn push_inbound_encoded(&self, frame: &[u8]) -> Result<(), RelayError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(RelayError::Closed);
        }
        validate_logical_message_len(frame.len()).map_err(RelayError::PeerMessageTooLarge)?;
        let mut inbound = Self::lock(&self.inbound);
        if inbound.frames.len() >= NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            return Err(RelayError::QueueCapacityExceeded {
                direction: "inbound",
                queued_messages: inbound.frames.len(),
            });
        }
        inbound.frames.push_back(frame.to_vec());
        Ok(())
    }

    /// Whether another inbound frame would be admitted now.
    fn inbound_has_room(&self) -> bool {
        Self::lock(&self.inbound).frames.len() < NATIVE_RELAY_QUEUE_MAX_MESSAGES
    }

    /// Drain the frames queued for the server.
    pub fn take_outbound_encoded(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(RelayError::Closed);
        }
        Ok(Self::lock(&self.outbound).frames.drain(..).collect())
    }

    /// Queued (inbound, outbound) frames.
    pub fn queue_depths(&self) -> (usize, usize) {
        (
            Self::lock(&self.inbound).frames.len(),
            Self::lock(&self.outbound).frames.len(),
        )
    }

    fn take_inbound(&self) -> Vec<Vec<u8>> {
        Self::lock(&self.inbound).frames.drain(..).collect()
    }

    fn push_outbound(&self, frames: Vec<Vec<u8>>) {
        if frames.is_empty() || self.retired.load(Ordering::Acquire) {
            return;
        }
        Self::lock(&self.outbound).frames.extend(frames);
        self.outbound_ready.notify_one();
    }

    /// Return unsent frames to the head of the outbound queue, in order.
    fn restore_outbound(&self, frames: Vec<Vec<u8>>) {
        let mut outbound = Self::lock(&self.outbound);
        for frame in frames.into_iter().rev() {
            outbound.frames.push_front(frame);
        }
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.outbound_ready.notify_one();
    }

    /// Retire the connection because the relay's database refused its
    /// upstream, recording why first so the carrier reads it.
    pub(crate) fn refuse(&self, reason: String) {
        *self
            .refusal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reason);
        self.retire();
    }

    /// Why the relay refused this connection, once it has.
    pub fn refusal(&self) -> Option<String> {
        self.refusal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

// ---- the relay handle ---------------------------------------------------------------

type RelayJob = Box<dyn FnOnce(&mut RelayWorker) + Send + 'static>;

enum RelayCommand {
    Run(RelayJob),
    Shutdown(mpsc::Sender<()>),
}

/// Thread-safe handle to one scope's relay owner.
#[derive(Clone)]
pub struct NativeRelay {
    inner: Arc<RelayInner>,
}

struct RelayInner {
    commands: Mutex<Option<mpsc::SyncSender<RelayCommand>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    alive: Arc<AtomicBool>,
    sqlite_path: PathBuf,
    schema_version: jazz::ids::SchemaVersionId,
    migrations: MigrationsKey,
    identity: DbIdentity,
}

impl RelayInner {
    fn shutdown(&self) {
        self.alive.store(false, Ordering::Release);
        let sender = self
            .commands
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(sender) = sender {
            let (done_tx, done_rx) = mpsc::channel();
            if sender.send(RelayCommand::Shutdown(done_tx)).is_ok() {
                let _ = done_rx.recv();
            }
        }
        let join = self
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

impl Drop for RelayInner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Marks the relay terminal when its owner thread ends, however it ends.
struct OwnerAlive(Arc<AtomicBool>);

impl Drop for OwnerAlive {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl NativeRelay {
    /// Open the scope's SQLite store on a new owner thread.
    pub fn spawn(config: RelayOpenConfig) -> Result<Self, RelayError> {
        // Before the thread exists and before SQLite is touched.
        config.validate()?;
        let sqlite_path = config.sqlite_path.clone();
        let schema_version = config.schema.version_id();
        let migrations = config.migrations_key();
        let identity = config.identity;
        let alive = Arc::new(AtomicBool::new(true));
        let (commands, receiver) =
            mpsc::sync_channel::<RelayCommand>(NATIVE_RELAY_OWNER_COMMAND_MAX);
        let (started_tx, started_rx) = mpsc::channel();
        let owner_alive = Arc::clone(&alive);
        let join = thread::Builder::new()
            .name("jazz-native-relay".to_owned())
            .spawn(move || {
                let _alive = OwnerAlive(owner_alive);
                let mut worker = match RelayWorker::open(config) {
                    Ok(worker) => {
                        let _ = started_tx.send(Ok(()));
                        worker
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                        return;
                    }
                };
                worker.run(receiver);
            })
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        started_rx.recv().map_err(|_| {
            RelayError::OwnerThread("owner exited before opening relay".to_owned())
        })??;
        Ok(Self {
            inner: Arc::new(RelayInner {
                commands: Mutex::new(Some(commands)),
                join: Mutex::new(Some(join)),
                alive,
                sqlite_path,
                schema_version,
                migrations,
                identity,
            }),
        })
    }

    pub fn abi_version(&self) -> u16 {
        NATIVE_RELAY_ABI_V1
    }

    /// Verify that a host wrapper understands this embedded native relay
    /// before opening a scope.
    pub fn ensure_abi_compatible(wrapper_range: NativeRelayAbiRange) -> Result<u16, RelayError> {
        ensure_native_relay_abi_compatible(wrapper_range)
    }

    fn is_alive(&self) -> bool {
        self.inner.alive.load(Ordering::Acquire)
    }

    /// Attach a generic in-memory UI peer with a fresh identity.
    pub fn attach_client(
        &self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
    ) -> Result<NativeRelayClient, RelayError> {
        let id = self.run(move |worker| worker.attach(identity, claims, 0))?;
        Ok(NativeRelayClient {
            relay: self.clone(),
            id,
        })
    }

    /// Attach a foreground on a leased node, continuing after the lease's
    /// previous holder.
    pub(crate) fn attach_foreground_client(
        &self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
        lease: ForegroundNodeLease,
    ) -> Result<NativeRelayClient, RelayError> {
        if identity.author != self.inner.identity.author {
            return Err(RelayError::ScopeConfigurationMismatch);
        }
        let id = self.run(move |worker| worker.attach(identity, claims, lease.high_water))?;
        Ok(NativeRelayClient {
            relay: self.clone(),
            id,
        })
    }

    /// Run one drive turn now: apply arrived frames, tick, move frames.
    pub fn pump(&self) -> Result<(), RelayError> {
        self.run(|worker| {
            worker.drive();
            Ok(())
        })
    }

    /// Frames between the relay and its host-carried upstream. Connecting it
    /// is the host's choice: a relay with a socket worker never uses it.
    pub fn wire(&self) -> Result<NativeRelayWire, RelayError> {
        self.run(|worker| Ok(worker.host_upstream()))
    }

    /// Writes the server has not confirmed. A foreground's pending write is
    /// the relay store's too once handed to it, so this is the larger of the
    /// store's count and any foreground's, not their sum.
    pub(crate) fn unconfirmed_writes(&self) -> Result<usize, RelayError> {
        self.run(|worker| {
            Ok(worker
                .clients
                .values()
                .map(|client| client.binding.pending_upload_count())
                .fold(worker.persistent.pending_upload_count(), usize::max))
        })
    }

    /// The last upstream the relay's database refused, if the connection
    /// since has not been replaced.
    pub(crate) fn upstream_refusal(&self) -> Result<Option<UpstreamRefusal>, RelayError> {
        self.run(|worker| Ok(worker.refusal.clone()))
    }

    /// Diagnostics: attached peers and host-carried upstream queue depths.
    pub(crate) fn diagnostics(&self) -> Result<(u64, usize, usize), RelayError> {
        self.run(|worker| {
            let (inbound, outbound) = worker
                .upstream
                .as_ref()
                .map(|upstream| upstream.wire.queue_depths())
                .unwrap_or_default();
            Ok((worker.clients.len() as u64, inbound, outbound))
        })
    }

    /// Bind new claims to the scope's author on the relay and every
    /// foreground.
    pub(crate) fn refresh_account_claims(
        &self,
        author: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Result<(), RelayError> {
        self.run(move |worker| {
            worker.refresh_claims(author, claims);
            Ok(())
        })
    }

    /// Report the path to the authoritative server to the relay database.
    pub(crate) fn set_remote_link_hint(&self, hint: RemoteLinkHint) -> Result<(), RelayError> {
        self.run(move |worker| {
            worker.remote_link_hint = Some(hint);
            worker.persistent.db.set_remote_link_hint(hint);
            worker.persistent.hooks.progress.wake_all();
            worker.persistent_dirty.set(true);
            Ok(())
        })
    }

    fn begin_socket_upstream(&self) -> Result<(u64, NativeRelayWire), RelayError> {
        self.run(|worker| Ok(worker.begin_socket_upstream()))
    }

    fn retire_socket_upstream(&self, generation: u64) -> Result<(), RelayError> {
        self.run(move |worker| {
            worker.retire_socket_upstream(generation);
            Ok(())
        })
    }

    /// Run `operation` on the owner thread and wait for its result.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        if !self.is_alive() {
            return Err(RelayError::Closed);
        }
        let (response_tx, response_rx) = mpsc::channel();
        let job: RelayJob = Box::new(move |worker| {
            let _ = response_tx.send(operation(worker));
        });
        {
            let commands = self
                .inner
                .commands
                .lock()
                .map_err(|_| RelayError::Poisoned("relay command queue"))?;
            commands
                .as_ref()
                .ok_or(RelayError::Closed)?
                .try_send(RelayCommand::Run(job))
                .map_err(|error| match error {
                    mpsc::TrySendError::Full(_) => RelayError::OwnerQueueFull,
                    mpsc::TrySendError::Disconnected(_) => RelayError::Closed,
                })?;
        }
        response_rx.recv().map_err(|_| {
            self.inner.alive.store(false, Ordering::Release);
            RelayError::Closed
        })?
    }
}

/// Stable handle for one process-local UI peer.
#[derive(Clone)]
pub struct NativeRelayClient {
    relay: NativeRelay,
    id: u64,
}

impl NativeRelayClient {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Run one ordinary core turn for this foreground and its relay. A
    /// failure of an earlier drive is reported once.
    pub(crate) fn pump_foreground(&self) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.client(id)?;
            worker.drive();
            match worker.drive_error.take() {
                Some(error) => Err(RelayError::Drive(error)),
                None => Ok(()),
            }
        })
    }

    /// Execute one foreground command on the owner thread.
    pub(crate) fn execute(
        &self,
        command: ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.execute_foreground(id, command))
    }

    pub(crate) fn set_foreground_wake_callback(
        &self,
        foreground: u64,
        wake: Option<Arc<ForegroundWakeState>>,
    ) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            let client = worker.client_mut(id)?;
            client.wake = wake.map(|wake| (foreground, wake));
            Ok(())
        })
    }

    /// The highest commit seq this foreground's database handed out; the
    /// lease's next holder starts above it.
    pub(crate) fn minted_high_water(&self) -> Result<u64, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| Ok(worker.client(id)?.binding.db.seq_high_water()))
    }

    /// Run an operation against this UI peer's database on the owner thread.
    pub fn with_db<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Db) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| operation(&worker.client(id)?.binding.db))
    }

    /// Give frames to this peer as if its relay had sent them.
    pub(crate) fn push_frame(&self, frame: Vec<u8>) -> Result<(), RelayError> {
        validate_logical_message_len(frame.len()).map_err(RelayError::PeerMessageTooLarge)?;
        let id = self.id;
        self.relay.run(move |worker| {
            worker.client(id)?.link.receive([frame]);
            Ok(())
        })
    }

    /// Take the frames this peer is sending to its relay.
    pub(crate) fn take_frames(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| Ok(worker.client(id)?.link.take_outbound()))
    }

    pub fn close(self) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| worker.detach(id))
    }
}

// ---- the owner ------------------------------------------------------------------------

/// The connection the relay's upstream link currently runs over.
struct UpstreamConnection {
    generation: u64,
    link: Link,
    wire: NativeRelayWire,
}

type PendingFuture = Pin<Box<dyn Future<Output = Result<ForegroundDbCommandResponse, HostError>>>>;

/// A foreground operation waiting for the server or the relay.
struct PendingOperation {
    future: PendingFuture,
    /// The permission-advice request to cancel with it.
    advice: Option<u64>,
}

struct ForegroundSubscription {
    stream: SubscriptionStream,
    /// Events produced but not yet drained, already encoded.
    buffer: VecDeque<ForegroundSubscriptionEvent>,
    /// Its first event was produced: nothing time-bound is left to poll.
    opened: bool,
    closed: bool,
}

/// One committed write a later wait may name by its public id.
struct TrackedWrite {
    write: binding_host::Write,
    /// A wait took over reporting the write's rejection.
    claimed: Rc<Cell<bool>>,
}

/// A rejected write no wait has claimed (yet).
struct MutationErrorReport {
    claimed: Option<Rc<Cell<bool>>>,
    event: serde_json::Value,
}

/// One attached UI peer: its in-memory database and both ends of its link.
struct Foreground {
    binding: Binding,
    /// The relay's persistent database. Permission advice is asked of it:
    /// it is the replica linked to the authority, for the same author, and
    /// a relay answers its own subscribers' advice requests `Unknown`.
    authority: Binding,
    dirty: Rc<Cell<bool>>,
    /// The foreground's upstream link to the relay.
    link: Link,
    /// The relay's subscriber link serving it.
    served: Link,
    subscriptions: BTreeMap<u64, ForegroundSubscription>,
    pending: BTreeMap<u64, PendingOperation>,
    /// Results of pending operations that completed before being polled.
    completed: BTreeMap<u64, ForegroundDbCommandResponse>,
    transactions: BTreeMap<u64, (String, ForegroundTransactionKind)>,
    writes: HashMap<TransactionId, TrackedWrite>,
    write_order: VecDeque<TransactionId>,
    uploads: BTreeMap<u64, binding_host::StreamingMutation>,
    mutation_errors: Rc<RefCell<Vec<MutationErrorReport>>>,
    /// The platform callback and the host handle it is called with.
    wake: Option<(u64, Arc<ForegroundWakeState>)>,
    /// Something changed for this foreground since the drive began.
    notify: bool,
    next_handle: u64,
}

impl Foreground {
    fn handle(&mut self) -> u64 {
        self.next_handle += 1;
        self.next_handle
    }

    /// Remember a committed write for later waits; a rejected one is
    /// reported unless a wait claims it first.
    fn track(&mut self, write: binding_host::Write) -> (TransactionId, RowUuid) {
        let claimed = Rc::new(Cell::new(false));
        if let Some(error) = &write.rejected {
            self.mutation_errors.borrow_mut().push(MutationErrorReport {
                claimed: Some(Rc::clone(&claimed)),
                event: mutation_error_event(
                    &write.tx_id.to_string(),
                    error,
                    write.serializable,
                    false,
                ),
            });
            self.notify = true;
        }
        self.binding.hooks.track(&write, &claimed);
        let ids = (write.tx_id, write.row_id);
        if self
            .writes
            .insert(write.tx_id, TrackedWrite { write, claimed })
            .is_none()
        {
            self.write_order.push_back(ids.0);
        }
        while self.write_order.len() > NATIVE_RELAY_FOREGROUND_WRITES_MAX {
            if let Some(oldest) = self.write_order.pop_front() {
                self.writes.remove(&oldest);
            }
        }
        ids
    }

    fn transaction(&self, handle: u64) -> Result<&(String, ForegroundTransactionKind), HostError> {
        self.transactions
            .get(&handle)
            .ok_or_else(|| HostError::Argument(format!("unknown foreground transaction {handle}")))
    }

    /// Start or finish a pollable operation.
    fn pending_or_ready(
        &mut self,
        future: PendingFuture,
        advice: Option<u64>,
    ) -> Result<ForegroundDbCommandResponse, HostError> {
        let mut operation = PendingOperation { future, advice };
        match poll_once(&mut operation.future) {
            Poll::Ready(result) => result,
            Poll::Pending => {
                if self.pending.len() >= NATIVE_RELAY_FOREGROUND_PENDING_MAX {
                    if let Some(id) = operation.advice {
                        self.authority.cancel_permission_advice(id);
                    }
                    return Err(HostError::Argument(
                        "foreground operation capacity reached; poll or cancel one first"
                            .to_owned(),
                    ));
                }
                let handle = self.handle();
                self.pending.insert(handle, operation);
                Ok(ForegroundDbCommandResponse::Pending { operation: handle })
            }
        }
    }

    /// Poll every pending operation; keep the ones that completed.
    fn poll_pending(&mut self) {
        let ready = self
            .pending
            .iter_mut()
            .filter_map(
                |(handle, operation)| match poll_once(&mut operation.future) {
                    Poll::Ready(result) => Some((*handle, result)),
                    Poll::Pending => None,
                },
            )
            .collect::<Vec<_>>();
        for (handle, result) in ready {
            self.pending.remove(&handle);
            self.completed.insert(handle, operation_response(result));
            self.notify = true;
        }
    }

    /// Buffer every event the subscriptions produced.
    fn collect_events(&mut self) -> Result<(), String> {
        for subscription in self.subscriptions.values_mut() {
            while !subscription.closed
                && let Some(event) = subscription.stream.try_next_event()
            {
                subscription.opened = true;
                let event = match binding_host::event_chunk(event).map_err(|e| e.to_string())? {
                    EventChunk::Delta {
                        delta,
                        reset,
                        settled,
                        tier,
                    } => ForegroundSubscriptionEvent::Delta {
                        reset,
                        settled,
                        tier,
                        delta,
                    },
                    EventChunk::Rejected { detail, .. } => {
                        ForegroundSubscriptionEvent::Rejected { reason: detail }
                    }
                    EventChunk::Closed => {
                        subscription.closed = true;
                        ForegroundSubscriptionEvent::Closed
                    }
                };
                subscription.buffer.push_back(event);
                self.notify = true;
            }
        }
        Ok(())
    }

    /// Whether time-bound work is pending that a drive must re-check.
    fn awaits_time(&self) -> bool {
        !self.pending.is_empty() || self.subscriptions.values().any(|s| !s.opened)
    }

    fn wake(&mut self) {
        if !std::mem::take(&mut self.notify) {
            return;
        }
        if let Some((handle, wake)) = &self.wake {
            wake.wake(*handle, FOREGROUND_WAKE_IMMEDIATE, 0);
        }
    }

    fn close(mut self) {
        for (_, operation) in std::mem::take(&mut self.pending) {
            if let Some(id) = operation.advice {
                self.authority.cancel_permission_advice(id);
            }
        }
        for (_, (id, _)) in std::mem::take(&mut self.transactions) {
            let _ = self.binding.rollback_transaction(&id);
        }
        for (_, mut upload) in std::mem::take(&mut self.uploads) {
            let _ = upload.abort();
        }
        self.subscriptions.clear();
        self.link.close();
        self.served.close();
        let _ = block_on(self.binding.close());
    }
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

pub(crate) struct RelayWorker {
    schema: JazzSchema,
    migrations: Migrations,
    persistent: Binding,
    persistent_dirty: Rc<Cell<bool>>,
    upstream: Option<UpstreamConnection>,
    generation: u64,
    clients: BTreeMap<u64, Foreground>,
    next_client: u64,
    /// The last drive failed; reported by the next foreground tick.
    drive_error: Option<String>,
    /// The last upstream the relay's database refused: the core's stable
    /// error code and the reason reported with it. Cleared when a new
    /// upstream connection begins.
    refusal: Option<UpstreamRefusal>,
    /// Where the persistent store lives and whom it is, to rebuild it.
    sqlite_path: PathBuf,
    identity: DbIdentity,
    /// The scope's latest claims and remote-link hint, given again to a
    /// rebuilt store.
    claims: Option<BTreeMap<String, Value>>,
    remote_link_hint: Option<RemoteLinkHint>,
    /// The store was rebuilt once in this process (see
    /// [`RelayWorker::rebuild_stale_store`]): never again, so a bundle that
    /// disagrees with its server cannot rebuild forever.
    rebuilt: bool,
    /// The last drive stopped at its round bound with work left.
    redrive: bool,
    /// When time-bound waits were last evaluated.
    timed: Instant,
}

/// A binding's tick requests set `dirty`; the owner's drive ticks it.
fn hook(binding: &Binding) -> Rc<Cell<bool>> {
    let dirty = Rc::new(Cell::new(true));
    let flag = Rc::clone(&dirty);
    *binding.hooks.schedule_tick.borrow_mut() = Some(Box::new(move || flag.set(true)));
    dirty
}

/// Refuse a file a pre-SQLite native relay wrote: its ordered-KV tables
/// would otherwise be taken for an unknown Jazz schema.
fn ensure_sqlite_store(path: &std::path::Path) -> Result<(), RelayError> {
    if !path.exists() {
        return Ok(());
    }
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| RelayError::Storage(error.to_string()))?;
    let legacy: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('kv', 'column_families')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| RelayError::Storage(error.to_string()))?;
    if legacy == 2 {
        return Err(RelayError::UnsupportedStorage);
    }
    Ok(())
}

/// A relay store's config, for its persistent store and each session's.
/// Like the client bindings' ([`binding_host::db_config`]), it has no chunk
/// store, so a tick never runs chunk-store I/O (whose failures a later tick
/// retries) and every tick error is treated as terminal by the JavaScript
/// runtime (`core_tick_failed`). Adding a chunk store here means first
/// giving hosts a way to tell those failures apart.
/// Beside a store: the schema version it was last rebuilt for (#5145).
fn rebuilt_marker(sqlite_path: &std::path::Path) -> PathBuf {
    let mut path = sqlite_path.as_os_str().to_owned();
    path.push(".rebuilt-for");
    PathBuf::from(path)
}

/// Delete a store's SQLite files: its `-wal` and `-shm` first, so a crash
/// part-way never leaves a write-ahead log beside no main file, or one for a
/// newer file. Missing files are fine.
pub(crate) fn delete_store_files(sqlite_path: &std::path::Path) -> std::io::Result<()> {
    for suffix in ["-wal", "-shm", ""] {
        let mut path = sqlite_path.as_os_str().to_owned();
        path.push(suffix);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Forget that a store was rebuilt: a store deleted by the app starts over.
pub(crate) fn forget_rebuilt_marker(sqlite_path: &std::path::Path) {
    let _ = std::fs::remove_file(rebuilt_marker(sqlite_path));
}

fn relay_db_config(schema: JazzSchema, identity: DbIdentity, migrations: &Migrations) -> DbConfig {
    DbConfig::new(schema, identity)
        .with_migrations(migrations.schemas.clone(), migrations.lenses.clone())
}

impl RelayWorker {
    fn open(config: RelayOpenConfig) -> Result<Self, RelayError> {
        ensure_sqlite_store(&config.sqlite_path)?;
        if let Some(parent) = config.sqlite_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| RelayError::Storage(error.to_string()))?;
        }
        let db = block_on(Db::open_at(
            relay_db_config(config.schema.clone(), config.identity, &config.migrations),
            &config.sqlite_path,
            FileOptions::default(),
        ))
        .map_err(|error| RelayError::Host(error.into()))?;
        let persistent = Binding::new(db, config.identity, false);
        let persistent_dirty = hook(&persistent);
        Ok(Self {
            schema: config.schema,
            migrations: config.migrations,
            persistent,
            persistent_dirty,
            upstream: None,
            generation: 0,
            clients: BTreeMap::new(),
            next_client: 0,
            drive_error: None,
            refusal: None,
            sqlite_path: config.sqlite_path,
            identity: config.identity,
            claims: None,
            remote_link_hint: None,
            rebuilt: false,
            redrive: false,
            timed: Instant::now(),
        })
    }

    fn run(&mut self, receiver: mpsc::Receiver<RelayCommand>) {
        loop {
            let command = if self.redrive {
                match receiver.try_recv() {
                    Ok(command) => Some(command),
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            } else if self.awaits_time() {
                match receiver.recv_timeout(NATIVE_RELAY_POLL_INTERVAL) {
                    Ok(command) => Some(command),
                    Err(mpsc::RecvTimeoutError::Timeout) => None,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            } else {
                match receiver.recv() {
                    Ok(command) => Some(command),
                    Err(_) => break,
                }
            };
            match command {
                Some(RelayCommand::Run(job)) => job(self),
                Some(RelayCommand::Shutdown(done)) => {
                    self.shutdown();
                    let _ = done.send(());
                    return;
                }
                None => {}
            }
            // Time-bound waits are evaluated on tick, at least once a poll
            // interval: also while commands keep arriving (a foreground
            // polling its pending reads).
            if self.awaits_time() && self.timed.elapsed() >= NATIVE_RELAY_POLL_INTERVAL {
                self.timed = Instant::now();
                for client in self.clients.values() {
                    if client.awaits_time() {
                        client.dirty.set(true);
                    }
                }
                self.persistent_dirty.set(true);
            }
            self.drive();
        }
        self.shutdown();
    }

    fn awaits_time(&self) -> bool {
        self.clients.values().any(Foreground::awaits_time)
    }

    fn shutdown(&mut self) {
        for (_, client) in std::mem::take(&mut self.clients) {
            client.close();
        }
        if let Some(mut upstream) = self.upstream.take() {
            upstream.link.close();
            upstream.wire.retire();
        }
        let _ = block_on(self.persistent.close());
    }

    fn client(&self, id: u64) -> Result<&Foreground, RelayError> {
        self.clients.get(&id).ok_or(RelayError::UnknownClient(id))
    }

    fn client_mut(&mut self, id: u64) -> Result<&mut Foreground, RelayError> {
        self.clients
            .get_mut(&id)
            .ok_or(RelayError::UnknownClient(id))
    }

    fn attach(
        &mut self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
        high_water: u64,
    ) -> Result<u64, RelayError> {
        // A relay serves its own identity's sessions only: it uploads what
        // they write as itself.
        self.persistent
            .db
            .check_serves(identity.author)
            .map_err(|error| RelayError::Host(error.into()))?;
        let db = block_on(Db::open(relay_db_config(
            self.schema.clone(),
            identity,
            &self.migrations,
        )))
        .map_err(|error| RelayError::Host(error.into()))?;
        // Continue after the node's previous holder: its transaction ids
        // were (seq, node), and the relay deduplicates by them.
        if high_water > 0 {
            db.seed_seq_floor(high_water);
        }
        db.set_identity_claims(identity.author, claims.clone());
        let binding = Binding::new(db, identity, false);
        binding.set_non_durable_client();
        let dirty = hook(&binding);
        let mutation_errors = Rc::new(RefCell::new(Vec::new()));
        let reports = Rc::clone(&mutation_errors);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, claimed| {
                reports.borrow_mut().push(MutationErrorReport {
                    claimed,
                    event: mutation_error_event(&tx_id.to_string(), error, serializable, true),
                });
                true
            }));
        self.persistent
            .db
            .set_identity_claims(identity.author, claims.clone());
        self.claims = Some(claims.clone());
        let served = self
            .persistent
            .accept_subscriber(identity.author, claims)
            .map_err(RelayError::Host)?;
        let link = binding.connect_upstream();
        self.next_client += 1;
        let id = self.next_client;
        self.clients.insert(
            id,
            Foreground {
                binding,
                authority: self.persistent.clone(),
                dirty,
                link,
                served,
                subscriptions: BTreeMap::new(),
                pending: BTreeMap::new(),
                completed: BTreeMap::new(),
                transactions: BTreeMap::new(),
                writes: HashMap::new(),
                write_order: VecDeque::new(),
                uploads: BTreeMap::new(),
                mutation_errors,
                wake: None,
                notify: false,
                next_handle: 0,
            },
        );
        self.drive();
        Ok(id)
    }

    fn detach(&mut self, id: u64) -> Result<(), RelayError> {
        let client = self
            .clients
            .remove(&id)
            .ok_or(RelayError::UnknownClient(id))?;
        client.close();
        self.persistent_dirty.set(true);
        self.drive();
        Ok(())
    }

    fn refresh_claims(&mut self, author: AuthorSubject, claims: BTreeMap<String, Value>) {
        self.claims = Some(claims.clone());
        self.persistent
            .db
            .set_identity_claims(author, claims.clone());
        self.persistent.hooks.progress.wake_all();
        self.persistent_dirty.set(true);
        for client in self.clients.values() {
            client
                .binding
                .db
                .set_identity_claims(author, claims.clone());
            client.binding.hooks.progress.wake_all();
            client.dirty.set(true);
        }
    }

    /// Connect the relay's upstream through a fresh host-carried wire,
    /// replacing any earlier connection.
    fn begin_socket_upstream(&mut self) -> (u64, NativeRelayWire) {
        self.refusal = None;
        if let Some(mut previous) = self.upstream.take() {
            previous.link.close();
            previous.wire.retire();
        }
        self.generation += 1;
        let link = self.persistent.connect_upstream();
        let wire = NativeRelayWire::default();
        self.upstream = Some(UpstreamConnection {
            generation: self.generation,
            link,
            wire: wire.clone(),
        });
        self.persistent_dirty.set(true);
        (self.generation, wire)
    }

    /// Detach the upstream connection `generation`, unless a newer one
    /// replaced it already.
    fn retire_socket_upstream(&mut self, generation: u64) {
        if self
            .upstream
            .as_ref()
            .is_some_and(|upstream| upstream.generation == generation)
            && let Some(mut upstream) = self.upstream.take()
        {
            upstream.link.close();
            upstream.wire.retire();
            self.persistent_dirty.set(true);
        }
    }

    /// The current upstream wire, connecting one the host carries if there
    /// is none.
    fn host_upstream(&mut self) -> NativeRelayWire {
        match &self.upstream {
            Some(upstream) => upstream.wire.clone(),
            None => self.begin_socket_upstream().1,
        }
    }

    /// Stop the current upstream connection because the relay's database
    /// refused it, recording the refusal's code and reason for foregrounds.
    pub(crate) fn refuse_upstream(&mut self, error: &jazz::db::Error) {
        if error.code == jazz::db::ErrorCode::StoreOpenedWithoutMigrations
            && self.rebuild_stale_store()
        {
            return;
        }
        let Some(upstream) = &self.upstream else {
            return;
        };
        let reason = format!("native relay upstream refused: {error}");
        tracing::warn!(target: "jazz_native_relay", "{reason}");
        upstream.wire.refuse(reason.clone());
        self.refusal = Some(UpstreamRefusal {
            code: error.code.as_str().to_owned(),
            reason,
        });
    }

    /// Writes the relay's store holds that its server has not confirmed.
    #[cfg(test)]
    pub(crate) fn persistent_pending_uploads(&self) -> usize {
        self.persistent.pending_upload_count()
    }

    /// The current upstream connection's generation (tests carry the
    /// upstream themselves, and serve each new connection afresh).
    #[cfg(test)]
    pub(crate) fn upstream_generation(&self) -> Option<u64> {
        self.upstream.as_ref().map(|upstream| upstream.generation)
    }

    /// Rebuild a store an older relay opened without the app's migrations
    /// (#5145), when nothing would be lost: neither it nor any foreground
    /// holds a write the server has not confirmed, nor a refused one kept
    /// for retry. The new store opens with
    /// the migrations and syncs everything again from the server; every
    /// foreground is served by it, and the socket worker reconnects. Once
    /// per schema version: a marker beside the store remembers the version
    /// it was rebuilt for, so a bundle that disagrees with its server does
    /// not rebuild and re-sync on every launch. `false` leaves the refusal to
    /// be reported, so the app can reset the store itself
    /// (`deleteClientStorage()`), dropping its writes.
    fn rebuild_stale_store(&mut self) -> bool {
        let marker = rebuilt_marker(&self.sqlite_path);
        let version: String = self
            .schema
            .version_id()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if self.rebuilt
            || std::fs::read_to_string(&marker).is_ok_and(|rebuilt| rebuilt == version)
            || self.persistent.pending_upload_count() > 0
            // Refused commits kept for the app's listeners or a retry are
            // its writes too: a store holding any is not rebuilt.
            || self
                .persistent
                .db
                .rejected_transactions()
                .map_or(true, |rejected| !rejected.is_empty())
            || self
                .clients
                .values()
                .any(|client| client.binding.pending_upload_count() > 0)
        {
            return false;
        }
        self.rebuilt = true;
        match self.reopen_fresh_store() {
            Ok(()) => {
                // Best effort: without it, the next launch may rebuild once more.
                let _ = std::fs::write(&marker, &version);
                tracing::warn!(
                    target: "jazz_native_relay",
                    "rebuilt a store opened without the app's migrations; it syncs again from the server"
                );
                true
            }
            Err(error) => {
                self.drive_error = Some(format!(
                    "native relay could not rebuild its store opened without the app's migrations: {error}"
                ));
                false
            }
        }
    }

    fn reopen_fresh_store(&mut self) -> Result<(), String> {
        // Transaction ids are (seq, node): a rebuilt store goes on above every
        // seq this node minted, so none is reused.
        let high_water = self.persistent.db.seq_high_water();
        if let Some(mut upstream) = self.upstream.take() {
            upstream.link.close();
            // Not a refusal: the socket worker reconnects to the new store.
            upstream.wire.retire();
        }
        for client in self.clients.values_mut() {
            client.link.close();
            client.served.close();
        }
        block_on(self.persistent.close()).map_err(|error| error.to_string())?;
        delete_store_files(&self.sqlite_path).map_err(|error| error.to_string())?;
        let db = block_on(Db::open_at(
            relay_db_config(self.schema.clone(), self.identity, &self.migrations),
            &self.sqlite_path,
            FileOptions::default(),
        ))
        .map_err(|error| error.to_string())?;
        db.seed_seq_floor(high_water);
        if let Some(hint) = self.remote_link_hint {
            db.set_remote_link_hint(hint);
        }
        let author = self.identity.author;
        let claims = self.claims.clone().unwrap_or_default();
        db.set_identity_claims(author, claims.clone());
        self.persistent = Binding::new(db, self.identity, false);
        self.persistent_dirty = hook(&self.persistent);
        for client in self.clients.values_mut() {
            client.authority = self.persistent.clone();
            client.served = self
                .persistent
                .accept_subscriber(author, claims.clone())
                .map_err(|error| error.to_string())?;
            client.link = client.binding.connect_upstream();
            client.dirty.set(true);
        }
        self.persistent_dirty.set(true);
        Ok(())
    }

    /// Tick what asked for a tick and move frames until nothing moves.
    pub(crate) fn drive(&mut self) {
        self.redrive = false;
        for round in 0.. {
            if round == NATIVE_RELAY_DRIVE_ROUNDS {
                self.redrive = true;
                break;
            }
            let mut progressed = false;
            if let Some(upstream) = &self.upstream {
                let frames = upstream.wire.take_inbound();
                if !frames.is_empty() {
                    upstream.link.receive(frames);
                    progressed = true;
                }
            }
            for client in self.clients.values() {
                if client.dirty.replace(false) {
                    if let Err(error) = block_on(client.binding.tick()) {
                        self.drive_error = Some(error.to_string());
                    }
                    progressed = true;
                }
                let frames = client.link.take_outbound();
                if !frames.is_empty() {
                    client.served.receive(frames);
                    progressed = true;
                }
            }
            if self.persistent_dirty.replace(false) {
                if let Err(error) = block_on(self.persistent.tick()) {
                    self.drive_error = Some(error.to_string());
                }
                progressed = true;
            }
            for client in self.clients.values() {
                let frames = client.served.take_outbound();
                if !frames.is_empty() {
                    client.link.receive(frames);
                    progressed = true;
                }
            }
            if let Some(upstream) = &self.upstream {
                upstream.wire.push_outbound(upstream.link.take_outbound());
            }
            if !progressed {
                break;
            }
        }
        // A refused upstream syncs nothing: stop its connection and say why,
        // rather than keeping the socket open.
        if let Some(error) = self
            .upstream
            .as_ref()
            .filter(|upstream| upstream.wire.refusal().is_none())
            .and_then(|upstream| upstream.link.upstream_refusal())
        {
            self.refuse_upstream(&error);
        }
        for client in self.clients.values_mut() {
            if let Err(error) = client.collect_events() {
                self.drive_error = Some(error);
            }
            client.poll_pending();
            if !client.mutation_errors.borrow().is_empty() {
                client.notify |= client
                    .mutation_errors
                    .borrow()
                    .iter()
                    .any(|report| report.claimed.as_ref().is_none_or(|claimed| !claimed.get()));
            }
            client.wake();
        }
    }

    fn execute_foreground(
        &mut self,
        id: u64,
        command: ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, RelayError> {
        let client = self.client_mut(id)?;
        let response = match foreground_command(client, command) {
            Ok(response) => response,
            Err(error) => operation_error(error),
        };
        Ok(response)
    }
}

// ---- foreground commands ------------------------------------------------------------

/// The response for a failed foreground operation: a core error keeps its
/// display text and adds its stable code and reason name.
fn operation_error(error: HostError) -> ForegroundDbCommandResponse {
    match error {
        HostError::Engine(error) => ForegroundDbCommandResponse::CodedOperationError {
            code: error.code.as_str().to_owned(),
            reason_name: error.reason.map(|reason| reason.as_str().to_owned()),
            reason: error.to_string(),
        },
        HostError::Argument(reason) => ForegroundDbCommandResponse::OperationError { reason },
    }
}

fn operation_response(
    result: Result<ForegroundDbCommandResponse, HostError>,
) -> ForegroundDbCommandResponse {
    result.unwrap_or_else(operation_error)
}

/// A `MutationErrorEvent` for transaction `tx` rejected with `error` (by
/// the authority, if `upstream`), as NAPI reports it.
fn mutation_error_event(
    tx: &str,
    error: &jazz::db::Error,
    serializable: bool,
    upstream: bool,
) -> serde_json::Value {
    let (code, reason) = binding_host::mutation_error_parts(error, upstream);
    serde_json::json!({
        "code": code,
        "reason": reason,
        "transaction": {
            "transactionId": tx,
            "kind": binding_host::transaction_kind(serializable),
            "sealed": true,
            "latestSettlement": {
                "kind": "rejected",
                "transactionId": tx,
                "code": code,
                "reason": reason,
            },
        },
    })
}

fn json(options: &str) -> Result<serde_json::Value, HostError> {
    if options.trim().is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_str(options)
        .map_err(|error| HostError::Argument(format!("invalid foreground options: {error}")))
}

/// Mutation options with the command's row id, as the binding reads them.
fn mutation_options(options: &str, row: Option<[u8; 16]>) -> Result<serde_json::Value, HostError> {
    let mut options = match json(options)? {
        serde_json::Value::Null => serde_json::Map::new(),
        serde_json::Value::Object(options) => options,
        _ => return Err("foreground mutation options must be an object".into()),
    };
    if let Some(row) = row {
        options.insert("rowId".to_owned(), row.to_vec().into());
    }
    Ok(serde_json::Value::Object(options))
}

fn mutation_name(kind: ForegroundMutationKind) -> &'static str {
    match kind {
        ForegroundMutationKind::Insert => "insert",
        ForegroundMutationKind::Update => "update",
        ForegroundMutationKind::Upsert => "upsert",
        ForegroundMutationKind::Delete => "delete",
        ForegroundMutationKind::Restore => "restore",
    }
}

fn kind_name(kind: ForegroundTransactionKind) -> &'static str {
    match kind {
        ForegroundTransactionKind::Mergeable => "mergeable",
        ForegroundTransactionKind::Serializable => "serializable",
    }
}

fn required_row(
    row: Option<[u8; 16]>,
    mutation: ForegroundMutationKind,
) -> Result<[u8; 16], HostError> {
    row.ok_or_else(|| HostError::Argument(format!("{} requires a row id", mutation_name(mutation))))
}

fn advice(advice: &str) -> ForegroundPermissionAdvice {
    match advice {
        "allowed" => ForegroundPermissionAdvice::Allowed,
        "denied" => ForegroundPermissionAdvice::Denied,
        _ => ForegroundPermissionAdvice::Unknown,
    }
}

/// Stage one mutation in an open transaction; an insert returns its row.
fn stage(
    client: &Foreground,
    transaction: u64,
    mutation: ForegroundMutationKind,
    table: &str,
    row: Option<[u8; 16]>,
    cells: &[u8],
    options: &str,
) -> Result<Option<RowUuid>, HostError> {
    let (id, _) = client.transaction(transaction)?;
    let binding = &client.binding;
    match mutation {
        ForegroundMutationKind::Insert => binding
            .insert_in_transaction(id, table, cells, &mutation_options(options, row)?)
            .map(Some),
        ForegroundMutationKind::Update => binding
            .update_in_transaction(
                id,
                table,
                &required_row(row, mutation)?,
                cells,
                &mutation_options(options, None)?,
            )
            .map(|()| None),
        ForegroundMutationKind::Upsert => binding
            .upsert_in_transaction(
                id,
                table,
                &required_row(row, mutation)?,
                cells,
                &mutation_options(options, None)?,
            )
            .map(|()| None),
        ForegroundMutationKind::Delete => binding
            .delete_in_transaction(
                id,
                table,
                &required_row(row, mutation)?,
                &mutation_options(options, None)?,
            )
            .map(|()| None),
        ForegroundMutationKind::Restore => binding
            .restore_in_transaction(
                id,
                table,
                &required_row(row, mutation)?,
                (!cells.is_empty()).then_some(cells),
                &mutation_options(options, None)?,
            )
            .map(|()| None),
    }
}

#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StreamingOptions {
    updated_at_ms: Option<f64>,
    head: Option<jazz::protocol::BranchSelector>,
    base: Option<jazz::protocol::BranchViewBase>,
    /// The open transaction the finished value is staged in.
    transaction: Option<String>,
}

fn foreground_command(
    client: &mut Foreground,
    command: ForegroundDbCommandRequest,
) -> Result<ForegroundDbCommandResponse, HostError> {
    use ForegroundDbCommandRequest as Request;
    use ForegroundDbCommandResponse as Response;
    let binding = client.binding.clone();
    Ok(match command {
        Request::All {
            query,
            options_json,
            transaction,
        } => {
            let transaction = transaction
                .map(|handle| client.transaction(handle).map(|(id, _)| id.clone()))
                .transpose()?;
            let read = binding.all(
                query,
                &json(&options_json)?,
                transaction.as_deref(),
                None,
                None,
            )?;
            client.pending_or_ready(
                Box::pin(async move { Ok(Response::Rows { rows: read.await? }) }),
                None,
            )?
        }
        Request::LocalCurrentRow { table, row_id } => Response::Rows {
            rows: binding.local_current_row(&table, &row_id)?,
        },
        Request::Subscribe {
            query,
            options_json,
        } => {
            let stream = binding.subscribe(&query, &json(&options_json)?, None, None)?;
            let handle = client.handle();
            client.subscriptions.insert(
                handle,
                ForegroundSubscription {
                    stream,
                    buffer: VecDeque::new(),
                    opened: false,
                    closed: false,
                },
            );
            client.collect_events().map_err(HostError::Argument)?;
            Response::Subscribed {
                subscription: handle,
            }
        }
        Request::DrainSubscription { subscription } => {
            client.collect_events().map_err(HostError::Argument)?;
            let subscription = client.subscriptions.get_mut(&subscription).ok_or_else(|| {
                HostError::Argument(format!("unknown subscription {subscription}"))
            })?;
            Response::SubscriptionEvents {
                events: subscription.buffer.drain(..).collect(),
            }
        }
        Request::Unsubscribe { subscription } => {
            let closed = client.subscriptions.remove(&subscription).is_some();
            if closed {
                client.dirty.set(true);
            }
            Response::Unsubscribed { closed }
        }
        Request::Poll { operation } => match client.completed.remove(&operation) {
            Some(response) => response,
            None => {
                let pending = client.pending.get_mut(&operation).ok_or_else(|| {
                    HostError::Argument(format!("unknown foreground operation {operation}"))
                })?;
                match poll_once(&mut pending.future) {
                    Poll::Ready(result) => {
                        client.pending.remove(&operation);
                        result?
                    }
                    Poll::Pending => Response::Pending { operation },
                }
            }
        },
        Request::Cancel { operation } => {
            let cancelled = match client.pending.remove(&operation) {
                Some(pending) => {
                    if let Some(id) = pending.advice {
                        client.authority.cancel_permission_advice(id);
                    }
                    true
                }
                None => client.completed.remove(&operation).is_some(),
            };
            Response::Cancelled { cancelled }
        }
        Request::BeginTransaction { kind } => {
            if client.transactions.len() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
                return Err("too many open foreground transactions".into());
            }
            let id = jazz::tools::OpenTransactionId::new().to_string();
            binding.begin_transaction(&id, kind_name(kind), None, None)?;
            let handle = client.handle();
            client.transactions.insert(handle, (id, kind));
            Response::TransactionOpened {
                transaction: handle,
            }
        }
        Request::Insert {
            transaction,
            table,
            cells,
            row_id,
        } => {
            let row = stage(
                client,
                transaction,
                ForegroundMutationKind::Insert,
                &table,
                row_id,
                &cells,
                "",
            )?
            .expect("an insert returns its row");
            Response::Inserted {
                row_id: *row.as_bytes(),
            }
        }
        Request::Update {
            transaction,
            table,
            row_id,
            patch,
        } => {
            stage(
                client,
                transaction,
                ForegroundMutationKind::Update,
                &table,
                Some(row_id),
                &patch,
                "",
            )?;
            Response::MutationStaged
        }
        Request::Upsert {
            transaction,
            table,
            row_id,
            cells,
        } => {
            stage(
                client,
                transaction,
                ForegroundMutationKind::Upsert,
                &table,
                Some(row_id),
                &cells,
                "",
            )?;
            Response::MutationStaged
        }
        Request::Delete {
            transaction,
            table,
            row_id,
        } => {
            stage(
                client,
                transaction,
                ForegroundMutationKind::Delete,
                &table,
                Some(row_id),
                &[],
                "",
            )?;
            Response::MutationStaged
        }
        Request::StageMutation {
            transaction,
            mutation,
            table,
            row_id,
            cells,
            options_json,
        } => match stage(
            client,
            transaction,
            mutation,
            &table,
            row_id,
            &cells,
            &options_json,
        )? {
            Some(row) => Response::Inserted {
                row_id: *row.as_bytes(),
            },
            None => Response::MutationStaged,
        },
        Request::CommitTransaction { transaction } => {
            let (id, kind) = client.transaction(transaction)?.clone();
            client.transactions.remove(&transaction);
            let write = binding.commit_transaction(&id, Some(kind_name(kind)))?;
            let (tx_id, _) = client.track(write);
            Response::TransactionCommitted {
                tx_id: *tx_id.as_bytes(),
            }
        }
        Request::RollbackTransaction { transaction } => {
            let rolled_back = match client.transactions.remove(&transaction) {
                Some((id, _)) => {
                    binding.rollback_transaction(&id)?;
                    true
                }
                None => false,
            };
            Response::TransactionRolledBack { rolled_back }
        }
        Request::WaitForCoreTransaction { tx_id } => {
            wait_for_write(client, tx_id, "global", false)?
        }
        Request::WaitForTransaction {
            tx_id,
            tier,
            observe_only,
        } => wait_for_write(client, tx_id, &tier, observe_only)?,
        Request::WaitForPendingWrites { tier } => {
            match jazz::binding_args::durability_tier(&tier).map_err(HostError::Argument)? {
                DurabilityTier::None | DurabilityTier::Local => Response::Rows { rows: Vec::new() },
                DurabilityTier::Global => {
                    let wait = binding.wait_for_pending_writes_global();
                    client.pending_or_ready(
                        Box::pin(async move {
                            wait.await?;
                            Ok(Response::Rows { rows: Vec::new() })
                        }),
                        None,
                    )?
                }
            }
        }
        Request::CaptureSnapshot => {
            let capture = binding.capture_snapshot();
            client.pending_or_ready(
                Box::pin(async move {
                    Ok(Response::Rows {
                        rows: capture.await?,
                    })
                }),
                None,
            )?
        }
        Request::PermissionAdvice { action } => {
            let action = match action {
                ForegroundPermissionAdviceAction::Insert { table, cells } => {
                    binding_host::advice::insert(table, &cells)
                }
                ForegroundPermissionAdviceAction::Read { table, row } => {
                    binding_host::advice::read(table, &row)
                }
                ForegroundPermissionAdviceAction::Update { table, row, patch } => {
                    binding_host::advice::update(table, &row, &patch)
                }
                ForegroundPermissionAdviceAction::Delete { table, row } => {
                    binding_host::advice::delete(table, &row)
                }
            }?;
            let (id, future) = client
                .authority
                .request_permission_advice(action, None, None)?;
            client.pending_or_ready(
                Box::pin(async move {
                    Ok(Response::PermissionAdvice {
                        advice: advice(future.await?),
                    })
                }),
                Some(id),
            )?
        }
        Request::WriteState { .. } => {
            return Err(binding_host::not_supported("write state snapshots"));
        }
        Request::DrainMutationErrors => {
            let reports = std::mem::take(&mut *client.mutation_errors.borrow_mut());
            let events = reports
                .into_iter()
                .filter(|report| report.claimed.as_ref().is_none_or(|claimed| !claimed.get()))
                .map(|report| report.event)
                .collect::<Vec<_>>();
            Response::MutationErrors {
                events_json: serde_json::Value::Array(events).to_string(),
            }
        }
        Request::TakeSlowQueryReport { app } => Response::SlowQueryReport {
            lines_json: serde_json::Value::from(binding.take_slow_query_report(app.as_deref()))
                .to_string(),
        },
        Request::SetSlowQueryThresholdMs { ms } => {
            binding.set_slow_query_threshold_ms(ms as f64)?;
            Response::SlowQueryThresholdSet
        }
        Request::PendingUploadCount => Response::PendingUploadCount {
            count: binding.pending_upload_count() as u64,
        },
        Request::UploadStatus => Response::UploadStatus {
            status_json: serde_json::to_string(&binding.upload_status())
                .expect("an upload status serializes"),
        },
        Request::BeginStreamingMutation {
            mutation,
            table,
            row_id,
            cells,
            column,
            options_json,
        } => {
            let options = match json(&options_json)? {
                serde_json::Value::Null => StreamingOptions::default(),
                options => serde_json::from_value(options).map_err(|error| {
                    HostError::Argument(format!("invalid streaming mutation options: {error}"))
                })?,
            };
            let upload = binding.begin_streaming_mutation(
                table,
                &row_id,
                &cells,
                column,
                Some(mutation_name(mutation)),
                None,
                None,
                options.updated_at_ms,
                options.head,
                options.base,
                options.transaction.as_deref(),
            )?;
            let handle = client.handle();
            client.uploads.insert(handle, upload);
            Response::StreamingMutationOpened { upload: handle }
        }
        Request::PushStreamingMutation { upload, chunk } => {
            let mutation = client.uploads.get_mut(&upload).ok_or_else(|| {
                HostError::Argument(format!("unknown streaming mutation {upload}"))
            })?;
            if let Err(error) = mutation.push(&chunk) {
                client.uploads.remove(&upload);
                return Err(error);
            }
            Response::StreamingMutationPushed
        }
        Request::FinishStreamingMutation { upload } => {
            let mut mutation = client.uploads.remove(&upload).ok_or_else(|| {
                HostError::Argument(format!("unknown streaming mutation {upload}"))
            })?;
            if mutation.in_transaction() {
                mutation.finish_in_transaction()?;
                Response::MutationStaged
            } else {
                let (tx_id, _) = client.track(mutation.finish()?);
                Response::TransactionCommitted {
                    tx_id: *tx_id.as_bytes(),
                }
            }
        }
        Request::AbortStreamingMutation { upload } => Response::StreamingMutationAborted {
            aborted: match client.uploads.remove(&upload) {
                Some(mut mutation) => mutation.abort()?,
                None => false,
            },
        },
        Request::UpdateLargeValues {
            table,
            row_id,
            patch,
            descriptors_json,
            updated_at_ms,
        } => {
            let mutations: Vec<LargeValueUpdate> = serde_json::from_str(&descriptors_json)
                .map_err(|error| {
                    HostError::Argument(format!("invalid partial-value update descriptor: {error}"))
                })?;
            let write = binding.update_large_values(
                &table,
                &row_id,
                &patch,
                mutations,
                updated_at_ms.map(|ms| ms as f64),
            )?;
            let (tx_id, _) = client.track(write);
            Response::TransactionCommitted {
                tx_id: *tx_id.as_bytes(),
            }
        }
        Request::DirectMutation {
            mutation,
            table,
            row_id,
            cells,
            options_json,
        } => {
            let write = match mutation {
                ForegroundMutationKind::Insert => {
                    binding.insert(&table, &cells, &mutation_options(&options_json, row_id)?)
                }
                ForegroundMutationKind::Update => binding.update(
                    &table,
                    &required_row(row_id, mutation)?,
                    &cells,
                    &mutation_options(&options_json, None)?,
                ),
                ForegroundMutationKind::Upsert => binding.upsert(
                    &table,
                    &required_row(row_id, mutation)?,
                    &cells,
                    &mutation_options(&options_json, None)?,
                ),
                ForegroundMutationKind::Delete => binding.delete(
                    &table,
                    &required_row(row_id, mutation)?,
                    &mutation_options(&options_json, None)?,
                ),
                ForegroundMutationKind::Restore => binding.restore(
                    &table,
                    &required_row(row_id, mutation)?,
                    (!cells.is_empty()).then_some(cells.as_slice()),
                    &mutation_options(&options_json, None)?,
                ),
            }?;
            let (tx_id, row) = client.track(write);
            Response::MutationCommitted {
                tx_id: *tx_id.as_bytes(),
                row_id: *row.as_bytes(),
            }
        }
        // Host-level commands never reach a foreground's owner.
        Request::Probe
        | Request::Tick
        | Request::Close
        | Request::DisconnectNativeUpstream
        | Request::ReconnectNativeUpstream
        | Request::NativeConnectionStatus
        | Request::NativeSessionMetadata
        | Request::DrainTickEvents
        | Request::DeleteStore => {
            return Err("host command reached the relay owner".into());
        }
    })
}

/// Wait for a write this foreground committed to reach `tier`. Unless
/// `observe_only`, the wait claims reporting the write's rejection.
fn wait_for_write(
    client: &mut Foreground,
    tx_id: [u8; 16],
    tier: &str,
    observe_only: bool,
) -> Result<ForegroundDbCommandResponse, HostError> {
    let tracked = client
        .writes
        .get(&TransactionId(tx_id))
        .ok_or_else(|| HostError::Argument("unknown foreground transaction id".to_owned()))?;
    let wait = client.binding.wait_for_write(
        &tracked.write,
        tier,
        (!observe_only).then(|| Rc::clone(&tracked.claimed)),
    )?;
    client.pending_or_ready(
        Box::pin(async move {
            wait.await?;
            Ok(ForegroundDbCommandResponse::TransactionSettled { tx_id })
        }),
        None,
    )
}

// ---- registry -----------------------------------------------------------------------

/// A registry is owned by the platform host (application process), not global
/// Rust state. That makes teardown explicit and lets Android services, iOS app
/// delegates, and test processes choose their own lifecycle semantics.
#[derive(Default)]
pub struct NativeRelayRegistry {
    relays: Mutex<BTreeMap<RelayScope, NativeRelay>>,
}

impl NativeRelayRegistry {
    pub fn open_admitted(&self, config: RelayOpenConfig) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        if let Some(existing) = relays.get(&config.scope) {
            if existing.inner.sqlite_path != config.sqlite_path
                || existing.inner.schema_version != config.schema.version_id()
                || existing.inner.migrations != config.migrations_key()
                || existing.inner.identity != config.identity
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            if existing.is_alive() {
                return Ok(existing.clone());
            }
            existing.inner.shutdown();
            relays.remove(&config.scope);
        }
        // Linked identities share an account's durable root, but never its
        // live authorization scope. Keep the old owner exclusive until close
        // has joined it.
        if relays
            .values()
            .any(|relay| relay.inner.sqlite_path == config.sqlite_path)
        {
            return Err(RelayError::StorageAlreadyOwned);
        }
        let relay = NativeRelay::spawn(config.clone())?;
        relays.insert(config.scope, relay.clone());
        Ok(relay)
    }

    pub fn close(&self, scope: &RelayScope) -> Result<bool, RelayError> {
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        let Some(relay) = relays.remove(scope) else {
            return Ok(false);
        };
        relay.inner.shutdown();
        Ok(true)
    }
}

// ---- the socket worker --------------------------------------------------------------

/// Native-owned socket lifecycle for one relay scope.
///
/// Android and iOS call this shared worker from their private session setup;
/// neither platform gets a raw protocol codec or reconnect loop. The worker
/// supplies the bearer only to the normal Core WebSocket prelude and always
/// uses the authenticated, non-SYSTEM connection mode.
pub struct NativeRelaySocketWorker {
    activation: Mutex<Option<mpsc::Sender<()>>>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

/// What the device's operating system says about its network path
/// (`NWPathMonitor` on iOS). The default is a usable path on unknown
/// interfaces, which is how a host without a monitor behaves: nothing in the
/// worker changes until a monitor reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkPath {
    /// The device has a route out at all.
    pub satisfied: bool,
    /// The interface kinds the path uses, as [`NETWORK_INTERFACE_WIFI`] and
    /// friends. Zero when unknown.
    pub interfaces: u32,
}

impl Default for NetworkPath {
    fn default() -> Self {
        Self {
            satisfied: true,
            interfaces: 0,
        }
    }
}

pub const NETWORK_INTERFACE_WIFI: u32 = 1 << 0;
pub const NETWORK_INTERFACE_CELLULAR: u32 = 1 << 1;
pub const NETWORK_INTERFACE_WIRED: u32 = 1 << 2;
pub const NETWORK_INTERFACE_OTHER: u32 = 1 << 3;

/// Interfaces the device only stops routing through when they go away: Wi-Fi,
/// wired, and other (a VPN's tunnel, whose sockets die with it). The path
/// leaves cellular as soon as Wi-Fi is preferred, while cellular itself stays
/// up and keeps carrying the sockets opened on it, so a link opened on
/// cellular is not dropped for that.
const NETWORK_INTERFACES_GONE_WHEN_LEFT: u32 =
    NETWORK_INTERFACE_WIFI | NETWORK_INTERFACE_WIRED | NETWORK_INTERFACE_OTHER;

/// However often the path changes, it cuts a backoff short at most once per
/// this long after an attempt began: a flapping path must not become a
/// reconnect loop.
const PATH_RETRY_FLOOR: Duration = Duration::from_secs(1);

/// The device's one network path, set by the platform's monitor and read by
/// every socket worker.
static NETWORK_PATH: std::sync::LazyLock<tokio::sync::watch::Sender<NetworkPath>> =
    std::sync::LazyLock::new(|| tokio::sync::watch::channel(NetworkPath::default()).0);

/// Report the device's network path. Only a real change reaches the workers.
pub fn report_network_path(path: NetworkPath) {
    NETWORK_PATH.send_if_modified(|current| {
        let changed = *current != path;
        *current = path;
        changed
    });
}

/// Follow the device's network path.
pub fn network_path() -> tokio::sync::watch::Receiver<NetworkPath> {
    NETWORK_PATH.subscribe()
}

/// One wait's view of the network path, from the moment it was taken.
///
/// A socket bound to an interface the device no longer uses is dead even if
/// nothing has told it so yet: the operating system's word arrives at once,
/// the transport's silence watchdog only after its deadline.
struct PathWatch {
    path: tokio::sync::watch::Receiver<NetworkPath>,
    taken_on: NetworkPath,
}

impl PathWatch {
    fn new(path: &tokio::sync::watch::Receiver<NetworkPath>) -> Self {
        let mut path = path.clone();
        let taken_on = *path.borrow_and_update();
        Self { path, taken_on }
    }

    /// A watch that compares against `taken_on`, a path seen earlier.
    fn since(path: &tokio::sync::watch::Receiver<NetworkPath>, taken_on: NetworkPath) -> Self {
        let mut path = path.clone();
        path.borrow_and_update();
        Self { path, taken_on }
    }

    /// Resolves once the path can no longer carry a connection opened when
    /// this watch was taken: the device lost its route, or stopped using a
    /// Wi-Fi or wired interface the connection may be bound to. An interface
    /// added alongside does not end it. While `connecting`, a route that came
    /// back after the attempt began without one ends it too: that attempt was
    /// dialled into no network and may hang until its timeout, while a fresh
    /// one can land at once. Cancel-safe.
    async fn lost(&mut self, connecting: bool) -> String {
        loop {
            if self.path.changed().await.is_err() {
                return std::future::pending().await;
            }
            let now = *self.path.borrow_and_update();
            if !now.satisfied {
                return "the device lost its network path".to_owned();
            }
            if !self.taken_on.satisfied || self.taken_on.interfaces == 0 {
                if connecting {
                    return "the device's network path came back".to_owned();
                }
                // A link that got through while the device reported no path
                // lives on the path it reports now.
                self.taken_on = now;
                continue;
            }
            if self.taken_on.interfaces & NETWORK_INTERFACES_GONE_WHEN_LEFT & !now.interfaces != 0 {
                return "the device's network path moved to another interface".to_owned();
            }
        }
    }

    /// The connection was admitted. One dialled while the device reported no
    /// path got through anyway, so the monitor was behind: the link lives on
    /// the path the device reports now, and is watched against that.
    fn admitted(&mut self) {
        if !self.taken_on.satisfied || self.taken_on.interfaces == 0 {
            self.taken_on = *self.path.borrow();
        }
    }

    /// Resolves once the device has a usable path that differs from the one
    /// this watch was taken on, at once if it already has. Cancel-safe.
    async fn usable_again(&mut self) {
        let now = *self.path.borrow();
        if now.satisfied && now != self.taken_on {
            return;
        }
        loop {
            if self.path.changed().await.is_err() {
                return std::future::pending().await;
            }
            if self.path.borrow_and_update().satisfied {
                return;
            }
        }
    }
}

/// Private native socket inputs. This is intentionally absent from postcard
/// commands and diagnostics: JavaScript can neither configure the endpoint
/// nor read its bearer.
pub struct NativeRelaySocketConfig {
    pub server_url: String,
    pub app_id: AppId,
    pub peer_identity: AuthorSubject,
    pub auth: AuthConfig,
    /// The upgrade request's `Authorization` header (see
    /// [`NativeTransportRequest::authorization`]).
    pub authorization: Option<String>,
    /// Every event of the worker's [`UpstreamLink`], in order.
    pub on_event: Arc<dyn Fn(LinkEvent) + Send + Sync>,
    /// The device's network path ([`network_path`] in production).
    pub network_path: tokio::sync::watch::Receiver<NetworkPath>,
    /// Retry timing ([`LinkPolicy::default`] in production).
    pub link_policy: LinkPolicy,
}

impl std::fmt::Debug for NativeRelaySocketConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeRelaySocketConfig")
            .field("server_url", &self.server_url)
            .field("app_id", &self.app_id)
            .field("peer_identity", &self.peer_identity)
            .field("auth", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl NativeRelaySocketWorker {
    /// Start the production native WebSocket worker.
    pub fn start(relay: NativeRelay, config: NativeRelaySocketConfig) -> Result<Self, RelayError> {
        Self::start_with_connector(relay, config, Arc::new(NativeWebSocketConnector))
    }

    /// Composition seam for deterministic tests. Production uses
    /// [`NativeWebSocketConnector`]; the connector owns TLS, WebSocket
    /// framing and Core's authenticated handshake.
    pub fn start_with_connector(
        relay: NativeRelay,
        config: NativeRelaySocketConfig,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<Self, RelayError> {
        let worker = Self::prepare_with_connector(relay, config, connector)?;
        worker.activate();
        Ok(worker)
    }

    pub(crate) fn activate(&self) {
        if let Some(start) = self
            .activation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = start.send(());
        }
    }

    /// Reserve every fallible resource without connecting: a caller can
    /// prepare a replacement before it retires the worker it replaces.
    pub(crate) fn prepare_with_connector(
        relay: NativeRelay,
        config: NativeRelaySocketConfig,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<Self, RelayError> {
        // A socket presents exactly one user credential: a local-first
        // account's own token, or a session stated to the app's backend with
        // the app's bearer on the upgrade. Never a backend or admin secret.
        let token = config
            .auth
            .jwt_token
            .as_deref()
            .is_some_and(|t| !t.is_empty());
        let stated = config.auth.backend_session.is_some()
            && config
                .authorization
                .as_deref()
                .is_some_and(|a| !a.is_empty());
        if config.peer_identity == AuthorSubject::SYSTEM
            || token == stated
            || (token && config.authorization.is_some())
            || config.auth.backend_secret.is_some()
            || config.auth.admin_secret.is_some()
        {
            return Err(RelayError::Socket(
                "native relay sockets require an ordinary non-SYSTEM user session".to_owned(),
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(tokio::sync::Notify::new());
        let thread_cancelled = Arc::clone(&cancelled);
        let thread_wake = Arc::clone(&wake);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        let (activate, activation) = mpsc::channel();
        let join = thread::Builder::new()
            .name("jazz-native-relay-socket".to_owned())
            .spawn(move || {
                if activation.recv().is_err() {
                    return;
                }
                runtime.block_on(run_socket_worker(
                    relay,
                    config,
                    connector,
                    thread_cancelled,
                    thread_wake,
                ));
            })
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        Ok(Self {
            activation: Mutex::new(Some(activate)),
            cancelled,
            wake,
            join: Mutex::new(Some(join)),
        })
    }

    pub fn cancel(&self) {
        self.activation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

impl Drop for NativeRelaySocketWorker {
    fn drop(&mut self) {
        self.cancel();
        let join = self
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

/// Detaches its connection from the relay when the socket loop leaves it.
struct SocketUpstreamLease {
    relay: NativeRelay,
    generation: u64,
    wire: NativeRelayWire,
}

impl Drop for SocketUpstreamLease {
    fn drop(&mut self) {
        // Stop stale bridge traffic at once, even before the owner runs.
        self.wire.retire();
        while let Err(RelayError::OwnerQueueFull) =
            self.relay.retire_socket_upstream(self.generation)
        {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// How one established connection ended.
enum LinkEnd {
    /// The worker was cancelled.
    Cancelled,
    /// The link ended and can be retried.
    Lost(LinkFailure),
    /// The relay is gone or failed: nothing can connect again.
    Stop(String),
}

/// Drives the shared [`UpstreamLink`] machine: this worker owns the socket
/// and the timer, the machine decides when to connect, back off, wait for a
/// new credential or stop. A renewed credential replaces the whole worker.
async fn run_socket_worker(
    relay: NativeRelay,
    config: NativeRelaySocketConfig,
    connector: Arc<dyn NativeTransportConnector>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
) {
    let started = Instant::now();
    // A fresh random seed per worker, so a fleet of devices does not retry
    // in lockstep after a server restart.
    let seed = {
        use std::hash::BuildHasher;
        std::collections::hash_map::RandomState::new().hash_one(std::process::id())
    };
    let mut link = UpstreamLink::new(config.link_policy, seed);
    let mut actions = link.handle(0, LinkInput::ServerConfigured(true));
    // The path the last attempt was made on: the backoff it earned is a
    // judgement about that path, not about one the device moved to since.
    let mut attempted_on = *config.network_path.borrow();
    let mut attempt_began: Option<tokio::time::Instant> = None;
    loop {
        for event in link.drain_events() {
            (config.on_event)(event);
        }
        let mut next = None;
        for action in actions.drain(..) {
            match action {
                LinkAction::Connect { attempt, .. } => next = Some(Err(attempt)),
                LinkAction::ArmTimer { token, after_ms } => next = Some(Ok((token, after_ms))),
                // One attempt and one timer at a time: each ends before the
                // worker asks the machine for the next step.
                LinkAction::Close(_) | LinkAction::CancelTimer(_) => {}
            }
        }
        let input = match next {
            // Waiting for a credential or stopped: only cancellation (or a
            // replacement worker) ends this.
            None => loop {
                if cancelled.load(Ordering::Acquire) {
                    return;
                }
                wake.notified().await;
            },
            Some(Ok((token, after_ms))) => {
                let sleep = tokio::time::sleep(Duration::from_millis(after_ms));
                tokio::pin!(sleep);
                let mut path = PathWatch::since(&config.network_path, attempted_on);
                let mut path_moved = false;
                loop {
                    tokio::select! {
                        () = &mut sleep => break LinkInput::TimerFired(token),
                        // The backoff spares a server that is down; a device
                        // whose own path came back, or moved, has nothing to
                        // wait for, beyond the floor that keeps a flapping
                        // path from turning into a reconnect loop.
                        () = path.usable_again(), if !path_moved => {
                            path_moved = true;
                            let now = tokio::time::Instant::now();
                            let earliest = attempt_began.map_or(now, |began| began + PATH_RETRY_FLOOR);
                            if earliest < sleep.deadline() {
                                sleep.as_mut().reset(earliest.max(now));
                            }
                        }
                        () = wake.notified() => {
                            if cancelled.load(Ordering::Acquire) {
                                return;
                            }
                        }
                    }
                }
            }
            Some(Err(attempt)) => {
                let admitted = || {
                    // Admission asks for nothing more: the machine only
                    // reports the link live.
                    let now = started.elapsed().as_millis() as u64;
                    let _ = link.handle(now, LinkInput::Admitted(attempt));
                    for event in link.drain_events() {
                        (config.on_event)(event);
                    }
                };
                let mut path = PathWatch::new(&config.network_path);
                attempted_on = path.taken_on;
                attempt_began = Some(tokio::time::Instant::now());
                match connect_and_bridge(
                    &relay,
                    &config,
                    &*connector,
                    &cancelled,
                    &wake,
                    &mut path,
                    admitted,
                )
                .await
                {
                    LinkEnd::Cancelled => return,
                    LinkEnd::Lost(failure) => LinkInput::Ended(attempt, failure),
                    LinkEnd::Stop(message) => {
                        LinkInput::Ended(attempt, LinkFailure::Fatal(message))
                    }
                }
            }
        };
        if cancelled.load(Ordering::Acquire) {
            return;
        }
        actions = link.handle(started.elapsed().as_millis() as u64, input);
    }
}

/// One connection attempt and, once admitted, the life of its link.
/// `admitted` runs once the relay has installed the connection.
async fn connect_and_bridge(
    relay: &NativeRelay,
    config: &NativeRelaySocketConfig,
    connector: &dyn NativeTransportConnector,
    cancelled: &AtomicBool,
    wake: &Arc<tokio::sync::Notify>,
    path: &mut PathWatch,
    admitted: impl FnOnce(),
) -> LinkEnd {
    let inbound_wake = Arc::clone(wake);
    let request = NativeTransportRequest {
        requested_link: NativeTransportLink::OrdinarySession,
        server_url: config.server_url.clone(),
        app_id: config.app_id,
        peer_identity: config.peer_identity,
        auth: config.auth.clone(),
        authorization: config.authorization.clone(),
        bundle: None,
        wake: Arc::new(move || inbound_wake.notify_one()),
    };
    // Taken before the attempt: a path lost between dialling and admission
    // ends this connection like one lost afterwards.
    let connecting = connector.connect(request);
    tokio::pin!(connecting);
    let connected = loop {
        tokio::select! {
            result = &mut connecting => break result,
            reason = path.lost(true) => {
                return LinkEnd::Lost(LinkFailure::Network(format!(
                    "native relay socket connect abandoned: {reason}"
                )));
            }
            () = wake.notified() => {
                if cancelled.load(Ordering::Acquire) {
                    return LinkEnd::Cancelled;
                }
            }
        }
    };
    let connected = match connected {
        Ok(connected) => connected,
        Err(NativeTransportError::Retryable(error)) => {
            return LinkEnd::Lost(LinkFailure::Network(format!(
                "native relay socket connect failed: {error}"
            )));
        }
        // TLS, a proxy refusing the upgrade, a rejected session: it can
        // clear without a new credential, so it is retried.
        Err(NativeTransportError::Terminal(error)) => {
            return LinkEnd::Lost(LinkFailure::Refused(format!(
                "native relay socket connect failed: {error}"
            )));
        }
        // Retrying the same request with the same credential cannot
        // succeed (#4248).
        Err(NativeTransportError::AuthRejected(error)) => {
            return LinkEnd::Lost(LinkFailure::AuthRejected {
                reason: AuthFailureReason::from_message(&error),
                message: format!("native relay socket connect failed: {error}"),
            });
        }
    };
    let (generation, wire) = loop {
        if cancelled.load(Ordering::Acquire) {
            return LinkEnd::Cancelled;
        }
        match relay.begin_socket_upstream() {
            Ok(installed) => break installed,
            Err(RelayError::OwnerQueueFull) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => {
                return LinkEnd::Stop(format!("native relay upstream install failed: {error}"));
            }
        }
    };
    let lease = SocketUpstreamLease {
        relay: relay.clone(),
        generation,
        wire,
    };
    admitted();
    path.admitted();
    let end = bridge_connection(
        relay,
        &lease.wire,
        connected.transport,
        connected.terminal,
        cancelled,
        wake,
        path,
    )
    .await;
    drop(lease);
    end
}

/// Move frames between one established connection and the relay until the
/// connection ends.
async fn bridge_connection(
    relay: &NativeRelay,
    wire: &NativeRelayWire,
    mut transport: Box<dyn WireTransport + Send>,
    mut terminal: jazz::tools::native_transport_connector::NativeTransportTerminalFuture,
    cancelled: &AtomicBool,
    wake: &tokio::sync::Notify,
    path: &mut PathWatch,
) -> LinkEnd {
    // Losing an admitted link is an outage, whatever the transport calls it,
    // unless the server refused the credential.
    let terminal_end = |terminal: NativeTransportTerminal| {
        LinkEnd::Lost(match terminal {
            NativeTransportTerminal::Failed(NativeTransportError::AuthRejected(error)) => {
                LinkFailure::AuthRejected {
                    reason: AuthFailureReason::from_message(&error),
                    message: format!("native relay socket terminated: {error}"),
                }
            }
            NativeTransportTerminal::Failed(
                NativeTransportError::Terminal(error) | NativeTransportError::Retryable(error),
            )
            | NativeTransportTerminal::PeerClosed(error) => {
                LinkFailure::Network(format!("native relay socket terminated: {error}"))
            }
            NativeTransportTerminal::OwnerDropped => {
                LinkFailure::Network("native relay socket dropped".to_owned())
            }
        })
    };
    // A wire the owner retired because it refused the upstream stops the
    // link with that refusal, however the bridge found it closed: the
    // refusal can land between the check below and a queue operation.
    let lost = || match wire.refusal() {
        Some(reason) => LinkEnd::Stop(reason),
        None => LinkEnd::Lost(LinkFailure::Network("native relay wire closed".to_owned())),
    };
    loop {
        if cancelled.load(Ordering::Acquire) {
            return LinkEnd::Cancelled;
        }
        // The relay refused this upstream: retrying the same link cannot
        // succeed, so the link stops with the refusal until a reconnect.
        if let Some(reason) = wire.refusal() {
            return LinkEnd::Stop(reason);
        }
        // The relay's frames out.
        let outbound = match wire.take_outbound_encoded() {
            Ok(frames) => frames,
            Err(_) => return lost(),
        };
        let mut unsent = outbound.into_iter();
        let mut failed = false;
        while let Some(frame) = unsent.next() {
            match transport.send_frame(frame.clone()) {
                Ok(()) => {}
                Err(TransportError::Backpressure) => {
                    wire.restore_outbound(std::iter::once(frame).chain(unsent.by_ref()).collect());
                    break;
                }
                Err(TransportError::Failed(_)) => {
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            // The socket pump closed: its terminal decides what follows.
            return tokio::select! {
                terminal = &mut terminal => terminal_end(terminal),
                _ = tokio::time::sleep(Duration::from_secs(5)) => lost(),
            };
        }
        // The server's frames in, while the relay has room for them.
        let mut received = false;
        while wire.inbound_has_room() {
            let Some(frame) = transport.try_recv_frame() else {
                break;
            };
            if wire.push_inbound_encoded(&frame).is_err() {
                return lost();
            }
            received = true;
        }
        if received
            && let Err(error) = relay.pump()
            && !matches!(error, RelayError::OwnerQueueFull)
        {
            if cancelled.load(Ordering::Acquire) {
                return LinkEnd::Cancelled;
            }
            return LinkEnd::Stop(format!("native relay owner pump failed: {error}"));
        }
        tokio::select! {
            terminal = &mut terminal => return terminal_end(terminal),
            // The socket may never hear of it: a dead route is silence.
            reason = path.lost(false) => {
                return LinkEnd::Lost(LinkFailure::Network(format!(
                    "native relay socket dropped: {reason}"
                )));
            }
            _ = wire.outbound_ready.notified() => {},
            _ = wake.notified() => {},
            _ = tokio::time::sleep(Duration::from_millis(50)) => {},
        }
    }
}

// ---- errors -------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("invalid native relay ABI range {minimum}..={maximum}")]
    InvalidAbiRange { minimum: u16, maximum: u16 },
    #[error(
        "native relay ABI {native} is incompatible with wrapper range {minimum}..={maximum}; a new native development/release build is required"
    )]
    IncompatibleAbi {
        native: u16,
        minimum: u16,
        maximum: u16,
    },
    #[error("native relay peer message exceeds the logical-message limit: {0}")]
    PeerMessageTooLarge(String),
    #[error(
        "native relay {direction} queue capacity exceeded ({queued_messages} messages); drain or pump before retrying"
    )]
    QueueCapacityExceeded {
        direction: &'static str,
        queued_messages: usize,
    },
    #[error("invalid native relay scope: {0}")]
    InvalidScope(String),
    #[error("failed to open native relay owner thread: {0}")]
    OwnerThread(String),
    #[error("native relay host entropy failed: {0}")]
    Entropy(String),
    #[error("native relay is closed")]
    Closed,
    #[error("native relay owner command queue is full; retry after the next scheduled tick")]
    OwnerQueueFull,
    #[error("native relay internal mutex poisoned: {0}")]
    Poisoned(&'static str),
    #[error("native relay does not know UI client {0}")]
    UnknownClient(u64),
    #[error("a native relay scope is already open with a different storage path or schema")]
    ScopeConfigurationMismatch,
    #[error(
        "native account storage is still owned by another live scope; close it before switching identities"
    )]
    StorageAlreadyOwned,
    #[error("native relay UI client id space exhausted")]
    ClientIdExhausted,
    #[error("native relay storage failed: {0}")]
    Storage(String),
    #[error(
        "the native relay store was written by a pre-SQLite Jazz build; delete it and resync from Core"
    )]
    UnsupportedStorage,
    #[error("Jazz database failed: {0}")]
    Host(HostError),
    #[error("native relay drive failed: {0}")]
    Drive(String),
    #[error("native relay socket: {0}")]
    Socket(String),
}

#[cfg(test)]
mod relay_config_tests {
    use super::*;

    /// See [`relay_db_config`]: relay stores, like the bindings', have no
    /// chunk store.
    #[test]
    fn relay_configs_have_no_chunk_store() {
        let schema = JazzSchema::new(&jazz::tools::SchemaBuilder::new().build()).unwrap();
        let identity = DbIdentity {
            node: jazz::ids::NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        };
        let config = relay_db_config(schema, identity, &Migrations::default());
        assert!(config.chunk_store.is_none());
        assert!(!config.host_chunk_io);
    }
}
