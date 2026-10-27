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
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use jazz::binding_host::{self, Binding, EventChunk, HostError, Link};
use jazz::db::{
    Db, DbConfig, DbIdentity, LargeValueUpdate, RemoteLinkHint, SubscriptionStream, TickScheduler,
    TickUrgency, block_on,
};
use jazz::ids::{AuthorSubject, RowUuid};
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

use crate::catalogue_cache::CatalogueCache;
use crate::node_lease::ForegroundNodeLease;
use crate::storage::{ClientRelayScope, RelayDb as AdmittedStorage};
use crate::{
    FOREGROUND_WAKE_IMMEDIATE, ForegroundDbCommandRequest, ForegroundDbCommandResponse,
    ForegroundMutationKind, ForegroundPermissionAdvice, ForegroundPermissionAdviceAction,
    ForegroundSubscriptionEvent, ForegroundTransactionKind, ForegroundWakeState,
    NATIVE_RELAY_ABI_V1, NativeRelayAbiRange, NativeRelayTransport,
    ensure_native_relay_abi_compatible,
};

/// Frames one host-carried queue holds before a producer sees backpressure.
const NATIVE_RELAY_QUEUE_MAX_MESSAGES: usize = 1024;
const NATIVE_RELAY_DRAIN_MAX_MESSAGES: usize = 64;
const NATIVE_RELAY_QUEUE_MAX_BYTES: usize = jazz::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;
/// Commands which cross from a platform call into a relay owner must not
/// accumulate without bound.
const NATIVE_RELAY_OWNER_COMMAND_MAX: usize = 1_024;
/// Rounds of one drive before the owner yields to queued commands. A drive
/// that hits it continues on the next owner turn.
const NATIVE_RELAY_DRIVE_ROUNDS: usize = 64;
const NATIVE_RELAY_PUMP_MAX_CLIENTS: usize = 64;
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
    // Versioned, length-delimited ledger owner identity; lengths count UTF-8 bytes.
    pub(crate) fn ledger_owner_encoding(&self) -> String {
        let mut owner = String::from("jazz-native-relay-owner-v1:");
        for field in [
            &self.app_namespace,
            &self.storage_namespace,
            self.auth_scope.as_ref().expect("validated relay scope"),
        ] {
            owner.push_str(&format!("{}:{field}", field.len()));
        }
        owner
    }

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
    pub identity: DbIdentity,
    #[cfg(test)]
    pub(crate) thread_start_counter: Option<Arc<AtomicUsize>>,
}

impl RelayOpenConfig {
    fn validate(&self) -> Result<(), RelayError> {
        ensure_native_relay_abi_compatible(self.supported_abi)?;
        self.scope.validate()
    }
}

// ---- host-carried frame queues ------------------------------------------------------

#[derive(Default)]
struct FrameQueue {
    frames: VecDeque<Vec<u8>>,
    bytes: usize,
    in_flight_frames: usize,
    in_flight_bytes: usize,
}
impl FrameQueue {
    fn drain(&mut self) -> Vec<Vec<u8>> {
        let count = self.frames.len().min(NATIVE_RELAY_DRAIN_MAX_MESSAGES);
        let frames: Vec<_> = self.frames.drain(..count).collect();
        self.bytes -= frames.iter().map(Vec::len).sum::<usize>();
        frames
    }
    fn room(&self) -> (usize, usize) {
        (
            NATIVE_RELAY_QUEUE_MAX_MESSAGES
                .saturating_sub(self.frames.len() + self.in_flight_frames),
            NATIVE_RELAY_QUEUE_MAX_BYTES.saturating_sub(self.bytes + self.in_flight_bytes),
        )
    }
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
    received: Arc<AtomicBool>,
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
        jazz::db::decode_message(frame).map_err(RelayError::DecodePeerMessage)?;
        let mut inbound = Self::lock(&self.inbound);
        if inbound.room().0 == 0 || frame.len() > inbound.room().1 {
            return Err(RelayError::QueueCapacityExceeded {
                direction: "inbound",
                queued_messages: inbound.frames.len(),
            });
        }
        inbound.bytes += frame.len();
        inbound.frames.push_back(frame.to_vec());
        self.received.store(true, Ordering::Release);
        Ok(())
    }

    /// Whether another inbound frame would be admitted now.
    fn inbound_has_room(&self) -> bool {
        let inbound = Self::lock(&self.inbound);
        let (frames, bytes) = inbound.room();
        frames > 0 && bytes >= jazz::protocol_limits::MAX_WIRE_FRAME_BYTES
    }

    /// Drain the frames queued for the server.
    pub fn take_outbound_encoded(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(RelayError::Closed);
        }
        Ok(Self::lock(&self.outbound).drain())
    }

    /// Queued (inbound, outbound) frames.
    pub fn queue_depths(&self) -> (usize, usize) {
        (
            Self::lock(&self.inbound).frames.len(),
            Self::lock(&self.outbound).frames.len(),
        )
    }

    fn take_inbound(&self) -> Vec<Vec<u8>> {
        Self::lock(&self.inbound).drain()
    }

    fn collect_outbound(&self, link: &Link) {
        if self.retired.load(Ordering::Acquire) {
            return;
        }
        let mut outbound = Self::lock(&self.outbound);
        let (frame_budget, byte_budget) = outbound.room();
        let frames = link.take_outbound_with_budget(frame_budget, byte_budget);
        if frames.is_empty() {
            return;
        }
        outbound.bytes += frames.iter().map(Vec::len).sum::<usize>();
        outbound.frames.extend(frames);
        drop(outbound);
        self.outbound_ready.notify_one();
    }

    // The socket reserves its drained batch until sends finish. Otherwise the
    // owner could fill that space while a blocked socket returns unsent frames.
    fn take_socket_batch(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(RelayError::Closed);
        }
        let mut outbound = Self::lock(&self.outbound);
        debug_assert_eq!(outbound.in_flight_frames, 0);
        let frames = outbound.drain();
        outbound.in_flight_frames = frames.len();
        outbound.in_flight_bytes = frames.iter().map(Vec::len).sum();
        Ok(frames)
    }

    fn finish_socket_batch(&self, frames: Vec<Vec<u8>>) {
        let mut outbound = Self::lock(&self.outbound);
        outbound.in_flight_frames = 0;
        outbound.in_flight_bytes = 0;
        outbound.bytes += frames.iter().map(Vec::len).sum::<usize>();
        for frame in frames.into_iter().rev() {
            outbound.frames.push_front(frame);
        }
        debug_assert!(outbound.bytes <= NATIVE_RELAY_QUEUE_MAX_BYTES);
        debug_assert!(outbound.frames.len() <= NATIVE_RELAY_QUEUE_MAX_MESSAGES);
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.outbound_ready.notify_one();
    }
}

// ---- the relay handle ---------------------------------------------------------------

type RelayJob = Box<dyn FnOnce(&mut RelayWorker) + Send + 'static>;

struct CommandSlot(Arc<AtomicUsize>);
impl Drop for CommandSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

enum RelayCommand {
    Run(RelayJob),
    Shutdown(mpsc::Sender<Result<(), RelayError>>),
}

/// Thread-safe handle to one scope's relay owner.
#[derive(Clone)]
pub struct NativeRelay {
    inner: Arc<RelayInner>,
}

struct RelayInner {
    commands: Mutex<Option<mpsc::SyncSender<RelayCommand>>>,
    shutdown_gate: Mutex<()>,
    command_slots: Arc<AtomicUsize>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    alive: Arc<AtomicBool>,
    sqlite_path: PathBuf,
    schema_version: jazz::ids::SchemaVersionId,
    identity: DbIdentity,
    scope_admitted: bool,
    account_host: bool,
    account_root: Option<PathBuf>,
}

impl RelayInner {
    fn shutdown(&self) -> Result<(), RelayError> {
        let _gate = self
            .shutdown_gate
            .lock()
            .map_err(|_| RelayError::Poisoned("relay shutdown"))?;
        self.alive.store(false, Ordering::Release);
        let sender = self
            .commands
            .lock()
            .map_err(|_| RelayError::Poisoned("relay commands"))?
            .clone();
        if let Some(sender) = sender {
            let (done_tx, done_rx) = mpsc::channel();
            match sender.try_send(RelayCommand::Shutdown(done_tx)) {
                Ok(()) => {
                    // A reported close failure retains the owner and leases.
                    // A missing response means the thread ended; join below
                    // before any replacement can acquire the storage owner.
                    if let Ok(result) = done_rx.recv() {
                        result?;
                    }
                }
                Err(mpsc::TrySendError::Full(_)) => return Err(RelayError::OwnerQueueFull),
                Err(mpsc::TrySendError::Disconnected(_)) => {}
            }
            self.commands
                .lock()
                .map_err(|_| RelayError::Poisoned("relay commands"))?
                .take();
        }
        if let Some(join) = self
            .join
            .lock()
            .map_err(|_| RelayError::Poisoned("relay join"))?
            .take()
        {
            join.join()
                .map_err(|_| RelayError::OwnerThread("owner panicked while closing".into()))?;
        }
        Ok(())
    }
}

impl Drop for RelayInner {
    fn drop(&mut self) {
        let _ = self.shutdown();
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
        Self::spawn_with_scope(config, None)
    }

    fn spawn_with_scope(
        config: RelayOpenConfig,
        admitted: Option<ClientRelayScope>,
    ) -> Result<Self, RelayError> {
        let scope_admitted = admitted.is_some();
        let account_host = admitted.as_ref().is_some_and(|scope| scope.account_host);
        let account_root = admitted
            .as_ref()
            .and_then(|scope| scope.account_root.clone());
        // Before the thread exists and before SQLite is touched.
        config.validate()?;
        let sqlite_path = config.sqlite_path.clone();
        let schema_version = config.schema.version_id();
        let identity = config.identity;
        let alive = Arc::new(AtomicBool::new(true));
        let (commands, receiver) =
            mpsc::sync_channel::<RelayCommand>(NATIVE_RELAY_OWNER_COMMAND_MAX + 1);
        let (started_tx, started_rx) = mpsc::channel();
        let owner_alive = Arc::clone(&alive);
        let join = thread::Builder::new()
            .name("jazz-native-relay".to_owned())
            .spawn(move || {
                let _alive = OwnerAlive(owner_alive);
                #[cfg(test)]
                if let Some(counter) = &config.thread_start_counter {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                let mut worker = match RelayWorker::open(config, admitted) {
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
                shutdown_gate: Mutex::new(()),
                command_slots: Arc::new(AtomicUsize::new(0)),
                join: Mutex::new(Some(join)),
                alive,
                sqlite_path,
                schema_version,
                identity,
                scope_admitted,
                account_host,
                account_root,
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

    pub(crate) fn host_upstream_hint(&self) -> Result<RemoteLinkHint, RelayError> {
        self.run(|worker| {
            Ok(
                if worker.upstream.as_ref().is_some_and(|upstream| {
                    upstream.wire.received.load(Ordering::Acquire)
                        && !upstream.wire.retired.load(Ordering::Acquire)
                }) {
                    RemoteLinkHint::Live
                } else {
                    RemoteLinkHint::NoServer
                },
            )
        })
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
            worker.set_remote_link_hint(hint);
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
        self.enqueue(operation)?.recv().map_err(|_| {
            self.inner.alive.store(false, Ordering::Release);
            RelayError::Closed
        })?
    }

    /// Admit work independently of waiting for its response. Dropping the
    /// receiver cannot cancel a command that already owns a queue slot.
    fn enqueue<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<mpsc::Receiver<Result<T, RelayError>>, RelayError> {
        if !self.is_alive() {
            return Err(RelayError::Closed);
        }
        self.inner
            .command_slots
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used < NATIVE_RELAY_OWNER_COMMAND_MAX).then_some(used + 1)
            })
            .map_err(|_| RelayError::OwnerQueueFull)?;
        let slot = CommandSlot(self.inner.command_slots.clone());
        let (response_tx, response_rx) = mpsc::channel();
        let job: RelayJob = Box::new(move |worker| {
            let _slot = slot;
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
        Ok(response_rx)
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
        self.relay.run(move |worker| {
            let client = worker.client(id)?;
            let result = operation(&client.binding.db);
            // Direct core calls do not pass through Binding's scheduling
            // hooks. Drive their writes, queued reads and cleanup before the
            // owner sleeps, including after a partially completed operation.
            client.dirty.set(true);
            result
        })
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
    author: AuthorSubject,
    claims: BTreeMap<String, Value>,
    /// The relay's persistent database. Permission advice is asked of it:
    /// it is the replica linked to the authority, for the same author, and
    /// a relay answers its own subscribers' advice requests `Unknown`.
    authority: Binding,
    dirty: Rc<Cell<bool>>,
    deadline: Rc<Cell<Option<Instant>>>,
    /// The foreground's upstream link to the relay.
    link: Link,
    /// The relay's subscriber link serving it.
    served: Link,
    subscriptions: BTreeMap<u64, ForegroundSubscription>,
    pending: BTreeMap<u64, PendingOperation>,
    /// Results of pending operations that completed before being polled.
    completed: BTreeMap<u64, ForegroundDbCommandResponse>,
    transaction_reads: BTreeMap<u64, String>,
    sealing_operations: BTreeSet<u64>,
    completed_seals: BTreeMap<u64, String>,
    transactions: BTreeMap<u64, (String, ForegroundTransactionKind)>,
    writes: HashMap<TransactionId, TrackedWrite>,
    write_order: VecDeque<TransactionId>,
    uploads: BTreeMap<u64, binding_host::StreamingMutation>,
    staged: BTreeMap<u64, binding_host::StagedStreamingMutation>,
    initializations: Rc<RefCell<BTreeMap<String, InitializationSlot>>>,
    initialization_slots: Rc<Cell<usize>>,
    catalogue_cache: Option<Rc<RefCell<CatalogueCache>>>,
    mutation_errors: Rc<RefCell<Vec<MutationErrorReport>>>,
    /// The platform callback and the host handle it is called with.
    wake: Option<(u64, Arc<ForegroundWakeState>)>,
    /// Something changed for this foreground since the drive began.
    notify: bool,
    next_handle: u64,
    #[cfg(test)]
    close_error_for_test: Option<HostError>,
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
                if self.pending.len() + self.completed.len() >= NATIVE_RELAY_FOREGROUND_PENDING_MAX
                {
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
            let response = operation_response(result);
            if self.sealing_operations.remove(&handle)
                && let ForegroundDbCommandResponse::Rows { rows } = &response
                && let Ok(value) = serde_json::from_slice::<serde_json::Value>(rows)
                && let Some(token) = value.get("token").and_then(|token| token.as_str())
            {
                self.completed_seals.insert(handle, token.to_owned());
            }
            self.completed.insert(handle, response);
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
                    EventChunk::Rejected { detail } => {
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

    fn wake(&mut self) {
        if !std::mem::take(&mut self.notify) {
            return;
        }
        if let Some((handle, wake)) = &self.wake {
            wake.wake(*handle, FOREGROUND_WAKE_IMMEDIATE, 0);
        }
    }

    fn close(&mut self) -> Result<(), HostError> {
        // The owner drive must retain these writes before the foreground's
        // in-memory projection and links are retired.
        let mut durable = self.binding.wait_for_pending_writes(DurabilityTier::Local);
        if let Poll::Ready(result) = poll_once(&mut durable) {
            result?;
        } else {
            return Err("foreground writes are waiting for the durable owner; retry close".into());
        }
        // Inject at the real close boundary, after durable-local validation.
        // This tests host aggregation without corrupting an in-memory database.
        #[cfg(test)]
        if let Some(error) = self.close_error_for_test.take() {
            return Err(error);
        }
        self.binding.db.stop_mutation_admission();
        for (_, operation) in std::mem::take(&mut self.pending) {
            if let Some(id) = operation.advice {
                self.authority.cancel_permission_advice(id);
            }
        }
        self.initializations.borrow_mut().clear();
        for (_, (id, _)) in std::mem::take(&mut self.transactions) {
            self.binding.rollback_transaction(&id)?;
        }
        for (_, mut upload) in std::mem::take(&mut self.uploads) {
            upload.abort()?;
        }
        for (_, mut staged) in std::mem::take(&mut self.staged) {
            staged.abort()?;
        }
        self.subscriptions.clear();
        block_on(self.binding.tick())?;
        self.binding.close_initializations();
        self.binding.db.retire_storage_owner()?;
        self.link.close();
        self.served.close();
        Ok(())
    }
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

pub(crate) struct RelayWorker {
    schema: JazzSchema,
    persistent: Binding,
    // Keep both exclusive leases until the upstream host retires the owner.
    // Retain file locks until the owner thread has released its databases.
    _storage: AdmittedStorage,
    catalogue_cache: Option<Rc<RefCell<CatalogueCache>>>,
    closing: bool,
    persistent_dirty: Rc<Cell<bool>>,
    persistent_deadline: Rc<Cell<Option<Instant>>>,
    remote_hint: RemoteLinkHint,
    upstream: Option<UpstreamConnection>,
    generation: u64,
    clients: BTreeMap<u64, Foreground>,
    next_client: u64,
    /// The last drive failed; reported by the next foreground tick.
    drive_error: Option<String>,
    /// The last drive stopped at its round bound with work left.
    redrive: bool,
    /// Continue a bounded sweep without restarting at the first foreground.
    drive_cursor: Option<u64>,
    drive_remaining: usize,
    #[cfg(test)]
    core_tick_turns: u64,
}

/// Select at most one foreground batch, resuming strictly after the prior
/// cursor and wrapping once. Range iteration avoids collecting every client.
fn bounded_round_robin_ids<T>(clients: &BTreeMap<u64, T>, after: Option<u64>) -> Vec<u64> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    match after {
        Some(after) => clients
            .range((Excluded(after), Unbounded))
            .chain(clients.range((Unbounded, Included(after))))
            .take(NATIVE_RELAY_PUMP_MAX_CLIENTS)
            .map(|(&id, _)| id)
            .collect(),
        None => clients
            .keys()
            .take(NATIVE_RELAY_PUMP_MAX_CLIENTS)
            .copied()
            .collect(),
    }
}

/// Core deadlines, including first-load fallback and connection-attempt
/// windows, are authoritative. Untimed remote waits need no polling timer.
struct OwnerTickScheduler {
    dirty: Rc<Cell<bool>>,
    deadline: Rc<Cell<Option<Instant>>>,
}
impl TickScheduler for OwnerTickScheduler {
    fn schedule_tick(&self, _urgency: TickUrgency) {
        self.dirty.set(true);
    }
    fn schedule_tick_after(&self, delay_ms: u64) {
        let Some(at) = Instant::now().checked_add(Duration::from_millis(delay_ms)) else {
            return;
        };
        if self.deadline.get().is_none_or(|previous| at < previous) {
            self.deadline.set(Some(at));
        }
    }
}
fn hook(binding: &Binding) -> (Rc<Cell<bool>>, Rc<Cell<Option<Instant>>>) {
    let dirty = Rc::new(Cell::new(true));
    let deadline = Rc::new(Cell::new(None));
    let flag = Rc::clone(&dirty);
    *binding.hooks.schedule_tick.borrow_mut() = Some(Box::new(move || flag.set(true)));
    binding
        .db
        .set_tick_scheduler(Some(Rc::new(OwnerTickScheduler {
            dirty: dirty.clone(),
            deadline: deadline.clone(),
        })));
    (dirty, deadline)
}

impl RelayWorker {
    fn open(
        config: RelayOpenConfig,
        admitted: Option<ClientRelayScope>,
    ) -> Result<Self, RelayError> {
        let catalogue_cache = admitted
            .as_ref()
            .map(|_| CatalogueCache::load(&config))
            .transpose()?;
        let cached = catalogue_cache
            .as_ref()
            .and_then(|cache| cache.capture.as_deref());
        let storage = AdmittedStorage::open_persistent(&config, admitted.as_ref(), cached)?;
        let persistent = storage.binding.clone();
        let (persistent_dirty, persistent_deadline) = hook(&persistent);
        persistent.db.set_remote_link_hint(RemoteLinkHint::NoServer);
        Ok(Self {
            schema: config.schema,
            persistent,
            _storage: storage,
            catalogue_cache: catalogue_cache.map(|cache| Rc::new(RefCell::new(cache))),
            closing: false,
            persistent_dirty,
            persistent_deadline,
            remote_hint: RemoteLinkHint::NoServer,
            upstream: None,
            generation: 0,
            clients: BTreeMap::new(),
            next_client: 0,
            drive_error: None,
            redrive: false,
            drive_cursor: None,
            drive_remaining: 0,
            #[cfg(test)]
            core_tick_turns: 0,
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
            } else if let Some(deadline) = self.next_deadline() {
                match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
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
                Some(RelayCommand::Run(job)) => {
                    if !self.closing {
                        job(self);
                    }
                }
                Some(RelayCommand::Shutdown(done)) => {
                    self.closing = true;
                    let result = self.shutdown();
                    let complete = result.is_ok();
                    let _ = done.send(result);
                    if complete {
                        return;
                    }
                }
                None => {}
            }
            self.wake_due_deadlines();
            self.drive();
        }
        let _ = self.shutdown();
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.persistent_deadline
            .get()
            .into_iter()
            .chain(
                self.clients
                    .values()
                    .filter_map(|client| client.deadline.get()),
            )
            .min()
    }

    fn wake_due_deadlines(&self) {
        let now = Instant::now();
        if self.persistent_deadline.get().is_some_and(|at| at <= now) {
            self.persistent_deadline.set(None);
            self.persistent_dirty.set(true);
        }
        for client in self.clients.values() {
            if client.deadline.get().is_some_and(|at| at <= now) {
                client.deadline.set(None);
                client.dirty.set(true);
            }
        }
    }

    fn set_remote_link_hint(&mut self, hint: RemoteLinkHint) {
        self.remote_hint = hint;
        self.persistent.db.set_remote_link_hint(hint);
        self.persistent.hooks.progress.wake_all();
        self.persistent_dirty.set(true);
        for client in self.clients.values() {
            client.binding.db.set_remote_link_hint(hint);
            client.binding.hooks.progress.wake_all();
            client.dirty.set(true);
        }
    }

    fn shutdown(&mut self) -> Result<(), RelayError> {
        self.drive();
        let ids = self.clients.keys().copied().collect::<Vec<_>>();
        let mut close_error = None;
        for id in ids {
            match self.clients.get_mut(&id).expect("live foreground").close() {
                Ok(()) => {
                    self.clients.remove(&id);
                }
                Err(error) => {
                    // Retain this foreground for retry, but still close every
                    // independent sibling. The shared owner and its storage
                    // leases remain live until all foregrounds finish.
                    if close_error.is_none() {
                        close_error = Some(RelayError::Host(error));
                    }
                }
            }
        }
        if let Some(error) = close_error {
            return Err(error);
        }
        self.persistent.db.stop_mutation_admission();
        self.persistent.close_initializations();
        if let Some(mut upstream) = self.upstream.take() {
            upstream.link.close();
            upstream.wire.retire();
        }
        block_on(self.persistent.tick()).map_err(RelayError::Host)?;
        if let Some(cache) = &self.catalogue_cache {
            match cache
                .borrow_mut()
                .poll_refresh(&self.persistent.db, &mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(result) => {
                    result?;
                }
                Poll::Pending => {
                    return Err(RelayError::Drive(
                        "catalogue flush is pending; retry close".into(),
                    ));
                }
            }
        }
        self.persistent
            .db
            .retire_storage_owner()
            .map_err(RelayError::Db)?;
        Ok(())
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
        let db = block_on(Db::open(DbConfig::new(self.schema.clone(), identity)))
            .map_err(|error| RelayError::Host(error.into()))?;
        // Continue after the node's previous holder: its transaction ids
        // were (seq, node), and the relay deduplicates by them.
        if high_water > 0 {
            db.seed_seq_floor(high_water);
        }
        if let Some(capture) = self
            .catalogue_cache
            .as_ref()
            .and_then(|cache| cache.borrow().capture.clone())
        {
            // SAFETY: the host loaded this exact app/environment cache after admission.
            unsafe { db.install_cached_catalogue(&capture) }.map_err(RelayError::Db)?;
        }
        db.set_identity_claims(identity.author, claims.clone());
        let binding = Binding::new(db, identity, false);
        if self.persistent.db.is_scope_isolated_storage_owner() {
            binding
                .set_private_owner_client()
                .map_err(RelayError::Host)?;
        } else {
            binding.set_non_durable_client();
        }
        let (dirty, deadline) = hook(&binding);
        let mutation_errors = Rc::new(RefCell::new(Vec::new()));
        let reports = Rc::clone(&mutation_errors);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, claimed| {
                reports.borrow_mut().push(MutationErrorReport {
                    claimed,
                    event: mutation_error_event(&tx_id.to_string(), error, serializable, true),
                });
            }));
        self.persistent
            .db
            .set_identity_claims(identity.author, claims.clone());
        let served = self
            .persistent
            .accept_subscriber(identity.author, claims.clone());
        served
            .set_outbound_limits(
                NATIVE_RELAY_QUEUE_MAX_MESSAGES,
                NATIVE_RELAY_QUEUE_MAX_BYTES,
            )
            .map_err(RelayError::Host)?;
        let link = binding.connect_upstream();
        binding.db.set_remote_link_hint(self.remote_hint);
        link.set_outbound_limits(
            NATIVE_RELAY_QUEUE_MAX_MESSAGES,
            NATIVE_RELAY_QUEUE_MAX_BYTES,
        )
        .map_err(RelayError::Host)?;
        self.next_client += 1;
        let id = self.next_client;
        self.clients.insert(
            id,
            Foreground {
                binding,
                author: identity.author,
                claims,
                authority: self.persistent.clone(),
                dirty,
                deadline,
                link,
                served,
                subscriptions: BTreeMap::new(),
                pending: BTreeMap::new(),
                completed: BTreeMap::new(),
                transaction_reads: BTreeMap::new(),
                sealing_operations: BTreeSet::new(),
                completed_seals: BTreeMap::new(),
                transactions: BTreeMap::new(),
                writes: HashMap::new(),
                write_order: VecDeque::new(),
                uploads: BTreeMap::new(),
                staged: BTreeMap::new(),
                initializations: Rc::default(),
                initialization_slots: Rc::default(),
                catalogue_cache: self.catalogue_cache.clone(),
                mutation_errors,
                wake: None,
                notify: false,
                next_handle: 0,
                #[cfg(test)]
                close_error_for_test: None,
            },
        );
        self.drive();
        Ok(id)
    }

    fn detach(&mut self, id: u64) -> Result<(), RelayError> {
        self.drive();
        self.client_mut(id)?.close().map_err(RelayError::Host)?;
        self.clients.remove(&id);
        self.persistent_dirty.set(true);
        self.drive();
        Ok(())
    }

    fn refresh_claims(&mut self, author: AuthorSubject, claims: BTreeMap<String, Value>) {
        self.persistent
            .db
            .set_identity_claims(author, claims.clone());
        self.persistent.hooks.progress.wake_all();
        self.persistent_dirty.set(true);
        for client in self.clients.values_mut() {
            if client.author == author && client.claims != claims {
                client.claims = claims.clone();
                for operation in client.pending.values() {
                    if let Some(id) = operation.advice {
                        client.authority.cancel_permission_advice(id);
                    }
                }
                for response in client.completed.values_mut() {
                    if matches!(
                        response,
                        ForegroundDbCommandResponse::PermissionAdvice { .. }
                    ) {
                        *response = ForegroundDbCommandResponse::PermissionAdvice {
                            advice: ForegroundPermissionAdvice::Unknown,
                        };
                        client.notify = true;
                    }
                }
            }
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
        if let Some(mut previous) = self.upstream.take() {
            previous.link.close();
            previous.wire.retire();
        }
        self.generation += 1;
        let link = self.persistent.connect_upstream();
        link.set_outbound_limits(
            NATIVE_RELAY_QUEUE_MAX_MESSAGES,
            NATIVE_RELAY_QUEUE_MAX_BYTES,
        )
        .expect("fresh link admits native queue limits");
        self.set_remote_link_hint(RemoteLinkHint::Live);
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
            self.set_remote_link_hint(RemoteLinkHint::NoServer);
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

    /// Tick what asked for a tick and move frames until nothing moves.
    pub(crate) fn drive(&mut self) {
        self.redrive = false;
        if self.drive_remaining == 0 {
            self.drive_remaining = self.clients.len();
        }
        let ids = bounded_round_robin_ids(&self.clients, self.drive_cursor);
        self.drive_cursor = ids.last().copied();
        self.drive_remaining = self.drive_remaining.saturating_sub(ids.len());
        let mut touched = false;
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
            for id in &ids {
                let client = &self.clients[id];
                if client.dirty.replace(false) {
                    #[cfg(test)]
                    {
                        self.core_tick_turns += 1;
                    }
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
                #[cfg(test)]
                {
                    self.core_tick_turns += 1;
                }
                if let Err(error) = block_on(self.persistent.tick()) {
                    self.drive_error = Some(error.to_string());
                }
                progressed = true;
            }
            if let Some(cache) = &self.catalogue_cache
                && let Poll::Ready(Err(error)) = cache
                    .borrow_mut()
                    .poll_refresh(&self.persistent.db, &mut Context::from_waker(Waker::noop()))
            {
                self.drive_error = Some(error.to_string());
            }
            for id in &ids {
                let client = &self.clients[id];
                let frames = client.served.take_outbound();
                if !frames.is_empty() {
                    client.link.receive(frames);
                    progressed = true;
                }
            }
            if let Some(upstream) = &self.upstream {
                upstream.wire.collect_outbound(&upstream.link);
            }
            if !progressed {
                break;
            }
            touched = true;
        }
        // Progress may have sent a response to a foreground outside this
        // batch. Complete one bounded sweep before parking; a quiet sweep
        // must not reschedule itself forever.
        if touched {
            self.drive_remaining = self.clients.len().saturating_sub(ids.len());
        }
        self.redrive |= self.drive_remaining > 0;
        for id in ids {
            let client = self
                .clients
                .get_mut(&id)
                .expect("selected foreground remains attached");
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

/// Refuse selectors that this mutation kind would otherwise silently ignore.
fn validate_mutation_options(kind: ForegroundMutationKind, options: &str) -> Result<(), HostError> {
    let options = json(options)?;
    let unsupported: &[&str] = match kind {
        ForegroundMutationKind::Insert | ForegroundMutationKind::Restore => &["head", "base"],
        ForegroundMutationKind::Update
        | ForegroundMutationKind::Delete
        | ForegroundMutationKind::Upsert => &["branch"],
    };
    for key in unsupported {
        if options.get(*key).is_some() {
            return Err(format!("{} option `{key}` is not supported", mutation_name(kind)).into());
        }
    }
    Ok(())
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
    validate_mutation_options(mutation, options)?;
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
            let response = client.pending_or_ready(
                Box::pin(async move { Ok(Response::Rows { rows: read.await? }) }),
                None,
            )?;
            if let (Some(transaction), Response::Pending { operation }) = (transaction, &response) {
                client.transaction_reads.insert(*operation, transaction);
            }
            response
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
        Request::Poll { operation } => {
            client.completed_seals.remove(&operation);
            let response = match client.completed.remove(&operation) {
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
            };
            if !matches!(response, Response::Pending { .. }) {
                client.transaction_reads.remove(&operation);
                client.sealing_operations.remove(&operation);
            }
            response
        }
        Request::Cancel { operation } => {
            client.transaction_reads.remove(&operation);
            client.sealing_operations.remove(&operation);
            if let Some(token) = client.completed_seals.remove(&operation) {
                client.initializations.borrow_mut().remove(&token);
            }
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
            if client.transactions.len() + client.initialization_slots.get()
                >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX
            {
                return Err(format!(
                    "foreground transaction capacity {} exceeded",
                    NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX
                )
                .into());
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
                    let reads = client
                        .transaction_reads
                        .iter()
                        .filter_map(|(operation, transaction)| {
                            (transaction == &id).then_some(*operation)
                        })
                        .collect::<Vec<_>>();
                    for operation in reads {
                        client.pending.remove(&operation);
                        client.transaction_reads.remove(&operation);
                        client.completed.insert(
                            operation,
                            operation_error(HostError::Engine(jazz::db::Error::new(
                                jazz::db::ErrorCode::NotObserved,
                                "transaction rolled back before read delivery",
                            ))),
                        );
                    }
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
                DurabilityTier::None => Response::Rows { rows: Vec::new() },
                DurabilityTier::Local => {
                    let db = binding.db.clone();
                    client.pending_or_ready(
                        Box::pin(async move {
                            db.wait_for_pending_writes(DurabilityTier::Local).await?;
                            Ok(Response::Rows { rows: Vec::new() })
                        }),
                        None,
                    )?
                }
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
        Request::WriteState { tx_id } => {
            let tx_id = TransactionId(tx_id);
            let tracked = client
                .writes
                .get(&tx_id)
                .ok_or_else(|| HostError::Argument(format!("unknown foreground write {tx_id}")))?;
            let state = client.binding.write_state(&tracked.write)?;
            Response::WriteState {
                state_json: serde_json::to_string(&state)
                    .map_err(|error| HostError::Argument(error.to_string()))?,
            }
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
        Request::BeginStreamingMutation {
            mutation,
            table,
            row_id,
            cells,
            column,
            options_json,
        } => {
            if client.uploads.len() + client.staged.len() >= NATIVE_RELAY_FOREGROUND_PENDING_MAX {
                return Err("foreground streaming upload capacity exceeded".into());
            }
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
            let mutation = client.uploads.get_mut(&upload).ok_or_else(|| {
                HostError::Argument(format!("unknown streaming mutation {upload}"))
            })?;
            let write = mutation.finish()?;
            client.uploads.remove(&upload);
            let (tx_id, _) = client.track(write);
            Response::TransactionCommitted {
                tx_id: *tx_id.as_bytes(),
            }
        }
        Request::StageStreamingMutation { upload } => {
            let mutation = client.uploads.get_mut(&upload).ok_or_else(|| {
                HostError::Argument(format!("unknown streaming mutation {upload}"))
            })?;
            let staged = mutation.stage()?;
            client.uploads.remove(&upload);
            let handle = client.handle();
            client.staged.insert(handle, staged);
            Response::StreamingMutationStaged { staged: handle }
        }
        Request::AttachStagedStreamingMutation {
            staged,
            transaction,
        } => {
            let (id, _) = client.transaction(transaction)?.clone();
            let mut mutation = client
                .staged
                .remove(&staged)
                .ok_or("unknown staged streaming mutation")?;
            mutation.attach(&id)?;
            Response::StagedStreamingMutationAttached
        }
        Request::AbortStagedStreamingMutation { staged } => {
            Response::StagedStreamingMutationAborted {
                aborted: match client.staged.remove(&staged) {
                    Some(mut mutation) => mutation.abort()?,
                    None => false,
                },
            }
        }
        Request::InitializationV1 { version, action } => {
            initialization_command(client, version, action)?
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
            validate_mutation_options(mutation, &options_json)?;
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
        | Request::NativeSessionMetadata => {
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
    let wait = client.binding.wait_for_write(&tracked.write, tier)?;
    if !observe_only {
        tracked.claimed.set(true);
    }
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
    pub fn open(&self, config: RelayOpenConfig) -> Result<NativeRelay, RelayError> {
        self.open_with_scope(config, None)
    }

    pub(crate) fn open_admitted(&self, config: RelayOpenConfig) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        // SAFETY: callers resolved an immutable trusted host admission capability;
        // arbitrary public RelayScope text never reaches this constructor.
        let scope = unsafe {
            ClientRelayScope::from_admitted_storage_owner(
                config.scope.ledger_owner_encoding(),
                config.identity.author,
            )
        };
        self.open_with_scope(config, Some(scope))
    }

    pub(crate) fn open_account_owner(
        &self,
        config: RelayOpenConfig,
        account_root: PathBuf,
    ) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        // SAFETY: only the native account-session capability route calls this
        // method. Arbitrary private-scope JSON admission still uses the exact
        // subject route above, even if it names the same account UUID.
        let scope = unsafe {
            ClientRelayScope::from_admitted_account_owner(
                config.scope.ledger_owner_encoding(),
                config.identity.author,
                account_root,
            )
        };
        self.open_with_scope(config, Some(scope))
    }

    fn open_with_scope(
        &self,
        config: RelayOpenConfig,
        client_relay_scope: Option<ClientRelayScope>,
    ) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        if let Some(existing) = relays.get(&config.scope) {
            if existing.inner.sqlite_path != config.sqlite_path
                || existing.inner.schema_version != config.schema.version_id()
                || existing.inner.identity != config.identity
                || existing.inner.scope_admitted != client_relay_scope.is_some()
                || existing.inner.account_host
                    != client_relay_scope
                        .as_ref()
                        .is_some_and(|scope| scope.account_host)
                || existing.inner.account_root.as_ref()
                    != client_relay_scope
                        .as_ref()
                        .and_then(|scope| scope.account_root.as_ref())
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            if existing.is_alive() {
                return Ok(existing.clone());
            }
            let stale = existing.clone();
            match stale.inner.shutdown() {
                Ok(()) => {}
                // shutdown has joined the panicked thread and released its
                // leases. Every alias stays terminal; only this registry may
                // now open a separate replacement runtime.
                Err(RelayError::OwnerThread(_)) => {
                    debug_assert!(stale.inner.join.lock().unwrap().is_none());
                }
                Err(error) => return Err(error),
            }
            relays.remove(&config.scope);
        }
        // Linked identities share an account's durable root, but never its
        // live authorization scope. Keep the old owner exclusive until close
        // has joined it, including when that owner has become terminal.
        if relays
            .values()
            .any(|relay| relay.inner.sqlite_path == config.sqlite_path)
        {
            return Err(RelayError::StorageAlreadyOwned);
        }
        let relay = NativeRelay::spawn_with_scope(config.clone(), client_relay_scope)?;
        relays.insert(config.scope, relay.clone());
        Ok(relay)
    }

    pub fn close(&self, scope: &RelayScope) -> Result<bool, RelayError> {
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        let Some(relay) = relays.get(scope).cloned() else {
            return Ok(false);
        };
        relay.inner.shutdown()?;
        relays.remove(scope);
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

/// Private native socket inputs. This is intentionally absent from postcard
/// commands and diagnostics: JavaScript can neither configure the endpoint
/// nor read its bearer.
pub struct NativeRelaySocketConfig {
    pub server_url: String,
    pub app_id: AppId,
    pub peer_identity: AuthorSubject,
    pub auth: AuthConfig,
    pub reconnect_delay: Duration,
    pub on_event: Arc<dyn Fn(NativeRelaySocketEvent) + Send + Sync>,
}

impl std::fmt::Debug for NativeRelaySocketConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeRelaySocketConfig")
            .field("server_url", &self.server_url)
            .field("app_id", &self.app_id)
            .field("peer_identity", &self.peer_identity)
            .field("auth", &"<redacted>")
            .field("reconnect_delay", &self.reconnect_delay)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeRelaySocketEvent {
    Connected,
    Reconnecting,
    /// A connection or transport failed non-retryably; the worker retries
    /// unless `Stopped` follows. The scope-owned host surfaces a failure
    /// before `Connected` (the upstream rejected the session), or one the
    /// worker stops on, to foreground ticks until a later `Connected`.
    TerminalError(String),
    Stopped,
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
        if config.peer_identity == AuthorSubject::SYSTEM
            || config.auth.jwt_token.as_deref().is_none_or(str::is_empty)
            || config.auth.backend_secret.is_some()
            || config.auth.admin_secret.is_some()
            || config.auth.backend_session.is_some()
        {
            return Err(RelayError::Socket(
                "native relay sockets require an ordinary non-SYSTEM bearer session".to_owned(),
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
    /// The link ended; the worker reconnects.
    Reconnect,
    /// The relay is gone or failed: the worker stops.
    Stop,
}

async fn run_socket_worker(
    relay: NativeRelay,
    config: NativeRelaySocketConfig,
    connector: Arc<dyn NativeTransportConnector>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
) {
    while !cancelled.load(Ordering::Acquire) {
        if !relay.is_alive() {
            (config.on_event)(NativeRelaySocketEvent::TerminalError(
                RelayError::Closed.to_string(),
            ));
            break;
        }
        let inbound_wake = Arc::clone(&wake);
        let request = NativeTransportRequest {
            requested_link: NativeTransportLink::OrdinarySession,
            server_url: config.server_url.clone(),
            app_id: config.app_id,
            peer_identity: config.peer_identity,
            auth: config.auth.clone(),
            wake: Arc::new(move || inbound_wake.notify_one()),
        };
        let connected = tokio::select! {
            result = connector.connect(request) => result,
            _ = wake.notified() => {
                if cancelled.load(Ordering::Acquire) {
                    break;
                }
                continue;
            }
        };
        let connected = match connected {
            Ok(connected) => connected,
            Err(error) => {
                if !error.is_retryable() {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                        "native relay socket connect failed: {error}"
                    )));
                    break;
                }
                (config.on_event)(NativeRelaySocketEvent::Reconnecting);
                tokio::select! {
                    _ = tokio::time::sleep(config.reconnect_delay) => {},
                    _ = wake.notified() => {},
                }
                continue;
            }
        };
        let (generation, wire) = loop {
            if cancelled.load(Ordering::Acquire) {
                (config.on_event)(NativeRelaySocketEvent::Stopped);
                return;
            }
            match relay.begin_socket_upstream() {
                Ok(installed) => break installed,
                Err(RelayError::OwnerQueueFull) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(error) => {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                        "native relay upstream install failed: {error}"
                    )));
                    (config.on_event)(NativeRelaySocketEvent::Stopped);
                    return;
                }
            }
        };
        let lease = SocketUpstreamLease {
            relay: relay.clone(),
            generation,
            wire,
        };
        (config.on_event)(NativeRelaySocketEvent::Connected);
        let end = bridge_connection(
            &relay,
            &lease.wire,
            connected.transport,
            connected.terminal,
            &config,
            &cancelled,
            &wake,
        )
        .await;
        drop(lease);
        match end {
            LinkEnd::Cancelled | LinkEnd::Stop => break,
            LinkEnd::Reconnect => {
                if !cancelled.load(Ordering::Acquire) {
                    (config.on_event)(NativeRelaySocketEvent::Reconnecting);
                }
            }
        }
    }
    (config.on_event)(NativeRelaySocketEvent::Stopped);
}

#[derive(Debug)]
enum NativeRelayWireBridgeError {
    SocketPumpClosed,
    Relay(RelayError),
}

fn check_native_carrier<W: WireTransport>(
    upstream: &NativeRelayTransport<W>,
) -> Result<(), NativeRelayWireBridgeError> {
    upstream.check().map_err(|error| {
        if upstream
            .remote_wire_error()
            .is_some_and(|error| error.asks_reconnect_later())
        {
            NativeRelayWireBridgeError::SocketPumpClosed
        } else {
            NativeRelayWireBridgeError::Relay(RelayError::ForegroundCommand(format!(
                "native upstream receive: {error:?}"
            )))
        }
    })
}

fn bridge_native_relay_wire_once_classified<W: WireTransport>(
    wire: &NativeRelayWire,
    upstream: &mut NativeRelayTransport<W>,
) -> Result<bool, NativeRelayWireBridgeError> {
    check_native_carrier(upstream)?;
    let mut progressed = false;
    let batch = wire
        .take_socket_batch()
        .map_err(NativeRelayWireBridgeError::Relay)?;
    let mut unsent = batch.into_iter();
    let mut blocked = Vec::new();
    let mut failure = None;
    while let Some(frame) = unsent.next() {
        match upstream.send(frame.clone()) {
            Ok(()) => progressed = true,
            Err(TransportError::Backpressure) => {
                blocked = std::iter::once(frame).chain(unsent.by_ref()).collect();
                break;
            }
            Err(TransportError::Failed(message)) if message == "websocket pump is closed" => {
                failure = Some(NativeRelayWireBridgeError::SocketPumpClosed);
                break;
            }
            Err(error) => {
                failure = Some(NativeRelayWireBridgeError::Relay(
                    RelayError::ForegroundCommand(format!("native upstream send: {error:?}")),
                ));
                break;
            }
        }
    }
    wire.finish_socket_batch(blocked);
    if let Some(error) = failure {
        return Err(error);
    }
    for _ in 0..NATIVE_RELAY_DRAIN_MAX_MESSAGES {
        if !wire.inbound_has_room() {
            break;
        }
        let Some(frame) = upstream.receive() else {
            check_native_carrier(upstream)?;
            break;
        };
        wire.push_inbound_encoded(&frame)
            .map_err(NativeRelayWireBridgeError::Relay)?;
        progressed = true;
    }
    Ok(progressed)
}

/// Move frames between one established connection and the relay until the
/// connection ends.
async fn bridge_connection(
    relay: &NativeRelay,
    wire: &NativeRelayWire,
    transport: Box<dyn WireTransport + Send>,
    mut terminal: jazz::tools::native_transport_connector::NativeTransportTerminalFuture,
    config: &NativeRelaySocketConfig,
    cancelled: &AtomicBool,
    wake: &tokio::sync::Notify,
) -> LinkEnd {
    let mut transport = NativeRelayTransport::current(transport);
    let terminal_end = |terminal: NativeTransportTerminal| {
        if let NativeTransportTerminal::Failed(NativeTransportError::Terminal(error)) = terminal {
            (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                "native relay socket terminated: {error}"
            )));
        }
        LinkEnd::Reconnect
    };
    loop {
        if cancelled.load(Ordering::Acquire) {
            return LinkEnd::Cancelled;
        }
        if !relay.is_alive() {
            (config.on_event)(NativeRelaySocketEvent::TerminalError(
                RelayError::Closed.to_string(),
            ));
            return LinkEnd::Stop;
        }
        let (progressed, failed) =
            match bridge_native_relay_wire_once_classified(wire, &mut transport) {
                Ok(progressed) => (progressed, false),
                Err(NativeRelayWireBridgeError::SocketPumpClosed) => (false, true),
                Err(NativeRelayWireBridgeError::Relay(error)) => {
                    if !cancelled.load(Ordering::Acquire) {
                        (config.on_event)(NativeRelaySocketEvent::TerminalError(error.to_string()));
                    }
                    return LinkEnd::Stop;
                }
            };
        if failed {
            // The socket's own terminal classifies the failure, but neither
            // cancellation nor owner retirement waits for a lost terminal.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if cancelled.load(Ordering::Acquire) {
                    return LinkEnd::Cancelled;
                }
                if !relay.is_alive() {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(
                        RelayError::Closed.to_string(),
                    ));
                    return LinkEnd::Stop;
                }
                tokio::select! {
                    terminal = &mut terminal => return terminal_end(terminal),
                    _ = wake.notified() => {},
                    _ = tokio::time::sleep_until(deadline) => return LinkEnd::Reconnect,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                }
            }
        }
        if progressed
            && let Err(error) = relay.pump()
            && !matches!(error, RelayError::OwnerQueueFull)
        {
            if !cancelled.load(Ordering::Acquire) {
                (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                    "native relay owner pump failed: {error}"
                )));
            }
            return LinkEnd::Stop;
        }
        tokio::select! {
            terminal = &mut terminal => return terminal_end(terminal),
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
    #[error("invalid native relay semantic frame: {0}")]
    DecodePeerMessage(postcard::Error),
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
    #[error("Jazz database failed: {0}")]
    Db(jazz::db::Error),
    #[error("foreground NativeDb command failed: {0}")]
    ForegroundCommand(String),
    #[error("native relay drive failed: {0}")]
    Drive(String),
    #[error("native relay socket: {0}")]
    Socket(String),
}

// Initialization capabilities belong to one foreground. Keeping the slot in a
// drop guard makes cancellation, close and failed sealing retire the same batch.
struct InitializationSlot {
    binding: Binding,
    open: String,
    token: Option<String>,
    slots: Rc<Cell<usize>>,
}
impl Drop for InitializationSlot {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self.binding.cancel_initialization_transaction(&token);
        } else {
            let _ = self.binding.rollback_transaction(&self.open);
        }
        self.slots.set(self.slots.get().saturating_sub(1));
    }
}

fn initialization_command(
    client: &mut Foreground,
    version: u8,
    action: crate::InitializationAction,
) -> Result<ForegroundDbCommandResponse, HostError> {
    use crate::{ForegroundDbCommandResponse as Response, InitializationAction as Action};
    if version != 1 {
        return Err("unsupported native initialization command version".into());
    }
    let binding = client.binding.clone();
    match action {
        Action::HasAuthenticatedCatalogue => {
            let cache = client
                .catalogue_cache
                .as_ref()
                .cloned()
                .ok_or("authenticated catalogue requires an admitted cache owner")?;
            let owner = client.authority.db.clone();
            let requested_ready = binding.db.has_authenticated_catalogue()?;
            client.pending_or_ready(
                Box::pin(async move {
                    let ready = std::future::poll_fn(move |context| {
                        cache.borrow_mut().poll_refresh(&owner, context)
                    })
                    .await
                    .map_err(|error| HostError::Argument(error.to_string()))?;
                    Ok(Response::Rows {
                        rows: if requested_ready && ready {
                            b"true".to_vec()
                        } else {
                            b"false".to_vec()
                        },
                    })
                }),
                None,
            )
        }
        Action::Seal { transaction } => {
            let (open, kind) = client.transaction(transaction)?.clone();
            if kind != ForegroundTransactionKind::Serializable {
                return Err("initialization requires a serializable transaction".into());
            }
            if client.initialization_slots.get() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
                return Err("initialization seal capacity exceeded".into());
            }
            let sealing_binding = binding.clone();
            let sealing_open = open.clone();
            let mut sealing = Box::pin(async move {
                sealing_binding
                    .seal_initialization_transaction_with_owner(&sealing_open)
                    .await
            });
            let first = match sealing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(Err(error)) => {
                    if !binding
                        .db
                        .has_open_transaction(open.parse().map_err(HostError::Argument)?)
                    {
                        client.transactions.remove(&transaction);
                    }
                    return Err(error);
                }
                Poll::Ready(Ok(seal)) => Some(seal),
                Poll::Pending => None,
            };
            client.transactions.remove(&transaction);
            client
                .initialization_slots
                .set(client.initialization_slots.get() + 1);
            let mut slot = InitializationSlot {
                binding: binding.clone(),
                open: open.clone(),
                token: None,
                slots: client.initialization_slots.clone(),
            };
            let seals = client.initializations.clone();
            let response = client.pending_or_ready(
                Box::pin(async move {
                    let seal = match first {
                        Some(seal) => seal,
                        None => sealing.await?,
                    };
                    binding.check_initialization_token(&seal.token)?;
                    let token = jazz::tools::OpenTransactionId::new().to_string();
                    slot.token = Some(seal.token);
                    let rows =
                        serde_json::json!({"token":token,"reservedTxId":seal.reserved_tx_id})
                            .to_string()
                            .into_bytes();
                    seals.borrow_mut().insert(token, slot);
                    Ok(Response::Rows { rows })
                }),
                None,
            )?;
            if let Response::Pending { operation } = &response {
                client.sealing_operations.insert(*operation);
            }
            Ok(response)
        }
        Action::Publish { token } => {
            let mut seal = client.initializations.borrow_mut().remove(&token).ok_or(
                "initialization seal is unknown, consumed, or belongs to another foreground",
            )?;
            let write = binding.publish_initialization_transaction(
                seal.token.as_deref().expect("sealed initialization"),
            )?;
            seal.token = None;
            let (tx_id, _) = client.track(write);
            Ok(Response::TransactionCommitted {
                tx_id: *tx_id.as_bytes(),
            })
        }
        Action::Cancel { token } => {
            let mut seal = client.initializations.borrow_mut().remove(&token).ok_or(
                "initialization seal is unknown, consumed, or belongs to another foreground",
            )?;
            binding.cancel_initialization_transaction(
                seal.token.as_deref().expect("sealed initialization"),
            )?;
            seal.token = None;
            Ok(Response::Rows { rows: Vec::new() })
        }
        Action::RecordAbsence {
            transaction,
            table,
            row,
        } => {
            let (open, _) = client.transaction(transaction)?;
            binding.record_initialization_insert_absence(open, &table, &row)?;
            Ok(Response::Rows { rows: Vec::new() })
        }
        Action::Status { ids } => {
            // Core validates the exact admitted owner's author and durable
            // reservation; a foreground's pending copy is not Local durability.
            Ok(Response::Rows {
                rows: client
                    .authority
                    .initialization_transaction_status(&ids)?
                    .into_bytes(),
            })
        }
    }
}

#[cfg(test)]
#[path = "relay_tests.rs"]
pub(crate) mod tests;
