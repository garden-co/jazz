//! Native, process-local Jazz relay for mobile and future platform bindings.
//!
//! The relay is deliberately a host component, not another Jazz runtime. It
//! owns a durable [`jazz::db::Db`] over SQLite and serves one in-memory `Db`
//! for each UI runtime over the ordinary Jazz peer protocol. React Native,
//! Swift, and Kotlin bindings put their ABI-specific command codecs above this
//! crate; they do not implement query, write, policy, or sync behavior here.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::lock::Mutex as LocalMutex;
use jazz::db::{Db, DbConfig, DbIdentity, PeerConnection, Transport, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::protocol::SyncMessage;
use jazz::protocol_limits::validate_logical_message_len;
use jazz::schema::JazzSchema;
use jazz::wire::{TransportError, decode_sync_message, encode_sync_message};
use jazz_storage_sqlite::SqliteStorage;
use thiserror::Error;

/// Increment this only for a breaking change to the native command/wire ABI.
/// JS wrappers must compare this with their expected range during startup and
/// explain that an OTA update needs a new native development build when it is
/// incompatible.
pub const NATIVE_RELAY_ABI_VERSION: u16 = 1;

/// Codec-owned commands accepted by the native relay C ABI.
///
/// `Probe` is intentionally the only command until the shared relay command
/// taxonomy is specified. JNI/Swift wrappers must carry these postcard bytes
/// unchanged; they must not recreate database, query, or mutation semantics.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelayCommandRequest {
    Probe,
    Open {
        scope: RelayScopeRequest,
        sqlite_path: String,
        schema_json: String,
        identity: DbIdentity,
    },
    Attach {
        relay: u64,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
    },
    CloseClient {
        client: u64,
    },
    CloseRelay {
        relay: u64,
    },
    Pump {
        relay: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelayScopeRequest {
    pub app_namespace: String,
    pub storage_namespace: String,
    pub auth_scope: Option<String>,
}

impl From<RelayScopeRequest> for RelayScope {
    fn from(value: RelayScopeRequest) -> Self {
        Self {
            app_namespace: value.app_namespace,
            storage_namespace: value.storage_namespace,
            auth_scope: value.auth_scope,
        }
    }
}

/// Codec-owned response for [`RelayCommandRequest`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelayCommandResponse {
    Probe { abi_version: u16 },
    Opened { relay: u64 },
    Attached { client: u64 },
    Closed { closed: bool },
    Pumped,
}

/// ABI-owned response buffer. On successful execution, `data` is allocated by
/// Rust and must be released exactly once through
/// [`jazz_native_relay_bytes_free`]. Do not copy this struct before freeing it.
#[repr(C)]
pub struct JazzNativeRelayBytes {
    pub data: *mut u8,
    pub len: usize,
}

impl JazzNativeRelayBytes {
    const EMPTY: Self = Self {
        data: std::ptr::null_mut(),
        len: 0,
    };
}

/// Status returned by the native C ABI. Diagnostic strings and Rust error
/// types intentionally remain inside the host binding; callers branch only on
/// these stable classes.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JazzNativeRelayStatus {
    Ok = 0,
    InvalidArgument = 1,
    InvalidCommand = 2,
    EncodeFailure = 3,
    InvalidHandle = 4,
    LifecycleFailure = 5,
}

/// Explicit host-owned lifecycle registry for JNI/Swift. No global relay map.
pub struct NativeRelayHost {
    registry: NativeRelayRegistry,
    relays: BTreeMap<u64, (RelayScope, NativeRelay)>,
    clients: BTreeMap<u64, (u64, NativeRelayClient)>,
    next_handle: u64,
}

impl Default for NativeRelayHost {
    fn default() -> Self {
        Self {
            registry: NativeRelayRegistry::default(),
            relays: BTreeMap::new(),
            clients: BTreeMap::new(),
            next_handle: 1,
        }
    }
}

impl NativeRelayHost {
    fn allocate(&mut self) -> Result<u64, RelayError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        Ok(handle)
    }

    fn execute(
        &mut self,
        command: RelayCommandRequest,
    ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
        match command {
            RelayCommandRequest::Probe => Ok(RelayCommandResponse::Probe {
                abi_version: NATIVE_RELAY_ABI_VERSION,
            }),
            RelayCommandRequest::Open {
                scope,
                sqlite_path,
                schema_json,
                identity,
            } => {
                let public_schema = serde_json::from_str(&schema_json)
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let schema = JazzSchema::new(&public_schema)
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let scope = RelayScope::from(scope);
                let relay = self
                    .registry
                    .open(RelayOpenConfig {
                        supported_abi: NativeRelayAbiRange {
                            minimum: NATIVE_RELAY_ABI_VERSION,
                            maximum: NATIVE_RELAY_ABI_VERSION,
                        },
                        scope: scope.clone(),
                        sqlite_path: PathBuf::from(sqlite_path),
                        schema,
                        identity,
                        #[cfg(test)]
                        thread_start_counter: None,
                    })
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                self.relays.insert(handle, (scope, relay));
                Ok(RelayCommandResponse::Opened { relay: handle })
            }
            RelayCommandRequest::Attach {
                relay: relay_handle,
                identity,
                claims,
            } => {
                let relay = self
                    .relays
                    .get(&relay_handle)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .1
                    .clone();
                let client = relay
                    .attach_client(identity, claims)
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                self.clients.insert(handle, (relay_handle, client));
                Ok(RelayCommandResponse::Attached { client: handle })
            }
            RelayCommandRequest::CloseClient { client } => Ok(RelayCommandResponse::Closed {
                closed: self
                    .clients
                    .remove(&client)
                    .map(|(_, client)| client.close().is_ok())
                    .unwrap_or(false),
            }),
            RelayCommandRequest::CloseRelay { relay } => {
                let Some((scope, _)) = self.relays.remove(&relay) else {
                    return Ok(RelayCommandResponse::Closed { closed: false });
                };
                let clients = self
                    .clients
                    .iter()
                    .filter_map(|(handle, (owner, _))| (*owner == relay).then_some(*handle))
                    .collect::<Vec<_>>();
                for handle in clients {
                    if let Some((_, client)) = self.clients.remove(&handle) {
                        let _ = client.close();
                    }
                }
                let closed = self
                    .registry
                    .close(&scope)
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                Ok(RelayCommandResponse::Closed { closed })
            }
            RelayCommandRequest::Pump { relay } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .1
                    .pump()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                Ok(RelayCommandResponse::Pumped)
            }
        }
    }
}

/// C ABI seam for Android/JNI, Swift, and other platform artifact wrappers.
///
/// The platform layer may use this probe before it decodes any relay command.
/// It deliberately exposes no `Db`, storage, row, or query handles; commands
/// stay behind the future shared binary relay codec.
#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_abi_version() -> u16 {
    NATIVE_RELAY_ABI_VERSION
}

/// Execute one codec-owned native relay command.
///
/// `request` is a complete postcard [`RelayCommandRequest`]. On `Ok`, `out`
/// receives Rust-owned postcard [`RelayCommandResponse`] bytes. On any error,
/// `out` is reset to an empty buffer. This function has no storage side effects
/// until a future command explicitly defines them.
///
/// # Safety
///
/// When `request_len` is nonzero, `request` must point to that many readable
/// bytes. `out` must be a valid, writable `JazzNativeRelayBytes` for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_execute(
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() || (request.is_null() && request_len != 0) {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    // SAFETY: `out` is non-null and exclusively owned by the caller for this
    // call. Reset it before decoding so every error has one unambiguous state.
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    let request = if request_len == 0 {
        &[]
    } else {
        // SAFETY: non-null was checked above; the caller supplies exactly this
        // many immutable request bytes for the duration of this call.
        unsafe { std::slice::from_raw_parts(request, request_len) }
    };
    let command = match postcard::from_bytes::<RelayCommandRequest>(request) {
        Ok(command) => command,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let response = match command {
        RelayCommandRequest::Probe => RelayCommandResponse::Probe {
            abi_version: NATIVE_RELAY_ABI_VERSION,
        },
        _ => return JazzNativeRelayStatus::InvalidCommand,
    };
    let bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(_) => return JazzNativeRelayStatus::EncodeFailure,
    };
    let boxed = bytes.into_boxed_slice();
    // SAFETY: `out` was validated above; the returned allocation remains owned
    // by Rust until the matching free call below.
    unsafe {
        *out = JazzNativeRelayBytes {
            len: boxed.len(),
            data: Box::into_raw(boxed).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Opaque C-owned native relay host. It owns one scope registry and all handles.
#[repr(C)]
pub struct JazzNativeRelayHost {
    inner: NativeRelayHost,
}

#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_host_new() -> *mut JazzNativeRelayHost {
    Box::into_raw(Box::new(JazzNativeRelayHost {
        inner: NativeRelayHost::default(),
    }))
}

#[unsafe(no_mangle)]
/// # Safety
///
/// `host` must be null or an unfreed pointer returned by host_new, and no
/// concurrent call may retain or execute it while this function runs.
pub unsafe extern "C" fn jazz_native_relay_host_free(host: *mut JazzNativeRelayHost) {
    if !host.is_null() {
        unsafe {
            drop(Box::from_raw(host));
        }
    }
}

/// Execute lifecycle commands against one explicit host context.
///
/// # Safety
/// `host`, request bytes, and `out` follow the same validity rules as
/// [`jazz_native_relay_execute`]; `host` must be returned by host_new and not freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_execute(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if host.is_null() || out.is_null() || (request.is_null() && request_len != 0) {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe {
        *out = JazzNativeRelayBytes::EMPTY;
    }
    let request = if request_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(request, request_len) }
    };
    let command = match postcard::from_bytes::<RelayCommandRequest>(request) {
        Ok(command) => command,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let response = match unsafe { (&mut *host).inner.execute(command) } {
        Ok(response) => response,
        Err(status) => return status,
    };
    let bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(_) => return JazzNativeRelayStatus::EncodeFailure,
    };
    let boxed = bytes.into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: boxed.len(),
            data: Box::into_raw(boxed).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Release a response buffer returned by [`jazz_native_relay_execute`].
///
/// The struct is reset before returning, making repeated frees of the *same
/// struct* a no-op. Copying the struct and freeing both copies is invalid.
///
/// # Safety
///
/// `bytes` must be null or point to a writable `JazzNativeRelayBytes` returned
/// by this ABI (or its reset empty value); callers must not free copied values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_bytes_free(bytes: *mut JazzNativeRelayBytes) {
    if bytes.is_null() {
        return;
    }
    // SAFETY: a non-null caller-owned output struct was supplied.
    let bytes = unsafe { &mut *bytes };
    if bytes.data.is_null() {
        bytes.len = 0;
        return;
    }
    // SAFETY: only `jazz_native_relay_execute` creates this allocation, with
    // exactly the recorded length and capacity. Reset before dropping so a
    // second call on this struct cannot free it again.
    let allocation = unsafe { Vec::from_raw_parts(bytes.data, bytes.len, bytes.len) };
    *bytes = JazzNativeRelayBytes::EMPTY;
    drop(allocation);
}

/// Inclusive ABI-version range understood by a native host wrapper.
///
/// This is deliberately independent of any particular binding generator.
/// Future TurboModule, Swift, and Kotlin wrappers carry this range in their
/// open request, so an OTA JavaScript update cannot accidentally issue commands
/// to an incompatible embedded native library.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeRelayAbiRange {
    pub minimum: u16,
    pub maximum: u16,
}

impl NativeRelayAbiRange {
    pub const fn includes(self, version: u16) -> bool {
        self.minimum <= version && version <= self.maximum
    }

    pub fn validate(self) -> Result<(), RelayError> {
        if self.minimum > self.maximum {
            return Err(RelayError::InvalidAbiRange {
                minimum: self.minimum,
                maximum: self.maximum,
            });
        }
        Ok(())
    }
}

/// Check the native relay ABI before opening storage or allocating an owner
/// thread. Bindings should surface the resulting error unchanged: users need a
/// new native development or release build, rather than a cache reset.
pub fn ensure_native_relay_abi_compatible(
    wrapper_range: NativeRelayAbiRange,
) -> Result<u16, RelayError> {
    wrapper_range.validate()?;
    if wrapper_range.includes(NATIVE_RELAY_ABI_VERSION) {
        Ok(NATIVE_RELAY_ABI_VERSION)
    } else {
        Err(RelayError::IncompatibleAbi {
            native: NATIVE_RELAY_ABI_VERSION,
            minimum: wrapper_range.minimum,
            maximum: wrapper_range.maximum,
        })
    }
}

/// Explicit process-local persistence/synchronization scope.
///
/// Authentication material is intentionally absent. `auth_scope` is an opaque
/// stable subject/tenant discriminator supplied by the host after validation;
/// tokens are sent to an upstream connection, never used as storage names.
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
        Ok(())
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
    thread_start_counter: Option<Arc<AtomicUsize>>,
}

impl RelayOpenConfig {
    fn validate(&self) -> Result<(), RelayError> {
        ensure_native_relay_abi_compatible(self.supported_abi)?;
        self.scope.validate()
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

    /// Run an ABI-adapter operation against this UI runtime on the relay's
    /// owning thread. The closure is intentionally Rust-only; public bindings
    /// expose a small encoded command surface instead of leaking `Db` handles
    /// across JSI/JNI/Swift boundaries.
    pub fn with_db<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Db<MemoryStorage>) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            let client = worker
                .clients
                .get(&id)
                .ok_or(RelayError::UnknownClient(id))?;
            operation(&client.db)
        })
    }

    pub fn close(self) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker
                .clients
                .remove(&id)
                .map(|_| ())
                .ok_or(RelayError::UnknownClient(id))
        })
    }
}

/// Thread-safe handle to one executor-local relay owner.
#[derive(Clone)]
pub struct NativeRelay {
    inner: Arc<RelayInner>,
}

struct RelayInner {
    jobs: Mutex<Option<mpsc::Sender<RelayCommand>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    wire: NativeRelayWire,
    sqlite_path: PathBuf,
    schema_version: jazz::ids::SchemaVersionId,
}

impl Drop for RelayInner {
    fn drop(&mut self) {
        let Some(sender) = self.jobs.lock().ok().and_then(|mut sender| sender.take()) else {
            return;
        };
        let (done_tx, done_rx) = mpsc::channel();
        let _ = sender.send(RelayCommand::Shutdown(done_tx));
        let _ = done_rx.recv();
        if let Ok(mut join) = self.join.lock()
            && let Some(join) = join.take()
        {
            let _ = join.join();
        }
    }
}

/// Thread-safe upstream protocol queues owned by the host integration.
///
/// A native network/ABI wrapper writes authenticated upstream messages to
/// `inbound` and drains `outbound`. The relay only sees normal `SyncMessage`
/// traffic through a regular `Db::connect_upstream` transport.
#[derive(Clone, Default)]
pub struct NativeRelayWire {
    inbound: Arc<Mutex<VecDeque<SyncMessage>>>,
    outbound: Arc<Mutex<VecDeque<SyncMessage>>>,
}

impl NativeRelayWire {
    pub fn push_inbound(&self, message: SyncMessage) -> Result<(), RelayError> {
        self.inbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream inbound queue"))?
            .push_back(message);
        Ok(())
    }

    pub fn take_outbound(&self) -> Result<Vec<SyncMessage>, RelayError> {
        Ok(self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream outbound queue"))?
            .drain(..)
            .collect())
    }

    /// Admit one postcard-encoded ordinary peer message from a binding.
    ///
    /// This is the binary boundary for native hosts: it preserves Jazz's
    /// shared `SyncMessage` vocabulary instead of inventing a React Native
    /// object API. The caller supplies one complete logical message; network
    /// framing and fragmentation remain the responsibility of its transport.
    pub fn push_inbound_encoded(&self, bytes: &[u8]) -> Result<(), RelayError> {
        validate_encoded_peer_message_len(bytes.len())?;
        let message = decode_sync_message(bytes).map_err(RelayError::DecodePeerMessage)?;
        self.push_inbound(message)
    }

    /// Drain ordinary peer messages as postcard payloads for a binding.
    ///
    /// The encoded payload uses the canonical Jazz sync-message codec. A
    /// future TurboModule transports these bytes as `ArrayBuffer`/`Uint8Array`,
    /// keeping it thin and shared.
    pub fn take_outbound_encoded(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream outbound queue"))?;
        // Encode while the batch remains queued. A failed codec/size check
        // leaves every message intact for retry and diagnostics.
        let encoded = outbound
            .iter()
            .map(|message| {
                let bytes = encode_sync_message(message).map_err(RelayError::EncodePeerMessage)?;
                validate_logical_message_len(bytes.len())
                    .map_err(RelayError::PeerMessageTooLarge)?;
                Ok(bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        outbound.clear();
        Ok(encoded)
    }
}

fn validate_encoded_peer_message_len(len: usize) -> Result<(), RelayError> {
    validate_logical_message_len(len).map_err(RelayError::PeerMessageTooLarge)
}

struct QueueTransport {
    inbound: Arc<Mutex<VecDeque<SyncMessage>>>,
    outbound: Arc<Mutex<VecDeque<SyncMessage>>>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay outbound queue poisoned".to_owned()))?
            .push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.lock().ok()?.pop_front()
    }
}

struct DuplexTransport {
    inbound: Arc<Mutex<VecDeque<SyncMessage>>>,
    outbound: Arc<Mutex<VecDeque<SyncMessage>>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay client queue poisoned".to_owned()))?
            .push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.lock().ok()?.pop_front()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let left = Arc::new(Mutex::new(VecDeque::new()));
    let right = Arc::new(Mutex::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            inbound: Arc::clone(&left),
            outbound: Arc::clone(&right),
        }),
        Box::new(DuplexTransport {
            inbound: right,
            outbound: left,
        }),
    )
}

struct ConnectedClient {
    db: Db<MemoryStorage>,
    // The core stores weak references for lifecycle ownership; retaining both
    // endpoints is what keeps the normal peer protocol connection alive.
    _upstream: Rc<LocalMutex<PeerConnection<MemoryStorage>>>,
    _served: Rc<LocalMutex<PeerConnection<SqliteStorage>>>,
}

struct RelayWorker {
    persistent: Db<SqliteStorage>,
    _upstream: Rc<LocalMutex<PeerConnection<SqliteStorage>>>,
    clients: BTreeMap<u64, ConnectedClient>,
    next_client_id: u64,
    schema: JazzSchema,
}

impl RelayWorker {
    fn open(config: RelayOpenConfig, wire: NativeRelayWire) -> Result<Self, RelayError> {
        let column_families = config.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let persistent = block_on(Db::open(DbConfig {
            schema: config.schema.clone(),
            storage: SqliteStorage::open(config.sqlite_path, &refs).map_err(RelayError::Storage)?,
            identity: config.identity,
            id_source: None,
        }))
        .map_err(RelayError::Db)?;
        let upstream = block_on(persistent.connect_upstream(Box::new(QueueTransport {
            inbound: wire.inbound,
            outbound: wire.outbound,
        })));
        Ok(Self {
            persistent,
            _upstream: upstream,
            clients: BTreeMap::new(),
            next_client_id: 1,
            schema: config.schema,
        })
    }

    fn attach_client(
        &mut self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
    ) -> Result<u64, RelayError> {
        let column_families = self.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig {
            schema: self.schema.clone(),
            storage: MemoryStorage::new(&refs),
            identity,
            id_source: None,
        }))
        .map_err(RelayError::Db)?;
        let (client_transport, relay_transport) = duplex();
        let upstream = block_on(db.connect_upstream(client_transport));
        let served =
            self.persistent
                .accept_subscriber_with_claims(relay_transport, identity.author, claims);
        let id = self.next_client_id;
        self.next_client_id = self
            .next_client_id
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        self.clients.insert(
            id,
            ConnectedClient {
                db,
                _upstream: upstream,
                _served: served,
            },
        );
        Ok(id)
    }

    fn pump(&mut self) -> Result<(), RelayError> {
        // One fair relay turn has exactly three protocol phases. A UI upload
        // becomes relay input, the relay applies/forwards it, then UI clients
        // observe resulting view/fate messages. More cascades schedule another
        // host turn; spinning until quiescence here would make a busy
        // subscription graph monopolize the native owner thread.
        for client in self.clients.values() {
            block_on(client.db.tick()).map_err(RelayError::Db)?;
        }
        block_on(self.persistent.tick()).map_err(RelayError::Db)?;
        for client in self.clients.values() {
            block_on(client.db.tick()).map_err(RelayError::Db)?;
        }
        Ok(())
    }
}

type RelayJob = Box<dyn FnOnce(&mut RelayWorker) + Send + 'static>;

enum RelayCommand {
    Run(RelayJob),
    Shutdown(mpsc::Sender<()>),
}

impl NativeRelay {
    pub fn spawn(config: RelayOpenConfig) -> Result<Self, RelayError> {
        // This is before channel/thread creation and the worker's SQLite open.
        config.validate()?;
        let sqlite_path = config.sqlite_path.clone();
        let schema_version = config.schema.version_id();
        let wire = NativeRelayWire::default();
        let (commands, receiver) = mpsc::channel::<RelayCommand>();
        let (started_tx, started_rx) = mpsc::channel();
        let owner_wire = wire.clone();
        #[cfg(test)]
        if let Some(counter) = &config.thread_start_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let join = thread::Builder::new()
            .name("jazz-native-relay".to_owned())
            .spawn(move || {
                let mut worker = match RelayWorker::open(config, owner_wire) {
                    Ok(worker) => {
                        let _ = started_tx.send(Ok(()));
                        worker
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                        return;
                    }
                };
                while let Ok(command) = receiver.recv() {
                    match command {
                        RelayCommand::Run(job) => job(&mut worker),
                        RelayCommand::Shutdown(done) => {
                            drop(worker);
                            let _ = done.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        started_rx.recv().map_err(|_| {
            RelayError::OwnerThread("owner exited before opening relay".to_owned())
        })??;
        Ok(Self {
            inner: Arc::new(RelayInner {
                jobs: Mutex::new(Some(commands)),
                join: Mutex::new(Some(join)),
                wire,
                sqlite_path,
                schema_version,
            }),
        })
    }

    pub fn abi_version(&self) -> u16 {
        NATIVE_RELAY_ABI_VERSION
    }

    /// Verify that a host wrapper understands this embedded native relay before
    /// opening a scope. This is also available as
    /// [`ensure_native_relay_abi_compatible`] for wrappers that must check
    /// before constructing a relay.
    pub fn ensure_abi_compatible(wrapper_range: NativeRelayAbiRange) -> Result<u16, RelayError> {
        ensure_native_relay_abi_compatible(wrapper_range)
    }

    pub fn wire(&self) -> NativeRelayWire {
        self.inner.wire.clone()
    }

    pub fn attach_client(
        &self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
    ) -> Result<NativeRelayClient, RelayError> {
        let id = self.run(move |worker| worker.attach_client(identity, claims))?;
        Ok(NativeRelayClient {
            relay: self.clone(),
            id,
        })
    }

    pub fn pump(&self) -> Result<(), RelayError> {
        self.run(|worker| worker.pump())
    }

    fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        let (response_tx, response_rx) = mpsc::channel();
        let job: RelayJob = Box::new(move |worker| {
            let _ = response_tx.send(operation(worker));
        });
        self.inner
            .jobs
            .lock()
            .map_err(|_| RelayError::Poisoned("relay command queue"))?
            .as_ref()
            .ok_or(RelayError::Closed)?
            .send(RelayCommand::Run(job))
            .map_err(|_| RelayError::Closed)?;
        response_rx.recv().map_err(|_| RelayError::Closed)?
    }
}

/// A registry is owned by the platform host (application process), not global
/// Rust state. That makes teardown explicit and lets Android services, iOS app
/// delegates, and test processes choose their own lifecycle semantics.
#[derive(Default)]
pub struct NativeRelayRegistry {
    relays: Mutex<BTreeMap<RelayScope, NativeRelay>>,
}

impl NativeRelayRegistry {
    pub fn open(&self, config: RelayOpenConfig) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        if let Some(existing) = relays.get(&config.scope) {
            if existing.inner.sqlite_path != config.sqlite_path
                || existing.inner.schema_version != config.schema.version_id()
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            return Ok(existing.clone());
        }
        let relay = NativeRelay::spawn(config.clone())?;
        relays.insert(config.scope, relay.clone());
        Ok(relay)
    }

    pub fn close(&self, scope: &RelayScope) -> Result<bool, RelayError> {
        Ok(self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?
            .remove(scope)
            .is_some())
    }
}

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
    #[error("failed to decode native relay peer message: {0}")]
    DecodePeerMessage(postcard::Error),
    #[error("failed to encode native relay peer message: {0}")]
    EncodePeerMessage(postcard::Error),
    #[error("invalid native relay scope: {0}")]
    InvalidScope(String),
    #[error("failed to open native relay owner thread: {0}")]
    OwnerThread(String),
    #[error("native relay is closed")]
    Closed,
    #[error("native relay internal mutex poisoned: {0}")]
    Poisoned(&'static str),
    #[error("native relay does not know UI client {0}")]
    UnknownClient(u64),
    #[error("a native relay scope is already open with a different storage path or schema")]
    ScopeConfigurationMismatch,
    #[error("native relay UI client id space exhausted")]
    ClientIdExhausted,
    #[error("SQLite storage failed: {0}")]
    Storage(jazz::groove::storage::Error),
    #[error("Jazz database failed: {0}")]
    Db(jazz::db::Error),
}

#[cfg(test)]
mod tests {
    // This is intentionally an internal transport-ownership test: the public
    // user-visible behavior of rows/subscriptions belongs to the Db suites.
    // Here we prove the native host does not accidentally create one durable
    // store per UI runtime or share it across explicit auth scopes.
    use super::*;
    use jazz::ids::{AuthorId, NodeUuid, RowUuid};
    use jazz::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use jazz::tx::DurabilityTier;

    fn schema() -> JazzSchema {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap()
    }

    fn config(path: PathBuf, auth_scope: Option<&str>) -> RelayOpenConfig {
        RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_VERSION,
                maximum: NATIVE_RELAY_ABI_VERSION,
            },
            scope: RelayScope {
                app_namespace: "native-relay-test".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: auth_scope.map(str::to_owned),
            },
            sqlite_path: path,
            schema: schema(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xa1; 16]),
                author: AuthorId::from_bytes([0xa2; 16]),
            },
            thread_start_counter: None,
        }
    }

    #[test]
    fn relay_shares_one_scope_and_forwards_two_ui_client_writes_upstream() {
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let first = registry
            .open(config(directory.path().join("alice.sqlite"), Some("alice")))
            .unwrap();
        let same = registry
            .open(config(directory.path().join("alice.sqlite"), Some("alice")))
            .unwrap();
        let other = registry
            .open(config(directory.path().join("bob.sqlite"), Some("bob")))
            .unwrap();
        assert!(Arc::ptr_eq(&first.inner, &same.inner));
        assert!(!Arc::ptr_eq(&first.inner, &other.inner));
        assert!(matches!(
            registry.open(config(directory.path().join("wrong.sqlite"), Some("alice"))),
            Err(RelayError::ScopeConfigurationMismatch)
        ));

        let first_client = first
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xb1; 16]),
                    author: AuthorId::from_bytes([0xb2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let second_client = same
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorId::from_bytes([0xc2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        assert_ne!(first_client.id(), second_client.id());

        first_client
            .with_db(|db| {
                let write = block_on(db.insert_with_id(
                    "todos",
                    RowUuid::from_bytes([0xd1; 16]),
                    BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                ))
                .map_err(RelayError::Db)?;
                block_on(write.wait(DurabilityTier::Local)).map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();
        second_client
            .with_db(|db| {
                let write = block_on(db.insert_with_id(
                    "todos",
                    RowUuid::from_bytes([0xd2; 16]),
                    BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
                ))
                .map_err(RelayError::Db)?;
                block_on(write.wait(DurabilityTier::Local)).map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();
        first.pump().unwrap();
        let outbound = first.wire().take_outbound().unwrap();
        let forwarded_rows = outbound
            .iter()
            .filter_map(|message| match message {
                SyncMessage::CommitUnit { versions, .. } => Some(versions),
                _ => None,
            })
            .flatten()
            .map(jazz::protocol::VersionRecord::row_uuid)
            .collect::<Vec<_>>();
        assert_eq!(forwarded_rows.len(), 2);
        assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd1; 16])));
        assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd2; 16])));
        assert_eq!(
            outbound
                .iter()
                .filter(|message| matches!(message, SyncMessage::CommitUnit { .. }))
                .count(),
            2,
            "each in-memory UI client must reach the shared persistent relay and its upstream"
        );
    }

    #[test]
    fn abi_handshake_accepts_supported_versions_before_storage_opens() {
        assert_eq!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_VERSION,
                maximum: NATIVE_RELAY_ABI_VERSION,
            })
            .unwrap(),
            NATIVE_RELAY_ABI_VERSION
        );
        assert_eq!(
            NativeRelay::ensure_abi_compatible(NativeRelayAbiRange {
                minimum: 0,
                maximum: NATIVE_RELAY_ABI_VERSION,
            })
            .unwrap(),
            NATIVE_RELAY_ABI_VERSION
        );
    }

    #[test]
    fn abi_handshake_rejects_invalid_and_incompatible_ranges() {
        assert!(matches!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: 2,
                maximum: 1,
            }),
            Err(RelayError::InvalidAbiRange {
                minimum: 2,
                maximum: 1,
            })
        ));
        assert!(matches!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_VERSION.saturating_add(1),
                maximum: u16::MAX,
            }),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_VERSION
        ));
    }

    #[test]
    fn incompatible_open_creates_no_relay_or_sqlite_store() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-exist.sqlite");
        let mut open = config(sqlite_path.clone(), Some("alice"));
        open.supported_abi = NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_VERSION.saturating_add(1),
            maximum: u16::MAX,
        };
        let registry = NativeRelayRegistry::default();

        assert!(matches!(
            registry.open(open),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_VERSION
        ));
        assert!(
            !sqlite_path.exists(),
            "ABI rejection must happen before SQLite creates a database"
        );
        assert!(
            registry.relays.lock().unwrap().is_empty(),
            "ABI rejection must not register a partially-open relay"
        );
    }

    #[test]
    fn direct_incompatible_spawn_creates_no_owner_thread_or_sqlite_store() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-exist-direct.sqlite");
        let mut open = config(sqlite_path.clone(), Some("alice"));
        open.supported_abi = NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_VERSION.saturating_add(1),
            maximum: u16::MAX,
        };
        let threads_started = Arc::new(AtomicUsize::new(0));
        open.thread_start_counter = Some(Arc::clone(&threads_started));

        assert!(matches!(
            NativeRelay::spawn(open),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_VERSION
        ));
        assert_eq!(threads_started.load(Ordering::Relaxed), 0);
        assert!(
            !sqlite_path.exists(),
            "ABI rejection must happen before SQLite creates a database"
        );
    }

    #[test]
    fn encoded_peer_messages_use_the_shared_postcard_contract() {
        let wire = NativeRelayWire::default();
        let message = SyncMessage::SessionClaims {
            identity: AuthorId::SYSTEM,
            claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
        };
        let bytes = encode_sync_message(&message).unwrap();

        wire.push_inbound_encoded(&bytes).unwrap();
        assert_eq!(
            wire.inbound.lock().unwrap().pop_front(),
            Some(message.clone())
        );

        wire.outbound.lock().unwrap().push_back(message);
        let encoded = wire.take_outbound_encoded().unwrap();
        assert_eq!(encoded.len(), 1);
        assert!(wire.outbound.lock().unwrap().is_empty());
        assert_eq!(
            decode_sync_message(&encoded[0]).unwrap(),
            SyncMessage::SessionClaims {
                identity: AuthorId::SYSTEM,
                claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
            }
        );
    }

    #[test]
    fn encoded_peer_messages_reject_invalid_bytes() {
        assert!(matches!(
            NativeRelayWire::default().push_inbound_encoded(&[0xff]),
            Err(RelayError::DecodePeerMessage(_))
        ));
    }

    #[test]
    fn encoded_peer_messages_reject_the_exact_logical_message_limit_boundary() {
        let bytes = vec![0; MAX_LOGICAL_MESSAGE_BYTES + 1];
        assert!(matches!(
            NativeRelayWire::default().push_inbound_encoded(&bytes),
            Err(RelayError::PeerMessageTooLarge(message))
                if message.contains(&(MAX_LOGICAL_MESSAGE_BYTES + 1).to_string())
        ));
    }

    #[test]
    fn outbound_queue_keeps_messages_when_an_oversized_batch_is_rejected() {
        let normal = SyncMessage::SessionClaims {
            identity: AuthorId::SYSTEM,
            claims: BTreeMap::new(),
        };
        let oversized = SyncMessage::SessionClaims {
            identity: AuthorId::SYSTEM,
            claims: BTreeMap::from([(
                "payload".to_owned(),
                Value::String("x".repeat(MAX_LOGICAL_MESSAGE_BYTES + 1)),
            )]),
        };
        let wire = NativeRelayWire::default();
        wire.outbound
            .lock()
            .unwrap()
            .extend([normal.clone(), oversized]);

        assert!(matches!(
            wire.take_outbound_encoded(),
            Err(RelayError::PeerMessageTooLarge(_))
        ));
        let queued = wire.outbound.lock().unwrap();
        assert_eq!(queued.len(), 2, "a rejected batch must not be drained");
        assert_eq!(queued.front(), Some(&normal));
        assert!(matches!(
            queued.back(),
            Some(SyncMessage::SessionClaims { claims, .. })
                if matches!(claims.get("payload"), Some(Value::String(value)) if value.len() == MAX_LOGICAL_MESSAGE_BYTES + 1)
        ));
    }

    #[test]
    fn c_host_lifecycle_open_attach_close_and_bounded_pump_are_handle_safe() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null());
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorId::from_bytes([0x72; 16]),
        };
        let open = RelayCommandRequest::Open {
            scope: RelayScopeRequest {
                app_namespace: "host-receipt".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-subject".to_owned()),
            },
            sqlite_path: directory.path().join("host.sqlite").display().to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity,
        };
        unsafe fn command(
            host: *mut JazzNativeRelayHost,
            request: RelayCommandRequest,
        ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
            let bytes = postcard::to_allocvec(&request).unwrap();
            let mut output = JazzNativeRelayBytes::EMPTY;
            let status = unsafe {
                jazz_native_relay_host_execute(host, bytes.as_ptr(), bytes.len(), &mut output)
            };
            if !matches!(status, JazzNativeRelayStatus::Ok) {
                return Err(status);
            }
            let response = postcard::from_bytes(unsafe {
                std::slice::from_raw_parts(output.data, output.len)
            })
            .unwrap();
            unsafe { jazz_native_relay_bytes_free(&mut output) };
            Ok(response)
        }
        let relay = match unsafe { command(host, open) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let client = match unsafe {
            command(
                host,
                RelayCommandRequest::Attach {
                    relay,
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0x73; 16]),
                        author: AuthorId::from_bytes([0x74; 16]),
                    },
                    claims: BTreeMap::new(),
                },
            )
        }
        .unwrap()
        {
            RelayCommandResponse::Attached { client } => client,
            response => panic!("unexpected attach response: {response:?}"),
        };
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseClient { client }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseClient { client }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Err(JazzNativeRelayStatus::InvalidHandle)
        ));
        unsafe { jazz_native_relay_host_free(host) };
    }
}
