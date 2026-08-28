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
use jazz::protocol_limits::{MAX_LOGICAL_MESSAGE_BYTES, validate_logical_message_len};
use jazz::schema::JazzSchema;
use jazz::storage_codec_profile::epoch_1_storage_codec_profile;
use jazz::wire::{TransportError, decode_sync_message, encode_sync_message};
use jazz_storage_sqlite::{Durability as SqliteDurability, SqliteStorage};
use thiserror::Error;

/// Increment this only for a breaking change to the native command/wire ABI.
/// JS wrappers must compare this with their expected range during startup and
/// explain that an OTA update needs a new native development build when it is
/// incompatible.
pub const NATIVE_RELAY_ABI_VERSION: u16 = 3;

const NATIVE_RELAY_QUEUE_MAX_MESSAGES: usize = 1024;
const NATIVE_RELAY_QUEUE_MAX_BYTES: usize = MAX_LOGICAL_MESSAGE_BYTES;
const NATIVE_RELAY_DRAIN_MAX_MESSAGES: usize = 64;
const NATIVE_RELAY_DRAIN_TARGET_BYTES: usize = 8 * 1024 * 1024;
const NATIVE_RELAY_PUMP_MAX_CLIENTS: usize = 64;
/// Trusted platform admission carries schema and validated claims, but must
/// remain bounded independently of the generic peer-frame budget.
const NATIVE_RELAY_ADMISSION_MAX_BYTES: usize = 1024 * 1024;

/// Codec-owned commands accepted by the native relay C ABI.
///
/// This surface owns relay lifecycle and ordinary peer-frame transport only.
/// JNI/Swift wrappers carry these postcard bytes unchanged; query, mutation,
/// and row semantics remain absent.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelayCommandRequest {
    Probe,
    Open {
        supported_abi_minimum: u16,
        supported_abi_maximum: u16,
        admitted_scope: AdmissionCapability,
    },
    Attach {
        relay: u64,
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
    /// Give one complete canonical Jazz peer frame to an attached in-memory
    /// UI client. The host never decodes rows or queries here.
    SendClientFrame {
        client: u64,
        frame: Vec<u8>,
    },
    /// Drain frames destined for one attached in-memory UI client.
    ReceiveClientFrames {
        client: u64,
    },
    /// Give one complete canonical Jazz peer frame to the relay's upstream
    /// transport.
    SendRelayFrame {
        relay: u64,
        frame: Vec<u8>,
    },
    /// Drain frames destined for the relay's upstream transport.
    ReceiveRelayFrames {
        relay: u64,
    },
    /// Host-only lifecycle diagnostics. This deliberately exposes handles and
    /// queue depths, never database rows, query state, or auth material.
    Diagnostics {
        relay: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelayScopeRequest {
    pub app_namespace: String,
    pub storage_namespace: String,
    pub auth_scope: Option<String>,
}

/// Normalized configuration admitted internally after the strict trusted JSON
/// boundary. It is deliberately not a [`RelayCommandRequest`] variant.
#[derive(Clone, Debug, PartialEq)]
struct RelayScopeAdmissionRequest {
    pub scope: RelayScopeRequest,
    pub sqlite_path: String,
    pub schema_json: String,
    pub identity: DbIdentity,
    pub claims: BTreeMap<String, Value>,
}

/// JSON-shaped form accepted only by the platform-owned admission C entry
/// point. Keeping this separate from the postcard command codec makes the
/// platform integration practical without letting JavaScript construct scope
/// configuration. Rust still owns validation and normalization.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayScopeAdmissionJson {
    scope: TrustedRelayScopeJson,
    sqlite_path: String,
    schema_json: String,
    identity: TrustedRelayIdentityJson,
    claims: BTreeMap<String, Value>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayScopeJson {
    app_namespace: String,
    storage_namespace: String,
    auth_scope: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayIdentityJson {
    node: jazz::ids::NodeUuid,
    author: jazz::ids::AuthorSubject,
}

impl TrustedRelayScopeAdmissionJson {
    fn normalize(self) -> Result<RelayScopeAdmissionRequest, JazzNativeRelayStatus> {
        if self.sqlite_path.trim().is_empty() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let scope = RelayScopeRequest {
            app_namespace: self.scope.app_namespace,
            storage_namespace: self.scope.storage_namespace,
            auth_scope: self.scope.auth_scope,
        };
        RelayScope::from(scope.clone())
            .validate()
            .map_err(relay_status)?;
        if matches!(self.identity.author, jazz::ids::AuthorSubject::System) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        reject_bearer_claims(&self.claims)?;
        // Parse and reserialize before storing so the trusted boundary has one
        // normalized schema spelling and malformed JSON cannot reach admission.
        let schema_value = serde_json::from_str::<serde_json::Value>(&self.schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema_json = serde_json::to_string(&schema_value)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        Ok(RelayScopeAdmissionRequest {
            scope,
            sqlite_path: self.sqlite_path,
            schema_json,
            identity: DbIdentity {
                node: self.identity.node,
                author: self.identity.author,
            },
            claims: self.claims,
        })
    }
}

fn reject_bearer_claims(claims: &BTreeMap<String, Value>) -> Result<(), JazzNativeRelayStatus> {
    // These values belong exclusively to upstream-session negotiation. The
    // relay receives validated identity claims, never a bearer credential that
    // could be persisted or exposed through diagnostics.
    const CREDENTIAL_CLAIMS: &[&str] = &[
        "authorization",
        "access_token",
        "refresh_token",
        "id_token",
        "bearer_token",
        "token",
    ];
    if claims.keys().any(|key| {
        let normalized = key.to_ascii_lowercase();
        CREDENTIAL_CLAIMS.contains(&normalized.as_str())
    }) {
        return Err(JazzNativeRelayStatus::LifecycleFailure);
    }
    Ok(())
}

/// Unguessable authority to open one host-admitted native scope.
///
/// Its representation is opaque to JavaScript and platform bindings. They
/// carry its raw 32 bytes only as a handle for ordinary relay commands.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct AdmissionCapability([u8; 32]);

impl std::fmt::Debug for AdmissionCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AdmissionCapability([redacted])")
    }
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
    Probe {
        abi_version: u16,
    },
    Opened {
        relay: u64,
    },
    Attached {
        client: u64,
    },
    Closed {
        closed: bool,
    },
    Pumped,
    Frames {
        frames: Vec<Vec<u8>>,
    },
    Diagnostics {
        attached_clients: u64,
        inbound_frames: u64,
        outbound_frames: u64,
    },
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
    InvalidAbiRange = 6,
    IncompatibleAbi = 7,
    Backpressure = 8,
}

/// Explicit host-owned lifecycle registry for JNI/Swift. No global relay map.
pub struct NativeRelayHost {
    registry: NativeRelayRegistry,
    admitted_scopes: BTreeMap<AdmissionCapability, AdmittedRelayScope>,
    relays: BTreeMap<u64, OpenedRelay>,
    clients: BTreeMap<u64, (u64, NativeRelayClient)>,
    next_handle: u64,
    #[cfg(test)]
    thread_start_counter: Option<Arc<AtomicUsize>>,
}

#[derive(Clone)]
struct AdmittedRelayScope {
    config: RelayOpenConfig,
    claims: BTreeMap<String, Value>,
}

struct OpenedRelay {
    scope: RelayScope,
    admitted_scope: AdmissionCapability,
    relay: NativeRelay,
}

impl Default for NativeRelayHost {
    fn default() -> Self {
        Self {
            registry: NativeRelayRegistry::default(),
            admitted_scopes: BTreeMap::new(),
            relays: BTreeMap::new(),
            clients: BTreeMap::new(),
            next_handle: 1,
            #[cfg(test)]
            thread_start_counter: None,
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

    fn allocate_admission_capability(&self) -> Result<AdmissionCapability, RelayError> {
        loop {
            let mut bytes = [0_u8; 32];
            getrandom::fill(&mut bytes).map_err(|error| RelayError::Entropy(error.to_string()))?;
            let capability = AdmissionCapability(bytes);
            if !self.admitted_scopes.contains_key(&capability) {
                return Ok(capability);
            }
        }
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
                supported_abi_minimum,
                supported_abi_maximum,
                admitted_scope,
            } => {
                let supported_abi = NativeRelayAbiRange {
                    minimum: supported_abi_minimum,
                    maximum: supported_abi_maximum,
                };
                ensure_native_relay_abi_compatible(supported_abi).map_err(|error| match error {
                    RelayError::InvalidAbiRange { .. } => JazzNativeRelayStatus::InvalidAbiRange,
                    RelayError::IncompatibleAbi { .. } => JazzNativeRelayStatus::IncompatibleAbi,
                    _ => JazzNativeRelayStatus::LifecycleFailure,
                })?;
                let mut config = self
                    .admitted_scopes
                    .get(&admitted_scope)
                    .map(|admitted| admitted.config.clone())
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                config.supported_abi = supported_abi;
                let scope = config.scope.clone();
                let relay = self.registry.open(config).map_err(relay_status)?;
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                self.relays.insert(
                    handle,
                    OpenedRelay {
                        scope,
                        admitted_scope,
                        relay,
                    },
                );
                Ok(RelayCommandResponse::Opened { relay: handle })
            }
            RelayCommandRequest::Attach {
                relay: relay_handle,
            } => {
                let (relay, author, claims) = {
                    let opened = self
                        .relays
                        .get(&relay_handle)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                    let admitted = self
                        .admitted_scopes
                        .get(&opened.admitted_scope)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                    (
                        opened.relay.clone(),
                        admitted.config.identity.author,
                        admitted.claims.clone(),
                    )
                };
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let client = relay
                    .attach_client(client_identity(handle, author), claims)
                    .map_err(relay_status)?;
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
                let Some(opened) = self.relays.remove(&relay) else {
                    return Ok(RelayCommandResponse::Closed { closed: false });
                };
                let scope = opened.scope;
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
                let final_alias = !self
                    .relays
                    .values()
                    .any(|remaining| remaining.scope == scope);
                if final_alias {
                    self.registry
                        .close(&scope)
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                }
                Ok(RelayCommandResponse::Closed { closed: true })
            }
            RelayCommandRequest::Pump { relay } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .pump()
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::SendClientFrame { client, frame } => {
                self.clients
                    .get(&client)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .1
                    .wire()
                    .push_inbound_encoded(&frame)
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::ReceiveClientFrames { client } => {
                Ok(RelayCommandResponse::Frames {
                    frames: self
                        .clients
                        .get(&client)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                        .1
                        .wire()
                        .take_outbound_encoded()
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
                })
            }
            RelayCommandRequest::SendRelayFrame { relay, frame } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .wire()
                    .push_inbound_encoded(&frame)
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::ReceiveRelayFrames { relay } => Ok(RelayCommandResponse::Frames {
                frames: self
                    .relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .wire()
                    .take_outbound_encoded()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
            }),
            RelayCommandRequest::Diagnostics { relay } => {
                let relay_handle = relay;
                let relay = self
                    .relays
                    .get(&relay_handle)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                let (inbound_frames, outbound_frames) = relay
                    .relay
                    .wire()
                    .queue_depths()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                Ok(RelayCommandResponse::Diagnostics {
                    attached_clients: self
                        .clients
                        .iter()
                        .filter(|(_, (owner, _))| *owner == relay_handle)
                        .count() as u64,
                    inbound_frames: inbound_frames as u64,
                    outbound_frames: outbound_frames as u64,
                })
            }
        }
    }

    fn admit_scope(
        &mut self,
        request: RelayScopeAdmissionRequest,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        RelayScope::from(request.scope.clone())
            .validate()
            .map_err(relay_status)?;
        if matches!(request.identity.author, jazz::ids::AuthorSubject::System) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        reject_bearer_claims(&request.claims)?;
        let public_schema = serde_json::from_str(&request.schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema =
            JazzSchema::new(&public_schema).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let config = RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_VERSION,
                maximum: NATIVE_RELAY_ABI_VERSION,
            },
            scope: request.scope.into(),
            sqlite_path: PathBuf::from(request.sqlite_path),
            schema,
            identity: request.identity,
            #[cfg(test)]
            thread_start_counter: self.thread_start_counter.clone(),
        };
        // A scope is immutable once trusted code has admitted it. Reject a
        // conflicting second configuration before JavaScript can receive a
        // capability, even if no relay alias has been opened yet.
        if self.admitted_scopes.values().any(|admitted| {
            admitted.config.scope == config.scope
                && (admitted.config.sqlite_path != config.sqlite_path
                    || admitted.config.schema.version_id() != config.schema.version_id()
                    || admitted.config.identity != config.identity
                    || admitted.claims != request.claims)
        }) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let handle = self.allocate_admission_capability().map_err(relay_status)?;
        self.admitted_scopes.insert(
            handle,
            AdmittedRelayScope {
                config,
                claims: request.claims,
            },
        );
        Ok(handle)
    }

    fn revoke_scope(&mut self, admitted_scope: AdmissionCapability) -> bool {
        if self.admitted_scopes.remove(&admitted_scope).is_none() {
            return false;
        }
        let relay_handles = self
            .relays
            .iter()
            .filter_map(|(handle, opened)| {
                (opened.admitted_scope == admitted_scope).then_some(*handle)
            })
            .collect::<Vec<_>>();
        let mut removed_scopes = Vec::new();
        for relay_handle in relay_handles {
            if let Some(opened) = self.relays.remove(&relay_handle) {
                removed_scopes.push(opened.scope);
            }
            let client_handles = self
                .clients
                .iter()
                .filter_map(|(handle, (owner, _))| (*owner == relay_handle).then_some(*handle))
                .collect::<Vec<_>>();
            for client_handle in client_handles {
                if let Some((_, client)) = self.clients.remove(&client_handle) {
                    let _ = client.close();
                }
            }
        }
        removed_scopes.sort();
        removed_scopes.dedup();
        for scope in removed_scopes {
            if !self.relays.values().any(|opened| opened.scope == scope) {
                let _ = self.registry.close(&scope);
            }
        }
        true
    }
}

fn client_identity(handle: u64, author: jazz::ids::AuthorSubject) -> DbIdentity {
    let mut node = [0_u8; 16];
    node[..8].copy_from_slice(b"JAZZRN\0\0");
    node[8..].copy_from_slice(&handle.to_be_bytes());
    DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes(node),
        author,
    }
}

fn relay_status(error: RelayError) -> JazzNativeRelayStatus {
    match error {
        RelayError::InvalidAbiRange { .. } => JazzNativeRelayStatus::InvalidAbiRange,
        RelayError::IncompatibleAbi { .. } => JazzNativeRelayStatus::IncompatibleAbi,
        RelayError::QueueCapacityExceeded { .. } => JazzNativeRelayStatus::Backpressure,
        RelayError::Db(error) if error.code == jazz::db::ErrorCode::Backpressure => {
            JazzNativeRelayStatus::Backpressure
        }
        _ => JazzNativeRelayStatus::LifecycleFailure,
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
/// until a future command explicitly defines them. `out` must already be empty
/// or freed before reuse; resetting an owned buffer would lose its allocation.
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
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    // SAFETY: `out` is non-null and exclusively owned by the caller for this
    // call. Reset it before decoding so every error has one unambiguous state.
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if request.is_null() && request_len != 0 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
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
    inner: Mutex<NativeRelayHost>,
}

#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_host_new() -> *mut JazzNativeRelayHost {
    Box::into_raw(Box::new(JazzNativeRelayHost {
        inner: Mutex::new(NativeRelayHost::default()),
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
/// `out` must already be empty or freed before reuse.
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
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe {
        *out = JazzNativeRelayBytes::EMPTY;
    }
    if host.is_null() || (request.is_null() && request_len != 0) {
        return JazzNativeRelayStatus::InvalidArgument;
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
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let response = match host.execute(command) {
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

/// Admit one complete trusted scope described as strict JSON by Kotlin or
/// Swift/Objective-C. This is intentionally a separate platform-only entry
/// point: generic JavaScript `execute` accepts only [`RelayCommandRequest`]
/// and can never carry paths, schema, claims, or bearer credentials.
///
/// The returned bytes are exactly one random 256-bit capability. They are
/// opaque to JavaScript; the platform host may hand them to foreground code,
/// but only the native relay can interpret them.
///
/// # Safety
/// `host`, request bytes, and `out` follow the same rules as
/// [`jazz_native_relay_host_execute`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_admit_scope_json(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if host.is_null()
        || request.is_null()
        || request_len == 0
        || request_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = unsafe { std::slice::from_raw_parts(request, request_len) };
    let request = match serde_json::from_slice::<TrustedRelayScopeAdmissionJson>(request) {
        Ok(request) => request,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let request = match request.normalize() {
        Ok(request) => request,
        Err(status) => return status,
    };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let capability = match host.admit_scope(request) {
        Ok(capability) => capability,
        Err(status) => return status,
    };
    let mut capability = capability.0.to_vec();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: capability.len(),
            data: capability.as_mut_ptr(),
        };
    }
    std::mem::forget(capability);
    JazzNativeRelayStatus::Ok
}

/// Revoke exactly one opaque 256-bit admission capability held by trusted
/// platform lifecycle code. This avoids making Kotlin/Swift encode a postcard
/// revocation request and remains deliberately unavailable to JavaScript.
///
/// # Safety
/// `host` must be a live host pointer. When non-null, `capability` must point
/// to exactly 32 readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_revoke_scope_capability(
    host: *mut JazzNativeRelayHost,
    capability: *const u8,
    capability_len: usize,
) -> JazzNativeRelayStatus {
    if host.is_null() || capability.is_null() || capability_len != 32 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let capability = unsafe { std::slice::from_raw_parts(capability, capability_len) };
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(capability);
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let _ = host.revoke_scope(AdmissionCapability(bytes));
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
            Some(auth_scope) if !auth_scope.trim().is_empty() => {}
            Some(_) => {
                return Err(RelayError::InvalidScope(
                    "auth scope must not be empty".to_owned(),
                ));
            }
            None => {
                return Err(RelayError::InvalidScope(
                    "auth scope is required for a persistent relay".to_owned(),
                ));
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
    wire: NativeRelayWire,
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

    pub fn wire(&self) -> NativeRelayWire {
        self.wire.clone()
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
    identity: DbIdentity,
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
#[derive(Clone)]
pub struct NativeRelayWire {
    inbound: Arc<Mutex<BoundedMessageQueue>>,
    outbound: Arc<Mutex<BoundedMessageQueue>>,
}

impl Default for NativeRelayWire {
    fn default() -> Self {
        Self {
            inbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
            outbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
        }
    }
}

struct QueuedMessage {
    message: SyncMessage,
    encoded_len: usize,
}

#[derive(Default)]
struct BoundedMessageQueue {
    messages: VecDeque<QueuedMessage>,
    encoded_bytes: usize,
}

impl BoundedMessageQueue {
    fn push(&mut self, message: SyncMessage, direction: &'static str) -> Result<(), RelayError> {
        if self.messages.len() >= NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            return Err(RelayError::QueueCapacityExceeded {
                direction,
                queued_messages: self.messages.len(),
                queued_bytes: self.encoded_bytes,
            });
        }
        let encoded_len = encode_sync_message(&message)
            .map_err(RelayError::EncodePeerMessage)?
            .len();
        validate_encoded_peer_message_len(encoded_len)?;
        let next_bytes = self.encoded_bytes.saturating_add(encoded_len);
        if next_bytes > NATIVE_RELAY_QUEUE_MAX_BYTES {
            return Err(RelayError::QueueCapacityExceeded {
                direction,
                queued_messages: self.messages.len(),
                queued_bytes: self.encoded_bytes,
            });
        }
        self.messages.push_back(QueuedMessage {
            message,
            encoded_len,
        });
        self.encoded_bytes = next_bytes;
        Ok(())
    }

    fn pop(&mut self) -> Option<SyncMessage> {
        let queued = self.messages.pop_front()?;
        self.encoded_bytes -= queued.encoded_len;
        Some(queued.message)
    }

    fn drain_messages(&mut self) -> Vec<SyncMessage> {
        let mut drained = Vec::new();
        let mut drained_bytes = 0_usize;
        while drained.len() < NATIVE_RELAY_DRAIN_MAX_MESSAGES {
            let Some(front) = self.messages.front() else {
                break;
            };
            if !drained.is_empty()
                && drained_bytes.saturating_add(front.encoded_len) > NATIVE_RELAY_DRAIN_TARGET_BYTES
            {
                break;
            }
            let queued = self.messages.pop_front().expect("front was present");
            drained_bytes += queued.encoded_len;
            self.encoded_bytes -= queued.encoded_len;
            drained.push(queued.message);
        }
        drained
    }

    fn len(&self) -> usize {
        self.messages.len()
    }
}

impl NativeRelayWire {
    pub fn push_inbound(&self, message: SyncMessage) -> Result<(), RelayError> {
        self.inbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream inbound queue"))?
            .push(message, "inbound")
    }

    pub fn take_outbound(&self) -> Result<Vec<SyncMessage>, RelayError> {
        Ok(self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream outbound queue"))?
            .drain_messages())
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
            .messages
            .iter()
            .take(NATIVE_RELAY_DRAIN_MAX_MESSAGES)
            .scan(0_usize, |bytes, queued| {
                if *bytes != 0
                    && bytes.saturating_add(queued.encoded_len) > NATIVE_RELAY_DRAIN_TARGET_BYTES
                {
                    return None;
                }
                *bytes += queued.encoded_len;
                Some(&queued.message)
            })
            .map(|message| {
                let bytes = encode_sync_message(message).map_err(RelayError::EncodePeerMessage)?;
                validate_logical_message_len(bytes.len())
                    .map_err(RelayError::PeerMessageTooLarge)?;
                Ok(bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for _ in 0..encoded.len() {
            let _ = outbound.pop();
        }
        Ok(encoded)
    }

    pub fn queue_depths(&self) -> Result<(usize, usize), RelayError> {
        let inbound = self
            .inbound
            .lock()
            .map_err(|_| RelayError::Poisoned("native relay inbound queue"))?
            .len();
        let outbound = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("native relay outbound queue"))?
            .len();
        Ok((inbound, outbound))
    }
}

fn validate_encoded_peer_message_len(len: usize) -> Result<(), RelayError> {
    validate_logical_message_len(len).map_err(RelayError::PeerMessageTooLarge)
}

struct QueueTransport {
    wire: NativeRelayWire,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.wire
            .outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay outbound queue poisoned".to_owned()))?
            .push(message, "outbound")
            .map_err(transport_queue_error)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.wire.inbound.lock().ok()?.pop()
    }
}

struct DuplexTransport {
    wire: NativeRelayWire,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.wire
            .outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay client queue poisoned".to_owned()))?
            .push(message, "client outbound")
            .map_err(transport_queue_error)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.wire.inbound.lock().ok()?.pop()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>, NativeRelayWire) {
    let wire = NativeRelayWire::default();
    let reverse = NativeRelayWire {
        inbound: Arc::clone(&wire.outbound),
        outbound: Arc::clone(&wire.inbound),
    };
    (
        Box::new(DuplexTransport { wire: wire.clone() }),
        Box::new(DuplexTransport { wire: reverse }),
        wire,
    )
}

struct ConnectedClient {
    db: Db<MemoryStorage>,
    wire: NativeRelayWire,
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
    pump_cursor: Option<u64>,
    schema: JazzSchema,
}

impl RelayWorker {
    fn open(config: RelayOpenConfig, wire: NativeRelayWire) -> Result<Self, RelayError> {
        let column_families = config.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let codec_profile = epoch_1_storage_codec_profile().map_err(RelayError::Storage)?;
        let persistent = block_on(Db::open(DbConfig {
            schema: config.schema.clone(),
            storage: SqliteStorage::open_with_durability_and_codec_profile(
                config.sqlite_path,
                &refs,
                SqliteDurability::WalNoSync,
                &codec_profile,
            )
            .map_err(RelayError::Storage)?,
            identity: config.identity,
            id_source: None,
        }))
        .map_err(RelayError::Db)?;
        let upstream =
            block_on(persistent.connect_upstream(Box::new(QueueTransport { wire: wire.clone() })));
        Ok(Self {
            persistent,
            _upstream: upstream,
            clients: BTreeMap::new(),
            next_client_id: 1,
            pump_cursor: None,
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
            storage: MemoryStorage::new(&refs).expect("valid memory storage families"),
            identity,
            id_source: None,
        }))
        .map_err(RelayError::Db)?;
        let (client_transport, relay_transport, wire) = duplex();
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
                wire,
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
        let client_ids = bounded_round_robin_ids(&self.clients, self.pump_cursor);
        if let Some(last) = client_ids.last() {
            self.pump_cursor = Some(*last);
        }
        for id in &client_ids {
            let client = &self.clients[id];
            map_tick_result(block_on(client.db.tick()))?;
        }
        map_tick_result(block_on(self.persistent.tick()))?;
        for id in &client_ids {
            let client = &self.clients[id];
            map_tick_result(block_on(client.db.tick()))?;
        }
        Ok(())
    }
}

fn map_tick_result<T>(result: Result<T, jazz::db::Error>) -> Result<(), RelayError> {
    result.map(|_| ()).map_err(RelayError::Db)
}

fn transport_queue_error(error: RelayError) -> TransportError {
    match error {
        RelayError::QueueCapacityExceeded { .. } => TransportError::Backpressure,
        error => TransportError::Failed(error.to_string()),
    }
}

fn bounded_round_robin_ids<T>(clients: &BTreeMap<u64, T>, cursor: Option<u64>) -> Vec<u64> {
    let mut client_ids = clients.keys().copied().collect::<Vec<_>>();
    if let Some(cursor) = cursor {
        let split = client_ids.partition_point(|id| *id <= cursor);
        client_ids.rotate_left(split);
    }
    client_ids.truncate(NATIVE_RELAY_PUMP_MAX_CLIENTS);
    client_ids
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
        let identity = config.identity;
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
                identity,
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
            wire: self.run(move |worker| {
                worker
                    .clients
                    .get(&id)
                    .map(|client| client.wire.clone())
                    .ok_or(RelayError::UnknownClient(id))
            })?,
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
                || existing.inner.identity != config.identity
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
    #[error(
        "native relay {direction} queue capacity exceeded ({queued_messages} messages, {queued_bytes} encoded bytes); drain or pump before retrying"
    )]
    QueueCapacityExceeded {
        direction: &'static str,
        queued_messages: usize,
        queued_bytes: usize,
    },
    #[error("invalid native relay scope: {0}")]
    InvalidScope(String),
    #[error("failed to open native relay owner thread: {0}")]
    OwnerThread(String),
    #[error("native relay host entropy failed: {0}")]
    Entropy(String),
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
    use jazz::db::InsertOptions;
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
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
                author: AuthorSubject::for_test_bytes([0xa2; 16]),
            },
            thread_start_counter: None,
        }
    }

    #[test]
    fn public_relay_open_rejects_a_planted_groove_only_manifest() {
        // This is an internal physical-admission receipt. The public relay
        // opener must not silently adopt a root that was initialized through
        // SQLite's generic Groove-only convenience API.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("relay.sqlite");
        let config = config(path.clone(), Some("alice"));
        let column_families = config.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        drop(SqliteStorage::open(&path, &refs).expect("plant generic Groove manifest"));

        let registry = NativeRelayRegistry::default();
        assert!(
            registry.open(config).is_err(),
            "the public relay open must reject a manifest that omits Jazz codecs"
        );
    }

    #[test]
    fn attached_client_identity_preserves_the_admitted_canonical_author() {
        let admitted = AuthorSubject::authenticated("https://issuer.example", "opaque-subject")
            .expect("fixture issuer and subject are valid");

        let client = client_identity(42, admitted);

        assert_eq!(client.author, admitted);
        assert_eq!(
            client.author.canonical(),
            r#"["https://issuer.example","opaque-subject"]"#
        );
        assert_ne!(client.author, AuthorSubject::SYSTEM);
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
        let mut wrong_identity = config(directory.path().join("alice.sqlite"), Some("alice"));
        wrong_identity.identity = DbIdentity {
            node: NodeUuid::from_bytes([0xe1; 16]),
            author: AuthorSubject::for_test_bytes([0xe2; 16]),
        };
        assert!(matches!(
            registry.open(wrong_identity),
            Err(RelayError::ScopeConfigurationMismatch)
        ));
        let mut wrong_schema = config(directory.path().join("alice.sqlite"), Some("alice"));
        wrong_schema.schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes").column("body", ColumnType::Text))
                .build(),
        )
        .unwrap();
        assert!(matches!(
            registry.open(wrong_schema),
            Err(RelayError::ScopeConfigurationMismatch)
        ));

        let first_client = first
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xb1; 16]),
                    author: AuthorSubject::for_test_bytes([0xb2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let second_client = same
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xc2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        assert_ne!(first_client.id(), second_client.id());

        first_client
            .with_db(|db| {
                let write = block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0xd1; 16])),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                block_on(write.wait(DurabilityTier::Local)).map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();
        second_client
            .with_db(|db| {
                let write = block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0xd2; 16])),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                block_on(write.wait(DurabilityTier::Local)).map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();

        // Saturate the stateful upstream transport before the relay tries to
        // forward either commit. Core must retain the unsent protocol state on
        // `TransportError::Backpressure`, then retry it after the host drains.
        let filler = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };
        while first.wire().queue_depths().unwrap().1 < NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            first
                .wire()
                .outbound
                .lock()
                .unwrap()
                .push(filler.clone(), "test saturated upstream")
                .unwrap();
        }
        first.pump().unwrap();
        assert_eq!(
            first.wire().queue_depths().unwrap().1,
            NATIVE_RELAY_QUEUE_MAX_MESSAGES,
            "backpressured stateful messages must remain pending instead of displacing the queue",
        );
        while first.wire().queue_depths().unwrap().1 != 0 {
            let drained = first.wire().take_outbound().unwrap();
            assert!(!drained.is_empty());
        }
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
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
        };
        let bytes = encode_sync_message(&message).unwrap();

        wire.push_inbound_encoded(&bytes).unwrap();
        assert_eq!(wire.inbound.lock().unwrap().pop(), Some(message.clone()));

        wire.outbound
            .lock()
            .unwrap()
            .push(message, "test outbound")
            .unwrap();
        let encoded = wire.take_outbound_encoded().unwrap();
        assert_eq!(encoded.len(), 1);
        assert_eq!(wire.outbound.lock().unwrap().len(), 0);
        assert_eq!(
            decode_sync_message(&encoded[0]).unwrap(),
            SyncMessage::SessionClaims {
                identity: AuthorSubject::SYSTEM,
                claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
            }
        );
    }

    #[test]
    fn client_transport_keeps_the_wire_boundary_opaque_and_directional() {
        let directory = tempfile::tempdir().unwrap();
        let relay =
            NativeRelay::spawn(config(directory.path().join("relay.sqlite"), Some("alice")))
                .unwrap();
        let client = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0x91; 16]),
                    author: AuthorSubject::for_test_bytes([0x92; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let frame = encode_sync_message(&SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        })
        .unwrap();

        client.wire().push_inbound_encoded(&frame).unwrap();
        assert_eq!(client.wire().queue_depths().unwrap(), (1, 0));
        relay.pump().unwrap();
        assert_eq!(client.wire().queue_depths().unwrap().0, 0);
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
    fn native_queue_applies_count_backpressure_without_losing_admitted_messages() {
        let message = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };
        let wire = NativeRelayWire::default();
        for _ in 0..NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            wire.push_inbound(message.clone()).unwrap();
        }
        assert!(matches!(
            wire.push_inbound(message),
            Err(RelayError::QueueCapacityExceeded {
                queued_messages: NATIVE_RELAY_QUEUE_MAX_MESSAGES,
                ..
            })
        ));
        assert_eq!(
            wire.queue_depths().unwrap().0,
            NATIVE_RELAY_QUEUE_MAX_MESSAGES
        );
    }

    #[test]
    fn native_queue_drains_in_bounded_batches() {
        let wire = NativeRelayWire::default();
        for _ in 0..(NATIVE_RELAY_DRAIN_MAX_MESSAGES + 1) {
            wire.outbound
                .lock()
                .unwrap()
                .push(
                    SyncMessage::SessionClaims {
                        identity: AuthorSubject::SYSTEM,
                        claims: BTreeMap::new(),
                    },
                    "test outbound",
                )
                .unwrap();
        }
        assert_eq!(
            wire.take_outbound_encoded().unwrap().len(),
            NATIVE_RELAY_DRAIN_MAX_MESSAGES
        );
        assert_eq!(wire.queue_depths().unwrap().1, 1);
    }

    #[test]
    fn pump_client_selection_is_bounded_and_round_robins() {
        let clients = (1..=(NATIVE_RELAY_PUMP_MAX_CLIENTS as u64 + 3))
            .map(|id| (id, ()))
            .collect::<BTreeMap<_, _>>();
        let first = bounded_round_robin_ids(&clients, None);
        assert_eq!(first.len(), NATIVE_RELAY_PUMP_MAX_CLIENTS);
        assert_eq!(first[0], 1);
        let second = bounded_round_robin_ids(&clients, first.last().copied());
        assert_eq!(&second[..3], &[65, 66, 67]);
        assert_eq!(second[3], 1);
    }

    #[test]
    fn c_host_lifecycle_open_attach_close_and_bounded_pump_are_handle_safe() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null());
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::for_test_bytes([0x72; 16]),
        };
        let admission = RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "host-receipt".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-subject".to_owned()),
            },
            sqlite_path: directory.path().join("host.sqlite").display().to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity,
            claims: BTreeMap::new(),
        };
        let admitted_scope = unsafe { (*host).inner.lock().unwrap().admit_scope(admission) }
            .expect("test admission is valid");
        let open = RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_VERSION,
            supported_abi_maximum: NATIVE_RELAY_ABI_VERSION,
            admitted_scope,
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
        let relay = match unsafe { command(host, open.clone()) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alias = match unsafe { command(host, open.clone()) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected alias response: {response:?}"),
        };
        assert_ne!(relay, alias);
        unsafe {
            let host = (*host).inner.lock().unwrap();
            assert!(Arc::ptr_eq(
                &host.relays.get(&relay).unwrap().relay.inner,
                &host.relays.get(&alias).unwrap().relay.inner,
            ));
        }
        let client = match unsafe { command(host, RelayCommandRequest::Attach { relay }) }.unwrap()
        {
            RelayCommandResponse::Attached { client } => client,
            response => panic!("unexpected attach response: {response:?}"),
        };
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay: alias }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseClient { client }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        let reopened = match unsafe { command(host, open) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected reopen response: {response:?}"),
        };
        unsafe {
            let host = (*host).inner.lock().unwrap();
            assert!(Arc::ptr_eq(
                &host.relays.get(&alias).unwrap().relay.inner,
                &host.relays.get(&reopened).unwrap().relay.inner,
            ));
        }
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Err(JazzNativeRelayStatus::InvalidHandle)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay: alias }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay: reopened }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay: reopened }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn c_host_open_rejects_wrapper_abi_before_storage_and_resets_output() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-open.sqlite");
        let host = jazz_native_relay_host_new();
        let threads_started = Arc::new(AtomicUsize::new(0));
        unsafe {
            (*host).inner.lock().unwrap().thread_start_counter = Some(Arc::clone(&threads_started))
        };
        let admitted_scope = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "abi-rejection".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-subject".to_owned()),
                    },
                    sqlite_path: sqlite_path.display().to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0x81; 16]),
                        author: AuthorSubject::for_test_bytes([0x82; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .unwrap()
        };
        let request = |minimum, maximum| RelayCommandRequest::Open {
            supported_abi_minimum: minimum,
            supported_abi_maximum: maximum,
            admitted_scope,
        };
        for (request, expected) in [
            (request(3, 2), JazzNativeRelayStatus::InvalidAbiRange),
            (request(2, 2), JazzNativeRelayStatus::IncompatibleAbi),
        ] {
            let encoded = postcard::to_allocvec(&request).unwrap();
            let mut output = JazzNativeRelayBytes {
                data: std::ptr::dangling_mut(),
                len: 99,
            };
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_execute(
                        host,
                        encoded.as_ptr(),
                        encoded.len(),
                        &mut output,
                    )
                },
                expected
            );
            assert!(output.data.is_null());
            assert_eq!(output.len, 0);
            assert!(!sqlite_path.exists());
            assert_eq!(threads_started.load(Ordering::Relaxed), 0);
        }
        let mut output = JazzNativeRelayBytes {
            data: std::ptr::dangling_mut(),
            len: 77,
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_execute(
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    1,
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidArgument
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        unsafe { jazz_native_relay_host_free(host) };
    }

    // This is necessarily an internal ABI-boundary test: Kotlin/Swift do not
    // run in this Rust target, but the production C entry proves their only
    // permitted admission path rejects malformed/credential-bearing config
    // before a JavaScript-visible capability exists.
    #[test]
    fn trusted_json_admission_is_strict_bounded_and_never_echoes_config() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorSubject::for_test_bytes([0x92; 16]),
        };
        let request = |claims: BTreeMap<String, Value>| {
            serde_json::json!({
                "scope": {
                    "app_namespace": "trusted-host",
                    "storage_namespace": "primary",
                    "auth_scope": "opaque-validated-subject",
                },
                "sqlite_path": directory.path().join("trusted.sqlite").display().to_string(),
                "schema_json": serde_json::to_string(schema().public_schema()).unwrap(),
                "identity": serde_json::to_value(identity).unwrap(),
                "claims": serde_json::to_value(claims).unwrap(),
            })
        };

        // The platform-owned C ABI is the only production path that can mint
        // a capability. Exercise the JSON forms serde would otherwise map to
        // `Option::None` here, and prove they fail before either registry can
        // change. This is intentionally an ABI-boundary test rather than a
        // Rust-only `RelayScope` unit test.
        for (case, auth_scope) in [
            ("omitted", None),
            ("null", Some(serde_json::Value::Null)),
            ("empty", Some(serde_json::Value::String(String::new()))),
            (
                "whitespace",
                Some(serde_json::Value::String(" \t\n ".to_owned())),
            ),
        ] {
            let mut rejected = request(BTreeMap::new());
            let scope = rejected["scope"]
                .as_object_mut()
                .expect("trusted request has an object scope");
            match auth_scope {
                Some(auth_scope) => {
                    scope.insert("auth_scope".to_owned(), auth_scope);
                }
                None => {
                    scope.remove("auth_scope");
                }
            }
            let rejected = serde_json::to_vec(&rejected).unwrap();
            let mut output = JazzNativeRelayBytes {
                data: std::ptr::dangling_mut(),
                len: 99,
            };
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_admit_scope_json(
                        host,
                        rejected.as_ptr(),
                        rejected.len(),
                        &mut output,
                    )
                },
                JazzNativeRelayStatus::LifecycleFailure,
                "{case} auth scope must fail closed",
            );
            assert!(output.data.is_null(), "{case} must not return a capability");
            assert_eq!(output.len, 0, "{case} must not return a capability");
            let host_state = unsafe { (*host).inner.lock().unwrap() };
            assert!(
                host_state.admitted_scopes.is_empty(),
                "{case} must not mutate the admission registry"
            );
            assert!(host_state.relays.is_empty(), "{case} must not open a relay");
        }

        let encoded = serde_json::to_vec(&request(BTreeMap::from([(
            "role".to_owned(),
            Value::String("member".to_owned()),
        )])))
        .unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    encoded.as_ptr(),
                    encoded.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(output.len, 32, "only the opaque capability crosses out");
        assert_ne!(
            unsafe { std::slice::from_raw_parts(output.data, output.len) },
            encoded.as_slice(),
            "the trusted config must never be reflected to JavaScript"
        );
        unsafe { jazz_native_relay_bytes_free(&mut output) };

        let mut unknown = request(BTreeMap::new());
        unknown["unexpected"] = serde_json::Value::Bool(true);
        let unknown = serde_json::to_vec(&unknown).unwrap();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    unknown.as_ptr(),
                    unknown.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidCommand
        );

        let mut bearer = request(BTreeMap::from([(
            "access_token".to_owned(),
            Value::String("never-persist-a-bearer".to_owned()),
        )]));
        // Keep this otherwise-valid request outside the already-admitted
        // scope, so rejection proves the credential filter rather than the
        // immutable-scope configuration check below.
        bearer["scope"]["auth_scope"] =
            serde_json::Value::String("different-valid-subject".to_owned());
        let bearer = serde_json::to_vec(&bearer).unwrap();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    bearer.as_ptr(),
                    bearer.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::LifecycleFailure
        );

        let oversized = vec![b'x'; NATIVE_RELAY_ADMISSION_MAX_BYTES + 1];
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    oversized.as_ptr(),
                    oversized.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidArgument
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn trusted_admission_rejects_conflicting_scope_before_open() {
        let directory = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let admission = |sqlite_path: &str,
                         schema_json: String,
                         identity: DbIdentity,
                         claims: BTreeMap<String, Value>| {
            RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "trusted-host".to_owned(),
                    storage_namespace: "primary".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: sqlite_path.to_owned(),
                schema_json,
                identity,
                claims,
            }
        };
        let primary = directory
            .path()
            .join("primary.sqlite")
            .display()
            .to_string();
        let schema_json = serde_json::to_string(schema().public_schema()).unwrap();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        };
        let claims = BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]);
        host.admit_scope(admission(
            &primary,
            schema_json.clone(),
            identity,
            claims.clone(),
        ))
        .unwrap();

        let other_path = directory.path().join("other.sqlite").display().to_string();
        let other_schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("other").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let other_identity = DbIdentity {
            node: NodeUuid::from_bytes([0xb1; 16]),
            author: AuthorSubject::for_test_bytes([0xb2; 16]),
        };
        for (label, request) in [
            (
                "SQLite path",
                admission(&other_path, schema_json.clone(), identity, claims.clone()),
            ),
            (
                "schema",
                admission(
                    &primary,
                    serde_json::to_string(other_schema.public_schema()).unwrap(),
                    identity,
                    claims.clone(),
                ),
            ),
            (
                "durable identity",
                admission(
                    &primary,
                    schema_json.clone(),
                    other_identity,
                    claims.clone(),
                ),
            ),
            (
                "validated claims",
                admission(
                    &primary,
                    schema_json.clone(),
                    identity,
                    BTreeMap::from([("role".to_owned(), Value::String("admin".to_owned()))]),
                ),
            ),
        ] {
            assert_eq!(
                host.admit_scope(request),
                Err(JazzNativeRelayStatus::LifecycleFailure),
                "a scope cannot mint a second capability with changed trusted {label}"
            );
            assert_eq!(
                host.admitted_scopes.len(),
                1,
                "failed {label} admission must not leave a usable capability"
            );
        }
    }

    #[test]
    fn relay_scope_requires_a_nonempty_authenticated_scope() {
        let directory = tempfile::tempdir().unwrap();
        let mut relay = config(directory.path().join("empty-auth.sqlite"), Some("   "));

        assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));

        relay.scope.auth_scope = None;
        assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));
    }

    #[test]
    fn admitted_scope_capabilities_are_unguessable_and_revocation_closes_all_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let admission = |name: &str, byte: u8| RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "capability-test".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some(name.to_owned()),
            },
            sqlite_path: directory
                .path()
                .join(format!("{name}.sqlite"))
                .display()
                .to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([byte; 16]),
                author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
            },
            claims: BTreeMap::from([("sub".to_owned(), Value::String(name.to_owned()))]),
        };
        unsafe fn admit(
            host: *mut JazzNativeRelayHost,
            request: RelayScopeAdmissionRequest,
        ) -> AdmissionCapability {
            unsafe { (*host).inner.lock().unwrap().admit_scope(request) }
                .expect("test admission is valid")
        }
        let alice = unsafe { admit(host, admission("alice", 0xa1)) };
        let bob = unsafe { admit(host, admission("bob", 0xb1)) };
        assert_ne!(alice, bob);
        assert_ne!(alice.0, [0; 32]);
        assert_ne!(bob.0, [0; 32]);

        let open = |admitted_scope| RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_VERSION,
            supported_abi_maximum: NATIVE_RELAY_ABI_VERSION,
            admitted_scope,
        };
        let execute = |request| unsafe { (*host).inner.lock().unwrap().execute(request) };
        let alice_relay = match execute(open(alice)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alice_alias = match execute(open(alice)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alice_client =
            match execute(RelayCommandRequest::Attach { relay: alice_relay }).unwrap() {
                RelayCommandResponse::Attached { client } => client,
                response => panic!("unexpected attach response: {response:?}"),
            };
        let bob_relay = match execute(open(bob)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };

        let mut guessed = alice;
        guessed.0[0] ^= 0x80;
        assert_eq!(
            execute(open(guessed)),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );

        assert!(unsafe { (*host).inner.lock().unwrap().revoke_scope(alice) });

        assert_eq!(
            execute(open(alice)),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::Pump { relay: alice_relay }),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::Pump { relay: alice_alias }),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::CloseClient {
                client: alice_client
            })
            .unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert!(matches!(
            execute(RelayCommandRequest::Pump { relay: bob_relay }),
            Ok(RelayCommandResponse::Pumped)
        ));
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn c_host_serializes_concurrent_commands() {
        let host = jazz_native_relay_host_new() as usize;
        let probe = postcard::to_allocvec(&RelayCommandRequest::Probe).unwrap();
        let workers = (0..8)
            .map(|_| {
                let probe = probe.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let mut output = JazzNativeRelayBytes::EMPTY;
                        assert_eq!(
                            unsafe {
                                jazz_native_relay_host_execute(
                                    host as *mut JazzNativeRelayHost,
                                    probe.as_ptr(),
                                    probe.len(),
                                    &mut output,
                                )
                            },
                            JazzNativeRelayStatus::Ok
                        );
                        unsafe { jazz_native_relay_bytes_free(&mut output) };
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        unsafe { jazz_native_relay_host_free(host as *mut JazzNativeRelayHost) };
    }
}
