//! Native, process-local Jazz relay for mobile and future platform bindings.
//!
//! The relay is deliberately a host component, not another Jazz runtime. It
//! owns a durable [`jazz::db::Db`] over SQLite and serves one in-memory `Db`
//! for each UI runtime over the ordinary Jazz peer protocol. React Native,
//! Swift, and Kotlin bindings put their ABI-specific command codecs above this
//! crate; they do not implement query, write, policy, or sync behavior here.

mod account_crypto;
mod account_session;
pub use account_crypto::{
    jazz_native_relay_account_secret, jazz_native_relay_mint_local_first_token,
};
pub use account_session::{
    jazz_native_relay_host_lease_attach_account_schema_json,
    jazz_native_relay_host_lease_begin_account_session_json,
    jazz_native_relay_host_lease_refresh_account_session,
    jazz_native_relay_host_lease_release_account_session,
};
mod node_lease;
mod relay;
pub use relay::*;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use jazz::binding_host;
use jazz::db::{DbIdentity, RemoteLinkHint};
use jazz::ids::NodeUuid;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::NativeTransportConnector;
use jazz::tools::websocket_prelude_auth::AuthConfig;
use jazz_native_transport::NativeWebSocketConnector;
use node_lease::{ForegroundNodeLease, ForegroundNodeLeasePool};

/// The current native-relay ABI version. (The name is kept for the exported
/// `jazz-rn` constant.) Breaking command/response changes bump the value:
/// 1 was the first public ABI; 2 added `CodedOperationError` answers to
/// existing commands, so a JS bundle and native build from different ABIs
/// refuse to open rather than misread a failure; 3 added its `reason_name`.
/// Append-only commands use optional native capabilities while existing
/// command layouts remain stable.
pub const NATIVE_RELAY_ABI_V1: u16 = 3;

const FOREGROUND_WAKE_IMMEDIATE: u8 = 0;
const FOREGROUND_WAKE_CANCELLED: u8 = 3;
pub type ForegroundWakeCallback = unsafe extern "C" fn(*mut c_void, u64, u8, u64);

/// Foreground commands are copied across the JSI/C boundary and decoded before
/// they reach a foreground owner. Keep this independent from both peer-frame
/// and trusted-admission budgets.
const NATIVE_RELAY_FOREGROUND_COMMAND_MAX_BYTES: usize = 1024 * 1024;
/// Direct C callers are a test/embedding seam, never a platform JSI runtime.
/// Keep their aliases distinct from every platform-issued runtime token.
const DIRECT_FOREGROUND_RUNTIME_TOKEN: u64 = u64::MAX;
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

/// Credential-bearing setup is a private platform-to-relay handoff.  The
/// bearer is decoded only through Jazz's shared *unverified* scope projection
/// so a refresh can select a distinct local cache.  It is never verified,
/// turned into claims, or exposed to postcard/JSI; Core authentication remains
/// authoritative.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateSessionSetupJson {
    server_url: String,
    app_id: String,
    jwt: String,
    storage_root: String,
}

#[derive(Clone)]
struct PendingPrivateSession {
    claims: BTreeMap<String, Value>,
    scope: RelayScopeRequest,
    sqlite_path: String,
    identity: DbIdentity,
    socket: Option<PrivateRelaySocketSession>,
}

/// Ephemeral native-only input retained only until trusted revocation. It is
/// never part of an admitted scope, relay diagnostics, postcard, or SQLite.
#[derive(Clone, PartialEq, Eq)]
struct PrivateRelaySocketSession {
    server_url: String,
    app_id: String,
    bearer: String,
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

/// A bearer may traverse plaintext only to a platform-local server used by the
/// test harness or an emulator. Real remote server sessions require HTTPS/WSS;
/// accepting arbitrary `http://` here would let a private-session bearer leak
/// before Core can authenticate it.
fn validate_private_session_endpoint(server_url: &str) -> Result<url::Url, JazzNativeRelayStatus> {
    let url =
        url::Url::parse(server_url.trim()).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
    match url.scheme() {
        "https" => Ok(url),
        "http" if private_plaintext_host_is_allowed(&url) => Ok(url),
        "http" if url.host_str().is_some() => Err(JazzNativeRelayStatus::RemotePlaintextEndpoint),
        "http" => Err(JazzNativeRelayStatus::LifecycleFailure),
        _ => Err(JazzNativeRelayStatus::LifecycleFailure),
    }
}

fn private_plaintext_host_is_allowed(url: &url::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || matches!(host, "10.0.2.2" | "10.0.3.2")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
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

/// Commands for one opaque in-memory foreground `Db`.
///
/// This is intentionally a separate vocabulary from [`RelayCommandRequest`]:
/// relay commands own persistent-relay lifecycle and peer frames, while these
/// commands own the existing byte-oriented `NativeDb` surface for one UI
/// runtime. Both are postcard and are versioned by [`NATIVE_RELAY_ABI_V1`].
/// A caller can carry an opaque foreground handle only after capability-only
/// admission; it can never smuggle an open configuration through this codec.
///
/// Query bytes use the canonical `Query` carrier already produced by the
/// shared JS codec. This is intentionally *not* a second RN query AST.
///
/// The greenfield JS/native command payloads are deployed together. Their
/// postcard layouts are pinned by the matching Rust and TypeScript byte corpus.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundDbCommandRequest {
    /// Verify that this attached foreground is still live and return the ABI.
    Probe,
    /// Run one bounded ordinary core turn for this foreground and its relay.
    Tick,
    /// Materialize the current local-first result for a canonical query.
    All {
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    },
    /// Open a local-first subscription for a canonical query.
    Subscribe {
        query: Vec<u8>,
        options_json: String,
    },
    /// Drain currently publishable events without waiting. Each delta is
    /// encoded through `jazz::binding_codec`, exactly like NAPI and WASM.
    DrainSubscription {
        subscription: u64,
    },
    /// Cancel one subscription and wait for the core finalization ack.
    Unsubscribe {
        subscription: u64,
    },
    /// Close this foreground alias. Repeated closes report `closed: false`.
    Close,
    /// Poll one foreground-owned operation which previously suspended on
    /// chunk or peer I/O. Polling never drives the owner thread to completion.
    Poll {
        operation: u64,
    },
    /// Drop one suspended operation. Repeated or unknown cancels report
    /// `cancelled: false`.
    Cancel {
        operation: u64,
    },
    /// Open a foreground-owned core transaction. The host chooses the opaque
    /// handle and binds it permanently to this foreground identity.
    BeginTransaction {
        kind: ForegroundTransactionKind,
    },
    /// Stage one full-cell insert under an open foreground transaction. This
    /// reuses the existing native encoded-cell record vocabulary.
    Insert {
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    },
    /// Stage one full-cell patch under an open foreground transaction.
    Update {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
    },
    /// Stage one full-cell upsert under an open foreground transaction.
    Upsert {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
    },
    /// Stage one soft delete under an open foreground transaction.
    Delete {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
    },
    /// Commit one open foreground transaction. The response returns the
    /// public committed `txId`, not the mutable transaction handle.
    CommitTransaction {
        transaction: u64,
    },
    /// Roll back one open foreground transaction. Closing or revoking a
    /// foreground also abandons all its still-open transactions.
    RollbackTransaction {
        transaction: u64,
    },
    /// Wait for a committed foreground transaction to reach authoritative
    /// Core admission. This remains a pending operation so the platform keeps
    /// driving its ordinary native relay ticks while the upstream Core path runs.
    WaitForCoreTransaction {
        tx_id: [u8; 16],
    },
    WaitForTransaction {
        tx_id: [u8; 16],
        tier: String,
        /// Postcard bool after tier: false claims errors, true only observes durability.
        observe_only: bool,
    },
    /// Options use the established native JSON option vocabulary; author identity
    /// remains bound to the admitted foreground capability.
    StageMutation {
        transaction: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    },
    DisconnectNativeUpstream,
    ReconnectNativeUpstream,
    NativeConnectionStatus,
    NativeSessionMetadata,
    WriteState {
        tx_id: [u8; 16],
    },
    DrainMutationErrors,
    BeginStreamingMutation {
        mutation: ForegroundMutationKind,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
        column: String,
        options_json: String,
    },
    PushStreamingMutation {
        upload: u64,
        chunk: Vec<u8>,
    },
    FinishStreamingMutation {
        upload: u64,
    },
    AbortStreamingMutation {
        upload: u64,
    },
    LocalCurrentRow {
        table: String,
        row_id: [u8; 16],
    },
    UpdateLargeValues {
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
        descriptors_json: String,
        updated_at_ms: Option<u64>,
    },
    DirectMutation {
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    },
    PermissionAdvice {
        action: ForegroundPermissionAdviceAction,
    },
    /// General shutdown barrier; identity remains bound to the native capability.
    WaitForPendingWrites {
        tier: String,
    },
    /// Capture the foreground's local frontier through its ordinary owner queue.
    CaptureSnapshot,
}

/// Append-only V1 advice grammar: Insert=0, Read=1, Update=2, Delete=3.
/// Identity comes exclusively from the admitted foreground, never this payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundPermissionAdviceAction {
    Insert {
        table: String,
        cells: Vec<u8>,
    },
    Read {
        table: String,
        row: [u8; 16],
    },
    Update {
        table: String,
        row: [u8; 16],
        patch: Vec<u8>,
    },
    Delete {
        table: String,
        row: [u8; 16],
    },
}

/// Append-only V1 advice result: Allowed=0, Denied=1, Unknown=2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundPermissionAdvice {
    Allowed,
    Denied,
    Unknown,
}

/// Frozen postcard mutation ordinals within the V1 StageMutation envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundMutationKind {
    Insert,
    Update,
    Upsert,
    Delete,
    Restore,
}

/// The two existing Jazz transaction semantics. This native byte vocabulary
/// merely selects them; it never interprets permissions, snapshots, or
/// transaction read/write sets itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundTransactionKind {
    Mergeable,
    // The serialized name keeps its pre-rename spelling for format compatibility.
    #[serde(rename = "Exclusive")]
    Serializable,
}

/// Response for [`ForegroundDbCommandRequest`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundDbCommandResponse {
    Probe {
        abi_version: u16,
    },
    Ticked,
    Rows {
        rows: Vec<u8>,
    },
    Subscribed {
        subscription: u64,
    },
    SubscriptionEvents {
        events: Vec<ForegroundSubscriptionEvent>,
    },
    Unsubscribed {
        closed: bool,
    },
    Closed {
        closed: bool,
    },
    /// The operation has suspended; callers must return to the owner loop so
    /// an ordinary Tick can advance peer/chunk I/O before polling again.
    Pending {
        operation: u64,
    },
    /// A previously pending foreground operation failed without producing a
    /// partial binding payload. This is terminal for that operation only.
    OperationError {
        reason: String,
    },
    /// A pending foreground operation was explicitly cancelled.
    Cancelled {
        cancelled: bool,
    },
    TransactionOpened {
        transaction: u64,
    },
    Inserted {
        row_id: [u8; 16],
    },
    MutationStaged,
    TransactionCommitted {
        tx_id: [u8; 16],
    },
    TransactionRolledBack {
        rolled_back: bool,
    },
    TransactionSettled {
        tx_id: [u8; 16],
    },
    NativeConnectionStatus {
        configured: bool,
        explicitly_offline: bool,
        connected: bool,
        /// The upstream refused the scope's credential, with its reason. Set
        /// until a connection with a refreshed credential succeeds; the host
        /// renews the credential rather than treating the failed tick as
        /// fatal (#4248).
        auth_rejected: Option<String>,
    },
    NativeSessionMetadata {
        node: [u8; 16],
        registry_authority: String,
        account_id: Option<[u8; 16]>,
        issuer: String,
        user_id: String,
    },
    WriteState {
        state_json: String,
    },
    MutationErrors {
        events_json: String,
    },
    StreamingMutationOpened {
        upload: u64,
    },
    StreamingMutationPushed,
    StreamingMutationAborted {
        aborted: bool,
    },
    MutationCommitted {
        tx_id: [u8; 16],
        row_id: [u8; 16],
    },
    PermissionAdvice {
        advice: ForegroundPermissionAdvice,
    },
    /// An `OperationError` caused by a core `jazz::db::Error`, carrying its
    /// stable `ErrorCode::as_str` code beside the unchanged reason text.
    /// Introduced by ABI 2: every other failure still uses `OperationError`.
    /// `reason_name` (ABI 3) is the error's `Reason::as_str`, when it has one.
    CodedOperationError {
        code: String,
        reason_name: Option<String>,
        reason: String,
    },
}

/// One already-materialized subscription event.  The byte payload deliberately
/// reuses the normal binding codec; the JSI bridge only copies bytes and never
/// interprets row/query state.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundSubscriptionEvent {
    Delta {
        reset: bool,
        settled: bool,
        tier: String,
        delta: Vec<u8>,
    },
    Rejected {
        reason: String,
    },
    Closed,
    /// Existing terminal-operation JSON codec, alongside ordinary row deltas.
    StructuredDelta {
        reset: bool,
        settled: bool,
        tier: String,
        delta: Vec<u8>,
        terminal_operations_json: String,
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
    RemotePlaintextEndpoint = 9,
    /// The relay's SQLite root was written in a storage format this build
    /// does not read. Opening it was refused before any record was read or
    /// changed; the application deletes the store and resyncs from Core.
    UnsupportedStorageCodecs = 10,
}

/// Explicit host-owned lifecycle registry for JNI/Swift. No global relay map.
pub struct NativeRelayHost {
    registry: NativeRelayRegistry,
    admitted_scopes: BTreeMap<AdmissionCapability, AdmittedRelayScope>,
    pending_private_sessions: BTreeMap<AdmissionCapability, PendingPrivateSession>,
    account_session_owners: BTreeMap<AdmissionCapability, account_session::AccountSessionOwner>,
    invalidated_runtime_tokens: BTreeSet<u64>,
    private_socket_sessions: BTreeMap<AdmissionCapability, PrivateRelaySocketSession>,
    /// One authenticated upstream worker owns each durable relay scope.  UI
    /// foregrounds are only peer leases on that relay; opening a second root
    /// must never open a competing bearer socket for the same SQLite store.
    private_scope_workers: BTreeMap<RelayScope, PrivateScopeSocketWorker>,
    explicitly_offline_scopes: BTreeSet<RelayScope>,
    relays: BTreeMap<u64, OpenedRelay>,
    clients: BTreeMap<u64, (u64, NativeRelayClient)>,
    /// Foreground aliases opened through the capability-only C ABI. Keeping
    /// this separate from the general command handles makes it impossible for
    /// JSI to turn a guessed number into a relay/client pairing.
    foregrounds: BTreeMap<u64, OpenedForeground>,
    /// Native-relay-owned foreground node leases, partitioned by durable
    /// relay scope. A foreground never chooses or retains this state itself.
    foreground_node_leases: BTreeMap<RelayScope, ForegroundNodeLeasePool>,
    next_handle: u64,
    /// Deterministic transport seam for host-level socket lifecycle tests.
    /// Production always uses [`NativeWebSocketConnector`].
    #[cfg(test)]
    socket_connector: Option<Arc<dyn NativeTransportConnector>>,
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

struct PrivateScopeSocketWorker {
    admitted_scope: AdmissionCapability,
    _worker: NativeRelaySocketWorker,
    connected: Arc<AtomicBool>,
    /// The upstream rejected this session before it connected, or the
    /// worker gave up after a failure. It is observable to foreground ticks
    /// until a new authenticated connection succeeds, so an upstream failure
    /// never silently becomes an indefinite query timeout.
    ///
    /// Losing an already-authenticated link is not latched here while the
    /// worker retries it: that is connection state (a failed remote link),
    /// and must not poison a valid foreground (#3630). A reconnect that the
    /// upstream then rejects latches as usual.
    terminal_error: Arc<Mutex<Option<String>>>,
    /// The upstream refused this worker's credential. The worker stops on
    /// it, so only a replacement built with a refreshed credential (or an
    /// explicit reconnect) can connect again.
    auth_rejected: Arc<Mutex<Option<String>>>,
    /// The worker's socket loop has exited and will never reconnect on its
    /// own. An explicit reconnect replaces such a worker.
    stopped: Arc<AtomicBool>,
}

impl PrivateScopeSocketWorker {
    /// The worker gave up after a terminal failure: it is no longer an
    /// upstream and nothing but a replacement can clear its failure.
    fn terminally_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
            && self
                .terminal_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some()
    }
}

struct OpenedForeground {
    scope: RelayScope,
    relay: u64,
    client: u64,
    /// Last native-socket reachability reported to this foreground's `Db`.
    remote_link_hint: Option<RemoteLinkHint>,
    /// This foreground was reported `Live` since it opened or last went
    /// explicitly offline; a disconnected worker is then backing off.
    link_was_live: bool,
    runtime_token: u64,
    wake: Option<Arc<ForegroundWakeState>>,
    lease: ForegroundNodeLease,
}

#[derive(Clone, Copy)]
struct ForegroundWakeRegistration {
    callback: ForegroundWakeCallback,
    context: usize,
}

/// The only two legitimate endings for a foreground lease. A clean explicit
/// close may return a node after reading its owner-local commit seq; every forced
/// teardown is uncertain and therefore permanently retires it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForegroundTeardown {
    CleanHandoff,
    Retire,
}
impl ForegroundWakeRegistration {
    fn cancelled(self, foreground: u64) {
        unsafe {
            (self.callback)(
                self.context as *mut c_void,
                foreground,
                FOREGROUND_WAKE_CANCELLED,
                0,
            )
        }
    }
}

/// Thread-safe ownership indirection between an owner-local `Db` scheduler and
/// a raw platform callback context.
///
/// Owner queue saturation can prevent the host from immediately replacing the
/// scheduler inside the thread-affine `Db`. Inerting this state is independent
/// of that queue: it synchronizes with an in-flight callback and guarantees
/// that a scheduler retained by the owner can never dereference the context
/// again. Only then may the host emit `CANCELLED`, allowing the platform to
/// release its registration safely.
struct ForegroundWakeState {
    registration: Mutex<Option<ForegroundWakeRegistration>>,
}

impl ForegroundWakeState {
    fn new(registration: ForegroundWakeRegistration) -> Self {
        Self {
            registration: Mutex::new(Some(registration)),
        }
    }

    fn wake(&self, foreground: u64, kind: u8, delay_ms: u64) {
        let registration = self
            .registration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(registration) = *registration else {
            return;
        };
        unsafe {
            (registration.callback)(
                registration.context as *mut c_void,
                foreground,
                kind,
                delay_ms,
            )
        }
    }

    /// Prevent every future raw-context call and wait for an in-flight one to
    /// finish before handing the registration back for its final cancellation.
    fn inert(&self) -> Option<ForegroundWakeRegistration> {
        self.registration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

impl Default for NativeRelayHost {
    fn default() -> Self {
        Self {
            registry: NativeRelayRegistry::default(),
            admitted_scopes: BTreeMap::new(),
            pending_private_sessions: BTreeMap::new(),
            account_session_owners: BTreeMap::new(),
            invalidated_runtime_tokens: BTreeSet::new(),
            private_socket_sessions: BTreeMap::new(),
            private_scope_workers: BTreeMap::new(),
            explicitly_offline_scopes: BTreeSet::new(),
            relays: BTreeMap::new(),
            clients: BTreeMap::new(),
            foregrounds: BTreeMap::new(),
            foreground_node_leases: BTreeMap::new(),
            next_handle: 1,
            #[cfg(test)]
            socket_connector: None,
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

    fn acquire_foreground_lease(
        &mut self,
        scope: &RelayScope,
    ) -> Result<ForegroundNodeLease, JazzNativeRelayStatus> {
        let pool = self
            .foreground_node_leases
            .entry(scope.clone())
            .or_default();
        if let Some(lease) = pool.acquire_reusable() {
            return Ok(lease);
        }
        loop {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes)
                .map_err(|error| relay_status(RelayError::Entropy(error.to_string())))?;
            match pool.acquire_fresh(NodeUuid::from_bytes(bytes)) {
                Ok(lease) => return Ok(lease),
                // CSPRNG collision against a live, reusable, or retired node
                // is vanishingly unlikely, but never turn one into a lease
                // alias or a spurious lifecycle failure.
                Err(node_lease::ForegroundNodeLeaseError::DuplicateNode) => {}
                Err(node_lease::ForegroundNodeLeaseError::InactiveLease) => {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
        }
    }

    fn clean_foreground_lease(
        &mut self,
        scope: &RelayScope,
        lease: ForegroundNodeLease,
        high_water: u64,
    ) -> Result<(), JazzNativeRelayStatus> {
        // This host is the relay's single serialized lifecycle owner. Updating
        // the pool immediately after native-core readout is its atomic durable
        // handoff boundary; a future durable pool backend must perform the
        // exact same transition transactionally before exposing the reuse.
        self.foreground_node_leases
            .get_mut(scope)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?
            .clean_handoff(ForegroundNodeLease {
                high_water,
                ..lease
            })
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)
    }

    fn retire_foreground_lease(&mut self, scope: &RelayScope, lease: ForegroundNodeLease) {
        let Some(pool) = self.foreground_node_leases.get_mut(scope) else {
            return;
        };
        let _ = pool.retire(lease);
    }

    /// Ensure the single native-owned upstream worker for this admitted
    /// persistent scope exists.  Foregrounds attach to the same relay through
    /// ordinary peers; the worker is deliberately owned by the scope rather
    /// than by any foreground or opaque relay alias.
    fn ensure_private_scope_worker(
        &mut self,
        admitted_scope: AdmissionCapability,
        scope: &RelayScope,
        relay: NativeRelay,
        peer_identity: jazz::ids::AuthorSubject,
    ) -> Result<(), JazzNativeRelayStatus> {
        if self.explicitly_offline_scopes.contains(scope) {
            return Ok(());
        }
        let Some(session) = self.private_socket_sessions.get(&admitted_scope).cloned() else {
            return Ok(());
        };
        if let Some(existing) = self.private_scope_workers.get(scope) {
            // Account attachment/refresh promotes one shared snapshot before
            // opening a foreground. Legacy admissions still require exact
            // capability ownership; mismatched snapshots cannot borrow it.
            return (existing.admitted_scope == admitted_scope
                || (self.account_session_owners.contains_key(&admitted_scope)
                    && self
                        .account_session_owners
                        .contains_key(&existing.admitted_scope)
                    && self.private_socket_sessions.get(&existing.admitted_scope)
                        == Some(&session)))
            .then_some(())
            .ok_or(JazzNativeRelayStatus::LifecycleFailure);
        }
        let worker = Self::prepare_private_scope_worker(
            admitted_scope,
            relay,
            peer_identity,
            session,
            self.socket_connector(),
        )?;
        worker._worker.activate();
        self.private_scope_workers.insert(scope.clone(), worker);
        Ok(())
    }

    fn socket_connector(&self) -> Arc<dyn NativeTransportConnector> {
        #[cfg(test)]
        if let Some(connector) = &self.socket_connector {
            return Arc::clone(connector);
        }
        Arc::new(NativeWebSocketConnector)
    }

    fn prepare_private_scope_worker(
        admitted_scope: AdmissionCapability,
        relay: NativeRelay,
        peer_identity: jazz::ids::AuthorSubject,
        session: PrivateRelaySocketSession,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<PrivateScopeSocketWorker, JazzNativeRelayStatus> {
        let terminal_error = Arc::new(Mutex::new(None));
        let terminal_for_event = Arc::clone(&terminal_error);
        let connected = Arc::new(AtomicBool::new(false));
        let connected_for_event = Arc::clone(&connected);
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_for_event = Arc::clone(&stopped);
        let auth_rejected = Arc::new(Mutex::new(None::<String>));
        let auth_rejected_for_event = Arc::clone(&auth_rejected);
        // A failure of an established link, held until the worker either
        // retries it (connection state) or stops on it (terminal).
        let link_failure = Mutex::new(None::<String>);
        let worker = NativeRelaySocketWorker::prepare_with_connector(
            relay,
            NativeRelaySocketConfig {
                server_url: session.server_url,
                app_id: AppId::from_string(&session.app_id)
                    .unwrap_or_else(|_| AppId::from_name(&session.app_id)),
                peer_identity,
                auth: AuthConfig {
                    jwt_token: Some(session.bearer),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::from_secs(1),
                on_event: Arc::new(move |event| {
                    let set_terminal = |error| {
                        *terminal_for_event
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = error;
                    };
                    let mut link_failure = link_failure
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match event {
                        NativeRelaySocketEvent::Connected => {
                            connected_for_event.store(true, Ordering::Release);
                            *link_failure = None;
                            *auth_rejected_for_event
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                            set_terminal(None);
                        }
                        // A refused credential is a rejected session: it
                        // latches like one, and the worker stops on it.
                        NativeRelaySocketEvent::AuthRejected(error) => {
                            connected_for_event.store(false, Ordering::Release);
                            *auth_rejected_for_event
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(error.clone());
                            set_terminal(Some(error));
                        }
                        NativeRelaySocketEvent::TerminalError(error) => {
                            if connected_for_event.swap(false, Ordering::AcqRel) {
                                *link_failure = Some(error);
                            } else {
                                set_terminal(Some(error));
                            }
                        }
                        NativeRelaySocketEvent::Reconnecting => {
                            connected_for_event.store(false, Ordering::Release);
                            *link_failure = None;
                        }
                        NativeRelaySocketEvent::Stopped => {
                            connected_for_event.store(false, Ordering::Release);
                            if let Some(error) = link_failure.take() {
                                set_terminal(Some(error));
                            }
                            stopped_for_event.store(true, Ordering::Release);
                        }
                    }
                }),
            },
            connector,
        )
        .map_err(relay_status)?;
        Ok(PrivateScopeSocketWorker {
            admitted_scope,
            _worker: worker,
            connected,
            terminal_error,
            auth_rejected,
            stopped,
        })
    }

    fn foreground_connectivity(
        &mut self,
        foreground: u64,
        disconnect: Option<bool>,
    ) -> Result<ForegroundDbCommandResponse, JazzNativeRelayStatus> {
        let opened = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let scope = opened.scope.clone();
        let relay = self
            .relays
            .get(&opened.relay)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let capability = relay.admitted_scope;
        let native_relay = relay.relay.clone();
        let configured = self.private_socket_sessions.contains_key(&capability);
        if disconnect.is_some() && !configured {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        match disconnect {
            Some(true) => {
                // Drop cancels and joins the native bearer socket. Only publish
                // explicit offline after the real transport has stopped.
                self.private_scope_workers.remove(&scope);
                self.explicitly_offline_scopes.insert(scope.clone());
            }
            Some(false) => {
                let author = self
                    .admitted_scopes
                    .get(&capability)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .config
                    .identity
                    .author;
                let was_offline = self.explicitly_offline_scopes.remove(&scope);
                // A worker that gave up after a terminal failure is no longer
                // an upstream. Explicit reconnect replaces it (dropping it
                // only joins an already-exited thread) so its latched failure
                // cannot outlive the caller's request to try again. A worker
                // that is still connected or retrying is left untouched.
                if self
                    .private_scope_workers
                    .get(&scope)
                    .is_some_and(PrivateScopeSocketWorker::terminally_stopped)
                {
                    self.private_scope_workers.remove(&scope);
                }
                if let Err(error) =
                    self.ensure_private_scope_worker(capability, &scope, native_relay, author)
                {
                    if was_offline {
                        self.explicitly_offline_scopes.insert(scope.clone());
                    }
                    return Err(error);
                }
            }
            None => {}
        }
        Ok(ForegroundDbCommandResponse::NativeConnectionStatus {
            configured,
            explicitly_offline: self.explicitly_offline_scopes.contains(&scope),
            connected: self
                .private_scope_workers
                .get(&scope)
                .is_some_and(|worker| worker.connected.load(Ordering::Acquire)),
            auth_rejected: self.private_scope_workers.get(&scope).and_then(|worker| {
                worker
                    .auth_rejected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            }),
        })
    }

    fn private_scope_terminal_error(&self, scope: &RelayScope) -> Option<String> {
        self.private_scope_workers.get(scope).and_then(|worker| {
            worker
                .terminal_error
                .lock()
                .ok()
                .and_then(|error| error.clone())
        })
    }

    fn execute(
        &mut self,
        command: RelayCommandRequest,
    ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
        match command {
            RelayCommandRequest::Probe => Ok(RelayCommandResponse::Probe {
                abi_version: NATIVE_RELAY_ABI_V1,
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
                let peer_identity = config.identity.author;
                let relay = self.registry.open_admitted(config).map_err(relay_status)?;
                self.ensure_private_scope_worker(
                    admitted_scope,
                    &scope,
                    relay.clone(),
                    peer_identity,
                )?;
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
                    .attach_client(fresh_client_identity(author).map_err(relay_status)?, claims)
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
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
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
                    .push_frame(frame)
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
                        .take_frames()
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
                })
            }
            RelayCommandRequest::SendRelayFrame { relay, frame } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .wire()
                    .and_then(|wire| wire.push_inbound_encoded(&frame))
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
                    .and_then(|wire| wire.take_outbound_encoded())
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
            }),
            RelayCommandRequest::Diagnostics { relay } => {
                let relay_handle = relay;
                let relay = self
                    .relays
                    .get(&relay_handle)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                let (_, inbound_frames, outbound_frames) = relay
                    .relay
                    .diagnostics()
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

    /// Open one in-memory foreground client for an already admitted scope.
    ///
    /// This deliberately bypasses the postcard lifecycle envelope: the
    /// capability is not a JavaScript command payload, and the C caller gets
    /// only an opaque foreground handle. Internally it still uses the same
    /// `NativeRelay` owner thread and ordinary peer link as `Open` + `Attach`.
    fn open_foreground(
        &mut self,
        admitted_scope: AdmissionCapability,
        runtime_token: u64,
    ) -> Result<u64, JazzNativeRelayStatus> {
        if runtime_token == 0 {
            return Err(JazzNativeRelayStatus::InvalidArgument);
        }
        if self.invalidated_runtime_tokens.contains(&runtime_token) {
            return Err(JazzNativeRelayStatus::InvalidHandle);
        }
        if self
            .account_session_owners
            .get(&admitted_scope)
            .is_some_and(|owner| owner.runtime_token != runtime_token)
        {
            return Err(JazzNativeRelayStatus::InvalidHandle);
        }
        let (config, author, claims) = {
            let admitted = self
                .admitted_scopes
                .get(&admitted_scope)
                .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
            (
                admitted.config.clone(),
                admitted.config.identity.author,
                admitted.claims.clone(),
            )
        };
        let scope = config.scope.clone();
        let relay = self.registry.open_admitted(config).map_err(relay_status)?;
        self.ensure_private_scope_worker(admitted_scope, &scope, relay.clone(), author)?;
        let relay_handle = self
            .allocate()
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        self.relays.insert(
            relay_handle,
            OpenedRelay {
                scope: scope.clone(),
                admitted_scope,
                relay: relay.clone(),
            },
        );
        let client_handle = self
            .allocate()
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let lease = self.acquire_foreground_lease(&scope)?;
        let client = match relay.attach_foreground_client(
            DbIdentity {
                node: lease.node,
                author,
            },
            claims,
            lease,
        ) {
            Ok(client) => client,
            Err(error) => {
                // The relay alias was not yet observable to the caller. Undo
                // it before returning the attachment failure.
                self.relays.remove(&relay_handle);
                let final_alias = !self.relays.values().any(|opened| opened.scope == scope);
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
                    let _ = self.registry.close(&scope);
                }
                // No foreground runtime observed this lease, so it is a known
                // zero-high-water failure rather than an uncertain handoff.
                if self.clean_foreground_lease(&scope, lease, 0).is_err() {
                    self.retire_foreground_lease(&scope, lease);
                }
                return Err(relay_status(error));
            }
        };
        self.clients.insert(client_handle, (relay_handle, client));
        let foreground = match self.allocate() {
            Ok(foreground) => foreground,
            Err(_) => {
                if let Some((_, client)) = self.clients.remove(&client_handle) {
                    let _ = client.close();
                }
                self.relays.remove(&relay_handle);
                let final_alias = !self.relays.values().any(|opened| opened.scope == scope);
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
                    let _ = self.registry.close(&scope);
                }
                // A client existed briefly. Its close failure means the
                // runtime may have minted after attachment, so conservatively
                // retire rather than returning the zero-water lease.
                self.retire_foreground_lease(&scope, lease);
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
        };
        self.foregrounds.insert(
            foreground,
            OpenedForeground {
                scope,
                relay: relay_handle,
                client: client_handle,
                remote_link_hint: None,
                link_was_live: false,
                runtime_token,
                wake: None,
                lease,
            },
        );
        Ok(foreground)
    }

    /// Report the relay-owned native socket's reachability to the foreground
    /// `Db`, which drives its local-first reads that wait for the server. The foreground's
    /// own upstream is the local relay core, which is always attached, so it
    /// cannot tell whether the authoritative server could answer. Only a
    /// change is reported, so an `Attempting` report timestamps the start of
    /// the attempt the relay first observed. A foreground without a native
    /// socket session has no server to wait for.
    fn sync_foreground_remote_link(&mut self, foreground: u64) {
        let Some(opened) = self.foregrounds.get(&foreground) else {
            return;
        };
        let Some(relay) = self.relays.get(&opened.relay) else {
            return;
        };
        if !self
            .private_socket_sessions
            .contains_key(&relay.admitted_scope)
        {
            self.report_foreground_remote_link(foreground, RemoteLinkHint::NoServer);
            return;
        }
        let scope = opened.scope.clone();
        let previous = opened.remote_link_hint;
        let explicitly_offline = self.explicitly_offline_scopes.contains(&scope);
        if explicitly_offline
            && opened.link_was_live
            && let Some(opened) = self.foregrounds.get_mut(&foreground)
        {
            // Going back online after an explicit offline period is a fresh
            // attempt.
            opened.link_was_live = false;
        }
        let Some(opened) = self.foregrounds.get(&foreground) else {
            return;
        };
        // A worker that is not connected after this foreground saw it live
        // is backing off between retries: nothing waits on that, as on the
        // TS and native-facade hosts. Only a first connection, or one after
        // an explicit offline period, is an attempt.
        let hint = if explicitly_offline {
            RemoteLinkHint::Failed
        } else {
            match self.private_scope_workers.get(&scope) {
                Some(worker) if worker.connected.load(Ordering::Acquire) => RemoteLinkHint::Live,
                Some(_) if self.private_scope_terminal_error(&scope).is_some() => {
                    RemoteLinkHint::Failed
                }
                Some(_) if opened.link_was_live => RemoteLinkHint::Failed,
                Some(_) => RemoteLinkHint::Attempting,
                None => RemoteLinkHint::Failed,
            }
        };
        if previous == Some(hint) {
            return;
        }
        self.report_foreground_remote_link(foreground, hint);
    }

    fn report_foreground_remote_link(&mut self, foreground: u64, hint: RemoteLinkHint) {
        if self
            .foregrounds
            .get(&foreground)
            .is_none_or(|opened| opened.remote_link_hint == Some(hint))
        {
            return;
        }
        // The hint describes the relay's own path to the server, so it goes
        // to the relay's persistent database, as a browser worker sets it on
        // its owner runtime. Foreground reads wait on the relay, not on it.
        let reported = self
            .foregrounds
            .get(&foreground)
            .and_then(|opened| self.relays.get(&opened.relay))
            .is_some_and(|opened| opened.relay.set_remote_link_hint(hint).is_ok());
        if reported && let Some(opened) = self.foregrounds.get_mut(&foreground) {
            opened.remote_link_hint = Some(hint);
            opened.link_was_live |= hint == RemoteLinkHint::Live;
        }
    }

    fn tick_foreground(&mut self, foreground: u64) -> Result<(), JazzNativeRelayStatus> {
        let opened = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let (relay, scope) = (opened.relay, opened.scope.clone());
        // A rejected session or a stopped upstream fails the tick. A lost
        // established link that the worker is retrying is not latched: it is
        // reported through the remote-link hint and `NativeConnectionStatus`
        // while local work continues.
        if self.private_scope_terminal_error(&scope).is_some() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        self.relays
            .get(&relay)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        self.foreground_client(foreground)?
            .pump_foreground()
            .map_err(relay_status)
    }

    /// Lease-scoped C ABI calls must not turn an opaque foreground handle into
    /// a cross-runtime capability. Handles are host-global, while a retained
    /// platform lease is scoped to exactly one installed JSI runtime.
    ///
    /// A missing handle remains valid for idempotent close; operations that
    /// require a live foreground perform their ordinary liveness check after
    /// this guard. A live handle owned by another runtime always fails.
    fn require_lease_foreground_runtime(
        &self,
        runtime_token: u64,
        foreground: u64,
    ) -> Result<(), JazzNativeRelayStatus> {
        match self.foregrounds.get(&foreground) {
            Some(opened) if opened.runtime_token == runtime_token => Ok(()),
            Some(_) => Err(JazzNativeRelayStatus::InvalidHandle),
            None => Ok(()),
        }
    }

    fn foreground_client(
        &self,
        foreground: u64,
    ) -> Result<&NativeRelayClient, JazzNativeRelayStatus> {
        let client = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?
            .client;
        self.clients
            .get(&client)
            .map(|(_, client)| client)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)
    }

    /// Retire or cleanly hand off exactly one foreground alias.
    ///
    /// Removing the public handle is deliberately the first step. It prevents
    /// a concurrent/later native call from beginning more work while this
    /// function drains the owner-thread state. The Rust scheduler is then
    /// cleared synchronously before the platform callback receives its
    /// cancellation notification; a queued platform turn may still run, but
    /// it has no native scheduler or foreground handle to re-enter.
    fn teardown_foreground(
        &mut self,
        foreground_handle: u64,
        teardown: ForegroundTeardown,
    ) -> Result<bool, JazzNativeRelayStatus> {
        let Some(foreground) = self.foregrounds.remove(&foreground_handle) else {
            return Ok(false);
        };

        // Do not use `set_foreground_wake_callback` here: the foreground was
        // intentionally removed above to make all new public work fail
        // synchronously. The retained client still reaches the owner thread
        // directly and clears its `Db` scheduler before its raw platform
        // callback context can be released.
        let client = self
            .clients
            .get(&foreground.client)
            .map(|(_, client)| client.clone());
        // First inert the shared state without touching the saturated owner
        // queue. This is the actual raw-context ownership boundary: even if
        // the owner retains its scheduler, it can no longer call the platform.
        let wake = foreground.wake.and_then(|wake| wake.inert());
        let wake_cleared = client
            .as_ref()
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
            .and_then(|client| {
                client
                    .set_foreground_wake_callback(foreground_handle, None)
                    .map_err(relay_status)
            });
        if let Some(wake) = wake {
            wake.cancelled(foreground_handle);
        }

        // Read out the runtime-owned commit seq only for an explicit clean close.
        // Revocation and platform invalidation have no supported clean
        // handoff, even if this synchronous teardown happens to succeed.
        let high_water = match teardown {
            ForegroundTeardown::CleanHandoff => client
                .as_ref()
                .ok_or(JazzNativeRelayStatus::LifecycleFailure)
                .and_then(|client| client.minted_high_water().map_err(relay_status)),
            ForegroundTeardown::Retire => Ok(0),
        };

        // `NativeRelayClient::close` removes the owner-local client. Dropping
        // it aborts pending foreground futures/subscriptions and its explicit
        // transaction drain abandons every mutable transaction handle.
        let client_closed = self
            .clients
            .remove(&foreground.client)
            .map(|(_, client)| client.close())
            .transpose()
            .map_err(relay_status);
        let opened = self.relays.remove(&foreground.relay);
        let final_alias = !self
            .relays
            .values()
            .any(|remaining| remaining.scope == foreground.scope);
        let relay_closed = match opened {
            Some(opened)
                if final_alias && !self.private_scope_workers.contains_key(&foreground.scope) =>
            {
                self.registry
                    .close(&opened.scope)
                    .map(|_| ())
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)
            }
            Some(_) => Ok(()),
            None => Err(JazzNativeRelayStatus::LifecycleFailure),
        };
        let owner_closed = matches!(client_closed, Ok(Some(())));
        let relay_closed = relay_closed.is_ok();
        let wake_cleared = wake_cleared.is_ok();
        match teardown {
            ForegroundTeardown::CleanHandoff
                if wake_cleared && owner_closed && relay_closed && high_water.is_ok() =>
            {
                if self
                    .clean_foreground_lease(
                        &foreground.scope,
                        foreground.lease,
                        high_water.expect("clean handoff already checked high water"),
                    )
                    .is_err()
                {
                    self.retire_foreground_lease(&foreground.scope, foreground.lease);
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
            ForegroundTeardown::Retire => {
                self.retire_foreground_lease(&foreground.scope, foreground.lease);
                if !wake_cleared || !owner_closed || !relay_closed {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
            ForegroundTeardown::CleanHandoff => {
                self.retire_foreground_lease(&foreground.scope, foreground.lease);
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
        }
        Ok(true)
    }

    fn close_foreground(&mut self, foreground_handle: u64) -> Result<bool, JazzNativeRelayStatus> {
        self.teardown_foreground(foreground_handle, ForegroundTeardown::CleanHandoff)
    }

    /// Platform runtime invalidation is an uncertain shutdown. It must retire
    /// only aliases owned by that token, preserve sibling runtimes, and never
    /// return a node identity to the reusable pool without a confirmed commit seq
    /// handoff.
    fn retire_foregrounds_for_runtime(
        &mut self,
        runtime_token: u64,
    ) -> Result<(), JazzNativeRelayStatus> {
        self.invalidated_runtime_tokens.insert(runtime_token);
        let foregrounds = self
            .foregrounds
            .iter()
            .filter_map(|(handle, opened)| {
                (opened.runtime_token == runtime_token).then_some(*handle)
            })
            .collect::<Vec<_>>();
        let mut failed = false;
        for handle in foregrounds {
            if self
                .teardown_foreground(handle, ForegroundTeardown::Retire)
                .is_err()
            {
                failed = true;
            }
        }
        let sessions = self
            .account_session_owners
            .iter()
            .filter_map(|(capability, owner)| {
                (owner.runtime_token == runtime_token).then_some(*capability)
            })
            .collect::<Vec<_>>();
        for session in sessions {
            if self.revoke_scope(session).is_err() {
                failed = true;
            }
        }
        (!failed)
            .then_some(())
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
    }

    fn set_foreground_wake_callback(
        &mut self,
        foreground: u64,
        callback: Option<ForegroundWakeCallback>,
        context: usize,
    ) -> Result<(), JazzNativeRelayStatus> {
        let client = self.foreground_client(foreground)?.clone();
        let wake = callback.map(|callback| {
            Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
                callback,
                context,
            }))
        });
        client
            .set_foreground_wake_callback(foreground, wake.clone())
            .map_err(relay_status)?;
        let previous = std::mem::replace(
            &mut self
                .foregrounds
                .get_mut(&foreground)
                .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                .wake,
            wake,
        );
        // The owner command completed before its response, so the prior
        // scheduler is already dropped. Inerting its host-held state now is
        // only defensive against future changes which retain scheduler clones.
        if let Some(previous) = previous {
            let _ = previous.inert();
        }
        Ok(())
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
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: request.scope.into(),
            sqlite_path: PathBuf::from(request.sqlite_path),
            schema,
            identity: request.identity,
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

    fn begin_private_session(
        &mut self,
        request: PrivateSessionSetupJson,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        let endpoint = validate_private_session_endpoint(&request.server_url)?;
        let origin = Some(endpoint.origin().ascii_serialization())
            .filter(|origin| origin != "null")
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?;
        let app_id = request.app_id.trim();
        if app_id.is_empty() || request.storage_root.trim().is_empty() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(&request.jwt)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?;
        let author = jazz::ids::AuthorSubject::authenticated(&issuer, &subject)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let canonical_subject = author.canonical().to_owned();
        let scope = RelayScopeRequest {
            app_namespace: origin,
            storage_namespace: app_id.to_owned(),
            auth_scope: Some(canonical_subject.clone()),
        };
        RelayScope::from(scope.clone())
            .validate()
            .map_err(relay_status)?;
        let digest = blake3::hash(
            format!(
                "{}\0{}\0{}",
                scope.app_namespace, scope.storage_namespace, canonical_subject
            )
            .as_bytes(),
        );
        let sqlite_path = PathBuf::from(request.storage_root)
            .join(format!("{}.sqlite", digest.to_hex()))
            .display()
            .to_string();
        let mut node = [0_u8; 16];
        getrandom::fill(&mut node).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let capability = self.allocate_admission_capability().map_err(relay_status)?;
        self.pending_private_sessions.insert(
            capability,
            PendingPrivateSession {
                claims: BTreeMap::new(),
                scope,
                sqlite_path,
                identity: DbIdentity {
                    node: NodeUuid::from_bytes(node),
                    author,
                },
                socket: Some(PrivateRelaySocketSession {
                    server_url: request.server_url,
                    app_id: request.app_id,
                    bearer: request.jwt,
                }),
            },
        );
        Ok(capability)
    }

    fn attach_canonical_schema(
        &mut self,
        session: AdmissionCapability,
        schema_json: &str,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        let mut pending = self
            .pending_private_sessions
            .remove(&session)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let runtime_owner = self.account_session_owners.remove(&session);
        let account_snapshot = runtime_owner.as_ref().map(|_| {
            (
                pending.claims.clone(),
                pending
                    .socket
                    .as_ref()
                    .map(|socket| socket.bearer.clone())
                    .unwrap_or_default(),
            )
        });
        if runtime_owner.is_some() {
            // Reuse the durable relay identity, but do not publish a new
            // credential/claims snapshot until schema/path validation succeeds.
            let scope = RelayScope::from(pending.scope.clone());
            if let Some((capability, existing)) = self
                .admitted_scopes
                .iter()
                .find(|(_, entry)| entry.config.scope == scope)
            {
                if !self.account_session_owners.contains_key(capability) {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
                let old_transport = self
                    .private_socket_sessions
                    .get(capability)
                    .map(|socket| (&socket.server_url, &socket.app_id));
                let new_transport = pending
                    .socket
                    .as_ref()
                    .map(|socket| (&socket.server_url, &socket.app_id));
                if old_transport != new_transport {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
                pending.identity = existing.config.identity;
                pending.claims = existing.claims.clone();
            }
        }
        // Canonicalize at this credential-free boundary before constructing a
        // JazzSchema. A malformed schema consumes the one-shot session setup;
        // callers must restart setup rather than attach a different schema.
        let value = serde_json::from_str::<serde_json::Value>(schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema_json =
            serde_json::to_string(&value).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let socket = pending.socket;
        let admitted = self.admit_scope(RelayScopeAdmissionRequest {
            scope: pending.scope,
            sqlite_path: pending.sqlite_path,
            schema_json,
            identity: pending.identity,
            claims: pending.claims,
        })?;
        if let Some(socket) = socket {
            self.private_socket_sessions.insert(admitted, socket);
        }
        if let Some(owner) = runtime_owner {
            self.account_session_owners.insert(admitted, owner);
        }
        if let Some((claims, token)) = account_snapshot
            && let Err(status) = self.promote_account_scope_snapshot(admitted, &token, claims)
        {
            let _ = self.revoke_scope(admitted);
            return Err(status);
        }
        Ok(admitted)
    }

    fn revoke_scope(
        &mut self,
        admitted_scope: AdmissionCapability,
    ) -> Result<bool, JazzNativeRelayStatus> {
        self.account_session_owners.remove(&admitted_scope);
        // Cancellation also consumes a setup that has not attached its schema
        // yet. Otherwise a late attachment could resurrect a canceled session.
        if self
            .pending_private_sessions
            .remove(&admitted_scope)
            .is_some()
        {
            return Ok(true);
        }
        let Some(admitted) = self.admitted_scopes.remove(&admitted_scope) else {
            return Ok(false);
        };
        // A sibling account context retains the shared upstream owner. Its
        // foreground and bearer outlive this context's normal shutdown.
        let successor = self.admitted_scopes.iter().find_map(|(capability, entry)| {
            (entry.config.scope == admitted.config.scope
                && self.account_session_owners.contains_key(capability)
                && self.private_socket_sessions.get(capability)
                    == self.private_socket_sessions.get(&admitted_scope))
            .then_some(*capability)
        });
        if let Some(successor) = successor
            && let Some(worker) = self.private_scope_workers.get_mut(&admitted.config.scope)
            && worker.admitted_scope == admitted_scope
        {
            worker.admitted_scope = successor;
        }
        // Removing the native-only session ensures a later re-admission must
        // provide a fresh bearer. Any opened worker is dropped below with its
        // relay alias, which synchronously cancels its socket thread.
        self.private_socket_sessions.remove(&admitted_scope);
        if !self
            .admitted_scopes
            .values()
            .any(|entry| entry.config.scope == admitted.config.scope)
        {
            self.explicitly_offline_scopes
                .remove(&admitted.config.scope);
        }
        // This is the scope worker's trusted lifetime boundary. Dropping it
        // synchronously cancels and joins its bearer socket before the
        // durable relay can be closed below.
        if self
            .private_scope_workers
            .get(&admitted.config.scope)
            .is_some_and(|worker| worker.admitted_scope == admitted_scope)
        {
            self.private_scope_workers.remove(&admitted.config.scope);
        }
        let relay_handles = self
            .relays
            .iter()
            .filter_map(|(handle, opened)| {
                (opened.admitted_scope == admitted_scope).then_some(*handle)
            })
            .collect::<Vec<_>>();
        // Revocation is an unclean lifecycle boundary. Each foreground must
        // synchronously reject new work, clear its owner wake, abort pending
        // work, and retire its lease after admission has stopped new opens.
        // The old `retain` merely hid aliases and left raw wake contexts plus
        // active node identities behind.
        let foregrounds = self
            .foregrounds
            .iter()
            .filter_map(|(handle, foreground)| {
                relay_handles.contains(&foreground.relay).then_some(*handle)
            })
            .collect::<Vec<_>>();
        let mut failed = false;
        for foreground in foregrounds {
            if self
                .teardown_foreground(foreground, ForegroundTeardown::Retire)
                .is_err()
            {
                failed = true;
            }
        }
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
            if !self.relays.values().any(|opened| opened.scope == scope)
                && !self
                    .admitted_scopes
                    .values()
                    .any(|entry| entry.config.scope == scope)
            {
                let _ = self.registry.close(&scope);
            }
        }
        // A scope-owned worker intentionally keeps the durable relay alive
        // after its last foreground lease closes. Revocation must therefore
        // also close a worker-only registry entry; otherwise a fresh private
        // session mints a new relay node but collides with the old SQLite
        // owner on restart.
        if !self
            .admitted_scopes
            .values()
            .any(|remaining| remaining.config.scope == admitted.config.scope)
        {
            self.registry
                .close(&admitted.config.scope)
                .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        }
        (!failed)
            .then_some(true)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
    }
}

/// Mint an ephemeral client identity for one foreground runtime.
///
/// A foreground `Db` owns a fresh in-memory commit sequence. Its node must therefore be
/// fresh too: deriving it from the host-local handle counter would repeat the
/// first node after every process restart, allowing same-clock restarts to
/// reuse a transaction identity. The persistent relay identity remains the
/// trusted platform configuration; this random identity is only for its
/// short-lived in-memory peers.
fn fresh_client_identity(author: jazz::ids::AuthorSubject) -> Result<DbIdentity, RelayError> {
    let mut node = [0_u8; 16];
    getrandom::fill(&mut node).map_err(|error| RelayError::Entropy(error.to_string()))?;
    Ok(DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes(node),
        author,
    })
}

fn relay_status(error: RelayError) -> JazzNativeRelayStatus {
    match error {
        RelayError::InvalidAbiRange { .. } => JazzNativeRelayStatus::InvalidAbiRange,
        RelayError::IncompatibleAbi { .. } => JazzNativeRelayStatus::IncompatibleAbi,
        RelayError::QueueCapacityExceeded { .. } | RelayError::OwnerQueueFull => {
            JazzNativeRelayStatus::Backpressure
        }
        RelayError::Host(binding_host::HostError::Engine(error))
            if error.code == jazz::db::ErrorCode::Backpressure =>
        {
            JazzNativeRelayStatus::Backpressure
        }
        RelayError::UnsupportedStorage => JazzNativeRelayStatus::UnsupportedStorageCodecs,
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
    NATIVE_RELAY_ABI_V1
}

/// Optional foreground capture support in the linked native artifact.
/// A feature query outside the command layout, so it does not change the ABI.
#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_supports_snapshot_capture() -> bool {
    true
}

/// Run one C ABI body written with `?` and collapse it into its status code.
fn ffi_call(body: impl FnOnce() -> Result<(), JazzNativeRelayStatus>) -> JazzNativeRelayStatus {
    match body() {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Fail with `InvalidArgument` unless `valid` holds.
fn require_arg(valid: bool) -> Result<(), JazzNativeRelayStatus> {
    if valid {
        Ok(())
    } else {
        Err(JazzNativeRelayStatus::InvalidArgument)
    }
}

/// Borrow a caller-owned ABI object, rejecting null.
///
/// # Safety
/// A non-null `ptr` must point to a live value for the duration of the call.
unsafe fn live_arg<'a, T>(ptr: *mut T) -> Result<&'a T, JazzNativeRelayStatus> {
    require_arg(!ptr.is_null())?;
    // SAFETY: non-null was checked above; the caller guarantees liveness.
    Ok(unsafe { &*ptr })
}

/// Reset a caller-owned output slot before any other work, so every error
/// leaves one unambiguous state. A null slot is an invalid argument.
///
/// # Safety
/// A non-null `out` must be writable and exclusively owned for this call.
unsafe fn reset_out<T>(out: *mut T, empty: T) -> Result<(), JazzNativeRelayStatus> {
    require_arg(!out.is_null())?;
    // SAFETY: non-null was checked above; the caller owns the slot.
    unsafe { *out = empty };
    Ok(())
}

/// Borrow `len` caller-owned bytes. A zero length needs no pointer.
///
/// # Safety
/// A non-null `ptr` must point to `len` readable bytes for this call.
unsafe fn bytes_arg<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], JazzNativeRelayStatus> {
    if len == 0 {
        return Ok(&[]);
    }
    require_arg(!ptr.is_null())?;
    // SAFETY: non-null was checked above; the caller supplies `len` bytes.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// Borrow a required, non-empty byte argument of at most `max_len` bytes.
///
/// # Safety
/// As for [`bytes_arg`].
unsafe fn bounded_bytes_arg<'a>(
    ptr: *const u8,
    len: usize,
    max_len: usize,
) -> Result<&'a [u8], JazzNativeRelayStatus> {
    require_arg(!ptr.is_null() && len != 0 && len <= max_len)?;
    unsafe { bytes_arg(ptr, len) }
}

/// Copy an opaque 256-bit admission capability argument.
///
/// # Safety
/// A non-null `ptr` must point to `len` readable bytes for this call.
unsafe fn capability_arg(
    ptr: *const u8,
    len: usize,
) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
    require_arg(!ptr.is_null() && len == 32)?;
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, 32) });
    Ok(AdmissionCapability(bytes))
}

/// Hand `bytes` to the caller. The allocation stays Rust-owned until the
/// matching [`jazz_native_relay_bytes_free`].
///
/// # Safety
/// `out` must already have been validated as writable.
unsafe fn write_bytes_out(out: *mut JazzNativeRelayBytes, bytes: Vec<u8>) {
    let bytes = bytes.into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: bytes.len(),
            data: Box::into_raw(bytes).cast(),
        };
    }
}

/// Hand an opaque capability to the caller as an owned 32-byte buffer.
///
/// # Safety
/// As for [`write_bytes_out`].
unsafe fn write_capability_out(out: *mut JazzNativeRelayBytes, capability: AdmissionCapability) {
    unsafe { write_bytes_out(out, capability.0.to_vec()) }
}

/// Encode `value` as postcard and hand it to the caller.
///
/// # Safety
/// As for [`write_bytes_out`].
unsafe fn write_postcard_out(
    out: *mut JazzNativeRelayBytes,
    value: &impl serde::Serialize,
) -> Result<(), JazzNativeRelayStatus> {
    let bytes = postcard::to_allocvec(value).map_err(|_| JazzNativeRelayStatus::EncodeFailure)?;
    unsafe { write_bytes_out(out, bytes) };
    Ok(())
}

fn lock_host(
    inner: &Mutex<NativeRelayHost>,
) -> Result<MutexGuard<'_, NativeRelayHost>, JazzNativeRelayStatus> {
    inner
        .lock()
        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)
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
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let request = unsafe { bytes_arg(request, request_len)? };
        let command = postcard::from_bytes::<RelayCommandRequest>(request)
            .map_err(|_| JazzNativeRelayStatus::InvalidCommand)?;
        let response = match command {
            RelayCommandRequest::Probe => RelayCommandResponse::Probe {
                abi_version: NATIVE_RELAY_ABI_V1,
            },
            _ => return Err(JazzNativeRelayStatus::InvalidCommand),
        };
        unsafe { write_postcard_out(out, &response) }
    })
}

/// Opaque C-owned native relay host. It owns one scope registry and all handles.
#[repr(C)]
pub struct JazzNativeRelayHost {
    inner: Arc<Mutex<NativeRelayHost>>,
}

impl JazzNativeRelayHost {
    fn lock(&self) -> Result<MutexGuard<'_, NativeRelayHost>, JazzNativeRelayStatus> {
        lock_host(&self.inner)
    }
}

/// Retained native-host ownership for a JSI factory and its foreground
/// HostObjects. A platform may release its `JazzNativeRelayHost` wrapper while
/// JavaScript finalizers still exist; this Arc keeps the Rust host state alive
/// until the final foreground object has become unreachable.
#[repr(C)]
pub struct JazzNativeRelayHostLease {
    inner: Arc<Mutex<NativeRelayHost>>,
    runtime_token: u64,
}

impl JazzNativeRelayHostLease {
    fn lock(&self) -> Result<MutexGuard<'_, NativeRelayHost>, JazzNativeRelayStatus> {
        lock_host(&self.inner)
    }

    /// Lock the host and reject a foreground owned by another runtime.
    fn lock_foreground(
        &self,
        foreground: u64,
    ) -> Result<MutexGuard<'_, NativeRelayHost>, JazzNativeRelayStatus> {
        let host = self.lock()?;
        host.require_lease_foreground_runtime(self.runtime_token, foreground)?;
        Ok(host)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_host_new() -> *mut JazzNativeRelayHost {
    Box::into_raw(Box::new(JazzNativeRelayHost {
        inner: Arc::new(Mutex::new(NativeRelayHost::default())),
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

/// Retain a host for private JSI foreground objects.
///
/// The returned lease keeps the host's Rust state alive after the platform
/// releases its original host wrapper. It is intentionally opaque: callers
/// can pass it only to the attached-foreground APIs and must release it once
/// the last factory/HostObject for that JS runtime has been invalidated.
///
/// # Safety
/// `host` must be a live pointer returned by [`jazz_native_relay_host_new`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_retain(
    host: *mut JazzNativeRelayHost,
    runtime_token: u64,
) -> *mut JazzNativeRelayHostLease {
    if host.is_null() || runtime_token == 0 || runtime_token == DIRECT_FOREGROUND_RUNTIME_TOKEN {
        return std::ptr::null_mut();
    }
    let inner = unsafe { Arc::clone(&(&*host).inner) };
    Box::into_raw(Box::new(JazzNativeRelayHostLease {
        inner,
        runtime_token,
    }))
}

/// Release one host lease returned by [`jazz_native_relay_host_retain`].
///
/// # Safety
/// `lease` must be null or an unfreed pointer returned by host_retain.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_free(lease: *mut JazzNativeRelayHostLease) {
    if !lease.is_null() {
        unsafe { drop(Box::from_raw(lease)) };
    }
}

/// Retire every alias belonging to this platform runtime. Repeating it is
/// safe, and it intentionally does not cleanly recycle foreground node IDs.
///
/// # Safety
/// `lease` must be a live lease returned by host_retain.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_invalidate_foreground_runtime(
    lease: *mut JazzNativeRelayHostLease,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let lease = unsafe { live_arg(lease)? };
        lease
            .lock()?
            .retire_foregrounds_for_runtime(lease.runtime_token)
    })
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
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let host = unsafe { live_arg(host)? };
        let request = unsafe { bytes_arg(request, request_len)? };
        let command = postcard::from_bytes::<RelayCommandRequest>(request)
            .map_err(|_| JazzNativeRelayStatus::InvalidCommand)?;
        let response = host.lock()?.execute(command)?;
        unsafe { write_postcard_out(out, &response) }
    })
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
/// The trusted platform must durably bind the supplied SQLite root to the
/// admitted app/environment/account ownership scope. Raw paths supplied by
/// application code are not sufficient admission. Account-session setup derives
/// this root from its versioned account identity; linked identities may share
/// that root but receive separate live session capabilities and repair ledgers.
/// `host`, request bytes, and `out` follow the same rules as
/// [`jazz_native_relay_host_execute`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_admit_scope_json(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let host = unsafe { live_arg(host)? };
        let request =
            unsafe { bounded_bytes_arg(request, request_len, NATIVE_RELAY_ADMISSION_MAX_BYTES)? };
        let request = serde_json::from_slice::<TrustedRelayScopeAdmissionJson>(request)
            .map_err(|_| JazzNativeRelayStatus::InvalidCommand)?
            .normalize()?;
        let capability = host.lock()?.admit_scope(request)?;
        unsafe { write_capability_out(out, capability) };
        Ok(())
    })
}

/// Begin private native session setup. This accepts a bearer only on the
/// trusted platform boundary and uses its unverified payload solely to choose
/// the local SQLite partition. It returns an opaque one-shot setup capability;
/// a schema must be attached separately without credentials.
///
/// # Safety
/// `host`, request bytes, and `out` obey the same validity and ownership rules
/// as [`jazz_native_relay_host_admit_scope_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_begin_private_session_json(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let host = unsafe { live_arg(host)? };
        let request =
            unsafe { bounded_bytes_arg(request, request_len, NATIVE_RELAY_ADMISSION_MAX_BYTES)? };
        let request = serde_json::from_slice::<PrivateSessionSetupJson>(request)
            .map_err(|_| JazzNativeRelayStatus::InvalidCommand)?;
        let capability = host.lock()?.begin_private_session(request)?;
        unsafe { write_capability_out(out, capability) };
        Ok(())
    })
}

/// Attach one credential-free canonical schema to a one-shot private setup.
///
/// # Safety
/// `host` and `out` are live ABI pointers; `session_capability` points to
/// exactly 32 readable bytes and `schema_json` points to `schema_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_attach_canonical_schema_json(
    host: *mut JazzNativeRelayHost,
    session_capability: *const u8,
    capability_len: usize,
    schema_json: *const u8,
    schema_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let host = unsafe { live_arg(host)? };
        let capability = unsafe { capability_arg(session_capability, capability_len)? };
        let schema = unsafe {
            bounded_bytes_arg(schema_json, schema_len, NATIVE_RELAY_ADMISSION_MAX_BYTES)?
        };
        let schema =
            std::str::from_utf8(schema).map_err(|_| JazzNativeRelayStatus::InvalidCommand)?;
        let capability = host.lock()?.attach_canonical_schema(capability, schema)?;
        unsafe { write_capability_out(out, capability) };
        Ok(())
    })
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
    ffi_call(|| {
        let host = unsafe { live_arg(host)? };
        let capability = unsafe { capability_arg(capability, capability_len)? };
        host.lock()?.revoke_scope(capability)?;
        Ok(())
    })
}

/// Open one actual memory-only foreground client from a trusted 32-byte
/// admission capability.
///
/// This is intentionally not expressible through [`RelayCommandRequest`]: a
/// foreground factory may carry the opaque capability, but it must never be
/// able to smuggle one into the generic JavaScript command channel. The output
/// is an opaque host-local handle, not a Rust `Db` pointer. The foreground
/// client runs on the relay owner thread and is already connected to the
/// admitted scope's persistent relay through the ordinary peer protocol.
///
/// # Safety
/// `host` must be live, `capability` must point to exactly 32 readable bytes,
/// and `out_foreground` must be writable for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_open_attached_foreground(
    host: *mut JazzNativeRelayHost,
    capability: *const u8,
    capability_len: usize,
    out_foreground: *mut u64,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let host = unsafe { live_arg(host)? };
        let capability = unsafe { capability_arg(capability, capability_len)? };
        unsafe { reset_out(out_foreground, 0)? };
        let foreground = host
            .lock()?
            .open_foreground(capability, DIRECT_FOREGROUND_RUNTIME_TOKEN)?;
        unsafe { *out_foreground = foreground };
        Ok(())
    })
}

/// Perform one bounded ordinary core tick for an attached foreground client.
///
/// This is the first native implementation of an existing `NativeDb` method:
/// it makes no row/object API available to JSI, but proves the handle invokes
/// the real memory `Db` and its peer link on the dedicated owner thread.
/// A full byte codec for reads/writes/subscriptions will extend this same
/// handle rather than open another runtime or access relay SQLite directly.
///
/// # Safety
/// `host` must be live and `foreground` must be an unclosed handle returned by
/// [`jazz_native_relay_host_open_attached_foreground`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_tick_attached_foreground(
    host: *mut JazzNativeRelayHost,
    foreground: u64,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let host = unsafe { live_arg(host)? };
        require_arg(foreground != 0)?;
        host.lock()?.tick_foreground(foreground)
    })
}

/// Close exactly one attached foreground client. It is intentionally
/// idempotent: a JSI HostObject finalizer and an explicit JavaScript `close`
/// may race during bridge teardown without turning a stale alias into an
/// error. The out flag reports whether this call owned the close transition.
///
/// # Safety
/// `host` must be live and `out_closed` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_close_attached_foreground(
    host: *mut JazzNativeRelayHost,
    foreground: u64,
    out_closed: *mut bool,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let host = unsafe { live_arg(host)? };
        require_arg(foreground != 0)?;
        unsafe { reset_out(out_closed, false)? };
        let closed = host.lock()?.close_foreground(foreground)?;
        unsafe { *out_closed = closed };
        Ok(())
    })
}

/// Lease-safe variant of [`jazz_native_relay_host_open_attached_foreground`].
/// Private JSI factories must use this form rather than retain a raw host
/// pointer across bridge teardown.
///
/// # Safety
/// `lease` must be live, `capability` exactly 32 readable bytes, and
/// `out_foreground` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_open_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    capability: *const u8,
    capability_len: usize,
    out_foreground: *mut u64,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let lease = unsafe { live_arg(lease)? };
        let capability = unsafe { capability_arg(capability, capability_len)? };
        unsafe { reset_out(out_foreground, 0)? };
        let foreground = lease
            .lock()?
            .open_foreground(capability, lease.runtime_token)?;
        unsafe { *out_foreground = foreground };
        Ok(())
    })
}

/// Lease-safe variant of [`jazz_native_relay_host_tick_attached_foreground`].
///
/// # Safety
/// `lease` must be live for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_tick_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let lease = unsafe { live_arg(lease)? };
        require_arg(foreground != 0)?;
        lease
            .lock_foreground(foreground)?
            .tick_foreground(foreground)
    })
}

/// Lease-safe variant of [`jazz_native_relay_host_close_attached_foreground`].
///
/// # Safety
/// `lease` must be live and `out_closed` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_close_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    out_closed: *mut bool,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let lease = unsafe { live_arg(lease)? };
        require_arg(foreground != 0)?;
        unsafe { reset_out(out_closed, false)? };
        let closed = lease
            .lock_foreground(foreground)?
            .close_foreground(foreground)?;
        unsafe { *out_closed = closed };
        Ok(())
    })
}

/// Register or clear the native-to-JavaScript wake sink for one attached
/// foreground. The callback may only schedule a later platform turn.
///
/// # Safety
/// `lease` must be live. A non-null callback context must remain valid until
/// this callback is cleared, the foreground is closed/revoked, or the lease is
/// released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_set_foreground_wake_callback(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    callback: Option<ForegroundWakeCallback>,
    context: *mut c_void,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        let lease = unsafe { live_arg(lease)? };
        require_arg(foreground != 0)?;
        lease
            .lock_foreground(foreground)?
            .set_foreground_wake_callback(foreground, callback, context as usize)
    })
}

/// Execute one complete postcard [`ForegroundDbCommandRequest`] against an
/// attached foreground `Db` retained by a private JSI factory.
///
/// This is the shared native database-command seam for RN, Swift, and Kotlin:
/// the platform binding only copies bytes in/out, while Rust owns the handle,
/// scheduling, and core operation. It is deliberately *not* reachable through
/// [`RelayCommandRequest`] or the public relay TurboModule command API.
///
/// # Safety
/// `lease` must be live, `foreground` must be an opaque handle returned by an
/// attached-foreground open, `request` must be readable for `request_len`
/// bytes (unless that length is zero), and `out` must be writable. On every
/// error `out` is reset to an empty buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_execute_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let lease = unsafe { live_arg(lease)? };
        require_arg(foreground != 0 && request_len <= NATIVE_RELAY_FOREGROUND_COMMAND_MAX_BYTES)?;
        let request = unsafe { bytes_arg(request, request_len)? };
        let command = decode_foreground_command(request)
            .map_err(|_| JazzNativeRelayStatus::InvalidCommand)?;
        let mut host = lease.lock_foreground(foreground)?;
        let response = host.execute_foreground_command(foreground, command)?;
        unsafe { write_postcard_out(out, &response) }
    })
}

impl NativeRelayHost {
    fn execute_foreground_command(
        &mut self,
        foreground: u64,
        command: ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, JazzNativeRelayStatus> {
        use ForegroundDbCommandRequest as Request;
        use ForegroundDbCommandResponse as Response;
        // Every command validates foreground liveness before returning a result.
        // This keeps a stale JSI object from treating `Probe` as a capability
        // oracle after explicit close or platform revocation.
        if !self.foregrounds.contains_key(&foreground) && !matches!(command, Request::Close) {
            return Err(JazzNativeRelayStatus::InvalidHandle);
        }
        if !matches!(command, Request::Close) {
            self.sync_foreground_remote_link(foreground);
        }
        let response = match command {
            Request::NativeSessionMetadata => {
                let opened = self
                    .foregrounds
                    .get(&foreground)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                let admitted = self
                    .relays
                    .get(&opened.relay)
                    .and_then(|relay| self.admitted_scopes.get(&relay.admitted_scope))
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                let author = admitted.config.identity.author;
                let (issuer, user_id) = author.principal_parts();
                Response::NativeSessionMetadata {
                    node: *opened.lease.node.0.as_bytes(),
                    registry_authority: opened.scope.app_namespace.clone(),
                    account_id: author.account_id().map(|account| *account.0.as_bytes()),
                    issuer,
                    user_id,
                }
            }
            Request::DisconnectNativeUpstream => {
                self.foreground_connectivity(foreground, Some(true))?
            }
            Request::ReconnectNativeUpstream => {
                self.foreground_connectivity(foreground, Some(false))?
            }
            Request::NativeConnectionStatus => self.foreground_connectivity(foreground, None)?,
            Request::Probe => Response::Probe {
                abi_version: NATIVE_RELAY_ABI_V1,
            },
            Request::Tick => {
                self.tick_foreground(foreground)?;
                Response::Ticked
            }
            Request::Close => Response::Closed {
                closed: self.close_foreground(foreground)?,
            },
            command => self
                .foreground_client(foreground)?
                .execute(command)
                .map_err(foreground_engine_status)?,
        };
        Ok(response)
    }
}

/// Decode one foreground command, refusing trailing or non-canonical bytes
/// so an encoder drift on either side of the ABI fails loudly.
fn decode_foreground_command(bytes: &[u8]) -> Result<ForegroundDbCommandRequest, String> {
    let (command, trailing) = postcard::take_from_bytes(bytes)
        .map_err(|error| format!("decode foreground command: {error}"))?;
    if !trailing.is_empty() {
        return Err("foreground command has trailing bytes".to_owned());
    }
    let canonical = postcard::to_allocvec(&command)
        .map_err(|error| format!("encode canonical foreground command: {error}"))?;
    if canonical != bytes {
        return Err("foreground command is not canonically encoded".to_owned());
    }
    Ok(command)
}

/// Map an owner-thread failure while running a foreground command. Database
/// refusals are answered in-band by the command itself; only the relay's own
/// lifecycle and backpressure reach the status code.
fn foreground_engine_status(error: RelayError) -> JazzNativeRelayStatus {
    match error {
        RelayError::QueueCapacityExceeded { .. } | RelayError::OwnerQueueFull => {
            JazzNativeRelayStatus::Backpressure
        }
        RelayError::UnknownClient(_) => JazzNativeRelayStatus::InvalidHandle,
        _ => JazzNativeRelayStatus::LifecycleFailure,
    }
}

/// Release any owned response buffer returned by this native C ABI.
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
    // SAFETY: the native C ABI creates this allocation with
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
    if wrapper_range.includes(NATIVE_RELAY_ABI_V1) {
        Ok(NATIVE_RELAY_ABI_V1)
    } else {
        Err(RelayError::IncompatibleAbi {
            native: NATIVE_RELAY_ABI_V1,
            minimum: wrapper_range.minimum,
            maximum: wrapper_range.maximum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod host;
    mod snapshot_capture_codec;
    mod socket_worker;

    /// The C header's ABI define, which the C and C++ probes compare the
    /// artifact against, is the ABI this crate speaks.
    #[test]
    fn c_header_names_the_relay_abi() {
        let header = include_str!("../include/jazz_native_relay.h");
        let define = format!("#define JAZZ_NATIVE_RELAY_ABI_VERSION {NATIVE_RELAY_ABI_V1}\n");
        assert!(header.contains(&define), "header must contain {define:?}");
    }
}
