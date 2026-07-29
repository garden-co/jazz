#![warn(missing_docs)]

//! Minimal operational shell vocabulary for deploying jazz nodes.
//!
//! This crate intentionally stops short of opening sockets, spawning runtimes,
//! or re-exposing database semantics. It gives integrators a small typed
//! surface for role/profile selection, listener/storage configuration, health
//! and metrics snapshots, and graceful drain state while the executable server
//! shape is still being designed.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::SystemTime;

use jazz::db::{
    CommitUnitTrust, Db, DbConfig, DbIdentity, Error as DbError, PeerConnection, ResumeCursor,
    RowCells, SeededRowIdSource, Transport, WireTransportAdapter,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
#[cfg(feature = "rocksdb")]
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, RowUuid, SchemaVersionId};
use jazz::node::EdgeCacheBudget;
use jazz::protocol::{CatalogueAck, CurrentWriteSchema, SchemaVersion, SyncMessage};
use jazz::schema::JazzSchema;
use jazz::wire::{TransportError, WireTransport};

mod admin_schema_convert;
pub mod auth_admission;
pub mod loopback_http;
pub mod loopback_websocket;

/// Result type returned by server shell helpers.
pub type Result<T> = std::result::Result<T, ConfigError>;

/// Result type returned by executable in-memory server shell operations.
pub type ShellResult<T> = std::result::Result<T, ShellError>;

/// Encoded Jazz wire frame bytes exchanged with a host transport.
pub type AbiBytes = Vec<u8>;

/// Opaque handle for a subscriber byte session accepted by the server shell.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServerSession {
    transport: usize,
    identity: AuthorId,
}

impl ServerSession {
    fn transport(self) -> usize {
        self.transport
    }

    fn identity(self) -> AuthorId {
        self.identity
    }
}

/// Process-local resume payload for an in-memory server subscriber session.
///
/// The `resume_token` is only valid for this running shell/runtime and is
/// consumed by [`InMemoryServerShell::resume_subscriber_session`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerSessionResume {
    /// Subscriber author identity to serve the resumed connection under.
    pub identity: AuthorId,
    /// One-shot in-process resume token returned by disconnect-for-resume.
    pub resume_token: u64,
}

/// Configuration for starting an in-memory executable server shell.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryServerShellConfig {
    /// Schema served by the in-memory database.
    pub schema: JazzSchema,
    /// Database identity used by server-owned local writes.
    pub identity: DbIdentity,
    /// Optional deterministic row-id seed for ABI writes.
    pub row_id_seed: Option<u64>,
    /// Optional edge-cache byte budget. `None` disables automatic eviction.
    pub edge_cache_budget: Option<EdgeCacheBudget>,
    /// Server role used for client-link semantics.
    pub role: NodeRole,
    /// Whether startup should publish the constructor schema into the runtime
    /// catalogue if the opened store does not already carry a durable write
    /// pointer for it.
    pub bootstrap_runtime_schema: bool,
}

impl InMemoryServerShellConfig {
    /// Construct a shell config from an explicit schema and database identity.
    pub fn new(schema: JazzSchema, identity: DbIdentity) -> Self {
        Self {
            schema,
            identity,
            row_id_seed: None,
            edge_cache_budget: None,
            role: NodeRole::Core,
            bootstrap_runtime_schema: false,
        }
    }

    /// Set a deterministic row-id seed for server-side ABI writes.
    pub fn with_row_id_seed(mut self, row_id_seed: u64) -> Self {
        self.row_id_seed = Some(row_id_seed);
        self
    }

    /// Configure automatic edge-cache eviction by byte budget.
    pub fn with_edge_cache_budget(mut self, budget: EdgeCacheBudget) -> Self {
        self.edge_cache_budget = Some(budget);
        self
    }

    /// Configure this shell's server role.
    pub fn with_role(mut self, role: NodeRole) -> Self {
        self.role = role;
        self
    }

    /// Ensure the constructor schema is present in the runtime catalogue at
    /// startup. Product `JazzServer` uses this for pre-seeded stores opened
    /// from a data directory; low-level loopback shells leave it disabled so
    /// their admin-publish tests observe an initially empty runtime lane.
    pub fn with_runtime_schema_bootstrap(mut self) -> Self {
        self.bootstrap_runtime_schema = true;
        self
    }
}

/// Library-only in-memory server shell backed by one ABI runtime and database.
#[derive(Debug)]
pub struct InMemoryServerShell {
    db: ShellDb,
    role: NodeRole,
    sessions: Vec<Option<ServerSessionState>>,
    resume_cursors: BTreeMap<u64, (AuthorId, ResumeCursor)>,
    next_resume_token: u64,
    runtime_schema_state: RuntimeSchemaState,
    metrics: InMemoryServerShellMetrics,
    drain_state: DrainState,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RuntimeSchemaState {
    current_write_revision: u64,
    last_published_schema: Option<SchemaVersionId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct InMemoryServerShellMetrics {
    active_sessions: u64,
    total_sessions: u64,
    rejected_sessions: u64,
    frames_received: u64,
    frames_sent: u64,
    bytes_received: u64,
    bytes_sent: u64,
    ticks: u64,
    tick_inbound: u64,
    tick_outbound: u64,
    tick_subscription_wakes: u64,
    tick_write_wakes: u64,
    last_tick: ShellTickStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ShellTickStats {
    inbound: u32,
    outbound: u32,
    subscription_wakes: u32,
    write_wakes: u32,
}

/// Resume state reported for a server-owned subscriber session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerResumeStatus {
    /// The session was accepted without a prior resume cursor.
    Fresh,
    /// The session was accepted from a process-local resume cursor.
    Resumed,
}

/// Direct transport diagnostics for a server-owned subscriber session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbiTransportDiagnostics {
    /// Process-local session slot identifier.
    pub session_id: String,
    /// Monotone session incarnation used by this process-local shell.
    pub epoch: u64,
    /// Whether the session was fresh or resumed from a cursor.
    pub resume_status: ServerResumeStatus,
    /// Number of inbound host frames currently staged for database processing.
    pub queued_inbound_frames: usize,
    /// Number of outbound host frames currently waiting to be drained.
    pub queued_outbound_frames: usize,
    /// Serialized byte size of the latest resume/catch-up response, if known.
    pub last_resume_bytes: Option<usize>,
}

enum ShellDb {
    Memory(Db<MemoryStorage>),
    #[cfg(feature = "rocksdb")]
    Rocks(Db<RocksDbStorage>),
}

struct ServerSessionState {
    connection: ShellPeerConnection,
    transport: SharedWireTransport,
    identity: AuthorId,
    epoch: u64,
    resume_status: ServerResumeStatus,
}

enum ShellPeerConnection {
    Memory(Rc<RefCell<PeerConnection<MemoryStorage>>>),
    #[cfg(feature = "rocksdb")]
    Rocks(Rc<RefCell<PeerConnection<RocksDbStorage>>>),
}

#[derive(Clone, Debug, Default)]
struct SharedWireTransport {
    queues: Rc<RefCell<WireQueues>>,
}

#[derive(Debug, Default)]
struct WireQueues {
    inbound: VecDeque<Vec<u8>>,
    outbound: VecDeque<Vec<u8>>,
}

impl WireTransport for SharedWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> std::result::Result<(), TransportError> {
        self.queues.borrow_mut().outbound.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().inbound.pop_front()
    }
}

impl fmt::Debug for ShellDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(_) => f.write_str("ShellDb::Memory(..)"),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(_) => f.write_str("ShellDb::Rocks(..)"),
        }
    }
}

impl fmt::Debug for ServerSessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerSessionState")
            .field("transport", &self.transport)
            .field("identity", &self.identity)
            .field("epoch", &self.epoch)
            .field("resume_status", &self.resume_status)
            .finish_non_exhaustive()
    }
}

impl ShellDb {
    fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        match self {
            Self::Memory(db) => db.set_edge_cache_budget(budget),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.set_edge_cache_budget(budget),
        }
    }

    fn current_write_schema(&self) -> CurrentWriteSchema {
        match self {
            Self::Memory(db) => db.current_write_schema(),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.current_write_schema(),
        }
    }

    fn catalogue_schema(&self, schema: SchemaVersionId) -> Option<JazzSchema> {
        match self {
            Self::Memory(db) => db.catalogue_schema(schema),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.catalogue_schema(schema),
        }
    }

    fn connect_upstream(&self, transport: Box<dyn Transport>) -> ShellPeerConnection {
        match self {
            Self::Memory(db) => ShellPeerConnection::Memory(db.connect_upstream(transport)),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => ShellPeerConnection::Rocks(db.connect_upstream(transport)),
        }
    }

    fn publish_schema(&self, schema: SchemaVersion) -> ShellResult<Vec<SyncMessage>> {
        match self {
            Self::Memory(db) => db.publish_schema(schema).map_err(Into::into),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.publish_schema(schema).map_err(Into::into),
        }
    }

    fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> ShellResult<Vec<SyncMessage>> {
        match self {
            Self::Memory(db) => db.set_current_write_schema(pointer).map_err(Into::into),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.set_current_write_schema(pointer).map_err(Into::into),
        }
    }

    fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn jazz::db::Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
    ) -> ShellPeerConnection {
        match (self, cursor) {
            (Self::Memory(db), Some(cursor)) => ShellPeerConnection::Memory(
                db.accept_subscriber_with_resume(transport, identity, cursor),
            ),
            (Self::Memory(db), None) => ShellPeerConnection::Memory(
                db.accept_subscriber_with_claims(transport, identity, claims),
            ),
            #[cfg(feature = "rocksdb")]
            (Self::Rocks(db), Some(cursor)) => ShellPeerConnection::Rocks(
                db.accept_subscriber_with_resume(transport, identity, cursor),
            ),
            #[cfg(feature = "rocksdb")]
            (Self::Rocks(db), None) => ShellPeerConnection::Rocks(
                db.accept_subscriber_with_claims(transport, identity, claims),
            ),
        }
    }

    fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn jazz::db::Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> ShellPeerConnection {
        match self {
            Self::Memory(db) => ShellPeerConnection::Memory(
                db.accept_subscriber_with_claims_and_trust(transport, identity, claims, trust),
            ),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => ShellPeerConnection::Rocks(
                db.accept_subscriber_with_claims_and_trust(transport, identity, claims, trust),
            ),
        }
    }

    fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn jazz::db::Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> ShellPeerConnection {
        match self {
            Self::Memory(db) => ShellPeerConnection::Memory(
                db.accept_edge_subscriber_with_claims(transport, identity, claims),
            ),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => ShellPeerConnection::Rocks(
                db.accept_edge_subscriber_with_claims(transport, identity, claims),
            ),
        }
    }

    fn detach_connection(&self, connection: &ShellPeerConnection) -> bool {
        match (self, connection) {
            (Self::Memory(db), ShellPeerConnection::Memory(connection)) => {
                db.detach_connection(connection)
            }
            #[cfg(feature = "rocksdb")]
            (Self::Rocks(db), ShellPeerConnection::Rocks(connection)) => {
                db.detach_connection(connection)
            }
            #[cfg(feature = "rocksdb")]
            _ => false,
        }
    }

    fn tick_stats(&self) -> ShellResult<jazz::db::DbTickStats> {
        match self {
            Self::Memory(db) => db.tick_stats().map_err(Into::into),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db.tick_stats().map_err(Into::into),
        }
    }

    fn seed_settled_mergeable_for_bootstrap(
        &self,
        table: String,
        row_id: RowUuid,
        author: AuthorId,
        cells: RowCells,
    ) -> ShellResult<()> {
        match self {
            Self::Memory(db) => db
                .seed_settled_mergeable_for_bootstrap(&table, row_id, author, cells)
                .map(|_| ())
                .map_err(Into::into),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(db) => db
                .seed_settled_mergeable_for_bootstrap(&table, row_id, author, cells)
                .map(|_| ())
                .map_err(Into::into),
        }
    }
}

impl ShellPeerConnection {
    fn take_resume_cursor(&self) -> Option<ResumeCursor> {
        match self {
            Self::Memory(connection) => connection.borrow_mut().take_resume_cursor(),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(connection) => connection.borrow_mut().take_resume_cursor(),
        }
    }

    fn last_resume_bytes(&self) -> Option<usize> {
        match self {
            Self::Memory(connection) => connection.borrow().last_resume_bytes(),
            #[cfg(feature = "rocksdb")]
            Self::Rocks(connection) => connection.borrow().last_resume_bytes(),
        }
    }
}

impl InMemoryServerShell {
    /// Start a shell with in-memory storage from explicit schema and DB identity.
    pub fn start(config: InMemoryServerShellConfig) -> ShellResult<Self> {
        Self::start_with_storage(config, StorageConfig::InMemory)
    }

    /// Start a shell with explicit storage from schema and DB identity.
    pub fn start_with_storage(
        config: InMemoryServerShellConfig,
        storage_config: StorageConfig,
    ) -> ShellResult<Self> {
        let edge_cache_budget = config.edge_cache_budget;
        let role = config.role;
        let bootstrap_runtime_schema = config.bootstrap_runtime_schema;
        let bootstrap_schema = config.schema.clone();
        let db = match &storage_config {
            StorageConfig::InMemory => {
                let refs = config.schema.column_families();
                let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
                let mut db_config =
                    DbConfig::new(config.schema, MemoryStorage::new(&refs), config.identity);
                if let Some(row_id_seed) = config.row_id_seed {
                    db_config = db_config.with_id_source(SeededRowIdSource::new(row_id_seed));
                }
                ShellDb::Memory(jazz::db::block_on(Db::open_history_complete(db_config))?)
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path } => {
                let refs = config.schema.column_families();
                let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
                let mut db_config = DbConfig::new(
                    config.schema,
                    RocksDbStorage::open(path, &refs).map_err(db_storage_error)?,
                    config.identity,
                );
                if let Some(row_id_seed) = config.row_id_seed {
                    db_config = db_config.with_id_source(SeededRowIdSource::new(row_id_seed));
                }
                ShellDb::Rocks(jazz::db::block_on(Db::open_history_complete(db_config))?)
            }
            #[cfg(not(feature = "rocksdb"))]
            StorageConfig::RocksDb { .. } => {
                return Err(ShellError::UnsupportedStorage {
                    storage: storage_config,
                });
            }
            StorageConfig::SQLite { .. } => {
                return Err(ShellError::UnsupportedStorage {
                    storage: storage_config,
                });
            }
        };
        db.set_edge_cache_budget(edge_cache_budget);

        let mut shell = Self {
            db,
            role,
            sessions: Vec::new(),
            resume_cursors: BTreeMap::new(),
            next_resume_token: 1,
            runtime_schema_state: RuntimeSchemaState::default(),
            metrics: InMemoryServerShellMetrics::default(),
            drain_state: DrainState::Running,
        };
        if bootstrap_runtime_schema {
            shell.bootstrap_runtime_schema(bootstrap_schema)?;
        }
        Ok(shell)
    }

    /// Return the shell's ABI runtime handle for diagnostics in unit tests.
    #[cfg(test)]
    pub fn runtime_handle(&self) -> usize {
        0
    }

    /// Return the shell's ABI database handle for diagnostics in unit tests.
    #[cfg(test)]
    pub fn db_handle(&self) -> usize {
        0
    }

    /// Return the most recent schema version published through this shell's runtime admin lane.
    pub fn last_published_runtime_schema(&self) -> Option<SchemaVersionId> {
        self.runtime_schema_state.last_published_schema
    }

    /// Return the current write-schema pointer revision last applied through this shell.
    pub fn runtime_write_schema_revision(&self) -> u64 {
        self.runtime_schema_state.current_write_revision
    }

    fn bootstrap_runtime_schema(&mut self, schema: JazzSchema) -> ShellResult<()> {
        let schema_id = schema.version_id();
        let current = self.db.current_write_schema();
        if current.schema == schema_id
            && current.revision > 0
            && self.db.catalogue_schema(schema_id).as_ref() == Some(&schema)
        {
            self.runtime_schema_state.current_write_revision = current.revision;
            self.runtime_schema_state.last_published_schema = Some(schema_id);
            return Ok(());
        }

        self.publish_runtime_schema(schema)?;
        Ok(())
    }

    /// Publish a schema to the local runtime catalogue and make it the current write schema.
    pub fn publish_runtime_schema(&mut self, schema: JazzSchema) -> ShellResult<SchemaVersionId> {
        let schema_version = SchemaVersion::new(schema);
        let schema_id = schema_version.id;
        let expected_schema = schema_version.schema.clone();
        let current = self.db.current_write_schema();
        if current.schema == schema_id
            && current.revision > 0
            && self.db.catalogue_schema(schema_id).as_ref() == Some(&expected_schema)
        {
            self.runtime_schema_state.current_write_revision = current.revision;
            self.runtime_schema_state.last_published_schema = Some(schema_id);
            return Ok(schema_id);
        }

        let publish_acks = catalogue_acks_from_messages(self.db.publish_schema(schema_version)?);
        let catalogue_matches =
            self.db.catalogue_schema(schema_id).as_ref() == Some(&expected_schema);
        let publish_applied = publish_acks
            .iter()
            .any(|ack| ack.applied && ack.schema == Some(schema_id));
        if !publish_applied && !catalogue_matches {
            return Err(ShellError::MissingEvent("CatalogueAck"));
        }
        if !catalogue_matches {
            return Err(ShellError::MissingEvent("CatalogueAck"));
        }

        let current = self.db.current_write_schema();
        if current.schema == schema_id && current.revision > 0 {
            self.runtime_schema_state.current_write_revision = current.revision;
            self.runtime_schema_state.last_published_schema = Some(schema_id);
            return Ok(schema_id);
        }

        let revision = current.revision.saturating_add(1);
        let set_current_acks =
            catalogue_acks_from_messages(self.db.set_current_write_schema(CurrentWriteSchema {
                revision,
                schema: schema_id,
            })?);
        let set_current_applied = set_current_acks.iter().any(|ack| {
            ack.applied && ack.revision == Some(revision) && ack.schema == Some(schema_id)
        });
        let current = self.db.current_write_schema();
        if !(set_current_applied || current.schema == schema_id && current.revision > 0) {
            return Err(ShellError::MissingEvent("CatalogueAck"));
        }

        self.runtime_schema_state.current_write_revision = current.revision;
        self.runtime_schema_state.last_published_schema = Some(schema_id);
        Ok(schema_id)
    }

    /// Accept one subscriber byte session under the supplied author identity.
    pub fn accept_subscriber_session(&mut self, identity: AuthorId) -> ShellResult<ServerSession> {
        self.accept_subscriber_session_with_claims(identity, BTreeMap::new())
    }

    /// Accept one subscriber byte session under the supplied identity and claims.
    pub fn accept_subscriber_session_with_claims(
        &mut self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> ShellResult<ServerSession> {
        self.accept_subscriber_session_with_claims_and_trust(
            identity,
            claims,
            CommitUnitTrust::Session,
        )
    }

    /// Accept one subscriber byte session under the supplied identity, claims, and trust mode.
    pub fn accept_subscriber_session_with_claims_and_trust(
        &mut self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> ShellResult<ServerSession> {
        if self.is_draining() {
            self.metrics.rejected_sessions += 1;
            return Err(ShellError::SessionRejected {
                drain_state: self.drain_state,
            });
        }
        let transport = SharedWireTransport::default();
        let transport_adapter = Box::new(WireTransportAdapter::current(transport.clone()));
        let connection = if self.role == NodeRole::Edge && trust == CommitUnitTrust::Session {
            self.db
                .accept_edge_subscriber_with_claims(transport_adapter, identity, claims)
        } else {
            self.db.accept_subscriber_with_claims_and_trust(
                transport_adapter,
                identity,
                claims,
                trust,
            )
        };
        let session_id = self.sessions.len();
        self.sessions.push(Some(ServerSessionState {
            connection,
            transport,
            identity,
            epoch: 1,
            resume_status: ServerResumeStatus::Fresh,
        }));
        self.note_session_admitted();
        Ok(ServerSession {
            transport: session_id,
            identity,
        })
    }

    /// Attach this edge shell to an upstream core transport.
    pub fn connect_upstream(&mut self, transport: Box<dyn Transport>) -> ShellResult<()> {
        let _connection = self.db.connect_upstream(transport);
        self.tick()?;
        Ok(())
    }

    /// Disconnect a subscriber session while preserving an in-process cursor.
    ///
    /// This is a real detach that queues normal transport-close events. The
    /// returned token is process-local, one-shot, and not a durable/network
    /// resume credential.
    pub fn disconnect_session_for_resume(
        &mut self,
        session: ServerSession,
    ) -> ShellResult<ServerSessionResume> {
        let state = self.take_session(session)?;
        let cursor = state.connection.take_resume_cursor();
        self.db.detach_connection(&state.connection);
        let Some(cursor) = cursor else {
            return Err(ShellError::MissingEvent("SubscriberResumeCursor"));
        };
        let resume_token = self.next_resume_token;
        self.next_resume_token = self.next_resume_token.saturating_add(1);
        self.resume_cursors
            .insert(resume_token, (session.identity(), cursor));
        self.note_session_closed();
        Ok(ServerSessionResume {
            identity: session.identity(),
            resume_token,
        })
    }

    /// Resume a subscriber session using a process-local one-shot token.
    pub fn resume_subscriber_session(
        &mut self,
        resume: ServerSessionResume,
    ) -> ShellResult<ServerSession> {
        if self.is_draining() {
            self.metrics.rejected_sessions += 1;
            return Err(ShellError::SessionRejected {
                drain_state: self.drain_state,
            });
        }
        let Some((identity, cursor)) = self.resume_cursors.remove(&resume.resume_token) else {
            return Err(ShellError::InvalidResumeToken(resume.resume_token));
        };
        if identity != resume.identity {
            self.resume_cursors
                .insert(resume.resume_token, (identity, cursor));
            return Err(ShellError::ResumeIdentityMismatch {
                expected: identity,
                actual: resume.identity,
            });
        }
        let transport = SharedWireTransport::default();
        let connection = self.db.accept_subscriber_with_claims(
            Box::new(WireTransportAdapter::current(transport.clone())),
            resume.identity,
            BTreeMap::new(),
            Some(cursor),
        );
        let session_id = self.sessions.len();
        self.sessions.push(Some(ServerSessionState {
            connection,
            transport,
            identity: resume.identity,
            epoch: resume.resume_token.saturating_add(1),
            resume_status: ServerResumeStatus::Resumed,
        }));
        self.note_session_admitted();
        Ok(ServerSession {
            transport: session_id,
            identity: resume.identity,
        })
    }

    /// Close a subscriber session without preserving a resume cursor.
    pub fn close_session(&mut self, session: ServerSession) -> ShellResult<()> {
        let state = self.take_session(session)?;
        self.db.detach_connection(&state.connection);
        self.note_session_closed();
        Ok(())
    }

    /// Start graceful drain and stop accepting new sessions.
    pub fn begin_drain(&mut self) {
        self.drain_state = if self.metrics.active_sessions == 0 {
            DrainState::Drained
        } else {
            DrainState::Draining
        };
    }

    /// Return the current shutdown/drain state.
    pub fn drain_state(&self) -> DrainState {
        self.drain_state
    }

    /// Return a live health snapshot for the in-memory shell.
    pub fn health_snapshot(&self) -> HealthSnapshot {
        let (status, message) = match self.drain_state {
            DrainState::Running => (HealthStatus::Ready, "ready"),
            DrainState::ShutdownRequested | DrainState::Draining => {
                (HealthStatus::Draining, "draining")
            }
            DrainState::Drained => (HealthStatus::Draining, "drained"),
            DrainState::Stopped => (HealthStatus::Unhealthy, "stopped"),
        };
        HealthSnapshot {
            status,
            role: NodeRole::Core,
            profile: DeploymentProfile::Local,
            drain_state: self.drain_state,
            message: message.to_owned(),
            observed_at: SystemTime::now(),
        }
    }

    /// Return live operational counters for the in-memory shell.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            active_sessions: self.metrics.active_sessions,
            total_sessions: self.metrics.total_sessions,
            rejected_sessions: self.metrics.rejected_sessions,
            frames_received: self.metrics.frames_received,
            frames_sent: self.metrics.frames_sent,
            bytes_received: self.metrics.bytes_received,
            bytes_sent: self.metrics.bytes_sent,
            ticks: self.metrics.ticks,
            tick_inbound: self.metrics.tick_inbound,
            tick_outbound: self.metrics.tick_outbound,
            tick_subscription_wakes: self.metrics.tick_subscription_wakes,
            tick_write_wakes: self.metrics.tick_write_wakes,
            last_tick_inbound: u64::from(self.metrics.last_tick.inbound),
            last_tick_outbound: u64::from(self.metrics.last_tick.outbound),
            last_tick_subscription_wakes: u64::from(self.metrics.last_tick.subscription_wakes),
            last_tick_write_wakes: u64::from(self.metrics.last_tick.write_wakes),
            protocol_version_mismatches: 0,
            subscription_full_diff_fallbacks: 0,
            storage_migrations_applied: 0,
        }
    }

    /// Stage encoded wire frames received from the host for a subscriber session.
    pub fn receive_frames(
        &mut self,
        session: ServerSession,
        frames: impl IntoIterator<Item = AbiBytes>,
    ) -> ShellResult<()> {
        for frame in frames {
            self.metrics.frames_received += 1;
            self.metrics.bytes_received += frame.len() as u64;
            self.session_state(session)?
                .transport
                .queues
                .borrow_mut()
                .inbound
                .push_back(frame);
        }
        Ok(())
    }

    /// Service the shell database's accepted subscriber connections once.
    pub fn tick(&mut self) -> ShellResult<()> {
        let stats = self.db.tick_stats()?;
        self.metrics.ticks += 1;
        let stats = ShellTickStats {
            inbound: 0,
            outbound: 0,
            subscription_wakes: stats.subscription_events as u32,
            write_wakes: 0,
        };
        self.metrics.last_tick = stats;
        self.metrics.tick_inbound += u64::from(stats.inbound);
        self.metrics.tick_outbound += u64::from(stats.outbound);
        self.metrics.tick_subscription_wakes += u64::from(stats.subscription_wakes);
        self.metrics.tick_write_wakes += u64::from(stats.write_wakes);
        Ok(())
    }

    /// Drain encoded wire frames ready to send to the host for a session.
    pub fn take_frames(&mut self, session: ServerSession) -> ShellResult<Vec<AbiBytes>> {
        let mut queues = self.session_state(session)?.transport.queues.borrow_mut();
        let frames = queues.outbound.drain(..).collect::<Vec<_>>();
        drop(queues);
        self.metrics.frames_sent += frames.len() as u64;
        self.metrics.bytes_sent += frames.iter().map(Vec::len).sum::<usize>() as u64;
        Ok(frames)
    }

    /// Return transport/session diagnostics for a subscriber session.
    pub fn session_diagnostics(
        &mut self,
        session: ServerSession,
    ) -> ShellResult<AbiTransportDiagnostics> {
        let state = self.session_state(session)?;
        let queues = state.transport.queues.borrow();
        Ok(AbiTransportDiagnostics {
            session_id: session.transport().to_string(),
            epoch: state.epoch,
            resume_status: state.resume_status,
            queued_inbound_frames: queues.inbound.len(),
            queued_outbound_frames: queues.outbound.len(),
            last_resume_bytes: state.connection.last_resume_bytes(),
        })
    }

    /// Seed a row through the server database for bootstrap/import flows.
    ///
    /// This bypasses pending client write semantics and immediately records a
    /// finalized mergeable commit in history-complete server state.
    pub fn seed_row_with_id(
        &mut self,
        table: impl Into<String>,
        row_id: RowUuid,
        cells: RowCells,
    ) -> ShellResult<()> {
        self.db
            .seed_settled_mergeable_for_bootstrap(table.into(), row_id, AuthorId::SYSTEM, cells)
    }

    fn is_draining(&self) -> bool {
        !matches!(self.drain_state, DrainState::Running)
    }

    fn note_session_admitted(&mut self) {
        self.metrics.active_sessions += 1;
        self.metrics.total_sessions += 1;
    }

    fn note_session_closed(&mut self) {
        self.metrics.active_sessions = self.metrics.active_sessions.saturating_sub(1);
        if self.is_draining() && self.metrics.active_sessions == 0 {
            self.drain_state = DrainState::Drained;
        }
    }

    fn session_state(&self, session: ServerSession) -> ShellResult<&ServerSessionState> {
        self.sessions
            .get(session.transport())
            .and_then(Option::as_ref)
            .filter(|state| state.identity == session.identity())
            .ok_or(ShellError::InvalidSession)
    }

    fn take_session(&mut self, session: ServerSession) -> ShellResult<ServerSessionState> {
        let state = self
            .sessions
            .get_mut(session.transport())
            .and_then(Option::take)
            .ok_or(ShellError::InvalidSession)?;
        if state.identity != session.identity() {
            return Err(ShellError::InvalidSession);
        }
        Ok(state)
    }
}

/// Error returned by the executable in-memory server shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShellError {
    /// A direct database operation failed.
    Db(String),
    /// A storage operation failed.
    Storage(String),
    /// A direct transport operation failed.
    Transport(String),
    /// A postcard payload could not be encoded or decoded.
    Codec(String),
    /// An expected direct database response was not produced.
    MissingEvent(&'static str),
    /// A server session handle is no longer live in this process.
    InvalidSession,
    /// A process-local resume token is unknown or has already been consumed.
    InvalidResumeToken(u64),
    /// A process-local resume token was presented for the wrong identity.
    ResumeIdentityMismatch {
        /// Identity that owns the resume cursor.
        expected: AuthorId,
        /// Identity supplied by the caller.
        actual: AuthorId,
    },
    /// A new session was rejected because the shell is draining.
    SessionRejected {
        /// Drain state that caused the rejection.
        drain_state: DrainState,
    },
    /// The requested storage backend is not implemented by this shell.
    UnsupportedStorage {
        /// Unsupported storage config.
        storage: StorageConfig,
    },
}

impl fmt::Display for ShellError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(error) => write!(f, "database error: {error}"),
            Self::Storage(error) => write!(f, "storage error: {error}"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Codec(error) => write!(f, "codec error: {error}"),
            Self::MissingEvent(event) => write!(f, "missing database response: {event}"),
            Self::InvalidSession => write!(f, "invalid or closed server session"),
            Self::InvalidResumeToken(token) => {
                write!(f, "invalid subscriber resume token: {token}")
            }
            Self::ResumeIdentityMismatch { expected, actual } => write!(
                f,
                "subscriber resume token belongs to {expected:?}, not {actual:?}"
            ),
            Self::SessionRejected { drain_state } => {
                write!(f, "session rejected while shell is {drain_state:?}")
            }
            Self::UnsupportedStorage { storage } => {
                write!(f, "unsupported server shell storage: {storage:?}")
            }
        }
    }
}

impl std::error::Error for ShellError {}

impl From<DbError> for ShellError {
    fn from(error: DbError) -> Self {
        Self::Db(error.to_string())
    }
}

impl From<TransportError> for ShellError {
    fn from(error: TransportError) -> Self {
        Self::Transport(format!("{error:?}"))
    }
}

impl From<postcard::Error> for ShellError {
    fn from(error: postcard::Error) -> Self {
        Self::Codec(error.to_string())
    }
}

fn catalogue_acks_from_messages(messages: Vec<SyncMessage>) -> Vec<CatalogueAck> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SyncMessage::CatalogueAck(ack) => Some(ack),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "rocksdb")]
fn db_storage_error(error: impl fmt::Display) -> ShellError {
    ShellError::Storage(error.to_string())
}

/// Deployment role for a server-side jazz node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeRole {
    /// Stateless protocol relay with no fate authority.
    Relay,
    /// Edge node that can cache and broker traffic near clients.
    Edge,
    /// Durable core node that owns authoritative storage.
    Core,
}

/// Operational deployment profile for packaging and validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentProfile {
    /// Local development defaults.
    Local,
    /// Test harness profile with in-memory storage and loopback listeners.
    Test,
    /// Production-oriented profile with explicit durable settings.
    Production,
}

/// Storage backend configuration for the shell.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageConfig {
    /// Volatile in-memory storage for tests and dry local runs.
    InMemory,
    /// RocksDB storage rooted at the provided path.
    RocksDb {
        /// Filesystem path for the RocksDB database.
        path: PathBuf,
    },
    /// SQLite storage at the provided path.
    SQLite {
        /// Filesystem path for the SQLite database.
        path: PathBuf,
    },
}

impl StorageConfig {
    /// Construct the default durable storage config rooted at a data dir.
    ///
    /// The current alpha CLI/dev-server shape exposes durable local state as a
    /// `dataDir`. In this shell, that maps to the first durable backend
    /// implemented in the Rust storage stack; today that is RocksDB.
    pub fn data_dir(path: impl Into<PathBuf>) -> Self {
        Self::RocksDb { path: path.into() }
    }

    fn is_durable(&self) -> bool {
        !matches!(self, Self::InMemory)
    }

    fn kind(&self) -> StorageKind {
        match self {
            Self::InMemory => StorageKind::InMemory,
            Self::RocksDb { .. } => StorageKind::RocksDb,
            Self::SQLite { .. } => StorageKind::SQLite,
        }
    }
}

/// Storage backend kind used in core-facing startup plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageKind {
    /// Volatile in-memory storage.
    InMemory,
    /// RocksDB storage.
    RocksDb,
    /// SQLite storage.
    SQLite,
}

/// Listener configuration for future transport endpoints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListenerConfig {
    /// Socket address the server would bind in a full implementation.
    pub bind_addr: SocketAddr,
    /// Path reserved for WebSocket sync transport.
    pub websocket_path: String,
    /// Path reserved for liveness/readiness health checks.
    pub health_path: String,
    /// Path reserved for metrics scraping.
    pub metrics_path: String,
    /// Maximum accepted frame size in bytes.
    pub max_frame_bytes: usize,
    /// Maximum concurrent connections accepted by policy.
    pub max_connections: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            websocket_path: "/sync".to_owned(),
            health_path: "/healthz".to_owned(),
            metrics_path: "/metrics".to_owned(),
            max_frame_bytes: 1 << 20,
            max_connections: 1024,
        }
    }
}

/// Top-level server shell configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfig {
    /// Stable node name used in logs, metrics, and health snapshots.
    pub node_name: String,
    /// Topology role for this server shell.
    pub role: NodeRole,
    /// Deployment profile used to validate operational defaults.
    pub profile: DeploymentProfile,
    /// Storage backend policy.
    pub storage: StorageConfig,
    /// Future listener policy. Dry runs validate but do not bind it.
    pub listener: ListenerConfig,
    /// Whether graceful shutdown should stop admitting new sessions first.
    pub drain_on_shutdown: bool,
}

impl ServerConfig {
    /// Construct a local in-memory core config suitable for tests and examples.
    pub fn local(node_name: impl Into<String>) -> Self {
        Self {
            node_name: node_name.into(),
            role: NodeRole::Core,
            profile: DeploymentProfile::Local,
            storage: StorageConfig::InMemory,
            listener: ListenerConfig::default(),
            drain_on_shutdown: true,
        }
    }
}

/// High-level health state exposed by operational probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthStatus {
    /// The shell is configured and ready to admit traffic.
    Ready,
    /// The shell is alive but intentionally not accepting normal traffic.
    Draining,
    /// The shell failed validation or detected an unrecoverable condition.
    Unhealthy,
}

/// Point-in-time health report for a shell instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    /// Overall health status.
    pub status: HealthStatus,
    /// Role reported by the shell.
    pub role: NodeRole,
    /// Deployment profile reported by the shell.
    pub profile: DeploymentProfile,
    /// Current shutdown/drain state.
    pub drain_state: DrainState,
    /// Human-readable status detail.
    pub message: String,
    /// Snapshot creation timestamp.
    pub observed_at: SystemTime,
}

impl Default for HealthSnapshot {
    fn default() -> Self {
        Self {
            status: HealthStatus::Ready,
            role: NodeRole::Core,
            profile: DeploymentProfile::Local,
            drain_state: DrainState::Running,
            message: "ready".to_owned(),
            observed_at: SystemTime::UNIX_EPOCH,
        }
    }
}

/// Point-in-time operational counters for a shell instance.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    /// Active admitted sessions.
    pub active_sessions: u64,
    /// Total admitted sessions since process start.
    pub total_sessions: u64,
    /// Rejected session admissions since process start.
    pub rejected_sessions: u64,
    /// Wire frames received from hosts since process start.
    pub frames_received: u64,
    /// Wire frames sent to hosts since process start.
    pub frames_sent: u64,
    /// Wire bytes received from hosts since process start.
    pub bytes_received: u64,
    /// Wire bytes sent to hosts since process start.
    pub bytes_sent: u64,
    /// Database drive ticks run by this shell.
    pub ticks: u64,
    /// Cumulative inbound work reported by database ticks.
    pub tick_inbound: u64,
    /// Cumulative outbound work reported by database ticks.
    pub tick_outbound: u64,
    /// Cumulative subscription wakeups reported by database ticks.
    pub tick_subscription_wakes: u64,
    /// Cumulative write wakeups reported by database ticks.
    pub tick_write_wakes: u64,
    /// Inbound work reported by the most recent tick.
    pub last_tick_inbound: u64,
    /// Outbound work reported by the most recent tick.
    pub last_tick_outbound: u64,
    /// Subscription wakeups reported by the most recent tick.
    pub last_tick_subscription_wakes: u64,
    /// Write wakeups reported by the most recent tick.
    pub last_tick_write_wakes: u64,
    /// Protocol version mismatches observed at admission.
    pub protocol_version_mismatches: u64,
    /// Maintained subscription view full-recompute paths observed through this shell.
    pub subscription_full_diff_fallbacks: u64,
    /// Number of storage migrations reported by startup checks.
    pub storage_migrations_applied: u64,
}

/// Coordinated shutdown and connection drain state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DrainState {
    /// Normal operation; new sessions may be admitted.
    #[default]
    Running,
    /// Shutdown was requested; new sessions should be rejected.
    ShutdownRequested,
    /// Existing sessions are being drained.
    Draining,
    /// All sessions drained and the shell can stop.
    Drained,
    /// Shutdown completed.
    Stopped,
}

/// Result of a dry-run startup check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DryRunReport {
    /// Role that would be used at startup.
    pub role: NodeRole,
    /// Deployment profile that would be used at startup.
    pub profile: DeploymentProfile,
    /// Listener address that was validated but not bound.
    pub listener: SocketAddr,
    /// Storage backend that was validated but not opened.
    pub storage: StorageConfig,
    /// Core-facing plan derived from server config and Jazz schema vocabulary.
    pub runtime_plan: ServerRuntimePlan,
    /// Initial health snapshot for the shell.
    pub health: HealthSnapshot,
    /// Initial metrics snapshot for the shell.
    pub metrics: MetricsSnapshot,
}

/// Core-facing runtime requirements produced without opening storage or sockets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerRuntimePlan {
    /// Core role selected for the future Jazz node runtime.
    pub core_role: NodeRole,
    /// Deployment profile selected for validation and packaging.
    pub profile: DeploymentProfile,
    /// Storage backend kind selected for the future core runtime.
    pub storage_kind: StorageKind,
    /// Number of schema-derived column families required by storage.
    pub schema_column_family_count: usize,
}

/// Placeholder operational shell around a future jazz node runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerShell {
    config: ServerConfig,
    drain_state: DrainState,
}

impl ServerShell {
    /// Create a shell after validating its configuration.
    pub fn new(config: ServerConfig) -> Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            config,
            drain_state: DrainState::Running,
        })
    }

    /// Borrow the validated shell configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Return the current shutdown/drain state.
    pub fn drain_state(&self) -> DrainState {
        self.drain_state
    }

    /// Validate the shell configuration without constructing a shell.
    pub fn validate_config(config: &ServerConfig) -> Result<()> {
        validate_config(config)
    }

    /// Validate shell configuration plus schema-facing storage requirements.
    ///
    /// This checks only names and paths; it does not open storage.
    pub fn validate_config_for_schema(config: &ServerConfig, schema: &JazzSchema) -> Result<()> {
        validate_config_for_schema(config, schema)
    }

    /// Validate startup policy and return the report a real start would expose.
    ///
    /// This method deliberately does not open storage, bind sockets, or start a
    /// runtime.
    pub fn start_dry_run(&self) -> Result<DryRunReport> {
        validate_config(&self.config)?;
        Ok(self.dry_run_report(0))
    }

    /// Validate startup policy against a Jazz schema and return a dry-run plan.
    ///
    /// This method deliberately does not open storage, bind sockets, or start a
    /// runtime.
    pub fn start_dry_run_for_schema(&self, schema: &JazzSchema) -> Result<DryRunReport> {
        validate_config_for_schema(&self.config, schema)?;
        Ok(self.dry_run_report(required_column_families(schema).len()))
    }

    fn dry_run_report(&self, schema_column_family_count: usize) -> DryRunReport {
        DryRunReport {
            role: self.config.role,
            profile: self.config.profile,
            listener: self.config.listener.bind_addr,
            storage: self.config.storage.clone(),
            runtime_plan: ServerRuntimePlan {
                core_role: self.config.role,
                profile: self.config.profile,
                storage_kind: self.config.storage.kind(),
                schema_column_family_count,
            },
            health: HealthSnapshot {
                role: self.config.role,
                profile: self.config.profile,
                drain_state: self.drain_state,
                ..HealthSnapshot::default()
            },
            metrics: MetricsSnapshot::default(),
        }
    }
}

/// Validate a server shell configuration.
pub fn validate_config(config: &ServerConfig) -> Result<()> {
    if config.node_name.trim().is_empty() {
        return Err(ConfigError::EmptyNodeName);
    }
    validate_path("websocket_path", &config.listener.websocket_path)?;
    validate_path("health_path", &config.listener.health_path)?;
    validate_path("metrics_path", &config.listener.metrics_path)?;
    if config.listener.max_frame_bytes == 0 {
        return Err(ConfigError::ZeroLimit("max_frame_bytes"));
    }
    if config.listener.max_connections == 0 {
        return Err(ConfigError::ZeroLimit("max_connections"));
    }
    if matches!(config.profile, DeploymentProfile::Production) && !config.storage.is_durable() {
        return Err(ConfigError::ProductionRequiresDurableStorage);
    }
    if matches!(config.role, NodeRole::Core | NodeRole::Edge)
        && matches!(config.profile, DeploymentProfile::Production)
        && !config.drain_on_shutdown
    {
        return Err(ConfigError::ProductionRequiresDrain);
    }
    Ok(())
}

/// Return RocksDB column families required by a Jazz schema.
///
/// This is a pure schema-lowering helper and does not open storage.
pub fn required_column_families(schema: &JazzSchema) -> Vec<String> {
    schema.column_families()
}

/// Validate a shell configuration against schema-facing storage requirements.
///
/// This does not instantiate `NodeState`, `Db`, sockets, or storage.
pub fn validate_config_for_schema(config: &ServerConfig, schema: &JazzSchema) -> Result<()> {
    validate_config(config)?;
    let column_families = required_column_families(schema);
    validate_schema_column_families(&column_families)?;
    if let StorageConfig::RocksDb { path } = &config.storage
        && path.as_os_str().is_empty()
    {
        return Err(ConfigError::MissingStoragePath {
            storage_kind: StorageKind::RocksDb,
        });
    }
    Ok(())
}

fn validate_schema_column_families(column_families: &[String]) -> Result<()> {
    if column_families.is_empty() {
        return Err(ConfigError::MissingSchemaColumnFamilies);
    }
    if let Some(column_family) = column_families
        .iter()
        .find(|column_family| column_family.trim().is_empty())
    {
        return Err(ConfigError::InvalidSchemaColumnFamily {
            value: column_family.clone(),
        });
    }
    Ok(())
}

fn validate_path(field: &'static str, path: &str) -> Result<()> {
    if !path.starts_with('/') || path.len() == 1 || path.contains(char::is_whitespace) {
        return Err(ConfigError::InvalidPath {
            field,
            value: path.to_owned(),
        });
    }
    Ok(())
}

/// Configuration validation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// Node name was empty or all whitespace.
    EmptyNodeName,
    /// A numeric limit was zero.
    ZeroLimit(&'static str),
    /// Listener path was not an absolute, non-root path without whitespace.
    InvalidPath {
        /// Name of the invalid config field.
        field: &'static str,
        /// Invalid value.
        value: String,
    },
    /// Production profile requires durable storage.
    ProductionRequiresDurableStorage,
    /// Production core and edge roles require coordinated drain.
    ProductionRequiresDrain,
    /// Durable storage config did not name a usable path.
    MissingStoragePath {
        /// Storage backend kind whose path was missing.
        storage_kind: StorageKind,
    },
    /// Schema lowering did not produce any column families.
    MissingSchemaColumnFamilies,
    /// Schema lowering produced an invalid column-family name.
    InvalidSchemaColumnFamily {
        /// Invalid column-family name.
        value: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyNodeName => write!(f, "node_name must not be empty"),
            Self::ZeroLimit(field) => write!(f, "{field} must be greater than zero"),
            Self::InvalidPath { field, value } => {
                write!(f, "{field} must be an absolute non-root path: {value:?}")
            }
            Self::ProductionRequiresDurableStorage => {
                write!(f, "production profile requires durable storage")
            }
            Self::ProductionRequiresDrain => {
                write!(
                    f,
                    "production core and edge roles require drain_on_shutdown"
                )
            }
            Self::MissingStoragePath { storage_kind } => {
                write!(f, "{storage_kind:?} storage path must not be empty")
            }
            Self::MissingSchemaColumnFamilies => {
                write!(f, "schema must provide at least one column family")
            }
            Self::InvalidSchemaColumnFamily { value } => {
                write!(f, "schema column-family name must not be empty: {value:?}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::groove::schema::ColumnType;
    use jazz::schema::{ColumnSchema, TableSchema};

    fn simple_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )])
    }

    #[test]
    fn validates_local_default_config() {
        let config = ServerConfig::local("dev-core");

        ServerShell::validate_config(&config).unwrap();
        let shell = ServerShell::new(config).unwrap();
        let report = shell.start_dry_run().unwrap();

        assert_eq!(report.role, NodeRole::Core);
        assert_eq!(report.profile, DeploymentProfile::Local);
        assert_eq!(report.listener, SocketAddr::from(([127, 0, 0, 1], 0)));
        assert_eq!(report.metrics, MetricsSnapshot::default());
        assert_eq!(report.health.status, HealthStatus::Ready);
        assert_eq!(
            report.runtime_plan,
            ServerRuntimePlan {
                core_role: NodeRole::Core,
                profile: DeploymentProfile::Local,
                storage_kind: StorageKind::InMemory,
                schema_column_family_count: 0,
            }
        );
    }

    #[test]
    fn rejects_empty_node_name() {
        let mut config = ServerConfig::local(" ");

        let err = validate_config(&config).unwrap_err();
        assert_eq!(err, ConfigError::EmptyNodeName);

        config.node_name = "node-a".to_owned();
        validate_config(&config).unwrap();
    }

    #[test]
    fn rejects_invalid_listener_paths() {
        let mut config = ServerConfig::local("dev-core");
        config.listener.websocket_path = "sync".to_owned();

        let err = validate_config(&config).unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidPath {
                field: "websocket_path",
                value: "sync".to_owned()
            }
        );
    }

    #[test]
    fn rejects_zero_limits() {
        let mut config = ServerConfig::local("dev-core");
        config.listener.max_connections = 0;

        let err = validate_config(&config).unwrap_err();
        assert_eq!(err, ConfigError::ZeroLimit("max_connections"));
    }

    #[test]
    fn production_requires_durable_storage_and_drain() {
        let mut config = ServerConfig::local("prod-core");
        config.profile = DeploymentProfile::Production;

        let err = validate_config(&config).unwrap_err();
        assert_eq!(err, ConfigError::ProductionRequiresDurableStorage);

        config.storage = StorageConfig::RocksDb {
            path: PathBuf::from("/var/lib/jazz/core"),
        };
        config.drain_on_shutdown = false;
        let err = validate_config(&config).unwrap_err();
        assert_eq!(err, ConfigError::ProductionRequiresDrain);

        config.drain_on_shutdown = true;
        validate_config(&config).unwrap();
    }

    #[test]
    fn health_snapshot_defaults_are_ready_core_local() {
        let health = HealthSnapshot::default();

        assert_eq!(health.status, HealthStatus::Ready);
        assert_eq!(health.role, NodeRole::Core);
        assert_eq!(health.profile, DeploymentProfile::Local);
        assert_eq!(health.drain_state, DrainState::Running);
        assert_eq!(health.message, "ready");
        assert_eq!(health.observed_at, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn exposes_required_column_families_for_jazz_schema() {
        let column_families = required_column_families(&simple_schema());

        assert!(
            column_families
                .iter()
                .any(|name| name == "__groove_class_meta")
        );
        assert!(
            column_families
                .iter()
                .any(|name| name == "__groove_class_history")
        );
        assert!(
            column_families
                .iter()
                .any(|name| name == "__groove_class_register")
        );
        assert!(
            !column_families
                .iter()
                .any(|name| name == "jazz_todos_history")
        );
        assert!(
            column_families
                .iter()
                .any(|name| name == "__groove_class_indices")
        );
    }

    #[test]
    fn schema_dry_run_maps_server_config_to_runtime_plan() {
        let schema = simple_schema();
        let mut config = ServerConfig::local("edge-a");
        config.role = NodeRole::Edge;
        config.storage = StorageConfig::RocksDb {
            path: PathBuf::from("/var/lib/jazz/edge-a"),
        };
        let expected_column_family_count = required_column_families(&schema).len();

        ServerShell::validate_config_for_schema(&config, &schema).unwrap();
        let report = ServerShell::new(config)
            .unwrap()
            .start_dry_run_for_schema(&schema)
            .unwrap();

        assert_eq!(
            report.runtime_plan,
            ServerRuntimePlan {
                core_role: NodeRole::Edge,
                profile: DeploymentProfile::Local,
                storage_kind: StorageKind::RocksDb,
                schema_column_family_count: expected_column_family_count,
            }
        );
        assert_eq!(report.storage.kind(), StorageKind::RocksDb);
    }

    #[test]
    fn rocksdb_schema_validation_requires_path() {
        let mut config = ServerConfig::local("core-a");
        config.storage = StorageConfig::RocksDb {
            path: PathBuf::new(),
        };

        let err = validate_config_for_schema(&config, &simple_schema()).unwrap_err();

        assert_eq!(
            err,
            ConfigError::MissingStoragePath {
                storage_kind: StorageKind::RocksDb
            }
        );
    }
}
