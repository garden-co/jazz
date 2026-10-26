//! The server's semantic runtime: one SQLite [`Db`] owned by a dedicated
//! thread, driven by commands from request and WebSocket tasks.
//!
//! The engine is single-threaded (`Rc`/`RefCell`), so every engine call runs
//! on the owner thread. Handles are `Send` and cheap to clone. Each
//! WebSocket session is one subscriber of the engine; its inbound frames are
//! queued for the owner and its outbound messages stream back in order.
//! Shutdown joins the owner, so the store is closed when it returns.

pub mod auth_admission;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc as std_mpsc};
use std::thread;
use std::time::Duration;

use crate::kv::SqliteKv;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, LensEffects, SubscriberId, SyncMessage, Transport,
    TransportError, WireError, block_on,
};
use jazz::ids::{AuthorSubject, MigrationLensId, NodeUuid, SchemaVersionId};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{TableName, TablePolicies};
use jazz::wire::{WireAuthorityEndpoint, WireFeatures};
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};

/// Encoded frames exchanged with a host transport.
pub type AbiBytes = Vec<u8>;

/// Where the runtime keeps its store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageConfig {
    /// A store that lives as long as the runtime (tests, dry runs, probes).
    InMemory,
    /// One SQLite file (WAL).
    SQLite {
        /// The database file.
        path: PathBuf,
    },
}

impl StorageConfig {
    /// The durable store inside a server data directory.
    pub fn data_dir(path: impl Into<PathBuf>) -> Self {
        Self::SQLite {
            path: path.into().join("jazz.sqlite"),
        }
    }
}

/// Deployment role of a server node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeRole {
    /// Stateless relay in front of a core. Not supported yet on SQLite.
    Relay,
    /// Durable core node that owns authoritative storage.
    Core,
}

/// How much the server trusts a link's commits, chosen at admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitUnitTrust {
    /// An end-user session: every read and write is checked against the
    /// table policies for its identity.
    Session,
    /// An authenticated backend. It is served with full access.
    TrustedBackend,
    /// An authenticated administrative or authority link, served with full
    /// access.
    TrustedAuthority,
}

/// Authority endpoints and features negotiated for one connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSessionContext {
    /// This server's endpoint for the connection.
    pub local: WireAuthorityEndpoint,
    /// The peer's endpoint, when it advertised one.
    pub remote: Option<WireAuthorityEndpoint>,
    /// The admitted identity of the link.
    pub link_identity: AuthorSubject,
    /// The negotiated wire features.
    pub negotiated_features: WireFeatures,
}

/// Capability selected by the server after authenticating a WebSocket and
/// negotiating its features.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ServerLinkAdmission {
    /// A normal session or backend link.
    #[default]
    OrdinarySession,
    /// Host-verified SYSTEM authority credential permitting scoped query
    /// delegation. On SQLite it is served as a trusted SYSTEM link; the sync
    /// protocol cannot express a delegated scope yet.
    AuthorityQueryDelegate,
    /// A durable relay for exactly the one session authenticated by this
    /// connection. On SQLite it is served as that session.
    ScopeIsolatedClientRelay {
        /// Fresh server-issued capability epoch for this accepted connection.
        admission_epoch: u64,
    },
}

/// Opaque handle for one admitted session.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServerSession(u64);

/// Bytes a session may have queued in each direction. Inbound frames past
/// it are refused (the peer ignored flow control); outbound messages past it
/// wait in the engine until the socket catches up.
const SESSION_QUEUE_BUDGET: usize = 8 * 1024 * 1024;
/// How often the owner ticks with no traffic, for time-based engine work
/// such as expiring staged large-value chunks.
const IDLE_TICK: Duration = Duration::from_secs(1);

/// Inbound messages one session may have applied per engine tick.
const INBOUND_MESSAGES_PER_TICK: usize = 16;

/// The runtime catalogue's key in its sidecar store.
const CATALOGUE_KEY: &[u8] = b"runtime-catalogue:v1";

type Job = Box<dyn FnOnce(&mut Owner) + Send>;
/// One activation: a schema and the migration lenses into it (its new
/// columns' defaults, and the tables and columns it renames).
type Step = (JazzSchema, Vec<MigrationLens>);

/// Sendable handle for the thread that owns the runtime's store.
#[derive(Clone)]
pub struct ServerRuntimeHandle {
    inner: Arc<Inner>,
}

struct Inner {
    jobs: Mutex<Option<std_mpsc::Sender<Job>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    activity: watch::Sender<u64>,
    next_session: AtomicU64,
    edges: Mutex<HashMap<ServerSession, Edge>>,
    tick_queued: Arc<AtomicBool>,
}

/// The socket side of one session.
struct Edge {
    /// Taken by [`ServerRuntimeHandle::open_wire_stream`].
    output: Option<tokio_mpsc::UnboundedReceiver<Outbound>>,
    ingress: Arc<AtomicUsize>,
    egress: Arc<AtomicUsize>,
}

type Outbound = (Result<Vec<AbiBytes>, String>, usize);

/// Activity subscription for a server runtime.
pub struct ServerRuntimeActivity {
    receiver: watch::Receiver<u64>,
}

impl ServerRuntimeActivity {
    /// Wait until something asked sessions to look for new output.
    pub async fn changed(&mut self) -> Result<(), String> {
        self.receiver
            .changed()
            .await
            .map_err(|_| "server runtime activity channel closed".to_owned())
    }
}

/// One session's ordered outbound frame batches.
pub struct ServerRuntimeFrameStream {
    receiver: tokio_mpsc::UnboundedReceiver<Outbound>,
    egress: Arc<AtomicUsize>,
    runtime: Weak<Inner>,
}

impl ServerRuntimeFrameStream {
    /// Receive the next batch of outbound frames.
    pub async fn recv(&mut self) -> Option<Result<Vec<AbiBytes>, String>> {
        let (result, bytes) = self.receiver.recv().await?;
        let before = self.egress.fetch_sub(bytes, Ordering::AcqRel);
        if before >= SESSION_QUEUE_BUDGET
            && before - bytes < SESSION_QUEUE_BUDGET
            && let Some(inner) = self.runtime.upgrade()
        {
            // Output the owner held back for this session can move again.
            let _ = ServerRuntimeHandle { inner }.queue_tick();
        }
        Some(result)
    }
}

impl ServerRuntimeHandle {
    /// Start a core runtime over `storage`.
    pub fn start_with_storage(schema: JazzSchema, storage: StorageConfig) -> Result<Self, String> {
        Self::start_with_storage_config(schema, storage, NodeRole::Core)
    }

    /// Start a runtime with an explicit role.
    pub fn start_with_storage_config(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
    ) -> Result<Self, String> {
        Self::start(schema, storage, role, false, false)
    }

    /// Start a core runtime for a catalogue-driven server whose permissions
    /// head is not published yet: until the first schema activation,
    /// session reads wait and session writes are rejected.
    pub fn start_awaiting_permissions(
        schema: JazzSchema,
        storage: StorageConfig,
    ) -> Result<Self, String> {
        Self::start(schema, storage, NodeRole::Core, false, true)
    }

    fn start(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
    ) -> Result<Self, String> {
        if role == NodeRole::Relay {
            return Err("relay nodes are not supported yet on SQLite".to_owned());
        }
        let (jobs, receiver) = std_mpsc::channel::<Job>();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (activity, _) = watch::channel(0_u64);
        let tick_queued = Arc::new(AtomicBool::new(false));
        let owner_ticks = Arc::clone(&tick_queued);
        let join = thread::Builder::new()
            .name("jazz-server-runtime".to_owned())
            .spawn(move || {
                let owner =
                    match Owner::open(schema, &storage, any_stored_catalogue, awaiting_permissions)
                    {
                        Ok(owner) => {
                            let _ = started_tx.send(Ok(()));
                            owner
                        }
                        Err(error) => {
                            let _ = started_tx.send(Err(error));
                            return;
                        }
                    };
                owner.run(receiver, &owner_ticks);
            })
            .map_err(|error| format!("failed to spawn server runtime thread: {error}"))?;
        started_rx
            .recv()
            .map_err(|_| "server runtime thread exited before startup".to_owned())??;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                activity,
                next_session: AtomicU64::new(1),
                edges: Mutex::new(HashMap::new()),
                tick_queued,
            }),
        })
    }

    /// Start a runtime whose schema comes from the administrative catalogue.
    /// A reopened store keeps everything earlier schemas added; the caller
    /// then re-applies its durable selection.
    pub fn start_with_catalogue_schema(
        schema: JazzSchema,
        storage: StorageConfig,
        role: NodeRole,
    ) -> Result<Self, String> {
        Self::start(schema, storage, role, true, true)
    }

    /// Subscribe to activity notices.
    pub fn subscribe_activity(&self) -> ServerRuntimeActivity {
        ServerRuntimeActivity {
            receiver: self.inner.activity.subscribe(),
        }
    }

    /// Wake every session to look for new output.
    pub fn notify_activity(&self) {
        self.inner.activity.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// Admit an authenticated session.
    pub async fn open_with_session_context(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
        _negotiated_features: WireFeatures,
        _session_context: Option<ConnectionSessionContext>,
        link_admission: ServerLinkAdmission,
    ) -> Result<ServerSession, String> {
        // The SQLite sync protocol has no per-query delegated scope yet, so
        // an authority link is served as the trusted SYSTEM link it is. A
        // client relay (a browser worker relaying for its tabs) is served as
        // the one session it authenticated as: everything it relays is read
        // and written under that session's policies, so it can never reach
        // past that scope, and its tabs share it.
        match link_admission {
            ServerLinkAdmission::OrdinarySession
            | ServerLinkAdmission::AuthorityQueryDelegate
            | ServerLinkAdmission::ScopeIsolatedClientRelay { .. } => {}
        }
        // Trusted links are served with full access; a session reads and
        // writes under its identity's policies.
        let served_as = match trust {
            CommitUnitTrust::Session => identity,
            CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority => {
                AuthorSubject::SYSTEM
            }
        };
        let session = ServerSession(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let (output, receiver) = tokio_mpsc::unbounded_channel();
        let egress = Arc::new(AtomicUsize::new(0));
        self.edges()?.insert(
            session,
            Edge {
                output: Some(receiver),
                ingress: Arc::new(AtomicUsize::new(0)),
                egress: Arc::clone(&egress),
            },
        );
        let opened = self
            .run(move |owner| {
                owner.open_session(session, served_as, claims, output, egress);
                Ok(())
            })
            .await;
        if opened.is_err() {
            self.edges()?.remove(&session);
        }
        opened.map(|()| session)
    }

    /// Take the session's ordered output stream. Output produced before
    /// this call is kept.
    pub fn open_wire_stream(
        &self,
        session: ServerSession,
    ) -> Result<ServerRuntimeFrameStream, String> {
        let mut edges = self.edges()?;
        let edge = edges
            .get_mut(&session)
            .ok_or_else(|| "unknown server session".to_owned())?;
        let receiver = edge
            .output
            .take()
            .ok_or_else(|| "session already has a wire stream".to_owned())?;
        Ok(ServerRuntimeFrameStream {
            receiver,
            egress: Arc::clone(&edge.egress),
            runtime: Arc::downgrade(&self.inner),
        })
    }

    /// Queue inbound frames (each one `postcard(SyncMessage)`) for the
    /// engine. Refused when the session already has its budget queued.
    pub fn receive_wire_frames(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<(), String> {
        if frames.is_empty() {
            return Ok(());
        }
        let bytes = frames.iter().map(Vec::len).sum::<usize>();
        let ingress = self
            .edges()?
            .get(&session)
            .map(|edge| Arc::clone(&edge.ingress))
            .ok_or_else(|| "unknown server session".to_owned())?;
        if ingress.fetch_add(bytes, Ordering::AcqRel) + bytes > SESSION_QUEUE_BUDGET {
            ingress.fetch_sub(bytes, Ordering::AcqRel);
            return Err("session ingress budget exceeded".to_owned());
        }
        self.send(Box::new(move |owner| {
            owner.receive(session, frames);
            ingress.fetch_sub(bytes, Ordering::AcqRel);
        }))
    }

    /// Ask the owner for a tick; requests coalesce. Output reaches every
    /// session's stream after each tick.
    pub fn request_wire_tick(&self, _session: ServerSession) -> Result<(), String> {
        self.queue_tick()
    }

    fn queue_tick(&self) -> Result<(), String> {
        if self.inner.tick_queued.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.send(Box::new(|_| {}))
    }

    /// Close a session.
    pub fn close(&self, session: ServerSession) {
        if let Ok(mut edges) = self.edges() {
            edges.remove(&session);
        }
        let _ = self.send(Box::new(move |owner| owner.close_session(session)));
    }

    /// Stop and join the owner thread; the store is closed when this returns.
    /// Safe to call more than once.
    pub async fn shutdown(&self) -> Result<(), String> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            inner
                .jobs
                .lock()
                .map_err(|_| "server runtime jobs lock poisoned".to_owned())?
                .take();
            let join = inner
                .join
                .lock()
                .map_err(|_| "server runtime join lock poisoned".to_owned())?
                .take();
            if let Some(join) = join {
                join.join()
                    .map_err(|_| "server runtime thread panicked".to_owned())?;
            }
            Ok(())
        })
        .await
        .map_err(|error| format!("server runtime shutdown task failed: {error}"))?
    }

    // ---- catalogue ---------------------------------------------------------

    /// Admit `schema` with its lens from an already admitted schema.
    pub async fn publish_schema_with_lens(
        &self,
        schema: JazzSchema,
        lens: MigrationLens,
        new_tables: Vec<String>,
        dropped_tables: Vec<String>,
    ) -> Result<SchemaVersionId, String> {
        self.run(move |owner| {
            let id = owner
                .catalogue
                .publish(schema.clone(), lens, new_tables, dropped_tables)?;
            // Clients on the published version read and write by it before
            // (and without) it becoming current.
            block_on(
                owner
                    .db
                    .admit_schema_with_lenses(schema, &owner.catalogue.lenses_for(id)),
            )
            .map_err(|error| error.to_string())?;
            owner.catalogue.persist()?;
            Ok(id)
        })
        .await
    }

    /// Whether `schema` is admitted.
    pub async fn runtime_catalogue_contains_schema(
        &self,
        schema: SchemaVersionId,
    ) -> Result<bool, String> {
        self.run(move |owner| Ok(owner.catalogue.schemas.contains_key(&schema)))
            .await
    }

    /// Whether `schema` and `lens` are admitted.
    #[doc(hidden)]
    pub async fn runtime_catalogue_contains(
        &self,
        schema: SchemaVersionId,
        lens: MigrationLensId,
    ) -> Result<(bool, bool), String> {
        self.run(move |owner| {
            Ok((
                owner.catalogue.schemas.contains_key(&schema),
                owner.catalogue.lenses.contains(&lens),
            ))
        })
        .await
    }

    /// Check that activating `schema` would succeed, without changing the
    /// store: it is replayed on a scratch store after everything activated
    /// so far.
    #[doc(hidden)]
    pub async fn validate_schema_activation(
        &self,
        revision: u64,
        schema: JazzSchema,
    ) -> Result<(), String> {
        self.run(move |owner| {
            owner.check_revision(revision)?;
            let mut steps = owner.catalogue.activated.clone();
            steps.push(schema);
            owner.catalogue.replay(&steps)
        })
        .await
    }

    /// Make `schema` current with `permissions` at catalogue `revision`.
    pub async fn activate_schema(
        &self,
        revision: u64,
        schema: JazzSchema,
        permissions: HashMap<TableName, TablePolicies>,
    ) -> Result<SchemaVersionId, String> {
        let result = self
            .run(move |owner| owner.activate(revision, schema, &permissions))
            .await;
        if result.is_ok() {
            self.notify_activity();
        }
        result
    }

    /// The installed deployment catalogue.
    pub async fn trusted_catalogue_snapshot(&self) -> Result<CatalogueSnapshot, String> {
        self.run(|owner| Ok(owner.catalogue.snapshot.clone())).await
    }

    #[doc(hidden)]
    pub async fn trusted_catalogue_snapshot_for_test(&self) -> Result<CatalogueSnapshot, String> {
        self.trusted_catalogue_snapshot().await
    }

    /// Check a deployment by replaying it on a scratch store.
    #[doc(hidden)]
    pub async fn validate_deployment_snapshot(
        &self,
        snapshot: CatalogueSnapshot,
    ) -> Result<(), String> {
        self.run(move |owner| {
            let steps = deployment_steps(&snapshot)?;
            owner.catalogue.replay_with_lenses(&steps)
        })
        .await
    }

    /// Install a complete deployment: every lineage step not yet activated,
    /// then its current write schema.
    #[doc(hidden)]
    pub async fn apply_deployment_snapshot(
        &self,
        snapshot: CatalogueSnapshot,
    ) -> Result<(), String> {
        let result = self
            .run(move |owner| {
                owner.apply_snapshot(snapshot)?;
                owner.release_held_reads();
                Ok(())
            })
            .await;
        if result.is_ok() {
            self.notify_activity();
        }
        result
    }

    fn edges(&self) -> Result<std::sync::MutexGuard<'_, HashMap<ServerSession, Edge>>, String> {
        self.inner
            .edges
            .lock()
            .map_err(|_| "server runtime session lock poisoned".to_owned())
    }

    fn send(&self, job: Job) -> Result<(), String> {
        self.inner
            .jobs
            .lock()
            .map_err(|_| "server runtime jobs lock poisoned".to_owned())?
            .as_ref()
            .ok_or_else(|| "server runtime is shut down".to_owned())?
            .send(job)
            .map_err(|_| "server runtime thread is not running".to_owned())
    }

    async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Owner) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let (reply, response) = oneshot::channel();
        self.send(Box::new(move |owner| {
            let _ = reply.send(job(owner));
        }))?;
        response
            .await
            .map_err(|_| "server runtime thread dropped the response".to_owned())?
    }
}

/// A deployment as ordered activation steps: each lineage schema with the
/// incoming lenses, then the write schema.
fn deployment_steps(snapshot: &CatalogueSnapshot) -> Result<Vec<Step>, String> {
    let genesis = snapshot
        .schemas
        .iter()
        .find(|schema| {
            !snapshot
                .lineages
                .iter()
                .any(|(_, publication)| publication.schema.id == schema.id)
        })
        .ok_or("deployment has no genesis schema")?;
    let mut steps = vec![(genesis.schema.clone(), Vec::new())];
    let mut lenses = Vec::new();
    let mut lineages = snapshot.lineages.iter().collect::<Vec<_>>();
    lineages.sort_by_key(|(seq, _)| *seq);
    for (_, publication) in lineages {
        for predecessor in &publication.predecessors {
            LensEffects::of(&predecessor.lens).map_err(|e| e.to_string())?;
            lenses.push(predecessor.lens.clone());
        }
        steps.push((publication.schema.schema.clone(), lenses.clone()));
    }
    let current = snapshot
        .schemas
        .iter()
        .find(|schema| schema.id == snapshot.current_write_schema.schema)
        .ok_or("deployment write schema is missing from its schemas")?;
    steps.push((current.schema.clone(), lenses));
    Ok(steps)
}

// ---- owner thread -------------------------------------------------------------

struct Owner {
    db: Db,
    sessions: HashMap<ServerSession, OwnerSession>,
    catalogue: Catalogue,
    /// No permissions head is published yet: session reads wait in
    /// `OwnerSession::held` and session commits are rejected.
    awaiting_permissions: bool,
}

struct OwnerSession {
    subscriber: SubscriberId,
    /// A session under its own identity (not a trusted backend or
    /// authority link).
    untrusted: bool,
    /// Reads held until the permissions head is published, in order.
    held: VecDeque<SyncMessage>,
    queues: Rc<SessionQueues>,
    output: tokio_mpsc::UnboundedSender<Outbound>,
    egress: Arc<AtomicUsize>,
}

#[derive(Default)]
struct SessionQueues {
    inbound: RefCell<VecDeque<SyncMessage>>,
    outbound: RefCell<VecDeque<SyncMessage>>,
    /// Inbound messages the engine may still take this tick.
    budget: std::cell::Cell<usize>,
}

/// The engine's side of one session.
struct SessionTransport(Rc<SessionQueues>);

impl Transport for SessionTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.0.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let budget = self.0.budget.get();
        if budget == 0 {
            return None;
        }
        let message = self.0.inbound.borrow_mut().pop_front()?;
        self.0.budget.set(budget - 1);
        Some(message)
    }
}

/// What the runtime admitted, in the order it was activated.
struct Catalogue {
    schemas: BTreeMap<SchemaVersionId, JazzSchema>,
    lenses: BTreeSet<MigrationLensId>,
    /// Every admitted lens, keyed by target schema.
    incoming: BTreeMap<SchemaVersionId, Vec<MigrationLens>>,
    activated: Vec<JazzSchema>,
    snapshot: CatalogueSnapshot,
    /// Where `snapshot` is kept between runs, beside a file store.
    store: Option<SqliteKv>,
}

impl Catalogue {
    fn new(genesis: &JazzSchema) -> Self {
        let version = SchemaVersion::new(genesis.clone());
        Self {
            schemas: BTreeMap::from([(version.id, genesis.clone())]),
            lenses: BTreeSet::new(),
            incoming: BTreeMap::new(),
            activated: vec![genesis.clone()],
            store: None,
            snapshot: CatalogueSnapshot {
                genesis_physical_identities: PhysicalIdentityManifest::allocate(genesis),
                schemas: vec![version.clone()],
                lineages: Vec::new(),
                current_write_schema: CurrentWriteSchema {
                    revision: 0,
                    schema: version.id,
                },
            },
        }
    }

    fn publish(
        &mut self,
        schema: JazzSchema,
        lens: MigrationLens,
        new_tables: Vec<String>,
        dropped_tables: Vec<String>,
    ) -> Result<SchemaVersionId, String> {
        LensEffects::of(&lens).map_err(|error| error.to_string())?;
        let Some(source) = self.schemas.get(&lens.source).cloned() else {
            return Err("lens source schema is not admitted".to_owned());
        };
        let id = schema.version_id();
        if !self.snapshot.schemas.iter().any(|version| version.id == id) {
            let publication = SchemaLineagePublication::author_from_prior_with_history(
                &source,
                &self.identities(lens.source)?,
                self.history(),
                SchemaVersion::new(schema.clone()),
                lens.clone(),
                new_tables,
                dropped_tables,
            )
            .map_err(str::to_owned)?;
            let sequence = self
                .snapshot
                .lineages
                .iter()
                .map(|(seq, _)| *seq)
                .max()
                .unwrap_or(0)
                + 1;
            self.snapshot.lineages.push((sequence, publication));
            self.snapshot
                .schemas
                .push(SchemaVersion::new(schema.clone()));
        }
        self.admit_lens(id, lens.clone());
        self.lenses.insert(lens.id);
        self.schemas.entry(id).or_insert(schema);
        Ok(id)
    }

    /// The physical identities `schema` was published with.
    fn identities(&self, schema: SchemaVersionId) -> Result<PhysicalIdentityManifest, String> {
        if let Some((_, publication)) = self
            .snapshot
            .lineages
            .iter()
            .find(|(_, publication)| publication.schema.id == schema)
        {
            return Ok(publication.physical_identities.clone());
        }
        if self.snapshot.schemas.first().map(|version| version.id) == Some(schema) {
            return Ok(self.snapshot.genesis_physical_identities.clone());
        }
        Err("lens source schema has no published identities".to_owned())
    }

    fn history(&self) -> Vec<PhysicalIdentityManifest> {
        std::iter::once(self.snapshot.genesis_physical_identities.clone())
            .chain(
                self.snapshot
                    .lineages
                    .iter()
                    .map(|(_, publication)| publication.physical_identities.clone()),
            )
            .collect()
    }

    /// Keep the snapshot for the next run of a file store.
    fn persist(&self) -> Result<(), String> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let bytes = postcard::to_allocvec(&self.snapshot)
            .map_err(|error| format!("encode runtime catalogue: {error}"))?;
        store.write_many(&[(CATALOGUE_KEY.to_vec(), bytes)])
    }

    fn admit_lens(&mut self, target: SchemaVersionId, lens: MigrationLens) {
        let incoming = self.incoming.entry(target).or_default();
        if !incoming.iter().any(|admitted| admitted.id == lens.id) {
            incoming.push(lens);
        }
    }

    fn lenses_for(&self, schema: SchemaVersionId) -> Vec<MigrationLens> {
        self.incoming.get(&schema).cloned().unwrap_or_default()
    }

    fn replay(&self, steps: &[JazzSchema]) -> Result<(), String> {
        let steps = steps
            .iter()
            .map(|schema| (schema.clone(), self.lenses_for(schema.version_id())))
            .collect::<Vec<_>>();
        self.replay_with_lenses(&steps)
    }

    /// Activate `steps` in order on an empty scratch store.
    fn replay_with_lenses(&self, steps: &[Step]) -> Result<(), String> {
        let Some(((first, _), rest)) = steps.split_first() else {
            return Ok(());
        };
        let scratch = block_on(Db::open(config(first.clone()))).map_err(|e| e.to_string())?;
        for (schema, lenses) in rest {
            block_on(scratch.activate_schema_with_lenses(schema.clone(), lenses))
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn config(schema: JazzSchema) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// `schema` with each table's policies replaced by `permissions` (a table
/// without an entry denies everything).
fn with_permissions(
    schema: &JazzSchema,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Result<JazzSchema, String> {
    let mut source = schema.public_schema().clone();
    if permissions.keys().any(|name| !source.contains_key(name)) {
        return Err("schema permissions reference an unknown table".to_owned());
    }
    for (name, table) in source.iter_mut() {
        table.policies = permissions.get(name).cloned().unwrap_or_default();
    }
    JazzSchema::new(&source).map_err(|error| format!("invalid schema permissions: {error}"))
}

impl Owner {
    /// Open the store. A file store also reopens the catalogue it admitted
    /// before: its genesis schema opens the store and every later schema is
    /// activated again (each a no-op on the stored tables), so the catalogue
    /// and its lineage survive restarts. `schema` starts a fresh catalogue
    /// only when the store has none, or (unless `any_stored_catalogue`) none
    /// that admits it.
    fn open(
        schema: JazzSchema,
        storage: &StorageConfig,
        any_stored_catalogue: bool,
        awaiting_permissions: bool,
    ) -> Result<Self, String> {
        let (db, store, stored) = match storage {
            StorageConfig::InMemory => (block_on(Db::open(config(schema.clone()))), None, None),
            StorageConfig::SQLite { path } => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|error| format!("create {}: {error}", parent.display()))?;
                }
                let store = SqliteKv::open(&path.with_extension("catalogue.sqlite"))?;
                let stored = match store.get(CATALOGUE_KEY)? {
                    Some(bytes) => Some(
                        postcard::from_bytes::<CatalogueSnapshot>(&bytes)
                            .map_err(|error| format!("decode runtime catalogue: {error}"))?,
                    ),
                    None => None,
                }
                .filter(|snapshot| {
                    any_stored_catalogue
                        || snapshot
                            .schemas
                            .iter()
                            .any(|version| version.id == schema.version_id())
                });
                let genesis = match &stored {
                    Some(snapshot) => deployment_steps(snapshot)?.remove(0).0,
                    None => schema.clone(),
                };
                let db = block_on(Db::open_at(
                    config(genesis),
                    path,
                    FileOptions {
                        synchronous_full: true,
                        ..FileOptions::default()
                    },
                ));
                (db, Some(store), stored)
            }
        };
        let db = db.map_err(|error| format!("open server store: {error}"))?;
        db.set_authority();
        let mut owner = Self {
            db,
            sessions: HashMap::new(),
            catalogue: Catalogue::new(&schema),
            awaiting_permissions,
        };
        if let Some(snapshot) = stored {
            owner.catalogue = Catalogue::new(&deployment_steps(&snapshot)?.remove(0).0);
            owner.apply_snapshot(snapshot)?;
        }
        owner.catalogue.store = store;
        owner.catalogue.persist()?;
        Ok(owner)
    }

    fn run(mut self, jobs: std_mpsc::Receiver<Job>, tick_queued: &AtomicBool) {
        loop {
            // Inbound work left over from a budgeted tick runs next, after
            // any jobs that arrived meanwhile.
            let wait = if self.inbound_pending() {
                Duration::ZERO
            } else {
                IDLE_TICK
            };
            match jobs.recv_timeout(wait) {
                Ok(job) => {
                    job(&mut self);
                    while let Ok(job) = jobs.try_recv() {
                        job(&mut self);
                    }
                }
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }
            tick_queued.store(false, Ordering::Release);
            self.tick();
        }
        for (_, session) in self.sessions.drain() {
            self.db.disconnect_subscriber(session.subscriber);
        }
    }

    fn open_session(
        &mut self,
        session: ServerSession,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        output: tokio_mpsc::UnboundedSender<Outbound>,
        egress: Arc<AtomicUsize>,
    ) {
        let claims = (identity != AuthorSubject::SYSTEM).then(|| {
            self.db.set_identity_claims(identity, claims.clone());
            claims
        });
        let queues = Rc::new(SessionQueues::default());
        let subscriber = self.db.accept_subscriber_with_claims(
            Box::new(SessionTransport(Rc::clone(&queues))),
            identity,
            claims,
        );
        self.sessions.insert(
            session,
            OwnerSession {
                subscriber,
                untrusted: identity != AuthorSubject::SYSTEM,
                held: VecDeque::new(),
                queues,
                output,
                egress,
            },
        );
    }

    fn receive(&mut self, session: ServerSession, frames: Vec<AbiBytes>) {
        let Some(owner_session) = self.sessions.get(&session) else {
            return;
        };
        let gated = self.awaiting_permissions && owner_session.untrusted;
        let Some(owner_session) = self.sessions.get_mut(&session) else {
            return;
        };
        let mut inbound = owner_session.queues.inbound.borrow_mut();
        for frame in frames {
            match jazz::db::decode_message(&frame) {
                Ok(SyncMessage::Commit { tx, .. }) if gated => {
                    owner_session
                        .queues
                        .outbound
                        .borrow_mut()
                        .push_back(SyncMessage::Fate {
                            tx,
                            through: self.db.seq_high_water(),
                            result: Err(WireError::Rejected(
                                "permissions_head_missing: no published permissions head"
                                    .to_owned(),
                            )),
                            held: Vec::new(),
                            global: true,
                        });
                }
                Ok(
                    message @ (SyncMessage::Subscribe { .. }
                    | SyncMessage::Unsubscribe { .. }
                    | SyncMessage::Fetch { .. }
                    | SyncMessage::Hold { .. }),
                ) if gated => owner_session.held.push_back(message),
                Ok(message) => inbound.push_back(message),
                Err(error) => {
                    drop(inbound);
                    let _ = owner_session
                        .output
                        .send((Err(format!("malformed sync frame: {error}")), 0));
                    self.close_session(session);
                    return;
                }
            }
        }
    }

    /// The permissions head is published: reads held meanwhile are served,
    /// ahead of anything that arrived after them.
    fn release_held_reads(&mut self) {
        self.awaiting_permissions = false;
        for session in self.sessions.values_mut() {
            let mut inbound = session.queues.inbound.borrow_mut();
            for message in std::mem::take(&mut session.held).into_iter().rev() {
                inbound.push_front(message);
            }
        }
    }

    fn close_session(&mut self, session: ServerSession) {
        if let Some(session) = self.sessions.remove(&session) {
            self.db.disconnect_subscriber(session.subscriber);
        }
    }

    fn inbound_pending(&self) -> bool {
        self.sessions
            .values()
            .any(|session| !session.queues.inbound.borrow().is_empty())
    }

    /// One engine tick, then every session's output up to its budget. Each
    /// session's inbound messages are taken a bounded number per tick, so a
    /// large batch gets its first answers out early and other sessions are
    /// served between its parts.
    fn tick(&mut self) {
        for session in self.sessions.values() {
            session.queues.budget.set(INBOUND_MESSAGES_PER_TICK);
        }
        if let Err(error) = block_on(self.db.tick()) {
            tracing::error!(%error, "server runtime tick failed");
        }
        let mut gone = Vec::new();
        for (id, session) in &self.sessions {
            let mut outbound = session.queues.outbound.borrow_mut();
            let mut frames = Vec::new();
            let mut bytes = 0;
            while session.egress.load(Ordering::Acquire) + bytes < SESSION_QUEUE_BUDGET
                && let Some(message) = outbound.pop_front()
            {
                match jazz::db::encode_frames(message) {
                    Ok(encoded) => {
                        bytes += encoded.iter().map(Vec::len).sum::<usize>();
                        frames.extend(encoded);
                    }
                    Err(error) => {
                        tracing::error!(%error, "encode outbound sync message");
                    }
                }
            }
            if frames.is_empty() {
                continue;
            }
            session.egress.fetch_add(bytes, Ordering::AcqRel);
            if session.output.send((Ok(frames), bytes)).is_err() {
                gone.push(*id);
            }
        }
        for id in gone {
            self.close_session(id);
        }
    }

    /// An activation at a revision older than the current one is stale: the
    /// runtime is ahead of whoever proposed it.
    fn check_revision(&self, revision: u64) -> Result<(), String> {
        let current = self.catalogue.snapshot.current_write_schema.revision;
        if revision < current {
            return Err(format!(
                "stale active schema revision {revision}: the runtime is at revision {current}"
            ));
        }
        Ok(())
    }

    fn activate(
        &mut self,
        revision: u64,
        schema: JazzSchema,
        permissions: &HashMap<TableName, TablePolicies>,
    ) -> Result<SchemaVersionId, String> {
        self.check_revision(revision)?;
        let id = schema.version_id();
        let lenses = self.catalogue.lenses_for(id);
        let selected = with_permissions(&schema, permissions)?;
        block_on(
            self.db
                .activate_schema_with_lenses(selected.clone(), &lenses),
        )
        .map_err(|error| error.to_string())?;
        self.catalogue
            .schemas
            .entry(id)
            .or_insert_with(|| schema.clone());
        self.catalogue.activated.push(schema);
        // The snapshot carries the write schema with its installed policies.
        let snapshot = &mut self.catalogue.snapshot;
        let selected = SchemaVersion::new(selected);
        match snapshot.schemas.iter_mut().find(|version| version.id == id) {
            Some(version) => *version = selected,
            None => snapshot.schemas.push(selected),
        }
        snapshot.current_write_schema = CurrentWriteSchema {
            revision,
            schema: id,
        };
        self.catalogue.persist()?;
        self.release_held_reads();
        Ok(id)
    }

    fn apply_snapshot(&mut self, snapshot: CatalogueSnapshot) -> Result<(), String> {
        let steps = deployment_steps(&snapshot)?;
        for (schema, lenses) in &steps {
            block_on(self.db.activate_schema_with_lenses(schema.clone(), lenses))
                .map_err(|error| error.to_string())?;
            self.catalogue
                .schemas
                .entry(schema.version_id())
                .or_insert_with(|| schema.clone());
            self.catalogue.activated.push(schema.clone());
        }
        for (_, publication) in &snapshot.lineages {
            for predecessor in &publication.predecessors {
                self.catalogue.lenses.insert(predecessor.lens.id);
                self.catalogue
                    .admit_lens(publication.schema.id, predecessor.lens.clone());
            }
        }
        // Published versions that were never made current stay readable by
        // the clients that declare them.
        let mut lineages = snapshot.lineages.iter().collect::<Vec<_>>();
        lineages.sort_by_key(|(sequence, _)| *sequence);
        for (_, publication) in lineages {
            let id = publication.schema.id;
            if self.catalogue.schemas.contains_key(&id) {
                continue;
            }
            let schema = publication.schema.schema.clone();
            block_on(
                self.db
                    .admit_schema_with_lenses(schema.clone(), &self.catalogue.lenses_for(id)),
            )
            .map_err(|error| error.to_string())?;
            self.catalogue.schemas.insert(id, schema);
        }
        self.catalogue.snapshot = snapshot;
        self.catalogue.persist()
    }
}
