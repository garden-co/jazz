//! The `Db` facade: Jazz's public database API over one SQLite connection.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::ids::{AuthorSubject, RowUuid};
use crate::protocol::{
    BranchSelector, BranchViewBase, CurrentWriteSchema, LensOp, MigrationLens, ReadViewSourceSpec,
    SchemaVersion,
};
use crate::query::Query;
use crate::records::Value;
use crate::schema::JazzSchema;
use crate::tools::OpenTransactionId;
use crate::tools::Operation;
use crate::tx::DurabilityTier;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, params_from_iter};

pub use crate::options::{
    DbConfig, DbIdentity, DeleteOptions, Error, ErrorCode, ExactWriteTarget, InsertOptions,
    ReadOpts, ReadTier, RestoreOptions, RowIdSource, SeededRowIdSource, UpdateOptions,
    UpsertOptions, WriteIdentity, WriteTarget, block_on,
};

use crate::compile::{Compiled, Ctx, Cut, Session, View};
use crate::tools::{CmpOp, PolicyExpr};

#[path = "fanout.rs"]
mod fanout;
pub use fanout::FanoutStats;
#[path = "large_ops.rs"]
mod large_ops;
pub use large_ops::{JsonSetEdit, LargeValueUpdate, LargeValueUpdatePage, LargeValueUpdateSplice};
#[path = "sync.rs"]
pub(crate) mod sync;
pub use crate::options::{REMOTE_LINK_ATTEMPT_WINDOW, TickScheduler, TickUrgency};
use crate::result_tree::{ResultNode, ResultRelation, ResultTree};
pub use crate::values::CurrentRow;
use crate::values::{
    Schema, Sql, Table, from_sql, quote, sql_literal, storage, to_sql, unsupported,
};
pub use sync::{
    Cell as SyncCell, FirstLoad, Image, LOCAL_BASE, QueryAttachment, RemoteLinkHint,
    SerializedReadResult, SubscriberId, SyncMessage, Transport, TransportError, Upstream,
    WireTransport, WireTransportAdapter, duplex,
};

/// Shape identity: equal for bindings of one query against one schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Shape([u8; 16]);

impl Shape {
    pub fn shape_id(&self) -> [u8; 16] {
        self.0
    }
}

/// Binding identity: the parameter values (and request claims) of one use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Binding([u8; 16]);

impl Binding {
    pub fn binding_id(&self) -> [u8; 16] {
        self.0
    }
}

/// A validated query plus its parameter binding. Compilation to SQL happens
/// per execution against the active schema, so handles survive schema
/// changes and the SQL statement cache carries the plan.
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    query: Query,
    binding: BTreeMap<String, Value>,
    shape: Shape,
    binding_id: Binding,
    request_claims: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    /// The schema view that prepared it; `None` follows the owner schema.
    schema: Option<Rc<Schema>>,
}

impl PreparedQuery {
    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn binding(&self) -> &Binding {
        &self.binding_id
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Evaluate claim operands with this request's claims for `author`.
    pub fn with_identity_claims(
        mut self,
        author: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.binding_id.0);
        hasher.update(format!("{author:?}{claims:?}").as_bytes());
        self.binding_id = Binding(short(hasher.finalize()));
        self.request_claims = Some((author, claims));
        self
    }
}

fn short(hash: blake3::Hash) -> [u8; 16] {
    hash.as_bytes()[..16].try_into().expect("16 bytes")
}

/// Result of a standalone write.
#[derive(Clone, Debug)]
pub struct WriteHandle {
    row: RowUuid,
    seq: u64,
}

impl WriteHandle {
    pub fn row_uuid(&self) -> RowUuid {
        self.row
    }

    /// The commit sequence number of the row version this write left.
    pub fn mergeable_tx_id(&self) -> u64 {
        self.seq
    }

    /// Local commits are durable once the call returns.
    pub async fn wait(&self, _tier: DurabilityTier) -> Result<(), Error> {
        Ok(())
    }
}

/// A committed transaction's sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxReceipt(pub u64);

impl From<TxReceipt> for u64 {
    fn from(receipt: TxReceipt) -> u64 {
        receipt.0
    }
}

/// One row newly visible to, or changed in, a subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct SubscriptionOutputRow {
    pub row: CurrentRow,
    pub previous_index: Option<usize>,
    pub index: usize,
}

impl std::ops::Deref for SubscriptionOutputRow {
    type Target = CurrentRow;

    fn deref(&self) -> &CurrentRow {
        &self.row
    }
}

/// One row that left a subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovedRow {
    pub table: String,
    pub row_uuid: RowUuid,
    pub index: usize,
}

/// Subscription events, shaped like `crate::options::SubscriptionEvent`.
#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionEvent {
    Delta {
        reset: bool,
        publishable: bool,
        added: Vec<SubscriptionOutputRow>,
        updated: Vec<SubscriptionOutputRow>,
        removed: Vec<RemovedRow>,
        settled: bool,
        tier: DurabilityTier,
    },
    Rejected {
        reason: String,
    },
    Closed,
}

/// What a read observes: deletions and the branch view.
#[derive(Clone, Debug, Default)]
struct ReadSpec {
    include_deleted: bool,
    view: Option<View>,
    cut: Option<Cut>,
}

impl ReadSpec {
    fn of(opts: &ReadOpts) -> Result<Self, Error> {
        let view = match &opts.read_view.source {
            ReadViewSourceSpec::Current => None,
            ReadViewSourceSpec::BranchView { head, base } => Some(View {
                head: head.clone(),
                base: match base {
                    None => None,
                    Some(BranchViewBase::Current(base)) => Some(base.clone()),
                    Some(BranchViewBase::Snapshot { .. }) => {
                        return Err(unsupported("frozen snapshot bases on SQLite"));
                    }
                },
            }),
            ReadViewSourceSpec::Snapshot { .. } => {
                return Err(unsupported("snapshot reads on SQLite"));
            }
        };
        Ok(Self {
            include_deleted: opts.include_deleted,
            view,
            cut: None,
        })
    }
}

/// A live subscription. Dropping it ends the subscription.
pub struct SubscriptionStream {
    queue: fanout::Queue,
    dropped: Rc<std::cell::Cell<usize>>,
    /// A withheld opening, released as one reset of the current rows.
    gate: Option<sync::Gate>,
    group: Weak<RefCell<fanout::Group>>,
    /// The remote attachment to release on drop.
    remote: Option<(Rc<RefCell<Inner>>, Query, BTreeMap<String, Value>)>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
        if let Some((inner, query, binding)) = self.remote.take() {
            Db { inner, view: None }.detach_remote(&query, &binding);
        }
    }
}

impl SubscriptionStream {
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        if let Some(gate) = &self.gate {
            if !gate.open() {
                return None;
            }
            let settled = gate.answered.get();
            self.gate = None;
            self.queue.borrow_mut().clear();
            let group = self.group.upgrade()?;
            let mut event = fanout::opening(&group.borrow().rows);
            if let SubscriptionEvent::Delta { settled: flag, .. } = &mut event {
                *flag = settled;
            }
            return Some(event);
        }
        self.queue.borrow_mut().pop_front()
    }

    pub async fn close(self) -> Result<(), Error> {
        Ok(())
    }

    /// Deltas are produced synchronously by the committing call, so the
    /// next event is either queued now or there is none yet.
    pub async fn next_event(&mut self) -> Option<SubscriptionEvent> {
        self.try_next_event()
    }
}

/// Test-visible runtime counters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
    /// SQLite keeps no per-query state; this counts distinct live shapes.
    pub arrangement_count: usize,
}

/// A schema lineage step authored for publication.
#[derive(Clone, Debug)]
pub struct SchemaPublication {
    version: SchemaVersion,
    lens: MigrationLens,
}

struct Inner {
    conn: Connection,
    /// Large-value chunks: the cache the collation and decoders read.
    chunks: Arc<crate::large::Chunks>,
    /// Nodes a large-value edit consolidated, stored when the edited cell
    /// is bound inside the write's transaction.
    pending_nodes: Vec<jazz_types::large_values::PreparedLargeValue>,
    /// The schema the current operation runs under: the owner's, or the
    /// schema view's that issued it.
    schema: Rc<Schema>,
    /// The owner's current schema.
    owner: Rc<Schema>,
    /// The stored tables: the union of every admitted schema.
    physical: Schema,
    /// Admitted structural schemas, by version, and registered views.
    admitted: Vec<JazzSchema>,
    views: BTreeMap<SchemaViewId, JazzSchema>,
    /// Open transaction batches, by id.
    batches: HashMap<OpenTransactionId, Batch>,
    identity: DbIdentity,
    /// Provenance for the write being applied (set by `apply_as`).
    writer: Option<Stamp>,
    /// The commit seq and time of the last local stamp: writes of one commit
    /// share a time, and later commits get strictly later ones, so
    /// `$createdAt` orders this node's writes even within one millisecond.
    last_stamp: (u64, i64),
    /// Interned author codes (see [`Inner::author_code`]), cached both ways.
    authors: RefCell<AuthorCodes>,
    ids: Box<dyn RowIdSource>,
    seq: u64,
    claims: HashMap<AuthorSubject, BTreeMap<String, Value>>,
    /// Live subscription groups and their invalidation index.
    fan: fanout::Fanout,
    /// Rows written since the last dispatch: (table, id, branch key).
    changed: RefCell<Vec<(String, RowUuid, Vec<u8>)>>,
    /// The open transaction's seq and the tables it wrote.
    tx: Option<(u64, BTreeSet<String>)>,
    publications: Vec<SchemaPublication>,
    /// Set once connected upstream: this store is a replica.
    client: Option<sync::Client>,
    /// Clients this store serves.
    sessions: Vec<sync::Session>,
    /// Rows written since the last server tick (only while serving).
    since_tick: Vec<(String, RowUuid, Vec<u8>)>,
    /// Ops of the open local transaction, for upload.
    capture: Vec<StagedOp>,
    /// Images a replica dropped since the last dispatch (evictions and
    /// rebases), for keyed invalidation.
    gone: RefCell<Vec<sync::Image>>,
}

/// Large-value nodes one tick may reclaim.
const RECLAIM_NODES_PER_TICK: usize = 256;

/// File store settings.
#[derive(Clone, Copy, Debug)]
pub struct FileOptions {
    /// `synchronous = FULL` (every commit survives power loss) instead of
    /// `NORMAL` (WAL stays consistent; the newest commits may be lost on
    /// power loss, never on a process crash). `NORMAL` is the default, as
    /// unsynced WAL writes were for the RocksDB store.
    pub synchronous_full: bool,
    /// Page cache bound per connection, in KiB.
    pub cache_kib: u64,
}

impl Default for FileOptions {
    fn default() -> Self {
        Self {
            synchronous_full: false,
            cache_kib: 8 * 1024,
        }
    }
}

/// Jazz's `Db` over SQLite.
pub struct Db {
    inner: Rc<RefCell<Inner>>,
    /// `Some` for a schema-view handle: operations run under this schema.
    view: Option<Rc<Schema>>,
}

#[derive(Clone, Copy)]
pub(crate) enum Who {
    /// Client-local optimistic staging: no local policy check (the
    /// authority decides on upload).
    Local,
    /// A trusted session identity: policies apply now.
    Session(AuthorSubject),
}

fn who(identity: WriteIdentity) -> Who {
    match identity {
        WriteIdentity::Session(author) if author != AuthorSubject::SYSTEM => Who::Session(author),
        _ => Who::Local,
    }
}

#[derive(Default)]
pub(crate) struct AuthorCodes {
    codes: HashMap<(String, String), i64>,
    parts: HashMap<i64, (String, String)>,
}

/// Who wrote a row version, and when (Unix ms).
#[derive(Clone)]
pub(crate) struct Stamp {
    at: i64,
    /// The author's interned code in `_jazz_authors`.
    author: i64,
}

impl Stamp {
    fn created(&self) -> [(&'static str, Sql); 2] {
        [
            ("$createdAt", Sql::Integer(self.at)),
            ("$createdBy", Sql::Integer(self.author)),
        ]
    }

    fn updated(&self) -> [(&'static str, Sql); 2] {
        [
            ("$updatedAt", Sql::Integer(self.at)),
            ("$updatedBy", Sql::Integer(self.author)),
        ]
    }
}

fn rejected(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::WriteRejected, message)
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Self, Error> {
        let conn = Connection::open_in_memory().map_err(storage)?;
        Self::open_on(config, conn, None)
    }

    /// A durable store in one SQLite file (WAL). Reopening restores rows,
    /// history, the commit seq and, for a replica, its pending uploads and
    /// shadow; they are resent on the next connection.
    pub async fn open_at(
        config: DbConfig,
        path: &std::path::Path,
        options: FileOptions,
    ) -> Result<Self, Error> {
        let conn = Connection::open(path).map_err(storage)?;
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = {}; PRAGMA cache_size = -{};",
            if options.synchronous_full {
                "FULL"
            } else {
                "NORMAL"
            },
            options.cache_kib
        ))
        .map_err(storage)?;
        Self::open_on(config, conn, Some(path))
    }

    fn open_on(
        config: DbConfig,
        conn: Connection,
        path: Option<&std::path::Path>,
    ) -> Result<Self, Error> {
        let schema = Schema::new(&config.schema);
        // The collation and functions must exist before any table that
        // uses them is created or written.
        let chunks = crate::large::Chunks::install(&conn, path)?;
        conn.execute_batch(crate::large::DDL).map_err(storage)?;
        // Nodes a transaction writes stay cached until it ends: until then
        // the reader connection cannot see them.
        let committed = chunks.clone();
        conn.commit_hook(Some(move || {
            committed.unpin();
            false
        }));
        let rolled_back = chunks.clone();
        conn.rollback_hook(Some(move || rolled_back.unpin()));
        let fresh: i64 = conn
            .query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_jazz_%'", [], |r| r.get(0))
            .map_err(storage)?;
        if fresh == 0 {
            conn.execute_batch(&schema.ddl()).map_err(storage)?;
        }
        conn.execute_batch(sync::SYNC_DDL).map_err(storage)?;
        // The commit seq resumes after the newest stored version.
        let mut seq = 0u64;
        for table in schema.tables.keys() {
            for name in [table.clone(), format!("{table}__h")] {
                let max: Option<i64> = conn
                    .query_row(
                        &format!("SELECT MAX(_seq) FROM {}", quote(&name)),
                        [],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                seq = seq.max(max.unwrap_or(0) as u64);
            }
        }
        let owner = Rc::new(schema.clone());
        let db = Self {
            view: None,
            inner: Rc::new(RefCell::new(Inner {
                conn,
                chunks,
                pending_nodes: Vec::new(),
                schema: owner.clone(),
                owner,
                physical: schema,
                admitted: vec![config.schema.clone()],
                views: BTreeMap::new(),
                batches: HashMap::new(),
                identity: config.identity,
                writer: None,
                last_stamp: (0, 0),
                authors: RefCell::new(AuthorCodes::default()),
                ids: config
                    .id_source
                    .unwrap_or_else(|| Box::new(crate::options::ProductionRowIdSource)),
                seq: 0,
                claims: HashMap::new(),
                fan: fanout::Fanout::default(),
                changed: RefCell::new(Vec::new()),
                tx: None,
                publications: Vec::new(),
                client: None,
                sessions: Vec::new(),
                since_tick: Vec::new(),
                capture: Vec::new(),
                gone: RefCell::new(Vec::new()),
            })),
        };
        {
            let mut inner = db.inner.borrow_mut();
            inner.seq = seq;
            inner.load_client()?;
            if inner.client.is_some() {
                inner.seq = inner.seq.max(LOCAL_BASE);
            }
        }
        Ok(db)
    }

    /// Every SQLite store holds complete history.
    pub async fn open_history_complete(config: DbConfig) -> Result<Self, Error> {
        Self::open(config).await
    }

    /// The shared state, set up to run under this handle's schema.
    fn inner(&self) -> std::cell::RefMut<'_, Inner> {
        let mut inner = self.inner.borrow_mut();
        inner.schema = self.view.clone().unwrap_or_else(|| inner.owner.clone());
        inner
    }

    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    pub fn set_identity_claims(&self, identity: AuthorSubject, claims: BTreeMap<String, Value>) {
        let mut inner = self.inner();
        inner.claims.insert(identity, claims);
        inner.refresh_identity(identity);
    }

    pub async fn tick(&self) -> Result<(), Error> {
        self.sync_tick()?;
        let mut inner = self.inner();
        inner.fan.collect();
        inner.reclaim_large_values(RECLAIM_NODES_PER_TICK)?;
        Ok(())
    }

    /// Diagnostics: stored large-value nodes, and roots referenced by cells.
    #[doc(hidden)]
    pub fn large_value_nodes_for_test(&self) -> Result<(u64, u64), Error> {
        let inner = self.inner();
        inner
            .conn
            .query_row(
                "SELECT (SELECT count(*) FROM _jazz_lv_node), \
                 (SELECT count(*) FROM _jazz_lv_root WHERE durable > 0)",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .map_err(storage)
    }

    /// Diagnostics: the chunks reads needed that nothing local holds.
    #[doc(hidden)]
    pub fn take_missing_chunks_for_test(&self) -> usize {
        self.inner().chunks.take_demand().len()
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.tick().await
    }

    // ---- preparation ----------------------------------------------------

    /// Diagnostics: the compiled SQL of a read and SQLite's query plan for
    /// it. The basis for future index suggestions.
    #[doc(hidden)]
    pub fn explain_query_plan(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<(String, String), Error> {
        let inner = self.inner();
        let compiled = inner.compile(prepared, identity, &ReadSpec::default())?;
        let mut statement = inner
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {}", compiled.sql))
            .map_err(storage)?;
        let rows = statement
            .query_map(params_from_iter(compiled.params.iter()), |r| {
                r.get::<_, String>(3)
            })
            .map_err(storage)?;
        let mut plan = Vec::new();
        for row in rows {
            plan.push(row.map_err(storage)?);
        }
        Ok((compiled.sql.clone(), plan.join(" | ")))
    }

    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    pub fn prepare_query_bound(
        &self,
        query: &Query,
        binding: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let inner = self.inner();
        // Validate: tables, columns, parameters, supported forms.
        let session = Session::system();
        Ctx::new(&inner.schema, None, &session, &binding).compile_query(query, false)?;
        let shape =
            blake3::hash(format!("{query:?}{:?}", inner.schema.jazz.version_id()).as_bytes());
        let bound = blake3::hash(format!("{binding:?}").as_bytes());
        Ok(PreparedQuery {
            query: query.clone(),
            binding,
            shape: Shape(short(shape)),
            binding_id: Binding(short(bound)),
            request_claims: None,
            schema: self.view.clone(),
        })
    }

    // ---- reads ----------------------------------------------------------

    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.inner().run(prepared, None, &ReadSpec::of(&opts)?)
    }

    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.inner().run(prepared, None, &ReadSpec::default())
    }

    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.inner()
            .run(prepared, Some(identity), &ReadSpec::of(&opts)?)
    }

    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let inner = self.inner();
        let roots = inner.run(prepared, None, &ReadSpec::of(&opts)?)?;
        let mut tree = Vec::with_capacity(roots.len());
        for row in roots {
            tree.push(inner.node(
                prepared,
                &prepared.query.table,
                &prepared.query.array_subqueries,
                row,
            )?);
        }
        Ok(ResultTree { roots: tree })
    }

    // ---- subscriptions --------------------------------------------------

    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, None, opts)
    }

    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, Some(identity), opts)
    }

    pub async fn subscribe_client_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, Some(identity), opts)
    }

    /// On a client a subscription also attaches remotely (unless
    /// LocalOnly); its opening is withheld while it waits for the server
    /// (the Remote tier, or `FirstLoad::WaitForRemote` while the link could
    /// answer), and its deltas report whether the server answered.
    fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        let read = ReadSpec::of(&opts)?;
        let client = self.is_client();
        if client && opts.tier != ReadTier::LocalOnly {
            Self::remote_shape(&prepared.query)?;
        }
        let remote = if client && opts.tier != ReadTier::LocalOnly {
            self.attach_remote(prepared)
        } else {
            None
        };
        let settled = match (&remote, client) {
            (Some(answered), _) => sync::SettledBy::Remote(answered.clone()),
            (None, true) => sync::SettledBy::Never,
            (None, false) => sync::SettledBy::Always,
        };
        let gate = match (&remote, self.link_hint()) {
            (Some(answered), Some(hint)) => {
                let deadline = match (opts.tier, &opts.first_load) {
                    (ReadTier::Remote, _) => Some(None),
                    (_, FirstLoad::WaitForRemote { timeout_ms })
                        if *timeout_ms > 0 && sync::may_wait(hint.get()) =>
                    {
                        Some(Some(
                            std::time::Instant::now()
                                + std::time::Duration::from_millis(*timeout_ms),
                        ))
                    }
                    _ => None,
                };
                let live = hint.get().hint == RemoteLinkHint::Live;
                deadline.map(|deadline| sync::Gate {
                    answered: answered.clone(),
                    hint,
                    live,
                    deadline,
                })
            }
            _ => None,
        };
        let mut inner = self.inner();
        let (queue, group) = inner.subscribe_member(prepared, identity, read, settled)?;
        Ok(SubscriptionStream {
            queue,
            dropped: inner.fan.dropped.clone(),
            gate,
            group,
            remote: remote.map(|_| {
                (
                    self.inner.clone(),
                    prepared.query.clone(),
                    prepared.binding.clone(),
                )
            }),
        })
    }

    /// How many distinct authors this database has interned.
    #[doc(hidden)]
    pub fn interned_authors_for_test(&self) -> usize {
        self.inner()
            .conn
            .query_row("SELECT COUNT(*) FROM _jazz_authors", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_or(0, |count| count as usize)
    }

    pub fn runtime_stats_for_test(&self) -> RuntimeStats {
        let live = self.inner().fan.live_members();
        let shapes: BTreeSet<Shape> = live.iter().map(|(shape, _)| *shape).collect();
        RuntimeStats {
            active_subscriptions: live.len(),
            active_prepared_shapes: shapes.len(),
            active_shape_params: live.iter().filter(|(_, bound)| *bound).count(),
            arrangement_count: shapes.len(),
        }
    }

    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.inner().fan.live_members().len()
    }

    pub fn fanout_stats(&self) -> fanout::FanoutStats {
        self.inner().fan.stats()
    }

    /// Deliver the rows written since the last dispatch to the groups they
    /// wake (keyed invalidation; see `fanout`).
    fn notify(&self) {
        let mut inner = self.inner();
        let changes = std::mem::take(&mut *inner.changed.borrow_mut());
        if !inner.sessions.is_empty() {
            inner.since_tick.extend(changes.iter().cloned());
        }
        inner.dispatch(changes);
    }

    // ---- writes ---------------------------------------------------------

    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        mut opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        if opts.row_id.is_none() {
            opts.row_id = Some(self.inner().ids.next_row_id());
        }
        let who = who(opts.identity);
        self.write_op(StagedOp::Insert(table.to_owned(), cells, opts), who)
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        self.write_op(StagedOp::Update(table.to_owned(), row, cells, opts), who)
    }

    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        self.write_op(StagedOp::Upsert(table.to_owned(), row, cells, opts), who)
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        self.write_op(StagedOp::Delete(table.to_owned(), row, opts), who)
    }

    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        self.write_op(StagedOp::Restore(table.to_owned(), row, content, opts), who)
    }

    /// One write. On a client, a local (unauthenticated) write is also an
    /// upload: it is recorded for the authority, which decides its fate.
    fn write_op(&self, op: StagedOp, who: Who) -> Result<WriteHandle, Error> {
        let local = matches!(who, Who::Local);
        self.with_upload(|| {
            let handle = self.write(op.table(), |inner, seq| inner.apply_as(&op, who, seq))?;
            if local {
                self.record_upload(vec![op], handle.seq, 0, sync::Kind::Mergeable);
            }
            Ok(handle)
        })
    }

    /// On a replica, a local commit and its upload record are one SQLite
    /// transaction (outside an open Jazz transaction, which already is).
    fn with_upload<R>(&self, f: impl FnOnce() -> Result<R, Error>) -> Result<R, Error> {
        let wrap = {
            let inner = self.inner.borrow();
            inner.client.is_some() && inner.tx.is_none()
        };
        if !wrap {
            return f();
        }
        self.inner
            .borrow()
            .conn
            .execute_batch("SAVEPOINT upload_local")
            .map_err(storage)?;
        let result = f();
        let end = if result.is_ok() {
            "RELEASE upload_local"
        } else {
            "ROLLBACK TO upload_local; RELEASE upload_local"
        };
        self.inner
            .borrow()
            .conn
            .execute_batch(end)
            .map_err(storage)?;
        result
    }

    /// Run one write atomically. Inside a transaction it joins the
    /// transaction's seq; standalone it commits and notifies at once.
    fn write(
        &self,
        table: &str,
        apply: impl FnOnce(&mut Inner, u64) -> Result<WriteHandle, Error>,
    ) -> Result<WriteHandle, Error> {
        let standalone = {
            let mut inner = self.inner();
            let (seq, standalone) = match &inner.tx {
                Some((seq, _)) => (*seq, false),
                None => (inner.seq + 1, true),
            };
            inner.conn.execute_batch("SAVEPOINT w").map_err(storage)?;
            let mark = inner.changed.borrow().len();
            match apply(&mut inner, seq) {
                Ok(handle) => {
                    inner.conn.execute_batch("RELEASE w").map_err(storage)?;
                    if handle.seq == seq {
                        if standalone {
                            inner.seq = seq;
                        } else if let Some((_, tables)) = &mut inner.tx {
                            tables.insert(table.to_owned());
                        }
                    }
                    (standalone && handle.seq == seq, handle)
                }
                Err(error) => {
                    inner
                        .conn
                        .execute_batch("ROLLBACK TO w; RELEASE w")
                        .map_err(storage)?;
                    inner.changed.borrow_mut().truncate(mark);
                    return Err(error);
                }
            }
        };
        let (notify, handle) = standalone;
        if notify {
            self.notify();
        }
        Ok(handle)
    }

    // ---- transactions ---------------------------------------------------

    pub async fn transaction<T>(
        &self,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_>) -> Result<T, Error>,
    ) -> Result<(T, TxReceipt), Error> {
        let author = self.inner().identity.author;
        self.transaction_for_identity(author, callback).await
    }

    /// Mergeable transactions stage their writes for the authority, so no
    /// local policy check runs here (as in Jazz).
    pub async fn transaction_for_identity<T>(
        &self,
        author: AuthorSubject,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_>) -> Result<T, Error>,
    ) -> Result<(T, TxReceipt), Error> {
        {
            let mut inner = self.inner();
            if inner.tx.is_some() {
                return Err(unsupported("nested transactions"));
            }
            inner.conn.execute_batch("SAVEPOINT tx").map_err(storage)?;
            inner.changed.borrow_mut().clear();
            let seq = inner.seq + 1;
            inner.tx = Some((seq, BTreeSet::new()));
        }
        let mut tx = MergeableTx {
            db: self,
            _author: author,
        };
        let result = callback(&mut tx).await;
        let captured;
        let changed = {
            let mut inner = self.inner();
            let (seq, tables) = inner.tx.take().expect("open transaction");
            captured = std::mem::take(&mut inner.capture);
            match &result {
                Ok(_) => {
                    if !tables.is_empty() {
                        inner.seq = seq;
                    }
                    Some((seq, tables))
                }
                Err(_) => {
                    inner
                        .conn
                        .execute_batch("ROLLBACK TO tx; RELEASE tx")
                        .map_err(storage)?;
                    inner.changed.borrow_mut().clear();
                    None
                }
            }
        };
        let value = result?;
        let (seq, _) = changed.expect("committed");
        // The upload record joins the transaction's SQLite savepoint.
        self.record_upload(captured, seq, 0, sync::Kind::Mergeable);
        self.inner
            .borrow()
            .conn
            .execute_batch("RELEASE tx")
            .map_err(storage)?;
        self.notify();
        Ok((value, TxReceipt(seq)))
    }

    // ---- schema evolution -------------------------------------------------

    pub fn author_schema_lineage_publication(
        &self,
        version: SchemaVersion,
        lens: MigrationLens,
        _removed_tables: Vec<String>,
        _removed_columns: Vec<String>,
    ) -> Result<SchemaPublication, Error> {
        Ok(SchemaPublication { version, lens })
    }

    pub async fn publish_schema_with_lens(
        &self,
        _revision: u64,
        publication: SchemaPublication,
    ) -> Result<(), Error> {
        self.inner().publications.push(publication);
        Ok(())
    }

    /// Make a published version current. An `AddColumn` lens is an
    /// `ALTER TABLE ADD COLUMN ... DEFAULT`: SQLite records the default in
    /// the schema and existing rows read it without being rewritten.
    pub async fn activate_catalogue_schema_for_test(
        &self,
        current: CurrentWriteSchema,
    ) -> Result<(), Error> {
        {
            let mut inner = self.inner();
            let publication = inner
                .publications
                .iter()
                .find(|publication| publication.version.id == current.schema)
                .cloned()
                .ok_or_else(|| Error::new(ErrorCode::Schema, "schema version was not published"))?;
            let mut ddl = String::new();
            for table in &publication.lens.table_lenses {
                if table.source_table != table.target_table {
                    return Err(unsupported("table rename lenses on SQLite"));
                }
                for op in &table.ops {
                    match op {
                        LensOp::AddColumn {
                            column, default, ..
                        } => {
                            let default = sql_literal(default)?;
                            for name in [
                                table.target_table.clone(),
                                format!("{}__h", table.target_table),
                            ] {
                                ddl.push_str(&format!(
                                    "ALTER TABLE {} ADD COLUMN {} DEFAULT {default};\n",
                                    quote(&name),
                                    quote(column)
                                ));
                            }
                        }
                        other => return Err(unsupported(format!("lens op {other:?} on SQLite"))),
                    }
                }
            }
            inner.conn.execute_batch(&ddl).map_err(storage)?;
            let schema = Schema::new(&publication.version.schema);
            // The lens already added the columns; record them physically.
            let _ = inner.physical.union(&schema)?;
            inner.owner = Rc::new(schema);
            inner.admitted.push(publication.version.schema.clone());
        }
        self.inner().reset_everything();
        Ok(())
    }
}

fn delta(
    reset: bool,
    added: Vec<SubscriptionOutputRow>,
    updated: Vec<SubscriptionOutputRow>,
    removed: Vec<RemovedRow>,
) -> SubscriptionEvent {
    SubscriptionEvent::Delta {
        reset,
        publishable: true,
        added,
        updated,
        removed,
        settled: true,
        tier: DurabilityTier::Local,
    }
}

/// A mergeable transaction: writes join one commit, reads see them.
pub struct MergeableTx<'a> {
    db: &'a Db,
    _author: AuthorSubject,
}

impl MergeableTx<'_> {
    pub async fn insert(
        &mut self,
        table: &str,
        cells: BTreeMap<String, Value>,
        opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        self.db
            .insert(
                table,
                cells,
                InsertOptions {
                    identity: WriteIdentity::Database,
                    ..opts
                },
            )
            .await
    }

    pub async fn update(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.db
            .update(
                table,
                row,
                cells,
                UpdateOptions {
                    identity: WriteIdentity::Database,
                    ..opts
                },
            )
            .await
    }

    pub async fn upsert(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<WriteHandle, Error> {
        self.db
            .upsert(
                table,
                row,
                cells,
                UpsertOptions {
                    identity: WriteIdentity::Database,
                    ..opts
                },
            )
            .await
    }

    pub async fn delete(
        &mut self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        self.db
            .delete(
                table,
                row,
                DeleteOptions {
                    identity: WriteIdentity::Database,
                    ..opts
                },
            )
            .await
    }

    pub async fn all_prepared(
        &mut self,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db.read(prepared)
    }
}

impl Inner {
    fn session(&self, author: AuthorSubject) -> Session {
        Session::for_author(
            author,
            self.claims.get(&author).cloned().unwrap_or_default(),
        )
    }

    /// Sessions for a read: (policy subject, claim source).
    fn read_sessions(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> (Option<Session>, Session) {
        let claims_for = |author: AuthorSubject| match &prepared.request_claims {
            Some((requested, claims)) if *requested == author => {
                Session::for_author(author, claims.clone())
            }
            _ => self.session(author),
        };
        let policy = identity.map(claims_for);
        let claims = policy
            .clone()
            .unwrap_or_else(|| claims_for(self.identity.author));
        (policy, claims)
    }

    fn compile(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Compiled, Error> {
        let (policy, claims) = self.read_sessions(prepared, identity);
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        Ctx::new(schema, policy.as_ref(), &claims, &prepared.binding)
            .with_view(read.view.clone())
            .with_cut(read.cut)
            .compile_query(&prepared.query, read.include_deleted)
    }

    fn run(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        Ok(self.run_with_deps(prepared, identity, read)?.0)
    }

    fn run_with_deps(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<(Vec<CurrentRow>, BTreeSet<String>), Error> {
        let mut compiled = self.compile(prepared, identity, read)?;
        let rows = self.execute(&compiled, &prepared.query.table)?;
        // Array relations are part of a subscription's dependencies.
        fn arrays(out: &mut BTreeSet<String>, arrays: &[crate::query::ArraySubquery]) {
            for array in arrays {
                out.insert(array.table.clone());
                arrays_nested(out, &array.nested_arrays);
            }
        }
        fn arrays_nested(out: &mut BTreeSet<String>, nested: &[crate::query::ArraySubquery]) {
            arrays(out, nested)
        }
        arrays(&mut compiled.tables, &prepared.query.array_subqueries);
        Ok((rows, compiled.tables))
    }

    fn execute(&self, compiled: &Compiled, table: &str) -> Result<Vec<CurrentRow>, Error> {
        let mut statement = self.conn.prepare_cached(&compiled.sql).map_err(storage)?;
        let names: Arc<str> = Arc::from(table);
        let mut rows = statement
            .query(params_from_iter(compiled.params.iter()))
            .map_err(storage)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(storage)? {
            let id: Vec<u8> = if compiled.aggregate {
                let key = match row.get_ref(0).map_err(storage)? {
                    ValueRef::Text(cell) => format!("{:?}", self.chunks.materialize(cell)?),
                    cell => format!("{:?}", Sql::from(cell)),
                };
                blake3::hash(key.as_bytes()).as_bytes()[..16].to_vec()
            } else {
                row.get(0).map_err(storage)?
            };
            let seq: i64 = row.get(1).map_err(storage)?;
            let deleted: i64 = row.get(2).map_err(storage)?;
            let cells = compiled
                .projection
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| {
                    let cell = row.get_ref(3 + i).map_err(storage)?;
                    Ok((name.clone(), from_sql(ty, cell, &self.chunks)?))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            out.push(CurrentRow {
                table: names.clone(),
                id: RowUuid(
                    uuid::Uuid::from_slice(&id)
                        .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                ),
                seq: seq as u64,
                deleted: deleted != 0,
                cells: Arc::new(cells),
            });
        }
        Ok(out)
    }

    fn node(
        &self,
        prepared: &PreparedQuery,
        table: &str,
        arrays: &[crate::query::ArraySubquery],
        row: CurrentRow,
    ) -> Result<ResultNode, Error> {
        let mut relations = BTreeMap::new();
        for array in arrays {
            let parent = if array.outer_column == "id" {
                Value::Uuid(row.id.0)
            } else {
                self.cell(table, row.id, &array.outer_column)?
            };
            let (policy, claims) = self.read_sessions(prepared, None);
            let compiled = Ctx::new(&self.schema, policy.as_ref(), &claims, &prepared.binding)
                .compile_children(array, &parent)?;
            let children = self.execute(&compiled, &array.table)?;
            let mut nodes = Vec::with_capacity(children.len());
            for child in children {
                nodes.push(self.node(prepared, &array.table, &array.nested_arrays, child)?);
            }
            relations.insert(array.column_name.clone(), ResultRelation::Array(nodes));
        }
        Ok(ResultNode { row, relations })
    }

    fn cell(&self, table: &str, row: RowUuid, column: &str) -> Result<Value, Error> {
        let ty = self
            .schema
            .table(table)?
            .column(column)
            .map(|c| c.ty.clone())
            .ok_or_else(|| unsupported("unknown correlation column"))?;
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1",
            quote(column),
            quote(table)
        );
        let cell: Sql = self
            .conn
            .query_row(&sql, [row.0.as_bytes().to_vec()], |r| r.get(0))
            .map_err(storage)?;
        Ok(from_sql(&ty, cell.as_ref(), &self.chunks)?.unwrap_or(Value::Nullable(None)))
    }

    // ---- write primitives ---------------------------------------------

    fn existing(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<(u64, bool)>, Error> {
        let sql = format!(
            "SELECT _seq, _del FROM {} WHERE id = ?1 AND _b = ?2",
            quote(table)
        );
        match self.conn.query_row(
            &sql,
            rusqlite::params![row.0.as_bytes().to_vec(), key],
            |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? != 0)),
        ) {
            Ok(found) => Ok(Some(found)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// Resolve which stored row a patch-style write addresses.
    fn locate(&self, table: &Table, row: RowUuid, target: &Target) -> Result<Located, Error> {
        let at = |key: Vec<u8>, cells: BTreeMap<String, Value>| -> Result<Located, Error> {
            Ok(match self.existing(&table.name, row, &key)? {
                Some((seq, deleted)) => Located::Row { key, seq, deleted },
                None => Located::Absent { cells },
            })
        };
        match target {
            Target::Root if table.branch_by.is_empty() => at(Vec::new(), BTreeMap::new()),
            Target::Root => {
                let sql = format!("SELECT _b FROM {} WHERE id = ?1", quote(&table.name));
                let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
                let keys: Vec<Vec<u8>> = statement
                    .query_map([row.0.as_bytes().to_vec()], |r| r.get(0))
                    .map_err(storage)?
                    .collect::<Result<_, _>>()
                    .map_err(storage)?;
                match keys.as_slice() {
                    [key] => at(key.clone(), BTreeMap::new()),
                    [] => Ok(Located::Absent {
                        cells: BTreeMap::new(),
                    }),
                    _ => Err(rejected(format!(
                        "row {row:?} exists on several branches of `{}`; name a branch target",
                        table.name
                    ))),
                }
            }
            Target::Exact(selector) => at(
                table.selector_key(selector)?,
                table.selector_cells(selector)?,
            ),
            Target::View { head, base } => {
                let head_key = table.selector_key(head)?;
                let head_cells = if table.branch_by.is_empty() {
                    BTreeMap::new()
                } else {
                    table.selector_cells(head)?
                };
                if let Some((seq, deleted)) = self.existing(&table.name, row, &head_key)? {
                    return Ok(Located::Row {
                        key: head_key,
                        seq,
                        deleted,
                    });
                }
                if let Some(base) = base {
                    let base_key = table.selector_key(base)?;
                    if base_key != head_key
                        && let Some((_, false)) = self.existing(&table.name, row, &base_key)?
                    {
                        return Ok(Located::Inherited {
                            from: base_key,
                            to: head_key,
                            head_cells,
                        });
                    }
                }
                Ok(Located::Absent { cells: head_cells })
            }
        }
    }

    /// Evaluate a policy SQL boolean against one stored row.
    fn holds(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding);
        let condition = policy(&mut ctx)?;
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition}, 0) FROM {} AS a WHERE a.id = ?{} AND a._b = ?{}",
            quote(&table.name),
            params.len() - 1,
            params.len()
        );
        let value: i64 = self
            .conn
            .query_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .map_err(storage)?;
        Ok(value != 0)
    }

    /// The current version of each of `ids`' rows in `table`: (id, branch
    /// key, seq, deleted, readable by `author`), in one query per chunk
    /// instead of one policy check per row.
    pub(crate) fn versions_readable(
        &self,
        table: &Table,
        author: Option<AuthorSubject>,
        ids: &[[u8; 16]],
    ) -> Result<Vec<([u8; 16], Vec<u8>, u64, bool, bool)>, Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding);
        let condition = match author {
            Some(author) => {
                let session = self.session(author);
                ctx.operation_policy(table, Operation::Select, "a", &session)?
            }
            None => "1".to_owned(),
        };
        let base = ctx.params;
        let mut out = Vec::new();
        for chunk in ids.chunks(500) {
            let mut params = base.clone();
            let first = params.len() + 1;
            params.extend(chunk.iter().map(|id| Sql::Blob(id.to_vec())));
            let marks: Vec<String> = (first..first + chunk.len())
                .map(|i| format!("?{i}"))
                .collect();
            let sql = format!(
                "SELECT a.id, a._b, a._seq, a._del, COALESCE({condition}, 0) FROM {} AS a WHERE a.id IN ({})",
                quote(&table.name),
                marks.join(", ")
            );
            let mut statement = self.conn.prepare(&sql).map_err(storage)?;
            let rows = statement
                .query_map(params_from_iter(params.iter()), |r| {
                    let id: Vec<u8> = r.get(0)?;
                    let seq: i64 = r.get(2)?;
                    let deleted: i64 = r.get(3)?;
                    let can: i64 = r.get(4)?;
                    Ok((
                        id,
                        r.get::<_, Vec<u8>>(1)?,
                        seq as u64,
                        deleted != 0,
                        can != 0,
                    ))
                })
                .map_err(storage)?;
            for row in rows {
                let (id, key, seq, deleted, can) = row.map_err(storage)?;
                let Ok(id) = <[u8; 16]>::try_from(id.as_slice()) else {
                    continue;
                };
                out.push((id, key, seq, deleted, can));
            }
        }
        Ok(out)
    }

    fn check(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        self.holds(table, row, key, |ctx| {
            ctx.operation_policy(table, operation, "a", &session)
        })
    }

    fn check_clause(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        clause: Option<&crate::tools::PolicyExpr>,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        self.holds(table, row, key, |ctx| {
            ctx.policy_clause(clause, table, "a", &session)
        })
    }

    fn stamp(&self) -> Result<Stamp, Error> {
        match &self.writer {
            Some(stamp) => Ok(stamp.clone()),
            None => Ok(Stamp {
                at: 0,
                author: self.author_code(&self.identity.author.principal_parts())?,
            }),
        }
    }

    /// The interned code of an author's `(issuer, subject)`. Rows store this
    /// small integer instead of repeating both strings in every version;
    /// codes are local to this database and resolved back at egress.
    pub(crate) fn author_code(&self, (issuer, subject): &(String, String)) -> Result<i64, Error> {
        if let Some(code) = self
            .authors
            .borrow()
            .codes
            .get(&(issuer.clone(), subject.clone()))
        {
            return Ok(*code);
        }
        self.conn
            .execute(
                "INSERT OR IGNORE INTO _jazz_authors (issuer, subject) VALUES (?1, ?2)",
                rusqlite::params![issuer, subject],
            )
            .map_err(storage)?;
        let code: i64 = self
            .conn
            .query_row(
                "SELECT n FROM _jazz_authors WHERE issuer = ?1 AND subject = ?2",
                rusqlite::params![issuer, subject],
                |row| row.get(0),
            )
            .map_err(storage)?;
        let mut authors = self.authors.borrow_mut();
        authors
            .codes
            .insert((issuer.clone(), subject.clone()), code);
        authors
            .parts
            .insert(code, (issuer.clone(), subject.clone()));
        Ok(code)
    }

    /// The `(issuer, subject)` an author code stands for.
    pub(crate) fn author_parts(&self, code: i64) -> Result<(String, String), Error> {
        if let Some(parts) = self.authors.borrow().parts.get(&code) {
            return Ok(parts.clone());
        }
        let parts: (String, String) = self
            .conn
            .query_row(
                "SELECT issuer, subject FROM _jazz_authors WHERE n = ?1",
                [code],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        let mut authors = self.authors.borrow_mut();
        authors.codes.insert(parts.clone(), code);
        authors.parts.insert(code, parts.clone());
        Ok(parts)
    }

    fn validate_cells(table: &Table, cells: &BTreeMap<String, Value>) -> Result<(), Error> {
        for name in cells.keys() {
            if name.starts_with('$') {
                return Err(rejected(format!(
                    "`{}.{name}` is engine-maintained provenance and cannot be written",
                    table.name
                )));
            }
            if table.column(name).is_none() {
                return Err(rejected(format!("unknown column `{}.{name}`", table.name)));
            }
        }
        Ok(())
    }

    /// Delete up to `budget` large-value nodes no cell or branch references.
    fn reclaim_large_values(&mut self, budget: usize) -> Result<usize, Error> {
        self.conn
            .execute_batch("SAVEPOINT reclaim")
            .map_err(storage)?;
        match crate::large::reclaim(&self.conn, &self.chunks, budget) {
            Ok(deleted) => {
                self.conn
                    .execute_batch("RELEASE reclaim")
                    .map_err(storage)?;
                Ok(deleted)
            }
            Err(error) => {
                let _ = self
                    .conn
                    .execute_batch("ROLLBACK TO reclaim; RELEASE reclaim");
                Err(error)
            }
        }
    }

    fn bind_value(value: &Value) -> Result<Sql, Error> {
        to_sql(value)?.ok_or_else(|| unsupported("storing NaN"))
    }

    /// A value as the cell `column` stores. A string, JSON or bytes value
    /// above the inline threshold is chunked: its nodes are stored in this
    /// transaction and the cell holds their descriptor.
    fn bind_cell(&self, column: &crate::values::Column, value: &Value) -> Result<Sql, Error> {
        let mut inner = value;
        while let Value::Nullable(Some(value)) = inner {
            inner = value;
        }
        let logical = match inner {
            Value::String(text) => text.as_bytes(),
            Value::Bytes(bytes) => bytes.as_slice(),
            Value::Large(large) => return self.bind_large(column, large),
            _ => return Self::bind_value(value),
        };
        if logical.len() <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES
            || !crate::large::large_capable(column)
        {
            return Self::bind_value(value);
        }
        let prepared = jazz_types::large_values::prepare(crate::large::kind(column), logical)
            .map_err(|e| rejected(format!("`{}`: {e}", column.name)))?;
        crate::large::store(&self.conn, &self.chunks, &prepared)?;
        Ok(Sql::Text(crate::large::chunked_cell(&prepared.value_ref)?))
    }

    /// A chunked value written by reference: an edit's consolidated tree
    /// (stored now, inside the write's transaction), or a tree whose nodes
    /// this store already holds.
    fn bind_large(
        &self,
        column: &crate::values::Column,
        large: &jazz_types::large_values::LargeValueRef,
    ) -> Result<Sql, Error> {
        if !crate::large::large_capable(column) || large.kind != crate::large::kind(column) {
            return Err(rejected(format!(
                "`{}` does not hold {:?} large values",
                column.name, large.kind
            )));
        }
        if let Some(prepared) = self
            .pending_nodes
            .iter()
            .find(|prepared| prepared.value_ref.root == large.root)
        {
            crate::large::store(&self.conn, &self.chunks, prepared)?;
        } else if !crate::large::root_is_local(&self.conn, large)? {
            return Err(unsupported(
                "writing a large value whose chunks this store does not hold",
            ));
        }
        Ok(Sql::Text(crate::large::chunked_cell(large)?))
    }

    /// Every stored column of `table`, including ones only other schema
    /// views know: history and copy-on-write carry whole stored rows.
    fn columns_list(&self, table: &Table) -> String {
        let physical = self.physical.table(&table.name).unwrap_or(table);
        physical
            .stored()
            .map(|c| format!(", {}", quote(&c.name)))
            .collect()
    }

    /// Keep the superseded image (linear history: history holds superseded
    /// images only).
    fn snapshot(&self, table: &Table, row: RowUuid, key: &[u8], seq: u64) -> Result<(), Error> {
        let columns = self.columns_list(table);
        let sql = format!(
            "INSERT INTO {} (id, _b, _seq, _del{columns}) SELECT id, _b, _seq, _del{columns} FROM {} \
             WHERE id = ?1 AND _b = ?2 AND _seq <> ?3",
            quote(&format!("{}__h", table.name)),
            quote(&table.name)
        );
        // An image written earlier in the same commit is not superseded
        // history: history holds committed images only.
        self.conn
            .execute(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key, seq as i64],
            )
            .map_err(storage)?;
        Ok(())
    }

    /// Copy-on-write: materialize an inherited base row into the head.
    fn copy_on_write(
        &self,
        table: &Table,
        row: RowUuid,
        from: &[u8],
        to: &[u8],
        head_cells: &BTreeMap<String, Value>,
        seq: u64,
    ) -> Result<(), Error> {
        let columns = self.columns_list(table);
        let sql = format!(
            "INSERT INTO {name} (id, _b, _seq, _del{columns}) SELECT id, ?1, ?2, _del{columns} FROM {name} \
             WHERE id = ?3 AND _b = ?4",
            name = quote(&table.name)
        );
        self.conn
            .execute(
                &sql,
                rusqlite::params![to, seq as i64, row.0.as_bytes().to_vec(), from],
            )
            .map_err(storage)?;
        self.set_cells(table, row, to, head_cells, seq, None)
    }

    fn set_cells(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cells: &BTreeMap<String, Value>,
        seq: u64,
        deleted: Option<bool>,
    ) -> Result<(), Error> {
        let mut sets = vec!["_seq = ?1".to_owned()];
        let mut values = vec![Sql::Integer(seq as i64)];
        if let Some(deleted) = deleted {
            values.push(Sql::Integer(deleted as i64));
            sets.push(format!("_del = ?{}", values.len()));
        }
        for (name, value) in cells {
            values.push(match table.column(name) {
                Some(column) => self.bind_cell(column, value)?,
                None => Self::bind_value(value)?,
            });
            sets.push(format!("{} = ?{}", quote(name), values.len()));
        }
        for (name, value) in self.stamp()?.updated() {
            values.push(value);
            sets.push(format!("{} = ?{}", quote(name), values.len()));
        }
        values.push(Sql::Blob(row.0.as_bytes().to_vec()));
        values.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "UPDATE {} SET {} WHERE id = ?{} AND _b = ?{}",
            quote(&table.name),
            sets.join(", "),
            values.len() - 1,
            values.len()
        );
        self.conn
            .execute(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.name.clone(), row, key.to_vec()));
        Ok(())
    }

    fn insert(
        &mut self,
        table_name: &str,
        mut cells: BTreeMap<String, Value>,
        row_id: Option<RowUuid>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        Self::validate_cells(table, &cells)?;
        if let Target::Exact(selector) | Target::View { head: selector, .. } = target
            && !table.branch_by.is_empty()
        {
            for (column, value) in table.selector_cells(selector)? {
                match cells.get(&column) {
                    Some(given) if *given != value => {
                        return Err(rejected(format!(
                            "`{table_name}.{column}` disagrees with the branch target"
                        )));
                    }
                    _ => {
                        cells.insert(column, value);
                    }
                }
            }
        }
        let key = table.row_branch_key(&cells);
        let row = row_id.unwrap_or_else(|| self.ids.next_row_id());
        if self.existing(table_name, row, &key)?.is_some() {
            return Err(rejected(format!(
                "row {row:?} already exists in `{table_name}`"
            )));
        }
        let mut names = vec!["id".to_owned(), "_b".to_owned(), "_seq".to_owned()];
        let mut values = vec![
            Sql::Blob(row.0.as_bytes().to_vec()),
            Sql::Blob(key.clone()),
            Sql::Integer(seq as i64),
        ];
        for column in &table.columns {
            let value = match (cells.get(&column.name), &column.default) {
                (Some(value), _) => self.bind_cell(column, value)?,
                (None, Some(default)) => self.bind_cell(column, default)?,
                (None, None) if column.nullable() => Sql::Null,
                (None, None) => {
                    return Err(rejected(format!(
                        "`{table_name}.{}` is required",
                        column.name
                    )));
                }
            };
            names.push(quote(&column.name));
            values.push(value);
        }
        let stamp = self.stamp()?;
        // The update stamp stays NULL until the row is updated.
        for (name, value) in stamp.created() {
            names.push(quote(name));
            values.push(value);
        }
        let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote(table_name),
            names.join(", "),
            placeholders.join(", ")
        );
        self.conn
            .execute(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table_name.to_owned(), row, key.clone()));
        if let Who::Session(author) = who
            && !self.check(table, row, &key, author, Operation::Insert)?
        {
            return Err(rejected(format!(
                "INSERT policy denied the new row in `{table_name}`"
            )));
        }
        Ok(WriteHandle { row, seq })
    }

    /// Read-for-write: a hidden row is indistinguishable from an absent one,
    /// and omitted cells must not be merged blind.
    fn authorize_patch(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        who: &Who,
        verb: &str,
    ) -> Result<(), Error> {
        if let Who::Session(author) = who {
            if !self.check(table, row, key, *author, Operation::Select)? {
                return Err(rejected(format!(
                    "read policy denied {verb}: the target requires read permission"
                )));
            }
            if !self.check_clause(
                table,
                row,
                key,
                *author,
                table.policies.update.using.as_ref(),
            )? {
                return Err(rejected(format!(
                    "{verb} policy (USING) denied the target row"
                )));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn patch(
        &mut self,
        table_name: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
        upsert: bool,
    ) -> Result<WriteHandle, Error> {
        let verb = if upsert { "UPSERT" } else { "UPDATE" };
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        Self::validate_cells(table, &cells)?;
        let key = match self.locate(table, row, target)? {
            Located::Absent { cells: branch, .. } if upsert => {
                let mut cells = cells;
                cells.extend(branch);
                return self.insert(table_name, cells, Some(row), &Target::Root, who, seq);
            }
            Located::Absent { .. } | Located::Row { deleted: true, .. } if !upsert => {
                return Err(rejected(format!(
                    "{verb} target {row:?} does not exist in `{table_name}`"
                )));
            }
            Located::Row {
                key,
                seq: current,
                deleted,
            } => {
                if !deleted {
                    self.authorize_patch(table, row, &key, &who, verb)?;
                }
                if cells.is_empty() && !deleted {
                    return Ok(WriteHandle { row, seq: current });
                }
                self.snapshot(table, row, &key, seq)?;
                // An upsert over a tombstone replaces it.
                self.set_cells(table, row, &key, &cells, seq, deleted.then_some(false))?;
                key
            }
            Located::Inherited {
                from,
                to,
                head_cells,
            } => {
                self.authorize_patch(table, row, &from, &who, verb)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, seq)?;
                self.set_cells(table, row, &to, &cells, seq, None)?;
                to
            }
            Located::Absent { .. } => unreachable!("absent upserts insert"),
        };
        if let Who::Session(author) = who
            && !self.check_clause(
                table,
                row,
                &key,
                author,
                table.policies.update.with_check.as_ref(),
            )?
        {
            return Err(rejected(format!(
                "{verb} policy (WITH CHECK) denied the new row"
            )));
        }
        Ok(WriteHandle { row, seq })
    }

    fn update(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        self.patch(table, row, cells, target, who, seq, false)
    }

    fn upsert(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        self.patch(table, row, cells, target, who, seq, true)
    }

    fn delete(
        &mut self,
        table_name: &str,
        row: RowUuid,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        let check = |inner: &Self, key: &[u8]| -> Result<(), Error> {
            if let Who::Session(author) = who {
                if !inner.check(table, row, key, author, Operation::Select)? {
                    return Err(rejected(
                        "read policy denied DELETE: the target requires read permission",
                    ));
                }
                if !inner.check(table, row, key, author, Operation::Delete)? {
                    return Err(rejected("DELETE policy denied the target row"));
                }
            }
            Ok(())
        };
        match self.locate(table, row, target)? {
            Located::Row {
                key,
                deleted: false,
                ..
            } => {
                check(self, &key)?;
                self.snapshot(table, row, &key, seq)?;
                self.set_cells(table, row, &key, &BTreeMap::new(), seq, Some(true))?;
            }
            // Deleting an inherited row is a head register: a head tombstone
            // that masks the base row.
            Located::Inherited {
                from,
                to,
                head_cells,
            } => {
                check(self, &from)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, seq)?;
                self.set_cells(table, row, &to, &BTreeMap::new(), seq, Some(true))?;
            }
            _ => {
                return Err(rejected(format!(
                    "DELETE target {row:?} does not exist in `{table_name}`"
                )));
            }
        }
        Ok(WriteHandle { row, seq })
    }

    fn restore(
        &mut self,
        table_name: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        let Located::Row {
            key, deleted: true, ..
        } = self.locate(table, row, target)?
        else {
            return Err(rejected(format!(
                "RESTORE target {row:?} is not deleted in `{table_name}`"
            )));
        };
        self.snapshot(table, row, &key, seq)?;
        let mut cells = BTreeMap::new();
        if let Some(content) = &content {
            Self::validate_cells(table, content)?;
            for column in &table.columns {
                if table.branch_by.contains(&column.name) {
                    continue;
                }
                let value = match content.get(&column.name) {
                    Some(value) => value.clone(),
                    None if column.nullable() => Value::Nullable(None),
                    None => {
                        return Err(rejected(format!(
                            "`{table_name}.{}` is required",
                            column.name
                        )));
                    }
                };
                cells.insert(column.name.clone(), value);
            }
        }
        self.set_cells(table, row, &key, &cells, seq, Some(false))?;
        if let Who::Session(author) = who
            && !self.check(table, row, &key, author, Operation::Insert)?
        {
            return Err(rejected("RESTORE policy denied the restored row"));
        }
        Ok(WriteHandle { row, seq })
    }
}

/// Which branch a write addresses.
enum Target {
    Root,
    Exact(BranchSelector),
    View {
        head: BranchSelector,
        base: Option<BranchSelector>,
    },
}

impl Target {
    fn exact(target: &ExactWriteTarget) -> Self {
        match target {
            ExactWriteTarget::Root => Self::Root,
            ExactWriteTarget::Branch(branch) => Self::Exact(branch.clone()),
        }
    }

    fn of(target: &WriteTarget) -> Result<Self, Error> {
        Ok(match target {
            WriteTarget::Root => Self::Root,
            WriteTarget::BranchView { head, base } => Self::View {
                head: head.clone(),
                base: match base {
                    None => None,
                    Some(BranchViewBase::Current(base)) => Some(base.clone()),
                    Some(BranchViewBase::Snapshot { .. }) => {
                        return Err(unsupported("writes over frozen snapshot bases on SQLite"));
                    }
                },
            },
        })
    }
}

/// Where a patch-style write lands.
enum Located {
    Row {
        key: Vec<u8>,
        seq: u64,
        deleted: bool,
    },
    Inherited {
        from: Vec<u8>,
        to: Vec<u8>,
        head_cells: BTreeMap<String, Value>,
    },
    Absent {
        cells: BTreeMap<String, Value>,
    },
}

/// A staged mergeable transaction handle (`Db::mergeable_tx`). Writes are
/// staged in order and applied as one commit; each staged write sees the
/// earlier ones, as Jazz's transaction overlay does.
pub struct MergeableTxHandle<'a> {
    db: &'a Db,
    author: AuthorSubject,
    ops: RefCell<Vec<StagedOp>>,
}

#[derive(Clone, Debug)]
pub(crate) enum StagedOp {
    Insert(String, BTreeMap<String, Value>, InsertOptions),
    Update(String, RowUuid, BTreeMap<String, Value>, UpdateOptions),
    Upsert(String, RowUuid, BTreeMap<String, Value>, UpsertOptions),
    Delete(String, RowUuid, DeleteOptions),
    Restore(
        String,
        RowUuid,
        Option<BTreeMap<String, Value>>,
        RestoreOptions,
    ),
    Move(String, BranchSelector, BranchSelector, RowUuid),
}

impl Db {
    pub async fn mergeable_tx(&self) -> Result<MergeableTxHandle<'_>, Error> {
        let author = self.inner().identity.author;
        self.mergeable_tx_for_identity(author).await
    }

    pub async fn mergeable_tx_for_identity(
        &self,
        author: AuthorSubject,
    ) -> Result<MergeableTxHandle<'_>, Error> {
        Ok(MergeableTxHandle {
            db: self,
            author,
            ops: RefCell::new(Vec::new()),
        })
    }
}

impl Db {
    fn move_row(
        &self,
        table: &str,
        from: BranchSelector,
        to: BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.write_op(StagedOp::Move(table.to_owned(), from, to, row), Who::Local)
            .map(drop)
    }
}

impl Inner {
    /// A branch move: a source tombstone plus a destination insert (or a
    /// restore of an earlier destination tombstone), in one commit.
    fn move_row(
        &mut self,
        table: &str,
        from: BranchSelector,
        to: BranchSelector,
        row: RowUuid,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let schema = self.schema.clone();
        let t = schema.table(table)?;
        let key = t.selector_key(&from)?;
        let mut cells = BTreeMap::new();
        for column in &t.columns {
            if t.branch_by.contains(&column.name) {
                continue;
            }
            let sql = format!(
                "SELECT {} FROM {} WHERE id = ?1 AND _b = ?2 AND _del = 0",
                quote(&column.name),
                quote(table)
            );
            let cell: Sql = self
                .conn
                .query_row(
                    &sql,
                    rusqlite::params![row.0.as_bytes().to_vec(), key],
                    |r| r.get(0),
                )
                .map_err(|_| rejected(format!("move source {row:?} does not exist")))?;
            let value = from_sql(&column.ty, cell.as_ref(), &self.chunks)?;
            if let Some(value) = value {
                cells.insert(column.name.clone(), value);
            }
        }
        self.delete(table, row, &Target::Exact(from), Who::Local, seq)?;
        match self.locate(t, row, &Target::Exact(to.clone()))? {
            Located::Row { deleted: true, .. } => {
                self.restore(table, row, Some(cells), &Target::Exact(to), Who::Local, seq)
            }
            _ => self.insert(table, cells, Some(row), &Target::Exact(to), Who::Local, seq),
        }
    }
}

impl MergeableTxHandle<'_> {
    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        mut opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        let row = match opts.row_id {
            Some(row) => row,
            None => self.db.inner().ids.next_row_id(),
        };
        opts.row_id = Some(row);
        self.ops
            .borrow_mut()
            .push(StagedOp::Insert(table.to_owned(), cells, opts));
        Ok(WriteHandle { row, seq: 0 })
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<(), Error> {
        self.ops
            .borrow_mut()
            .push(StagedOp::Update(table.to_owned(), row, cells, opts));
        Ok(())
    }

    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<(), Error> {
        self.ops
            .borrow_mut()
            .push(StagedOp::Upsert(table.to_owned(), row, cells, opts));
        Ok(())
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<(), Error> {
        self.ops
            .borrow_mut()
            .push(StagedOp::Delete(table.to_owned(), row, opts));
        Ok(())
    }

    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<(), Error> {
        self.ops
            .borrow_mut()
            .push(StagedOp::Restore(table.to_owned(), row, content, opts));
        Ok(())
    }

    /// A branch move is an explicit source delete plus a destination
    /// insert (or restore of an earlier destination tombstone).
    pub async fn move_between_branches(
        &self,
        table: &str,
        source: BranchSelector,
        destination: BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.ops
            .borrow_mut()
            .push(StagedOp::Move(table.to_owned(), source, destination, row));
        Ok(())
    }

    pub async fn commit(self) -> Result<TxReceipt, Error> {
        let ops = self.ops.into_inner();
        let db = self.db;
        let ((), receipt) = db
            .transaction_for_identity(self.author, async |_tx| {
                for op in ops {
                    match op {
                        StagedOp::Insert(t, c, o) => db
                            .insert(
                                &t,
                                c,
                                InsertOptions {
                                    identity: WriteIdentity::Database,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Update(t, r, c, o) => db
                            .update(
                                &t,
                                r,
                                c,
                                UpdateOptions {
                                    identity: WriteIdentity::Database,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Upsert(t, r, c, o) => db
                            .upsert(
                                &t,
                                r,
                                c,
                                UpsertOptions {
                                    identity: WriteIdentity::Database,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Delete(t, r, o) => db
                            .delete(
                                &t,
                                r,
                                DeleteOptions {
                                    identity: WriteIdentity::Database,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Restore(t, r, c, o) => db
                            .restore(
                                &t,
                                r,
                                c,
                                RestoreOptions {
                                    identity: WriteIdentity::Database,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Move(t, from, to, r) => db.move_row(&t, from, to, r)?,
                    }
                }
                Ok(())
            })
            .await?;
        Ok(receipt)
    }
}

// ---- schema views ---------------------------------------------------------

/// Content-addressed identity of an exact typed schema view (structure,
/// defaults and policies), as in Jazz.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaViewId([u8; 32]);

impl SchemaViewId {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn for_schema(schema: &JazzSchema) -> Self {
        let bytes =
            serde_json::to_vec(schema.public_schema()).expect("public schema always serializes");
        Self(blake3::derive_key("jazz typed schema view id v1", &bytes))
    }
}

fn schema_error(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Schema, message)
}

/// The changes a schema view may make over the owner's schema without an
/// explicit lens: add and drop columns, add tables. Everything else names
/// what it changed (Jazz's `direct_schema_view_lens` rules).
fn check_direct_lens(source: &JazzSchema, target: &JazzSchema) -> Result<(), Error> {
    for source_table in source.tables() {
        let Some(target_table) = target.tables().iter().find(|t| t.name == source_table.name)
        else {
            continue;
        };
        let name = &target_table.name;
        if source_table.references != target_table.references {
            return Err(schema_error(format!(
                "schema view changes references on {name} without an explicit lens"
            )));
        }
        if source_table.merge_strategies != target_table.merge_strategies {
            return Err(schema_error(format!(
                "schema view changes merge strategies on {name} without an explicit lens"
            )));
        }
        if source_table.indexed_columns != target_table.indexed_columns
            || source_table.composite_indexes != target_table.composite_indexes
        {
            return Err(schema_error(format!(
                "schema view changes indices on {name} without explicit index admission"
            )));
        }
        for column in &target_table.columns {
            if let Some(source) = source_table.columns.iter().find(|c| c.name == column.name)
                && source.column_type != column.column_type
            {
                return Err(schema_error(format!(
                    "schema view changes type of {name}.{} without an explicit lens",
                    column.name
                )));
            }
        }
    }
    Ok(())
}

fn same_tables(
    left: &JazzSchema,
    right: &JazzSchema,
    same: impl Fn(&crate::schema::TableSchema, &crate::schema::TableSchema) -> bool,
) -> bool {
    left.tables().len() == right.tables().len()
        && left.tables().iter().all(|l| {
            right
                .tables()
                .iter()
                .any(|r| l.name == r.name && same(l, r))
        })
}

impl Db {
    /// Register a typed schema view. The handle shares this owner's store,
    /// batches, clock and subscriptions; it reads and writes its own columns
    /// of the physical tables, which hold the union of every admitted view.
    pub async fn register_schema_view(&self, schema: JazzSchema) -> Result<Db, Error> {
        let logical = {
            let mut inner = self.inner.borrow_mut();
            let version = schema.version_id();
            if let Some(admitted) = inner.admitted.iter().find(|a| a.version_id() == version) {
                if !same_tables(admitted, &schema, |l, r| {
                    l.read_policy == r.read_policy && l.write_policies == r.write_policies
                }) {
                    return Err(schema_error(
                        "schema view policy metadata conflicts with its admitted structural schema",
                    ));
                }
                if !same_tables(admitted, &schema, |l, r| {
                    l.indexed_columns == r.indexed_columns
                        && l.composite_indexes == r.composite_indexes
                }) {
                    return Err(schema_error(
                        "schema view index metadata conflicts with its admitted structural schema",
                    ));
                }
            } else {
                check_direct_lens(&inner.owner.jazz, &schema)?;
                inner.admitted.push(schema.clone());
            }
            let logical = Schema::new(&schema);
            let ddl = inner.physical.union(&logical)?;
            inner.conn.execute_batch(&ddl).map_err(storage)?;
            inner
                .views
                .entry(SchemaViewId::for_schema(&schema))
                .or_insert(schema);
            logical
        };
        Ok(Db {
            inner: self.inner.clone(),
            view: Some(Rc::new(logical)),
        })
    }

    pub async fn schema_view(&self, id: SchemaViewId) -> Result<Db, Error> {
        let schema = self
            .inner
            .borrow()
            .views
            .get(&id)
            .cloned()
            .ok_or_else(|| schema_error(format!("schema view {id:?} is not registered")))?;
        self.register_schema_view(schema).await
    }

    pub fn schema_view_id(&self) -> SchemaViewId {
        let inner = self.inner.borrow();
        SchemaViewId::for_schema(&self.view.as_ref().unwrap_or(&inner.owner).jazz)
    }
}

// ---- open transaction batches ---------------------------------------------

/// An open, owner-wide transaction batch. Writes are staged (with the schema
/// view that staged them) and applied at commit; reads overlay them.
struct Batch {
    kind: BatchKind,
    ops: Vec<(Rc<Schema>, StagedOp)>,
}

enum BatchKind {
    Mergeable,
    /// Serializable: reads see the store as of `snapshot` plus the batch's
    /// own writes; each read's snapshot result is kept for validation.
    /// On a client the snapshot is pinned by the first read that reaches
    /// the authority (the authority's state then), or stays the replica's
    /// known cut when the first read answers offline.
    Exclusive {
        snapshot: u64,
        reads: Vec<ReadRecord>,
        pinned: bool,
    },
}

/// One read of a serializable transaction: what it returned (row ids and
/// versions) at the snapshot, re-checked at commit.
struct ReadRecord {
    prepared: PreparedQuery,
    rows: Vec<(RowUuid, u64)>,
}

fn conflict(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::TransactionConflict, message)
}

fn versions(rows: &[CurrentRow]) -> Vec<(RowUuid, u64)> {
    rows.iter().map(|row| (row.id, row.seq)).collect()
}

impl StagedOp {
    fn table(&self) -> &str {
        match self {
            StagedOp::Insert(t, ..)
            | StagedOp::Update(t, ..)
            | StagedOp::Upsert(t, ..)
            | StagedOp::Delete(t, ..)
            | StagedOp::Restore(t, ..)
            | StagedOp::Move(t, ..) => t,
        }
    }

    fn row(&self) -> Option<RowUuid> {
        match self {
            StagedOp::Insert(_, _, opts) => opts.row_id,
            StagedOp::Update(_, r, ..)
            | StagedOp::Upsert(_, r, ..)
            | StagedOp::Delete(_, r, _)
            | StagedOp::Restore(_, r, ..)
            | StagedOp::Move(_, _, _, r) => Some(*r),
        }
    }
}

impl Inner {
    /// Apply one staged write as client-local staging (the authority checks
    /// policies on upload, as for Jazz's mergeable batches).
    fn apply(&mut self, op: &StagedOp, seq: u64) -> Result<WriteHandle, Error> {
        self.apply_as(op, Who::Local, seq)
    }

    /// Apply one write as `who`: policies apply for a session identity.
    fn apply_as(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        let author = match who {
            Who::Session(author) => author,
            Who::Local => self.identity.author,
        };
        let (last_seq, last_at) = self.last_stamp;
        let at = if seq == last_seq {
            last_at
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64);
            now.max(last_at + 1)
        };
        self.last_stamp = (seq, at);
        self.writer = Some(Stamp {
            at,
            author: self.author_code(&author.principal_parts())?,
        });
        let result = self.apply_op(op, who, seq);
        self.writer = None;
        result
    }

    fn apply_op(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        match op.clone() {
            StagedOp::Insert(t, c, o) => {
                self.insert(&t, c, o.row_id, &Target::exact(&o.target), who, seq)
            }
            StagedOp::Update(t, r, c, o) => {
                self.update(&t, r, c, &Target::of(&o.target)?, who, seq)
            }
            StagedOp::Upsert(t, r, c, o) => {
                self.upsert(&t, r, c, &Target::of(&o.target)?, who, seq)
            }
            StagedOp::Delete(t, r, o) => self.delete(&t, r, &Target::of(&o.target)?, who, seq),
            StagedOp::Restore(t, r, c, o) => {
                self.restore(&t, r, c, &Target::exact(&o.target), who, seq)
            }
            StagedOp::Move(t, from, to, r) => self.move_row(&t, from, to, r, seq),
        }
    }

    /// Apply staged writes at `seq`, each under the schema that staged it.
    fn apply_all(&mut self, ops: &[(Rc<Schema>, StagedOp)], seq: u64) -> Result<(), Error> {
        let active = self.schema.clone();
        let result = ops.iter().try_for_each(|(schema, op)| {
            self.schema = schema.clone();
            self.apply(op, seq).map(drop)
        });
        self.schema = active;
        result
    }

    /// The newest committed version of `row` in `table` on any branch.
    fn newest_seq(&self, table: &str, row: RowUuid) -> Result<Option<u64>, Error> {
        let sql = format!("SELECT MAX(_seq) FROM {} WHERE id = ?1", quote(table));
        self.conn
            .query_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                r.get::<_, Option<i64>>(0)
            })
            .map(|seq| seq.map(|seq| seq as u64))
            .map_err(storage)
    }
}

/// Staging and reading handle for one open batch, through one schema view.
pub struct TxRef<'a> {
    db: &'a Db,
    id: OpenTransactionId,
}

pub type MergeableTxRef<'a> = TxRef<'a>;
pub type ExclusiveTxRef<'a> = TxRef<'a>;

impl Db {
    pub async fn begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.begin(id, BatchKind::Mergeable)
    }

    /// A serializable transaction: snapshot reads, validated at commit.
    /// On a client the snapshot is the server seq the replica is known
    /// complete through.
    pub async fn begin_exclusive(&self, id: OpenTransactionId) -> Result<(), Error> {
        let snapshot = {
            let inner = self.inner.borrow();
            inner
                .client
                .as_ref()
                .map_or(inner.seq, |client| client.known)
        };
        self.begin(
            id,
            BatchKind::Exclusive {
                snapshot,
                reads: Vec::new(),
                pinned: !self.is_client(),
            },
        )
    }

    fn begin(&self, id: OpenTransactionId, kind: BatchKind) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if inner.batches.contains_key(&id) {
            return Err(rejected(format!("transaction {id:?} is already open")));
        }
        inner.batches.insert(
            id,
            Batch {
                kind,
                ops: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn mergeable_tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
        TxRef { db: self, id }
    }

    pub fn exclusive_tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
        TxRef { db: self, id }
    }

    pub fn abandon_transaction_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.inner
            .borrow_mut()
            .batches
            .remove(&id)
            .map(drop)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))
    }

    pub fn abandon_exclusive_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.abandon_transaction_handle(id)
    }

    fn take_batch(&self, id: OpenTransactionId) -> Result<Batch, Error> {
        self.inner
            .borrow_mut()
            .batches
            .remove(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))
    }

    pub async fn commit_mergeable_handle(&self, id: OpenTransactionId) -> Result<TxReceipt, Error> {
        let batch = self.take_batch(id)?;
        self.commit_ops(&batch.ops, sync::Kind::Mergeable)
    }

    /// Serializable commit: every row the batch writes must still be at a
    /// version at or before the snapshot, and every read must return the
    /// same rows at the same versions now as it did at the snapshot (this
    /// catches phantoms: rows that entered or left a predicate or window).
    /// The check and the apply run under one store borrow, so nothing can
    /// commit in between.
    pub async fn commit_exclusive_handle(&self, id: OpenTransactionId) -> Result<TxReceipt, Error> {
        let batch = self.take_batch(id)?;
        let BatchKind::Exclusive {
            snapshot, reads, ..
        } = &batch.kind
        else {
            return Err(rejected(format!("transaction {id:?} is not serializable")));
        };
        // On a client the authority validates, against its own state; the
        // commit applies optimistically here meanwhile.
        if self.is_client() {
            let reads = reads
                .iter()
                .map(|read| sync::WireRead {
                    query: read.prepared.query.clone(),
                    binding: read.prepared.binding.clone(),
                    rows: read
                        .rows
                        .iter()
                        .map(|(row, seq)| (*row.0.as_bytes(), *seq))
                        .collect(),
                })
                .collect();
            return self.commit_ops(
                &batch.ops,
                sync::Kind::Serializable {
                    snapshot: *snapshot,
                    reads,
                },
            );
        }
        {
            let inner = self.inner();
            for (_, op) in &batch.ops {
                if let Some(row) = op.row()
                    && inner
                        .newest_seq(op.table(), row)?
                        .is_some_and(|seq| seq > *snapshot)
                {
                    return Err(conflict(
                        "row visible parent changed since transaction write was staged",
                    ));
                }
            }
            for read in reads {
                let now = inner.run(&read.prepared, None, &ReadSpec::default())?;
                if versions(&now) != read.rows {
                    return Err(conflict(
                        "a row read by the transaction changed since its snapshot",
                    ));
                }
            }
        }
        self.commit_ops(&batch.ops, sync::Kind::Mergeable)
    }

    fn commit_ops(
        &self,
        ops: &[(Rc<Schema>, StagedOp)],
        kind: sync::Kind,
    ) -> Result<TxReceipt, Error> {
        self.with_upload(|| self.commit_ops_now(ops, kind))
    }

    fn commit_ops_now(
        &self,
        ops: &[(Rc<Schema>, StagedOp)],
        kind: sync::Kind,
    ) -> Result<TxReceipt, Error> {
        let seq = {
            let mut inner = self.inner();
            let seq = inner.seq + 1;
            inner
                .conn
                .execute_batch("SAVEPOINT commit_batch")
                .map_err(storage)?;
            let mark = inner.changed.borrow().len();
            if let Err(error) = inner.apply_all(ops, seq) {
                inner
                    .conn
                    .execute_batch("ROLLBACK TO commit_batch; RELEASE commit_batch")
                    .map_err(storage)?;
                inner.changed.borrow_mut().truncate(mark);
                return Err(error);
            }
            inner
                .conn
                .execute_batch("RELEASE commit_batch")
                .map_err(storage)?;
            if !ops.is_empty() {
                inner.seq = seq;
            }
            seq
        };
        self.record_upload(ops.iter().map(|(_, op)| op.clone()).collect(), seq, 0, kind);
        self.notify();
        Ok(TxReceipt(seq))
    }

    /// Run `read` over the batch's staged writes (plus `extra`), then undo
    /// them: a savepoint replay is the overlay. Serializable batches read
    /// through the snapshot cut, which also admits their own writes.
    fn in_overlay<R>(
        &self,
        id: OpenTransactionId,
        extra: Option<StagedOp>,
        read: impl FnOnce(&Inner, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut inner = self.inner();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        let mut ops = batch.ops.clone();
        let snapshot = match &batch.kind {
            BatchKind::Mergeable => None,
            BatchKind::Exclusive { snapshot, .. } => Some(*snapshot),
        };
        if let Some(op) = extra {
            ops.push((inner.schema.clone(), op));
        }
        let own = inner.seq + 1;
        inner
            .conn
            .execute_batch("SAVEPOINT overlay")
            .map_err(storage)?;
        let mark = inner.changed.borrow().len();
        let result = inner
            .apply_all(&ops, own)
            .and_then(|()| read(&inner, snapshot.map(|seq| Cut { seq, own })));
        inner
            .conn
            .execute_batch("ROLLBACK TO overlay; RELEASE overlay")
            .map_err(storage)?;
        inner.changed.borrow_mut().truncate(mark);
        result
    }

    fn stage(&self, id: OpenTransactionId, op: StagedOp) -> Result<(), Error> {
        // Validate against the overlay now, so errors surface at the call.
        self.in_overlay(id, Some(op.clone()), |_, _| Ok(()))?;
        let mut inner = self.inner();
        let schema = inner.schema.clone();
        inner
            .batches
            .get_mut(&id)
            .expect("validated")
            .ops
            .push((schema, op));
        Ok(())
    }

    fn record_read(&self, id: OpenTransactionId, prepared: &PreparedQuery) -> Result<(), Error> {
        let mut inner = self.inner();
        let Some(Batch {
            kind: BatchKind::Exclusive { snapshot, .. },
            ..
        }) = inner.batches.get(&id)
        else {
            return Ok(());
        };
        let read = ReadSpec {
            cut: Some(Cut {
                seq: *snapshot,
                own: 0,
            }),
            ..ReadSpec::default()
        };
        let rows = versions(&inner.run(prepared, None, &read)?);
        if let Some(Batch {
            kind: BatchKind::Exclusive { reads, .. },
            ..
        }) = inner.batches.get_mut(&id)
        {
            reads.push(ReadRecord {
                prepared: prepared.clone(),
                rows,
            });
        }
        Ok(())
    }
}

impl TxRef<'_> {
    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        mut opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        let row = match opts.row_id {
            Some(row) => row,
            None => self.db.inner().ids.next_row_id(),
        };
        opts.row_id = Some(row);
        self.db
            .stage(self.id, StagedOp::Insert(table.to_owned(), cells, opts))?;
        Ok(WriteHandle { row, seq: 0 })
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.db.stage(
            self.id,
            StagedOp::Update(table.to_owned(), row, cells, opts),
        )?;
        Ok(WriteHandle { row, seq: 0 })
    }

    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<WriteHandle, Error> {
        self.db.stage(
            self.id,
            StagedOp::Upsert(table.to_owned(), row, cells, opts),
        )?;
        Ok(WriteHandle { row, seq: 0 })
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        self.db
            .stage(self.id, StagedOp::Delete(table.to_owned(), row, opts))?;
        Ok(WriteHandle { row, seq: 0 })
    }

    pub async fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        // A client hydrates a serializable read from the authority at the
        // snapshot first, unless the link cannot answer (then the replica
        // answers and the authority's validation decides).
        let snapshot = match self.db.inner.borrow().batches.get(&self.id) {
            Some(Batch {
                kind:
                    BatchKind::Exclusive {
                        snapshot, pinned, ..
                    },
                ..
            }) => Some((*snapshot, *pinned)),
            _ => None,
        };
        if let Some((snapshot, pinned)) = snapshot
            && self.db.is_client()
        {
            let at = pinned.then_some(snapshot);
            Db::remote_shape(&prepared.query)?;
            let through = self.db.fetch(prepared, at, None).await;
            if let Some(Batch {
                kind:
                    BatchKind::Exclusive {
                        snapshot, pinned, ..
                    },
                ..
            }) = self.db.inner.borrow_mut().batches.get_mut(&self.id)
                && !*pinned
            {
                // Pinned by the authority's answer, or (offline) at the
                // replica's known cut.
                if let Some(through) = through {
                    *snapshot = through;
                }
                *pinned = true;
            }
        }
        let rows = self.db.in_overlay(self.id, None, |inner, cut| {
            inner.run(
                prepared,
                None,
                &ReadSpec {
                    cut,
                    ..ReadSpec::default()
                },
            )
        })?;
        self.db.record_read(self.id, prepared)?;
        Ok(rows)
    }

    /// A point read is a one-row query, so it validates like any other read.
    pub async fn read(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let query = Query::from(table).filter(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(Value::Uuid(row.0)),
        ));
        let prepared = self.db.prepare_query(&query)?;
        let rows = self.all_prepared(&prepared).await?;
        Ok(rows.into_iter().next().map(|row| {
            row.cells
                .iter()
                .filter_map(|(name, value)| Some((name.clone(), value.clone()?)))
                .collect()
        }))
    }
}

impl Db {
    /// Local commits are final at once here (there is no authority fate to
    /// wait for), so this only checks the id names a commit.
    pub fn finalize_local_mergeable_commit_for_test(
        &self,
        tx: impl Into<u64>,
    ) -> Result<(), Error> {
        let tx = tx.into();
        if tx == 0 || tx > self.inner.borrow().seq {
            return Err(rejected(format!("no local commit {tx}")));
        }
        Ok(())
    }
}

/// An owned serializable transaction (`Db::exclusive_tx`). Reads and writes
/// go through the `TxRef` it derefs to; dropping it uncommitted abandons it.
pub struct ExclusiveTx<'a> {
    tx: TxRef<'a>,
    committed: bool,
}

impl<'a> std::ops::Deref for ExclusiveTx<'a> {
    type Target = TxRef<'a>;

    fn deref(&self) -> &TxRef<'a> {
        &self.tx
    }
}

impl ExclusiveTx<'_> {
    pub async fn commit(mut self) -> Result<TxReceipt, Error> {
        let receipt = self.tx.db.commit_exclusive_handle(self.tx.id).await;
        // A failed commit has already taken the batch, so there is nothing
        // left to abandon either way.
        self.committed = true;
        receipt
    }
}

impl Drop for ExclusiveTx<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.tx.db.abandon_transaction_handle(self.tx.id);
        }
    }
}

impl Db {
    pub async fn exclusive_tx(&self) -> Result<ExclusiveTx<'_>, Error> {
        let id = OpenTransactionId::new();
        self.begin_exclusive(id).await?;
        Ok(ExclusiveTx {
            tx: self.exclusive_tx_ref(id),
            committed: false,
        })
    }
}
