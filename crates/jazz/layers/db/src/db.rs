//! The `Db` facade: Jazz's public database API over one SQLite connection.

use crate::values::CachedRow;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, RowUuid};
use crate::protocol::{
    BranchSelector, BranchViewBase, CurrentWriteSchema, LensOp, MigrationLens, ReadViewSourceSpec,
    SchemaVersion,
};
use crate::query::Query;
use crate::records::{Value, ValueType};
use crate::schema::{JazzSchema, MergeStrategy};
use crate::tools::OpenTransactionId;
use crate::tools::Operation;
use crate::tx::DurabilityTier;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params_from_iter};

pub use crate::options::{
    DbConfig, DbIdentity, DeleteOptions, Error, ErrorCode, ExactWriteTarget, InsertOptions,
    Migrations, ReadOpts, ReadTier, Reason, RestoreOptions, RowIdSource, SeededRowIdSource,
    UpdateOptions, UpsertOptions, WriteIdentity, WriteTarget, block_on,
};

use crate::compile::{Compiled, Ctx, Cut, Overlay as CutOverlay, Session, View};
use crate::tools::{CmpOp, PolicyExpr};

#[path = "fanout.rs"]
mod fanout;
pub use fanout::FanoutStats;
#[path = "large_ops.rs"]
mod large_ops;
pub(crate) use large_ops::byte_splices;
pub use large_ops::{
    JsonSetEdit, LargeValueDeltaSplice, LargeValueStagingPolicy, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, StreamingMutationKind, StreamingValueUpload,
};
#[path = "sync.rs"]
pub(crate) mod sync;

#[path = "shard.rs"]
pub mod shard;
pub use crate::names::LensEffects;
pub use crate::options::REMOTE_LINK_ATTEMPT_WINDOW;
use crate::result_tree::{ResultNode, ResultRelation, ResultTree};
pub use crate::values::CurrentRow;
use crate::values::{
    AuthorKey, STATE_AS_SENT, STATE_DELETED, STATE_NO_CONTENT, Schema, Sql, Table, author_key,
    begin_savepoint, cell_from_sql, column_from_sql, end_savepoint, in_savepoint, quote, storage,
    to_sql, unsupported,
};
pub use sync::{
    AccessProof, Acting, BuiltOn, Cell as SyncCell, CellDelta, CorpusReader, FirstLoad,
    FrameJoiner, Image, LEAF, Outcome, Pos, RELAY, ReadSpan, RejectedTransaction, RelationSnapshot,
    RemoteLinkHint, SerializedReadResult, SubscriberId, SyncMessage, Transport, TransportError,
    TxName, Upstream, WireBase, WireChunk, WireChunkRef, WireDelta, WireError, WireKind, WireOp,
    WirePull, WireTarget, WireTransport, WireTransportAdapter, WireWrite, WriteConflict,
    decode_message, duplex, encode_frames, validate_frame_batch_for_corpus,
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
    /// The same query without its window (limit and offset).
    pub(crate) fn unwindowed(&self) -> PreparedQuery {
        let mut query = self.query.clone();
        query.limit = None;
        query.offset = 0;
        let shape = blake3::hash(format!("{:?}unwindowed", self.shape.0).as_bytes());
        PreparedQuery {
            query,
            shape: Shape(short(shape)),
            ..self.clone()
        }
    }

    /// The same query narrowed to rows `ids`.
    pub(crate) fn narrowed(&self, ids: &[RowUuid]) -> PreparedQuery {
        use crate::query::{Operand, Predicate};
        let mut query = self.query.clone();
        query.filters.push(Predicate::In(
            Operand::Column("id".to_owned()),
            ids.iter()
                .map(|id| Operand::Literal(Value::Uuid(id.0)))
                .collect(),
        ));
        let shape = blake3::hash(format!("{query:?}").as_bytes());
        PreparedQuery {
            query,
            shape: Shape(short(shape)),
            ..self.clone()
        }
    }

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
    /// Where the row version this write left is.
    seq: Pos,
}

impl WriteHandle {
    pub fn row_uuid(&self) -> RowUuid {
        self.row
    }

    /// The commit sequence number of the row version this write left.
    pub fn mergeable_tx_id(&self) -> u64 {
        self.seq.seq
    }

    /// The position of the row version this write left.
    pub fn version(&self) -> Pos {
        self.seq
    }

    /// Wait for the write at `tier`. A write is locally durable once the
    /// call that made it returns, so `Local` and `None` are met at once; an
    /// upstream tier is waited for with [`Db::wait_for_transaction`].
    pub async fn wait(&self, tier: DurabilityTier) -> Result<(), Error> {
        match tier {
            DurabilityTier::None | DurabilityTier::Local => Ok(()),
            DurabilityTier::Global => Err(Error::new(
                ErrorCode::Protocol,
                "a write handle cannot wait for Global durability: use \
                 Db::wait_for_transaction",
            )),
        }
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
    /// For an updated row, each [`ReadOpts::splices`] column's change from
    /// the row's previous result, when both versions are still held and
    /// readable. A column left out changed in a way the reader takes whole.
    pub splices: Vec<(String, Vec<LargeValueDeltaSplice>)>,
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
    /// A flat join's joined source rows ([`CurrentRow::joined_rows`]).
    pub joined: Vec<crate::values::JoinedRow>,
    /// A flat join's root version ([`CurrentRow::root_version`]).
    pub version: Option<crate::values::SourceVersion>,
    /// A union read's arm ([`CurrentRow::union_arm`]).
    pub union_arm: Option<Arc<str>>,
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
    /// A replica's `ReadTier::Remote` read: its pending local versions
    /// (at its own node) are left out, and a row they changed
    /// reads as its settled version, as the server lists it. Unlike a
    /// [`ReadSpec::cut`] it stays live.
    settled_only: bool,
    view: Option<View>,
    cut: Option<Cut>,
    /// The upstream position the read observes (see
    /// [`Inner::visible_read`]).
    position: Position,
    /// Columns whose changes updated rows carry as splices
    /// ([`ReadOpts::splices`]).
    splices: Vec<String>,
}

/// The upstream position a replica's read observes. Its upstream settles
/// each read apart (SPEC 8 §8.4), so its store can hold accepted versions
/// past the position a result is settled through; a read naming a position
/// reads as of it while the store does, plus this replica's own writes.
#[derive(Clone, Debug, Default, PartialEq)]
enum Position {
    /// The store's current state: an authority's read, a session's read
    /// served by this store, an internal one.
    #[default]
    Current,
    /// What the replica knows its upstream through (`known`): its own
    /// one-shot reads.
    Known,
    /// A live result's: the upstream read (by its key) it is a result of,
    /// as far as its own settle (or the connection's, when further).
    Remote(String),
}

impl ReadSpec {
    fn of(opts: &ReadOpts, inner: &Inner) -> Result<Self, Error> {
        let mut cut = None;
        let view = match &opts.read_view.source {
            ReadViewSourceSpec::Current => None,
            ReadViewSourceSpec::BranchView { head, base } => {
                let (base, base_cut) = match base {
                    None => (None, None),
                    Some(BranchViewBase::Current(base)) => (Some(base.clone()), None),
                    Some(BranchViewBase::Snapshot { branch, snapshot }) => (
                        Some(branch.clone()),
                        Some(inner.resolve_snapshot(snapshot)?),
                    ),
                };
                Some(View {
                    head: head.clone(),
                    base,
                    base_cut,
                })
            }
            ReadViewSourceSpec::Snapshot { snapshot } => {
                cut = Some(inner.resolve_snapshot(snapshot)?);
                None
            }
        };
        if opts.tier == ReadTier::Remote {
            inner.check_not_stranded()?;
        }
        Ok(Self {
            include_deleted: opts.read_view.include_deleted,
            settled_only: inner.client.is_some() && opts.tier == ReadTier::Remote,
            view,
            cut,
            position: if inner.client.is_some() {
                Position::Known
            } else {
                Position::Current
            },
            splices: opts.splices.clone(),
        })
    }

    /// Reads a frozen state: answered locally, never fetched.
    fn frozen(&self) -> bool {
        self.cut.is_some() || self.view.as_ref().is_some_and(|v| v.base_cut.is_some())
    }
}

/// A live subscription. Dropping it ends the subscription.
pub struct SubscriptionStream {
    queue: fanout::Queue,
    dropped: Rc<std::cell::Cell<usize>>,
    /// A withheld opening, released as one reset of the current rows.
    gate: Option<sync::Gate>,
    group: Weak<RefCell<fanout::Group>>,
    /// Why the server refused the remote read, once it has; reported once.
    refused: Option<Rc<RefCell<Option<Error>>>>,
    /// The remote attachment to release on drop.
    remote: Option<(
        Rc<Store>,
        Query,
        BTreeMap<String, Value>,
        bool,
        ReadViewSourceSpec,
        sync::RemoteReach,
    )>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
        if let Some((inner, query, binding, include_deleted, view, reach)) = self.remote.take() {
            Db { inner, view: None }.detach_remote(&query, &binding, include_deleted, &view, reach);
        }
    }
}

impl SubscriptionStream {
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        let reason = self
            .refused
            .as_ref()
            .and_then(|refused| refused.borrow().as_ref().map(Error::to_string));
        if let Some(reason) = reason {
            self.refused = None;
            self.gate = None;
            return Some(SubscriptionEvent::Rejected { reason });
        }
        if let Some(gate) = &self.gate {
            if !gate.open() {
                return None;
            }
            // The opening lists rows whose large values are complete: while
            // their chunks may still arrive, it waits for them.
            if gate.may_still_wait()
                && self
                    .group
                    .upgrade()
                    .is_some_and(|group| group.borrow().awaiting())
            {
                return None;
            }
            let settled = gate.answered.get();
            let global = settled && gate.global.get();
            self.gate = None;
            self.queue.borrow_mut().clear();
            let group = self.group.upgrade()?;
            let mut event = fanout::opening(&group.borrow().rows);
            if let SubscriptionEvent::Delta {
                settled: flag,
                tier,
                ..
            } = &mut event
            {
                *flag = settled;
                if global {
                    *tier = DurabilityTier::Global;
                }
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
}

/// A schema lineage step authored for publication.
#[derive(Clone, Debug)]
pub struct SchemaPublication {
    version: SchemaVersion,
    lens: MigrationLens,
}

struct Inner {
    conn: Connection,
    /// One-way, owner-wide fence for new application mutations during close.
    mutation_admission_closed: bool,
    /// Errors the next ticks fail with, for tests of a tick driver.
    #[cfg(any(test, feature = "testing"))]
    failing_ticks: VecDeque<ErrorCode>,
    /// Large-value chunks: the cache the collation and decoders read.
    chunks: Arc<crate::large::Chunks>,
    /// Nodes a large-value edit consolidated, stored when the edited cell
    /// is bound inside the write's transaction.
    pending_nodes: Vec<jazz_types::large_values::PreparedLargeValue>,
    /// Applying writes made over an older image (an authority ingesting
    /// an upload, a replica replaying its pending writes): an edit or a
    /// delete of a row deleted since applies and leaves it deleted (SPEC 4,
    /// INV-HIST-8). A fresh local write over a tombstone it can see is
    /// refused instead.
    merging: bool,
    /// On the authority, what each row of the upload being applied was
    /// built on (SPEC 4 §4.6), by (stored table, row, branch key): a text
    /// merge reads its base from it. Empty otherwise.
    merge_bases: HashMap<(String, RowUuid, Vec<u8>), sync::Built>,
    /// The values as sent of the upload's text writes, by row version and
    /// stored column: for a write its merge changed, the value and its
    /// bound cell (kept by [`Inner::record_as_sent`]); `None` for one left
    /// as sent.
    as_sent: RefCell<AsSent>,
    /// A transaction handle's staged writes left applied between its
    /// stages (see [`Db::in_overlay`]); undone by [`Store`] before any
    /// other use of the store.
    overlay: Option<Overlay>,
    /// Descriptors this store's own edit derived for the write in flight:
    /// trusted as built, so binding them re-validates nothing.
    derived: Vec<jazz_types::large_values::LargeValueRef>,
    /// What the session whose commit is being applied holds (SPEC 19.4):
    /// set while a synced commit applies, `None` for local writes.
    pub(crate) possession: Option<crate::large::Possession>,
    /// Limits on local streaming uploads and on pushed nodes awaiting
    /// their commit.
    staging: LargeValueStagingPolicy,
    /// Dropped uploads queue cleanup without recursively borrowing the owner.
    upload_cleanup: Rc<RefCell<Vec<[u8; 32]>>>,
    /// `conn.total_changes()` after the last large-value reclaim pass that
    /// committed and left nothing queued: until a row changes again (a
    /// trigger's change counts), a pass would find nothing. `None` runs the
    /// next pass, as an opened store does, for work a crash left queued.
    reclaimed_at: Option<u64>,
    /// Large-value reclaim passes run (diagnostics).
    reclaim_passes: u64,
    /// The staging ingress window: (start ms, encoded bytes admitted).
    ingress: RefCell<(u64, u64)>,
    /// The schema the current operation runs under: the owner's, or the
    /// schema view's that issued it.
    schema: Rc<Schema>,
    /// The owner's current schema.
    owner: Rc<Schema>,
    /// `owner.jazz.version_id()`, which hashes the whole schema.
    pub(crate) owner_version: crate::ids::SchemaVersionId,
    /// The stored tables: the union of every admitted schema, keyed and
    /// named by stored names.
    physical: Schema,
    /// The current schema named by stored names, which replicated writes
    /// apply under ([`Schema::stored_view`]), keyed by the owner it was
    /// built from and how many stored columns there were.
    stored_view: RefCell<Option<(*const Schema, usize, Rc<Schema>)>>,
    /// Admitted structural schemas with their version ids (hashing a
    /// whole schema is too slow to repeat per session per tick).
    admitted: Vec<(crate::ids::SchemaVersionId, JazzSchema)>,
    /// Where each admitted version stores its tables and columns.
    names: crate::names::Names,
    views: BTreeMap<SchemaViewId, JazzSchema>,
    /// Open transaction batches, by id.
    batches: HashMap<OpenTransactionId, Batch>,
    identity: DbIdentity,
    /// The node SYSTEM provenance names ([`crate::options::DbConfig::system_node`]).
    system_node: crate::ids::NodeUuid,
    /// Provenance for the write being applied (set by `apply_as`).
    writer: Option<Stamp>,
    /// The commit seq and time of the last local stamp: writes of one commit
    /// share a time, and later commits get strictly later ones, so
    /// `$createdAt` orders this node's writes even within one millisecond.
    last_stamp: (u64, i64),
    /// Interned author codes (see [`Inner::author_code`]), cached both ways.
    authors: RefCell<AuthorCodes>,
    /// By stored table and whether into its history: the columns the last
    /// image stored there had, and the insert that stores them. A sync
    /// batch stores thousands of images of a few tables.
    image_inserts: RefCell<HashMap<(String, bool), (Vec<String>, String)>>,
    /// While a replica stores a message's images: their rows' current
    /// images, read at once (see `Inner::begin_current_batch`).
    current_batch: RefCell<Option<HashMap<(String, RowUuid, Vec<u8>), Option<sync::Image>>>>,
    ids: Box<dyn RowIdSource>,
    /// The newest commit seq handed out: the authority's seq, or on a
    /// replica its own node's counter.
    seq: u64,
    /// A replica's own node code (`_n` of its unsettled versions, see
    /// [`sync::RELAY`]); 0 until it becomes one.
    pub(crate) me: i64,
    /// Its clock and holds as a shard ([`shard`]); inert unsharded.
    pub(crate) shard: shard::ShardState,
    pub(crate) claims: HashMap<AuthorSubject, BTreeMap<String, Value>>,
    /// Live subscription groups and their invalidation index.
    fan: fanout::Fanout,
    /// Rows written since the last dispatch: (table, id, branch key).
    changed: RefCell<Vec<(String, RowUuid, Vec<u8>)>>,
    /// The write checks of the unit being applied, run at its end.
    deferred: RefCell<Option<Vec<DeferredCheck>>>,
    /// The open transaction's seq and the tables it wrote.
    tx: Option<(u64, BTreeSet<String>)>,
    publications: Vec<SchemaPublication>,
    /// Set once connected upstream: this store is a replica.
    client: Option<sync::Client>,
    /// A replica whose store does not outlive it (a foreground over a
    /// durable relay): its local reads wait for the upstream's view too.
    non_durable: bool,
    /// Rows the authority named as written for this replica's own commits
    /// whose version was not this replica's image of them (see
    /// `Inner::as_written_images`), asked for whole instead.
    pub(crate) as_written_mismatches: u64,
    /// The global authority: what it settles is durable at `Global`.
    authority: bool,
    /// Uploads the authority refused, by commit seq, kept for a host that
    /// reports them (see [`Db::track_upstream_rejections`]).
    upstream_rejections: Option<Vec<UpstreamRejection>>,
    /// Clients this store serves.
    sessions: Vec<sync::Session>,
    /// Each stored table's column positions, the codes authored-column
    /// bitmasks use (SPEC 4 §4.6), read on first use.
    column_codes: RefCell<HashMap<String, HashMap<String, u32>>>,
    /// Sessions the server ended, with why, until the host takes them
    /// ([`Db::take_closed_subscribers`]). Kept only for a host that does
    /// ([`Db::track_closed_subscribers`]), so that one which never takes
    /// them does not accumulate them.
    closed: Option<Vec<(sync::SubscriberId, Error)>>,
    /// Query statements that ran slow, until the host takes them
    /// ([`Db::take_slow_queries`]).
    slow: RefCell<crate::slow::SlowLog>,
    /// Subscribers accepted so far (each one's generation).
    accepted: u64,
    /// Rows written since the last server tick (only while serving).
    since_tick: Vec<(String, RowUuid, Vec<u8>)>,
    /// Ops of the open local transaction, for upload, by stored names.
    pub(crate) capture: Vec<StagedOp>,
    /// Images a replica dropped since the last dispatch (evictions and
    /// rebases), for keyed invalidation.
    gone: RefCell<Vec<sync::Image>>,
    /// The app's versions and lenses (see [`DbConfig::migrations`]): a
    /// version this store lacks is admitted along them.
    migrations: Migrations,
    /// Whether `_jazz_withheld` exists (see `sync::WITHHELD_DDL`): branch
    /// views over a base read it only then.
    withheld: std::cell::Cell<bool>,
}

/// Large-value nodes one tick may reclaim.
const RECLAIM_NODES_PER_TICK: usize = 256;

/// Leaves one tick may move to, or delete from, the chunk store.
const CHUNK_STORE_NODES_PER_TICK: usize = 256;

/// Chunk-store operations in flight at once.
const CHUNK_STORE_CONCURRENCY: usize = 16;

/// File store settings.
#[derive(Clone, Copy, Debug)]
pub struct FileOptions {
    /// `synchronous = FULL` (every commit survives power loss) instead of
    /// `NORMAL` (WAL stays consistent; the newest commits may be lost on
    /// power loss, never on a process crash). `NORMAL` is the default.
    pub synchronous_full: bool,
    /// Page cache bound per connection, in KiB.
    pub cache_kib: u64,
    /// SQLite VFS the file lives on (`None`: the platform default). Browser
    /// builds register theirs (IndexedDB, OPFS) before opening.
    pub vfs: Option<&'static str>,
    pub journal: Journal,
    /// Page size a new file is created with (`None`: SQLite's default). A
    /// file that exists keeps the size it was created with.
    pub page_size: Option<u32>,
}

/// How the file journals its commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Journal {
    /// Write-ahead log: readers never block the writer.
    #[default]
    Wal,
    /// Rollback journal, for VFSes without shared memory (OPFS). The page
    /// cache never spills mid-transaction, so the large-value reader
    /// connection sees only committed pages even when the VFS's locks are
    /// no-ops.
    Rollback,
    /// Journal kept in memory and no syncs, for VFSes that persist whole
    /// commits themselves (IndexedDB). A crash can lose the newest commits,
    /// never tear one.
    Memory,
}

impl Default for FileOptions {
    fn default() -> Self {
        Self {
            synchronous_full: false,
            cache_kib: 8 * 1024,
            vfs: None,
            journal: Journal::Wal,
            page_size: None,
        }
    }
}

/// Jazz's `Db` over SQLite.
pub struct Db {
    inner: Rc<Store>,
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
    /// Unchecked like `Local` (the database identity stays the policy
    /// subject), with this author as the write's provenance.
    Attributed(AuthorSubject),
}

/// How long a LocalFirst read's first load waits for a relay's upstream
/// (`firstLoadRemoteWaitMs`), as a non-durable foreground tells its relay.
pub(crate) fn first_load_wait(tier: ReadTier, first_load: &FirstLoad) -> Option<u64> {
    match (tier, first_load) {
        (ReadTier::LocalFirst, FirstLoad::WaitForRemote { timeout_ms }) => Some(*timeout_ms),
        _ => None,
    }
}

fn who(identity: WriteIdentity) -> Who {
    match identity {
        WriteIdentity::Session(author) if author != AuthorSubject::SYSTEM => Who::Session(author),
        WriteIdentity::Attribution(author) if author != AuthorSubject::SYSTEM => {
            Who::Attributed(author)
        }
        _ => Who::Local,
    }
}

#[derive(Default)]
pub(crate) struct AuthorCodes {
    codes: HashMap<AuthorKey, i64>,
    parts: HashMap<i64, AuthorKey>,
    /// Codes found or interned inside an open transaction, with the
    /// connection's rollback count then: one holds until a rollback, which
    /// may have undone it. A sync batch interns a code per new author and
    /// then finds it for every row that author wrote.
    uncommitted: HashMap<AuthorKey, (i64, u64)>,
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

const STATEMENT_CACHE_CAPACITY: usize = 512;

/// The application tables a store holds, by their stored columns.
fn stored_schema(conn: &Connection, jazz: &JazzSchema) -> Result<Schema, Error> {
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()
        })
        .map_err(storage)?;
    let mut tables = BTreeMap::new();
    for name in names {
        if name.starts_with("_jazz")
            || name.starts_with("_cf_")
            || name.starts_with("sqlite_")
            || name.ends_with("__h")
        {
            continue;
        }
        let columns: Vec<String> = conn
            .prepare(&format!("PRAGMA table_info({})", quote(&name)))
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get(1))?
                    .collect::<Result<_, _>>()
            })
            .map_err(storage)?;
        let columns = columns
            .into_iter()
            .filter(|column| {
                !jazz_model::model::public_schema_convert::RESERVED_ROW_COLUMNS
                    .contains(&column.as_str())
            })
            .collect();
        let mut table = Table::stored_only(name.clone(), columns);
        // A large-capable column is one the large-value refcount triggers
        // watch (`large::triggers`): its kind comes from the schemas unioned
        // in.
        let large = crate::large::stored_large_columns(conn, &name)?;
        for column in &mut table.columns {
            if large.contains(&column.stored) {
                column.ty = ValueType::Nullable(Box::new(ValueType::String));
            }
        }
        tables.insert(name, table);
    }
    Ok(Schema {
        tables,
        jazz: jazz.clone(),
        version: jazz.version_id(),
        policies_from: None,
        stored_names: false,
    })
}

/// Run a script of statements one at a time. SQLite's statement length
/// limit applies to the whole text handed to one prepare, and schema DDL
/// outgrows Durable Object SQLite's 100 KB.
pub(crate) fn execute_script(conn: &Connection, script: &str) -> Result<(), Error> {
    let mut start = 0;
    for (end, _) in script.match_indices(';') {
        let statement = &script[start..=end];
        let text = std::ffi::CString::new(statement)
            .map_err(|_| Error::new(ErrorCode::Storage, "NUL in SQL"))?;
        // SAFETY: a NUL-terminated string; `sqlite3_complete` only reads it.
        // A `;` inside a trigger body or a literal leaves it incomplete.
        if unsafe { rusqlite::ffi::sqlite3_complete(text.as_ptr()) } != 0 {
            conn.execute_batch(statement).map_err(storage)?;
            start = end + 1;
        }
    }
    let rest = &script[start..];
    if !rest.trim().is_empty() {
        conn.execute_batch(rest).map_err(storage)?;
    }
    Ok(())
}

pub(crate) fn rejected(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::WriteRejected, message)
}

/// A write rejected for `reason`.
fn rejected_for(reason: Reason, message: impl Into<String>) -> Error {
    Error::with_reason(ErrorCode::WriteRejected, reason, message)
}

/// A new row (or restored content) without a value for a required column.
fn missing_required(table: &str, column: &str) -> Error {
    rejected_for(
        Reason::MissingRequired,
        format!("missing required field `{column}` in `{table}`"),
    )
}

/// A write to (or an insert reusing the id of) a deleted row.
fn row_already_deleted(row: RowUuid) -> Error {
    rejected_for(
        Reason::RowAlreadyDeleted,
        format!("row already deleted: {}", row.0),
    )
}

/// A restore of a live row.
fn row_not_deleted(row: RowUuid) -> Error {
    rejected_for(Reason::RowNotDeleted, format!("row not deleted: {}", row.0))
}

/// An insert reusing a live row's id.
fn row_already_exists(row: RowUuid) -> Error {
    rejected_for(
        Reason::RowAlreadyExists,
        format!("encoding error: object already exists: {}", row.0),
    )
}

/// Where a write's cells come from. Only values a write authors are checked
/// against their column's JSON Schema. Values it carries from the store (a
/// branch move, a copy-on-write) were accepted under the schema of their
/// time and are not new writes: schemas evolve through migrations and
/// lenses, and carrying a row never fails on a schema tightened since.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cells {
    Authored,
    Carried,
}

/// [`Inner::as_sent`]: per row version (stored table, row, branch key) and
/// stored column, a merged write's value as sent and its bound cell.
type AsSent = BTreeMap<(String, RowUuid, Vec<u8>), BTreeMap<String, Option<(Value, Sql)>>>;

/// A mergeable commit that would move a row to another branch.
fn branch_move() -> Error {
    Error::with_reason(
        ErrorCode::Schema,
        Reason::BranchMove,
        "invalid mergeable commit: branch column does not match exact branch key",
    )
}

impl Error {
    /// Whether a write was refused for the stored state of its row rather
    /// than for its arguments ([`Reason::is_apply_time`]). A host reports it
    /// through the write's handle, as an apply-time rejection, not from the
    /// call.
    pub fn is_apply_time_rejection(&self) -> bool {
        self.reason.is_some_and(Reason::is_apply_time)
    }
}

impl Error {
    /// Whether a read needs large-value chunks this replica has requested
    /// from upstream but not received yet.
    pub fn is_missing_chunks(&self) -> bool {
        crate::large::is_missing(self)
    }
}

/// A write over a row this replica does not hold: a missing observation,
/// not a read-policy decision.
fn update_target_not_loaded(operation: &str, table: &str, row: RowUuid) -> Error {
    Error::new(
        ErrorCode::NotObserved,
        format!(
            "{operation} on table {table} needs the current row, but row {} is not loaded \
             locally; read or subscribe to the row before you {} it",
            row.0,
            operation.to_lowercase()
        ),
    )
}

/// A relation facade query (hopTo, gather, match), as clients send it: the
/// ordinary query it stands for, the way Jazz's validation normalizes it.
/// A union stays a relation query ([`union_plan`] reads its arms when it
/// compiles). Projections that rename or narrow the output are not
/// supported yet. `None` when the query is not a relation query.
fn normalize_relation(query: &Query, schema: &Schema) -> Result<Option<Query>, Error> {
    use crate::query::{RelationProjectExpr, RelationRowIdRef};
    let Some(relation) = &query.relation else {
        return Ok(None);
    };
    let not_yet = |what: &str| crate::values::unsupported(what.to_owned());
    let has_ordinary_clauses = !query.filters.is_empty()
        || !query.joins.is_empty()
        || query.flat_join.is_some()
        || !query.policy_branches.is_empty()
        || !query.reachable.is_empty()
        || !query.inherits.is_empty()
        || !query.includes.is_empty()
        || !query.order_by.is_empty()
        || query.aggregate.is_some()
        || query.limit.is_some()
        || query.offset != 0;
    if has_ordinary_clauses {
        return Err(crate::values::query_error(
            "relation query cannot be combined with ordinary query clauses",
        ));
    }
    if union_plan(query, schema)?.is_some() {
        if query.select.is_some() || !query.array_subqueries.is_empty() {
            return Err(not_yet("selections or relations on union queries"));
        }
        return Ok(Some(query.clone()));
    }
    let relation_error = |e: crate::query::QueryError| crate::values::query_error(e.to_string());
    let mut resolved = crate::query::relation_query_to_query(relation).map_err(relation_error)?;
    if resolved.table != query.table {
        return Err(crate::values::query_error(
            "relation query output table does not match its Query envelope",
        ));
    }
    if let Some(columns) =
        crate::query::relation_output_projection_if_present(relation).map_err(relation_error)?
    {
        let (scope, _) =
            crate::query::relation_output_projection(relation).map_err(relation_error)?;
        let table = schema.table(&resolved.table)?;
        let mut projected = BTreeSet::new();
        let identity = columns.iter().all(|column| {
            let same = match &column.expr {
                RelationProjectExpr::RowId(RelationRowIdRef::Current) => column.alias == "id",
                RelationProjectExpr::Column(reference) => {
                    reference.scope.as_deref() == Some(scope.as_str())
                        && reference.column == column.alias
                }
                RelationProjectExpr::RowId(_) => false,
            };
            if same && column.alias != "id" {
                projected.insert(column.alias.as_str());
            }
            same
        });
        let full = identity
            && projected.len() == table.columns.len()
            && table
                .columns
                .iter()
                .all(|column| projected.contains(column.name.as_str()));
        if !full {
            return Err(not_yet(
                "relation projections that rename or narrow the output columns",
            ));
        }
    }
    resolved.array_subqueries = query.array_subqueries.clone();
    resolved.select = query.select.clone();
    resolved.partial = query.partial.clone();
    resolved.relation = None;
    Ok(Some(resolved))
}

/// A public `UNION ALL`: each arm, by label, as the ordinary query it
/// stands for, and the order and window over the whole union. Every arm
/// reads the output table under its own read policy, so an arm never
/// widens access; a row matched by several arms is one result row per
/// arm.
pub(crate) struct UnionPlan {
    pub arms: Vec<(String, Query)>,
    pub order_by: Vec<crate::query::OrderBy>,
    pub offset: usize,
    pub limit: Option<usize>,
}

/// The union a relation query stands for, or `None` when it is not one.
pub(crate) fn union_plan(query: &Query, schema: &Schema) -> Result<Option<UnionPlan>, Error> {
    let Some(parts) = query
        .relation
        .as_ref()
        .and_then(|relation| crate::query::relation_union_parts(&relation.rel))
    else {
        return Ok(None);
    };
    let invalid = |message: &str| crate::values::query_error(message.to_owned());
    if parts.inputs.is_empty() {
        return Err(invalid("union requires at least one input"));
    }
    let mut labels = BTreeSet::new();
    let mut arms = Vec::with_capacity(parts.inputs.len());
    for arm in parts.inputs {
        if arm.label.is_empty()
            || arm.label.len() > 4096
            || arm.label.contains('\0')
            || !labels.insert(arm.label.clone())
        {
            return Err(invalid(
                "union arm labels must be 1..=4096 bytes, NUL-free, and unique",
            ));
        }
        if crate::query::relation_union_parts(&arm.input).is_some() {
            return Err(crate::values::unsupported("unions nested in union arms"));
        }
        let mut envelope = Query::from(query.table.as_str());
        envelope.relation = Some(crate::query::RelationQuery { rel: arm.input });
        let resolved = normalize_relation(&envelope, schema)?.expect("a relation query");
        arms.push((arm.label, resolved));
    }
    let mut order_by = Vec::new();
    for term in parts.order_by.unwrap_or_default() {
        if term
            .column
            .scope
            .as_deref()
            .is_some_and(|scope| scope != query.table)
        {
            return Err(invalid(
                "union order_by must be scoped to the union output table",
            ));
        }
        order_by.push(crate::query::OrderBy {
            column: term.column.column,
            direction: term.direction,
        });
    }
    Ok(Some(UnionPlan {
        arms,
        order_by,
        offset: parts.offset.unwrap_or(0),
        limit: parts.limit,
    }))
}

/// A union read with its window widened to start at its first row (limit
/// plus offset), as [`crate::db::sync::remote_query`] does for an ordinary
/// window: a replica holds the whole prefix and applies the offset itself.
/// `None` when `query` is not a union with an offset.
pub(crate) fn union_prefix(query: &Query) -> Option<Query> {
    use crate::query::RelationExpr;
    let parts = crate::query::relation_union_parts(&query.relation.as_ref()?.rel)?;
    let offset = parts.offset.filter(|offset| *offset > 0)?;
    let mut rel = RelationExpr::Union {
        inputs: parts.inputs,
    };
    if let Some(terms) = parts.order_by {
        rel = RelationExpr::OrderBy {
            input: Box::new(rel),
            terms,
        };
    }
    if let Some(limit) = parts.limit {
        rel = RelationExpr::Limit {
            input: Box::new(rel),
            limit: limit.saturating_add(offset),
        };
    }
    let mut prefix = query.clone();
    prefix.relation = Some(crate::query::RelationQuery { rel });
    Some(prefix)
}

/// Whether a read is a window: a limit or offset, of the query or of the
/// union it stands for.
pub(crate) fn is_windowed(query: &Query) -> bool {
    query.limit.is_some()
        || query.offset > 0
        || query
            .relation
            .as_ref()
            .and_then(|relation| crate::query::relation_union_parts(&relation.rel))
            .is_some_and(|parts| parts.limit.is_some() || parts.offset.is_some_and(|o| o > 0))
}

/// The text leading a policy rejection's message (its reason is
/// [`Reason::AuthorizationDenied`], its code `WriteRejected`).
pub const AUTHORIZATION_DENIED: &str = Reason::AuthorizationDenied.as_str();

/// How far ahead of the authority's clock a session's explicit write
/// timestamp may be (SPEC 3, INV-TX-7).
pub const SKEW_TOLERANCE_MS: i64 = 30_000;

/// The text leading the message of a write the authority refuses for a
/// timestamp too far ahead (its reason is
/// [`Reason::ClientClockTooFarAhead`], its code `WriteRejected`).
pub const CLIENT_CLOCK_TOO_FAR_AHEAD: &str = Reason::ClientClockTooFarAhead.as_str();

/// A write a policy rejects.
fn denied(detail: impl std::fmt::Display) -> Error {
    rejected_for(
        Reason::AuthorizationDenied,
        format!("{AUTHORIZATION_DENIED}: {detail}"),
    )
}

impl Error {
    /// Whether a policy rejected the write.
    pub fn is_authorization_denied(&self) -> bool {
        self.reason == Some(Reason::AuthorizationDenied)
    }
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Self, Error> {
        let conn = Connection::open_in_memory().map_err(storage)?;
        crate::format::admit(&conn)?;
        Self::open_on(config, conn, None)
    }

    /// A durable store in one SQLite file (WAL unless `options.journal`
    /// says otherwise). Reopening restores rows, history, the commit seq
    /// and, for a replica, its pending uploads and history baselines; they are resent
    /// on the next connection.
    pub async fn open_at(
        config: DbConfig,
        path: &std::path::Path,
        options: FileOptions,
    ) -> Result<Self, Error> {
        Self::open_file(config, crate::host::StoreFile::open(path, options)?).await
    }

    /// Open on a file [`crate::host::StoreFile::open`] admitted, whose host
    /// tables may already have been read or written.
    pub async fn open_file(config: DbConfig, file: crate::host::StoreFile) -> Result<Self, Error> {
        let crate::host::StoreFile { conn, path } = file;
        Self::open_on(
            config,
            conn,
            path.as_ref().map(|(path, vfs)| (path.as_path(), *vfs)),
        )
    }

    /// Run `f` on the store's connection, outside any transaction handle's
    /// overlay (see [`crate::host`]).
    pub(crate) fn with_host_connection<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let inner = self.inner.borrow_mut();
        f(&inner.conn)
    }

    fn open_on(
        mut config: DbConfig,
        conn: Connection,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        // A schema this store has not admitted is reached along the lenses
        // the app ships: from a version it admitted, else from the first
        // version, each admitted as the authority admits it.
        let steps = {
            let names = crate::names::Names::load(&conn)?;
            migration_path(&config.migrations, &config.schema, |version| {
                names.get(version).is_some()
            })
        };
        let Some((first, _)) = steps.first() else {
            if let Some(error) = outside_migrations(&config.migrations, &config.schema, |version| {
                crate::names::Names::load(&conn)
                    .map(|names| names.get(version).is_some())
                    .unwrap_or(false)
            }) {
                return Err(error);
            }
            return Self::open_named(config, conn, path);
        };
        let target = std::mem::replace(&mut config.schema, first.clone());
        let db = Self::open_named(config, conn, path)?;
        db.admit_along_migrations(&target)?;
        {
            let mut inner = db.inner();
            let owner = Rc::new(inner.named(&target));
            inner.owner_version = target.version_id();
            inner.schema = owner.clone();
            inner.owner = owner;
            inner.reset_everything();
            // Pending writes upload under the version they were written
            // in, and replay by the names of the one now current.
            inner.client = None;
            inner.load_client()?;
        }
        Ok(db)
    }

    /// Admit `schema` along the app's lenses when this store lacks it and
    /// they lead to it from a version it admitted or from the first one
    /// (SPEC 10.7); whether they did.
    fn admit_along_migrations(&self, schema: &JazzSchema) -> Result<bool, Error> {
        let steps = {
            let inner = self.inner();
            migration_path(&inner.migrations, schema, |version| {
                inner.names.get(version).is_some()
            })
        };
        let along = !steps.is_empty();
        if !along {
            let inner = self.inner();
            if let Some(error) = outside_migrations(&inner.migrations, schema, |version| {
                inner.names.get(version).is_some()
            }) {
                return Err(error);
            }
        }
        for (schema, lenses) in steps {
            if self.inner().names.get(&schema.version_id()).is_none() {
                self.admit(schema, &lenses, None)?;
            }
        }
        Ok(along)
    }

    fn open_named(
        config: DbConfig,
        conn: Connection,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        // Before the first statement, so both see all of them.
        #[cfg(feature = "durable-object-limits")]
        crate::durable_object_limits::apply(&conn);
        #[cfg(feature = "sql-trace")]
        crate::sql_trace::install(&conn);
        // A version this file admitted keeps its names; another one is
        // named against the most recently admitted.
        let mut names = crate::names::Names::load(&conn)?;
        // The newest version (one new to this file, or the one it admitted
        // last) is current from now on; an older one (a server reopening
        // under its genesis schema, before it activates its current one)
        // leaves the tags as they are.
        let version = config.schema.version_id();
        let newest = names.get(&version).is_none() || names.latest_version() == Some(version);
        // Every version an earlier process admitted is admitted again, with
        // the names it was stored under.
        let mut admitted = crate::catalogue::admitted(&conn)?;
        let schema = names.resolve(&config.schema, &[], names.latest())?;
        names.check_branches(&schema)?;
        names.check_types(&schema)?;
        // Variants are tagged before any DDL: an enum default is its tag.
        names.mint(&conn, &schema)?;
        let chunks = crate::large::Chunks::install(path, config.chunk_store.clone())?;
        // Statements are keyed per table and verb; the default 16 entries
        // thrash as soon as a schema has a few tables.
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        conn.execute_batch(crate::large::DDL).map_err(storage)?;
        crate::large::open_offload(&conn, &chunks)?;
        // Nodes a transaction writes stay cached until it ends: until then
        // the reader connection cannot see them.
        let committed = chunks.clone();
        conn.commit_hook(Some(move || {
            committed.unpin();
            false
        }))
        .map_err(storage)?;
        let rolled_back = chunks.clone();
        // SQLite's own rollbacks (after a full disk, say) count too.
        let rollback_key = crate::values::rollback_key(&conn);
        conn.rollback_hook(Some(move || {
            rolled_back.unpin();
            crate::values::note_rollback(rollback_key);
        }))
        .map_err(storage)?;
        let fresh: i64 = conn
            // Durable Object SQLite keeps its own `_cf_*` tables in the file.
            .query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_jazz\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'", [], |r| r.get(0))
            .map_err(storage)?;
        // A reopened store keeps every column earlier schemas added; bring
        // it up to this schema without touching what is already there.
        let physical = if fresh == 0 {
            execute_script(&conn, &schema.ddl())?;
            let mut physical = Schema {
                tables: BTreeMap::new(),
                jazz: config.schema.clone(),
                version: config.schema.version_id(),
                policies_from: None,
                stored_names: false,
            };
            physical.union(&schema)?;
            physical
        } else {
            let mut physical = stored_schema(&conn, &config.schema)?;
            let ddl = physical.union(&schema)?;
            execute_script(&conn, &ddl)?;
            physical
        };
        names.admit(&conn, &schema)?;
        conn.execute_batch(sync::SYNC_DDL).map_err(storage)?;
        let withheld: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_jazz_withheld'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        // The commit seq resumes after the newest version the store
        // numbered as the authority (a replica's own counter resumes in
        // `load_client`).
        let mut seq = 0u64;
        for table in physical.tables.keys() {
            for name in [table.clone(), format!("{table}__h")] {
                let max: Option<i64> = conn
                    .cached_row(
                        &format!("SELECT MAX(_seq) FROM {} WHERE _n = 0", quote(&name)),
                        [],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                seq = seq.max(max.unwrap_or(0) as u64);
            }
        }
        // An accepted upload that wrote no rows still took a seq: its
        // dedupe entry records it.
        let settled: Option<i64> = conn
            .cached_row(
                "SELECT MAX(seq) FROM _jazz_transactions WHERE sn = 0",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        seq = seq.max(settled.unwrap_or(0) as u64);
        let owner = Rc::new(schema.clone());
        let db = Self {
            view: None,
            inner: Rc::new(Store::new(Inner {
                conn,
                mutation_admission_closed: false,
                #[cfg(any(test, feature = "testing"))]
                failing_ticks: VecDeque::new(),
                chunks,
                pending_nodes: Vec::new(),
                merging: false,
                merge_bases: HashMap::new(),
                as_sent: RefCell::new(BTreeMap::new()),
                overlay: None,
                derived: Vec::new(),
                possession: None,
                staging: LargeValueStagingPolicy::default(),
                upload_cleanup: Rc::default(),
                reclaimed_at: None,
                reclaim_passes: 0,
                ingress: RefCell::new((0, 0)),
                schema: owner.clone(),
                owner_version: owner.jazz.version_id(),
                owner,
                physical,
                stored_view: RefCell::new(None),
                admitted: {
                    let version = config.schema.version_id();
                    admitted.retain(|(id, _)| *id != version);
                    admitted.push((version, config.schema.clone()));
                    admitted
                },
                names,
                views: BTreeMap::new(),
                batches: HashMap::new(),
                system_node: config.system_node.unwrap_or(config.identity.node),
                identity: config.identity,
                writer: None,
                last_stamp: (0, 0),
                authors: RefCell::new(AuthorCodes::default()),
                image_inserts: RefCell::default(),
                current_batch: RefCell::default(),
                ids: config
                    .id_source
                    .unwrap_or_else(|| Box::new(crate::options::ProductionRowIdSource)),
                seq: 0,
                me: 0,
                shard: shard::ShardState::new(config.clock),
                claims: HashMap::new(),
                fan: fanout::Fanout::default(),
                changed: RefCell::new(Vec::new()),
                deferred: RefCell::new(None),
                tx: None,
                publications: Vec::new(),
                client: None,
                non_durable: false,
                as_written_mismatches: 0,
                authority: false,
                upstream_rejections: None,
                sessions: Vec::new(),
                column_codes: RefCell::default(),
                closed: None,
                slow: RefCell::default(),
                accepted: 0,
                since_tick: Vec::new(),
                capture: Vec::new(),
                gone: RefCell::new(Vec::new()),
                migrations: config.migrations,
                withheld: std::cell::Cell::new(withheld > 0),
            })),
        };
        {
            let mut inner = db.inner.borrow_mut();
            // The newest schema is current: its declaration order is tag
            // order.
            if newest {
                let owner = inner.owner.clone();
                crate::values::in_savepoint(&inner.conn, "jazz_align", || {
                    inner.align_tags(&owner)
                })?;
            }
            inner.seq = seq;
            inner.load_client()?;
            inner.load_shard()?;
        }
        Ok(db)
    }

    /// The newest commit seq this database has handed out; its transaction
    /// ids are `(seq, node)`. A host that later gives the same node to a
    /// fresh (in-memory) database seeds it with this through
    /// [`Db::seed_seq_floor`], so no transaction id is reused: the
    /// authority deduplicates uploads by id and would drop the new write.
    pub fn seq_high_water(&self) -> u64 {
        self.inner.borrow().seq
    }

    /// Never hand out a commit seq at or below `floor`.
    pub fn seed_seq_floor(&self, floor: u64) {
        let mut inner = self.inner.borrow_mut();
        inner.seq = inner.seq.max(floor);
    }

    /// Every SQLite store holds complete history; this one is also the
    /// global authority (see [`Db::set_authority`]).
    pub async fn open_history_complete(config: DbConfig) -> Result<Self, Error> {
        let db = Self::open(config).await?;
        db.set_authority();
        Ok(db)
    }

    /// Keep the uploads the authority refuses, for a host that tells its
    /// caller of the ones no wait claimed.
    /// Refused commits of this store's own are kept in its retry store
    /// until [`Db::discard_rejected_transaction`]; a reopened store that
    /// still holds some tracks again and reports them anew.
    pub fn track_upstream_rejections(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.upstream_rejections.get_or_insert_with(Vec::new);
    }

    /// Report again, through [`Db::take_upstream_rejections`], each refused
    /// commit the retry store still holds and that is not already waiting
    /// to be taken: for a host that could not deliver it before (no
    /// listener yet). Does nothing unless refusals are tracked.
    pub fn requeue_rejected_transactions(&self) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if inner.upstream_rejections.is_none() {
            return Ok(());
        }
        let writer = *inner.identity.node.as_bytes();
        let retained = inner.rejected_transactions()?;
        let queued = inner.upstream_rejections.as_mut().expect("tracked");
        for rejected in retained {
            if !queued
                .iter()
                .any(|r| r.writer == writer && r.seq == rejected.seq)
            {
                queued.push(UpstreamRejection {
                    writer,
                    seq: rejected.seq,
                    error: rejected.error,
                    serializable: rejected.serializable,
                });
            }
        }
        Ok(())
    }

    /// This store's own commits the authority refused, with their payloads,
    /// oldest first (INV-TX-9). Kept only while
    /// [`Db::track_upstream_rejections`] is on, until discarded.
    pub fn rejected_transactions(&self) -> Result<Vec<RejectedTransaction>, Error> {
        self.inner.borrow_mut().rejected_transactions()
    }

    /// Drop a refused commit's payload once its refusal was reported.
    pub fn discard_rejected_transaction(&self, seq: u64) -> Result<(), Error> {
        self.inner.borrow_mut().discard_rejected(seq)
    }

    /// The uploads the authority refused since the last call, by the node
    /// and seq of the commit (this store's, or the relayed session's that
    /// wrote it) (empty unless [`Db::track_upstream_rejections`] was called).
    pub fn take_upstream_rejections(&self) -> Vec<UpstreamRejection> {
        self.inner
            .borrow_mut()
            .upstream_rejections
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Serve as the global authority: subscriptions this store settles,
    /// its own and its subscribers', report `DurabilityTier::Global`. A
    /// store without the role (a relay with no upstream) settles at `Local`.
    pub fn set_authority(&self) {
        self.inner.borrow_mut().authority = true;
    }

    /// Whether this store is the global authority.
    pub fn is_authority(&self) -> bool {
        self.inner.borrow().authority
    }

    /// The identity of table `table` of this handle's schema: the same
    /// UUID on the authority and every replica of it, for the table's whole
    /// life (a rename keeps it), and never another app's. `Ok(None)` when
    /// the schema has no such table.
    ///
    /// Identities are the authority's: a replica answers once it has
    /// mirrored the authority's catalogue with this schema version in it,
    /// and fails with code `NotObserved` until then (a version only this
    /// replica admitted has none yet).
    pub fn table_identity(&self, table: &str) -> Result<Option<GlobalPhysicalTableId>, Error> {
        Ok(self
            .stored_identity(table, None)?
            .map(GlobalPhysicalTableId))
    }

    /// The identity of column `column` of table `table` of this handle's
    /// schema, as [`Db::table_identity`]. A column dropped and added again
    /// keeps its identity only with the type it had; with another one it is
    /// a new column.
    pub fn column_identity(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<GlobalPhysicalColumnId>, Error> {
        Ok(self
            .stored_identity(table, Some(column))?
            .map(GlobalPhysicalColumnId))
    }

    fn stored_identity(
        &self,
        table: &str,
        column: Option<&str>,
    ) -> Result<Option<uuid::Uuid>, Error> {
        let inner = self.inner();
        let schema = inner.schema.clone();
        let version = schema.jazz.version_id();
        let namespace = inner.namespace()?;
        let (Some(namespace), true) = (namespace, inner.names_confirmed(&version)) else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "this store has not received its authority's catalogue for this schema version yet",
            ));
        };
        let Some(table) = schema.tables.get(table) else {
            return Ok(None);
        };
        Ok(match column {
            None => Some(crate::names::identity(namespace, &table.stored, None)),
            Some(column) => table
                .columns
                .iter()
                .find(|c| c.name == column)
                .map(|c| crate::names::identity(namespace, &table.stored, Some(&c.stored))),
        })
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
        #[cfg(any(test, feature = "testing"))]
        if let Some(code) = self.inner().failing_ticks.pop_front() {
            return Err(Error::new(code, "a tick failure injected by a test"));
        }
        self.sync_tick()?;
        {
            let mut inner = self.inner();
            inner.fan.collect();
            inner.reclaim_large_values(RECLAIM_NODES_PER_TICK)?;
        }
        self.pump_chunk_store().await
    }

    /// Fetch the offloaded nodes reads wait on, move settled leaves to the
    /// chunk store and delete reclaimed ones from it ([`crate::chunk_store`]).
    /// A failed operation is retried next tick; the first failure is
    /// returned once the rest ran.
    async fn pump_chunk_store(&self) -> Result<(), Error> {
        let chunks = self.inner().chunks.clone();
        let Some(store) = chunks.store().cloned() else {
            return Ok(());
        };
        let Some(wanted) = chunks.begin_fetch() else {
            return Ok(());
        };
        let any = !wanted.is_empty();
        let mut fetched = self.fetch_from(&chunks, &*store, wanted).await;
        if any && fetched.is_ok() {
            // Messages held for those nodes are served now, not a tick later.
            fetched = self.sync_tick();
        }
        let moved = match chunks.offload_store() {
            Some(_) => self.offload_to(&*store).await,
            None => Ok(()),
        };
        chunks.end_fetch();
        fetched.and(moved)
    }

    /// Install what the chunk store holds for `wanted`; reads waiting on
    /// those nodes run again.
    async fn fetch_from(
        &self,
        chunks: &crate::large::Chunks,
        store: &dyn crate::chunk_store::ChunkStore,
        wanted: Vec<jazz_types::large_values::chunks::ChunkRequest>,
    ) -> Result<(), Error> {
        use futures::StreamExt;
        if wanted.is_empty() {
            return Ok(());
        }
        let keys: Vec<_> = wanted
            .iter()
            .map(crate::chunk_store::NodeKey::from)
            .collect();
        let answers: Vec<_> = futures::stream::iter(keys.iter().map(|key| store.get(key)))
            .buffered(CHUNK_STORE_CONCURRENCY)
            .collect()
            .await;
        let mut first = None;
        for (request, answer) in wanted.iter().zip(answers) {
            match answer.and_then(|bytes| chunks.install_from_store(request, bytes)) {
                Ok(()) => {}
                Err(error) => {
                    // Asked again by the next read that needs it.
                    first.get_or_insert(error);
                }
            }
        }
        self.inner().retry_awaiting();
        first.map_or(Ok(()), Err)
    }

    /// Move a batch of settled leaves to the chunk store, then delete a
    /// batch of reclaimed ones from it. Puts start only between SQLite
    /// transactions, so the mark that a put may have landed is durable
    /// before it does.
    async fn offload_to(&self, store: &dyn crate::chunk_store::ChunkStore) -> Result<(), Error> {
        use futures::StreamExt;
        let mut first = None;
        let batch = {
            let inner = self.inner();
            if inner.conn.is_autocommit() {
                crate::large::begin_offload(&inner.conn, CHUNK_STORE_NODES_PER_TICK)?
            } else {
                Vec::new()
            }
        };
        if !batch.is_empty() {
            let puts: Vec<_> =
                futures::stream::iter(batch.iter().map(|(key, bytes)| store.put(key, bytes)))
                    .buffered(CHUNK_STORE_CONCURRENCY)
                    .collect()
                    .await;
            let inner = self.inner();
            in_savepoint(&inner.conn, "jazz_lv_offloaded", || {
                for ((key, _), put) in batch.iter().zip(puts) {
                    match put {
                        Ok(()) => crate::large::offloaded(&inner.conn, key)?,
                        Err(error) => {
                            first.get_or_insert(error);
                        }
                    }
                }
                Ok(())
            })?;
        }
        let deletes =
            crate::large::pending_deletes(&self.inner().conn, CHUNK_STORE_NODES_PER_TICK)?;
        if !deletes.is_empty() {
            let done: Vec<_> = futures::stream::iter(deletes.iter().map(|key| store.delete(key)))
                .buffered(CHUNK_STORE_CONCURRENCY)
                .collect()
                .await;
            let inner = self.inner();
            in_savepoint(&inner.conn, "jazz_lv_deleted", || {
                for (key, done) in deletes.iter().zip(done) {
                    match done {
                        Ok(()) => crate::large::deleted(&inner.conn, key)?,
                        Err(error) => {
                            first.get_or_insert(error);
                        }
                    }
                }
                Ok(())
            })?;
        }
        first.map_or(Ok(()), Err)
    }

    /// [`Self::fetching`] over a fresh copy of `arg` per attempt; without a
    /// chunk store `arg` is not copied.
    async fn fetching_with<A: Clone, T>(
        &self,
        arg: A,
        attempt: impl Fn(A) -> Result<T, Error>,
    ) -> Result<T, Error> {
        // A peek: the store gate would undo a transaction's cached overlay.
        if self.inner.staging().chunks.store().is_none() {
            return attempt(arg);
        }
        self.fetching(|| attempt(arg.clone())).await
    }

    /// Run `attempt` until it no longer waits on offloaded nodes that only
    /// an asynchronous chunk store can answer, fetching them in between.
    /// Without such a store (or while a tick's fetch runs) a wait is the
    /// usual missing-chunks error.
    async fn fetching<T>(&self, mut attempt: impl FnMut() -> Result<T, Error>) -> Result<T, Error> {
        loop {
            match attempt() {
                Err(error) if crate::large::is_missing(&error) => {
                    let chunks = self.inner().chunks.clone();
                    let Some(store) = chunks.store().cloned().filter(|_| chunks.has_fetch()) else {
                        return Err(error);
                    };
                    let Some(wanted) = chunks.begin_fetch() else {
                        return Err(error);
                    };
                    let fetched = self.fetch_from(&chunks, &*store, wanted).await;
                    chunks.end_fetch();
                    fetched?;
                }
                result => return result,
            }
        }
    }

    /// Make the next ticks fail with `codes`, in order, before doing any
    /// work: a tick driver's failure handling is exercised without
    /// breaking the store.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn fail_next_ticks_for_test(&self, codes: impl IntoIterator<Item = ErrorCode>) {
        self.inner().failing_ticks.extend(codes);
    }

    /// Where the current schema stores `table`'s `column`: (stored table,
    /// stored column), the names a hand-built replicated write uses.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn stored_column_for_test(&self, table: &str, column: &str) -> (String, String) {
        let inner = self.inner();
        let table = inner
            .owner
            .table(table)
            .expect("a table of the current schema");
        (table.stored.clone(), table.stored_name(column).to_owned())
    }

    /// Diagnostics: stored large-value nodes, and roots referenced by cells.
    #[doc(hidden)]
    pub fn large_value_nodes_for_test(&self) -> Result<(u64, u64), Error> {
        let inner = self.inner();
        inner
            .conn
            .cached_row(
                "SELECT (SELECT count(*) FROM _jazz_lv_node), \
                 (SELECT count(*) FROM _jazz_lv_root WHERE refs > 0)",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .map_err(storage)
    }

    /// Diagnostics: large-value reclaim passes this store ran.
    #[doc(hidden)]
    pub fn large_value_reclaim_passes_for_test(&self) -> u64 {
        self.inner().reclaim_passes
    }

    /// Diagnostics: (epochs, encoded edit bytes they hold).
    #[doc(hidden)]
    pub fn large_value_epochs_for_test(&self) -> Result<(u64, u64), Error> {
        self.inner()
            .conn
            .cached_row(
                "SELECT count(*), coalesce(sum(length(edits)), 0) FROM _jazz_lv_epoch",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .map_err(storage)
    }

    /// Roll back the open SQLite transaction behind the database's back, as
    /// SQLite itself does after some errors (a full disk, an I/O error).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn rollback_sqlite_transaction_for_test(&self) {
        self.inner()
            .conn
            .execute_batch("ROLLBACK")
            .expect("an open SQLite transaction");
    }

    /// Diagnostics: the chunks reads needed that nothing local holds.
    #[doc(hidden)]
    pub fn take_missing_chunks_for_test(&self) -> usize {
        self.inner().chunks.take_demand().len()
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.tick().await
    }

    /// Permanently stop admitting application row mutations in this runtime
    /// and all its schema views. New transactions, further staging, stream
    /// pushes and commits return a storage error, including a callback
    /// transaction that was still running when this fence was set.
    ///
    /// Reads, transaction abandonment, stream aborts and ordinary sync/cleanup
    /// ticks remain available so already committed writes can settle. This is
    /// an in-memory shutdown phase, not a stored policy or storage retirement;
    /// a new storage owner starts with mutation admission open.
    pub fn stop_mutation_admission(&self) {
        self.inner().mutation_admission_closed = true;
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

    /// Query shapes that ran at or over the slow-query threshold since
    /// the last call, slowest total first, and how many further shapes the
    /// window had no room for. See [`crate::slow`].
    pub fn take_slow_queries(&self) -> (Vec<crate::slow::SlowQuery>, u64) {
        self.inner.borrow().slow.borrow_mut().take()
    }

    /// The slow-query report due now: one line per shape that ran slow
    /// since the last report, naming `app`. The first slow query is
    /// reported at once, later ones at most every
    /// [`crate::slow::REPORT_EVERY`]; empty in between. Hosts call it after
    /// ticking and write the lines where they report problems.
    pub fn take_slow_query_report(&self, app: Option<&str>) -> Vec<String> {
        self.take_due_slow_queries()
            .map(|(shapes, dropped)| crate::slow::report_lines(&shapes, dropped, app))
            .unwrap_or_default()
    }

    /// [`Db::take_slow_queries`] when a report is due (as
    /// [`Db::take_slow_query_report`] times it), for a host that reports
    /// the shapes itself; `None` in between.
    pub fn take_due_slow_queries(&self) -> Option<(Vec<crate::slow::SlowQuery>, u64)> {
        self.inner.borrow().slow.borrow_mut().take_due()
    }

    /// Record query runs taking at least `threshold` (`None`: none).
    /// Defaults to [`crate::slow::DEFAULT_SLOW_QUERY_THRESHOLD`].
    pub fn set_slow_query_threshold(&self, threshold: Option<std::time::Duration>) {
        self.inner
            .borrow()
            .slow
            .borrow_mut()
            .set_threshold(threshold);
    }

    /// Diagnostics: SQLite's query plan for the INSERT check a commit
    /// would run on `row` of `table` as `author`, with `grounded` rows of
    /// that table overlaid as a later grounding round sees them.
    #[doc(hidden)]
    pub fn explain_write_check_plan(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
        grounded: &[RowUuid],
    ) -> Result<String, Error> {
        let ids = grounded.iter().map(|id| *id.0.as_bytes()).collect();
        let inner = self.inner();
        let stored = inner.schema.table(table)?.stored.clone();
        let overlay = CutOverlay::Grounded(Arc::new(BTreeMap::from([(stored, ids)])));
        inner.explain_write_check(table, row, author, overlay)
    }

    /// Diagnostics: SQLite's query plan for the INSERT check of `row` of
    /// `table` as `author` against a unit's post-state (every row the unit
    /// wrote but `row`), as the post-state pass runs it.
    #[doc(hidden)]
    pub fn explain_post_state_check_plan(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
    ) -> Result<String, Error> {
        let inner = self.inner();
        let stored = inner.schema.table(table)?.stored.clone();
        let overlay = CutOverlay::Except {
            table: stored,
            row: *row.0.as_bytes(),
            key: Vec::new(),
        };
        inner.explain_write_check(table, row, author, overlay)
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
        let normalized = normalize_relation(query, &inner.schema)?;
        let query = normalized.as_ref().unwrap_or(query);
        // Validate: tables, columns, parameters, supported forms. Read
        // options are not known yet: validate the widest read (deleted rows
        // included, so their metadata columns are accepted).
        let session = Session::system();
        Ctx::new(&inner.schema, None, &session, &binding).compile_query(query, true)?;
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
        self.fetching(|| {
            let inner = self.inner();
            inner.run(prepared, None, &ReadSpec::of(&opts, &inner)?)
        })
        .await
    }

    /// Root rows with each array relation nested in a cell under its
    /// column name ([`ResultNode::into_nested_row`]).
    pub fn nest_relations(
        &self,
        prepared: &PreparedQuery,
        roots: Vec<CurrentRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.inner()
            .nest_relations(prepared, None, &ReadSpec::default(), roots)
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
        let inner = self.inner();
        inner.run(prepared, Some(identity), &ReadSpec::of(&opts, &inner)?)
    }

    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let inner = self.inner();
        let read = ReadSpec::of(&opts, &inner)?;
        let roots = inner.run(prepared, None, &read)?;
        let mut tree = Vec::with_capacity(roots.len());
        for row in roots {
            tree.push(inner.node(
                prepared,
                None,
                &read,
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
        let read = ReadSpec::of(&opts, &self.inner())?;
        let client = self.is_client();
        let non_durable = self.inner().non_durable;
        // A branch view's head (and live base) is kept current upstream,
        // over a frozen base too: only a snapshot is wholly this store's.
        let reach = client
            .then(|| sync::remote_reach(&opts, non_durable, read.cut.is_some()))
            .flatten();
        if reach.is_some() {
            self.remote_shape(&prepared.query)?;
        }
        let view = opts.read_view.source.clone();
        let attached = reach
            .and_then(|reach| self.attach_remote(prepared, read.include_deleted, &view, reach));
        // A live result reads as far as its own upstream read is settled.
        let read = match &attached {
            Some((key, _)) => ReadSpec {
                position: Position::Remote(key.clone()),
                ..read
            },
            None => read,
        };
        let remote = attached.map(|(_, flags)| flags);
        let settled = match (&remote, client) {
            (Some((answered, _, global)), _) => {
                sync::SettledBy::Remote(answered.clone(), global.clone())
            }
            (None, true) => sync::SettledBy::Never,
            (None, false) => sync::SettledBy::Always(self.inner().authority),
        };
        let gate = match (&remote, self.link_hint()) {
            (Some((answered, _, global)), Some(hint)) => {
                let deadline =
                    sync::remote_deadline(&opts, non_durable, sync::may_wait(hint.get()));
                let live = hint.get().hint == RemoteLinkHint::Live;
                deadline.map(|deadline| sync::Gate {
                    answered: answered.clone(),
                    global: global.clone(),
                    hint,
                    live,
                    deadline,
                    relay_view: non_durable && opts.tier != ReadTier::Remote,
                })
            }
            _ => None,
        };
        let include_deleted = read.include_deleted;
        let mut inner = self.inner();
        let (queue, group) = match inner.subscribe_member(prepared, identity, read, settled) {
            Ok(member) => member,
            Err(error) => {
                drop(inner);
                if let (Some(_), Some(reach)) = (&remote, reach) {
                    self.detach_remote(
                        &prepared.query,
                        &prepared.binding,
                        include_deleted,
                        &view,
                        reach,
                    );
                }
                return Err(error);
            }
        };
        Ok(SubscriptionStream {
            queue,
            dropped: inner.fan.dropped.clone(),
            gate,
            group,
            refused: remote.as_ref().map(|(_, refused, _)| refused.clone()),
            remote: remote.zip(reach).map(|(_, reach)| {
                (
                    self.inner.clone(),
                    prepared.query.clone(),
                    prepared.binding.clone(),
                    include_deleted,
                    view,
                    reach,
                )
            }),
        })
    }

    /// A row's provenance as Jazz's row metadata spells it: created at, by
    /// (canonical author), updated at, by. `None` when the store has no
    /// such row. `table` is a schema or stored table name.
    pub fn row_provenance(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<(u64, String, u64, String)>, Error> {
        let inner = self.inner();
        let schema = inner.schema.clone();
        let Some(table) = schema.table(table).ok().or_else(|| schema.by_stored(table)) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT \"$createdAt\", \"$createdBy\", COALESCE(\"$updatedAt\", \"$createdAt\"), \
             COALESCE(\"$updatedBy\", \"$createdBy\") FROM {} WHERE id = ?1 ORDER BY _b LIMIT 1",
            quote(&table.stored)
        );
        let stamps: Option<(Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = inner
            .conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .optional()
            .map_err(storage)?;
        let Some((created_at, Some(created_by), updated_at, Some(updated_by))) = stamps else {
            return Ok(None);
        };
        let canonical = |code: i64| -> Result<String, Error> {
            let (account, issuer, subject) = inner.author_parts(code)?;
            let accountless = account == jazz_types::account_registry::SYSTEM_ACCOUNT_ID.0
                && issuer != AuthorSubject::SYSTEM_ISSUER;
            Ok(if accountless {
                serde_json::to_string(&(issuer, subject))
            } else {
                serde_json::to_string(&(account.to_string(), issuer, subject))
            }
            .expect("author parts have a JSON spelling"))
        };
        Ok(Some((
            created_at.unwrap_or(0) as u64,
            canonical(created_by)?,
            updated_at.unwrap_or(0) as u64,
            canonical(updated_by)?,
        )))
    }

    /// How many distinct authors this database has interned.
    #[doc(hidden)]
    pub fn interned_authors_for_test(&self) -> usize {
        self.inner()
            .conn
            .cached_row("SELECT COUNT(*) FROM _jazz_authors", [], |row| {
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
        }
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

    /// Waits while a cross-shard part this shard holds writes one of `rows`
    /// (SPEC 15 §15.3): a local write of a held row lands after the part,
    /// never under it. Ready at once on an unsharded store.
    async fn unheld(&self, rows: &[Option<RowUuid>]) {
        let rows: Vec<[u8; 16]> = rows.iter().flatten().map(|r| *r.0.as_bytes()).collect();
        self.unheld_rows(&rows).await;
    }

    async fn unheld_rows(&self, rows: &[[u8; 16]]) {
        std::future::poll_fn(|cx| self.inner.borrow_mut().shard.wait_unheld(rows, cx)).await
    }

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
        let op = StagedOp::Insert(table.to_owned(), cells, opts);
        self.unheld(&[op.row()]).await;
        self.fetching_with(op, |op| self.write_op(op, who)).await
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        let op = StagedOp::Update(table.to_owned(), row, cells, opts);
        self.unheld(&[Some(row)]).await;
        self.fetching_with(op, |op| self.write_op(op, who)).await
    }

    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        let op = StagedOp::Upsert(table.to_owned(), row, cells, BTreeMap::new(), opts);
        self.unheld(&[Some(row)]).await;
        self.fetching_with(op, |op| self.write_op(op, who)).await
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        let op = StagedOp::Delete(table.to_owned(), row, opts);
        self.unheld(&[Some(row)]).await;
        self.fetching_with(op, |op| self.write_op(op, who)).await
    }

    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        let op = StagedOp::Restore(table.to_owned(), row, content, opts);
        self.unheld(&[Some(row)]).await;
        self.fetching_with(op, |op| self.write_op(op, who)).await
    }

    /// One write. On a client, a local (unauthenticated) write is also an
    /// upload: it is recorded for the authority, which decides its fate.
    fn write_op(&self, mut op: StagedOp, who: Who) -> Result<WriteHandle, Error> {
        self.inner().require_mutation_admission()?;
        if let Some(row) = op.row() {
            self.inner().refuse_held(&[*row.0.as_bytes()])?;
        }
        self.with_upload(|| {
            let table = op.table().to_owned();
            let (prior, mark) = {
                let inner = self.inner();
                let mark = inner.changed.borrow().len();
                (inner.seq, mark)
            };
            let (notify, handle) = self.write_unnotified(&table, |inner, seq| {
                let edits = inner.chunk_large_cells(&mut op)?;
                inner.counter_deltas(&mut op)?;
                inner.pending_nodes.extend(edits.pending_nodes);
                let applied = inner.apply_as(&op, who, seq);
                inner.clear_derived();
                applied
            })?;
            // A standalone write that changed nothing made no commit: its
            // handle names the row's current version, which may be the
            // newest commit's own seq, so recording it would upload a
            // second commit under that commit's transaction id.
            let committed = notify || self.inner().tx.is_some();
            // The upload record joins the write's SQLite transaction: if it
            // fails, `with_upload` rolls both back, and nobody has been told.
            if let Err(error) = committed
                .then(|| self.record_upload(vec![op], handle.seq.seq, sync::Kind::Mergeable, None))
                .transpose()
            {
                let mut inner = self.inner();
                inner.seq = prior;
                inner.changed.borrow_mut().truncate(mark);
                return Err(error);
            }
            if notify {
                self.notify();
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
        begin_savepoint(&self.inner.borrow().conn, "upload_local")?;
        let result = f();
        end_savepoint(&self.inner.borrow().conn, "upload_local", result.is_ok())?;
        result
    }

    /// Run one write atomically. Inside a transaction it joins the
    /// transaction's seq; standalone it commits. Returns whether a
    /// standalone commit happened, which the caller notifies subscribers of
    /// once its upload record is in.
    fn write_unnotified(
        &self,
        table: &str,
        apply: impl FnOnce(&mut Inner, u64) -> Result<WriteHandle, Error>,
    ) -> Result<(bool, WriteHandle), Error> {
        let standalone = {
            let mut inner = self.inner();
            let (seq, standalone) = match &inner.tx {
                Some((seq, _)) => (*seq, false),
                None => (inner.next_seq(), true),
            };
            begin_savepoint(&inner.conn, "w")?;
            let mark = inner.changed.borrow().len();
            match apply(&mut inner, seq) {
                Ok(handle) => {
                    end_savepoint(&inner.conn, "w", true)?;
                    let wrote = handle.seq == inner.own_pos(seq);
                    if wrote {
                        if standalone {
                            inner.seq = seq;
                        } else if let Some((_, tables)) = &mut inner.tx {
                            tables.insert(table.to_owned());
                        }
                    }
                    (standalone && wrote, handle)
                }
                Err(error) => {
                    inner.changed.borrow_mut().truncate(mark);
                    end_savepoint(&inner.conn, "w", false)?;
                    return Err(error);
                }
            }
        };
        Ok(standalone)
    }

    // ---- transactions ---------------------------------------------------

    pub async fn transaction<T>(
        &self,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_>) -> Result<T, Error>,
    ) -> Result<(T, TxReceipt), Error> {
        self.transaction_as(WriteIdentity::Database, callback).await
    }

    /// A transaction whose writes are attributed to `author`. Like every
    /// mergeable transaction it stages its writes for the authority, which
    /// checks the commit when it is uploaded; no local policy check runs
    /// here.
    pub async fn transaction_for_identity<T>(
        &self,
        author: AuthorSubject,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_>) -> Result<T, Error>,
    ) -> Result<(T, TxReceipt), Error> {
        self.transaction_as(WriteIdentity::Attribution(author), callback)
            .await
    }

    /// Run `callback` in one commit, its writes made as `identity`.
    async fn transaction_as<T>(
        &self,
        identity: WriteIdentity,
        callback: impl AsyncFnOnce(&mut MergeableTx<'_>) -> Result<T, Error>,
    ) -> Result<(T, TxReceipt), Error> {
        {
            let mut inner = self.inner();
            inner.require_mutation_admission()?;
            if inner.tx.is_some() {
                return Err(unsupported("nested transactions"));
            }
            begin_savepoint(&inner.conn, "tx")?;
            inner.changed.borrow_mut().clear();
            let seq = inner.next_seq();
            inner.tx = Some((seq, BTreeSet::new()));
        }
        let mut tx = MergeableTx { db: self, identity };
        let result = callback(&mut tx).await.and_then(|value| {
            self.inner().require_mutation_admission()?;
            Ok(value)
        });
        let captured;
        let prior;
        let changed = {
            let mut inner = self.inner();
            let (seq, tables) = inner.tx.take().expect("open transaction");
            captured = std::mem::take(&mut inner.capture);
            prior = inner.seq;
            match &result {
                Ok(_) => {
                    if !tables.is_empty() {
                        inner.seq = seq;
                    }
                    Some((seq, tables))
                }
                Err(_) => {
                    inner.abandon_savepoint("tx")?;
                    None
                }
            }
        };
        let value = result?;
        let (seq, _) = changed.expect("committed");
        // The upload record joins the transaction's SQLite savepoint.
        let recorded = self.record_stored_upload(captured, seq, sync::Kind::Mergeable, None);
        if let Err(error) = recorded {
            let mut inner = self.inner();
            inner.seq = prior;
            inner.abandon_savepoint("tx")?;
            return Err(error);
        }
        end_savepoint(&self.inner.borrow().conn, "tx", true)?;
        self.notify();
        Ok((value, TxReceipt(seq)))
    }

    // ---- schema evolution -------------------------------------------------

    pub fn author_schema_lineage_publication(
        &self,
        version: SchemaVersion,
        lens: MigrationLens,
    ) -> Result<SchemaPublication, Error> {
        Ok(SchemaPublication { version, lens })
    }

    pub async fn publish_schema_with_lens(
        &self,
        publication: SchemaPublication,
    ) -> Result<(), Error> {
        self.inner().publications.push(publication);
        Ok(())
    }

    /// Make a published version current with the lens it was published
    /// with (see [`Db::activate_schema_with_lenses`]).
    pub async fn activate_catalogue_schema_for_test(
        &self,
        current: CurrentWriteSchema,
    ) -> Result<(), Error> {
        let publication = self
            .inner()
            .publications
            .iter()
            .find(|publication| publication.version.id == current.schema)
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::Schema, "schema version was not published"))?;
        self.activate(publication.version.schema, &[publication.lens], None)
    }

    /// Make `schema` the current schema. The stored tables grow to hold it:
    /// new tables, new columns (`ADD COLUMN ... DEFAULT`, no rows rewritten)
    /// and new indexes. Columns other schemas added stay, so older clients
    /// keep their data. Changing a column's type is not supported yet, and a
    /// new required column needs a default.
    pub async fn activate_schema(&self, schema: JazzSchema) -> Result<(), Error> {
        self.activate(schema, &[], None)
    }

    /// [`Db::activate_schema`], with the migration lenses into `schema`
    /// (lenses into other versions are ignored). A lens's added columns
    /// take its defaults, and a renamed table or column keeps the stored
    /// table or column of its source, so every version reads and writes the
    /// same rows (see [`crate::names`]).
    pub async fn activate_schema_with_lenses(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
    ) -> Result<(), Error> {
        self.activate(schema, lenses, None)
    }

    /// [`Db::activate_schema_with_lenses`] as permissions revision
    /// `revision` of the store's catalogue (see [`crate::catalogue`]): its
    /// row records the schema and policies, and when they took effect. A
    /// store without a server numbers its activations itself.
    pub async fn activate_schema_at_revision(
        &self,
        revision: u64,
        schema: JazzSchema,
        lenses: &[MigrationLens],
    ) -> Result<(), Error> {
        self.activate(schema, lenses, Some(revision))
    }

    /// Admit `schema` without making it current: the stored tables grow to
    /// hold it as [`Db::activate_schema_with_lenses`] would, and sessions
    /// that declare it read and write by its names under the current
    /// schema's policies.
    pub async fn admit_schema_with_lenses(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
    ) -> Result<(), Error> {
        self.admit(schema, lenses, None)
    }

    fn activate(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
        revision: Option<u64>,
    ) -> Result<(), Error> {
        self.admit(schema, lenses, Some(revision))
    }

    /// Apply `change` to the store's catalogue in one transaction: publish
    /// its rows, admit its versions in order, then make its schema current
    /// at its revision. A change that fails anywhere leaves the file and
    /// this database as they were.
    pub async fn apply_catalogue(
        &self,
        change: crate::catalogue::CatalogueChange,
    ) -> Result<(), Error> {
        let saved = {
            let inner = self.inner();
            (
                inner.physical.clone(),
                inner.names.clone(),
                inner.admitted.clone(),
                inner.owner.clone(),
                inner.owner_version,
            )
        };
        begin_savepoint(&self.inner().conn, "jazz_catalogue")?;
        let applied = (|| {
            crate::catalogue::put(&self.inner().conn, &change.put)?;
            for (schema, lenses) in change.admit {
                self.admit(schema, &lenses, None)?;
            }
            if let Some((revision, schema, lenses)) = change.activate {
                self.admit(schema, &lenses, Some(Some(revision)))?;
            }
            Ok(())
        })();
        let ended = end_savepoint(&self.inner().conn, "jazz_catalogue", applied.is_ok());
        let result = applied.and(ended);
        if result.is_err() {
            let mut inner = self.inner();
            let (physical, names, admitted, owner, owner_version) = saved;
            inner.physical = physical;
            inner.names = names;
            inner.admitted = admitted;
            inner.owner = owner;
            inner.owner_version = owner_version;
            inner.reset_everything();
        }
        result
    }

    /// The current schema, with its policies.
    pub fn current_schema(&self) -> JazzSchema {
        self.inner().owner.jazz.clone()
    }

    /// The store's catalogue: every schema, lens and permissions revision
    /// it keeps (see [`crate::catalogue`]).
    pub fn catalogue(&self) -> Result<crate::catalogue::CatalogueRows, Error> {
        crate::catalogue::read(&self.inner().conn)
    }

    /// Admit `schema`; with `current`, also make it current, at that
    /// permissions revision if one is given.
    fn admit(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
        current: Option<Option<u64>>,
    ) -> Result<(), Error> {
        let make_current = current.is_some();
        {
            let mut inner = self.inner();
            let version = schema.version_id();
            let mut defaults = BTreeMap::new();
            let mut copies = BTreeMap::new();
            for lens in lenses.iter().filter(|lens| lens.target == version) {
                let effects = LensEffects::of(lens)?;
                defaults.extend(effects.defaults);
                copies.extend(effects.copies);
            }
            let base = inner.owner.names();
            let mut target = inner.names.resolve(&schema, lenses, Some(&base))?;
            let cells = inner.lens_cells(&target, lenses)?;
            let admitted: Vec<Schema> = inner
                .admitted
                .iter()
                .map(|(_, admitted)| inner.named(admitted))
                .collect();
            // Before a lens's defaults fill in: a branch column's default
            // is the schema's own, so a replica that never saw the lens
            // keys rows the same way.
            inner.names.check_branches(&target)?;
            inner.names.check_types(&target)?;
            if make_current {
                for table in target.tables.values() {
                    if let Some(current) = inner.owner.by_stored(&table.stored) {
                        check_branch_removal(current, table)?;
                    }
                }
            }
            for table in target.tables.values_mut() {
                let stored = inner.physical.tables.get(&table.stored);
                for column in &mut table.columns {
                    let earlier = admitted.iter().find_map(|admitted| {
                        admitted
                            .by_stored(&table.stored)
                            .and_then(|t| t.columns.iter().find(|c| c.stored == column.stored))
                    });
                    if earlier.is_some_and(|earlier| earlier.ty != column.ty) {
                        return Err(unsupported(format!(
                            "changing the type of {}.{}",
                            table.name, column.name
                        )));
                    }
                    if stored.is_some_and(|stored| stored.column(&column.stored).is_some()) {
                        continue;
                    }
                    if let Some(default) = defaults.get(&(table.name.clone(), column.name.clone()))
                    {
                        // Rows are keyed by the schema's own default (see
                        // `check_branches` above), so a lens may not key
                        // them by another one.
                        if table.branch_by.contains(&column.name)
                            && column.default.as_ref() != Some(default)
                        {
                            return Err(unsupported(format!(
                                "a lens default for branch column {}.{} other than its declared default",
                                table.name, column.name
                            )));
                        }
                        column.default = Some(default.clone());
                    }
                    // A copy is filled from its source.
                    let copy = copies.contains_key(&(table.name.clone(), column.name.clone()));
                    if stored.is_some() && column.default.is_none() && !column.nullable() && !copy {
                        return Err(unsupported(format!(
                            "adding required column {}.{} without a default",
                            table.name, column.name
                        )));
                    }
                }
            }
            inner.write_admission(&target, cells, make_current)?;
            if make_current {
                inner.owner_version = target.jazz.version_id();
                inner.owner = Rc::new(target);
                // Read policies that changed may read differently from
                // here: a subscription resuming from before opens fresh.
                let mut digest = blake3::Hasher::new();
                for table in &inner.owner.jazz.runtime().tables {
                    digest.update(format!("{}{:?}", table.name, table.read_policy).as_bytes());
                }
                let digest =
                    u64::from_le_bytes(digest.finalize().as_bytes()[..8].try_into().unwrap());
                crate::catalogue::record_activation(
                    &inner.conn,
                    current.flatten(),
                    &inner.owner.jazz,
                    digest,
                    inner.seq,
                )?;
            }
            if !inner
                .admitted
                .iter()
                .any(|(admitted, _)| *admitted == version)
            {
                inner.admitted.push((version, schema));
            }
        }
        let mut inner = self.inner();
        inner.reset_everything();
        // Clients store rows by these names: they learn the new version
        // before its rows.
        for session in &mut inner.sessions {
            session.owe_catalogue();
        }
        Ok(())
    }
}

impl Inner {
    /// Admit an upstream's versions under the names it stores them by
    /// (see `SyncMessage::Catalogue`), so this replica stores rows as it
    /// does. Stored names are derived alike on every store (SPEC 10.7), so
    /// a version this store admitted has the upstream's names already; one
    /// it names otherwise (admitted without the app's lenses) is not
    /// supported yet, and nothing is renamed.
    ///
    /// It then keeps the authority's `namespace` (refusing an upstream of
    /// another app) and the versions the authority `confirmed`. Returns
    /// whether it took any version.
    pub(crate) fn mirror_catalogue(
        &mut self,
        namespace: Option<[u8; 16]>,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
        confirmed: &[([u8; 16], [u8; 16])],
    ) -> Result<bool, Error> {
        let known = self.namespace()?;
        if let (Some(known), Some(namespace)) = (known, namespace)
            && known != namespace
        {
            return Err(Error::new(
                ErrorCode::Schema,
                "the upstream's catalogue is another app's: this store keeps a different authority namespace",
            ));
        }
        let mirrored = !versions.is_empty();
        if mirrored {
            self.mirror_versions(versions, cells)?;
        }
        let learned = known.is_none() && namespace.is_some();
        if let (true, Some(namespace)) = (learned, namespace) {
            self.persist_namespace(namespace)?;
        }
        let conn = &self.conn;
        let told = self.names.confirm(conn, confirmed)? || learned;
        // This store's own sessions (a worker's tabs) are told what it now
        // knows of the authority's catalogue.
        if told && !mirrored {
            for session in &mut self.sessions {
                session.owe_catalogue();
            }
        }
        Ok(mirrored)
    }

    /// Take the upstream's versions, names and lens cells: whole or not at
    /// all, in the file and in memory. A catalogue this store cannot take
    /// (a stored column it keeps with another type) leaves it unchanged.
    fn mirror_versions(
        &mut self,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
    ) -> Result<(), Error> {
        let (physical, names, admitted) = (
            self.physical.clone(),
            self.names.clone(),
            self.admitted.clone(),
        );
        let tags = self.names.tags().snapshot();
        begin_savepoint(&self.conn, "jazz_mirror")?;
        let mirrored = self.mirror_versions_steps(versions, cells);
        let ended = end_savepoint(&self.conn, "jazz_mirror", mirrored.is_ok());
        if let Err(error) = mirrored.and(ended) {
            self.physical = physical;
            self.names = names;
            self.admitted = admitted;
            self.names.tags().restore(tags);
            return Err(error);
        }
        let owner = Rc::new(self.named(&self.owner.jazz));
        self.schema = owner.clone();
        self.owner = owner;
        self.reset_everything();
        // This store's own sessions (a worker's tabs) are sent the versions
        // it just took.
        for session in &mut self.sessions {
            session.owe_catalogue();
        }
        Ok(())
    }

    fn mirror_versions_steps(
        &mut self,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
    ) -> Result<(), Error> {
        use crate::names::{LensCell, LensCellRule};
        let cells: Vec<LensCell> = cells
            .into_iter()
            .map(|cell| LensCell {
                table: cell.table,
                column: cell.column,
                since: crate::ids::SchemaVersionId::from_bytes(cell.since),
                rule: match cell.source {
                    Some(source) => LensCellRule::Copy(source),
                    None => LensCellRule::Dropped(Sql::from(&cell.default)),
                },
            })
            .collect();
        let entries: Vec<(
            crate::ids::SchemaVersionId,
            crate::values::StoredNames,
            JazzSchema,
        )> = versions
            .into_iter()
            .map(|entry| {
                let names = crate::values::StoredNames {
                    tables: entry
                        .names
                        .into_iter()
                        .map(|(table, stored, columns)| {
                            (table, (stored, columns.into_iter().collect()))
                        })
                        .collect(),
                };
                (entry.schema.id, names, entry.schema.schema)
            })
            .collect();
        let mut fresh = std::collections::HashSet::new();
        for (version, names, _) in &entries {
            match self.names.get(version) {
                Some(local) if local == names => {}
                Some(_) => {
                    return Err(unsupported(format!(
                        "schema version {}, which this store stores otherwise than its upstream (it was opened without the app's lenses into it),",
                        uuid::Uuid::from_bytes(*version.as_bytes())
                    )));
                }
                None => {
                    fresh.insert(*version);
                }
            }
        }
        for (version, names, schema) in entries {
            let jazz = if version == self.owner_version {
                self.owner.jazz.clone()
            } else {
                schema
            };
            let target = Schema::stored_as(&jazz, &names, self.names.tags());
            // A copy is filled once, when its version is first admitted.
            let fills = if fresh.contains(&version) {
                cells
                    .iter()
                    .filter(|cell| cell.since == version)
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            // The authority stores a column under a name this store keeps
            // with another type (each re-added it while apart): its values
            // are never read as this store's.
            self.names.check_types(&target)?;
            self.write_admission(&target, fills, version == self.owner_version)?;
            if !self.admitted.iter().any(|(id, _)| *id == version) {
                self.admitted.push((version, jazz));
            }
        }
        let conn = &self.conn;
        self.names.record_lens_cells(conn, cells)
    }

    /// An upstream image as this store keeps it: `None` for a table it
    /// does not store, without columns it does not (an upstream's that no
    /// version this replica knows names).
    pub(crate) fn stored_image(&self, mut image: sync::Image) -> Option<sync::Image> {
        let table = self.physical.by_stored(&image.table)?;
        image
            .cells
            .retain(|(name, _)| name.starts_with('$') || table.by_stored(name).is_some());
        Some(image)
    }

    /// Write an admitted version's stored tables and columns, its lens
    /// copies, names, lens cells and enum tags: whole or not at all, in the
    /// file and in memory. A `current` target (the schema made current)
    /// has its enum columns' tags put in its declaration order.
    fn write_admission(
        &mut self,
        target: &Schema,
        cells: Vec<crate::names::LensCell>,
        current: bool,
    ) -> Result<(), Error> {
        let (physical, names) = (self.physical.clone(), self.names.clone());
        let tags = self.names.tags().snapshot();
        begin_savepoint(&self.conn, "jazz_admit")?;
        let written = self.write_admission_steps(target, cells, current);
        let ended = end_savepoint(&self.conn, "jazz_admit", written.is_ok());
        let written = written.and(ended);
        if written.is_err() {
            self.physical = physical;
            self.names = names;
            self.names.tags().restore(tags);
        }
        written
    }

    fn write_admission_steps(
        &mut self,
        target: &Schema,
        cells: Vec<crate::names::LensCell>,
        current: bool,
    ) -> Result<(), Error> {
        // Variants are tagged before any DDL, so an enum column added with
        // a default stores the default's tag. A new copy starts with its
        // source's tags.
        for cell in &cells {
            if let crate::names::LensCellRule::Copy(source) = &cell.rule {
                self.names
                    .copy_tags(&self.conn, &cell.table, source, &cell.column)?;
            }
        }
        self.names.mint(&self.conn, target)?;
        // The current schema's declaration order is tag order, before any
        // DDL, so a column added with a default stores the default's tag.
        if current {
            self.align_tags(target)?;
        }
        let ddl = self.physical.union(target)?;
        execute_script(&self.conn, &ddl)?;
        // A new copy holds its source's values, in every row version.
        for cell in &cells {
            if let crate::names::LensCellRule::Copy(source) = &cell.rule {
                self.fill_copy(&cell.table, &cell.column, source)?;
            }
        }
        self.names.admit(&self.conn, target)?;
        self.names.record_lens_cells(&self.conn, cells)
    }

    /// Fill copy `column` of stored table `table`, in every row version,
    /// from `source`. An enum copy's tags are its own: a source cell is
    /// mapped to the copy's tag for the same name, which is the same tag
    /// unless the copy's column had tags before.
    fn fill_copy(&self, table: &str, column: &str, source: &str) -> Result<(), Error> {
        let (from, to) = (
            self.names.tags().column(table, source),
            self.names.tags().column(table, column),
        );
        let arms: String = {
            let (from, to) = (from.borrow(), to.borrow());
            from.iter()
                .filter_map(|(tag, name)| {
                    let copied = to.tag(name)?;
                    (copied != tag).then(|| format!(" WHEN {tag} THEN {copied}"))
                })
                .collect()
        };
        self.write_enum_cells(table, column, source, &arms)
    }

    /// Renumber the tags of every enum column of `schema` (the current
    /// schema) so its declared variants are in tag order (see
    /// [`crate::names::Names::align`]), rewriting each changed column's
    /// cells in every row version.
    fn align_tags(&self, schema: &Schema) -> Result<(), Error> {
        for table in schema.tables.values() {
            for column in &table.columns {
                let Some(variants) = &column.variants else {
                    continue;
                };
                let changed =
                    self.names
                        .align(&self.conn, &table.stored, &column.stored, variants)?;
                // A column not stored yet has no cells.
                let stored = self
                    .physical
                    .tables
                    .get(&table.stored)
                    .is_some_and(|t| t.columns.iter().any(|c| c.stored == column.stored));
                if changed.is_empty() || !stored {
                    continue;
                }
                let arms: String = changed
                    .iter()
                    .map(|(old, new)| format!(" WHEN {old} THEN {new}"))
                    .collect();
                self.write_enum_cells(&table.stored, &column.stored, &column.stored, &arms)?;
            }
        }
        Ok(())
    }

    /// Set enum column `column` of stored table `table`, in every row
    /// version, to `source`'s cells with each tag `arms` (`WHEN old THEN
    /// new` arms) names mapped.
    fn write_enum_cells(
        &self,
        table: &str,
        column: &str,
        source: &str,
        arms: &str,
    ) -> Result<(), Error> {
        let array = self
            .names
            .stored_type(table, source)
            .is_some_and(crate::values::is_array_type);
        for stored in [table.to_owned(), format!("{table}__h")] {
            let source_sql = format!("{}.{}", quote(&stored), quote(source));
            let value = match (arms.is_empty(), array) {
                (true, _) => source_sql,
                (false, false) => format!("(CASE {source_sql}{arms} ELSE {source_sql} END)"),
                (false, true) => format!(
                    "CASE WHEN {source_sql} IS NULL THEN NULL ELSE (SELECT json_group_array(\
                     CASE j.value{arms} ELSE j.value END ORDER BY j.key) \
                     FROM json_each({source_sql}) AS j) END"
                ),
            };
            self.conn
                .execute(
                    &format!("UPDATE {} SET {} = {value}", quote(&stored), quote(column)),
                    [],
                )
                .map_err(storage)?;
        }
        Ok(())
    }
}

/// A version made current may not drop a branch column of the current
/// one (SPEC 4, INV-BVIEW-19): an older version admitted beside it names
/// that column by its default. The other branch-column changes are refused
/// for every admitted version ([`crate::names::Names::check_branches`]).
fn check_branch_removal(current: &Table, next: &Table) -> Result<(), Error> {
    for column in &current.branch_by {
        let stored = current.stored_name(column);
        if !next.columns.iter().any(|c| c.stored == stored) {
            return Err(unsupported(format!(
                "removing branch column {}.{column}",
                next.name
            )));
        }
    }
    Ok(())
}

/// Whether a policy reads rows besides the one it decides on.
fn policy_reads_other_rows(policy: &PolicyExpr) -> bool {
    match policy {
        PolicyExpr::Exists { .. }
        | PolicyExpr::ExistsRel { .. }
        | PolicyExpr::Inherits { .. }
        | PolicyExpr::InheritsReferencing { .. } => true,
        PolicyExpr::And(parts) | PolicyExpr::Or(parts) => parts.iter().any(policy_reads_other_rows),
        PolicyExpr::Not(inner) => policy_reads_other_rows(inner),
        _ => false,
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
    /// Who every write is made as, chosen when the transaction opens.
    identity: WriteIdentity,
}

/// The identity of a write inside a transaction opened as `tx`: its own
/// options may not choose another.
fn in_transaction(own: WriteIdentity, tx: WriteIdentity) -> Result<WriteIdentity, Error> {
    if own != WriteIdentity::Database {
        return Err(Error::new(
            ErrorCode::Schema,
            "transaction identity is selected when the transaction is opened",
        ));
    }
    Ok(tx)
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
                    identity: in_transaction(opts.identity, self.identity)?,
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
                    identity: in_transaction(opts.identity, self.identity)?,
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
                    identity: in_transaction(opts.identity, self.identity)?,
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
                    identity: in_transaction(opts.identity, self.identity)?,
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

impl Drop for Inner {
    fn drop(&mut self) {
        // An abandoned handle's overlay never reaches the file.
        self.settle_overlay();
        // Before `conn` closes, so it closes last and checkpoints the WAL.
        self.chunks.close_reader();
        crate::values::forget_tag_undo(&self.conn);
    }
}

impl Inner {
    fn require_mutation_admission(&self) -> Result<(), Error> {
        if self.mutation_admission_closed {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        Ok(())
    }

    /// Undo everything since `SAVEPOINT name`. Its changes are forgotten
    /// first, so no subscriber hears of them whatever happens next.
    fn abandon_savepoint(&self, name: &str) -> Result<(), Error> {
        self.changed.borrow_mut().clear();
        end_savepoint(&self.conn, name, false)
    }

    /// An admitted schema under the names this store keeps it by.
    fn named(&self, schema: &JazzSchema) -> Schema {
        match self.names.get(&schema.version_id()) {
            Some(names) => Schema::stored_as(schema, names, self.names.tags()),
            None => Schema::new(schema, self.names.tags()),
        }
    }

    /// The current schema named by stored names: the schema replicated
    /// writes travel and apply by (see [`Schema::stored_view`]).
    pub(crate) fn stored_view(&self) -> Rc<Schema> {
        let columns = self
            .physical
            .tables
            .values()
            .map(|table| table.columns.len() + 1)
            .sum();
        let owner = Rc::as_ptr(&self.owner);
        let mut cached = self.stored_view.borrow_mut();
        if let Some((built, count, view)) = &*cached
            && *built == owner
            && *count == columns
        {
            return view.clone();
        }
        let view = Rc::new(Schema::stored_view(&self.owner, &self.physical));
        *cached = Some((owner, columns, view.clone()));
        view
    }

    /// `op`, written through `schema`, as it travels: by the stored names
    /// of its table and cells. An insert carries every column `schema`
    /// declares (its default, or null), so its receiver, which applies it
    /// under [`Inner::stored_view`], fills only what that version lacks.
    pub(crate) fn stored_op(&self, op: StagedOp, schema: &Schema) -> Result<StagedOp, Error> {
        if schema.stored_names {
            return Ok(op);
        }
        let table = schema.table(op.table())?;
        let stored = |cells: BTreeMap<String, Value>| -> BTreeMap<String, Value> {
            cells
                .into_iter()
                .map(|(name, value)| (table.stored_name(&name).to_owned(), value))
                .collect()
        };
        let name = table.stored.clone();
        // The columns this version's lineage dropped, with their backwards
        // defaults: a receiver cannot tell which lenses a writer descends
        // from, so the writer names them (SPEC 8.9).
        let version = if std::ptr::eq(schema, &*self.owner) {
            self.owner_version
        } else {
            schema.jazz.version_id()
        };
        let mut dropped = BTreeMap::new();
        if self.names.has_lens_cells() {
            let physical = self.physical.tables.get(&table.stored);
            for (column, default) in self.names.lens_cells(&version, table).1 {
                let Some(stored) = physical.and_then(|physical| physical.column(&column)) else {
                    continue;
                };
                let value = crate::values::column_from_sql(stored, default.as_ref(), &self.chunks)?
                    .unwrap_or(Value::Nullable(None));
                dropped.insert(column, value);
            }
        }
        // What a write that creates its row gives the columns it leaves out:
        // its version's defaults (branch columns stay as given: the target,
        // or else the column's immutable default, keys the row,
        // INV-BVIEW-19) and the dropped columns' backwards defaults.
        let created = |cells: &BTreeMap<String, Value>| -> BTreeMap<String, Value> {
            let mut created = BTreeMap::new();
            for column in &table.columns {
                if cells.contains_key(&column.name) || table.branch_by.contains(&column.name) {
                    continue;
                }
                let value = match &column.default {
                    Some(default) => default.clone(),
                    None if column.nullable() => Value::Nullable(None),
                    None => continue,
                };
                created.insert(column.stored.clone(), value);
            }
            created.extend(dropped.clone());
            created
        };
        // Branch selectors name branch columns too.
        let selector = |selector: BranchSelector| BranchSelector {
            values: selector
                .values
                .into_iter()
                .map(|(name, value)| (table.stored_name(&name).to_owned(), value))
                .collect(),
        };
        let exact = |target: ExactWriteTarget| match target {
            ExactWriteTarget::Root => ExactWriteTarget::Root,
            ExactWriteTarget::Branch(branch) => ExactWriteTarget::Branch(selector(branch)),
        };
        let view = |target: WriteTarget| match target {
            WriteTarget::Root => WriteTarget::Root,
            WriteTarget::BranchView { head, base } => WriteTarget::BranchView {
                head: selector(head),
                base: base.map(|base| match base {
                    BranchViewBase::Current(base) => BranchViewBase::Current(selector(base)),
                    BranchViewBase::Snapshot { branch, snapshot } => BranchViewBase::Snapshot {
                        branch: selector(branch),
                        snapshot,
                    },
                }),
            },
        };
        Ok(match op {
            StagedOp::Insert(_, cells, mut opts) => {
                opts.target = exact(opts.target);
                let created = created(&cells);
                let mut cells = stored(cells);
                cells.extend(created);
                StagedOp::Insert(name, cells, opts)
            }
            StagedOp::Update(_, row, cells, mut opts) => {
                opts.target = view(opts.target);
                StagedOp::Update(name, row, stored(cells), opts)
            }
            StagedOp::Upsert(_, row, cells, mut extra, mut opts) => {
                opts.target = view(opts.target);
                extra.extend(created(&cells));
                StagedOp::Upsert(name, row, stored(cells), extra, opts)
            }
            StagedOp::Delete(_, row, mut opts) => {
                opts.target = view(opts.target);
                StagedOp::Delete(name, row, opts)
            }
            StagedOp::Restore(_, row, cells, mut opts) => {
                opts.target = exact(opts.target);
                StagedOp::Restore(name, row, cells.map(stored), opts)
            }
            StagedOp::Move(_, from, to, row) => {
                StagedOp::Move(name, selector(from), selector(to), row)
            }
        })
    }

    /// The lens cells the lenses into `target` make on this store: a copy
    /// not stored yet, and each dropped column. Only lenses from a version
    /// this store admitted count: a store without the source holds no row
    /// an older version wrote.
    fn lens_cells(
        &self,
        target: &Schema,
        lenses: &[MigrationLens],
    ) -> Result<Vec<crate::names::LensCell>, Error> {
        use crate::names::{LensCell, LensCellRule};
        let version = target.jazz.version_id();
        let mut cells = Vec::new();
        for lens in lenses.iter().filter(|lens| lens.target == version) {
            let source = if lens.source == self.owner_version {
                (*self.owner).clone()
            } else {
                match self.admitted.iter().find(|(id, _)| *id == lens.source) {
                    Some((_, jazz)) => self.named(jazz),
                    None => continue,
                }
            };
            for table_lens in &lens.table_lenses {
                let (Some(from), Some(to)) = (
                    source.tables.get(&table_lens.source_table),
                    target.tables.get(&table_lens.target_table),
                ) else {
                    continue;
                };
                for op in &table_lens.ops {
                    match op {
                        LensOp::CopyColumn {
                            from: column,
                            to: copy,
                        } => {
                            let Some(source_column) = from.column(column) else {
                                return Err(schema_error(format!(
                                    "lens copies `{}.{column}`, but the source has no `{column}`",
                                    from.name
                                )));
                            };
                            if from.column(copy).is_some() {
                                return Err(schema_error(format!(
                                    "lens copies `{}.{column}` into `{copy}`, which the source already has",
                                    from.name
                                )));
                            }
                            let Some(copy_column) = to.column(copy) else {
                                return Err(schema_error(format!(
                                    "lens copies `{}.{column}` into `{copy}`, which `{}` lacks",
                                    from.name, to.name
                                )));
                            };
                            if copy_column.ty != source_column.ty {
                                return Err(unsupported(format!(
                                    "copying {}.{column} into a column of another type",
                                    from.name
                                )));
                            }
                            let stored = self.physical.tables.get(&to.stored);
                            if stored
                                .is_none_or(|stored| stored.column(&copy_column.stored).is_none())
                            {
                                cells.push(LensCell {
                                    table: to.stored.clone(),
                                    column: copy_column.stored.clone(),
                                    since: version,
                                    rule: LensCellRule::Copy(source_column.stored.clone()),
                                });
                            }
                        }
                        LensOp::DropColumn {
                            column,
                            backwards_default,
                        } => {
                            let Some(dropped) = from.column(column) else {
                                return Err(schema_error(format!(
                                    "lens drops `{}.{column}`, but the source has no `{column}`",
                                    from.name
                                )));
                            };
                            if to.columns.iter().any(|c| c.stored == dropped.stored) {
                                continue;
                            }
                            let default = crate::values::to_sql(backwards_default)?
                                .ok_or_else(|| unsupported("a NaN backwards default"))?;
                            cells.push(LensCell {
                                table: to.stored.clone(),
                                column: dropped.stored.clone(),
                                since: version,
                                rule: LensCellRule::Dropped(default),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(cells)
    }

    /// Runs a write through the statement cache. Writes to app tables
    /// fire the large-value refcount triggers, so preparing one compiles
    /// those trigger programs too; re-preparing per row dominated bulk
    /// writes.
    fn exec<P: rusqlite::Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.conn.prepare_cached(sql)?.execute(params)
    }

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
        self.read_ctx(prepared, policy.as_ref(), &claims, read)
            .compile_query(&prepared.query, read.include_deleted)
    }

    /// The compile context of a read of `prepared` (or of one of its
    /// relations) as `policy`, with `claims`, per `read`.
    pub(crate) fn read_ctx<'s>(
        &'s self,
        prepared: &'s PreparedQuery,
        policy: Option<&'s Session>,
        claims: &'s Session,
        read: &ReadSpec,
    ) -> Ctx<'s> {
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        Ctx::new(schema, policy, claims, &prepared.binding)
            .with_view(read.view.clone())
            .with_withheld(self.withheld.get())
            .with_cut(read.cut.clone(), read.settled_only)
            .with_history_origin(self.history_origin())
    }

    /// The node history reads name this store's own commits by: set on the
    /// authority only (see `compile::Ctx::history_origin`).
    pub(crate) fn history_origin(&self) -> Option<[u8; 16]> {
        self.client
            .is_none()
            .then(|| *self.identity.node.as_bytes())
    }

    pub(crate) fn run(
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
        let visible = self.visible_read(read, &compiled.tables);
        if let std::borrow::Cow::Owned(visible) = &visible {
            compiled = self.compile(prepared, identity, visible)?;
        }
        let rows = self.execute(&compiled, &prepared.query.table)?;
        // Array relations are part of a subscription's dependencies.
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        fn arrays(
            schema: &Schema,
            out: &mut BTreeSet<String>,
            list: &[crate::query::ArraySubquery],
        ) -> Result<(), Error> {
            for array in list {
                out.insert(schema.stored_table(&array.table)?.to_owned());
                arrays(schema, out, &array.nested_arrays)?;
            }
            Ok(())
        }
        arrays(
            schema,
            &mut compiled.tables,
            &prepared.query.array_subqueries,
        )?;
        Ok((rows, compiled.tables))
    }

    fn execute(&self, compiled: &Compiled, table: &str) -> Result<Vec<CurrentRow>, Error> {
        let started = web_time::Instant::now();
        let mut run = crate::slow::Run::default();
        let rows = self.execute_sql(&compiled.sql, compiled, table, &mut run)?;
        run.elapsed = started.elapsed();
        let mut slow = self.slow.borrow_mut();
        if slow.is_slow(run.elapsed) {
            run.rows = rows.len() as u64;
            let schema_version = self
                .conn
                .query_row("PRAGMA schema_version", [], |r| r.get(0))
                .ok();
            slow.record(table, &compiled.sql, run, schema_version, || {
                self.query_plan(&compiled.sql, &compiled.params)
                    .unwrap_or_else(|error| format!("(no plan: {error})"))
            });
        }
        Ok(rows)
    }

    /// SQLite's plan for `sql`, one step per line.
    fn query_plan(&self, sql: &str, params: &[Sql]) -> Result<String, Error> {
        let mut statement = self
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(storage)?;
        let steps = statement
            .query_map(params_from_iter(params.iter()), |r| r.get::<_, String>(3))
            .map_err(storage)?;
        let mut plan = Vec::new();
        for step in steps {
            plan.push(step.map_err(storage)?);
        }
        Ok(plan.join("\n"))
    }

    fn execute_sql(
        &self,
        sql: &str,
        compiled: &Compiled,
        table: &str,
        run: &mut crate::slow::Run,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut statement = self.conn.prepare_cached(sql).map_err(storage)?;
        // Cached statements keep counting across runs: start this one at 0.
        statement.reset_status(rusqlite::StatementStatus::FullscanStep);
        statement.reset_status(rusqlite::StatementStatus::Sort);
        let names: Arc<str> = Arc::from(table);
        let types: Arc<[ValueType]> = compiled
            .projection
            .iter()
            .map(|(_, ty, _)| ty.clone())
            .collect();
        let mut rows = statement
            .query(params_from_iter(compiled.params.iter()))
            .map_err(storage)?;
        let mut out = Vec::new();
        // A replica missing chunks reads on past the first blocked cell, so
        // one round trip asks for every row's missing nodes.
        let mut blocked = false;
        // A union's arm labels, shared by their rows.
        let mut arms: Vec<Arc<str>> = Vec::new();
        while let Some(row) = rows.next().map_err(storage)? {
            let id: Vec<u8> = if compiled.aggregate {
                // A group's key: the group-by cell, a chunked one by its
                // identity (SPEC 19.11).
                let key = format!("{:?}", Sql::from(row.get_ref(0).map_err(storage)?));
                blake3::hash(key.as_bytes()).as_bytes()[..16].to_vec()
            } else {
                row.get(0).map_err(storage)?
            };
            let n: i64 = row.get(1).map_err(storage)?;
            let seq: i64 = row.get(2).map_err(storage)?;
            let state: i64 = row.get(3).map_err(storage)?;
            // A flat join: the root's version, then each joined row and
            // its version.
            let flat = compiled.joined > 0;
            let version = |at: usize| -> Result<Option<crate::values::SourceVersion>, Error> {
                let Some(tx) = row.get::<_, Option<Vec<u8>>>(at).map_err(storage)? else {
                    return Ok(None);
                };
                let corrupt = || Error::new(ErrorCode::Storage, "transaction identity");
                let (time, node) = tx.split_at_checked(8).ok_or_else(corrupt)?;
                Ok(Some((
                    u64::from_be_bytes(time.try_into().map_err(|_| corrupt())?),
                    crate::ids::NodeUuid(uuid::Uuid::from_slice(node).map_err(|_| corrupt())?),
                )))
            };
            let root_version = if compiled.versioned {
                version(4)?
            } else {
                None
            };
            let joined = (0..compiled.joined)
                .map(|i| {
                    let id: Vec<u8> = row.get(5 + 2 * i).map_err(storage)?;
                    Ok(crate::values::JoinedRow {
                        row: uuid::Uuid::from_slice(&id)
                            .map(RowUuid)
                            .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                        version: version(6 + 2 * i)?,
                    })
                })
                .collect::<Result<Arc<[_]>, Error>>()?;
            let union_arm = if compiled.union {
                let label: String = row.get(4).map_err(storage)?;
                Some(match arms.iter().find(|arm| ***arm == *label) {
                    Some(arm) => arm.clone(),
                    None => {
                        let arm: Arc<str> = Arc::from(label);
                        arms.push(arm.clone());
                        arm
                    }
                })
            } else {
                None
            };
            let first = if flat {
                5 + 2 * compiled.joined
            } else {
                4 + usize::from(compiled.versioned) + usize::from(compiled.union)
            };
            let cells = compiled
                .projection
                .iter()
                .enumerate()
                .map(|(i, (name, ty, tags))| {
                    let cell = row.get_ref(first + i).map_err(storage)?;
                    let value = match compiled.partial.get(i).and_then(Option::as_ref) {
                        Some(part) => self.partial_cell(ty, cell, part),
                        None => cell_from_sql(ty, tags.as_ref(), cell, &self.chunks),
                    };
                    match value {
                        Err(error) if crate::large::is_missing(&error) => {
                            blocked = true;
                            Ok((name.clone(), None))
                        }
                        value => Ok((name.clone(), value?)),
                    }
                })
                .collect::<Result<Vec<_>, Error>>()?;
            out.push(CurrentRow {
                table: names.clone(),
                id: RowUuid(
                    uuid::Uuid::from_slice(&id)
                        .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                ),
                seq: Pos { n, seq: seq as u64 },
                deleted: state & STATE_DELETED != 0,
                cells: Arc::new(cells),
                types: types.clone(),
                joined,
                version: root_version,
                union_arm,
            });
        }
        drop(rows);
        run.scanned += statement.reset_status(rusqlite::StatementStatus::FullscanStep) as u64;
        run.sorted |= statement.reset_status(rusqlite::StatementStatus::Sort) > 0;
        if blocked {
            return Err(crate::large::missing());
        }
        Ok(out)
    }

    /// Root rows with each array relation nested in a cell under its
    /// column name ([`ResultNode::into_nested_row`]), read as `identity`.
    pub(crate) fn nest_relations(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        roots: Vec<CurrentRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        if prepared.query.array_subqueries.is_empty() {
            return Ok(roots);
        }
        roots
            .into_iter()
            .map(|row| {
                self.node(
                    prepared,
                    identity,
                    read,
                    &prepared.query.table,
                    &prepared.query.array_subqueries,
                    row,
                )?
                .into_nested_row()
            })
            .collect()
    }

    /// The versions of the rows a read returns, its relations' rows
    /// included: what a serializable read set compares at commit.
    pub(crate) fn read_versions(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<(RowUuid, Pos)>, Error> {
        let roots = self.run(prepared, identity, read)?;
        let mut out = versions(&roots);
        if !prepared.query.array_subqueries.is_empty() {
            for row in roots {
                let node = self.node(
                    prepared,
                    identity,
                    read,
                    &prepared.query.table,
                    &prepared.query.array_subqueries,
                    row,
                )?;
                child_versions(&node, &mut out);
            }
        }
        Ok(out)
    }

    /// A row with its array relations, read as the row was: through the
    /// read's view and cut, settled-only and members-only as it is.
    fn node(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        table: &str,
        arrays: &[crate::query::ArraySubquery],
        row: CurrentRow,
    ) -> Result<ResultNode, Error> {
        let mut relations = BTreeMap::new();
        for array in arrays {
            let (policy, claims) = self.read_sessions(prepared, identity);
            let ctx = || self.read_ctx(prepared, policy.as_ref(), &claims, read);
            let parent = if array.outer_column == "id" {
                Value::Uuid(row.id.0)
            } else {
                let (sql, params, ty, tags) =
                    ctx().compile_cell(table, &array.outer_column, row.id)?;
                let cell: Option<Sql> = self
                    .conn
                    .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
                    .optional()
                    .map_err(storage)?;
                match cell {
                    Some(cell) => cell_from_sql(&ty, tags.as_ref(), cell.as_ref(), &self.chunks)?
                        .unwrap_or(Value::Nullable(None)),
                    None => Value::Nullable(None),
                }
            };
            let compiled = ctx().compile_children(array, &parent)?;
            let children = self.execute(&compiled, &array.table)?;
            let mut nodes = Vec::with_capacity(children.len());
            for child in children {
                nodes.push(self.node(
                    prepared,
                    identity,
                    read,
                    &array.table,
                    &array.nested_arrays,
                    child,
                )?);
            }
            relations.insert(array.column_name.clone(), ResultRelation::Array(nodes));
        }
        Ok(ResultNode { row, relations })
    }

    // ---- write primitives ---------------------------------------------

    /// Where this store's own commit `seq` writes its versions: the
    /// authority's seq, or its own node's on a replica.
    pub(crate) fn own_pos(&self, seq: u64) -> Pos {
        Pos { n: self.me, seq }
    }

    fn existing(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<(Pos, bool)>, Error> {
        let sql = format!(
            "SELECT _n, _seq, (_st & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _b = ?2",
            quote(table)
        );
        match self.conn.cached_row(
            &sql,
            rusqlite::params![row.0.as_bytes().to_vec(), key],
            |r| {
                Ok((
                    Pos {
                        n: r.get(0)?,
                        seq: r.get::<_, i64>(1)? as u64,
                    },
                    r.get::<_, i64>(2)? != 0,
                ))
            },
        ) {
            Ok(found) => Ok(Some(found)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// [`Inner::existing`] as of `cut` (the current row when `None`): the
    /// newest version the cut sees, current or superseded.
    fn existing_at(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        cut: Option<&Cut>,
    ) -> Result<Option<(Pos, bool)>, Error> {
        let current = self.existing(table, row, key)?;
        let Some(cut) = cut else {
            return Ok(current);
        };
        if let Some((seq, _)) = current
            && cut.sees(seq)
        {
            return Ok(current);
        }
        let Some(version) = self.version_at(table, row, key, cut)? else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT (_st & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _b = ?2 \
             AND _n = ?3 AND _seq = ?4",
            quote(&format!("{table}__h"))
        );
        let deleted: i64 = self
            .conn
            .cached_row(
                &sql,
                rusqlite::params![
                    row.0.as_bytes().to_vec(),
                    key,
                    version.n,
                    version.seq as i64
                ],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(Some((version, deleted != 0)))
    }

    /// The newest superseded version of a row that `cut` sees.
    fn version_at(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        cut: &Cut,
    ) -> Result<Option<Pos>, Error> {
        let extra = if cut.extra.is_empty() {
            String::new()
        } else {
            let positions = cut
                .extra
                .iter()
                .map(|at| format!("({}, {})", at.n, at.seq))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" OR (_n, _seq) IN (VALUES {positions})")
        };
        let sql = format!(
            "SELECT _n, _seq FROM {} WHERE id = ?1 AND _b = ?2 AND (_st & {STATE_AS_SENT}) = 0 \
             AND ((_n, _seq) <= (?3, ?4){extra}) ORDER BY _n DESC, _seq DESC LIMIT 1",
            quote(&format!("{table}__h"))
        );
        self.conn
            .cached_row(
                &sql,
                rusqlite::params![
                    row.0.as_bytes().to_vec(),
                    key,
                    cut.seq.n,
                    cut.seq.seq as i64
                ],
                |r| {
                    Ok(Pos {
                        n: r.get(0)?,
                        seq: r.get::<_, i64>(1)? as u64,
                    })
                },
            )
            .optional()
            .map_err(storage)
    }

    /// Resolve which stored row a patch-style write addresses.
    fn locate(&self, table: &Table, row: RowUuid, target: &Target) -> Result<Located, Error> {
        let at = |key: Vec<u8>, cells: BTreeMap<String, Value>| -> Result<Located, Error> {
            Ok(match self.existing(&table.stored, row, &key)? {
                Some((seq, deleted)) => Located::Row { key, seq, deleted },
                None => {
                    self.refuse_withheld(table, row, &key)?;
                    Located::Absent { cells }
                }
            })
        };
        match target {
            Target::Root if table.branch_by.is_empty() => at(Vec::new(), BTreeMap::new()),
            Target::Root => {
                let sql = format!("SELECT _b FROM {} WHERE id = ?1", quote(&table.stored));
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
            Target::View { head, base, at } => {
                let head_key = table.selector_key(head)?;
                let head_cells = if table.branch_by.is_empty() {
                    BTreeMap::new()
                } else {
                    table.selector_cells(head)?
                };
                if let Some((seq, deleted)) = self.existing(&table.stored, row, &head_key)? {
                    return Ok(Located::Row {
                        key: head_key,
                        seq,
                        deleted,
                    });
                }
                self.refuse_withheld(table, row, &head_key)?;
                if let Some(base) = base {
                    let base_key = table.selector_key(base)?;
                    if base_key != head_key
                        && let Some((_, false)) =
                            self.existing_at(&table.stored, row, &base_key, at.as_ref())?
                    {
                        return Ok(Located::Inherited {
                            from: base_key,
                            to: head_key,
                            head_cells,
                            at: at.clone(),
                        });
                    }
                }
                Ok(Located::Absent { cells: head_cells })
            }
        }
    }

    /// A write to a version this replica may not read (a branch view hides
    /// the row, SPEC 8.4) is refused: it would land over a version nobody
    /// here can see.
    fn refuse_withheld(&self, table: &Table, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        if self.is_withheld(&table.stored, row, key)? {
            return Err(rejected(format!(
                "row {} of `{}` is hidden here: its version on this branch is not readable",
                row.0, table.name
            )));
        }
        Ok(())
    }

    /// Evaluate a policy SQL boolean against one stored row.
    fn holds(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        self.holds_at(table, row, key, None, policy)
    }

    /// [`Inner::holds`] with the policy's other rows read as of `cut`; the
    /// row under check is always its current version.
    fn holds_at(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        self.holds_counting(table, row, key, cut, policy)
            .map(|(holds, _)| holds)
    }

    /// [`Inner::holds_at`], also returning what of a write check's overlay
    /// the check read.
    fn holds_counting(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<(bool, OverlayRead), Error> {
        let (sql, params, read) = self.policy_check(table, row, key, cut, policy)?;
        let value: i64 = self
            .conn
            .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .map_err(storage)?;
        Ok((value != 0, read))
    }

    /// The statement [`Inner::holds_at`] runs: the policy SQL boolean for
    /// one stored row, its parameters, and what of the cut's overlay it
    /// reads.
    fn policy_check(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<(String, Vec<Sql>, OverlayRead), Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding).with_cut(cut, false);
        let condition = policy(&mut ctx)?;
        let read = OverlayRead {
            rows: ctx.overlaid_rows,
            tables: std::mem::take(&mut ctx.overlay_tables),
        };
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition}, 0) FROM {} AS a WHERE a.id = ?{} AND a._b = ?{}",
            quote(&table.stored),
            params.len() - 1,
            params.len()
        );
        Ok((sql, params, read))
    }

    /// SQLite's plan for the INSERT check a unit ending now would run on
    /// `row` (SPEC 7): its policy reads committed state, overlaid with the
    /// unit's rows `overlay` names.
    fn explain_write_check(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
        overlay: CutOverlay,
    ) -> Result<String, Error> {
        let table = self.schema.table(table)?;
        let own = self.own_pos(self.seq + 1);
        let cut = Cut {
            unsettled: false,
            seq: self.own_pos(self.seq),
            own,
            extra: Vec::new(),
            overlay: Some(overlay),
        };
        let session = self.session(author);
        let (sql, params, _) = self.policy_check(table, row, &[], Some(cut), |ctx| {
            ctx.operation_policy(table, Operation::Insert, "a", &session)
        })?;
        let mut statement = self
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(storage)?;
        let rows = statement
            .query_map(params_from_iter(params.iter()), |r| r.get::<_, String>(3))
            .map_err(storage)?;
        let mut plan = Vec::new();
        for row in rows {
            plan.push(row.map_err(storage)?);
        }
        Ok(plan.join(" | "))
    }

    /// [`Inner::holds`] for `row` as of `cut`: the row under check and
    /// the policy's other rows are each read as of `cut`.
    fn holds_as_of(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Cut,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding).with_cut(Some(cut), false);
        let condition = policy(&mut ctx)?;
        let source = ctx.source(table)?;
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition}, 0) FROM {source} AS a WHERE a.id = ?{} AND a._b = ?{}",
            params.len() - 1,
            params.len()
        );
        let value: Option<i64> = self
            .conn
            .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .optional()
            .map_err(storage)?;
        Ok(value.is_some_and(|value| value != 0))
    }

    /// Whether `author` may `operation` the version of `row` at `at`, with
    /// the policy's other rows as they stood then too.
    pub(crate) fn check_version(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        at: Pos,
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        self.holds_as_of(table, row, key, Cut::at(at), |ctx| {
            ctx.operation_policy(table, operation, "a", &session)
        })
    }

    pub(crate) fn check(
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

    /// Check a write's own policy as SPEC 7 has it: on committed state
    /// overlaid with the unit's grounded rows. Inside a unit
    /// ([`Inner::defer_checks`]) the check waits for the unit's end;
    /// alone, nothing else is grounded and it runs now.
    fn write_check(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        check: WriteCheck,
        seq: u64,
    ) -> Result<(), Error> {
        let deferred = DeferredCheck {
            table: table.name.clone(),
            row,
            key: key.to_vec(),
            author,
            check,
            replaces: false,
        };
        if let Some(pending) = self.deferred.borrow_mut().as_mut() {
            // A row's first write decides which policy its unit's end state
            // must pass.
            if !pending
                .iter()
                .any(|p| p.table == deferred.table && p.row == row && p.key == deferred.key)
            {
                pending.push(deferred);
            }
            return Ok(());
        }
        self.ground(vec![deferred], seq)
    }

    /// Start a unit: write checks wait for [`Inner::finish_checks`].
    pub(crate) fn defer_checks(&self) {
        *self.deferred.borrow_mut() = Some(Vec::new());
    }

    /// Drop a unit's pending checks (it rolled back).
    pub(crate) fn abandon_checks(&self) {
        *self.deferred.borrow_mut() = None;
    }

    /// End a unit written at `seq`: ground its writes (SPEC 7). The first
    /// round checks every write on committed state alone; a write that
    /// passes is grounded, and from the next round on its row shows its
    /// end state to the others. A write that never passes rejects the
    /// unit. A row the unit leaves deleted has no end state to check.
    /// Once every write is grounded, a write whose pass the rest of the
    /// unit could have taken away is checked again against the unit's
    /// whole post-state (see [`Inner::post_state_pass`]).
    pub(crate) fn finish_checks(&self, seq: u64) -> Result<(), Error> {
        let pending = self.deferred.borrow_mut().take().unwrap_or_default();
        self.ground(pending, seq)
    }

    fn ground(&self, mut pending: Vec<DeferredCheck>, seq: u64) -> Result<(), Error> {
        let schema = self.schema.clone();
        let mut live = Vec::with_capacity(pending.len());
        let several = pending.len() > 1;
        for mut check in pending.drain(..) {
            let table = schema.table(&check.table)?;
            if !self.is_deleted_version(table, check.row, &check.key)? {
                // Only a unit of several writes has a post-state pass.
                check.replaces = several
                    && (matches!(check.check, WriteCheck::WithCheck { .. })
                        || self.was_live_before(table, check.row, &check.key, seq)?);
                live.push(check);
            }
        }
        let mut pending = live;
        let mut grounded: BTreeMap<String, BTreeSet<[u8; 16]>> = BTreeMap::new();
        // Each write with the round it passed in and the tables its policy
        // reads through the overlay.
        let mut rounds: Vec<(DeferredCheck, usize, BTreeSet<String>)> =
            Vec::with_capacity(pending.len());
        let mut round = 0;
        let mut evidence = EvidenceBudget::default();
        loop {
            let cut = Cut {
                unsettled: false,
                seq: self.own_pos(seq.saturating_sub(1)),
                own: self.own_pos(seq),
                extra: Vec::new(),
                overlay: Some(CutOverlay::Grounded(Arc::new(grounded.clone()))),
            };
            let mut passed = Vec::new();
            let mut failed = Vec::new();
            for check in pending {
                let (holds, tables) =
                    self.write_check_holds(&schema, &check, cut.clone(), &mut evidence)?;
                if holds {
                    passed.push((check, tables));
                } else {
                    failed.push(check);
                }
            }
            if !failed.is_empty() && passed.is_empty() {
                return Err(write_check_denied(&failed[0]));
            }
            for (check, tables) in passed {
                let stored = schema.table(&check.table)?.stored.clone();
                grounded
                    .entry(stored)
                    .or_default()
                    .insert(*check.row.0.as_bytes());
                rounds.push((check, round, tables));
            }
            if failed.is_empty() {
                break;
            }
            pending = failed;
            round += 1;
        }
        self.post_state_pass(&schema, rounds, seq, &mut evidence)
    }

    /// Check `check`'s policy on the rows `cut` shows. Also returns the
    /// stored tables the policy read through the cut's overlay.
    fn write_check_holds(
        &self,
        schema: &Schema,
        check: &DeferredCheck,
        cut: Cut,
        evidence: &mut EvidenceBudget,
    ) -> Result<(bool, BTreeSet<String>), Error> {
        let table = schema.table(&check.table)?;
        let session = self.session(check.author);
        let (holds, read) =
            self.holds_counting(table, check.row, &check.key, Some(cut), |ctx| {
                match check.check {
                    WriteCheck::Insert => {
                        ctx.operation_policy(table, Operation::Insert, "a", &session)
                    }
                    WriteCheck::Restore => {
                        ctx.operation_policy(table, Operation::Update, "a", &session)
                    }
                    // Without a WITH CHECK, the new row must pass USING, as in
                    // Postgres.
                    WriteCheck::WithCheck { .. } => {
                        let update = &table.policies.update;
                        let clause = update.with_check.as_ref().or(update.using.as_ref());
                        ctx.policy_clause(clause, table, "a", &session)
                    }
                }
            })?;
        evidence.charge(read.rows)?;
        Ok((holds, read.tables))
    }

    /// The unit is accepted only if every write's policy also holds on the
    /// state the whole unit leaves behind (SPEC 7, INV-RLS-9): committed
    /// state overlaid with every write of the unit but the row under
    /// check, which is never evidence for itself. A write is checked again
    /// only when a table its policy reads holds a write grounded with or
    /// after it that could have taken its pass away: any write, under a
    /// policy that is not monotone in the rows present, or else one that
    /// replaced a row's earlier content. A unit that only adds rows under
    /// monotone policies is checked once per write.
    ///
    /// The post-state overlays every live row of the unit, so it binds no
    /// id list: a re-check reaches the unit's rows through the policy's
    /// correlated keys, like committed ones, and reads none of them
    /// wholesale. It is not charged to the evidence budget.
    fn post_state_pass(
        &self,
        schema: &Schema,
        rounds: Vec<(DeferredCheck, usize, BTreeSet<String>)>,
        seq: u64,
        evidence: &mut EvidenceBudget,
    ) -> Result<(), Error> {
        // Per stored table and round: how many writes passed in it, and
        // how many of those replaced a row's earlier content; then suffix
        // sums, so index `r` counts the writes grounded in round `r` or
        // later.
        let last = rounds.iter().map(|(_, r, _)| *r).max().unwrap_or(0);
        let mut later: HashMap<String, (Vec<usize>, Vec<usize>)> = HashMap::new();
        let mut stored_of = Vec::with_capacity(rounds.len());
        for (check, round, _) in &rounds {
            let stored = schema.table(&check.table)?.stored.clone();
            let (written, replaced) = later
                .entry(stored.clone())
                .or_insert_with(|| (vec![0; last + 2], vec![0; last + 2]));
            written[*round] += 1;
            if check.replaces {
                replaced[*round] += 1;
            }
            stored_of.push(stored);
        }
        for (written, replaced) in later.values_mut() {
            for r in (0..=last).rev() {
                written[r] += written[r + 1];
                replaced[r] += replaced[r + 1];
            }
        }
        let mut monotone: HashMap<(String, u8), bool> = HashMap::new();
        for ((check, round, tables), stored) in rounds.iter().zip(&stored_of) {
            // The writes grounded with or after this one, in the tables its
            // policy reads, other than its own.
            let (mut others, mut others_replacing) = (0, 0);
            for table in tables {
                if let Some((written, replaced)) = later.get(table) {
                    others += written[*round];
                    others_replacing += replaced[*round];
                }
            }
            if tables.contains(stored) {
                others -= 1;
                others_replacing -= usize::from(check.replaces);
            }
            if others == 0 {
                continue;
            }
            if others_replacing == 0 {
                let key = (check.table.clone(), check.check.kind());
                let grows = match monotone.get(&key) {
                    Some(grows) => *grows,
                    None => {
                        let grows = self.write_policy_is_monotone(schema, check)?;
                        monotone.insert(key, grows);
                        grows
                    }
                };
                if grows {
                    continue;
                }
            }
            let cut = Cut {
                unsettled: false,
                seq: self.own_pos(seq.saturating_sub(1)),
                own: self.own_pos(seq),
                extra: Vec::new(),
                overlay: Some(CutOverlay::Except {
                    table: stored.clone(),
                    row: *check.row.0.as_bytes(),
                    key: check.key.clone(),
                }),
            };
            if !self.write_check_holds(schema, check, cut, evidence)?.0 {
                return Err(write_check_denied(check));
            }
        }
        Ok(())
    }

    /// Whether `check`'s policy can only go from denied to allowed as rows
    /// are added: then a pass survives the unit's later inserts and
    /// restores, and only its updates can take it away.
    fn write_policy_is_monotone(
        &self,
        schema: &Schema,
        check: &DeferredCheck,
    ) -> Result<bool, Error> {
        let table = schema.table(&check.table)?;
        let governing = schema.policies_from.as_deref().unwrap_or(schema);
        let Some(table) = governing.by_stored(&table.stored) else {
            return Ok(true);
        };
        let policy = match check.check {
            WriteCheck::Insert => table.policy(Operation::Insert),
            WriteCheck::Restore => table.policy(Operation::Update),
            WriteCheck::WithCheck { .. } => {
                let update = &table.policies.update;
                update.with_check.as_ref().or(update.using.as_ref())
            }
        };
        Ok(policy.is_none_or(|policy| {
            policy_is_monotone(governing, table, policy, &mut BTreeSet::new())
        }))
    }

    /// Whether the row was live in the state a unit written at `seq` was
    /// made over.
    fn was_live_before(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<bool, Error> {
        let before = self.own_pos(seq.saturating_sub(1));
        let cut = Cut {
            unsettled: false,
            seq: before,
            own: before,
            extra: Vec::new(),
            overlay: None,
        };
        Ok(matches!(
            self.existing_at(&table.stored, row, key, Some(&cut))?,
            Some((_, false))
        ))
    }

    /// Whether `row`'s current version on branch `key` is a tombstone.
    fn is_deleted_version(&self, table: &Table, row: RowUuid, key: &[u8]) -> Result<bool, Error> {
        let sql = format!(
            "SELECT (_st & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _b = ?2",
            quote(&table.stored)
        );
        let deleted: Option<i64> = self
            .conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(deleted.is_none_or(|d| d != 0))
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
                author: self.author_code(&author_key(&self.stamp_author()))?,
            }),
        }
    }

    /// Who this database's own writes are attributed to: its author, or for
    /// the internal authority, the system at this node (as Jazz attributes
    /// system writes to the node that made them).
    pub(crate) fn stamp_author(&self) -> AuthorSubject {
        match self.identity.author {
            AuthorSubject::SYSTEM => AuthorSubject::system_at(self.system_node),
            author => author,
        }
    }

    /// The interned code of an author's account and principal. Rows store
    /// this small integer instead of repeating them in every version;
    /// codes are local to this database and resolved back at egress.
    pub(crate) fn author_code(&self, key: &AuthorKey) -> Result<i64, Error> {
        {
            let authors = self.authors.borrow();
            if let Some(code) = authors.codes.get(key) {
                return Ok(*code);
            }
            if let Some((code, rollbacks)) = authors.uncommitted.get(key)
                && *rollbacks == crate::values::rollbacks(&self.conn)
            {
                return Ok(*code);
            }
        }
        let (account, issuer, subject) = key;
        let account = account.as_bytes().to_vec();
        let find = |conn: &rusqlite::Connection| {
            conn.cached_row(
                "SELECT n FROM _jazz_authors WHERE account = ?1 AND issuer = ?2 AND subject = ?3",
                rusqlite::params![account, issuer, subject],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage)
        };
        let code = match find(&self.conn)? {
            Some(code) => code,
            None => {
                self.exec(
                    "INSERT INTO _jazz_authors (account, issuer, subject) VALUES (?1, ?2, ?3)",
                    rusqlite::params![account, issuer, subject],
                )
                .map_err(storage)?;
                self.conn.last_insert_rowid()
            }
        };
        self.remember_author(key, code);
        Ok(code)
    }

    /// Cache an author's code. One interned inside a savepoint (a
    /// transaction overlay, a batch that may fail) is undone by its
    /// rollback, and its code may then go to another author: until it is
    /// committed it holds only while no rollback happened.
    fn remember_author(&self, key: &AuthorKey, code: i64) {
        let mut authors = self.authors.borrow_mut();
        if self.conn.is_autocommit() {
            authors.uncommitted.remove(key);
            authors.codes.insert(key.clone(), code);
            authors.parts.insert(code, key.clone());
        } else {
            let rollbacks = crate::values::rollbacks(&self.conn);
            authors.uncommitted.insert(key.clone(), (code, rollbacks));
        }
    }

    /// The account and principal an author code stands for.
    pub(crate) fn author_parts(&self, code: i64) -> Result<AuthorKey, Error> {
        if let Some(parts) = self.authors.borrow().parts.get(&code) {
            return Ok(parts.clone());
        }
        let (account, issuer, subject): (Vec<u8>, String, String) = self
            .conn
            .cached_row(
                "SELECT account, issuer, subject FROM _jazz_authors WHERE n = ?1",
                [code],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(storage)?;
        let account = uuid::Uuid::from_slice(&account)
            .map_err(|e| Error::new(ErrorCode::Storage, format!("author account: {e}")))?;
        let parts = (account, issuer, subject);
        self.remember_author(&parts, code);
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
            let Some(column) = table.column(name) else {
                return Err(rejected(format!("unknown column `{}.{name}`", table.name)));
            };
            if let Some(variants) = &column.variants {
                for variant in crate::values::enum_names(&cells[name]) {
                    if !variants.iter().any(|v| v == variant) {
                        return Err(rejected(format!(
                            "`{}.{name}`: `{variant}` is not one of its variants",
                            table.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Delete up to `budget` large-value nodes no cell or branch references.
    /// A pass runs only when one could find work: a row changed since the
    /// last pass that emptied the queue, or an upload was dropped.
    fn reclaim_large_values(&mut self, budget: usize) -> Result<usize, Error> {
        if self.reclaimed_at == Some(self.conn.total_changes())
            && self.upload_cleanup.borrow().is_empty()
        {
            return Ok(0);
        }
        self.reclaim_passes += 1;
        self.reclaimed_at = None;
        self.release_dropped_uploads()?;
        let deleted = in_savepoint(&self.conn, "reclaim", || {
            crate::large::reclaim(&self.conn, &self.chunks, budget)
        })?;
        // A pass inside an open transaction may yet roll back, and one
        // that ran out of budget left work queued: both run again.
        if self.conn.is_autocommit() && !crate::large::reclaim_queued(&self.conn)? {
            self.reclaimed_at = Some(self.conn.total_changes());
        }
        Ok(deleted)
    }

    /// A written cell must be of its column's type: writes from any replica
    /// land here, so the store never holds a value its schema does not
    /// describe. A nullable column also takes the bare value; a large
    /// value's reference is checked where it is bound.
    fn check_cell(
        column: &crate::values::Column,
        value: &Value,
        cells: Cells,
    ) -> Result<(), Error> {
        let mut bare = value;
        while let Value::Nullable(Some(inner)) = bare {
            bare = inner;
        }
        if matches!(bare, Value::Large(_)) {
            return Ok(());
        }
        let ty = match (&column.ty, value) {
            (ValueType::Nullable(inner), value) if !matches!(value, Value::Nullable(_)) => inner,
            (ty, _) => ty,
        };
        value.check_type(ty).map_err(|error| match value {
            Value::Nullable(None) => rejected(format!("`{}` is required", column.name)),
            _ => rejected(format!("`{}`: {error}", column.name)),
        })?;
        if let Some(json) = &column.json_type
            && cells == Cells::Authored
        {
            json.check(value, &column.name).map_err(rejected)?;
        }
        Ok(())
    }

    /// A cell for stored column `column` of stored table `table` written
    /// as plain SQL keeps a value (a lens's cell, an image from another
    /// store): an enum variant's name as its tag here.
    pub(crate) fn enum_cell(&self, table: &str, column: &str, cell: Sql) -> Result<Sql, Error> {
        let Sql::Text(text) = &cell else {
            return Ok(cell);
        };
        let Some(ty) = self
            .names
            .stored_type(table, column)
            .filter(|ty| crate::values::is_enum_type(ty))
        else {
            return Ok(cell);
        };
        // The cell is the value as plain SQL stores it: a name, or a JSON
        // array of them.
        let value = if crate::values::is_array_type(ty) {
            let json: serde_json::Value = serde_json::from_slice(text)
                .map_err(|e| Error::new(ErrorCode::Storage, format!("array cell: {e}")))?;
            let serde_json::Value::Array(items) = json else {
                return Ok(cell);
            };
            Value::Array(
                items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(name) => Value::String(name),
                        _ => Value::Nullable(None),
                    })
                    .collect(),
            )
        } else {
            Value::String(String::from_utf8_lossy(text).into_owned())
        };
        self.enum_value_cell(table, column, &value)
    }

    /// Enum `value` as stored enum column `column` of stored table
    /// `table`'s cell: its variants' tags there, interned if new.
    fn enum_value_cell(&self, table: &str, column: &str, value: &Value) -> Result<Sql, Error> {
        self.names
            .intern(&self.conn, table, column, &crate::values::enum_names(value))?;
        let tags = self.names.tags().column(table, column);
        let cell = crate::values::enum_cell(&tags.borrow(), value)?;
        Ok(cell.unwrap_or(Sql::Null))
    }

    fn bind_value(value: &Value) -> Result<Sql, Error> {
        to_sql(value)?.ok_or_else(|| unsupported("storing NaN"))
    }

    /// A value as the cell `column` stores. A string, JSON or bytes value
    /// above the inline threshold is chunked: its nodes are stored in this
    /// transaction and the cell holds their descriptor.
    fn bind_cell(
        &self,
        table: &Table,
        column: &crate::values::Column,
        value: &Value,
        cells: Cells,
    ) -> Result<Sql, Error> {
        Self::check_cell(column, value, cells)?;
        if column.is_enum() {
            // Every variant a version declares has a tag; a write's are
            // checked against them, but a lens or merge may still carry a
            // removed one.
            self.names.intern(
                &self.conn,
                &table.stored,
                &column.stored,
                &crate::values::enum_names(value),
            )?;
            return column
                .cell_sql(value)?
                .ok_or_else(|| unsupported("storing NaN"));
        }
        let mut inner = value;
        while let Value::Nullable(Some(value)) = inner {
            inner = value;
        }
        let logical = match inner {
            Value::String(text) => text.as_bytes(),
            Value::Bytes(bytes) => bytes.as_slice(),
            Value::Large(large) => return self.bind_large(column, large, cells),
            _ => return Self::bind_value(value),
        };
        if logical.len() <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES
            || !crate::large::large_capable(column)
        {
            return Self::bind_value(value);
        }
        let prepared = jazz_types::large_values::prepare(crate::large::kind(column), logical)
            .map_err(|e| rejected(format!("`{}`: {e}", column.name)))?;
        crate::large::store(&self.conn, &self.chunks, &prepared, self.client.is_some())?;
        Ok(Sql::Integer(crate::large::intern(
            &self.conn,
            &self.chunks,
            &prepared.value_ref,
        )?))
    }

    /// A chunked value written by reference: an edit's consolidated tree
    /// (stored now, inside the write's transaction), or a tree whose nodes
    /// this store already holds. An authored JSON value is checked whole,
    /// wherever its tree is complete here: always at the authority, so no
    /// writer gets past a schema by sending a value large enough to chunk.
    fn bind_large(
        &self,
        column: &crate::values::Column,
        large: &jazz_types::large_values::LargeValueRef,
        cells: Cells,
    ) -> Result<Sql, Error> {
        if !crate::large::large_capable(column) || large.kind != crate::large::kind(column) {
            return Err(rejected(format!(
                "`{}` does not hold {:?} large values",
                column.name, large.kind
            )));
        }
        let mut complete = true;
        if let Some(prepared) = self
            .pending_nodes
            .iter()
            .find(|prepared| prepared.value_ref.root == large.root)
        {
            crate::large::store(&self.conn, &self.chunks, prepared, self.client.is_some())?;
        } else if self.derived.contains(large) {
            // Derived here from a stored tree by an append or splice.
            complete = self.client.is_none();
        } else if let Some(possession) = &self.possession
            && !crate::large::possessed(&self.conn, &self.chunks, large, possession)?
        {
            // Answered as for absent nodes: no oracle for what this store
            // holds for others (SPEC 19.9).
            return Err(rejected(format!(
                "`{}`: the large-value descriptor names chunks this store does not hold",
                column.name
            )));
        } else if self.client.is_some() {
            // A replica's tree may be only partly fetched: the authority
            // checks the value when it admits the write.
            complete = false;
        } else {
            // An authority admits a descriptor only over a complete, honest
            // tree. A replica's edits build on trees its upstream holds,
            // which it may have only partly fetched.
            if !crate::large::root_is_local(&self.conn, large)? {
                return Err(rejected(format!(
                    "`{}`: the large-value descriptor names chunks this store does not hold",
                    column.name
                )));
            }
            crate::large::admit(&self.conn, &self.chunks, large)?;
        }
        if let Some(json) = &column.json_type
            && cells == Cells::Authored
            && complete
        {
            let bytes = self.chunks.value_bytes(large)?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                rejected(format!(
                    "invalid JSON for column `{}`: not UTF-8",
                    column.name
                ))
            })?;
            json.check_text(text, &column.name).map_err(rejected)?;
        }
        if let Some(possession) = &self.possession {
            possession.settle.borrow_mut().push(large.root.object_hash);
        }
        self.large_cell(large)
    }

    /// Settle the receipts of the trees the applied commit's cells proved
    /// (SPEC 19.4): only after all its cells, which may share nodes.
    pub(crate) fn settle_possession(&self) -> Result<(), Error> {
        let Some(possession) = &self.possession else {
            return Ok(());
        };
        for root in possession.settle.take() {
            crate::large::settle_receipts(&self.conn, &root, &possession.author)?;
        }
        Ok(())
    }

    /// The cell of a chunked value this store holds the chunks for. A value
    /// an edit shrank to the inline threshold is stored inline, so each
    /// logical value has exactly one inline cell.
    pub(crate) fn large_cell(
        &self,
        large: &jazz_types::large_values::LargeValueRef,
    ) -> Result<Sql, Error> {
        if large.byte_length <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES as u64 {
            let bytes = self.chunks.value_bytes(large)?;
            return Ok(crate::large::inline_cell(large.kind, bytes));
        }
        Ok(Sql::Integer(crate::large::intern(
            &self.conn,
            &self.chunks,
            large,
        )?))
    }

    /// Every stored column of `table`, including ones only other schema
    /// views know: history and copy-on-write carry whole stored rows.
    fn columns_list(&self, table: &Table) -> String {
        let physical = self.physical.tables.get(&table.stored).unwrap_or(table);
        physical
            .stored()
            .map(|c| format!(", {}", quote(&c.stored)))
            .collect()
    }

    /// The stored value of `column` in `row` on branch `key`, if any.
    fn cell_at(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        column: &str,
    ) -> Result<Option<Value>, Error> {
        let Some(column) = table.columns.iter().find(|c| c.name == column) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1 AND _b = ?2",
            quote(&column.stored),
            quote(&table.stored)
        );
        let cell: Option<Sql> = self
            .conn
            .cached_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        match cell {
            Some(cell) => column_from_sql(column, cell.as_ref(), &self.chunks),
            None => Ok(None),
        }
    }

    /// Chunk a write's string, JSON and bytes cells above the inline
    /// threshold once, up front: each becomes a reference to its prepared
    /// tree, which the returned edits carry until the write applies. The
    /// write then travels (to the authority, in a pending upload, through
    /// every overlay replay) as that reference instead of the whole value,
    /// and its nodes are pushed as chunks ahead of it.
    pub(crate) fn chunk_large_cells(&self, op: &mut StagedOp) -> Result<LargeEdits, Error> {
        let mut edits = LargeEdits::default();
        let (table_name, cells) = match op {
            StagedOp::Insert(t, c, _)
            | StagedOp::Update(t, _, c, _)
            | StagedOp::Upsert(t, _, c, _, _) => (t.clone(), c),
            StagedOp::Restore(t, _, Some(c), _) => (t.clone(), c),
            _ => return Ok(edits),
        };
        let schema = self.schema.clone();
        let table = schema.table(&table_name)?;
        for column in &table.columns {
            if !crate::large::large_capable(column) {
                continue;
            }
            let Some(mut value) = cells.get_mut(&column.name) else {
                continue;
            };
            while let Value::Nullable(Some(inner)) = value {
                value = inner;
            }
            let logical = match &*value {
                Value::String(text) => text.as_bytes(),
                Value::Bytes(bytes) => bytes.as_slice(),
                _ => continue,
            };
            if logical.len() <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES {
                continue;
            }
            let prepared = jazz_types::large_values::prepare(crate::large::kind(column), logical)
                .map_err(|e| rejected(format!("`{}`: {e}", column.name)))?;
            *value = Value::Large(Box::new(prepared.value_ref.clone()));
            edits.pending_nodes.push(prepared);
        }
        Ok(edits)
    }

    /// Turn a write's counter cells from the values it sets into the
    /// changes it makes to what this node holds (see [`crate::merge`]).
    pub(crate) fn counter_deltas(&self, op: &mut StagedOp) -> Result<(), Error> {
        let (table_name, row, cells, target) = match op {
            StagedOp::Update(t, r, c, o) => (t.clone(), *r, c, &o.target),
            StagedOp::Upsert(t, r, c, _, o) => (t.clone(), *r, c, &o.target),
            _ => return Ok(()),
        };
        let schema = self.schema.clone();
        let table = schema.table(&table_name)?;
        let counters: Vec<String> = cells
            .keys()
            .filter(|name| {
                table
                    .columns
                    .iter()
                    .any(|c| &c.name == *name && c.merge == MergeStrategy::Counter)
            })
            .cloned()
            .collect();
        if counters.is_empty() {
            return Ok(());
        }
        let target = Target::of(target, self)?;
        let held_at = match self.locate(table, row, &target)? {
            Located::Row {
                key,
                deleted: false,
                ..
            } => Some(key),
            Located::Inherited { from, .. } => Some(from),
            _ => None,
        };
        for name in counters {
            let seen = match &held_at {
                Some(key) => self.cell_at(table, row, key, &name)?,
                None => None,
            };
            let delta = crate::merge::counter_delta(&name, &cells[&name], seen.as_ref())?;
            cells.insert(name, delta);
        }
        Ok(())
    }

    /// A write's merged cells against what branch `held` (if any) holds:
    /// counter changes added, g-sets unioned, and on the authority text
    /// merged over the write's base. `at` is the branch key and seq the
    /// write lands at.
    fn merged_cells(
        &self,
        table: &Table,
        row: RowUuid,
        held: Option<&[u8]>,
        at: Option<(&[u8], u64)>,
        mut cells: BTreeMap<String, Value>,
    ) -> Result<BTreeMap<String, Value>, Error> {
        for column in &table.columns {
            let Some(value) = cells.get(&column.name) else {
                continue;
            };
            let merged = match column.merge {
                MergeStrategy::Lww => continue,
                MergeStrategy::Text | MergeStrategy::Markdown | MergeStrategy::Html => {
                    match self.text_merged(table, row, held, at, column, value)? {
                        Some(merged) if column.nullable() => {
                            Value::Nullable(Some(Box::new(Value::String(merged))))
                        }
                        Some(merged) => Value::String(merged),
                        None => continue,
                    }
                }
                MergeStrategy::Counter | MergeStrategy::GSet => {
                    let current = match held {
                        Some(key) => self.cell_at(table, row, key, &column.name)?,
                        None => None,
                    };
                    if column.merge == MergeStrategy::Counter {
                        crate::merge::counter_apply(
                            &column.name,
                            &column.ty,
                            current.as_ref(),
                            value,
                        )?
                    } else {
                        crate::merge::gset_union(&column.name, current.as_ref(), value)?
                    }
                }
            };
            cells.insert(column.name.clone(), merged);
        }
        Ok(cells)
    }

    /// A text write merged on the authority (SPEC 4 §4.3): when a version
    /// its writer never saw authored the column, the merge of the value it
    /// was built on, the value held and its own; `None` keeps its value as
    /// sent. Every text write of an upload notes its value as sent (none
    /// when unmerged): a later write built on it, in the same upload or a
    /// later one, was built on that value.
    fn text_merged(
        &self,
        table: &Table,
        row: RowUuid,
        held: Option<&[u8]>,
        at: Option<(&[u8], u64)>,
        column: &crate::values::Column,
        value: &Value,
    ) -> Result<Option<String>, Error> {
        let Some((key, seq)) = at else {
            return Ok(None);
        };
        let version = (table.stored.clone(), row, key.to_vec());
        let Some(&built) = self.merge_bases.get(&version) else {
            return Ok(None);
        };
        let merged = self.text_merge(table, row, held, (key, seq), built, column, value)?;
        let (merged, sent) = match merged {
            Some((merged, cell)) => (Some(merged), Some((value.clone(), cell))),
            None => (None, None),
        };
        self.as_sent
            .borrow_mut()
            .entry(version)
            .or_default()
            .insert(column.stored.clone(), sent);
        Ok(merged)
    }

    /// [`Self::text_merged`]'s merge, with the write's value bound as its
    /// cell: `None` when there is nothing to merge or the merge leaves the
    /// value as sent. The fast path reads no value: whether to merge is
    /// decided first, from the row's `_base` and `_cols` alone. The write's
    /// value is bound before it is read, so a descriptor naming chunks its
    /// writer does not possess is refused (SPEC 19.4), merged or not.
    #[allow(clippy::too_many_arguments)]
    fn text_merge(
        &self,
        table: &Table,
        row: RowUuid,
        held: Option<&[u8]>,
        (key, seq): (&[u8], u64),
        built: sync::Built,
        column: &crate::values::Column,
        value: &Value,
    ) -> Result<Option<(String, Sql)>, Error> {
        let Some(held) = held else {
            return Ok(None);
        };
        // A clear (a NULL write) is never merged, and an edit that a clear
        // overtook stands as sent: either way the later write wins.
        if matches!(value, Value::Nullable(None)) {
            return Ok(None);
        }
        let current = |db: &Self| -> Result<Option<String>, Error> {
            Ok(match db.cell_at(table, row, held, &column.name)? {
                Some(current) => db.optional_text(&column.name, &current)?,
                None => None,
            })
        };
        let earlier = self
            .as_sent
            .borrow()
            .get(&(table.stored.clone(), row, key.to_vec()))
            .and_then(|columns| columns.get(&column.stored).cloned());
        let (base, current) = match earlier {
            // Built on an earlier write of the same upload, as sent: what
            // is held when that write was left as sent.
            Some(sent) => {
                let Some(current) = current(self)? else {
                    return Ok(None);
                };
                match sent {
                    Some((sent, _)) => (self.logical_text(&column.name, &sent)?, current),
                    None => (current.clone(), current),
                }
            }
            None => {
                let Some(base) =
                    self.merge_base((&table.stored, row, key), built, seq, &column.stored)?
                else {
                    return Ok(None);
                };
                // A NULL base merges as "": additions to an empty optional
                // column are all kept, as they are over an empty string.
                let base = match column_from_sql(column, base.as_ref(), &self.chunks)? {
                    Some(base) => self.optional_text(&column.name, &base)?.unwrap_or_default(),
                    None if column.nullable() => String::new(),
                    None => return Ok(None),
                };
                let Some(current) = current(self)? else {
                    return Ok(None);
                };
                (base, current)
            }
        };
        let cell = self.bind_cell(table, column, value, Cells::Authored)?;
        let incoming = self.logical_text(&column.name, value)?;
        let format = column.merge.text_format().expect("a text strategy");
        let merged = crate::text_merge::merge3(format, &base, &current, &incoming);
        Ok((merged != incoming).then_some((merged, cell)))
    }

    /// Keep, for each row version the upload at `seq` wrote whose text
    /// merge changed a value, that version as its writer sent it (SPEC 4
    /// §4.3): in `__h` at `_seq` = `-seq` with [`STATE_AS_SENT`] set, its
    /// merged columns holding their values as sent and every other cell
    /// the version's. A later write chained on it merges over these values
    /// ([`Inner::merge_base`]); no read takes it for a version.
    ///
    /// First drop the as-sent versions its sender `node` can no longer
    /// build on. Only a commit's own sender builds on it unsettled, and its
    /// commits arrive in order. Once one of them writes a row over an
    /// accepted version (or over none), every later one it sends is built
    /// on that write or a newer one, and a chain walk stops at the first
    /// accepted version: the sender's older as-sent versions of the row
    /// are dead. So a row keeps them only for its writers' current
    /// unsettled chains.
    pub(crate) fn record_as_sent(
        &self,
        seq: u64,
        node: [u8; 16],
        built: &HashMap<(String, RowUuid, Vec<u8>), sync::Built>,
        rows: &[(String, RowUuid, Vec<u8>)],
    ) -> Result<(), Error> {
        for row_key @ (stored, row, key) in rows {
            if matches!(built.get(row_key), Some(sync::Built::On(_))) {
                continue;
            }
            self.exec(
                &format!(
                    "DELETE FROM {} WHERE id = ?1 AND _b = ?2 AND _seq < 0 \
                     AND (_st & {STATE_AS_SENT}) <> 0 AND EXISTS (SELECT 1 FROM _jazz_transactions AS s \
                     JOIN _jazz_nodes AS n ON n.n = s.n WHERE s.sn = _n AND s.seq = -_seq AND n.node = ?3)",
                    quote(&format!("{stored}__h"))
                ),
                rusqlite::params![row.0.as_bytes().to_vec(), key, node.to_vec()],
            )
            .map_err(storage)?;
        }
        let as_sent = std::mem::take(&mut *self.as_sent.borrow_mut());
        for ((stored, row, key), columns) in as_sent {
            let Some(table) = self.physical.tables.get(&stored) else {
                continue;
            };
            let mut values = vec![
                Sql::Blob(row.0.as_bytes().to_vec()),
                Sql::Blob(key),
                Sql::Integer(seq as i64),
            ];
            let mut sent = BTreeMap::new();
            for (name, value) in columns {
                let Some((_, cell)) = value else {
                    continue;
                };
                values.push(cell);
                sent.insert(name, values.len());
            }
            if sent.is_empty() {
                continue;
            }
            let columns = self.columns_list(table);
            let cells: String = table
                .stored()
                .map(|c| match sent.get(&c.stored) {
                    Some(at) => format!(", ?{at}"),
                    None => format!(", {}", quote(&c.stored)),
                })
                .collect();
            let sql = format!(
                "INSERT INTO {} (id, _b, _n, _seq, _st, _base, _cols{columns}) \
                 SELECT id, _b, _n, -_seq, _st | {STATE_AS_SENT}, NULL, NULL{cells} FROM {} \
                 WHERE id = ?1 AND _b = ?2 AND _n = {} AND _seq = ?3",
                quote(&format!("{stored}__h")),
                quote(&stored),
                self.me
            );
            self.exec(&sql, params_from_iter(values.iter()))
                .map_err(storage)?;
        }
        Ok(())
    }

    /// How many as-sent versions ([`STATE_AS_SENT`]) the store keeps.
    #[cfg(feature = "testing")]
    pub(crate) fn as_sent_versions(&self) -> Result<u64, Error> {
        let mut count = 0;
        for stored in self.physical.tables.keys() {
            let n: i64 = self
                .conn
                .cached_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE (_st & {STATE_AS_SENT}) <> 0",
                        quote(&format!("{stored}__h"))
                    ),
                    [],
                    |r| r.get(0),
                )
                .map_err(storage)?;
            count += n as u64;
        }
        Ok(count)
    }

    /// [`Self::logical_text`] of a cell that may be NULL (`None`).
    fn optional_text(&self, column: &str, value: &Value) -> Result<Option<String>, Error> {
        match value {
            Value::Nullable(None) => Ok(None),
            value => self.logical_text(column, value).map(Some),
        }
    }

    /// The whole string a text column's value holds.
    fn logical_text(&self, column: &str, value: &Value) -> Result<String, Error> {
        match value {
            Value::String(text) => Ok(text.clone()),
            Value::Nullable(Some(value)) => self.logical_text(column, value),
            Value::Large(large) => String::from_utf8(self.chunks.value_bytes(large)?)
                .map_err(|_| rejected(format!("`{column}` holds a value that is not UTF-8"))),
            _ => Err(rejected(format!("`{column}` is not a string"))),
        }
    }

    /// Copy the row's current image into its history before it is
    /// superseded (history holds superseded images only).
    fn snapshot(&self, table: &Table, row: RowUuid, key: &[u8], seq: u64) -> Result<(), Error> {
        let columns = self.columns_list(table);
        let sql = format!(
            "INSERT INTO {} (id, _b, _n, _seq, _st, _base, _cols{columns}) \
             SELECT id, _b, _n, _seq, _st, _base, _cols{columns} FROM {} \
             WHERE id = ?1 AND _b = ?2 AND NOT (_n = {} AND _seq = ?3)",
            quote(&format!("{}__h", table.stored)),
            quote(&table.stored),
            self.me
        );
        // An image written earlier in the same commit is not superseded
        // history: history holds committed images only.
        self.exec(
            &sql,
            rusqlite::params![row.0.as_bytes().to_vec(), key, seq as i64],
        )
        .map_err(storage)?;
        Ok(())
    }

    /// Copy-on-write: materialize an inherited base row into the head.
    #[allow(clippy::too_many_arguments)]
    fn copy_on_write(
        &self,
        table: &Table,
        row: RowUuid,
        from: &[u8],
        to: &[u8],
        head_cells: &BTreeMap<String, Value>,
        at: Option<&Cut>,
        seq: u64,
    ) -> Result<(), Error> {
        let columns = self.columns_list(table);
        let id = row.0.as_bytes().to_vec();
        // A frozen base is copied as it was at the cut.
        let source = match at {
            None => Some(quote(&table.stored)),
            Some(cut) => match self.existing(&table.stored, row, from)? {
                Some((current, _)) if cut.sees(current) => Some(quote(&table.stored)),
                _ => self
                    .version_at(&table.stored, row, from, cut)?
                    .map(|version| {
                        format!(
                            "(SELECT * FROM {} WHERE _n = {} AND _seq = {})",
                            quote(&format!("{}__h", table.stored)),
                            version.n,
                            version.seq
                        )
                    }),
            },
        };
        let Some(source) = source else {
            return Err(rejected(format!(
                "row {row:?} did not exist in the frozen base of `{}`",
                table.name
            )));
        };
        // The copy keeps the base's deletion and authored no content.
        let sql = format!(
            "INSERT INTO {name} (id, _b, _n, _seq, _st{columns}) \
             SELECT id, ?1, {me}, ?2, _st | {STATE_NO_CONTENT}{columns} \
             FROM {source} WHERE id = ?3 AND _b = ?4",
            name = quote(&table.stored),
            me = self.me
        );
        self.exec(&sql, rusqlite::params![to, seq as i64, id, from])
            .map_err(storage)?;
        self.set_cells(table, row, to, head_cells, Cells::Carried, seq, None, false)
    }

    /// Write cells (and the deletion flag) into a row's current version
    /// at `seq`. `content` says whether the write authored content; a
    /// version stays a content version once any write of its commit did.
    #[allow(clippy::too_many_arguments)]
    fn set_cells(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cells: &BTreeMap<String, Value>,
        source: Cells,
        seq: u64,
        deleted: Option<bool>,
        content: bool,
    ) -> Result<(), Error> {
        // Right-hand sides read the row before this update: `_seq` is still
        // the previous version's.
        // What a new version was built on is the authority's to write
        // (SPEC 4 §4.6).
        let mut values = vec![Sql::Integer(seq as i64)];
        let deletion = match deleted {
            Some(deleted) => {
                values.push(Sql::Integer(if deleted { STATE_DELETED } else { 0 }));
                format!("?{}", values.len())
            }
            None => format!("(_st & {STATE_DELETED})"),
        };
        // The no-content bit stays set only while no write of the commit
        // authored content.
        let no_content = if content { 0 } else { STATE_NO_CONTENT };
        let me = self.me;
        let mut sets = vec![
            format!("_n = {me}, _seq = ?1"),
            "_base = NULL, _cols = NULL".to_owned(),
            format!(
                "_st = (CASE WHEN _n = {me} AND _seq = ?1 THEN _st & {no_content} ELSE {no_content} END) \
                 | {deletion}"
            ),
        ];
        for (name, value) in cells {
            let cell = match table.column(name) {
                Some(column) => self.bind_cell(table, column, value, source)?,
                None => Self::bind_value(value)?,
            };
            values.push(cell);
            sets.push(format!(
                "{} = ?{}",
                quote(table.stored_name(name)),
                values.len()
            ));
        }
        for (name, value) in self.stamp()?.updated() {
            values.push(value);
            sets.push(format!("{} = ?{}", quote(name), values.len()));
        }
        values.push(Sql::Blob(row.0.as_bytes().to_vec()));
        values.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "UPDATE {} SET {} WHERE id = ?{} AND _b = ?{}",
            quote(&table.stored),
            sets.join(", "),
            values.len() - 1,
            values.len()
        );
        self.exec(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.stored.clone(), row, key.to_vec()));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert(
        &mut self,
        table_name: &str,
        mut cells: BTreeMap<String, Value>,
        source: Cells,
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
        // A replicated insert from a version without a branch column keys
        // its row by that column's immutable default (INV-BVIEW-19).
        if schema.stored_names {
            for column in &table.branch_by {
                if !cells.contains_key(column)
                    && let Some(default) = table.column(column).and_then(|c| c.default.clone())
                {
                    cells.insert(column.clone(), default);
                }
            }
        }
        let key = table.row_branch_key(&cells);
        let row = row_id.unwrap_or_else(|| self.ids.next_row_id());
        let cells = self.merged_cells(table, row, None, None, cells)?;
        self.refuse_withheld(table, row, &key)?;
        match self.existing(&table.stored, row, &key)? {
            Some((_, true)) => return Err(row_already_deleted(row)),
            Some((_, false)) => return Err(row_already_exists(row)),
            None => {}
        }
        // Most stores have no lens cells; the owner's version is cached
        // (hashing a schema per insert would dominate a large batch's replay).
        let (copies, dropped) = if !self.names.has_lens_cells() {
            Default::default()
        } else if schema.stored_names {
            let copies = self
                .names
                .stored_copies(&table.stored, |column| cells.contains_key(column));
            (copies, Vec::new())
        } else {
            let version = if Rc::ptr_eq(&schema, &self.owner) {
                self.owner_version
            } else {
                schema.jazz.version_id()
            };
            self.names.lens_cells(&version, table)
        };
        let mut names = vec![
            "id".to_owned(),
            "_b".to_owned(),
            "_n".to_owned(),
            "_seq".to_owned(),
        ];
        let mut values = vec![
            Sql::Blob(row.0.as_bytes().to_vec()),
            Sql::Blob(key.clone()),
            Sql::Integer(self.me),
            Sql::Integer(seq as i64),
        ];
        for column in &table.columns {
            let given = match (cells.get(&column.name), &column.default) {
                (Some(value), _) => Some((value, source)),
                // A replicated insert names every column its writer's
                // version declares, and the backwards defaults of columns
                // its lineage dropped: one it leaves out, a later version
                // added, so the stored column's own default or a copy fills
                // it. Only a required column nothing fills is refused.
                (None, default)
                    if schema.stored_names
                        && (default.is_some()
                            || column.nullable()
                            || copies.iter().any(|(_, copy)| *copy == column.stored)) =>
                {
                    continue;
                }
                (None, Some(default)) => Some((default, Cells::Authored)),
                (None, None) if column.nullable() => None,
                (None, None) => {
                    return Err(missing_required(table_name, &column.name));
                }
            };
            let value = match given {
                Some((value, cells)) => self.bind_cell(table, column, value, cells)?,
                None => Sql::Null,
            };
            for (_, copy) in copies.iter().filter(|(c, _)| *c == column.name) {
                names.push(quote(copy));
                // An enum copy's tags are its own.
                values.push(match given {
                    Some((given, _)) if column.is_enum() => {
                        self.enum_value_cell(&table.stored, copy, given)?
                    }
                    _ => value.clone(),
                });
            }
            names.push(quote(&column.stored));
            values.push(value);
        }
        for (stored, default) in dropped {
            let default = self.enum_cell(&table.stored, &stored, default)?;
            names.push(quote(&stored));
            values.push(default);
        }
        // An enum column this version leaves out whose `DEFAULT` tag is
        // stale takes its default by name.
        for (stored, default) in self.names.enum_defaults(&table.stored) {
            if names.contains(&quote(&stored)) {
                continue;
            }
            let default = self.enum_value_cell(&table.stored, &stored, &Value::String(default))?;
            names.push(quote(&stored));
            values.push(default);
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
            quote(&table.stored),
            names.join(", "),
            placeholders.join(", ")
        );
        self.exec(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.stored.clone(), row, key.clone()));
        if let Some(author) = self.checked(who) {
            self.write_check(table, row, &key, author, WriteCheck::Insert, seq)?;
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
        })
    }

    /// The session a write's operation policies (USING, WITH CHECK,
    /// INSERT, DELETE) are checked as here: a trusted session's, at the
    /// authority. A replica leaves them to the authority on upload: it may
    /// lack rows those policies read (hidden from it).
    /// Undo a transaction handle's overlay, if one is applied.
    fn settle_overlay(&mut self) {
        let Some(overlay) = self.overlay.take() else {
            return;
        };
        self.changed.borrow_mut().truncate(overlay.mark);
        end_savepoint(&self.conn, "overlay", false).expect("rolling back a transaction overlay");
    }

    fn checked(&self, who: Who) -> Option<AuthorSubject> {
        match who {
            Who::Session(author) if self.client.is_none() => Some(author),
            _ => None,
        }
    }

    /// Read-for-write (SPEC 7, INV-RLS-20): a session may update or upsert
    /// only a row it may read, and omitted cells must not be merged blind.
    /// A replica acting for a session (a backend's request) refuses an
    /// update at once when it holds the row hidden from that session: the
    /// update merges into the session's view of the row. So it does an
    /// upsert under a read policy decided by the row alone. An upsert under
    /// a policy that reads other rows is the authority's to decide, as on
    /// upload: the replica may lack the rows that grant access, and the
    /// write's waits report the refusal.
    fn authorize_patch(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        who: &Who,
        verb: &str,
    ) -> Result<(), Error> {
        let conclusive = self.client.is_none()
            || verb != "UPSERT"
            || !table
                .policy(Operation::Select)
                .is_some_and(policy_reads_other_rows);
        if let Who::Session(author) = *who
            && conclusive
            && !self.check(table, row, key, author, Operation::Select)?
        {
            let detail = format!(
                "read policy denied {verb} on table {}: the operation requires read \
                 permission on the target row",
                table.name
            );
            // A replica refuses it locally, as main does; an authority
            // reports it as its policy rejection.
            return Err(if self.client.is_some() {
                rejected(detail)
            } else {
                denied(detail)
            });
        }
        if let Some(author) = self.checked(*who)
            && !self.check_clause(
                table,
                row,
                key,
                author,
                table.policies.update.using.as_ref(),
            )?
        {
            return Err(denied(format_args!(
                "{verb} policy (USING) denied the target row"
            )));
        }
        Ok(())
    }

    /// An update, or with `created` an upsert, which gives the columns
    /// `cells` leaves out `created`'s values if it creates the row.
    #[allow(clippy::too_many_arguments)]
    fn patch(
        &mut self,
        table_name: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        created: Option<BTreeMap<String, Value>>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let upsert = created.is_some();
        let created = created.unwrap_or_default();
        let verb = if upsert { "UPSERT" } else { "UPDATE" };
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        Self::validate_cells(table, &cells)?;
        let located = self.locate(table, row, target)?;
        // A row's branch columns are its branch key: a write cannot move it
        // to another branch.
        if let Located::Row { key, .. } | Located::Inherited { to: key, .. } = &located
            && table
                .branch_by
                .iter()
                .any(|column| cells.contains_key(column))
            && table.branch_key_after(key, &cells) != *key
        {
            return Err(branch_move());
        }
        // An upsert over a row this commit deleted writes it anew: it is
        // checked as the insert it is, not as an update of the deleted row.
        let mut reinserted = false;
        let key = match located {
            Located::Absent { cells: branch, .. } if upsert => {
                let mut cells = cells;
                cells.extend(branch);
                for (column, value) in created {
                    cells.entry(column).or_insert(value);
                }
                return self.insert(
                    table_name,
                    cells,
                    Cells::Authored,
                    Some(row),
                    &Target::Root,
                    who,
                    seq,
                );
            }
            // An update stages against the row's current cells, so a
            // replica must hold it. Saying so discloses nothing: the answer
            // is the same whether or not the row exists elsewhere.
            Located::Absent { .. } if self.client.is_some() => {
                return Err(update_target_not_loaded(verb, table_name, row));
            }
            Located::Absent { .. } if !upsert => {
                return Err(rejected(format!(
                    "{verb} target {row:?} does not exist in `{table_name}`"
                )));
            }
            // A deleted row's id stays reserved: only a restore revives it.
            // A delete staged earlier in this transaction is no tombstone
            // yet, so an upsert replaces it. A merged edit (made over an
            // image older than the delete) applies to the row's content and
            // leaves it deleted (SPEC 4, INV-HIST-8).
            Located::Row {
                deleted: true,
                seq: current,
                ..
            } if !self.merging && (!upsert || current != self.own_pos(seq)) => {
                return Err(row_already_deleted(row));
            }
            Located::Row {
                key,
                seq: current,
                deleted,
            } => {
                let mut cells = cells;
                let staged_tombstone = deleted && upsert && current == self.own_pos(seq);
                if staged_tombstone {
                    // Every column is written, as by an insert: nothing
                    // of the deleted version carries over.
                    reinserted = true;
                    for (column, value) in created {
                        cells.entry(column).or_insert(value);
                    }
                    for column in &table.columns {
                        // Branch columns are the row's key and stay.
                        if cells.contains_key(&column.name)
                            || table.branch_by.contains(&column.name)
                        {
                            continue;
                        }
                        let value = match &column.default {
                            Some(default) => default.clone(),
                            None if column.nullable() => Value::Nullable(None),
                            None => return Err(missing_required(table_name, &column.name)),
                        };
                        cells.insert(column.name.clone(), value);
                    }
                } else {
                    self.authorize_patch(table, row, &key, &who, verb)?;
                }
                if cells.is_empty() && !staged_tombstone {
                    return Ok(WriteHandle { row, seq: current });
                }
                let held = (!staged_tombstone).then_some(&key[..]);
                let cells = self.merged_cells(table, row, held, Some((&key, seq)), cells)?;
                self.snapshot(table, row, &key, seq)?;
                // An upsert over a tombstone replaces it.
                self.set_cells(
                    table,
                    row,
                    &key,
                    &cells,
                    Cells::Authored,
                    seq,
                    staged_tombstone.then_some(false),
                    true,
                )?;
                key
            }
            Located::Inherited {
                from,
                to,
                head_cells,
                at,
            } => {
                self.authorize_patch(table, row, &from, &who, verb)?;
                let cells = self.merged_cells(table, row, Some(&from), Some((&to, seq)), cells)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, at.as_ref(), seq)?;
                self.set_cells(table, row, &to, &cells, Cells::Authored, seq, None, true)?;
                to
            }
            Located::Absent { .. } => unreachable!("absent upserts insert"),
        };
        // The new row is checked on the evidence SPEC 7 gives a WITH CHECK
        // ([`Inner::write_check`]): a same-table EXISTS compares the new row
        // with the old one, not with itself.
        if let Some(author) = self.checked(who) {
            self.write_check(
                table,
                row,
                &key,
                author,
                if reinserted {
                    WriteCheck::Insert
                } else {
                    WriteCheck::WithCheck { upsert }
                },
                seq,
            )?;
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
        })
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
        self.patch(table, row, cells, None, target, who, seq)
    }

    /// Upsert `cells` into `row`; if it creates the row, `created` fills
    /// the columns `cells` leaves out (a replicated upsert's writer's
    /// defaults, see `stored_op`).
    #[allow(clippy::too_many_arguments)]
    fn upsert(
        &mut self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        created: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        self.patch(table, row, cells, Some(created), target, who, seq)
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
        // A delete by row id reads no user data (SPEC 7): its DELETE policy
        // alone decides, so a write-only principal may delete.
        let check = |inner: &Self, key: &[u8]| -> Result<(), Error> {
            if let Some(author) = inner.checked(who)
                && !inner.check(table, row, key, author, Operation::Delete)?
            {
                return Err(denied("DELETE policy denied the target row"));
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
                self.set_cells(
                    table,
                    row,
                    &key,
                    &BTreeMap::new(),
                    Cells::Authored,
                    seq,
                    Some(true),
                    false,
                )?;
            }
            // Deleting an inherited row is a head register: a head tombstone
            // that masks the base row.
            Located::Inherited {
                from,
                to,
                head_cells,
                at,
            } => {
                check(self, &from)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, at.as_ref(), seq)?;
                self.set_cells(
                    table,
                    row,
                    &to,
                    &BTreeMap::new(),
                    Cells::Authored,
                    seq,
                    Some(true),
                    false,
                )?;
            }
            // As for a patch: a replica leaves an unknown row to the authority.
            Located::Absent { .. } if self.client.is_some() => {}
            // A merged delete of a deleted row changes nothing: concurrent
            // deletes of one row all apply.
            Located::Row { deleted: true, .. } if self.merging => {}
            Located::Row { deleted: true, .. } => return Err(row_already_deleted(row)),
            _ => {
                return Err(rejected(format!(
                    "DELETE target {row:?} does not exist in `{table_name}`"
                )));
            }
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn restore(
        &mut self,
        table_name: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        source: Cells,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let schema = self.schema.clone();
        let table = schema.table(table_name)?;
        let key = match self.locate(table, row, target)? {
            Located::Row {
                key, deleted: true, ..
            } => key,
            // As for an update: a replica must hold the row to restore it,
            // and saying so discloses nothing.
            Located::Absent { .. } if self.client.is_some() => {
                return Err(update_target_not_loaded("RESTORE", table_name, row));
            }
            Located::Absent { .. } => {
                return Err(rejected(format!(
                    "RESTORE target {row:?} does not exist in `{table_name}`"
                )));
            }
            Located::Row { .. } | Located::Inherited { .. } => return Err(row_not_deleted(row)),
        };
        self.snapshot(table, row, &key, seq)?;
        let mut cells = BTreeMap::new();
        if let Some(content) = &content {
            Self::validate_cells(table, content)?;
            for column in &table.columns {
                if table.branch_by.contains(&column.name) {
                    continue;
                }
                // Restored content is a whole row: omitted columns take
                // their defaults, as on insert.
                let value = match (content.get(&column.name), &column.default) {
                    (Some(value), _) => value.clone(),
                    (None, Some(default)) => default.clone(),
                    (None, None) if column.nullable() => Value::Nullable(None),
                    (None, None) => {
                        return Err(missing_required(table_name, &column.name));
                    }
                };
                cells.insert(column.name.clone(), value);
            }
        }
        self.set_cells(
            table,
            row,
            &key,
            &cells,
            source,
            seq,
            Some(false),
            content.is_some(),
        )?;
        // A restore writes an existing row back: the UPDATE policy decides.
        if let Some(author) = self.checked(who) {
            self.write_check(table, row, &key, author, WriteCheck::Restore, seq)?;
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
        })
    }
}

/// A write's own policy check, deferred to the end of its unit.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteCheck {
    /// The INSERT policy on the new row.
    Insert,
    /// The UPDATE (or UPSERT) WITH CHECK on the new row.
    WithCheck { upsert: bool },
    /// The UPDATE policy on a restored row.
    Restore,
}

struct DeferredCheck {
    table: String,
    row: RowUuid,
    key: Vec<u8>,
    author: AuthorSubject,
    check: WriteCheck,
    /// Whether the write replaces content its row had before the unit: an
    /// update, or an insert or restore over a row the unit deleted first.
    /// A write that only adds a row to what policies read does not.
    replaces: bool,
}

impl WriteCheck {
    fn kind(&self) -> u8 {
        match self {
            WriteCheck::Insert => 0,
            WriteCheck::WithCheck { .. } => 1,
            WriteCheck::Restore => 2,
        }
    }
}

/// The most rows of its own writes a unit's write checks may read, summed
/// over every check (SPEC 7): past it, the unit is rejected as not
/// supported yet instead of being decided at unbounded cost.
const WRITE_CHECK_EVIDENCE_BUDGET: usize = 262_144;

/// What of a write check's overlay its policy read.
struct OverlayRead {
    /// Grounded rows bound into the check (see [`Ctx::overlaid_rows`]).
    rows: usize,
    /// Stored tables read through the overlay.
    tables: BTreeSet<String>,
}

/// The rows of its own writes a unit's write checks have read so far.
#[derive(Default)]
struct EvidenceBudget {
    read: usize,
}

impl EvidenceBudget {
    fn charge(&mut self, rows: usize) -> Result<(), Error> {
        self.read = self.read.saturating_add(rows);
        if self.read > WRITE_CHECK_EVIDENCE_BUDGET {
            return Err(rejected(format!(
                "Reading more than {WRITE_CHECK_EVIDENCE_BUDGET} rows of a transaction's own \
                 writes in its write-policy checks is not supported yet"
            )));
        }
        Ok(())
    }
}

fn write_check_denied(check: &DeferredCheck) -> Error {
    match check.check {
        WriteCheck::Insert => denied(format_args!(
            "INSERT policy denied the new row in `{}`",
            check.table
        )),
        WriteCheck::WithCheck { upsert } => denied(format_args!(
            "{} policy (WITH CHECK) denied the new row",
            if upsert { "UPSERT" } else { "UPDATE" }
        )),
        WriteCheck::Restore => denied("RESTORE policy denied the restored row"),
    }
}

/// Whether `policy` (of `table`) can only go from denied to allowed as rows
/// are added to the tables it reads. A `NOT` over anything that reads rows
/// is non-monotone, as is a relation that can drop rows when others appear
/// (a left join, an offset or a limit). Inherited policies are followed; a
/// cycle adds nothing new.
fn policy_is_monotone(
    schema: &Schema,
    table: &Table,
    policy: &PolicyExpr,
    seen: &mut BTreeSet<(String, u8)>,
) -> bool {
    let inherited = |schema: &Schema,
                     target: Option<&Table>,
                     operation: Operation,
                     seen: &mut BTreeSet<(String, u8)>| {
        let Some(target) = target else {
            return true;
        };
        if !seen.insert((target.name.clone(), operation as u8)) {
            return true;
        }
        target
            .policy(operation)
            .is_none_or(|policy| policy_is_monotone(schema, target, policy, seen))
    };
    match policy {
        PolicyExpr::Not(inner) => !policy_reads_other_rows(inner),
        PolicyExpr::And(parts) | PolicyExpr::Or(parts) => parts
            .iter()
            .all(|part| policy_is_monotone(schema, table, part, seen)),
        PolicyExpr::Exists {
            table: target,
            condition,
        } => {
            let target = schema.tables.get(target).unwrap_or(table);
            policy_is_monotone(schema, target, condition, seen)
        }
        PolicyExpr::ExistsRel { rel } => relation_is_monotone(rel),
        PolicyExpr::Inherits {
            operation,
            via_column,
            ..
        } => {
            let target = table
                .references
                .get(via_column)
                .and_then(|name| schema.tables.get(name));
            inherited(schema, target, *operation, seen)
        }
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            ..
        } => inherited(schema, schema.tables.get(source_table), *operation, seen),
        _ => true,
    }
}

fn relation_is_monotone(rel: &crate::query::RelationExpr) -> bool {
    use crate::query::{RelationExpr, RelationJoinKind};
    match rel {
        RelationExpr::TableScan { .. } => true,
        RelationExpr::Filter { input, .. }
        | RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. } => relation_is_monotone(input),
        RelationExpr::Union { inputs } => inputs.iter().all(|arm| relation_is_monotone(&arm.input)),
        RelationExpr::Join {
            left,
            right,
            join_kind,
            ..
        } => {
            matches!(join_kind, RelationJoinKind::Inner)
                && relation_is_monotone(left)
                && relation_is_monotone(right)
        }
        RelationExpr::Gather { seed, step, .. } => {
            relation_is_monotone(seed) && relation_is_monotone(step)
        }
        RelationExpr::Offset { .. } | RelationExpr::Limit { .. } => false,
    }
}

/// Which branch a write addresses.
enum Target {
    Root,
    Exact(BranchSelector),
    View {
        head: BranchSelector,
        base: Option<BranchSelector>,
        /// A frozen base's cut.
        at: Option<Cut>,
    },
}

impl Target {
    fn exact(target: &ExactWriteTarget) -> Self {
        match target {
            ExactWriteTarget::Root => Self::Root,
            ExactWriteTarget::Branch(branch) => Self::Exact(branch.clone()),
        }
    }

    fn of(target: &WriteTarget, inner: &Inner) -> Result<Self, Error> {
        Ok(match target {
            WriteTarget::Root => Self::Root,
            WriteTarget::BranchView { head, base } => {
                let (base, at) = match base {
                    None => (None, None),
                    Some(BranchViewBase::Current(base)) => (Some(base.clone()), None),
                    Some(BranchViewBase::Snapshot { branch, snapshot }) => (
                        Some(branch.clone()),
                        Some(inner.resolve_snapshot(snapshot)?),
                    ),
                };
                Self::View {
                    head: head.clone(),
                    base,
                    at,
                }
            }
        })
    }
}

/// Where a patch-style write lands.
enum Located {
    Row {
        key: Vec<u8>,
        seq: Pos,
        deleted: bool,
    },
    Inherited {
        from: Vec<u8>,
        to: Vec<u8>,
        head_cells: BTreeMap<String, Value>,
        /// A frozen base's cut.
        at: Option<Cut>,
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
    identity: WriteIdentity,
    ops: RefCell<Vec<StagedOp>>,
}

#[derive(Clone, Debug)]
pub(crate) enum StagedOp {
    Insert(String, BTreeMap<String, Value>, InsertOptions),
    Update(String, RowUuid, BTreeMap<String, Value>, UpdateOptions),
    /// The fourth field is what the upsert gives the columns it leaves
    /// out if it creates the row, by stored names: a replicated upsert's
    /// writer's defaults (see `Inner::stored_op`); empty on a local one.
    Upsert(
        String,
        RowUuid,
        BTreeMap<String, Value>,
        BTreeMap<String, Value>,
        UpsertOptions,
    ),
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
        self.inner().require_mutation_admission()?;
        Ok(MergeableTxHandle {
            db: self,
            identity: WriteIdentity::Database,
            ops: RefCell::new(Vec::new()),
        })
    }

    /// A staged transaction whose writes are authored by `author` (see
    /// [`Db::transaction_for_identity`]).
    pub async fn mergeable_tx_for_identity(
        &self,
        author: AuthorSubject,
    ) -> Result<MergeableTxHandle<'_>, Error> {
        self.inner().require_mutation_admission()?;
        Ok(MergeableTxHandle {
            db: self,
            identity: WriteIdentity::Attribution(author),
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
                "SELECT {} FROM {} WHERE id = ?1 AND _b = ?2 AND (_st & {STATE_DELETED}) = 0",
                quote(&column.stored),
                quote(&t.stored)
            );
            let cell: Sql = self
                .conn
                .cached_row(
                    &sql,
                    rusqlite::params![row.0.as_bytes().to_vec(), key],
                    |r| r.get(0),
                )
                .map_err(|_| rejected(format!("move source {row:?} does not exist")))?;
            let value = column_from_sql(column, cell.as_ref(), &self.chunks)?;
            if let Some(value) = value {
                cells.insert(column.name.clone(), value);
            }
        }
        self.delete(table, row, &Target::Exact(from), Who::Local, seq)?;
        // The moved row carries its stored cells: they are not re-checked.
        let to = Target::Exact(to);
        match self.locate(t, row, &to)? {
            Located::Row { deleted: true, .. } => self.restore(
                table,
                row,
                Some(cells),
                Cells::Carried,
                &to,
                Who::Local,
                seq,
            ),
            _ => self.insert(
                table,
                cells,
                Cells::Carried,
                Some(row),
                &to,
                Who::Local,
                seq,
            ),
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
        self.db.inner().require_mutation_admission()?;
        let row = match opts.row_id {
            Some(row) => row,
            None => self.db.inner().ids.next_row_id(),
        };
        opts.row_id = Some(row);
        self.ops
            .borrow_mut()
            .push(StagedOp::Insert(table.to_owned(), cells, opts));
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<(), Error> {
        self.db.inner().require_mutation_admission()?;
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
        self.db.inner().require_mutation_admission()?;
        self.ops.borrow_mut().push(StagedOp::Upsert(
            table.to_owned(),
            row,
            cells,
            BTreeMap::new(),
            opts,
        ));
        Ok(())
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<(), Error> {
        self.db.inner().require_mutation_admission()?;
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
        self.db.inner().require_mutation_admission()?;
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
        self.db.inner().require_mutation_admission()?;
        self.ops
            .borrow_mut()
            .push(StagedOp::Move(table.to_owned(), source, destination, row));
        Ok(())
    }

    pub async fn commit(self) -> Result<TxReceipt, Error> {
        let ops = self.ops.into_inner();
        let db = self.db;
        let identity = self.identity;
        let ((), receipt) = db
            .transaction_as(identity, async |_tx| {
                for op in ops {
                    match op {
                        StagedOp::Insert(t, c, o) => db
                            .insert(
                                &t,
                                c,
                                InsertOptions {
                                    identity: in_transaction(o.identity, identity)?,
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
                                    identity: in_transaction(o.identity, identity)?,
                                    ..o
                                },
                            )
                            .await
                            .map(drop)?,
                        StagedOp::Upsert(t, r, c, _, o) => db
                            .upsert(
                                &t,
                                r,
                                c,
                                UpsertOptions {
                                    identity: in_transaction(o.identity, identity)?,
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
                                    identity: in_transaction(o.identity, identity)?,
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
                                    identity: in_transaction(o.identity, identity)?,
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
    /// Ship more of the app's versions and lenses (see
    /// [`DbConfig::with_migrations`]): a client learns an app's migrations
    /// when it first sees the app, not only when it opens.
    pub fn add_migrations(&self, migrations: Migrations) {
        let mut inner = self.inner();
        for schema in migrations.schemas {
            let version = schema.version_id();
            if !inner
                .migrations
                .schemas
                .iter()
                .any(|s| s.version_id() == version)
            {
                inner.migrations.schemas.push(schema);
            }
        }
        for lens in migrations.lenses {
            if !inner
                .migrations
                .lenses
                .iter()
                .any(|l| l.source == lens.source && l.target == lens.target)
            {
                inner.migrations.lenses.push(lens);
            }
        }
    }

    pub async fn register_schema_view(&self, schema: JazzSchema) -> Result<Db, Error> {
        // A version the app's lenses lead to is admitted as the authority
        // admits it, not named against the current one.
        self.admit_along_migrations(&schema)?;
        let logical = {
            let mut inner = self.inner.borrow_mut();
            let version = schema.version_id();
            if let Some((_, admitted)) = inner.admitted.iter_mut().find(|(id, _)| *id == version) {
                let same_policies =
                    |l: &crate::schema::TableSchema, r: &crate::schema::TableSchema| {
                        l.read_policy == r.read_policy && l.write_policies == r.write_policies
                    };
                // A version mirrored from the upstream's catalogue travels
                // without its policies: the view brings them.
                if !same_tables(admitted, &schema, same_policies)
                    && same_tables(admitted, &schema.without_permissions(), same_policies)
                {
                    *admitted = schema.clone();
                }
                if !same_tables(admitted, &schema, same_policies) {
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
                // A version this file admitted before (it has stored names)
                // was checked then; it may since be more than one lens away.
                if inner.names.get(&version).is_none() {
                    check_direct_lens(&inner.owner.jazz, &schema)?;
                }
            }
            let base = inner.owner.names();
            let logical = inner.names.resolve(&schema, &[], Some(&base))?;
            inner.names.check_branches(&logical)?;
            inner.names.check_types(&logical)?;
            inner.write_admission(&logical, Vec::new(), false)?;
            if !inner.admitted.iter().any(|(id, _)| *id == version) {
                inner.admitted.push((version, schema.clone()));
            }
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
/// A batch's staged writes applied in the open `overlay` savepoint.
struct Overlay {
    batch: OpenTransactionId,
    /// How many of the batch's writes it holds.
    applied: usize,
    /// `changed` before the overlay, restored when it is undone.
    mark: usize,
}

/// The store behind its one gate: every borrow first undoes a transaction
/// handle's overlay, so nothing but that handle's next stage or read sees
/// it.
struct Store(RefCell<Inner>);

impl Store {
    fn new(inner: Inner) -> Self {
        Self(RefCell::new(inner))
    }

    fn borrow_mut(&self) -> std::cell::RefMut<'_, Inner> {
        let mut inner = self.0.borrow_mut();
        inner.settle_overlay();
        inner
    }

    fn borrow(&self) -> std::cell::Ref<'_, Inner> {
        // A borrow already held means the overlay was undone when it was
        // taken.
        if let Ok(mut inner) = self.0.try_borrow_mut() {
            inner.settle_overlay();
        }
        self.0.borrow()
    }

    /// The store with a handle's overlay kept for its next stage.
    fn staging(&self) -> std::cell::RefMut<'_, Inner> {
        self.0.borrow_mut()
    }
}

struct Batch {
    kind: BatchKind,
    ops: Vec<(Rc<Schema>, StagedOp)>,
    /// What the batch's partial large-value updates built: descriptors
    /// derived from stored trees and the nodes consolidations prepared,
    /// trusted and stored whenever the staged writes apply.
    edits: LargeEdits,
}

#[derive(Default)]
pub(crate) struct LargeEdits {
    pub(crate) derived: Vec<jazz_types::large_values::LargeValueRef>,
    pub(crate) pending_nodes: Vec<jazz_types::large_values::PreparedLargeValue>,
}

/// An admitted read's boundary in the batch's append-only staging buffers.
/// Capturing lengths retains no copy of unrelated staged writes.
#[derive(Clone, Copy, PartialEq, Eq)]
struct BatchPrefix {
    ops: usize,
    derived: usize,
    pending_nodes: usize,
}

impl Batch {
    /// The rows the batch writes, by id.
    fn rows(&self) -> Vec<[u8; 16]> {
        self.ops
            .iter()
            .filter_map(|(_, op)| op.row())
            .map(|row| *row.0.as_bytes())
            .collect()
    }

    fn prefix(&self) -> BatchPrefix {
        BatchPrefix {
            ops: self.ops.len(),
            derived: self.edits.derived.len(),
            pending_nodes: self.edits.pending_nodes.len(),
        }
    }
}

enum BatchKind {
    Mergeable,
    /// Serializable: reads see the store as of `snapshot` plus the batch's
    /// own writes; each read's snapshot result is kept for validation.
    /// On a client the first successful read pins the snapshot: the
    /// authority's state for an answered remote read, or the replica's
    /// known cut for a LocalOnly read or a LocalFirst offline fallback.
    Serializable {
        snapshot: Pos,
        reads: Vec<ReadRecord>,
        pinned: bool,
        /// On a client, its own commits still unsettled at begin: the
        /// snapshot includes them (reads see what this client wrote).
        own: Vec<u64>,
    },
}

/// One read of a serializable transaction: what it returned (row ids and
/// versions) at the snapshot, re-checked at commit.
struct ReadRecord {
    prepared: PreparedQuery,
    include_deleted: bool,
    rows: Vec<(RowUuid, Pos)>,
}

/// An upload the authority refused: the node and local seq of the commit
/// that wrote it (this store's, or a relayed session's), why, and whether
/// it was a serializable transaction.
#[derive(Debug, Clone)]
pub struct UpstreamRejection {
    pub writer: [u8; 16],
    pub seq: u64,
    pub error: Error,
    pub serializable: bool,
}

fn conflict(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::TransactionConflict, message)
}

fn versions(rows: &[CurrentRow]) -> Vec<(RowUuid, Pos)> {
    rows.iter().map(|row| (row.id, row.seq)).collect()
}

/// The versions of a relation tree's child rows, depth first.
fn child_versions(node: &ResultNode, out: &mut Vec<(RowUuid, Pos)>) {
    for relation in node.relations.values() {
        if let ResultRelation::Array(children) = relation {
            for child in children {
                out.push((child.row.id, child.row.seq));
                child_versions(child, out);
            }
        }
    }
}

impl StagedOp {
    fn identity(&self) -> Option<WriteIdentity> {
        match self {
            StagedOp::Insert(.., o) => Some(o.identity),
            StagedOp::Update(.., o) => Some(o.identity),
            StagedOp::Upsert(.., o) => Some(o.identity),
            StagedOp::Delete(.., o) => Some(o.identity),
            StagedOp::Restore(.., o) => Some(o.identity),
            StagedOp::Move(..) => None,
        }
    }

    /// The write's provenance author when it is not the database's own: a
    /// trusted session's, or an attribution.
    pub(crate) fn author(&self) -> Option<AuthorSubject> {
        match self.identity()? {
            WriteIdentity::Session(author) | WriteIdentity::Attribution(author)
                if author != AuthorSubject::SYSTEM =>
            {
                Some(author)
            }
            _ => None,
        }
    }

    /// The write's explicit provenance timestamp, if it names one.
    pub(crate) fn updated_at_ms(&self) -> Option<u64> {
        match self {
            StagedOp::Insert(.., o) => o.updated_at_ms,
            StagedOp::Update(.., o) => o.updated_at_ms,
            StagedOp::Upsert(.., o) => o.updated_at_ms,
            StagedOp::Delete(.., o) => o.updated_at_ms,
            StagedOp::Restore(.., o) => o.updated_at_ms,
            StagedOp::Move(..) => None,
        }
    }

    /// Whether the write is a trusted session's: its policies are checked
    /// as that session at the authority.
    pub(crate) fn is_session(&self) -> bool {
        matches!(self.identity(), Some(WriteIdentity::Session(author)) if author != AuthorSubject::SYSTEM)
    }

    /// Attribute the write to `author`: as its session (`session`), whose
    /// policies apply, or as provenance only.
    pub(crate) fn attribute(&mut self, author: AuthorSubject, session: bool) {
        let identity = if session {
            WriteIdentity::Session(author)
        } else {
            WriteIdentity::Attribution(author)
        };
        match self {
            StagedOp::Insert(.., o) => o.identity = identity,
            StagedOp::Update(.., o) => o.identity = identity,
            StagedOp::Upsert(.., o) => o.identity = identity,
            StagedOp::Delete(.., o) => o.identity = identity,
            StagedOp::Restore(.., o) => o.identity = identity,
            StagedOp::Move(..) => {}
        }
    }

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
    /// Re-apply a staged write unchecked, keeping its provenance.
    fn apply(&mut self, op: &StagedOp, seq: u64) -> Result<WriteHandle, Error> {
        let who = op.author().map_or(Who::Local, Who::Attributed);
        self.apply_as(op, who, seq)
    }

    /// Apply one write as `who`: policies apply for a session identity.
    fn apply_as(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        let author = match who {
            Who::Session(author) | Who::Attributed(author) => author,
            Who::Local => self.stamp_author(),
        };
        let now = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        if let Some(session) = self.checked(who) {
            Self::admit_session_write(op, session, now)?;
        }
        let (last_seq, last_at) = self.last_stamp;
        let at = if seq == last_seq {
            last_at
        } else {
            now.max(last_at + 1)
        };
        self.last_stamp = (seq, at);
        // An explicit timestamp (a backfill) stamps this write alone; the
        // clock's monotonic stamps go on.
        let at = op.updated_at_ms().map_or(at, |ms| ms as i64);
        self.writer = Some(Stamp {
            at,
            author: self.author_code(&author_key(&author))?,
        });
        let result = self.apply_op(op, who, seq);
        self.writer = None;
        result
    }

    /// What the authority refuses of a session's write before any policy
    /// runs (SPEC 7, INV-RLS-15; SPEC 3, INV-TX-7): every write by an
    /// anonymous identity, and an explicit timestamp more than
    /// [`SKEW_TOLERANCE_MS`] ahead of the authority's clock, which would
    /// stamp provenance in the future.
    fn admit_session_write(op: &StagedOp, session: AuthorSubject, now: i64) -> Result<(), Error> {
        if session.is_anonymous() {
            return Err(denied("anonymous identities cannot write"));
        }
        if let Some(at) = op.updated_at_ms()
            && at as i64 > now.saturating_add(SKEW_TOLERANCE_MS)
        {
            return Err(rejected_for(
                Reason::ClientClockTooFarAhead,
                format!(
                    "{CLIENT_CLOCK_TOO_FAR_AHEAD}: the write's timestamp is {}ms ahead of the authority's clock",
                    at as i64 - now
                ),
            ));
        }
        Ok(())
    }

    fn apply_op(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        match op.clone() {
            StagedOp::Insert(t, c, o) => {
                let target = Target::exact(&o.target);
                self.insert(&t, c, Cells::Authored, o.row_id, &target, who, seq)
            }
            StagedOp::Update(t, r, c, o) => {
                let target = Target::of(&o.target, self)?;
                self.update(&t, r, c, &target, who, seq)
            }
            StagedOp::Upsert(t, r, c, created, o) => {
                let target = Target::of(&o.target, self)?;
                self.upsert(&t, r, c, created, &target, who, seq)
            }
            StagedOp::Delete(t, r, o) => {
                let target = Target::of(&o.target, self)?;
                self.delete(&t, r, &target, who, seq)
            }
            StagedOp::Restore(t, r, c, o) => {
                let target = Target::exact(&o.target);
                self.restore(&t, r, c, Cells::Authored, &target, who, seq)
            }
            StagedOp::Move(t, from, to, r) => self.move_row(&t, from, to, r, seq),
        }
    }

    /// Apply staged writes at `seq`, each under the schema that staged it.
    /// Ops from `checked` on are checked as their own identity would be (a
    /// session's policies apply) instead of re-applied unchecked: how a
    /// newly staged write is validated.
    fn apply_all_checking(
        &mut self,
        ops: &[(Rc<Schema>, StagedOp)],
        checked: usize,
        seq: u64,
    ) -> Result<(), Error> {
        let active = self.schema.clone();
        let result = ops.iter().enumerate().try_for_each(|(i, (schema, op))| {
            self.schema = schema.clone();
            if i < checked {
                self.apply(op, seq).map(drop)
            } else {
                let identity = op.identity().map_or(Who::Local, who);
                self.apply_as(op, identity, seq).map(drop)
            }
        });
        self.schema = active;
        result
    }

    /// [`Inner::apply_all_checking`] trusting and storing what a batch's partial
    /// large-value updates built.
    fn apply_with_edits(
        &mut self,
        ops: &[(Rc<Schema>, StagedOp)],
        edits: &LargeEdits,
        checked: usize,
        seq: u64,
    ) -> Result<(), Error> {
        self.derived.extend(edits.derived.iter().cloned());
        self.pending_nodes
            .extend(edits.pending_nodes.iter().cloned());
        let result = self.apply_all_checking(ops, checked, seq);
        self.clear_derived();
        result
    }

    /// On a replica, the newest version of `row` the authority settled.
    fn settled_seq(&self, table: &str, row: RowUuid) -> Result<Option<u64>, Error> {
        let sql = format!(
            "SELECT MAX(_seq) FROM {} WHERE id = ?1 AND _n = 0",
            quote(table)
        );
        self.conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                r.get::<_, Option<i64>>(0)
            })
            .map(|seq| seq.map(|seq| seq as u64))
            .map_err(storage)
    }

    /// The newest committed version of `row` in stored table `table` on
    /// any branch.
    pub(crate) fn newest_seq(&self, table: &str, row: RowUuid) -> Result<Option<Pos>, Error> {
        let sql = format!(
            "SELECT _n, _seq FROM {} WHERE id = ?1 ORDER BY _n DESC, _seq DESC LIMIT 1",
            quote(table)
        );
        self.conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                Ok(Pos {
                    n: r.get(0)?,
                    seq: r.get::<_, i64>(1)? as u64,
                })
            })
            .optional()
            .map_err(storage)
    }
}

/// Staging and reading handle for one open batch, through one schema view.
pub struct TxRef<'a> {
    db: &'a Db,
    id: OpenTransactionId,
}

impl Db {
    pub async fn begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.begin(id, BatchKind::Mergeable)
    }

    /// A serializable transaction: snapshot reads, validated at commit.
    /// On a client the snapshot is the position its server served it
    /// complete through, plus its own unsettled commits: never positions it
    /// numbered itself, which would hold every authority version.
    pub async fn begin_serializable(&self, id: OpenTransactionId) -> Result<(), Error> {
        let (snapshot, own) = {
            let inner = self.inner.borrow();
            let snapshot = match &inner.client {
                Some(client) => Pos {
                    n: client.known_n,
                    seq: client.known,
                },
                None => inner.frontier(),
            };
            (snapshot, inner.unsettled_own())
        };
        self.begin(
            id,
            BatchKind::Serializable {
                snapshot,
                reads: Vec::new(),
                pinned: !self.is_client(),
                own,
            },
        )
    }

    fn begin(&self, id: OpenTransactionId, kind: BatchKind) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        inner.require_mutation_admission()?;
        if inner.batches.contains_key(&id) {
            return Err(rejected(format!("transaction {id:?} is already open")));
        }
        inner.batches.insert(
            id,
            Batch {
                kind,
                ops: Vec::new(),
                edits: LargeEdits::default(),
            },
        );
        Ok(())
    }

    /// A fresh row id from this database's id source.
    pub fn next_row_id(&self) -> RowUuid {
        self.inner().ids.next_row_id()
    }

    /// The open transaction `id`, mergeable or serializable.
    pub fn tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
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

    fn take_batch(&self, id: OpenTransactionId) -> Result<Batch, Error> {
        self.inner().require_mutation_admission()?;
        self.inner
            .borrow_mut()
            .batches
            .remove(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))
    }

    pub async fn commit_mergeable_handle(&self, id: OpenTransactionId) -> Result<TxReceipt, Error> {
        let batch = self.take_batch(id)?;
        self.unheld_rows(&batch.rows()).await;
        if !matches!(batch.kind, BatchKind::Mergeable) {
            return Err(rejected(format!("transaction {id:?} is not mergeable")));
        }
        self.commit_ops(&batch, sync::Kind::Mergeable)
    }

    /// Serializable commit: every row the batch writes must still be at a
    /// version at or before the snapshot, and every read must return the
    /// same rows at the same versions now as it did at the snapshot (this
    /// catches phantoms: rows that entered or left a predicate or window).
    /// The check and the apply run under one store borrow, so nothing can
    /// commit in between.
    pub async fn commit_serializable_handle(
        &self,
        id: OpenTransactionId,
    ) -> Result<TxReceipt, Error> {
        let batch = self.take_batch(id)?;
        self.unheld_rows(&batch.rows()).await;
        let BatchKind::Serializable {
            snapshot,
            reads,
            own,
            ..
        } = &batch.kind
        else {
            return Err(rejected(format!("transaction {id:?} is not serializable")));
        };
        // On a client the authority validates, against its own state; the
        // commit applies optimistically here meanwhile. A row the replica
        // already knows moved past the snapshot is a conflict now (with no
        // own writes still unsettled at the snapshot, whose landing seqs
        // only the authority can place).
        if self.is_client() {
            // A row a later local commit wrote (one not unsettled at begin)
            // changed since the snapshot: the authority gets that commit
            // first, so it would refuse this one too.
            // An own commit unsettled at begin counts wherever it landed.
            let landed = self.inner().own_landings(own)?;
            {
                let inner = self.inner();
                for (schema, op) in &batch.ops {
                    if let Some(row) = op.row()
                        && inner
                            .newest_seq(schema.stored_table(op.table())?, row)?
                            .is_some_and(|at| {
                                let ours = at.n == inner.me && own.contains(&at.seq)
                                    || landed.values().any(|&landed| landed == at);
                                !(at.accepted() || ours)
                            })
                    {
                        return Err(conflict(
                            "row visible parent changed since transaction write was staged",
                        ));
                    }
                }
            }
            if own.is_empty() {
                let inner = self.inner();
                for (schema, op) in &batch.ops {
                    if let Some(row) = op.row()
                        && inner
                            .settled_seq(schema.stored_table(op.table())?, row)?
                            .is_some_and(|seq| Pos::at(seq) > *snapshot)
                    {
                        return Err(conflict(
                            "row visible parent changed since transaction write was staged",
                        ));
                    }
                }
            }
            // A read row the replica now holds at another version changed,
            // and the authority, which gets this replica's earlier writes
            // first, would refuse the commit too; so it is a conflict now.
            // A row read at an own unsettled commit's local version that now
            // holds a server version (or the version a relay with no server
            // settled it at) may only have settled: the authority decides
            // that one.
            {
                let inner = self.inner();
                for read in reads {
                    let spec = ReadSpec {
                        include_deleted: read.include_deleted,
                        ..ReadSpec::default()
                    };
                    let now = inner.read_versions(&read.prepared, None, &spec)?;
                    let changed = now.len() != read.rows.len()
                        || now.iter().zip(&read.rows).any(|(now, then)| {
                            now.0 != then.0
                                || (now.1 != then.1
                                    && !(then.1.n == inner.me
                                        && own.contains(&then.1.seq)
                                        && (now.1.accepted()
                                            || landed.get(&then.1.seq) == Some(&now.1))))
                        });
                    if changed {
                        return Err(conflict(
                            "a row read by the transaction changed since its snapshot",
                        ));
                    }
                }
            }
            let (reads, by) = {
                let inner = self.inner();
                let reads = reads
                    .iter()
                    .map(|read| {
                        Ok(sync::WireRead {
                            version: *read
                                .prepared
                                .schema
                                .as_ref()
                                .map_or(inner.owner_version, |schema| schema.version)
                                .as_bytes(),
                            query: read.prepared.query.clone(),
                            binding: read.prepared.binding.clone(),
                            include_deleted: read.include_deleted,
                            rows: read
                                .rows
                                .iter()
                                .map(|(row, at)| {
                                    Ok((*row.0.as_bytes(), inner.node_of(at.n)?, at.seq))
                                })
                                .collect::<Result<_, Error>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                (reads, inner.node_of(snapshot.n)?)
            };
            return self.commit_ops(
                &batch,
                sync::Kind::Serializable {
                    snapshot: snapshot.seq,
                    by,
                    reads,
                    own: own.clone(),
                },
            );
        }
        {
            let inner = self.inner();
            for (schema, op) in &batch.ops {
                if let Some(row) = op.row()
                    && inner
                        .newest_seq(schema.stored_table(op.table())?, row)?
                        .is_some_and(|at| at > *snapshot)
                {
                    return Err(conflict(
                        "row visible parent changed since transaction write was staged",
                    ));
                }
            }
            for read in reads {
                let spec = ReadSpec {
                    include_deleted: read.include_deleted,
                    ..ReadSpec::default()
                };
                if inner.read_versions(&read.prepared, None, &spec)? != read.rows {
                    return Err(conflict(
                        "a row read by the transaction changed since its snapshot",
                    ));
                }
            }
        }
        self.commit_ops(&batch, sync::Kind::Mergeable)
    }

    fn commit_ops(&self, batch: &Batch, kind: sync::Kind) -> Result<TxReceipt, Error> {
        self.with_upload(|| self.commit_ops_now(batch, kind))
    }

    fn commit_ops_now(&self, batch: &Batch, kind: sync::Kind) -> Result<TxReceipt, Error> {
        self.inner().require_mutation_admission()?;
        self.inner().refuse_held(&batch.rows())?;
        let ops = &batch.ops;
        let (seq, prior, mark) = {
            let mut inner = self.inner();
            let seq = inner.next_seq();
            begin_savepoint(&inner.conn, "commit_batch")?;
            let mark = inner.changed.borrow().len();
            inner.defer_checks();
            let applied = inner
                .apply_with_edits(ops, &batch.edits, ops.len(), seq)
                .and_then(|()| inner.finish_checks(seq));
            if let Err(error) = applied {
                inner.abandon_checks();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "commit_batch", false)?;
                return Err(error);
            }
            end_savepoint(&inner.conn, "commit_batch", true)?;
            let prior = inner.seq;
            if !ops.is_empty() || kind.validates_reads() {
                inner.seq = seq;
            }
            (seq, prior, mark)
        };
        // `commit_ops` holds the upload record and the commit in one SQLite
        // transaction and rolls both back on an error.
        // Each op travels by stored names, so one transaction may write
        // through views of several versions.
        let recorded = ops
            .iter()
            .map(|(schema, op)| self.inner().stored_op(op.clone(), schema))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|ops| self.record_stored_upload(ops, seq, kind, None));
        if let Err(error) = recorded {
            let mut inner = self.inner();
            inner.seq = prior;
            inner.changed.borrow_mut().truncate(mark);
            return Err(error);
        }
        self.notify();
        Ok(TxReceipt(seq))
    }

    /// Run `read` over the batch's staged writes (plus `extra`, checked as
    /// its identity would be, its counter changes taken from what the
    /// batch sees). The staged writes are replayed into a savepoint, the
    /// overlay, which stays applied for the batch's next stage or read and
    /// is undone before anything else uses the store ([`Store`]): staging
    /// a batch write by write replays it once, not once per write. `extra`
    /// joins the overlay when it applies; `read` sees it and is undone.
    /// Serializable batches read through the snapshot cut, which also
    /// admits their own writes.
    fn in_overlay<R>(
        &self,
        id: OpenTransactionId,
        extra: Option<&mut StagedOp>,
        read: impl FnOnce(&mut Inner, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut inner = self.inner.staging();
        inner.schema = self.view.clone().unwrap_or_else(|| inner.owner.clone());
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        let staged = batch.ops.len();
        let live = inner
            .overlay
            .as_ref()
            .is_some_and(|overlay| overlay.batch == id && overlay.applied == staged);
        let ops = (!live).then(|| batch.ops.clone());
        let edits = LargeEdits {
            derived: batch.edits.derived.clone(),
            pending_nodes: batch.edits.pending_nodes.clone(),
        };
        let snapshot = match &batch.kind {
            BatchKind::Mergeable => None,
            BatchKind::Serializable { snapshot, own, .. } => {
                Some(inner.serializable_cut(*snapshot, own)?)
            }
        };
        let own = inner.seq + 1;
        inner.derived.extend(edits.derived);
        inner.pending_nodes.extend(edits.pending_nodes);
        if let Some(ops) = ops {
            inner.settle_overlay();
            begin_savepoint(&inner.conn, "overlay")?;
            let mark = inner.changed.borrow().len();
            if let Err(error) = inner.apply_all_checking(&ops, ops.len(), own) {
                inner.clear_derived();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "overlay", false)?;
                return Err(error);
            }
            inner.overlay = Some(Overlay {
                batch: id,
                applied: staged,
                mark,
            });
        }
        if let Err(error) = begin_savepoint(&inner.conn, "overlay_step") {
            inner.clear_derived();
            inner.settle_overlay();
            return Err(error);
        }
        let mark = inner.changed.borrow().len();
        let joins = extra.is_some();
        let applied = match extra {
            Some(op) => inner.counter_deltas(op).and_then(|()| {
                let schema = inner.schema.clone();
                inner.apply_all_checking(&[(schema, op.clone())], 0, own)
            }),
            None => Ok(()),
        };
        inner.clear_derived();
        let result = applied.and_then(|()| {
            let own = inner.own_pos(own);
            read(&mut inner, snapshot.map(|cut| Cut { own, ..cut }))
        });
        let keep = joins && result.is_ok();
        if !keep {
            inner.changed.borrow_mut().truncate(mark);
        }
        if let Err(error) = end_savepoint(&inner.conn, "overlay_step", keep) {
            inner.settle_overlay();
            return Err(error);
        }
        if keep && let Some(overlay) = inner.overlay.as_mut() {
            overlay.applied += 1;
        }
        result
    }

    /// Run `read` over the batch's writes staged before `prefix` was
    /// captured, so a read that waited (for authority coverage, or behind a
    /// cancelled read) does not see writes staged meanwhile. When nothing
    /// was staged since, this is [`Self::in_overlay`] and keeps its cached
    /// overlay; otherwise the prefix is replayed in a throwaway savepoint,
    /// which undoes the cached overlay, so the next stage replays the batch.
    fn in_overlay_prefix<R>(
        &self,
        id: OpenTransactionId,
        prefix: BatchPrefix,
        read: impl FnOnce(&mut Inner, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        // Peek without the store gate, which would undo the cached overlay.
        let grown = self
            .inner
            .staging()
            .batches
            .get(&id)
            .is_none_or(|batch| batch.prefix() != prefix);
        if !grown {
            return self.in_overlay(id, None, read);
        }
        let mut inner = self.inner();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        let unavailable = || {
            Error::new(
                ErrorCode::NotObserved,
                "transaction staged prefix is no longer available",
            )
        };
        let ops = batch
            .ops
            .get(..prefix.ops)
            .ok_or_else(unavailable)?
            .to_vec();
        let edits = LargeEdits {
            derived: batch
                .edits
                .derived
                .get(..prefix.derived)
                .ok_or_else(unavailable)?
                .to_vec(),
            pending_nodes: batch
                .edits
                .pending_nodes
                .get(..prefix.pending_nodes)
                .ok_or_else(unavailable)?
                .to_vec(),
        };
        let snapshot = match &batch.kind {
            BatchKind::Mergeable => None,
            BatchKind::Serializable { snapshot, own, .. } => {
                Some(inner.serializable_cut(*snapshot, own)?)
            }
        };
        let own = inner.seq + 1;
        begin_savepoint(&inner.conn, "overlay")?;
        let mark = inner.changed.borrow().len();
        let result = inner
            .apply_with_edits(&ops, &edits, ops.len(), own)
            .and_then(|()| {
                let own = inner.own_pos(own);
                read(&mut inner, snapshot.map(|cut| Cut { own, ..cut }))
            });
        inner.changed.borrow_mut().truncate(mark);
        end_savepoint(&inner.conn, "overlay", false)?;
        result
    }

    fn stage(&self, id: OpenTransactionId, op: StagedOp) -> Result<(), Error> {
        self.stage_with_edits(id, op, LargeEdits::default())
    }

    fn stage_op(&self, id: OpenTransactionId, mut op: StagedOp) -> Result<(), Error> {
        // Validate against the overlay now, so errors surface at the call;
        // a counter write's change is from what the transaction sees.
        self.in_overlay(id, Some(&mut op), |_, _| Ok(()))
            .map_err(|error| self.serializable_write_conflict(id, &op, error))?;
        let mut inner = self.inner.staging();
        let schema = self.view.clone().unwrap_or_else(|| inner.owner.clone());
        inner
            .batches
            .get_mut(&id)
            .expect("validated")
            .ops
            .push((schema, op));
        Ok(())
    }

    /// A serializable write the store refuses for its row's state (an id
    /// taken, a row deleted) conflicts when that state is newer than the
    /// transaction's snapshot: at the snapshot the write was valid, and a
    /// retry sees what moved.
    fn serializable_write_conflict(
        &self,
        id: OpenTransactionId,
        op: &StagedOp,
        error: Error,
    ) -> Error {
        if !error.is_apply_time_rejection() {
            return error;
        }
        let inner = self.inner();
        let Some(Batch {
            kind: BatchKind::Serializable { snapshot, own, .. },
            ..
        }) = inner.batches.get(&id)
        else {
            return error;
        };
        let (Some(row), Ok(table)) = (op.row(), inner.schema.stored_table(op.table())) else {
            return error;
        };
        let moved = inner.serializable_cut(*snapshot, own).and_then(|cut| {
            Ok(inner
                .newest_seq(table, row)?
                .is_some_and(|seq| !cut.sees(seq)))
        });
        match moved {
            Ok(true) => conflict("a row written by the transaction changed since its snapshot"),
            _ => error,
        }
    }

    /// [`Self::stage`] a write whose large values a partial update built:
    /// the batch keeps what it built for every later apply.
    pub(crate) fn stage_with_edits(
        &self,
        id: OpenTransactionId,
        mut op: StagedOp,
        mut edits: LargeEdits,
    ) -> Result<(), Error> {
        let marks = {
            let mut inner = self.inner.staging();
            inner.require_mutation_admission()?;
            inner.schema = self.view.clone().unwrap_or_else(|| inner.owner.clone());
            let chunked = inner.chunk_large_cells(&mut op)?;
            edits.pending_nodes.extend(chunked.pending_nodes);
            let batch = inner
                .batches
                .get_mut(&id)
                .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
            let marks = (batch.edits.derived.len(), batch.edits.pending_nodes.len());
            batch.edits.derived.extend(edits.derived);
            batch.edits.pending_nodes.extend(edits.pending_nodes);
            marks
        };
        let staged = self.stage_op(id, op);
        if staged.is_err()
            && let Some(batch) = self.inner.staging().batches.get_mut(&id)
        {
            batch.edits.derived.truncate(marks.0);
            batch.edits.pending_nodes.truncate(marks.1);
        }
        staged
    }

    fn record_read(
        &self,
        id: OpenTransactionId,
        prepared: &PreparedQuery,
        include_deleted: bool,
    ) -> Result<(), Error> {
        // A mergeable read records nothing: peek without the store gate,
        // so the batch's cached overlay stays for its next stage or read.
        if !matches!(
            self.inner.staging().batches.get(&id),
            Some(Batch {
                kind: BatchKind::Serializable { .. },
                ..
            })
        ) {
            return Ok(());
        }
        let mut inner = self.inner();
        let Some(Batch {
            kind: BatchKind::Serializable { snapshot, own, .. },
            ..
        }) = inner.batches.get(&id)
        else {
            return Ok(());
        };
        let read = ReadSpec {
            include_deleted,
            cut: Some(inner.serializable_cut(*snapshot, own)?),
            ..ReadSpec::default()
        };
        let rows = inner.read_versions(prepared, None, &read)?;
        if let Some(Batch {
            kind: BatchKind::Serializable { reads, pinned, .. },
            ..
        }) = inner.batches.get_mut(&id)
        {
            // Even a LocalOnly read observes this cut. Later remote coverage
            // must not move a snapshot whose results have already escaped.
            *pinned = true;
            reads.push(ReadRecord {
                prepared: prepared.clone(),
                include_deleted,
                rows,
            });
        }
        Ok(())
    }
}

impl TxRef<'_> {
    /// Stage one write, fetching what an asynchronous chunk store holds for
    /// it first if it needs that.
    async fn stage(&self, op: StagedOp) -> Result<(), Error> {
        self.db
            .fetching_with(op, |op| self.db.stage(self.id, op))
            .await
    }

    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        mut opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        let row = match opts.row_id {
            Some(row) => row,
            // Drawing an id leaves the batch's overlay applied.
            None => self.db.inner.staging().ids.next_row_id(),
        };
        opts.row_id = Some(row);
        self.stage(StagedOp::Insert(table.to_owned(), cells, opts))
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    pub async fn update(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<WriteHandle, Error> {
        self.stage(StagedOp::Update(table.to_owned(), row, cells, opts))
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<WriteHandle, Error> {
        self.stage(StagedOp::Upsert(
            table.to_owned(),
            row,
            cells,
            BTreeMap::new(),
            opts,
        ))
        .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        self.stage(StagedOp::Delete(table.to_owned(), row, opts))
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    /// Undelete `row`, optionally with replacement content.
    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<WriteHandle, Error> {
        self.stage(StagedOp::Restore(table.to_owned(), row, content, opts))
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
        })
    }

    pub async fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_with(prepared, false).await
    }

    /// A read in the transaction, its relations nested in their parents'
    /// cells; `include_deleted` lists deleted rows too (with their deletion
    /// metadata), as outside one.
    pub async fn all_prepared_with(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_as(prepared, include_deleted, None).await
    }

    /// As [`Self::all_prepared_with`], seeing only what `identity`'s read
    /// policies let it see (`None`: everything).
    pub async fn all_prepared_as(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_at(prepared, include_deleted, identity, ReadTier::LocalFirst)
            .await
    }

    /// As [`Self::all_prepared_as`], at `tier`: a `Remote` read waits for
    /// the authority's answer, offline too. `LocalOnly` never asks the
    /// authority; `LocalFirst` can answer offline from the replica's known cut.
    /// The first successful serializable read fixes its snapshot for later reads.
    pub async fn all_prepared_at(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
        tier: ReadTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        // Capture before hydration: later writes must not leak into an
        // already admitted read, including when another read is cancelled.
        // Peeks skip the store gate, which would undo the cached overlay.
        let prefix = self
            .db
            .inner
            .staging()
            .batches
            .get(&self.id)
            .ok_or_else(|| rejected("transaction is not open"))?
            .prefix();
        // Except for LocalOnly, a client hydrates a serializable read from
        // the authority at the snapshot first. LocalFirst may answer from
        // the replica when the link cannot answer.
        let snapshot = match self.db.inner.staging().batches.get(&self.id) {
            Some(Batch {
                kind:
                    BatchKind::Serializable {
                        snapshot, pinned, ..
                    },
                ..
            }) => Some((*snapshot, *pinned)),
            _ => None,
        };
        if let (Some(_), Some(flat_join)) = (snapshot, &prepared.query.flat_join) {
            // Its read set would need each occurrence's joined rows.
            let joined = flat_join
                .sources
                .iter()
                .map(|source| format!("`{}`", source.table))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "Reading a flat join of `{}` with {joined} is not supported in serializable \
                     transactions yet",
                    prepared.query.table
                ),
            ));
        }
        if snapshot.is_some() && include_deleted && !prepared.query.array_subqueries.is_empty() {
            // Its read set would need each deleted row's related rows.
            let related = prepared
                .query
                .array_subqueries
                .iter()
                .map(|relation| format!("`{}`", relation.table))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "Reading deleted `{}` rows together with {related} is not supported in \
                     serializable transactions yet",
                    prepared.query.table
                ),
            ));
        }
        if let Some((snapshot, pinned)) = snapshot
            && self.db.is_client()
            && tier != ReadTier::LocalOnly
        {
            let at = pinned.then_some(snapshot);
            self.db.remote_shape(&prepared.query)?;
            let through = self
                .db
                .fetch(
                    prepared,
                    at,
                    include_deleted,
                    if tier == ReadTier::Remote {
                        sync::RemoteReach::REMOTE
                    } else {
                        sync::RemoteReach::BEST_EFFORT
                    },
                    None,
                    None,
                    tier == ReadTier::Remote,
                    &ReadViewSourceSpec::Current,
                )
                .await?;
            if let Some(Batch {
                kind:
                    BatchKind::Serializable {
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
        // Relations nest inside the overlay, so they see staged rows too.
        let rows = self.db.in_overlay_prefix(self.id, prefix, |inner, cut| {
            let read = ReadSpec {
                include_deleted,
                cut,
                ..ReadSpec::default()
            };
            let roots = inner.run(prepared, identity, &read)?;
            inner.nest_relations(prepared, identity, &read, roots)
        })?;
        self.db.record_read(self.id, prepared, include_deleted)?;
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

/// An owned serializable transaction (`Db::serializable_tx`). Reads and writes
/// go through the `TxRef` it derefs to; dropping it uncommitted abandons it.
pub struct SerializableTx<'a> {
    tx: TxRef<'a>,
    committed: bool,
}

impl<'a> std::ops::Deref for SerializableTx<'a> {
    type Target = TxRef<'a>;

    fn deref(&self) -> &TxRef<'a> {
        &self.tx
    }
}

impl SerializableTx<'_> {
    pub async fn commit(mut self) -> Result<TxReceipt, Error> {
        let receipt = self.tx.db.commit_serializable_handle(self.tx.id).await;
        // A failed commit has already taken the batch, so there is nothing
        // left to abandon either way.
        self.committed = true;
        receipt
    }
}

impl Drop for SerializableTx<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.tx.db.abandon_transaction_handle(self.tx.id);
        }
    }
}

impl Db {
    pub async fn serializable_tx(&self) -> Result<SerializableTx<'_>, Error> {
        let id = OpenTransactionId::new();
        self.begin_serializable(id).await?;
        Ok(SerializableTx {
            tx: self.tx_ref(id),
            committed: false,
        })
    }
}

/// The versions, each with the lenses into it, a store passes through to
/// reach `target` along `migrations`: starting at a version it `admitted`
/// (else at a version no lens leads to), ending at `target`. Empty when the
/// store admitted `target` or no such path exists.
/// Why a store shipping the app's `migrations` may not take `schema`: it
/// has not admitted it, and they do not lead to it, so it would name it
/// otherwise than the server does (SPEC 10.7). None for a store shipping
/// none (an authority, a tool reading any version) and for the empty schema.
fn outside_migrations(
    migrations: &Migrations,
    schema: &JazzSchema,
    admitted: impl Fn(&crate::ids::SchemaVersionId) -> bool,
) -> Option<Error> {
    let version = schema.version_id();
    let shipped = migrations.schemas.iter().any(|s| s.version_id() == version)
        || migrations.lenses.iter().any(|lens| lens.target == version);
    if migrations.schemas.is_empty()
        || shipped
        || schema.runtime().tables.is_empty()
        || admitted(&version)
    {
        return None;
    }
    Some(schema_error(format!(
        "schema version {} is not among the app's migrations: regenerate them \
         (`jazz-tools validate`) and pass them to the client",
        uuid::Uuid::from_bytes(*version.as_bytes())
    )))
}

/// The versions to admit, each with the lens it is admitted along, to
/// reach `target` along the app's migrations: every store takes the same
/// path, so each derives the names its authority did (SPEC 10.7).
///
/// Of several lenses into a version, every source is admitted first and
/// the version is named by the lens from the smallest one, as the
/// authority's `Names::resolve` picks it. A version no lens leads into but
/// compatible with one some lens does (the same tables and columns, see
/// `compatibility_id`) is reached from that one by an empty lens, as
/// deployment connects them.
fn migration_path(
    migrations: &Migrations,
    target: &JazzSchema,
    admitted: impl Fn(&crate::ids::SchemaVersionId) -> bool,
) -> Vec<(JazzSchema, Vec<MigrationLens>)> {
    fn walk(
        migrations: &Migrations,
        schema: &JazzSchema,
        admitted: &dyn Fn(&crate::ids::SchemaVersionId) -> bool,
        visiting: &mut BTreeSet<crate::ids::SchemaVersionId>,
    ) -> Option<Vec<(JazzSchema, Vec<MigrationLens>)>> {
        let version = schema.version_id();
        if admitted(&version) {
            return Some(vec![(schema.clone(), Vec::new())]);
        }
        if !visiting.insert(version) {
            return None;
        }
        let path = step(migrations, schema, admitted, visiting);
        visiting.remove(&version);
        path
    }
    fn step(
        migrations: &Migrations,
        schema: &JazzSchema,
        admitted: &dyn Fn(&crate::ids::SchemaVersionId) -> bool,
        visiting: &mut BTreeSet<crate::ids::SchemaVersionId>,
    ) -> Option<Vec<(JazzSchema, Vec<MigrationLens>)>> {
        let version = schema.version_id();
        let source_of = |id: crate::ids::SchemaVersionId| {
            migrations.schemas.iter().find(|s| s.version_id() == id)
        };
        let mut into: Vec<&MigrationLens> = migrations
            .lenses
            .iter()
            .filter(|lens| lens.target == version)
            .collect();
        into.sort_by_key(|lens| lens.source);
        if !into.is_empty() {
            // Every lens's source is admitted first, as on the authority,
            // so this version is named by the lens from the smallest one
            // and refused where the lenses disagree (`Names::resolve`). A
            // lens from a version the app's migrations lack leaves no
            // path: without it this version could be named otherwise.
            let mut path: Vec<(JazzSchema, Vec<MigrationLens>)> = Vec::new();
            for lens in &into {
                let source = source_of(lens.source)?;
                for step in walk(migrations, source, admitted, visiting)? {
                    let version = step.0.version_id();
                    if !path.iter().any(|(s, _)| s.version_id() == version) {
                        path.push(step);
                    }
                }
            }
            path.push((
                schema.clone(),
                into.iter().map(|lens| (*lens).clone()).collect(),
            ));
            return Some(path);
        }
        // No lens leads here: from the smallest compatible version one
        // leads into (or this store admitted), by an empty lens.
        let compatibility = schema.compatibility_id();
        let mut anchors: Vec<&JazzSchema> = migrations
            .schemas
            .iter()
            .filter(|s| s.version_id() != version && s.compatibility_id() == compatibility)
            .filter(|s| {
                let id = s.version_id();
                admitted(&id) || migrations.lenses.iter().any(|lens| lens.target == id)
            })
            .collect();
        anchors.sort_by_key(|s| s.version_id());
        for anchor in anchors {
            let Ok(empty) = MigrationLens::new(anchor.version_id(), version, Vec::new()) else {
                continue;
            };
            if let Some(mut path) = walk(migrations, anchor, admitted, visiting) {
                path.push((schema.clone(), vec![empty]));
                return Some(path);
            }
        }
        // A first version: admitted without a lens.
        Some(vec![(schema.clone(), Vec::new())])
    }
    if admitted(&target.version_id()) {
        return Vec::new();
    }
    let mut visiting = BTreeSet::new();
    match walk(migrations, target, &admitted, &mut visiting) {
        Some(path) if path.len() > 1 || !path[0].1.is_empty() => path,
        _ => Vec::new(),
    }
}
