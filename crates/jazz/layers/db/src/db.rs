//! The `Db` facade: Jazz's public database API over one SQLite connection.

use crate::storage::Storage;
use crate::values::CachedRow;
use futures::future::LocalBoxFuture;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::{Poll, Waker};

use crate::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, RowUuid};
use crate::protocol::{
    BranchSelector, BranchViewBase, LensOp, MigrationLens, ReadViewSourceSpec, ReadViewSpec,
};
use crate::query::Query;
use crate::records::{Value, ValueType};
use crate::schema::{JazzSchema, MergeStrategy};
use crate::storage::Cell as ValueRef;
use crate::storage::OptionalExtension;
use crate::storage::params_from_iter;
use crate::tools::OpenTransactionId;
use crate::tools::Operation;
use crate::tx::DurabilityTier;

pub use crate::options::{
    DbConfig, DbIdentity, DeleteOptions, Error, ErrorCode, ExactWriteTarget, InsertOptions,
    Migrations, ReadOpts, ReadTier, Reason, RestoreOptions, RowIdSource, SeededRowIdSource,
    UpdateOptions, UpsertOptions, WriteIdentity, WriteTarget, block_on,
};

use crate::compile::{Compiled, Ctx, Cut, InsertedRows, Overlay as CutOverlay, Session, View};
use crate::tools::{CmpOp, PolicyExpr};

#[path = "fanout.rs"]
mod fanout;
pub use fanout::FanoutStats;
#[cfg(any(test, feature = "testing"))]
pub use fanout::{
    fail_rechecks_for_test, set_correlation_filter_for_test, set_fanout_oracle_for_test,
    set_policy_pins_for_test,
};
#[path = "large_ops.rs"]
mod large_ops;
pub(crate) use large_ops::byte_splices;
pub use large_ops::{
    JsonSetEdit, LargeValueDeltaSplice, LargeValueStagingPolicy, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, NOT_CHAR_BOUNDARY, StreamingMutationKind,
    StreamingValueUpload,
};
#[path = "sync.rs"]
pub(crate) mod sync;
#[cfg(any(test, feature = "testing"))]
pub use sync::{
    FollowMemoStats, FollowReadHook, follow_memo_stats_for_test, set_follow_memo_oracle_for_test,
    set_follow_read_hook_for_test,
};
pub use sync::{ORIGIN_ENTRY_RETENTION_MS, origin_node};

#[cfg(feature = "e2ee-client")]
#[path = "e2ee_ledger.rs"]
pub(crate) mod e2ee_ledger;
#[path = "e2ee_query.rs"]
mod e2ee_query;
#[path = "e2ee_read.rs"]
mod e2ee_read;
#[path = "e2ee_write.rs"]
mod e2ee_write;
#[path = "shard.rs"]
pub mod shard;
pub use crate::names::LensEffects;
pub use crate::options::REMOTE_LINK_ATTEMPT_WINDOW;
use crate::result_tree::{ResultNode, ResultRelation, ResultTree};
pub use crate::values::CurrentRow;
use crate::values::{
    AuthorKey, STATE_AS_SENT, STATE_DELETED, STATE_NO_CONTENT, Schema, Sql, Table, author_key,
    begin_savepoint, cell_from_sql, column_from_sql, end_savepoint, quote, storage, to_sql,
    unsupported,
};
pub use sync::{
    ACCEPTED, AccessProof, Acting, BuiltOn, CatalogueCell, CatalogueSchema, CatalogueUpdate,
    CatalogueVersion, Cell as SyncCell, CellDelta, CorpusReader, FirstLoad, FrameJoiner, Image,
    MAX_SEQ, Outcome, PENDING, Pos, ReadSpan, RejectedTransaction, RelationSnapshot,
    RemoteLinkHint, SerializedReadResult, Since, SubscriberId, SyncMessage, Transport,
    TransportError, TxName, UnreadableRow, UploadRefusal, UploadStatus, Upstream, WireBase,
    WireChunk, WireChunkRef, WireDelta, WireError, WireKind, WireOp, WirePull, WireTarget,
    WireTransport, WireTransportAdapter, WireWrite, WriteConflict, decode_message, duplex,
    encode_frames, validate_frame_batch_for_corpus,
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
    /// What the encryption hooks keep of a query they rewrote.
    planned: Option<Rc<e2ee_query::Planned>>,
}

impl PreparedQuery {
    fn source_schema<'a>(&'a self, owner: &'a Rc<Schema>) -> &'a Rc<Schema> {
        self.schema.as_ref().unwrap_or(owner)
    }

    /// An unrelated requested subject does not replace the executing author's registry.
    fn fixed_claims_for(&self, author: AuthorSubject) -> Option<&BTreeMap<String, Value>> {
        self.request_claims
            .as_ref()
            .and_then(|(requested, claims)| (*requested == author).then_some(claims))
    }

    /// The same query with a different query text, under a shape of its
    /// own (`tag` names the change), so no plan cached for the original
    /// answers it.
    fn with_query(&self, query: Query, tag: &str) -> PreparedQuery {
        let shape =
            jazz_types::sha256::hash(format!("{:?}{tag}{query:?}", self.shape.0).as_bytes());
        PreparedQuery {
            query,
            shape: Shape(short(shape)),
            ..self.clone()
        }
    }

    /// The same query without its window (limit and offset).
    pub(crate) fn unwindowed(&self) -> PreparedQuery {
        let mut query = self.query.clone();
        query.limit = None;
        query.offset = 0;
        let shape = jazz_types::sha256::hash(format!("{:?}unwindowed", self.shape.0).as_bytes());
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
        let shape = jazz_types::sha256::hash(format!("{query:?}").as_bytes());
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
        let mut hasher = jazz_types::sha256::Hasher::new();
        hasher.update(&self.binding_id.0);
        hasher.update(format!("{author:?}{claims:?}").as_bytes());
        self.binding_id = Binding(short(hasher.finalize()));
        self.request_claims = Some((author, claims));
        self
    }
}

/// How many validated query shapes a db remembers before it starts over.
const VALIDATED_SHAPES_MAX: usize = 4096;

fn short(hash: jazz_types::sha256::Hash) -> [u8; 16] {
    hash.as_bytes()[..16].try_into().expect("16 bytes")
}

/// Whether `query` or one of its relations, nested ones too, reads a
/// window (a `limit` or an `offset`).
fn has_window(query: &Query) -> bool {
    fn arrays(list: &[crate::query::ArraySubquery]) -> bool {
        list.iter()
            .any(|array| array.limit.is_some() || array.offset > 0 || arrays(&array.nested_arrays))
    }
    query.limit.is_some() || query.offset > 0 || arrays(&query.array_subqueries)
}

/// Whether and where a root row falls in `query`'s ordered result
/// depends on that row's own version alone, policies aside: its filters
/// and order name root columns, and nothing reads other rows to admit it.
fn row_local(query: &Query) -> bool {
    use crate::query::{ArraySubqueryRequirement, JoinMode};
    query.joins.is_empty()
        && query.flat_join.is_none()
        && query.policy_branches.is_empty()
        && query.reachable.is_empty()
        && query.inherits.is_empty()
        && query.relation.is_none()
        && query.aggregate.is_none()
        && query
            .includes
            .iter()
            .all(|include| !include.require && include.join_mode == JoinMode::Holes)
        && query
            .array_subqueries
            .iter()
            .all(|array| array.requirement == ArraySubqueryRequirement::Optional)
}

/// Whether `policy` decides a row by that row and the session alone.
fn row_local_policy(policy: &PolicyExpr) -> bool {
    match policy {
        PolicyExpr::And(all) | PolicyExpr::Or(all) => all.iter().all(row_local_policy),
        PolicyExpr::Not(inner) => row_local_policy(inner),
        PolicyExpr::Cmp { .. }
        | PolicyExpr::SessionCmp { .. }
        | PolicyExpr::IsNull { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::IsNotNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::Contains { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::In { .. }
        | PolicyExpr::InList { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::True
        | PolicyExpr::False => true,
        // False outside a write check, so it reads nothing here.
        PolicyExpr::InsertedByThisTransaction => true,
        _ => false,
    }
}

/// Widen the window `[offset, offset + limit)` of a row-local read to hold
/// every row an overlay of `n` staged rows could bring into it (see
/// [`Store::recorded_window`]): each staged row shifts the rest by at most
/// one place, so the window grows by `n` past its end and by up to `n`
/// before its start.
fn widen(limit: &mut Option<usize>, offset: &mut usize, n: usize) {
    let before = (*offset).min(n);
    *offset -= before;
    if let Some(limit) = limit {
        *limit = limit.saturating_add(n).saturating_add(before);
    }
}

/// Result of a standalone write.
#[derive(Clone, Debug)]
pub struct WriteHandle {
    row: RowUuid,
    /// Where the row version this write left is.
    seq: Pos,
    /// The local seq of the commit that wrote it.
    tx: u64,
}

impl WriteHandle {
    pub fn row_uuid(&self) -> RowUuid {
        self.row
    }

    /// The commit sequence number of the row version this write left.
    pub fn mergeable_tx_id(&self) -> u64 {
        self.tx
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

/// What [`Db::write_as_origin`] did.
#[derive(Debug)]
pub enum OriginWrite {
    /// It wrote, as the origin's commit.
    Written(WriteHandle),
    /// The origin's commit was already made here, or is known to the
    /// authority's servers: nothing was written again.
    Repeat,
}

/// What a store knows of a commit made as another writer's.
#[derive(Debug)]
pub(crate) enum OriginFate {
    Unseen,
    /// Made here, its upload not yet decided.
    Pending,
    Accepted,
    Rejected(Error),
}

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
        /// A reset that stands for deltas the reader fell too far behind
        /// to be told one by one (see
        /// [`Db::set_subscription_backlog_limit`]): the current result,
        /// which supersedes them. Always `false` on other deltas.
        coalesced: bool,
        /// The rows the result leaves out because the client's encryption
        /// hooks could not open them (protocol §4.3), replacing the list
        /// the reader was told before: always set on an opening or a reset,
        /// and on a delta when the list changed (a key arrived, or a row
        /// became unreadable). `None`: unchanged. Empty without hooks.
        unreadable: Option<Arc<[UnreadableRow]>>,
    },
    /// The subscription ended with an error: `code` says what kind,
    /// `message` is the error's own message, `reason` the error as text
    /// (with its code).
    Rejected {
        code: ErrorCode,
        message: String,
        reason: String,
    },
    Closed,
}

/// A text merge that ran past its work budget: the write stood as sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeFallback {
    /// The table and column, by the names the merging schema has for
    /// them: on an authority, the stored names (`name~xxxxxx` for one a
    /// later schema version added).
    pub table: String,
    pub row: RowUuid,
    pub column: String,
    /// The longest of the three values merged, in bytes.
    pub bytes: usize,
}

/// The fallbacks a host has not taken yet: the first few, and a count of
/// the rest, so a host that never takes them holds no more.
#[derive(Default)]
struct MergeFallbacks {
    kept: Vec<MergeFallback>,
    dropped: u64,
}

impl MergeFallbacks {
    const KEPT: usize = 64;

    fn record(&mut self, fallback: MergeFallback) {
        if self.kept.len() < Self::KEPT {
            self.kept.push(fallback);
        } else {
            self.dropped += 1;
        }
    }
}

/// What a read observes: deletions and the branch view.
#[derive(Clone, Debug, Default)]
struct ReadSpec {
    include_deleted: bool,
    /// A replica's `ReadTier::Remote` read: its pending local versions
    /// (at `_clock >= 1<<52`) are left out, and a row they changed
    /// reads as its settled version, as the server lists it. Unlike a
    /// [`ReadSpec::cut`] it stays live.
    settled_only: bool,
    view: Option<View>,
    cut: Option<Cut>,
    /// The upstream position the read observes (see
    /// [`Store::visible_read`]).
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
    /// A one-shot read its upstream answered: as far as that answer's
    /// settle (or the connection's, when further), so the rows it fetched
    /// show even before the connection's position reaches them.
    At(sync::Pos),
    /// A live result's: the upstream read (by its key) it is a result of,
    /// as far as its own settle (or the connection's, when further).
    Remote(String),
}

impl ReadSpec {
    async fn of(opts: &ReadOpts, inner: &Store) -> Result<Self, Error> {
        let mut cut = None;
        let view = match &opts.read_view.source {
            ReadViewSourceSpec::Current => None,
            ReadViewSourceSpec::BranchView { head, base } => {
                let (base, base_cut) = match base {
                    None => (None, None),
                    Some(BranchViewBase::Current(base)) => (Some(base.clone()), None),
                    Some(BranchViewBase::Snapshot { branch, snapshot }) => (
                        Some(branch.clone()),
                        Some(inner.resolve_snapshot(snapshot).await?),
                    ),
                };
                Some(View {
                    head: head.clone(),
                    base,
                    base_cut,
                })
            }
            ReadViewSourceSpec::Snapshot { snapshot } => {
                cut = Some(inner.resolve_snapshot(snapshot).await?);
                None
            }
        };
        if opts.tier == ReadTier::Remote {
            inner.check_not_stranded()?;
        }
        Ok(Self {
            include_deleted: opts.read_view.include_deleted,
            settled_only: inner.client().is_some() && opts.tier == ReadTier::Remote,
            view,
            cut,
            position: if inner.client().is_some() {
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
    group: Weak<fanout::GroupSlot>,
    /// The remote read it follows: why the server refused it, once it
    /// has, is reported once and ends the stream.
    refused: Option<Rc<sync::ReadSignal>>,
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
            inner.release_read(sync::ReadCleanup::Subscription {
                query: Box::new(query),
                binding,
                include_deleted,
                view,
                reach,
            });
        }
    }
}

impl SubscriptionStream {
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        let refused = self.refused.as_ref().and_then(|signal| {
            signal
                .refusal()
                .map(|error| (error.code, error.message.clone(), error.to_string()))
        });
        if let Some((code, message, reason)) = refused {
            self.refused = None;
            self.gate = None;
            return Some(SubscriptionEvent::Rejected {
                code,
                message,
                reason,
            });
        }
        if let Some(gate) = &self.gate {
            if !gate.open() {
                return None;
            }
            // The opening lists rows whose large values are complete: while
            // their chunks may still arrive, it waits for them.
            let group = self.group.upgrade()?;
            let group = group.published()?;
            if gate.may_still_wait() && group.awaiting() {
                return None;
            }
            // Opened on an answer, or past its wait: either way it reports
            // what the read was last confirmed as (an outage leaves that
            // standing).
            let settled = gate.signal.confirmed();
            let global = settled && gate.signal.confirmed_globally();
            self.gate = None;
            self.queue.borrow_mut().clear();
            let mut event = fanout::opening(&group.rows, &group.unreadable);
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
        let mut queue = self.queue.borrow_mut();
        if !queue.reset_due() {
            return queue.pop_front(None);
        }
        // A refresh can suspend with the group borrowed. Leave the reset due
        // until its complete row list is available.
        let group = self.group.upgrade();
        let group = group.as_ref().and_then(|group| group.published());
        queue.pop_front(
            group
                .as_ref()
                .map(|group| (group.rows.as_slice(), &group.unreadable)),
        )
    }

    /// Changed rows its undelivered deltas hold: at most its backlog limit
    /// ([`Db::set_subscription_backlog_limit`]), past which they coalesce.
    pub fn backlog_rows(&self) -> usize {
        self.queue.borrow().backlog_rows()
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

/// What a store holds open between calls ([`Db::held_for_test`]).
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Held {
    /// A transaction handle's staged writes are applied, in a SQLite
    /// savepoint, for its next stage.
    pub overlay: bool,
    /// Transaction handles that are open (staged, not committed or rolled
    /// back).
    pub open_batches: usize,
    /// The connection is inside a SQLite transaction or savepoint.
    pub sqlite_transaction: bool,
}

#[cfg(any(test, feature = "testing"))]
impl Held {
    /// Nothing is held.
    pub fn is_nothing(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static DERIVED_BINDS: Cell<usize> = const { Cell::new(0) };
}

/// How many cells this thread bound from a descriptor its own edit derived
/// for the write in flight ([`Store::derived`]), trusted as built.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub fn derived_binds_for_test() -> usize {
    DERIVED_BINDS.with(Cell::get)
}

/// Test-visible runtime counters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
}

// Data used by both database handles and the SQL execution path.

impl Store {
    #[track_caller]
    fn client(&self) -> Option<std::cell::Ref<'_, sync::Client>> {
        std::cell::Ref::filter_map(self.client.borrow(), Option::as_ref).ok()
    }

    #[track_caller]
    fn client_mut(&self) -> Option<std::cell::RefMut<'_, sync::Client>> {
        std::cell::RefMut::filter_map(self.client.borrow_mut(), Option::as_mut).ok()
    }

    fn names(&self) -> Rc<crate::names::Names> {
        self.names.borrow().clone()
    }
    fn admitted(&self) -> Rc<Vec<(crate::ids::SchemaVersionId, JazzSchema)>> {
        self.admitted.borrow().clone()
    }
    fn owner(&self) -> Rc<Schema> {
        self.owner.borrow().clone()
    }
    fn physical(&self) -> Rc<Schema> {
        self.physical.borrow().clone()
    }
}

/// Whether a read decodes its rows' projected cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decode {
    Cells,
    Nothing,
}

/// Large-value nodes one tick may reclaim.
const RECLAIM_NODES_PER_TICK: usize = 256;

/// Leaves one tick may move to, or delete from, the chunk store.
const CHUNK_STORE_NODES_PER_TICK: usize = 256;

/// [`Db::take_chunk_io`] or [`Db::finish_chunk_io`] on a store whose tick
/// runs its chunk I/O itself.
fn not_host_run() -> Error {
    Error::new(
        ErrorCode::Protocol,
        "chunk I/O: this store runs its chunk I/O itself; open it with DbConfig::with_host_chunk_io to run it from the host",
    )
}

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
    /// builds register theirs (IndexedDB) before opening.
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
    /// Rollback journal, for VFSes without shared memory. The page
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

/// A callback can poll multiple writes on its scoped handle while the outer
/// operation holds the store lease. Serialize those writes with each other.
struct OriginWriteContext {
    key: Cell<Option<([u8; 16], u64)>>,
    operation: futures::lock::Mutex<()>,
}

/// Jazz's `Db` over SQLite.
pub struct Db {
    inner: Rc<Store>,
    /// `Some` for a schema-view handle: operations run under this schema.
    view: Option<Rc<Schema>>,
    /// On a host-run chunk store: a read that waits on offloaded nodes
    /// returns the missing-chunks error at once, leaving them wanted for
    /// the host's fetch lane, instead of fetching them on this thread
    /// ([`Self::deferring_chunk_fetches`]).
    defer_fetches: bool,
    /// Only the temporary handle passed to an origin write owns its attribution.
    origin: Option<OriginWriteContext>,
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
/// A zero wait is `FirstLoad::Deliver`: no wait at all.
pub(crate) fn first_load_wait(tier: ReadTier, first_load: &FirstLoad) -> Option<u64> {
    match (tier, first_load) {
        (ReadTier::LocalFirst, FirstLoad::WaitForRemote { timeout_ms }) if *timeout_ms > 0 => {
            Some(*timeout_ms)
        }
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

/// Rows whose parent cells one batched statement reads
/// ([`Store::relation_parents`]): their ids bind as one JSON array of
/// about 35 bytes each, well under Durable Object SQLite's 2 MiB string.
const BATCH_IDS: usize = 4096;

/// At most how many bytes of parent values one batched relation statement
/// binds ([`Store::relation_level`]), as one JSON array: well under
/// Durable Object SQLite's 2 MiB string.
const BATCH_BYTES: usize = 256 * 1024;

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static BATCH_LIMITS: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((BATCH_IDS, BATCH_BYTES)) };
}

/// [`BATCH_IDS`] and [`BATCH_BYTES`], or smaller in tests that cross them.
fn batch_limits() -> (usize, usize) {
    #[cfg(any(test, feature = "testing"))]
    {
        BATCH_LIMITS.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        (BATCH_IDS, BATCH_BYTES)
    }
}

/// Tests only: split batched relation reads at `ids` rows per parent-cell
/// read and `bytes` of bound parent values per children read, on this
/// thread; `None` restores the defaults.
#[cfg(any(test, feature = "testing"))]
pub fn set_relation_batch_limits_for_test(limits: Option<(usize, usize)>) {
    BATCH_LIMITS.with(|cell| cell.set(limits.unwrap_or((BATCH_IDS, BATCH_BYTES))));
}

/// The length of `value` as an item of [`crate::compile::json_list`]'s
/// array; `None` when it has no exact JSON form.
fn json_item_len(value: &Sql) -> Option<usize> {
    match value {
        Sql::Null => Some(4),
        Sql::Blob(bytes) => Some(2 * bytes.len() + 2),
        Sql::Integer(n) => Some(n.to_string().len()),
        Sql::Text(text) => serde_json::to_string(std::str::from_utf8(text).ok()?)
            .ok()
            .map(|json| json.len()),
        Sql::Real(_) => None,
    }
}

/// One read's nesting of its relations (`Store::node`), shared by every row
/// it nests: one read, as one identity, through one view and cut. Its
/// relations' rows depend only on the relation and the parent value, so
/// the run builds each relation's SQL (and each parent-cell read) once and
/// rebinds only the parent, and reads each (relation, parent value) once
/// where values repeat (relations keyed by a parent's column).
///
/// It lives for one call and dies with it, so nothing it holds outlives the
/// read: relations are keyed by address, valid while the run borrows the
/// query that owns them.
struct RelationRun<'q> {
    namespace: Option<[u8; 16]>,
    owner: Rc<Schema>,
    prepared: &'q PreparedQuery,
    read: &'q ReadSpec,
    sessions: (Option<Session>, Session),
    /// Whether to reuse at all (always, outside tests that compare with the
    /// uncached path).
    reuse: bool,
    /// Per relation and parent shape (an array of references or a scalar):
    /// its compiled children and the parameter holding the parent.
    sql: HashMap<(*const crate::query::ArraySubquery, bool), (Compiled, usize)>,
    /// Per relation: the read of its parent cell.
    cells: HashMap<*const crate::query::ArraySubquery, crate::compile::CellRead>,
    /// Per relation keyed by a parent's column, parent shape and bound
    /// parent value: its children.
    children:
        HashMap<(*const crate::query::ArraySubquery, bool, Option<ParentKey>), Vec<ResultNode>>,
    /// Whether to nest a level's rows with one statement per relation
    /// ([`Store::nest_level`]) rather than per row (always, outside tests
    /// that compare the two).
    /// Each relation is then read once per run, so there is nothing to
    /// cache for it.
    batch: bool,
    /// A live result's relation statements, kept across its runs
    /// ([`Store::nest_relations_kept`]); `None` for any other read.
    kept: Option<&'q mut RelationPlans>,
    /// The relation columns from the root down to the level being nested:
    /// with the relation's own column, what names its kept statement.
    path: Vec<String>,
}

/// A live result's compiled relation statements, kept across its runs: a
/// re-run (or a later chunk of parents) rebinds only the parents' list
/// instead of compiling the relation again. Keyed by the relation's path
/// from the root (one result's query names one relation per path), its
/// parents' shape and whether they bind as blobs; `None` where the
/// children are too wide to batch.
///
/// A statement is compiled from the schema, the reader's sessions, the
/// relation and the store's enum tags; it is kept only while all of them
/// are as they were ([`PlanStamp`]).
#[derive(Default)]
pub(crate) struct RelationPlans {
    stamp: Option<PlanStamp>,
    by: HashMap<(String, bool, bool), Option<(Compiled, usize)>>,
    /// Statements reused (for [`FanoutStats::kept_plans`]).
    pub(crate) hits: u64,
}

impl RelationPlans {
    /// Drop every kept statement (the result's schema or claims changed).
    pub(crate) fn clear(&mut self) {
        self.stamp = None;
        self.by.clear();
    }
}

/// What a kept statement was compiled under. The schema is held, not just
/// its address, so a schema built later cannot pass for it.
pub(crate) struct PlanStamp {
    namespace: Option<[u8; 16]>,
    schema: Rc<Schema>,
    sessions: (Option<Session>, Session),
    withheld: bool,
    settled_only: bool,
    include_deleted: bool,
    history_origin: Option<[u8; 16]>,
    tags: u64,
}

/// What a statement compiled now would be compiled under
/// ([`Store::plan_inputs`]): compared with a kept [`PlanStamp`] where it
/// stands, and copied into one only when they differ.
pub(crate) struct PlanInputs<'a> {
    namespace: Option<[u8; 16]>,
    schema: Rc<Schema>,
    sessions: &'a (Option<Session>, Session),
    withheld: bool,
    settled_only: bool,
    include_deleted: bool,
    history_origin: Option<[u8; 16]>,
    tags: u64,
}

impl PlanStamp {
    fn holds(&self, now: &PlanInputs<'_>) -> bool {
        Rc::ptr_eq(&self.schema, &now.schema)
            && self.namespace == now.namespace
            && same_sessions(&self.sessions, now.sessions)
            && self.withheld == now.withheld
            && self.settled_only == now.settled_only
            && self.include_deleted == now.include_deleted
            && self.history_origin == now.history_origin
            && self.tags == now.tags
    }

    /// Whether the statements kept under `kept` are still what `now`
    /// compiles. If not, `kept` becomes `now`'s stamp and the caller drops
    /// its statements.
    pub(crate) fn keep(kept: &mut Option<PlanStamp>, now: &PlanInputs<'_>) -> bool {
        if kept.as_ref().is_some_and(|kept| kept.holds(now)) {
            return true;
        }
        *kept = Some(PlanStamp {
            namespace: now.namespace,
            schema: now.schema.clone(),
            sessions: now.sessions.clone(),
            withheld: now.withheld,
            settled_only: now.settled_only,
            include_deleted: now.include_deleted,
            history_origin: now.history_origin,
            tags: now.tags,
        });
        false
    }
}

/// Whether two reads authorize as the same identity with the same claims:
/// what a compiled read binds of its sessions.
fn same_sessions(a: &(Option<Session>, Session), b: &(Option<Session>, Session)) -> bool {
    let same = |a: &Session, b: &Session| {
        a.system == b.system
            && a.issuer == b.issuer
            && a.subject == b.subject
            && a.account == b.account
            && a.claims == b.claims
    };
    match (&a.0, &b.0) {
        (None, None) => {}
        (Some(x), Some(y)) if same(x, y) => {}
        _ => return false,
    }
    same(&a.1, &b.1)
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static PLAN_ORACLE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether every kept statement is compared with one compiled afresh.
pub(crate) fn plan_oracle() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        PLAN_ORACLE.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        false
    }
}

/// Tests only: compare every kept statement (a relation's or a re-check's)
/// with one compiled afresh, on this thread, and panic where they differ.
#[cfg(any(test, feature = "testing"))]
pub fn set_plan_oracle_for_test(on: bool) {
    PLAN_ORACLE.with(|cell| cell.set(on));
}

/// Whether two compiles of one read are the same statement.
pub(crate) fn same_compiled(a: &Compiled, b: &Compiled) -> bool {
    a.sql == b.sql
        && a.params == b.params
        && a.tables == b.tables
        && a.aggregate == b.aggregate
        && a.joined == b.joined
        && a.versioned == b.versioned
        && a.union == b.union
        && a.writer == b.writer
        && a.partial == b.partial
        && a.projection.len() == b.projection.len()
        && a.projection.iter().zip(&b.projection).all(|(x, y)| {
            x.0 == y.0
                && x.1 == y.1
                // An enum cell's tags are the store's own entry for the
                // stored column (`TagRegistry::column`), which every
                // compile under one schema hands out: the same column is
                // the same `Rc`, and equal tags in another one would be
                // another column's.
                && match (&x.2, &y.2) {
                    (None, None) => true,
                    (Some(p), Some(q)) => Rc::ptr_eq(p, q),
                    _ => false,
                }
        })
}

impl<'q> RelationRun<'q> {
    fn new(
        inner: &Store,
        prepared: &'q PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &'q ReadSpec,
        namespace: Option<[u8; 16]>,
    ) -> Self {
        Self {
            namespace,
            prepared,
            read,
            owner: inner.owner(),
            sessions: inner.read_sessions(prepared, identity),
            reuse: relation_reuse(),
            sql: HashMap::new(),
            cells: HashMap::new(),
            children: HashMap::new(),
            batch: relation_batch(),
            kept: None,
            path: Vec::new(),
        }
    }

    fn ctx<'a>(&'a self, inner: &'a Store) -> Ctx<'a> {
        let (policy, claims) = &self.sessions;
        inner.read_ctx(
            &self.owner,
            self.prepared,
            policy.as_ref(),
            claims,
            self.read,
            self.namespace,
        )
    }
}

/// A bound parent value as a map key: equal keys bind equal SQL values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ParentKey {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ParentKey {
    fn of(value: &Sql) -> Self {
        match value {
            Sql::Null => Self::Null,
            Sql::Integer(v) => Self::Integer(*v),
            Sql::Real(v) => Self::Real(v.to_bits()),
            Sql::Text(v) => Self::Text(v.clone()),
            Sql::Blob(v) => Self::Blob(v.clone()),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static RELATION_REUSE: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Whether relation nesting reuses SQL and rows within a read.
fn relation_reuse() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        RELATION_REUSE.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        true
    }
}

/// Tests only: nest relations without reusing SQL or rows within a read
/// (`false`), on this thread, to compare with the reusing path.
#[cfg(any(test, feature = "testing"))]
pub fn set_relation_reuse_for_test(reuse: bool) {
    RELATION_REUSE.with(|cell| cell.set(reuse));
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static RELATION_BATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Whether relation nesting reads a level's rows with one statement per
/// relation.
fn relation_batch() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        RELATION_BATCH.with(std::cell::Cell::get)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        true
    }
}

/// Tests only: nest relations one parent row at a time (`false`), on this
/// thread, to compare with the batched path.
#[cfg(any(test, feature = "testing"))]
pub fn set_relation_batch_for_test(batch: bool) {
    RELATION_BATCH.with(|cell| cell.set(batch));
}

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static VM_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Tests only: the SQLite virtual-machine steps this thread's query
/// statements have run since the last call, then zero. Steps grow with the
/// rows a statement visits, so a test can bound a read's work by the rows
/// it returns rather than by the rows the store holds.
#[cfg(any(test, feature = "testing"))]
pub fn take_vm_steps_for_test() -> u64 {
    VM_STEPS.with(|cell| cell.replace(0))
}

/// The application tables a store holds, by their stored columns.
async fn stored_schema(
    conn: &Storage,
    jazz: &JazzSchema,
    names: &BTreeSet<String>,
) -> Result<Schema, Error> {
    let mut tables = BTreeMap::new();
    for name in names {
        let columns: Vec<String> = (async {
            let mut statement = conn.prepare(&format!("PRAGMA table_info({})", quote(name)))?;
            {
                statement
                    .query_map([], |row| row.get(1))
                    .await?
                    .collect::<_>()
                    .await
            }
        })
        .await
        .map_err(storage)?;
        if columns.is_empty() {
            return Err(Error::new(
                ErrorCode::Storage,
                format!("Jazz catalogue references missing table {name}"),
            ));
        }
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
        let large = crate::large::stored_large_columns(conn, name).await?;
        for column in &mut table.columns {
            if large.contains(&column.stored) {
                column.ty = ValueType::Nullable(Box::new(ValueType::String));
            }
        }
        tables.insert(name.clone(), table);
    }
    Ok(Schema {
        tables,
        jazz: jazz.clone(),
        version: jazz.version_id(),
        policies_from: None,
        bundle: None,
        stored_names: false,
    })
}

/// Each column of stored table `table` that its DDL gives a default, as
/// the default's SQL text.
async fn ddl_defaults(conn: &Storage, table: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({})", quote(table)))
        .map_err(storage)?;
    let mut rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, Option<String>>(4)?))
        })
        .await
        .map_err(storage)?;
    let mut defaults = BTreeMap::new();
    while let Some(row) = rows.next().await {
        if let (column, Some(default)) = row.map_err(storage)? {
            defaults.insert(column, default);
        }
    }
    Ok(defaults)
}

/// The value of a DDL default's SQL text. It is this store's own DDL, as
/// `Schema::union` and `Table::ddl` wrote it: a literal.
async fn ddl_literal(conn: &Storage, literal: &str) -> Result<Sql, Error> {
    conn.query_row(&format!("SELECT {literal}"), [], |row| row.get::<_, Sql>(0))
        .await
        .map_err(storage)
}

/// Whether an upstream's default `value` can be column `column`'s: of its
/// type, or, for an enum column (or a nullable one, or an array of them),
/// its variants' names. Never a large value: it would name the upstream's
/// chunks.
fn default_fits(column: &crate::values::Column, value: &Value) -> bool {
    fn names_variants(variants: &[String], ty: &ValueType, value: &Value) -> bool {
        match (ty, value) {
            (ValueType::Nullable(_), Value::Nullable(None)) => true,
            (ValueType::Nullable(inner), Value::Nullable(Some(value))) => {
                names_variants(variants, inner, value)
            }
            (ValueType::Array(inner), Value::Array(items)) => items
                .iter()
                .all(|item| names_variants(variants, inner, item)),
            (ValueType::Nullable(_) | ValueType::Array(_), _) => false,
            (_, Value::String(name)) => variants.iter().any(|variant| variant == name),
            _ => false,
        }
    }
    fn large(value: &Value) -> bool {
        match value {
            Value::Large(_) => true,
            Value::Nullable(Some(value)) => large(value),
            Value::Array(items) | Value::Tuple(items) => items.iter().any(large),
            _ => false,
        }
    }
    if large(value) {
        return false;
    }
    match &column.variants {
        Some(variants) => names_variants(variants, &column.ty, value),
        None => value.check_type(&column.ty).is_ok(),
    }
}

/// Run a script of statements one at a time. SQLite's statement length
/// limit applies to the whole text handed to one prepare, and schema DDL
/// outgrows Durable Object SQLite's 100 KB.
pub(crate) async fn execute_script(conn: &Storage, script: &str) -> Result<(), Error> {
    // One transaction for the whole script: outside one, every statement
    // commits (and syncs the file) alone, and a schema of a few hundred
    // tables then paid a durable commit per table, index and trigger when
    // its store opened. It also leaves no half-created schema behind.
    conn.scope("jazz_ddl", async |conn| {
        if script.contains('\0') {
            return Err(Error::new(ErrorCode::Storage, "NUL in SQL"));
        }
        #[cfg(feature = "sqlite")]
        if !conn.is_managed() {
            let mut start = 0;
            for (end, _) in script.match_indices(';') {
                let statement = &script[start..=end];
                let text = std::ffi::CString::new(statement)
                    .map_err(|_| Error::new(ErrorCode::Storage, "NUL in SQL"))?;
                // SAFETY: a NUL-terminated string; `sqlite3_complete` only reads
                // it. A `;` inside a trigger body or a literal leaves it incomplete.
                if unsafe { rusqlite::ffi::sqlite3_complete(text.as_ptr()) } != 0 {
                    conn.execute_batch(statement).await.map_err(storage)?;
                    start = end + 1;
                }
            }
            let rest = &script[start..];
            if !rest.trim().is_empty() {
                conn.execute_batch(rest).await.map_err(storage)?;
            }
            return Ok(());
        }
        conn.execute_batch(script).await.map_err(storage)
    })
    .await
}

pub(crate) fn rejected(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::WriteRejected, message)
}

/// A write rejected for `reason`.
fn rejected_for(reason: Reason, message: impl Into<String>) -> Error {
    Error::with_reason(ErrorCode::WriteRejected, reason, message)
}

/// What Jazz's account registry tells of an account: the checks beside the
/// account tables' policies need its founder (E2EE protocol R5, R6), which
/// no policy can read.
pub trait AccountRegistryView {
    /// Whether `account` was founded by a device-account identity; `None`
    /// when the registry holds no identity of it. Read on the store's own
    /// connection inside the write's transaction. A second connection to
    /// a shared in-memory database is locked out while the store writes.
    fn founded_by_device_account<'a>(
        &'a self,
        store: &'a Storage,
        account: jazz_types::account_registry::AccountId,
    ) -> LocalBoxFuture<'a, Result<Option<bool>, String>>;

    /// Whether `account` was founded by a device-account identity for another
    /// audience than the app's own, whose accounts an encrypting app does
    /// not serve: with no record of which namespace founded it, such an
    /// account's kind (E2EE protocol R6) and reset rights (R5) cannot be
    /// told, so no account record is accepted for it. False by default.
    /// Reads on the same connection as [`Self::founded_by_device_account`].
    fn founded_for_another_audience<'a>(
        &'a self,
        _store: &'a Storage,
        _account: jazz_types::account_registry::AccountId,
    ) -> LocalBoxFuture<'a, Result<bool, String>> {
        Box::pin(async { Ok(false) })
    }
}

/// A write that would change or remove a row of a system table, which no
/// writer may do (`verb` is "updated" or "deleted").
fn system_row_immutable(table: &str, verb: &str) -> Error {
    rejected(format!(
        "rows of `{table}` are never {verb}: it is one of Jazz's system tables"
    ))
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

/// [`Store::as_sent`]: per row version (stored table, row, branch key) and
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

/// How far past its own clock a store counts an HLC it received (see
/// [`Store::observe_hlc`]): the shard offset bound.
pub const HLC_SKEW_MS: u64 = shard::HLC_MAX_OFFSET_MS;

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

/// A denial the commit's grounding found, which says so: each write passed
/// its check when it was staged.
fn refused_at_commit(mut error: Error) -> Error {
    if error.is_authorization_denied() {
        error
            .message
            .push_str(", on the state the whole transaction leaves behind");
    }
    error
}

/// A session's update or upsert of a row it may not read, or that does not
/// exist: the two read alike, so the answer reveals neither.
fn unreadable_target(verb: &str, table: &str) -> Error {
    denied(format_args!(
        "read policy denied {verb} on table {table}: the row does not exist or the session \
         may not read it"
    ))
}

/// A session's delete its DELETE policy refuses, or of a row that does not
/// exist: the two read alike, so the answer reveals neither.
fn delete_denied() -> Error {
    denied("DELETE policy denied the target row, or it does not exist")
}

impl Error {
    /// Whether a policy rejected the write.
    pub fn is_authorization_denied(&self) -> bool {
        self.reason == Some(Reason::AuthorizationDenied)
    }
}

impl Db {
    /// Open an in-memory database on `config`.
    ///
    /// This is the low-level open that servers, relays and tests use, and it
    /// does not refuse an app whose schema holds Jazz's system tables (an
    /// encrypted app): a server stores and serves encrypted cells as they
    /// are. Only the client open paths refuse such a schema until the
    /// client's encryption hooks are installed
    /// ([`crate::binding_host::refuse_encrypted_app`]); the same holds for
    /// the other `open_*` constructors.
    #[cfg(feature = "sqlite")]
    pub async fn open(config: DbConfig) -> Result<Self, Error> {
        #[cfg(not(feature = "managed-storage-emulation"))]
        let conn = Storage::open_in_memory().map_err(storage)?;
        #[cfg(feature = "managed-storage-emulation")]
        let conn = Storage::managed_emulation_in_memory().map_err(storage)?;
        Self::open_storage(config, conn).await
    }

    /// Open on `storage` (a host's SQL: [`Storage::from_backend`]), with
    /// no file: chunks live in the store itself.
    pub async fn open_storage(config: DbConfig, storage: Storage) -> Result<Self, Error> {
        Self::open_on(config, storage, None).await
    }

    /// A durable store in one SQLite file (WAL unless `options.journal`
    /// says otherwise). Reopening restores rows, history, the commit seq
    /// and, for a replica, its pending uploads and history baselines; they are resent
    /// on the next connection.
    #[cfg(feature = "sqlite")]
    pub async fn open_at(
        config: DbConfig,
        path: &std::path::Path,
        options: FileOptions,
    ) -> Result<Self, Error> {
        Self::open_file(config, crate::host::StoreFile::open(path, options).await?).await
    }

    /// Open on a file [`crate::host::StoreFile::open`] admitted, whose host
    /// tables may already have been read or written.
    pub async fn open_file(config: DbConfig, file: crate::host::StoreFile) -> Result<Self, Error> {
        let crate::host::StoreFile { conn, path, .. } = file;
        Self::open_on(
            config,
            conn,
            path.as_ref().map(|(path, vfs)| (path.as_path(), *vfs)),
        )
        .await
    }

    /// Initialize the real store and publish its first catalogue atomically.
    /// Failure leaves existing host and application tables unchanged; the
    /// returned database is usable only after the outer scope commits.
    pub async fn open_file_with_catalogue(
        config: DbConfig,
        file: crate::host::StoreFile,
        change: crate::catalogue::CatalogueChange,
    ) -> Result<Self, Error> {
        Self::open_file_with_catalogue_checked(config, file, change, &[], async |_| Ok(())).await
    }

    /// Initialize and publish the first catalogue together with its host-table
    /// values, checking the admitted catalogue before committing.
    pub async fn open_file_with_catalogue_checked(
        config: DbConfig,
        file: crate::host::StoreFile,
        change: crate::catalogue::CatalogueChange,
        kept: &[(crate::host::HostTable, Vec<u8>, Vec<u8>)],
        check: impl AsyncFnOnce(&Db) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        let crate::host::StoreFile { conn, path, .. } = file;
        let conn = Rc::new(conn);
        conn.db_scope("jazz_initialize", async || {
            crate::format::admit(&conn).await?;
            let db = Self::open_initialized(
                config,
                conn.clone(),
                path.as_ref().map(|(path, vfs)| (path.as_path(), *vfs)),
            )
            .await?;
            db.apply_catalogue_checked(change, kept, check).await?;
            Ok(db)
        })
        .await
    }

    /// Run `f` on the store's connection, outside any transaction handle's
    /// overlay (see [`crate::host`]).
    pub(crate) async fn with_host_connection<T>(
        &self,
        f: impl AsyncFnOnce(&crate::storage::Storage) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let _operation = self.write_operation().await;
        let conn = self.inner.without_overlay().conn.clone();
        f(&conn).await
    }

    async fn open_on(
        config: DbConfig,
        conn: crate::storage::Storage,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        let conn = Rc::new(conn);
        conn.db_scope("jazz_initialize", async || {
            crate::format::admit(&conn).await?;
            Self::open_initialized(config, conn.clone(), path).await
        })
        .await
    }

    async fn open_initialized(
        mut config: DbConfig,
        conn: Rc<crate::storage::Storage>,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        #[cfg(feature = "e2ee-client")]
        if let Some(hooks) = &config.e2ee {
            crate::e2ee::refuse_stored_defaults(&config.schema, hooks.declarations())?;
        }
        // Before the first statement is prepared: the flag is read at
        // prepare time.
        crate::host::stable_plans(&conn)?;
        // A schema this store has not admitted is reached along the lenses
        // the app ships: from a version it admitted, else from the first
        // version, each admitted as the authority admits it.
        let names = crate::names::Names::load(&conn).await?;
        let admitted = |version: &crate::ids::SchemaVersionId| names.get(version).is_some();
        let steps = migration_path(&config.migrations, &config.schema, admitted);
        let Some((first, _)) = steps.first() else {
            if let Some(error) = outside_migrations(&config.migrations, &config.schema, admitted) {
                return Err(error);
            }
            return Self::open_named(config, conn, path).await;
        };
        let target = std::mem::replace(&mut config.schema, first.clone());
        let db = Self::open_named(config, conn, path).await?;
        // Installing the schema: admitted along the lenses, checked against
        // the one the store opened under, and its declaration order made
        // tag order, all or nothing.
        begin_savepoint(&db.inner.without_overlay().conn, "jazz_install")?;
        let installed: Result<_, Error> = (async {
            let _ = db.admit_along_migrations(&target).await?;
            {
                let inner = db.inner.without_overlay();
                let owner = Rc::new(inner.named(&target));
                for table in owner.tables.values() {
                    if let Some(current) = inner.owner().by_stored(&table.stored) {
                        check_branch_removal(current, table)?;
                    }
                }
                inner.align_tags(&owner).await?;
                Ok(owner)
            }
        })
        .await;
        end_savepoint(
            &db.inner.without_overlay().conn,
            "jazz_install",
            installed.is_ok(),
        )?;
        let owner = installed?;
        {
            let inner = db.inner.without_overlay();
            inner.owner_version.set(target.version_id());
            inner.owner.replace(owner);
            inner.reset_everything().await;
            // Pending writes upload under the version they were written
            // in, and replay by the names of the one now current.
            inner.pending_revision_base.set(
                inner.pending_revision_base.get()
                    + inner.client().map_or(0, |client| client.pending_revision())
                    + 1,
            );
            inner.client.replace(None);
            inner.load_client().await?;
            db.inner
                .pending
                .replace(inner.client().map(|client| client.pending.clone()));
            db.inner.publish_schema();
        }
        Ok(db)
    }

    /// Admit `schema` along the app's lenses when this store lacks it and
    /// they lead to it from a version it admitted or from the first one
    /// (SPEC 10.7); whether they did.
    async fn admit_along_migrations(&self, schema: &JazzSchema) -> Result<bool, Error> {
        let steps = {
            let inner = self.inner.without_overlay();
            migration_path(&self.inner.migrations.borrow(), schema, |version| {
                inner.names().get(version).is_some()
            })
        };
        let along = !steps.is_empty();
        if !along {
            let inner = self.inner.without_overlay();
            if let Some(error) =
                outside_migrations(&self.inner.migrations.borrow(), schema, |version| {
                    inner.names().get(version).is_some()
                })
            {
                return Err(error);
            }
        }
        for (schema, lenses) in steps {
            if self.inner.names().get(&schema.version_id()).is_none() {
                self.admit(schema, &lenses).await?;
            }
        }
        Ok(along)
    }

    async fn open_named(
        config: DbConfig,
        conn: Rc<crate::storage::Storage>,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        // Before the first statement, so both see all of them.
        #[cfg(feature = "durable-object-limits")]
        if let Some(native) = conn.native_conn() {
            crate::durable_object_limits::apply(native);
        }
        #[cfg(feature = "sql-trace")]
        if let Some(native) = conn.native_conn() {
            crate::sql_trace::install(native);
        }
        // A version this file admitted keeps its names; another one is
        // named against the most recently admitted.
        let mut names = crate::names::Names::load(&conn).await?;
        // The newest version (one new to this file, or the one it admitted
        // last) is current from now on; an older one (a server reopening
        // under its genesis schema, or an older client) leaves the tags as
        // they are.
        let version = config.schema.version_id();
        let newest = names.get(&version).is_none() || names.latest_version() == Some(version);
        // Every version an earlier process admitted is admitted again, with
        // the names it was stored under.
        let mut admitted = crate::catalogue::admitted(&conn).await?;
        let schema = names.resolve(&config.schema, &[], names.latest())?;
        names.check_branches(&schema)?;
        names.check_types(&schema)?;
        // A version new to this file without lenses is named against the
        // latest admitted one. It may lack a branch column (an older client
        // reads it at its default), but may not give one another type: that
        // would be a new stored column, and the rows are keyed by the old.
        if names.get(&version).is_none()
            && let Some((_, latest)) = names
                .latest_version()
                .and_then(|latest| admitted.iter().find(|(id, _)| *id == latest))
        {
            let before = names.resolve(latest, &[], names.latest())?;
            for table in schema.tables.values() {
                if let Some(current) = before.by_stored(&table.stored) {
                    check_branch_retype(current, table)?;
                }
            }
        }
        // Variants are tagged before any DDL: an enum default is its tag.
        names.mint(&conn, &schema).await?;
        let chunks = crate::large::Chunks::install_reading(
            path,
            conn.shared_reader(),
            config.chunk_store.clone(),
            config.host_chunk_io,
        )?;
        // Statements are keyed per table and verb; the default 16 entries
        // thrash as soon as a schema has a few tables.
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        conn.execute_batch(crate::large::DDL)
            .await
            .map_err(storage)?;
        crate::large::open_offload(&conn, &chunks).await?;
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
        let owned_tables = names.stored_tables();
        let fresh = owned_tables.is_empty();
        // A reopened store keeps every column earlier schemas added; bring
        // it up to this schema without touching what is already there.
        let physical = if fresh {
            execute_script(&conn, &schema.ddl()).await?;
            let mut physical = Schema {
                tables: BTreeMap::new(),
                jazz: config.schema.clone(),
                version: config.schema.version_id(),
                policies_from: None,
                bundle: None,
                stored_names: false,
            };
            physical.union(&schema)?;
            physical
        } else {
            // SQLite knows a stored column by name only. Every version the
            // file admitted gives the columns it stores their types, in
            // admission order and then the opening schema, as the live
            // store's union grew: the opening schema is not always the
            // newest (a server core reopens under its genesis while later
            // versions' tables and columns take writes, #4769).
            let mut physical = stored_schema(&conn, &config.schema, &owned_tables).await?;
            let mut ddl = String::new();
            for (version, earlier) in &admitted {
                if *version == config.schema.version_id() {
                    continue;
                }
                if let Some(stored) = names.get(version) {
                    ddl.push_str(&physical.union(&Schema::stored_as(
                        earlier,
                        stored,
                        names.tags(),
                    ))?);
                }
            }
            ddl.push_str(&physical.union(&schema)?);
            execute_script(&conn, &ddl).await?;
            physical
        };
        names.admit(&conn, &schema).await?;
        conn.execute_batch(sync::PENDING_DDL)
            .await
            .map_err(storage)?;
        conn.execute_batch(sync::SYNC_DDL).await.map_err(storage)?;
        let withheld: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_jazz_withheld'",
                [],
                |r| r.get(0),
            )
            .await.map_err(storage)?;
        // The commit seq resumes after the newest version the store
        // numbered as the authority (a replica's own counter resumes in
        // `load_client`).
        let mut seq = 0u64;
        for table in physical.tables.keys() {
            for name in [table.clone(), format!("{table}__h")] {
                let max: Option<i64> = conn
                    .cached_row(
                        &format!(
                            "SELECT MAX(_clock) FROM {} WHERE _clock >= 0 AND _clock < ?1",
                            quote(&name)
                        ),
                        [sync::band_start(sync::PENDING)],
                        |r| r.get(0),
                    )
                    .await
                    .map_err(storage)?;
                seq = seq.max(max.unwrap_or(0) as u64);
            }
        }
        // An accepted upload that wrote no rows still took a seq: its
        // dedupe entry records it.
        let settled: Option<i64> = conn
            .cached_row(
                "SELECT MAX(t) FROM _jazz_transactions WHERE t < ?1",
                [sync::band_start(sync::PENDING)],
                |r| r.get(0),
            )
            .await
            .map_err(storage)?;
        seq = seq.max(settled.unwrap_or(0) as u64);
        let version = config.schema.version_id();
        admitted.retain(|(id, _)| *id != version);
        admitted.push((version, config.schema.clone()));
        let owner = Rc::new(schema.clone());
        let fan = fanout::Fanout::default();
        let coalesced = fan.coalesced.clone();
        let fanout_stats = fan.stats.clone();
        let backlog_limit = fan.backlog_limit.clone();
        let db = Self {
            view: None,
            defer_fetches: false,
            origin: None,
            inner: Rc::new(Store {
                sessions: RefCell::default(),

                #[cfg(any(test, feature = "testing"))]
                composite_scans: Default::default(),
                accepted: Default::default(),
                #[cfg(any(test, feature = "testing"))]
                overlay_replays: Default::default(),
                #[cfg(any(test, feature = "testing"))]
                failing_pulls: Default::default(),
                #[cfg(any(test, feature = "testing"))]
                failing_ticks: Default::default(),
                replayed_commits: Cell::default(),
                echo_probes: Cell::default(),
                next_advice: Cell::new(sync::fresh_advice_base()),
                upstream: RefCell::default(),
                transports: RefCell::default(),
                session_controls: RefCell::default(),
                migrations: RefCell::new(config.migrations),
                backlog_limit,
                fanout_stats,
                coalesced,
                pending_revision_base: Cell::default(),
                pending: RefCell::default(),
                refusals: RefCell::default(),
                batches: RefCell::default(),
                prior_seq: Cell::new(0),
                prior_served: Cell::default(),
                prior_hold: Cell::default(),
                prior_ack_point: Cell::default(),
                released_reads: RefCell::default(),
                published_schema: RefCell::new(owner.clone()),
                write_layouts: RefCell::default(),
                published_versions: RefCell::default(),
                validated_shapes: RefCell::default(),
                upload_cleanup: Rc::default(),
                reclaimed_at: Cell::default(),
                ingress: Cell::default(),
                #[cfg(any(test, feature = "testing"))]
                reclaim_passes: Cell::default(),
                operation: futures::lock::Mutex::new(()),
                staging_policy: Cell::default(),
                unit_inserts: RefCell::default(),
                account_registry: RefCell::default(),
                #[cfg(feature = "e2ee-client")]
                e2ee: config.e2ee,
                add_creator_as_member: config.add_creator_as_member,
                e2ee_salt: {
                    let mut salt = [0; 32];
                    if cfg!(feature = "e2ee-client") {
                        salt[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
                        salt[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
                    }
                    salt
                },

                client: RefCell::new(None),
                fan: RefCell::new(fan),
                composite_tables: Default::default(),
                names: RefCell::new(Rc::new(names)),
                admitted: RefCell::new(Rc::new(admitted)),
                owner_version: Cell::new(owner.jazz.version_id()),
                owner: RefCell::new(owner),
                physical: RefCell::new(Rc::new(physical)),
                clock: config
                    .clock
                    .unwrap_or_else(|| Box::new(shard::wall_clock_ms)),
                resumes_after: Cell::default(),
                shard: RefCell::default(),
                claims: RefCell::new(HashMap::new()),
                pending_nodes: RefCell::new(Vec::new()),
                derived: RefCell::new(Vec::new()),
                since_tick: RefCell::new(Vec::new()),
                merge_bases: RefCell::new(HashMap::new()),
                possession: RefCell::new(None),
                own_order: RefCell::new(HashMap::new()),
                bundles: RefCell::new(HashMap::new()),
                views: RefCell::new(BTreeMap::new()),
                overlay: RefCell::new(None),
                seq: Cell::new(0),
                merging: Cell::new(false),
                me: Cell::new(0),
                last_stamp: Cell::new((0, 0)),
                hlc_last: Cell::new(0),
                as_sent: RefCell::new(BTreeMap::new()),
                stored_view: RefCell::new(None),
                retention: RefCell::default(),
                retention_by_version: RefCell::default(),
                bundle_views: RefCell::default(),
                version_stored_views: RefCell::default(),
                store_retry: RefCell::default(),
                authors: RefCell::new(AuthorCodes::default()),
                image_inserts: RefCell::default(),
                current_batch: RefCell::default(),
                check_plans: RefCell::new(CheckPlans::default()),
                changed: RefCell::new(Vec::new()),
                deferred: RefCell::new(None),
                trusted_rows: RefCell::new(BTreeMap::new()),
                inherited_heads: RefCell::new(HashMap::new()),
                column_codes: RefCell::default(),
                gone: RefCell::new(Vec::new()),
                #[cfg(any(test, feature = "testing"))]
                evidence_copies: std::cell::Cell::new(0),
                authority: Default::default(),
                chunks,
                clock_for_test: Default::default(),
                closed: Default::default(),
                conn,
                follow_memo: Rc::default(),
                wakes: Rc::default(),
                held_acks: Default::default(),
                ids: RefCell::new(
                    config
                        .id_source
                        .unwrap_or_else(|| Box::new(crate::options::ProductionRowIdSource)),
                ),
                kept_variants: Default::default(),
                merge_budget: Cell::new(jazz_text::MERGE_WORK_BUDGET),
                merge_fallbacks: Default::default(),
                mutation_admission_closed: Default::default(),
                non_durable: Default::default(),
                pins_required: Default::default(),
                pruned_txs: std::cell::Cell::new(None),
                reopenable: path.is_some(),
                seed_floor: Default::default(),
                slow: Default::default(),
                system_node: config.system_node.unwrap_or(config.identity.node),
                identity: config.identity,
                unsupported_versions: Default::default(),
                upstream_rejections: Default::default(),
                withheld: std::cell::Cell::new(withheld > 0),
            }),
        };
        db.inner.publish_schema();
        {
            let inner = db.inner.without_overlay();
            // The newest schema is current: its declaration order is tag
            // order.
            if newest {
                let owner = inner.owner();
                let conn = inner.conn.clone();
                conn.db_scope("jazz_align", async || inner.align_tags(&owner).await)
                    .await?;
            }
            inner.seq.set(seq);
            inner.load_client().await?;
            db.inner
                .pending
                .replace(inner.client().map(|client| client.pending.clone()));
            inner.load_shard().await?;
            // Validate persisted namespace metadata when opening the store.
            inner.stored_namespace().await?;
        }
        Ok(db)
    }

    /// The newest commit seq this database has handed out; its transaction
    /// ids are `(seq, node)`. A host that later gives the same node to a
    /// fresh (in-memory) database seeds it with this through
    /// [`Db::seed_seq_floor`], so no transaction id is reused: the
    /// authority deduplicates uploads by id and would drop the new write.
    pub fn seq_high_water(&self) -> u64 {
        let seq = match self.inner.operation.try_lock() {
            Some(_lease) => self.inner.seq.get(),
            None => self.inner.prior_seq.get(),
        };
        seq.max(self.inner.seed_floor.get())
    }

    /// Never hand out a commit seq at or below `floor`.
    pub fn seed_seq_floor(&self, floor: u64) {
        self.inner
            .seed_floor
            .set(self.inner.seed_floor.get().max(floor));
        if let Some(_lease) = self.inner.operation.try_lock() {
            self.inner.seq.set(self.inner.seq.get().max(floor));
        }
    }

    /// Report pending revisions above `floor` from now on: a host that
    /// replaces a store with a fresh one continues the old one's revision,
    /// so a status read across the swap never looks unchanged.
    pub fn seed_pending_revision(&self, floor: u64) {
        let current = self.inner.pending_revision();
        if current <= floor {
            self.inner
                .pending_revision_base
                .set(self.inner.pending_revision_base.get() + floor + 1 - current);
        }
    }

    /// Every SQLite store holds complete history; this one is also the
    /// global authority (see [`Db::set_authority`]).
    #[cfg(feature = "sqlite")]
    pub async fn open_history_complete(config: DbConfig) -> Result<Self, Error> {
        let db = Self::open(config).await?;
        db.set_authority().await?;
        Ok(db)
    }

    /// Keep the uploads the authority refuses, for a host that tells its
    /// caller of the ones no wait claimed.
    /// Refused commits of this store's own are kept in its retry store
    /// until [`Db::discard_rejected_transaction`]; a reopened store that
    /// still holds some tracks again and reports them anew.
    pub fn track_upstream_rejections(&self) {
        self.inner
            .upstream_rejections
            .borrow_mut()
            .get_or_insert_with(Vec::new);
    }

    /// Report again, through [`Db::take_upstream_rejections`], each refused
    /// commit the retry store still holds and that is not already waiting
    /// to be taken: for a host that could not deliver it before (no
    /// listener yet). Does nothing unless refusals are tracked.
    pub async fn requeue_rejected_transactions(&self) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        if inner.upstream_rejections.borrow().is_none() {
            return Ok(());
        }
        let writer = *inner.identity.node.as_bytes();
        let retained = inner.rejected_transactions().await?;
        let mut rejections = inner.upstream_rejections.borrow_mut();
        let queued = rejections.as_mut().expect("tracked");
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

    /// The catalogue versions an upstream sent that this build cannot read
    /// (a column type or other variant it does not know): each skipped, with a
    /// `SchemaVersionUnsupported` error saying why. The store keeps syncing
    /// the versions it knows.
    pub fn unsupported_schema_versions(&self) -> Vec<(crate::ids::SchemaVersionId, Error)> {
        self.inner
            .unsupported_versions
            .borrow()
            .iter()
            .map(|(id, why)| {
                let mut error = Error::new(
                    ErrorCode::Schema,
                    format!(
                        "schema version {} is not supported by this version of Jazz ({why}); \
                         upgrade it to read that version",
                        id.0
                    ),
                );
                error.reason = Some(Reason::SchemaVersionUnsupported);
                (*id, error)
            })
            .collect()
    }

    /// This store's own commits the authority refused, with their payloads,
    /// oldest first (INV-TX-9). Kept only while
    /// [`Db::track_upstream_rejections`] is on, until discarded.
    pub async fn rejected_transactions(&self) -> Result<Vec<RejectedTransaction>, Error> {
        let _operation = self.write_operation().await;
        self.inner.without_overlay().rejected_transactions().await
    }

    /// Drop a refused commit's payload once its refusal was reported.
    pub async fn discard_rejected_transaction(&self, seq: u64) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        self.inner.without_overlay().discard_rejected(seq).await
    }

    /// The uploads the authority refused since the last call, by the node
    /// and seq of the commit (this store's, or the relayed session's that
    /// wrote it) (empty unless [`Db::track_upstream_rejections`] was called).
    pub fn take_upstream_rejections(&self) -> Vec<UpstreamRejection> {
        self.inner
            .upstream_rejections
            .borrow_mut()
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Serve as the global authority: subscriptions this store settles,
    /// its own and its subscribers', report `DurabilityTier::Global`. A
    /// store without the role (a relay with no upstream) settles at `Local`.
    ///
    /// A store with no upstream mints its authority namespace here, once,
    /// and keeps it in the file (INV-SYNC-54), so a copy of the store taken
    /// before any client connected (a backup) carries it: its replicas
    /// still follow it once it is restored. A store that cannot write it
    /// now mints it when it first serves its catalogue, as before.
    ///
    /// # Errors
    ///
    /// Returns a protocol error if the database was opened with encryption
    /// hooks (`DbConfig::with_e2ee`). Only clients seal and open encrypted
    /// cells, and an authority must store and serve them as they are.
    pub async fn set_authority(&self) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        if inner.e2ee_hooks().is_some() {
            return Err(Error::new(
                ErrorCode::Protocol,
                "a database with encryption hooks cannot serve as the authority",
            ));
        }
        inner.authority.set(true);
        if inner.publishes_catalogue() {
            // A storage failure here surfaces again, with its error, from
            // the first catalogue it serves, which mints it then.
            let _ = inner.namespace().await;
        }
        Ok(())
    }

    /// Read account founders from `registry` in the checks beside the
    /// account tables' policies (E2EE protocol R5, R6). Without one, a
    /// store has no account links, so a session's own login tells how its
    /// account was founded.
    pub fn set_account_registry(&self, registry: Rc<dyn AccountRegistryView>) {
        self.inner.account_registry.replace(Some(registry));
    }

    /// Refuse every session but SYSTEM's that is not pinned to a published
    /// bundle: a core serves its app only through createJazz, which pins
    /// each session it opens, so no session is governed by this store's
    /// own permissions.
    pub fn require_session_pins(&self) {
        self.inner.pins_required.set(true);
    }

    /// Run `write`, one standalone write, under a thin client's own id for
    /// it, `key` (its node and that node's counter, SPEC 8 §8.14). The
    /// commit's writer is [`sync::origin_node`] of `author` (the session the
    /// write runs as) and the client's node, at the client's counter: the
    /// same on every server, and no other identity's. The authority records
    /// the commit's fate under it; a replica uploads it under it. A resend
    /// of the same key by the same author, here or through another server of
    /// the same authority, is not written again: it returns what this store
    /// knows of the first. A write that fails, or makes no commit, leaves
    /// nothing under the id. Issue the write through the handle supplied to the callback.
    pub async fn write_as_origin(
        &self,
        key: crate::tx::TxId,
        author: &AuthorSubject,
        write: impl AsyncFnOnce(&Db) -> Result<WriteHandle, Error>,
    ) -> Result<OriginWrite, Error> {
        let _operation = self.inner.acquire_operation().await;
        let (node, tx) = (sync::origin_node(author, *key.node.as_bytes()), key.time.0);
        {
            let inner = self.inner.without_overlay();
            match inner.origin_fate(node, tx, author).await? {
                OriginFate::Unseen => {}
                OriginFate::Pending | OriginFate::Accepted => return Ok(OriginWrite::Repeat),
                OriginFate::Rejected(error) => return Err(error),
            }
        }
        let scoped = Db {
            inner: self.inner.clone(),
            view: self.view.clone(),
            defer_fetches: self.defer_fetches,
            origin: Some(OriginWriteContext {
                key: Cell::new(Some((node, tx))),
                operation: futures::lock::Mutex::new(()),
            }),
        };
        write(&scoped).await.map(OriginWrite::Written)
    }

    /// Whether the lease [`Db::write_operation`] takes is held (by someone:
    /// a lease does not say whose).
    fn holds_write_operation(&self) -> bool {
        match &self.origin {
            Some(origin) => origin.operation.try_lock().is_none(),
            None => self.inner.operation.try_lock().is_none(),
        }
    }

    async fn write_operation(&self) -> futures::lock::MutexGuard<'_, ()> {
        if let Some(origin) = &self.origin {
            origin.operation.lock().await
        } else {
            self.inner.acquire_operation().await
        }
    }

    /// Whether this store is the global authority.
    pub fn is_authority(&self) -> bool {
        self.inner.authority.get()
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
    pub async fn table_identity(
        &self,
        table: &str,
    ) -> Result<Option<GlobalPhysicalTableId>, Error> {
        Ok(self
            .stored_identity(table, None)
            .await?
            .map(GlobalPhysicalTableId))
    }

    /// The logical names in this handle's schema, keyed by stable table identity.
    /// None until the authority's catalogue for this version has been observed.
    pub async fn table_names_by_identity(
        &self,
    ) -> Result<Option<BTreeMap<GlobalPhysicalTableId, String>>, Error> {
        let _operation = self.write_operation().await;
        let schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let Some(namespace) = inner.namespace().await? else {
            return Ok(None);
        };
        if !inner.names_confirmed(&schema.jazz.version_id()) {
            return Ok(None);
        }
        Ok(Some(
            schema
                .tables
                .iter()
                .map(|(name, table)| {
                    (
                        GlobalPhysicalTableId(crate::names::identity(
                            namespace,
                            &table.stored,
                            None,
                        )),
                        name.clone(),
                    )
                })
                .collect(),
        ))
    }

    /// The identity of column `column` of table `table` of this handle's
    /// schema, as [`Db::table_identity`]. A column dropped and added again
    /// keeps its identity only with the type it had; with another one it is
    /// a new column.
    pub async fn column_identity(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<GlobalPhysicalColumnId>, Error> {
        Ok(self
            .stored_identity(table, Some(column))
            .await?
            .map(GlobalPhysicalColumnId))
    }

    async fn stored_identity(
        &self,
        table: &str,
        column: Option<&str>,
    ) -> Result<Option<uuid::Uuid>, Error> {
        let _operation = self.write_operation().await;
        let active_schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let schema = active_schema.clone();
        let version = schema.jazz.version_id();
        let namespace = inner.namespace().await?;
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

    /// Select this handle's vocabulary and current governing policy.
    fn operation_schema(&self) -> Rc<Schema> {
        self.execution_schema(&self.inner.published_schema.borrow())
    }

    /// Retained current-governed views keep their names, but not an old owner
    /// policy. Exact registered views and published bundle pins stay fixed.
    fn execution_schema(&self, owner: &Rc<Schema>) -> Rc<Schema> {
        let Some(view) = &self.view else {
            return owner.clone();
        };
        match &view.policies_from {
            Some(policies) if policies.bundle.is_none() => {
                if Rc::ptr_eq(policies, owner) {
                    view.clone()
                } else {
                    Rc::new((**view).clone().with_policies_of(owner.clone()))
                }
            }
            _ => view.clone(),
        }
    }

    /// Keep prepared names while selecting policy from the executing handle.
    fn execution_query<'a>(
        &self,
        prepared: &'a PreparedQuery,
    ) -> std::borrow::Cow<'a, PreparedQuery> {
        // A peek: choosing policies must not settle an open transaction's
        // overlay; `inner()` would, on every identity read in the batch.
        let owner = self.inner.published_schema.borrow();
        let schema = self.execution_schema(&owner);
        let names = prepared.schema.as_ref().unwrap_or(&owner);
        let governing = schema.policies_from.as_ref().unwrap_or(&schema);
        let prior = names.policies_from.as_ref().unwrap_or(names);
        if Rc::ptr_eq(prior, governing) {
            return std::borrow::Cow::Borrowed(prepared);
        }
        std::borrow::Cow::Owned(PreparedQuery {
            schema: Some(Rc::new(
                (**names).clone().with_policies_of(governing.clone()),
            )),
            ..prepared.clone()
        })
    }

    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Bind `claims` to `identity`; returns whether they changed.
    pub async fn set_identity_claims(
        &self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> bool {
        let _operation = self.write_operation().await;
        // A peek: a host repeats an identity's claims before each of its
        // writes, and the store gate would undo an open transaction's
        // cached overlay, so its next stage would replay every write it
        // staged so far.
        if self.inner.claims.borrow().get(&identity) == Some(&claims) {
            return false;
        }
        let inner = self.inner.without_overlay();
        inner.claims.borrow_mut().insert(identity, claims);
        inner.refresh_identity(identity).await;
        true
    }

    pub async fn tick(&self) -> Result<(), Error> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(code) = {
            let _operation = self.write_operation().await;
            self.inner.failing_ticks.borrow_mut().pop_front()
        } {
            return Err(Error::new(code, "a tick failure injected by a test"));
        }
        self.inner.wakes.host_ticked();
        self.sync_tick().await?;
        {
            let _operation = self.write_operation().await;
            let conn = {
                let inner = self.inner.without_overlay();
                inner.fan.borrow_mut().collect();
                inner.conn.clone()
            };
            self.inner
                .reclaim_large_values(&conn, RECLAIM_NODES_PER_TICK)
                .await?;
        }
        self.pump_chunk_store().await
    }

    /// Fetch the offloaded nodes reads wait on, move settled leaves to the
    /// chunk store and delete reclaimed ones from it ([`crate::chunk_store`]),
    /// unless the host runs that I/O. The fetches run first, so messages
    /// held for those nodes are served before the puts start. A failed
    /// operation is retried next tick; the first failure is returned once
    /// the rest ran.
    async fn pump_chunk_store(&self) -> Result<(), Error> {
        let chunks = self.inner.chunks.clone();
        if chunks.host_io() {
            return Ok(());
        }
        let Some(store) = chunks.store().cloned() else {
            return Ok(());
        };
        let Some(mut io) = self.begin_chunk_io(&chunks, true).await? else {
            return Ok(());
        };
        let wanted = std::mem::take(&mut io.fetch);
        let any = !wanted.is_empty();
        let mut fetched = self.fetch_from(&chunks, &*store, wanted).await;
        if let Some(lease) = &mut io.lease {
            // Fetched (or failed, and asked again by the read that needs it).
            lease.unfetched.clear();
        }
        if any && fetched.is_ok() {
            fetched = self.sync_tick().await;
        }
        let done = io.run(&*store).await;
        let applied = self.end_chunk_io(&chunks, done).await;
        fetched.and(applied)
    }

    /// The next batch of chunk-store work, for a store opened
    /// [`crate::options::DbConfig::with_host_chunk_io`]: `None` while the
    /// last one is out or without a chunk store. Hand what running it came
    /// to back with [`Self::finish_chunk_io`]; a batch dropped instead ends
    /// there, and its work goes to the next one.
    pub async fn take_chunk_io(&self) -> Result<Option<crate::chunk_store::ChunkIo>, Error> {
        self.take_host_chunk_io(true).await
    }

    /// [`Self::take_chunk_io`] with no fetches in it: for a host whose
    /// fetches back off after failing ([`Self::take_chunk_fetches`]), so
    /// its uploads go on meanwhile without asking the store again for what
    /// it failed to give. The reads refused for those stay refused.
    pub async fn take_chunk_io_holding_fetches(
        &self,
    ) -> Result<Option<crate::chunk_store::ChunkIo>, Error> {
        self.take_host_chunk_io(false).await
    }

    async fn take_host_chunk_io(
        &self,
        fetches: bool,
    ) -> Result<Option<crate::chunk_store::ChunkIo>, Error> {
        let chunks = self.inner.chunks.clone();
        if !chunks.host_io() {
            return Err(not_host_run());
        }
        if chunks.store().is_none() {
            return Ok(None);
        }
        self.begin_chunk_io(&chunks, fetches).await
    }

    /// The offloaded nodes reads wait on, to fetch beside the batch that is
    /// out (or backing off) so they do not wait for its uploads: `None`
    /// when no read waits. Hand what running it came to back with
    /// [`Self::finish_chunk_fetches`]. A node a read waits on is
    /// referenced, so no delete in the batch removes it. Dropped instead,
    /// its nodes are wanted again. A node a fetch is already out for, in
    /// either lane, is not fetched again.
    ///
    /// The fetches that failed last are asked again here, as by the next
    /// batch: a host backing its fetches off takes these only once the
    /// wait is over.
    pub async fn take_chunk_fetches(&self) -> Result<Option<crate::chunk_store::ChunkIo>, Error> {
        let _operation = self.write_operation().await;
        let chunks = self.inner.chunks.clone();
        if !chunks.host_io() {
            return Err(not_host_run());
        }
        if chunks.store().is_none() {
            return Ok(None);
        }
        if chunks.clear_failed() {
            let inner = self.inner.without_overlay();
            inner.retry_awaiting().await;
            inner.retry_store_failed_pulls();
        }
        let wanted = chunks.take_fetch();
        if wanted.is_empty() {
            return Ok(None);
        }
        let mut io = crate::chunk_store::ChunkIo::unleased(wanted.clone());
        io.refetch = Some(crate::chunk_store::Refetch {
            chunks: std::sync::Arc::downgrade(&chunks),
            unfetched: wanted,
        });
        Ok(Some(io))
    }

    /// Install fetches [`Self::take_chunk_fetches`] handed out, and serve
    /// the messages held for them. Reads of a node the store failed to
    /// answer are refused with why until the next batch starts; the first
    /// failure is returned. Anything else is refused.
    pub async fn finish_chunk_fetches(
        &self,
        mut done: crate::chunk_store::ChunkIoDone,
    ) -> Result<(), Error> {
        let chunks = self.inner.chunks.clone();
        if !chunks.host_io() {
            return Err(not_host_run());
        }
        let ours = done.io.refetch.as_ref().is_some_and(|refetch| {
            std::ptr::eq(refetch.chunks.as_ptr(), std::sync::Arc::as_ptr(&chunks))
        });
        if !ours
            || done.io.lease.is_some()
            || !done.io.puts.is_empty()
            || !done.io.deletes.is_empty()
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "chunk I/O: these are not fetches this store handed out",
            ));
        }
        // Answered: nothing goes back to the wanted nodes.
        if let Some(mut refetch) = done.io.refetch.take() {
            refetch.unfetched.clear();
        }
        let mut first = None;
        self.install_fetched(&chunks, &done.io.fetch, done.fetched, true, &mut first)
            .await;
        self.sync_tick().await?;
        first.map_or(Ok(()), Err)
    }

    /// Apply a batch [`Self::take_chunk_io`] handed out: fetched nodes are
    /// installed and the reads and requests waiting on them run again,
    /// moved leaves leave SQLite, deleted ones leave the queue. Failed
    /// operations are retried in a later batch; the first failure is
    /// returned once the rest are applied. A batch this store did not hand
    /// out, or not the one out now, is refused.
    pub async fn finish_chunk_io(
        &self,
        done: crate::chunk_store::ChunkIoDone,
    ) -> Result<(), Error> {
        let chunks = self.inner.chunks.clone();
        if !chunks.host_io() {
            return Err(not_host_run());
        }
        let outstanding = chunks.outstanding();
        let ours = done.io.lease.as_ref().is_some_and(|lease| {
            std::ptr::eq(lease.chunks.as_ptr(), std::sync::Arc::as_ptr(&chunks))
                && Some(lease.batch) == outstanding
        });
        if !ours {
            return Err(Error::new(
                ErrorCode::Protocol,
                match outstanding {
                    None => "chunk I/O: no batch is out to finish",
                    Some(_) => "chunk I/O: this is not the batch that is out",
                },
            ));
        }
        self.end_chunk_io(&chunks, done).await
    }

    /// Start a batch: what reads wait on, and (between SQLite transactions,
    /// so the mark that a put may have landed is durable before it does)
    /// settled leaves to put and reclaimed ones to delete.
    /// `fetches: false` leaves what reads wait on, and the last failed
    /// fetches, for later.
    async fn begin_chunk_io(
        &self,
        chunks: &std::sync::Arc<crate::large::Chunks>,
        fetches: bool,
    ) -> Result<Option<crate::chunk_store::ChunkIo>, Error> {
        let _operation = self.write_operation().await;
        // The last batch's failed fetches are asked again: results that
        // were refused for them run again, and want what they still lack
        // in this batch.
        if fetches && chunks.outstanding().is_none() && chunks.clear_failed() {
            let inner = self.inner.without_overlay();
            inner.retry_awaiting().await;
            inner.retry_store_failed_pulls();
        }
        let Some((batch, fetch)) = chunks.begin_fetch(fetches) else {
            return Ok(None);
        };
        // From here the lease ends the batch however this returns.
        let lease = crate::chunk_store::Lease {
            chunks: std::sync::Arc::downgrade(chunks),
            batch,
            unfetched: fetch.clone(),
        };
        let mut io = crate::chunk_store::ChunkIo::unleased(fetch);
        io.lease = Some(lease);
        if chunks.offload_store().is_some() {
            let inner = self.inner.without_overlay();
            if inner.conn.is_autocommit() {
                io.puts =
                    crate::large::begin_offload(&inner.conn, CHUNK_STORE_NODES_PER_TICK).await?;
            }
            io.deletes =
                crate::large::pending_deletes(&inner.conn, CHUNK_STORE_NODES_PER_TICK).await?;
        }
        Ok(Some(io))
    }

    /// End a batch [`Self::begin_chunk_io`] started, once it is applied.
    async fn end_chunk_io(
        &self,
        chunks: &crate::large::Chunks,
        mut done: crate::chunk_store::ChunkIoDone,
    ) -> Result<(), Error> {
        let lease = done.io.lease.take();
        let applied = self.apply_chunk_io(chunks, done).await;
        if let Some(mut lease) = lease {
            // Fetches that failed are asked again by the reads that need them.
            lease.unfetched.clear();
        }
        applied
    }

    async fn apply_chunk_io(
        &self,
        chunks: &crate::large::Chunks,
        done: crate::chunk_store::ChunkIoDone,
    ) -> Result<(), Error> {
        let crate::chunk_store::ChunkIoDone {
            io,
            fetched,
            put,
            deleted,
        } = done;
        let mut first = None;
        let mut fetch_failed = None;
        let any = !io.fetch.is_empty();
        if any {
            self.install_fetched(chunks, &io.fetch, fetched, true, &mut fetch_failed)
                .await;
        }
        {
            let _operation = self.write_operation().await;
            let inner = self.inner.without_overlay();
            if !io.puts.is_empty() {
                inner
                    .conn
                    .clone()
                    .scope("jazz_lv_offloaded", async |conn| {
                        for ((key, _), put) in io.puts.iter().zip(put) {
                            match put {
                                Ok(()) => crate::large::offloaded(conn, key).await?,
                                Err(error) => {
                                    first.get_or_insert(error);
                                }
                            }
                        }
                        Ok(())
                    })
                    .await?;
            }
            if !io.deletes.is_empty() {
                inner
                    .conn
                    .clone()
                    .scope("jazz_lv_deleted", async |conn| {
                        for (key, done) in io.deletes.iter().zip(deleted) {
                            match done {
                                Ok(()) => crate::large::deleted(conn, key).await?,
                                Err(error) => {
                                    first.get_or_insert(error);
                                }
                            }
                        }
                        Ok(())
                    })
                    .await?;
            }
        }
        if any {
            // Messages held for those nodes are served now, not a tick
            // later: with them, or refused with why they could not be.
            self.sync_tick().await?;
        }
        fetch_failed.or(first).map_or(Ok(()), Err)
    }

    /// Install what the chunk store answered for `wanted`; reads waiting
    /// on those nodes run again. For the host's batch (`refuse`), reads of
    /// a node it failed to fetch are refused with why until the next batch,
    /// rather than waiting on it; a read that fetched itself gets the error
    /// back directly.
    async fn install_fetched(
        &self,
        chunks: &crate::large::Chunks,
        wanted: &[jazz_types::large_values::chunks::ChunkRequest],
        answers: Vec<Result<Option<bytes::Bytes>, Error>>,
        refuse: bool,
        first: &mut Option<Error>,
    ) {
        let _operation = self.write_operation().await;
        for (request, answer) in wanted.iter().zip(answers) {
            if let Err(error) = answer.and_then(|bytes| chunks.install_from_store(request, bytes)) {
                if refuse {
                    chunks.fetch_failed(request, error.clone());
                }
                first.get_or_insert(error);
            }
        }
        chunks.landed(wanted);
        self.inner.without_overlay().retry_awaiting().await;
    }

    /// Fetch `wanted` from the chunk store and install it.
    async fn fetch_from(
        &self,
        chunks: &crate::large::Chunks,
        store: &dyn crate::chunk_store::ChunkStore,
        wanted: Vec<jazz_types::large_values::chunks::ChunkRequest>,
    ) -> Result<(), Error> {
        if wanted.is_empty() {
            return Ok(());
        }
        // Dropped before the answer, the nodes are wanted again: no fetch
        // is out for them any more.
        struct Out<'a>(
            &'a crate::large::Chunks,
            Option<Vec<jazz_types::large_values::chunks::ChunkRequest>>,
        );
        impl Drop for Out<'_> {
            fn drop(&mut self) {
                if let Some(requests) = self.1.take() {
                    self.0.want_again(requests);
                }
            }
        }
        let mut out = Out(chunks, Some(wanted.clone()));
        let done = crate::chunk_store::ChunkIo::unleased(wanted)
            .run(store)
            .await;
        out.1 = None;
        let mut first = None;
        self.install_fetched(chunks, &done.io.fetch, done.fetched, false, &mut first)
            .await;
        first.map_or(Ok(()), Err)
    }

    /// [`Self::fetching`] over a fresh copy of `arg` per attempt; without a
    /// chunk store `arg` is not copied.
    async fn fetching_with<A: Clone, T>(
        &self,
        rows: &[[u8; 16]],
        arg: A,
        attempt: impl AsyncFn(A) -> Result<T, Error>,
    ) -> Result<T, Error> {
        // A peek: the store gate would undo a transaction's cached overlay.
        if self.inner.chunks.store().is_none() {
            let _operation = self
                .wait_for_rows(self.write_operation().await, rows)
                .await?;
            return attempt(arg).await;
        }
        self.fetching(rows, async || attempt(arg.clone()).await)
            .await
    }

    /// Run `attempt` until it no longer waits on offloaded nodes that only
    /// an asynchronous chunk store can answer, fetching them in between.
    /// Without such a store (or while a tick's fetch runs) a wait is the
    /// usual missing-chunks error.
    async fn fetching<T>(
        &self,
        rows: &[[u8; 16]],
        mut attempt: impl AsyncFnMut() -> Result<T, Error>,
    ) -> Result<T, Error> {
        loop {
            let result = {
                let _operation = self
                    .wait_for_rows(self.write_operation().await, rows)
                    .await?;
                attempt().await
            };
            match result {
                Err(error) if crate::large::is_missing(&error) => {
                    let chunks = self.inner.chunks.clone();
                    let Some(store) = chunks.store().cloned().filter(|_| chunks.has_fetch()) else {
                        return Err(error);
                    };
                    if chunks.host_io() {
                        if self.defer_fetches {
                            // The host's fetch lane reads them, off this
                            // thread and with its backoff.
                            return Err(error);
                        }
                        // Beside the host's batch, which may be out: a
                        // node a read waits on is referenced, so no
                        // delete in that batch removes it.
                        let wanted = chunks.take_fetch();
                        if wanted.is_empty() {
                            return Err(error);
                        }
                        self.fetch_from(&chunks, &*store, wanted).await?;
                        continue;
                    }
                    let Some((batch, wanted)) = chunks.begin_fetch(true) else {
                        return Err(error);
                    };
                    // Ends the batch even if this future is dropped.
                    let _lease = crate::chunk_store::Lease {
                        chunks: std::sync::Arc::downgrade(&chunks),
                        batch,
                        unfetched: Vec::new(),
                    };
                    self.fetch_from(&chunks, &*store, wanted).await?;
                }
                result => return result,
            }
        }
    }

    /// From now on, a write may hold the variants `kept` names for its
    /// table and column (by the names the write's schema gives them)
    /// though its schema does not declare them, as rows a lens or merge
    /// carries already can; any other undeclared name is still refused.
    /// Only for importing rows another store kept: alpha.59 keeps a
    /// variant a later schema removed in rows written before, and the
    /// import names the variants its earlier versions declared.
    #[doc(hidden)]
    pub fn keep_variants_for_import(&self, kept: BTreeMap<(String, String), BTreeSet<String>>) {
        self.inner.kept_variants.replace(kept);
    }

    /// Run this store's wall clock at `ms` (Unix ms) from now on, or the
    /// real one again for `None`: what tests of time-ordered writes set
    /// instead of sleeping.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_clock_for_test(&self, ms: Option<i64>) {
        self.inner.clock_for_test.set(ms);
    }

    /// Let text merges take `budget` diff steps: what tests of a merge past
    /// its budget set instead of merging very large rewrites.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_merge_budget_for_test(&self, budget: u64) {
        self.inner.merge_budget.set(budget);
    }

    /// The versions open serializable transaction `id` recorded its reads
    /// at, each pending one with the writer it names: what its commit is
    /// validated by.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn read_set_for_test(
        &self,
        id: OpenTransactionId,
    ) -> Vec<(RowUuid, Pos, Option<([u8; 16], u64)>)> {
        match self.inner.batches.borrow().get(&id) {
            Some(Batch {
                kind: BatchKind::Serializable { reads, .. },
                ..
            }) => reads
                .iter()
                .flat_map(|read| read.rows.iter())
                .map(|&(row, (at, writer))| (row, at, writer))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Set this store's last commit HLC, as if it had handed out `hlc`.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_hlc_for_test(&self, hlc: u64) {
        self.inner.hlc_last.set(hlc);
    }

    /// Make the next ticks fail with `codes`, in order, before doing any
    /// work: a tick driver's failure handling is exercised without
    /// breaking the store.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn fail_next_ticks_for_test(&self, codes: impl IntoIterator<Item = ErrorCode>) {
        self.inner.failing_ticks.borrow_mut().extend(codes);
    }

    /// Fail the next `pulls` pulls replicas send this store with a storage
    /// error before they read anything, as a store that cannot read its
    /// file would.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn fail_next_pulls_for_test(&self, pulls: usize) {
        self.inner.failing_pulls.set(pulls);
    }

    /// Where the current schema stores `table`'s `column`: (stored table,
    /// stored column), the names a hand-built replicated write uses.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn stored_column_for_test(&self, table: &str, column: &str) -> (String, String) {
        let inner = &self.inner;
        let owner_schema = inner.owner();
        let table = owner_schema
            .table(table)
            .expect("a table of the current schema");
        (table.stored.clone(), table.stored_name(column).to_owned())
    }

    /// Whether any admitted version has a table named `logical`: the
    /// admitted versions, not the owner's schema, which a running server
    /// keeps at its first while deployments admit later versions.
    pub fn admits_table(&self, logical: &str) -> bool {
        self.inner.names().stored_table_of(logical).is_some()
    }

    /// Whether the store holds a live row `row` of the table the admitted
    /// versions name `logical`, wherever the owner's schema stands.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn holds_row_for_test(&self, logical: &str, row: RowUuid) -> Result<bool, Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let Some(stored) = inner.names().stored_table_of(logical).map(str::to_owned) else {
            return Ok(false);
        };
        Ok(matches!(
            inner.existing(&stored, row, &[]).await?,
            Some((_, false))
        ))
    }

    /// Diagnostics: stored large-value nodes, and roots referenced by cells.
    #[doc(hidden)]
    pub async fn large_value_nodes_for_test(&self) -> Result<(u64, u64), Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        inner
            .conn
            .cached_row(
                "SELECT (SELECT count(*) FROM _jazz_lv_node), \
                 (SELECT count(*) FROM _jazz_lv_root WHERE refs > 0)",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .await
            .map_err(storage)
    }

    /// Diagnostics: stored large-value nodes whose bytes live only in the
    /// chunk store.
    #[doc(hidden)]
    pub async fn offloaded_large_value_nodes_for_test(&self) -> Result<u64, Error> {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .conn
            .cached_row(
                "SELECT count(*) FROM _jazz_lv_node WHERE length(bytes) = 0",
                [],
                |r| r.get::<_, i64>(0).map(|n| n as u64),
            )
            .await
            .map_err(storage)
    }

    /// Diagnostics: large-value reclaim passes this store ran.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn large_value_reclaim_passes_for_test(&self) -> u64 {
        self.inner.reclaim_passes.get()
    }

    /// Diagnostics: (epochs, encoded edit bytes they hold).
    #[doc(hidden)]
    pub async fn large_value_epochs_for_test(&self) -> Result<(u64, u64), Error> {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .conn
            .cached_row(
                "SELECT count(*), coalesce(sum(length(edits)), 0) FROM _jazz_lv_epoch",
                [],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .await
            .map_err(storage)
    }

    /// Roll back the open SQLite transaction behind the database's back, as
    /// SQLite itself does after some errors (a full disk, an I/O error).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub async fn rollback_sqlite_transaction_for_test(&self) {
        let _operation = self.write_operation().await;
        self.inner
            .as_ref()
            .conn
            .execute_batch("ROLLBACK")
            .await
            .expect("an open SQLite transaction");
    }

    /// Diagnostics: the chunks reads needed that nothing local holds.
    #[doc(hidden)]
    pub fn take_missing_chunks_for_test(&self) -> usize {
        self.inner.chunks.take_demand().len()
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.tick().await
    }

    /// Permanently stop admitting application row mutations in this runtime
    /// and all its schema views. New transactions, further staging, stream
    /// pushes and commits return a storage error, including transactions
    /// opened before this fence was set.
    ///
    /// Reads, transaction abandonment, stream aborts and ordinary sync/cleanup
    /// ticks remain available so already committed writes can settle. This is
    /// an in-memory shutdown phase, not a stored policy or storage retirement;
    /// a new storage owner starts with mutation admission open.
    pub fn stop_mutation_admission(&self) {
        self.inner.mutation_admission_closed.set(true);
    }

    // ---- preparation ----------------------------------------------------

    /// Diagnostics: the compiled SQL of a read and SQLite's query plan for
    /// it. The basis for future index suggestions.
    #[doc(hidden)]
    pub async fn explain_query_plan(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<(String, String), Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let compiled = inner.compile(
            prepared,
            identity,
            &ReadSpec::default(),
            inner.read_namespace(prepared).await?,
        )?;
        let mut statement = inner
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {}", compiled.sql))
            .map_err(storage)?;
        let mut rows = statement
            .query_map(params_from_iter(compiled.params.iter()), |r| {
                r.get::<_, String>(3)
            })
            .await
            .map_err(storage)?;
        let mut plan = Vec::new();
        while let Some(row) = rows.next().await {
            plan.push(row.map_err(storage)?);
        }
        Ok((compiled.sql.clone(), plan.join(" | ")))
    }

    /// Diagnostics for the Inspector's `explain`: for each of `rows` of
    /// `table` (current, not deleted), which of its read policy's clauses
    /// hold for `author` with `claims` (as bound to an identity: see
    /// [`crate::binding_args::policy_claims`]). The clauses are the arms of the
    /// policy's top-level `Or` (one clause when it is not an `Or`); none when
    /// the table has no read policy, which denies every row. The policy is
    /// the one reads are judged by (an older schema's rows by the current
    /// schema's policies). Answers the number of clauses and, for each row
    /// found (current, not deleted), whether each holds. Branched tables are
    /// not supported yet.
    #[doc(hidden)]
    pub async fn explain_read_clauses(
        &self,
        table: &str,
        rows: &[RowUuid],
        author: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Result<(usize, Vec<(RowUuid, Vec<bool>)>), Error> {
        let _operation = self.write_operation().await;
        let active_schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let table = active_schema.table(table)?;
        if !table.branch_by.is_empty() {
            return Err(unsupported("explain for a table with branches"));
        }
        // As `Ctx::under_policy_schema` judges reads: by the schema whose
        // policies govern this table's rows.
        let governing = match active_schema.policies_from.as_deref() {
            None => Some(table),
            Some(current) => current.by_stored(&table.stored),
        };
        let clauses: Vec<PolicyExpr> = match governing.and_then(|t| t.policy(Operation::Select)) {
            None => Vec::new(),
            Some(PolicyExpr::Or(exprs)) => exprs.clone(),
            Some(policy) => vec![policy.clone()],
        };
        if rows.is_empty() {
            return Ok((clauses.len(), Vec::new()));
        }
        let session = Session::for_author(author, claims);
        let binding = BTreeMap::new();
        let none = Session::default();
        let mut ctx = Ctx::new(&active_schema, None, &none, &binding).with_namespace(
            inner
                .identity_namespace(active_schema.policy_schema())
                .await?,
        );
        let mut columns = Vec::with_capacity(clauses.len());
        for clause in &clauses {
            let condition = ctx.policy_clause(Some(clause), table, "a", &session)?;
            columns.push(format!(", COALESCE({condition}, 0)"));
        }
        let source = ctx.source(table)?;
        let mut params = ctx.params;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            params.push(Sql::Blob(row.0.as_bytes().to_vec()));
            ids.push(format!("?{}", params.len()));
        }
        let sql = format!(
            "SELECT a.id{} FROM {source} AS a WHERE a.id IN ({}) AND (a._state & {STATE_DELETED}) = 0",
            columns.concat(),
            ids.join(", "),
        );
        let mut statement = inner.conn.prepare(&sql).map_err(storage)?;
        let count = clauses.len();
        let mut found = statement
            .query_map(params_from_iter(params.iter()), |r| {
                let id: Vec<u8> = r.get(0)?;
                let mut holds = Vec::with_capacity(count);
                for index in 0..count {
                    holds.push(r.get::<_, i64>(index + 1)? != 0);
                }
                Ok((id, holds))
            })
            .await
            .map_err(storage)?;
        let mut answer = Vec::new();
        while let Some(row) = found.next().await {
            let (id, holds) = row.map_err(storage)?;
            let id: [u8; 16] = id
                .try_into()
                .map_err(|_| crate::values::query_error("a row id is not 16 bytes"))?;
            answer.push((RowUuid::from_bytes(id), holds));
        }
        Ok((count, answer))
    }

    /// Text merges that ran past [`jazz_text::MERGE_WORK_BUDGET`] since the
    /// last call, so the incoming write stood as sent and the held value's
    /// concurrent edits left the current text (history keeps them), and
    /// how many more there were than the list keeps. Hosts call it after
    /// ticking and report them.
    pub fn take_merge_fallbacks(&self) -> (Vec<MergeFallback>, u64) {
        let mut fallbacks = self.inner.merge_fallbacks.borrow_mut();
        (
            std::mem::take(&mut fallbacks.kept),
            std::mem::take(&mut fallbacks.dropped),
        )
    }

    /// Query shapes that ran at or over the slow-query threshold since
    /// the last call, slowest total first, and how many further shapes the
    /// window had no room for. See [`crate::slow`].
    pub fn take_slow_queries(&self) -> (Vec<crate::slow::SlowQuery>, u64) {
        self.inner.slow.borrow_mut().take()
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
        self.inner.slow.borrow_mut().take_due()
    }

    /// Record query runs taking at least `threshold` (`None`: none).
    /// Defaults to [`crate::slow::DEFAULT_SLOW_QUERY_THRESHOLD`].
    pub fn set_slow_query_threshold(&self, threshold: Option<std::time::Duration>) {
        self.inner.slow.borrow_mut().set_threshold(threshold);
    }

    /// How many changed rows a subscription's undelivered deltas may hold
    /// before they are coalesced into one reset of its current result
    /// (marked `coalesced`), for reads that set no
    /// [`ReadOpts::backlog_rows`]. Defaults to
    /// [`fanout::DEFAULT_SUBSCRIPTION_BACKLOG_LIMIT`]. It cannot be turned
    /// off: a reader that stops reading holds at most this many changed
    /// rows plus its result, however fast others write.
    pub fn set_subscription_backlog_limit(&self, limit: std::num::NonZeroUsize) {
        self.inner.backlog_limit.set(Some(limit));
    }

    /// Diagnostics: SQLite's query plan for the INSERT check a commit
    /// would run on `row` of `table` as `author`, with `grounded` rows of
    /// that table overlaid as a later grounding round sees them.
    #[doc(hidden)]
    pub async fn explain_write_check_plan(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
        grounded: &[RowUuid],
    ) -> Result<String, Error> {
        let _operation = self.write_operation().await;
        let active_schema = self.operation_schema();
        let ids = grounded.iter().map(|id| *id.0.as_bytes()).collect();
        let inner = self.inner.without_overlay();
        let stored = active_schema.table(table)?.stored.clone();
        let overlay = CutOverlay::Grounded(Arc::new(BTreeMap::from([(stored, ids)])));
        inner
            .explain_write_check(&active_schema, table, row, author, overlay)
            .await
    }

    /// Diagnostics: SQLite's query plan for the INSERT check of `row` of
    /// `table` as `author` against a unit's post-state (every row the unit
    /// wrote but `row`), as the post-state pass runs it.
    #[doc(hidden)]
    pub async fn explain_post_state_check_plan(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
    ) -> Result<String, Error> {
        let _operation = self.write_operation().await;
        let active_schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let stored = active_schema.table(table)?.stored.clone();
        let overlay = CutOverlay::Except {
            table: stored,
            row: *row.0.as_bytes(),
            key: Vec::new(),
        };
        inner
            .explain_write_check(&active_schema, table, row, author, overlay)
            .await
    }

    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    pub fn prepare_query_bound(
        &self,
        query: &Query,
        binding: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let schema = self.execution_schema(&self.inner.published_schema.borrow());
        let mut validated = self.inner.validated_shapes.borrow_mut();
        let normalized = normalize_relation(query, &schema)?;
        let query = normalized.as_ref().unwrap_or(query);
        // Encrypted equality filters become token filters before anything
        // validates, stores or uploads the query.
        let (physical, planned) = self.inner.plan_query(&schema, query)?;
        let query = physical.as_ref();
        let mut shape = format!("{query:?}{:?}", schema.jazz.version_id());
        // The held epochs a rewritten query's tokens were computed for: a
        // new one is a new query, even offline, where its text is the same.
        // So is a new residual: its filter and window are not in the text.
        if let Some(planned) = &planned {
            shape.push_str(&format!("{:?}{:?}", planned.epochs, planned.residual_key));
        }
        let shape = jazz_types::sha256::hash(shape.as_bytes());
        let bound = jazz_types::sha256::hash(format!("{binding:?}").as_bytes());
        let key = (short(shape), short(bound));
        if !validated.contains(&key) {
            // Validate: tables, columns, parameters, supported forms, and
            // every relation as its reads will compile it. Read options are
            // not known yet: validate the widest read (deleted rows
            // included, so their metadata columns are accepted). Once per
            // shape: reads, fanout and paging don't validate again.
            let session = Session::system();
            Ctx::new(&schema, None, &session, &binding).compile_query(query, true)?;
            Ctx::new(&schema, None, &session, &binding).validate_prepared(query)?;
            if validated.len() >= VALIDATED_SHAPES_MAX {
                validated.clear();
            }
            validated.insert(key);
        }
        Ok(PreparedQuery {
            query: physical.into_owned(),
            binding,
            shape: Shape(short(shape)),
            binding_id: Binding(short(bound)),
            request_claims: None,
            schema: self.view.clone(),
            planned,
        })
    }

    // ---- reads ----------------------------------------------------------

    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.fetching(&[], async || {
            let inner = self.inner.without_overlay();
            inner
                .run_for_reader(prepared, None, &ReadSpec::of(&opts, inner).await?, false)
                .await
        })
        .await
    }

    /// [`Db::all`] as a strict read: one a write is built on, which fails
    /// rather than leave out a row the encryption hooks cannot open
    /// (protocol §4.4).
    pub async fn all_strict(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.fetching(&[], async || {
            let inner = self.inner.without_overlay();
            inner
                .run_for_reader(prepared, None, &ReadSpec::of(&opts, inner).await?, true)
                .await
        })
        .await
    }

    /// A one-shot read with array relations nested in their parents' cells,
    /// as a subscription nests them: roots and related rows alike read as
    /// `identity` (its read policy applies to both; `None`: unchecked), at
    /// the one read position `opts` gives.
    pub async fn all_nested(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: Option<AuthorSubject>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.fetching(&[], async || {
            // Resolved on every attempt: a chunk wait may outlive a policy
            // revision.
            let prepared = match identity {
                Some(_) => self.execution_query(prepared),
                None => std::borrow::Cow::Borrowed(prepared),
            };
            let inner = self.inner.without_overlay();
            let read = ReadSpec::of(&opts, inner).await?;
            let roots = inner
                .run_for_reader(&prepared, identity, &read, false)
                .await?;
            inner
                .nest_relations(&prepared, identity, &read, roots)
                .await
        })
        .await
    }

    /// Root rows with each array relation nested in a cell under its
    /// column name ([`ResultNode::into_nested_row`]). Related rows are read
    /// without a policy check, at the default read spec: to read them as an
    /// identity, use [`Db::all_nested`].
    pub async fn nest_relations(
        &self,
        prepared: &PreparedQuery,
        roots: Vec<CurrentRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .nest_relations(prepared, None, &ReadSpec::default(), roots)
            .await
    }

    /// A read of the store as it is.
    pub async fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .run_for_reader(prepared, None, &ReadSpec::default(), false)
            .await
    }

    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        let _operation = self.write_operation().await;
        let prepared = self.execution_query(prepared);
        let inner = self.inner.without_overlay();
        inner
            .run_for_reader(
                &prepared,
                Some(identity),
                &ReadSpec::of(&opts, inner).await?,
                false,
            )
            .await
    }

    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let read = ReadSpec::of(&opts, inner).await?;
        let roots = inner.run_for_reader(prepared, None, &read, false).await?;
        let mut run = RelationRun::new(
            inner,
            prepared,
            None,
            &read,
            inner.read_namespace(prepared).await?,
        );
        let tree = inner
            .nest(
                &mut run,
                &prepared.query.table,
                &prepared.query.array_subqueries,
                roots,
            )
            .await?;
        Ok(ResultTree { roots: tree })
    }

    // ---- subscriptions --------------------------------------------------

    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, None, opts, false).await
    }

    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, Some(identity), opts, false)
            .await
    }

    /// Restore a reader's watches; its first matching change emits a full opening.
    pub async fn restore_subscription_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(prepared, Some(identity), opts, true)
            .await
    }

    /// On a client a subscription also attaches remotely (unless
    /// LocalOnly); its opening is withheld while it waits for the server
    /// (the Remote tier, or `FirstLoad::WaitForRemote` with a non-zero wait
    /// while the link could answer), and its deltas report whether the
    /// server answered.
    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        opts: ReadOpts,
        dormant: bool,
    ) -> Result<SubscriptionStream, Error> {
        let _operation = self.write_operation().await;
        let read = ReadSpec::of(&opts, self.inner.without_overlay()).await?;
        let client = self.is_client();
        let non_durable = self.inner.non_durable.get();
        // A branch view's head (and live base) is kept current upstream,
        // over a frozen base too: only a snapshot is wholly this store's.
        let reach = client
            .then(|| sync::remote_reach(&opts, non_durable, read.cut.is_some()))
            .flatten();
        if reach.is_some() {
            self.remote_shape(&prepared.query)?;
        }
        let view = opts.read_view.source.clone();
        let attached = match reach {
            Some(reach) => {
                self.attach_remote(prepared, read.include_deleted, &view, reach)
                    .await
            }
            None => None,
        };
        // A live result reads as far as its own upstream read is settled.
        let read = match &attached {
            Some((key, _)) => ReadSpec {
                position: Position::Remote(key.clone()),
                ..read
            },
            None => read,
        };
        let remote = attached.map(|(_, signal)| signal);
        let settled = match (&remote, client) {
            (Some(signal), _) => sync::SettledBy::Remote(signal.clone()),
            (None, true) => sync::SettledBy::Never,
            (None, false) => sync::SettledBy::Always(self.inner.authority.get()),
        };
        let gate = match (&remote, self.link_hint()) {
            (Some(signal), Some(hint)) => {
                let deadline =
                    sync::remote_deadline(&opts, non_durable, sync::may_wait(hint.get()));
                let live = hint.get().hint == RemoteLinkHint::Live;
                if let Some(Some(at)) = deadline {
                    self.inner.wakes.wake_at(at);
                    self.inner.wakes.note_link(hint.get());
                }
                deadline.map(|deadline| sync::Gate {
                    signal: signal.clone(),
                    hint,
                    live,
                    deadline,
                    relay_view: non_durable && opts.tier != ReadTier::Remote,
                    wakes: Rc::clone(&self.inner.wakes),
                })
            }
            _ => None,
        };
        let include_deleted = read.include_deleted;
        let inner = self.inner.without_overlay();
        let backlog = fanout::Backlog::Reader(opts.backlog_rows);
        let (queue, group) = match inner
            .subscribe_member(prepared, identity, read, settled, backlog, dormant)
            .await
        {
            Ok(member) => member,
            Err(error) => {
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
            dropped: inner.fan.borrow().dropped.clone(),
            gate,
            group,
            refused: remote.clone(),
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
    pub async fn row_provenance(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<(u64, String, u64, String)>, Error> {
        let _operation = self.write_operation().await;
        let active_schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let schema = active_schema.clone();
        let Some(table) = schema.table(table).ok().or_else(|| schema.by_stored(table)) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT \"$createdAt\", \"$createdBy\", COALESCE(\"$updatedAt\", \"$createdAt\"), \
             COALESCE(\"$updatedBy\", \"$createdBy\") FROM {} WHERE id = ?1 ORDER BY _branch LIMIT 1",
            quote(&table.stored)
        );
        let stamps: Option<(Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = inner
            .conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .await
            .optional()
            .map_err(storage)?;
        let Some((created_at, Some(created_by), updated_at, Some(updated_by))) = stamps else {
            return Ok(None);
        };
        let canonical = async |code: i64| -> Result<String, Error> {
            let (account, issuer, subject) = inner.author_parts(code).await?;
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
            canonical(created_by).await?,
            updated_at.unwrap_or(0) as u64,
            canonical(updated_by).await?,
        )))
    }

    /// How many distinct authors this database has interned.
    #[doc(hidden)]
    pub async fn interned_authors_for_test(&self) -> usize {
        let _operation = self.write_operation().await;
        self.inner
            .without_overlay()
            .conn
            .cached_row("SELECT COUNT(*) FROM _jazz_authors", [], |row| {
                row.get::<_, i64>(0)
            })
            .await
            .map_or(0, |count| count as usize)
    }

    pub fn runtime_stats_for_test(&self) -> RuntimeStats {
        let live = self.inner.fan.borrow().live_members();
        let shapes: BTreeSet<Shape> = live.iter().map(|(shape, _)| *shape).collect();
        RuntimeStats {
            active_subscriptions: live.len(),
            active_prepared_shapes: shapes.len(),
            active_shape_params: live.iter().filter(|(_, bound)| *bound).count(),
        }
    }

    pub fn fanout_stats(&self) -> fanout::FanoutStats {
        fanout::FanoutStats {
            coalesced: self.inner.coalesced.get(),
            ..self.inner.fanout_stats.borrow().clone()
        }
    }

    /// A one-shot read with array relations nested as a subscription nests
    /// them, as `identity` (`None`: unchecked): the full re-run a live
    /// result is compared with in tests.
    #[cfg(any(test, feature = "testing"))]
    pub async fn all_nested_for_test(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        identity: Option<AuthorSubject>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let read = ReadSpec::of(&opts, inner).await?;
        let rows = inner
            .run_for_reader(prepared, identity, &read, false)
            .await?;
        inner.nest_relations(prepared, identity, &read, rows).await
    }

    /// Deliver the rows written since the last dispatch to the groups they
    /// wake (keyed invalidation; see `fanout`).
    async fn notify(&self) {
        let inner = self.inner.without_overlay();
        let changes = std::mem::take(&mut *inner.changed.borrow_mut());
        if !inner.sessions.borrow().is_empty() {
            inner
                .since_tick
                .borrow_mut()
                .extend(changes.iter().cloned());
        }
        inner.dispatch(changes).await;
    }

    // ---- writes ---------------------------------------------------------

    /// A shard hold may be granted while this operation is queued or fetching
    /// chunks. Check under the lease on every attempt, and release it while
    /// waiting so the shard can land or release the part.
    async fn wait_for_rows<'a>(
        &'a self,
        mut operation: futures::lock::MutexGuard<'a, ()>,
        rows: &[[u8; 16]],
    ) -> Result<futures::lock::MutexGuard<'a, ()>, Error> {
        while !rows.is_empty() && self.inner.shard.borrow().holds_any(rows) {
            // An origin callback owns the outer lease until it returns.
            if self.origin.is_some() {
                self.inner.refuse_held(rows)?;
            }
            drop(operation);
            std::future::poll_fn(|cx| self.inner.shard.borrow_mut().wait_unheld(rows, cx)).await;
            operation = self.write_operation().await;
        }
        Ok(operation)
    }

    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        mut opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        if opts.row_id.is_none() {
            opts.row_id = Some(self.inner.ids.borrow_mut().next_row_id());
        }
        let who = who(opts.identity);
        let op = StagedOp::Insert(table.to_owned(), cells, opts);
        self.write_with_fetch(op, who).await
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
        self.write_with_fetch(op, who).await
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
        self.write_with_fetch(op, who).await
    }

    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<WriteHandle, Error> {
        let who = who(opts.identity);
        let op = StagedOp::Delete(table.to_owned(), row, opts);
        self.write_with_fetch(op, who).await
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
        self.write_with_fetch(op, who).await
    }

    async fn write_with_fetch(&self, op: StagedOp, who: Who) -> Result<WriteHandle, Error> {
        self.write_streamed_with_fetch(op, who, None).await
    }

    /// [`Db::write_with_fetch`] for a row whose stream and encrypted
    /// siblings may already have been sealed (`streamed`). With encryption
    /// hooks, each attempt seals the write as its caller named it, inside
    /// the write's own transaction, before anything chunks or stores it.
    pub(crate) async fn write_streamed_with_fetch(
        &self,
        op: StagedOp,
        who: Who,
        streamed: Option<large_ops::StreamedSeal<'_>>,
    ) -> Result<WriteHandle, Error> {
        let row = op.row().map(|row| *row.0.as_bytes());
        // Boxed: a sealed write's state stays off the caller's stack,
        // which a client thread keeps small.
        if self.inner.e2ee_hooks().is_some() {
            return Box::pin(self.fetching_with(row.as_slice(), op, async |op| {
                self.write_sealed(op, who, streamed).await
            }))
            .await;
        }
        let op = {
            let _operation = self.write_operation().await;
            self.prepare_write(op)?
        };
        self.fetching_with(row.as_slice(), op, async |op| {
            self.write_prepared(op, who).await
        })
        .await
    }

    /// One write. On a client, a local (unauthenticated) write is also an
    /// upload: it is recorded for the authority, which decides its fate.
    fn prepare_write(&self, op: StagedOp) -> Result<PreparedWrite, Error> {
        let schema = self.operation_schema();
        self.inner.as_ref().prepare_write(schema, op)
    }

    async fn write_prepared(&self, mut op: PreparedWrite, who: Who) -> Result<WriteHandle, Error> {
        self.commit_prepared(op.op.row(), async |inner, seq, origin| {
            let handle = inner.apply_authored(&mut op, who, seq, origin).await?;
            Ok((handle, [op]))
        })
        .await
    }

    async fn commit_prepared<Ops: AsRef<[PreparedWrite]>>(
        &self,
        row: Option<RowUuid>,
        apply: impl AsyncFnOnce(
            &Store,
            u64,
            Option<([u8; 16], u64)>,
        ) -> Result<(WriteHandle, Ops), Error>,
    ) -> Result<WriteHandle, Error> {
        self.inner.without_overlay().require_mutation_admission()?;
        if let Some(row) = row {
            self.inner.refuse_held(&[*row.0.as_bytes()])?;
        }
        self.with_upload(async || {
            let (prior, mark) = {
                let inner = self.inner.without_overlay();
                (inner.seq.get(), inner.changed.borrow().len())
            };
            let (notify, handle, ops) = self.write_unnotified(apply).await?;
            // A no-op handle names an existing version, so only record an
            // upload for a new commit. Its upload joins the transaction.
            if notify
                && let Err(error) = self
                    .record_upload(ops.as_ref(), handle.tx, sync::Kind::Mergeable, None)
                    .await
            {
                let inner = self.inner.without_overlay();
                inner.seq.set(prior);
                inner.changed.borrow_mut().truncate(mark);
                return Err(error);
            }
            if notify {
                self.notify().await;
            }
            Ok(handle)
        })
        .await
    }

    /// Seal a write and commit its internal companion rows before it, in
    /// the same transaction.
    ///
    /// The caller holds the write operation's lease ([`Db::write_operation`],
    /// taken by [`Db::fetching_with`] for every caller), as
    /// [`Db::prepare_write`]'s callers do: the schema it reads stays the one
    /// the write is sealed and applied under.
    async fn write_sealed(
        &self,
        mut op: StagedOp,
        who: Who,
        streamed: Option<large_ops::StreamedSeal<'_>>,
    ) -> Result<WriteHandle, Error> {
        debug_assert!(
            self.holds_write_operation(),
            "a sealed write runs under the write operation's lease"
        );
        let schema = self.operation_schema();
        let mut companions = Vec::new();
        let written = self
            .commit_prepared(op.row(), async |inner, seq, origin| {
                let staged = inner.seal_op(&schema, &mut op, streamed).await?;
                companions = e2ee_write::companion_names(&staged);
                let member = match &op {
                    StagedOp::Insert(table, _, options) => {
                        inner.creator_member(&schema, table, options).await?
                    }
                    _ => None,
                };
                let mut ops = Vec::with_capacity(staged.len() + 1);
                // Each apply clears its derived values and nodes. Save the
                // caller's edits while applying the companions.
                let given = (inner.derived.take(), inner.pending_nodes.take());
                for companion in staged {
                    let mut companion = inner.prepare_write(schema.clone(), companion)?;
                    inner
                        .apply_authored(&mut companion, who, seq, origin)
                        .await?;
                    ops.push(companion);
                }
                inner.derived.borrow_mut().extend(given.0);
                inner.pending_nodes.borrow_mut().extend(given.1);
                let mut op = inner.prepare_write(schema.clone(), op)?;
                let handle = inner.apply_authored(&mut op, who, seq, origin).await?;
                ops.push(op);
                if let Some(member) = member {
                    let mut member = inner.creator_member_write(schema, member).await?;
                    inner.apply_authored(&mut member, who, seq, origin).await?;
                    ops.push(member);
                }
                Ok((handle, ops))
            })
            .await;
        if written.is_err()
            && !companions.is_empty()
            && let Some(hooks) = self.inner.e2ee_hooks()
        {
            hooks.companions_dropped(&companions);
        }
        written
    }

    /// On a replica, a local commit and its upload record are one SQLite
    /// transaction.
    async fn with_upload<R>(&self, f: impl AsyncFnOnce() -> Result<R, Error>) -> Result<R, Error> {
        let wrap = {
            let inner = self.inner.without_overlay();
            inner.client().is_some()
        };
        if !wrap {
            return f().await;
        }
        let conn = self.inner.without_overlay().conn.clone();
        conn.db_scope("upload_local", async || f().await).await
    }

    /// Run one write atomically. Returns whether it committed a change;
    /// the caller notifies subscribers once its upload record is in.
    async fn write_unnotified<Ops>(
        &self,
        apply: impl AsyncFnOnce(
            &Store,
            u64,
            Option<([u8; 16], u64)>,
        ) -> Result<(WriteHandle, Ops), Error>,
    ) -> Result<(bool, WriteHandle, Ops), Error> {
        let standalone = {
            let inner = self.inner.without_overlay();
            let seq = inner.fresh_seq();
            let conn = inner.conn.clone();
            let mark = inner.changed.borrow().len();
            // A standalone commit takes the writer id a thin client's write
            // gave it (`Db::write_as_origin`), on every attempt of the write
            // (`Db::fetching_with` runs one again). Its seq is not used up
            // unless it commits, and the next commit takes it: what this one
            // put in `own_order` for it goes back, so no other commit
            // inherits its writer id, nor it another's (and `fresh_seq`
            // drops whatever an exit after this one leaves).
            let origin = self.origin.as_ref().and_then(|origin| origin.key.get());
            let prior = inner.own_order.borrow().get(&seq).copied();
            let restore = |inner: &Store| match prior {
                Some(order) => {
                    inner.own_order.borrow_mut().insert(seq, order);
                }
                None => {
                    inner.own_order.borrow_mut().remove(&seq);
                }
            };
            let applied = conn
                .db_scope("w", async || {
                    let (handle, ops) = apply(inner, seq, origin).await?;
                    // The authority records the fate of a commit made under a
                    // client's id in the commit itself.
                    {
                        let wrote = handle.seq == inner.own_pos(seq);
                        if let (true, true, Some((node, tx))) =
                            (wrote, inner.me.get() == sync::ACCEPTED, origin)
                        {
                            inner.record_fate(node, tx, &Ok(())).await?;
                            inner.record_settled(node, tx, Some(Pos::at(seq))).await?;
                            // Its entry ages out if all it wrote is latest-only,
                            // as the commit's version declares (SPEC 8 §8.14).
                            let prunable = {
                                let changed = inner.changed.borrow();
                                changed.len() > mark
                                    && changed[mark..].iter().all(|(stored, ..)| {
                                        inner
                                            .retention_of(inner.owner_version.get())
                                            .contains(stored)
                                    })
                            };
                            inner.note_origin_entry(node, tx, prunable).await?;
                        }
                        Ok((wrote, handle, ops))
                    }
                })
                .await;
            match applied {
                Ok((wrote, handle, ops)) => {
                    if !wrote {
                        restore(inner);
                    } else if let Some(origin) = &self.origin {
                        // The client's write is made: no later commit of the
                        // same call takes its id.
                        origin.key.set(None);
                    }
                    if wrote {
                        inner.seq.set(inner.seq.get().max(seq));
                    }
                    (wrote, handle, ops)
                }
                Err(error) => {
                    inner.changed.borrow_mut().truncate(mark);
                    restore(inner);
                    return Err(error);
                }
            }
        };
        Ok(standalone)
    }

    // ---- schema evolution -------------------------------------------------

    /// Admit `schema`, with the migration lenses into it (lenses into other
    /// versions are ignored), as a deploy does. The stored tables grow to
    /// hold it: new tables, new columns (`ADD COLUMN ... DEFAULT`, no rows
    /// rewritten) and new indexes; a lens's added columns take its
    /// defaults, and a renamed table or column keeps the stored table or
    /// column of its source, so every version reads and writes the same
    /// rows (see [`crate::names`]). Changing a column's type in place is
    /// not supported yet, and a new required column needs a default.
    ///
    /// Admitting never makes `schema` current: sessions that declare it
    /// read and write by its names, and a store installs it as its own
    /// schema only when it opens under it (a running store never switches
    /// schema).
    pub async fn admit_schema_with_lenses(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
    ) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        self.admit(schema, lenses).await?;
        self.inner.without_overlay().publish_schema();
        Ok(())
    }

    /// Apply `change` to the store's catalogue in one transaction: publish
    /// its rows, then admit its versions in order. A change that fails anywhere leaves the file and
    /// this database as they were.
    pub async fn apply_catalogue(
        &self,
        change: crate::catalogue::CatalogueChange,
    ) -> Result<(), Error> {
        self.apply_catalogue_checked(change, &[], async |_| Ok(()))
            .await
    }

    /// Apply a catalogue change and run a host's check after admission
    /// assigns stored names. Then keep the new host values in the same
    /// transaction. A failed check or conflicting host value rolls the whole
    /// change back. The check runs inside this call's operation, on a handle
    /// of its own.
    pub async fn apply_catalogue_checked(
        &self,
        change: crate::catalogue::CatalogueChange,
        kept: &[(crate::host::HostTable, Vec<u8>, Vec<u8>)],
        check: impl AsyncFnOnce(&Db) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        self.apply_catalogue_now(change, kept, check).await
    }

    /// A handle on this store for a callback that runs while this handle's
    /// operation holds the store: its own calls serialize with each other,
    /// not with the operation that runs it.
    fn under_operation(&self) -> Db {
        Db {
            inner: self.inner.clone(),
            view: self.view.clone(),
            defer_fetches: self.defer_fetches,
            origin: Some(OriginWriteContext {
                key: Cell::new(None),
                operation: futures::lock::Mutex::new(()),
            }),
        }
    }

    async fn apply_catalogue_now(
        &self,
        change: crate::catalogue::CatalogueChange,
        kept: &[(crate::host::HostTable, Vec<u8>, Vec<u8>)],
        check: impl AsyncFnOnce(&Db) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let versions_changed = !change.admit.is_empty();
        let saved = {
            let inner = self.inner.without_overlay();
            (
                inner.physical(),
                inner.names(),
                inner.admitted(),
                inner.owner(),
                inner.owner_version.get(),
            )
        };
        let conn = self.inner.without_overlay().conn.clone();
        let result = conn
            .db_scope("jazz_catalogue", async || {
                crate::catalogue::put(&conn, &change.put).await?;
                for (schema, lenses) in change.admit {
                    self.admit(schema, &lenses).await?;
                }
                check(&self.under_operation()).await?;
                for (table, key, value) in kept {
                    if let Some(existing) =
                        crate::host::put_if_absent(&conn, *table, key, value).await?
                        && existing != *value
                    {
                        return Err(Error::new(
                            ErrorCode::Schema,
                            "this change keeps a value that conflicts with the one kept before",
                        ));
                    }
                }
                Ok(())
            })
            .await;
        if result.is_err() {
            let inner = self.inner.without_overlay();
            let (physical, names, admitted, owner, owner_version) = saved;
            inner.physical.replace(physical);
            inner.names.replace(names);
            inner.admitted.replace(admitted);
            inner.owner.replace(owner);
            inner.owner_version.set(owner_version);
            inner.reset_everything().await;
        } else if versions_changed {
            self.inner.without_overlay().publish_schema();
        }
        result
    }

    /// Publish the bundle that selects `schema`'s structure as the write
    /// schema, with `schema`'s policies, and return its id (see
    /// [`crate::catalogue::bundle_id`]). Nothing is activated: sessions
    /// pinned to it are governed by it. Publishing it again changes nothing.
    pub async fn publish_bundle(
        &self,
        schema: &jazz_model::model::public_schema::Schema,
    ) -> Result<crate::catalogue::BundleId, Error> {
        let hash = crate::catalogue::structural_hash(schema);
        let permissions = crate::catalogue::permissions_of(schema);
        let id = crate::catalogue::bundle_id(&hash, &permissions)?;
        let published_at = crate::catalogue::now_millis();
        self.apply_catalogue(crate::catalogue::CatalogueChange {
            put: crate::catalogue::CataloguePut {
                bundles: vec![(hash, permissions, published_at)],
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
        Ok(id)
    }

    /// The current schema, with its policies.
    pub fn current_schema(&self) -> JazzSchema {
        self.inner.published_schema.borrow().jazz.clone()
    }

    /// The store's catalogue: every schema, lens and permissions revision
    /// it keeps (see [`crate::catalogue`]).
    pub async fn catalogue(&self) -> Result<crate::catalogue::CatalogueRows, Error> {
        let _operation = self.write_operation().await;
        crate::catalogue::read(&self.inner.without_overlay().conn).await
    }

    /// The direct targets of this schema's published forward migrations.
    pub async fn catalogue_successors(
        &self,
        hash: &jazz_model::model::public_schema::SchemaHash,
    ) -> Result<HashSet<jazz_model::model::public_schema::SchemaHash>, Error> {
        let _operation = self.write_operation().await;
        crate::catalogue::successors(&self.inner.without_overlay().conn, hash).await
    }

    /// The schemas reachable from this hash through published forward migrations.
    pub async fn catalogue_descendants(
        &self,
        hash: &jazz_model::model::public_schema::SchemaHash,
    ) -> Result<HashSet<jazz_model::model::public_schema::SchemaHash>, Error> {
        let _operation = self.write_operation().await;
        crate::catalogue::descendants(&self.inner.without_overlay().conn, hash).await
    }

    /// A published schema and the identities of its tables and columns,
    /// read together from the current catalogue transaction.
    pub async fn catalogue_schema_and_stored_names(
        &self,
        hash: &jazz_model::model::public_schema::SchemaHash,
    ) -> Result<
        (
            jazz_model::model::public_schema::Schema,
            crate::values::StoredNames,
        ),
        Error,
    > {
        let _operation = self.write_operation().await;
        let conn = &self.inner.without_overlay().conn;
        let schema = crate::catalogue::schema(conn, hash)
            .await?
            .ok_or_else(|| schema_error(format!("schema {hash} is missing from the catalogue")))?;
        let version = JazzSchema::new(&schema)
            .map_err(|error| schema_error(error.to_string()))?
            .version_id();
        // Read the transaction's catalogue: another instance can have
        // admitted this version since the runtime loaded its own names.
        let mut statement = conn
            .prepare_cached(
                "SELECT tbl, col, stored_tbl, stored_col FROM _jazz_names \
             WHERE n = (SELECT n FROM _jazz_versions WHERE version = ?1)",
            )
            .map_err(storage)?;
        let rows: Vec<(String, String, String, String)> = statement
            .query_map([version.as_bytes().as_slice()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .await
            .map_err(storage)?
            .collect()
            .await
            .map_err(storage)?;
        let mut names = crate::values::StoredNames::default();
        for (table, column, stored_table, stored_column) in rows {
            let (_, columns) = names
                .tables
                .entry(table)
                .or_insert_with(|| (stored_table, BTreeMap::new()));
            if !column.is_empty() {
                columns.insert(column, stored_column);
            }
        }
        Ok((schema, names))
    }

    /// Admit `schema` with the lenses into it.
    async fn admit(&self, schema: JazzSchema, lenses: &[MigrationLens]) -> Result<(), Error> {
        {
            let inner = self.inner.without_overlay();
            let version = schema.version_id();
            let mut defaults = BTreeMap::new();
            let mut copies = BTreeMap::new();
            for lens in lenses.iter().filter(|lens| lens.target == version) {
                let effects = LensEffects::of(lens)?;
                defaults.extend(effects.defaults);
                copies.extend(effects.copies);
            }
            let base = inner.owner().names();
            let mut target = inner.names().resolve(&schema, lenses, Some(&base))?;
            let cells = inner.lens_cells(&target, lenses)?;
            let admitted: Vec<Schema> = inner
                .admitted()
                .iter()
                .map(|(_, admitted)| inner.named(admitted))
                .collect();
            // Before a lens's defaults fill in: a branch column's default
            // is the schema's own, so a replica that never saw the lens
            // keys rows the same way.
            inner.names().check_branches(&target)?;
            inner.names().check_types(&target)?;
            for table in target.tables.values_mut() {
                let physical_schema = inner.physical();
                let stored = physical_schema.tables.get(&table.stored);
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
            inner.write_admission(&target, cells, false).await?;
            if !inner
                .admitted()
                .iter()
                .any(|(admitted, _)| *admitted == version)
            {
                Rc::make_mut(&mut *inner.admitted.borrow_mut()).push((version, schema));
            }
        }
        let inner = self.inner.without_overlay();
        inner.reset_everything().await;
        // Clients store rows by these names: they learn the new version
        // before its rows.
        for session in &mut *inner.sessions.borrow_mut() {
            session.owe_catalogue();
        }
        Ok(())
    }
}

/// Why a replica refuses an upstream whose authority namespace is not the
/// one it mirrors (INV-SYNC-54). Nothing on the wire tells a recreated server
/// store from another app's, so the message names both, and the one way on.
/// A store keeps `version` under other names than its authority, which never
/// confirmed them: it was opened without the app's migrations.
pub(crate) fn store_opened_without_migrations(version: &crate::ids::SchemaVersionId) -> Error {
    Error::new(
        ErrorCode::StoreOpenedWithoutMigrations,
        format!(
            "this store keeps schema version {} under other names than the server, which never confirmed them: it was opened without the app's migrations, so it took this version as the app's first. That happens when the app ships no migration into this version: `defineApp(schema, {{ migrations: \"none\" }})`, a migrations/index.ts that misses it (run `npx jazz-tools validate`), or React Native before alpha.61. Ship the app's migrations (see https://jazz.tools/docs/schemas/migrations), then rebuild this store, or reset it with deleteClientStorage(), which drops its unsynced writes",
            uuid::Uuid::from_bytes(*version.as_bytes())
        ),
    )
}

pub(crate) fn authority_changed() -> Error {
    Error::new(
        ErrorCode::AuthorityChanged,
        "the server's authority changed: its catalogue keeps a different authority namespace \
         than this store, so its store was recreated (a deleted data directory, a restarted \
         in-memory server, a new environment behind the same URL) or it is another app's. \
         This store's rows and unsynced writes belong to the old history and do not sync with \
         it; reset the local store (`db.deleteClientStorage()` in jazz-tools, or open a fresh \
         store) and reconnect, or point the client back at the old server",
    )
}

impl Store {
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
    pub(crate) async fn mirror_catalogue(
        &self,
        namespace: Option<[u8; 16]>,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
        defaults: Vec<sync::CatalogueDefault>,
        confirmed: &[([u8; 16], [u8; 16])],
    ) -> Result<bool, Error> {
        let known = self.namespace().await?;
        if let (Some(known), Some(namespace)) = (known, namespace)
            && known != namespace
        {
            return Err(authority_changed());
        }
        // A version this build cannot read is skipped, with its lens
        // cells; the versions it knows are taken as usual.
        let (versions, cells) = self.skip_unsupported_versions(versions, cells);
        let mirrored = !versions.is_empty();
        if mirrored {
            self.mirror_versions(versions, cells, defaults).await?;
        }
        let learned = known.is_none() && namespace.is_some();
        if let (true, Some(namespace)) = (learned, namespace) {
            self.persist_namespace(namespace).await?;
        }
        let conn = &self.conn;
        let mut names = self.names();
        let told = Rc::make_mut(&mut names).confirm(conn, confirmed).await? || learned;
        self.names.replace(names);
        // This store's own sessions (a worker's tabs) are told what it now
        // knows of the authority's catalogue.
        if told && !mirrored {
            for session in &mut *self.sessions.borrow_mut() {
                session.owe_catalogue();
            }
        }
        Ok(mirrored)
    }

    /// `versions` without the ones this build cannot read (recorded in
    /// `unsupported_versions`), and `cells` without those versions' cells.
    fn skip_unsupported_versions(
        &self,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
    ) -> (Vec<sync::CatalogueVersion>, Vec<sync::CatalogueCell>) {
        let mut skipped = BTreeSet::new();
        let versions = versions
            .into_iter()
            .filter(|version| match &version.schema {
                sync::CatalogueSchema::Known(_) => true,
                sync::CatalogueSchema::Unsupported { id, error, .. } => {
                    tracing::warn!(
                        version = %id.0,
                        "skipping a catalogue schema version this build cannot read: {error}"
                    );
                    self.unsupported_versions
                        .borrow_mut()
                        .insert(*id, error.clone());
                    skipped.insert(*id.0.as_bytes());
                    false
                }
            })
            .collect();
        let cells = cells
            .into_iter()
            .filter(|cell| !skipped.contains(&cell.since))
            .collect();
        (versions, cells)
    }

    /// Take the upstream's versions, names and lens cells: whole or not at
    /// all, in the file and in memory. A catalogue this store cannot take
    /// (a stored column it keeps with another type) leaves it unchanged.
    async fn mirror_versions(
        &self,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
        defaults: Vec<sync::CatalogueDefault>,
    ) -> Result<(), Error> {
        let (physical, names, admitted) = (self.physical(), self.names(), self.admitted());
        let tags = self.names().tags().snapshot();
        let conn = self.conn.clone();
        let mirrored = conn
            .db_scope("jazz_mirror", async || {
                self.mirror_versions_steps(versions, cells, defaults).await
            })
            .await;
        if let Err(error) = mirrored {
            self.physical.replace(physical);
            self.names.replace(names);
            self.admitted.replace(admitted);
            self.names().tags().restore(tags);
            return Err(error);
        }
        let owner = Rc::new(self.named(&self.owner().jazz));
        self.owner.replace(owner.clone());
        self.reset_everything().await;
        // This store's own sessions (a worker's tabs) are sent the versions
        // it just took.
        for session in &mut *self.sessions.borrow_mut() {
            session.owe_catalogue();
        }
        Ok(())
    }

    /// Give each column of mirrored `target` this store does not store yet
    /// the default its upstream added it with: a lens's default for a
    /// column it adds is in no schema, and rows written before the column
    /// read it (as on the authority) only if the `ADD COLUMN` gives it.
    /// A default that does not fit the column is left out (the column then
    /// reads as before) rather than refusing the whole catalogue: it only
    /// changes how rows written before the column read.
    fn add_with_upstream_defaults(
        &self,
        target: &mut Schema,
        defaults: &BTreeMap<(&str, &str), &Value>,
    ) {
        if defaults.is_empty() {
            return;
        }
        for table in target.tables.values_mut() {
            let physical_schema = self.physical();
            let stored = physical_schema.tables.get(&table.stored);
            for column in &mut table.columns {
                // The upstream's default wins over the schema's own, as the
                // lens's does on the authority (see `admit`).
                if stored.is_some_and(|stored| stored.column(&column.stored).is_some()) {
                    continue;
                }
                let Some(value) = defaults.get(&(table.stored.as_str(), column.stored.as_str()))
                else {
                    continue;
                };
                if default_fits(column, value) {
                    column.default = Some((*value).clone());
                }
            }
        }
    }

    /// The defaults the columns of `versions` were added with in this
    /// store, read from the stored tables' DDL, which is what rows written
    /// before a column read (see [`sync::CatalogueDefault`]). An enum
    /// default goes by its variant's name: tags are this store's own.
    pub(crate) async fn added_defaults<'a>(
        &self,
        versions: impl IntoIterator<Item = (&'a JazzSchema, &'a crate::values::StoredNames)>,
    ) -> Result<Vec<sync::CatalogueDefault>, Error> {
        let mut found = BTreeMap::new();
        let mut ddl: HashMap<String, BTreeMap<String, String>> = HashMap::new();
        for (jazz, names) in versions {
            let schema = Schema::stored_as(jazz, names, self.names().tags());
            for table in schema.tables.values() {
                if !ddl.contains_key(&table.stored) {
                    let defaults = ddl_defaults(&self.conn, &table.stored).await?;
                    ddl.insert(table.stored.clone(), defaults);
                }
                let by_name: BTreeMap<String, String> = self
                    .names()
                    .enum_defaults(&table.stored)
                    .into_iter()
                    .collect();
                for column in &table.columns {
                    let key = (table.stored.clone(), column.stored.clone());
                    if found.contains_key(&key) {
                        continue;
                    }
                    let Some(literal) = ddl[&table.stored].get(&column.stored) else {
                        continue;
                    };
                    let value = match by_name.get(&column.stored) {
                        Some(name) => {
                            let name = Value::String(name.clone());
                            Some(match column.nullable() {
                                true => Value::Nullable(Some(Box::new(name))),
                                false => name,
                            })
                        }
                        None => {
                            // A default this store cannot read is left out
                            // (see `add_with_upstream_defaults`), not a
                            // reason to send no catalogue.
                            let Ok(cell) = ddl_literal(&self.conn, literal).await else {
                                continue;
                            };
                            if matches!(cell, Sql::Null) {
                                continue;
                            }
                            crate::values::column_from_sql(column, cell.as_ref(), &self.chunks)
                                .await
                                .ok()
                                .flatten()
                        }
                    };
                    if let Some(value) = value {
                        found.insert(key, value);
                    }
                }
            }
        }
        Ok(found
            .into_iter()
            .map(|((table, column), default)| sync::CatalogueDefault {
                table,
                column,
                default,
            })
            .collect())
    }

    async fn mirror_versions_steps(
        &self,
        versions: Vec<sync::CatalogueVersion>,
        cells: Vec<sync::CatalogueCell>,
        defaults: Vec<sync::CatalogueDefault>,
    ) -> Result<(), Error> {
        use crate::names::{LensCell, LensCellRule};
        let defaults: BTreeMap<(&str, &str), &Value> = defaults
            .iter()
            .map(|d| ((d.table.as_str(), d.column.as_str()), &d.default))
            .collect();
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
                let sync::CatalogueSchema::Known(version) = entry.schema else {
                    unreachable!("unsupported versions are skipped first")
                };
                (version.id, names, version.schema)
            })
            .collect();
        let mut fresh = std::collections::HashSet::new();
        for (version, names, _) in &entries {
            match self.names().get(version) {
                Some(local) if local == names => {}
                // No authority ever confirmed this version's names here: the
                // store minted them itself, opened without the lenses that
                // lead to it (a client that dropped the app's migrations).
                Some(_) if !self.names().confirmed(version) => {
                    return Err(store_opened_without_migrations(version));
                }
                Some(_) => {
                    return Err(crate::values::query_error(format!(
                        "this store keeps schema version {} under other names than its upstream: \
                         the two took different histories into it, so the app's migrations \
                         differ between them (one was opened without them, or with a \
                         migrations/index.ts from another branch). Ship the same migrations \
                         everywhere (`npx jazz-tools validate` regenerates migrations/index.ts; \
                         see https://jazz.tools/docs/schemas/migrations), then reset this store",
                        uuid::Uuid::from_bytes(*version.as_bytes())
                    )));
                }
                None => {
                    fresh.insert(*version);
                }
            }
        }
        for (version, names, schema) in entries {
            let jazz = if version == self.owner_version.get() {
                self.owner().jazz.clone()
            } else {
                schema
            };
            let mut target = Schema::stored_as(&jazz, &names, self.names().tags());
            self.add_with_upstream_defaults(&mut target, &defaults);
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
            self.names().check_types(&target)?;
            self.write_admission(&target, fills, version == self.owner_version.get())
                .await?;
            if !self.admitted().iter().any(|(id, _)| *id == version) {
                Rc::make_mut(&mut *self.admitted.borrow_mut()).push((version, jazz));
            }
        }
        let conn = &self.conn;
        if cells.is_empty() {
            return Ok(());
        }
        let mut names = self.names();
        Rc::make_mut(&mut names)
            .record_lens_cells(conn, cells)
            .await?;
        self.names.replace(names);
        Ok(())
    }

    /// An upstream image as this store keeps it: `None` for a table it
    /// does not store, without columns it does not (an upstream's that no
    /// version this replica knows names).
    pub(crate) fn stored_image(&self, mut image: sync::Image) -> Option<sync::Image> {
        let physical_schema = self.physical();
        let table = physical_schema.by_stored(&image.table)?;
        image
            .cells
            .retain(|(name, _)| name.starts_with('$') || table.by_stored(name).is_some());
        Some(image)
    }

    /// Write an admitted version's stored tables and columns, its lens
    /// copies, names, lens cells and enum tags: whole or not at all, in the
    /// file and in memory. A `current` target (the schema made current)
    /// has its enum columns' tags put in its declaration order.
    async fn write_admission(
        &self,
        target: &Schema,
        cells: Vec<crate::names::LensCell>,
        current: bool,
    ) -> Result<(), Error> {
        let (physical, names) = (self.physical(), self.names());
        let tags = self.names().tags().snapshot();
        let conn = self.conn.clone();
        let written = conn
            .db_scope("jazz_admit", async || {
                self.write_admission_steps(target, cells, current).await
            })
            .await;
        if written.is_err() {
            self.physical.replace(physical);
            self.names.replace(names);
            self.names().tags().restore(tags);
        }
        written
    }

    async fn write_admission_steps(
        &self,
        target: &Schema,
        cells: Vec<crate::names::LensCell>,
        current: bool,
    ) -> Result<(), Error> {
        // Variants are tagged before any DDL, so an enum column added with
        // a default stores the default's tag. A new copy starts with its
        // source's tags.
        for cell in &cells {
            if let crate::names::LensCellRule::Copy(source) = &cell.rule {
                self.names()
                    .copy_tags(&self.conn, &cell.table, source, &cell.column)
                    .await?;
            }
        }
        self.names().mint(&self.conn, target).await?;
        // The current schema's declaration order is tag order, before any
        // DDL, so a column added with a default stores the default's tag.
        if current {
            self.align_tags(target).await?;
        }
        let ddl = Rc::make_mut(&mut *self.physical.borrow_mut()).union(target)?;
        execute_script(&self.conn, &ddl).await?;
        // A new copy holds its source's values, in every row version.
        for cell in &cells {
            if let crate::names::LensCellRule::Copy(source) = &cell.rule {
                self.fill_copy(&cell.table, &cell.column, source).await?;
            }
        }
        let mut names = self.names();
        let next = Rc::make_mut(&mut names);
        next.admit(&self.conn, target).await?;
        next.record_lens_cells(&self.conn, cells).await?;
        self.names.replace(names);
        Ok(())
    }

    /// Fill copy `column` of stored table `table`, in every row version,
    /// from `source`. An enum copy's tags are its own: a source cell is
    /// mapped to the copy's tag for the same name, which is the same tag
    /// unless the copy's column had tags before.
    async fn fill_copy(&self, table: &str, column: &str, source: &str) -> Result<(), Error> {
        let (from, to) = (
            self.names().tags().column(table, source),
            self.names().tags().column(table, column),
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
        self.write_enum_cells(table, column, source, &arms).await
    }

    /// Renumber the tags of every enum column of `schema` (the current
    /// schema) so its declared variants are in tag order (see
    /// [`crate::names::Names::align`]), rewriting each changed column's
    /// cells in every row version.
    async fn align_tags(&self, schema: &Schema) -> Result<(), Error> {
        for table in schema.tables.values() {
            for column in &table.columns {
                let Some(variants) = &column.variants else {
                    continue;
                };
                let changed = self
                    .names()
                    .align(&self.conn, &table.stored, &column.stored, variants)
                    .await?;
                // A column not stored yet has no cells.
                let stored = self
                    .physical()
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
                self.write_enum_cells(&table.stored, &column.stored, &column.stored, &arms)
                    .await?;
            }
        }
        Ok(())
    }

    /// Set enum column `column` of stored table `table`, in every row
    /// version, to `source`'s cells with each tag `arms` (`WHEN old THEN
    /// new` arms) names mapped.
    async fn write_enum_cells(
        &self,
        table: &str,
        column: &str,
        source: &str,
        arms: &str,
    ) -> Result<(), Error> {
        let array = self
            .names()
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
                .await
                .map_err(storage)?;
        }
        Ok(())
    }
}

/// A version made current may not drop a branch column of the current
/// one (SPEC 4, INV-BVIEW-19): an older version admitted beside it names
/// that column by its default. The other branch-column changes are refused
/// for every admitted version ([`crate::names::Names::check_branches`]).
/// A branch column `next` keeps by name but stores elsewhere (its type
/// changed) is refused: `current`'s rows are keyed by the old one.
fn check_branch_retype(current: &Table, next: &Table) -> Result<(), Error> {
    for column in &current.branch_by {
        let stored = current.stored_name(column);
        if let Some(kept) = next.columns.iter().find(|c| c.name == *column)
            && kept.stored != stored
        {
            return Err(unsupported(format!(
                "changing the type of branch column {}.{column}",
                next.name
            )));
        }
    }
    Ok(())
}

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
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::InheritsFrom { .. } => true,
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
        coalesced: false,
        unreadable: None,
    }
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

impl Drop for Store {
    fn drop(&mut self) {
        // An abandoned handle's overlay never reaches the file.
        self.settle_overlay();
        // Before `conn` closes, so it closes last and checkpoints the WAL.
        self.chunks.close_reader();
        crate::values::forget_tag_undo(&self.conn);
    }
}

impl Store {
    fn require_mutation_admission(&self) -> Result<(), Error> {
        if self.mutation_admission_closed.get() {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        Ok(())
    }

    /// Published bundle `id`, resolved: the write version it selects and
    /// that version governed by the bundle's policies. `None` when this
    /// store has no such bundle, or has not admitted its write version.
    ///
    /// A bundle selects a structural schema by hash, and several hashes
    /// compile to one version (defaults and indexes do not change it): the
    /// bundle's schema is read from the catalogue, and only its version
    /// must be admitted, whichever of its hashes this store admitted or
    /// opened under. It is named by that version's stored names, and
    /// sessions pinned to it that declare no other version read and write
    /// by it, so by its own defaults (see `Store::governed_by`).
    pub(crate) async fn bundle(
        &self,
        id: &crate::catalogue::BundleId,
    ) -> Result<Option<(crate::ids::SchemaVersionId, Rc<Schema>)>, Error> {
        if let Some(found) = self.bundles.borrow().get(id) {
            return Ok(Some(found.clone()));
        }
        let Some((hash, permissions)) = crate::catalogue::bundle(&self.conn, id).await? else {
            return Ok(None);
        };
        let held = self
            .admitted()
            .iter()
            .map(|(_, schema)| schema)
            .find(|schema| crate::catalogue::structural_hash(schema.public_schema()) == hash)
            .map(|schema| schema.public_schema().clone());
        let structural = match held {
            Some(structural) => structural,
            None => match crate::catalogue::schema(&self.conn, &hash).await? {
                Some(structural) => structural,
                None => return Ok(None),
            },
        };
        let mut public = structural;
        for (name, table) in public.iter_mut() {
            table.policies = permissions.get(name).cloned().unwrap_or_default();
        }
        let governing = JazzSchema::new(&public).map_err(|error| {
            Error::new(
                ErrorCode::Schema,
                format!("bundle {id} does not compile: {error}"),
            )
        })?;
        let version = governing.version_id();
        let admitted = version == self.owner_version.get()
            || self.admitted().iter().any(|(id, _)| *id == version);
        if !admitted || self.names().get(&version).is_none() {
            return Ok(None);
        }
        let mut named = self.named(&governing);
        named.bundle = Some(*id);
        let resolved = (version, Rc::new(named));
        self.bundles.borrow_mut().insert(*id, resolved.clone());
        Ok(Some(resolved))
    }

    fn governed_versions(&self) -> HashMap<crate::ids::SchemaVersionId, Rc<Schema>> {
        let mut versions: HashMap<_, _> = self
            .admitted()
            .iter()
            .filter(|(id, _)| *id != self.owner_version.get())
            .map(|(id, schema)| {
                (
                    *id,
                    Rc::new(self.named(schema).with_policies_of(self.owner())),
                )
            })
            .collect();
        versions.insert(
            self.owner_version.get(),
            Rc::new(
                self.named(&self.owner().jazz)
                    .with_policies_of(self.owner()),
            ),
        );
        versions
    }

    fn named(&self, schema: &JazzSchema) -> Schema {
        match self.names().get(&schema.version_id()) {
            Some(names) => Schema::stored_as(schema, names, self.names().tags()),
            None => Schema::new(schema, self.names().tags()),
        }
    }

    /// The current schema named by stored names: the schema replicated
    /// writes travel and apply by (see [`Schema::stored_view`]).
    pub(crate) fn stored_view(&self) -> Rc<Schema> {
        let columns = self
            .physical()
            .tables
            .values()
            .map(|table| table.columns.len() + 1)
            .sum();
        let owner = Rc::as_ptr(&self.owner());
        let mut cached = self.stored_view.borrow_mut();
        if let Some((built, count, view)) = &*cached
            && *built == owner
            && *count == columns
        {
            return view.clone();
        }
        let view = Rc::new(Schema::stored_view(&self.owner(), &self.physical()));
        *cached = Some((owner, columns, view.clone()));
        view
    }

    /// Encode an already resolved write for upload. The source defaults
    /// travel with creation writes; neither names nor layouts are resolved here.
    async fn upload_op(&self, prepared: &PreparedWrite) -> Result<StagedOp, Error> {
        let write = &*prepared.layout;
        let op = &prepared.op;
        if write.stored_names {
            return Ok(op.clone());
        }
        let table = &write.table;
        // The columns this version's lineage dropped, with their backwards
        // defaults: a receiver cannot tell which lenses a writer descends
        // from, so the writer names them (SPEC 8.9).
        let version = write.version;
        let mut dropped = BTreeMap::new();
        if self.names().has_lens_cells() {
            let physical_schema = self.physical();
            let physical = physical_schema.tables.get(&table.stored);
            for (column, default) in self.names().lens_cells(&version, table).1 {
                let Some(stored) = physical.and_then(|physical| physical.column(&column)) else {
                    continue;
                };
                let value = crate::values::column_from_sql(stored, default.as_ref(), &self.chunks)
                    .await?
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
        let created = match op {
            StagedOp::Insert(_, cells, _) | StagedOp::Upsert(_, _, cells, _, _) => created(cells),
            _ => BTreeMap::new(),
        };
        let mut stored = op.clone();
        match &mut stored {
            StagedOp::Insert(_, cells, _) => cells.extend(created),
            // What the write itself gives a created row (the cells the
            // encryption hooks sealed for it) wins over the defaults.
            StagedOp::Upsert(_, _, _, extra, _) => {
                let given = std::mem::replace(extra, created);
                extra.extend(given);
            }
            _ => {}
        }
        Ok(stored)
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
            let source = if lens.source == self.owner_version.get() {
                (*self.owner()).clone()
            } else {
                match self.admitted().iter().find(|(id, _)| *id == lens.source) {
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
                            let physical_schema = self.physical();
                            let stored = physical_schema.tables.get(&to.stored);
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
    async fn exec<P: crate::storage::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> crate::storage::SqlResult<usize> {
        self.conn.prepare_cached(sql)?.execute(params).await
    }

    fn session(&self, author: AuthorSubject) -> Session {
        Session::for_author(
            author,
            self.claims
                .borrow()
                .get(&author)
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// Sessions for a read: (policy subject, claim source).
    fn read_sessions(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> (Option<Session>, Session) {
        let claims_for = |author: AuthorSubject| match prepared.fixed_claims_for(author) {
            Some(claims) => Session::for_author(author, claims.clone()),
            None => self.session(author),
        };
        let policy = identity.map(claims_for);
        let claims = policy
            .clone()
            .unwrap_or_else(|| claims_for(self.identity.author));
        (policy, claims)
    }

    async fn read_namespace(&self, prepared: &PreparedQuery) -> Result<Option<[u8; 16]>, Error> {
        self.identity_namespace(prepared.source_schema(&self.owner()).policy_schema())
            .await
    }

    fn compile(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        namespace: Option<[u8; 16]>,
    ) -> Result<Compiled, Error> {
        let (policy, claims) = self.read_sessions(prepared, identity);
        let compiled = self
            .read_ctx(
                &self.owner(),
                prepared,
                policy.as_ref(),
                &claims,
                read,
                namespace,
            )
            .compile_query(&prepared.query, read.include_deleted)?;
        self.refuse_latest_only_cut(&compiled, read)?;
        Ok(compiled)
    }

    /// A read at a past cut (a serializable snapshot, a snapshot reference,
    /// an explicit seq) needs the versions a latest-only table drops, so it
    /// is refused rather than answered from another version (SPEC 4 §4.9,
    /// `INV-HIST-27`). A replica's settled read is not a past cut: it
    /// reads the superseded version the table keeps.
    pub(crate) fn refuse_latest_only_cut(
        &self,
        compiled: &Compiled,
        read: &ReadSpec,
    ) -> Result<(), Error> {
        let past = read.cut.is_some() && !read.settled_only;
        if !past && !compiled.versioned {
            return Ok(());
        }
        // The reading schema may predate the deploy that made the table
        // latest-only: the store's retention decides, not its declaration.
        match compiled.tables.iter().find(|table| self.latest_only(table)) {
            Some(stored) => {
                let owner_schema = self.owner();
                let name = owner_schema
                    .by_stored(stored)
                    .map_or(stored.as_str(), |t| &t.name);
                Err(unsupported(match past {
                    true => format!("reading latest-only table `{name}` at a snapshot"),
                    false => format!("reading the history of latest-only table `{name}`"),
                }))
            }
            None => Ok(()),
        }
    }

    /// The compile context of a read of `prepared` (or of one of its
    /// relations) as `policy`, with `claims`, per `read`.
    pub(crate) fn read_ctx<'s>(
        &'s self,
        owner: &'s Rc<Schema>,
        prepared: &'s PreparedQuery,
        policy: Option<&'s Session>,
        claims: &'s Session,
        read: &ReadSpec,
        namespace: Option<[u8; 16]>,
    ) -> Ctx<'s> {
        let schema = prepared.source_schema(owner);
        Ctx::new(schema, policy, claims, &prepared.binding)
            .with_namespace(namespace)
            .with_view(read.view.clone())
            .with_withheld(self.withheld.get())
            .with_cut(read.cut.clone(), read.settled_only)
            .with_history_origin(self.history_origin())
    }

    /// The node history reads name this store's own commits by: set on the
    /// authority only (see `compile::Ctx::history_origin`).
    pub(crate) fn history_origin(&self) -> Option<[u8; 16]> {
        self.client()
            .is_none()
            .then(|| *self.identity.node.as_bytes())
    }

    pub(crate) async fn run(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        Ok(self.run_with_deps(prepared, identity, read).await?.0)
    }

    /// [`Store::run`] for a reader: the rows leave the store, so the
    /// encryption hooks open them first. A `strict` read fails rather than
    /// leave out a row it cannot open (protocol §4.4).
    pub(crate) async fn run_for_reader(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        strict: bool,
    ) -> Result<Vec<CurrentRow>, Error> {
        Ok(self
            .run_for_reader_reporting(prepared, identity, read, strict)
            .await?
            .0)
    }

    /// [`Store::run_for_reader`], with the rows the encryption hooks left
    /// out because they could not open them (none without hooks, and none
    /// for a strict read, which fails instead).
    pub(crate) async fn run_for_reader_reporting(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        strict: bool,
    ) -> Result<(Vec<CurrentRow>, Vec<UnreadableRow>), Error> {
        let mut rows = self.run(prepared, identity, read).await?;
        let unreadable = self.open_read(prepared, read, strict, &mut rows, None)?;
        Ok((rows, unreadable))
    }

    /// [`Store::run`] for a caller that uses only each row's identity,
    /// version, deletion, writer and joined rows, never its cells: the
    /// projected cells are not decoded (each reads as `None`), so no chunk
    /// of a large value is read. A partial selection is still refused where
    /// the cell alone says so ([`Store::check_part`]). A session's read at
    /// the authority answers with stored images, not these rows.
    pub(crate) async fn run_without_cells(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (compiled, _) = self.compile_visible(prepared, identity, read).await?;
        let mut run = crate::slow::Run::default();
        let started = web_time::Instant::now();
        let rows = self
            .execute_sql(
                &compiled.sql,
                &compiled,
                &prepared.query.table,
                &mut run,
                None,
                Decode::Nothing,
            )
            .await?;
        run.elapsed = started.elapsed();
        self.record_slow(&compiled, &prepared.query.table, run, rows.len())
            .await;
        Ok(rows)
    }

    async fn run_with_deps(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<(Vec<CurrentRow>, BTreeSet<String>), Error> {
        let (compiled, tables) = self.compile_visible(prepared, identity, read).await?;
        let rows = self.execute(&compiled, &prepared.query.table).await?;
        Ok((rows, tables))
    }

    /// `prepared` compiled for `read` as visible over its tables, and those
    /// tables (its root's and its array relations').
    async fn compile_visible(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<(Compiled, BTreeSet<String>), Error> {
        let namespace = self.read_namespace(prepared).await?;
        let mut compiled = self.compile(prepared, identity, read, namespace)?;
        // Array relations are part of a subscription's dependencies, and
        // of the tables its cut is decided over.
        let tables = self.with_relation_tables(prepared, &compiled.tables)?;
        let visible = self.visible_read(read, &tables);
        if let std::borrow::Cow::Owned(visible) = &visible {
            compiled = self.compile(prepared, identity, visible, namespace)?;
        }
        Ok((compiled, tables))
    }

    /// `tables` (a compiled root's) and the stored tables of `prepared`'s
    /// array relations, nested ones too: what a read's cut is decided
    /// over, so its relations read as of the same position as its roots.
    pub(crate) fn with_relation_tables(
        &self,
        prepared: &PreparedQuery,
        tables: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, Error> {
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
        let mut out = tables.clone();
        let owner_schema = self.owner();
        let schema = prepared.source_schema(&owner_schema);
        arrays(schema, &mut out, &prepared.query.array_subqueries)?;
        Ok(out)
    }

    async fn execute(&self, compiled: &Compiled, table: &str) -> Result<Vec<CurrentRow>, Error> {
        self.execute_keyed(compiled, table, None).await
    }

    /// [`Store::execute`]; with `keys`, each row carries an integer after
    /// its writer (a batched relation's parent index), pushed there in row
    /// order.
    async fn execute_keyed(
        &self,
        compiled: &Compiled,
        table: &str,
        keys: Option<&mut Vec<i64>>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let started = web_time::Instant::now();
        let mut run = crate::slow::Run::default();
        let rows = self
            .execute_sql(
                &compiled.sql,
                compiled,
                table,
                &mut run,
                keys,
                Decode::Cells,
            )
            .await?;
        run.elapsed = started.elapsed();
        self.record_slow(compiled, table, run, rows.len()).await;
        Ok(rows)
    }

    /// Note `run` of `compiled` in the slow-query log if it was slow.
    async fn record_slow(
        &self,
        compiled: &Compiled,
        table: &str,
        mut run: crate::slow::Run,
        rows: usize,
    ) {
        if self.slow.borrow().is_slow(run.elapsed) {
            run.rows = rows as u64;
            let schema_version = self
                .conn
                .query_row("PRAGMA schema_version", [], |r| r.get(0))
                .await
                .ok();
            let plan = if self.slow.borrow().needs_plan(&compiled.sql, schema_version) {
                self.query_plan(&compiled.sql, &compiled.params)
                    .await
                    .unwrap_or_else(|error| format!("(no plan: {error})"))
            } else {
                String::new()
            };
            self.slow
                .borrow_mut()
                .record(table, &compiled.sql, run, schema_version, || plan);
        }
    }

    /// SQLite's plan for `sql`, one step per line.
    async fn query_plan(&self, sql: &str, params: &[Sql]) -> Result<String, Error> {
        let mut statement = self
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(storage)?;
        let mut steps = statement
            .query_map(params_from_iter(params.iter()), |r| r.get::<_, String>(3))
            .await
            .map_err(storage)?;
        let mut plan = Vec::new();
        while let Some(step) = steps.next().await {
            plan.push(step.map_err(storage)?);
        }
        Ok(plan.join("\n"))
    }

    async fn execute_sql(
        &self,
        sql: &str,
        compiled: &Compiled,
        table: &str,
        run: &mut crate::slow::Run,
        mut keys: Option<&mut Vec<i64>>,
        decode: Decode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut statement = self.conn.prepare_cached(sql).map_err(storage)?;
        // Cached statements keep counting across runs: start this one at 0.
        statement.reset_status(crate::storage::StatementStatus::FullscanStep);
        statement.reset_status(crate::storage::StatementStatus::Sort);
        #[cfg(any(test, feature = "testing"))]
        statement.reset_status(crate::storage::StatementStatus::VmStep);
        let names: Arc<str> = Arc::from(table);
        let types: Arc<[ValueType]> = compiled
            .projection
            .iter()
            .map(|(_, ty, _)| ty.clone())
            .collect();
        let mut rows = statement
            .query(params_from_iter(compiled.params.iter()))
            .await
            .map_err(storage)?;
        let mut out = Vec::new();
        // A replica missing chunks reads on past the first blocked cell, so
        // one round trip asks for every row's missing nodes. The first
        // blocked cell's error is the read's: it says whether the chunks are
        // on their way or were refused.
        let mut blocked: Option<Error> = None;
        // A union's arm labels, shared by their rows.
        let mut arms: Vec<Arc<str>> = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage)? {
            let id: Vec<u8> = if compiled.aggregate {
                // A group's key: the group-by cell, a chunked one by its
                // identity (SPEC 19.11).
                let key = format!("{:?}", Sql::from(row.get_ref(0).map_err(storage)?));
                jazz_types::sha256::hash(key.as_bytes()).as_bytes()[..16].to_vec()
            } else {
                row.get(0).map_err(storage)?
            };
            let at = Pos::from_t(row.get(1).map_err(storage)?);
            let state: i64 = row.get(2).map_err(storage)?;
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
                version(3)?
            } else {
                None
            };
            let joined = (0..compiled.joined)
                .map(|i| {
                    let id: Vec<u8> = row.get(4 + 2 * i).map_err(storage)?;
                    Ok(crate::values::JoinedRow {
                        row: uuid::Uuid::from_slice(&id)
                            .map(RowUuid)
                            .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                        version: version(5 + 2 * i)?,
                    })
                })
                .collect::<Result<Arc<[_]>, Error>>()?;
            let union_arm = if compiled.union {
                let label: String = row.get(3).map_err(storage)?;
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
                4 + 2 * compiled.joined
            } else {
                3 + usize::from(compiled.versioned) + usize::from(compiled.union)
            };
            let mut cells = Vec::with_capacity(compiled.projection.len());
            for (i, (name, ty, tags)) in compiled.projection.iter().enumerate() {
                let cell = row.get_ref(first + i).map_err(storage)?;
                let part = compiled.partial.get(i).and_then(Option::as_ref);
                let value = if decode == Decode::Nothing {
                    // An invalid range or split still refuses the read (#5502).
                    if let Some(part) = part {
                        self.check_part(ty, cell, part).await?;
                    }
                    None
                } else {
                    let decoded = match part {
                        Some(part) => self.partial_cell(ty, cell, part).await,
                        None => cell_from_sql(ty, tags.as_ref(), cell, &self.chunks).await,
                    };
                    match decoded {
                        Err(error) if crate::large::is_missing(&error) => {
                            blocked.get_or_insert(error);
                            None
                        }
                        value => value?,
                    }
                };
                cells.push((name.clone(), value));
            }
            let writer = if compiled.writer {
                let w: Vec<u8> = row
                    .get(first + compiled.projection.len())
                    .map_err(storage)?;
                Some(Arc::from(w))
            } else {
                None
            };
            if let Some(keys) = keys.as_deref_mut() {
                let at = first + compiled.projection.len() + usize::from(compiled.writer);
                keys.push(row.get(at).map_err(storage)?);
            }
            out.push(CurrentRow {
                table: names.clone(),
                id: RowUuid(
                    uuid::Uuid::from_slice(&id)
                        .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                ),
                seq: at,
                writer,
                deleted: state & STATE_DELETED != 0,
                cells: Arc::new(cells),
                types: types.clone(),
                joined,
                version: root_version,
                union_arm,
            });
        }
        drop(rows);
        run.scanned += statement.reset_status(crate::storage::StatementStatus::FullscanStep) as u64;
        run.sorted |= statement.reset_status(crate::storage::StatementStatus::Sort) > 0;
        #[cfg(any(test, feature = "testing"))]
        {
            let steps = statement.reset_status(crate::storage::StatementStatus::VmStep) as u64;
            VM_STEPS.with(|cell| cell.set(cell.get() + steps));
        }
        if let Some(error) = blocked {
            return Err(error);
        }
        Ok(out)
    }

    /// Root rows with each array relation nested in a cell under its
    /// column name ([`ResultNode::into_nested_row`]), read as `identity`.
    pub(crate) async fn nest_relations(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        roots: Vec<CurrentRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.nest_relations_in(prepared, identity, read, roots, None)
            .await
    }

    /// [`Self::nest_relations`] for a live result that keeps its relation
    /// statements in `plans` across runs. They are used only for a read
    /// with no branch view and no cut, and dropped first when what they
    /// were compiled under changed ([`PlanStamp`]). `prepared` may be the
    /// result's own query, narrowed at its root, or with relations pruned
    /// by a scoped re-nest: a kept statement is named by its relation's
    /// path, and a scoped re-nest prunes only optional relations (a level
    /// whose dropped relation is required is read whole), so the children
    /// statement on a path, which reads only the required nested
    /// relations, is the same with or without the pruning.
    pub(crate) async fn nest_relations_kept(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        roots: Vec<CurrentRow>,
        plans: &mut RelationPlans,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.nest_relations_in(prepared, identity, read, roots, Some(plans))
            .await
    }

    /// What a statement compiled now for `prepared`, read as `sessions`
    /// through `read` (no view, no cut), is compiled under.
    pub(crate) fn plan_inputs<'a>(
        &'a self,
        prepared: &'a PreparedQuery,
        sessions: &'a (Option<Session>, Session),
        read: &ReadSpec,
        namespace: Option<[u8; 16]>,
    ) -> PlanInputs<'a> {
        PlanInputs {
            namespace,
            schema: prepared.schema.clone().unwrap_or_else(|| self.owner()),
            sessions,
            withheld: self.withheld.get(),
            settled_only: read.settled_only,
            include_deleted: read.include_deleted,
            history_origin: self.history_origin(),
            tags: crate::values::tag_epoch(),
        }
    }

    async fn nest_relations_in(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
        roots: Vec<CurrentRow>,
        plans: Option<&mut RelationPlans>,
    ) -> Result<Vec<CurrentRow>, Error> {
        if prepared.query.array_subqueries.is_empty() {
            return Ok(roots);
        }
        // Related rows read as of the cut the roots were read through (the
        // same decision, over the same tables; a cut read is its own).
        let namespace = self.read_namespace(prepared).await?;
        let visible;
        let read = if read.position != Position::Current && read.cut.is_none() {
            let compiled = self.compile(prepared, identity, read, namespace)?;
            let tables = self.with_relation_tables(prepared, &compiled.tables)?;
            visible = self.visible_read(read, &tables).into_owned();
            &visible
        } else {
            read
        };
        let mut run = RelationRun::new(self, prepared, identity, read, namespace);
        if let Some(plans) = plans
            && read.view.is_none()
            && read.cut.is_none()
        {
            if !PlanStamp::keep(
                &mut plans.stamp,
                &self.plan_inputs(prepared, &run.sessions, read, namespace),
            ) {
                plans.by.clear();
            }
            run.kept = Some(plans);
        }
        self.nest(
            &mut run,
            &prepared.query.table,
            &prepared.query.array_subqueries,
            roots,
        )
        .await?
        .into_iter()
        .map(ResultNode::into_nested_row)
        .collect()
    }

    /// The rows the first `prefix` staged writes of `batch` write, counted
    /// by stored table: how many rows its overlay can have moved in or
    /// out of a read of each table.
    fn staged_rows(batch: &Batch, prefix: usize) -> Result<BTreeMap<String, usize>, Error> {
        let mut rows: BTreeMap<String, BTreeSet<RowUuid>> = BTreeMap::new();
        let mut unnamed = BTreeMap::<String, usize>::new();
        for prepared in batch.ops.iter().take(prefix) {
            let op = &prepared.op;
            let table = op.table().to_owned();
            match op.row() {
                Some(row) => {
                    rows.entry(table).or_default().insert(row);
                }
                // Each insert without an id is a row of its own.
                None => *unnamed.entry(table).or_default() += 1,
            }
        }
        let mut out: BTreeMap<String, usize> = rows
            .into_iter()
            .map(|(table, rows)| (table, rows.len()))
            .collect();
        for (table, n) in unnamed {
            *out.entry(table).or_default() += n;
        }
        Ok(out)
    }

    /// What a serializable transaction records for a windowed read it made
    /// through its overlay of staged writes, `staged` (rows by stored table,
    /// see [`Self::staged_rows`]); `None` when that is `prepared` itself.
    ///
    /// The recorded read is re-run without the overlay, here at commit and
    /// by the authority. Under a `limit` or `offset`, a staged write can move
    /// a row out of the window the app saw and promote one the recorded
    /// window lacks, so a concurrent change to that row would go unnoticed
    /// (issue 5521). The recorded window is widened to cover every row the
    /// overlay could have promoted. It may hold rows the app did not see:
    /// that can add conflicts, never miss one.
    ///
    /// - When a window's membership and order depend on each row's own
    ///   columns only (row-local filters, root-column order, no joins,
    ///   traversals, inheritance, required includes or relations, no read
    ///   policy that reads other rows), each staged row of the window's
    ///   table moves at most one other row across each edge of the window
    ///   (a write to the same row twice counts once). With `n` such
    ///   rows, the window `[offset, offset + limit)` grows to
    ///   `[offset - min(offset, n), offset + limit + n)`: rows can be pushed
    ///   in from before it (staged inserts or moves ahead of it) as well as
    ///   from after it (staged deletes or moves out of it).
    /// - Otherwise one staged write (to a joined row, a policy's input, a
    ///   child a relation requires) can move any number of rows, and no
    ///   count bounds them. Recording the whole filtered result instead
    ///   would cost what the table holds, not what the read touches, so if
    ///   the transaction wrote a table such a window depends on, the read
    ///   is refused as not supported yet. Without such a write the overlay
    ///   leaves the window as it is and the read records as before.
    ///
    /// Relation (array subquery) windows widen the same way, per parent:
    /// a child row correlates with the parent its version names, so `n`
    /// staged rows of the child table move at most `n` rows across each edge
    /// of any one parent's window.
    fn recorded_window(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        staged: &BTreeMap<String, usize>,
        namespace: Option<[u8; 16]>,
    ) -> Result<Option<PreparedQuery>, Error> {
        if staged.is_empty() || !has_window(&prepared.query) {
            return Ok(None);
        }
        let owner_schema = self.owner();
        let schema = prepared.schema.as_deref().unwrap_or(&owner_schema);
        let policies = schema.policy_schema();
        // A read policy that reads other rows (`EXISTS`, inheritance) admits
        // rows by them, as a join does.
        let policied = |stored: &str| {
            identity.is_some()
                && policies
                    .by_stored(stored)
                    .and_then(|table| table.policy(Operation::Select))
                    .is_some_and(|policy| !row_local_policy(policy))
        };
        // The tables the transaction wrote that the read depends on,
        // found once and only if a window needs them.
        let mut touched: Option<Vec<String>> = None;
        let mut refuse = |window: &str| -> Result<(), Error> {
            if touched.is_none() {
                let tables = self.with_relation_tables(
                    prepared,
                    &self.read_dependencies(prepared, identity, namespace)?,
                )?;
                touched = Some(
                    staged
                        .keys()
                        .filter(|table| tables.contains(*table))
                        .map(|table| {
                            let name = schema.by_stored(table).map_or(table.as_str(), |t| &t.name);
                            format!("`{name}`")
                        })
                        .collect(),
                );
            }
            let written = touched.as_deref().unwrap_or_default();
            if written.is_empty() {
                return Ok(());
            }
            Err(Error::new(
                ErrorCode::Query,
                format!(
                    "Reading a limited or offset window of `{window}` that depends on other \
                     rows (a join, a required include or relation, or a policy that reads \
                     other rows) after writing {} in the same transaction is not supported in \
                     serializable transactions yet",
                    written.join(", ")
                ),
            ))
        };
        let mut query = prepared.query.clone();
        let root = schema.stored_table(&query.table)?;
        if query.limit.is_some() || query.offset > 0 {
            if row_local(&query) && !policied(root) {
                widen(
                    &mut query.limit,
                    &mut query.offset,
                    staged.get(root).copied().unwrap_or(0),
                );
            } else {
                refuse(&query.table)?;
            }
        }
        fn arrays(
            schema: &Schema,
            staged: &BTreeMap<String, usize>,
            policied: &dyn Fn(&str) -> bool,
            refuse: &mut dyn FnMut(&str) -> Result<(), Error>,
            list: &mut [crate::query::ArraySubquery],
        ) -> Result<(), Error> {
            for array in list {
                let table = schema.stored_table(&array.table)?;
                if array.limit.is_some() || array.offset > 0 {
                    let local = array.nested_arrays.iter().all(|nested| {
                        nested.requirement == crate::query::ArraySubqueryRequirement::Optional
                    }) && !policied(table);
                    if local {
                        let n = staged.get(table).copied().unwrap_or(0);
                        widen(&mut array.limit, &mut array.offset, n);
                    } else {
                        refuse(&array.table)?;
                    }
                }
                arrays(schema, staged, policied, refuse, &mut array.nested_arrays)?;
            }
            Ok(())
        }
        arrays(
            schema,
            staged,
            &policied,
            &mut refuse,
            &mut query.array_subqueries,
        )?;
        if query == prepared.query {
            return Ok(None);
        }
        Ok(Some(prepared.with_query(query, "widened")))
    }

    /// The versions of the rows a read returns, its relations' rows
    /// included: what a serializable read set compares at commit.
    pub(crate) async fn read_versions(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<(RowUuid, sync::Rank)>, Error> {
        let roots = self.run(prepared, identity, read).await?;
        let mut out = versions(&roots);
        // An aggregate's rows have no version of their own (each is a
        // group, at position 0): named by their values too, a group whose
        // value changed reads as another row, so the commit conflicts.
        if prepared.query.aggregate.is_some() {
            for (version, row) in out.iter_mut().zip(&roots) {
                version.1 = aggregate_read_id(row);
            }
        }
        if !prepared.query.array_subqueries.is_empty() {
            let mut run = RelationRun::new(
                self,
                prepared,
                identity,
                read,
                self.read_namespace(prepared).await?,
            );
            let nodes = self
                .nest(
                    &mut run,
                    &prepared.query.table,
                    &prepared.query.array_subqueries,
                    roots,
                )
                .await?;
            for node in &nodes {
                child_versions(node, &mut out);
            }
        }
        out.into_iter()
            .map(|(_, row, at, writer)| Ok((row, Self::read_rank(at, writer))))
            .collect()
    }

    /// A version a read returned, with its writer when pending: two
    /// writers' versions of a row can share a position. The read carries
    /// the writer of the version its cut picked, current or in history.
    /// Aggregates and history reads return only accepted versions, so a
    /// pending version without a writer should not occur; one is named by
    /// [`sync::NO_WRITER`] as a fail-safe, and its commit conflicts rather
    /// than validating against another writer's version.
    fn read_rank(at: Pos, writer: Option<Arc<[u8]>>) -> sync::Rank {
        if at.accepted() {
            return (at, None);
        }
        let writer = writer.as_deref().and_then(sync::writer_of_bytes);
        (at, Some(writer.unwrap_or(sync::NO_WRITER)))
    }

    /// A row with its array relations, read as the row was: through the
    /// read's view and cut, settled-only and members-only as it is.
    async fn node(
        &self,
        run: &mut RelationRun<'_>,
        table: &str,
        arrays: &[crate::query::ArraySubquery],
        row: CurrentRow,
    ) -> Result<ResultNode, Error> {
        let mut relations = BTreeMap::new();
        for array in arrays {
            let parent = if array.outer_column == "id" {
                Value::Uuid(row.id.0)
            } else {
                self.relation_parent(run, table, array, row.id).await?
            };
            let nodes = self.relation(run, array, &parent).await?;
            relations.insert(array.column_name.clone(), ResultRelation::Array(nodes));
        }
        Ok(ResultNode { row, relations })
    }

    /// `rows` of `table`, each with `arrays` nested: a level at a time
    /// ([`Store::nest_level`]), or row by row ([`Store::node`]) in tests
    /// that compare the two.
    async fn nest(
        &self,
        run: &mut RelationRun<'_>,
        table: &str,
        arrays: &[crate::query::ArraySubquery],
        rows: Vec<CurrentRow>,
    ) -> Result<Vec<ResultNode>, Error> {
        if run.batch && !arrays.is_empty() {
            return self.nest_level(run, table, arrays, rows).await;
        }
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(Box::pin(self.node(run, table, arrays, row)).await?);
        }
        Ok(out)
    }

    /// One level of nesting: for each relation, its parent values for all
    /// of `rows` in one statement (for a relation keyed by a column other
    /// than the parent's id) and the children of all of them in another
    /// ([`Store::relation_level`]); the children's own relations then
    /// nest the same way, one level down. Each row gets what
    /// [`Store::node`] would give it.
    async fn nest_level(
        &self,
        run: &mut RelationRun<'_>,
        table: &str,
        arrays: &[crate::query::ArraySubquery],
        rows: Vec<CurrentRow>,
    ) -> Result<Vec<ResultNode>, Error> {
        let mut relations: Vec<BTreeMap<String, ResultRelation>> =
            (0..rows.len()).map(|_| BTreeMap::new()).collect();
        for array in arrays {
            let parents = if array.outer_column == "id" {
                rows.iter().map(|row| Value::Uuid(row.id.0)).collect()
            } else {
                self.relation_parents(run, table, array, &rows).await?
            };
            let children = self.relation_level(run, array, &parents).await?;
            for (slot, nodes) in relations.iter_mut().zip(children) {
                slot.insert(array.column_name.clone(), ResultRelation::Array(nodes));
            }
        }
        Ok(rows
            .into_iter()
            .zip(relations)
            .map(|(row, relations)| ResultNode { row, relations })
            .collect())
    }

    /// [`Store::relation_parent`] of each of `rows`, read in one
    /// statement per [`BATCH_IDS`] rows.
    async fn relation_parents(
        &self,
        run: &mut RelationRun<'_>,
        table: &str,
        array: &crate::query::ArraySubquery,
        rows: &[CurrentRow],
    ) -> Result<Vec<Value>, Error> {
        let mut found: HashMap<RowUuid, Value> = HashMap::with_capacity(rows.len());
        for chunk in rows.chunks(batch_limits().0.max(1)) {
            let ids =
                crate::compile::json_ids(chunk.iter().map(|row| row.id.0.as_bytes().as_slice()));
            let (cell, _) = run
                .ctx(self)
                .compile_cells(table, &array.outer_column, ids)?;
            let mut statement = self.conn.prepare_cached(&cell.sql).map_err(storage)?;
            let mut cells = statement
                .query(params_from_iter(cell.params.iter()))
                .await
                .map_err(storage)?;
            while let Some(row) = cells.next().await.map_err(storage)? {
                let id: Vec<u8> = row.get(0).map_err(storage)?;
                let id = RowUuid(
                    uuid::Uuid::from_slice(&id)
                        .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
                );
                let value = cell_from_sql(
                    &cell.ty,
                    cell.tags.as_ref(),
                    row.get_ref(1).map_err(storage)?,
                    &self.chunks,
                )
                .await?
                .unwrap_or(Value::Nullable(None));
                found.entry(id).or_insert(value);
            }
        }
        Ok(rows
            .iter()
            .map(|row| found.get(&row.id).cloned().unwrap_or(Value::Nullable(None)))
            .collect())
    }

    /// The children `array` relates to each of `parents`, each with its own
    /// relations: what [`Store::relation`] returns for each, read with one
    /// statement for all distinct parent values (per [`BATCH_BYTES`] of
    /// them, as bound). A parent that binds as SQL `NULL` (or not at all,
    /// NaN) has no children, as `= NULL` matches no row. These fall back
    /// to [`Store::relation`]: a parent value too large to share a bound
    /// list (over [`BATCH_BYTES`] encoded), and for the whole level,
    /// parents with no exact shared JSON form (reals, text that is not
    /// UTF-8, blobs mixed with other values), of mixed shapes, or children
    /// too wide to batch (see [`crate::compile::Ctx::compile_children_batch`]).
    async fn relation_level(
        &self,
        run: &mut RelationRun<'_>,
        array: &crate::query::ArraySubquery,
        parents: &[Value],
    ) -> Result<Vec<Vec<ResultNode>>, Error> {
        let mut index: HashMap<ParentKey, usize> = HashMap::new();
        // Each distinct bound value, and a parent that binds it.
        let mut values: Vec<(Sql, &Value)> = Vec::new();
        let mut of_parent: Vec<Option<usize>> = Vec::with_capacity(parents.len());
        let mut shape = None;
        let mut batchable = true;
        for parent in parents {
            let elements = match parent {
                Value::Nullable(Some(inner)) => matches!(**inner, Value::Array(_)),
                value => matches!(value, Value::Array(_)),
            };
            match crate::values::to_sql(parent)? {
                None | Some(Sql::Null) => of_parent.push(None),
                Some(sql) => {
                    batchable &= *shape.get_or_insert(elements) == elements;
                    let next = values.len();
                    let at = *index.entry(ParentKey::of(&sql)).or_insert(next);
                    if at == next {
                        values.push((sql, parent));
                    }
                    of_parent.push(Some(at));
                }
            }
        }
        let elements = shape.unwrap_or(false);
        let (_, max_bytes) = batch_limits();
        // Chunks of distinct values, each bound as one list of at most
        // `max_bytes` (as encoded); values alone over it read per parent.
        let mut chunks: Vec<Vec<usize>> = Vec::new();
        let mut alone: Vec<usize> = Vec::new();
        if batchable {
            let mut chunk = Vec::new();
            let mut bytes = 2;
            for (at, (value, _)) in values.iter().enumerate() {
                let Some(size) = json_item_len(value) else {
                    batchable = false;
                    break;
                };
                if size + 3 > max_bytes {
                    alone.push(at);
                    continue;
                }
                if !chunk.is_empty() && bytes + size + 1 > max_bytes {
                    chunks.push(std::mem::take(&mut chunk));
                    bytes = 2;
                }
                chunk.push(at);
                bytes += size + 1;
            }
            if !chunk.is_empty() {
                chunks.push(chunk);
            }
        }
        let mut rows = Vec::new();
        let mut keys = Vec::new();
        for (n, chunk) in chunks.iter().enumerate() {
            if !batchable {
                break;
            }
            let list: Vec<&Sql> = chunk.iter().map(|at| &values[*at].0).collect();
            let blobs = list.iter().any(|value| matches!(value, Sql::Blob(_)));
            let Some((_, list)) = crate::compile::json_list(&list, "?") else {
                batchable = false;
                break;
            };
            let Some((compiled, _)) = self.children_batch(run, array, elements, list, blobs)?
            else {
                // The children's width does not depend on the chunk.
                debug_assert_eq!(n, 0);
                batchable = false;
                break;
            };
            let mut local = Vec::new();
            rows.extend(
                self.execute_keyed(&compiled, &array.table, Some(&mut local))
                    .await?,
            );
            for key in local {
                let at = usize::try_from(key)
                    .ok()
                    .and_then(|key| chunk.get(key))
                    .ok_or_else(|| Error::new(ErrorCode::Storage, "batched relation parent key"))?;
                keys.push(*at);
            }
        }
        if !batchable {
            let mut out = Vec::with_capacity(parents.len());
            for parent in parents {
                out.push(Box::pin(self.relation(run, array, parent)).await?);
            }
            return Ok(out);
        }
        // The levels below this one are on its path.
        run.path.push(array.column_name.clone());
        let nodes = Box::pin(self.nest(run, &array.table, &array.nested_arrays, rows)).await;
        run.path.pop();
        let nodes = nodes?;
        let mut groups: Vec<Vec<ResultNode>> = (0..values.len()).map(|_| Vec::new()).collect();
        for (at, node) in keys.into_iter().zip(nodes) {
            groups[at].push(node);
        }
        for at in alone {
            groups[at] = self.relation(run, array, values[at].1).await?;
        }
        // A value shared by several parents gives each a copy; its last
        // parent takes the original.
        let mut uses = vec![0usize; values.len()];
        for at in of_parent.iter().flatten() {
            uses[*at] += 1;
        }
        Ok(of_parent
            .into_iter()
            .map(|at| match at {
                None => Vec::new(),
                Some(at) => {
                    uses[at] -= 1;
                    if uses[at] == 0 {
                        std::mem::take(&mut groups[at])
                    } else {
                        groups[at].clone()
                    }
                }
            })
            .collect())
    }

    /// The batched children statement of `array` for the parents' JSON
    /// `list` ([`Ctx::compile_children_batch`]): a live result's kept one
    /// with `list` bound in, else compiled (and kept, for a live result).
    fn children_batch(
        &self,
        run: &mut RelationRun<'_>,
        array: &crate::query::ArraySubquery,
        elements: bool,
        list: Sql,
        blobs: bool,
    ) -> Result<Option<(Compiled, usize)>, Error> {
        if run.kept.is_none() {
            return run
                .ctx(self)
                .compile_children_batch(array, elements, list, blobs);
        }
        let mut path = run.path.join("\u{1f}");
        path.push('\u{1f}');
        path.push_str(&array.column_name);
        let key = (path, elements, blobs);
        let kept = run
            .kept
            .as_ref()
            .and_then(|plans| plans.by.get(&key).cloned());
        let Some(kept) = kept else {
            let fresh = run
                .ctx(self)
                .compile_children_batch(array, elements, list, blobs)?;
            if let Some(plans) = run.kept.as_mut() {
                plans.by.insert(key, fresh.clone());
            }
            return Ok(fresh);
        };
        if let Some(plans) = run.kept.as_mut() {
            plans.hits += 1;
            self.fan.borrow().stats.borrow_mut().kept_plans += 1;
        }
        let kept = kept.map(|(mut compiled, at)| {
            compiled.params[at] = list.clone();
            (compiled, at)
        });
        if plan_oracle() {
            let fresh = run
                .ctx(self)
                .compile_children_batch(array, elements, list, blobs)?;
            let same = match (&kept, &fresh) {
                (None, None) => true,
                (Some((a, i)), Some((b, j))) => i == j && same_compiled(a, b),
                _ => false,
            };
            assert!(
                same,
                "a kept relation statement differs from a fresh one: {}",
                key.0.replace('\u{1f}', ".")
            );
        }
        Ok(kept)
    }

    /// The value of row `id` that `array` correlates its children with
    /// (`array.outer_column`), read through the run's view and cut.
    async fn relation_parent(
        &self,
        run: &mut RelationRun<'_>,
        table: &str,
        array: &crate::query::ArraySubquery,
        id: RowUuid,
    ) -> Result<Value, Error> {
        let key = array as *const crate::query::ArraySubquery;
        let cell = match run.reuse.then(|| run.cells.get(&key)).flatten() {
            Some(cached) => {
                let mut cell = cached.clone();
                cell.params[cell.id_param] = Sql::Blob(id.0.as_bytes().to_vec());
                cell
            }
            None => {
                let cell = run.ctx(self).compile_cell(table, &array.outer_column, id)?;
                if run.reuse {
                    run.cells.insert(key, cell.clone());
                }
                cell
            }
        };
        let value: Option<Sql> = self
            .conn
            .cached_row(&cell.sql, params_from_iter(cell.params.iter()), |r| {
                r.get(0)
            })
            .await
            .optional()
            .map_err(storage)?;
        Ok(match value {
            Some(value) => {
                cell_from_sql(&cell.ty, cell.tags.as_ref(), value.as_ref(), &self.chunks)
                    .await?
                    .unwrap_or(Value::Nullable(None))
            }
            None => Value::Nullable(None),
        })
    }

    /// The children `array` relates to `parent`, each with its own
    /// relations. Within one run the same relation of the same parent value
    /// reads the same rows, so a relation keyed by a parent's column (a
    /// forward reference, or an array of them) reads them once. One keyed
    /// by the parent's own id meets each parent once, so it is not kept.
    async fn relation(
        &self,
        run: &mut RelationRun<'_>,
        array: &crate::query::ArraySubquery,
        parent: &Value,
    ) -> Result<Vec<ResultNode>, Error> {
        let key = array as *const crate::query::ArraySubquery;
        let elements = match parent {
            Value::Nullable(Some(inner)) => matches!(**inner, Value::Array(_)),
            value => matches!(value, Value::Array(_)),
        };
        let bound = crate::values::to_sql(parent)?;
        let memo = (key, elements, bound.as_ref().map(ParentKey::of));
        let memoise = run.reuse && array.outer_column != "id";
        if memoise && let Some(nodes) = run.children.get(&memo) {
            return Ok(nodes.clone());
        }
        // Only the parent's own parameter differs between parents of one
        // shape (a scalar, or an array of references).
        let compiled = match run
            .reuse
            .then(|| run.sql.get(&(key, elements)))
            .flatten()
            .zip(bound.as_ref())
        {
            Some(((cached, at), bound)) => {
                let mut compiled = cached.clone();
                compiled.params[*at] = bound.clone();
                compiled
            }
            None => {
                let (compiled, at) = run.ctx(self).compile_children(array, parent)?;
                if run.reuse
                    && let Some(at) = at
                {
                    run.sql.insert((key, elements), (compiled.clone(), at));
                }
                compiled
            }
        };
        let children = self.execute(&compiled, &array.table).await?;
        let mut nodes = Vec::with_capacity(children.len());
        for child in children {
            nodes.push(Box::pin(self.node(run, &array.table, &array.nested_arrays, child)).await?);
        }
        if memoise {
            run.children.insert(memo, nodes.clone());
        }
        Ok(nodes)
    }

    // ---- write primitives ---------------------------------------------

    /// Where this store's own commit `seq` writes its versions: the
    /// authority's seq, or pending at the commit's HLC on a replica.
    pub(crate) fn own_pos(&self, seq: u64) -> Pos {
        if self.me.get() == sync::ACCEPTED {
            Pos::at(seq)
        } else {
            Pos::pending(self.commit_hlc(seq))
        }
    }

    /// Own commit `seq`'s version: its position and, pending, its writer.
    pub(crate) fn own_rank(&self, seq: u64) -> sync::Rank {
        let at = self.own_pos(seq);
        if at.accepted() {
            return (at, None);
        }
        let writer = match self.own_order.borrow().get(&seq) {
            Some(&(_, node, tx)) => (node, tx),
            None => (*self.identity.node.as_bytes(), seq),
        };
        (at, Some(writer))
    }

    async fn existing(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<Option<(Pos, bool)>, Error> {
        let sql = format!(
            "SELECT _clock, (_state & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _branch = ?2",
            quote(table)
        );
        match self
            .conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key],
                |r| Ok((Pos::from_t(r.get(0)?), r.get::<_, i64>(1)? != 0)),
            )
            .await
        {
            Ok(found) => Ok(Some(found)),
            Err(crate::storage::SqlError::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(storage(error)),
        }
    }

    /// [`Store::existing`] as of `cut` (the current row when `None`): the
    /// newest version the cut sees, current or superseded.
    async fn existing_at(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        cut: Option<&Cut>,
    ) -> Result<Option<(Pos, bool)>, Error> {
        let current = self.existing(table, row, key).await?;
        let Some(cut) = cut else {
            return Ok(current);
        };
        if let Some((seq, _)) = current
            && cut.sees(self.current_rank(table, row, key, seq).await?)
        {
            return Ok(current);
        }
        let Some((version, w)) = self.version_at(table, row, key, cut).await? else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT (_state & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _branch = ?2 \
             AND _clock = ?3 AND _writer = ?4",
            quote(&format!("{table}__h"))
        );
        let deleted: i64 = self
            .conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key, version.t(), w],
                |r| r.get(0),
            )
            .await
            .map_err(storage)?;
        Ok(Some((version, deleted != 0)))
    }

    /// A row's current version at `at` as a cut names it: with its writer,
    /// when pending.
    pub(crate) async fn current_rank(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        at: Pos,
    ) -> Result<sync::Rank, Error> {
        if at.accepted() {
            return Ok((at, None));
        }
        Ok((
            at,
            sync::writer_of_bytes(&self.current_writer(table, row, key, at).await?),
        ))
    }

    /// The newest superseded version of a row that `cut` sees: its
    /// position and writer (`_writer`).
    async fn version_at(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        cut: &Cut,
    ) -> Result<Option<(Pos, Vec<u8>)>, Error> {
        // Positions are integers and writers fixed-width bytes, so the
        // named versions are inlined.
        let extra: String = cut
            .extra
            .iter()
            .map(|(at, writer)| match writer.filter(|_| !at.accepted()) {
                None => format!(" OR _clock = {}", at.t()),
                Some((node, tx)) => format!(
                    " OR (_clock = {} AND _writer = X'{}')",
                    at.t(),
                    sync::writer_bytes(node, tx)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                ),
            })
            .collect();
        let sql = format!(
            "SELECT _clock, _writer FROM {} WHERE id = ?1 AND _branch = ?2 AND (_state & {STATE_AS_SENT}) = 0 \
             AND (_clock <= ?3{extra}) ORDER BY _clock DESC, _writer DESC LIMIT 1",
            quote(&format!("{table}__h"))
        );
        self.conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key, cut.seq.t()],
                |r| Ok((Pos::from_t(r.get(0)?), r.get(1)?)),
            )
            .await
            .optional()
            .map_err(storage)
    }

    /// Resolve which stored row a patch-style write addresses.
    async fn locate(&self, table: &Table, row: RowUuid, target: &Target) -> Result<Located, Error> {
        let at = async |key: Vec<u8>, cells: BTreeMap<String, Value>| -> Result<Located, Error> {
            Ok(match self.existing(&table.stored, row, &key).await? {
                Some((seq, deleted)) => Located::Row { key, seq, deleted },
                None => {
                    self.refuse_withheld(table, row, &key).await?;
                    Located::Absent { cells }
                }
            })
        };
        match target {
            Target::Root if table.branch_by.is_empty() => at(Vec::new(), BTreeMap::new()).await,
            Target::Root => {
                let sql = format!("SELECT _branch FROM {} WHERE id = ?1", quote(&table.stored));
                let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
                let keys: Vec<Vec<u8>> = statement
                    .query_map([row.0.as_bytes().to_vec()], |r| r.get(0))
                    .await
                    .map_err(storage)?
                    .collect::<_>()
                    .await
                    .map_err(storage)?;
                match keys.as_slice() {
                    [key] => at(key.clone(), BTreeMap::new()).await,
                    [] => Ok(Located::Absent {
                        cells: BTreeMap::new(),
                    }),
                    _ => Err(rejected(format!(
                        "row {row:?} exists on several branches of `{}`; name a branch target",
                        table.name
                    ))),
                }
            }
            Target::Exact(selector) => {
                at(
                    table.selector_key(selector)?,
                    table.selector_cells(selector)?,
                )
                .await
            }
            Target::View { head, base, at } => {
                let head_key = table.selector_key(head)?;
                let head_cells = if table.branch_by.is_empty() {
                    BTreeMap::new()
                } else {
                    table.selector_cells(head)?
                };
                if let Some((seq, deleted)) = self.existing(&table.stored, row, &head_key).await? {
                    return Ok(Located::Row {
                        key: head_key,
                        seq,
                        deleted,
                    });
                }
                self.refuse_withheld(table, row, &head_key).await?;
                if let Some(base) = base {
                    let base_key = table.selector_key(base)?;
                    if base_key != head_key
                        && let Some((_, false)) = self
                            .existing_at(&table.stored, row, &base_key, at.as_ref())
                            .await?
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
    async fn refuse_withheld(&self, table: &Table, row: RowUuid, key: &[u8]) -> Result<(), Error> {
        if self.is_withheld(&table.stored, row, key).await? {
            return Err(rejected(format!(
                "row {} of `{}` is hidden here: its version on this branch is not readable",
                row.0, table.name
            )));
        }
        Ok(())
    }

    /// Evaluate a policy SQL boolean against one stored row.
    async fn holds(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        self.holds_at(active_schema, table, row, key, None, policy)
            .await
    }

    /// [`Store::holds`] with the policy's other rows read as of `cut`; the
    /// row under check is always its current version.
    async fn holds_at(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        self.holds_counting(active_schema, table, row, key, cut, policy)
            .await
            .map(|(holds, _)| holds)
    }

    /// [`Store::holds_at`], also returning what of a write check's overlay
    /// the check read.
    async fn holds_counting(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<(bool, OverlayRead), Error> {
        let (sql, params, read) = self
            .policy_check_in(active_schema, table, row, key, cut, policy)
            .await?;
        let value: i64 = self
            .conn
            .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .await
            .map_err(storage)?;
        Ok((value != 0, read))
    }

    /// The statement [`Store::holds_at`] runs: the policy SQL boolean for
    /// one stored row, its parameters, and what of the cut's overlay it
    /// reads.
    async fn policy_check_in(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<(String, Vec<Sql>, OverlayRead), Error> {
        let namespace = self.identity_namespace(schema.policy_schema()).await?;
        let (condition, mut params, read) =
            self.policy_condition_in(schema, namespace, cut, policy)?;
        let sql = policy_check_sql(table, &condition, params.len());
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        Ok((sql, params, read))
    }

    /// A policy compiled in `schema` as an SQL boolean over the row `a`,
    /// with its parameters (none for the row and branch yet) and what of
    /// the cut's overlay it reads.
    fn policy_condition_in(
        &self,
        schema: &Schema,
        namespace: Option<[u8; 16]>,
        cut: Option<Cut>,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<(String, Vec<Sql>, OverlayRead), Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(schema, None, &claims, &binding)
            .with_cut(cut, false)
            .with_namespace(namespace);
        let condition = policy(&mut ctx)?;
        let read = OverlayRead {
            rows: ctx.overlaid_rows,
            tables: std::mem::take(&mut ctx.overlay_tables),
        };
        Ok((condition, ctx.params, read))
    }

    /// SQLite's plan for the INSERT check a unit ending now would run on
    /// `row` (SPEC 7): its policy reads committed state, overlaid with the
    /// unit's rows `overlay` names.
    async fn explain_write_check(
        &self,
        active_schema: &Rc<Schema>,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
        overlay: CutOverlay,
    ) -> Result<String, Error> {
        let table = active_schema.table(table)?;
        let own = self.own_rank(self.seq.get() + 1);
        let cut = Cut {
            unsettled: false,
            seq: self.own_pos(self.seq.get()),
            own,
            extra: Vec::new(),
            overlay: Some(overlay),
            inserted: Arc::default(),
        };
        let session = self.session(author);
        let (sql, params, _) = self
            .policy_check_in(active_schema, table, row, &[], Some(cut), |ctx| {
                ctx.operation_policy(table, Operation::Insert, "a", &session)
            })
            .await?;
        let mut statement = self
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(storage)?;
        let mut rows = statement
            .query_map(params_from_iter(params.iter()), |r| r.get::<_, String>(3))
            .await
            .map_err(storage)?;
        let mut plan = Vec::new();
        while let Some(row) = rows.next().await {
            plan.push(row.map_err(storage)?);
        }
        Ok(plan.join(" | "))
    }

    /// [`Store::holds`] for `row` as of `cut`: the row under check and
    /// the policy's other rows are each read as of `cut`.
    async fn holds_as_of(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        cut: Cut,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<bool, Error> {
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(schema, None, &claims, &binding)
            .with_cut(Some(cut), false)
            .with_namespace(self.identity_namespace(schema.policy_schema()).await?);
        let condition = policy(&mut ctx)?;
        let source = ctx.source(table)?;
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition}, 0) FROM {source} AS a WHERE a.id = ?{} AND a._branch = ?{}",
            params.len() - 1,
            params.len()
        );
        let value: Option<i64> = self
            .conn
            .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .await
            .optional()
            .map_err(storage)?;
        Ok(value.is_some_and(|value| value != 0))
    }

    /// Whether `author` may `operation` the version of `row` at `at`, by
    /// `schema`'s policies (`table` is `schema`'s).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn check_version(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        at: sync::Rank,
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        // A pending version by its writer: another writer's can share its
        // position.
        let cut = match at.1.filter(|_| !at.0.accepted()) {
            Some(_) => Cut {
                seq: Pos::from_t(at.0.t() - 1),
                own: at,
                ..Cut::default()
            },
            None => Cut::at(at.0),
        };
        self.holds_as_of(schema, table, row, key, cut, |ctx| {
            ctx.operation_policy(table, operation, "a", &session)
        })
        .await
    }

    pub(crate) async fn check(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        self.check_in(active_schema, table, row, key, author, operation)
            .await
    }

    /// [`Store::check`] by `schema`'s policies (`table` is `schema`'s).
    ///
    /// The statement is a function of the schema, the table, the operation,
    /// the confirmed namespace, the author and claims, and the current enum tags;
    /// only the row and branch parameters differ per call. So it is kept
    /// ([`CheckPlans`]) and compiled again only when one of those changed.
    pub(crate) async fn check_in(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        self.check_in_planned(schema, table, row, key, author, operation, false)
            .await
    }

    /// [`Store::check_in`] for a row the caller has just read from this
    /// store's stored table, at this branch key, with nothing written since:
    /// a policy that compiles to the constant `1` passes it without running
    /// a statement. For a row that is not there the statement would fail
    /// (it reads no row), so the caller must have read it.
    pub(crate) async fn check_in_existing(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        self.check_in_planned(schema, table, row, key, author, operation, true)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn check_in_planned(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
        existing: bool,
    ) -> Result<bool, Error> {
        let namespace = self.identity_namespace(schema.policy_schema()).await?;
        let plan_key = self
            .check_plans
            .borrow()
            .key(schema, table, operation, author, namespace);
        let plan = {
            let claims_by_author = self.claims.borrow();
            let claims = claims_by_author.get(&author);
            let epoch = crate::values::tag_epoch();
            let kept = self.check_plans.borrow().get(&plan_key, claims, epoch);
            match kept {
                Some(plan) => plan,
                None => {
                    let session = self.session(author);
                    let (condition, params, _) =
                        self.policy_condition_in(schema, namespace, None, |ctx| {
                            ctx.operation_policy(table, operation, "a", &session)
                        })?;
                    let plan = Rc::new(CheckPlan {
                        claims: claims.cloned(),
                        epoch,
                        sql: policy_check_sql(table, &condition, params.len()),
                        constant: condition == "1" && params.is_empty(),
                        params,
                    });
                    self.check_plans.borrow_mut().put(plan_key, plan.clone());
                    plan
                }
            }
        };
        if existing && plan.constant {
            debug_assert!(
                self.row_at(table, row, key).await?,
                "check_in_existing on a row {row:?} of {} that is not there",
                table.stored
            );
            return Ok(true);
        }
        let mut params = plan.params.clone();
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let value: i64 = self
            .conn
            .cached_row(&plan.sql, params_from_iter(params.iter()), |r| r.get(0))
            .await
            .map_err(storage)?;
        Ok(value != 0)
    }

    /// Whether stored table `table` holds `row` at branch key `key`.
    async fn row_at(&self, table: &Table, row: RowUuid, key: &[u8]) -> Result<bool, Error> {
        (async {
            let mut statement = self.conn.prepare_cached(&format!(
                "SELECT 1 FROM {} WHERE id = ?1 AND _branch = ?2",
                quote(&table.stored)
            ))?;
            {
                statement
                    .query_row(
                        crate::storage::params![row.0.as_bytes().as_slice(), key],
                        |_| Ok(()),
                    )
                    .await
                    .optional()
            }
        })
        .await
        .map(|found| found.is_some())
        .map_err(storage)
    }

    /// Drop the kept policy checks (the schema changed).
    pub(crate) fn clear_check_plans(&self) {
        self.check_plans.borrow_mut().clear();
    }

    /// [`Store::check_in`] compiled afresh, as before plans were kept: the
    /// oracle the kept plans are tested against.
    #[cfg(any(test, feature = "testing"))]
    async fn check_in_fresh(
        &self,
        schema: &Schema,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        let (sql, params, _) = self
            .policy_check_in(schema, table, row, key, None, |ctx| {
                ctx.operation_policy(table, operation, "a", &session)
            })
            .await?;
        let value: i64 = self
            .conn
            .cached_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
            .await
            .map_err(storage)?;
        Ok(value != 0)
    }

    /// Check a write's own policy as SPEC 7 has it: on committed state
    /// overlaid with the unit's grounded rows. Inside a unit
    /// ([`Store::defer_checks`]) the check waits for the unit's end;
    /// alone, nothing else is grounded and it runs now.
    #[allow(clippy::too_many_arguments)]
    async fn write_check(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        check: WriteCheck,
        seq: u64,
    ) -> Result<(), Error> {
        let deferred = DeferredCheck {
            schema: active_schema.clone(),
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
        self.ground(vec![deferred], seq).await
    }

    /// Start a unit: write checks wait for [`Store::finish_checks`].
    pub(crate) fn defer_checks(&self) {
        *self.deferred.borrow_mut() = Some(Vec::new());
        self.trusted_rows.borrow_mut().clear();
        self.unit_inserts.borrow_mut().clear();
    }

    /// Note a unit's fresh insert of `row` on branch `key` (see
    /// [`Store::unit_inserts`]). A write outside a unit is a single write:
    /// it has none.
    fn note_unit_insert(&self, table: &Table, row: RowUuid, key: &[u8]) {
        if self.deferred.borrow().is_some() {
            self.unit_inserts
                .borrow_mut()
                .entry(table.stored.clone())
                .or_default()
                .insert((*row.0.as_bytes(), key.to_vec()));
        }
    }

    /// Note a unit's write that no policy checks, so its row grounds the
    /// unit's checked writes from the first round on.
    fn note_trusted(&self, table: &Table, row: RowUuid) {
        if self.deferred.borrow().is_some() {
            self.trusted_rows
                .borrow_mut()
                .entry(table.stored.clone())
                .or_default()
                .insert(*row.0.as_bytes());
        }
    }

    /// Start applying a unit, or replaying a staging overlay: no head row
    /// another unit copied is this one's ([`Store::inherited_heads`]).
    pub(crate) fn start_unit_heads(&self) {
        self.inherited_heads.borrow_mut().clear();
    }

    /// Drop a unit's pending checks (it rolled back).
    pub(crate) fn abandon_checks(&self) {
        *self.deferred.borrow_mut() = None;
        self.trusted_rows.borrow_mut().clear();
        self.unit_inserts.borrow_mut().clear();
    }

    /// End a unit written at `seq`: ground its writes (SPEC 7). The first
    /// round checks every write on committed state alone; a write that
    /// passes is grounded, and from the next round on its row shows its
    /// end state to the others. A write that never passes rejects the
    /// unit. A row the unit leaves deleted has no end state to check.
    /// Once every write is grounded, a write whose pass the rest of the
    /// unit could have taken away is checked again against the unit's
    /// whole post-state (see [`Store::post_state_pass`]).
    pub(crate) async fn finish_checks(&self, seq: u64) -> Result<(), Error> {
        self.finish_checks_after(seq, Arc::default(), Arc::default())
            .await
    }

    /// [`Store::finish_checks`] for a unit that continues writes already
    /// grounded: `grounded` (stored table to rows) shows its end state to
    /// the unit's checks from the first round on, and `inserted` are those
    /// writes' fresh inserts.
    pub(crate) async fn finish_checks_after(
        &self,
        seq: u64,
        grounded: Arc<GroundedRows>,
        inserted: Arc<InsertedRows>,
    ) -> Result<(), Error> {
        let pending = self.deferred.borrow_mut().take().unwrap_or_default();
        let trusted = std::mem::take(&mut *self.trusted_rows.borrow_mut());
        let mut inserted = inserted;
        for (table, rows) in self.unit_inserts.borrow().iter() {
            Arc::make_mut(&mut inserted)
                .entry(table.clone())
                .or_default()
                .extend(rows.iter().cloned());
        }
        // Nothing to ground: writes no policy checks (a backend's own, a
        // replica's) leave only trusted rows, and merging those into the
        // set a staged write shares with its batch would copy that set on
        // every stage.
        if pending.is_empty() {
            return Ok(());
        }
        let mut grounded = grounded;
        #[cfg(any(test, feature = "testing"))]
        if !trusted.is_empty() && Arc::strong_count(&grounded) > 1 {
            self.evidence_copies.set(self.evidence_copies.get() + 1);
        }
        for (table, rows) in trusted {
            Arc::make_mut(&mut grounded)
                .entry(table)
                .or_default()
                .extend(rows);
        }
        self.ground_after(pending, seq, grounded, inserted).await
    }

    async fn ground(&self, pending: Vec<DeferredCheck>, seq: u64) -> Result<(), Error> {
        self.ground_after(pending, seq, Arc::default(), Arc::default())
            .await
    }

    async fn ground_after(
        &self,
        mut pending: Vec<DeferredCheck>,
        seq: u64,
        mut grounded: Arc<GroundedRows>,
        inserted: Arc<InsertedRows>,
    ) -> Result<(), Error> {
        // Each check runs under the schema its write was staged with.
        let mut live = Vec::with_capacity(pending.len());
        let several = pending.len() > 1;
        for mut check in pending.drain(..) {
            let table = check.schema.table(&check.table)?;
            if !self
                .is_deleted_version(table, check.row, &check.key)
                .await?
            {
                // Only a unit of several writes has a post-state pass.
                check.replaces = several
                    && (matches!(check.check, WriteCheck::WithCheck { .. })
                        || self
                            .was_live_before(table, check.row, &check.key, seq)
                            .await?);
                live.push(check);
            }
        }
        let mut pending = live;
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
                own: self.own_rank(seq),
                extra: Vec::new(),
                overlay: Some(CutOverlay::Grounded(grounded.clone())),
                inserted: inserted.clone(),
            };
            let mut passed = Vec::new();
            let mut failed = Vec::new();
            for check in pending {
                let schema = check.schema.clone();
                let (holds, tables) = self
                    .write_check_holds(&schema, &check, cut.clone(), &mut evidence)
                    .await?;
                if holds {
                    passed.push((check, tables));
                } else {
                    failed.push(check);
                }
            }
            if !failed.is_empty() && passed.is_empty() {
                return Err(write_check_denied(&failed[0]));
            }
            drop(cut);
            // Only a further round reads what this one grounded.
            if failed.is_empty() {
                rounds.extend(
                    passed
                        .into_iter()
                        .map(|(check, tables)| (check, round, tables)),
                );
                break;
            }
            for (check, tables) in passed {
                let stored = check.schema.table(&check.table)?.stored.clone();
                Arc::make_mut(&mut grounded)
                    .entry(stored)
                    .or_default()
                    .insert(*check.row.0.as_bytes());
                rounds.push((check, round, tables));
            }
            pending = failed;
            round += 1;
        }
        self.post_state_pass(rounds, seq, &inserted, &mut evidence)
            .await
    }

    /// Check `check`'s policy on the rows `cut` shows. Also returns the
    /// stored tables the policy read through the cut's overlay.
    async fn write_check_holds(
        &self,
        schema: &Rc<Schema>,
        check: &DeferredCheck,
        cut: Cut,
        evidence: &mut EvidenceBudget,
    ) -> Result<(bool, BTreeSet<String>), Error> {
        let table = schema.table(&check.table)?;
        let session = self.session(check.author);
        let (holds, read) = self
            .holds_counting(
                schema,
                table,
                check.row,
                &check.key,
                Some(cut.clone()),
                |ctx| {
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
                },
            )
            .await?;
        evidence.charge(read.rows)?;
        let holds = holds
            && match check.check {
                WriteCheck::Insert => self.account_record_holds(schema, table, check, cut).await?,
                _ => true,
            };
        Ok((holds, read.tables))
    }

    /// The checks beside the account tables' insert policies that read
    /// Jazz's account registry and an identity's generation (E2EE protocol
    /// R5, R6): a reset identity (generation above 0) only for an account
    /// not founded by a device-account identity, by a session that isn't a
    /// device-account session, with the account's identity of the
    /// generation before;
    /// a root whose `kind` names how its account was founded. The policies
    /// check the rest, `allowAccountReset` included. True for any other
    /// table.
    async fn account_record_holds(
        &self,
        schema: &Rc<Schema>,
        table: &Table,
        check: &DeferredCheck,
        cut: Cut,
    ) -> Result<bool, Error> {
        use jazz_model::schema::{ACCOUNT_IDENTITIES_TABLE, ACCOUNT_ROOTS_TABLE};
        let names = self.names();
        let logical = Self::logical_table_name(&names, table);
        let identities = logical == ACCOUNT_IDENTITIES_TABLE;
        if !identities && logical != ACCOUNT_ROOTS_TABLE {
            return Ok(true);
        }
        let Some(account) = check.author.account_id() else {
            return Ok(false);
        };
        let registry = self.account_registry.borrow().clone();
        if let Some(registry) = &registry
            && registry
                .founded_for_another_audience(&self.conn, account)
                .await
                .map_err(|error| Error::new(ErrorCode::Storage, error))?
        {
            return Ok(false);
        }
        let device_account_session =
            check.author.principal_parts().0 == jazz_types::identity::DEVICE_ACCOUNT_ISSUER;
        // A store without the account, or without a registry, has no
        // links: the session's own login founded its account.
        let founded_by_device_account = async || -> Result<bool, Error> {
            let known = match &registry {
                Some(registry) => registry
                    .founded_by_device_account(&self.conn, account)
                    .await
                    .map_err(|error| Error::new(ErrorCode::Storage, error))?,
                None => None,
            };
            Ok(known.unwrap_or(device_account_session))
        };
        if !identities {
            let kind = match self.account_cell(table, check, "kind").await? {
                Some(Value::String(kind)) => kind,
                _ => return Ok(false),
            };
            let founded = if founded_by_device_account().await? {
                "device-account"
            } else {
                "app"
            };
            return Ok(kind == founded);
        }
        let generation = match self.account_cell(table, check, "generation").await? {
            Some(Value::I64(generation)) => generation,
            _ => return Ok(false),
        };
        if generation == 0 {
            return Ok(true);
        }
        if generation < 0 || device_account_session || founded_by_device_account().await? {
            return Ok(false);
        }
        // The account's own identity of the generation before.
        let previous = PolicyExpr::Exists {
            table: ACCOUNT_IDENTITIES_TABLE.to_owned(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "$createdBy.account".to_owned(),
                    op: CmpOp::Eq,
                    value: crate::tools::PolicyValue::SessionRef(vec!["user".to_owned()]),
                },
                PolicyExpr::Cmp {
                    column: "generation".to_owned(),
                    op: CmpOp::Eq,
                    value: crate::tools::PolicyValue::Literal(crate::tools::Value::BigInt(
                        generation - 1,
                    )),
                },
            ])),
        };
        let session = self.session(check.author);
        self.holds_at(schema, table, check.row, &check.key, Some(cut), |ctx| {
            ctx.policy_clause(Some(&previous), table, "a", &session)
        })
        .await
    }

    /// The name the admitted versions give `table`. A write uploaded from a
    /// client is staged under the stored view, whose tables are named by
    /// their stored names: a table a later migration introduced is
    /// `name~hash`, which Jazz's fixed names do not match. The owner's
    /// schema is not the source: a running server keeps its first schema
    /// as owner while deployments admit later versions, so the admitted
    /// versions' stored-name map is read.
    fn logical_table_name<'a>(names: &'a crate::names::Names, table: &'a Table) -> &'a str {
        names
            .logical_table(&table.stored)
            .unwrap_or(table.name.as_str())
    }

    /// Cell `column` of the account table row that `check` writes. A write
    /// uploaded from a client is checked through the stored view, whose
    /// columns are named by their stored names (`name~hash`, the name they
    /// were introduced under). The account tables are Jazz's fixed
    /// fragment, never renamed, so a column's stored name starts with its
    /// name.
    async fn account_cell(
        &self,
        table: &Table,
        check: &DeferredCheck,
        column: &str,
    ) -> Result<Option<Value>, Error> {
        let introduced_as = |stored: &str| stored.split('~').next() == Some(column);
        let Some(column) = table
            .column(column)
            .or_else(|| table.stored().find(|c| introduced_as(&c.stored)))
        else {
            return Ok(None);
        };
        let value = match self
            .stored_cell(table, check.row, &check.key, column)
            .await?
        {
            Some(cell) => column_from_sql(column, cell.as_ref(), &self.chunks).await?,
            None => None,
        };
        // A column a later version introduced is stored nullable (older
        // rows lack it): the cell of a row that has it is its value.
        Ok(match value {
            Some(Value::Nullable(Some(inner))) => Some(*inner),
            other => other,
        })
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
    async fn post_state_pass(
        &self,
        rounds: Vec<(DeferredCheck, usize, BTreeSet<String>)>,
        seq: u64,
        inserted: &Arc<InsertedRows>,
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
            let stored = check.schema.table(&check.table)?.stored.clone();
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
                        let grows = self.write_policy_is_monotone(&check.schema, check)?;
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
                own: self.own_rank(seq),
                extra: Vec::new(),
                overlay: Some(CutOverlay::Except {
                    table: stored.clone(),
                    row: *check.row.0.as_bytes(),
                    key: check.key.clone(),
                }),
                inserted: inserted.clone(),
            };
            if !self
                .write_check_holds(&check.schema, check, cut, evidence)
                .await?
                .0
            {
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
    async fn was_live_before(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<bool, Error> {
        Ok(self.deleted_before(table, row, key, seq).await? == Some(false))
    }

    /// Whether the row had any version, live or deleted, in the state a
    /// unit written at `seq` was made over.
    async fn existed_before(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<bool, Error> {
        Ok(self.deleted_before(table, row, key, seq).await?.is_some())
    }

    async fn deleted_before(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<Option<bool>, Error> {
        let before = seq.saturating_sub(1);
        let cut = Cut {
            unsettled: false,
            seq: self.own_pos(before),
            own: self.own_rank(before),
            extra: Vec::new(),
            overlay: None,
            inserted: Arc::default(),
        };
        Ok(self
            .existing_at(&table.stored, row, key, Some(&cut))
            .await?
            .map(|(_, deleted)| deleted))
    }

    /// A USING or read-for-write check of the row a write acts on
    /// (SPEC 7, INV-RLS-9). While a unit is decided
    /// ([`Store::defer_checks`]: at the authority on upload, in a relay's
    /// admission, and in a node's own commit) such a check reads committed
    /// state, as the grounding's first round does: the row as it stood
    /// before the unit and the policy's other rows likewise, never the
    /// unit's own earlier writes. So the unit's decision does not depend on
    /// the order of its writes, and no write vouches for another's USING.
    /// A branch-head row the unit copied from its base acts on that base
    /// row, as the unit's first write to it did. A row the unit created or
    /// revived has no committed state to act on: the end-state check of
    /// its first write ([`Store::write_check`]) decides it. For such a row,
    /// and outside a unit, this returns `None`: the caller checks the
    /// current version.
    async fn holds_on_acted_row(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
        policy: impl FnOnce(&mut Ctx<'_>) -> Result<String, Error>,
    ) -> Result<Option<bool>, Error> {
        if self.deferred.borrow().is_none() {
            return Ok(None);
        }
        let acted = if self.was_live_before(table, row, key, seq).await? {
            key.to_vec()
        } else {
            let base = self
                .inherited_heads
                .borrow()
                .get(&(table.stored.clone(), *row.0.as_bytes(), key.to_vec()))
                .cloned();
            match base {
                Some(from) if self.was_live_before(table, row, &from, seq).await? => from,
                _ => return Ok(None),
            }
        };
        let key = &acted[..];
        let cut = Cut {
            unsettled: false,
            seq: self.own_pos(seq.saturating_sub(1)),
            own: self.own_rank(seq),
            extra: Vec::new(),
            overlay: Some(CutOverlay::Grounded(Arc::default())),
            inserted: Arc::default(),
        };
        self.holds_as_of(active_schema, table, row, key, cut, policy)
            .await
            .map(Some)
    }

    /// Whether `row`'s current version on branch `key` is a tombstone.
    async fn is_deleted_version(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
    ) -> Result<bool, Error> {
        let sql = format!(
            "SELECT (_state & {STATE_DELETED}) <> 0 FROM {} WHERE id = ?1 AND _branch = ?2",
            quote(&table.stored)
        );
        let deleted: Option<i64> = self
            .conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .await
            .optional()
            .map_err(storage)?;
        Ok(deleted.is_none_or(|d| d != 0))
    }

    async fn check_clause(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        author: AuthorSubject,
        clause: Option<&crate::tools::PolicyExpr>,
    ) -> Result<bool, Error> {
        let session = self.session(author);
        self.holds(active_schema, table, row, key, |ctx| {
            ctx.policy_clause(clause, table, "a", &session)
        })
        .await
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
    pub(crate) async fn author_code(&self, key: &AuthorKey) -> Result<i64, Error> {
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
        let find = async |conn: &Storage| {
            conn.cached_row(
                "SELECT n FROM _jazz_authors WHERE account = ?1 AND issuer = ?2 AND subject = ?3",
                crate::storage::params![account, issuer, subject],
                |row| row.get::<_, i64>(0),
            )
            .await
            .optional()
            .map_err(storage)
        };
        let code = match find(&self.conn).await? {
            Some(code) => code,
            None => {
                self.exec(
                    "INSERT INTO _jazz_authors (account, issuer, subject) VALUES (?1, ?2, ?3)",
                    crate::storage::params![account, issuer, subject],
                )
                .await
                .map_err(storage)?;
                self.conn.last_insert_rowid().await.map_err(storage)?
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
    pub(crate) async fn author_parts(&self, code: i64) -> Result<AuthorKey, Error> {
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
            .await
            .map_err(storage)?;
        let account = uuid::Uuid::from_slice(&account)
            .map_err(|e| Error::new(ErrorCode::Storage, format!("author account: {e}")))?;
        let parts = (account, issuer, subject);
        self.remember_author(&parts, code);
        Ok(parts)
    }

    fn validate_write_cells(
        &self,
        write: &WriteTable,
        cells: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        self.validate_named_cells(
            &write.source,
            cells
                .iter()
                .map(|(name, value)| (write.column_name(name), value)),
        )
    }

    fn validate_named_cells<'a>(
        &self,
        table: &Table,
        cells: impl Iterator<Item = (&'a str, &'a Value)>,
    ) -> Result<(), Error> {
        for (name, value) in cells {
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
                for variant in crate::values::enum_names(value) {
                    if !variants.iter().any(|v| v == variant)
                        && !self
                            .kept_variants
                            .borrow()
                            .get(&(table.name.clone(), name.to_owned()))
                            .is_some_and(|kept| kept.contains(variant))
                    {
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

    /// A written cell must be of its column's type: writes from any replica
    /// land here, so the store never holds a value its schema does not
    /// describe. A nullable column also takes the bare value; a large
    /// value's reference is checked where it is bound.
    fn check_cell<'a>(
        column: &crate::values::Column,
        value: &'a Value,
        cells: Cells,
    ) -> Result<&'a Value, Error> {
        let mut bare = value;
        while let Value::Nullable(Some(inner)) = bare {
            bare = inner;
        }
        if matches!(bare, Value::Large(_)) {
            return Ok(bare);
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
        Ok(bare)
    }

    /// A cell for stored column `column` of stored table `table` written
    /// as plain SQL keeps a value (a lens's cell, an image from another
    /// store): an enum variant's name as its tag here.
    pub(crate) async fn enum_cell(
        &self,
        table: &str,
        column: &str,
        cell: Sql,
    ) -> Result<Sql, Error> {
        let Sql::Text(text) = &cell else {
            return Ok(cell);
        };
        let names = self.names();
        let Some(ty) = names
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
        self.enum_value_cell(table, column, &value).await
    }

    /// Enum `value` as stored enum column `column` of stored table
    /// `table`'s cell: its variants' tags there, interned if new.
    async fn enum_value_cell(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<Sql, Error> {
        self.names()
            .intern(&self.conn, table, column, &crate::values::enum_names(value))
            .await?;
        let tags = self.names().tags().column(table, column);
        let cell = crate::values::enum_cell(&tags.borrow(), value)?;
        Ok(cell.unwrap_or(Sql::Null))
    }

    fn bind_value(value: &Value) -> Result<Sql, Error> {
        to_sql(value)?.ok_or_else(|| unsupported("storing NaN"))
    }

    /// A value as the cell `column` stores. A string, JSON or bytes value
    /// above the inline threshold is chunked: its nodes are stored in this
    /// transaction and the cell holds their descriptor.
    async fn bind_cell(
        &self,
        table: &Table,
        column: &crate::values::Column,
        value: &Value,
        cells: Cells,
    ) -> Result<Sql, Error> {
        let inner = Self::check_cell(column, value, cells)?;
        if column.is_enum() {
            // Every variant a version declares has a tag; a write's are
            // checked against them, but a lens or merge may still carry a
            // removed one.
            self.names()
                .intern(
                    &self.conn,
                    &table.stored,
                    &column.stored,
                    &crate::values::enum_names(value),
                )
                .await?;
            return column
                .cell_sql(value)?
                .ok_or_else(|| unsupported("storing NaN"));
        }
        let logical = match inner {
            Value::String(text) => text.as_bytes(),
            Value::Bytes(bytes) => bytes.as_slice(),
            Value::Large(large) => {
                self.refuse_latest_only_large(table)?;
                return self.bind_large(column, large, cells).await;
            }
            _ => return Self::bind_value(value),
        };
        if logical.len() <= crate::large::inline_max(column) || !crate::large::large_capable(column)
        {
            return Self::bind_value(value);
        }
        self.refuse_latest_only_large(table)?;
        let prepared = jazz_types::large_values::prepare(crate::large::kind(column), logical)
            .map_err(|e| rejected(format!("`{}`: {e}", column.name)))?;
        crate::large::store(&self.conn, &self.chunks, &prepared, self.client().is_some()).await?;
        Ok(Sql::Integer(
            crate::large::intern(&self.conn, &self.chunks, &prepared.value_ref).await?,
        ))
    }

    /// A chunked value names the version it extends (a delta) or proves
    /// access through (SPEC 19.4, 19.8), which a latest-only table drops
    /// (SPEC 4 §4.9), so such a table holds inline values only.
    fn refuse_latest_only_large(&self, table: &Table) -> Result<(), Error> {
        if self.latest_only(&table.stored) {
            return Err(unsupported(format!(
                "a value over {} bytes in latest-only table `{}`",
                jazz_types::large_values::INLINE_VALUE_MAX_BYTES,
                table.name
            )));
        }
        Ok(())
    }

    fn check_large_kind(
        column: &crate::values::Column,
        large: &jazz_types::large_values::LargeValueRef,
    ) -> Result<(), Error> {
        if !crate::large::large_capable(column) || large.kind != crate::large::kind(column) {
            return Err(rejected(format!(
                "`{}` does not hold {:?} large values",
                column.name, large.kind
            )));
        }
        Ok(())
    }

    /// A chunked value written by reference: an edit's consolidated tree
    /// (stored now, inside the write's transaction), or a tree whose nodes
    /// this store already holds. An authored JSON value is checked whole,
    /// wherever its tree is complete here: always at the authority, so no
    /// writer gets past a schema by sending a value large enough to chunk.
    async fn bind_large(
        &self,
        column: &crate::values::Column,
        large: &jazz_types::large_values::LargeValueRef,
        cells: Cells,
    ) -> Result<Sql, Error> {
        Self::check_large_kind(column, large)?;
        let possession = self.possession.borrow().clone();
        let mut complete = true;
        let prepared = self
            .pending_nodes
            .borrow()
            .iter()
            .find(|prepared| prepared.value_ref.root == large.root)
            .cloned();
        if let Some(prepared) = prepared {
            crate::large::store(&self.conn, &self.chunks, &prepared, self.client().is_some())
                .await?;
        } else if self.derived.borrow().contains(large) {
            // Derived here from a stored tree by an append or splice.
            #[cfg(any(test, feature = "testing"))]
            DERIVED_BINDS.with(|binds| binds.set(binds.get() + 1));
            complete = self.client().is_none();
        } else if let Some(possession) = &possession
            && !crate::large::possessed(&self.conn, &self.chunks, large, possession).await?
        {
            // Answered as for absent nodes: no oracle for what this store
            // holds for others (SPEC 19.9).
            return Err(rejected(format!(
                "`{}`: the large-value descriptor names chunks this store does not hold",
                column.name
            )));
        } else if self.client().is_some() {
            // A replica's tree may be only partly fetched: the authority
            // checks the value when it admits the write.
            complete = false;
        } else {
            // An authority admits a descriptor only over a complete, honest
            // tree. A replica's edits build on trees its upstream holds,
            // which it may have only partly fetched.
            if !crate::large::root_is_local(&self.conn, large).await? {
                return Err(rejected(format!(
                    "`{}`: the large-value descriptor names chunks this store does not hold",
                    column.name
                )));
            }
            crate::large::admit(&self.conn, &self.chunks, large).await?;
        }
        if let Some(json) = &column.json_type
            && cells == Cells::Authored
            && complete
        {
            let bytes = self.chunks.value_bytes(large).await?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                rejected(format!(
                    "invalid JSON for column `{}`: not UTF-8",
                    column.name
                ))
            })?;
            json.check_text(text, &column.name).map_err(rejected)?;
        }
        if let Some(possession) = &possession {
            possession.settle.borrow_mut().push(large.root.object_hash);
        }
        self.large_cell(large, crate::large::inline_max(column))
            .await
    }

    /// Settle the receipts of the trees the applied commit's cells proved
    /// (SPEC 19.4): only after all its cells, which may share nodes.
    pub(crate) async fn settle_possession(&self) -> Result<(), Error> {
        let Some(possession) = self.possession.borrow().clone() else {
            return Ok(());
        };
        for root in possession.settle.take() {
            crate::large::settle_receipts(&self.conn, &root, &possession.author).await?;
        }
        Ok(())
    }

    /// The cell of a chunked value received from upstream: always the
    /// chunked cell. Receiving a descriptor never pulls nodes (SPEC 19.8),
    /// so one at or under this column's threshold (from a peer under
    /// another threshold or schema) is not read to inline it; cells are
    /// store-local, and its first local write stores it canonically.
    pub(crate) async fn received_cell(
        &self,
        large: &jazz_types::large_values::LargeValueRef,
    ) -> Result<Sql, Error> {
        Ok(Sql::Integer(
            crate::large::intern(&self.conn, &self.chunks, large).await?,
        ))
    }

    /// The cell of a chunked value this store holds the chunks for, in a
    /// column whose inline threshold is `inline_max`
    /// ([`crate::large::inline_max`]). A value an edit shrank to the
    /// threshold is stored inline, so each logical value has exactly one
    /// inline cell.
    pub(crate) async fn large_cell(
        &self,
        large: &jazz_types::large_values::LargeValueRef,
        inline_max: usize,
    ) -> Result<Sql, Error> {
        if large.byte_length <= inline_max as u64 {
            let bytes = self.chunks.value_bytes(large).await?;
            return Ok(crate::large::inline_cell(large.kind, bytes));
        }
        Ok(Sql::Integer(
            crate::large::intern(&self.conn, &self.chunks, large).await?,
        ))
    }

    /// Every stored column of `table`, including ones only other schema
    /// views know: history and copy-on-write carry whole stored rows.
    fn columns_list(&self, table: &Table) -> String {
        let physical_schema = self.physical();
        let physical = physical_schema.tables.get(&table.stored).unwrap_or(table);
        physical
            .stored()
            .map(|c| format!(", {}", quote(&c.stored)))
            .collect()
    }

    /// The stored value of `column` in `row` on branch `key`, if any.
    async fn cell_at(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        column: &str,
    ) -> Result<Option<Value>, Error> {
        let Some(column) = table.columns.iter().find(|c| c.name == column) else {
            return Ok(None);
        };
        match self.stored_cell(table, row, key, column).await? {
            Some(cell) => column_from_sql(column, cell.as_ref(), &self.chunks).await,
            None => Ok(None),
        }
    }

    /// Chunk a write's string, JSON and bytes cells above the inline
    /// threshold once, up front: each becomes a reference to its prepared
    /// tree, which the returned edits carry until the write applies. The
    /// write then travels (to the authority, in a pending upload, through
    /// every overlay replay) as that reference instead of the whole value,
    /// and its nodes are pushed as chunks ahead of it.
    ///
    /// A root update over a chunked value writes the edit that makes it
    /// ([`Self::whole_write_as_edit`]): an editor that writes its document
    /// whole on every keystroke costs a few bytes per keystroke, on the wire
    /// and in history.
    pub(crate) async fn chunk_large_cells(
        &self,
        prepared: &mut PreparedWrite,
    ) -> Result<LargeEdits, Error> {
        let mut edits = LargeEdits::default();
        let write = &*prepared.layout;
        let op = &mut prepared.op;
        let (row, cells, mut insert_only) = match op {
            StagedOp::Insert(_, c, _) => (None, c, None),
            StagedOp::Update(_, r, c, o) => {
                let root = matches!(o.target, crate::options::WriteTarget::Root);
                (root.then_some((*r, o.identity)), c, None)
            }
            StagedOp::Upsert(_, r, c, created, o) => {
                let root = matches!(o.target, crate::options::WriteTarget::Root);
                (root.then_some((*r, o.identity)), c, Some(created))
            }
            StagedOp::Restore(_, _, Some(c), _) => (None, c, None),
            _ => return Ok(edits),
        };
        let table = &write.table;
        for column in &table.columns {
            if !crate::large::large_capable(column) {
                continue;
            }
            // A created row's cells are new values: no edit of the row's.
            let (mut value, base) = if let Some(value) = cells.get_mut(&column.name) {
                (value, row.as_ref())
            } else if let Some(value) = insert_only.as_mut().and_then(|c| c.get_mut(&column.name)) {
                (value, None)
            } else {
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
            if logical.len() <= crate::large::inline_max(column) {
                continue;
            }
            if let Some((row, identity)) = base {
                let marks = (
                    self.derived.borrow().len(),
                    self.pending_nodes.borrow().len(),
                );
                let read = large_ops::PreparedCell::new(
                    write.policies.clone(),
                    &write.source.name,
                    *row,
                    &write
                        .source
                        .by_stored_column(&column.stored)
                        .unwrap_or(column)
                        .name,
                );
                if let Ok(read) = read
                    && let Some(edited) = self.whole_write_as_edit(&read, logical, *identity).await
                {
                    // The write carries what the edit made, as any edit's does.
                    edits
                        .derived
                        .extend(self.derived.borrow_mut().drain(marks.0..));
                    edits
                        .pending_nodes
                        .extend(self.pending_nodes.borrow_mut().drain(marks.1..));
                    *value = edited;
                    continue;
                }
            }
            let prepared = jazz_types::large_values::prepare(crate::large::kind(column), logical)
                .map_err(|e| rejected(format!("`{}`: {e}", column.name)))?;
            *value = Value::Large(Box::new(prepared.value_ref.clone()));
            edits.pending_nodes.push(Rc::new(prepared));
        }
        Ok(edits)
    }

    /// Turn a write's counter cells from the values it sets into the
    /// changes it makes to what this node holds (see [`crate::merge`]).
    pub(crate) async fn counter_deltas(&self, prepared: &mut PreparedWrite) -> Result<(), Error> {
        let write = &*prepared.layout;
        let table = &write.table;
        let op = &mut prepared.op;
        let (row, cells, target) = match op {
            StagedOp::Update(_, r, c, o) => (*r, c, &o.target),
            StagedOp::Upsert(_, r, c, _, o) => (*r, c, &o.target),
            _ => return Ok(()),
        };
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
        let target = Target::of(target, self).await?;
        let held_at = match self.locate(table, row, &target).await? {
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
                Some(key) => self.cell_at(table, row, key, &name).await?,
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
    async fn merged_cells(
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
                    match self
                        .text_merged(table, row, held, at, column, value)
                        .await?
                    {
                        Some(merged) => self.merged_text(table, row, held, column, merged).await?,
                        None => continue,
                    }
                }
                MergeStrategy::Counter | MergeStrategy::GSet => {
                    let current = match held {
                        Some(key) => self.cell_at(table, row, key, &column.name).await?,
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

    /// A text merge's result as the column stores it. Over a chunked held
    /// value, it is that value with the one splice that makes the merge, so
    /// the merge costs its epoch a few bytes and travels as an edit, as the
    /// keystrokes it merges did; any other merge is written whole.
    async fn merged_text(
        &self,
        table: &Table,
        row: RowUuid,
        held: Option<&[u8]>,
        column: &crate::values::Column,
        merged: String,
    ) -> Result<Value, Error> {
        // A held value this store cannot read here (its chunks not fetched)
        // leaves the merge whole, as a whole write is left.
        let edited = match held {
            Some(held) if merged.len() > crate::large::inline_max(column) => {
                (async {
                    let cell = self
                        .stored_cell(table, row, held, column)
                        .await
                        .ok()
                        .flatten()?;
                    self.as_edit_of(&table.name, column, cell, merged.as_bytes())
                        .await
                })
                .await
            }
            _ => None,
        };
        let value = edited.unwrap_or(Value::String(merged));
        Ok(if column.nullable() {
            Value::Nullable(Some(Box::new(value)))
        } else {
            value
        })
    }

    /// A column's stored cell in branch `key` of a row: its SQLite value,
    /// before chunks are read.
    async fn stored_cell(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        column: &crate::values::Column,
    ) -> Result<Option<Sql>, Error> {
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2",
            quote(&column.stored),
            quote(&table.stored)
        );
        self.conn
            .cached_row(
                &sql,
                crate::storage::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .await
            .optional()
            .map_err(storage)
    }

    /// A text write merged on the authority (SPEC 4 §4.3): when a version
    /// its writer never saw authored the column, the merge of the value it
    /// was built on, the value held and its own; `None` keeps its value as
    /// sent. Every text write of an upload notes its value as sent (none
    /// when unmerged): a later write built on it, in the same upload or a
    /// later one, was built on that value.
    async fn text_merged(
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
        let Some(built) = self.merge_bases.borrow().get(&version).copied() else {
            return Ok(None);
        };
        let merged = self
            .text_merge(table, row, held, (key, seq), built, column, value)
            .await?;
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
    /// decided first, from the row's `_base` and `_touched` alone. The write's
    /// value is bound before it is read, so a descriptor naming chunks its
    /// writer does not possess is refused (SPEC 19.4), merged or not.
    #[allow(clippy::too_many_arguments)]
    async fn text_merge(
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
        let current = async |db: &Self| -> Result<Option<String>, Error> {
            Ok(match db.cell_at(table, row, held, &column.name).await? {
                Some(current) => db.optional_text(&column.name, &current).await?,
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
                let Some(current) = current(self).await? else {
                    return Ok(None);
                };
                match sent {
                    Some((sent, _)) => (self.logical_text(&column.name, &sent).await?, current),
                    None => (current.clone(), current),
                }
            }
            None => {
                let Some(base) = self
                    .merge_base((&table.stored, row, key), built, seq, &column.stored)
                    .await?
                else {
                    return Ok(None);
                };
                // A NULL base merges as "": additions to an empty optional
                // column are all kept, as they are over an empty string.
                let base = match column_from_sql(column, base.as_ref(), &self.chunks).await? {
                    Some(base) => self
                        .optional_text(&column.name, &base)
                        .await?
                        .unwrap_or_default(),
                    None if column.nullable() => String::new(),
                    None => return Ok(None),
                };
                let Some(current) = current(self).await? else {
                    return Ok(None);
                };
                (base, current)
            }
        };
        let cell = self
            .bind_cell(table, column, value, Cells::Authored)
            .await?;
        let incoming = self.logical_text(&column.name, value).await?;
        let format = column.merge.text_format().expect("a text strategy");
        let Some(merged) = jazz_text::try_merge3_within(
            format,
            &base,
            &current,
            &incoming,
            self.merge_budget.get(),
        ) else {
            // Past the work budget the write stands as sent: the held
            // value's concurrent edits leave the current text (history
            // keeps them). The host is told, so it can say so.
            self.merge_fallbacks.borrow_mut().record(MergeFallback {
                table: table.name.clone(),
                row,
                column: column.name.clone(),
                bytes: base.len().max(current.len()).max(incoming.len()),
            });
            return Ok(None);
        };
        Ok((merged != incoming).then_some((merged, cell)))
    }

    /// Keep, for each row version the upload at `seq` wrote whose text
    /// merge changed a value, that version as its writer sent it (SPEC 4
    /// §4.3): in `__h` at `_clock` = minus its own `_clock` with [`STATE_AS_SENT`] set, its
    /// merged columns holding their values as sent and every other cell
    /// the version's. A later write chained on it merges over these values
    /// ([`Store::merge_base`]); no read takes it for a version.
    ///
    /// First drop the as-sent versions its sender `node` can no longer
    /// build on. Only a commit's own sender builds on it unsettled, and its
    /// commits arrive in order. Once one of them writes a row over an
    /// accepted version (or over none), every later one it sends is built
    /// on that write or a newer one, and a chain walk stops at the first
    /// accepted version: the sender's older as-sent versions of the row
    /// are dead. So a row keeps them only for its writers' current
    /// unsettled chains.
    pub(crate) async fn record_as_sent(
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
                    "DELETE FROM {} WHERE id = ?1 AND _branch = ?2 AND _clock < 0 \
                     AND (_state & {STATE_AS_SENT}) <> 0 AND EXISTS (SELECT 1 FROM _jazz_transactions AS s \
                     JOIN _jazz_nodes AS n ON n.n = s.n WHERE s.t = -_clock AND n.node = ?3)",
                    quote(&format!("{stored}__h"))
                ),
                crate::storage::params![row.0.as_bytes().to_vec(), key, node.to_vec()],
            )
            .await.map_err(storage)?;
        }
        let as_sent = std::mem::take(&mut *self.as_sent.borrow_mut());
        for ((stored, row, key), columns) in as_sent {
            let physical_schema = self.physical();
            let Some(table) = physical_schema.tables.get(&stored) else {
                continue;
            };
            let mut values = vec![
                Sql::Blob(row.0.as_bytes().to_vec()),
                Sql::Blob(key),
                Sql::Integer(self.own_pos(seq).t()),
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
                "INSERT INTO {} (id, _branch, _clock, _writer, _state, _base, _touched{columns}) \
                 SELECT id, _branch, -_clock, _writer, _state | {STATE_AS_SENT}, NULL, NULL{cells} FROM {} \
                 WHERE id = ?1 AND _branch = ?2 AND _clock = ?3",
                quote(&format!("{stored}__h")),
                quote(&stored),
            );
            self.exec(&sql, params_from_iter(values.iter()))
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    /// How many as-sent versions ([`STATE_AS_SENT`]) the store keeps.
    #[cfg(feature = "testing")]
    pub(crate) async fn as_sent_versions(&self) -> Result<u64, Error> {
        let mut count = 0;
        for stored in self.physical().tables.keys() {
            let n: i64 = self
                .conn
                .cached_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE (_state & {STATE_AS_SENT}) <> 0",
                        quote(&format!("{stored}__h"))
                    ),
                    [],
                    |r| r.get(0),
                )
                .await
                .map_err(storage)?;
            count += n as u64;
        }
        Ok(count)
    }

    /// [`Self::logical_text`] of a cell that may be NULL (`None`).
    async fn optional_text(&self, column: &str, value: &Value) -> Result<Option<String>, Error> {
        match value {
            Value::Nullable(None) => Ok(None),
            value => self.logical_text(column, value).await.map(Some),
        }
    }

    /// The whole string a text column's value holds.
    async fn logical_text(&self, column: &str, value: &Value) -> Result<String, Error> {
        match value {
            Value::String(text) => Ok(text.clone()),
            Value::Nullable(Some(value)) => Box::pin(self.logical_text(column, value)).await,
            Value::Large(large) => String::from_utf8(self.chunks.value_bytes(large).await?)
                .map_err(|_| rejected(format!("`{column}` holds a value that is not UTF-8"))),
            _ => Err(rejected(format!("`{column}` is not a string"))),
        }
    }

    /// Copy the row's current image into its history before it is
    /// superseded (history holds superseded images only).
    async fn snapshot(
        &self,
        prune: bool,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        seq: u64,
    ) -> Result<(), Error> {
        let id = row.0.as_bytes().to_vec();
        let current: Option<(i64, Vec<u8>)> = self
            .conn
            .cached_row(
                &format!(
                    "SELECT _clock, _writer FROM {} WHERE id = ?1 AND _branch = ?2",
                    quote(&table.stored)
                ),
                crate::storage::params![id, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .await
            .optional()
            .map_err(storage)?;
        let Some((at, w)) = current.map(|(t, w)| (Pos::from_t(t), w)) else {
            return Ok(());
        };
        // An image written earlier in the same commit is not superseded
        // history: history holds committed images only.
        if at == self.own_pos(seq) && w == self.own_writer_at(seq) {
            return Ok(());
        }
        let columns = self.columns_list(table);
        let sql = format!(
            "INSERT INTO {} (id, _branch, _clock, _writer, _state, _base, _touched{columns}) \
             SELECT id, _branch, _clock, _writer, _state, _base, _touched{columns} FROM {} \
             WHERE id = ?1 AND _branch = ?2",
            quote(&format!("{}__h", table.stored)),
            quote(&table.stored),
        );
        self.exec(&sql, crate::storage::params![id, key])
            .await
            .map_err(storage)?;
        self.prune_history(prune, &table.stored, row, key).await
    }

    /// Whether stored table `stored` may have dropped history: some
    /// admitted version, or the owner's schema, keeps only its latest
    /// version (SPEC 4 §4.9). Reads that need history refuse such a table
    /// and its writes count as blind, even once every bundle that declared
    /// it so is rolled back: what was pruned stays pruned.
    pub(crate) fn latest_only(&self, stored: &str) -> bool {
        self.retention().contains(stored)
    }

    /// The stored tables that may have dropped history: those any admitted
    /// version, or the owner's schema, declares latest-only. Cached, keyed
    /// by what was admitted: a version id names its schema's content.
    pub(crate) fn retention(&self) -> Rc<BTreeSet<String>> {
        let key = (
            self.names().len(),
            self.admitted().len(),
            self.owner_version.get(),
        );
        if let Some((cached, set)) = &*self.retention.borrow()
            && *cached == key
        {
            return set.clone();
        }
        let mut latest = (*self.retention_of(self.owner_version.get())).clone();
        for (version, _) in self.admitted().iter() {
            latest.extend(self.retention_of(*version).iter().cloned());
        }
        let latest = Rc::new(latest);
        *self.retention.borrow_mut() = Some((key, latest.clone()));
        latest
    }

    /// The stored tables schema `version` declares latest-only: the ones a
    /// write made under it prunes (SPEC 4 §4.9). The owner's version, and a
    /// version this store has not admitted, go by the owner's schema.
    pub(crate) fn retention_of(
        &self,
        version: crate::ids::SchemaVersionId,
    ) -> Rc<BTreeSet<String>> {
        if let Some(set) = self.retention_by_version.borrow().get(&version) {
            return set.clone();
        }
        let admitted = match (
            self.admitted().iter().find(|(id, _)| *id == version),
            self.names().get(&version),
        ) {
            (Some((_, jazz)), Some(names)) if version != self.owner_version.get() => Some(
                jazz.tables()
                    .iter()
                    .filter(|table| table.history == jazz_model::schema::HistoryRetention::Latest)
                    .filter_map(|table| {
                        names
                            .tables
                            .get(&table.name)
                            .map(|(stored, _)| stored.clone())
                    })
                    .collect::<BTreeSet<String>>(),
            ),
            _ => None,
        };
        let cached = admitted.is_some() || version == self.owner_version.get();
        let declared = Rc::new(admitted.unwrap_or_else(|| {
            self.owner()
                .tables
                .values()
                .filter(|table| table.latest_only)
                .map(|table| table.stored.clone())
                .collect()
        }));
        // A version's content is fixed, but one not admitted yet may be
        // later: only an admitted version's (or the owner's) is kept.
        if cached {
            self.retention_by_version
                .borrow_mut()
                .insert(version, declared.clone());
        }
        declared
    }

    /// For a latest-only table, drop the row's accepted versions in history
    /// older than its newest one there (`INV-HIST-26`). That one stays: it
    /// is the image just superseded, which keyed fanout and a commit's
    /// write checks read, or on a replica the settled image under its
    /// pending writes. Pending versions (a writer's, past every accepted
    /// `_clock`) stay. Through the history table's key, so it touches only
    /// this row.
    pub(crate) async fn prune_history(
        &self,
        prune: bool,
        stored: &str,
        row: RowUuid,
        key: &[u8],
    ) -> Result<(), Error> {
        if !prune {
            return Ok(());
        }
        let history = quote(&format!("{stored}__h"));
        let pending = Pos::UNSETTLED.t();
        self.exec(
            &format!(
                "DELETE FROM {history} WHERE id = ?1 AND _branch = ?2 AND _writer = X'' \
                 AND _clock >= 0 AND _clock < (SELECT max(_clock) FROM {history} \
                 WHERE id = ?1 AND _branch = ?2 AND _writer = X'' AND _clock BETWEEN 0 AND ?3 - 1)"
            ),
            crate::storage::params![row.0.as_bytes().to_vec(), key, pending],
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Copy-on-write: materialize an inherited base row into the head.
    #[allow(clippy::too_many_arguments)]
    async fn copy_on_write(
        &self,
        stamp: &Stamp,
        table: &Table,
        row: RowUuid,
        from: &[u8],
        to: &[u8],
        head_cells: &BTreeMap<String, Value>,
        at: Option<&Cut>,
        seq: u64,
    ) -> Result<(), Error> {
        self.inherited_heads
            .borrow_mut()
            .entry((table.stored.clone(), *row.0.as_bytes(), to.to_vec()))
            .or_insert_with(|| from.to_vec());
        let columns = self.columns_list(table);
        let id = row.0.as_bytes().to_vec();
        // A frozen base is copied as it was at the cut.
        let source = match at {
            None => Some(quote(&table.stored)),
            Some(cut) => match self.existing(&table.stored, row, from).await? {
                Some((current, _))
                    if cut.sees(self.current_rank(&table.stored, row, from, current).await?) =>
                {
                    Some(quote(&table.stored))
                }
                _ => self
                    .version_at(&table.stored, row, from, cut)
                    .await?
                    .map(|(version, w)| {
                        format!(
                            "(SELECT * FROM {} WHERE _clock = {} AND _writer = X'{}' AND id = ?3 \
                             AND _branch = ?4)",
                            quote(&format!("{}__h", table.stored)),
                            version.t(),
                            w.iter().map(|b| format!("{b:02x}")).collect::<String>()
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
            "INSERT INTO {name} (id, _branch, _clock, _writer, _state{columns}) \
             SELECT id, ?1, ?2, ?5, _state | {STATE_NO_CONTENT}{columns} \
             FROM {source} WHERE id = ?3 AND _branch = ?4",
            name = quote(&table.stored),
        );
        let w = self.own_writer_at(seq);
        self.exec(
            &sql,
            crate::storage::params![to, self.own_pos(seq).t(), id, from, w],
        )
        .await
        .map_err(storage)?;
        self.set_cells(
            stamp,
            table,
            table,
            row,
            to,
            head_cells,
            Cells::Carried,
            seq,
            None,
            false,
        )
        .await
    }

    /// Write cells (and the deletion flag) into a row's current version
    /// at `seq`. `content` says whether the write authored content; a
    /// version stays a content version once any write of its commit did.
    #[allow(clippy::too_many_arguments)]
    async fn set_cells(
        &self,
        stamp: &Stamp,
        table: &Table,
        declared: &Table,
        row: RowUuid,
        key: &[u8],
        cells: &BTreeMap<String, Value>,
        source: Cells,
        seq: u64,
        deleted: Option<bool>,
        content: bool,
    ) -> Result<(), Error> {
        // Right-hand sides read the row before this update: `_clock` is still
        // the previous version's.
        // What a new version was built on is the authority's to write
        // (SPEC 4 §4.6).
        let mut values = vec![Sql::Integer(self.own_pos(seq).t())];
        let deletion = match deleted {
            Some(deleted) => {
                values.push(Sql::Integer(if deleted { STATE_DELETED } else { 0 }));
                format!("?{}", values.len())
            }
            None => format!("(_state & {STATE_DELETED})"),
        };
        // The no-content bit stays set only while no write of the commit
        // authored content.
        let no_content = if content { 0 } else { STATE_NO_CONTENT };
        values.push(Sql::Blob(self.own_writer_at(seq)));
        let mut sets = vec![
            "_clock = ?1".to_owned(),
            format!("_writer = ?{}", values.len()),
            "_base = NULL, _touched = NULL".to_owned(),
            format!(
                "_state = (CASE WHEN _clock = ?1 THEN _state & {no_content} ELSE {no_content} END) \
                 | {deletion}"
            ),
        ];
        for (name, value) in cells {
            let cell = match table.column(name) {
                Some(column) => {
                    self.bind_cell(
                        table,
                        declared.by_stored_column(&column.stored).unwrap_or(column),
                        value,
                        source,
                    )
                    .await?
                }
                None => Self::bind_value(value)?,
            };
            values.push(cell);
            sets.push(format!(
                "{} = ?{}",
                quote(table.stored_name(name)),
                values.len()
            ));
        }
        for (name, value) in stamp.updated() {
            values.push(value);
            sets.push(format!("{} = ?{}", quote(name), values.len()));
        }
        values.push(Sql::Blob(row.0.as_bytes().to_vec()));
        values.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "UPDATE {} SET {} WHERE id = ?{} AND _branch = ?{}",
            quote(&table.stored),
            sets.join(", "),
            values.len() - 1,
            values.len()
        );
        self.exec(&sql, params_from_iter(values.iter()))
            .await
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.stored.clone(), row, key.to_vec()));
        Ok(())
    }

    /// The writer of a version commit `seq` writes: none for an accepted one.
    fn own_writer_at(&self, seq: u64) -> Vec<u8> {
        if self.own_pos(seq).accepted() {
            Vec::new()
        } else {
            self.own_writer(seq)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert(
        &self,
        stamp: &Stamp,
        write: &WriteTable,
        mut cells: BTreeMap<String, Value>,
        source: Cells,
        row_id: Option<RowUuid>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let table = &write.table;
        let table_name = &write.source.name;
        self.validate_write_cells(write, &cells)?;
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
        if write.stored_names {
            for column in &table.branch_by {
                if !cells.contains_key(column)
                    && let Some(default) = table.column(column).and_then(|c| c.default.clone())
                {
                    cells.insert(column.clone(), default);
                }
            }
        }
        let key = table.row_branch_key(&cells);
        let row = row_id.unwrap_or_else(|| self.ids.borrow_mut().next_row_id());
        let cells = self.merged_cells(table, row, None, None, cells).await?;
        self.refuse_withheld(table, row, &key).await?;
        match self.existing(&table.stored, row, &key).await? {
            Some((_, true)) => return Err(row_already_deleted(row)),
            Some((_, false)) => return Err(row_already_exists(row)),
            None => {}
        }
        // Most stores have no lens cells; the owner's version is cached
        // (hashing a schema per insert would dominate a large batch's replay).
        let (copies, dropped) = if !self.names().has_lens_cells() {
            Default::default()
        } else if write.stored_names {
            let copies = self
                .names()
                .stored_copies(&table.stored, |column| cells.contains_key(column));
            (copies, Vec::new())
        } else {
            let version = write.version;
            self.names().lens_cells(&version, table)
        };
        let mut names = vec![
            "id".to_owned(),
            "_branch".to_owned(),
            "_clock".to_owned(),
            "_writer".to_owned(),
        ];
        let mut values = vec![
            Sql::Blob(row.0.as_bytes().to_vec()),
            Sql::Blob(key.clone()),
            Sql::Integer(self.own_pos(seq).t()),
            Sql::Blob(self.own_writer_at(seq)),
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
                    if write.stored_names
                        && (default.is_some()
                            || column.nullable()
                            || copies.iter().any(|(_, copy)| *copy == column.stored)) =>
                {
                    continue;
                }
                (None, Some(default)) => Some((default, Cells::Authored)),
                (None, None) if column.nullable() => None,
                (None, None) => {
                    return Err(missing_required(
                        table_name,
                        write.column_name(&column.stored),
                    ));
                }
            };
            let value = match given {
                Some((value, cells)) => {
                    self.bind_cell(
                        table,
                        write
                            .source
                            .by_stored_column(&column.stored)
                            .unwrap_or(column),
                        value,
                        cells,
                    )
                    .await?
                }
                None => Sql::Null,
            };
            for (_, copy) in copies.iter().filter(|(c, _)| *c == column.name) {
                names.push(quote(copy));
                // An enum copy's tags are its own.
                values.push(match given {
                    Some((given, _)) if column.is_enum() => {
                        self.enum_value_cell(&table.stored, copy, given).await?
                    }
                    _ => value.clone(),
                });
            }
            names.push(quote(&column.stored));
            values.push(value);
        }
        for (stored, default) in dropped {
            let default = self.enum_cell(&table.stored, &stored, default).await?;
            names.push(quote(&stored));
            values.push(default);
        }
        // An enum column this version leaves out whose `DEFAULT` tag is
        // stale takes its default by name.
        for (stored, default) in self.names().enum_defaults(&table.stored) {
            if names.contains(&quote(&stored)) {
                continue;
            }
            let default = self
                .enum_value_cell(&table.stored, &stored, &Value::String(default))
                .await?;
            names.push(quote(&stored));
            values.push(default);
        }
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
            .await
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.stored.clone(), row, key.clone()));
        // The row had no version, the unit's own writes included: a fresh
        // insert.
        self.note_unit_insert(table, row, &key);
        if let Some(author) = self.checked(who) {
            self.write_check(
                &write.policies,
                &write.source,
                row,
                &key,
                author,
                WriteCheck::Insert,
                seq,
            )
            .await?;
        } else {
            self.note_trusted(table, row);
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
            tx: seq,
        })
    }

    /// The session a write's operation policies (USING, WITH CHECK,
    /// INSERT, DELETE) are checked as here: a trusted session's, at the
    /// authority. A replica leaves them to the authority on upload: it may
    /// lack rows those policies read (hidden from it).
    /// Undo a transaction handle's overlay, if one is applied.
    fn settle_overlay(&self) {
        let Some(overlay) = self.overlay.take() else {
            return;
        };
        self.changed.borrow_mut().truncate(overlay.mark);
        end_savepoint(&self.conn, "overlay", false).expect("rolling back a transaction overlay");
        self.start_unit_heads();
    }

    fn checked(&self, who: Who) -> Option<AuthorSubject> {
        match who {
            Who::Session(author) if self.client().is_none() => Some(author),
            _ => None,
        }
    }

    /// The refusal of a write to a deleted row. At the authority, a session
    /// that may read the row is told it is deleted, and one that may not is
    /// told what it is told for a missing row, so a tombstone reveals no
    /// more than a live row does. A replica evaluates no policy for a
    /// session's write, so it gives every session the missing row's answer.
    async fn refuse_deleted(
        &self,
        active_schema: &Rc<Schema>,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        who: &Who,
        verb: &str,
    ) -> Result<Error, Error> {
        if let Who::Session(author) = *who
            && (self.client().is_some()
                || !self
                    .check(active_schema, table, row, key, author, Operation::Select)
                    .await?)
        {
            return Ok(if verb == "DELETE" {
                delete_denied()
            } else {
                unreadable_target(verb, &table.name)
            });
        }
        Ok(row_already_deleted(row))
    }

    /// Read-for-write (SPEC 7, INV-RLS-20): a session may update or upsert
    /// only a row it may read, and omitted cells must not be merged blind.
    /// Only the authority decides it, with the other write policies. A
    /// replica acting for a session (a backend's request) stages the write
    /// and its waits report the authority's refusal: the replica may lack
    /// the rows that grant access (a fresh process has loaded none).
    /// Until the authority answers, the staged version is judged by the
    /// read policy like any other: a patch of a column the policy reads,
    /// on a hidden row, can show the row's other cells to a read that
    /// includes pending versions, the writer's or another session's that
    /// the patch names (`owner = carol`). Those are `LocalFirst` and
    /// `LocalOnly` reads, and every read inside an open transaction, which
    /// sees its own staged writes whatever its tier. A `Remote` read
    /// outside a transaction leaves pending versions out (it is
    /// settled-only on a replica), so it shows only accepted rows under
    /// their own policy; that is a backend's default read.
    async fn authorize_patch(
        &self,
        write: &WriteTable,
        row: RowUuid,
        key: &[u8],
        who: &Who,
        verb: &str,
        seq: u64,
    ) -> Result<(), Error> {
        let active_schema = &write.policies;
        let table = &write.source;
        let Some(author) = self.checked(*who) else {
            return Ok(());
        };
        let readable = self
            .holds_on_acted_row(active_schema, table, row, key, seq, |ctx| {
                ctx.operation_policy(table, Operation::Select, "a", &self.session(author))
            })
            .await?;
        if !match readable {
            Some(allowed) => allowed,
            None => {
                self.check(active_schema, table, row, key, author, Operation::Select)
                    .await?
            }
        } {
            return Err(unreadable_target(verb, &table.name));
        }
        let using = table.policies.update.using.as_ref();
        let usable = self
            .holds_on_acted_row(active_schema, table, row, key, seq, |ctx| {
                ctx.policy_clause(using, table, "a", &self.session(author))
            })
            .await?;
        if !match usable {
            Some(allowed) => allowed,
            None => {
                self.check_clause(active_schema, table, row, key, author, using)
                    .await?
            }
        } {
            return Err(denied(format_args!(
                "{verb} policy (USING) denied the target row"
            )));
        }
        Ok(())
    }

    /// An update, or with `created` an upsert, which gives the columns
    /// `cells` leaves out `created`'s values if it creates the row.
    #[allow(clippy::too_many_arguments)]
    async fn patch(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
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
        let table = &write.table;
        let table_name = &write.source.name;
        self.validate_write_cells(write, &cells)?;
        let located = self.locate(table, row, target).await?;
        // A system table's rows are never updated, by any writer: the
        // system, trusted backends and sessions alike, before any policy.
        if jazz_model::schema::is_system_table(table_name)
            && !(upsert && matches!(located, Located::Absent { .. }))
        {
            return Err(system_row_immutable(table_name, "updated"));
        }
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
                return self
                    .insert(
                        stamp,
                        write,
                        cells,
                        Cells::Authored,
                        Some(row),
                        &Target::Root,
                        who,
                        seq,
                    )
                    .await;
            }
            // An update stages against the row's current cells, so a
            // replica must hold it. Saying so discloses nothing: the answer
            // is the same whether or not the row exists elsewhere.
            Located::Absent { .. } if self.client().is_some() => {
                return Err(update_target_not_loaded(verb, table_name, row));
            }
            // A session is told what it is told for a row it may not read,
            // so a write doesn't reveal whether a hidden row exists.
            Located::Absent { .. } if !upsert && self.checked(who).is_some() => {
                return Err(unreadable_target(verb, table_name));
            }
            Located::Absent { .. } if !upsert => {
                return Err(rejected(format!(
                    "{verb} target {} does not exist in `{table_name}`",
                    row.0
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
                key,
            } if !self.merging.get() && (!upsert || current != self.own_pos(seq)) => {
                return Err(self
                    .refuse_deleted(&write.policies, &write.source, row, &key, &who, verb)
                    .await?);
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
                    self.authorize_patch(write, row, &key, &who, verb, seq)
                        .await?;
                }
                if cells.is_empty() && !staged_tombstone {
                    return Ok(WriteHandle {
                        row,
                        seq: current,
                        tx: self.own_seq_at((current, None)).unwrap_or(current.seq),
                    });
                }
                let held = (!staged_tombstone).then_some(&key[..]);
                let cells = self
                    .merged_cells(table, row, held, Some((&key, seq)), cells)
                    .await?;
                self.snapshot(prune, table, row, &key, seq).await?;
                // An upsert over a tombstone replaces it.
                self.set_cells(
                    stamp,
                    table,
                    &write.source,
                    row,
                    &key,
                    &cells,
                    Cells::Authored,
                    seq,
                    staged_tombstone.then_some(false),
                    true,
                )
                .await?;
                key
            }
            Located::Inherited {
                from,
                to,
                head_cells,
                at,
            } => {
                self.authorize_patch(write, row, &from, &who, verb, seq)
                    .await?;
                let cells = self
                    .merged_cells(table, row, Some(&from), Some((&to, seq)), cells)
                    .await?;
                self.copy_on_write(stamp, table, row, &from, &to, &head_cells, at.as_ref(), seq)
                    .await?;
                self.set_cells(
                    stamp,
                    table,
                    &write.source,
                    row,
                    &to,
                    &cells,
                    Cells::Authored,
                    seq,
                    None,
                    true,
                )
                .await?;
                to
            }
            Located::Absent { .. } => unreachable!("absent upserts insert"),
        };
        // An upsert over a tombstone inserts afresh only a row that had no
        // version before the unit (one the unit inserted and deleted): over
        // an earlier row it brings that row back, as a restore does.
        if reinserted
            && self.deferred.borrow().is_some()
            && !self.existed_before(table, row, &key, seq).await?
        {
            self.note_unit_insert(table, row, &key);
        }
        // The new row is checked on the evidence SPEC 7 gives a WITH CHECK
        // ([`Store::write_check`]): a same-table EXISTS compares the new row
        // with the old one, not with itself.
        if let Some(author) = self.checked(who) {
            self.write_check(
                &write.policies,
                &write.source,
                row,
                &key,
                author,
                if reinserted {
                    WriteCheck::Insert
                } else {
                    WriteCheck::WithCheck { upsert }
                },
                seq,
            )
            .await?;
        } else {
            self.note_trusted(table, row);
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
            tx: seq,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn update(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        self.patch(prune, stamp, write, row, cells, None, target, who, seq)
            .await
    }

    /// Upsert `cells` into `row`; if it creates the row, `created` fills
    /// the columns `cells` leaves out (a replicated upsert's writer's
    /// defaults, see `upload_op`).
    #[allow(clippy::too_many_arguments)]
    async fn upsert(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        created: BTreeMap<String, Value>,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        self.patch(
            prune,
            stamp,
            write,
            row,
            cells,
            Some(created),
            target,
            who,
            seq,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
        row: RowUuid,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let table = &write.table;
        let table_name = &write.source.name;
        // A system table's rows are never deleted by any writer, except
        // member rows, which policies govern. The table may be named by its
        // stored name (a client's upload), so compare the owner's name.
        if jazz_model::schema::is_system_table(table_name)
            && Self::logical_table_name(&self.names(), table) != jazz_model::schema::MEMBERS_TABLE
        {
            return Err(system_row_immutable(table_name, "deleted"));
        }
        // A delete by row id reads no user data (SPEC 7): its DELETE policy
        // alone decides, so a write-only principal may delete.
        let check = async |inner: &Self, key: &[u8]| -> Result<(), Error> {
            if let Some(author) = inner.checked(who) {
                let allowed = inner
                    .holds_on_acted_row(&write.policies, &write.source, row, key, seq, |ctx| {
                        ctx.operation_policy(
                            &write.source,
                            Operation::Delete,
                            "a",
                            &inner.session(author),
                        )
                    })
                    .await?;
                if !match allowed {
                    Some(allowed) => allowed,
                    None => {
                        inner
                            .check(
                                &write.policies,
                                &write.source,
                                row,
                                key,
                                author,
                                Operation::Delete,
                            )
                            .await?
                    }
                } {
                    return Err(delete_denied());
                }
            }
            Ok(())
        };
        match self.locate(table, row, target).await? {
            Located::Row {
                key,
                deleted: false,
                ..
            } => {
                check(self, &key).await?;
                self.snapshot(prune, table, row, &key, seq).await?;
                self.set_cells(
                    stamp,
                    table,
                    &write.source,
                    row,
                    &key,
                    &BTreeMap::new(),
                    Cells::Authored,
                    seq,
                    Some(true),
                    false,
                )
                .await?;
            }
            // Deleting an inherited row is a head register: a head tombstone
            // that masks the base row.
            Located::Inherited {
                from,
                to,
                head_cells,
                at,
            } => {
                check(self, &from).await?;
                self.copy_on_write(stamp, table, row, &from, &to, &head_cells, at.as_ref(), seq)
                    .await?;
                self.set_cells(
                    stamp,
                    table,
                    &write.source,
                    row,
                    &to,
                    &BTreeMap::new(),
                    Cells::Authored,
                    seq,
                    Some(true),
                    false,
                )
                .await?;
            }
            // As for a patch: a replica leaves an unknown row to the authority.
            Located::Absent { .. } if self.client().is_some() => {}
            // A merged delete of a deleted row changes nothing: concurrent
            // deletes of one row all apply.
            Located::Row { deleted: true, .. } if self.merging.get() => {}
            Located::Row {
                deleted: true, key, ..
            } => {
                return Err(self
                    .refuse_deleted(&write.policies, &write.source, row, &key, &who, "DELETE")
                    .await?);
            }
            // As for an update: a session can't tell a missing row from one
            // its DELETE policy refuses.
            _ if self.checked(who).is_some() => return Err(delete_denied()),
            _ => {
                return Err(rejected(format!(
                    "DELETE target {} does not exist in `{table_name}`",
                    row.0
                )));
            }
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
            tx: seq,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn restore(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        source: Cells,
        target: &Target,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let table = &write.table;
        let table_name = &write.source.name;
        // A system row is never updated, so a deleted one is not restored
        // either, with content or without: a restore writes the row back.
        if jazz_model::schema::is_system_table(table_name) {
            return Err(system_row_immutable(table_name, "restored"));
        }
        let key = match self.locate(table, row, target).await? {
            Located::Row {
                key, deleted: true, ..
            } => key,
            // As for an update: a replica must hold the row to restore it,
            // and saying so discloses nothing.
            Located::Absent { .. } if self.client().is_some() => {
                return Err(update_target_not_loaded("RESTORE", table_name, row));
            }
            Located::Absent { .. } => {
                return Err(rejected(format!(
                    "RESTORE target {} does not exist in `{table_name}`",
                    row.0
                )));
            }
            Located::Row { .. } | Located::Inherited { .. } => return Err(row_not_deleted(row)),
        };
        self.snapshot(prune, table, row, &key, seq).await?;
        let mut cells = BTreeMap::new();
        if let Some(content) = &content {
            self.validate_write_cells(write, content)?;
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
                        return Err(missing_required(
                            table_name,
                            write.column_name(&column.stored),
                        ));
                    }
                };
                cells.insert(column.name.clone(), value);
            }
        }
        self.set_cells(
            stamp,
            table,
            &write.source,
            row,
            &key,
            &cells,
            source,
            seq,
            Some(false),
            content.is_some(),
        )
        .await?;
        // A restore writes an existing row back: the UPDATE policy decides.
        if let Some(author) = self.checked(who) {
            self.write_check(
                &write.policies,
                &write.source,
                row,
                &key,
                author,
                WriteCheck::Restore,
                seq,
            )
            .await?;
        } else {
            self.note_trusted(table, row);
        }
        Ok(WriteHandle {
            row,
            seq: self.own_pos(seq),
            tx: seq,
        })
    }
}

/// See [`Store::inherited_heads`].
type InheritedHeads = HashMap<(String, [u8; 16], Vec<u8>), Vec<u8>>;

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

#[derive(Clone)]
struct DeferredCheck {
    /// The schema the write was made through, which names its table.
    schema: Rc<Schema>,
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
        // The candidate row's policies can take a pass away as rows change
        // (a member row removed, the row itself updated): keep the full
        // post-state pass.
        PolicyExpr::InheritsFrom { .. } => false,
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

    async fn of(target: &WriteTarget, inner: &Store) -> Result<Self, Error> {
        Ok(match target {
            WriteTarget::Root => Self::Root,
            WriteTarget::BranchView { head, base } => {
                let (base, at) = match base {
                    None => (None, None),
                    Some(BranchViewBase::Current(base)) => (Some(base.clone()), None),
                    Some(BranchViewBase::Snapshot { branch, snapshot }) => (
                        Some(branch.clone()),
                        Some(inner.resolve_snapshot(snapshot).await?),
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

/// A write's resolved layout, source diagnostics and authorization. It is
/// immutable and shared by every replay of the prepared operation.
struct WriteTable {
    table: Table,
    source: Table,
    policies: Rc<Schema>,
    version: crate::ids::SchemaVersionId,
    stored_names: bool,
}

struct WriteLayouts {
    schema: Rc<Schema>,
    version: crate::ids::SchemaVersionId,
    tables: HashMap<String, Rc<WriteTable>>,
}

#[derive(Clone)]
struct PreparedWrite {
    layout: Rc<WriteTable>,
    op: StagedOp,
}

/// A staged mergeable transaction handle (`Db::mergeable_tx`). Writes are
/// staged in order and applied as one commit; each staged write sees the
/// earlier ones, as Jazz's transaction overlay does. Dropping the handle
/// discards its staged writes. Other Db operations never join this transaction.
pub struct MergeableTxHandle<'a> {
    tx: TxRef<'a>,
    identity: WriteIdentity,
}

#[derive(Clone, Debug)]
pub(crate) enum StagedOp {
    Insert(String, BTreeMap<String, Value>, InsertOptions),
    Update(String, RowUuid, BTreeMap<String, Value>, UpdateOptions),
    /// The fourth field is what the upsert gives the columns it leaves
    /// out if it creates the row, by stored names: a replicated upsert's
    /// writer's defaults (see `Store::upload_op`); empty on a local one.
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
        let id = OpenTransactionId::new();
        self.begin_mergeable(id).await?;
        Ok(MergeableTxHandle {
            tx: self.tx_ref(id),
            identity: WriteIdentity::Database,
        })
    }

    /// A staged transaction whose writes are authored by `author`.
    pub async fn mergeable_tx_for_identity(
        &self,
        author: AuthorSubject,
    ) -> Result<MergeableTxHandle<'_>, Error> {
        let id = OpenTransactionId::new();
        self.begin_mergeable(id).await?;
        Ok(MergeableTxHandle {
            tx: self.tx_ref(id),
            identity: WriteIdentity::Attribution(author),
        })
    }
}

impl Store {
    /// A branch move: a source tombstone plus a destination insert (or a
    /// restore of an earlier destination tombstone), in one commit.
    #[allow(clippy::too_many_arguments)]
    async fn move_row(
        &self,
        prune: bool,
        stamp: &Stamp,
        write: &WriteTable,
        from: BranchSelector,
        to: BranchSelector,
        row: RowUuid,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let t = &write.table;
        let key = t.selector_key(&from)?;
        let mut cells = BTreeMap::new();
        for column in &t.columns {
            if t.branch_by.contains(&column.name) {
                continue;
            }
            let sql = format!(
                "SELECT {} FROM {} WHERE id = ?1 AND _branch = ?2 AND (_state & {STATE_DELETED}) = 0",
                quote(&column.stored),
                quote(&t.stored)
            );
            let cell: Sql = self
                .conn
                .cached_row(
                    &sql,
                    crate::storage::params![row.0.as_bytes().to_vec(), key],
                    |r| r.get(0),
                )
                .await
                .map_err(|_| rejected(format!("move source {row:?} does not exist")))?;
            let value = column_from_sql(column, cell.as_ref(), &self.chunks).await?;
            if let Some(value) = value {
                cells.insert(column.name.clone(), value);
            }
        }
        self.delete(
            prune,
            stamp,
            write,
            row,
            &Target::Exact(from),
            Who::Local,
            seq,
        )
        .await?;
        // The moved row carries its stored cells: they are not re-checked.
        let to = Target::Exact(to);
        match self.locate(t, row, &to).await? {
            Located::Row { deleted: true, .. } => {
                self.restore(
                    prune,
                    stamp,
                    write,
                    row,
                    Some(cells),
                    Cells::Carried,
                    &to,
                    Who::Local,
                    seq,
                )
                .await
            }
            _ => {
                self.insert(
                    stamp,
                    write,
                    cells,
                    Cells::Carried,
                    Some(row),
                    &to,
                    Who::Local,
                    seq,
                )
                .await
            }
        }
    }
}

impl MergeableTxHandle<'_> {
    pub async fn insert(
        &self,
        table: &str,
        cells: BTreeMap<String, Value>,
        opts: InsertOptions,
    ) -> Result<WriteHandle, Error> {
        self.tx
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
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpdateOptions,
    ) -> Result<(), Error> {
        self.tx
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
            .map(drop)
    }
    pub async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: BTreeMap<String, Value>,
        opts: UpsertOptions,
    ) -> Result<(), Error> {
        self.tx
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
            .map(drop)
    }
    pub async fn delete(
        &self,
        table: &str,
        row: RowUuid,
        opts: DeleteOptions,
    ) -> Result<(), Error> {
        self.tx
            .delete(
                table,
                row,
                DeleteOptions {
                    identity: in_transaction(opts.identity, self.identity)?,
                    ..opts
                },
            )
            .await
            .map(drop)
    }
    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<(), Error> {
        self.tx
            .restore(
                table,
                row,
                content,
                RestoreOptions {
                    identity: in_transaction(opts.identity, self.identity)?,
                    ..opts
                },
            )
            .await
            .map(drop)
    }
    pub async fn move_between_branches(
        &self,
        table: &str,
        source: BranchSelector,
        destination: BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        self.tx
            .stage(StagedOp::Move(table.to_owned(), source, destination, row))
            .await
    }
    pub async fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.tx.all_prepared(prepared).await
    }
    pub async fn commit(self) -> Result<TxReceipt, Error> {
        self.tx.db.commit_mergeable_handle(self.tx.id).await
    }
}
impl Drop for MergeableTxHandle<'_> {
    fn drop(&mut self) {
        // Commit normally takes the batch. Also clean up if admission was
        // closed before it could do so, or if the handle was abandoned.
        let _ = self.tx.db.abandon_transaction_handle(self.tx.id);
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
        Self(jazz_types::sha256::derive_key(
            "jazz typed schema view id v1",
            &bytes,
        ))
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
    /// A handle that reads and writes as a sync session declaring schema
    /// `version` is served: by that admitted version's names, governed by
    /// the current schema's policies. `None` while `version` is not
    /// admitted.
    pub fn governed_view(&self, version: crate::ids::SchemaVersionId) -> Option<Db> {
        let view = self
            .inner
            .published_versions
            .borrow()
            .get(&version)?
            .clone();
        Some(Db {
            inner: self.inner.clone(),
            view: Some(view),
            defer_fetches: self.defer_fetches,
            origin: None,
        })
    }

    /// Grows each time the chunk store answered fetches (or reads of nodes
    /// it failed to fetch may ask again): a read that failed waiting on
    /// offloaded nodes ([`Self::deferring_chunk_fetches`]) is worth trying
    /// again only once this changed.
    pub fn chunk_fetches_answered(&self) -> u64 {
        self.inner.chunks.answered()
    }

    /// This handle, but a read or write that waits on offloaded nodes of a
    /// host-run chunk store ([`crate::options::DbConfig::with_host_chunk_io`])
    /// fails at once with the missing-chunks error instead of fetching them
    /// on this thread; they stay wanted, so the host's next fetches read
    /// them and the caller tries again once they land. A server serving
    /// many sessions on one thread uses it, so one session's slow chunk
    /// store read never stalls the others.
    pub fn deferring_chunk_fetches(mut self) -> Db {
        self.defer_fetches = true;
        self
    }

    /// [`Self::governed_view`] for a session pinned to published bundle
    /// `bundle` (see [`Self::check_session_pin`]): governed by the
    /// bundle's policies instead of the current schema's. Without a bundle
    /// it is [`Self::governed_view`]. `None` while `version` is not
    /// admitted or the bundle does not resolve.
    pub async fn pinned_view(
        &self,
        version: crate::ids::SchemaVersionId,
        bundle: Option<&crate::catalogue::BundleId>,
    ) -> Result<Option<Db>, Error> {
        let _operation = self.write_operation().await;
        let Some(bundle) = bundle else {
            return Ok(self.governed_view(version));
        };
        let inner = self.inner.without_overlay();
        let Some((_, governing)) = inner.bundle(bundle).await? else {
            return Ok(None);
        };
        let Some(view) = inner.governed_by(version, governing) else {
            return Ok(None);
        };
        Ok(Some(Db {
            inner: self.inner.clone(),
            view: Some(view),
            defer_fetches: self.defer_fetches,
            origin: None,
        }))
    }

    /// Ship more of the app's versions and lenses (see
    /// [`DbConfig::with_migrations`]): a client learns an app's migrations
    /// when it first sees the app, not only when it opens.
    pub fn add_migrations(&self, migrations: Migrations) {
        let mut known = self.inner.migrations.borrow_mut();
        for schema in migrations.schemas {
            let version = schema.version_id();
            if !known.schemas.iter().any(|s| s.version_id() == version) {
                known.schemas.push(schema);
            }
        }
        for lens in migrations.lenses {
            if !known
                .lenses
                .iter()
                .any(|l| l.source == lens.source && l.target == lens.target)
            {
                known.lenses.push(lens);
            }
        }
    }

    /// Register a typed schema view. The handle shares this owner's store,
    /// batches, clock and subscriptions; it reads and writes its own columns
    /// of the physical tables, which hold the union of every admitted view.
    pub async fn register_schema_view(&self, schema: JazzSchema) -> Result<Db, Error> {
        let _operation = self.write_operation().await;
        self.register_view_under_lease(schema).await
    }

    async fn register_view_under_lease(&self, schema: JazzSchema) -> Result<Db, Error> {
        // A version the app's lenses lead to is admitted as the authority
        // admits it, not named against the current one.
        self.admit_along_migrations(&schema).await?;
        let logical = {
            let inner = self.inner.without_overlay();
            let version = schema.version_id();
            if let Some((_, admitted)) = Rc::make_mut(&mut *inner.admitted.borrow_mut())
                .iter_mut()
                .find(|(id, _)| *id == version)
            {
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
                if inner.names().get(&version).is_none() {
                    check_direct_lens(&inner.owner().jazz, &schema)?;
                }
            }
            let base = inner.owner().names();
            let logical = inner.names().resolve(&schema, &[], Some(&base))?;
            inner.names().check_branches(&logical)?;
            inner.names().check_types(&logical)?;
            inner.write_admission(&logical, Vec::new(), false).await?;
            if !inner.admitted().iter().any(|(id, _)| *id == version) {
                Rc::make_mut(&mut *inner.admitted.borrow_mut()).push((version, schema.clone()));
            }
            inner
                .views
                .borrow_mut()
                .entry(SchemaViewId::for_schema(&schema))
                .or_insert(schema);
            self.inner.publish_schema();
            logical
        };
        Ok(Db {
            inner: self.inner.clone(),
            view: Some(Rc::new(logical)),
            defer_fetches: self.defer_fetches,
            origin: None,
        })
    }

    pub async fn schema_view(&self, id: SchemaViewId) -> Result<Db, Error> {
        let _operation = self.write_operation().await;
        let schema = self
            .inner
            .views
            .borrow()
            .get(&id)
            .cloned()
            .ok_or_else(|| schema_error(format!("schema view {id:?} is not registered")))?;
        self.register_view_under_lease(schema).await
    }

    pub fn schema_view_id(&self) -> SchemaViewId {
        SchemaViewId::for_schema(
            &self
                .view
                .as_ref()
                .unwrap_or(&self.inner.published_schema.borrow())
                .jazz,
        )
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

/// Rows a unit's checks may take as evidence, by stored table.
pub(crate) type GroundedRows = BTreeMap<String, BTreeSet<[u8; 16]>>;

/// The store behind its one gate: every borrow first undoes a transaction
/// handle's overlay, so nothing but that handle's next stage or read sees
/// it.
struct Store {
    wakes: Rc<sync::Wakes>,
    sessions: RefCell<Vec<sync::Session>>,
    /// Set once connected upstream: this store is a replica.
    client: RefCell<Option<sync::Client>>,
    /// Live subscription groups and their invalidation index.
    fan: RefCell<fanout::Fanout>,
    /// Per stored table, how many composite support column sets the
    /// server's sessions hold (see `sync::CompositeTables`): a written row
    /// of a table no session follows that way skips the sessions' sets.
    pub(crate) composite_tables: sync::CompositeTables,
    /// Where each admitted version stores its tables and columns.
    names: RefCell<Rc<crate::names::Names>>,

    /// Admitted structural schemas with their version ids (hashing a
    /// whole schema is too slow to repeat per session per tick).
    admitted: RefCell<Rc<Vec<(crate::ids::SchemaVersionId, JazzSchema)>>>,

    /// `owner.jazz.version_id()`, which hashes the whole schema.
    pub(crate) owner_version: Cell<crate::ids::SchemaVersionId>,
    /// The owner's current schema.
    owner: RefCell<Rc<Schema>>,
    /// The stored tables: the union of every admitted schema, keyed and
    /// named by stored names.
    physical: RefCell<Rc<Schema>>,
    clock: Box<dyn Fn() -> u64>,
    resumes_after: Cell<Option<u64>>,
    /// Its clock and holds as a shard ([`shard`]); inert unsharded.
    pub(crate) shard: RefCell<shard::ShardState>,
    pub(crate) claims: RefCell<HashMap<AuthorSubject, BTreeMap<String, Value>>>,
    /// Nodes a large-value edit consolidated, stored when the edited cell
    /// is bound inside the write's transaction.
    pending_nodes: RefCell<Vec<Rc<jazz_types::large_values::PreparedLargeValue>>>,
    /// Descriptors this store's own edit derived for the write in flight:
    /// trusted as built, so binding them re-validates nothing.
    derived: RefCell<Vec<jazz_types::large_values::LargeValueRef>>,
    /// Rows written since the last server tick (only while serving).
    since_tick: RefCell<Vec<(String, RowUuid, Vec<u8>)>>,
    /// On the authority, what each row of the upload being applied was
    /// built on (SPEC 4 §4.6), by (stored table, row, branch key): a text
    /// merge reads its base from it. Empty otherwise.
    merge_bases: RefCell<HashMap<(String, RowUuid, Vec<u8>), sync::Built>>,
    /// What the session whose commit is being applied holds (SPEC 19.4):
    /// set while a synced commit applies, `None` for local writes.
    pub(crate) possession: RefCell<Option<Rc<crate::large::Possession>>>,
    /// The order of each of this store's own commits it knows unsettled
    /// (on a relay, of each session commit it carries too), by local seq:
    /// its HLC is where the commit's versions are ([`Pos::pending`]), its
    /// writer (node, tx) what history keeps them under (`_writer`).
    pub(crate) own_order: RefCell<HashMap<u64, sync::Order>>,
    /// Published bundles sessions are pinned to, resolved: the write
    /// version each selects and the schema its policies govern by.
    bundles:
        RefCell<HashMap<crate::catalogue::BundleId, (crate::ids::SchemaVersionId, Rc<Schema>)>>,
    views: RefCell<BTreeMap<SchemaViewId, JazzSchema>>,
    /// A transaction handle's staged writes left applied between its
    /// stages (see [`Db::in_overlay`]); undone by [`Store`] before any
    /// other use of the store.
    overlay: RefCell<Option<Overlay>>,
    /// The newest commit seq handed out: the authority's seq, or on a
    /// replica its own commit counter (its commits' tx).
    seq: Cell<u64>,
    /// Applying writes made over an older image (an authority ingesting
    /// an upload, a replica replaying its pending writes): an edit or a
    /// delete of a row deleted since applies and leaves it deleted (SPEC 4,
    /// INV-HIST-8). A fresh local write over a tombstone it can see is
    /// refused instead.
    merging: Cell<bool>,
    /// The kind of this store's own versions: [`sync::PENDING`] on a
    /// replica, [`sync::ACCEPTED`] (0) until it becomes one.
    pub(crate) me: Cell<i64>,
    /// The commit seq and time of the last local stamp: writes of one commit
    /// share a time, and later commits get strictly later ones, so
    /// `$createdAt` orders this node's writes even within one millisecond.
    last_stamp: Cell<(u64, i64)>,
    /// The newest commit HLC this store gave or saw (see
    /// [`Store::next_hlc`]). Apart from `last_stamp`: a clock seen from
    /// another writer moves no `$createdAt`/`$updatedAt` this store stamps.
    hlc_last: Cell<u64>,
    /// The values as sent of the upload's text writes, by row version and
    /// stored column: for a write its merge changed, the value and its
    /// bound cell (kept by [`Store::record_as_sent`]); `None` for one left
    /// as sent.
    as_sent: RefCell<AsSent>,
    /// The current schema named by stored names, which replicated writes
    /// apply under ([`Schema::stored_view`]), keyed by the owner it was
    /// built from and how many stored columns there were.
    stored_view: RefCell<Option<(*const Schema, usize, Rc<Schema>)>>,
    /// [`Store::retention`], keyed by how many versions were named and
    /// admitted and the owner's version.
    retention: RefCell<
        Option<(
            (usize, usize, crate::ids::SchemaVersionId),
            Rc<BTreeSet<String>>,
        )>,
    >,
    /// [`Store::retention_of`], by version.
    retention_by_version: RefCell<HashMap<crate::ids::SchemaVersionId, Rc<BTreeSet<String>>>>,
    /// Views governed by a bundle's policies (see `Store::governed_view`),
    /// by bundle, with the view each was built from.
    bundle_views: RefCell<HashMap<crate::catalogue::BundleId, (Rc<Schema>, Rc<Schema>)>>,
    /// The stored views sessions' commits apply under (see
    /// `Store::session_stored_view`), by bundle and written version, with
    /// the stored view each was built from.
    #[allow(clippy::type_complexity)]
    version_stored_views: RefCell<
        HashMap<
            (
                Option<crate::catalogue::BundleId>,
                crate::ids::SchemaVersionId,
            ),
            (Rc<Schema>, Rc<Schema>),
        >,
    >,
    /// When pulls the chunk store failed to answer at once are asked of it
    /// again.
    pub(crate) store_retry: RefCell<sync::StoreRetry>,
    /// Interned author codes (see [`Store::author_code`]), cached both ways.
    authors: RefCell<AuthorCodes>,
    /// By stored table and whether into its history: the columns the last
    /// image stored there had, and the insert that stores them. A sync
    /// batch stores thousands of images of a few tables.
    image_inserts: RefCell<HashMap<(String, bool), (Vec<String>, String)>>,
    /// While a replica stores a message's images: their rows' current
    /// images, read at once (see `Store::begin_current_batch`).
    current_batch: RefCell<Option<HashMap<(String, RowUuid, Vec<u8>), Option<sync::Image>>>>,
    /// Policy-check statements kept per (schema, table, operation, author):
    /// see [`Store::check_in`]. Per store, not per thread: one thread can
    /// host several stores, and a schema one of them frees can be
    /// reallocated at the same address for another.
    check_plans: RefCell<CheckPlans>,
    /// Rows written since the last dispatch: (table, id, branch key).
    changed: RefCell<Vec<(String, RowUuid, Vec<u8>)>>,
    /// The write checks of the unit being applied, run at its end.
    deferred: RefCell<Option<Vec<DeferredCheck>>>,
    /// Rows a unit's trusted writes (no write check) left behind, by stored
    /// table: they need no evidence, so its grounding starts from them.
    trusted_rows: RefCell<BTreeMap<String, BTreeSet<[u8; 16]>>>,
    /// The inherited rows the unit being applied (or the staging overlay)
    /// copied into a branch head: (stored table, row, head key) to the base
    /// key it acted on. The unit's later writes to the head row act on that
    /// base row (SPEC 7). Cleared at every unit and overlay boundary
    /// ([`Store::start_unit_heads`]), never left to another unit.
    inherited_heads: RefCell<InheritedHeads>,
    /// Each stored table's column positions, the codes authored-column
    /// bitmasks use (SPEC 4 §4.6), read on first use.
    column_codes: RefCell<HashMap<String, HashMap<String, u32>>>,
    /// Images a replica dropped since the last dispatch (evictions and
    /// rebases), for keyed invalidation.
    gone: RefCell<Vec<sync::Image>>,
    /// One-way, owner-wide fence for new application mutations during close.
    mutation_admission_closed: Cell<bool>,
    /// Shared so a scope can run while the store is borrowed afresh
    /// inside it ([`crate::storage::Storage::scope`]).
    conn: Rc<crate::storage::Storage>,
    /// Enum variants a write may hold though its schema does not declare
    /// them, by table and column: only an import of rows another store
    /// kept sets any ([`Db::keep_variants_for_import`]).
    kept_variants: RefCell<BTreeMap<(String, String), BTreeSet<String>>>,
    /// Times a write check copied the evidence set it shares with its
    /// batch: what tests of staging cost count.
    #[cfg(any(test, feature = "testing"))]
    evidence_copies: std::cell::Cell<usize>,
    /// Large-value chunks: the cache the collation and decoders read.
    chunks: Arc<crate::large::Chunks>,
    /// The node SYSTEM provenance names ([`crate::options::DbConfig::system_node`]).
    system_node: crate::ids::NodeUuid,
    identity: DbIdentity,
    /// The diff steps a text merge may take ([`jazz_text::MERGE_WORK_BUDGET`]
    /// unless a test lowers it).
    merge_budget: Cell<u64>,
    /// A wall clock tests set (see `Db::set_clock_for_test`).
    clock_for_test: Cell<Option<i64>>,
    seed_floor: Cell<u64>,
    ids: RefCell<Box<dyn RowIdSource>>,
    /// Uploads the authority refused, by commit seq, kept for a host that
    /// reports them (see [`Db::track_upstream_rejections`]).
    upstream_rejections: RefCell<Option<Vec<UpstreamRejection>>>,
    /// A replica whose store does not outlive it (a foreground over a
    /// durable relay): its local reads wait for the upstream's view too.
    non_durable: Cell<bool>,
    /// This store lives in a file, which may reopen as an older state than
    /// it last uploaded from: a lost tail (an IndexedDB commit still queued
    /// at a crash, power loss under `synchronous=NORMAL`) or a restored
    /// backup. As a replica it numbers its own commits after its wall
    /// clock too, so a reopened file never hands out a transaction id it
    /// may already have uploaded (see [`Store::next_seq`]).
    pub(crate) reopenable: bool,
    /// Every session but SYSTEM's must be pinned to a published bundle
    /// (see [`Db::require_session_pins`]).
    pub(crate) pins_required: Cell<bool>,
    /// Whether `_jazz_withheld` exists (see `sync::WITHHELD_DDL`): branch
    /// views over a base read it only then.
    withheld: std::cell::Cell<bool>,
    /// Query statements that ran slow, until the host takes them
    /// ([`Db::take_slow_queries`]).
    slow: RefCell<crate::slow::SlowLog>,
    /// Catalogue versions an upstream sent that this build cannot read,
    /// with why (see [`crate::sync::CatalogueSchema`]): skipped, not taken.
    unsupported_versions: RefCell<BTreeMap<crate::ids::SchemaVersionId, String>>,
    /// Text merges that ran past their work budget since the host last
    /// took them ([`Db::take_merge_fallbacks`]).
    merge_fallbacks: RefCell<MergeFallbacks>,
    /// The global authority: what it settles is durable at `Global`.
    authority: Cell<bool>,
    /// Durability acks to sessions held until the host has persisted the
    /// store, for a host whose file writes complete after the commit
    /// returns (see [`Db::defer_durability_acks`]).
    held_acks: RefCell<Option<sync::HeldAcks>>,
    /// Whether `_jazz_pruned_txs` exists, as last read (see
    /// `sync::has_pruned_txs`).
    pruned_txs: std::cell::Cell<Option<sync::Known>>,
    /// Sessions the server ended, with why, until the host takes them
    /// ([`Db::take_closed_subscribers`]). Kept only for a host that does
    /// ([`Db::track_closed_subscribers`]), so that one which never takes
    /// them does not accumulate them.
    closed: RefCell<Option<Vec<(sync::SubscriberId, Error)>>>,
    /// While a server tick fans out to its sessions: the rows each
    /// followed key found, read once for every session following it
    /// (see `sync::FollowMemo`).
    follow_memo: Rc<RefCell<Option<sync::FollowMemo>>>,
    /// Limits on local streaming uploads and on pushed nodes awaiting
    /// their commit.
    staging_policy: Cell<LargeValueStagingPolicy>,
    /// Dropped uploads queue cleanup without recursively borrowing the owner.
    upload_cleanup: Rc<RefCell<Vec<[u8; 32]>>>,
    /// `conn.total_changes()` after the last large-value reclaim pass that
    /// committed and left nothing queued: until a row changes again (a
    /// trigger's change counts), a pass would find nothing. `None` runs the
    /// next pass, as an opened store does, for work a crash left queued.
    reclaimed_at: Cell<Option<u64>>,
    /// Large-value reclaim passes run (diagnostics).
    #[cfg(any(test, feature = "testing"))]
    reclaim_passes: Cell<u64>,
    /// The staging ingress window: (start ms, encoded bytes admitted).
    ingress: Cell<(u64, u64)>,
    /// Ticks' gatherings of composite support column sets (see
    /// `Db::composite_scans_for_test`).
    #[cfg(any(test, feature = "testing"))]
    composite_scans: Cell<usize>,
    /// Subscribers accepted so far (each one's generation).
    accepted: Cell<u64>,
    /// Staged writes replayed into a transaction overlay so far: what tests
    /// of staging cost count.
    #[cfg(any(test, feature = "testing"))]
    overlay_replays: Cell<usize>,
    /// Pulls a replica sends that fail with a storage error before they
    /// read anything (see `Db::fail_next_pulls_for_test`).
    #[cfg(any(test, feature = "testing"))]
    failing_pulls: Cell<usize>,
    /// Errors the next ticks fail with, for tests of a tick driver.
    #[cfg(any(test, feature = "testing"))]
    failing_ticks: RefCell<VecDeque<ErrorCode>>,
    fanout_stats: Rc<RefCell<fanout::FanoutStats>>,
    coalesced: Rc<Cell<u64>>,
    prior_served: Cell<u64>,
    prior_hold: Cell<Option<u64>>,
    next_advice: Cell<u64>,
    upstream: RefCell<Option<Rc<sync::UpstreamPort>>>,
    prior_ack_point: Cell<u64>,
    transports: RefCell<HashMap<sync::SubscriberId, Rc<sync::SessionPort>>>,
    session_controls: RefCell<Vec<sync::SessionControl>>,
    migrations: RefCell<Migrations>,
    backlog_limit: Rc<Cell<Option<std::num::NonZeroUsize>>>,
    pending_revision_base: Cell<u64>,
    pending: RefCell<Option<Rc<RefCell<sync::PendingQueue>>>>,
    refusals: RefCell<sync::Refusals>,
    batches: RefCell<HashMap<OpenTransactionId, Batch>>,
    /// Last sequence at entry to the active operation, before its provisional writes.
    prior_seq: Cell<u64>,
    /// Pending commits a rebase replayed (see `Db::rebase`).
    replayed_commits: Cell<u64>,
    /// Echoes a serving pass examined to match its images (see
    /// `Db::echo_probes`).
    echo_probes: Cell<u64>,
    released_reads: RefCell<Vec<sync::ReadCleanup>>,
    /// Schema available to synchronous callers while execution is suspended.
    published_schema: RefCell<Rc<Schema>>,
    /// Resolved tables for the most recently used write schema. Staged operations
    /// retain their own layout when a different view or policy bundle is used.
    write_layouts: RefCell<Option<WriteLayouts>>,
    published_versions: RefCell<HashMap<crate::ids::SchemaVersionId, Rc<Schema>>>,
    /// Query shapes (with their binding) `prepare` has already validated
    /// against this schema version: it validates each once. Cleared when
    /// it reaches [`VALIDATED_SHAPES_MAX`].
    validated_shapes: RefCell<HashSet<([u8; 16], [u8; 16])>>,
    // One logical operation may release its state guard between SQL phases.
    // The lease keeps a second operation from using its transaction in between.
    operation: futures::lock::Mutex<()>,
    /// Rows the unit being applied inserted fresh (no version before the
    /// unit), by stored table: what `InsertedByThisTransaction` holds for
    /// in its write checks. Kept after the unit's checks end, so a staged
    /// write's batch can note them, until the next unit starts.
    unit_inserts: RefCell<InsertedRows>,
    /// Jazz's account registry, as the account-record checks beside the
    /// system tables' policies read it (see [`Db::set_account_registry`]).
    account_registry: RefCell<Option<Rc<dyn AccountRegistryView>>>,
    /// The client's encryption hooks ([`crate::e2ee`]).
    #[cfg(feature = "e2ee-client")]
    e2ee: Option<Rc<dyn crate::e2ee::E2eeHooks>>,
    /// Keys this database's digests of plaintext search values, chosen at
    /// random when it opens: a digest confirms no guessed value elsewhere.
    e2ee_salt: [u8; 32],
    add_creator_as_member: bool,
}

impl Store {
    #[inline]
    fn e2ee_hooks(&self) -> Option<&Rc<dyn crate::e2ee::E2eeHooks>> {
        #[cfg(feature = "e2ee-client")]
        {
            self.e2ee.as_ref()
        }
        #[cfg(not(feature = "e2ee-client"))]
        {
            None
        }
    }

    /// Delete up to `budget` large-value nodes no cell or branch references.
    /// A pass runs only when one could find work: a row changed since the
    /// last pass that emptied the queue, or an upload was dropped.
    async fn reclaim_large_values(&self, conn: &Storage, budget: usize) -> Result<usize, Error> {
        if conn
            .total_changes()
            .await
            .ok()
            .is_some_and(|at| self.reclaimed_at.get() == Some(at))
            && self.upload_cleanup.borrow().is_empty()
        {
            return Ok(0);
        }
        #[cfg(any(test, feature = "testing"))]
        self.reclaim_passes.set(self.reclaim_passes.get() + 1);
        self.reclaimed_at.set(None);
        self.release_dropped_uploads(conn).await?;
        let deleted = conn
            .scope("reclaim", async |conn| {
                crate::large::reclaim(conn, &self.chunks, budget).await
            })
            .await?;
        // A pass inside an open transaction may yet roll back, and one
        // that ran out of budget left work queued: both run again.
        if conn.is_autocommit() && !crate::large::reclaim_queued(conn).await? {
            self.reclaimed_at.set(conn.total_changes().await.ok());
        }
        Ok(deleted)
    }

    fn publish_schema(&self) {
        self.published_versions.replace(self.governed_versions());
        self.published_schema.replace(self.owner());
    }

    fn session_control(&self, control: sync::SessionControl) {
        if let Some(_lease) = self.operation.try_lock() {
            for queued in self.session_controls.take() {
                queued.apply(self);
            }
            control.apply(self);
        } else {
            self.session_controls.borrow_mut().push(control);
        }
    }

    async fn acquire_operation(&self) -> futures::lock::MutexGuard<'_, ()> {
        let lease = self.operation.lock().await;
        self.drain_released_reads();
        for control in self.session_controls.take() {
            control.apply(self);
        }
        self.seq.set(self.seq.get().max(self.seed_floor.get()));
        self.prior_seq.set(self.seq.get());
        self.prior_hold.set(self.shard.borrow().lowest_hold());
        self.prior_served.set(self.served_through());
        self.prior_ack_point.set(
            self.held_acks
                .borrow()
                .as_ref()
                .map_or(0, |acks| acks.issued),
        );
        lease
    }

    fn without_overlay(&self) -> &Self {
        self.settle_overlay();
        self
    }

    fn release_read(&self, read: sync::ReadCleanup) {
        if let Some(_lease) = self.operation.try_lock() {
            read.release(self);
        } else {
            let unsubscribe = matches!(read, sync::ReadCleanup::Subscription { .. });
            self.released_reads.borrow_mut().push(read);
            if unsubscribe {
                // The current operation might have already drained read cleanup.
                self.wakes.wake_now();
            }
        }
    }

    fn drain_released_reads(&self) {
        for read in self.released_reads.take() {
            read.release(self);
        }
    }
}

struct Batch {
    kind: BatchKind,
    reads_state: Rc<RefCell<TransactionReads>>,
    ops: Vec<PreparedWrite>,
    /// The rows `ops` write, by stored table: a later staged write's
    /// evidence (see [`Db::in_overlay`]). Shared with that write's check
    /// rather than copied, so a stage costs the same however many writes
    /// came before it.
    rows: Arc<GroundedRows>,
    /// The rows `ops` inserted fresh, by stored table: what a later staged
    /// write's `InsertedByThisTransaction` holds for (see
    /// [`Db::in_overlay`]).
    inserted: Arc<InsertedRows>,
    /// The check of each row's first write in `ops`, by stored table and
    /// row: the policy a later staged write to that row continues, so
    /// staging decides the row by its first write as the commit and the
    /// authority do (SPEC 7).
    first_checks: HashMap<(String, [u8; 16]), Vec<DeferredCheck>>,
    /// What the batch's partial large-value updates built: descriptors
    /// derived from stored trees and the nodes consolidations prepared,
    /// trusted and stored whenever the staged writes apply.
    edits: LargeEdits,
    /// Leases of the streamed values the batch's writes hold: their chunks
    /// stay until the batch commits or is dropped.
    uploads: Vec<crate::db::large_ops::UploadLease>,
    /// The claims its session writes were staged with: its upload carries
    /// these, not whatever its author's claims are by the time it commits
    /// (another handle acting for the same user may have registered
    /// others meanwhile).
    claims: Option<BTreeMap<String, Value>>,
    /// The companion rows its sealed writes staged, told to the encryption
    /// hooks if the batch goes without committing them.
    companions: e2ee_write::StagedCompanions,
}

/// Per-batch identity and admission order; an old future never observes a reused ID.
#[derive(Default)]
struct TransactionReads {
    queue: VecDeque<Rc<TransactionReadTicket>>,
    closed: bool,
}

impl TransactionReads {
    fn take_wakers(&self) -> Vec<Waker> {
        self.queue
            .iter()
            .filter_map(|ticket| ticket.waker.borrow_mut().take())
            .collect()
    }
}

/// Keep at most the current task's waker for each admitted read.
#[derive(Default)]
struct TransactionReadTicket {
    waker: RefCell<Option<Waker>>,
}

struct TransactionRead {
    state: Rc<RefCell<TransactionReads>>,
    ticket: Rc<TransactionReadTicket>,
    ledger: bool,
}

impl TransactionRead {
    fn is_first(&self, state: &TransactionReads) -> bool {
        state
            .queue
            .front()
            .is_some_and(|ticket| Rc::ptr_eq(ticket, &self.ticket))
    }

    async fn wait_turn(&self) {
        // The enclosing poll checks the batch lifetime and registers our waker.
        std::future::poll_fn(|_| {
            if self.is_first(&self.state.borrow()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Drop for TransactionRead {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.state.borrow_mut();
            let wakers = state.take_wakers();
            state
                .queue
                .retain(|ticket| !Rc::ptr_eq(ticket, &self.ticket));
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

fn close_transaction_reads(state: &Rc<RefCell<TransactionReads>>) {
    let wakers = {
        let mut state = state.borrow_mut();
        state.closed = true;
        state.take_wakers()
    };
    for waker in wakers {
        waker.wake();
    }
}

#[derive(Default)]
pub(crate) struct LargeEdits {
    pub(crate) derived: Vec<jazz_types::large_values::LargeValueRef>,
    pub(crate) pending_nodes: Vec<Rc<jazz_types::large_values::PreparedLargeValue>>,
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
            .filter_map(|op| op.op.row())
            .map(|row| *row.0.as_bytes())
            .collect()
    }
    fn snapshot(&self) -> Option<(Pos, Rc<[u64]>)> {
        match &self.kind {
            BatchKind::Mergeable => None,
            BatchKind::Serializable { snapshot, own, .. } => Some((*snapshot, own.clone())),
        }
    }

    fn prefix(&self) -> BatchPrefix {
        BatchPrefix {
            ops: self.ops.len(),
            derived: self.edits.derived.len(),
            pending_nodes: self.edits.pending_nodes.len(),
        }
    }

    /// Drops what a failed sealed stage added, its companions' ops
    /// included.
    fn restore_prefix(&mut self, prefix: BatchPrefix) {
        self.ops.truncate(prefix.ops);
        self.restore_edits(prefix);
    }

    /// Drops the edits a failed stage added. Not `ops`: a stage pushes its
    /// op only once it succeeded, so ops past `prefix.ops` are other
    /// stages' of the same transaction, made while this one awaited.
    fn restore_edits(&mut self, prefix: BatchPrefix) {
        self.edits.derived.truncate(prefix.derived);
        self.edits.pending_nodes.truncate(prefix.pending_nodes);
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
        own: Rc<[u64]>,
    },
}

/// One read of a serializable transaction: what it returned (row ids and
/// versions) at the snapshot, re-checked at commit.
struct ReadRecord {
    prepared: PreparedQuery,
    include_deleted: bool,
    /// The view it read through.
    through: ReadThrough,
    rows: Vec<(RowUuid, sync::Rank)>,
}

/// The view a transaction's read goes through: the root view, or a branch
/// view, resolved (`view`) and as the reader named it (`source`, for the
/// authority to resolve again).
#[derive(Clone, Debug)]
struct ReadThrough {
    view: Option<View>,
    source: ReadViewSourceSpec,
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

/// A read row's version: its table, row, position and writer, when the
/// read carried it.
type ReadVersion = (Arc<str>, RowUuid, Pos, Option<Arc<[u8]>>);

/// An aggregate row as a serializable read records it: its group (the row
/// id the read gives it) and every value it computed, so the authority's
/// re-run matches only while the aggregate's values are unchanged.
pub(crate) fn aggregate_read_id(row: &CurrentRow) -> RowUuid {
    let mut hasher = jazz_types::sha256::Hasher::new();
    hasher.update(b"jazz serializable aggregate read v1\0");
    hasher.update(row.id.0.as_bytes());
    let cells = postcard::to_allocvec(&*row.cells).expect("encode aggregate values");
    hasher.update(&(cells.len() as u64).to_be_bytes());
    hasher.update(&cells);
    let digest = hasher.finalize();
    RowUuid::from_bytes(digest.as_bytes()[..16].try_into().expect("16 bytes"))
}

fn versions(rows: &[CurrentRow]) -> Vec<ReadVersion> {
    rows.iter()
        .map(|row| (row.table.clone(), row.id, row.seq, row.writer.clone()))
        .collect()
}

/// The versions of a relation tree's child rows, depth first.
fn child_versions(node: &ResultNode, out: &mut Vec<ReadVersion>) {
    for relation in node.relations.values() {
        if let ResultRelation::Array(children) = relation {
            for child in children {
                out.push((
                    child.row.table.clone(),
                    child.row.id,
                    child.row.seq,
                    child.row.writer.clone(),
                ));
                child_versions(child, out);
            }
        }
    }
}

impl Table {
    fn stored_branch_selector(&self, branch: BranchSelector) -> BranchSelector {
        BranchSelector {
            values: branch
                .values
                .into_iter()
                .map(|(name, value)| (self.stored_name(&name).to_owned(), value))
                .collect(),
        }
    }

    fn stored_exact_write_target(&self, target: ExactWriteTarget) -> ExactWriteTarget {
        match target {
            ExactWriteTarget::Root => ExactWriteTarget::Root,
            ExactWriteTarget::Branch(branch) => {
                ExactWriteTarget::Branch(self.stored_branch_selector(branch))
            }
        }
    }

    fn stored_write_target(&self, target: WriteTarget) -> WriteTarget {
        match target {
            WriteTarget::Root => WriteTarget::Root,
            WriteTarget::BranchView { head, base } => WriteTarget::BranchView {
                head: self.stored_branch_selector(head),
                base: base.map(|base| match base {
                    BranchViewBase::Current(branch) => {
                        BranchViewBase::Current(self.stored_branch_selector(branch))
                    }
                    BranchViewBase::Snapshot { branch, snapshot } => BranchViewBase::Snapshot {
                        branch: self.stored_branch_selector(branch),
                        snapshot,
                    },
                }),
            },
        }
    }
}

impl StagedOp {
    fn by_stored_names(self, table: &Table) -> Self {
        let cells = |values: BTreeMap<String, Value>| {
            values
                .into_iter()
                .map(|(name, value)| (table.stored_name(&name).to_owned(), value))
                .collect()
        };
        let name = table.stored.clone();
        match self {
            Self::Insert(_, values, mut options) => {
                options.target = table.stored_exact_write_target(options.target);
                Self::Insert(name, cells(values), options)
            }
            Self::Update(_, row, values, mut options) => {
                options.target = table.stored_write_target(options.target);
                Self::Update(name, row, cells(values), options)
            }
            Self::Upsert(_, row, values, created, mut options) => {
                options.target = table.stored_write_target(options.target);
                // A local upsert's created cells (the ones the encryption
                // hooks sealed for a created row) are named as its cells.
                Self::Upsert(name, row, cells(values), cells(created), options)
            }
            Self::Delete(_, row, mut options) => {
                options.target = table.stored_write_target(options.target);
                Self::Delete(name, row, options)
            }
            Self::Restore(_, row, values, mut options) => {
                options.target = table.stored_exact_write_target(options.target);
                Self::Restore(name, row, values.map(cells), options)
            }
            Self::Move(_, from, to, row) => Self::Move(
                name,
                table.stored_branch_selector(from),
                table.stored_branch_selector(to),
                row,
            ),
        }
    }
}

impl WriteTable {
    fn column_name<'a>(&'a self, stored: &'a str) -> &'a str {
        self.source
            .by_stored_column(stored)
            .map_or(stored, |column| &column.name)
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

impl Store {
    /// Re-apply a staged write unchecked, keeping its provenance.
    async fn apply(
        &self,
        op: &PreparedWrite,
        seq: u64,
        retention: Option<&BTreeSet<String>>,
    ) -> Result<WriteHandle, Error> {
        let who = op.op.author().map_or(Who::Local, Who::Attributed);
        self.apply_as(op, who, seq, None, retention).await
    }

    /// Wall-clock Unix ms (or the one a test set).
    pub(crate) fn now_ms(&self) -> i64 {
        self.clock_for_test.get().unwrap_or_else(|| {
            web_time::SystemTime::now()
                .duration_since(web_time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64)
        })
    }

    /// The clock commit HLCs read (Unix ms): this store's configured clock
    /// (`DbConfig::with_clock`), or the one a test set.
    pub(crate) fn hlc_clock_ms(&self) -> u64 {
        self.clock_for_test
            .get()
            .map_or_else(|| (self.clock)(), |ms| ms.max(0) as u64)
    }

    /// The next commit HLC: after every one this store gave or saw, and
    /// not before its wall clock.
    pub(crate) fn next_hlc(&self) -> u64 {
        let next = self.peek_hlc();
        self.hlc_last.set(next);
        next
    }

    /// The HLC a commit begun now would take ([`Store::next_hlc`]).
    pub(crate) fn peek_hlc(&self) -> u64 {
        // Past the last a pending `_clock` holds, commits share it and order by
        // writer and tx.
        self.hlc_last
            .get()
            .saturating_add(1)
            .max(shard::hlc(self.hlc_clock_ms()))
            .min(sync::MAX_SEQ)
    }

    /// `seen` as this store counts an HLC it was sent: no further ahead
    /// than [`HLC_SKEW_MS`] past its own clock, and never past the last a
    /// pending `_clock` holds.
    pub(crate) fn bounded_hlc(&self, seen: u64) -> u64 {
        let ceiling = shard::hlc(self.hlc_clock_ms().saturating_add(HLC_SKEW_MS));
        seen.min(ceiling).min(sync::MAX_SEQ)
    }

    /// Hold this store's HLC past one it received (a position or a commit
    /// clock), so a commit it makes after seeing that orders after it. A
    /// received HLC counts no further ahead than [`HLC_SKEW_MS`] past this
    /// store's wall clock: a writer's clock far ahead cannot carry this
    /// store's commits with it.
    pub(crate) fn observe_hlc(&self, seen: u64) {
        self.hlc_last
            .set(self.hlc_last.get().max(self.bounded_hlc(seen)));
    }

    /// The seq a new local commit takes ([`Store::next_seq`]). It is above
    /// every seq this store committed, so on a replica an `own_order` entry
    /// already there is stale, left by a commit at it that never happened
    /// (a write that failed, or whose upload record did): the new commit
    /// must not inherit its HLC or its writer id.
    fn fresh_seq(&self) -> u64 {
        let seq = self.next_seq();
        if self.me.get() != sync::ACCEPTED {
            self.own_order.borrow_mut().remove(&seq);
        }
        seq
    }

    /// Give commit `seq` its HLC if it has none yet: its writes are at it,
    /// written by this store's node as tx `seq`.
    fn stamp_commit(&self, seq: u64, origin: Option<([u8; 16], u64)>) {
        if self.me.get() == sync::ACCEPTED {
            return;
        }
        // A commit made under a thin client's id is kept under it: a relay
        // uploads it so, so a resend of it anywhere gets its fate (SPEC 8
        // §8.14).
        if origin.is_some() || !self.own_order.borrow().contains_key(&seq) {
            let hlc = self.next_hlc();
            let (node, tx) = origin.unwrap_or((*self.identity.node.as_bytes(), seq));
            self.own_order.borrow_mut().insert(seq, (hlc, node, tx));
        }
    }

    /// Commit `seq`'s HLC, or the one a commit begun now would take.
    pub(crate) fn commit_hlc(&self, seq: u64) -> u64 {
        self.own_order
            .borrow()
            .get(&seq)
            .map_or_else(|| self.peek_hlc(), |order| order.0)
    }

    /// Commit `seq`'s writer, as history keeps it (`_writer`).
    pub(crate) fn own_writer(&self, seq: u64) -> Vec<u8> {
        match self.own_order.borrow().get(&seq) {
            Some(&(_, node, tx)) => sync::writer_bytes(node, tx),
            None => sync::writer_bytes(*self.identity.node.as_bytes(), seq),
        }
    }

    /// Which of this store's own commits (on a relay, or of the session
    /// commits it carries) wrote version `at`: by its writer, or with none
    /// given, the latest in order at its position.
    pub(crate) fn own_seq_at(&self, at: sync::Rank) -> Option<u64> {
        let (pos, writer) = at;
        if pos.accepted() || self.me.get() == sync::ACCEPTED {
            return None;
        }
        self.own_order
            .borrow()
            .iter()
            .filter(|(_, order)| {
                order.0 == pos.seq
                    && writer.is_none_or(|(node, tx)| (order.1, order.2) == (node, tx))
            })
            .max_by_key(|(_, order)| **order)
            .map(|(seq, _)| *seq)
    }

    /// Apply `op`, a write this store authors as `who`, at `seq`: its
    /// large values chunked and its counter changes resolved first.
    async fn apply_authored(
        &self,
        op: &mut PreparedWrite,
        who: Who,
        seq: u64,
        origin: Option<([u8; 16], u64)>,
    ) -> Result<WriteHandle, Error> {
        let edits = self.chunk_large_cells(op).await?;
        self.counter_deltas(op).await?;
        self.derived.borrow_mut().extend(edits.derived);
        self.pending_nodes.borrow_mut().extend(edits.pending_nodes);
        let applied = self.apply_as(op, who, seq, origin, None).await;
        self.clear_derived();
        applied
    }

    /// Apply one write as `who`: policies apply for a session identity.
    async fn apply_as(
        &self,
        prepared: &PreparedWrite,
        who: Who,
        seq: u64,
        origin: Option<([u8; 16], u64)>,
        retention: Option<&BTreeSet<String>>,
    ) -> Result<WriteHandle, Error> {
        let op = &prepared.op;
        let author = match who {
            Who::Session(author) | Who::Attributed(author) => author,
            Who::Local => self.stamp_author(),
        };
        let now = self.now_ms();
        if let Some(session) = self.checked(who) {
            Self::admit_session_write(op, session, now)?;
        }
        let (last_seq, last_at) = self.last_stamp.get();
        let at = if seq == last_seq {
            last_at
        } else {
            now.max(last_at.saturating_add(1))
        };
        self.last_stamp.set((seq, at));
        // An explicit timestamp (a backfill) stamps this write alone; the
        // clock's monotonic stamps go on.
        let at = op.updated_at_ms().map_or(at, |ms| ms as i64);
        self.stamp_commit(seq, origin);
        let stamp = Stamp {
            at,
            author: self.author_code(&author_key(&author)).await?,
        };
        let stored = &prepared.layout.table.stored;
        let prune = retention.map_or_else(
            || self.retention_of(self.owner_version.get()).contains(stored),
            |tables| tables.contains(stored),
        );
        self.apply_op(prune, &stamp, prepared, who, seq).await
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

    fn prepare_write(&self, schema: Rc<Schema>, op: StagedOp) -> Result<PreparedWrite, Error> {
        let source = schema.table(op.table())?;
        // Validate the caller's vocabulary before translating it. In
        // particular, a physical column name is not an alias a caller may
        // use to bypass the source version's declared columns.
        let cells = match &op {
            StagedOp::Insert(_, cells, _)
            | StagedOp::Update(_, _, cells, _)
            | StagedOp::Upsert(_, _, cells, _, _)
            | StagedOp::Restore(_, _, Some(cells), _) => Some(cells),
            _ => None,
        };
        if let Some(cells) = cells {
            self.validate_named_cells(
                source,
                cells.iter().map(|(name, value)| (name.as_str(), value)),
            )?;
        }
        let layout = self.write_layout(&schema, source);
        let op = if schema.stored_names {
            op
        } else {
            op.by_stored_names(source)
        };
        Ok(PreparedWrite { op, layout })
    }

    fn write_layout(&self, schema: &Rc<Schema>, source: &Table) -> Rc<WriteTable> {
        let version = if Rc::ptr_eq(schema, &self.owner()) {
            self.owner_version.get()
        } else {
            schema.version
        };
        let mut cached = self.write_layouts.borrow_mut();
        // Two views with the same structural version can have different permissions.
        let layouts = match &mut *cached {
            Some(layouts) if Rc::ptr_eq(&layouts.schema, schema) && layouts.version == version => {
                layouts
            }
            other => other.insert(WriteLayouts {
                schema: schema.clone(),
                version,
                tables: HashMap::new(),
            }),
        };
        if let Some(layout) = layouts.tables.get(&source.name) {
            return layout.clone();
        }
        let mut table = source.physical();
        if table.branch_key_names.is_empty() {
            table.branch_key_names = source.branch_by.clone();
        }
        let layout = Rc::new(WriteTable {
            table,
            source: source.clone(),
            version,
            stored_names: schema.stored_names,
            policies: schema.clone(),
        });
        layouts.tables.insert(source.name.clone(), layout.clone());
        layout
    }

    /// Received and persisted commits already name storage identities and
    /// carry the writer's defaults. They never pass through logical renaming.
    fn admit_stored_write(&self, schema: Rc<Schema>, op: StagedOp) -> Result<PreparedWrite, Error> {
        debug_assert!(schema.stored_names);
        self.prepare_write(schema, op)
    }

    async fn apply_op(
        &self,
        prune: bool,
        stamp: &Stamp,
        prepared: &PreparedWrite,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let write = &*prepared.layout;
        match prepared.op.clone() {
            StagedOp::Insert(_, c, o) => {
                let target = Target::exact(&o.target);
                self.insert(
                    stamp,
                    write,
                    c,
                    Cells::Authored,
                    o.row_id,
                    &target,
                    who,
                    seq,
                )
                .await
            }
            StagedOp::Update(_, r, c, o) => {
                let target = Target::of(&o.target, self).await?;
                self.update(prune, stamp, write, r, c, &target, who, seq)
                    .await
            }
            StagedOp::Upsert(_, r, c, created, o) => {
                let target = Target::of(&o.target, self).await?;
                self.upsert(prune, stamp, write, r, c, created, &target, who, seq)
                    .await
            }
            StagedOp::Delete(_, r, o) => {
                let target = Target::of(&o.target, self).await?;
                self.delete(prune, stamp, write, r, &target, who, seq).await
            }
            StagedOp::Restore(_, r, c, o) => {
                let target = Target::exact(&o.target);
                self.restore(
                    prune,
                    stamp,
                    write,
                    r,
                    c,
                    Cells::Authored,
                    &target,
                    who,
                    seq,
                )
                .await
            }
            StagedOp::Move(_, from, to, r) => {
                self.move_row(prune, stamp, write, from, to, r, seq).await
            }
        }
    }

    /// Apply staged writes at `seq`, each under the schema that staged it.
    /// Ops from `checked` on are checked as their own identity would be (a
    /// session's policies apply) instead of re-applied unchecked: how a
    /// newly staged write is validated.
    async fn apply_all_checking(
        &self,
        ops: &[PreparedWrite],
        checked: usize,
        seq: u64,
    ) -> Result<(), Error> {
        for (i, op) in ops.iter().enumerate() {
            if i < checked {
                self.apply(op, seq, None).await?;
            } else {
                let identity = op.op.identity().map_or(Who::Local, who);
                self.apply_as(op, identity, seq, None, None).await?;
            }
        }
        Ok(())
    }

    /// [`Store::apply_all_checking`] trusting and storing what a batch's partial
    /// large-value updates built.
    async fn apply_with_edits(
        &self,
        ops: &[PreparedWrite],
        edits: &LargeEdits,
        checked: usize,
        seq: u64,
    ) -> Result<(), Error> {
        self.derived
            .borrow_mut()
            .extend(edits.derived.iter().cloned());
        self.pending_nodes
            .borrow_mut()
            .extend(edits.pending_nodes.iter().cloned());
        let result = self.apply_all_checking(ops, checked, seq).await;
        self.clear_derived();
        result
    }

    /// On a replica, the newest version of `row` the authority settled.
    async fn settled_seq(&self, table: &str, row: RowUuid) -> Result<Option<u64>, Error> {
        let sql = format!(
            "SELECT MAX(_clock) FROM {} WHERE id = ?1 AND _clock >= 0 AND _clock < {}",
            quote(table),
            sync::band_start(sync::PENDING)
        );
        self.conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                r.get::<_, Option<i64>>(0)
            })
            .await
            .map(|seq| seq.map(|seq| seq as u64))
            .map_err(storage)
    }

    /// [`Self::newest_seq`] with its writer, when pending.
    pub(crate) async fn newest_version(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<Option<sync::Rank>, Error> {
        let sql = format!(
            "SELECT _clock, _branch FROM {} WHERE id = ?1 ORDER BY _clock DESC LIMIT 1",
            quote(table)
        );
        let newest: Option<(i64, Vec<u8>)> = self
            .conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .await
            .optional()
            .map_err(storage)?;
        match newest {
            Some((t, key)) => Ok(Some(
                self.current_rank(table, row, &key, Pos::from_t(t)).await?,
            )),
            None => Ok(None),
        }
    }

    /// The newest committed version of `row` in stored table `table` on
    /// any branch.
    pub(crate) async fn newest_seq(&self, table: &str, row: RowUuid) -> Result<Option<Pos>, Error> {
        let sql = format!(
            "SELECT _clock FROM {} WHERE id = ?1 ORDER BY _clock DESC LIMIT 1",
            quote(table)
        );
        self.conn
            .cached_row(&sql, [row.0.as_bytes().to_vec()], |r| {
                Ok(Pos::from_t(r.get(0)?))
            })
            .await
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
        let _operation = self.write_operation().await;
        self.begin(id, BatchKind::Mergeable)
    }

    /// A serializable transaction: snapshot reads, validated at commit.
    /// On a client the snapshot is the position its server served it
    /// complete through, plus its own unsettled commits: never positions it
    /// numbered itself, which would hold every authority version.
    pub async fn begin_serializable(&self, id: OpenTransactionId) -> Result<(), Error> {
        let _operation = self.write_operation().await;
        let (snapshot, own) = {
            let inner = self.inner.without_overlay();
            let snapshot = match inner.client() {
                Some(client) => Pos::at(client.known),
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
                own: own.into(),
            },
        )
    }

    fn begin(&self, id: OpenTransactionId, kind: BatchKind) -> Result<(), Error> {
        self.inner.without_overlay().require_mutation_admission()?;
        let mut batches = self.inner.batches.borrow_mut();
        if batches.contains_key(&id) {
            return Err(rejected(format!("transaction {id:?} is already open")));
        }
        batches.insert(
            id,
            Batch {
                kind,
                reads_state: Rc::default(),
                ops: Vec::new(),
                rows: Arc::default(),
                inserted: Arc::default(),
                first_checks: HashMap::new(),
                edits: LargeEdits::default(),
                uploads: Vec::new(),
                claims: None,
                companions: Default::default(),
            },
        );
        Ok(())
    }

    /// A fresh row id from this database's id source. Drawing one leaves
    /// an open transaction's overlay applied: a host draws one per staged
    /// insert, and undoing the overlay would make each stage replay the
    /// batch.
    pub fn next_row_id(&self) -> RowUuid {
        self.inner.ids.borrow_mut().next_row_id()
    }

    /// How many staged writes have been replayed into transaction overlays:
    /// staging a batch write by write replays each write at most once
    /// unless something else used the store in between.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn overlay_replays_for_test(&self) -> usize {
        // A peek: the store gate would undo the cached overlay.
        self.inner.overlay_replays.get()
    }

    /// What this store holds open between calls: anything a later call
    /// must find as it was left. A peek: the store gate would undo the
    /// overlay, so it is read without passing it. A store serving as a
    /// core must hold nothing between the calls it serves, so that it
    /// also runs on storage that cannot keep a transaction open (Durable
    /// Objects' `transactionSync`).
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn held_for_test(&self) -> Held {
        let inner = self.inner.as_ref();
        Held {
            overlay: inner.overlay.borrow().is_some(),
            open_batches: self.inner.batches.borrow().len(),
            sqlite_transaction: !inner.conn.is_autocommit(),
        }
    }

    /// How many times a staged write's check copied the evidence set it
    /// shares with its batch (see [`Batch::rows`]).
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn evidence_copies_for_test(&self) -> usize {
        self.inner.evidence_copies.get()
    }

    /// The open transaction `id`, mergeable or serializable.
    pub fn tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
        TxRef { db: self, id }
    }

    pub fn abandon_transaction_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        let batch = self
            .inner
            .batches
            .borrow_mut()
            .remove(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        if let Some(_lease) = self.inner.operation.try_lock()
            && self
                .inner
                .overlay
                .borrow()
                .as_ref()
                .is_some_and(|overlay| overlay.batch == id)
        {
            self.inner.settle_overlay();
        }
        close_transaction_reads(&batch.reads_state);
        Ok(())
    }

    fn take_batch(&self, id: OpenTransactionId) -> Result<Batch, Error> {
        self.inner.without_overlay().require_mutation_admission()?;
        let batch = self
            .inner
            .batches
            .borrow_mut()
            .remove(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        // Refuse before publication, and wake futures only after releasing Store.
        let unfinished = !batch.reads_state.borrow().queue.is_empty();
        close_transaction_reads(&batch.reads_state);
        if unfinished {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "transaction has unfinished reads",
            ));
        }
        Ok(batch)
    }

    async fn take_unheld_batch(
        &self,
        id: OpenTransactionId,
    ) -> Result<(Batch, futures::lock::MutexGuard<'_, ()>), Error> {
        let operation = self.write_operation().await;
        let batch = self.take_batch(id)?;
        let operation = self.wait_for_rows(operation, &batch.rows()).await?;
        Ok((batch, operation))
    }

    pub async fn commit_mergeable_handle(&self, id: OpenTransactionId) -> Result<TxReceipt, Error> {
        let (batch, _operation) = self.take_unheld_batch(id).await?;
        if !matches!(batch.kind, BatchKind::Mergeable) {
            return Err(rejected(format!("transaction {id:?} is not mergeable")));
        }
        self.commit_ops(batch, sync::Kind::Mergeable).await
    }

    /// Serializable commit: every row the batch writes must still be at a
    /// version at or before the snapshot, and every read must return the
    /// same rows at the same versions now as it did at the snapshot (this
    /// catches phantoms: rows that entered or left a predicate or window).
    /// The check and the apply run under one store borrow, so nothing can
    /// commit in between. An unfinished admitted read refuses and consumes the
    /// handle before any writes are published.
    pub async fn commit_serializable_handle(
        &self,
        id: OpenTransactionId,
    ) -> Result<TxReceipt, Error> {
        let (batch, _operation) = self.take_unheld_batch(id).await?;
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
            let landed = self.inner.without_overlay().own_landings(own).await?;
            {
                let inner = self.inner.without_overlay();
                for prepared in &batch.ops {
                    let op = &prepared.op;
                    if let Some(row) = op.row()
                        && inner
                            .newest_version(op.table(), row)
                            .await?
                            .is_some_and(|at| {
                                let ours = inner.own_seq_at(at).is_some_and(|tx| own.contains(&tx))
                                    || landed.values().any(|&landed| landed == at.0);
                                !(at.0.accepted() || ours)
                            })
                    {
                        return Err(conflict(
                            "row visible parent changed since transaction write was staged",
                        ));
                    }
                }
            }
            if own.is_empty() {
                let inner = self.inner.without_overlay();
                for prepared in &batch.ops {
                    let op = &prepared.op;
                    if let Some(row) = op.row()
                        && inner
                            .settled_seq(op.table(), row)
                            .await?
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
                let inner = self.inner.without_overlay();
                for read in reads {
                    let spec = ReadSpec {
                        include_deleted: read.include_deleted,
                        view: read.through.view.clone(),
                        ..ReadSpec::default()
                    };
                    let now = inner.read_versions(&read.prepared, None, &spec).await?;
                    let changed = now.len() != read.rows.len()
                        || now.iter().zip(&read.rows).any(|(now, then)| {
                            now.0 != then.0
                                || (now.1 != then.1
                                    && !(inner.own_seq_at(then.1).is_some_and(|tx| {
                                        own.contains(&tx)
                                            && (now.1.0.accepted()
                                                || landed.get(&tx) == Some(&now.1.0))
                                    })))
                        });
                    if changed {
                        return Err(conflict(
                            "a row read by the transaction changed since its snapshot",
                        ));
                    }
                }
            }
            let reads = {
                let inner = self.inner.without_overlay();
                let reads = reads
                    .iter()
                    .map(|read| {
                        Ok(sync::WireRead {
                            version: *read
                                .prepared
                                .schema
                                .as_ref()
                                .map_or(inner.owner_version.get(), |schema| schema.version)
                                .as_bytes(),
                            query: read.prepared.query.clone(),
                            binding: read.prepared.binding.clone(),
                            include_deleted: read.include_deleted,
                            rows: read
                                .rows
                                .iter()
                                .map(|(row, at)| {
                                    let (by, seq) = inner.read_version(*at);
                                    (*row.0.as_bytes(), by, seq)
                                })
                                .collect(),
                            view: read.through.source.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                debug_assert!(snapshot.accepted(), "a snapshot is the authority's");
                reads
            };
            let kind = sync::Kind::Serializable {
                snapshot: snapshot.seq,
                reads,
                own: own.to_vec(),
            };
            return self.commit_ops(batch, kind).await;
        }
        {
            let inner = self.inner.without_overlay();
            for prepared in &batch.ops {
                let op = &prepared.op;
                if let Some(row) = op.row()
                    && inner
                        .newest_seq(op.table(), row)
                        .await?
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
                    view: read.through.view.clone(),
                    ..ReadSpec::default()
                };
                if inner.read_versions(&read.prepared, None, &spec).await? != read.rows {
                    return Err(conflict(
                        "a row read by the transaction changed since its snapshot",
                    ));
                }
            }
        }
        self.commit_ops(batch, sync::Kind::Mergeable).await
    }

    async fn commit_ops(&self, mut batch: Batch, kind: sync::Kind) -> Result<TxReceipt, Error> {
        crate::db::large_ops::renew_upload_leases(self.inner.without_overlay(), &batch.uploads)
            .await?;
        self.inner
            .without_overlay()
            .add_creator_members(&mut batch)
            .await?;
        let receipt = self
            .with_upload(async || self.commit_ops_now(&batch, kind).await)
            .await?;
        batch.companions.committed();
        Ok(receipt)
    }

    async fn commit_ops_now(&self, batch: &Batch, kind: sync::Kind) -> Result<TxReceipt, Error> {
        self.inner.without_overlay().require_mutation_admission()?;
        self.inner.refuse_held(&batch.rows())?;
        let ops = &batch.ops;
        let (seq, prior, mark) = {
            let inner = self.inner.without_overlay();
            let seq = inner.fresh_seq();
            let conn = inner.conn.clone();
            let mark = inner.changed.borrow().len();
            // Each write passed its check as it was staged, with the rows
            // staged before it as evidence. The commit grounds the unit's
            // end state as the authority will on upload (SPEC 7), so a unit
            // commits here exactly when the authority accepts it: a row the
            // unit deleted is no evidence, and rows can't ground each other
            // in a cycle.
            inner.start_unit_heads();
            let applied = conn
                .db_scope("commit_batch", async || {
                    inner.defer_checks();
                    let applied = (async {
                        let () = inner.apply_with_edits(ops, &batch.edits, 0, seq).await?;
                        inner.finish_checks(seq).await.map_err(refused_at_commit)
                    })
                    .await;
                    if applied.is_err() {
                        inner.abandon_checks();
                    }
                    applied
                })
                .await;
            if let Err(error) = applied {
                inner.changed.borrow_mut().truncate(mark);
                return Err(error);
            }
            let prior = inner.seq.get();
            if !ops.is_empty() || kind.validates_reads() {
                inner.seq.set(inner.seq.get().max(seq));
            }
            (seq, prior, mark)
        };
        // `commit_ops` holds the upload record and the commit in one SQLite
        // transaction and rolls both back on an error.
        // Each op travels by stored names, so one transaction may write
        // through views of several versions.
        let recorded = (async {
            let mut encoded = Vec::with_capacity(ops.len());
            for op in ops {
                encoded.push(self.inner.without_overlay().upload_op(op).await?);
            }
            let ops = encoded;
            {
                self.record_stored_upload(ops, seq, kind, None, batch.claims.clone(), None)
                    .await
            }
        })
        .await;
        if let Err(error) = recorded {
            let inner = self.inner.without_overlay();
            inner.seq.set(prior);
            inner.changed.borrow_mut().truncate(mark);
            return Err(error);
        }
        self.notify().await;
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
    async fn in_overlay<R>(
        &self,
        id: OpenTransactionId,
        extra: Option<&mut PreparedWrite>,
        read: impl AsyncFnOnce(&Store, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let inner = self.inner.as_ref();
        let (extra_row, first_checks, staged, staged_rows, staged_inserts, ops, edits, snapshot) = {
            let batches = self.inner.batches.borrow();
            let batch = batches
                .get(&id)
                .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
            let staged = batch.ops.len();
            let extra_row = extra
                .as_deref()
                .and_then(|op| Some((op.op.table().to_owned(), *op.op.row()?.0.as_bytes())));
            let first_checks = extra_row
                .as_ref()
                .and_then(|key| batch.first_checks.get(key))
                .cloned()
                .unwrap_or_default();
            // A row is never evidence for its own check.
            let staged_rows = match extra.as_deref() {
                Some(op) => {
                    let mut rows = batch.rows.clone();
                    if let Some(row) = op.op.row() {
                        let stored = op.op.table();
                        if rows
                            .get(stored)
                            .is_some_and(|set| set.contains(row.0.as_bytes()))
                            && let Some(set) = Arc::make_mut(&mut rows).get_mut(stored)
                        {
                            set.remove(row.0.as_bytes());
                        }
                    }
                    rows
                }
                None => Arc::default(),
            };
            let staged_inserts = match extra.as_deref() {
                Some(_) => batch.inserted.clone(),
                None => Arc::default(),
            };
            let live = inner
                .overlay
                .borrow()
                .as_ref()
                .is_some_and(|overlay| overlay.batch == id && overlay.applied == staged);
            let ops = (!live).then(|| batch.ops.clone());
            let edits = LargeEdits {
                derived: batch.edits.derived.clone(),
                pending_nodes: batch.edits.pending_nodes.clone(),
            };
            (
                extra_row,
                first_checks,
                staged,
                staged_rows,
                staged_inserts,
                ops,
                edits,
                batch.snapshot(),
            )
        };
        let snapshot = match snapshot {
            Some((snapshot, own)) => Some(inner.serializable_cut(snapshot, &own).await?),
            None => None,
        };
        let own = inner.seq.get() + 1;
        inner.derived.borrow_mut().extend(edits.derived);
        inner.pending_nodes.borrow_mut().extend(edits.pending_nodes);
        if let Some(ops) = ops {
            inner.settle_overlay();
            inner.start_unit_heads();
            begin_savepoint(&inner.conn, "overlay")?;
            #[cfg(any(test, feature = "testing"))]
            {
                self.inner
                    .overlay_replays
                    .set(self.inner.overlay_replays.get() + ops.len());
            }
            let mark = inner.changed.borrow().len();
            if let Err(error) = inner.apply_all_checking(&ops, ops.len(), own).await {
                inner.clear_derived();
                inner.changed.borrow_mut().truncate(mark);
                end_savepoint(&inner.conn, "overlay", false)?;
                return Err(error);
            }
            inner.overlay.replace(Some(Overlay {
                batch: id,
                applied: staged,
                mark,
            }));
        }
        if let Err(error) = begin_savepoint(&inner.conn, "overlay_step") {
            inner.clear_derived();
            inner.settle_overlay();
            return Err(error);
        }
        let mark = inner.changed.borrow().len();
        // A step that is undone leaves no head rows behind.
        let heads = inner.inherited_heads.borrow().clone();
        let joins = extra.is_some();
        let mut first = Vec::new();
        let mut first_key = None;
        let applied = match extra {
            Some(op) => {
                (async {
                    let () = inner.counter_deltas(op).await?;
                    {
                        // The write continues the unit its batch staged so far:
                        // those writes passed their checks, so their rows are
                        // evidence for this one's (SPEC 7), as at the authority
                        // that receives the unit. The commit checks the whole unit.
                        inner.defer_checks();
                        if let Some(pending) = inner.deferred.borrow_mut().as_mut() {
                            pending.extend(first_checks);
                        }
                        let applied = (async {
                            let () = inner
                                .apply_all_checking(std::slice::from_ref(op), 0, own)
                                .await?;
                            if let Some(key) = &extra_row
                                && let Some(pending) = inner.deferred.borrow().as_ref()
                            {
                                first.extend(pending.iter().cloned());
                                first_key = Some(key.clone());
                            }
                            inner
                                .finish_checks_after(own, staged_rows, staged_inserts)
                                .await
                        })
                        .await;
                        if applied.is_err() {
                            inner.abandon_checks();
                        }
                        applied
                    }
                })
                .await
            }
            None => Ok(()),
        };
        inner.clear_derived();
        let result = (async {
            applied?;
            {
                let own = inner.own_rank(own);
                read(inner, snapshot.map(|cut| Cut { own, ..cut })).await
            }
        })
        .await;
        let keep = joins && result.is_ok();
        if !keep {
            inner.changed.borrow_mut().truncate(mark);
            *inner.inherited_heads.borrow_mut() = heads;
        }
        if let Err(error) = end_savepoint(&inner.conn, "overlay_step", keep) {
            inner.settle_overlay();
            return Err(error);
        }
        if keep && let Some(overlay) = inner.overlay.borrow_mut().as_mut() {
            overlay.applied += 1;
        }
        if keep
            && let Some(key) = first_key
            && !first.is_empty()
            && let Some(batch) = self.inner.batches.borrow_mut().get_mut(&id)
        {
            batch.first_checks.entry(key).or_insert(first);
        }
        result
    }

    /// Run `read` over the batch's writes staged before `prefix` was
    /// captured, so a read that waited (for authority coverage, or behind a
    /// cancelled read) does not see writes staged meanwhile. When nothing
    /// was staged since, this is [`Self::in_overlay`] and keeps its cached
    /// overlay; otherwise the prefix is replayed in a throwaway savepoint,
    /// which undoes the cached overlay, so the next stage replays the batch.
    async fn in_overlay_prefix<R>(
        &self,
        id: OpenTransactionId,
        prefix: BatchPrefix,
        read: impl AsyncFnOnce(&Store, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        // Peek without the store gate, which would undo the cached overlay.
        let grown = self
            .inner
            .batches
            .borrow()
            .get(&id)
            .is_none_or(|batch| batch.prefix() != prefix);
        if !grown {
            return self.in_overlay(id, None, read).await;
        }
        let inner = self.inner.without_overlay();
        let (ops, edits, snapshot) = {
            let batches = self.inner.batches.borrow();
            let batch = batches
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
            (ops, edits, batch.snapshot())
        };
        let snapshot = match snapshot {
            Some((snapshot, own)) => Some(inner.serializable_cut(snapshot, &own).await?),
            None => None,
        };
        let own = inner.seq.get() + 1;
        begin_savepoint(&inner.conn, "overlay")?;
        #[cfg(any(test, feature = "testing"))]
        {
            self.inner
                .overlay_replays
                .set(self.inner.overlay_replays.get() + ops.len());
        }
        let mark = inner.changed.borrow().len();
        let result = (async {
            inner.apply_with_edits(&ops, &edits, ops.len(), own).await?;
            {
                let own = inner.own_rank(own);
                read(inner, snapshot.map(|cut| Cut { own, ..cut })).await
            }
        })
        .await;
        inner.changed.borrow_mut().truncate(mark);
        end_savepoint(&inner.conn, "overlay", false)?;
        result
    }

    async fn stage(&self, id: OpenTransactionId, op: StagedOp) -> Result<(), Error> {
        self.stage_with_edits(id, op, LargeEdits::default()).await
    }

    async fn stage_op(&self, id: OpenTransactionId, mut op: PreparedWrite) -> Result<(), Error> {
        // Validate against the overlay now, so errors surface at the call;
        // a counter write's change is from what the transaction sees.
        // The write's fresh inserts, for the batch's later writes' checks.
        let inserted = match self
            .in_overlay(id, Some(&mut op), async |inner, _| {
                Ok(std::mem::take(&mut *inner.unit_inserts.borrow_mut()))
            })
            .await
        {
            Ok(inserted) => inserted,
            Err(error) => return Err(self.serializable_write_conflict(id, &op, error).await),
        };
        let inner = self.inner.as_ref();
        let stored = match op.op.row() {
            Some(row) => Some((op.op.table().to_owned(), row)),
            None => None,
        };
        let staged_claims = match op.op.identity() {
            Some(WriteIdentity::Session(author)) if op.op.is_session() => Some(
                inner
                    .claims
                    .borrow()
                    .get(&author)
                    .cloned()
                    .unwrap_or_default(),
            ),
            _ => None,
        };
        let mut batches = self.inner.batches.borrow_mut();
        let batch = batches
            .get_mut(&id)
            .ok_or_else(|| rejected("transaction is no longer open"))?;
        if batch.claims.is_none() {
            batch.claims = staged_claims;
        }
        if let Some((table, row)) = stored {
            Arc::make_mut(&mut batch.rows)
                .entry(table)
                .or_default()
                .insert(*row.0.as_bytes());
        }
        for (table, rows) in inserted {
            Arc::make_mut(&mut batch.inserted)
                .entry(table)
                .or_default()
                .extend(rows);
        }
        batch.ops.push(op);
        Ok(())
    }

    /// A serializable write the store refuses for its row's state (an id
    /// taken, a row deleted) conflicts when that state is newer than the
    /// transaction's snapshot: at the snapshot the write was valid, and a
    /// retry sees what moved.
    async fn serializable_write_conflict(
        &self,
        id: OpenTransactionId,
        prepared: &PreparedWrite,
        error: Error,
    ) -> Error {
        let op = &prepared.op;
        if !error.is_apply_time_rejection() {
            return error;
        }
        let inner = self.inner.without_overlay();
        let Some((snapshot, own)) = self
            .inner
            .batches
            .borrow()
            .get(&id)
            .and_then(Batch::snapshot)
        else {
            return error;
        };
        let Some(row) = op.row() else {
            return error;
        };
        let moved: Result<bool, Error> = (async {
            let cut = inner.serializable_cut(snapshot, &own).await?;
            {
                Ok(inner
                    .newest_version(op.table(), row)
                    .await?
                    .is_some_and(|version| !cut.sees(version)))
            }
        })
        .await;
        match moved {
            Ok(true) => conflict("a row written by the transaction changed since its snapshot"),
            _ => error,
        }
    }

    /// Stage a write whose large values a partial update built:
    /// the batch keeps what it built for every later apply. The encryption
    /// hooks seal it first, so a refusal surfaces here, not at commit.
    pub(crate) async fn stage_with_edits(
        &self,
        id: OpenTransactionId,
        op: StagedOp,
        edits: LargeEdits,
    ) -> Result<(), Error> {
        self.stage_streamed(id, op, edits, None).await
    }

    /// [`Self::stage_with_edits`] of a write whose `streamed` column holds a
    /// value the encryption hooks sealed as it streamed (see
    /// [`Db::write_streamed_with_fetch`]).
    pub(crate) async fn stage_streamed(
        &self,
        id: OpenTransactionId,
        op: StagedOp,
        edits: LargeEdits,
        streamed: Option<large_ops::StreamedSeal<'_>>,
    ) -> Result<(), Error> {
        if cfg!(feature = "e2ee-client") {
            self.stage_sealed(id, op, edits, streamed).await
        } else {
            self.stage_chunked(id, op, edits).await
        }
    }

    /// [`Self::stage_with_edits`] of a write already sealed.
    pub(crate) async fn stage_chunked(
        &self,
        id: OpenTransactionId,
        op: StagedOp,
        edits: LargeEdits,
    ) -> Result<(), Error> {
        let op = self.prepare_write(op)?;
        self.stage_prepared_with_edits(id, op, edits).await
    }

    async fn stage_prepared_with_edits(
        &self,
        id: OpenTransactionId,
        mut op: PreparedWrite,
        mut edits: LargeEdits,
    ) -> Result<(), Error> {
        // A whole write over a chunked value becomes its edit of the value
        // the batch sees: its earlier writes applied, no other batch's.
        let chunked = self
            .in_overlay(id, None, async |inner, _| {
                inner.chunk_large_cells(&mut op).await
            })
            .await?;
        let prefix = {
            let inner = self.inner.as_ref();
            inner.require_mutation_admission()?;
            edits.derived.extend(chunked.derived);
            edits.pending_nodes.extend(chunked.pending_nodes);
            let mut batches = self.inner.batches.borrow_mut();
            let batch = batches
                .get_mut(&id)
                .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
            let prefix = batch.prefix();
            batch.edits.derived.extend(edits.derived);
            batch.edits.pending_nodes.extend(edits.pending_nodes);
            prefix
        };
        let staged = self.stage_op(id, op).await;
        if staged.is_err()
            && let Some(batch) = self.inner.batches.borrow_mut().get_mut(&id)
        {
            batch.restore_edits(prefix);
        }
        staged
    }

    /// Keep a streamed value's upload lease for as long as batch `id` is
    /// open.
    pub(crate) fn hold_upload(
        &self,
        id: OpenTransactionId,
        lease: crate::db::large_ops::UploadLease,
    ) -> Result<(), Error> {
        self.inner
            .batches
            .borrow_mut()
            .get_mut(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?
            .uploads
            .push(lease);
        Ok(())
    }

    /// The read a serializable batch `id` records (and hydrates) for one
    /// of `prepared` made through its first `prefix` staged writes, as
    /// `identity`: its windows widened over what those writes could have
    /// promoted into them (see [`Store::recorded_window`]). `None` when
    /// that is `prepared` itself, or the batch is not serializable.
    async fn widened_read(
        &self,
        id: OpenTransactionId,
        prefix: usize,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<Option<PreparedQuery>, Error> {
        if !has_window(&prepared.query) {
            return Ok(None);
        }
        // Peek without the store gate: most reads stop here.
        let staged = match self.inner.batches.borrow().get(&id) {
            Some(
                batch @ Batch {
                    kind: BatchKind::Serializable { .. },
                    ..
                },
            ) => Store::staged_rows(batch, prefix)?,
            _ => return Ok(None),
        };
        if staged.is_empty() {
            return Ok(None);
        }
        let inner = self.inner.without_overlay();
        inner.recorded_window(
            prepared,
            identity,
            &staged,
            inner.read_namespace(prepared).await?,
        )
    }

    async fn record_read(
        &self,
        id: OpenTransactionId,
        prepared: &PreparedQuery,
        include_deleted: bool,
        snapshot: Pos,
        through: ReadThrough,
    ) -> Result<(), Error> {
        let inner = self.inner.without_overlay();
        let Some((_, own)) = self
            .inner
            .batches
            .borrow()
            .get(&id)
            .and_then(Batch::snapshot)
        else {
            return Ok(());
        };
        // A branch read records the rows of its view: the head's, or the
        // base's it inherits, so a change to either (or a head row masking
        // a base one) conflicts.
        let read = ReadSpec {
            include_deleted,
            cut: Some(inner.serializable_cut(snapshot, &own).await?),
            view: through.view.clone(),
            ..ReadSpec::default()
        };
        let rows = inner.read_versions(prepared, None, &read).await?;
        if let Some(Batch {
            kind:
                BatchKind::Serializable {
                    snapshot: pinned_snapshot,
                    reads,
                    pinned,
                    ..
                },
            ..
        }) = self.inner.batches.borrow_mut().get_mut(&id)
        {
            // Even a LocalOnly read observes this cut. Later remote coverage
            // must not move a snapshot whose results have already escaped.
            *pinned_snapshot = snapshot;
            *pinned = true;
            reads.push(ReadRecord {
                prepared: prepared.clone(),
                include_deleted,
                through,
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
            .fetching_with(&[], op, async |op| self.db.stage(self.id, op).await)
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
            None => self.db.inner.ids.borrow_mut().next_row_id(),
        };
        opts.row_id = Some(row);
        self.stage(StagedOp::Insert(table.to_owned(), cells, opts))
            .await?;
        Ok(WriteHandle {
            row,
            seq: Pos::default(),
            tx: 0,
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
            tx: 0,
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
            tx: 0,
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
            tx: 0,
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
            tx: 0,
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
    /// Reads run in first-poll order, retaining the writes staged at that poll.
    /// The first successful serializable read fixes its snapshot for later reads.
    pub async fn all_prepared_at(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
        tier: ReadTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_in(
            prepared,
            include_deleted,
            identity,
            tier,
            &ReadViewSourceSpec::Current,
        )
        .await
    }

    /// As [`Self::all_prepared_at`], through `source`: a branch view (a head
    /// over an optional live or frozen base) composes as it does outside a
    /// transaction, over the transaction's staged writes.
    ///
    /// Only a mergeable transaction reads through a branch view so far: a
    /// serializable one records its read set for the root view alone, so a
    /// branch read there is refused rather than validated against the wrong
    /// rows. A historic (snapshot) source is refused in either.
    pub async fn all_prepared_in(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
        tier: ReadTier,
        source: &ReadViewSourceSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        let opts = ReadOpts {
            tier,
            read_view: ReadViewSpec {
                source: source.clone(),
                include_deleted,
            },
            ..ReadOpts::default()
        };
        self.all_prepared_with_options(prepared, identity, &opts, None)
            .await
    }

    /// Binding entry point: as [`Self::all_prepared_in`], keeping the
    /// caller's whole read options (its first-load policy) and coverage
    /// check for a mergeable read's fetch, as outside a transaction. This
    /// does not select a different identity or upgrade a mergeable read's
    /// read set.
    pub(crate) async fn all_prepared_with_options(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        opts: &ReadOpts,
        coverage: Option<&dyn Fn() -> bool>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_admitted(Ok(prepared), identity, opts, coverage, false)
            .await
            .map(|(rows, _)| rows)
    }

    pub(super) async fn all_prepared_admitted(
        &self,
        prepared: Result<&PreparedQuery, Error>,
        identity: Option<AuthorSubject>,
        opts: &ReadOpts,
        coverage: Option<&dyn Fn() -> bool>,
        ledger: bool,
    ) -> Result<(Vec<CurrentRow>, Option<Pos>), Error> {
        let source = &opts.read_view.source;
        if let ReadViewSourceSpec::Snapshot { .. } = source {
            return Err(Error::new(
                ErrorCode::Query,
                "Reading a snapshot inside a transaction is not supported yet",
            ));
        }
        // Capture staging before queueing: later writes must not leak into
        // the prefix this read admitted, even when its predecessor is cancelled.
        let (prefix, state) = {
            let batches = self.db.inner.batches.borrow();
            let batch = batches
                .get(&self.id)
                .ok_or_else(|| rejected("transaction is not open"))?;
            (batch.prefix(), batch.reads_state.clone())
        };
        let guard = TransactionRead {
            state,
            ticket: Rc::default(),
            ledger,
        };
        guard
            .state
            .borrow_mut()
            .queue
            .push_back(guard.ticket.clone());
        let reading = async {
            let outcome = match prepared {
                Ok(prepared) => {
                    self.read_admitted(prepared, identity, opts, coverage, prefix, &guard)
                        .await
                }
                Err(error) => Err(error),
            };
            // Preparation and hydration may fail before this read's turn.
            if outcome.is_err() {
                guard.wait_turn().await;
            }
            outcome
        };
        let mut reading = std::pin::pin!(reading);
        std::future::poll_fn(|cx| {
            {
                let state = guard.state.borrow();
                if state.closed {
                    return Poll::Ready(Err(rejected("transaction is no longer open")));
                }
                {
                    let mut waker = guard.ticket.waker.borrow_mut();
                    if waker
                        .as_ref()
                        .is_none_or(|prior| !prior.will_wake(cx.waker()))
                    {
                        *waker = Some(cx.waker().clone());
                    }
                }
                if !guard.is_first(&state) {
                    // The first read still owns snapshot selection. Only ledger
                    // reads can start a fetch ahead of their turn, at that cut.
                    let pinned_ledger = ledger
                        && matches!(
                            self.db.inner.batches.borrow().get(&self.id),
                            Some(Batch {
                                kind: BatchKind::Serializable { pinned: true, .. },
                                ..
                            })
                        );
                    if !pinned_ledger {
                        return Poll::Pending;
                    }
                }
            }
            reading.as_mut().poll(cx)
        })
        .await
    }

    /// The branch view `source` reads through (`None`: the root view). A
    /// live base needs nothing from the store; only a frozen one resolves
    /// its snapshot, under the store gate (which applies the staged writes
    /// of open transactions, so it is not taken for every branch read).
    async fn read_view(
        &self,
        source: &ReadViewSourceSpec,
        include_deleted: bool,
    ) -> Result<Option<View>, Error> {
        Ok(match source {
            ReadViewSourceSpec::Current | ReadViewSourceSpec::Snapshot { .. } => None,
            ReadViewSourceSpec::BranchView {
                head,
                base: None | Some(BranchViewBase::Current(_)),
            } => Some(View {
                head: head.clone(),
                base: match source {
                    ReadViewSourceSpec::BranchView {
                        base: Some(BranchViewBase::Current(base)),
                        ..
                    } => Some(base.clone()),
                    _ => None,
                },
                base_cut: None,
            }),
            ReadViewSourceSpec::BranchView { .. } => {
                let frozen = ReadOpts {
                    read_view: ReadViewSpec {
                        source: source.clone(),
                        include_deleted,
                    },
                    ..ReadOpts::for_read_tier(ReadTier::LocalOnly)
                };
                ReadSpec::of(&frozen, self.db.inner.without_overlay())
                    .await?
                    .view
            }
        })
    }

    async fn read_admitted(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        opts: &ReadOpts,
        coverage: Option<&dyn Fn() -> bool>,
        prefix: BatchPrefix,
        admission: &TransactionRead,
    ) -> Result<(Vec<CurrentRow>, Option<Pos>), Error> {
        let tier = opts.tier;
        let include_deleted = opts.read_view.include_deleted;
        let source = &opts.read_view.source;
        // Except for LocalOnly, a client hydrates a serializable read from
        // the authority at the snapshot first. LocalFirst may answer from
        // the replica when the link cannot answer.
        let snapshot = match self.db.inner.batches.borrow().get(&self.id) {
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
        // A serializable read resolves its view before fetching, so a
        // frozen base it cannot resolve fails before asking the authority.
        let mut view = match snapshot {
            Some(_) => Some(self.read_view(source, include_deleted).await?),
            None => None,
        };
        let answer = if let Some((snapshot, pinned)) = snapshot
            && self.db.is_client()
            && tier != ReadTier::LocalOnly
        {
            let at = pinned.then_some(snapshot);
            // Hydrate what the read will record, not just what it returns:
            // a widened window's extra rows are what the overlay promotes.
            let widened = self
                .db
                .widened_read(self.id, prefix.ops, prepared, identity)
                .await?;
            let hydrated = widened.as_ref().unwrap_or(prepared);
            self.db.remote_shape(&hydrated.query)?;
            self.db
                .fetch(
                    hydrated,
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
                    source,
                )
                .await?
        } else {
            None
        };
        // Only the admitted head can take the cut, read its staged
        // prefix, and record the result.
        admission.wait_turn().await;
        let snapshot = snapshot.map(|(snapshot, pinned)| {
            if pinned {
                snapshot
            } else {
                answer.unwrap_or(snapshot)
            }
        });
        let non_durable = self.db.inner.non_durable.get();
        if snapshot.is_none()
            && self.db.is_client()
            && let Some(reach) = sync::remote_reach(opts, non_durable, false)
            && let Some(deadline) = sync::remote_deadline(opts, non_durable, true)
        {
            // A mergeable read fetches as a read outside a transaction does.
            // A cold non-durable foreground asks its durable relay (its
            // worker) at its own tier and first-load wait, including while
            // that relay's server is offline; a durable client asks the
            // server on Remote, or on LocalFirst with `WaitForRemote`. A
            // Remote read waits for the server while offline (`strict`).
            //
            // Records nothing: the read keeps its admitted prefix and its
            // no-read-set semantics below. Unlike a read outside a
            // transaction, it neither pins the fetched position
            // (`Position::At`) nor filters a Remote read to settled rows: it
            // has no snapshot, so it reads the overlay's current cut (at
            // least as new as that answer), with other writers' unsettled
            // rows visible, as a serializable read's overlay shows them.
            // As outside a transaction, the query must have a synced shape.
            self.db.remote_shape(&prepared.query)?;
            self.db
                .fetch(
                    prepared,
                    None,
                    include_deleted,
                    reach,
                    deadline,
                    coverage,
                    tier == ReadTier::Remote,
                    source,
                )
                .await?;
        }
        // A frozen base resolves its snapshot only now: a cold foreground
        // learns the state it was captured at from the fetch above.
        let view = match view.take() {
            Some(view) => view,
            None => self.read_view(source, include_deleted).await?,
        };
        let through = ReadThrough {
            view,
            source: source.clone(),
        };
        // Relations nest inside the overlay, so they see staged rows too.
        // Offloaded leaves are fetched as for any read.
        let (rows, prepared) = self
            .db
            .fetching_with(&[], (), async |()| {
                // Both remote hydration and offloaded chunks may yield while
                // owner policies change. Resolve policy at each evaluation.
                let prepared = match identity {
                    Some(_) => self.db.execution_query(prepared),
                    None => std::borrow::Cow::Borrowed(prepared),
                };
                let rows = self
                    .db
                    .in_overlay_prefix(self.id, prefix, async |inner, cut| {
                        // Keep the overlay's own writes at the fetched cut.
                        let cut = cut.map(|cut| Cut {
                            seq: snapshot.unwrap_or(cut.seq),
                            ..cut
                        });
                        let read = ReadSpec {
                            include_deleted,
                            cut,
                            view: through.view.clone(),
                            ..ReadSpec::default()
                        };
                        let roots = inner
                            .run_for_reader(&prepared, identity, &read, true)
                            .await?;
                        inner
                            .nest_relations(&prepared, identity, &read, roots)
                            .await
                    })
                    .await?;
                Ok((rows, prepared))
            })
            .await?;
        if let Some(snapshot) = snapshot {
            if admission.ledger
                && rows.iter().any(|row| {
                    let at = row.seq();
                    !at.accepted() || at.seq > snapshot.seq
                })
            {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!(
                        "a ledger read of `{}` found a row not accepted at its cut",
                        prepared.query.table
                    ),
                ));
            }
            let widened = self
                .db
                .widened_read(self.id, prefix.ops, &prepared, identity)
                .await?;
            self.db
                .record_read(
                    self.id,
                    widened.as_ref().unwrap_or(&prepared),
                    include_deleted,
                    snapshot,
                    through,
                )
                .await?;
        }
        Ok((rows, snapshot))
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

/// The statement checking a policy `condition` (with `params` leading
/// parameters) on one row of `table` at one branch key: the row and the
/// key are the next two parameters.
fn policy_check_sql(table: &Table, condition: &str, params: usize) -> String {
    format!(
        "SELECT COALESCE({condition}, 0) FROM {} AS a WHERE a.id = ?{} AND a._branch = ?{}",
        quote(&table.stored),
        params + 1,
        params + 2
    )
}

/// A policy check's statement for one [`CheckKey`], under the claims and
/// the enum-tag epoch it was compiled with.
struct CheckPlan {
    claims: Option<BTreeMap<String, Value>>,
    epoch: u64,
    sql: String,
    /// The parameters before the row and branch ones.
    params: Vec<Sql>,
    /// The policy compiled to `1`: any existing row passes.
    constant: bool,
}

/// What a kept policy check was compiled for. The version, bundle, naming
/// and governing schema keep schemas of different versions apart; they do
/// not tell two policy sets of one version apart (a permissions-only
/// change keeps the version, and the new schema may take the old one's
/// address). That is why a schema change clears every kept check
/// ([`Store::clear_check_plans`] from `reset_everything`).
#[derive(Clone, PartialEq, Eq, Hash)]
struct CheckKey {
    namespace: Option<[u8; 16]>,
    schema: usize,
    version: crate::ids::SchemaVersionId,
    bundle: Option<crate::catalogue::BundleId>,
    stored_names: bool,
    governing: Option<(
        usize,
        crate::ids::SchemaVersionId,
        Option<crate::catalogue::BundleId>,
    )>,
    table: String,
    operation: Operation,
    author: AuthorSubject,
}

impl CheckKey {
    fn new(
        schema: &Schema,
        table: &Table,
        operation: Operation,
        author: AuthorSubject,
        namespace: Option<[u8; 16]>,
    ) -> Self {
        Self {
            namespace,
            schema: std::ptr::from_ref(schema) as usize,
            version: schema.version,
            bundle: schema.bundle,
            stored_names: schema.stored_names,
            governing: schema.policies_from.as_ref().map(|governing| {
                (
                    Rc::as_ptr(governing) as usize,
                    governing.version,
                    governing.bundle,
                )
            }),
            table: table.stored.clone(),
            operation,
            author,
        }
    }
}

/// The kept policy checks of a store. A key keeps a plan for each of the
/// last [`CheckPlans::PER_KEY`] claims it was checked under, so an author
/// whose claims alternate (a session served with request claims beside its
/// own) does not compile on every switch. At [`CheckPlans::cap`] keys the
/// store forgets them all and starts again: authors come and go, and the
/// map must not grow with every one a long-lived server has seen. A store
/// whose live keys (authors × tables × operations × schemas) exceed the
/// cap therefore compiles about as often as with no kept checks at all.
struct CheckPlans {
    plans: HashMap<CheckKey, Vec<Rc<CheckPlan>>>,
    cap: usize,
    /// Tests: key every schema as if it took the address of the last one
    /// dropped, which an allocator may or may not do.
    #[cfg(any(test, feature = "testing"))]
    reused_addresses: bool,
}

impl Default for CheckPlans {
    fn default() -> Self {
        Self {
            plans: HashMap::new(),
            cap: Self::CAP,
            #[cfg(any(test, feature = "testing"))]
            reused_addresses: false,
        }
    }
}

impl CheckPlans {
    /// Each kept key costs about 1.8 KB of resident memory with one claims
    /// set (its key, the statement's text and parameters, the claims it
    /// was compiled under, and the map entry), measured as the RSS growth
    /// of 8 000 authors each kept once under a membership `EXISTS` read
    /// policy; a key holding [`CheckPlans::PER_KEY`] claims sets costs up
    /// to about twice that. At 16 384 keys the worst case was about 30 MB
    /// per store, up to 60 MB with two claims sets per key. At 4 096 it is
    /// about 7 MB, up to 15 MB, while still holding every author of a
    /// deployment of a few hundred authors × tables × operations.
    const CAP: usize = 4_096;
    const PER_KEY: usize = 2;

    fn key(
        &self,
        schema: &Schema,
        table: &Table,
        operation: Operation,
        author: AuthorSubject,
        namespace: Option<[u8; 16]>,
    ) -> CheckKey {
        #[allow(unused_mut)]
        let mut key = CheckKey::new(schema, table, operation, author, namespace);
        #[cfg(any(test, feature = "testing"))]
        if self.reused_addresses {
            key.schema = 0;
            if let Some((address, ..)) = &mut key.governing {
                *address = 0;
            }
        }
        key
    }

    fn get(
        &self,
        key: &CheckKey,
        claims: Option<&BTreeMap<String, Value>>,
        epoch: u64,
    ) -> Option<Rc<CheckPlan>> {
        self.plans
            .get(key)?
            .iter()
            .find(|plan| plan.epoch == epoch && plan.claims.as_ref() == claims)
            .cloned()
    }

    fn put(&mut self, key: CheckKey, plan: Rc<CheckPlan>) {
        if self.plans.len() >= self.cap && !self.plans.contains_key(&key) {
            self.plans.clear();
        }
        let kept = self.plans.entry(key).or_default();
        kept.retain(|old| old.epoch == plan.epoch && old.claims != plan.claims);
        kept.insert(0, plan);
        kept.truncate(Self::PER_KEY);
    }

    fn clear(&mut self) {
        self.plans.clear();
    }
}

#[cfg(any(test, feature = "testing"))]
impl Db {
    /// For each branch key `row` of `table` is stored at: `author`'s
    /// `operation` check as a store runs it (kept plans; `existing`: as for
    /// a row just read), and compiled afresh.
    pub async fn policy_check_for_test(
        &self,
        table: &str,
        row: RowUuid,
        author: AuthorSubject,
        operation: Operation,
        existing: bool,
    ) -> Result<Vec<(bool, bool)>, Error> {
        let _operation = self.write_operation().await;
        let schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let table = schema.table(table)?;
        let keys: Vec<Vec<u8>> = {
            let mut statement = inner
                .conn
                .prepare(&format!(
                    "SELECT _branch FROM {} WHERE id = ?1",
                    quote(&table.stored)
                ))
                .map_err(storage)?;
            let rows = statement
                .query_map([row.0.as_bytes().as_slice()], |r| r.get::<_, Vec<u8>>(0))
                .await
                .map_err(storage)?;
            rows.collect().await.map_err(storage)?
        };
        let mut checks = Vec::with_capacity(keys.len());
        for key in &keys {
            let kept = if existing {
                inner
                    .check_in_existing(&schema, table, row, key, author, operation)
                    .await?
            } else {
                inner
                    .check_in(&schema, table, row, key, author, operation)
                    .await?
            };
            let fresh = inner
                .check_in_fresh(&schema, table, row, key, author, operation)
                .await?;
            checks.push((kept, fresh));
        }
        Ok(checks)
    }

    /// `author`'s plain `operation` check of `row`, which `table` does not
    /// hold, at the branch key `beside` is stored at: as a store runs it
    /// (kept plans) and compiled afresh. Each is `None` where it errs.
    pub async fn absent_policy_check_for_test(
        &self,
        table: &str,
        row: RowUuid,
        beside: RowUuid,
        author: AuthorSubject,
        operation: Operation,
    ) -> Result<(Option<bool>, Option<bool>), Error> {
        let _operation = self.write_operation().await;
        let schema = self.operation_schema();
        let inner = self.inner.without_overlay();
        let table = schema.table(table)?;
        let key: Vec<u8> = inner
            .conn
            .query_row(
                &format!("SELECT _branch FROM {} WHERE id = ?1", quote(&table.stored)),
                [beside.0.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .await
            .map_err(storage)?;
        assert!(!inner.row_at(table, row, &key).await?, "{row:?} is stored");
        Ok((
            inner
                .check_in(&schema, table, row, &key, author, operation)
                .await
                .ok(),
            inner
                .check_in_fresh(&schema, table, row, &key, author, operation)
                .await
                .ok(),
        ))
    }

    /// How many (schema, table, operation, author) keys hold kept checks.
    pub fn check_plan_keys_for_test(&self) -> usize {
        self.inner.check_plans.borrow().plans.len()
    }

    /// Key kept checks as if every new schema took the address of the one
    /// it replaced.
    pub fn reuse_check_key_addresses_for_test(&self) {
        self.inner.check_plans.borrow_mut().reused_addresses = true;
    }

    /// Bound the kept checks at `cap` keys.
    pub fn set_check_plan_cap_for_test(&self, cap: usize) {
        self.inner.check_plans.borrow_mut().cap = cap;
    }
}

#[cfg(test)]
mod check_plan_tests {
    use super::*;

    async fn namespace_replica() -> (Db, RowUuid, AuthorSubject) {
        use jazz_model::model::public_schema::{
            ColumnType, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
        };

        let schema = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("scope", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_delete(PolicyExpr::Cmp {
                        column: "scope".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::TableIdentity {
                            table: "notes".into(),
                        },
                    })),
            )
            .build();
        let alice = AuthorSubject::for_test_bytes([1; 16]);
        let db = Db::open(DbConfig::new(
            JazzSchema::new(&schema).unwrap(),
            DbIdentity {
                node: crate::ids::NodeUuid::from_bytes([2; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ))
        .await
        .unwrap();
        let store = db.inner.as_ref();
        let version = store.owner_version.get();
        let digest = crate::names::names_digest(store.names().get(&version).unwrap());
        store
            .mirror_catalogue(
                None,
                vec![],
                vec![],
                vec![],
                &[(*version.as_bytes(), digest)],
            )
            .await
            .unwrap();
        let row = RowUuid::from_bytes([3; 16]);
        let scope =
            crate::names::identity([7; 16], &store.owner().table("notes").unwrap().stored, None);
        db.insert(
            "notes",
            BTreeMap::from([("scope".into(), Value::Uuid(scope))]),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (db, row, alice)
    }

    /// Alice's replica denies a delete until the authority namespace arrives.
    /// Comparing its kept check with a fresh compile requires the store's test oracle.
    #[test]
    fn kept_check_grants_after_the_namespace_arrives() {
        block_on(async {
            let (db, row, alice) = namespace_replica().await;
            let check = || db.policy_check_for_test("notes", row, alice, Operation::Delete, false);
            assert_eq!(check().await.unwrap(), [(false, false)]);
            db.inner
                .mirror_catalogue(Some([7; 16]), vec![], vec![], vec![], &[])
                .await
                .unwrap();
            assert_eq!(check().await.unwrap(), [(true, true)]);
        });
    }

    /// Rolling back Alice's first read of an already committed namespace preserves her grant.
    /// The store-level test seeds the metadata independently and controls the read's savepoint.
    #[test]
    fn rolling_back_a_read_keeps_the_committed_namespace() {
        block_on(async {
            let (db, row, alice) = namespace_replica().await;
            let store = db.inner.as_ref();
            store
                .exec(
                    "INSERT INTO _jazz_meta (k, v) VALUES (?1, ?2)",
                    crate::storage::params![sync::NAMESPACE_META, vec![7u8; 16]],
                )
                .await
                .unwrap();
            store
                .conn
                .db_scope_then("namespace", async || {
                    assert_eq!(store.stored_namespace().await.unwrap(), Some([7; 16]));
                    crate::storage::Scoped::Undo(())
                })
                .await
                .unwrap();
            assert_eq!(
                db.policy_check_for_test("notes", row, alice, Operation::Delete, false)
                    .await
                    .unwrap(),
                [(true, true)]
            );
        });
    }

    fn key(table: &str) -> CheckKey {
        CheckKey {
            namespace: None,
            schema: 0,
            version: crate::ids::SchemaVersionId::from_bytes([0; 16]),
            bundle: None,
            stored_names: false,
            governing: None,
            table: table.to_owned(),
            operation: Operation::Select,
            author: AuthorSubject::SYSTEM,
        }
    }

    fn claims(role: &str) -> Option<BTreeMap<String, Value>> {
        Some(BTreeMap::from([(
            "role".to_owned(),
            Value::String(role.to_owned()),
        )]))
    }

    fn plan(role: &str, epoch: u64) -> Rc<CheckPlan> {
        Rc::new(CheckPlan {
            claims: claims(role),
            epoch,
            sql: role.to_owned(),
            params: Vec::new(),
            constant: false,
        })
    }

    fn kept(plans: &CheckPlans, table: &str, role: &str, epoch: u64) -> bool {
        plans
            .get(&key(table), claims(role).as_ref(), epoch)
            .is_some_and(|plan| plan.sql == role)
    }

    /// A key keeps its last two claim sets, the newest displacing the
    /// oldest, and only plans of the current tag epoch.
    #[test]
    fn a_key_keeps_its_last_two_claim_sets_of_one_epoch() {
        let mut plans = CheckPlans::default();
        plans.put(key("docs"), plan("a", 1));
        plans.put(key("docs"), plan("b", 1));
        assert!(kept(&plans, "docs", "a", 1) && kept(&plans, "docs", "b", 1));
        assert!(!kept(&plans, "docs", "a", 2), "another epoch");
        assert!(!kept(&plans, "docs", "c", 1), "other claims");
        assert!(!kept(&plans, "teams", "a", 1), "another key");
        plans.put(key("docs"), plan("c", 1));
        assert!(!kept(&plans, "docs", "a", 1), "the oldest left");
        assert!(kept(&plans, "docs", "b", 1) && kept(&plans, "docs", "c", 1));
        // The same claims again replace their plan, not the other one.
        plans.put(key("docs"), plan("b", 1));
        assert!(kept(&plans, "docs", "b", 1) && kept(&plans, "docs", "c", 1));
        plans.put(key("docs"), plan("b", 2));
        assert!(kept(&plans, "docs", "b", 2));
        assert!(!kept(&plans, "docs", "c", 1) && !kept(&plans, "docs", "c", 2));
        assert_eq!(plans.plans[&key("docs")].len(), 1, "stale epochs left");
    }

    /// A new key at the cap forgets every kept check; a kept key's new plan
    /// does not.
    #[test]
    fn a_new_key_at_the_cap_forgets_the_rest() {
        let mut plans = CheckPlans {
            cap: 2,
            ..Default::default()
        };
        plans.put(key("a"), plan("r", 1));
        plans.put(key("b"), plan("r", 1));
        plans.put(key("b"), plan("s", 1));
        assert!(
            kept(&plans, "a", "r", 1) && kept(&plans, "b", "r", 1) && kept(&plans, "b", "s", 1)
        );
        plans.put(key("c"), plan("r", 1));
        assert_eq!(plans.plans.len(), 1);
        assert!(kept(&plans, "c", "r", 1) && !kept(&plans, "a", "r", 1));
    }
}

/// White-box on purpose: how many commits a store's DDL costs, and that a
/// failing statement undoes the statements before it, are not visible
/// through the public API (a store that fails to open is just an error).
#[cfg(test)]
mod execute_script_tests {
    use super::execute_script;
    use crate::options::block_on;
    use crate::storage::{Scoped, Storage};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// `tables` tables, each with an index and a trigger whose body holds
    /// a `;` (so the script is not split inside it).
    fn script(tables: usize) -> String {
        (0..tables)
            .map(|i| {
                format!(
                    "CREATE TABLE t{i} (a, b); CREATE INDEX t{i}_a ON t{i}(a); \
                     CREATE TRIGGER t{i}_tr AFTER INSERT ON t{i} BEGIN SELECT 1; END;"
                )
            })
            .collect()
    }

    /// Native SQLite and an emulated Durable Object store.
    fn both() -> [(&'static str, Storage); 2] {
        [
            ("native", Storage::open_in_memory().unwrap()),
            ("managed", Storage::managed_emulation_in_memory().unwrap()),
        ]
    }

    fn objects(storage: &Storage) -> i64 {
        block_on(storage.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name LIKE 't%'",
            [],
            |r| r.get(0),
        ))
        .unwrap()
    }

    /// A schema's DDL is one commit (one sync of the file), however many
    /// statements it holds.
    #[test]
    fn a_script_commits_once() {
        for (owner, storage) in both() {
            let commits = Arc::new(AtomicUsize::new(0));
            let counted = commits.clone();
            storage
                .commit_hook(Some(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    false
                }))
                .unwrap();
            block_on(execute_script(&storage, &script(50))).unwrap();
            assert_eq!(commits.load(Ordering::SeqCst), 1, "{owner}");
            assert_eq!(objects(&storage), 150, "{owner}");
        }
    }

    /// A failing statement undoes the whole script: no half-created schema.
    #[test]
    fn a_failed_script_leaves_nothing() {
        for (owner, storage) in both() {
            let failing = format!("{} CREATE TABLE t0 (again);", script(3));
            assert!(
                crate::options::block_on(execute_script(&storage, &failing)).is_err(),
                "{owner}"
            );
            assert_eq!(objects(&storage), 0, "{owner}");
            assert!(storage.is_autocommit(), "{owner}");
        }
    }

    /// Inside a caller's scope the script commits, or is undone, with it.
    #[test]
    fn a_script_inside_a_scope_goes_with_it() {
        for (owner, storage) in both() {
            block_on(storage.scope_then("caller", async |storage| {
                execute_script(storage, &script(2)).await.unwrap();
                assert!(!storage.is_autocommit(), "{owner}");
                Scoped::Undo(())
            }))
            .unwrap();
            assert_eq!(objects(&storage), 0, "{owner}");
        }
    }
}

#[cfg(test)]
mod independent_state_tests {
    use super::*;
    use jazz_model::model::public_schema::SchemaBuilder;

    /// Alice can allocate IDs and access host settings and events while execution
    /// owns the operation lease. This unit test holds the private lease to model
    /// a suspended database operation deterministically.
    #[test]
    fn host_state_does_not_borrow_execution_state() {
        let identity = DbIdentity {
            node: crate::ids::NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        };
        let schema = JazzSchema::new(&SchemaBuilder::new().build()).unwrap();
        let db = block_on(Db::open(
            DbConfig::new(schema, identity).with_id_source(SeededRowIdSource::new(42)),
        ))
        .unwrap();
        let mut expected = SeededRowIdSource::new(42);
        let execution = db.inner.as_ref();
        let _operation = block_on(execution.acquire_operation());
        db.check_serves(AuthorSubject::SYSTEM).unwrap();
        assert_eq!(db.next_row_id(), expected.next_row_id());
        assert_eq!(
            execution.ids.borrow_mut().next_row_id(),
            expected.next_row_id()
        );
        assert_eq!(db.next_row_id(), expected.next_row_id());
        execution.authority.set(true);
        assert!(db.is_authority());
        db.require_session_pins();
        assert!(execution.pins_required.get());
        let policy = LargeValueStagingPolicy {
            max_age_ms: 123,
            ..Default::default()
        };
        db.set_large_value_staging_policy(policy);
        assert_eq!(db.inner.staging_policy.get().max_age_ms, 123);
        db.set_slow_query_threshold(None);
        assert!(db.take_slow_queries().0.is_empty());
        assert!(db.take_merge_fallbacks().0.is_empty());
        db.track_upstream_rejections();
        execution
            .upstream_rejections
            .borrow_mut()
            .as_mut()
            .unwrap()
            .push(UpstreamRejection {
                writer: [1; 16],
                seq: 1,
                error: rejected("refused"),
                serializable: false,
            });
        assert_eq!(db.take_upstream_rejections().len(), 1);
        assert!(db.take_upstream_rejections().is_empty());
        db.track_closed_subscribers();
        assert!(execution.closed.borrow().is_some());
        assert!(db.take_closed_subscribers().is_empty());
        assert_eq!(db.chunk_fetches_answered(), execution.chunks.answered());
    }
}
