//! The `Db` facade: Jazz's public database API over one SQLite connection.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::ids::{AuthorSubject, RowUuid};
use crate::protocol::{
    BranchSelector, BranchViewBase, CurrentWriteSchema, MigrationLens, ReadViewSourceSpec,
    SchemaVersion,
};
use crate::query::Query;
use crate::records::{Value, ValueType};
use crate::schema::history::history_source_table;
use crate::schema::{JazzSchema, MergeStrategy};
use crate::tools::OpenTransactionId;
use crate::tools::Operation;
use crate::tx::{DurabilityTier, Fate, RejectionReason};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params_from_iter};

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
pub use large_ops::{
    JsonSetEdit, LargeValueStagingPolicy, LargeValueUpdate, LargeValueUpdatePage,
    LargeValueUpdateSplice, StagedStreamingValue, StreamingMutationKind, StreamingValueUpload,
};
#[path = "catalogue.rs"]
mod catalogue;
pub use catalogue::{CatalogueHeadV1, CatalogueSchemaV1};
#[path = "catalogue_policy.rs"]
mod catalogue_policy;
pub use catalogue_policy::PolicyDescriptorV1;

#[path = "catalogue_cache.rs"]
mod catalogue_cache;
pub use catalogue_cache::{
    AuthenticatedCatalogueState, merge_catalogue_captures, validate_catalogue_capture_replacement,
};
#[path = "initialization.rs"]
mod initialization;
pub use initialization::{InitializationSeal, InitializationTransactionStatus, ReservedTxId};
#[path = "accepted.rs"]
mod accepted;
#[path = "candidates.rs"]
mod candidates;
#[path = "ordinary_unit.rs"]
mod ordinary_unit;
#[path = "sync.rs"]
pub(crate) mod sync;
#[path = "writer_order.rs"]
mod writer_order;
pub use crate::names::LensEffects;
pub use crate::options::{REMOTE_LINK_ATTEMPT_WINDOW, TickScheduler, TickUrgency};
use crate::result_tree::{ResultNode, ResultRelation, ResultTree};
pub use crate::values::CurrentRow;
use crate::values::{
    AuthorKey, Schema, Sql, Table, author_key, from_sql, quote, storage, to_sql, unsupported,
};
pub use accepted::{AcceptedContent, AcceptedRow, BindingSettledRead};
pub use sync::ScopedReadRowV1;
pub use sync::{
    Cell as SyncCell, ContentWitnessRef, FirstLoad, Image, LOCAL_BASE, OwnerDurabilityBarrier,
    OwnerFrozenBaseV1, OwnerInputBaseV1, QueryAttachment, RelationSnapshot, RemoteLinkHint,
    SerializedReadResult, SubscriberId, SyncMessage, Transport, TransportError, Upstream,
    WireError, WireTransport, WireTransportAdapter, duplex,
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
    /// This trusted-backend read uses the authenticated catalogue governor.
    authority_policies: bool,
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

/// Application-visible fate and durability for a local write transaction.
/// Mirrors the existing binding contract; this is not a persisted encoding.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WriteState {
    pub fate: Fate,
    pub global_time: Option<jazz_types::time::GlobalTime>,
    pub durability: DurabilityTier,
}

impl WriteState {
    pub(crate) fn rejected(error: &Error, durability: DurabilityTier) -> Self {
        let reason = match error.code {
            ErrorCode::TransactionConflict => RejectionReason::SerializableConflict,
            ErrorCode::WriteRejected if !error.is_apply_time_rejection() => {
                RejectionReason::AuthorizationDenied
            }
            _ => RejectionReason::MalformedCommit(error.message.clone()),
        };
        Self {
            fate: Fate::Rejected(reason),
            global_time: None,
            durability,
        }
    }
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
    /// A flat join's joined source rows ([`CurrentRow::joined_rows`]).
    pub joined: Vec<crate::values::JoinedRow>,
    /// A flat join's root version ([`CurrentRow::root_version`]).
    pub version: Option<crate::values::SourceVersion>,
    /// A UNION leaf at root source position zero.
    pub union_arm: Option<String>,
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
    scoped_evidence: bool,
    permission_cut: Option<u64>,
    permission_own: u64,
    include_deleted: bool,
    /// A replica's read of the server's view (`ReadTier::Remote`).
    members_only: bool,
    view: Option<View>,
    cut: Option<Cut>,
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
        Ok(Self {
            scoped_evidence: false,
            permission_cut: None,
            permission_own: 0,
            include_deleted: opts.read_view.include_deleted,
            members_only: inner.client.is_some() && opts.tier == ReadTier::Remote,
            view,
            cut,
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
    /// Strict Remote streams retain this gate for later authority loss.
    gate: Option<sync::Gate>,
    strict_remote: bool,
    gate_opened: bool,
    /// Last result delivered by a strict stream, for reconciling an outage.
    delivered: Option<Vec<CurrentRow>>,
    group: Weak<RefCell<fanout::Group>>,
    /// Why the server refused the remote read, once it has; reported once.
    refused: Option<Rc<RefCell<Option<Error>>>>,
    /// The remote attachment to release on drop.
    remote: Option<(Rc<RefCell<Inner>>, Query, BTreeMap<String, Value>, bool)>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
        if let Some((inner, query, binding, include_deleted)) = self.remote.take() {
            Db { inner, view: None }.detach_remote(&query, &binding, include_deleted);
        }
    }
}

impl SubscriptionStream {
    /// Also notify when fresh authority evidence arrives for this query's
    /// dependencies, even if their optimistic contents did not change. Such
    /// notifications carry no row edits and do not qualify a read as accepted.
    pub fn observe_accepted_evidence(&mut self) {
        if let Some(group) = self.group.upgrade() {
            group.borrow_mut().observe_accepted_evidence(&self.queue);
        }
    }

    /// Whether polling can return an event now, without consuming it. Hosts
    /// use this to notify an idle foreground only when there is work to drain.
    pub fn has_pending_event(&self) -> bool {
        if self
            .refused
            .as_ref()
            .is_some_and(|refused| refused.borrow().is_some())
        {
            return true;
        }
        if let Some(gate) = &self.gate {
            if !gate.open() || self.gate_waiting_for_content(gate) {
                return false;
            }
            if !self.gate_opened {
                return true;
            }
        }
        !self.queue.borrow().is_empty()
    }

    fn gate_waiting_for_content(&self, gate: &sync::Gate) -> bool {
        (gate.wait_for_owner || gate.may_still_wait())
            && self
                .group
                .upgrade()
                .is_some_and(|group| group.borrow().awaiting())
    }

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
                // A restored authority answer must replace any deltas that
                // accumulated from the local preview while it was unavailable.
                self.gate_opened = false;
                self.queue.borrow_mut().clear();
                return None;
            }
            // The opening lists rows whose large values are complete: while
            // their chunks may still arrive, it waits for them.
            if self.gate_waiting_for_content(gate) {
                return None;
            }
            if self.gate_opened {
                // The consumer may not have polled during the outage. Never
                // replay its queued Local deltas after the gate reopens.
                let local_delta = matches!(
                    self.queue.borrow().front(),
                    Some(SubscriptionEvent::Delta { settled, tier, .. })
                        if !settled || *tier != DurabilityTier::Global
                );
                if !local_delta {
                    return self.pop_event();
                }
            }
            let settled = gate.answered.get();
            let global = settled && gate.global.get();
            self.gate_opened = true;
            if !self.strict_remote {
                self.gate = None;
            }
            self.queue.borrow_mut().clear();
            let group = self.group.upgrade()?;
            let group = group.borrow();
            let mut event = match &self.delivered {
                Some(before) => fanout::diff(before, &group.rows)
                    .unwrap_or_else(|| delta(false, Vec::new(), Vec::new(), Vec::new())),
                None => fanout::opening(&group.rows),
            };
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
            self.remember(&event);
            return Some(event);
        }
        self.pop_event()
    }

    fn pop_event(&mut self) -> Option<SubscriptionEvent> {
        let event = self.queue.borrow_mut().pop_front()?;
        self.remember(&event);
        Some(event)
    }

    fn remember(&mut self, event: &SubscriptionEvent) {
        if !self.strict_remote {
            return;
        }
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        else {
            return;
        };
        if !reset && added.is_empty() && updated.is_empty() && removed.is_empty() {
            return;
        }
        let before = self.delivered.take().unwrap_or_default();
        let omitted: BTreeSet<usize> = removed
            .iter()
            .map(|row| row.index)
            .chain(updated.iter().filter_map(|row| row.previous_index))
            .collect();
        let mut kept = before
            .into_iter()
            .enumerate()
            .filter(|(index, _)| !reset && !omitted.contains(index))
            .map(|(_, row)| row)
            .peekable();
        let mut placed: BTreeMap<usize, CurrentRow> = added
            .iter()
            .chain(updated)
            .map(|row| (row.index, row.row.clone()))
            .collect();
        let mut rows = Vec::new();
        while !placed.is_empty() || kept.peek().is_some() {
            let row = placed
                .remove(&rows.len())
                .or_else(|| kept.next())
                .expect("subscription delta has contiguous output indexes");
            rows.push(row);
        }
        self.delivered = Some(rows);
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
    /// A cache bootstrap requires accepted metadata for each requested schema.
    catalogue_cache_required: bool,
    /// Shared by every view; observation moves the capture once.
    catalogue_capture_pending: std::cell::Cell<bool>,
    /// Policies for views admitted from authenticated authority metadata. Kept
    /// separately from the replica's caller-supplied, possibly policy-free owner.
    view_policy_source: Option<Rc<Schema>>,
    view_policies_dirty: bool,
    candidate_checks: RefCell<Option<Vec<candidates::DeferredCheck>>>,
    candidate_read_evidence: Option<crate::compile::CandidateEvidence>,
    ordinary_checks: RefCell<Option<Vec<ordinary_unit::Check>>>,
    /// Large-value chunks: the cache the collation and decoders read.
    chunks: Arc<crate::large::Chunks>,
    /// Nodes a large-value edit consolidated, stored when the edited cell
    /// is bound inside the write's transaction.
    pending_nodes: Vec<jazz_types::large_values::PreparedLargeValue>,
    /// Descriptors this store's own edit derived for the write in flight:
    /// trusted as built, so binding them re-validates nothing.
    derived: Vec<jazz_types::large_values::LargeValueRef>,
    /// Limits on local streaming uploads and on pushed nodes awaiting
    /// their commit.
    staging: LargeValueStagingPolicy,
    /// Upload leases dropped by capabilities or completed/abandoned batches.
    /// The queue is separate from the owner borrow, so Drop never re-enters it.
    upload_cleanup: Rc<RefCell<Vec<[u8; 32]>>>,
    /// The staging ingress window: (start ms, encoded bytes admitted).
    ingress: RefCell<(u64, u64)>,
    /// The schema the current operation runs under: the owner's, or the
    /// schema view's that issued it.
    schema: Rc<Schema>,
    /// The owner's current schema.
    owner: Rc<Schema>,
    /// The stored tables: the union of every admitted schema, keyed and
    /// named by stored names.
    physical: Schema,
    /// Admitted structural schemas, by version, and registered views.
    admitted: Vec<JazzSchema>,
    /// Where each admitted version stores its tables and columns.
    names: crate::names::Names,
    views: BTreeMap<SchemaViewId, JazzSchema>,
    /// Open transaction batches, by id.
    batches: HashMap<OpenTransactionId, Batch>,
    retired_initializations: BTreeSet<OpenTransactionId>,
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
    pub(crate) claims: HashMap<AuthorSubject, BTreeMap<String, Value>>,
    /// Live subscription groups and their invalidation index.
    fan: fanout::Fanout,
    /// Rows written since the last dispatch: (table, id, branch key).
    changed: RefCell<Vec<(String, RowUuid, Vec<u8>)>>,
    /// In-memory frontier for accepted-evidence subscription notifications.
    accepted_notifications_through: u64,
    accepted_notifications_known: u64,
    /// The open transaction's seq and the tables it wrote.
    tx: Option<(u64, BTreeSet<String>)>,
    publications: Vec<SchemaPublication>,
    /// Set once connected upstream: this store is a replica.
    client: Option<sync::Client>,
    /// A replica whose store does not outlive it (a foreground over a
    /// durable relay): its local reads wait for the upstream's view too.
    non_durable: bool,
    scope_isolated_owner: bool,
    owner_projection_error: Option<Error>,
    deferred_owner_durability: bool,
    retired: bool,
    mutation_admission_closed: bool,
    scheduler: Option<Rc<dyn TickScheduler>>,
    read_deadlines: Vec<Weak<web_time::Instant>>,
    /// The global authority: what it settles is durable at `Global`.
    authority: bool,
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
    /// SQLite VFS the file lives on (`None`: the platform default). Browser
    /// builds register theirs (IndexedDB, OPFS) before opening.
    pub vfs: Option<&'static str>,
    pub journal: Journal,
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
    /// Unchecked like `Local` (the database identity stays the policy
    /// subject), with this author as the write's provenance.
    Attributed(AuthorSubject),
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
                !matches!(
                    column.as_str(),
                    "rid" | "id" | "_b" | "_seq" | "_del" | "_c"
                )
            })
            .collect();
        tables.insert(name.clone(), Table::stored_only(name, columns));
    }
    Ok(Schema {
        tables,
        jazz: jazz.clone(),
        policies_from: None,
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

const ROW_ALREADY_DELETED: &str = "row already deleted: ";
const ROW_ALREADY_EXISTS: &str = "encoding error: object already exists: ";
const ROW_NOT_DELETED: &str = "row not deleted: ";

const MISSING_REQUIRED: &str = "missing required field ";
const BRANCH_MOVE: &str = "invalid mergeable commit: branch column does not match exact branch key";

/// A new row (or restored content) without a value for a required column.
fn missing_required(table: &str, column: &str) -> Error {
    rejected(format!("{MISSING_REQUIRED}`{column}` in `{table}`"))
}

/// A write to (or an insert reusing the id of) a deleted row.
fn row_already_deleted(row: RowUuid) -> Error {
    rejected(format!("{ROW_ALREADY_DELETED}{}", row.0))
}

/// A restore of a live row.
fn row_not_deleted(row: RowUuid) -> Error {
    rejected(format!("{ROW_NOT_DELETED}{}", row.0))
}

/// An insert reusing a live row's id.
fn row_already_exists(row: RowUuid) -> Error {
    rejected(format!("{ROW_ALREADY_EXISTS}{}", row.0))
}

impl Error {
    /// Whether a write was refused for the stored state of its row rather
    /// than for its arguments: its id is deleted or taken, it restores a
    /// live row, it would create a row without a required value (an upsert
    /// may create or patch), or it would move a row to another branch. A host reports it through
    /// the write's handle, as an apply-time rejection, not from the call.
    pub fn is_apply_time_rejection(&self) -> bool {
        match self.code {
            ErrorCode::WriteRejected => [
                ROW_ALREADY_DELETED,
                ROW_ALREADY_EXISTS,
                ROW_NOT_DELETED,
                MISSING_REQUIRED,
            ]
            .iter()
            .any(|prefix| self.message.starts_with(prefix)),
            ErrorCode::Schema => self.message == BRANCH_MOVE,
            _ => false,
        }
    }
}

impl Error {
    /// Whether a read needs large-value chunks this replica has requested
    /// from upstream but not received yet.
    pub fn is_missing_chunks(&self) -> bool {
        crate::large::is_missing(self)
    }
}

/// An update of a row this replica does not hold: a missing observation,
/// not a read-policy decision.
fn update_target_not_loaded(operation: &str, table: &str, row: RowUuid) -> Error {
    Error::new(
        ErrorCode::NotObserved,
        format!(
            "{operation} on table {table} needs the current row, but row {} is not loaded \
             locally; read or subscribe to the row before updating it",
            row.0
        ),
    )
}

/// A relation facade query (hopTo, gather, match), as clients send it: the
/// ordinary query it stands for, the way Jazz's validation normalizes it.
/// Projections that rename or narrow the output are not supported yet.
/// Unions retain their relation tree for SQL compilation. `None` when the
/// query is not a relation query.
pub(super) fn normalize_relation(query: &Query, schema: &Schema) -> Result<Option<Query>, Error> {
    use crate::query::{RelationProjectExpr, RelationRowIdRef};
    let Some(relation) = &query.relation else {
        return Ok(None);
    };
    let not_yet = |what: &str| crate::values::unsupported(what.to_owned());
    let relation_error = |e: crate::query::QueryError| crate::values::query_error(e.to_string());
    let mut resolved = crate::query::relation_query_to_query(relation).map_err(relation_error)?;
    if resolved.table != query.table {
        return Err(crate::values::query_error(
            "relation query output table does not match its Query envelope",
        ));
    }
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
    if crate::query::relation_union_parts(&relation.rel).is_some() {
        return Ok(None);
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
    resolved.relation = None;
    Ok(Some(resolved))
}

/// The reason a policy rejects a write, leading its message (the code
/// stays `WriteRejected`): authority rejections are reported by it.
pub const AUTHORIZATION_DENIED: &str = "authorization_denied";

/// A write a policy rejects.
fn denied(detail: impl std::fmt::Display) -> Error {
    rejected(format!("{AUTHORIZATION_DENIED}: {detail}"))
}

impl Error {
    /// Whether a policy rejected the write.
    pub fn is_authorization_denied(&self) -> bool {
        self.code == ErrorCode::WriteRejected
            && self
                .message
                .strip_prefix(AUTHORIZATION_DENIED)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
    }
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Self, Error> {
        let conn = Connection::open_in_memory().map_err(storage)?;
        Self::open_on(config, conn, None)
    }

    /// A durable store in one SQLite file (WAL unless `options.journal`
    /// says otherwise). Reopening restores rows, history, the commit seq
    /// and, for a replica, its pending uploads and shadow; they are resent
    /// on the next connection.
    pub async fn open_at(
        config: DbConfig,
        path: &std::path::Path,
        options: FileOptions,
    ) -> Result<Self, Error> {
        let flags = rusqlite::OpenFlags::default();
        let conn = match options.vfs {
            None => Connection::open_with_flags(path, flags),
            Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs),
        }
        .map_err(storage)?;
        let synchronous = match (options.journal, options.synchronous_full) {
            (Journal::Memory, _) => "OFF",
            (_, true) => "FULL",
            (_, false) => "NORMAL",
        };
        let journal = match options.journal {
            Journal::Wal => "WAL",
            Journal::Rollback => "TRUNCATE",
            Journal::Memory => "MEMORY",
        };
        let cache_spill = if options.journal == Journal::Wal {
            ""
        } else {
            " PRAGMA cache_spill = OFF;"
        };
        // `journal_mode` answers with a row, so it is queried, not batched.
        let mode: String = conn
            .query_row(&format!("PRAGMA journal_mode = {journal}"), [], |r| {
                r.get(0)
            })
            .map_err(storage)?;
        if !mode.eq_ignore_ascii_case(journal) {
            return Err(Error::new(
                ErrorCode::Storage,
                format!("journal_mode {journal} is not supported on this VFS yet (got {mode})"),
            ));
        }
        conn.execute_batch(&format!(
            "PRAGMA synchronous = {synchronous}; PRAGMA cache_size = -{};{cache_spill}",
            options.cache_kib
        ))
        .map_err(storage)?;
        Self::open_on(config, conn, Some((path, options.vfs)))
    }

    fn open_on(
        config: DbConfig,
        conn: Connection,
        path: Option<(&std::path::Path, Option<&'static str>)>,
    ) -> Result<Self, Error> {
        conn.execute_batch(sync::OWNER_DDL).map_err(storage)?;
        let scope_owner = sync::stored_owner_subject(&conn)?;
        if scope_owner.is_some_and(|subject| subject != config.identity.author) {
            return Err(Error::new(
                ErrorCode::Storage,
                "scope-isolated storage belongs to another subject",
            ));
        }
        if scope_owner.is_some() {
            sync::check_owner_open_schema(&conn, config.schema.version_id())?;
        }
        // A version this file admitted keeps its names; another one is
        // named against the most recently admitted.
        let mut names = crate::names::Names::load(&conn)?;
        let schema = names.resolve(&config.schema, &[], names.latest())?;
        let chunks = crate::large::Chunks::install(path)?;
        // Statements are keyed per table and verb; the default 16 entries
        // thrash as soon as a schema has a few tables.
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        #[cfg(feature = "durable-object-limits")]
        crate::durable_object_limits::apply(&conn);
        #[cfg(feature = "sql-trace")]
        crate::sql_trace::install(&conn);
        conn.execute_batch(crate::large::DDL).map_err(storage)?;
        // Nodes a transaction writes stay cached until it ends: until then
        // the reader connection cannot see them.
        let committed = chunks.clone();
        conn.commit_hook(Some(move || {
            committed.unpin();
            false
        }))
        .map_err(storage)?;
        let rolled_back = chunks.clone();
        conn.rollback_hook(Some(move || rolled_back.unpin()))
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
                policies_from: None,
            };
            physical.union(&schema)?;
            physical
        } else {
            let mut physical = stored_schema(&conn, &config.schema)?;
            let ddl = physical.union(&schema)?;
            execute_script(&conn, &ddl)?;
            physical
        };
        crate::array_index::install(&conn, &schema)?;
        names.admit(&conn, &schema)?;
        conn.execute_batch(sync::SYNC_DDL).map_err(storage)?;
        conn.execute_batch(sync::RELAY_DDL).map_err(storage)?;
        conn.execute_batch(candidates::TEMP_DDL).map_err(storage)?;
        conn.execute_batch(accepted::ACCEPTED_DDL)
            .map_err(storage)?;
        conn.execute_batch(catalogue::DDL).map_err(storage)?;
        conn.execute_batch(catalogue_policy::DDL).map_err(storage)?;
        conn.execute_batch(catalogue_cache::DDL).map_err(storage)?;
        conn.execute_batch(initialization::DDL).map_err(storage)?;
        conn.execute_batch(sync::owner_init::DDL).map_err(storage)?;
        let catalogue_cache_required = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _jazz_catalogue_cache_mode_v1 WHERE slot=1)",
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        // The commit seq resumes after the newest stored version.
        let mut seq = 0u64;
        for table in physical.tables.keys() {
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
        writer_order::install(&conn, seq == 0)?;
        let owner = Rc::new(schema.clone());
        let db = Self {
            view: None,
            inner: Rc::new(RefCell::new(Inner {
                conn,
                catalogue_cache_required,
                catalogue_capture_pending: std::cell::Cell::new(true),
                view_policy_source: None,
                view_policies_dirty: false,
                candidate_checks: RefCell::new(None),
                candidate_read_evidence: None,
                ordinary_checks: RefCell::new(None),
                chunks,
                pending_nodes: Vec::new(),
                derived: Vec::new(),
                staging: LargeValueStagingPolicy::default(),
                upload_cleanup: Rc::default(),
                ingress: RefCell::new((0, 0)),
                schema: owner.clone(),
                owner,
                physical,
                admitted: vec![config.schema.clone()],
                names,
                views: BTreeMap::new(),
                batches: HashMap::new(),
                retired_initializations: BTreeSet::new(),
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
                accepted_notifications_through: 0,
                accepted_notifications_known: 0,
                tx: None,
                publications: Vec::new(),
                client: None,
                non_durable: false,
                scope_isolated_owner: false,
                owner_projection_error: None,
                deferred_owner_durability: false,
                retired: false,
                mutation_admission_closed: false,
                scheduler: None,
                read_deadlines: Vec::new(),
                authority: false,
                sessions: Vec::new(),
                since_tick: Vec::new(),
                capture: Vec::new(),
                gone: RefCell::new(Vec::new()),
            })),
        };
        {
            let mut inner = db.inner.borrow_mut();
            inner.seq = seq;
            inner.accepted_notifications_through = inner.accepted_observed()?;
            inner.restore_issued_seq()?;
            inner.load_client()?;
            inner.accepted_notifications_known = inner
                .client
                .as_ref()
                .map_or(inner.seq, |client| client.known);
            if inner.client.is_some() {
                inner.seq = inner.seq.max(LOCAL_BASE);
            }
        }
        if scope_owner.is_some() {
            db.set_scope_isolated_storage_owner()?;
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
        db.initialize_authority_catalogue()?;
        Ok(db)
    }

    /// Serve as the global authority: subscriptions this store settles,
    /// its own and its subscribers', report `DurabilityTier::Global`. A
    /// store without the role (a relay with no upstream) settles at `Local`.
    pub fn set_authority(&self) {
        let mut inner = self.inner.borrow_mut();
        if inner.scope_isolated_owner {
            inner.owner_projection_error = Some(unsupported(
                "promoting a scope-isolated storage owner to authority",
            ));
            return;
        }
        inner.authority = true;
    }

    /// The shared state, set up to run under this handle's schema.
    fn inner(&self) -> std::cell::RefMut<'_, Inner> {
        let mut inner = self.inner.borrow_mut();
        inner.schema = self
            .view
            .as_ref()
            .map(|view| inner.effective_schema(view))
            .unwrap_or_else(|| inner.owner.clone());
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
        let deferred = self.inner().deferred_owner_durability;
        if !deferred && let Some(barrier) = self.prepare_owner_durability()? {
            self.confirm_owner_durability(barrier)?;
            self.flush_owner_durability()?;
        }
        let mut inner = self.inner();
        inner.fan.collect();
        inner.reclaim_large_values(RECLAIM_NODES_PER_TICK)?;
        drop(inner);
        self.schedule_read_deadlines();
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

    /// Permanently stop admitting new application writes in this runtime and
    /// every retained schema view. Existing uploads, receipts, rollback and
    /// storage cleanup can still advance through `tick` before retirement.
    /// This fence is in memory; a new storage owner starts its own lifecycle.
    pub fn stop_mutation_admission(&self) {
        self.inner().mutation_admission_closed = true;
    }

    /// Revoke the owner connection before its external storage lease is
    /// released. Retained views, streams or transports may still own Db clones;
    /// none may read or write this file after retirement. Installing SQLite's
    /// authorizer also expires statements already in the preparation cache.
    /// This is a one-way runtime fence, not a stored application policy.
    pub fn retire_storage_owner(&self) -> Result<(), Error> {
        let mut inner = self.inner();
        inner
            .conn
            .authorizer(Some(|_: rusqlite::hooks::AuthContext<'_>| {
                rusqlite::hooks::Authorization::Deny
            }))
            .map_err(storage)?;
        inner.retired = true;
        Ok(())
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
        inner.require_catalogue_schema(&inner.schema)?;
        let normalized = normalize_relation(query, &inner.schema)?;
        let query = normalized.as_ref().unwrap_or(query);
        // Validate: tables, columns, parameters, supported forms. Read
        // options are not known yet: validate the widest read (deleted rows
        // included, so their metadata columns are accepted).
        let session = Session::system();
        Ctx::new(&inner.schema, None, &session, &binding)
            .with_catalogue(&*inner)
            .compile_query(query, true)?;
        let shape =
            blake3::hash(format!("{query:?}{:?}", inner.schema.jazz.version_id()).as_bytes());
        let bound = blake3::hash(format!("{binding:?}").as_bytes());
        Ok(PreparedQuery {
            query: query.clone(),
            binding,
            shape: Shape(short(shape)),
            binding_id: Binding(short(bound)),
            request_claims: None,
            authority_policies: false,
            schema: self.view.clone(),
        })
    }

    // ---- reads ----------------------------------------------------------

    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        let inner = self.inner();
        inner.run(prepared, None, &ReadSpec::of(&opts, &inner)?)
    }

    /// Root rows with each array relation nested in a cell under its
    /// column name ([`ResultNode::into_nested_row`]).
    pub fn nest_relations(
        &self,
        prepared: &PreparedQuery,
        roots: Vec<CurrentRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.inner().nest_relations(prepared, None, roots)
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
        let roots = inner.run(prepared, None, &ReadSpec::of(&opts, &inner)?)?;
        let mut tree = Vec::with_capacity(roots.len());
        for row in roots {
            tree.push(inner.node(
                prepared,
                None,
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
        let mut read = ReadSpec::of(&opts, &self.inner())?;
        let client = self.is_client();
        // A frozen read is answered from the replica alone.
        let remote_read = client && opts.tier != ReadTier::LocalOnly && !read.frozen();
        // A synced subscription follows the server's results: a row the
        // server stopped listing leaves it, though the copy stays readable.
        read.members_only |= remote_read;
        if remote_read {
            Self::remote_shape(&prepared.query)?;
        }
        let remote = if remote_read {
            self.attach_remote(prepared, read.include_deleted)
        } else {
            None
        };
        let settled = match (&remote, client) {
            (Some((answered, _, global)), _) => {
                sync::SettledBy::Remote(answered.clone(), global.clone())
            }
            (None, true) => sync::SettledBy::Never,
            (None, false) => sync::SettledBy::Always(self.inner().authority),
        };
        let foreground = self.inner().non_durable;
        let gate = match (&remote, self.link_hint()) {
            (Some((answered, _, global)), Some(hint)) => {
                let remote_wait = match &opts.first_load {
                    FirstLoad::WaitForRemote { timeout_ms } => *timeout_ms,
                    _ => 0,
                };
                let strict = opts.tier == ReadTier::Remote;
                let wait_for_remote = strict || (remote_wait > 0 && sync::may_wait(hint.get()));
                if strict || foreground || wait_for_remote {
                    let deadline = (!strict && wait_for_remote).then(|| {
                        web_time::Instant::now() + std::time::Duration::from_millis(remote_wait)
                    });
                    Some(sync::Gate {
                        answered: answered.clone(),
                        global: global.clone(),
                        live: hint.get().hint == RemoteLinkHint::Live,
                        hint,
                        deadline,
                        wait_for_owner: foreground,
                        require_global: strict || (foreground && wait_for_remote),
                        _wake: self.read_deadline(deadline),
                        _attempt_wake: self.read_attempt_deadline(),
                    })
                } else {
                    None
                }
            }
            _ => None,
        };
        let include_deleted = read.include_deleted;
        let mut inner = self.inner();
        let (queue, group) = match inner.subscribe_member(prepared, identity, read, settled) {
            Ok(member) => member,
            Err(error) => {
                if remote.is_some() {
                    inner.release_remote(&prepared.query, &prepared.binding, include_deleted);
                }
                return Err(error);
            }
        };
        let stream = SubscriptionStream {
            queue,
            dropped: inner.fan.dropped.clone(),
            gate,
            strict_remote: opts.tier == ReadTier::Remote && remote.is_some(),
            gate_opened: false,
            delivered: None,
            group,
            refused: remote.as_ref().map(|(_, refused, _)| refused.clone()),
            remote: remote.map(|_| {
                (
                    self.inner.clone(),
                    prepared.query.clone(),
                    prepared.binding.clone(),
                    include_deleted,
                )
            }),
        };
        drop(inner);
        self.schedule_pending_tick();
        Ok(stream)
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
            .query_row(&sql, [row.0.as_bytes().to_vec()], |r| {
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

    /// SQLite's actual row-change count on this connection, including triggers.
    /// Reading this diagnostic does not run cleanup or alter stored state.
    #[cfg(any(test, feature = "testing"))]
    pub fn sqlite_total_changes_for_test(&self) -> u64 {
        self.inner.borrow().conn.total_changes()
    }

    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.inner().fan.live_members().len()
    }

    pub fn fanout_stats(&self) -> fanout::FanoutStats {
        self.inner().fan.stats()
    }

    /// Deliver the rows written since the last dispatch to the groups they
    /// wake (keyed invalidation; see `fanout`).
    fn notify(&self) -> Result<(), Error> {
        let mut inner = self.inner();
        let accepted = inner.accepted_notifications()?;
        let changes = std::mem::take(&mut *inner.changed.borrow_mut());
        if !inner.sessions.is_empty() {
            inner.since_tick.extend(changes.iter().cloned());
        }
        inner.dispatch(changes);
        inner.notify_accepted_evidence(accepted);
        Ok(())
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
    fn write_op(&self, mut op: StagedOp, who: Who) -> Result<WriteHandle, Error> {
        self.inner().require_mutation_admission()?;
        if matches!(op.identity(), Some(WriteIdentity::Attribution(_))) {
            self.inner().require_attribution_backend()?;
        }
        self.with_upload(|| {
            let table = op.table().to_owned();
            let before = self.inner().seq;
            let handle = self.write(&table, |inner, seq| {
                let edits = inner.chunk_large_cells(&mut op)?;
                inner.counter_deltas(&mut op)?;
                inner.pending_nodes.extend(edits.pending_nodes);
                let unit = [(inner.schema.clone(), op.clone())];
                let applied = inner.candidate_unit(&unit, seq, false, false, |inner| {
                    inner.apply_as(&op, who, seq)
                });
                inner.clear_derived();
                applied
            })?;
            // A no-op returns the row's existing sequence. Uploading it as a
            // new unit would reuse that transaction identity with different
            // bytes, replacing a still-pending write's journal entry. Inside
            // an open transaction, capture still belongs to that one unit.
            if self.inner().tx.is_some() || handle.seq > before {
                self.record_upload(vec![op], handle.seq, None, sync::Kind::Mergeable)?;
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
        if result.is_ok() {
            self.inner
                .borrow()
                .conn
                .execute_batch("RELEASE upload_local")
                .map_err(storage)?;
        } else {
            self.inner
                .borrow()
                .rollback_savepoint("upload_local")
                .map_err(storage)?;
        }
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
            inner.require_initialization_allocator()?;
            let stored_table = inner.schema.stored_table(table)?.to_owned();
            let (seq, standalone) = match &inner.tx {
                Some((seq, _)) => (*seq, false),
                None => (inner.seq + 1, true),
            };
            inner.conn.execute_batch("SAVEPOINT w").map_err(storage)?;
            let mark = inner.changed.borrow().len();
            let applied = apply(&mut inner, seq).and_then(|handle| {
                if standalone && handle.seq == seq {
                    inner.record_local_commit(seq, None, inner.identity.author)?;
                    inner.record_local_writer_order(seq, mark)?;
                    inner.record_issued_seq(seq)?;
                }
                Ok(handle)
            });
            match applied {
                Ok(handle) => {
                    inner.conn.execute_batch("RELEASE w").map_err(storage)?;
                    if handle.seq == seq {
                        if standalone {
                            inner.seq = seq;
                        } else if let Some((_, tables)) = &mut inner.tx {
                            tables.insert(stored_table);
                        }
                    }
                    (standalone && handle.seq == seq, handle)
                }
                Err(error) => {
                    inner.rollback_savepoint("w").map_err(storage)?;
                    inner.changed.borrow_mut().truncate(mark);
                    return Err(error);
                }
            }
        };
        let (notify, handle) = standalone;
        if notify {
            self.notify()?;
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
            inner.require_mutation_admission()?;
            inner.require_foreground_writes("commits")?;
            inner.require_initialization_allocator()?;
            inner.require_owner_subject(author)?;
            if inner.tx.is_some() {
                return Err(unsupported("nested transactions"));
            }
            inner.conn.execute_batch("SAVEPOINT tx").map_err(storage)?;
            inner.changed.borrow_mut().clear();
            let seq = inner.seq + 1;
            inner.tx = Some((seq, BTreeSet::new()));
            inner.begin_ordinary_callback();
        }
        let mut guard = CallbackTransactionGuard {
            db: self,
            active: true,
        };
        let mut tx = MergeableTx {
            db: self,
            _author: author,
        };
        let result = callback(&mut tx).await.and_then(|value| {
            self.inner().require_mutation_admission()?;
            Ok(value)
        });
        let captured;
        let changed = {
            let mut inner = self.inner();
            let (seq, tables) = inner.tx.take().expect("open transaction");
            captured = std::mem::take(&mut inner.capture);
            let checks = inner.ordinary_checks.borrow_mut().take();
            match &result {
                Ok(_) => {
                    if let Err(error) = inner
                        .finish_ordinary_callback(seq, &tables, checks)
                        .and_then(|()| inner.record_local_commit(seq, None, inner.identity.author))
                        .and_then(|()| inner.record_callback_writer_order(seq, &tables))
                        .and_then(|()| inner.record_issued_seq(seq))
                    {
                        inner.rollback_savepoint("tx").map_err(storage)?;
                        guard.active = false;
                        inner.changed.borrow_mut().clear();
                        return Err(error);
                    }
                    inner.seq = seq;
                    Some((seq, tables))
                }
                Err(_) => {
                    inner.rollback_savepoint("tx").map_err(storage)?;
                    guard.active = false;
                    inner.changed.borrow_mut().clear();
                    None
                }
            }
        };
        let value = result?;
        let (seq, _) = changed.expect("committed");
        // The upload record joins the transaction's SQLite savepoint.
        let identity = captured
            .is_empty()
            .then_some(if author == AuthorSubject::SYSTEM {
                WriteIdentity::Database
            } else {
                WriteIdentity::Session(author)
            });
        self.record_upload(captured, seq, identity, sync::Kind::Mergeable)?;
        self.inner
            .borrow()
            .conn
            .execute_batch("RELEASE tx")
            .map_err(storage)?;
        guard.active = false;
        self.notify()?;
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
        self.activate(publication.version.schema, &[publication.lens])
    }

    /// Make `schema` the current schema. The stored tables grow to hold it:
    /// new tables, new columns (`ADD COLUMN ... DEFAULT`, no rows rewritten)
    /// and new indexes. Columns other schemas added stay, so older clients
    /// keep their data. Changing a column's type is not supported yet, and a
    /// new required column needs a default.
    pub async fn activate_schema(&self, schema: JazzSchema) -> Result<(), Error> {
        self.activate(schema, &[])
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
        self.activate(schema, lenses)
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
        self.admit(schema, lenses, false)
    }

    fn activate(&self, schema: JazzSchema, lenses: &[MigrationLens]) -> Result<(), Error> {
        self.admit(schema, lenses, true)
    }

    fn admit(
        &self,
        schema: JazzSchema,
        lenses: &[MigrationLens],
        make_current: bool,
    ) -> Result<(), Error> {
        {
            let mut inner = self.inner();
            let version = schema.version_id();
            inner.require_owner_schema_change(version)?;
            let mut defaults = BTreeMap::new();
            for lens in lenses.iter().filter(|lens| lens.target == version) {
                defaults.extend(LensEffects::of(lens)?.defaults);
            }
            let base = inner.owner.names();
            let mut target = inner.names.resolve(&schema, lenses, Some(&base))?;
            let admitted: Vec<Schema> = inner
                .admitted
                .iter()
                .map(|admitted| inner.named(admitted))
                .collect();
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
                            "changing the type of {}.{} on SQLite",
                            table.name, column.name
                        )));
                    }
                    if stored.is_some_and(|stored| stored.column(&column.stored).is_some()) {
                        continue;
                    }
                    if let Some(default) = defaults.get(&(table.name.clone(), column.name.clone()))
                    {
                        column.default = Some(default.clone());
                    }
                    if stored.is_some() && column.default.is_none() && !column.nullable() {
                        return Err(unsupported(format!(
                            "adding required column {}.{} without a default",
                            table.name, column.name
                        )));
                    }
                }
            }
            let ddl = inner.physical.union(&target)?;
            execute_script(&inner.conn, &ddl)?;
            crate::array_index::install(&inner.conn, &target)?;
            let Inner { names, conn, .. } = &mut *inner;
            names.admit(conn, &target)?;
            if make_current {
                if inner.scope_isolated_owner {
                    inner
                        .conn
                        .execute(
                            "UPDATE _jazz_owner_scope_v1 SET schema_id=?1 WHERE singleton=1",
                            [version.as_bytes().as_slice()],
                        )
                        .map_err(storage)?;
                }
                inner.owner = Rc::new(target);
            }
            if !inner
                .admitted
                .iter()
                .any(|admitted| admitted.version_id() == version)
            {
                inner.admitted.push(schema);
            }
        }
        let mut inner = self.inner();
        inner.reset_everything();
        for session in &mut inner.sessions {
            session.revalidate();
        }
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

// A callback may suspend at any await. Dropping its future must retire both
// SQLite state and the deferred proof collector before another unit starts.
struct CallbackTransactionGuard<'a> {
    db: &'a Db,
    active: bool,
}
impl Drop for CallbackTransactionGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut inner = self.db.inner.borrow_mut();
        if inner.rollback_savepoint("tx").is_err() {
            // Failed rollback cannot leave this handle open for further writes.
            inner.mutation_admission_closed = true;
        }
        inner.tx = None;
        inner.ordinary_checks.borrow_mut().take();
        inner.capture.clear();
        inner.changed.borrow_mut().clear();
        inner.clear_derived();
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
    /// A retained handle pins names, not an authority's old permission grant.
    /// Plain local views retain their exact policy contract.
    fn effective_schema(&self, schema: &Rc<Schema>) -> Rc<Schema> {
        let Some(previous) = &schema.policies_from else {
            return schema.clone();
        };
        let current = self.view_policy_source.as_ref().unwrap_or(&self.owner);
        if Rc::ptr_eq(previous, current) {
            schema.clone()
        } else {
            Rc::new((**schema).clone().with_policies_of(current.clone()))
        }
    }

    /// Keep the owner's compatible physical defaults and indexes when adopting
    /// the authority's policy semantics for the same structural schema.
    fn named_policy_schema(&self, policies: &JazzSchema) -> Result<Schema, Error> {
        if policies.version_id() == self.owner.jazz.version_id() {
            Ok(self.named(&self.owner.jazz.with_permissions_from(policies)))
        } else if self.names.get(&policies.version_id()).is_some() {
            Ok(self.named(policies))
        } else {
            self.named_catalogue_policy_schema(policies)
        }
    }

    /// An admitted schema under the names this store keeps it by.
    fn named(&self, schema: &JazzSchema) -> Schema {
        match self.names.get(&schema.version_id()) {
            Some(names) => Schema::stored_as(schema, names),
            None => Schema::new(schema),
        }
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

    /// Retained authenticated views already refresh their policy governor in
    /// effective_schema. Only a bootstrap query needs to acquire it here.
    fn needs_authority_policies(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
    ) -> Result<bool, Error> {
        if identity.is_none_or(|identity| identity == AuthorSubject::SYSTEM) {
            return Ok(false);
        }
        let schema = prepared.schema.as_ref().unwrap_or(&self.owner);
        if schema.policies_from.is_some() && self.view_policy_source.is_some() {
            return Ok(false);
        }
        Ok(self.policy_descriptor()?.is_some())
    }

    fn compile(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Compiled, Error> {
        self.require_owner_subject(identity.unwrap_or(self.identity.author))?;
        let (policy, claims) = self.read_sessions(prepared, identity);
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let schema = if read.scoped_evidence || prepared.authority_policies {
            let descriptor = self.policy_descriptor()?.ok_or_else(|| {
                Error::new(
                    ErrorCode::NotObserved,
                    "scoped permission policy descriptor is not resident",
                )
            })?;
            Rc::new((*schema).clone().with_policies_of(Rc::new(
                self.named_policy_schema(&descriptor.schema.schema)?,
            )))
        } else {
            schema
        };
        self.require_catalogue_schema(&schema)?;
        let compiled = Ctx::new(&schema, policy.as_ref(), &claims, &prepared.binding)
            .with_owner_previews(
                self.owner_previews(identity)
                    && !read.scoped_evidence
                    && !prepared.authority_policies
                    && read.permission_cut.is_none()
                    && self.candidate_read_evidence.is_none()
                    && history_source_table(&prepared.query.table).is_none(),
            )
            .with_scoped_evidence(read.scoped_evidence)
            .with_candidate_evidence(self.candidate_read_evidence)
            .with_permission_cut(read.permission_cut, read.permission_own)
            .with_catalogue(self)
            .with_view(read.view.clone())
            .with_cut(read.cut.clone())
            .with_history_origin(self.history_origin())
            .with_members_only(read.members_only)
            .compile_query(&prepared.query, read.include_deleted)?;
        if self.candidate_read_evidence.is_some() && !compiled.preps.is_empty() {
            return Err(unsupported(
                "provisional reads requiring large-value preprocessing in candidate authorization units",
            ));
        }
        Ok(compiled)
    }

    /// The node history reads name this store's own commits by: set on the
    /// authority only (see `compile::Ctx::history_origin`).
    pub(crate) fn history_origin(&self) -> Option<[u8; 16]> {
        self.client
            .is_none()
            .then(|| *self.identity.node.as_bytes())
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
        self.with_preps(&compiled.preps, |ties| {
            let sql = match &compiled.ranked_sql {
                Some(ranked) if ties => ranked,
                _ => &compiled.sql,
            };
            self.execute_sql(sql, compiled, table)
        })
    }

    /// Answer `preps` into `_jazz_lv_scratch`, run `body` (told whether a
    /// rank prep found chunked values sharing a sort key), then clear the
    /// answers. Statements that need no prep run directly.
    pub(crate) fn with_preps<T>(
        &self,
        preps: &[crate::compile::Prep],
        body: impl FnOnce(bool) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if preps.is_empty() {
            return body(false);
        }
        self.conn
            .execute_batch("SAVEPOINT jazz_scratch")
            .map_err(storage)?;
        let result = (|| {
            let mut ties = false;
            for prep in preps {
                ties |= crate::large::answer(&self.conn, &self.chunks, prep)?;
            }
            body(ties)
        })();
        let mut clear = self
            .conn
            .prepare_cached("DELETE FROM _jazz_lv_scratch WHERE slot = ?1")
            .map_err(storage)?;
        for prep in preps {
            clear.execute([prep.slot()]).map_err(storage)?;
        }
        self.conn
            .execute_batch("RELEASE jazz_scratch")
            .map_err(storage)?;
        result
    }

    fn execute_sql(
        &self,
        sql: &str,
        compiled: &Compiled,
        table: &str,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut statement = self.conn.prepare_cached(sql).map_err(storage)?;
        let names: Arc<str> = Arc::from(table);
        let types: Arc<[ValueType]> = compiled
            .projection
            .iter()
            .map(|(_, ty)| ty.clone())
            .collect();
        let mut rows = statement
            .query(params_from_iter(compiled.params.iter()))
            .map_err(storage)?;
        let mut out = Vec::new();
        // A replica missing chunks reads on past the first blocked cell, so
        // one round trip asks for every row's missing nodes.
        let mut blocked = false;
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
            let root_version = if flat { version(3)? } else { None };
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
            let first = if flat { 4 + 2 * compiled.joined } else { 3 };
            let cells = compiled
                .projection
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| {
                    let cell = row.get_ref(first + i).map_err(storage)?;
                    match from_sql(ty, cell, &self.chunks) {
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
                seq: seq as u64,
                deleted: deleted != 0,
                cells: Arc::new(cells),
                types: types.clone(),
                joined,
                version: root_version,
                union_arm: if compiled.union_arms.is_empty() {
                    None
                } else {
                    let arm: u32 = row
                        .get(first + compiled.projection.len())
                        .map_err(storage)?;
                    Some(Arc::from(
                        compiled
                            .union_arms
                            .get(arm as usize)
                            .ok_or_else(|| {
                                Error::new(ErrorCode::Storage, "invalid union occurrence")
                            })?
                            .as_str(),
                    ))
                },
            });
        }
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
                    &prepared.query.table,
                    &prepared.query.array_subqueries,
                    row,
                )?
                .into_nested_row()
            })
            .collect()
    }

    /// Root rows and relation children in the read set's stable traversal order.
    fn read_rows(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        read: &ReadSpec,
    ) -> Result<Vec<CurrentRow>, Error> {
        let roots = self.run(prepared, identity, read)?;
        let mut out = roots.clone();
        if !prepared.query.array_subqueries.is_empty() {
            for row in roots {
                let node = self.node(
                    prepared,
                    identity,
                    &prepared.query.table,
                    &prepared.query.array_subqueries,
                    row,
                )?;
                child_read_rows(&node, &mut out);
            }
        }
        Ok(out)
    }

    fn node(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
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
            self.require_owner_subject(identity.unwrap_or(self.identity.author))?;
            let (policy, claims) = self.read_sessions(prepared, identity);
            let compiled = Ctx::new(&self.schema, policy.as_ref(), &claims, &prepared.binding)
                .with_owner_previews(self.owner_previews(identity))
                .with_catalogue(self)
                .compile_children(array, &parent)?;
            let children = self.execute(&compiled, &array.table)?;
            let mut nodes = Vec::with_capacity(children.len());
            for child in children {
                nodes.push(self.node(
                    prepared,
                    identity,
                    &array.table,
                    &array.nested_arrays,
                    child,
                )?);
            }
            relations.insert(array.column_name.clone(), ResultRelation::Array(nodes));
        }
        Ok(ResultNode { row, relations })
    }

    fn cell(&self, table: &str, row: RowUuid, column: &str) -> Result<Value, Error> {
        let table = self.schema.table(table)?;
        let column = table
            .column(column)
            .ok_or_else(|| unsupported("unknown correlation column"))?;
        let ty = column.ty.clone();
        let sql = format!(
            "SELECT {} FROM {} WHERE id = ?1",
            quote(&column.stored),
            quote(&table.stored)
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

    /// [`Inner::existing`] as of `cut` (the current row when `None`): the
    /// newest version the cut sees, current or superseded.
    fn existing_at(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        cut: Option<&Cut>,
    ) -> Result<Option<(u64, bool)>, Error> {
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
            "SELECT _del FROM {} WHERE id = ?1 AND _b = ?2 AND _seq = ?3",
            quote(&format!("{table}__h"))
        );
        let deleted: i64 = self
            .conn
            .query_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key, version as i64],
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
    ) -> Result<Option<u64>, Error> {
        let extra = cut
            .extra
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT MAX(_seq) FROM {} WHERE id = ?1 AND _b = ?2 AND (_seq <= ?3 OR _seq IN ({extra}))",
            quote(&format!("{table}__h"))
        );
        let version: Option<i64> = self
            .conn
            .query_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key, cut.seq as i64],
                |r| r.get(0),
            )
            .map_err(storage)?;
        Ok(version.map(|v| v as u64))
    }

    /// Resolve which stored row a patch-style write addresses.
    fn locate(&self, table: &Table, row: RowUuid, target: &Target) -> Result<Located, Error> {
        let at = |key: Vec<u8>, cells: BTreeMap<String, Value>| -> Result<Located, Error> {
            Ok(match self.existing(&table.stored, row, &key)? {
                Some((seq, deleted)) => Located::Row { key, seq, deleted },
                None => Located::Absent { cells },
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
        let binding = BTreeMap::new();
        let claims = Session::default();
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding)
            .with_catalogue(self)
            .with_cut(cut)
            .with_candidate_evidence(self.candidate_read_evidence);
        let condition = policy(&mut ctx)?;
        let preps = std::mem::take(&mut ctx.preps);
        let mut params = ctx.params;
        params.push(Sql::Blob(row.0.as_bytes().to_vec()));
        params.push(Sql::Blob(key.to_vec()));
        let sql = format!(
            "SELECT COALESCE({condition}, 0) FROM {} AS a WHERE a.id = ?{} AND a._b = ?{}",
            quote(&table.stored),
            params.len() - 1,
            params.len()
        );
        let value: i64 = self.with_preps(&preps, |_| {
            self.conn
                .query_row(&sql, params_from_iter(params.iter()), |r| r.get(0))
                .map_err(storage)
        })?;
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
        let mut ctx = Ctx::new(&self.schema, None, &claims, &binding)
            .with_owner_previews(self.owner_previews(author))
            .with_catalogue(self);
        let mut condition = match author {
            Some(author) => {
                let session = self.session(author);
                ctx.read_policy(table, "a", &session)?
            }
            None => "1".to_owned(),
        };
        let preps = std::mem::take(&mut ctx.preps);
        let mut base = ctx.params;
        if self.client.is_some() {
            // A reconnect's Hold must respect upstream revocations even when
            // the retained image still passes this relay's local policy.
            base.push(Sql::Text(table.stored.as_bytes().to_vec()));
            condition = format!(
                "({condition}) AND NOT EXISTS (SELECT 1 FROM _jazz_revoked AS r \
                 WHERE r.t = ?{} AND r.id = a.id AND r.k = a._b)",
                base.len()
            );
        }
        self.with_preps(&preps, |_| self.versions_in(table, &condition, &base, ids))
    }

    fn versions_in(
        &self,
        table: &Table,
        condition: &str,
        base: &[Sql],
        ids: &[[u8; 16]],
    ) -> Result<Vec<([u8; 16], Vec<u8>, u64, bool, bool)>, Error> {
        let mut out = Vec::new();
        for chunk in ids.chunks(500) {
            let mut params = base.to_vec();
            params.push(crate::compile::json_ids(
                chunk.iter().map(|id| id.as_slice()),
            ));
            let sql = format!(
                "SELECT a.id, a._b, a._seq, a._del, COALESCE({condition}, 0) FROM {} AS a \
                 WHERE a.id IN (SELECT unhex(value) FROM json_each(?{}))",
                quote(&table.stored),
                params.len()
            );
            let mut statement = self.conn.prepare_cached(&sql).map_err(storage)?;
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
        denial: &str,
    ) -> Result<bool, Error> {
        if self.defer_ordinary_operation(table, row, key, author, operation, denial)?
            || self.defer_candidate_check(table, row, key, author, table.policy(operation))?
        {
            return Ok(true);
        }
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
        denial: &str,
    ) -> Result<bool, Error> {
        if self.defer_ordinary_check(table, row, key, author, clause, false, denial)?
            || self.defer_candidate_check(table, row, key, author, clause)?
        {
            return Ok(true);
        }
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

    /// A rolled-back savepoint may remove interned authors and allow their
    /// integer codes to be reused. Neither direction of the cache may outlive
    /// that rollback, including speculative reads, advice and failed sync.
    fn rollback_savepoint(&self, name: &str) -> rusqlite::Result<()> {
        *self.authors.borrow_mut() = AuthorCodes::default();
        let name = quote(name);
        self.conn
            .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))
    }

    /// Who this database's own writes are attributed to: its author, or for
    /// the internal authority, the system at this node (as Jazz attributes
    /// system writes to the node that made them).
    pub(crate) fn stamp_author(&self) -> AuthorSubject {
        match self.identity.author {
            AuthorSubject::SYSTEM => AuthorSubject::system_at(self.identity.node),
            author => author,
        }
    }

    /// The interned code of an author's account and principal. Rows store
    /// this small integer instead of repeating them in every version;
    /// codes are local to this database and resolved back at egress.
    pub(crate) fn author_code(&self, key: &AuthorKey) -> Result<i64, Error> {
        if let Some(code) = self.authors.borrow().codes.get(key) {
            return Ok(*code);
        }
        let (account, issuer, subject) = key;
        let account = account.as_bytes().to_vec();
        let find = |conn: &rusqlite::Connection| {
            conn.query_row(
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

    /// Cache an author's code, but only once it is committed: one interned
    /// inside a savepoint (a transaction overlay, a batch that may fail) is
    /// undone by its rollback, and its code may then go to another author.
    fn remember_author(&self, key: &AuthorKey, code: i64) {
        if self.conn.is_autocommit() {
            let mut authors = self.authors.borrow_mut();
            authors.codes.insert(key.clone(), code);
            authors.parts.insert(code, key.clone());
        }
    }

    /// The account and principal an author code stands for.
    pub(crate) fn author_parts(&self, code: i64) -> Result<AuthorKey, Error> {
        if let Some(parts) = self.authors.borrow().parts.get(&code) {
            return Ok(parts.clone());
        }
        let (account, issuer, subject): (Vec<u8>, String, String) = self
            .conn
            .query_row(
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
            if table.column(name).is_none() {
                return Err(rejected(format!("unknown column `{}.{name}`", table.name)));
            }
        }
        Ok(())
    }

    /// Delete up to `budget` large-value nodes no cell or branch references.
    fn reclaim_large_values(&mut self, budget: usize) -> Result<usize, Error> {
        self.release_dropped_uploads()?;
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
                let _ = self.rollback_savepoint("reclaim");
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
        crate::large::store(&self.conn, &self.chunks, &prepared, self.client.is_some())?;
        let prefix = crate::large::sort_prefix(crate::large::kind(column), logical);
        Ok(Sql::Text(crate::large::chunked_cell(
            &prepared.value_ref,
            &prefix,
        )?))
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
            crate::large::store(&self.conn, &self.chunks, prepared, self.client.is_some())?;
        } else if self.derived.contains(large) {
            // Derived here from a stored tree by an append or splice.
        } else if self.client.is_none() {
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
        self.large_cell(large)
    }

    /// The cell of a chunked value this store holds the chunks for. A value
    /// an edit shrank to the inline threshold is stored inline, so each
    /// logical value has exactly one cell and equality compares cells.
    pub(crate) fn large_cell(
        &self,
        large: &jazz_types::large_values::LargeValueRef,
    ) -> Result<Sql, Error> {
        use jazz_types::large_values::LargeValueKind;
        if large.byte_length <= jazz_types::large_values::INLINE_VALUE_MAX_BYTES as u64 {
            let bytes = self.chunks.value_bytes(large)?;
            return Ok(Sql::Text(match large.kind {
                LargeValueKind::Bytes => crate::large::inline_bytes(&bytes),
                LargeValueKind::String | LargeValueKind::Json => bytes,
            }));
        }
        let prefix = self.chunks.prefix(large)?;
        Ok(Sql::Text(crate::large::chunked_cell(large, &prefix)?))
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

    /// Keep the superseded image (linear history: history holds superseded
    /// images only).
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
            .query_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        match cell {
            Some(cell) => from_sql(&column.ty, cell.as_ref(), &self.chunks),
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
            | StagedOp::Upsert(t, _, c, _) => (t.clone(), c),
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
            StagedOp::Upsert(t, r, c, o) => (t.clone(), *r, c, &o.target),
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
    /// counter changes added, g-sets unioned.
    fn merged_cells(
        &self,
        table: &Table,
        row: RowUuid,
        held: Option<&[u8]>,
        mut cells: BTreeMap<String, Value>,
    ) -> Result<BTreeMap<String, Value>, Error> {
        for column in &table.columns {
            let Some(value) = cells.get(&column.name) else {
                continue;
            };
            let merged = match column.merge {
                MergeStrategy::Lww => continue,
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

    /// Whether `row` on branch `key` has a superseded committed image.
    fn has_history(&self, table: &Table, row: RowUuid, key: &[u8]) -> Result<bool, Error> {
        let sql = format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE id = ?1 AND _b = ?2)",
            quote(&format!("{}__h", table.stored))
        );
        self.conn
            .query_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), key],
                |r| r.get(0),
            )
            .map_err(storage)
    }

    fn snapshot(&self, table: &Table, row: RowUuid, key: &[u8], seq: u64) -> Result<(), Error> {
        let columns = self.columns_list(table);
        let sql = format!(
            "INSERT INTO {} (id, _b, _seq, _del, _c{columns}) SELECT id, _b, _seq, _del, _c{columns} \
             FROM {} WHERE id = ?1 AND _b = ?2 AND _seq <> ?3",
            quote(&format!("{}__h", table.stored)),
            quote(&table.stored)
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
                            "(SELECT * FROM {} WHERE _seq = {version})",
                            quote(&format!("{}__h", table.stored))
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
        let sql = format!(
            "INSERT INTO {name} (id, _b, _seq, _del, _c{columns}) SELECT id, ?1, ?2, _del, 0{columns} \
             FROM {source} WHERE id = ?3 AND _b = ?4",
            name = quote(&table.stored)
        );
        self.exec(&sql, rusqlite::params![to, seq as i64, id, from])
            .map_err(storage)?;
        self.set_cells(table, row, to, head_cells, seq, None, false)
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
        seq: u64,
        deleted: Option<bool>,
        content: bool,
    ) -> Result<(), Error> {
        // Right-hand sides read the row before this update: `_seq` is still
        // the previous version's.
        let mut sets = vec![
            "_seq = ?1".to_owned(),
            format!(
                "_c = CASE WHEN _seq = ?1 THEN max(_c, {content}) ELSE {content} END",
                content = content as u8
            ),
        ];
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
        let cells = self.merged_cells(table, row, None, cells)?;
        match self.existing(&table.stored, row, &key)? {
            Some((_, true)) => return Err(row_already_deleted(row)),
            Some((_, false)) => return Err(row_already_exists(row)),
            None => {}
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
                    return Err(missing_required(table_name, &column.name));
                }
            };
            names.push(quote(&column.stored));
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
            quote(&table.stored),
            names.join(", "),
            placeholders.join(", ")
        );
        self.exec(&sql, params_from_iter(values.iter()))
            .map_err(storage)?;
        self.changed
            .borrow_mut()
            .push((table.stored.clone(), row, key.clone()));
        if let Some(author) = self.checked(who)
            && !self.check(
                table,
                row,
                &key,
                author,
                Operation::Insert,
                &format!("INSERT policy denied the new row in `{table_name}`"),
            )?
        {
            return Err(denied(format_args!(
                "INSERT policy denied the new row in `{table_name}`"
            )));
        }
        Ok(WriteHandle { row, seq })
    }

    /// Read-for-write: a hidden row is indistinguishable from an absent one,
    /// and omitted cells must not be merged blind.
    /// The session a write's operation policies (USING, WITH CHECK,
    /// INSERT, DELETE) are checked as here: a trusted session's, at the
    /// authority. A replica checks only that the session may read its
    /// target ([`Who::Session`]) and leaves the rest to the authority on
    /// upload: it may lack rows those policies read (hidden from it).
    fn checked(&self, who: Who) -> Option<AuthorSubject> {
        match who {
            Who::Session(author) if self.client.is_none() => Some(author),
            _ => None,
        }
    }

    fn authorize_patch(
        &self,
        table: &Table,
        row: RowUuid,
        key: &[u8],
        who: &Who,
        verb: &str,
    ) -> Result<(), Error> {
        if let Who::Session(author) = *who
            && !self.check(
                table,
                row,
                key,
                author,
                Operation::Select,
                &format!("read policy denied {verb}: the target requires read permission"),
            )?
        {
            return Err(denied(format_args!(
                "read policy denied {verb}: the target requires read permission"
            )));
        }
        if let Some(author) = self.checked(*who) {
            // UPDATE may constrain the old row, the new row, or both. An
            // absent clause is not an additional denial when the other one
            // explicitly grants UPDATE; absence of both still denies it.
            if !table.policies.has_explicit_update_policy() {
                return Err(denied(format_args!("{verb} requires an UPDATE policy")));
            }
            if let Some(using) = table.policies.update.using.as_ref()
                && !self.check_clause(
                    table,
                    row,
                    key,
                    author,
                    Some(using),
                    &format!("{verb} policy (USING) denied the target row"),
                )?
            {
                return Err(denied(format_args!(
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
            return Err(schema_error(BRANCH_MOVE));
        }
        let key = match located {
            Located::Absent { cells: branch, .. } if upsert => {
                let mut cells = cells;
                cells.extend(branch);
                return self.insert(table_name, cells, Some(row), &Target::Root, who, seq);
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
            // yet, so an upsert replaces it.
            Located::Row {
                deleted: true,
                seq: current,
                ..
            } if !upsert || current != seq => return Err(row_already_deleted(row)),
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
                let cells = self.merged_cells(table, row, (!deleted).then_some(&key[..]), cells)?;
                self.snapshot(table, row, &key, seq)?;
                // An upsert over a tombstone replaces it.
                self.set_cells(
                    table,
                    row,
                    &key,
                    &cells,
                    seq,
                    deleted.then_some(false),
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
                let cells = self.merged_cells(table, row, Some(&from), cells)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, at.as_ref(), seq)?;
                self.set_cells(table, row, &to, &cells, seq, None, true)?;
                to
            }
            Located::Absent { .. } => unreachable!("absent upserts insert"),
        };
        // The new row is checked against the other rows as they were
        // before this transaction, as a policy's subqueries see the
        // statement's snapshot: a same-table EXISTS compares the new row
        // with the old one, not with itself.
        if let Some(author) = self.checked(who)
            && let Some(with_check) = table.policies.update.with_check.as_ref()
            && !self.defer_ordinary_check(
                table,
                row,
                &key,
                author,
                Some(with_check),
                true,
                &format!("{verb} policy (WITH CHECK) denied the new row"),
            )?
            && !self.defer_candidate_check(table, row, &key, author, Some(with_check))?
            && !{
                let session = self.session(author);
                let before = Some(Cut::at(seq.saturating_sub(1)));
                self.holds_at(table, row, &key, before, |ctx| {
                    ctx.policy_clause(Some(with_check), table, "a", &session)
                })?
            }
        {
            return Err(denied(format_args!(
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
        let check = |inner: &Self, key: &[u8], fresh: bool| -> Result<(), Error> {
            // Deleting a row this same commit inserted needs no read
            // permission: nothing persisted is revealed.
            if let Who::Session(author) = who
                && !fresh
                && !inner.check(
                    table,
                    row,
                    key,
                    author,
                    Operation::Select,
                    "read policy denied DELETE: the target requires read permission",
                )?
            {
                return Err(denied(
                    "read policy denied DELETE: the target requires read permission",
                ));
            }
            if let Some(author) = inner.checked(who)
                && !inner.check(
                    table,
                    row,
                    key,
                    author,
                    Operation::Delete,
                    "DELETE policy denied the target row",
                )?
            {
                return Err(denied("DELETE policy denied the target row"));
            }
            Ok(())
        };
        match self.locate(table, row, target)? {
            Located::Row {
                key,
                deleted: false,
                seq: current,
            } => {
                let fresh = current == seq && !self.has_history(table, row, &key)?;
                check(self, &key, fresh)?;
                self.snapshot(table, row, &key, seq)?;
                self.set_cells(table, row, &key, &BTreeMap::new(), seq, Some(true), false)?;
            }
            // Deleting an inherited row is a head register: a head tombstone
            // that masks the base row.
            Located::Inherited {
                from,
                to,
                head_cells,
                at,
            } => {
                check(self, &from, false)?;
                self.copy_on_write(table, row, &from, &to, &head_cells, at.as_ref(), seq)?;
                self.set_cells(table, row, &to, &BTreeMap::new(), seq, Some(true), false)?;
            }
            // As for a patch: a replica leaves an unknown row to the authority.
            Located::Absent { .. } if self.client.is_some() => {}
            Located::Row { deleted: true, .. } => return Err(row_already_deleted(row)),
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
        let key = match self.locate(table, row, target)? {
            Located::Row {
                key, deleted: true, ..
            } => key,
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
            seq,
            Some(false),
            content.is_some(),
        )?;
        // A restore writes an existing row back: the UPDATE policy decides.
        if let Some(author) = self.checked(who)
            && !self.check(
                table,
                row,
                &key,
                author,
                Operation::Update,
                "RESTORE policy denied the restored row",
            )?
        {
            return Err(denied("RESTORE policy denied the restored row"));
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
        seq: u64,
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
                quote(&column.stored),
                quote(&t.stored)
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
            let authenticated =
                inner.client.is_some() && inner.catalogue_schema(version)?.is_some();
            if authenticated {
                let descriptor = inner.policy_descriptor()?;
                let policy_free = schema == schema.without_permissions();
                let exact = descriptor.as_ref().is_some_and(|descriptor| {
                    descriptor.schema.id == version
                        && same_tables(&descriptor.schema.schema, &schema, |l, r| {
                            l.read_policy == r.read_policy && l.write_policies == r.write_policies
                        })
                });
                if !policy_free && !exact {
                    return Err(schema_error(
                        "schema view policy metadata conflicts with its admitted structural schema",
                    ));
                }
                // This view's permissions come from authenticated metadata,
                // never from the caller. A split head/descriptor denies access.
                if inner.view_policy_source.is_none() {
                    inner.view_policy_source = Some(Rc::new(inner.named_policy_schema(
                        &descriptor.map_or_else(
                            || inner.owner.jazz.without_permissions(),
                            |descriptor| descriptor.schema.schema,
                        ),
                    )?));
                }
            }
            if let Some(admitted) = inner.admitted.iter().find(|a| a.version_id() == version) {
                if !authenticated
                    && !same_tables(admitted, &schema, |l, r| {
                        l.read_policy == r.read_policy && l.write_policies == r.write_policies
                    })
                {
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
                inner.admitted.push(schema.clone());
            }
            let base = inner.owner.names();
            let logical = inner.names.resolve(&schema, &[], Some(&base))?;
            let ddl = inner.physical.union(&logical)?;
            execute_script(&inner.conn, &ddl)?;
            crate::array_index::install(&inner.conn, &logical)?;
            let Inner { names, conn, .. } = &mut *inner;
            names.admit(conn, &logical)?;
            inner
                .views
                .entry(SchemaViewId::for_schema(&schema))
                .or_insert(schema);
            if authenticated {
                logical.with_policies_of(inner.view_policy_source.as_ref().unwrap().clone())
            } else {
                logical
            }
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
    identity: WriteIdentity,
    ops: Vec<(Rc<Schema>, StagedOp)>,
    /// Hold staged ciphertext until the commit (including its upload record)
    /// is durable, or the batch is abandoned. Maintenance cannot expire it.
    uploads: Vec<Rc<large_ops::UploadLease>>,
    /// What the batch's partial large-value updates built: descriptors
    /// derived from stored trees and the nodes consolidations prepared,
    /// trusted and stored whenever the staged writes apply.
    edits: LargeEdits,
    reads_state: Rc<RefCell<accepted::ReadState>>,
    paired_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
}

#[derive(Default)]
pub(crate) struct LargeEdits {
    pub(crate) derived: Vec<jazz_types::large_values::LargeValueRef>,
    pub(crate) pending_nodes: Vec<jazz_types::large_values::PreparedLargeValue>,
}

/// An admitted read's boundary in the batch's append-only staging buffers.
/// Capturing lengths is constant time and retains no copy of unrelated writes.
#[derive(Clone, Copy)]
struct BatchPrefix {
    ops: usize,
    derived: usize,
    pending_nodes: usize,
}

impl Batch {
    fn prefix(&self) -> BatchPrefix {
        BatchPrefix {
            ops: self.ops.len(),
            derived: self.edits.derived.len(),
            pending_nodes: self.edits.pending_nodes.len(),
        }
    }

    fn reader(&self) -> Option<AuthorSubject> {
        match who(self.identity) {
            Who::Session(author) => Some(author),
            Who::Local | Who::Attributed(_) => None,
        }
    }
}

enum BatchKind {
    Mergeable,
    /// Serializable: reads see the store as of `snapshot` plus the batch's
    /// own writes; each read's snapshot result is kept for validation.
    /// On a client the snapshot is pinned by the first read that reaches
    /// the authority (the authority's state then), or stays the replica's
    /// known cut when the first read answers offline.
    Serializable {
        snapshot: u64,
        accepted_begin: Cut,
        reads: Vec<ReadRecord>,
        pinned: bool,
        /// On a client, its own commits still unsettled at begin: the
        /// snapshot includes them (reads see what this client wrote).
        own: Vec<u64>,
    },
}

/// One read of a serializable transaction: what it returned (row ids and
/// versions) at the snapshot, re-checked at commit.
#[derive(Clone)]
struct ScopedReadSnapshot {
    through: u64,
    roots: Vec<[u8; 16]>,
    evidence: Vec<ScopedReadRowV1>,
}

struct ReadRecord {
    prepared: PreparedQuery,
    scoped: Option<ScopedReadSnapshot>,
    include_deleted: bool,
    rows: Vec<(RowUuid, u64)>,
    /// Publication aliases may move, or retain their number while replay changes
    /// a counter. The observed content distinguishes those two cases.
    content: Vec<[u8; 32]>,
}

fn conflict(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::TransactionConflict, message)
}

fn versions(rows: &[CurrentRow]) -> Vec<(RowUuid, u64)> {
    rows.iter().map(|row| (row.id, row.seq)).collect()
}

/// Root rows precede each relation tree's child rows, depth first.
fn child_read_rows(node: &ResultNode, out: &mut Vec<CurrentRow>) {
    for relation in node.relations.values() {
        if let ResultRelation::Array(children) = relation {
            for child in children {
                out.push(child.row.clone());
                child_read_rows(child, out);
            }
        }
    }
}

/// jazz.sqlite.serializable-read-content.v1: BLAKE3 derive-key over the
/// versioned postcard preimage pinned by sqlite_owner_read_content_v1.json.
/// Union rows bind that preimage to their semantic arm under the separate
/// serializable-union-read-content.v1 role.
/// Numeric publication aliases are deliberately excluded; original history
/// identities, result occurrences, deletion and every projected cell remain.
fn read_content(row: &CurrentRow) -> Result<[u8; 32], Error> {
    let version = |version: crate::values::SourceVersion| (version.0, *version.1.as_bytes());
    let joined = row
        .joined
        .iter()
        .map(|joined| (*joined.row.as_bytes(), joined.version.map(version)))
        .collect::<Vec<_>>();
    let bytes = postcard::to_allocvec(&(
        row.table.as_ref(),
        row.id.as_bytes(),
        row.deleted,
        row.cells.as_ref(),
        joined,
        row.version.map(version),
    ))
    .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
    if let Some(arm) = row.union_arm() {
        // A support-row change can move the same unchanged root between arms.
        // Keep v1 plain-row bytes fixed and bind the semantic union occurrence
        // in a separately versioned preimage, admitted by the union wire kind.
        let bytes = postcard::to_allocvec(&(bytes, arm))
            .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
        return Ok(blake3::derive_key(
            "jazz.sqlite.serializable-union-read-content.v1",
            &bytes,
        ));
    }
    Ok(blake3::derive_key(
        "jazz.sqlite.serializable-read-content.v1",
        &bytes,
    ))
}

fn read_contents(rows: &[CurrentRow]) -> Result<Vec<[u8; 32]>, Error> {
    rows.iter().map(read_content).collect()
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

    /// Stamp the write at `at` (Unix ms) instead of the applying clock.
    pub(crate) fn stamp_at(&mut self, at: u64) {
        match self {
            StagedOp::Insert(.., o) => o.updated_at_ms = Some(at),
            StagedOp::Update(.., o) => o.updated_at_ms = Some(at),
            StagedOp::Upsert(.., o) => o.updated_at_ms = Some(at),
            StagedOp::Delete(.., o) => o.updated_at_ms = Some(at),
            StagedOp::Restore(.., o) => o.updated_at_ms = Some(at),
            StagedOp::Move(..) => {}
        }
    }

    /// Attribute the write to `author` (unchecked, as an uploaded write is
    /// re-applied locally).
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
    /// Apply one staged write as client-local staging (the authority checks
    /// policies on upload, as for Jazz's mergeable batches).
    /// Re-apply a staged write unchecked, keeping its provenance.
    fn apply(&mut self, op: &StagedOp, seq: u64) -> Result<WriteHandle, Error> {
        let who = op.author().map_or(Who::Local, Who::Attributed);
        self.apply_as(op, who, seq)
    }

    fn require_mutation_admission(&self) -> Result<(), Error> {
        if self.mutation_admission_closed || self.retired {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        Ok(())
    }

    /// Attributed writes require a SYSTEM backend. On a replica the authority
    /// additionally requires a trusted connection before accepting attribution.
    fn require_attribution_backend(&self) -> Result<(), Error> {
        if self.identity.author != AuthorSubject::SYSTEM {
            return Err(unsupported(
                "backend-attributed writes outside a SYSTEM SQLite backend",
            ));
        }
        Ok(())
    }

    /// Apply one write as `who`: policies apply for a session identity.
    fn apply_as(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        self.require_foreground_writes("writes")?;
        if self.non_durable {
            // Refuse shapes the owner recovery envelope cannot reconstruct,
            // before minting an optimistic write or durable upload.
            self.frozen_coordinates(std::slice::from_ref(op))?;
        }
        self.apply_replayed_as(op, who, seq)
    }

    /// Internal replay of a previously admitted unit. Public staging still
    /// enters apply_as and requires a qualified private-owner connection.
    fn apply_replayed_as(
        &mut self,
        op: &StagedOp,
        who: Who,
        seq: u64,
    ) -> Result<WriteHandle, Error> {
        let author = match who {
            Who::Session(author) | Who::Attributed(author) => author,
            Who::Local => self.stamp_author(),
        };
        self.require_owner_subject(author)?;
        let (last_seq, last_at) = self.last_stamp;
        let at = if seq == last_seq {
            last_at
        } else {
            let now = web_time::SystemTime::now()
                .duration_since(web_time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64);
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

    fn apply_op(&mut self, op: &StagedOp, who: Who, seq: u64) -> Result<WriteHandle, Error> {
        self.require_catalogue_schema(&self.schema)?;
        match op.clone() {
            StagedOp::Insert(t, c, o) => {
                self.insert(&t, c, o.row_id, &Target::exact(&o.target), who, seq)
            }
            StagedOp::Update(t, r, c, o) => {
                let target = Target::of(&o.target, self)?;
                self.update(&t, r, c, &target, who, seq)
            }
            StagedOp::Upsert(t, r, c, o) => {
                let target = Target::of(&o.target, self)?;
                self.upsert(&t, r, c, &target, who, seq)
            }
            StagedOp::Delete(t, r, o) => {
                let target = Target::of(&o.target, self)?;
                self.delete(&t, r, &target, who, seq)
            }
            StagedOp::Restore(t, r, c, o) => {
                self.restore(&t, r, c, &Target::exact(&o.target), who, seq)
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

    /// On a replica, the newest version of `row` the authority settled
    /// (pending local versions sit above [`sync::LOCAL_BASE`]).
    fn settled_seq(&self, table: &str, row: RowUuid) -> Result<Option<u64>, Error> {
        let sql = format!(
            "SELECT MAX(_seq) FROM {} WHERE id = ?1 AND _seq < ?2",
            quote(table)
        );
        self.conn
            .query_row(
                &sql,
                rusqlite::params![row.0.as_bytes().to_vec(), sync::LOCAL_BASE as i64],
                |r| r.get::<_, Option<i64>>(0),
            )
            .map(|seq| seq.map(|seq| seq as u64))
            .map_err(storage)
    }

    /// The newest committed version of `row` in stored table `table` on
    /// any branch.
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
pub type SerializableTxRef<'a> = TxRef<'a>;

impl Db {
    pub async fn begin_mergeable(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.begin(id, BatchKind::Mergeable)
    }

    /// Open a mergeable batch whose reads and writes use an authenticated
    /// session. Replica commits are checked by the authority under that session.
    pub async fn begin_mergeable_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.begin_mergeable(id).await?;
        self.inner().batches.get_mut(&id).expect("opened").identity =
            WriteIdentity::Session(author);
        Ok(())
    }

    /// Open a SYSTEM backend mergeable batch with separate row provenance.
    /// Replica attribution still requires a trusted connection to the authority.
    /// Staged upload attachment continues to require a serializable batch.
    pub async fn begin_mergeable_for_attribution(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.inner().require_attribution_backend()?;
        self.begin_mergeable(id).await?;
        self.inner().batches.get_mut(&id).expect("opened").identity =
            WriteIdentity::Attribution(author);
        Ok(())
    }

    /// A serializable transaction: snapshot reads, validated at commit.
    /// On a client the snapshot is the server seq the replica is known
    /// complete through.
    pub async fn begin_serializable(&self, id: OpenTransactionId) -> Result<(), Error> {
        let (snapshot, own) = {
            let inner = self.inner.borrow();
            let snapshot = inner
                .client
                .as_ref()
                .map_or(inner.seq, |client| client.known);
            (snapshot, inner.unsettled_own())
        };
        let accepted_begin = self.inner().accepted_cut(snapshot)?;
        self.begin(
            id,
            BatchKind::Serializable {
                snapshot,
                accepted_begin,
                reads: Vec::new(),
                // A cold replica can acquire its first authority coordinate on
                // read. Once it knows a coordinate, public begin fixes that cut.
                pinned: !self.is_client() || snapshot != 0,
                own,
            },
        )
    }

    /// Open a serializable batch for an authenticated session.
    /// The identity governs its reads, every write and staged upload attachment.
    /// A replica's authority checks the transport identity; only a trusted
    /// backend link may submit a different session's writes and claims.
    pub async fn begin_serializable_for_identity(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.begin_serializable(id).await?;
        self.inner
            .borrow_mut()
            .batches
            .get_mut(&id)
            .expect("opened")
            .identity = WriteIdentity::Session(author);
        Ok(())
    }

    /// Open a SYSTEM backend batch with separate row provenance.
    /// Upload capabilities must bind the same author. On a replica, attribution
    /// is accepted only over a trusted backend connection to the authority.
    pub async fn begin_serializable_for_attribution(
        &self,
        id: OpenTransactionId,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        self.inner().require_attribution_backend()?;
        self.begin_serializable(id).await?;
        self.inner().batches.get_mut(&id).expect("opened").identity =
            WriteIdentity::Attribution(author);
        Ok(())
    }

    fn begin(&self, id: OpenTransactionId, kind: BatchKind) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        inner.require_mutation_admission()?;
        if inner.batches.contains_key(&id) || inner.retired_initializations.contains(&id) {
            return Err(rejected(format!("transaction {id:?} is already open")));
        }
        inner.batches.insert(
            id,
            Batch {
                kind,
                identity: WriteIdentity::Database,
                ops: Vec::new(),
                uploads: Vec::new(),
                edits: LargeEdits::default(),
                reads_state: Rc::new(RefCell::new(accepted::ReadState::default())),
                paired_scope: None,
            },
        );
        Ok(())
    }

    /// A fresh row id from this database's id source.
    pub fn next_row_id(&self) -> RowUuid {
        self.inner().ids.next_row_id()
    }

    pub fn mergeable_tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
        TxRef { db: self, id }
    }

    pub fn serializable_tx_ref(&self, id: OpenTransactionId) -> TxRef<'_> {
        TxRef { db: self, id }
    }

    pub fn abandon_transaction_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.take_batch(id).map(drop)
    }

    /// Whether this runtime still owns an open mutable batch. Host adapters
    /// use this after a fallible seal to distinguish a retryable preflight
    /// failure from a consumed transaction. It grants no publication rights.
    pub fn has_open_transaction(&self, id: OpenTransactionId) -> bool {
        self.inner.borrow().batches.contains_key(&id)
    }

    pub fn abandon_serializable_handle(&self, id: OpenTransactionId) -> Result<(), Error> {
        self.abandon_transaction_handle(id)
    }

    fn take_batch(&self, id: OpenTransactionId) -> Result<Batch, Error> {
        let batch = self
            .inner
            .borrow_mut()
            .batches
            .remove(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        accepted::close_pending_reads(&batch.reads_state);
        Ok(batch)
    }

    pub async fn commit_mergeable_handle(&self, id: OpenTransactionId) -> Result<TxReceipt, Error> {
        let batch = self.take_batch(id)?;
        batch.reads_state.borrow().check_commit()?;
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
        self.commit_serializable_batch(&batch, None)
    }

    fn commit_serializable_batch(
        &self,
        batch: &Batch,
        reservation: Option<ReservedTxId>,
    ) -> Result<TxReceipt, Error> {
        batch.reads_state.borrow().check_commit()?;
        let BatchKind::Serializable {
            snapshot,
            reads,
            own,
            ..
        } = &batch.kind
        else {
            return Err(rejected("transaction is not serializable"));
        };
        // On a client the authority validates, against its own state; the
        // commit applies optimistically here meanwhile. A row the replica
        // already knows moved past the snapshot is a conflict now (with no
        // own writes still unsettled at the snapshot, whose landing seqs
        // only the authority can place).
        if self.is_client() {
            if own.is_empty() {
                let inner = self.inner();
                for (schema, op) in &batch.ops {
                    if let Some(row) = op.row()
                        && inner
                            .settled_seq(schema.stored_table(op.table())?, row)?
                            .is_some_and(|seq| seq > *snapshot)
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
            // holds a server version may only have settled: the authority
            // decides that one.
            {
                let inner = self.inner();
                for read in reads {
                    let spec = ReadSpec {
                        include_deleted: read.include_deleted,
                        ..ReadSpec::default()
                    };
                    let current = if let Some(scoped) = &read.scoped {
                        let schema = read.prepared.schema.as_ref().unwrap_or(&inner.owner);
                        let table = schema.stored_table(&read.prepared.query.table)?;
                        for (row, seq) in &read.rows {
                            if inner
                                .settled_seq(table, *row)?
                                .is_some_and(|current| current > *seq)
                            {
                                return Err(conflict(
                                    "an accepted scoped read row changed since its snapshot",
                                ));
                            }
                        }
                        // A partial cache cannot prove a fresh query result. Recheck
                        // the exact admitted snapshot here; the normal authority
                        // commit path still validates the original full query and
                        // its content against current state, including phantoms.
                        inner.run_scoped_snapshot(
                            &read.prepared,
                            batch.reader(),
                            read.include_deleted,
                            scoped,
                        )?
                    } else {
                        inner.read_rows(&read.prepared, batch.reader(), &spec)?
                    };
                    let now = versions(&current);
                    let mut changed =
                        now.len() != read.rows.len() || read_contents(&current)? != read.content;
                    for (now, then) in now.iter().zip(&read.rows) {
                        let same_version = now.1 == then.1
                            || (own.contains(&then.1) && now.1 < LOCAL_BASE)
                            || (now.1 >= LOCAL_BASE
                                && then.1 >= LOCAL_BASE
                                && inner.local_origin(now.1)? == inner.local_origin(then.1)?);
                        changed |= now.0 != then.0 || !same_version;
                    }
                    if changed {
                        return Err(conflict(
                            "a row read by the transaction changed since its snapshot",
                        ));
                    }
                }
            }
            let content = reads.iter().map(|read| read.content.clone()).collect();
            let reads = {
                let inner = self.inner();
                let positions = own
                    .iter()
                    .map(|&position| {
                        inner
                            .local_origin(position)
                            .map(|id| ((*id.node.as_bytes(), id.time.0), position))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                reads
                    .iter()
                    .map(|read| {
                        let rows = read
                            .rows
                            .iter()
                            .map(|(row, seq)| {
                                let position = if *seq >= LOCAL_BASE {
                                    let id = inner.local_origin(*seq)?;
                                    positions
                                        .get(&(*id.node.as_bytes(), id.time.0))
                                        .copied()
                                        .unwrap_or(*seq)
                                } else {
                                    *seq
                                };
                                Ok((*row.as_bytes(), position))
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        Ok(sync::WireRead {
                            query: read.prepared.query.clone(),
                            binding: read.prepared.binding.clone(),
                            include_deleted: read.include_deleted,
                            rows,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            };
            return self.commit_ops_reserved(
                batch,
                sync::Kind::Serializable {
                    snapshot: *snapshot,
                    reads,
                    own: own.clone(),
                    content,
                },
                reservation,
            );
        }
        {
            let inner = self.inner();
            for (schema, op) in &batch.ops {
                if let Some(row) = op.row()
                    && inner
                        .newest_seq(schema.stored_table(op.table())?, row)?
                        .is_some_and(|seq| seq > *snapshot)
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
                let observed = inner.read_rows(&read.prepared, batch.reader(), &spec)?;
                if versions(&observed) != read.rows || read_contents(&observed)? != read.content {
                    return Err(conflict(
                        "a row read by the transaction changed since its snapshot",
                    ));
                }
            }
        }
        self.commit_ops_reserved(batch, sync::Kind::Mergeable, reservation)
    }

    fn commit_ops(&self, batch: &Batch, kind: sync::Kind) -> Result<TxReceipt, Error> {
        self.commit_ops_reserved(batch, kind, None)
    }

    fn commit_ops_reserved(
        &self,
        batch: &Batch,
        kind: sync::Kind,
        reservation: Option<ReservedTxId>,
    ) -> Result<TxReceipt, Error> {
        let _scope = accepted::ClaimsScope::new(self, batch.paired_scope.as_ref());
        self.with_upload(|| self.commit_ops_now(batch, kind, reservation))
    }

    fn commit_ops_now(
        &self,
        batch: &Batch,
        kind: sync::Kind,
        reservation: Option<ReservedTxId>,
    ) -> Result<TxReceipt, Error> {
        self.inner().require_mutation_admission()?;
        self.inner().require_foreground_writes("commits")?;
        self.inner().require_initialization_allocator()?;
        let ops = &batch.ops;
        let seq = {
            let mut inner = self.inner();
            let seq = inner.seq + 1;
            inner
                .conn
                .execute_batch("SAVEPOINT commit_batch")
                .map_err(storage)?;
            let mark = inner.changed.borrow().len();
            let serializable = matches!(batch.kind, BatchKind::Serializable { .. });
            let applied = inner
                .candidate_unit(ops, seq, serializable, false, |inner| {
                    inner.apply_with_edits(ops, &batch.edits, 0, seq)
                })
                .and_then(|()| inner.record_local_commit(seq, reservation, inner.identity.author))
                .and_then(|()| inner.record_local_writer_order(seq, mark))
                .and_then(|()| inner.record_issued_seq(seq));
            if let Err(error) = applied {
                inner.rollback_savepoint("commit_batch").map_err(storage)?;
                inner.changed.borrow_mut().truncate(mark);
                return Err(error);
            }
            inner
                .conn
                .execute_batch("RELEASE commit_batch")
                .map_err(storage)?;
            inner.seq = seq;
            seq
        };
        self.record_upload(
            ops.iter().map(|(_, op)| op.clone()).collect(),
            seq,
            Some(batch.identity),
            kind,
        )?;
        self.notify()?;
        Ok(TxReceipt(seq))
    }

    /// Run `read` over the batch's staged writes (plus `extra`), then undo
    /// them: a savepoint replay is the overlay. Serializable batches read
    /// through the snapshot cut, which also admits their own writes.
    fn in_overlay<R>(
        &self,
        id: OpenTransactionId,
        extra: Option<StagedOp>,
        read: impl FnOnce(&mut Inner, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.in_overlay_prefix(id, None, extra, read)
    }

    /// An asynchronous read observes only writes admitted before its first poll,
    /// even when a prior read or authority hydration delays its evaluation.
    fn in_overlay_prefix<R>(
        &self,
        id: OpenTransactionId,
        prefix: Option<BatchPrefix>,
        extra: Option<StagedOp>,
        read: impl FnOnce(&mut Inner, Option<Cut>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut inner = self.inner();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
        let prefix = prefix.unwrap_or_else(|| batch.prefix());
        let unavailable = || {
            Error::new(
                ErrorCode::NotObserved,
                "transaction staged prefix is no longer available",
            )
        };
        let mut ops = batch
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
        let checked = ops.len();
        if let Some(op) = extra {
            ops.push((inner.schema.clone(), op));
        }
        let own = inner.seq + 1;
        inner
            .conn
            .execute_batch("SAVEPOINT overlay")
            .map_err(storage)?;
        let mark = inner.changed.borrow().len();
        let result = inner.candidate_unit(&ops, own, snapshot.is_some(), true, |inner| {
            inner.apply_with_edits(&ops, &edits, checked, own)?;
            read(inner, snapshot.map(|cut| Cut { own, ..cut }))
        });
        inner.rollback_savepoint("overlay").map_err(storage)?;
        inner.changed.borrow_mut().truncate(mark);
        result
    }

    fn stage(&self, id: OpenTransactionId, op: StagedOp) -> Result<(), Error> {
        self.stage_with_edits(id, op, LargeEdits::default())
    }

    fn stage_op(&self, id: OpenTransactionId, mut op: StagedOp) -> Result<(), Error> {
        {
            let inner = self.inner.borrow();
            inner.require_mutation_admission()?;
            let batch = inner
                .batches
                .get(&id)
                .ok_or_else(|| rejected(format!("transaction {id:?} is not open")))?;
            let identity = op.identity().unwrap_or(WriteIdentity::Database);
            if identity != WriteIdentity::Database && identity != batch.identity {
                return Err(rejected("write identity differs from its transaction"));
            }
            if let WriteIdentity::Session(author) = batch.identity {
                op.attribute(author, true);
            } else if let WriteIdentity::Attribution(author) = batch.identity {
                inner.require_attribution_backend()?;
                op.attribute(author, false);
            }
        }
        // A counter write's change is from what the transaction sees.
        self.in_overlay(id, None, |inner, _| inner.counter_deltas(&mut op))?;
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

    /// [`Self::stage`] a write whose large values a partial update built:
    /// the batch keeps what it built for every later apply.
    pub(crate) fn stage_with_edits(
        &self,
        id: OpenTransactionId,
        mut op: StagedOp,
        mut edits: LargeEdits,
    ) -> Result<(), Error> {
        let marks = {
            let mut inner = self.inner();
            inner.require_mutation_admission()?;
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
            && let Some(batch) = self.inner().batches.get_mut(&id)
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
        let mut inner = self.inner();
        let Some(
            batch @ Batch {
                kind: BatchKind::Serializable { snapshot, own, .. },
                ..
            },
        ) = inner.batches.get(&id)
        else {
            return Ok(());
        };
        let read = ReadSpec {
            include_deleted,
            cut: Some(inner.serializable_cut(*snapshot, own)?),
            ..ReadSpec::default()
        };
        let observed = inner.read_rows(prepared, batch.reader(), &read)?;
        let rows = versions(&observed);
        let content = read_contents(&observed)?;
        let prepared = if prepared.request_claims.is_none() {
            let author = batch.reader().unwrap_or(inner.identity.author);
            prepared.clone().with_identity_claims(
                author,
                inner.claims.get(&author).cloned().unwrap_or_default(),
            )
        } else {
            prepared.clone()
        };
        if let Some(Batch {
            kind: BatchKind::Serializable { reads, pinned, .. },
            ..
        }) = inner.batches.get_mut(&id)
        {
            // Initialization absence reads enter here without remote hydration.
            // Once any read has observed this cut, later reads must not advance it.
            *pinned = true;
            reads.push(ReadRecord {
                prepared: prepared.clone(),
                scoped: None,
                include_deleted,
                rows,
                content,
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

    /// Undelete `row`, optionally with replacement content.
    pub async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        content: Option<BTreeMap<String, Value>>,
        opts: RestoreOptions,
    ) -> Result<WriteHandle, Error> {
        self.db.stage(
            self.id,
            StagedOp::Restore(table.to_owned(), row, content, opts),
        )?;
        Ok(WriteHandle { row, seq: 0 })
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
        self.all_prepared_at_tier(prepared, include_deleted, ReadTier::LocalFirst)
            .await
    }

    pub(super) async fn all_prepared_at_tier(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        tier: ReadTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        // An explicit host scope cannot silently reinterpret a session-bound
        // transaction. Keep the transaction's reader and reject the mismatch
        // before registering a read or waiting for remote coverage.
        if let Some((requested, _)) = &prepared.request_claims
            && self
                .db
                .inner()
                .batches
                .get(&self.id)
                .and_then(Batch::reader)
                .is_some_and(|bound| bound != *requested)
        {
            return Err(rejected(
                "open transaction identity differs from its bound identity",
            ));
        }
        let mut guard = self.db.transaction_read_guard(self.id, false)?;
        // Capture before waiting for earlier reads. Their authority round trip
        // must not make later staging visible to this already-admitted read.
        let prefix = self
            .db
            .inner()
            .batches
            .get(&self.id)
            .ok_or_else(|| rejected("transaction is not open"))?
            .prefix();
        guard.wait().await?;
        self.db.check_read_guard(self.id, &guard)?;
        let scope = self
            .db
            .inner()
            .batches
            .get(&self.id)
            .and_then(|batch| batch.paired_scope.clone());
        let prepared = match scope {
            Some((author, claims)) => {
                guard.require_success();
                if prepared
                    .request_claims
                    .as_ref()
                    .is_some_and(|scope| *scope != (author, claims.clone()))
                {
                    return Err(rejected(
                        "transaction read claims differ from its paired reads",
                    ));
                }
                prepared.clone().with_identity_claims(author, claims)
            }
            None => prepared.clone(),
        };
        let result = self
            .all_prepared_with_inner(&prepared, include_deleted, tier, &guard, prefix)
            .await;
        guard.finish(&result);
        result
    }

    async fn all_prepared_with_inner(
        &self,
        prepared: &PreparedQuery,
        include_deleted: bool,
        tier: ReadTier,
        guard: &accepted::ReadGuard,
        prefix: BatchPrefix,
    ) -> Result<Vec<CurrentRow>, Error> {
        // A client hydrates a serializable read at the transaction snapshot.
        // LocalFirst can use the replica's known cut when offline; Remote must
        // obtain authority coverage, and LocalOnly never requests it.
        let snapshot = match self.db.inner.borrow().batches.get(&self.id) {
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
            Db::remote_shape(&prepared.query)?;
            let fetch = async {
                if tier == ReadTier::Remote {
                    self.db.wait_for_read_transport().await;
                }
                let through = self.db.fetch(prepared, at, include_deleted, None).await?;
                if tier == ReadTier::Remote && through.is_none() {
                    return Err(Error::new(
                        ErrorCode::NotObserved,
                        "authority coverage unavailable for transaction read",
                    ));
                }
                Ok(through)
            };
            let mut fetch = std::pin::pin!(fetch);
            let through = std::future::poll_fn(|cx| {
                guard.watch(cx.waker());
                if let Err(error) = self.db.check_read_guard(self.id, guard) {
                    return std::task::Poll::Ready(Err(error));
                }
                fetch.as_mut().poll(cx)
            })
            .await?;
            self.db.check_read_guard(self.id, guard)?;
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
        let rows = self
            .db
            .in_overlay_prefix(self.id, Some(prefix), None, |inner, cut| {
                let reader = inner.batches.get(&self.id).and_then(Batch::reader);
                let roots = inner.run(
                    prepared,
                    reader,
                    &ReadSpec {
                        include_deleted,
                        cut,
                        ..ReadSpec::default()
                    },
                )?;
                inner.nest_relations(prepared, reader, roots)
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
            tx: self.serializable_tx_ref(id),
            committed: false,
        })
    }
}
