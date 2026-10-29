//! The plain types of the `Db` API: configuration, identities, read and
//! write options, the error type, and host scheduling hooks.

use std::time::Duration;

use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::protocol::{BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec};
use crate::schema::JazzSchema;

pub use crate::local_executor::block_on;

/// Identity attached to locally-authored writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DbIdentity {
    /// Node identity.
    pub node: NodeUuid,
    /// Application author identity.
    pub author: AuthorSubject,
}

/// Configuration for [`crate::db::Db::open`] and [`crate::db::Db::open_at`].
pub struct DbConfig {
    /// Runtime schema.
    pub schema: JazzSchema,
    /// Local identity.
    pub identity: DbIdentity,
    /// Row id source used by inserts. `None` selects the production source.
    pub id_source: Option<Box<dyn RowIdSource>>,
    /// Where settled large-value leaf bytes live. `None` keeps them in the
    /// SQLite file ([`crate::chunk_store`]).
    pub chunk_store: Option<std::sync::Arc<dyn crate::chunk_store::ChunkStore>>,
}

impl DbConfig {
    /// Build a config using the production row id source.
    pub fn new(schema: JazzSchema, identity: DbIdentity) -> Self {
        Self {
            schema,
            identity,
            id_source: None,
            chunk_store: None,
        }
    }

    /// Override the row id source, typically with [`SeededRowIdSource`] in tests.
    pub fn with_id_source(mut self, id_source: impl RowIdSource + 'static) -> Self {
        self.id_source = Some(Box::new(id_source));
        self
    }

    /// Keep settled large-value leaf bytes in `store` instead of the SQLite
    /// file. A store that once had one must always be opened with it.
    pub fn with_chunk_store(
        mut self,
        store: std::sync::Arc<dyn crate::chunk_store::ChunkStore>,
    ) -> Self {
        self.chunk_store = Some(store);
        self
    }
}

/// Source of uuidv7-shaped row ids for inserts.
pub trait RowIdSource {
    /// Return the next row id.
    fn next_row_id(&mut self) -> RowUuid;
}

/// Production row id source using the system clock and OS randomness.
///
/// Tests and simulations should use [`SeededRowIdSource`] instead.
#[derive(Clone, Debug, Default)]
pub struct ProductionRowIdSource;

impl RowIdSource for ProductionRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        RowUuid(uuid::Uuid::now_v7())
    }
}

/// Deterministic uuidv7-shaped row id source for tests and simulations.
#[derive(Clone, Debug)]
pub struct SeededRowIdSource {
    millis: u64,
    state: u64,
}

impl SeededRowIdSource {
    /// Create a deterministic source from a caller-provided seed.
    pub fn new(seed: u64) -> Self {
        Self {
            millis: seed & ((1_u64 << 48) - 1),
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }
}

impl RowIdSource for SeededRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        let millis = self.millis & ((1_u64 << 48) - 1);
        self.millis = self.millis.wrapping_add(1);

        let rand_a = (splitmix64(&mut self.state) & 0x0fff) as u16;
        let rand_b = splitmix64(&mut self.state) & ((1_u64 << 62) - 1);

        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
        let version_and_rand_a = 0x7000_u16 | rand_a;
        bytes[6..8].copy_from_slice(&version_and_rand_a.to_be_bytes());
        let variant_and_rand_b = 0x8000_0000_0000_0000_u64 | rand_b;
        bytes[8..16].copy_from_slice(&variant_and_rand_b.to_be_bytes());
        RowUuid::from_bytes(bytes)
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Where a read gets its results and whether it includes pending writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum ReadTier {
    /// Include local data and pending writes, and request results upstream.
    LocalFirst,
    /// Read the server-confirmed view without a pending-write overlay.
    Remote,
    /// Include locally known data and pending writes without requesting upstream results.
    LocalOnly,
}

/// One-shot read options.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReadOpts {
    /// Read semantics.
    pub tier: ReadTier,
    /// Semantic read view to evaluate against.
    pub read_view: ReadViewSpec,
    /// What to do with an unsettled opening. Host read-option state only; an
    /// absent serde field is [`FirstLoad::Deliver`].
    #[serde(default)]
    pub first_load: FirstLoad,
    /// Large-capable columns whose changes a subscription's updated rows
    /// also carry as splices from the row's previous result (SPEC 19.12).
    #[serde(default)]
    pub splices: Vec<String>,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            tier: ReadTier::LocalFirst,
            read_view: ReadViewSpec::default(),
            first_load: FirstLoad::Deliver,
            splices: Vec::new(),
        }
    }
}

impl ReadOpts {
    /// Construct a read from product semantics.
    pub fn for_read_tier(tier: ReadTier) -> Self {
        Self {
            tier,
            ..Self::default()
        }
    }

    /// Deliver `columns`' changes in updated rows as splices too.
    pub fn with_splices(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.splices = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Evaluate the query as a live head branch composed over an optional base.
    pub fn branch_view(mut self, head: BranchSelector, base: Option<BranchViewBase>) -> Self {
        self.read_view.source = ReadViewSourceSpec::BranchView { head, base };
        self
    }
}

/// How long a local-first read with [`FirstLoad::WaitForRemote`] keeps
/// waiting while the host reports [`RemoteLinkHint::Attempting`].
pub const REMOTE_LINK_ATTEMPT_WINDOW: Duration = Duration::from_secs(5);

/// What a local-first read does with its first load (its unsettled opening).
///
/// This is a host read option, not a durable encoding: it is never persisted
/// or sent to a peer.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum FirstLoad {
    /// Deliver the opening as evaluated (ordinary local-first behaviour).
    #[default]
    Deliver,
    /// Withhold the unsettled local opening for at most `timeout_ms` while the
    /// remote could answer, then behave like local-first. A zero timeout is
    /// [`FirstLoad::Deliver`].
    WaitForRemote {
        /// The longest the opening may wait, in milliseconds.
        timeout_ms: u64,
    },
}

/// What the host knows about the path to the authoritative server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteLinkHint {
    /// No server is configured; nothing can answer remotely.
    NoServer,
    /// A connection attempt has started and has neither succeeded nor failed.
    Attempting,
    /// The path to the server is admitted and serviceable.
    Live,
    /// The attempt failed, the link is backing off, or the application
    /// disconnected explicitly. Nothing waits.
    Failed,
}

impl RemoteLinkHint {
    /// Parse the host binding spelling. `connecting`, `connected` and
    /// `unavailable` are accepted as aliases for the TypeScript link-state
    /// names.
    pub fn from_host_str(value: &str) -> Option<Self> {
        match value {
            "none" | "None" | "no-server" | "NoServer" => Some(Self::NoServer),
            "attempting" | "Attempting" | "connecting" | "Connecting" => Some(Self::Attempting),
            "live" | "Live" | "connected" | "Connected" => Some(Self::Live),
            "failed" | "Failed" | "unavailable" | "Unavailable" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Identity used to author and authorize a standalone write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WriteIdentity {
    /// Use the identity that opened the database.
    #[default]
    Database,
    /// Author and authorize the write as this trusted session identity.
    Session(AuthorSubject),
    /// Attribute provenance while retaining the database identity as policy subject.
    Attribution(AuthorSubject),
}

/// Exact branch selected by an insert or restore.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ExactWriteTarget {
    /// Write to the database's current root branch.
    #[default]
    Root,
    /// Write to one exact branch key.
    Branch(BranchSelector),
}

/// Root or head-over-base view selected by an update, upsert, or delete.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum WriteTarget {
    /// Write to the database's current root branch.
    #[default]
    Root,
    /// Write through a branch view, materializing inherited state in `head`.
    BranchView {
        /// Exact branch receiving the local write.
        head: BranchSelector,
        /// Optional inherited base view visible below `head`.
        base: Option<BranchViewBase>,
    },
}

/// Options for inserts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InsertOptions {
    /// Caller-supplied row id, or a generated UUIDv7 row id when omitted.
    pub row_id: Option<RowUuid>,
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Exact branch receiving the row.
    pub target: ExactWriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for updates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the patch is applied.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for upserts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpsertOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the upsert is applied.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for deletes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeleteOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the row is deleted.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for restores.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RestoreOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Exact branch whose deletion is restored.
    pub target: ExactWriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Public API error with stable machine-readable codes.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// The finer kind within `code`, when callers act on it: they classify
    /// by this, never by matching `message`.
    #[serde(default)]
    pub reason: Option<Reason>,
    /// Human-readable detail.
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            ErrorCode::TransactionConflict => {
                write!(formatter, "(transaction_conflict): {}", self.message)
            }
            _ => write!(formatter, "{:?}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    #[doc(hidden)]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            reason: None,
            message: message.into(),
        }
    }

    /// An error of a kind callers tell apart by [`Error::reason`].
    #[doc(hidden)]
    pub fn with_reason(code: ErrorCode, reason: Reason, message: impl Into<String>) -> Self {
        Self {
            code,
            reason: Some(reason),
            message: message.into(),
        }
    }

    /// The authority rejected a serializable commit because a read it made
    /// no longer holds. The message is the text earlier bindings matched.
    pub(crate) fn serialization_conflict() -> Self {
        Self::with_reason(
            ErrorCode::WriteRejected,
            Reason::SerializationConflict,
            "SerializationConflict (reason: Serializable transaction conflicted with another write)",
        )
    }

    /// Whether the authority rejected this serializable commit as a conflict.
    pub fn is_serialization_conflict(&self) -> bool {
        self.reason == Some(Reason::SerializationConflict)
    }

    /// Whether this serializable transaction conflicted, at the replica or
    /// at the authority: running it again may succeed.
    pub fn is_conflict(&self) -> bool {
        self.code == ErrorCode::TransactionConflict || self.is_serialization_conflict()
    }
}

impl From<crate::query::QueryError> for Error {
    fn from(error: crate::query::QueryError) -> Self {
        Self::new(ErrorCode::Query, error.to_string())
    }
}

/// Why an error happened, finer than its [`ErrorCode`]: the kinds the
/// engine, hosts and bindings act on differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// A write policy denied the write (code `WriteRejected`).
    AuthorizationDenied,
    /// The write's explicit timestamp is too far ahead of the authority's
    /// clock (code `WriteRejected`, SPEC 3, INV-TX-7).
    ClientClockTooFarAhead,
    /// A read the serializable commit made no longer holds at the
    /// authority (code `WriteRejected`).
    SerializationConflict,
    /// A write to, or an insert reusing the id of, a deleted row (code
    /// `WriteRejected`).
    RowAlreadyDeleted,
    /// An insert reusing a live row's id (code `WriteRejected`).
    RowAlreadyExists,
    /// A restore of a live row (code `WriteRejected`).
    RowNotDeleted,
    /// A new row, or restored content, without a value for a required
    /// column (code `WriteRejected`).
    MissingRequired,
    /// A mergeable commit that would move a row to another branch (code
    /// `Schema`).
    BranchMove,
    /// A read needs large-value chunks this replica requested from
    /// upstream but has not received yet (code `NotObserved`).
    MissingChunks,
    /// The server holds commits from an untrusted session until it has a
    /// published permissions head to check them by (code `WriteRejected`).
    PermissionsHeadMissing,
    /// The write was built on an unsettled write the authority rejected,
    /// so it is rejected too (code `WriteRejected`, SPEC 4 §4.6).
    PredecessorRejected,
    /// A store configured as one user was asked to serve a session of
    /// another identity (code `Protocol`): relays serve one identity.
    IdentityMismatch,
}

impl Reason {
    /// Every reason, in declaration order.
    pub const ALL: [Reason; 12] = [
        Reason::AuthorizationDenied,
        Reason::ClientClockTooFarAhead,
        Reason::SerializationConflict,
        Reason::RowAlreadyDeleted,
        Reason::RowAlreadyExists,
        Reason::RowNotDeleted,
        Reason::MissingRequired,
        Reason::BranchMove,
        Reason::MissingChunks,
        Reason::PermissionsHeadMissing,
        Reason::PredecessorRejected,
        Reason::IdentityMismatch,
    ];

    /// The stable, snake_case name of this reason.
    ///
    /// Like [`ErrorCode::as_str`], a durable contract: bindings expose it to
    /// JavaScript as `error.reason`, and the sync wire carries it by this
    /// name, so a peer that does not know a newer reason still decodes the
    /// error. Pinned by the `error_code_strings` integration test.
    pub const fn as_str(self) -> &'static str {
        match self {
            Reason::AuthorizationDenied => "authorization_denied",
            Reason::ClientClockTooFarAhead => "client_clock_too_far_ahead",
            Reason::SerializationConflict => "serialization_conflict",
            Reason::RowAlreadyDeleted => "row_already_deleted",
            Reason::RowAlreadyExists => "row_already_exists",
            Reason::RowNotDeleted => "row_not_deleted",
            Reason::MissingRequired => "missing_required",
            Reason::BranchMove => "branch_move",
            Reason::MissingChunks => "missing_chunks",
            Reason::PermissionsHeadMissing => "permissions_head_missing",
            Reason::PredecessorRejected => "predecessor_rejected",
            Reason::IdentityMismatch => "identity_mismatch",
        }
    }

    /// The reason named `name` by [`Reason::as_str`], if this build knows it.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|reason| reason.as_str() == name)
    }

    /// Whether a write was refused for the stored state of its row rather
    /// than for its arguments: its id is deleted or taken, it restores a
    /// live row, it would create a row without a required value (an upsert
    /// may create or patch), or it would move a row to another branch.
    pub const fn is_apply_time(self) -> bool {
        matches!(
            self,
            Reason::RowAlreadyDeleted
                | Reason::RowAlreadyExists
                | Reason::RowNotDeleted
                | Reason::MissingRequired
                | Reason::BranchMove
        )
    }
}

/// Stable API error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ErrorCode {
    /// Schema validation failed.
    Schema,
    /// Query validation or binding failed.
    Query,
    /// Write was rejected.
    WriteRejected,
    /// A serializable transaction's snapshot was invalidated.
    TransactionConflict,
    /// Storage failed.
    Storage,
    /// Protocol or local operation failed.
    Protocol,
    /// Local transport queue is full and the operation should be retried later.
    Backpressure,
    /// Requested observation is not locally available.
    NotObserved,
    /// Historical read must be evaluated by a complete-history server.
    HistoricalReadRequiresServer,
    /// An existing local store was written with a storage format this build
    /// does not read.
    UnsupportedStorageCodecs,
}

impl ErrorCode {
    /// The stable, snake_case name of this code.
    ///
    /// This string is a durable contract: bindings expose it to JavaScript as
    /// `error.code`. Every spelling is written out here rather than derived
    /// from `Debug`, so renaming a variant cannot silently change it. The
    /// table is pinned by the `error_code_strings` integration test.
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Schema => "schema",
            ErrorCode::Query => "query",
            ErrorCode::WriteRejected => "write_rejected",
            ErrorCode::TransactionConflict => "transaction_conflict",
            ErrorCode::Storage => "storage",
            ErrorCode::Protocol => "protocol",
            ErrorCode::Backpressure => "backpressure",
            ErrorCode::NotObserved => "not_observed",
            ErrorCode::HistoricalReadRequiresServer => "historical_read_requires_server",
            ErrorCode::UnsupportedStorageCodecs => "unsupported_storage_codecs",
        }
    }
}
