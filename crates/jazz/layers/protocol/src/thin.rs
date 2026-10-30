//! Thin client requests and responses (SPEC 8.14).
//!
//! A thin client holds no replica: the server evaluates its queries under
//! the connection's session and sends finished results, and the client's
//! writes run on the server as that session. Both travel as their own
//! [`crate::wire::WireFrame`] variants, outside the ordered sync channels,
//! once both peers negotiated [`crate::wire::FEATURE_THIN_RESULTS`].
//!
//! Payloads reuse the binding codec the browser and Node bindings already
//! speak: queries are postcard [`jazz_model::query::Query`] bytes, cells are
//! binding-codec named cells, and results are binding-codec row batches and
//! subscription deltas.

use serde::{Deserialize, Serialize};

/// One request from a thin client. `id` is the client's, unique per
/// connection; every response to it carries the same `id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThinRequest {
    /// Client-chosen correlation id.
    pub id: u64,
    /// What the client asks for.
    pub op: ThinOp,
}

/// A thin client's request.
///
/// Options travel as the JSON objects the native bindings take (byte arrays
/// as arrays of numbers): read options `{ tier?, include_deleted?,
/// read_view?, first_load_remote_wait_ms?, splices? }` and write options
/// `{ rowId?, branch?, head?, base?, updatedAtMs? }`. They never name an
/// identity: every write runs as the session, and every read reads as it.
/// `transaction` is an open transaction this session began, as its id text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThinOp {
    /// Bind the connection to the schema the client was built against,
    /// as the public schema JSON the bindings open with. The server serves
    /// it only while its version is admitted. Must come before any other
    /// request; the server answers [`ThinBody::Ok`] or an error.
    Schema {
        /// Public schema JSON.
        #[serde(with = "serde_bytes")]
        schema: Vec<u8>,
    },
    /// Open a live query. The server answers with [`ThinBody::Event`]s
    /// until the client unsubscribes, or [`ThinBody::Closed`] / an error.
    Subscribe {
        /// Postcard `Query`.
        #[serde(with = "serde_bytes")]
        query: Vec<u8>,
        /// JSON read options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
    },
    /// Close the live query opened by request `subscription`. Answered
    /// with [`ThinBody::Closed`] for that request.
    Unsubscribe {
        /// The `id` of the `Subscribe` request.
        subscription: u64,
    },
    /// Run a query once, inside `transaction` if given (read-your-writes,
    /// and recorded for a serializable commit). Answered with
    /// [`ThinBody::Rows`].
    Query {
        /// Postcard `Query`.
        #[serde(with = "serde_bytes")]
        query: Vec<u8>,
        /// JSON read options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Insert a row. Answered with [`ThinBody::Written`], or
    /// [`ThinBody::Staged`] inside a transaction.
    Insert {
        /// Table name, in the bound schema.
        table: String,
        /// Binding-codec named cells.
        #[serde(with = "serde_bytes")]
        cells: Vec<u8>,
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Update a row. Answered as [`ThinOp::Insert`].
    Update {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells to change.
        #[serde(with = "serde_bytes")]
        patch: Vec<u8>,
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Insert or update a row. Answered as [`ThinOp::Insert`].
    Upsert {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells.
        #[serde(with = "serde_bytes")]
        cells: Vec<u8>,
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Soft-delete a row. Answered as [`ThinOp::Insert`].
    Delete {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Undelete a row, with `cells` or the values it was deleted with.
    /// Answered as [`ThinOp::Insert`].
    Restore {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells.
        cells: Option<serde_bytes::ByteBuf>,
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Partial large-value updates with an ordinary patch. Answered as
    /// [`ThinOp::Insert`].
    UpdateLargeValues {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells to change.
        #[serde(with = "serde_bytes")]
        patch: Vec<u8>,
        /// JSON partial-value update descriptors, as the bindings take them.
        #[serde(with = "serde_bytes")]
        mutations: Vec<u8>,
        /// `$updatedAt` override, in ms.
        updated_at_ms: Option<u64>,
        /// Open transaction id.
        transaction: Option<String>,
    },
    /// Begin a transaction (`kind` is `mergeable` or `serializable`) under
    /// the client-chosen id. Answered with [`ThinBody::Ok`]; it ends with
    /// the connection unless committed or rolled back first.
    Begin {
        /// Open transaction id text.
        transaction: String,
        /// Transaction kind.
        kind: String,
    },
    /// Commit an open transaction. Answered with [`ThinBody::Written`]
    /// (with a zero row), or the error that rejected it.
    Commit {
        /// Open transaction id text.
        transaction: String,
    },
    /// Roll back an open transaction. Answered with [`ThinBody::Ok`].
    Rollback {
        /// Open transaction id text.
        transaction: String,
    },
    /// Capture a snapshot for snapshot read views. Answered with
    /// [`ThinBody::Snapshot`].
    CaptureSnapshot,
    /// Begin uploading one large value of a row mutation in chunks; the
    /// request's `id` names the upload. Answered with [`ThinBody::Ok`].
    BeginStream {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells, without the streamed column.
        #[serde(with = "serde_bytes")]
        cells: Vec<u8>,
        /// The streamed column.
        column: String,
        /// `insert`, `update` or `upsert`.
        mutation: String,
        /// JSON write options.
        #[serde(with = "serde_bytes")]
        options: Vec<u8>,
    },
    /// The next chunk of upload `stream`. Answered with [`ThinBody::Ok`]
    /// once applied.
    StreamChunk {
        /// The `id` of the `BeginStream` request.
        stream: u64,
        /// Value bytes.
        #[serde(with = "serde_bytes")]
        chunk: Vec<u8>,
    },
    /// Publish upload `stream` as its row mutation. Answered with
    /// [`ThinBody::Written`].
    FinishStream {
        /// The `id` of the `BeginStream` request.
        stream: u64,
    },
    /// Drop upload `stream`. Answered with [`ThinBody::Ok`].
    AbortStream {
        /// The `id` of the `BeginStream` request.
        stream: u64,
    },
}

/// The server's answer to request `id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThinResponse {
    /// The request this answers.
    pub id: u64,
    /// The answer.
    pub body: ThinBody,
}

/// An answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThinBody {
    /// Done, nothing to return.
    Ok,
    /// A one-shot query's result: binding-codec row batches, or a relation
    /// snapshot for a query with nested relations.
    Rows {
        /// Binding-codec bytes.
        #[serde(with = "serde_bytes")]
        rows: Vec<u8>,
        /// Whether `rows` is a relation snapshot.
        relation: bool,
    },
    /// A live query's next change.
    Event {
        /// Binding-codec subscription delta.
        #[serde(with = "serde_bytes")]
        delta: Vec<u8>,
        /// Replaces everything delivered so far.
        reset: bool,
        /// The result is settled at `tier`.
        settled: bool,
        /// Durability tier the result is settled at, as the bindings spell it.
        tier: String,
    },
    /// The live query ended (unsubscribed, or the server closed it).
    Closed,
    /// A write staged in an open transaction; it commits with it.
    Staged {
        /// The written row.
        row: [u8; 16],
    },
    /// A captured snapshot, as its persisted text's bytes.
    Snapshot {
        /// Persisted snapshot reference.
        #[serde(with = "serde_bytes")]
        snapshot: Vec<u8>,
    },
    /// A write committed on the server.
    Written {
        /// The written row.
        row: [u8; 16],
        /// The server sequence number of the row version it left.
        seq: u64,
    },
    /// The request failed. Ends a live query.
    Error {
        /// Stable error code (`ErrorCode` spelling), or `Argument` for a
        /// malformed request.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}
