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

/// Which result a read reflects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThinReadTier {
    /// Everything the server holds now.
    LocalFirst,
    /// Only what the global authority confirmed.
    Remote,
}

/// A thin client's request.
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
        /// Include soft-deleted rows.
        include_deleted: bool,
        /// Read tier.
        tier: ThinReadTier,
    },
    /// Close the live query opened by request `subscription`. Answered
    /// with [`ThinBody::Closed`] for that request.
    Unsubscribe {
        /// The `id` of the `Subscribe` request.
        subscription: u64,
    },
    /// Run a query once. Answered with [`ThinBody::Rows`].
    Query {
        /// Postcard `Query`.
        #[serde(with = "serde_bytes")]
        query: Vec<u8>,
        /// Include soft-deleted rows.
        include_deleted: bool,
        /// Read tier.
        tier: ThinReadTier,
    },
    /// Insert a row. Answered with [`ThinBody::Written`].
    Insert {
        /// Table name, in the bound schema.
        table: String,
        /// Client-chosen row id; the server picks one when absent.
        row: Option<[u8; 16]>,
        /// Binding-codec named cells.
        #[serde(with = "serde_bytes")]
        cells: Vec<u8>,
        /// `$updatedAt` override, in ms.
        updated_at_ms: Option<u64>,
    },
    /// Update a row. Answered with [`ThinBody::Written`].
    Update {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells to change.
        #[serde(with = "serde_bytes")]
        patch: Vec<u8>,
        /// `$updatedAt` override, in ms.
        updated_at_ms: Option<u64>,
    },
    /// Insert or update a row. Answered with [`ThinBody::Written`].
    Upsert {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// Binding-codec named cells.
        #[serde(with = "serde_bytes")]
        cells: Vec<u8>,
        /// `$updatedAt` override, in ms.
        updated_at_ms: Option<u64>,
    },
    /// Soft-delete a row. Answered with [`ThinBody::Written`].
    Delete {
        /// Table name, in the bound schema.
        table: String,
        /// Row id.
        row: [u8; 16],
        /// `$updatedAt` override, in ms.
        updated_at_ms: Option<u64>,
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
