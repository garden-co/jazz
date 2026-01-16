//! Groove - Core library for Jazz distributed database
//!
//! ## Modules
//!
//! - `object` - ObjectId (UUIDv7, Crockford Base32), object primitives
//! - `commit` - Commit graph nodes, CommitId
//! - `branch` - Branch management, delete/truncation → spec: docs/content/docs/internals/deletes-and-truncation.mdx
//! - `node` - LocalNode for managing objects
//! - `storage` - Content/commit storage traits → spec: docs/content/docs/internals/streaming-and-persistence.mdx
//! - `listener` - Sync callback subscriptions
//! - `merge` - Merge strategies (LWW)
//! - `sql/` - SQL layer → spec: docs/content/docs/internals/sql-layer.mdx
//!   - `sql/query_graph/` - Incremental queries → spec: docs/content/docs/internals/incremental-queries.mdx
//!   - `sql/policy.rs` - ReBAC policies → spec: docs/content/docs/internals/rebac-policies.mdx
//!   - `sql/binary.rs` - Binary encoding → spec: docs/content/docs/internals/binary-data-and-blobs.mdx
//! - `sync/` - Sync protocol

mod branch;
mod commit;
mod listener;
mod merge;
mod node;
mod object;
pub mod sql;
mod storage;
pub mod sync;

pub use branch::{Branch, BranchError, DEFAULT_ENV, DEFAULT_USER_BRANCH, SchemaBranchName};
pub use commit::{Commit, CommitId};
pub use listener::{
    ByteDiff, DiffRange, ListenerError, ListenerId, ObjectCallback, ObjectKey,
    ObjectListenerRegistry, ObjectState, compute_change_ranges,
};
pub use merge::{LastWriterWins, MergeStrategy};
pub use node::{LocalNode, ObjectChange, generate_object_id};
pub use object::{Object, ObjectId, ObjectIdParseError, SchemaId};
pub use storage::{
    ChunkHash, ChunkStore, CommitMeta, CommitStore, ContentRef, Environment, INLINE_THRESHOLD,
    MemoryContentStore, MemoryEnvironment, Storage, SyncStateStore,
};

// Re-export ViewerContext for policy evaluation with JWT claims
#[cfg(feature = "sync-server")]
pub use sql::ViewerContext;
