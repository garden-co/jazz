#![warn(missing_docs)]
#![allow(
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::enum_variant_names,
    clippy::for_kv_map,
    clippy::large_enum_variant,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    clippy::needless_borrow,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

//! Jazz is the local-first database layer above groove storage and IVM. The
//! public reading order is `Db` facade -> [`node`] storage-backed core ->
//! groove query/storage primitives -> the underlying key-value store; [`peer`]
//! and [`protocol`] sit beside the node as sync-link state and wire vocabulary.
//! Start with `jazz/API.md` for the facade, `jazz/SPEC/4_history_merging.md`
//! for merge/currency semantics, `jazz/SPEC/6_queries.md` for query/read rules,
//! `jazz/SPEC/10_lenses_migrations.md` for schema migration, and
//! `jazz/BRANCHES.md` for branch behavior.
//!
//! ```no_run
//! use std::collections::BTreeMap;
//!
//! use jazz::ids::{AuthorId, NodeUuid, RowUuid};
//! use jazz::protocol::SyncMessage;
//! use jazz::schema::{JazzSchema, Policy, TableSchema};
//! use jazz::node::{MergeableCommit, NodeState};
//! use jazz::tx::{DeletionEvent, DurabilityTier};
//! use jazz::groove::records::Value;
//! use jazz::groove::schema::{ColumnSchema, ColumnType};
//! use jazz::groove::storage::RocksDbStorage;
//!
//! fn open_node(node: NodeUuid, schema: JazzSchema) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
//!     let dir = tempfile::tempdir().unwrap();
//!     let cfs = schema.column_families();
//!     let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
//!     let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
//!     let node = NodeState::new(node, schema, storage).unwrap();
//!     (dir, node)
//! }
//!
//! let owner = AuthorId::from_bytes([0xa1; 16]);
//! let schema = JazzSchema::new([TableSchema::new(
//!     "todos",
//!     [
//!         ColumnSchema::new("title", ColumnType::String),
//!         ColumnSchema::new("owner", ColumnType::Uuid),
//!     ],
//! )
//! .with_read_policy(Policy::owner_only("todos", "owner"))
//! .with_write_policy(Policy::owner_only("todos", "owner"))]);
//!
//! let (_writer_dir, mut writer) = open_node(NodeUuid::from_bytes([1; 16]), schema.clone());
//! let (_core_dir, mut core) = open_node(NodeUuid::from_bytes([9; 16]), schema.clone());
//! let row = RowUuid::from_bytes([7; 16]);
//! let cells = BTreeMap::from([
//!     ("title".to_owned(), Value::String("draft".to_owned())),
//!     ("owner".to_owned(), Value::Uuid(owner.0)),
//! ]);
//!
//! let (tx_id, unit) = writer
//!     .commit_mergeable_unit(
//!         MergeableCommit::new("todos", row, 1_000)
//!             .made_by(owner)
//!             .cells(cells),
//!     )
//!     .unwrap();
//! let local_rows = writer.current_rows("todos", DurabilityTier::Local).unwrap();
//! assert_eq!(local_rows[0].row_uuid(), row);
//! assert_eq!(local_rows[0].cell(&schema.tables[0], "title"), Some(Value::String("draft".to_owned())));
//!
//! let SyncMessage::CommitUnit { tx, versions } = unit else { unreachable!() };
//! let [fate] = core.ingest_commit_unit(tx, versions, 1_000).unwrap().try_into().unwrap();
//! writer.apply_sync_message(fate).unwrap();
//!
//! let tx_id = core.open_exclusive().unwrap();
//! core.tx_read(tx_id, "todos", row).unwrap();
//! core.tx_write(
//!     tx_id,
//!     "todos",
//!     row,
//!     BTreeMap::from([
//!         ("title".to_owned(), Value::String("done".to_owned())),
//!         ("owner".to_owned(), Value::Uuid(owner.0)),
//!     ]),
//!     None::<DeletionEvent>,
//! )
//! .unwrap();
//! let (_exclusive, _unit) = core.commit_exclusive(tx_id, owner, 1_001).unwrap();
//! assert!(!core.row_history("todos", row).unwrap().is_empty());
//! ```

/// Re-export of the underlying groove crate used for storage setup.
pub use groove;

/// High-level thread-affine database facade.
pub mod db;
/// Poll ready-immediate database futures without an async runtime.
pub use db::block_on;
/// Wire-stable identifiers.
pub mod ids;
/// Shared text merge strategy machinery.
pub mod merge_strategy;
/// Storage-backed node implementation and local API.
pub mod node;
/// Independent semantic oracle used by tests and harnesses.
#[cfg(any(test, feature = "testing"))]
pub mod oracle;
/// Per-peer sync state and metrics.
pub mod peer;
/// Simulation-first sync and local event messages.
pub mod protocol;
/// Protocol admission and semantic size limits.
pub mod protocol_limits;
/// Pure query AST, validation, canonicalization, and ids.
pub mod query;
/// Jazz schema and storage lowering.
pub mod schema;
/// Pure plaintext operation substrate and deterministic text-merge walk.
pub mod text_merge;
/// Logical time and sequence counters.
pub mod time;
/// Transaction, fate, and history vocabulary.
pub mod tx;
/// Versioned transport frames around the semantic sync protocol.
pub mod wire;
