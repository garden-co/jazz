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
    clippy::type_complexity,
    async_fn_in_trait
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
//! use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
//! use jazz::protocol::SyncMessage;
//! use jazz::schema::JazzSchema;
//! use jazz::node::{MergeableCommit, NodeState};
//! use jazz::tx::{DeletionEvent, DurabilityTier};
//! use jazz::groove::records::Value;
//! use jazz::groove::storage::MemoryStorage;
//! use jazz::db::doctest_support::block_on;
//! use jazz::tools::{
//!     CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
//!     TableSchemaBuilder,
//! };
//!
//! fn open_node(node: NodeUuid, schema: JazzSchema) -> NodeState {
//!     let cfs = schema.column_families();
//!     let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
//!     block_on(NodeState::new(node, schema, MemoryStorage::new(&refs).expect("valid memory storage families"))).unwrap()
//! }
//!
//! let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
//! let owner_policy = PolicyExpr::Cmp {
//!     column: "owner".to_owned(),
//!     op: CmpOp::Eq,
//!     value: PolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
//! };
//! let source = SchemaBuilder::new()
//!     .table(
//!         TableSchemaBuilder::new("todos")
//!             .column("title", ColumnType::Text)
//!             .column("owner", ColumnType::Text)
//!             .policies(
//!                 TablePolicies::new()
//!                     .with_select(owner_policy.clone())
//!                     .with_insert(owner_policy.clone())
//!                     .with_update(Some(owner_policy.clone()), owner_policy.clone())
//!                     .with_delete(owner_policy),
//!             ),
//!     )
//!     .build();
//! let schema = JazzSchema::new(&source).unwrap();
//!
//! let mut writer = open_node(NodeUuid::from_bytes([1; 16]), schema.clone());
//! let mut core = open_node(NodeUuid::from_bytes([9; 16]), schema.clone());
//! let row = RowUuid::from_bytes([7; 16]);
//! let cells = BTreeMap::from([
//!     ("title".to_owned(), Value::String("draft".to_owned())),
//!     ("owner".to_owned(), Value::String(owner.canonical().to_owned())),
//! ]);
//!
//! let (tx_id, unit) = block_on(writer
//!     .commit_mergeable_unit(
//!         MergeableCommit::new("todos", row, 1_000)
//!             .made_by(owner)
//!             .cells(cells),
//!     ))
//!     .unwrap();
//! let local_rows = block_on(writer.current_rows("todos", DurabilityTier::Local)).unwrap();
//! assert_eq!(local_rows[0].row_uuid(), row);
//! assert_eq!(local_rows[0].cell(&schema.tables()[0], "title"), Some(Value::String("draft".to_owned())));
//!
//! let SyncMessage::CommitUnit { tx, versions } = unit else { unreachable!() };
//! let outcome = block_on(core.ingest_commit_unit(tx, versions, 1_000)).unwrap();
//! let [fate] = block_on(core.persist_and_settle_outcome(outcome)).unwrap().try_into().unwrap();
//! block_on(writer.apply_sync_message(fate)).unwrap();
//!
//! let tx_id = jazz::tools::OpenTransactionId::new();
//! block_on(core.open_exclusive(tx_id)).unwrap();
//! block_on(core.tx_read(tx_id, "todos", row)).unwrap();
//! block_on(core.tx_write(
//!     tx_id,
//!     "todos",
//!     row,
//!     BTreeMap::from([
//!         ("title".to_owned(), Value::String("done".to_owned())),
//!         ("owner".to_owned(), Value::String(owner.canonical().to_owned())),
//!     ]),
//!     None::<DeletionEvent>,
//! ))
//! .unwrap();
//! let (_exclusive, _unit) = block_on(core.commit_exclusive(tx_id, owner, 1_001)).unwrap();
//! assert!(!block_on(core.row_history("todos", row)).unwrap().is_empty());
//! ```

/// Re-export of the underlying groove crate used for storage setup.
pub use groove;

/// Poll ready-immediate database futures without an async runtime.
pub use db::block_on;
pub use jazz_db::binding_codec;
#[cfg(feature = "cold-settle-attribution")]
pub use jazz_db::cold_settle_attribution;
pub use jazz_db::db;
pub use jazz_db::foreground_node_lease;
pub use jazz_db::result_tree;
pub use jazz_db::row;
pub use jazz_model::model;
pub use jazz_model::query;
pub use jazz_model::row_input;
pub use jazz_model::schema;
pub use jazz_node::node;
pub use jazz_node::peer;
pub use jazz_protocol::authorization_scope;
pub use jazz_protocol::protocol;
pub use jazz_protocol::protocol_limits;
pub use jazz_types::account_registry;
pub use jazz_types::app_id;
#[cfg(any(test, feature = "testing", feature = "runtime"))]
use jazz_types::debug_env;
pub use jazz_types::identity;
pub use jazz_types::ids;
pub use jazz_types::local_executor;
pub use jazz_types::object;
pub use jazz_types::postcard_exact;
#[cfg(any(test, feature = "testing"))]
pub use node::oracle;
/// Platform-neutral client and server runtime APIs used by target shells.
#[cfg(feature = "runtime")]
pub mod serving;
pub use jazz_protocol::storage_codec_profile;
#[cfg(test)]
mod storage_codec_profile_tests;
pub use jazz_types::time;
/// Public runtime and data-model support APIs formerly provided by jazz-tools.
// The tools API was a separate crate before consolidation and intentionally
// retains its existing documentation policy.
#[allow(missing_docs)]
pub mod tools;
pub use jazz_model::tx;
pub use jazz_protocol::wire;

/// Bounded metadata-only delivery diagnostics for native acceptance failures.
#[doc(hidden)]
#[cfg(any(test, feature = "testing"))]
pub use jazz_types::delivery_diagnostics;
