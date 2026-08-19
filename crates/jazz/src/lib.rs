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
//! use jazz::ids::{AuthorId, NodeUuid, RowUuid};
//! use jazz::protocol::SyncMessage;
//! use jazz::schema::JazzSchema;
//! use jazz::node::{MergeableCommit, NodeState};
//! use jazz::tx::{DeletionEvent, DurabilityTier};
//! use jazz::groove::records::Value;
//! use jazz::groove::storage::MemoryStorage;
//! use jazz::tools::{
//!     CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
//!     TableSchemaBuilder,
//! };
//!
//! fn open_node(node: NodeUuid, schema: JazzSchema) -> NodeState<MemoryStorage> {
//!     let cfs = schema.column_families();
//!     let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
//!     NodeState::new(node, schema, MemoryStorage::new(&refs)).unwrap()
//! }
//!
//! let owner = AuthorId::from_bytes([0xa1; 16]);
//! let owner_policy = PolicyExpr::Cmp {
//!     column: "owner".to_owned(),
//!     op: CmpOp::Eq,
//!     value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
//! };
//! let source = SchemaBuilder::new()
//!     .table(
//!         TableSchemaBuilder::new("todos")
//!             .column("title", ColumnType::Text)
//!             .column("owner", ColumnType::Uuid)
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
//! assert_eq!(local_rows[0].cell(&schema.tables()[0], "title"), Some(Value::String("draft".to_owned())));
//!
//! let SyncMessage::CommitUnit { tx, versions } = unit else { unreachable!() };
//! let [fate] = core.ingest_commit_unit(tx, versions, 1_000).unwrap().try_into().unwrap();
//! writer.apply_sync_message(fate).unwrap();
//!
//! let tx_id = jazz::tools::OpenTransactionId::new();
//! core.open_exclusive(tx_id).unwrap();
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

// Legacy synchronous tests import these traits explicitly while the async API
// migration is in progress. New async lifecycle tests intentionally do not:
// they poll futures directly so suspension and ordering remain observable.
#[cfg(test)]
pub(crate) mod legacy_test_future {
    use std::future::Future;

    use crate::ids::{AuthorId, BranchId};
    use crate::node::{Error, MergeableCommit, NodeState};
    use crate::protocol::{SyncMessage, VersionRecord};
    use crate::tools::OpenTransactionId;
    use crate::tx::{BranchLineage, Fate, Transaction, TxId};
    use groove::storage::{OrderedKvStorage, ReopenableStorage};

    pub(crate) trait ResultFutureExt<T, E>: Future<Output = Result<T, E>> {
        fn unwrap(self) -> T
        where
            Self: Sized,
            E: std::fmt::Debug,
        {
            crate::db::block_on(self).unwrap()
        }

        fn expect(self, message: &str) -> T
        where
            Self: Sized,
            E: std::fmt::Debug,
        {
            crate::db::block_on(self).expect(message)
        }

        fn unwrap_or_else<F>(self, op: F) -> T
        where
            Self: Sized,
            F: FnOnce(E) -> T,
        {
            crate::db::block_on(self).unwrap_or_else(op)
        }

        fn unwrap_err(self) -> E
        where
            Self: Sized,
            T: std::fmt::Debug,
        {
            crate::db::block_on(self).unwrap_err()
        }

        fn expect_err(self, message: &str) -> E
        where
            Self: Sized,
            T: std::fmt::Debug,
        {
            crate::db::block_on(self).expect_err(message)
        }

        fn is_err(self) -> bool
        where
            Self: Sized,
        {
            crate::db::block_on(self).is_err()
        }

        fn map_err<F, O>(self, op: F) -> Result<T, O>
        where
            Self: Sized,
            F: FnOnce(E) -> O,
        {
            crate::db::block_on(self).map_err(op)
        }
    }

    impl<F, T, E> ResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

    pub(crate) trait OptionFutureExt<T>: Future<Output = Option<T>> {
        fn unwrap(self) -> T
        where
            Self: Sized,
        {
            crate::db::block_on(self).unwrap()
        }

        fn expect(self, message: &str) -> T
        where
            Self: Sized,
        {
            crate::db::block_on(self).expect(message)
        }

        fn is_none(self) -> bool
        where
            Self: Sized,
        {
            crate::db::block_on(self).is_none()
        }
    }

    impl<F, T> OptionFutureExt<T> for F where F: Future<Output = Option<T>> {}

    pub(crate) trait SettledNodeTestExt {
        fn commit_mergeable_settled(&mut self, commit: MergeableCommit) -> Result<TxId, Error>;
        fn commit_mergeable_unit_settled(
            &mut self,
            commit: MergeableCommit,
        ) -> Result<(TxId, SyncMessage), Error>;
        fn commit_mergeable_many_settled(
            &mut self,
            commits: Vec<MergeableCommit>,
        ) -> Result<TxId, Error>;
        fn commit_mergeable_on_branch_settled(
            &mut self,
            branch: BranchId,
            commit: MergeableCommit,
        ) -> Result<TxId, Error>;
        fn commit_mergeable_many_on_branch_settled(
            &mut self,
            branch: BranchId,
            commits: Vec<MergeableCommit>,
        ) -> Result<TxId, Error>;
        fn merge_back_branch_settled(&mut self, branch: BranchId) -> Result<TxId, Error>;
        fn merge_back_branch_as_settled(
            &mut self,
            branch: BranchId,
            identity: AuthorId,
        ) -> Result<TxId, Error>;
        fn merge_lineage_into_settled(
            &mut self,
            source: BranchLineage,
            target: BranchLineage,
        ) -> Result<TxId, Error>;
        fn commit_exclusive_settled(
            &mut self,
            tx_id: OpenTransactionId,
            author: AuthorId,
            now_ms: u64,
        ) -> Result<(TxId, SyncMessage), Error>;
        fn apply_sync_message_settled(
            &mut self,
            message: SyncMessage,
        ) -> Result<Vec<SyncMessage>, Error>;
        fn apply_trusted_catalogue_message_settled(
            &mut self,
            message: SyncMessage,
        ) -> Result<Vec<SyncMessage>, Error>;
        fn ingest_commit_unit_settled(
            &mut self,
            tx: Transaction,
            versions: Vec<VersionRecord>,
            now_ms: u64,
        ) -> Result<Vec<SyncMessage>, Error>;
        fn finalize_local_mergeable_commit_settled(&mut self, tx_id: TxId) -> Result<(), Error>;
        fn finalize_local_exclusive_commit_settled(
            &mut self,
            tx: Transaction,
            versions: Vec<VersionRecord>,
        ) -> Result<Fate, Error>;
    }

    impl<S> SettledNodeTestExt for NodeState<S>
    where
        S: OrderedKvStorage + ReopenableStorage,
    {
        fn commit_mergeable_settled(&mut self, commit: MergeableCommit) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.commit_mergeable(commit).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn commit_mergeable_unit_settled(
            &mut self,
            commit: MergeableCommit,
        ) -> Result<(TxId, SyncMessage), Error> {
            crate::db::block_on(async {
                let (published, unit) = self.commit_mergeable_unit(commit).await?;
                let tx_id = self.persist_and_settle_transaction(published).await?;
                Ok((tx_id, unit))
            })
        }

        fn commit_mergeable_many_settled(
            &mut self,
            commits: Vec<MergeableCommit>,
        ) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.commit_mergeable_many(commits).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn commit_mergeable_on_branch_settled(
            &mut self,
            branch: BranchId,
            commit: MergeableCommit,
        ) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.commit_mergeable_on_branch(branch, commit).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn commit_mergeable_many_on_branch_settled(
            &mut self,
            branch: BranchId,
            commits: Vec<MergeableCommit>,
        ) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self
                    .commit_mergeable_many_on_branch(branch, commits)
                    .await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn merge_back_branch_settled(&mut self, branch: BranchId) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.merge_back_branch(branch).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn merge_back_branch_as_settled(
            &mut self,
            branch: BranchId,
            identity: AuthorId,
        ) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.merge_back_branch_as(branch, identity).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn merge_lineage_into_settled(
            &mut self,
            source: BranchLineage,
            target: BranchLineage,
        ) -> Result<TxId, Error> {
            crate::db::block_on(async {
                let published = self.merge_lineage_into(source, target).await?;
                self.persist_and_settle_transaction(published).await
            })
        }

        fn commit_exclusive_settled(
            &mut self,
            tx_id: OpenTransactionId,
            author: AuthorId,
            now_ms: u64,
        ) -> Result<(TxId, SyncMessage), Error> {
            crate::db::block_on(async {
                let (published, unit) = self.commit_exclusive(tx_id, author, now_ms).await?;
                let tx_id = self.persist_and_settle_transaction(published).await?;
                Ok((tx_id, unit))
            })
        }

        fn apply_sync_message_settled(
            &mut self,
            message: SyncMessage,
        ) -> Result<Vec<SyncMessage>, Error> {
            crate::db::block_on(async {
                let outcome = self.apply_sync_message(message).await?;
                self.persist_and_settle_outcome(outcome).await
            })
        }

        fn apply_trusted_catalogue_message_settled(
            &mut self,
            message: SyncMessage,
        ) -> Result<Vec<SyncMessage>, Error> {
            crate::db::block_on(async {
                let outcome = self.apply_trusted_catalogue_message(message).await?;
                self.persist_and_settle_outcome(outcome).await
            })
        }

        fn ingest_commit_unit_settled(
            &mut self,
            tx: Transaction,
            versions: Vec<VersionRecord>,
            now_ms: u64,
        ) -> Result<Vec<SyncMessage>, Error> {
            crate::db::block_on(async {
                let outcome = self.ingest_commit_unit(tx, versions, now_ms).await?;
                self.persist_and_settle_outcome(outcome).await
            })
        }

        fn finalize_local_mergeable_commit_settled(&mut self, tx_id: TxId) -> Result<(), Error> {
            crate::db::block_on(async {
                let outcome = self.finalize_local_mergeable_commit(tx_id).await?;
                self.persist_and_settle_outcome(outcome).await
            })
        }

        fn finalize_local_exclusive_commit_settled(
            &mut self,
            tx: Transaction,
            versions: Vec<VersionRecord>,
        ) -> Result<Fate, Error> {
            crate::db::block_on(async {
                let outcome = self.finalize_local_exclusive_commit(tx, versions).await?;
                self.persist_and_settle_outcome(outcome).await
            })
        }
    }
}

/// Re-export of the underlying groove crate used for storage setup.
pub use groove;

/// Shared, fail-closed state for authority-issued authorization-scope receipts.
pub mod authorization_scope;
/// Shared binary row payload contract for the NAPI and WASM bindings.
pub mod binding_codec;

/// Disabled-by-default counters used by the native cold-settle attribution bench.
#[cfg(feature = "cold-settle-attribution")]
pub mod cold_settle_attribution;
/// High-level thread-affine database facade.
pub mod db;
/// Poll ready-immediate database futures without an async runtime.
pub use db::block_on;
/// Wire-stable identifiers.
pub mod ids;
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
/// Canonical recursive structured query-result boundary types.
pub mod result_tree;
/// Jazz schema and storage lowering.
pub mod schema;
/// Platform-neutral client and server runtime APIs used by target shells.
#[cfg(feature = "runtime")]
pub mod serving;
#[cfg(test)]
mod test_public_schema;
/// Logical time and sequence counters.
pub mod time;
/// Public runtime and data-model support APIs formerly provided by jazz-tools.
// The tools API was a separate crate before consolidation and intentionally
// retains its existing documentation policy.
#[allow(missing_docs)]
pub mod tools;
/// Transaction, fate, and history vocabulary.
pub mod tx;
/// Versioned transport frames around the semantic sync protocol.
pub mod wire;
