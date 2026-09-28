//! Legacy synchronous tests import these traits explicitly while the async API
//! migration is in progress. New async lifecycle tests intentionally do not:
//! they poll futures directly so suspension and ordering remain observable.
#![allow(missing_docs)]

use crate::ids::{AuthorSubject, SchemaVersionId};
use crate::model::transaction::OpenTransactionId;
use crate::node::{ContributionMergeRequest, Error, MergeableCommit, NodeState};
use crate::protocol::{CatalogueSnapshot, SyncMessage, VersionRecord};
use crate::time::{GlobalTime, TxTime};
use crate::tx::{DurabilityTier, Fate, Transaction, TxId};
use groove::storage::{OrderedKvStorage, ReopenableStorage};

pub use jazz_types::legacy_test_future::{FutureResolveExt, OptionFutureExt, ResultFutureExt};

pub trait SettledNodeTestExt {
    fn commit_mergeable_settled(&mut self, commit: MergeableCommit) -> Result<TxId, Error>;
    fn commit_mergeable_unit_settled(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(TxId, SyncMessage), Error>;
    fn commit_mergeable_many_settled(
        &mut self,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error>;
    fn merge_branch_contributions_settled(
        &mut self,
        request: ContributionMergeRequest,
    ) -> Result<Option<TxId>, Error>;
    fn commit_mergeable_in_schema_settled(
        &mut self,
        schema: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error>;
    fn commit_mergeable_at_settled(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error>;
    fn commit_mergeable_open_settled<F>(
        &mut self,
        open: OpenTransactionId,
        next_now_ms: F,
    ) -> Result<TxId, Error>
    where
        F: FnMut() -> u64;
    fn apply_trusted_catalogue_snapshot_settled(
        &mut self,
        snapshot: CatalogueSnapshot,
    ) -> Result<(), Error>;
    fn commit_exclusive_settled(
        &mut self,
        tx_id: OpenTransactionId,
        author: AuthorSubject,
        now_ms: u64,
    ) -> Result<(TxId, SyncMessage), Error>;
    fn apply_sync_message_settled(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error>;
    /// Activate the permissions declared by an admitted fixture schema.
    fn activate_catalogue_schema_settled(
        &mut self,
        pointer: crate::protocol::CurrentWriteSchema,
    ) -> Result<(), Error>;
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
    fn transaction_state_settled(
        &mut self,
        tx_id: TxId,
    ) -> Option<(Fate, Option<GlobalTime>, DurabilityTier)>;
}

impl<S> SettledNodeTestExt for NodeState<S>
where
    S: OrderedKvStorage + ReopenableStorage,
{
    fn commit_mergeable_settled(&mut self, commit: MergeableCommit) -> Result<TxId, Error> {
        crate::local_executor::block_on(async {
            let published = self.commit_mergeable(commit).await?;
            self.persist_and_settle_transaction(published).await
        })
    }

    fn commit_mergeable_unit_settled(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(TxId, SyncMessage), Error> {
        crate::local_executor::block_on(async {
            let (published, unit) = self.commit_mergeable_unit(commit).await?;
            let tx_id = self.persist_and_settle_transaction(published).await?;
            Ok((tx_id, unit))
        })
    }

    fn commit_mergeable_many_settled(
        &mut self,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error> {
        crate::local_executor::block_on(async {
            let published = self.commit_mergeable_many(commits).await?;
            self.persist_and_settle_transaction(published).await
        })
    }

    fn merge_branch_contributions_settled(
        &mut self,
        request: ContributionMergeRequest,
    ) -> Result<Option<TxId>, Error> {
        crate::local_executor::block_on(async {
            let Some(published) = self.merge_branch_contributions(request).await? else {
                return Ok(None);
            };
            self.persist_and_settle_transaction(published)
                .await
                .map(Some)
        })
    }

    fn commit_mergeable_in_schema_settled(
        &mut self,
        schema: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error> {
        crate::local_executor::block_on(async {
            let published = self.commit_mergeable_in_schema(schema, commit).await?;
            self.persist_and_settle_transaction(published).await
        })
    }

    fn commit_mergeable_at_settled(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<TxId, Error> {
        crate::local_executor::block_on(async {
            let published = self.commit_mergeable_at(commit, made_at).await?;
            self.persist_and_settle_transaction(published).await
        })
    }

    fn commit_mergeable_open_settled<F>(
        &mut self,
        open: OpenTransactionId,
        next_now_ms: F,
    ) -> Result<TxId, Error>
    where
        F: FnMut() -> u64,
    {
        crate::local_executor::block_on(async {
            let published = self.commit_mergeable_open(open, next_now_ms).await?;
            self.persist_and_settle_transaction(published).await
        })
    }

    fn apply_trusted_catalogue_snapshot_settled(
        &mut self,
        snapshot: CatalogueSnapshot,
    ) -> Result<(), Error> {
        crate::local_executor::block_on(async {
            let outcome = self.apply_trusted_catalogue_snapshot(snapshot).await?;
            self.persist_and_settle_outcome(outcome).await
        })
    }

    fn commit_exclusive_settled(
        &mut self,
        tx_id: OpenTransactionId,
        author: AuthorSubject,
        now_ms: u64,
    ) -> Result<(TxId, SyncMessage), Error> {
        crate::local_executor::block_on(async {
            let (published, unit) = self.commit_exclusive(tx_id, author, now_ms).await?;
            let tx_id = self.persist_and_settle_transaction(published).await?;
            Ok((tx_id, unit))
        })
    }

    fn apply_sync_message_settled(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error> {
        crate::local_executor::block_on(async {
            let outcome = self.apply_sync_message(message).await?;
            self.persist_and_settle_outcome(outcome).await
        })
    }

    fn activate_catalogue_schema_settled(
        &mut self,
        pointer: crate::protocol::CurrentWriteSchema,
    ) -> Result<(), Error> {
        let schema = self
            .catalogue_schemas()
            .get(&pointer.schema)
            .ok_or(Error::InvalidCatalogueUpdate(
                "fixture schema is not admitted",
            ))?
            .schema
            .clone();
        crate::local_executor::block_on(self.activate_schema(pointer.revision, schema))
    }

    fn apply_trusted_catalogue_message_settled(
        &mut self,
        message: SyncMessage,
    ) -> Result<Vec<SyncMessage>, Error> {
        crate::local_executor::block_on(async {
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
        crate::local_executor::block_on(async {
            let outcome = self.ingest_commit_unit(tx, versions, now_ms).await?;
            self.persist_and_settle_outcome(outcome).await
        })
    }

    fn finalize_local_mergeable_commit_settled(&mut self, tx_id: TxId) -> Result<(), Error> {
        crate::local_executor::block_on(async {
            let outcome = self.finalize_local_mergeable_commit(tx_id).await?;
            self.persist_and_settle_outcome(outcome).await
        })
    }

    fn transaction_state_settled(
        &mut self,
        tx_id: TxId,
    ) -> Option<(Fate, Option<GlobalTime>, DurabilityTier)> {
        crate::local_executor::block_on(self.transaction_state(tx_id))
    }
}
