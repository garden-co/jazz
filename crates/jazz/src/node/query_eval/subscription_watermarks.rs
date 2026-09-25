//! Subscription watermark record v1 ("Q at W") uses Groove's native
//! typed-record codec. It names the seq a row-local view was settled through
//! and the supporting revision installed there. The held set itself is not
//! stored: for a row-local view it is the synced local rows that match Q, and
//! any row whose seq moved past W is replaced or dropped by Core's catch-up.

use super::*;
use crate::protocol::{AuthorityResultKey, SupportingRow};
use crate::schema::SUBSCRIPTION_WATERMARKS_STORE;

impl<S: OrderedKvStorage> NodeState<S> {
    /// A view whose membership depends only on each row's own image and on
    /// nothing the session carries, so a catch-up from W is exact.
    pub(crate) fn watermark_catch_up_view(&self, shape: &ValidatedQuery) -> bool {
        let query = shape.query();
        if !crate::peer::row_local_membership(query) || crate::peer::query_uses_claims(query) {
            return false;
        }
        self.table(&query.table).is_ok_and(|table| {
            table.read_policy.as_ref().is_none_or(|policy| {
                crate::peer::row_local_membership(policy) && !crate::peer::query_uses_claims(policy)
            })
        })
    }

    async fn subscription_watermark_key(
        &self,
        key: &AuthorityResultKey,
    ) -> Result<Vec<Value>, Error> {
        let (scope, digest) = match &key.policy_binding {
            Some(policy) => {
                self.persist_policy_binding_directory(policy).await?;
                (1, policy.directory_digest().to_vec())
            }
            None => (0, Vec::new()),
        };
        Ok(vec![
            Value::Uuid(key.binding_view.shape_id.0),
            Value::Uuid(key.binding_view.binding_id.0),
            Value::Uuid(key.binding_view.read_view.id),
            Value::U8(scope),
            Value::Bytes(digest),
        ])
    }

    /// Record that this receiver settled a row-local view through
    /// `settled_through` with `revision` installed.
    pub(crate) async fn purge_subscription_watermarks(&mut self) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(SUBSCRIPTION_WATERMARKS_STORE)?;
        let deletes = store
            .prefix_entries(&[])
            .await?
            .into_iter()
            .map(|entry| groove::db::DirectRecordStoreWrite::Delete { key: entry.key })
            .collect::<Vec<_>>();
        if !deletes.is_empty() {
            store.write_many(&deletes).await?;
        }
        self.query.persisted_watermarks.clear();
        Ok(())
    }

    pub(crate) async fn persist_subscription_watermark(
        &mut self,
        key: &AuthorityResultKey,
        settled_through: GlobalTime,
        revision: [u8; 16],
    ) -> Result<(), Error> {
        self.query.watermark_restore_seen.insert(key.clone());
        if self.query.persisted_watermarks.get(key) == Some(&(settled_through, revision)) {
            return Ok(());
        }
        let store_key = self.subscription_watermark_key(key).await?;
        self.database
            .direct_record_store(SUBSCRIPTION_WATERMARKS_STORE)?
            .set(
                &store_key,
                &[
                    Value::U8(1),
                    Value::U64(settled_through.0),
                    Value::Bytes(revision.to_vec()),
                ],
            )
            .await?;
        self.query
            .persisted_watermarks
            .insert(key.clone(), (settled_through, revision));
        Ok(())
    }

    /// After a reopen, rebuild the receipt a row-local view needs to declare
    /// "Q at W": the stored watermark plus the held set, read from synced
    /// local rows. Returns whether a receipt was restored.
    pub(crate) async fn restore_subscription_watermark(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        key: &AuthorityResultKey,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        if self.query.watermarks_invalidated
            || !self.query.watermark_restore_seen.insert(key.clone())
            || self.query.authority_results.contains_key(key)
            || !self.watermark_catch_up_view(shape)
        {
            return Ok(false);
        }
        let store_key = self.subscription_watermark_key(key).await?;
        let store = self
            .database
            .direct_record_store(SUBSCRIPTION_WATERMARKS_STORE)?;
        let Some(record) = store.get(&store_key).await? else {
            return Ok(false);
        };
        let values = record.to_values()?;
        let [
            Value::U8(1),
            Value::U64(settled_through),
            Value::Bytes(revision),
        ] = values.as_slice()
        else {
            return Err(Error::InvalidStoredValue(
                "invalid subscription watermark record v1",
            ));
        };
        let settled_through = GlobalTime(*settled_through);
        let revision: [u8; 16] = revision.as_slice().try_into().map_err(|_| {
            Error::InvalidStoredValue("subscription watermark revision must be 16 bytes")
        })?;
        drop(values);
        drop(store);
        if settled_through.0 == 0 || revision == [0; 16] {
            return Err(Error::InvalidStoredValue(
                "subscription watermark record v1 must name a settled revision",
            ));
        }
        let table = shape.query().table.clone();
        let physical_table = self.scope_physical_table(shape.schema_version(), &table)?;
        let branch = BranchKey::default().canonical_bytes();
        let mut held = BTreeMap::new();
        for row in self
            .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
            .await?
        {
            let Some(tx) = self.current_row_tx_id(&row).await else {
                continue;
            };
            let input = SupportingRow {
                physical_table,
                version_table: table.clone().into(),
                row: row.row_uuid(),
                version: RowVersionRefEntry {
                    tx,
                    schema_version: None,
                    layer: ResultRowLayer::Content,
                    batch: Some(tx),
                    branch_or_prefix: (!branch.is_empty()).then(|| branch.clone()),
                    row_digest: None,
                },
            };
            held.insert(crate::node::CoveredInputCoordinate::from(&input), input);
        }
        let state = self.query.authority_results.entry(key.clone()).or_default();
        if state.settled_through.is_some() || state.supporting_revision.is_some() {
            return Ok(false);
        }
        state.applied_view_update_generation = state.applied_view_update_generation.max(1);
        state.source_closure = crate::node::AuthoritySourceClosure::Claimed {
            generation: state.applied_view_update_generation,
        };
        state.covered_input_versions = held;
        state.settled_through = Some(settled_through);
        state.supporting_revision = Some(revision);
        Ok(true)
    }
}
