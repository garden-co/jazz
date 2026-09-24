//! Read-only derivations over the settled global layer. This module owns
//! historical global winners, visible global content helpers, content-head
//! discovery, and global-current update derivation; writes to global-current
//! tables remain in [`super::ingest`], winner scans in [`super::currency`], and
//! record encoding in [`super::codec`]. It is the node layer's read side over
//! authority-settled groove state.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Return the global winner for a physical row lineage. Callers which
    /// validate an authored version must resolve its physical table through the
    /// version's schema, rather than interpreting its logical table name in the
    /// current write schema: a migration lens may have renamed that table.
    pub(super) async fn visible_global_tx_id_for_physical_table_now(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        let current_table = physical_global_current_table_name(table_id);
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await
            .ok()??;
        let record = raw.record();
        let tx_time = TxTime(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                .ok()?,
        );
        let tx_node_alias = NodeAlias(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                .ok()?,
        );
        Some(TxId::new(tx_time, self.node_for_alias(tx_node_alias)?))
    }

    pub(super) async fn global_currency_changed_after(
        &mut self,
        table: &str,
        global_base: GlobalTime,
    ) -> Result<bool, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.local_schema_version_id, table)?;
        let Some(raw) = self
            .database
            .index_last_raw(
                "jazz_global_changes",
                "by_table_global_time",
                &[
                    Value::U64(table_id.0),
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                ],
            )
            .await?
        else {
            return Ok(false);
        };
        let record = raw.record();
        Ok(record.get_u64(GlobalChangeRowRecord::FIELD_GLOBAL_TIME_IDX)? > global_base.0)
    }

    pub(super) async fn global_currency_changed_outside_snapshot(
        &mut self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<bool, Error> {
        if snapshot.dots.is_empty() {
            return self
                .global_currency_changed_after(table, snapshot.global_base)
                .await;
        }
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.local_schema_version_id, table)?;
        let records = self
            .database
            .index_scan_raw(
                "jazz_global_changes",
                "by_table_global_time",
                &[Value::U64(table_id.0)],
            )
            .await?
            .into_iter()
            .map(|raw| raw.owned_record())
            .collect::<Vec<_>>();
        for record in records {
            let record = record.borrowed();
            if record.get_u64(GlobalChangeRowRecord::FIELD_GLOBAL_TIME_IDX)?
                <= snapshot.global_base.0
            {
                continue;
            }
            let alias = NodeAlias(record.get_u64(GlobalChangeRowRecord::FIELD_TX_NODE_ID_IDX)?);
            let node = self.node_for_alias(alias).ok_or(Error::InvalidStoredValue(
                "global change node alias must exist",
            ))?;
            let tx_id = TxId::new(
                TxTime(record.get_u64(GlobalChangeRowRecord::FIELD_TX_TIME_IDX)?),
                node,
            );
            if !self.snapshot_covers(tx_id, snapshot).await {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return the transaction whose row image is currently globally visible,
    /// together with that image's deletion state.
    async fn visible_global_current_now(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<(TxId, bool)> {
        let current_table = self
            .physical_current_table_for_schema(schema_version, table, PhysicalCurrentClass::Global)
            .ok()?;
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await
            .ok()??;
        let record = raw.record();
        let tx_time = TxTime(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                .ok()?,
        );
        let tx_node_alias = NodeAlias(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                .ok()?,
        );
        let deleted = record
            .get_nullable_enum(GlobalCurrentRowRecord::FIELD__DELETION_IDX)
            .ok()?
            .is_some_and(|tag| {
                matches!(
                    deletion_event_from_value(Value::EnumTag(tag)),
                    Ok(DeletionEvent::Deleted)
                )
            });
        let tx_node = self.node_for_alias(tx_node_alias)?;
        Some((TxId::new(tx_time, tx_node), deleted))
    }

    /// Return the transaction whose row state is currently observed by an
    /// exclusive read: the transaction of the current row image, deleted or not.
    pub(super) async fn visible_global_row_tx_id_now(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        self.visible_global_current_now(schema_version, table, row_uuid)
            .await
            .map(|(tx_id, _)| tx_id)
    }

    pub(super) async fn visible_global_content_tx_id_in_schema_now(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        self.visible_global_current_now(schema_version, table, row_uuid)
            .await
            .and_then(|(tx_id, deleted)| (!deleted).then_some(tx_id))
    }

    /// Apply this accepted transaction's writes to global current, merging
    /// each into the row's post-image per column.
    pub(super) async fn global_current_updates_for_versions(
        &mut self,
        batch: &DatabaseBatch,
        tx_id: TxId,
        versions: &[VersionRow],
    ) -> Result<Vec<VersionRow>, Error> {
        let mut updates = BTreeMap::<(String, BranchKey, RowUuid), VersionRow>::new();
        for version in versions {
            let authored_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "global version schema alias must exist",
                ))?;
            let table_schema = self.table_in_schema(version.table(), authored_schema)?;
            if let Some(merged) = self
                .merged_global_post_image(batch, authored_schema, &table_schema, version, tx_id)
                .await?
            {
                updates.insert(
                    (
                        version.table().to_owned(),
                        version.branch_key().clone(),
                        version.row_uuid(),
                    ),
                    merged,
                );
            }
        }
        Ok(updates.into_values().collect())
    }
}
