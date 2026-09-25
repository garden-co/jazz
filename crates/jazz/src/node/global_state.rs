//! Read-only derivations over the settled global layer. This module owns
//! historical global winners, visible global content helpers, content-head
//! discovery, and global-current update derivation; writes to global-current
//! tables remain in [`super::ingest`], winner scans in [`super::currency`], and
//! record encoding in [`super::codec`]. It is the node layer's read side over
//! authority-settled groove state.

use super::*;
use crate::schema::GLOBAL_CURRENT_BY_SEQ_INDEX;

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

    /// Global-current records of `table` whose latest accepted seq is after
    /// `position`, in seq order, read from the current table's `by_seq` index.
    async fn global_current_records_after(
        &mut self,
        table: &str,
        position: GlobalTime,
    ) -> Result<Vec<groove::records::OwnedRecord>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.local_schema_version_id, table)?;
        let current_table = physical_global_current_table_name(table_id);
        let branch = Value::Bytes(BranchKey::default().canonical_bytes());
        Ok(self
            .database
            .index_scan_range_raw(
                &current_table,
                GLOBAL_CURRENT_BY_SEQ_INDEX,
                &[
                    branch.clone(),
                    Value::Nullable(Some(Box::new(Value::U64(position.0.saturating_add(1))))),
                ],
                &[
                    branch,
                    Value::Nullable(Some(Box::new(Value::U64(u64::MAX)))),
                ],
            )
            .await?
            .into_iter()
            .map(|raw| raw.owned_record())
            .collect())
    }

    /// Rows of `table` whose global current changed after `position`.
    pub(super) async fn global_rows_changed_after(
        &mut self,
        table: &str,
        position: GlobalTime,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        let mut rows = BTreeSet::new();
        for record in self.global_current_records_after(table, position).await? {
            rows.insert(RowUuid(
                record
                    .borrowed()
                    .get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?,
            ));
        }
        Ok(rows)
    }

    /// Answer "Q at W" for a single-table supporting set from the `by_seq`
    /// index: rows of `current` whose seq moved past `position` are sent
    /// again, and rows that moved past it but are no longer in the set leave.
    /// Everything at or below the watermark is already held by the receiver.
    pub(crate) async fn supporting_catch_up_after<'a>(
        &mut self,
        table: &str,
        physical_table: crate::ids::GlobalPhysicalTableId,
        current: impl Iterator<Item = &'a crate::protocol::SupportingRow>,
        position: GlobalTime,
        predecessor: [u8; 16],
    ) -> Result<crate::protocol::SupportingRowsUpdate, Error> {
        let mut moved = BTreeMap::new();
        for record in self.global_current_records_after(table, position).await? {
            let record = record.borrowed();
            let alias = NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
            let node = self.node_for_alias(alias).ok_or(Error::InvalidStoredValue(
                "global current node alias must exist",
            ))?;
            let tx_id = TxId::new(
                TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                node,
            );
            moved.insert(
                RowUuid(record.get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?),
                tx_id,
            );
        }
        let mut changed = Vec::new();
        for row in current.filter(|row| row.physical_table == physical_table) {
            if moved.remove(&row.row).is_some() {
                changed.push(row.clone());
            }
        }
        let branch = BranchKey::default().canonical_bytes();
        let left = moved
            .into_iter()
            .map(|(row, tx)| crate::protocol::SupportingRow {
                physical_table,
                version_table: table.to_owned().into(),
                row,
                version: crate::protocol::RowVersionRefEntry {
                    tx,
                    schema_version: None,
                    layer: crate::protocol::ResultRowLayer::Content,
                    batch: Some(tx),
                    branch_or_prefix: (!branch.is_empty()).then(|| branch.clone()),
                    row_digest: None,
                },
            })
            .collect();
        Ok(crate::protocol::SupportingRowsUpdate::CatchUp {
            predecessor,
            revision: *uuid::Uuid::new_v4().as_bytes(),
            changed,
            left,
        })
    }

    /// The largest seq of any accepted change to `table`.
    pub(super) async fn global_table_seq(&mut self, table: &str) -> Result<GlobalTime, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.local_schema_version_id, table)?;
        let current_table = physical_global_current_table_name(table_id);
        let Some(raw) = self
            .database
            .index_last_raw(
                &current_table,
                GLOBAL_CURRENT_BY_SEQ_INDEX,
                &[Value::Bytes(BranchKey::default().canonical_bytes())],
            )
            .await?
        else {
            return Ok(GlobalTime(0));
        };
        Ok(GlobalTime(
            raw.record()
                .get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
                .unwrap_or(0),
        ))
    }

    pub(super) async fn global_currency_changed_after(
        &mut self,
        table: &str,
        global_base: GlobalTime,
    ) -> Result<bool, Error> {
        Ok(self.global_table_seq(table).await? > global_base)
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
        for record in self
            .global_current_records_after(table, snapshot.global_base)
            .await?
        {
            let record = record.borrowed();
            let alias = NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
            let node = self.node_for_alias(alias).ok_or(Error::InvalidStoredValue(
                "global current node alias must exist",
            ))?;
            let tx_id = TxId::new(
                TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
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
