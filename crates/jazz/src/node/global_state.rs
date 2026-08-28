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
    /// Return the global winner for a physical history register. Callers which
    /// validate an authored version must resolve its physical table through the
    /// version's schema, rather than interpreting its logical table name in the
    /// current write schema: a migration lens may have renamed that table.
    pub(super) async fn visible_global_layer_tx_id_for_physical_table_now(
        &mut self,
        table_id: PhysicalTableId,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Option<TxId> {
        let current_table = match layer {
            VersionLayer::Content => physical_global_current_table_name(table_id),
            VersionLayer::Deletion => physical_register_global_current_table_name(table_id),
        };
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
            self.physical_table_id_for_schema(self.catalogue.current_schema_version_id, table)?;
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
            self.physical_table_id_for_schema(self.catalogue.current_schema_version_id, table)?;
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

    /// Return the transaction whose row state is currently observed by an
    /// exclusive read: the deleting transaction while deleted, otherwise the
    /// visible content transaction.
    pub(super) async fn visible_global_row_tx_id_now(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let deletion_current_table = self
            .physical_current_table_for_schema(
                schema_version,
                table,
                VersionLayer::Deletion,
                PhysicalCurrentClass::Global,
            )
            .ok()?;
        if let Some(raw) = self
            .database
            .primary_key_get_raw(
                &deletion_current_table,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await
            .ok()?
        {
            let record = raw.record();
            let deletion = deletion_event_from_value(
                record
                    .get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)
                    .ok()?,
            )
            .ok()?;
            if deletion == DeletionEvent::Deleted {
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
                let tx_node = self.node_for_alias(tx_node_alias)?;
                return Some(TxId::new(tx_time, tx_node));
            }
        }
        self.visible_global_content_tx_id_now(table, row_uuid).await
    }

    pub(super) async fn visible_global_content_tx_id_now(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let deletion_current_table = self
            .physical_current_table_for_schema(
                schema_version,
                table,
                VersionLayer::Deletion,
                PhysicalCurrentClass::Global,
            )
            .ok()?;
        if let Some(raw) = self
            .database
            .primary_key_get_raw(
                &deletion_current_table,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await
            .ok()?
        {
            let record = raw.record();
            let deletion = deletion_event_from_value(
                record
                    .get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)
                    .ok()?,
            )
            .ok()?;
            if deletion == DeletionEvent::Deleted {
                return None;
            }
        }

        let content_current_table = self
            .physical_current_table_for_schema(
                schema_version,
                table,
                VersionLayer::Content,
                PhysicalCurrentClass::Global,
            )
            .ok()?;
        let raw = self
            .database
            .primary_key_get_raw(
                &content_current_table,
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
        let tx_node = self.node_for_alias(tx_node_alias)?;
        Some(TxId::new(tx_time, tx_node))
    }

    pub(super) async fn global_current_updates_for_versions(
        &mut self,
        tx_id: TxId,
        versions: &[VersionRow],
    ) -> Result<Vec<VersionRow>, Error> {
        let mut updates = BTreeMap::<(String, BranchKey, RowUuid, VersionLayer), VersionRow>::new();
        let version_made_at = self
            .transaction_made_at(tx_id)
            .await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        for version in versions {
            let authored_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "global version schema alias must exist",
                ))?;
            let previous_current = self
                .query_global_layer_winner_in_schema_and_branch(
                    authored_schema,
                    &version.table,
                    version.branch_key(),
                    version.row_uuid(),
                    version.layer(),
                )
                .await?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous).await?,
                ))
            } else {
                None
            };
            let new_is_current =
                version_wins_over_open_winner(&version, tx_id, version_made_at, previous_winner);
            debug_assert!(
                new_is_current || previous_current.is_some(),
                "clock condition violated: global winner after state update must be the previous winner or stated version"
            );
            if new_is_current {
                updates.insert(
                    (
                        version.table().to_owned(),
                        version.branch_key().clone(),
                        version.row_uuid(),
                        version.layer(),
                    ),
                    version.clone(),
                );
            }
        }
        Ok(updates.into_values().collect())
    }
}
