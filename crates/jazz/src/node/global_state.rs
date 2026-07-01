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
    pub(super) fn global_layer_winner_at(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        global_base: GlobalSeq,
    ) -> Result<Option<VersionRow>, Error> {
        let prefix = [
            Value::Bytes(table.as_bytes().to_vec()),
            Value::Uuid(row_uuid.0),
            Value::Bytes(version_layer_string(layer).into_bytes()),
        ];
        let upper = [
            Value::Bytes(table.as_bytes().to_vec()),
            Value::Uuid(row_uuid.0),
            Value::Bytes(version_layer_string(layer).into_bytes()),
            Value::U64(global_base.0),
        ];
        let Some(raw) = self.database.primary_key_last_before_or_at_raw(
            "jazz_global_changes",
            &prefix,
            &upper,
        )?
        else {
            return Ok(None);
        };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalChangeRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias = NodeAlias(record.get_u64(GlobalChangeRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias(table, row_uuid, layer, tx_time, tx_node_alias)
    }

    pub(super) fn visible_global_content_tx_id_at(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        global_base: GlobalSeq,
    ) -> Result<Option<TxId>, Error> {
        let deleted = self
            .global_layer_winner_at(table, row_uuid, VersionLayer::Deletion, global_base)?
            .is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted));
        if deleted {
            return Ok(None);
        }
        let Some(content) =
            self.global_layer_winner_at(table, row_uuid, VersionLayer::Content, global_base)?
        else {
            return Ok(None);
        };
        self.version_tx_id(&content).map(Some)
    }

    pub(super) fn global_currency_changed_after(
        &mut self,
        table: &str,
        global_base: GlobalSeq,
    ) -> Result<bool, Error> {
        let Some(start) = global_base.0.checked_add(1) else {
            return Ok(false);
        };
        let mut changed_rows = BTreeSet::new();
        for raw in self.database.index_scan_range_raw(
            "jazz_global_changes",
            "by_global_seq",
            &[Value::U64(start)],
            &[Value::U64(u64::MAX)],
        )? {
            let record = raw.record();
            let changed_table = String::from_utf8(
                record
                    .get_bytes(GlobalChangeRowRecord::FIELD_TABLE_NAME_IDX)?
                    .to_vec(),
            )
            .map_err(|_| Error::InvalidStoredValue("change table name must be utf8"))?;
            if changed_table == table {
                changed_rows.insert(RowUuid(
                    record.get_uuid(GlobalChangeRowRecord::FIELD_ROW_UUID_IDX)?,
                ));
            }
        }
        for row_uuid in changed_rows {
            if self.visible_global_content_tx_id_at(table, row_uuid, global_base)?
                != self.visible_global_content_tx_id_now(table, row_uuid)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn visible_global_content_tx_id_now(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Option<TxId> {
        let table_schema = self.table(table).ok()?.clone();
        let storage_tables = table_schema.global_current_storage_tables();
        let deletion_current_table = &storage_tables[1].name;
        let deletion_descriptor = storage_tables[1].record_schema();
        for raw in self
            .database
            .primary_key_scan_raw(deletion_current_table, &[Value::Uuid(row_uuid.0)])
            .ok()?
        {
            let record = BorrowedRecord::new(raw.record().raw(), &deletion_descriptor);
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

        let content_current_table = &storage_tables[0].name;
        let raw = self
            .database
            .primary_key_scan_raw(content_current_table, &[Value::Uuid(row_uuid.0)])
            .ok()?
            .into_iter()
            .next()?;
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

    /// Return locally held content heads for a row.
    pub(super) fn content_heads(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let versions = self.query_row_versions(table, row_uuid)?;
        let candidate_indices = versions
            .iter()
            .enumerate()
            .filter(|(_, version)| version.layer() == VersionLayer::Content)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        Ok(
            content_head_indices(&versions, &candidate_indices, &self.node_aliases)
                .into_iter()
                .map(|idx| versions[idx].clone())
                .collect(),
        )
    }

    pub(super) fn global_current_updates(&mut self, tx_id: TxId) -> Result<Vec<VersionRow>, Error> {
        let mut updates = BTreeMap::<(String, RowUuid, VersionLayer), VersionRow>::new();
        let version_made_at = self
            .transaction_made_at(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        for version in self.query_versions_for_tx(tx_id)? {
            let previous_current = self.query_global_layer_winner(
                &version.table,
                version.row_uuid(),
                version.layer(),
            )?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous)?,
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
                        version.row_uuid(),
                        version.layer(),
                    ),
                    version,
                );
            }
        }
        Ok(updates.into_values().collect())
    }
}
