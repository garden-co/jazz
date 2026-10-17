//! Currency and version-selection reads over local and global storage. This
//! module owns row-version scans, winner lookup, transaction lookup, and
//! history queries that implement the merge/currency rules in `jazz/README.md`;
//! write ingestion lives in [`super::ingest`], read-only global derivations in
//! [`super::global_state`], and storage encoding in [`super::codec`]. It is a
//! node-level read layer over groove tables.

use super::*;
use crate::schema::RuntimeSchema;

#[cfg(test)]
thread_local! {
    pub(super) static HISTORY_PAYLOAD_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static TRANSACTION_PAYLOAD_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) async fn query_row_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        self.query_row_versions_in_branch(table, &BranchKey::default(), row_uuid)
            .await
    }

    pub(super) async fn query_row_versions_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for storage_table in self.version_storage_sources(table)? {
            let raws = self
                .history_and_current_records(
                    &storage_table,
                    &[
                        Value::Bytes(branch_key.canonical_bytes()),
                        Value::Uuid(row_uuid.0),
                    ],
                )
                .await?;
            for record in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, record)?);
            }
        }
        let aliases = &self.node_aliases;
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, aliases).expect("valid version tx id"),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_versions_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: Option<RowUuid>,
    ) -> Result<Vec<VersionRow>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        let branch = BranchKey::default();
        let mut content_prefix = vec![Value::Bytes(branch.canonical_bytes())];
        if let Some(row_uuid) = row_uuid {
            content_prefix.push(Value::Uuid(row_uuid.0));
        }
        let mut versions = Vec::new();
        for (storage_table, prefix) in [(physical_history_table_name(table_id), content_prefix)] {
            let records = self
                .history_and_current_records(&storage_table, &prefix)
                .await?;
            for record in records {
                // Keep the authoring name; the schema selected the physical
                // identity before the scan, including after a table rename.
                versions.push(self.decode_stored_physical_history_record(&storage_table, record)?);
            }
        }
        let aliases = &self.node_aliases;
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, aliases).expect("valid version tx id"),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_table_versions_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for storage_table in self.version_storage_sources(table)? {
            let raws = self
                .history_and_current_records(
                    &storage_table,
                    &[Value::Bytes(branch_key.canonical_bytes())],
                )
                .await?;
            for raw in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, raw)?);
            }
        }
        let aliases = &self.node_aliases;
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, aliases).expect("valid version tx id"),
            )
        });
        Ok(versions)
    }

    /// The row's local winner (its newest pending write, else its newest
    /// accepted one). Update and delete permission advice read it through
    /// `local_current_row_exists`.
    pub(super) async fn query_local_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_winner_from_pk(table, row_uuid).await
    }

    pub(super) async fn query_local_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        // The pending overlay, when the row has one, is the local row: it is
        // the synced image with every pending patch folded on top.
        self.query_current_winner_in_branch(
            table,
            branch_key,
            row_uuid,
            PhysicalCurrentClass::Ahead,
        )
        .await
    }

    /// The row as this node sees it locally: the pending overlay when the row
    /// has one, and the synced (global current) image otherwise. A settled
    /// row has no overlay, so an overlay-only read would miss it entirely.
    pub(super) async fn query_local_view_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        if let Some(overlay) = self
            .query_local_winner_in_branch(table, branch_key, row_uuid)
            .await?
        {
            return Ok(Some(overlay));
        }
        self.query_global_winner_in_branch(table, branch_key, row_uuid)
            .await
    }

    /// Return the newest locally known version for a row/layer except one
    /// candidate transaction.  Authority finalization persists its candidate
    /// before assigning fate, so policy classification must be able to find
    /// the next older pending or accepted version rather than falling straight
    /// through to global current state.
    pub(super) async fn query_local_winner_in_branch_excluding_tx(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        excluded_tx_id: TxId,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None;
        for candidate in self
            .query_row_versions_in_branch(table, branch_key, row_uuid)
            .await?
        {
            if self.version_tx_id(&candidate)? == excluded_tx_id {
                continue;
            }
            let candidate_tx = self.version_tx_id(&candidate)?;
            let replace = match winner.as_ref() {
                None => true,
                Some(current) => {
                    let current_tx = self.version_tx_id(current)?;
                    candidate.tx_time().sort_key(candidate_tx.node)
                        > current.tx_time().sort_key(current_tx.node)
                }
            };
            if replace {
                winner = Some(candidate);
            }
        }
        Ok(winner)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) async fn query_global_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = if self
            .table_in_schema_ref(table, self.catalogue.active_schema.schema)
            .is_ok()
        {
            self.catalogue.active_schema.schema
        } else {
            self.table_in_schema_ref(table, self.catalogue.local_schema_version_id)?;
            self.catalogue.local_schema_version_id
        };
        self.query_global_winner_in_schema(schema_version, table, row_uuid)
            .await
    }

    pub(super) async fn query_global_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_current_winner_in_branch(
            table,
            branch_key,
            row_uuid,
            PhysicalCurrentClass::Global,
        )
        .await
    }

    async fn query_current_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        class: PhysicalCurrentClass,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = self.catalogue.active_schema.schema;
        let current_table = self.physical_current_table_for_schema(schema_version, table, class)?;
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?;
        let Some(raw) = raw else { return Ok(None) };
        let variant_tag = raw.variant_tag();
        let current = raw.owned_record();
        if let Some(winner) = self.history_image_from_current_record(
            schema_version,
            table,
            variant_tag,
            current.borrowed(),
        )? {
            return Ok(Some(winner));
        }
        let current = current.borrowed();
        let tx_time = TxTime(current.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(current.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_branch(
            schema_version,
            table,
            branch_key,
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    /// Read the global winner through a specified schema's physical lineage.
    /// Incoming historical versions retain their authored table literal, which
    /// need not exist in the currently selected write/read schema after a
    /// rename.
    pub(super) async fn query_global_winner_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_global_winner_in_schema_and_branch(
            schema_version,
            table,
            &BranchKey::default(),
            row_uuid,
        )
        .await
    }

    pub(super) async fn query_global_winner_in_schema_and_branch(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            PhysicalCurrentClass::Global,
        )?;
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let variant_tag = raw.variant_tag();
        let current = raw.owned_record();
        if let Some(winner) = self.history_image_from_current_record(
            schema_version,
            table,
            variant_tag,
            current.borrowed(),
        )? {
            return Ok(Some(winner));
        }
        let current = current.borrowed();
        let tx_time = TxTime(current.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(current.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_branch(
            schema_version,
            table,
            branch_key,
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    /// Global current stores the winner's full body, so the winner is read
    /// from it rather than re-fetched from history by `(tx_time, node)`;
    /// history holds only superseded images (SPEC 2 §2.7.1), so the row's
    /// latest image is stored nowhere else. The two layouts differ only in
    /// the `global_time` cell and the provenance timestamps: current stores
    /// whole milliseconds, history the same instant as an HLC. Provenance
    /// HLCs are always minted from milliseconds (logical counter zero), so
    /// the conversion is lossless.
    ///
    /// The image is built in the history layout of the current row's own
    /// schema variant (`variant_tag`), not the physical table's widest
    /// layout: once a lineage adds a column, rows of the older schema are
    /// stored narrower than the table. An ahead-current row is the synced
    /// image with pending patches folded on top, so it has no history row
    /// equal to it; callers must not substitute the history row of its
    /// newest pending write.
    pub(super) fn history_image_from_current_record(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        variant_tag: u32,
        current: groove::records::BorrowedRecord<'_>,
    ) -> Result<Option<VersionRow>, Error> {
        let history_table =
            physical_history_table_name(self.physical_table_id_for_schema(schema_version, table)?);
        let Some(record) =
            self.history_record_from_current(&history_table, variant_tag, current)?
        else {
            return Ok(None);
        };
        self.decode_history_owned_record(table, &history_table, record)
            .map(Some)
    }

    /// The history record of `history_table` equal to a global-current
    /// record of the same physical table: the record the row's latest write
    /// would have in history. `None` when the current record's layout is not
    /// a byte-for-byte image of the history variant's.
    pub(super) fn history_record_from_current(
        &self,
        history_table: &str,
        variant_tag: u32,
        current: groove::records::BorrowedRecord<'_>,
    ) -> Result<Option<OwnedRecord>, Error> {
        let Some(history_descriptor) = self
            .database
            .table_schema(history_table)?
            .record_schema_for_variant(variant_tag)
        else {
            return Ok(None);
        };
        let current_descriptor = current.descriptor();
        let global_time_idx = GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX;
        // Across a schema lineage the current table may be a different
        // physical projection than this schema's history table; only the
        // same-lineage layout is a byte-for-byte image of the history row.
        // After `authored_columns` history has `counter_signs`, `seq` and
        // the base fields; current has only the base fields. A settled
        // image's signs are empty, its seq is the current row's, and its
        // base is the one current carries.
        let Some(signs_idx) = history_descriptor.field_index(crate::schema::COUNTER_SIGNS_FIELD)
        else {
            return Ok(None);
        };
        let seq_idx = signs_idx + 1;
        if history_descriptor.field_index(crate::schema::SEQ_FIELD) != Some(seq_idx) {
            return Ok(None);
        }
        let source_of = |index: usize| {
            if index > seq_idx {
                // The base fields follow `authored_columns` in current, and a
                // union row's extra cells follow the base fields in both
                // layouts (SPEC 10 §10.3, "Union row cases").
                index - 1
            } else if index >= global_time_idx {
                index + 1
            } else {
                index
            }
        };
        if current_descriptor.fields().len() + 2 != history_descriptor.fields().len() + 1
            || (0..history_descriptor.fields().len()).any(|index| {
                if index == signs_idx || index == seq_idx {
                    return false;
                }
                let source = source_of(index);
                current_descriptor.fields()[source].value_type
                    != history_descriptor.fields()[index].value_type
            })
        {
            return Ok(None);
        }
        let raw = history_descriptor.create_with_encoded_fields::<Error>(
            current.raw().len() + 16,
            |index, output| {
                if index == signs_idx {
                    history_descriptor.encode_field_into(
                        index,
                        &Value::Bytes(Vec::new()),
                        output,
                    )?;
                    return Ok(());
                }
                if index == seq_idx {
                    let seq = current.get_nullable_u64(global_time_idx)?.unwrap_or(0);
                    history_descriptor.encode_field_into(index, &Value::U64(seq), output)?;
                    return Ok(());
                }
                let value = match index {
                    HistoryRowRecord::FIELD_CREATED_AT_IDX => {
                        Some(current.get_u48(GlobalCurrentRowRecord::FIELD_CREATED_AT_IDX)?)
                    }
                    HistoryRowRecord::FIELD_UPDATED_AT_IDX => {
                        Some(current.get_u48(GlobalCurrentRowRecord::FIELD_UPDATED_AT_IDX)?)
                    }
                    _ => None,
                };
                if let Some(ms) = value {
                    // Both layouts store Unix milliseconds; refuse what the
                    // internal HLC cannot hold.
                    TxTime::from_physical_ms(ms).map_err(|_| {
                        Error::InvalidStoredValue("current provenance ms exceeds HLC range")
                    })?;
                    history_descriptor.encode_field_into(index, &Value::U48(ms), output)?;
                    return Ok(());
                }
                let source = source_of(index);
                let span = current_descriptor.field_span(current.raw(), source)?;
                output.extend_from_slice(&current.raw()[span]);
                Ok(())
            },
        )?;
        Ok(Some(OwnedRecord::new(raw, history_descriptor)))
    }

    /// The row's stored image written by `(tx_time, tx_node_alias)` at one of
    /// `seqs`, as a record of `history_table`. History holds only superseded
    /// images; the row's latest accepted image is its global-current record
    /// (SPEC 2 §2.7.1). So an accepted seq is read from current when current
    /// is at that seq and written by that transaction, and from history
    /// otherwise; seq 0 (a pending record) is read from history. Every read
    /// is a point read.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stored_image_record(
        &self,
        batch: Option<&DatabaseBatch>,
        history_table: &str,
        branch_key: &[u8],
        row_uuid: RowUuid,
        seqs: &[GlobalTime],
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<OwnedRecord>, Error> {
        for seq in seqs {
            if *seq != GlobalTime(0)
                && let Some(record) = self
                    .current_image_record_at(
                        batch,
                        history_table,
                        branch_key,
                        row_uuid,
                        *seq,
                        tx_time,
                        tx_node_alias,
                    )
                    .await?
            {
                return Ok(Some(record));
            }
            let key = history_key_values(branch_key, row_uuid, *seq, tx_time, tx_node_alias);
            let raw = match batch {
                Some(batch) => self
                    .database
                    .primary_key_get_raw_in_batch(batch, history_table, &key)
                    .await?
                    .map(|raw| raw.owned_record()),
                None => self
                    .database
                    .primary_key_get_raw(history_table, &key)
                    .await?
                    .map(|raw| raw.owned_record()),
            };
            if raw.is_some() {
                return Ok(raw);
            }
        }
        Ok(None)
    }

    /// The row's global-current image as a record of `history_table`, when
    /// current is at `seq` and was written by `(tx_time, tx_node_alias)`.
    #[allow(clippy::too_many_arguments)]
    async fn current_image_record_at(
        &self,
        batch: Option<&DatabaseBatch>,
        history_table: &str,
        branch_key: &[u8],
        row_uuid: RowUuid,
        seq: GlobalTime,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<OwnedRecord>, Error> {
        let Some(table_id) = physical_version_table_id(history_table) else {
            return Ok(None);
        };
        let current_table = physical_global_current_table_name(table_id);
        let key = [Value::Bytes(branch_key.to_vec()), Value::Uuid(row_uuid.0)];
        let raw = match batch {
            Some(batch) => self
                .database
                .primary_key_get_raw_in_batch(batch, &current_table, &key)
                .await?
                .map(|raw| (raw.variant_tag(), raw.owned_record())),
            None => self
                .database
                .primary_key_get_raw(&current_table, &key)
                .await?
                .map(|raw| (raw.variant_tag(), raw.owned_record())),
        };
        let Some((variant_tag, current)) = raw else {
            return Ok(None);
        };
        let record = current.borrowed();
        if record.get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)? != Some(seq.0)
            || record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)? != tx_time.0
            || record.get_u32(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)? != tx_node_alias.0
        {
            return Ok(None);
        }
        self.history_record_from_current(history_table, variant_tag, record)
    }

    /// The global-current images under `prefix` (`[]`, `[branch]` or
    /// `[branch, row]`) as records of `history_table`: the latest accepted
    /// image of each row, which history does not hold (SPEC 2 §2.7.1).
    /// Scans of a row's or table's accepted images read these beside history.
    pub(super) async fn current_image_records(
        &self,
        history_table: &str,
        prefix: &[Value],
    ) -> Result<Vec<OwnedRecord>, Error> {
        let Some(table_id) = physical_version_table_id(history_table) else {
            return Ok(Vec::new());
        };
        let current_table = physical_global_current_table_name(table_id);
        if self.database.table_schema(&current_table).is_err() {
            return Ok(Vec::new());
        }
        let raws = self
            .database
            .primary_key_scan_raw(&current_table, prefix)
            .await?
            .into_iter()
            .map(|raw| (raw.variant_tag(), raw.owned_record()))
            .collect::<Vec<_>>();
        let mut records = Vec::with_capacity(raws.len());
        for (variant_tag, current) in raws {
            if current
                .borrowed()
                .get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_TIME_IDX)?
                .is_none()
            {
                continue;
            }
            if let Some(record) =
                self.history_record_from_current(history_table, variant_tag, current.borrowed())?
            {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// History records of `history_table` under `prefix` followed by the
    /// global-current images there: every image the node holds, superseded
    /// and latest. A record history and current both hold (a layout current
    /// cannot convert keeps its history copy) is returned once.
    pub(super) async fn history_and_current_records(
        &self,
        history_table: &str,
        prefix: &[Value],
    ) -> Result<Vec<OwnedRecord>, Error> {
        let mut records = self
            .database
            .primary_key_scan_raw(history_table, prefix)
            .await?
            .into_iter()
            .map(|raw| raw.owned_record())
            .collect::<Vec<_>>();
        let mut keys = records
            .iter()
            .map(history_record_identity)
            .collect::<Result<BTreeSet<_>, Error>>()?;
        for record in self.current_image_records(history_table, prefix).await? {
            if keys.insert(history_record_identity(&record)?) {
                records.push(record);
            }
        }
        Ok(records)
    }

    pub(super) async fn query_winner_from_pk(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_winner_from_pk_in_branch(table, &BranchKey::default(), row_uuid)
            .await
    }

    /// The row's local winner: its newest pending write when it has one, and
    /// otherwise its newest accepted write. History is keyed by seq with
    /// pending writes at seq 0, so the pending range is read first. The
    /// newest accepted write is the row's global-current image (history
    /// holds only superseded images), or history's last key when current
    /// has none.
    pub(super) async fn query_winner_from_pk_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Option<VersionRow>, Error> {
        // (pending, seq, version): a pending write beats an accepted one, a
        // later seq beats an earlier one, then the newer transaction wins.
        let mut winner: Option<(bool, GlobalTime, VersionRow)> = None;
        for storage_table in self.version_storage_sources(table)? {
            let row_prefix = vec![
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
            ];
            let mut pending_prefix = row_prefix.clone();
            pending_prefix.push(Value::U64(0));
            let mut found = self
                .database
                .primary_key_last_raw(&storage_table, &pending_prefix)
                .await?
                .map(|raw| (true, raw.owned_record()));
            if found.is_none() {
                // The row's newest accepted image is its global-current
                // record; history holds only superseded ones (SPEC 2
                // §2.7.1), so it is read only when current has no image.
                found = self
                    .current_image_records(&storage_table, &row_prefix)
                    .await?
                    .pop()
                    .map(|record| (false, record));
            }
            if found.is_none() {
                found = self
                    .database
                    .primary_key_last_raw(&storage_table, &row_prefix)
                    .await?
                    .map(|raw| (false, raw.owned_record()));
            }
            let Some((pending, raw)) = found else {
                continue;
            };
            let candidate = self.decode_history_owned_record(table, &storage_table, raw)?;
            let seq = candidate.seq()?;
            let replace = match winner.as_ref() {
                None => true,
                Some((existing_pending, existing_seq, existing)) => {
                    let candidate_key = candidate
                        .tx_time()
                        .sort_key(self.version_tx_id(&candidate)?.node);
                    let existing_key = existing
                        .tx_time()
                        .sort_key(self.version_tx_id(existing)?.node);
                    (pending, seq, candidate_key) > (*existing_pending, *existing_seq, existing_key)
                }
            };
            if replace {
                winner = Some((pending, seq, candidate));
            }
        }
        Ok(winner.map(|(_, _, version)| version))
    }

    #[cfg(test)]
    pub(super) async fn query_all_versions(&mut self) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for table in self.catalogue.schema.tables.clone() {
            versions.extend(self.query_table_versions(&table.name).await?);
        }
        versions.sort_by_key(|version| {
            (
                version.table,
                version.row_uuid(),
                self.version_tx_id(version).expect("valid version tx id"),
            )
        });
        Ok(versions)
    }

    /// Every image of the table this node holds: history's superseded and
    /// pending records and each row's global-current image.
    pub(super) async fn query_table_versions(
        &mut self,
        table: &str,
    ) -> Result<Vec<VersionRow>, Error> {
        self.query_table_versions_from(table, true).await
    }

    /// The table's history records only: superseded and pending images,
    /// never a row's latest accepted image, which is its global-current
    /// record (SPEC 2 §2.7.1).
    pub(super) async fn query_table_history_versions(
        &mut self,
        table: &str,
    ) -> Result<Vec<VersionRow>, Error> {
        self.query_table_versions_from(table, false).await
    }

    async fn query_table_versions_from(
        &mut self,
        table: &str,
        include_current: bool,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions_by_key = BTreeMap::new();
        for storage_table in self.version_storage_sources(table)? {
            let raws = if include_current {
                self.history_and_current_records(&storage_table, &[])
                    .await?
            } else {
                self.database
                    .primary_key_scan_raw(&storage_table, &[])
                    .await?
                    .into_iter()
                    .map(|raw| raw.owned_record())
                    .collect()
            };
            for record in raws {
                let version = self.decode_history_owned_record(table, &storage_table, record)?;
                let tx_id = self.version_tx_id(&version)?;
                versions_by_key.insert(
                    (version.branch_key().clone(), version.row_uuid(), tx_id),
                    version,
                );
            }
        }
        let mut versions = versions_by_key.into_values().collect::<Vec<_>>();
        let aliases = &self.node_aliases;
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, aliases).expect("valid version tx id"),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_versions_for_tx(
        &mut self,
        tx_id: TxId,
    ) -> Result<Vec<VersionRow>, Error> {
        #[cfg(test)]
        record_query_versions_for_tx_call();

        let Some(tx) = self.query_transaction(tx_id).await? else {
            return Ok(Vec::new());
        };
        self.query_versions_for_tx_header(tx_id, tx.node_alias, tx.global_time)
            .await
    }

    /// [`Self::query_versions_for_tx`] for a transaction whose alias and seq
    /// the caller already read from its record, without assembling (and so
    /// auditing) the whole transaction.
    pub(super) async fn query_versions_for_tx_header(
        &mut self,
        tx_id: TxId,
        node_alias: NodeAlias,
        global_time: Option<GlobalTime>,
    ) -> Result<Vec<VersionRow>, Error> {
        if let Some(mut versions) = self.cached_tx_versions(tx_id) {
            versions.sort_by(|left, right| {
                left.table()
                    .cmp(right.table())
                    .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
            });
            return Ok(versions);
        }
        // The transaction record lists every history row this node stored
        // for it; each is one exact history point read. A listed row that is
        // no longer stored (evicted) is skipped.
        let mut versions = Vec::new();
        let touched_rows = self
            .load_tx_touched_rows(None, tx_id.time, node_alias)
            .await?;
        let seqs = history_seq_candidates(global_time);
        for (table_id, branch_key, row_uuid) in touched_rows.iter() {
            let storage_table = physical_history_table_name(table_id);
            let found = self
                .stored_image_record(
                    None,
                    &storage_table,
                    branch_key,
                    row_uuid,
                    &seqs,
                    tx_id.time,
                    node_alias,
                )
                .await?;
            let Some(record) = found else {
                continue;
            };
            versions.push(self.decode_stored_physical_history_record(&storage_table, record)?);
        }
        versions.sort_by(|left, right| {
            left.table()
                .cmp(right.table())
                .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
        });
        Ok(versions)
    }

    pub(super) async fn query_versions_for_tx_rows_by_alias(
        &mut self,
        tx_id: TxId,
        tx_node_alias: NodeAlias,
        rows: &BTreeSet<(String, RowUuid)>,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for (table, row_uuid) in rows {
            if let Some(version) = self
                .query_version_by_alias(table, *row_uuid, tx_id.time, tx_node_alias)
                .await?
            {
                versions.push(version);
            }
        }
        versions.sort_by(|left, right| {
            left.table()
                .cmp(right.table())
                .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
        });
        Ok(versions)
    }

    pub(super) fn version_storage_sources(&mut self, table: &str) -> Result<Vec<String>, Error> {
        let cache_key = table.to_owned();
        if let Some(sources) = self.query.version_storage_sources_cache.get(&cache_key) {
            return Ok(sources.clone());
        }
        let mut sources = self.physical_version_storage_sources(table);
        sources.sort();
        sources.dedup();
        if sources.is_empty() {
            return Err(Error::TableNotFound(table.to_owned()));
        }
        self.query
            .version_storage_sources_cache
            .insert(cache_key, sources.clone());
        Ok(sources)
    }

    fn physical_version_storage_sources(&self, table: &str) -> Vec<String> {
        self.catalogue
            .physical_mappings
            .values()
            .filter_map(|mapping| mapping.tables.get(table))
            .map(|mapping| physical_history_table_name(mapping.table_id))
            .collect()
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn decode_history_record(
        &mut self,
        table: &str,
        record: BorrowedRecord<'_>,
    ) -> Result<VersionRow, Error> {
        self.decode_history_owned_record(
            table,
            "",
            OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
        )
    }

    /// Decode a record read from a `jazz_physical_{id}_history` table. The
    /// logical table comes from the catalogue mapping of the record's own
    /// schema version, so no table name is passed.
    pub(super) fn decode_stored_physical_history_record(
        &mut self,
        storage_table: &str,
        record: OwnedRecord,
    ) -> Result<VersionRow, Error> {
        if physical_version_table_id(storage_table).is_none() {
            return Err(Error::InvalidStoredValue(
                "history record is not from a physical history table",
            ));
        }
        self.decode_history_owned_record("", storage_table, record)
    }

    fn remember_history_tx_seq(
        &self,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
        record: BorrowedRecord<'_>,
    ) -> Result<Option<GlobalTime>, Error> {
        let global_time = record
            .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
            .map(GlobalTime);
        if let Some(global_time) = global_time {
            let mut seqs = self.history_tx_seqs.borrow_mut();
            if seqs.len() >= HISTORY_TX_SEQ_CACHE_MAX_ENTRIES {
                seqs.clear();
            }
            seqs.insert((tx_time, tx_node_alias), global_time);
        }
        Ok(global_time)
    }

    /// The seq a transaction's history records are stored at: its global
    /// time once it has one (SPEC 2 §2.7.1).
    pub(super) async fn history_tx_seq(
        &self,
        batch: Option<&DatabaseBatch>,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<GlobalTime>, Error> {
        if let Some(seq) = self.history_tx_seqs.borrow().get(&(tx_time, tx_node_alias)) {
            return Ok(Some(*seq));
        }
        let primary_key = [Value::U64(tx_time.0), Value::U32(tx_node_alias.0)];
        let raw = match batch {
            Some(batch) => {
                self.database
                    .primary_key_get_raw_in_batch(batch, "jazz_transactions", &primary_key)
                    .await?
            }
            None => {
                self.database
                    .primary_key_get_raw("jazz_transactions", &primary_key)
                    .await?
            }
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        self.remember_history_tx_seq(tx_time, tx_node_alias, raw.record())
    }

    pub(super) fn decode_history_owned_record(
        &mut self,
        requested_table: &str,
        storage_table: &str,
        record: OwnedRecord,
    ) -> Result<VersionRow, Error> {
        #[cfg(test)]
        HISTORY_PAYLOAD_DECODES.with(|count| count.set(count.get() + 1));
        let record_view = record.borrowed();
        let schema_alias =
            SchemaVersionAlias(record_view.get_u64(HistoryRowRecord::FIELD_SCHEMA_VERSION_IDX)?);
        let schema_version =
            self.schema_version_for_alias(schema_alias)
                .ok_or(Error::InvalidStoredValue(
                    "version storage schema version alias must exist",
                ))?;
        let table = if !storage_table.starts_with("jazz_physical_") {
            requested_table.to_owned()
        } else {
            let table_id = physical_version_table_id(storage_table).ok_or(
                Error::InvalidStoredValue("physical version storage logical table mapping missing"),
            )?;
            self.catalogue
                .physical_mappings
                .get(&schema_version)
                .and_then(|mapping| {
                    mapping.tables.iter().find_map(|(logical_table, mapping)| {
                        (mapping.table_id == table_id).then(|| logical_table.clone())
                    })
                })
                .ok_or(Error::InvalidStoredValue(
                    "physical version storage logical table mapping missing",
                ))?
        };
        let table_schema = self.table_in_schema_ref(&table, schema_version)?;
        let record_view = record.borrowed();
        let tx_node_alias = NodeAlias(record_view.get_u32(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?);
        let tx_node =
            self.node_aliases
                .node_for_alias(tx_node_alias)
                .ok_or(Error::InvalidStoredValue(
                    "history tx node alias must exist",
                ))?;
        let tx_time = TxTime(record_view.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?);
        let _ = TxId::new(tx_time, tx_node);
        let version = VersionRow {
            table: groove::Intern::new(table),
            branch_key: RuntimeSchema::decode_persisted_branch_key(
                table_schema,
                record_view.get_bytes(HistoryRowRecord::FIELD_BRANCH_KEY_IDX)?,
            )
            .map_err(|_| Error::InvalidStoredValue("invalid stored branch key"))?,
            record,
        };
        version.validate_canonical()?;
        Ok(version)
    }

    pub(super) async fn query_transaction(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<StoredTransaction>, Error> {
        let Some((alias, header)) = self
            .query_transaction_fields(tx_id, |_, alias, record| {
                Ok((
                    alias,
                    OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
                ))
            })
            .await?
        else {
            return Ok(None);
        };
        self.stored_transaction_from_header(tx_id, alias, header.borrowed())
            .await
            .map(Some)
    }

    pub(super) async fn query_transaction_state(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<(Fate, Option<GlobalTime>, DurabilityTier)>, Error> {
        // Status is a projection, not a payload audit. Full transaction readers
        // continue validating author and contribution identities independently.
        self.query_transaction_fields(tx_id, |_, _, record| {
            Ok((
                fate_from_encoded_fields(record)?,
                record
                    .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
                    .map(GlobalTime),
                durability_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
                )?,
            ))
        })
        .await
    }

    /// The stored transaction's global time, if the transaction is stored.
    pub(super) async fn query_transaction_global_time(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<Option<GlobalTime>>, Error> {
        self.query_transaction_fields(tx_id, |_, _, record| {
            Ok(record
                .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
                .map(GlobalTime))
        })
        .await
    }

    pub(in crate::node) async fn query_transaction_fields<T>(
        &mut self,
        tx_id: TxId,
        decode: impl Fn(&Self, NodeAlias, BorrowedRecord<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        if let Some(alias) = self.node_aliases.get(&tx_id.node).copied() {
            // Recovery rejects conflicting durable aliases, and alias creation
            // installs this mapping only after persistence. A missing exact
            // transaction cannot be hiding under another alias for this UUID.
            // Do not cache the miss: a later received transaction must be read.
            return self
                .query_transaction_fields_by_alias(tx_id, alias, &decode)
                .await;
        }
        if self.absent_node_alias == Some(tx_id.node) {
            // Preserve the ordinary read's table/poison check even though no
            // storage operation is needed for this proven catalogue absence.
            self.database.table_schema("jazz_nodes")?;
            return Ok(None);
        }
        let mut aliases = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_nodes", &[])
            .await?
        {
            let record = raw.record();
            if record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)? == tx_id.node.0 {
                let alias = NodeAlias(record.get_u32(NodeAliasRowRecord::FIELD_ID_IDX)?);
                aliases.push(alias);
            }
        }
        if aliases.is_empty() {
            self.absent_node_alias = Some(tx_id.node);
            return Ok(None);
        }
        if let [alias] = aliases.as_slice() {
            // Alias identity does not depend on this particular transaction
            // existing. Recover it even when the transaction is still absent.
            self.node_aliases.insert(tx_id.node, *alias);
        }
        for expected_alias in aliases {
            if let Some(tx) = self
                .query_transaction_fields_by_alias(tx_id, expected_alias, &decode)
                .await?
            {
                self.node_aliases.insert(tx_id.node, expected_alias);
                return Ok(Some(tx));
            }
        }
        Ok(None)
    }

    pub(super) async fn preload_transaction_memo(
        &mut self,
        tx_ids: impl IntoIterator<Item = TxId>,
        context: &mut super::policy::ViewEvaluationContext,
    ) -> Result<(), Error> {
        let mut by_alias = BTreeMap::<(NodeUuid, NodeAlias), BTreeSet<TxTime>>::new();
        for tx_id in tx_ids {
            if context.tx_rows.contains_key(&tx_id) {
                continue;
            }
            if let Some(alias) = self.node_aliases.get(&tx_id.node).copied() {
                by_alias
                    .entry((tx_id.node, alias))
                    .or_default()
                    .insert(tx_id.time);
            } else {
                let tx = self.query_transaction(tx_id).await?;
                context.tx_rows.insert(tx_id, tx);
            }
        }

        for ((node, alias), times) in by_alias {
            if times.len() == 1 {
                let time = *times.iter().next().expect("non-empty time set");
                let tx_id = TxId::new(time, node);
                let tx = self.query_transaction_by_alias(tx_id, alias).await?;
                context.tx_rows.insert(tx_id, tx);
                continue;
            }

            let min_time = times.iter().next().expect("non-empty time set");
            let max_time = times.iter().next_back().expect("non-empty time set");
            let Some(end_time) = max_time.0.checked_add(1) else {
                for time in times {
                    let tx_id = TxId::new(time, node);
                    let tx = self.query_transaction_by_alias(tx_id, alias).await?;
                    context.tx_rows.insert(tx_id, tx);
                }
                continue;
            };

            for time in &times {
                context.tx_rows.insert(TxId::new(*time, node), None);
            }
            for raw in self
                .database
                .primary_key_scan_range_raw(
                    "jazz_transactions",
                    &[Value::U64(min_time.0), Value::U32(0)],
                    &[Value::U64(end_time), Value::U32(0)],
                )
                .await?
            {
                let record = raw.record();
                let row_alias = NodeAlias(record.get_u32(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
                let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
                if row_alias != alias || !times.contains(&time) {
                    continue;
                }
                let tx_id = TxId::new(time, node);
                let tx = self
                    .stored_transaction_from_header(tx_id, alias, record)
                    .await?;
                context.tx_rows.insert(tx_id, Some(tx));
            }
        }
        Ok(())
    }

    async fn query_transaction_by_alias(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
    ) -> Result<Option<StoredTransaction>, Error> {
        let Some((alias, header)) = self
            .query_transaction_fields_by_alias(tx_id, expected_alias, &|_, alias, record| {
                Ok((
                    alias,
                    OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
                ))
            })
            .await?
        else {
            return Ok(None);
        };
        self.stored_transaction_from_header(tx_id, alias, header.borrowed())
            .await
            .map(Some)
    }

    async fn query_transaction_fields_by_alias<T>(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
        decode: &impl Fn(&Self, NodeAlias, BorrowedRecord<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let Some(raw) = self
            .database
            .primary_key_get_raw(
                "jazz_transactions",
                &[Value::U64(tx_id.time.0), Value::U32(expected_alias.0)],
            )
            .await?
        else {
            return Ok(None);
        };
        let record = raw.record();
        let node_alias = NodeAlias(record.get_u32(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
        let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
        if node_alias != expected_alias || time != tx_id.time {
            return Ok(None);
        }
        self.remember_history_tx_seq(time, node_alias, record)?;
        decode(self, expected_alias, record).map(Some)
    }

    /// Assemble a stored transaction from its `jazz_transactions` record and
    /// the records beside it: its `jazz_pending_transactions` record while it
    /// is unsettled, and its `jazz_tx_provenance` record when its flags say it
    /// has one (SPEC 2 §2.8).
    async fn stored_transaction_from_header(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
        record: BorrowedRecord<'_>,
    ) -> Result<StoredTransaction, Error> {
        #[cfg(test)]
        TRANSACTION_PAYLOAD_DECODES.with(|count| count.set(count.get() + 1));
        let kind =
            tx_kind_from_discriminant(record.get_enum(TransactionRowRecord::FIELD_KIND_IDX)?)?;
        let flags = transaction_flags(record)?;
        let made_by =
            RowAuthor::from_record(record.get_record(TransactionRowRecord::FIELD_MADE_BY_IDX)?)
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?
                .as_author_subject();
        let permission_subject = transaction_permission_subject(record, made_by)?;
        let fate = fate_from_encoded_fields(record)?;
        let global_time = record
            .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
            .map(GlobalTime);
        let key = [Value::U64(tx_id.time.0), Value::U32(expected_alias.0)];
        let pending = if global_time.is_none() && !matches!(fate, Fate::Rejected(_)) {
            self.database
                .primary_key_get_raw(PENDING_TRANSACTIONS_TABLE, &key)
                .await?
        } else {
            None
        };
        let evidence = match &pending {
            Some(raw) => {
                let pending = raw.record();
                let evidence_slot = |idx: usize| -> Result<Option<Vec<u8>>, Error> {
                    match pending.get_idx(idx)? {
                        Value::Nullable(None) => Ok(None),
                        Value::Nullable(Some(value)) => match *value {
                            Value::Bytes(bytes) => Ok(Some(bytes)),
                            _ => Err(Error::InvalidStoredValue(
                                "exclusive read evidence slot is not bytes",
                            )),
                        },
                        _ => Err(Error::InvalidStoredValue(
                            "exclusive read evidence slot is not nullable",
                        )),
                    }
                };
                let slots = [
                    evidence_slot(PendingTransactionRowRecord::FIELD_BASE_SNAPSHOT_IDX)?,
                    evidence_slot(PendingTransactionRowRecord::FIELD_ROW_READ_SET_IDX)?,
                    evidence_slot(PendingTransactionRowRecord::FIELD_ABSENT_READ_SET_IDX)?,
                    evidence_slot(PendingTransactionRowRecord::FIELD_PREDICATE_READ_SET_IDX)?,
                ];
                if kind != TxKind::Exclusive && slots.iter().any(Option::is_some) {
                    return Err(Error::InvalidStoredValue(
                        "mergeable transaction carries exclusive read evidence",
                    ));
                }
                if !matches!(fate, Fate::Pending) && slots.iter().any(Option::is_some) {
                    return Err(Error::InvalidStoredValue(
                        "settled transaction keeps exclusive read evidence",
                    ));
                }
                super::exclusive_read_evidence::decode_evidence_slots(
                    slots[0].as_deref(),
                    slots[1].as_deref(),
                    slots[2].as_deref(),
                    slots[3].as_deref(),
                )?
            }
            None => super::exclusive_read_evidence::decode_evidence_slots(None, None, None, None)?,
        };
        let contribution_merge = if flags & TX_FLAG_HAS_PROVENANCE != 0 {
            let raw = self
                .database
                .primary_key_get_raw(TX_PROVENANCE_TABLE, &key)
                .await?
                .ok_or(Error::InvalidStoredValue(
                    "transaction flags name a missing provenance record",
                ))?;
            let provenance = raw.record();
            Some(self.contribution_merge_from_storage_record(
                <OwnedRecord as records::RecordField>::read(
                    &provenance,
                    TxProvenanceRowRecord::FIELD_CONTRIBUTION_MERGE_IDX,
                )?,
            )?)
        } else {
            None
        };
        let tx = Transaction {
            tx_id,
            kind,
            n_total_writes: record.get_u32(TransactionRowRecord::FIELD_N_TOTAL_WRITES_IDX)?,
            made_by,
            permission_subject,
            base_snapshot: evidence.base_snapshot,
            row_read_set: evidence.row_read_set,
            absent_read_set: evidence.absent_read_set,
            predicate_read_set: evidence.predicate_read_set,
            user_metadata_json: record
                .get_nullable_string(TransactionRowRecord::FIELD_USER_METADATA_IDX)?
                .map(str::to_owned),
            contribution_merge,
        };
        // Recovery and ordinary durable reads must fail closed on malformed
        // strategy-defined contribution identities before they can influence
        // a later merge calculation.
        self.validate_contribution_merge_operation_identities(&tx)?;
        Ok(StoredTransaction {
            tx,
            node_alias: expected_alias,
            fate,
            global_time,
            durability: durability_from_discriminant(
                record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
            )?,
            is_partial: flags & TX_FLAG_VIEW_SCOPED_CARDINALITY != 0,
        })
    }

    pub(super) async fn transaction_exists(&self, tx_id: TxId) -> Result<bool, Error> {
        let Some(expected_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Ok(false);
        };
        Ok(self
            .database
            .primary_key_get_raw(
                "jazz_transactions",
                &[Value::U64(tx_id.time.0), Value::U32(expected_alias.0)],
            )
            .await?
            .is_some())
    }

    pub(super) async fn query_version_by_alias(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        for storage_table in self.version_storage_sources(table)? {
            if let Some(version) = self
                .query_version_by_alias_with_storage(
                    table,
                    &storage_table,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                )
                .await?
            {
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    /// Resolve an exact historical witness through the schema that authored
    /// its table literal. Shared deletion history is keyed by physical table,
    /// so an old `todos` witness must not construct its prefix via renamed
    /// current `tasks` schema state.
    pub(super) async fn query_version_by_alias_in_branch(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let storage_table =
            physical_history_table_name(self.physical_table_id_for_schema(schema_version, table)?);
        self.query_version_by_alias_with_storage_in_schema(
            schema_version,
            table,
            &storage_table,
            branch_key,
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_version_by_alias_with_storage(
        &mut self,
        table: &str,
        storage_table: &str,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = if self
            .table_in_schema_ref(table, self.catalogue.active_schema.schema)
            .is_ok()
        {
            self.catalogue.active_schema.schema
        } else {
            self.catalogue.local_schema_version_id
        };
        self.query_version_by_alias_with_storage_in_schema(
            schema_version,
            table,
            storage_table,
            &BranchKey::default(),
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_version_by_alias_with_storage_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        storage_table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let _ = schema_version;
        let seqs = history_seq_candidates(self.history_tx_seq(None, tx_time, tx_node_alias).await?);
        let raw = self
            .stored_image_record(
                None,
                storage_table,
                &branch_key.canonical_bytes(),
                row_uuid,
                &seqs,
                tx_time,
                tx_node_alias,
            )
            .await?;
        let Some(record) = raw else {
            return Ok(None);
        };
        self.decode_history_owned_record(table, storage_table, record)
            .map(Some)
    }
}
