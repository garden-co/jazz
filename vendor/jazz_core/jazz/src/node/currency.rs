//! Currency and version-selection reads over local and global storage. This
//! module owns row-version scans, winner lookup, transaction lookup, and
//! history queries that implement the merge/currency rules in `jazz/README.md`;
//! write ingestion lives in [`super::ingest`], read-only global derivations in
//! [`super::global_state`], and storage encoding in [`super::codec`]. It is a
//! node-level read layer over groove tables.

use super::*;
use crate::schema::{partition_history_table_name, partition_register_table_name};

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn query_row_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        self.table(table)?;
        let mut versions = Vec::new();
        for (storage_table, descriptor) in self.version_storage_sources(table)? {
            let raws = self
                .database
                .primary_key_scan_raw(&storage_table, &[Value::Uuid(row_uuid.0)])?
                .into_iter()
                .map(|raw| raw.raw().to_vec())
                .collect::<Vec<_>>();
            for bytes in raws {
                versions.push(
                    self.decode_history_record(table, BorrowedRecord::new(&bytes, &descriptor))?,
                );
            }
        }
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, &aliases).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn query_local_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_layer_winner_from_pk(table, row_uuid, layer)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn query_global_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let current_table = match layer {
            VersionLayer::Content => global_current_table_name(table),
            VersionLayer::Deletion => register_global_current_table_name(table),
        };
        let raw = self
            .database
            .primary_key_scan_raw(&current_table, &[Value::Uuid(row_uuid.0)])?;
        let Some(raw) = raw.first() else {
            return Ok(None);
        };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias(table, row_uuid, layer, tx_time, tx_node_alias)
    }

    pub(super) fn query_layer_winner_from_pk(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.table(table)?;
        let mut winner = None;
        for (storage_table, descriptor) in self.version_storage_sources_for_layer(table, layer)? {
            let Some(raw) = self
                .database
                .primary_key_last_raw(&storage_table, &[Value::Uuid(row_uuid.0)])?
                .map(|raw| raw.raw().to_vec())
            else {
                continue;
            };
            let candidate =
                self.decode_history_record(table, BorrowedRecord::new(&raw, &descriptor))?;
            let candidate_tx = self.version_tx_id(&candidate)?;
            if winner.as_ref().is_none_or(|existing: &VersionRow| {
                candidate.tx_time().sort_key(candidate_tx.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            }) {
                winner = Some(candidate);
            }
        }
        Ok(winner)
    }

    #[cfg(test)]
    pub(super) fn query_all_versions(&mut self) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for table in self.catalogue.schema.tables.clone() {
            versions.extend(self.query_table_versions(&table.name)?);
        }
        versions.sort_by_key(|version| {
            (
                version.table,
                version.row_uuid(),
                self.version_tx_id(version).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    pub(super) fn query_table_versions(&mut self, table: &str) -> Result<Vec<VersionRow>, Error> {
        self.table(table)?;
        let mut versions_by_key = BTreeMap::new();
        for (storage_table, descriptor) in self.version_storage_sources(table)? {
            let raws = self
                .database
                .primary_key_scan_raw(&storage_table, &[])?
                .into_iter()
                .map(|raw| raw.raw().to_vec())
                .collect::<Vec<_>>();
            for bytes in raws {
                let version =
                    self.decode_history_record(table, BorrowedRecord::new(&bytes, &descriptor))?;
                let tx_id = self.version_tx_id(&version)?;
                versions_by_key.insert((version.row_uuid(), tx_id, version.layer()), version);
            }
        }
        let mut versions = versions_by_key.into_values().collect::<Vec<_>>();
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, &aliases).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    pub(super) fn query_versions_for_tx(&mut self, tx_id: TxId) -> Result<Vec<VersionRow>, Error> {
        #[cfg(test)]
        record_query_versions_for_tx_call();

        let Some(tx) = self.query_transaction(tx_id)? else {
            return Ok(Vec::new());
        };
        let cached_tables = self.cached_tx_version_tables(tx_id);
        let tables = cached_tables
            .clone()
            .unwrap_or_else(|| self.tx_version_scan_tables());
        let mut versions = Vec::new();
        for table in tables {
            for (storage_table, descriptor) in self.version_storage_sources(&table)? {
                let raws = self
                    .database
                    .index_scan_raw(
                        &storage_table,
                        "by_tx",
                        &[Value::U64(tx_id.time.0), Value::U64(tx.node_alias.0)],
                    )?
                    .into_iter()
                    .map(|raw| raw.raw().to_vec())
                    .collect::<Vec<_>>();
                for raw in raws {
                    let version =
                        self.decode_history_record(&table, BorrowedRecord::new(&raw, &descriptor))?;
                    versions.push(version);
                }
            }
        }
        if cached_tables.is_none() {
            self.cache_tx_version_tables(
                tx_id,
                versions
                    .iter()
                    .map(|version| version.table().to_owned())
                    .collect(),
            );
        }
        versions.sort_by(|left, right| {
            left.table()
                .cmp(right.table())
                .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                .then_with(|| left.layer().cmp(&right.layer()))
        });
        Ok(versions)
    }

    fn tx_version_scan_tables(&self) -> BTreeSet<String> {
        self.catalogue
            .schema
            .tables
            .iter()
            .map(|table| table.name.clone())
            .chain(
                self.catalogue
                    .partitions
                    .iter()
                    .map(|(table, _)| table.clone()),
            )
            .collect()
    }

    fn version_storage_sources(
        &self,
        table: &str,
    ) -> Result<Vec<(String, records::RecordDescriptor)>, Error> {
        let mut sources = Vec::new();
        sources.extend(self.version_storage_sources_for_layer(table, VersionLayer::Content)?);
        sources.extend(self.version_storage_sources_for_layer(table, VersionLayer::Deletion)?);
        Ok(sources)
    }

    fn version_storage_sources_for_layer(
        &self,
        table: &str,
        layer: VersionLayer,
    ) -> Result<Vec<(String, records::RecordDescriptor)>, Error> {
        let base_table = self.table_in_schema(table, self.catalogue.current_schema_version_id)?;
        let mut sources = vec![match layer {
            VersionLayer::Content => (
                history_table_name(table),
                base_table.history_storage_table().record_schema(),
            ),
            VersionLayer::Deletion => (
                register_table_name(table),
                base_table.register_storage_table().record_schema(),
            ),
        }];
        sources.extend(self.partition_storage_sources_for_layer(table, layer)?);
        Ok(sources)
    }

    fn partition_storage_sources_for_layer(
        &self,
        table: &str,
        layer: VersionLayer,
    ) -> Result<Vec<(String, records::RecordDescriptor)>, Error> {
        let mut sources = Vec::new();
        for (logical_table, schema_version) in &self.catalogue.partitions {
            if logical_table != table || *schema_version == self.catalogue.current_schema_version_id
            {
                continue;
            }
            let table_schema = self.table_in_schema(table, *schema_version)?;
            sources.push(match layer {
                VersionLayer::Content => (
                    partition_history_table_name(table, *schema_version),
                    table_schema
                        .history_partition_storage_table(*schema_version)
                        .record_schema(),
                ),
                VersionLayer::Deletion => (
                    partition_register_table_name(table, *schema_version),
                    table_schema
                        .register_partition_storage_table(*schema_version)
                        .record_schema(),
                ),
            });
        }
        Ok(sources)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn decode_history_record(
        &mut self,
        table: &str,
        record: BorrowedRecord<'_>,
    ) -> Result<VersionRow, Error> {
        let table_schema = self.table(table)?.clone();
        let tx_node_alias = if record.descriptor().field_index("_deletion").is_some() {
            NodeAlias(record.get_u64(RegisterRowRecord::FIELD_TX_NODE_ID_IDX)?)
        } else {
            NodeAlias(record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?)
        };
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "history tx node alias must exist",
            ))?;
        let tx_time = if record.descriptor().field_index("_deletion").is_some() {
            TxTime(record.get_u64(RegisterRowRecord::FIELD_TX_TIME_IDX)?)
        } else {
            TxTime(record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?)
        };
        let _ = TxId::new(tx_time, tx_node);
        self.decode_history_record_parts(table, record, &table_schema)
    }

    pub(super) fn decode_history_record_parts(
        &self,
        table: &str,
        record: BorrowedRecord<'_>,
        _table_schema: &TableSchema,
    ) -> Result<VersionRow, Error> {
        Ok(VersionRow {
            table: groove::Intern::new(table.to_owned()),
            record: OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
        })
    }

    pub(super) fn query_transaction(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<StoredTransaction>, Error> {
        if let Some(alias) = self.node_aliases.get(&tx_id.node).copied()
            && let Some(tx) = self.query_transaction_by_alias(tx_id, alias)?
        {
            return Ok(Some(tx));
        }
        let mut aliases = Vec::new();
        for raw in self.database.primary_key_scan_raw("jazz_nodes", &[])? {
            let record = raw.record();
            if record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)? == tx_id.node.0 {
                let alias = NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?);
                aliases.push(alias);
            }
        }
        for expected_alias in aliases {
            if let Some(tx) = self.query_transaction_by_alias(tx_id, expected_alias)? {
                self.node_aliases.insert(tx_id.node, expected_alias);
                return Ok(Some(tx));
            }
        }
        Ok(None)
    }

    fn query_transaction_by_alias(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
    ) -> Result<Option<StoredTransaction>, Error> {
        for raw in self.database.primary_key_scan_raw(
            "jazz_transactions",
            &[Value::U64(tx_id.time.0), Value::U64(expected_alias.0)],
        )? {
            let record = raw.record();
            let node_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
            if node_alias != expected_alias || time != tx_id.time {
                continue;
            }
            let tx = Transaction {
                tx_id,
                kind: tx_kind_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_KIND_IDX)?,
                )?,
                n_total_writes: record.get_u32(TransactionRowRecord::FIELD_N_TOTAL_WRITES_IDX)?,
                made_by: AuthorId(record.get_uuid(TransactionRowRecord::FIELD_MADE_BY_IDX)?),
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: record
                    .get_nullable_string(TransactionRowRecord::FIELD_USER_METADATA_IDX)?
                    .map(str::to_owned),
                source_branch: record
                    .get_nullable_uuid(TransactionRowRecord::FIELD_SOURCE_BRANCH_IDX)?
                    .map(BranchId),
            };
            let fate = fate_from_encoded_fields(record)?;
            return Ok(Some(StoredTransaction {
                tx,
                node_alias,
                fate,
                global_seq: record
                    .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_SEQ_IDX)?
                    .map(GlobalSeq),
                durability: durability_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
                )?,
            }));
        }
        Ok(None)
    }

    pub(super) fn transaction_exists(&self, tx_id: TxId) -> Result<bool, Error> {
        let Some(expected_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Ok(false);
        };
        Ok(!self
            .database
            .primary_key_scan_raw(
                "jazz_transactions",
                &[Value::U64(tx_id.time.0), Value::U64(expected_alias.0)],
            )?
            .is_empty())
    }

    pub(super) fn transaction_exists_memo(
        &mut self,
        tx_id: TxId,
        memo: &mut IngestMemo,
    ) -> Result<bool, Error> {
        if let Some(exists) = memo.tx_exists.get(&tx_id) {
            return Ok(*exists);
        }
        let exists = self.transaction_exists(tx_id)?;
        memo.tx_exists.insert(tx_id, exists);
        Ok(exists)
    }

    pub(super) fn transaction_made_at(&self, tx_id: TxId) -> Result<Option<TxTime>, Error> {
        if !self.node_aliases.contains_key(&tx_id.node) {
            return Ok(None);
        }
        if self.transaction_exists(tx_id)? {
            return Ok(Some(tx_id.time));
        }
        Ok(None)
    }

    pub(super) fn transaction_made_at_memo(
        &mut self,
        tx_id: TxId,
        memo: &mut IngestMemo,
    ) -> Result<Option<TxTime>, Error> {
        if let Some(made_at) = memo.tx_made_at.get(&tx_id) {
            return Ok(*made_at);
        }
        let made_at = self.transaction_made_at(tx_id)?;
        memo.tx_made_at.insert(tx_id, made_at);
        if made_at.is_some() {
            memo.tx_exists.insert(tx_id, true);
        }
        Ok(made_at)
    }

    pub(super) fn version_storage_descriptor(
        &self,
        table: &str,
        layer: VersionLayer,
    ) -> Result<records::RecordDescriptor, Error> {
        let table = self.table(table)?;
        Ok(match layer {
            VersionLayer::Content => table.history_storage_table().record_schema(),
            VersionLayer::Deletion => table.register_storage_table().record_schema(),
        })
    }

    pub(super) fn query_version_by_alias(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let descriptor = self.version_storage_descriptor(table, layer)?;
        self.query_version_by_alias_with_descriptor(
            table,
            row_uuid,
            layer,
            tx_time,
            tx_node_alias,
            &descriptor,
        )
    }

    pub(super) fn query_version_by_alias_with_descriptor(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
        descriptor: &records::RecordDescriptor,
    ) -> Result<Option<VersionRow>, Error> {
        let storage_table = version_storage_table_name(table, layer);
        let raw = self
            .database
            .primary_key_scan_raw(
                &storage_table,
                &[
                    Value::Uuid(row_uuid.0),
                    Value::U64(tx_time.0),
                    Value::U64(tx_node_alias.0),
                ],
            )?
            .first()
            .map(|raw| raw.raw().to_vec());
        let Some(raw) = raw else {
            return Ok(None);
        };
        self.decode_history_record(table, BorrowedRecord::new(&raw, descriptor))
            .map(Some)
    }
}
