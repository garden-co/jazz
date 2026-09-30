//! Startup recovery and durable-state rehydration for a node. This module owns
//! rebuilding aliases, schema/lens catalogues, pending edges,
//! rejected payloads, and peer/subscription state from groove storage; normal
//! ingestion lives in [`super::ingest`], storage record layouts in
//! [`super::codec`]. It is the node layer's bridge from persisted groove tables
//! back to in-memory state.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn rejected_versions_for(
        &mut self,
        alias: NodeAlias,
        tx_id: TxId,
    ) -> Result<Vec<RejectedVersion>, Error> {
        let mut versions = Vec::new();
        for table_id in self.physical_table_ids() {
            let storage_table = physical_rejected_versions_table_name(table_id);
            for raw in self
                .database
                .primary_key_scan_raw(
                    &storage_table,
                    &[Value::U64(tx_id.time.0), Value::U64(alias.0)],
                )
                .await?
            {
                let record = raw.record();
                let node_id = record.get_u64(RejectedVersionRowRecord::FIELD_TX_NODE_ID_IDX)?;
                let time = record.get_u64(RejectedVersionRowRecord::FIELD_TX_TIME_IDX)?;
                if node_id != alias.0 || time != tx_id.time.0 {
                    continue;
                }
                let schema_alias = SchemaVersionAlias(u64::from(raw.variant_tag()));
                let schema_version = self.schema_version_for_alias(schema_alias).ok_or(
                    Error::InvalidStoredValue("rejected row schema version alias missing"),
                )?;
                let logical_table =
                    self.logical_table_for_physical_alias(table_id, schema_alias)?;
                let logical_descriptor = self
                    .table_in_schema_ref(&logical_table, schema_version)?
                    .rejected_versions_storage_table()
                    .record_schema();
                versions.push(RejectedVersion::new(
                    logical_table,
                    OwnedRecord::new(raw.raw().to_vec(), logical_descriptor),
                ));
            }
        }
        versions.sort_by_key(|version| {
            (
                version.table(),
                version.row_uuid(),
                version.deletion().is_some(),
            )
        });
        Ok(versions)
    }

    pub(super) async fn recover_from_storage(&mut self) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        {
            self.recover_from_storage_inner(None).await
        }
        #[cfg(not(feature = "testing"))]
        self.recover_from_storage_inner().await
    }

    #[cfg(feature = "testing")]
    pub(super) async fn recover_from_storage_with_receipt(
        &mut self,
        receipt: &mut NodeOpenReceipt,
    ) -> Result<(), Error> {
        self.recover_from_storage_inner(Some(receipt)).await
    }

    async fn recover_from_storage_inner(
        &mut self,
        #[cfg(feature = "testing")] mut receipt: Option<&mut NodeOpenReceipt>,
    ) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        for raw in self
            .database
            .primary_key_scan_raw("jazz_nodes", &[])
            .await?
        {
            let record = raw.record();
            let alias = record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?;
            let uuid = NodeUuid(record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)?);
            if let Some(existing) = self.node_aliases.get(&uuid) {
                if *existing != NodeAlias(alias) {
                    return Err(Error::InvalidStoredValue(
                        "node UUID has conflicting durable aliases",
                    ));
                }
            }
            if self
                .node_aliases
                .node_for_alias(NodeAlias(alias))
                .is_some_and(|existing_uuid| existing_uuid != uuid)
            {
                return Err(Error::InvalidStoredValue(
                    "node alias maps to multiple durable UUIDs",
                ));
            }
            self.node_aliases.insert(uuid, NodeAlias(alias));
        }
        let alias_to_node = self
            .node_aliases
            .iter()
            .map(|(node, alias)| (*alias, *node))
            .collect::<BTreeMap<_, _>>();

        // The transaction-clock high-water mark is the newest stored
        // transaction: `jazz_transactions` is keyed `(time, node_id)`, and every
        // history row is written in the same batch as its transaction record,
        // so no history version can carry a later `tx_time`.
        if let Some(raw) = self
            .database
            .primary_key_last_raw("jazz_transactions", &[])
            .await?
        {
            self.merge_tx_time(TxTime(
                raw.record().get_u64(TransactionRowRecord::FIELD_TIME_IDX)?,
            ));
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_catalogue_state = started.elapsed();
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let mut accepted_global_times = Vec::new();
        #[cfg(feature = "testing")]
        let mut global_time_records_scanned = 0usize;
        // Nullable index keys order `None` before `Some`. Range over only the
        // `Some` bucket so local pending/rejected transactions cannot make
        // recovery O(total transactions). The range end is exclusive, hence
        // the separate exact lookup preserves the prior u64::MAX behavior.
        let first_global_time = Value::Nullable(Some(Box::new(Value::U64(0))));
        let last_global_time = Value::Nullable(Some(Box::new(Value::U64(u64::MAX))));
        let mut sequenced_transactions = self
            .database
            .index_scan_range_raw(
                "jazz_transactions",
                "by_global_time",
                std::slice::from_ref(&first_global_time),
                std::slice::from_ref(&last_global_time),
            )
            .await?;
        sequenced_transactions.extend(
            self.database
                .index_scan_raw(
                    "jazz_transactions",
                    "by_global_time",
                    std::slice::from_ref(&last_global_time),
                )
                .await?,
        );
        for raw in sequenced_transactions {
            #[cfg(feature = "testing")]
            {
                global_time_records_scanned += 1;
            }
            let record = raw.record();
            let global_time =
                record.get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?;
            if global_time.is_some()
                && durability_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
                )? != DurabilityTier::Global
            {
                return Err(Error::InvalidStoredValue(
                    "global timestamp requires Global durability",
                ));
            }
            if !matches!(fate_from_encoded_fields(record)?, Fate::Accepted) {
                continue;
            }
            if let Some(global_time) = global_time {
                let node = self
                    .node_for_alias(NodeAlias(
                        record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?,
                    ))
                    .ok_or(Error::InvalidStoredValue(
                        "transaction node alias must exist",
                    ))?;
                accepted_global_times.push((
                    GlobalTime(global_time),
                    TxId::new(
                        TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
                        node,
                    ),
                ));
            }
        }
        accepted_global_times.sort();
        accepted_global_times.dedup();
        #[cfg(feature = "testing")]
        if let Some(receipt) = &mut receipt {
            receipt.accepted_global_times = accepted_global_times.len();
            receipt.global_time_records_scanned = global_time_records_scanned;
        }
        for (global_time, tx_id) in accepted_global_times {
            self.record_applied_global_time(global_time, tx_id);
        }
        if self.history_complete {
            self.clock.committed_global_time = self.clock.global_time_register;
            self.clock.applied_global_times_after_frontier.clear();
            self.clock.frontier_dots.clear();
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_global_times = started.elapsed();
        }

        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let mut rejected_headers = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_rejected_transactions", &[])
            .await?
        {
            let record = raw.record();
            let node_alias =
                NodeAlias(record.get_u64(RejectedTransactionRowRecord::FIELD_NODE_ID_IDX)?);
            let node = *alias_to_node
                .get(&node_alias)
                .ok_or(Error::InvalidStoredValue(
                    "rejected transaction node alias must exist",
                ))?;
            if node != self.node_uuid {
                continue;
            }
            let tx_id = TxId::new(
                TxTime(record.get_u64(RejectedTransactionRowRecord::FIELD_TIME_IDX)?),
                node,
            );
            rejected_headers.push((
                node_alias,
                tx_id,
                OwnedRecord::new(raw.raw().to_vec(), record.descriptor()),
            ));
        }
        for (node_alias, tx_id, record) in rejected_headers {
            let versions = self.rejected_versions_for(node_alias, tx_id).await?;
            self.rejections
                .rejected_transactions
                .insert(tx_id, RejectedTransaction::new(tx_id, record, versions));
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_pending_and_rejected = started.elapsed();
        }
        Ok(())
    }
}

// The current API cannot author a legacy edge receipt. Only this test fixture
// writes the retired tag; the replay tests use normal Db reopen and transport.
#[cfg(any(test, feature = "testing"))]
impl<S: OrderedKvStorage> NodeState<S> {
    #[doc(hidden)]
    pub async fn persist_legacy_edge_receipt_for_test(&mut self, tx_id: TxId) {
        let stored = self.query_transaction(tx_id).await.unwrap().unwrap();
        let mut values = transaction_values(
            stored.node_alias,
            &stored.tx,
            Fate::Accepted,
            None,
            DurabilityTier::Local,
            Value::Nullable(None),
            &stored.touched_rows,
        )
        .unwrap();
        values[TransactionRowRecord::FIELD_DURABILITY_IDX] = Value::EnumTag(2);
        let mut batch = self.database.open_batch();
        batch.update("jazz_transactions", values);
        let applied = self.apply_node_batch(batch).await.unwrap();
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted).unwrap();
    }
}
