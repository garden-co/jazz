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

        if let Some(raw) = self
            .database
            .primary_key_last_raw("jazz_transactions", &[])
            .await?
        {
            self.merge_tx_time(TxTime(
                raw.record().get_u64(TransactionRowRecord::FIELD_TIME_IDX)?,
            ));
        }
        let physical_table_ids = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        for table_id in physical_table_ids {
            if let Some(raw) = self
                .database
                .index_last_raw(&physical_history_table_name(table_id), "by_tx", &[])
                .await?
            {
                self.merge_tx_time(TxTime(
                    raw.record().get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?,
                ));
            }
        }
        if let Some(raw) = self
            .database
            .index_last_raw(SHARED_DELETION_HISTORY_TABLE, "by_tx", &[])
            .await?
        {
            self.merge_tx_time(TxTime(
                raw.record()
                    .get_u64(SharedDeletionHistoryRowRecord::FIELD_TX_TIME_IDX)?,
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
                accepted_global_times.push(GlobalTime(global_time));
            }
        }
        accepted_global_times.sort();
        accepted_global_times.dedup();
        #[cfg(feature = "testing")]
        if let Some(receipt) = &mut receipt {
            receipt.accepted_global_times = accepted_global_times.len();
            receipt.global_time_records_scanned = global_time_records_scanned;
        }
        for global_time in accepted_global_times {
            self.record_applied_global_time(global_time);
        }
        if self.history_complete {
            self.clock.committed_global_time = self.clock.global_time_register;
            self.clock.applied_global_times_after_frontier.clear();
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_global_times = started.elapsed();
        }

        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let mut pending_edges = Vec::new();
        let mut pending_parent_time_bound = PendingParentTimeBound::Empty;
        for raw in self
            .database
            .primary_key_scan_raw("jazz_pending_edges", &[])
            .await?
        {
            let record = raw.record();
            let child_alias =
                NodeAlias(record.get_u64(PendingEdgeRowRecord::FIELD_CHILD_NODE_ID_IDX)?);
            let parent_alias =
                NodeAlias(record.get_u64(PendingEdgeRowRecord::FIELD_PARENT_NODE_ID_IDX)?);
            let Some(child_node) = alias_to_node.get(&child_alias).copied() else {
                return Err(Error::InvalidStoredValue(
                    "pending edge child alias must exist",
                ));
            };
            let Some(parent_node) = alias_to_node.get(&parent_alias).copied() else {
                return Err(Error::InvalidStoredValue(
                    "pending edge parent alias must exist",
                ));
            };
            let child = TxId::new(
                TxTime(record.get_u64(PendingEdgeRowRecord::FIELD_CHILD_TIME_IDX)?),
                child_node,
            );
            let parent = TxId::new(
                TxTime(record.get_u64(PendingEdgeRowRecord::FIELD_PARENT_TIME_IDX)?),
                parent_node,
            );
            // Validate the persisted row-version coordinate even though the
            // in-memory rejection graph only needs TxIds. Otherwise a corrupt
            // pending constraint could silently survive reopen.
            let _ = pending_edge_coordinate_from_record(record)?;
            pending_parent_time_bound.observe(parent.time);
            pending_edges.push((child, parent));
        }
        self.rejections.pending_parent_time_bound = pending_parent_time_bound;
        for (child, parent) in pending_edges {
            if self
                .query_transaction(child)
                .await?
                .is_some_and(|tx| matches!(tx.fate, Fate::Pending))
                && self
                    .query_transaction(parent)
                    .await?
                    .is_some_and(|tx| matches!(tx.fate, Fate::Pending))
            {
                self.record_child_edges(child, [parent]).await;
            }
        }

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
    /// Rewrite a pending transaction's audit row as a build from before
    /// `jazz.exclusive-read-evidence.v1` wrote it: slots 5-8 null.
    #[doc(hidden)]
    pub async fn persist_without_exclusive_read_evidence_for_test(&mut self, tx_id: TxId) {
        let stored = self.query_transaction(tx_id).await.unwrap().unwrap();
        let mut tx = stored.tx.clone();
        tx.base_snapshot = None;
        tx.row_read_set = None;
        tx.absent_read_set = None;
        tx.predicate_read_set = None;
        let contribution_merge = self
            .contribution_merge_storage_value(tx.contribution_merge.as_ref())
            .unwrap();
        let values = transaction_values(
            stored.node_alias,
            &tx,
            stored.fate.clone(),
            stored.global_time,
            stored.durability,
            contribution_merge,
        )
        .unwrap();
        let mut batch = self.database.open_batch();
        batch.update("jazz_transactions", values);
        let applied = self.database.apply_batch(batch).await.unwrap();
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted).unwrap();
    }

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
        )
        .unwrap();
        values[TransactionRowRecord::FIELD_DURABILITY_IDX] = Value::EnumTag(2);
        let mut batch = self.database.open_batch();
        batch.update("jazz_transactions", values);
        let applied = self.database.apply_batch(batch).await.unwrap();
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted).unwrap();
    }
}
