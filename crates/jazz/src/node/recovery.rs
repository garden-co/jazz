//! Startup recovery and durable-state rehydration for a node. This module owns
//! rebuilding aliases, schema/lens catalogues, branch metadata, pending edges,
//! rejected payloads, and peer/subscription state from groove storage; normal
//! ingestion lives in [`super::ingest`], storage record layouts in
//! [`super::codec`], and branch mutation APIs in [`super::branches`]. It is the
//! node layer's bridge from persisted groove tables back to in-memory state.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn rejected_versions_for(
        &mut self,
        alias: NodeAlias,
        tx_id: TxId,
    ) -> Result<Vec<RejectedVersion>, Error> {
        let mut versions = Vec::new();
        for table_id in self.physical_table_ids() {
            let storage_table = physical_rejected_versions_table_name(table_id);
            for raw in self.database.primary_key_scan_raw(
                &storage_table,
                &[Value::U64(tx_id.time.0), Value::U64(alias.0)],
            )? {
                let record = raw.record();
                let node_id = record.get_u64(RejectedVersionRowRecord::FIELD_TX_NODE_ID_IDX)?;
                let time = record.get_u64(RejectedVersionRowRecord::FIELD_TX_TIME_IDX)?;
                if node_id != alias.0 || time != tx_id.time.0 {
                    continue;
                }
                let schema_alias = SchemaVersionAlias(raw.schema_version());
                let schema_version = self.schema_version_for_alias(schema_alias).ok_or(
                    Error::InvalidStoredValue("rejected row schema version alias missing"),
                )?;
                let logical_table =
                    self.logical_table_for_physical_alias(table_id, schema_alias)?;
                let logical_descriptor = self
                    .table_in_schema(&logical_table, schema_version)?
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

    pub(super) fn recover_from_storage(&mut self) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        {
            self.recover_from_storage_inner(None)
        }
        #[cfg(not(feature = "testing"))]
        self.recover_from_storage_inner()
    }

    #[cfg(feature = "testing")]
    pub(super) fn recover_from_storage_with_receipt(
        &mut self,
        receipt: &mut NodeOpenReceipt,
    ) -> Result<(), Error> {
        self.recover_from_storage_inner(Some(receipt))
    }

    fn recover_from_storage_inner(
        &mut self,
        #[cfg(feature = "testing")] mut receipt: Option<&mut NodeOpenReceipt>,
    ) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let cleanly_closed = self.take_valid_clean_close_marker()?;
        let storage_consistent_through = if cleanly_closed {
            None
        } else {
            self.valid_storage_consistency_marker()?
        };
        for raw in self.database.primary_key_scan_raw("jazz_nodes", &[])? {
            let record = raw.record();
            let alias = record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?;
            let uuid = NodeUuid(record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)?);
            self.node_aliases.insert(uuid, NodeAlias(alias));
        }
        let branch_records = self
            .database
            .primary_key_scan_raw("jazz_branches", &[])?
            .into_iter()
            .map(|raw| raw.raw().to_vec())
            .collect::<Vec<_>>();
        let branch_catalogue_schema = self.catalogue.schema.lower_catalogue_meta_to_groove();
        let branch_descriptor = branch_catalogue_schema
            .table("jazz_branches")
            .ok_or(Error::InvalidStoredValue("branches table must exist"))?
            .record_schema();
        for raw in branch_records {
            self.recover_branch_record(BorrowedRecord::new(&raw, &branch_descriptor))?;
        }

        let alias_to_node = self
            .node_aliases
            .iter()
            .map(|(node, alias)| (*alias, *node))
            .collect::<BTreeMap<_, _>>();

        if let Some(raw) = self
            .database
            .primary_key_last_raw("jazz_transactions", &[])?
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
            if let Some(raw) = self.database.index_last_raw(
                &physical_history_table_name(table_id),
                "by_tx",
                &[],
            )? {
                self.merge_tx_time(TxTime(
                    raw.record().get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?,
                ));
            }
            if let Some(raw) = self.database.index_last_raw(
                &physical_register_table_name(table_id),
                "by_tx",
                &[],
            )? {
                self.merge_tx_time(TxTime(
                    raw.record().get_u64(RegisterRowRecord::FIELD_TX_TIME_IDX)?,
                ));
            }
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_catalogue_state = started.elapsed();
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let mut accepted_global_seqs = Vec::new();
        #[cfg(feature = "testing")]
        let mut global_sequence_records_scanned = 0usize;
        // Nullable index keys order `None` before `Some`. Range over only the
        // `Some` bucket so local pending/rejected transactions cannot make
        // recovery O(total transactions). The range end is exclusive, hence
        // the separate exact lookup preserves the prior u64::MAX behavior.
        let first_global_seq = Value::Nullable(Some(Box::new(Value::U64(0))));
        let last_global_seq = Value::Nullable(Some(Box::new(Value::U64(u64::MAX))));
        let mut sequenced_transactions = self.database.index_scan_range_raw(
            "jazz_transactions",
            "by_global_seq",
            std::slice::from_ref(&first_global_seq),
            std::slice::from_ref(&last_global_seq),
        )?;
        sequenced_transactions.extend(self.database.index_scan_raw(
            "jazz_transactions",
            "by_global_seq",
            std::slice::from_ref(&last_global_seq),
        )?);
        for raw in sequenced_transactions {
            #[cfg(feature = "testing")]
            {
                global_sequence_records_scanned += 1;
            }
            let record = raw.record();
            let global_seq = record.get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_SEQ_IDX)?;
            if global_seq.is_some()
                && durability_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
                )? != DurabilityTier::Global
            {
                return Err(Error::InvalidStoredValue(
                    "global sequence requires Global durability",
                ));
            }
            if !matches!(fate_from_encoded_fields(record)?, Fate::Accepted) {
                continue;
            }
            if let Some(global_seq) = global_seq {
                accepted_global_seqs.push(GlobalSeq(global_seq));
            }
        }
        accepted_global_seqs.sort();
        accepted_global_seqs.dedup();
        #[cfg(feature = "testing")]
        if let Some(receipt) = &mut receipt {
            receipt.accepted_global_sequences = accepted_global_seqs.len();
            receipt.global_sequence_records_scanned = global_sequence_records_scanned;
        }
        for global_seq in accepted_global_seqs {
            self.record_applied_global_seq(global_seq);
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_global_sequences = started.elapsed();
        }

        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        let mut pending_edges = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_pending_edges", &[])?
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
            pending_edges.push((child, parent));
        }
        for (child, parent) in pending_edges {
            if self
                .query_transaction(child)?
                .is_some_and(|tx| matches!(tx.fate, Fate::Pending))
                && self
                    .query_transaction(parent)?
                    .is_some_and(|tx| matches!(tx.fate, Fate::Pending))
            {
                self.record_child_edges(child, [parent]);
            }
        }

        let mut rejected_headers = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_rejected_transactions", &[])?
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
            let versions = self.rejected_versions_for(node_alias, tx_id)?;
            self.rejections
                .rejected_transactions
                .insert(tx_id, RejectedTransaction::new(tx_id, record, versions));
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_pending_and_rejected = started.elapsed();
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| web_time::Instant::now());
        if !cleanly_closed {
            self.cleanup_settled_ahead_current_leftovers(storage_consistent_through)?;
        }
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_unclean_close = started.elapsed();
        }
        Ok(())
    }
}
