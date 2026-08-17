//! Startup recovery and durable-state rehydration for a node. This module owns
//! rebuilding aliases, schema/lens catalogues, branch metadata, pending edges,
//! rejected payloads, and peer/subscription state from groove storage; normal
//! ingestion lives in [`super::ingest`], storage record layouts in
//! [`super::codec`], and branch mutation APIs in [`super::branches`]. It is the
//! node layer's bridge from persisted groove tables back to in-memory state.

use super::*;

use crate::schema::{CLEAN_CLOSE_MARKERS_STORE, STORAGE_CONSISTENCY_MARKERS_STORE};

const CLEAN_CLOSE_MARKER_NAME: &str = "node-clean-close";
const CLEAN_CLOSE_MARKER_VERSION: u64 = 1;
const STORAGE_CONSISTENCY_MARKER_NAME: &str = "settled-ahead-current-clean-through";
const STORAGE_CONSISTENCY_MARKER_VERSION: u64 = 1;

impl<S> NodeState<S>
where
    S: ResidentStorage,
{
    pub(super) fn persist_clean_close_marker(&self) -> Result<(), Error> {
        self.database
            .direct_record_store(CLEAN_CLOSE_MARKERS_STORE)?
            .set(
                &clean_close_marker_key(),
                &[
                    Value::U64(CLEAN_CLOSE_MARKER_VERSION),
                    Value::Uuid(self.node_uuid.0),
                ],
            )?;
        Ok(())
    }

    fn take_valid_clean_close_marker(&mut self) -> Result<bool, Error> {
        let store = self
            .database
            .direct_record_store(CLEAN_CLOSE_MARKERS_STORE)?;
        let key = clean_close_marker_key();
        let Some(record) = store.get(&key)? else {
            return Ok(false);
        };
        store.delete(&key)?;

        let version = match record.get_idx(0)? {
            Value::U64(value) => value,
            _ => return Ok(false),
        };
        let node = match record.get_idx(1)? {
            Value::Uuid(value) => value,
            _ => return Ok(false),
        };
        Ok(version == CLEAN_CLOSE_MARKER_VERSION && node == self.node_uuid.0)
    }

    pub(super) fn persist_storage_consistency_marker_through(
        &self,
        tx_time: TxTime,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(STORAGE_CONSISTENCY_MARKERS_STORE)?;
        let key = storage_consistency_marker_key();
        if let Some(record) = store.get(&key)?
            && matches!(
                record.get_idx(0)?,
                Value::U64(STORAGE_CONSISTENCY_MARKER_VERSION)
            )
            && matches!(record.get_idx(1)?, Value::Uuid(node) if node == self.node_uuid.0)
            && let Value::U64(existing) = record.get_idx(2)?
            && existing >= tx_time.0
        {
            return Ok(());
        }
        store.set(
            &key,
            &[
                Value::U64(STORAGE_CONSISTENCY_MARKER_VERSION),
                Value::Uuid(self.node_uuid.0),
                Value::U64(tx_time.0),
            ],
        )?;
        Ok(())
    }

    fn valid_storage_consistency_marker(&self) -> Result<Option<TxTime>, Error> {
        let store = self
            .database
            .direct_record_store(STORAGE_CONSISTENCY_MARKERS_STORE)?;
        let Some(record) = store.get(&storage_consistency_marker_key())? else {
            return Ok(None);
        };
        let version = match record.get_idx(0)? {
            Value::U64(value) => value,
            _ => return Ok(None),
        };
        let node = match record.get_idx(1)? {
            Value::Uuid(value) => value,
            _ => return Ok(None),
        };
        let tx_time = match record.get_idx(2)? {
            Value::U64(value) => value,
            _ => return Ok(None),
        };
        if version == STORAGE_CONSISTENCY_MARKER_VERSION && node == self.node_uuid.0 {
            Ok(Some(TxTime(tx_time)))
        } else {
            Ok(None)
        }
    }

    fn recover_clock_checkpoint(&mut self) -> Result<bool, Error> {
        let Some(raw) = self
            .database
            .primary_key_get_raw("jazz_node_recovery_state", &[Value::Uuid(self.node_uuid.0)])?
        else {
            return Ok(false);
        };
        let record = raw.record();
        if record.get_u64(1)? != 1 {
            return Err(Error::InvalidStoredValue(
                "node recovery checkpoint version is unsupported",
            ));
        }
        let bytes = record.get_bytes(6)?;
        if bytes.len() % std::mem::size_of::<u64>() != 0 {
            return Err(Error::InvalidStoredValue(
                "node recovery checkpoint global sequence set is malformed",
            ));
        }
        let above = bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|bytes| {
                GlobalSeq(u64::from_le_bytes(
                    bytes.try_into().expect("u64 chunk has exact length"),
                ))
            })
            .collect::<BTreeSet<_>>();
        let watermark = GlobalSeq(record.get_u64(5)?);
        if above.iter().any(|sequence| *sequence <= watermark) {
            return Err(Error::InvalidStoredValue(
                "node recovery checkpoint contains a sequence below its watermark",
            ));
        }
        self.clock.tx_time = TxTime(record.get_u64(2)?);
        self.clock.next_global_seq = GlobalSeq(record.get_u64(3)?);
        self.clock.global_seq_exhausted = match record.get_u64(4)? {
            0 => false,
            1 => true,
            _ => {
                return Err(Error::InvalidStoredValue(
                    "node recovery checkpoint exhausted flag is malformed",
                ));
            }
        };
        self.clock.applied_global_watermark = watermark;
        self.clock.applied_global_above_watermark = above;
        Ok(true)
    }

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
                let schema_alias = SchemaVersionAlias(u64::from(raw.variant_tag()));
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

        let recovered_clock = self.recover_clock_checkpoint()?;
        #[cfg(feature = "testing")]
        if let Some(receipt) = &mut receipt {
            receipt.clock_checkpoint_loaded = recovered_clock;
        }
        if !recovered_clock {
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
            }
            if let Some(raw) =
                self.database
                    .index_last_raw(SHARED_DELETION_HISTORY_TABLE, "by_tx", &[])?
            {
                self.merge_tx_time(TxTime(
                    raw.record()
                        .get_u64(SharedDeletionHistoryRowRecord::FIELD_TX_TIME_IDX)?,
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
        let sequenced_transactions = if recovered_clock {
            Vec::new()
        } else {
            let first_global_seq = Value::Nullable(Some(Box::new(Value::U64(0))));
            let last_global_seq = Value::Nullable(Some(Box::new(Value::U64(u64::MAX))));
            let mut transactions = self.database.index_scan_range_raw(
                "jazz_transactions",
                "by_global_seq",
                std::slice::from_ref(&first_global_seq),
                std::slice::from_ref(&last_global_seq),
            )?;
            transactions.extend(self.database.index_scan_raw(
                "jazz_transactions",
                "by_global_seq",
                std::slice::from_ref(&last_global_seq),
            )?);
            transactions
        };
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

fn clean_close_marker_key() -> [Value; 1] {
    [Value::String(CLEAN_CLOSE_MARKER_NAME.to_owned())]
}

fn storage_consistency_marker_key() -> [Value; 1] {
    [Value::String(STORAGE_CONSISTENCY_MARKER_NAME.to_owned())]
}
