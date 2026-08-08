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
        for table in self.catalogue.schema.tables.clone() {
            let storage_table = rejected_versions_table_name(&table.name);
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
                versions.push(RejectedVersion::new(
                    table.name.clone(),
                    OwnedRecord::new(raw.raw().to_vec(), record.descriptor()),
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
        for raw in self
            .database
            .primary_key_scan_raw("jazz_schema_versions", &[])?
        {
            let record = raw.record();
            let alias =
                SchemaVersionAlias(record.get_u64(SchemaVersionAliasRowRecord::FIELD_ID_IDX)?);
            let uuid =
                SchemaVersionId(record.get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX)?);
            self.catalogue.schema_version_aliases.insert(uuid, alias);
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
        for table in self.catalogue.schema.tables.clone() {
            if let Some(raw) =
                self.database
                    .index_last_raw(&history_table_name(&table.name), "by_tx", &[])?
            {
                self.merge_tx_time(TxTime(
                    raw.record().get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?,
                ));
            }
            if let Some(raw) =
                self.database
                    .index_last_raw(&register_table_name(&table.name), "by_tx", &[])?
            {
                self.merge_tx_time(TxTime(
                    raw.record().get_u64(RegisterRowRecord::FIELD_TX_TIME_IDX)?,
                ));
            }
        }
        let mut accepted_global_seqs = Vec::new();
        for raw in self
            .database
            .index_scan_raw("jazz_transactions", "by_global_seq", &[])?
        {
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
        for global_seq in accepted_global_seqs {
            self.record_applied_global_seq(global_seq);
        }

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
        self.recover_branch_merge_reservations()?;
        if !cleanly_closed {
            self.cleanup_settled_ahead_current_leftovers(storage_consistent_through)?;
        }
        Ok(())
    }

    fn recover_branch_merge_reservations(&mut self) -> Result<(), Error> {
        let reservations = self
            .database
            .direct_record_store(BRANCH_MERGE_RESERVATIONS_STORE)?
            .prefix_entries(&[])?
            .into_iter()
            .map(|entry| {
                let branch_id = match entry.key.as_slice() {
                    [Value::Uuid(id)] => BranchId(*id),
                    _ => {
                        return Err(Error::InvalidStoredValue(
                            "branch merge reservation key must be branch uuid",
                        ));
                    }
                };
                let encoded = match entry.value.get_idx(0)? {
                    Value::Bytes(encoded) => encoded,
                    _ => {
                        return Err(Error::InvalidStoredValue(
                            "branch merge reservation must contain commit-unit bytes",
                        ));
                    }
                };
                if encoded.len() > MAX_COMMIT_UNIT_BYTES {
                    return Err(Error::InvalidStoredValue(
                        "branch merge reservation exceeds encoded-size limit",
                    ));
                }
                let reservation =
                    postcard::from_bytes::<PendingBranchMerge>(&encoded).map_err(|_| {
                        Error::InvalidStoredValue("branch merge reservation must decode")
                    })?;
                if commit_unit_limit_violation(&reservation.tx, &reservation.versions, None)
                    .is_some()
                {
                    return Err(Error::InvalidStoredValue(
                        "branch merge reservation exceeds commit-unit limits",
                    ));
                }
                Ok((branch_id, reservation))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for (branch_id, reservation) in reservations {
            if !self
                .branches
                .branches
                .get(&branch_id)
                .is_some_and(|branch| branch.state == codec::BranchState::Open)
            {
                self.database
                    .direct_record_store(BRANCH_MERGE_RESERVATIONS_STORE)?
                    .delete(&[Value::Uuid(branch_id.0)])?;
                continue;
            }
            let tx_id = reservation.tx.tx_id;
            self.merge_tx_time(tx_id.time);
            self.branches
                .pending_merge_backs
                .insert(branch_id, reservation.clone());
            let Some(fate) = self.query_transaction(tx_id)?.map(|stored| stored.fate) else {
                continue;
            };
            if matches!(fate, Fate::Accepted | Fate::Rejected(_)) {
                if !self.durable_reserved_commit_unit_matches(&reservation, &fate)? {
                    return Err(Error::ConflictingCommitUnit(tx_id));
                }
                self.settle_reserved_branch_merge(branch_id, tx_id, &fate)?;
            }
        }
        Ok(())
    }
}
