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
            self.validate_current_row_storage_layout(&table)?;
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
            if !matches!(fate_from_encoded_fields(record)?, Fate::Accepted) {
                continue;
            }
            if let Some(global_seq) =
                record.get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_SEQ_IDX)?
            {
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
        self.cleanup_settled_ahead_current_leftovers()?;
        Ok(())
    }

    fn validate_current_row_storage_layout(&self, table: &TableSchema) -> Result<(), Error> {
        let storage_tables = table.global_current_storage_tables();
        self.validate_content_current_rows(&storage_tables[0])?;
        self.validate_register_current_rows(&storage_tables[1])?;

        let storage_tables = table.ahead_current_storage_tables();
        self.validate_content_current_rows(&storage_tables[0])?;
        self.validate_register_current_rows(&storage_tables[1])?;
        Ok(())
    }

    fn validate_content_current_rows(
        &self,
        storage_table: &groove::schema::TableSchema,
    ) -> Result<(), Error> {
        let descriptor = storage_table.record_schema();
        for raw in self
            .database
            .primary_key_scan_raw(&storage_table.name, &[])?
        {
            let record = BorrowedRecord::new(raw.raw(), &descriptor);
            record.get_u64(GlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX)?;
            record.get_idx(GlobalCurrentRowRecord::FIELD_PARENTS_IDX)?;
            record.get_nullable_u64(GlobalCurrentRowRecord::FIELD_GLOBAL_SEQ_IDX)?;
        }
        Ok(())
    }

    fn validate_register_current_rows(
        &self,
        storage_table: &groove::schema::TableSchema,
    ) -> Result<(), Error> {
        let descriptor = storage_table.record_schema();
        for raw in self
            .database
            .primary_key_scan_raw(&storage_table.name, &[])?
        {
            let record = BorrowedRecord::new(raw.raw(), &descriptor);
            record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX)?;
            record.get_idx(RegisterGlobalCurrentRowRecord::FIELD_PARENTS_IDX)?;
            record.get_nullable_u64(RegisterGlobalCurrentRowRecord::FIELD_GLOBAL_SEQ_IDX)?;
        }
        Ok(())
    }
}
