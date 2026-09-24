impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn ingest_transaction_and_versions(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.ingest_transaction_and_versions_with_current_indexes(
            tx, versions, fate, global_time, durability, true, false,
        )
        .await
    }

    pub(super) async fn ingest_transaction_fragment_without_current_indexes(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.ingest_transaction_and_versions_with_current_indexes(
            tx, versions, fate, global_time, durability, false, true,
        )
        .await
    }

    pub(super) async fn ingest_view_scoped_transaction_with_current_indexes(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.ingest_transaction_and_versions_with_current_indexes(
            tx, versions, fate, global_time, durability, true, true,
        )
        .await
    }

    pub(super) async fn stage_view_scoped_transaction_with_current_indexes(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
        staged_global_times: &mut Vec<GlobalTime>,
        staged_content_versions: &mut Vec<VersionRow>,
    ) -> Result<(), Error> {
        let (staged_versions, fate, global_time) = self
            .stage_transaction_and_versions_with_current_indexes(
                batch,
                tx,
                versions,
                fate.clone(),
                global_time,
                durability,
                true,
                true,
                Some(staged_content_versions),
            )
            .await?;
        self.finalize_staged_transaction_ingest(
            batch,
            fate,
            global_time,
            staged_global_times,
            &staged_versions,
        )
        .await
    }

    pub(super) async fn publish_pending_transaction_and_versions(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        durability: DurabilityTier,
    ) -> Result<PublishedTransaction, Error> {
        let tx_id = tx.tx_id;
        let mut batch = self.database.open_batch();
        let _ = self.stage_transaction_and_versions_with_current_indexes(
            &mut batch,
            tx,
            versions,
            Fate::Pending,
            None,
            durability,
            true,
            false,
            None,
        )
        .await?;
        let persistence = self.database.apply_batch(batch).await?;
        self.invalidate_tx_version_table_names_cache(tx_id);
        self.pending_persistence.insert(tx_id);
        Ok(PublishedTransaction { tx_id, persistence })
    }

    async fn prepare_exact_history_version(
        &mut self, tx_node_alias: NodeAlias, tx_time: TxTime, version: &VersionRecord,
    ) -> Result<VersionRow, Error> {
        let author_schema = version.schema_version();
        let table = self.table_in_schema(version.table(), author_schema)?;
        let schema_alias = self.ensure_schema_version_alias(author_schema).await?;
        let authored = self.authored_column_ids_for_names(author_schema, version.table(), version.authored_columns())?;
        VersionRow::from_wire_with_schema_version(
            &table, version, authored, tx_node_alias, schema_alias, tx_time,
            (author_schema != self.catalogue.local_schema_version_id).then_some(author_schema),
        )
    }

    async fn ingest_transaction_and_versions_with_current_indexes(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
        update_current_indexes: bool,
        view_scoped_cardinality: bool,
    ) -> Result<(), Error> {
        let batch = self.database.open_batch();
        self.ingest_transaction_and_versions_with_current_indexes_in_batch(
            batch, tx, versions, fate, global_time, durability, update_current_indexes,
            view_scoped_cardinality,
        ).await
    }

    async fn ingest_transaction_and_versions_with_current_indexes_in_batch(
        &mut self,
        mut batch: DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
        update_current_indexes: bool,
        view_scoped_cardinality: bool,
    ) -> Result<(), Error> {
        let tx_id = tx.tx_id;
        // Staging owns the decoded records and derived-index working set. Keep
        // it behind an async allocation boundary so admission does not retain
        // that state on the caller's poll stack.
        let (staged_versions, fate, global_time) = Box::pin(self.stage_transaction_and_versions_with_current_indexes(
            &mut batch,
            tx,
            versions,
            fate.clone(),
            global_time,
            durability,
            update_current_indexes,
            view_scoped_cardinality,
            None,
        ))
        .await?;
        let mut staged_global_times = Vec::new();
        self.finalize_staged_transaction_ingest(
            &mut batch,
            fate,
            global_time,
            &mut staged_global_times,
            &staged_versions,
        )
        .await?;
        batch.deliver_notifications(groove::db::NotificationTiming::AfterPersistence);
        let applied = self.database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted)?;
        // A later complete payload may have appended to a transaction first
        // seen through a view-scoped fragment. The staging cache contains only
        // this call's new versions, so discard it before any constraint or
        // caller re-query observes the assembled transaction.
        self.invalidate_tx_version_tables_cache(tx_id);
        Ok(())
    }

    async fn stage_transaction_and_versions_with_current_indexes(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
        update_current_indexes: bool,
        view_scoped_cardinality: bool,
        staged_content_versions: Option<&mut Vec<VersionRow>>,
    ) -> Result<(Vec<VersionRow>, Fate, Option<GlobalTime>), Error> {
        // Provenance operation identities participate in merge deduplication.
        // Admit them before accepting staged values or writing any derived
        // transaction/current state, on every local, remote, and view ingress.
        let contribution_merge = self.admit_contribution_merge_for_storage(&tx)?;
        let large_value_descriptors = version_indirect_descriptors(&versions);
        for staged_id in self
            .current_staged_ids_for_descriptors(&large_value_descriptors, false)
            .await?
        {
            batch.accept_large_value(staged_id);
        }
        self.merge_tx_time(tx.tx_id.time);
        let tx_node_alias = self.ensure_node_alias(tx.tx_id.node).await?;
        for schema in versions.iter().map(VersionRecord::schema_version).collect::<BTreeSet<_>>() {
            self.ensure_schema_version_alias(schema).await?;
        }
        let stored_tx = self.query_transaction(tx.tx_id).await?;
        // A discarded Pending view may have preserved only its transaction
        // identity. Every extension of a view-scoped fragment must retain
        // settlement learned in the meantime, including after restart and
        // earlier partial extensions, before indexing new bodies.
        let (fate, global_time, durability) = if let Some(stored) = stored_tx.as_ref()
            && stored.view_scoped_cardinality
        {
            if let (Some(current), Some(next)) = (stored.global_time, global_time)
                && next < current
            {
                return Err(Error::NonMonotoneState("global seq cannot move backwards"));
            }
            (
                next_fate(&stored.fate, fate)?,
                global_time.or(stored.global_time),
                durability.max(stored.durability),
            )
        } else {
            (fate, global_time, durability)
        };
        let tx_already_known = stored_tx.is_some();
        let preserve_authoritative_cardinality = view_scoped_cardinality
            && stored_tx
                .as_ref()
                .is_some_and(|stored| !stored.view_scoped_cardinality);
        let storage_tx = if preserve_authoritative_cardinality {
            &stored_tx.as_ref().expect("checked above").tx
        } else {
            &tx
        };
        let contribution_merge = if std::ptr::eq(storage_tx, &tx) {
            contribution_merge
        } else {
            self.admit_contribution_merge_for_storage(storage_tx)?
        };
        let tx_values = transaction_values_with_cardinality_scope(
            tx_node_alias,
            storage_tx,
            fate.clone(),
            global_time,
            durability,
            view_scoped_cardinality && !preserve_authoritative_cardinality,
            contribution_merge,
        )?;
        if tx_already_known {
            batch.update("jazz_transactions", tx_values);
        } else {
            batch.insert("jazz_transactions", tx_values);
        }

        let mut pending_global_updates =
            BTreeMap::<(String, BranchKey, RowUuid), VersionRow>::new();
        let mut content_versions = Vec::new();
        let mut stored_versions = Vec::new();
        for version in versions {
            let author_schema = version.schema_version();
            let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
            let table_schema = source_table_schema;
            let schema_version_alias = self.ensure_schema_version_alias(author_schema).await?;
            let authored_column_ids = self.authored_column_ids_for_names(
                author_schema,
                version.table(),
                version.authored_columns(),
            )?;
            let stored = VersionRow::from_wire_with_schema_version(
                &table_schema,
                &version,
                authored_column_ids,
                tx_node_alias,
                schema_version_alias,
                tx.tx_id.time,
                (author_schema != self.catalogue.local_schema_version_id)
                    .then_some(author_schema),
            )?;
            if !matches!(fate, Fate::Rejected(_)) {
                content_versions.push(stored.clone());
            }
            stored_versions.push(stored.clone());
            if update_current_indexes && matches!(fate, Fate::Accepted) {
                if global_time.is_some() {
                    let key = (
                        stored.table().to_owned(),
                        stored.branch_key().clone(),
                        stored.row_uuid(),
                    );
                    if let Some(merged) = self
                        .merged_global_post_image(
                            batch,
                            author_schema,
                            &table_schema,
                            &stored,
                            tx.tx_id,
                        )
                        .await?
                    {
                        pending_global_updates.insert(key, merged);
                    }
                }
            }
            let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
            let storage_key = self.version_storage_primary_key(&stored)?;
            if batch.ensure_exact(&self.database, history_table.as_ref(), storage_key, groove_record).await?
                == groove::db::EnsureExactOutcome::Conflict
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if update_current_indexes && !matches!(fate, Fate::Rejected(_)) && global_time.is_none()
            {
                self.write_ahead_current_insert(batch, &stored)?;
            }
        }
        if update_current_indexes && !matches!(fate, Fate::Rejected(_)) {
            if let Some(staged) = staged_content_versions {
                staged.extend(content_versions.iter().cloned());
            }
        }
        if update_current_indexes && matches!(fate, Fate::Accepted) {
            if let Some(global_time) = global_time {
                for stored in pending_global_updates.values() {
                    self.write_global_current_update(batch, stored, global_time)?;
                }
            }
        }
        self.cache_tx_versions(tx.tx_id, stored_versions.clone());
        Ok((stored_versions, fate, global_time))
    }

    async fn finalize_staged_transaction_ingest(
        &mut self,
        batch: &mut DatabaseBatch,
        fate: Fate,
        global_time: Option<GlobalTime>,
        staged_global_times: &mut Vec<GlobalTime>,
        staged_versions: &[VersionRow],
    ) -> Result<(), Error> {
        if matches!(fate, Fate::Accepted)
            && let Some(global_time) = global_time
        {
            staged_global_times.push(global_time);
            let advanced_global_times = self.record_applied_global_time(global_time);
            self.cleanup_fated_ahead_current_for_versions(batch, staged_versions)?;
            if !advanced_global_times.is_empty() {
                for advanced in advanced_global_times
                    .into_iter()
                    .filter(|advanced| *advanced != global_time)
                {
                    self.prune_ahead_current_for_global_time(batch, advanced)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn translate_cells_to_current_write_schema(
        &mut self,
        source: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<(SchemaVersionId, String), Error> {
        let target = self.catalogue.active_schema.schema;
        if source == target {
            return Ok((source, table.to_owned()));
        }
        if let Some(path) = self.compiled_lens_path(source, target, table)? {
            return Ok((target, apply_compiled_lens_path(&path, cells)));
        }
        Ok((source, table.to_owned()))
    }

    /// A wire row version is a complete row under the schema id it declares.
    /// An unknown schema cannot be checked until its catalogue value arrives,
    /// but a known schema must never accept a descriptor borrowed from another
    /// version: that would make the omitted trailing columns indistinguishable
    /// from an authored value and reintroduce partial-row sync semantics.
    fn malformed_authored_version_reason<V: std::borrow::Borrow<VersionRecord>>(&self, versions: &[V]) -> Option<String> {
        for version in versions {
            let version = version.borrow();
            for (field, physical_ms) in [
                ("created_at_ms", version.created_at_ms()),
                ("updated_at_ms", version.updated_at_ms()),
            ] {
                if crate::time::TxTime::from_physical_ms(physical_ms).is_err() {
                    return Some(format!(
                        "row version for table '{}' has {field} outside the packed HLC physical-millisecond range",
                        version.table()
                    ));
                }
            }
            if let Err(Error::InvalidMergeableCommit(reason)) =
                validate_canonical_version_parts(version.branch_key())
            {
                return Some(reason.to_owned());
            }
            let Some(schema) = self
                .catalogue
                .catalogue_schemas
                .get(&version.schema_version())
            else {
                continue;
            };
            let Some(table) = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == version.table())
            else {
                return Some(format!(
                    "row version table '{}' is absent from its authored schema",
                    version.table()
                ));
            };
            if version.record().descriptor() != &prepared_wire_record_descriptor(table) {
                return Some(format!(
                    "row version for table '{}' does not carry the complete descriptor of its authored schema",
                    version.table()
                ));
            }
            if let Some(reason) = Self::malformed_authored_branch_key_reason(
                &schema.schema,
                table,
                version,
            ) {
                return Some(reason);
            }
        }
        None
    }

    fn malformed_authored_branch_key_reason(
        schema: &JazzSchema,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Option<String> {
        let branch_cells = match schema.validate_authored_branch_key(table, version.branch_key()) {
            Ok(cells) => cells,
            Err(reason) => {
                return Some(format!(
                    "row version for table '{}' has an invalid branch key: {reason}",
                    version.table()
                ));
            }
        };
        {
            for (column, branch_value) in branch_cells {
                let Some(position) = table
                    .columns
                    .iter()
                    .position(|candidate| candidate.name == column)
                else {
                    return Some(format!(
                        "row version for table '{}' binds a missing branch column '{column}'",
                        version.table()
                    ));
                };
                if version.cell_at(position) != Some(branch_value) {
                    return Some(format!(
                        "row version for table '{}' has branch key content that disagrees with column '{column}'",
                        version.table()
                    ));
                }
            }
        }
        None
    }

    /// Validate row versions carried by a view or repair payload before that
    /// payload may advance local receiver state. View payloads cannot park for
    /// a missing catalogue entry: unlike an authored commit unit, they have no
    /// protocol disposition that can defer a partial application of the frame.
    pub(super) fn validate_view_payload_versions(
        &self,
        versions: &[VersionRecord],
    ) -> Result<(), Error> {
        self.validate_view_payload_versions_prepared(versions, &mut BTreeMap::new())
    }

    pub(super) fn validate_view_payload_versions_prepared<'a>(
        &self,
        versions: &'a [VersionRecord],
        descriptors: &mut BTreeMap<(SchemaVersionId, &'a str), groove::records::RecordDescriptor>,
    ) -> Result<(), Error> {
        // View/repair bodies come from an admitted authority encoder. Check
        // semantic catalogue compatibility below, not their byte representation.
        // Catalogue state cannot change during this shared-borrow preflight.
        for version in versions {
            let schema = self
                .catalogue
                .catalogue_schemas
                .get(&version.schema_version())
                .ok_or(Error::MalformedViewUpdate(
                    "row version names an unknown authored schema",
                ))?;
            let table = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == version.table())
                .ok_or(Error::MalformedViewUpdate(
                    "row version table is absent from its authored schema",
                ))?;
            let descriptor = descriptors
                .entry((version.schema_version(), version.table()))
                .or_insert_with(|| table.wire_record_descriptor());
            if version.record().descriptor() != descriptor {
                return Err(Error::MalformedViewUpdate(
                    "row version does not carry the complete descriptor of its authored schema",
                ));
            }
            if crate::time::TxTime::from_physical_ms(version.created_at_ms()).is_err()
                || crate::time::TxTime::from_physical_ms(version.updated_at_ms()).is_err()
            {
                return Err(Error::MalformedViewUpdate(
                    "row version provenance exceeds packed HLC physical-millisecond range",
                ));
            }
            if Self::malformed_authored_branch_key_reason(&schema.schema, table, version).is_some() {
                return Err(Error::MalformedViewUpdate(
                    "row version does not carry a valid authored branch key",
                ));
            }
        }
        Ok(())
    }

    async fn reject_malformed_commit(
        &mut self,
        tx: Transaction,
        reason: String,
    ) -> Result<Vec<SyncMessage>, Error> {
        let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
        // Malformed provenance cannot be encoded safely. Retain the terminal
        // outcome, but never make an invalid payload durable merely to report
        // it; recovery continues to reject any independently corrupted stored
        // record at decode time.
        let storage_tx = Transaction {
            contribution_merge: None,
            ..tx.clone()
        };
        self.ingest_rejected_transaction(storage_tx, fate.clone())
            .await?;
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_time: None,
            durability: None,
        }])
    }

    /// Ensure every known authored schema named by an arriving commit has a
    /// local alias and registered shared-storage variant. Unknown schemas stay
    /// parked until their catalogue lineage arrives and re-enters this path.
    async fn prepare_authored_schema_variants_for_commit<V: std::borrow::Borrow<VersionRecord>>(
        &mut self,
        versions: &[V],
    ) -> Result<(), Error> {
        if self.malformed_authored_version_reason(versions).is_some() {
            return Err(Error::InvalidStoredValue(
                "wire version record does not match authored schema",
            ));
        }
        if versions.iter().any(|version| {
            let version = version.borrow();
            !self
                .catalogue
                .catalogue_schemas
                .contains_key(&version.schema_version())
        }) {
            return Ok(());
        }

        let authored_variants = versions
            .iter()
            .map(|version| { let version = version.borrow(); (version.table().to_owned(), version.schema_version()) })
            .collect::<BTreeSet<_>>();
        let mut registered_mapping = false;
        for (table, schema_version) in authored_variants {
            self.table_in_schema_ref(&table, schema_version)?;
            registered_mapping |= !self
                .catalogue
                .schema_version_aliases
                .contains_key(&schema_version)
                || !self
                    .catalogue
                    .physical_mappings
                    .contains_key(&schema_version);
            self.ensure_schema_version_alias(schema_version).await?;
        }
        if registered_mapping {
            self.synchronize_physical_version_tables().await?;
        }
        Ok(())
    }

    pub(super) async fn ingest_rejected_transaction(
        &mut self,
        tx: Transaction,
        fate: Fate,
    ) -> Result<(), Error> {
        let contribution_merge = self.admit_contribution_merge_for_storage(&tx)?;
        if self.query_transaction(tx.tx_id).await?.is_some() {
            return self.apply_fate_update(tx.tx_id, fate, None, None).await;
        }
        let tx_node_alias = self.ensure_node_alias(tx.tx_id.node).await?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                fate.clone(),
                None,
                DurabilityTier::Local,
                contribution_merge,
            )?,
        );
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        Ok(())
    }
}
