impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Return local synchronization counters.
    pub fn sync_metrics(&self) -> &SyncMetrics {
        &self.sync_metrics
    }

    pub(crate) fn record_dropped_peer_request(&mut self) {
        self.sync_metrics.dropped_peer_request_messages += 1;
    }

    pub(crate) fn record_transport_backpressure_retry(&mut self) {
        self.sync_metrics.transport_backpressure_retries += 1;
    }

    pub(crate) fn record_authoritative_reset_missing_payload_fallback(&mut self) {
        self.sync_metrics
            .authoritative_reset_missing_payload_fallbacks += 1;
    }

    pub(crate) fn record_peer_payload_inventory_missing_fallback(&mut self) {
        self.sync_metrics.peer_payload_inventory_missing_fallbacks += 1;
    }

    /// Deterministic counters for query-engine read authorization paths.
    pub fn query_engine_read_metrics(&self) -> &QueryEngineReadMetrics {
        &self.query_engine_read_metrics
    }

    /// Reset query-engine read authorization counters.
    pub fn reset_query_engine_read_metrics(&mut self) {
        self.query_engine_read_metrics = QueryEngineReadMetrics::default();
    }

    /// Published schema-version payloads known to this node.
    pub fn catalogue_schemas(&self) -> &BTreeMap<SchemaVersionId, SchemaVersion> {
        &self.catalogue.catalogue_schemas
    }

    /// Highest contiguously activated authoritative catalogue position.
    pub fn active_catalogue_seq(&self) -> u64 {
        self.catalogue.active_catalogue_seq
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn set_catalogue_activation_failpoint(
        &mut self,
        failpoint: CatalogueActivationFailpoint,
    ) {
        self.catalogue_activation_failpoint = Some(failpoint);
    }

    /// Published migration lenses known to this node.
    pub fn catalogue_lenses(&self) -> &BTreeMap<MigrationLensId, MigrationLens> {
        &self.catalogue.catalogue_lenses
    }

    /// Current dynamic-catalogue bootstrap state.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn catalogue_bootstrap_state(&self) -> CatalogueBootstrapState {
        self.catalogue_bootstrap_state
    }

    /// Return the authoritative current-write pointer, or fail closed before
    /// an edge has adopted its first trusted catalogue snapshot.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.require_catalogue_ready()?;
        Ok(self.catalogue.current_write_schema)
    }

    /// Return the active read-schema only after an authority catalogue has
    /// been durably adopted.  Dynamic-edge callers must use this instead of
    /// treating the temporary system schema as an application schema.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_current_schema(&self) -> Result<&JazzSchema, Error> {
        self.require_catalogue_ready()?;
        Ok(&self.catalogue.schema)
    }

    /// Apply an in-memory-only mutation for white-box tests of invalid compiled
    /// policy states. The node must already have been created from a valid
    /// public schema; this helper never persists or publishes the mutation.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn mutate_current_schema_for_testing(
        &mut self,
        mutate: impl FnOnce(&mut crate::schema::RuntimeSchema),
    ) {
        let mut schema = self.catalogue.schema.clone();
        mutate(schema.runtime_mut_for_testing());
        self.catalogue.schema = schema.clone();
        self.catalogue
            .catalogue_schemas
            .get_mut(&self.catalogue.current_schema_version_id)
            .expect("current schema is present in the test catalogue")
            .schema = schema;
    }

    pub(crate) fn require_catalogue_ready(&self) -> Result<(), Error> {
        self.database.ensure_usable()?;
        if self.catalogue_bootstrap_state == CatalogueBootstrapState::Uninitialized {
            return Err(Error::CatalogueUninitialized);
        }
        Ok(())
    }

    /// Current write-schema pointer known to this node.
    ///
    /// An uninitialized dynamic edge has no current application schema; the
    /// temporary system-only layout must not leak through this API.
    pub fn current_write_schema(&self) -> Result<CurrentWriteSchema, Error> {
        self.try_current_write_schema()
    }

    pub(crate) fn catalogue_snapshot(&self) -> Result<crate::protocol::CatalogueSnapshot, Error> {
        self.require_catalogue_ready()?;
        let mut schemas = self
            .catalogue
            .catalogue_schemas
            .values()
            .cloned()
            .collect::<Vec<_>>();
        schemas.sort_by_key(|schema| schema.id);
        let mut lineages = self
            .catalogue
            .active_lineages_by_target
            .values()
            .map(|lineage| (lineage.catalogue_seq, lineage.publication.clone()))
            .collect::<Vec<_>>();
        lineages.sort_by_key(|(catalogue_seq, _)| *catalogue_seq);
        // The write pointer is deliberately independent of the authority's
        // unique genesis.  Once a lineage is active it normally points at a
        // descendant, whose manifest must never be re-labelled as genesis in
        // a snapshot (doing so lets a receiver allocate a different physical
        // root for the real genesis schema).
        let lineage_targets = self
            .catalogue
            .active_lineages_by_target
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let genesis = self
            .catalogue
            .catalogue_schemas
            .keys()
            .find(|schema| !lineage_targets.contains(schema))
            .copied()
            .ok_or(Error::InvalidStoredValue("catalogue genesis schema is missing"))?;
        if self
            .catalogue
            .catalogue_schemas
            .keys()
            .filter(|schema| !lineage_targets.contains(schema))
            .nth(1)
            .is_some()
        {
            return Err(Error::InvalidStoredValue(
                "catalogue has multiple genesis schemas",
            ));
        }
        let genesis_physical_identities = self
            .catalogue
            .physical_mappings
            .get(&genesis)
            .ok_or(Error::InvalidStoredValue("genesis physical mapping missing"))?
            .identities
            .clone();
        Ok(crate::protocol::CatalogueSnapshot {
            genesis_physical_identities,
            schemas,
            lineages,
            current_write_schema: self.catalogue.current_write_schema,
        })
    }

    /// Return a historical read handle at an exact global settle position.
    pub fn at(&mut self, position: GlobalTime) -> HistoricalRead<'_, S> {
        HistoricalRead {
            node: self,
            position,
        }
    }

    /// Return a historical read handle for the latest settle position whose
    /// transaction time is less than or equal to `time`.
    ///
    /// This is deterministic, not a wall-clock truth claim: concurrent or
    /// offline writers can settle in an order that disagrees with transaction
    /// HLC time, so this convenience address is best-effort under clock skew.
    pub fn at_time(&mut self, time: TxTime) -> Result<HistoricalRead<'_, S>, Error> {
        let position = crate::db::block_on(self.resolve_time_travel_position(time))?;
        Ok(self.at(position))
    }

    /// Return whether this node can answer a historical query locally.
    ///
    /// v1 is conservative: authorities/history-complete nodes can answer cuts
    /// up to their contiguous applied watermark; partial clients return false
    /// so callers route the one-shot read to a server in a later protocol slice.
    pub fn is_history_complete_for(&self, _shape: &ValidatedQuery, position: GlobalTime) -> bool {
        self.history_complete && position <= self.clock.committed_global_time
    }

    /// Whether this node was opened as a complete serving authority.
    pub(crate) fn is_history_complete(&self) -> bool {
        self.history_complete
    }

    /// Return current rows for a subscription at the requested tier.
    pub async fn subscription_current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table(table)?.clone();
        let subscription = self.whole_table_subscription_key(table)?;
        match settled {
            DurabilityTier::None | DurabilityTier::Local => self.current_rows(table, settled).await,
            DurabilityTier::Edge => self.current_rows(table, settled).await,
            DurabilityTier::Global => {
                let binding_view_key =
                    BindingViewKey::from_canonical_subscription_key(subscription);
                let Some(row_result_set) = self.query.settled_result_sets.get(&binding_view_key)
                else {
                    return Ok(Vec::new());
                };
                let row_entries = row_result_set
                    .iter()
                    .filter_map(ResultMemberEntry::as_row)
                    .collect::<Vec<_>>();
                let mut rows = Vec::new();
                for (entry_table, row_uuid, tx_id) in row_entries {
                    if entry_table.as_str() != table {
                        continue;
                    }
                    let tx_node_alias = self
                        .node_aliases
                        .get(&tx_id.node)
                        .copied()
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    let version = self
                        .query_version_by_alias(
                            table,
                            row_uuid,
                            VersionLayer::Content,
                            tx_id.time,
                            tx_node_alias,
                        )
                        .await?
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    rows.push(self.current_row_from_materialized_version(&table_schema, &version)?);
                }
                sort_current_rows(&mut rows);
                Ok(rows)
            }
        }
    }

    /// Return the legacy transaction fate tuple.
    pub async fn transaction_state(
        &mut self,
        tx_id: TxId,
    ) -> Option<(Fate, Option<GlobalTime>, DurabilityTier)> {
        self.transaction_record(tx_id).await.map(|record| {
            let durability = if self.pending_persistence.contains(&tx_id) {
                DurabilityTier::None
            } else {
                record.durability
            };
            (record.fate, record.global_time, durability)
        })
    }

    /// Return the durable audit record for a transaction, including rejected
    /// transactions whose row versions were removed from history.
    pub async fn transaction_record(&mut self, tx_id: TxId) -> Option<TransactionRecord> {
        self.query_transaction(tx_id)
            .await
            .ok()
            .flatten()
            .map(|stored| stored.to_record())
    }

    /// Return locally originated transactions that still need upstream settlement.
    ///
    /// Client reconnect restores these durable transactions into its in-memory
    /// upload queue. A transaction is locally originated only when both its
    /// creating node and author match the reopened client's identity; history
    /// from other devices sharing an author is never replayed by this client.
    pub async fn pending_transaction_ids_for(
        &mut self,
        node: NodeUuid,
        author: AuthorSubject,
    ) -> Result<Vec<TxId>, Error> {
        Ok(self.pending_transaction_scan_for(node, author).await?.tx_ids)
    }

    /// Return unsettled transactions by `author`, irrespective of their
    /// originating node. A dedicated browser relay uses this when it reopens:
    /// relayed main-thread commits retain the main thread's node id, so the
    /// relay's ordinary local-origin recovery scan cannot find them.
    pub(crate) async fn pending_transaction_ids_for_author(
        &mut self,
        author: AuthorSubject,
    ) -> Result<Vec<TxId>, Error> {
        self.below_global_transaction_ids_for_author(author, true).await
    }

    pub(crate) async fn unresolved_transaction_ids_for_author(
        &mut self,
        author: AuthorSubject,
    ) -> Result<Vec<TxId>, Error> {
        self.below_global_transaction_ids_for_author(author, false)
            .await
    }

    async fn below_global_transaction_ids_for_author(
        &mut self,
        author: AuthorSubject,
        include_accepted: bool,
    ) -> Result<Vec<TxId>, Error> {
        let mut candidates = Vec::new();
        for raw in self.database.index_scan_raw(
            "jazz_transactions",
            "by_global_time",
            &[Value::Nullable(None)],
        )
        .await?
        {
            let record = raw.record();
            let fate = record.get_enum(TransactionRowRecord::FIELD_FATE_IDX)?;
            if AuthorSubject::from_canonical(
                record.get_str(TransactionRowRecord::FIELD_MADE_BY_IDX)?,
            )
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?
                != author
                || !(fate == 0 || (include_accepted && fate == 1))
                || durability_from_discriminant(
                    record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
                )? >= DurabilityTier::Global
            {
                continue;
            }
            candidates.push((
                NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?),
                TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
            ));
        }
        let mut tx_ids = Vec::with_capacity(candidates.len());
        for (alias, time) in candidates {
            let Some(node) = self.resolve_node_alias(alias).await? else {
                continue;
            };
            tx_ids.push(TxId::new(time, node));
        }
        tx_ids.sort();
        tx_ids.dedup();
        Ok(tx_ids)
    }

    pub(crate) async fn transaction_row_keys(
        &mut self,
        tx_ids: &[TxId],
    ) -> Result<BTreeSet<(String, RowUuid)>, Error> {
        let mut row_keys = BTreeSet::new();
        for tx_id in tx_ids {
            row_keys.extend(
                self.query_versions_for_tx(*tx_id).await?
                    .into_iter()
                    .map(|version| (version.table().to_owned(), version.row_uuid())),
            );
        }
        Ok(row_keys)
    }

    /// Find replayable local transactions in the null slice of
    /// `by_global_time`. The sequence/durability invariant makes every
    /// below-Global transaction sequence-null, so settled history is outside
    /// this scan without a second index or an upgrade backfill.
    async fn pending_transaction_scan_for(
        &mut self,
        node: NodeUuid,
        author: AuthorSubject,
    ) -> Result<PendingTransactionScan, Error> {
        let Some(node_alias) = self.node_aliases.get(&node).copied() else {
            return Ok(PendingTransactionScan::default());
        };

        let mut scan = PendingTransactionScan::default();
        for raw in self.database.index_scan_raw(
            "jazz_transactions",
            "by_global_time",
            &[Value::Nullable(None)],
        )
        .await?
        {
            scan.records_visited += 1;
            let record = raw.record();
            if NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?) != node_alias
                || AuthorSubject::from_canonical(
                    record.get_str(TransactionRowRecord::FIELD_MADE_BY_IDX)?,
                )
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?
                    != author
            {
                continue;
            }
            if !matches!(
                record.get_enum(TransactionRowRecord::FIELD_FATE_IDX)?,
                0 | 1
            ) || durability_from_discriminant(
                record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
            )? >= DurabilityTier::Global
            {
                continue;
            }
            scan.tx_ids.push(TxId::new(
                TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?),
                node,
            ));
        }
        scan.tx_ids.sort();
        scan.tx_ids.dedup();
        Ok(scan)
    }

    /// Resolve creator/updater provenance for a projected current row.
    pub fn row_provenance(&mut self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        row.provenance()
    }

    pub(crate) async fn current_row_tx_id(&mut self, row: &CurrentRow) -> Option<TxId> {
        let (time, alias) = row.projected_tx_alias()?;
        Some(TxId::new(
            time,
            self.resolve_node_alias(alias).await.ok()??,
        ))
    }

    pub(crate) async fn persist_known_state_fact_for_authority_result(
        &self,
        authority_result_key: AuthorityResultKey,
        settled_through: GlobalTime,
    ) -> Result<(), Error> {
        self.database
            .direct_record_store(KNOWN_STATE_FACTS_STORE)?
            .set(
                &known_state_fact_key(&authority_result_key),
                &[
                    Value::U64(settled_through.0),
                    Value::U64(
                        self.query
                            .authorization_progress_by_binding_view
                            .get(&authority_result_key.binding_view)
                            .copied()
                            .unwrap_or(u64::MAX),
                    ),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn load_known_state_fact(
        &mut self,
        binding_view_key: BindingViewKey,
    ) -> Result<Option<GlobalTime>, Error> {
        let authority_result_key = AuthorityResultKey::unscoped(binding_view_key);
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        let Some(record) = store.get(&known_state_fact_key(&authority_result_key)).await? else {
            return Ok(None);
        };
        let settled_through = match record.get_idx(0)? {
            Value::U64(value) => GlobalTime(value),
            _ => {
                return Err(Error::InvalidStoredValue(
                    "known-state settled-through must be u64",
                ));
            }
        };
        self.query
            .settled_through_by_binding_view
            .insert(binding_view_key, settled_through);
        if let Value::U64(progress) = record.get_idx(1)?
            && progress != u64::MAX
        {
            self.query
                .authorization_progress_by_binding_view
                .insert(binding_view_key, progress);
        }
        Ok(Some(settled_through))
    }

    pub(crate) async fn clear_all_known_state_facts(&mut self) -> Result<(), Error> {
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        let keys = store
            .prefix_entries(&[])
            .await?
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        for key in keys {
            store.delete(&key).await?;
        }
        self.query.settled_through_by_binding_view.clear();
        self.query.authorization_progress_by_binding_view.clear();
        self.clear_all_settled_result_state().await?;
        Ok(())
    }

    pub(crate) async fn persist_settled_result_state_delta_for_authority_result(
        &self,
        authority_result_key: AuthorityResultKey,
        cleared: bool,
        member_adds: &[ResultMemberEntry],
        member_removes: &[ResultMemberEntry],
        member_rewrite: Option<&BTreeSet<ResultMemberEntry>>,
        fact_adds: &[ViewFactEntry],
        fact_removes: &[ViewFactEntry],
        fact_rewrite: Option<&BTreeSet<ViewFactEntry>>,
    ) -> Result<(), Error> {
        self.persist_settled_result_members_delta(
            authority_result_key.clone(),
            cleared,
            member_adds,
            member_removes,
            member_rewrite,
        )
        .await?;
        self.persist_settled_program_facts_delta(
            authority_result_key,
            cleared,
            fact_adds,
            fact_removes,
            fact_rewrite,
        )
        .await?;
        Ok(())
    }

    async fn persist_settled_result_members_delta(
        &self,
        authority_result_key: AuthorityResultKey,
        cleared: bool,
        adds: &[ResultMemberEntry],
        removes: &[ResultMemberEntry],
        rewrite: Option<&BTreeSet<ResultMemberEntry>>,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(SETTLED_RESULT_MEMBERS_STORE)?;
        if cleared || rewrite.is_some() {
            let prefix = authority_result_store_prefix(&authority_result_key);
            let keys = store
                .prefix_entries(&prefix)
                .await?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            let mut operations = keys
                .into_iter()
                .map(|key| DirectRecordStoreWrite::Delete { key })
                .collect::<Vec<_>>();
            if let Some(members) = rewrite {
                for member in members {
                    operations.push(settled_result_member_storage_write(&authority_result_key, member)?);
                }
            } else {
                for member in adds {
                    operations.push(settled_result_member_storage_write(&authority_result_key, member)?);
                }
            }
            store.write_many(&operations).await?;
            return Ok(());
        }

        let mut operations = Vec::with_capacity(removes.len() + adds.len());
        for member in removes {
            operations.push(DirectRecordStoreWrite::Delete {
                key: settled_result_member_key(&authority_result_key, member)?,
            });
        }
        for member in adds {
            operations.push(settled_result_member_storage_write(&authority_result_key, member)?);
        }
        if !operations.is_empty() {
            store.write_many(&operations).await?;
        }
        Ok(())
    }

    async fn persist_settled_program_facts_delta(
        &self,
        authority_result_key: AuthorityResultKey,
        cleared: bool,
        adds: &[ViewFactEntry],
        removes: &[ViewFactEntry],
        rewrite: Option<&BTreeSet<ViewFactEntry>>,
    ) -> Result<(), Error> {
        let store = self
            .database
            .direct_record_store(SETTLED_PROGRAM_FACTS_STORE)?;
        if cleared || rewrite.is_some() {
            let prefix = authority_result_store_prefix(&authority_result_key);
            let keys = store
                .prefix_entries(&prefix)
                .await?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            let mut operations = keys
                .into_iter()
                .map(|key| DirectRecordStoreWrite::Delete { key })
                .collect::<Vec<_>>();
            if let Some(facts) = rewrite {
                for fact in facts {
                    operations.push(settled_program_fact_storage_write(&authority_result_key, fact)?);
                }
            } else {
                for fact in adds {
                    operations.push(settled_program_fact_storage_write(&authority_result_key, fact)?);
                }
            }
            store.write_many(&operations).await?;
            return Ok(());
        }

        let mut operations = Vec::with_capacity(removes.len() + adds.len());
        for fact in removes {
            operations.push(DirectRecordStoreWrite::Delete {
                key: settled_program_fact_key(&authority_result_key, fact)?,
            });
        }
        for fact in adds {
            operations.push(settled_program_fact_storage_write(&authority_result_key, fact)?);
        }
        if !operations.is_empty() {
            store.write_many(&operations).await?;
        }
        Ok(())
    }

    async fn clear_all_settled_result_state(&mut self) -> Result<(), Error> {
        for store_name in [SETTLED_RESULT_MEMBERS_STORE, SETTLED_PROGRAM_FACTS_STORE] {
            let store = self.database.direct_record_store(store_name)?;
            let keys = store
                .prefix_entries(&[])
                .await?
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            for key in keys {
                store.delete(&key).await?;
            }
        }
        self.query.settled_result_sets.clear();
        self.query.local_materialized_window_binding_views.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        self.database.flush().await?;
        self.persist_clean_close_marker().await?;
        self.database.close().await?;
        Ok(())
    }

    async fn recover_known_state_facts(&mut self) -> Result<(), Error> {
        self.query.settled_through_by_binding_view.clear();
        self.query.authorization_progress_by_binding_view.clear();
        self.query.settled_result_sets.clear();
        self.query.local_materialized_window_binding_views.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        // Validate the complete durable closure off to the side.  Open/recovery
        // must not leave even a prefix of the recovered state resident when a
        // later store entry is malformed.
        let mut settled_through_by_binding_view = BTreeMap::new();
        let mut authorization_progress_by_binding_view = BTreeMap::new();
        let store = self.database.direct_record_store(KNOWN_STATE_FACTS_STORE)?;
        for entry in store.prefix_entries(&[]).await? {
            let authority_result_key = authority_result_key_from_store_prefix(
                &entry.key,
                "known-state authority result key must be valid",
            )?;
            let settled_through = match entry.value.get_idx(0)? {
                Value::U64(value) => GlobalTime(value),
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state settled-through must be u64",
                    ));
                }
            };
            let binding_view_key = authority_result_key.binding_view;
            settled_through_by_binding_view.insert(binding_view_key, settled_through);
            match entry.value.get_idx(1)? {
                Value::U64(progress) if progress != u64::MAX => {
                    authorization_progress_by_binding_view.insert(binding_view_key, progress);
                }
                Value::U64(_) => {}
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "known-state authorization progress must be u64",
                    ));
                }
            }
        }
        let store = self
            .database
            .direct_record_store(SETTLED_RESULT_MEMBERS_STORE)?;
        let mut recovered_members = Vec::new();
        for entry in store.prefix_entries(&[]).await? {
            let Some((member_digest, prefix)) = entry.key.split_last() else {
                return Err(Error::InvalidStoredValue("settled result member key is empty"));
            };
            let authority_result_key = authority_result_key_from_store_prefix(
                prefix,
                "settled result member binding key must be valid",
            )?;
            let member_digest = match member_digest {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled result member digest must be bytes",
                    ));
                }
            };
            if member_digest.len() != 32 {
                return Err(Error::InvalidStoredValue(
                    "settled result member digest must be 32 bytes",
                ));
            }
            let member_bytes = match entry.value.get_idx(0)? {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled result member payload must be bytes",
                    ));
                }
            };
            if settled_result_member_digest(&member_bytes).as_slice() != member_digest {
                return Err(Error::InvalidStoredValue(
                    "settled result member payload does not match its digest",
                ));
            }
            let member = result_member_from_storage_bytes(&member_bytes)?;
            recovered_members.push((authority_result_key.binding_view, member));
        }
        let mut settled_result_sets =
            BTreeMap::<BindingViewKey, BTreeSet<ResultMemberEntry>>::new();
        let mut settled_result_row_index =
            BTreeMap::<BindingViewKey, BTreeMap<ResultRowMembershipKey, ResultMemberEntry>>::new();
        for (binding_view_key, member) in recovered_members {
            if let Some(row_key) = Self::result_member_row_key(&member) {
                settled_result_row_index
                    .entry(binding_view_key)
                    .or_default()
                    .insert(row_key, member.clone());
            }
            settled_result_sets
                .entry(binding_view_key)
                .or_default()
                .insert(member);
        }

        let store = self
            .database
            .direct_record_store(SETTLED_PROGRAM_FACTS_STORE)?;
        let mut settled_program_facts = BTreeMap::<BindingViewKey, BTreeSet<ViewFactEntry>>::new();
        for entry in store.prefix_entries(&[]).await? {
            let Some((fact_digest, prefix)) = entry.key.split_last() else {
                return Err(Error::InvalidStoredValue("settled program fact key is empty"));
            };
            let authority_result_key = authority_result_key_from_store_prefix(
                prefix,
                "settled program fact binding key must be valid",
            )?;
            let fact_digest = match fact_digest {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact digest must be bytes",
                    ));
                }
            };
            if fact_digest.len() != 32 {
                return Err(Error::InvalidStoredValue(
                    "settled program fact digest must be 32 bytes",
                ));
            }
            let fact_bytes = match entry.value.get_idx(0)? {
                Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "settled program fact payload must be bytes",
                    ));
                }
            };
            if settled_program_fact_digest(&fact_bytes).as_slice() != fact_digest {
                return Err(Error::InvalidStoredValue(
                    "settled program fact payload does not match its digest",
                ));
            }
            let fact = codec::program_fact_from_storage_bytes(&fact_bytes)?;
            settled_program_facts
                .entry(authority_result_key.binding_view)
                .or_default()
                .insert(fact);
        }
        self.query.settled_through_by_binding_view = settled_through_by_binding_view;
        self.query.authorization_progress_by_binding_view = authorization_progress_by_binding_view;
        self.query.settled_result_sets = settled_result_sets;
        self.query.settled_result_row_index = settled_result_row_index;
        self.query.settled_program_facts = settled_program_facts;
        Ok(())
    }

    /// Return locally-originated rejected transactions retained for retry.
    pub fn rejected_transactions(&self) -> Vec<TxId> {
        self.rejections
            .rejected_transactions
            .keys()
            .copied()
            .collect()
    }

    /// Return a locally-originated rejected transaction payload retained for retry.
    pub fn rejected_transaction(&self, tx_id: TxId) -> Option<RejectedTransaction> {
        self.rejections.rejected_transactions.get(&tx_id).cloned()
    }

    /// Discard a locally-retained rejected transaction after the app acknowledges it.
    pub async fn discard_rejection(&mut self, tx_id: TxId) -> Result<(), Error> {
        if tx_id.node != self.node_uuid {
            return Ok(());
        }
        let Some(alias) = self.node_aliases.get(&self.node_uuid).copied() else {
            return Ok(());
        };
        let mut batch = self.database.open_batch();
        batch.delete(
            "jazz_rejected_transactions",
            rejected_transaction_primary_key(alias, tx_id),
        );
        for table_id in self.physical_table_ids() {
            let storage_table = physical_rejected_versions_table_name(table_id);
            for raw in self.database.primary_key_scan_raw(
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
                batch.delete(
                    storage_table.clone(),
                    rejected_version_primary_key_from_record(&record)?,
                );
            }
        }
        let applied = self.database.apply_batch(batch).await?;
let persisted = applied.persist().await;
self.database.finish_persistence(persisted)?;
        self.rejections.rejected_transactions.remove(&tx_id);
        Ok(())
    }

    /// Return stored edit-history entries for one row ordered by HLC
    /// observation order.
    ///
    /// The parents DAG is the authoritative causal structure; HLC order is a
    /// readable observation order. This method intentionally does no policy
    /// filtering: per the README visibility rule, if a current version is
    /// readable then all history for that visible row is readable, and a node
    /// only stores versions it may hold. Rejected transaction versions are not
    /// returned because rejection cleanup removes their stored row versions;
    /// use [`SingleNode::transaction_record`] for the transaction audit state.
    pub async fn row_history(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<HistoryEntry>, Error> {
        let mut entries = Vec::new();
        for version in self.query_row_versions(table, row_uuid).await? {
            let tx_id = self.version_tx_id(&version)?;
            let tx = self
                .query_transaction(tx_id)
                .await?
                .ok_or(Error::MissingTransaction(tx_id))?;
            let local_current = self
                .query_local_layer_winner_in_branch(
                    table,
                    version.branch_key(),
                    row_uuid,
                    version.layer(),
                )
                .await?
                .as_ref()
                .map(|winner| {
                    self.version_tx_id(winner)
                        .is_ok_and(|winner_tx| winner_tx == tx_id)
                })
                .unwrap_or(false);
            let global_current = self
                .query_global_layer_winner_in_branch(
                    table,
                    version.branch_key(),
                    row_uuid,
                    version.layer(),
                )
                .await?
                .as_ref()
                .map(|winner| {
                    self.version_tx_id(winner)
                        .is_ok_and(|winner_tx| winner_tx == tx_id)
                })
                .unwrap_or(false);
            entries.push(version.to_history_entry(&tx, local_current, global_current));
        }
        entries.sort_by_key(|entry| entry.tx_id().time.sort_key(entry.tx_id().node));
        Ok(entries)
    }

    /// Consume the node and return the underlying groove database.
    pub fn into_database(self) -> Database {
        self.database.into_inner()
    }

    /// Eagerly remove a Groove subscription from the runtime.
    pub(crate) fn unsubscribe_groove_subscription(
        &mut self,
        subscription_id: groove::ivm::SubscriptionId,
    ) -> bool {
        self.database.unsubscribe(subscription_id)
    }

    /// Resume suspended Groove evaluation, awaiting storage until every active
    /// session completes. This does not create an empty IVM tick; Groove
    /// remains the sole owner of evaluation and hydration progress.
    pub(crate) async fn drive_query_runtime(&mut self) -> Result<(), Error> {
        self.database.drive_progress().await.map_err(Error::Groove)
    }

    /// Resume all query work that can make progress now, without holding the
    /// caller open for storage-blocked nodes.
    #[allow(dead_code)] // Test-only and feature-gated direct callers use the no-owner form.
    pub(crate) async fn drive_ready_query_runtime(&mut self) -> Result<(), Error> {
        self.drive_ready_query_runtime_with_waker(None).await
    }

    /// Resume runnable query work and retain a host-owned wake bridge for any
    /// cold storage operation that starts during this owner turn.
    pub(crate) async fn drive_ready_query_runtime_with_waker(
        &mut self,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<(), Error> {
        self.database
            .drive_ready_progress_with_waker(progress_waker)
            .await
            .map_err(Error::Groove)
    }

    /// Whether an earlier non-blocking query-runtime turn left resumable work.
    pub(crate) fn has_pending_query_runtime(&self) -> bool {
        self.database.has_pending_progress()
    }

    pub(crate) async fn set_initial_sync_flush_cadence(
        &mut self,
        every: usize,
    ) -> Result<(), Error> {
        debug_assert!(every > 0);
        self.initial_sync_flush_cadence = Some(every);
        // The cadence only relaxes durability while the first snapshot is
        // active. Normal client writes retain a boundary per committed batch.
        self.database.set_write_flush_cadence(1).await?;
        Ok(())
    }

    pub(super) async fn begin_initial_sync_flush_cadence(&mut self) -> Result<(), Error> {
        let Some(every) = self.initial_sync_flush_cadence else {
            return Ok(());
        };
        if self.initial_sync_flush_active || self.initial_sync_flush_completed {
            return Ok(());
        }
        self.database.set_write_flush_cadence(every).await?;
        self.initial_sync_flush_active = true;
        Ok(())
    }

    pub(super) async fn finish_initial_sync_flush_cadence(&mut self) -> Result<(), Error> {
        if !self.initial_sync_flush_active {
            return Ok(());
        }
        self.database.flush_write_boundary().await?;
        self.database.set_write_flush_cadence(1).await?;
        self.initial_sync_flush_active = false;
        self.initial_sync_flush_completed = true;
        Ok(())
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only history-class byte estimate. The underlying contract is
    /// cheap whole-class sizing, not logical-prefix accounting.
    pub async fn history_class_bytes_for_test(&self) -> Result<Option<u64>, Error> {
        self.database
            .approximate_class_bytes("__groove_class_history")
            .await
            .map_err(Error::Groove)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only estimate of all Jazz physical-class bytes. This is the
    /// cheap class-CF meter used for memory-amplification receipts; it is not a
    /// logical table-prefix scan.
    pub async fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        let mut total = 0_u64;
        for class_cf in [
            "__groove_class_history",
            "__groove_class_register",
            "__groove_class_global_current",
            "__groove_class_ahead_current",
            "__groove_class_changes",
            "__groove_class_indices",
            "__groove_class_content",
            "__groove_class_meta",
        ] {
            total += self
                .database
                .approximate_class_bytes(class_cf)
                .await
                .map_err(Error::Groove)?
                .unwrap_or_default();
        }
        Ok(total)
    }

    pub(crate) fn groove_runtime_token(&self) -> u64 {
        self.groove_runtime_token
    }

    /// Simulate a live catalogue change that invalidates prepared Groove
    /// handles without replacing the durable node state.
    ///
    /// Production takes this path for changes such as same-version policy
    /// updates: existing subscriptions must rehydrate against the new runtime
    /// token while already-received authority state remains available.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn invalidate_groove_runtime_for_test(&mut self) {
        self.groove_runtime_token = crate::node::next_groove_runtime_token();
    }

    /// Return metrics for the most recent committed storage batch, if any.
    pub fn last_commit_metrics(&self) -> Option<&CommitMetrics> {
        self.database.last_commit_metrics()
    }

    /// Return metrics for the most recent Groove runtime tick, if any.
    pub fn last_tick_metrics(&self) -> Option<&groove::ivm::TickMetrics> {
        self.database.last_tick_metrics()
    }

    /// Test/bench-only runtime diagnostics used by performance receipts.
    #[cfg(any(test, feature = "testing"))]
    pub fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.database.runtime_stats()
    }

    /// Return accumulated storage-read metrics since the last reset.
    pub fn storage_read_metrics(&self) -> groove::db::StorageReadMetrics {
        self.database.storage_read_metrics()
    }

    /// Reset accumulated storage-read metrics.
    pub fn reset_storage_read_metrics(&self) {
        self.database.reset_storage_read_metrics();
    }

    /// Return accumulated storage-read metrics and reset them.
    pub fn take_storage_read_metrics(&self) -> groove::db::StorageReadMetrics {
        self.database.take_storage_read_metrics()
    }

}
