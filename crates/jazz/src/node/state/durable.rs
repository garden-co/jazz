impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    const SCOPE_RELAY_REPAIR_LEDGER_FORMAT_V1: u64 = 1;

    /// Record exact row versions successfully applied from this durable
    /// relay's selected authority. Deliberately append-only: later authority
    /// removal controls future delivery but cannot erase retained knowledge.
    pub(crate) async fn record_scope_relay_authoritative_bundles(
        &self,
        bundles: &[VersionBundle],
    ) -> Result<(), Error> {
        let Some(scope) = self.client_relay_scope() else {
            return Ok(());
        };
        let (owner, subject) = scope.durable_components();
        let digest = scope.durable_digest();
        let store = self
            .database
            .direct_record_store(SCOPE_RELAY_REPAIR_LEDGER_STORE)?;
        let mut writes = Vec::new();
        for bundle in bundles {
            for version in &bundle.versions {
                let table_id =
                    self.physical_table_id_for_schema(version.schema_version(), version.table())?;
                writes.push(DirectRecordStoreWrite::Set {
                    key: vec![
                        Value::Bytes(digest.to_vec()),
                        Value::U64(table_id.0),
                        Value::Uuid(version.row_uuid().0),
                        Value::U64(bundle.tx.tx_id.time.0),
                        Value::Uuid(bundle.tx.tx_id.node.0),
                    ],
                    value: vec![
                        Value::U64(Self::SCOPE_RELAY_REPAIR_LEDGER_FORMAT_V1),
                        Value::String(owner.to_owned()),
                        Value::Nullable(
                            subject
                                .as_ref()
                                .map(|value| Box::new(Value::String(value.clone()))),
                        ),
                    ],
                });
            }
        }
        if !writes.is_empty() {
            store.write_many(&writes).await?;
        }
        Ok(())
    }

    /// A row-version payload is durable same-scope disclosure evidence only
    /// when its pending repair still belongs to the selected authority
    /// receipt. Stale/fallback payloads can be ingested as cache data but are
    /// never repair authority.
    pub(crate) async fn record_scope_relay_authoritative_repair_payloads(
        &self,
        bundles: &[VersionBundle],
        authority_receipt_eligible: bool,
    ) -> Result<(), Error> {
        if authority_receipt_eligible {
            self.record_scope_relay_authoritative_bundles(bundles)
                .await?;
        }
        Ok(())
    }

    /// Record a foreground transaction once this exact scope's durable relay
    /// has accepted it locally. Its caller establishes the live admitted
    /// session and author ownership; this helper only persists the immutable
    /// row-version identities.
    pub(crate) async fn record_scope_relay_authored_pending_versions(
        &self,
        tx: &Transaction,
        versions: &[VersionRecord],
        admitted_session: AuthorSubject,
    ) -> Result<(), Error> {
        let Some(scope) = self.client_relay_scope() else {
            return Ok(());
        };
        if !scope.admits_session(admitted_session) || tx.made_by != admitted_session {
            return Ok(());
        }
        let (owner, subject) = scope.durable_components();
        let digest = scope.durable_digest();
        let store = self
            .database
            .direct_record_store(SCOPE_RELAY_REPAIR_LEDGER_STORE)?;
        let mut writes = Vec::new();
        for version in versions {
            let table_id =
                self.physical_table_id_for_schema(version.schema_version(), version.table())?;
            writes.push(DirectRecordStoreWrite::Set {
                key: vec![
                    Value::Bytes(digest.to_vec()),
                    Value::U64(table_id.0),
                    Value::Uuid(version.row_uuid().0),
                    Value::U64(tx.tx_id.time.0),
                    Value::Uuid(tx.tx_id.node.0),
                ],
                value: vec![
                    Value::U64(Self::SCOPE_RELAY_REPAIR_LEDGER_FORMAT_V1),
                    Value::String(owner.to_owned()),
                    Value::Nullable(
                        subject
                            .as_ref()
                            .map(|value| Box::new(Value::String(value.clone()))),
                    ),
                ],
            });
        }
        if !writes.is_empty() {
            store.write_many(&writes).await?;
        }
        Ok(())
    }

    pub(crate) async fn scope_relay_repair_ledger_contains(
        &self,
        table_id: PhysicalTableId,
        request: &RowVersionRef,
    ) -> Result<bool, Error> {
        let Some(scope) = self.client_relay_scope() else {
            return Ok(false);
        };
        let (expected_owner, expected_subject) = scope.durable_components();
        let store = self
            .database
            .direct_record_store(SCOPE_RELAY_REPAIR_LEDGER_STORE)?;
        let record = store
            .get(&[
                Value::Bytes(scope.durable_digest().to_vec()),
                Value::U64(table_id.0),
                Value::Uuid(request.row_uuid.0),
                Value::U64(request.tx_time.0),
                Value::Uuid(request.tx_node_id.0),
            ])
            .await?;
        let Some(record) = record else {
            return Ok(false);
        };
        match record.get_idx(0)? {
            Value::U64(Self::SCOPE_RELAY_REPAIR_LEDGER_FORMAT_V1) => {}
            _ => {
                return Err(Error::InvalidStoredValue(
                    "unknown scope relay ledger format",
                ));
            }
        }
        let owner = match record.get_idx(1)? {
            Value::String(value) => value,
            _ => {
                return Err(Error::InvalidStoredValue(
                    "scope relay ledger owner must be string",
                ));
            }
        };
        let subject = match record.get_idx(2)? {
            Value::Nullable(None) => None,
            Value::Nullable(Some(value)) => match value.as_ref() {
                Value::String(value) => Some(value.to_owned()),
                _ => {
                    return Err(Error::InvalidStoredValue(
                        "scope relay ledger subject must be string",
                    ));
                }
            },
            _ => {
                return Err(Error::InvalidStoredValue(
                    "scope relay ledger subject must be nullable string",
                ));
            }
        };
        if owner != expected_owner || subject != expected_subject {
            return Err(Error::InvalidStoredValue(
                "scope relay ledger value does not match admitted scope",
            ));
        }
        Ok(true)
    }

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

    pub(crate) fn schema_with_active_permissions(&self, id: SchemaVersionId) -> Option<&JazzSchema> {
        if id == self.catalogue.active_schema.schema {
            Some(&self.catalogue.active_schema.compiled)
        } else {
            self.catalogue.catalogue_schemas.get(&id).map(|schema| &schema.schema)
        }
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
        Ok(self.catalogue.active_schema.wire_pointer())
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
        if self.catalogue.active_schema.schema == self.catalogue.local_schema_version_id {
            self.catalogue.active_schema.compiled = schema.clone();
        }
        self.catalogue
            .catalogue_schemas
            .get_mut(&self.catalogue.local_schema_version_id)
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
        if let Some(schema) = schemas.iter_mut().find(|schema| schema.id == self.catalogue.active_schema.schema) {
            schema.schema = self.catalogue.active_schema.compiled.clone();
        }
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
            .ok_or(Error::InvalidStoredValue(
                "catalogue genesis schema is missing",
            ))?;
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
            .ok_or(Error::InvalidStoredValue(
                "genesis physical mapping missing",
            ))?
            .identities
            .clone();
        Ok(crate::protocol::CatalogueSnapshot {
            genesis_physical_identities,
            schemas,
            lineages,
            current_write_schema: self.catalogue.active_schema.wire_pointer(),
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
        match settled {
            DurabilityTier::None | DurabilityTier::Local => self.current_rows(table, settled).await,
            DurabilityTier::Edge => self.current_rows(table, settled).await,
            DurabilityTier::Global => {
                // This convenience surface is used by topology tests, but it
                // must exercise the same local Groove terminal as a serving
                // subscription. Reading the authority result cache here would
                // silently preserve the retired result-output bypass.
                let shape = crate::query::Query::from(table)
                    .validate(&self.catalogue.schema)
                    .map_err(|error| Error::Query(Box::new(error)))?;
                let binding = shape
                    .bind(BTreeMap::new())
                    .map_err(|error| Error::Query(Box::new(error)))?;
                if self.is_history_complete() {
                    self.query_rows_with_prepared_plan_for_identity(
                        &shape,
                        &binding,
                        DurabilityTier::Global,
                        None,
                        AuthorSubject::SYSTEM,
                    )
                    .await
                } else {
                    // A partial client has no authority to reopen this query
                    // as SYSTEM. It consumes the exact scoped CoveredInput
                    // closure installed by the received whole-table
                    // subscription, through the same receiver-local graph as
                    // every other client read.
                    self.query_rows_for_client(
                        &shape,
                        &binding,
                        DurabilityTier::Global,
                        AuthorSubject::SYSTEM,
                    )
                    .await
                }
            }
        }
    }

    /// Return the legacy transaction fate tuple by projecting stored status.
    /// Payload author/contribution validation belongs to full transaction reads;
    /// this read retains storage framing and status-field validation.
    pub async fn transaction_state(
        &mut self,
        tx_id: TxId,
    ) -> Option<(Fate, Option<GlobalTime>, DurabilityTier)> {
        self.query_transaction_state(tx_id)
            .await
            .ok()
            .flatten()
            .map(|(fate, global_time, stored_durability)| {
                let durability = if self.pending_persistence.contains(&tx_id) {
                    DurabilityTier::None
                } else {
                    stored_durability
                };
                (fate, global_time, durability)
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
        Ok(self
            .pending_transaction_scan_for(node, author)
            .await?
            .tx_ids)
    }

    /// Return unsettled transactions by `author`, irrespective of their
    /// originating node. A dedicated browser relay uses this when it reopens:
    /// relayed main-thread commits retain the main thread's node id, so the
    /// relay's ordinary local-origin recovery scan cannot find them.
    pub(crate) async fn pending_transaction_ids_for_author(
        &mut self,
        author: AuthorSubject,
    ) -> Result<Vec<TxId>, Error> {
        self.below_global_transaction_ids(Some(author), false, false)
            .await
    }

    /// A Global synchronization barrier also needs the authority timestamp.
    /// A Global durability observation alone is not a completed write receipt.
    pub(crate) async fn synchronizing_transaction_ids_for_author(
        &mut self,
        author: AuthorSubject,
    ) -> Result<Vec<TxId>, Error> {
        self.below_global_transaction_ids(Some(author), false, true)
            .await
    }

    /// A trusted backend owns every author scope created by its node. Restrict
    /// this scan by transaction origin, never by its SYSTEM display identity.
    pub(crate) async fn synchronizing_transaction_ids_for_node(
        &mut self,
        node: NodeUuid,
    ) -> Result<Vec<TxId>, Error> {
        Ok(self
            .below_global_transaction_ids(None, false, true)
            .await?
            .into_iter()
            .filter(|tx| tx.node == node)
            .collect())
    }

    async fn below_global_transaction_ids(
        &mut self,
        author: Option<AuthorSubject>,
        edge_only: bool,
        include_missing_authority_timestamp: bool,
    ) -> Result<Vec<TxId>, Error> {
        let mut candidates = Vec::new();
        for raw in self
            .database
            .index_scan_raw(
                "jazz_transactions",
                "by_global_time",
                &[Value::Nullable(None)],
            )
            .await?
        {
            let record = raw.record();
            let fate = record.get_enum(TransactionRowRecord::FIELD_FATE_IDX)?;
            let made_by =
                RowAuthor::from_record(record.get_record(TransactionRowRecord::FIELD_MADE_BY_IDX)?)
                    .map_err(|_| groove::records::Error::NonCanonicalRecord)?
                    .as_author_subject();
            let durability = durability_from_discriminant(
                record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
            )?;
            if author.is_some_and(|author| !durable_author_matches(author, made_by))
                || if edge_only {
                    fate != 1 || durability != DurabilityTier::Edge
                } else {
                    !(fate == 0 || fate == 1)
                        || (!include_missing_authority_timestamp
                            && durability >= DurabilityTier::Global)
                }
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
        // `$madeBy` is durable provenance. A local system capability records
        // its specific node origin, while authority evaluation continues to
        // use `AuthorSubject::SYSTEM` separately.
        let durable_author = if author == AuthorSubject::SYSTEM {
            AuthorSubject::system_at(node)
        } else {
            author
        };

        let mut scan = PendingTransactionScan::default();
        for raw in self
            .database
            .index_scan_raw(
                "jazz_transactions",
                "by_global_time",
                &[Value::Nullable(None)],
            )
            .await?
        {
            scan.records_visited += 1;
            let record = raw.record();
            if NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?) != node_alias
                || RowAuthor::from_record(
                    record.get_record(TransactionRowRecord::FIELD_MADE_BY_IDX)?,
                )
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?
                .as_author_subject()
                    != durable_author
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
        Some(TxId::new(time, self.resolve_node_alias(alias).await.ok()??))
    }

    /// Discard authority proof during rebuild or eviction, preserving live receipt ordering.
    pub(crate) fn invalidate_subscription_scopes(&mut self) {
        // Rebuild/eviction invalidates authority proof, not the ordering of
        // receipts awaited by still-live foregrounds. Reusing generation one
        // after invalidation can strand a read already waiting for > one.
        // Keep only this process-local counter; no membership, settlement,
        // predecessor, compiled source, or pending publication survives.
        #[cfg(any(test, feature = "testing"))]
        crate::delivery_diagnostics::record(|| format!("invalidate_scopes runtime={} receipts={}", self.groove_runtime_token(), self.query.authority_results.len()));
        for state in self.query.authority_results.values_mut() {
            *state = AuthorityResultState {
                applied_view_update_generation: state.applied_view_update_generation,
                ..AuthorityResultState::default()
            };
        }
        self.query.retained_root_window_sources.clear();
    }

    async fn persist_policy_binding_directory(
        &self,
        policy: &PolicyBindingKey,
    ) -> Result<(), Error> {
        let digest = policy.directory_digest();
        let claims = policy
            .directory_value()
            .map_err(|_| Error::InvalidStoredValue("policy binding claims must encode"))?;
        let store = self
            .database
            .direct_record_store(AUTHORITY_POLICY_BINDINGS_STORE)?;
        let key = [Value::Bytes(digest.to_vec())];
        if let Some(existing) = store.get(&key).await? {
            let Value::String(subject) = existing.get_idx(0)? else {
                return Err(Error::InvalidStoredValue(
                    "policy binding directory subject must be string",
                ));
            };
            let existing = crate::protocol::PolicyBindingKey::from_directory_value(
                AuthorSubject::from_canonical(&subject).map_err(|_| {
                    Error::InvalidStoredValue("policy binding directory subject is invalid")
                })?,
                existing.get_idx(1)?,
            )
            .map_err(|_| {
                Error::InvalidStoredValue("policy binding directory claims are invalid")
            })?;
            if existing != *policy {
                return Err(Error::InvalidStoredValue(
                    "policy binding digest aliases a distinct exact policy identity",
                ));
            }
            return Ok(());
        }
        store
            .set(
                &key,
                &[
                    Value::String(policy.identity.canonical().to_owned()),
                    claims,
                ],
            )
            .await?;
        Ok(())
    }

    /// Retired scope stores are disposable caches, never native data. Keep their
    /// registered store identities so historical roots open, but interpret no
    /// JPFK/JSIR payload, policy key, cursor or generation during recovery.
    async fn discard_legacy_subscription_scopes(&self) -> Result<(), Error> {
        for name in [KNOWN_STATE_FACTS_STORE, SETTLED_PROGRAM_FACTS_STORE] {
            let store = self.database.direct_record_store(name)?;
            let deletes = store
                .prefix_entries(&[])
                .await?
                .into_iter()
                .map(|entry| DirectRecordStoreWrite::Delete { key: entry.key })
                .collect::<Vec<_>>();
            if !deletes.is_empty() {
                store.write_many(&deletes).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        self.database.flush().await?;
        self.persist_clean_close_marker().await?;
        self.database.close().await?;
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
        for class_cf in self.try_current_schema()?.physical_column_families() {
            total += self
                .database
                .approximate_class_bytes(&class_cf)
                .await
                .map_err(Error::Groove)?
                .unwrap_or_default();
        }
        Ok(total)
    }

    /// All peer connections share the cached database-owner wake. Check and
    /// register under the owner lock so settlement cannot race registration.
    pub(crate) fn defer_catalogue_for_persistence(
        &self,
        waker: Option<&std::task::Waker>,
    ) -> Result<bool, Error> {
        Ok(self.database.wait_for_publication_settlement(waker)?)
    }

    pub(crate) fn physical_identity_generation(&self) -> u64 {
        self.physical_identity_generation
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

    #[cfg(test)]
    pub(crate) async fn rebuild_groove_runtime_for_test(&mut self) -> Result<(), Error> {
        self.rebuild_database_slot().await
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

/// Compare an authority/session request with durable provenance without ever
/// turning persisted node attribution back into the system capability.
fn durable_author_matches(requested: AuthorSubject, stored: AuthorSubject) -> bool {
    match requested {
        AuthorSubject::System => matches!(stored, AuthorSubject::SystemAt(_)),
        _ => stored == requested,
    }
}
