impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Open or create a node over the supplied storage.
    pub async fn new(node_uuid: NodeUuid, schema: JazzSchema, storage: S) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_history_complete(node_uuid, schema, storage, false).await
    }

    /// Open an edge-local runtime before it has received an authenticated
    /// authority catalogue.
    ///
    /// Unlike [`NodeState::new`], this deliberately does *not* create a
    /// durable genesis from `JazzSchema::empty()`.  Its only valid transition
    /// is installation of a trusted catalogue snapshot; application reads,
    /// writes, and current-schema access fail closed beforehand.
    pub(crate) async fn new_catalogue_uninitialized(
        node_uuid: NodeUuid,
        storage: S,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        let (storage, durable_genesis) = Self::discover_durable_catalogue_genesis(storage).await?;
        if let Some(schema) = durable_genesis {
            // A fresh process cannot assume the temporary empty schema it
            // would use for an uninitialized edge.  Recover the authority
            // genesis from the durable catalogue first, then use the normal
            // ready open path so all physical layouts are reconstructed from
            // the real lineage.
            return Self::new_with_options_inner(
                node_uuid,
                schema,
                storage,
                false,
                CatalogueBootstrapState::Ready,
                #[cfg(feature = "testing")]
                None,
            )
            .await;
        }
        Self::new_with_options_inner(
            node_uuid,
            JazzSchema::empty(),
            storage,
            false,
            CatalogueBootstrapState::Uninitialized,
            #[cfg(feature = "testing")]
            None,
        )
        .await
    }

    /// Read only the fixed catalogue metadata layout to discover an already
    /// durable authority genesis before choosing an application schema for a
    /// fresh process.  This is the inverse of the uninitialized constructor:
    /// it never uses `JazzSchema::empty()` as a genesis candidate.
    async fn discover_durable_catalogue_genesis(
        storage: S,
    ) -> Result<(BoxedStorage, Option<JazzSchema>), Error>
    where
        S: ReopenableStorage + 'static,
    {
        let bootstrap_schema = JazzSchema::empty();
        // Dynamic discovery must inspect the fixed history/branch/fate stores
        // too: an empty catalogue does not make an existing Jazz store safe to
        // repurpose as an uninitialized edge.
        let meta_schema = bootstrap_schema.lower_to_groove();
        let meta_database =
            Database::new_with_storage_layout(meta_schema, storage, StorageLayout::jazz_class_v1())
                .await?;
        let mut genesis = None;
        let mut schemas = BTreeMap::new();
        let mut bootstrap_ready = None;
        let mut staged_lineages = BTreeMap::new();
        let mut active_lineages = BTreeMap::new();
        let mut has_catalogue_residue = false;
        for raw in meta_database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .await?
        {
            has_catalogue_residue = true;
            let record = raw.record();
            match record.get_bytes(CatalogueRowRecord::FIELD_KIND_IDX)? {
                b"genesis" => {
                    let schema =
                        SchemaVersionId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?);
                    if genesis.replace(schema).is_some() {
                        return Err(Error::InvalidStoredValue(
                            "duplicate catalogue genesis marker",
                        ));
                    }
                }
                b"schema" => {
                    let schema: SchemaVersion = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if schema.id
                        != SchemaVersionId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?)
                        || schemas.insert(schema.id, schema).is_some()
                    {
                        return Err(Error::InvalidStoredValue("catalogue schema id mismatch"));
                    }
                }
                b"schema_lineage_active" => {
                    let active: SchemaLineageActivation = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if active.id.0 != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || active.catalogue_seq == 0
                        || active_lineages
                            .insert(active.id, active.catalogue_seq)
                            .is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "catalogue bootstrap active lineage marker is malformed",
                        ));
                    }
                }
                b"schema_lineage_staged" => {
                    let staged: StagedSchemaLineage = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if staged.publication.id.0
                        != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || staged.catalogue_seq == 0
                        || staged_lineages
                            .insert(staged.publication.id, staged)
                            .is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "catalogue bootstrap staged lineage marker is malformed",
                        ));
                    }
                }
                b"bootstrap_ready" => {
                    let ready: CatalogueBootstrapReady = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if ready.genesis.0 != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || bootstrap_ready.replace(ready).is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "duplicate or malformed catalogue bootstrap marker",
                        ));
                    }
                }
                _ => {}
            }
        }
        let mapping_ids = meta_database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .await?
            .into_iter()
            .map(|raw| {
                Ok(SchemaVersionId(
                    raw.record()
                        .get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX)?,
                ))
            })
            .collect::<Result<BTreeSet<_>, Error>>()?;
        let durable_pointer = meta_database
            .primary_key_last_raw("jazz_catalogue_pointer", &[])
            .await?
            .map(|raw| {
                let record = raw.record();
                Ok::<CurrentWriteSchema, Error>(CurrentWriteSchema {
                    revision: record.get_u64(CataloguePointerRowRecord::FIELD_REVISION_IDX)?,
                    schema: SchemaVersionId(
                        record.get_uuid(CataloguePointerRowRecord::FIELD_SCHEMA_IDX)?,
                    ),
                })
            })
            .transpose()?;
        let mut has_non_catalogue_residue = false;
        for table in [
            "jazz_transactions",
            "jazz_rejected_transactions",
            "jazz_pending_edges",
            "jazz_merge_heads",
            "jazz_global_changes",
            "jazz_deletion_history",
        ] {
            if !meta_database
                .primary_key_scan_raw(table, &[])
                .await?
                .is_empty()
            {
                has_non_catalogue_residue = true;
                break;
            }
        }
        let has_residue = has_catalogue_residue
            || !mapping_ids.is_empty()
            || durable_pointer.is_some()
            || has_non_catalogue_residue;
        let schema = match bootstrap_ready {
            None if !has_residue => None,
            None if has_non_catalogue_residue => {
                return Err(Error::InvalidStoredValue(
                    "dynamic catalogue state cannot initialize over durable history",
                ));
            }
            None => {
                return Err(Error::InvalidStoredValue(
                    "dynamic catalogue state has no bootstrap completion marker",
                ));
            }
            Some(ready) => {
                let mut active_lineage_targets = BTreeSet::new();
                let mut active_catalogue_sequences = BTreeSet::new();
                for staged in staged_lineages.values() {
                    Self::validate_durable_staged_lineage(staged, &schemas)?;
                    match active_lineages.remove(&staged.publication.id) {
                        Some(sequence) if sequence == staged.catalogue_seq => {
                            if schemas.get(&staged.publication.schema.id)
                                != Some(&staged.publication.schema)
                                || !active_lineage_targets.insert(staged.publication.schema.id)
                            {
                                return Err(Error::InvalidStoredValue(
                                    "catalogue bootstrap active lineage does not own its schema",
                                ));
                            }
                            if !active_catalogue_sequences.insert(sequence) {
                                return Err(Error::InvalidStoredValue(
                                    "catalogue bootstrap active lineage marker is malformed",
                                ));
                            }
                        }
                        Some(_) => {
                            return Err(Error::InvalidStoredValue(
                                "catalogue bootstrap active lineage marker conflicts with payload",
                            ));
                        }
                        None if schemas.contains_key(&staged.publication.schema.id)
                            || mapping_ids.contains(&staged.publication.schema.id) =>
                        {
                            return Err(Error::InvalidStoredValue(
                                "catalogue bootstrap inactive lineage has durable target state",
                            ));
                        }
                        None => {}
                    }
                }
                if !active_lineages.is_empty() {
                    return Err(Error::InvalidStoredValue(
                        "catalogue bootstrap active lineage is missing canonical payload",
                    ));
                }
                let expected_schema_ids = std::iter::once(ready.genesis)
                    .chain(active_lineage_targets)
                    .collect::<BTreeSet<_>>();
                if genesis != Some(ready.genesis)
                    || durable_pointer != Some(ready.current_write_schema)
                    || !schemas.contains_key(&ready.genesis)
                    || schemas.keys().copied().collect::<BTreeSet<_>>() != expected_schema_ids
                    || mapping_ids != expected_schema_ids
                    || active_catalogue_sequences.len()
                        != usize::try_from(ready.active_catalogue_seq).map_err(|_| {
                            Error::InvalidStoredValue("catalogue bootstrap sequence exceeds usize")
                        })?
                    || active_catalogue_sequences
                        .iter()
                        .copied()
                        .ne(1..=ready.active_catalogue_seq)
                {
                    return Err(Error::InvalidStoredValue(
                        "catalogue bootstrap completion marker does not match durable catalogue",
                    ));
                }
                Some(
                    schemas
                        .remove(&ready.genesis)
                        .expect("validated durable genesis schema must exist")
                        .schema,
                )
            }
        };
        Ok((meta_database.into_storage(), schema))
    }

    /// Open or create a node that is known to hold complete settled history.
    ///
    /// This is the authority/local-complete constructor for historical reads.
    /// Ordinary downstream clients should use [`NodeState::new`], which fails
    /// historical handle reads closed until a complete-history subscription
    /// path marks the queried shape complete in a later slice.
    pub async fn new_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_history_complete(node_uuid, schema, storage, true).await
    }

    /// Rebuild the groove layer over the same storage using the standard open path.
    pub async fn reopen_in_place(self) -> Result<NodeState<BoxedStorage>, Error>
    where
        S: ReopenableStorage + 'static,
    {
        let NodeState {
            node_uuid,
            catalogue,
            catalogue_bootstrap_state,
            database,
            chunk_resolver,
            history_complete,
            ..
        } = self;
        let storage = database.into_inner().into_storage();
        let mut reopened = match catalogue_bootstrap_state {
            CatalogueBootstrapState::Uninitialized => {
                NodeState::<BoxedStorage>::new_catalogue_uninitialized(node_uuid, storage).await?
            }
            CatalogueBootstrapState::Ready => {
                NodeState::<BoxedStorage>::new_with_history_complete(
                    node_uuid,
                    catalogue.schema,
                    storage,
                    history_complete,
                )
                .await?
            }
        };
        reopened
            .database
            .set_missing_chunk_resolver(chunk_resolver.clone());
        reopened.local_chunk_reader = reopened.database.local_chunk_reader();
        reopened.chunk_resolver = chunk_resolver;
        reopened.content_runtime_provider = reopened.database.owned_chunk_provider();
        Ok(reopened)
    }

    async fn new_with_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_options(node_uuid, schema, storage, history_complete).await
    }

    async fn new_with_options(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
    ) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_with_options_inner(
            node_uuid,
            schema,
            storage,
            history_complete,
            CatalogueBootstrapState::Ready,
            #[cfg(feature = "testing")]
            None,
        )
        .await
    }

    #[cfg(feature = "testing")]
    /// Open a node and attribute durable recovery without changing open semantics.
    pub async fn new_with_open_receipt_for_test(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: S,
        history_complete: bool,
    ) -> Result<(Self, NodeOpenReceipt), Error>
    where
        S: ReopenableStorage + 'static,
    {
        let mut receipt = NodeOpenReceipt::default();
        let node = Self::new_with_options_inner(
            node_uuid,
            schema,
            storage,
            history_complete,
            CatalogueBootstrapState::Ready,
            Some(&mut receipt),
        )
        .await?;
        Ok((node, receipt))
    }

    async fn new_with_options_inner<T>(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        storage: T,
        history_complete: bool,
        catalogue_bootstrap_state: CatalogueBootstrapState,
        #[cfg(feature = "testing")] mut receipt: Option<&mut NodeOpenReceipt>,
    ) -> Result<Self, Error>
    where
        T: ReopenableStorage + 'static,
        S: ReopenableStorage,
    {
        let current_schema_version_id = schema.version_id();
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        let CatalogueOpenState {
            storage,
            mut schemas,
            mut lenses,
            mut schema_version_aliases,
            mut physical_mappings,
            mut staged_lineages,
            pending_lineages,
            mut active_lineages_by_target,
            mut active_catalogue_seq,
            pending_write_pointers,
            next_physical_table_id,
            next_physical_column_id,
            current_write_schema,
            catalogue_bootstrap_marker,
        } = Self::open_catalogue_stage(schema.clone(), storage, catalogue_bootstrap_state).await?;
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.catalogue_open = started.elapsed();
        }
        let mut registration_schemas = schemas.clone();
        let mut registration_aliases = schema_version_aliases.clone();
        let mut registration_mappings = physical_mappings.clone();
        for staged in staged_lineages.values() {
            registration_schemas.insert(
                staged.publication.schema.id,
                staged.publication.schema.clone(),
            );
            registration_aliases.insert(staged.publication.schema.id, staged.alias);
            registration_mappings.insert(staged.publication.schema.id, staged.mapping.clone());
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        let mut database = Self::open_full_database(
            &schema,
            &registration_schemas,
            &registration_aliases,
            &registration_mappings,
            storage,
        )
        .await?;
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.database_open = started.elapsed();
        }
        loop {
            let next = active_catalogue_seq.saturating_add(1);
            let Some(staged) = staged_lineages.get(&next).cloned() else {
                break;
            };
            if !schemas.contains_key(&staged.publication.lens.source) {
                break;
            }
            let mut batch = database.open_batch();
            Self::write_active_schema_lineage_to_batch(&mut batch, &staged)?;
            if catalogue_bootstrap_marker {
                let ready = CatalogueBootstrapReady {
                    genesis: current_schema_version_id,
                    current_write_schema,
                    active_catalogue_seq: next,
                };
                batch.update(
                    "jazz_catalogue",
                    vec![
                        Value::Bytes(b"bootstrap_ready".to_vec()),
                        Value::Uuid(ready.genesis.0),
                        Value::Bytes(serde_json::to_vec(&ready)?),
                    ],
                );
            }
            let applied = database.apply_batch(batch).await?;
            let persisted = applied.persist().await;
            database.finish_persistence(persisted)?;
            schemas.insert(
                staged.publication.schema.id,
                staged.publication.schema.clone(),
            );
            lenses.insert(staged.publication.lens.id, staged.publication.lens.clone());
            schema_version_aliases.insert(staged.publication.schema.id, staged.alias);
            physical_mappings.insert(staged.publication.schema.id, staged.mapping.clone());
            active_lineages_by_target.insert(staged.publication.schema.id, staged.clone());
            staged_lineages.remove(&next);
            active_catalogue_seq = next;
        }
        let current_schema_version_alias = schema_version_aliases
            .get(&current_schema_version_id)
            .copied();
        // Schema identity excludes policy and physical-index metadata. On a
        // reopen, retain the durable payload previously learned from the
        // authority instead of reverting to the caller's structural schema;
        // otherwise the first catalogue snapshot spuriously changes runtime
        // semantics and rebuilds warm subscriptions during resume.
        let active_schema = schemas
            .get(&current_schema_version_id)
            .map(|version| version.schema.clone())
            .unwrap_or_else(|| schema.clone());
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        let chunk_resolver: Rc<dyn groove::chunks::MissingChunkResolver> =
            Rc::new(groove::chunks::UnavailableChunkResolver);
        database.set_missing_chunk_resolver(chunk_resolver.clone());
        let content_runtime_provider = database.owned_chunk_provider();
        let local_chunk_reader = database.local_chunk_reader();
        let mut node = Self {
            node_uuid,
            self_node_alias: None,
            catalogue: SchemaCatalogue {
                current_schema_version_id,
                current_schema_version_alias,
                schema: active_schema,
                schema_version_aliases,
                catalogue_schemas: schemas,
                catalogue_lenses: lenses,
                physical_mappings,
                staged_lineages,
                pending_lineages,
                active_lineages_by_target,
                active_catalogue_seq,
                pending_write_pointers,
                next_physical_table_id,
                next_physical_column_id,
                lens_path_cache: BTreeMap::new(),
                compiled_lens_cache: BTreeMap::new(),
                physical_write_plan_cache: BTreeMap::new(),
                current_write_schema,
            },
            catalogue_bootstrap_state,
            catalogue_bootstrap_marker,
            clock: Clock {
                tx_time: TxTime::default(),
                global_time_register: GlobalTime::default(),
                locally_minted_global_times: BTreeSet::new(),
                committed_global_time: GlobalTime(0),
                applied_global_times_after_frontier: BTreeSet::new(),
            },
            parking: Parking::default(),
            query: QueryServing {
                query_shape_cache: BTreeMap::new(),
                read_policy_authorization_request_cache: BTreeMap::new(),
                policy_authorization_graph_cache: BTreeMap::new(),
                policy_proof_stack: Vec::new(),
                tx_version_tables_cache: BTreeMap::new(),
                tx_versions_cache: BTreeMap::new(),
                tx_version_tables_cache_order: VecDeque::new(),
                tx_version_tables_cache_order_set: BTreeSet::new(),
                version_storage_sources_cache: BTreeMap::new(),
                registered_shapes: BTreeMap::new(),
                registered_bindings: BTreeMap::new(),
                applied_view_update_generations: BTreeMap::new(),
                settled_result_sets: BTreeMap::new(),
                settled_result_row_index: BTreeMap::new(),
                settled_program_facts: BTreeMap::new(),
                settled_through_by_binding_view: BTreeMap::new(),
                authorization_progress_by_binding_view: BTreeMap::new(),
                known_state_declared_binding_views: BTreeSet::new(),
                initial_hydration_binding_views: BTreeSet::new(),
                deferred_publication_binding_views: BTreeSet::new(),
                pending_authoritative_reset_binding_views: BTreeSet::new(),
                pending_opening_binding_views: BTreeSet::new(),
                pending_terminal_operations_by_binding_view: BTreeMap::new(),
            },
            open_tx: OpenTxState {
                open_transactions: BTreeMap::new(),
                closed_batches: BTreeSet::new(),
                local_permission_subjects: BTreeMap::new(),
            },
            rejections: RejectionTracking::default(),
            database: DatabaseSlot::new(database),
            local_chunk_reader,
            chunk_resolver,
            large_value_staging_policy: LargeValueStagingPolicy::default(),
            large_value_ingress: RefCell::new(LargeValueIngressState::default()),
            content_runtime_provider,
            storage_type: std::marker::PhantomData,
            groove_runtime_token: next_groove_runtime_token(),
            history_complete,
            authored_commit_durability: DurabilityTier::Local,
            pending_persistence: BTreeSet::new(),
            node_aliases: BTreeMap::new(),
            ahead_current_keys: FxHashSet::default(),
            sync_metrics: SyncMetrics::default(),
            query_engine_read_metrics: QueryEngineReadMetrics::default(),
            #[cfg(any(test, feature = "testing"))]
            merge_head_reachability_walks: 0,
            session_claims: BTreeMap::new(),
            session_claim_revisions: BTreeMap::new(),
            permissions_ready: true,
            catalogue_activation_failed: false,
            #[cfg(any(test, feature = "testing"))]
            catalogue_activation_failpoint: None,
            initial_sync_flush_cadence: None,
            initial_sync_flush_active: false,
            initial_sync_flush_completed: false,
        };
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.state_init = started.elapsed();
        }
        let known_schema_versions = node
            .catalogue
            .catalogue_schemas
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for schema_version in known_schema_versions {
            node.ensure_provisional_physical_mapping(schema_version)
                .await?;
        }
        node.synchronize_physical_version_tables().await?;
        node.recover_pending_schema_lineages().await?;
        node.recover_pending_catalogue_pointers().await?;
        #[cfg(feature = "testing")]
        if let Some(receipt) = receipt.as_deref_mut() {
            let started = Instant::now();
            node.recover_from_storage_with_receipt(receipt).await?;
            receipt.recover_storage = started.elapsed();
        } else {
            node.recover_from_storage().await?;
        }
        #[cfg(not(feature = "testing"))]
        node.recover_from_storage().await?;
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        node.recover_known_state_facts().await?;
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.recover_known_state = started.elapsed();
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        #[cfg(feature = "testing")]
        if let Some(receipt) = receipt.as_deref_mut() {
            node.rebuild_ahead_current_keys_with_receipt(receipt)
                .await?;
        } else {
            node.rebuild_ahead_current_keys().await?;
        }
        #[cfg(not(feature = "testing"))]
        node.rebuild_ahead_current_keys().await?;
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.rebuild_ahead_current = started.elapsed();
        }
        #[cfg(feature = "testing")]
        let started = receipt.as_ref().map(|_| Instant::now());
        let self_node_alias = node.ensure_node_alias(node_uuid).await?;
        node.self_node_alias = Some(self_node_alias);
        let schema_alias = node
            .ensure_schema_version_alias(current_schema_version_id)
            .await?;
        node.catalogue.current_schema_version_alias = Some(schema_alias);
        #[cfg(feature = "testing")]
        if let (Some(receipt), Some(started)) = (&mut receipt, started) {
            receipt.finalize_catalogue = started.elapsed();
        }
        Ok(node)
    }

    async fn open_full_database(
        schema: &JazzSchema,
        catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
        schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
        physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
        storage: BoxedStorage,
    ) -> Result<Database, Error> {
        debug_assert_lowered_layouts(schema);
        let mut lowered = schema.lower_to_groove();
        lowered.tables.extend(physical_version_storage_tables(
            catalogue_schemas,
            schema_version_aliases,
            physical_mappings,
        )?);
        let layout = StorageLayout::jazz_class_v1();
        Database::new_with_storage_layout(lowered, storage, layout)
            .await
            .map_err(Error::from)
    }

    pub(crate) fn committed_global_time(&self) -> GlobalTime {
        self.clock.committed_global_time
    }

    /// Stable identity of the authority issuing wire-level receipts.
    pub(crate) fn node_uuid(&self) -> NodeUuid {
        self.node_uuid
    }

    pub(crate) fn authored_commit_durability(&self) -> DurabilityTier {
        self.authored_commit_durability
    }

    pub(crate) fn set_non_durable_client(&mut self) {
        self.authored_commit_durability = DurabilityTier::None;
    }

    /// Attach process-local auth claims to an accepted subscriber identity.
    pub(crate) fn set_session_claims(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) {
        if self.session_claims.get(&identity) == Some(&claims) {
            return;
        }
        self.session_claims.insert(identity, claims);
        let revision = self.session_claim_revisions.entry(identity).or_default();
        *revision = revision
            .checked_add(1)
            .expect("session claim revision overflow must stop authorization delivery");
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
    }

    /// Admit application/provider claims for a synthetic test topology.
    ///
    /// Production admission already stores provider claims in the internal,
    /// collision-proof namespace. Test fixtures use this named boundary so
    /// their readable raw claim names cannot accidentally exercise the
    /// forbidden legacy flat-claim lookup path.
    #[cfg(test)]
    pub(crate) fn set_test_provider_claims(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> BTreeMap<String, Value> {
        let admitted: BTreeMap<String, Value> = claims
            .into_iter()
            .map(|(name, value)| {
                let storage_name = if name == "user"
                    || name == "authMode"
                    || name.starts_with(crate::query::PROVIDER_CLAIM_PREFIX)
                {
                    name
                } else {
                    crate::query::provider_claim_key(&name)
                };
                (storage_name, value)
            })
            .collect();
        self.set_session_claims(identity, admitted.clone());
        admitted
    }

    /// Install claims through the same local-admission path used by a trusted
    /// subscriber connection. This exists only for synthetic topology tests
    /// that exercise `NodeState`/`PeerState` directly and therefore have no
    /// serving transport on which to perform normal session admission.
    #[cfg(feature = "testing")]
    pub fn admit_test_session_claims(
        &mut self,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) {
        self.set_session_claims(identity, claims);
    }

    /// Return the revision of process-local claims for `identity`.
    pub(crate) fn session_claim_revision(&self, identity: AuthorSubject) -> u64 {
        self.session_claim_revisions
            .get(&identity)
            .copied()
            .unwrap_or_default()
    }

    /// Return the current process-local claims together with their revisions.
    ///
    /// Upstream links use this snapshot to decide which claims have not yet
    /// reached that particular connection.
    pub(crate) fn session_claims_with_revisions(
        &self,
    ) -> Vec<(AuthorSubject, BTreeMap<String, Value>, u64)> {
        self.session_claims
            .iter()
            .map(|(identity, claims)| {
                (
                    *identity,
                    claims.clone(),
                    self.session_claim_revision(*identity),
                )
            })
            .collect()
    }

    /// Gate session-scoped serving until an authority has installed its
    /// permissions head. Local/offline nodes stay ready by default.
    pub(crate) fn set_permissions_ready(&mut self, ready: bool) {
        self.permissions_ready = ready;
    }

    pub(crate) fn permissions_ready(&self) -> bool {
        self.permissions_ready
    }

    /// Replace the policy-blind immutable content backend used by Groove.
    /// The backend instance and verified cache survive catalogue rebuilds.
    pub fn set_chunk_storage(&mut self, storage: Rc<dyn groove::chunks::ChunkStorage>) {
        self.database.set_chunk_storage(storage);
        self.database
            .set_missing_chunk_resolver(self.chunk_resolver.clone());
        let runtime_provider = self.database.owned_chunk_provider();
        self.local_chunk_reader
            .refresh_from(&self.database.local_chunk_reader());
        self.content_runtime_provider = runtime_provider;
    }

    /// Install Jazz's sync-plane fallback for chunks absent from Groove's
    /// local storage. The resolver carries no authorization state; ordinary
    /// row/view delivery is what discloses locators to callers.
    pub fn set_missing_chunk_resolver(
        &mut self,
        resolver: Rc<dyn groove::chunks::MissingChunkResolver>,
    ) {
        self.database.set_missing_chunk_resolver(resolver.clone());
        let runtime_provider = self.database.owned_chunk_provider();
        self.chunk_resolver = resolver;
        self.content_runtime_provider = runtime_provider;
    }

    /// Consult Groove's local immutable storage without invoking the peer
    /// fallback. Peer forwarding uses this to avoid recursive request loops.
    pub async fn local_chunk(
        &self,
        locator: groove::large_values::Locator,
        expected_hash: groove::large_values::ContentHash,
    ) -> Result<bytes::Bytes, groove::chunks::ChunkStorageError> {
        self.local_chunk_reader.get(locator, expected_hash).await
    }

    pub(crate) fn local_chunk_reader_handle(&self) -> groove::chunks::LocalChunkReader {
        self.local_chunk_reader.clone()
    }

    /// Replace Jazz's policy for unpublished Groove staging receipts.
    pub fn set_large_value_staging_policy(&mut self, policy: LargeValueStagingPolicy) {
        self.large_value_staging_policy = policy;
    }

    pub(super) async fn enforce_large_value_staging_policy(
        &self,
        newest: &groove::large_values::StagedLargeValue,
    ) -> Result<(), Error> {
        if !self.admit_large_value_ingress(newest.accounting.encoded_bytes) {
            self.database.evict_staged_large_value(newest.id).await?;
            return Err(Error::LargeValueIngressRateLimited);
        }
        Ok(())
    }

    pub(super) fn admit_large_value_ingress(&self, encoded_bytes: u64) -> bool {
        let now_ms: u64 = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let mut ingress = self.large_value_ingress.borrow_mut();
        if ingress.window_started_ms == 0
            || now_ms.saturating_sub(ingress.window_started_ms)
                >= self.large_value_staging_policy.window_ms.max(1)
        {
            ingress.window_started_ms = now_ms;
            ingress.admitted_bytes = 0;
        }
        let next = ingress.admitted_bytes.saturating_add(encoded_bytes);
        if next > self.large_value_staging_policy.incoming_bytes_per_window {
            return false;
        }
        ingress.admitted_bytes = next;
        true
    }

    pub(super) async fn ensure_large_value_stages_current(
        &self,
        ids: &BTreeSet<groove::large_values::StagedLargeValueId>,
    ) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }
        let staged = self
            .database
            .staged_large_values()
            .await?
            .into_iter()
            .map(|staged| (staged.id, staged))
            .collect::<BTreeMap<_, _>>();
        for id in ids {
            if !staged.contains_key(id) {
                return Err(Error::LargeValueStageExpired);
            }
        }
        Ok(())
    }

    pub(super) async fn current_staged_ids_for_descriptors(
        &self,
        descriptors: &[groove::large_values::LargeValueRef],
        require_every_descriptor: bool,
    ) -> Result<Vec<groove::large_values::StagedLargeValueId>, Error> {
        if descriptors.is_empty() {
            return Ok(Vec::new());
        }
        let staged = self.database.staged_large_values().await?;
        let mut ids = Vec::new();
        for descriptor in descriptors {
            let receipt = staged
                .iter()
                .find(|receipt| &receipt.value_ref == descriptor);
            if let Some(receipt) = receipt {
                ids.push(receipt.id);
            } else if require_every_descriptor {
                return Err(Error::LargeValueStageExpired);
            }
        }
        Ok(ids)
    }

    pub(crate) async fn require_staged_large_values_for_versions(
        &self,
        versions: &[VersionRecord],
    ) -> Result<(), Error> {
        let descriptors = version_indirect_descriptors(versions);
        self.current_staged_ids_for_descriptors(&descriptors, true)
            .await
            .map(|_| ())
    }

    /// Evict unpublished Groove staging roots older than the configured Jazz
    /// policy. TTL is maintenance and resource-management policy only; normal
    /// operations continue any journal or receipt that remains present.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        let max_age_ms = self.large_value_staging_policy.max_age_ms;
        let now_ms: u64 = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let mut evicted = 0;
        for staged in self.database.staged_large_values().await? {
            if now_ms.saturating_sub(staged.created_at_ms) > max_age_ms
                && self.database.evict_staged_large_value(staged.id).await?
            {
                evicted += 1;
            }
        }
        for upload in self.database.pending_large_value_uploads().await? {
            if now_ms.saturating_sub(upload.created_at_ms) > max_age_ms
                && self
                    .database
                    .evict_pending_large_value_upload(upload.id)
                    .await?
            {
                evicted += 1;
            }
        }
        Ok(evicted)
    }

    /// Evict an opaque Groove staging root selected by Jazz policy. All
    /// persisted mechanics remain in Groove and repeated eviction is harmless.
    pub async fn evict_staged_large_value(
        &self,
        id: groove::large_values::StagedLargeValueId,
    ) -> Result<bool, Error> {
        Ok(self.database.evict_staged_large_value(id).await?)
    }

    pub(crate) async fn stage_large_value_chunk_batch(
        &self,
        upload_id: groove::large_values::StagedLargeValueId,
        kind: groove::large_values::LargeValueKind,
        chunks: Vec<groove::large_values::StagedChunk>,
    ) -> Result<(), Error> {
        let encoded_bytes = chunks.iter().try_fold(0_u64, |total, chunk| {
            total.checked_add(u64::try_from(chunk.encoded.len()).map_err(|_| {
                Error::InvalidStoredValue("large-value chunk size exceeds u64")
            })?)
            .ok_or(Error::InvalidStoredValue(
                "large-value chunk batch accounting overflow",
            ))
        })?;
        if !self.admit_large_value_ingress(encoded_bytes) {
            self.database.evict_staged_large_value(upload_id).await?;
            return Err(Error::LargeValueIngressRateLimited);
        }
        let staged = self
            .database
            .stage_large_value_chunk_batch_if_current(
                upload_id,
                kind,
                chunks,
            )
            .await?;
        if !staged {
            return Err(Error::LargeValueStageExpired);
        }
        Ok(())
    }

    /// Establish the one pending journal that later local stream operations
    /// must continue. Only this initialization path may create it.
    pub(crate) async fn begin_streaming_large_value_upload(
        &self,
        upload_id: groove::large_values::StagedLargeValueId,
        kind: groove::large_values::LargeValueKind,
    ) -> Result<(), Error> {
        self.database
            .stage_large_value_chunk_batch(upload_id, kind, Vec::new())
            .await?;
        Ok(())
    }

    pub(crate) async fn evict_pending_large_value_upload(
        &self,
        upload_id: groove::large_values::StagedLargeValueId,
    ) -> Result<(), Error> {
        self.database
            .evict_pending_large_value_upload(upload_id)
            .await?;
        Ok(())
    }

    pub(crate) async fn finalize_large_value_upload(
        &self,
        upload_id: groove::large_values::StagedLargeValueId,
        value_ref: groove::large_values::LargeValueRef,
    ) -> Result<groove::large_values::StagedLargeValue, Error> {
        // Presence is the semantic boundary: maintenance may evict an old
        // journal, but wall-clock age alone cannot reject an active upload.
        let Some(staged) = self
            .database
            .finalize_large_value_upload_if_current(
                upload_id,
                value_ref,
            )
            .await?
        else {
            return Err(Error::LargeValueStageExpired);
        };
        Ok(staged)
    }

    /// Test helper exercising the same internally allocated admission path as
    /// production writes.
    #[cfg(test)]
    pub(crate) async fn attach_large_cell_for_test(
        &self,
        mut commit: MergeableCommit,
        column: impl Into<String>,
        kind: groove::large_values::LargeValueKind,
        bytes: &[u8],
    ) -> Result<(MergeableCommit, groove::large_values::LargeValueRef), Error> {
        let staged = self
            .database
            .prepare_and_stage_large_value(kind, bytes)
            .await?;
        self.enforce_large_value_staging_policy(&staged).await?;
        let column = column.into();
        commit
            .cells
            .insert(column.clone(), Value::Large(staged.value_ref.clone()));
        commit.prepared_large_columns.insert(column);
        commit.staged_large_values.push(staged.id);
        Ok((commit, staged.value_ref))
    }

    /// Consolidate through Groove-owned storage. Jazz receives only the
    /// publishable descriptor used by its high-level row-write API.
    pub async fn consolidate_and_stage_large_value(
        &self,
        value: groove::large_values::LargeValueRef,
    ) -> Result<groove::large_values::StagedLargeValue, Error> {
        let staged = self
            .database
            .consolidate_and_stage_large_value(value)
            .await?;
        self.enforce_large_value_staging_policy(&staged).await?;
        Ok(staged)
    }

    /// Prepare and stage a logical append through Groove's bounded tail and
    /// localized consolidation path.
    pub async fn append_and_stage_large_value(
        &self,
        value: groove::large_values::LargeValueRef,
        bytes: Vec<u8>,
    ) -> Result<groove::large_values::StagedLargeValue, Error> {
        let staged = self
            .database
            .append_and_stage_large_value(value, bytes)
            .await?;
        self.enforce_large_value_staging_policy(&staged).await?;
        Ok(staged)
    }

    /// Prepare and stage a logical byte-coordinate splice through Groove.
    pub async fn edit_and_stage_large_value(
        &self,
        value: groove::large_values::LargeValueRef,
        offset: u64,
        delete_length: u64,
        insert_bytes: Vec<u8>,
    ) -> Result<groove::large_values::StagedLargeValue, Error> {
        let staged = self
            .database
            .edit_and_stage_large_value(value, offset, delete_length, insert_bytes)
            .await?;
        self.enforce_large_value_staging_policy(&staged).await?;
        Ok(staged)
    }

    pub(crate) async fn read_large_value_range(
        &self,
        value: &groove::large_values::LargeValueRef,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, Error> {
        Ok(self.database.read_large_value_range(value, range).await?)
    }

    pub(crate) async fn read_large_text_utf16_range(
        &self,
        value: &groove::large_values::LargeValueRef,
        range: std::ops::Range<u64>,
    ) -> Result<String, Error> {
        Ok(self
            .database
            .read_large_text_utf16_range(value, range)
            .await?)
    }

    pub(crate) async fn read_large_json_pointer(
        &self,
        value: &groove::large_values::LargeValueRef,
        pointer: &str,
    ) -> Result<Option<serde_json::Value>, Error> {
        Ok(self
            .database
            .read_large_json_pointer(value, pointer)
            .await?)
    }

    /// Materialize physical indirect scalar arms before returning cells across
    /// a public transaction read boundary.
    pub(crate) async fn hydrate_large_value_cells(
        &self,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        // Transaction cells are application scalars today; their schema is
        // not carried through this internal helper. Binding records, which may
        // contain collected nested records, use the descriptor-directed walker
        // below instead.
        for value in cells.values_mut() {
            let target = match value {
                Value::Nullable(Some(inner)) => inner.as_mut(),
                value => value,
            };
            let Value::Large(value_ref) = target else {
                continue;
            };
            *target = self.materialize_large_value(value_ref).await?;
        }
        Ok(())
    }

    pub(crate) async fn hydrate_current_rows(&self, rows: &mut [CurrentRow]) -> Result<(), Error> {
        for row in rows {
            let descriptor = row.record.descriptor().clone();
            let mut values = row.record.to_values()?;
            self.hydrate_record_values(&mut values, &descriptor).await?;
            row.record =
                std::sync::Arc::new(OwnedRecord::new(descriptor.create(&values)?, descriptor));
        }
        Ok(())
    }

    /// Materialize physical indirect scalars in one encoded record before a
    /// language binding exposes that record outside Jazz.
    pub(crate) async fn hydrate_encoded_record(
        &self,
        descriptor: &groove::records::RecordDescriptor,
        raw: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let mut values = descriptor.bind(raw).to_values()?;
        self.hydrate_record_values(&mut values, descriptor).await?;
        *raw = descriptor.create(&values)?;
        Ok(())
    }

    /// Hydrate every physical indirect scalar in a descriptor-owned record.
    ///
    /// Values are decoded into a temporary tree and re-encoded only after the
    /// full walk succeeds. In particular, a retryable missing chunk leaves the
    /// retained subscription event byte-identical for the next attempt.
    async fn hydrate_record_values(
        &self,
        values: &mut [Value],
        descriptor: &records::RecordDescriptor,
    ) -> Result<(), Error> {
        if values.len() != descriptor.fields().len() {
            return Err(Error::InvalidStoredValue(
                "binding record has a descriptor/value arity mismatch",
            ));
        }
        for (value, field) in values.iter_mut().zip(descriptor.fields()) {
            self.hydrate_value_for_binding(value, &field.value_type).await?;
        }
        Ok(())
    }

    fn hydrate_value_for_binding<'a>(
        &'a self,
        value: &'a mut Value,
        value_type: &'a records::ValueType,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + 'a>> {
        use records::ValueType;

        Box::pin(async move {
            match value_type {
                ValueType::String => match value {
                    Value::String(_) => Ok(()),
                    Value::Large(value_ref)
                        if matches!(
                            value_ref.kind,
                            groove::large_values::LargeValueKind::String
                                | groove::large_values::LargeValueKind::Json
                        ) => {
                        *value = self.materialize_large_value(value_ref).await?;
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding string value does not match its descriptor",
                    )),
                },
                ValueType::Bytes => match value {
                    Value::Bytes(_) => Ok(()),
                    Value::Large(value_ref)
                        if value_ref.kind == groove::large_values::LargeValueKind::Bytes =>
                    {
                        *value = self.materialize_large_value(value_ref).await?;
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding bytes value does not match its descriptor",
                    )),
                },
                ValueType::Nullable(inner) => match value {
                    Value::Nullable(None) => Ok(()),
                    Value::Nullable(Some(value)) => {
                        self.hydrate_value_for_binding(value, inner).await
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding nullable value does not match its descriptor",
                    )),
                },
                ValueType::Array(element_type) => match value {
                    Value::Array(values) => {
                        for value in values {
                            self.hydrate_value_for_binding(value, element_type).await?;
                        }
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding array value does not match its descriptor",
                    )),
                },
                ValueType::Tuple(member_types) => match value {
                    Value::Tuple(values) if values.len() == member_types.len() => {
                        for (value, member_type) in values.iter_mut().zip(member_types) {
                            self.hydrate_value_for_binding(value, member_type).await?;
                        }
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding tuple value does not match its descriptor",
                    )),
                },
                ValueType::Record(descriptor) => match value {
                    Value::Record(record) if record.descriptor() == descriptor.as_ref() => {
                        let mut values = record.to_values()?;
                        self.hydrate_record_values(&mut values, descriptor).await?;
                        *record = OwnedRecord::new(descriptor.create(&values)?, **descriptor);
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding nested record does not match its descriptor",
                    )),
                },
                ValueType::Enum(schema) => match value {
                    Value::Enum(enum_value) => {
                        let tag = enum_value.tag();
                        let case = schema.case(tag)?;
                        if enum_value.record().descriptor() != &case.payload {
                            return Err(Error::InvalidStoredValue(
                                "binding enum payload does not match its descriptor",
                            ));
                        }
                        let mut values = enum_value.record().to_values()?;
                        self.hydrate_record_values(&mut values, &case.payload).await?;
                        *enum_value = records::EnumValue::new(
                            tag,
                            OwnedRecord::new(
                                case.payload.create(&values)?,
                                case.payload,
                            ),
                        );
                        Ok(())
                    }
                    _ => Err(Error::InvalidStoredValue(
                        "binding enum value does not match its descriptor",
                    )),
                },
                ValueType::U8 if matches!(value, Value::U8(_)) => Ok(()),
                ValueType::U16 if matches!(value, Value::U16(_)) => Ok(()),
                ValueType::U32 if matches!(value, Value::U32(_)) => Ok(()),
                ValueType::U64 if matches!(value, Value::U64(_)) => Ok(()),
                ValueType::I32 if matches!(value, Value::I32(_)) => Ok(()),
                ValueType::I64 if matches!(value, Value::I64(_)) => Ok(()),
                ValueType::Bool if matches!(value, Value::Bool(_)) => Ok(()),
                ValueType::Uuid if matches!(value, Value::Uuid(_)) => Ok(()),
                ValueType::F64 => match value {
                    Value::F64(value) if !value.is_nan() => Ok(()),
                    _ => Err(Error::InvalidStoredValue(
                        "binding float value does not match its descriptor",
                    )),
                },
                ValueType::EnumTag(schema) => match value {
                    Value::EnumTag(value) => schema.variant(*value).map(|_| ()).map_err(Into::into),
                    Value::String(value) => schema.discriminant(value).map(|_| ()).map_err(Into::into),
                    _ => Err(Error::InvalidStoredValue(
                        "binding enum tag value does not match its descriptor",
                    )),
                },
                _ => Err(Error::InvalidStoredValue(
                    "binding value does not match its descriptor",
                )),
            }
        })
    }

    async fn materialize_large_value(
        &self,
        value_ref: &groove::large_values::LargeValueRef,
    ) -> Result<Value, Error> {
        let bytes = self
            .database
            .read_large_value_range(value_ref, 0..value_ref.byte_length)
            .await?;
        match value_ref.kind {
            groove::large_values::LargeValueKind::Bytes => Ok(Value::Bytes(bytes)),
            groove::large_values::LargeValueKind::String
            | groove::large_values::LargeValueKind::Json => Ok(Value::String(
                String::from_utf8(bytes)
                    .map_err(|_| Error::InvalidStoredValue("large text is not valid UTF-8"))?,
            )),
        }
    }

    async fn rebuild_database_slot(&mut self) -> Result<(), Error> {
        // Reopening the database refreshes Groove's physical table catalogue.
        // Parking is in-memory delivery state, not derivable from storage, so a
        // live refresh must retain it for the caller to drain afterwards.
        let parking = self.parking.clone();
        let old_database = self.database.take();
        let storage = old_database.into_storage();
        let mut database = Self::open_full_database(
            &self.catalogue.schema,
            &self.catalogue.catalogue_schemas,
            &self.catalogue.schema_version_aliases,
            &self.catalogue.physical_mappings,
            storage,
        )
        .await?;
        database.set_missing_chunk_resolver(self.chunk_resolver.clone());
        self.content_runtime_provider = database.owned_chunk_provider();
        // Existing peer I/O pumps retain clones of this local-only lookup
        // service. Retarget all of them before dropping the rebuilt facade's
        // temporary reader, rather than leaving a live browser/socket link on
        // OrderedChunkStorage's deliberately weak old storage handle.
        self.local_chunk_reader
            .refresh_from(&database.local_chunk_reader());
        self.database.replace(database);
        self.register_physical_history_variant_projections().await?;
        self.register_physical_current_variant_projections().await?;
        self.groove_runtime_token = next_groove_runtime_token();
        self.invalidate_runtime_handles_after_database_rebuild();
        self.parking = parking;
        Ok(())
    }

    /// A catalogue change rebuilds Groove's in-memory graph registry, but it
    /// does not restart this node. Keep recovered durable facts intact while
    /// dropping handles and plans that were compiled against the old registry.
    fn invalidate_runtime_handles_after_database_rebuild(&mut self) {
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.tx_version_tables_cache.clear();
        self.query.tx_versions_cache.clear();
        self.query.tx_version_tables_cache_order.clear();
        self.query.tx_version_tables_cache_order_set.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.settled_result_sets.clear();
        self.query.settled_result_row_index.clear();
        self.query.settled_program_facts.clear();
        self.query.settled_through_by_binding_view.clear();
        self.query.authorization_progress_by_binding_view.clear();
        self.query.known_state_declared_binding_views.clear();
        self.query.initial_hydration_binding_views.clear();
        self.query.deferred_publication_binding_views.clear();
        self.query.pending_authoritative_reset_binding_views.clear();
        self.query.pending_opening_binding_views.clear();
    }

    fn result_member_row_key(member: &ResultMemberEntry) -> Option<ResultRowMembershipKey> {
        member.output_occurrence_id()
    }

    fn insert_settled_result_member_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        member: ResultMemberEntry,
    ) {
        if let Some(row_key) = Self::result_member_row_key(&member) {
            self.query
                .settled_result_row_index
                .entry(binding_view_key)
                .or_default()
                .insert(row_key, member.clone());
        }
        self.query
            .settled_result_sets
            .entry(binding_view_key)
            .or_default()
            .insert(member);
    }

    fn remove_settled_result_member_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        member: &ResultMemberEntry,
    ) -> bool {
        let removed = self
            .query
            .settled_result_sets
            .get_mut(&binding_view_key)
            .is_some_and(|members| members.remove(member));
        if removed
            && let Some(row_key) = Self::result_member_row_key(member)
            && self
                .query
                .settled_result_row_index
                .get(&binding_view_key)
                .and_then(|index| index.get(&row_key))
                == Some(member)
            && let Some(index) = self
                .query
                .settled_result_row_index
                .get_mut(&binding_view_key)
        {
            index.remove(&row_key);
        }
        removed
    }

    fn remove_settled_result_member_for_occurrence_indexed(
        &mut self,
        binding_view_key: BindingViewKey,
        occurrence_id: ResultRowMembershipKey,
    ) -> Option<ResultMemberEntry> {
        let previous = self
            .query
            .settled_result_row_index
            .get_mut(&binding_view_key)
            .and_then(|index| index.remove(&occurrence_id))?;
        if let Some(members) = self.query.settled_result_sets.get_mut(&binding_view_key) {
            members.remove(&previous);
        }
        Some(previous)
    }

    fn clear_settled_result_view(&mut self, binding_view_key: BindingViewKey) {
        self.query.settled_result_sets.remove(&binding_view_key);
        self.query
            .settled_result_row_index
            .remove(&binding_view_key);
    }

    async fn open_catalogue_stage<T>(
        schema: JazzSchema,
        storage: T,
        catalogue_bootstrap_state: CatalogueBootstrapState,
    ) -> Result<CatalogueOpenState, Error>
    where
        T: ReopenableStorage + 'static,
    {
        let current_schema_version_id = schema.version_id();
        let meta_schema = schema.lower_catalogue_meta_to_groove();
        let mut meta_database =
            Database::new_with_storage_layout(meta_schema, storage, StorageLayout::jazz_class_v1())
                .await?;
        let mut catalogue_schemas = BTreeMap::new();
        let mut catalogue_lenses = BTreeMap::new();
        let mut staged_lineages_by_id = BTreeMap::new();
        let mut pending_lineages = BTreeMap::new();
        let mut active_lineages_by_id = BTreeMap::new();
        let mut pending_write_pointers = BTreeMap::new();
        let mut genesis_schema = None;
        let mut catalogue_bootstrap_ready = None;
        for raw in meta_database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .await?
        {
            let record = raw.record();
            match record.get_bytes(CatalogueRowRecord::FIELD_KIND_IDX)? {
                b"schema" => {
                    let schema_version: SchemaVersion = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if schema_version.id
                        != SchemaVersionId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?)
                    {
                        return Err(Error::InvalidStoredValue("catalogue schema id mismatch"));
                    }
                    catalogue_schemas.insert(schema_version.id, schema_version);
                }
                b"lens" => {
                    let lens: MigrationLens = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if lens.id
                        != MigrationLensId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?)
                    {
                        return Err(Error::InvalidStoredValue("catalogue lens id mismatch"));
                    }
                    catalogue_lenses.insert(lens.id, lens);
                }
                b"schema_lineage_staged" => {
                    let staged: StagedSchemaLineage = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if staged.publication.id.0
                        != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || staged.publication.id != staged.publication.content_id()
                    {
                        return Err(Error::InvalidStoredValue(
                            "staged schema lineage id mismatch",
                        ));
                    }
                    staged_lineages_by_id.insert(staged.publication.id, staged);
                }
                b"schema_lineage_pending" => {
                    let pending: PendingSchemaLineage = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if pending.publication.id.0
                        != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || pending.publication.id != pending.publication.content_id()
                        || pending_lineages
                            .insert(pending.catalogue_seq, pending)
                            .is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "pending schema lineage identity conflict",
                        ));
                    }
                }
                b"schema_lineage_active" => {
                    let active: SchemaLineageActivation = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if active.id.0 != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || active.catalogue_seq == 0
                        || active_lineages_by_id.insert(active.id, active).is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "active schema lineage id mismatch",
                        ));
                    }
                }
                b"write_pointer_pending" => {
                    let pointer: CurrentWriteSchema = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    pending_write_pointers.insert(pointer.revision, pointer);
                }
                b"genesis" => {
                    let schema =
                        SchemaVersionId(record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?);
                    if genesis_schema.replace(schema).is_some() {
                        return Err(Error::InvalidStoredValue(
                            "duplicate catalogue genesis marker",
                        ));
                    }
                }
                b"bootstrap_ready" => {
                    let ready: CatalogueBootstrapReady = serde_json::from_slice(
                        record.get_bytes(CatalogueRowRecord::FIELD_PAYLOAD_IDX)?,
                    )?;
                    if ready.genesis.0 != record.get_uuid(CatalogueRowRecord::FIELD_ID_IDX)?
                        || catalogue_bootstrap_ready.replace(ready).is_some()
                    {
                        return Err(Error::InvalidStoredValue(
                            "duplicate or malformed catalogue bootstrap marker",
                        ));
                    }
                }
                _ => return Err(Error::InvalidStoredValue("unknown catalogue kind")),
            }
        }
        let mut staged_lineages = BTreeMap::new();
        let mut active_lineages_by_target = BTreeMap::new();
        let mut lineage_targets = BTreeSet::new();
        let mut active_lineages_by_seq = BTreeMap::new();
        for active in active_lineages_by_id.values() {
            if active_lineages_by_seq
                .insert(active.catalogue_seq, active.id)
                .is_some()
            {
                return Err(Error::InvalidStoredValue(
                    "duplicate active catalogue sequence",
                ));
            }
        }
        for staged in staged_lineages_by_id.into_values() {
            Self::validate_durable_staged_lineage(&staged, &catalogue_schemas)?;
            if staged.catalogue_seq == 0 {
                return Err(Error::InvalidStoredValue(
                    "staged schema lineage sequence must be nonzero",
                ));
            }
            if !lineage_targets.insert(staged.publication.schema.id) {
                return Err(Error::InvalidStoredValue(
                    "duplicate durable schema lineage target",
                ));
            }
            if let Some(active) = active_lineages_by_id.remove(&staged.publication.id) {
                if active.catalogue_seq != staged.catalogue_seq {
                    return Err(Error::InvalidStoredValue(
                        "active schema lineage payload conflicts with marker",
                    ));
                }
                active_lineages_by_target.insert(staged.publication.schema.id, staged);
            } else if active_lineages_by_seq.contains_key(&staged.catalogue_seq) {
                return Err(Error::InvalidStoredValue(
                    "staged catalogue sequence conflicts with active lineage",
                ));
            } else if staged_lineages
                .insert(staged.catalogue_seq, staged)
                .is_some()
            {
                return Err(Error::InvalidStoredValue(
                    "duplicate staged catalogue sequence",
                ));
            }
        }
        if !active_lineages_by_id.is_empty() {
            return Err(Error::InvalidStoredValue(
                "active schema lineage is missing canonical payload",
            ));
        }
        for (expected, actual) in (1..).zip(active_lineages_by_seq.keys().copied()) {
            if actual != expected {
                return Err(Error::InvalidStoredValue(
                    "active catalogue sequences are not contiguous",
                ));
            }
        }
        let active_catalogue_seq = active_lineages_by_seq
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0);
        let mut schema_version_aliases = BTreeMap::new();
        let mut physical_mappings = BTreeMap::new();
        for raw in meta_database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .await?
        {
            let record = raw.record();
            let mapping: SchemaPhysicalMapping = serde_json::from_slice(
                record.get_bytes(SchemaVersionAliasRowRecord::FIELD_PHYSICAL_MAPPING_IDX)?,
            )?;
            let schema_version =
                SchemaVersionId(record.get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX)?);
            let alias =
                SchemaVersionAlias(record.get_u64(SchemaVersionAliasRowRecord::FIELD_ID_IDX)?);
            schema_version_aliases.insert(schema_version, alias);
            physical_mappings.insert(schema_version, mapping);
        }
        validate_physical_variant_cases(&physical_mappings, &schema_version_aliases)?;
        let mut next_physical_table_id = 1;
        let mut next_physical_column_id = 1;
        for mapping in physical_mappings.values() {
            for table in mapping.tables.values() {
                let table_successor = table
                    .table_id
                    .0
                    .checked_add(1)
                    .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?;
                next_physical_table_id = next_physical_table_id.max(table_successor);
                for column_id in table.columns.values() {
                    let column_successor = column_id
                        .0
                        .checked_add(1)
                        .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?;
                    next_physical_column_id = next_physical_column_id.max(column_successor);
                }
            }
        }
        for staged in staged_lineages.values() {
            for table in staged.mapping.tables.values() {
                next_physical_table_id = next_physical_table_id.max(
                    table
                        .table_id
                        .0
                        .checked_add(1)
                        .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?,
                );
                for column in table.columns.values() {
                    next_physical_column_id = next_physical_column_id.max(
                        column
                            .0
                            .checked_add(1)
                            .ok_or(Error::InvalidStoredValue("physical column id exhausted"))?,
                    );
                }
            }
        }
        match genesis_schema {
            Some(genesis) if genesis != current_schema_version_id => {
                return Err(Error::InvalidStoredValue(
                    "opened schema does not match durable catalogue genesis",
                ));
            }
            None if !catalogue_schemas.is_empty() => {
                return Err(Error::InvalidStoredValue(
                    "catalogue schemas exist without a genesis marker",
                ));
            }
            _ => {}
        }
        if catalogue_bootstrap_state == CatalogueBootstrapState::Uninitialized
            && (genesis_schema.is_some()
                || !catalogue_schemas.is_empty()
                || !catalogue_lenses.is_empty()
                || !physical_mappings.is_empty()
                || !schema_version_aliases.is_empty()
                || active_catalogue_seq != 0
                || !staged_lineages.is_empty()
                || !pending_lineages.is_empty()
                || !active_lineages_by_target.is_empty()
                || !pending_write_pointers.is_empty())
        {
            return Err(Error::InvalidStoredValue(
                "uninitialized catalogue open requires empty durable catalogue state",
            ));
        }
        let had_current_schema = catalogue_schemas.contains_key(&current_schema_version_id);
        if !had_current_schema {
            catalogue_schemas.insert(
                current_schema_version_id,
                SchemaVersion::new(schema.clone()),
            );
        }
        if !physical_mappings.contains_key(&current_schema_version_id)
            || !schema_version_aliases.contains_key(&current_schema_version_id)
        {
            let mapping = match physical_mappings.get(&current_schema_version_id) {
                Some(mapping) => mapping.clone(),
                None => allocate_provisional_physical_mapping(
                    &schema,
                    &mut next_physical_table_id,
                    &mut next_physical_column_id,
                )?,
            };
            let alias = schema_version_aliases
                .get(&current_schema_version_id)
                .copied()
                .unwrap_or(SchemaVersionAlias(
                    schema_version_aliases
                        .values()
                        .map(|alias| alias.0)
                        .max()
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or(Error::InvalidStoredValue("schema version alias exhausted"))?,
                ));
            if catalogue_bootstrap_state == CatalogueBootstrapState::Ready {
                let mut batch = meta_database.open_batch();
                if genesis_schema.is_none() {
                    batch.update(
                        "jazz_catalogue",
                        vec![
                            Value::Bytes(b"genesis".to_vec()),
                            Value::Uuid(current_schema_version_id.0),
                            Value::Bytes(Vec::new()),
                        ],
                    );
                }
                if !had_current_schema {
                    batch.update(
                        "jazz_catalogue",
                        vec![
                            Value::Bytes(b"schema".to_vec()),
                            Value::Uuid(current_schema_version_id.0),
                            Value::Bytes(serde_json::to_vec(&SchemaVersion::new(schema.clone()))?),
                        ],
                    );
                }
                Self::write_schema_version_mapping_to_batch(
                    &mut batch,
                    alias,
                    current_schema_version_id,
                    &mapping,
                )?;
                let applied = meta_database.apply_batch(batch).await?;
                let persisted = applied.persist().await;
                meta_database.finish_persistence(persisted)?;
            }
            schema_version_aliases.insert(current_schema_version_id, alias);
            physical_mappings.insert(current_schema_version_id, mapping);
        }
        let mut current_write_schema = CurrentWriteSchema {
            revision: 0,
            schema: current_schema_version_id,
        };
        if let Some(raw) = meta_database
            .primary_key_last_raw("jazz_catalogue_pointer", &[])
            .await?
        {
            let record = raw.record();
            current_write_schema = CurrentWriteSchema {
                revision: record.get_u64(CataloguePointerRowRecord::FIELD_REVISION_IDX)?,
                schema: SchemaVersionId(
                    record.get_uuid(CataloguePointerRowRecord::FIELD_SCHEMA_IDX)?,
                ),
            };
        }
        Ok(CatalogueOpenState {
            storage: meta_database.into_storage(),
            schemas: catalogue_schemas,
            lenses: catalogue_lenses,
            schema_version_aliases,
            physical_mappings,
            staged_lineages,
            pending_lineages,
            active_lineages_by_target,
            active_catalogue_seq,
            pending_write_pointers,
            next_physical_table_id,
            next_physical_column_id,
            current_write_schema,
            catalogue_bootstrap_marker: catalogue_bootstrap_ready.is_some(),
        })
    }

    fn validate_durable_staged_lineage(
        staged: &StagedSchemaLineage,
        catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    ) -> Result<(), Error> {
        Self::validate_schema_lineage_publication(&staged.publication).map_err(|_| {
            Error::InvalidStoredValue(
                "staged schema lineage violates trusted publication invariants",
            )
        })?;
        let source = catalogue_schemas
            .get(&staged.publication.lens.source)
            .ok_or(Error::InvalidStoredValue(
                "staged schema lineage source is missing",
            ))?;
        Self::validate_migration_lens_between(
            &staged.publication.lens,
            source,
            &staged.publication.schema,
        )
        .map_err(|_| Error::InvalidStoredValue("staged schema lineage lens is invalid"))?;
        Self::validate_lineage_table_partition(
            &source.schema,
            &staged.publication.schema.schema,
            &staged.publication.lens,
            &staged.publication.new_tables,
            &staged.publication.dropped_tables,
        )
        .map_err(|_| {
            Error::InvalidStoredValue("staged schema lineage table partition is invalid")
        })?;
        Ok(())
    }
}
