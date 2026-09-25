/// The authorization source for a row-version repair response. Scope relays
/// may disclose only exact row versions in their durable authority ledger;
/// they never re-evaluate a foreground's read policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowVersionRepairAuthorization {
    EnforceReadPolicy(AuthorSubject),
    RetainedScopeLedger,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(crate) fn table(&self, table: &str) -> Result<&TableSchema, Error> {
        self.catalogue
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn table_in_schema(
        &self,
        table: &str,
        schema_version: SchemaVersionId,
    ) -> Result<TableSchema, Error> {
        self.table_in_schema_ref(table, schema_version).cloned()
    }

    pub(super) fn table_in_schema_ref(
        &self,
        table: &str,
        schema_version: SchemaVersionId,
    ) -> Result<&TableSchema, Error> {
        if schema_version == self.catalogue.active_schema.schema {
            return self.catalogue.active_schema.compiled.tables.iter()
                .find(|candidate| candidate.name == table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()));
        }
        self.catalogue
            .catalogue_schemas
            .get(&schema_version)
            .and_then(|schema| {
                schema
                    .schema
                    .tables
                    .iter()
                    .find(|candidate| candidate.name == table)
            })
            .or_else(|| {
                (schema_version == self.catalogue.local_schema_version_id)
                    .then(|| self.table(table).ok())
                    .flatten()
            })
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn shortest_lens_path_cached(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
    ) -> Option<Vec<(MigrationLensId, LensPathDirection)>> {
        let key = LensPathCacheKey { source, target };
        if let Some(path) = self.catalogue.lens_path_cache.get(&key) {
            return path.clone();
        }
        let path = self.shortest_lens_path(source, target);
        self.catalogue.lens_path_cache.insert(key, path.clone());
        path
    }

    fn shortest_lens_path(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
    ) -> Option<Vec<(MigrationLensId, LensPathDirection)>> {
        if source == target {
            return Some(Vec::new());
        }

        let mut seen = BTreeSet::from([source]);
        let mut queue = VecDeque::from([(source, Vec::new())]);
        while let Some((schema, path)) = queue.pop_front() {
            for (lens, direction) in self.ordered_lens_edges(schema) {
                let next = match direction {
                    LensPathDirection::Forward => lens.target,
                    LensPathDirection::Reverse => lens.source,
                };
                if seen.contains(&next) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push((lens.id, direction));
                if next == target {
                    return Some(next_path);
                }
                seen.insert(next);
                queue.push_back((next, next_path));
            }
        }
        None
    }

    pub(super) fn compiled_lens_path(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
    ) -> Result<Option<CompiledLensPath>, Error> {
        let key = CompiledLensCacheKey {
            source,
            target,
            table: table.to_owned(),
        };
        if let Some(path) = self.catalogue.compiled_lens_cache.get(&key) {
            return Ok(path.clone());
        }

        let Some(steps) = self.shortest_lens_path_cached(source, target) else {
            self.catalogue.compiled_lens_cache.insert(key, None);
            return Ok(None);
        };
        let mut current_table = table.to_owned();
        let mut ops = Vec::new();
        for (lens_id, direction) in steps {
            let lens = self
                .catalogue
                .catalogue_lenses
                .get(&lens_id)
                .ok_or(Error::InvalidCatalogueUpdate("lens chain is unknown"))?;
            let table_lens = match direction {
                LensPathDirection::Forward => lens
                    .table_lenses
                    .iter()
                    .find(|candidate| candidate.source_table == current_table),
                LensPathDirection::Reverse => lens
                    .table_lenses
                    .iter()
                    .find(|candidate| candidate.target_table == current_table),
            };
            let Some(table_lens) = table_lens else {
                self.catalogue.compiled_lens_cache.insert(key, None);
                return Ok(None);
            };
            match direction {
                LensPathDirection::Forward => {
                    for op in &table_lens.ops {
                        push_compiled_forward_lens_op(op, &mut ops)?;
                    }
                    current_table = table_lens.target_table.clone();
                }
                LensPathDirection::Reverse => {
                    for op in table_lens.ops.iter().rev() {
                        push_compiled_reverse_lens_op(op, &mut ops)?;
                    }
                    current_table = table_lens.source_table.clone();
                }
            }
        }
        let path = Some(CompiledLensPath {
            target_table: current_table,
            ops,
        });
        self.catalogue.compiled_lens_cache.insert(key, path.clone());
        Ok(path)
    }

    fn ordered_lens_edges(
        &self,
        schema: SchemaVersionId,
    ) -> Vec<(&MigrationLens, LensPathDirection)> {
        let mut edges = self
            .catalogue
            .catalogue_lenses
            .values()
            .filter_map(|lens| {
                if lens.source == schema {
                    Some((lens, LensPathDirection::Forward))
                } else if lens.target == schema {
                    Some((lens, LensPathDirection::Reverse))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        edges.sort_by_key(|(lens, direction)| {
            let next = match direction {
                LensPathDirection::Forward => lens.target,
                LensPathDirection::Reverse => lens.source,
            };
            (next, lens.id)
        });
        edges
    }

    fn node_for_alias(&self, alias: NodeAlias) -> Option<NodeUuid> {
        self.node_aliases.node_for_alias(alias)
    }

    pub(super) async fn resolve_node_alias(
        &mut self,
        alias: NodeAlias,
    ) -> Result<Option<NodeUuid>, Error> {
        if let Some(node) = self.node_for_alias(alias) {
            return Ok(Some(node));
        }
        for raw in self
            .database
            .primary_key_scan_raw("jazz_nodes", &[])
            .await?
        {
            let record = raw.record();
            if NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?) != alias {
                continue;
            }
            let node = NodeUuid(record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)?);
            self.node_aliases.insert(node, alias);
            if node == self.node_uuid {
                self.self_node_alias = Some(alias);
            }
            return Ok(Some(node));
        }
        Ok(None)
    }

    pub(super) fn version_tx_id(&self, version: &VersionRow) -> Result<TxId, Error> {
        let node =
            self.node_for_alias(version.tx_node_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history tx node alias must exist",
                ))?;
        Ok(TxId::new(version.tx_time(), node))
    }

    async fn version_made_at(&mut self, version: &VersionRow) -> Result<TxTime, Error> {
        let tx_id = self.version_tx_id(version)?;
        self.transaction_made_at(tx_id)
            .await?
            .ok_or(Error::MissingTransaction(tx_id))
    }

    fn version_record_from_row(&self, version: &VersionRow) -> Result<VersionRecord, Error> {
        let schema_version = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let table = self.table_in_schema_ref(version.table(), schema_version)?;
        let authored_columns = self.authored_columns_for_version(version)?;
        VersionRecord::from_stored(version, table, schema_version, authored_columns)
    }

    pub(crate) async fn row_version_payloads_for_refs(
        &mut self,
        requests: &[RowVersionRef],
        authorization: RowVersionRepairAuthorization,
    ) -> Result<Vec<VersionBundle>, Error> {
        let mut by_tx = BTreeMap::<TxId, Vec<VersionRow>>::new();
        let mut requests_by_tx = BTreeMap::<TxId, BTreeSet<&RowVersionRef>>::new();
        for request in requests {
            requests_by_tx
                .entry(request.tx_id())
                .or_default()
                .insert(request);
        }
        for (tx_id, requests) in requests_by_tx {
            // A transaction may contain hundreds of requested rows. Decode it
            // once, and index only requested rows, rather than decoding and
            // scanning the whole transaction once per requested coordinate.
            // Process one transaction at a time so unrelated siblings are not
            // retained across the entire repair batch.
            let requested_rows = requests
                .iter()
                .map(|request| request.row_uuid)
                .collect::<BTreeSet<_>>();
            let mut versions_by_row =
                BTreeMap::<RowUuid, Vec<(PhysicalTableId, VersionRow)>>::new();
            for version in self.query_versions_for_tx(tx_id).await? {
                if requested_rows.contains(&version.row_uuid())
                    && let Ok(table_id) = self.physical_table_id_for_version(&version)
                {
                    versions_by_row
                        .entry(version.row_uuid())
                        .or_default()
                        .push((table_id, version));
                }
            }
            for request in requests {
                // A repair request names the receiver's projected table.  The
                // stored body, however, remains canonically authored under the
                // table name from its source schema.  Match the two through the
                // catalogue's durable physical identity, rather than requiring
                // those logical names to be equal.
                //
                // A reused logical name is ambiguous by itself, but the complete
                // repair reference also names the row and transaction. Resolve
                // that body first and require it to identify exactly one of the
                // physical lineages which carried the requested logical name.
                let candidate_mappings = self
                    .catalogue
                    .physical_mappings
                    .iter()
                    .filter_map(|(schema_version, mapping)| {
                        mapping
                            .tables
                            .get(request.table.as_str())
                            .map(|mapping| (*schema_version, mapping.table_id))
                    })
                    .collect::<Vec<_>>();
                if candidate_mappings.is_empty() {
                    return Err(Error::TableNotFound(request.table.to_string()));
                }
                let matching_versions = versions_by_row
                    .get(&request.row_uuid)
                    .into_iter()
                    .flatten()
                    .filter(|(table_id, version)| {
                        version.tx_time() == request.tx_time
                            && self.node_for_alias(version.tx_node_alias())
                                == Some(request.tx_node_id)
                            && candidate_mappings
                                .iter()
                                .any(|(_, candidate)| candidate == table_id)
                    })
                    .map(|(table_id, version)| (*table_id, version.clone()))
                    .collect::<Vec<_>>();
                let matching_table_ids = matching_versions
                    .iter()
                    .map(|(table_id, _)| *table_id)
                    .collect::<BTreeSet<_>>();
                let [requested_table_id] =
                    matching_table_ids.iter().copied().collect::<Vec<_>>()[..]
                else {
                    return Err(Error::InvalidStoredValue(
                        "repair request row maps to zero or multiple physical tables",
                    ));
                };
                let request_schema = [
                    self.catalogue.active_schema.schema,
                    self.catalogue.local_schema_version_id,
                ]
                .into_iter()
                .find(|schema_version| {
                    candidate_mappings
                        .iter()
                        .any(|(candidate_schema, table_id)| {
                            candidate_schema == schema_version && *table_id == requested_table_id
                        })
                })
                .or_else(|| {
                    candidate_mappings
                        .iter()
                        .find_map(|(schema_version, table_id)| {
                            (*table_id == requested_table_id).then_some(*schema_version)
                        })
                })
                .ok_or(Error::InvalidStoredValue(
                    "repair request physical table must have a schema mapping",
                ))?;
                match authorization {
                    RowVersionRepairAuthorization::EnforceReadPolicy(identity) => {
                        if !self
                            .dry_run_read_current_allows_in_schema(
                                &request.table,
                                request.row_uuid,
                                request_schema,
                                identity,
                                true,
                            )
                            .await?
                        {
                            continue;
                        }
                    }
                    RowVersionRepairAuthorization::RetainedScopeLedger => {
                        if !self
                            .scope_relay_repair_ledger_contains(requested_table_id, request)
                            .await?
                        {
                            continue;
                        }
                    }
                }
                for (table_id, version) in matching_versions {
                    if table_id == requested_table_id {
                        // Content and the deletion register can share a transaction.
                        // The repair coordinate names the row/transaction, so return
                        // every matching layer rather than whichever sorts first.
                        by_tx.entry(tx_id).or_default().push(version);
                    }
                }
            }
        }
        let mut out = Vec::new();
        for (tx_id, versions) in by_tx {
            let stored = self
                .query_transaction(tx_id)
                .await?
                .ok_or(Error::MissingTransaction(tx_id))?;
            out.push(
                self.version_bundle_for_maintained_view_versions_with_tx(&stored, &versions)
                    .await?,
            );
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub(crate) async fn apply_row_version_payloads_for_requests(
        &mut self,
        requests: &[RowVersionRef],
        version_bundles: Vec<VersionBundle>,
    ) -> Result<Vec<VersionBundle>, Error> {
        for bundle in &version_bundles {
            crate::protocol::validate_version_records(&bundle.versions)
                .map_err(|_| Error::MalformedViewUpdate("malformed version receipt"))?;
        }
        let requested_physical = requests
            .iter()
            .map(|request| {
                let table_ids = self
                    .catalogue
                    .physical_mappings
                    .values()
                    .filter_map(|mapping| {
                        mapping
                            .tables
                            .get(request.table.as_str())
                            .map(|mapping| mapping.table_id)
                    })
                    .collect::<BTreeSet<_>>();
                if table_ids.is_empty() {
                    return Err(Error::TableNotFound(request.table.to_string()));
                }
                Ok((
                    request.row_uuid,
                    request.tx_time,
                    request.tx_node_id,
                    table_ids,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // One `RowVersionPayloads` message is one repair frame. Preflight and
        // filter every bundle before ingesting the first one so a malformed
        // later carrier cannot leave earlier transaction/history/clock state
        // behind after the frame is rejected.
        let mut prevalidated_bundles = Vec::new();
        for mut bundle in version_bundles {
            ingest::validate_received_view_bundle_global_time_durability(
                bundle.global_time,
                bundle.durability,
            )?;
            let versions = std::mem::take(&mut bundle.versions)
                .into_iter()
                .filter(|version| {
                    self.physical_table_id_for_schema(version.schema_version(), version.table())
                        .is_ok_and(|table_id| {
                            requested_physical.iter().any(
                                |(row_uuid, tx_time, tx_node_id, table_ids)| {
                                    *row_uuid == version.row_uuid()
                                        && *tx_time == bundle.tx.tx_id.time
                                        && *tx_node_id == bundle.tx.tx_id.node
                                        && table_ids.contains(&table_id)
                                },
                            )
                        })
                })
                .collect::<Vec<_>>();
            if versions.is_empty() {
                continue;
            }
            for (row_uuid, tx_time, tx_node_id, table_ids) in &requested_physical {
                let matched_table_ids = versions
                    .iter()
                    .filter(|version| {
                        *row_uuid == version.row_uuid()
                            && *tx_time == bundle.tx.tx_id.time
                            && *tx_node_id == bundle.tx.tx_id.node
                    })
                    .filter_map(|version| {
                        self.physical_table_id_for_schema(version.schema_version(), version.table())
                            .ok()
                    })
                    .filter(|table_id| table_ids.contains(table_id))
                    .collect::<BTreeSet<_>>();
                if matched_table_ids.len() > 1 {
                    return Err(Error::InvalidStoredValue(
                        "repair response maps to multiple physical tables",
                    ));
                }
            }
            self.validate_view_payload_versions(&versions)?;
            bundle.versions = versions;
            prevalidated_bundles.push(bundle);
        }
        let mut applied_bundles = Vec::with_capacity(prevalidated_bundles.len());
        for mut bundle in prevalidated_bundles {
            // View and repair carriers can replay a locally-authorized
            // transaction, but their transport never selects its durable
            // policy capability. Retain a local stored hint for restart; do
            // not import or republish a received one.
            bundle.tx = transaction_without_permission_subject(&bundle.tx);
            match bundle.scope {
                crate::protocol::VersionBundleScope::ViewScoped => {
                    // A row repair is still a partial transaction carrier.
                    // Its visible cardinality cannot certify that a withheld
                    // sibling/parent coordinate does not exist.
                    self.ingest_view_scoped_transaction_with_current_indexes(
                        bundle.tx.clone(),
                        bundle.versions.clone(),
                        bundle.fate.clone(),
                        bundle.global_time,
                        bundle.durability,
                    )
                    .await?;
                }
                crate::protocol::VersionBundleScope::CompleteTransaction => {
                    self.ingest_known_transaction(
                        bundle.tx.clone(),
                        bundle.versions.clone(),
                        bundle.fate.clone(),
                        bundle.global_time,
                        bundle.durability,
                    )
                    .await?;
                }
            }
            applied_bundles.push(bundle);
        }
        Ok(applied_bundles)
    }

    #[allow(dead_code)]
    pub(crate) async fn missing_known_state_row_version_refs(
        &mut self,
        message: &SyncMessage,
    ) -> Result<Vec<RowVersionRef>, Error> {
        let (subscription, version_carriers, program_fact_adds) = match message {
            SyncMessage::ViewUpdate(payload) if payload.peer_payload_inventory.opening_pending => {
                // Pending is not a row snapshot. Let normal admission reject
                // any attached rows immediately, rather than trying to repair
                // their bytes and postponing the protocol error indefinitely.
                return Ok(Vec::new());
            }
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                version_carriers,
                supporting_rows: program_fact_adds,
                ..
            }) => (*subscription, version_carriers, program_fact_adds),
            _ => return Ok(Vec::new()),
        };
        let normalized_bundles = expand_version_carriers(version_carriers)
            .map_err(|_| Error::UnsupportedSyncMessage("malformed version-bundle run"))?;
        // Index inline witnesses once. Searching every incoming body for
        // every supporting row makes initial hydration quadratic.
        let incoming_versions = normalized_bundles
            .iter()
            .flat_map(|bundle| {
                bundle
                    .versions
                    .iter()
                    .map(move |version| (bundle.tx.tx_id, version))
            })
            .filter_map(|(tx, version)| {
                self.physical_table_id_for_schema(version.schema_version(), version.table())
                    .ok()
                    .map(|table| {
                        (
                            tx,
                            version.row_uuid(),
                            table,
                            if version.deletion().is_some() {
                                crate::protocol::ResultRowLayer::Deletion
                            } else {
                                crate::protocol::ResultRowLayer::Content
                            },
                            version.branch_key().canonical_bytes(),
                        )
                    })
            })
            .collect::<BTreeSet<_>>();
        let Some(registered_shape) = self.registered_shape(subscription.shape_id) else {
            // A late update may race a local unsubscribe. There is no live
            // result shape to repair or apply, so preserve the existing
            // stale-update behavior and ignore it rather than turning normal
            // teardown into a protocol error.
            return Ok(Vec::new());
        };
        let result_schema_version = registered_shape.schema_version();
        let table_names = self
            .catalogue
            .physical_mappings
            .get(&result_schema_version)
            .map(|mapping| {
                mapping
                    .identities
                    .tables
                    .iter()
                    .map(|(name, identity)| (identity.id, name.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let mut missing = BTreeSet::new();
        // Every referenced native body must be available, including retained rows
        // whose bytes may have been evicted since the previous complete snapshot.
        for row in program_fact_adds.added_rows() {
            let tx_id = row.version.tx;
            let version_ref = RowVersionRef::new(row.version_table.to_string(), row.row, tx_id);
            if self.inline_version_bundle_covers(
                &version_ref,
                result_schema_version,
                &row.version,
                &incoming_versions,
            )? {
                continue;
            }
            let resident = match table_names.get(&row.physical_table) {
                Some(table) => {
                    self.local_supporting_row_version(row, result_schema_version, table)
                        .await?
                }
                None => None,
            };
            let transaction_exists = if resident.is_some() {
                Some(self.transaction_exists(tx_id).await?)
            } else {
                None
            };
            #[cfg(any(test, feature = "testing"))]
            crate::delivery_diagnostics::record(|| format!("repair_body_lookup runtime={} subscription={subscription:?} physical={:?} row_hash={} tx_hash={} layer={:?} table_known={} alias_known={} resident={} transaction_exists={transaction_exists:?}", self.groove_runtime_token(), row.physical_table, crate::delivery_diagnostics::opaque_hash(&row.row), crate::delivery_diagnostics::opaque_hash(&tx_id), row.version.layer, table_names.contains_key(&row.physical_table), self.node_aliases.contains_key(&tx_id.node), resident.is_some()));
            if resident.is_none() || transaction_exists == Some(false) {
                missing.insert(version_ref);
            }
        }
        Ok(missing.into_iter().collect())
    }

    /// Check inline ViewUpdate bundles against the same unambiguous physical
    /// identity used by row-version repair. A logical name may be dropped and
    /// later reused for another physical lineage, so name equality is never
    /// sufficient evidence that the incoming body covers a result member.
    fn inline_version_bundle_covers(
        &self,
        request: &RowVersionRef,
        result_schema_version: SchemaVersionId,
        version: &crate::protocol::RowVersionRefEntry,
        incoming_versions: &BTreeSet<(
            TxId,
            RowUuid,
            PhysicalTableId,
            crate::protocol::ResultRowLayer,
            Vec<u8>,
        )>,
    ) -> Result<bool, Error> {
        // Unlike a standalone RowVersionRef repair request, an inline witness
        // is carried by a registered subscription whose schema version makes
        // a reused logical table name unambiguous.
        let requested_table_id =
            match self.physical_table_id_for_schema(result_schema_version, &request.table) {
                Ok(table_id) => table_id,
                Err(Error::TableNotFound(_)) => {
                    // Contributor facts deliberately name their authored table,
                    // which may have been renamed out of the current result
                    // schema. An inline body can cover it only when that old name
                    // still has a unique physical lineage across the catalogue.
                    let candidates = self
                        .catalogue
                        .physical_mappings
                        .values()
                        .filter_map(|mapping| {
                            mapping
                                .tables
                                .get(request.table.as_str())
                                .map(|table| table.table_id)
                        })
                        .collect::<BTreeSet<_>>();
                    if candidates.is_empty() {
                        return Err(Error::TableNotFound(request.table.to_string()));
                    }
                    if candidates.len() != 1 {
                        return Ok(false);
                    }
                    *candidates.iter().next().expect("unique candidate")
                }
                Err(error) => return Err(error),
            };
        Ok(incoming_versions.contains(&(
            request.tx_id(),
            request.row_uuid,
            requested_table_id,
            version.layer,
            version.branch_or_prefix.clone().unwrap_or_default(),
        )))
    }

    async fn local_supporting_row_version(
        &mut self,
        row: &crate::protocol::SupportingRow,
        read_schema: SchemaVersionId,
        table: &str,
    ) -> Result<Option<VersionRow>, Error> {
        debug_assert_eq!(
            self.scope_physical_table(read_schema, table)?,
            row.physical_table
        );
        self.covered_input_version(row, read_schema).await
    }

    fn mint_tx_time(&mut self, now_ms: u64) -> Result<TxTime, Error> {
        let made_at = TxTime::tick(
            self.clock
                .tx_time
                .max(self.clock.reservation_high_water.get()),
            now_ms,
        )?;
        self.clock.tx_time = made_at;
        self.clock.reservation_high_water.set(made_at);
        Ok(made_at)
    }

    fn merge_tx_time(&mut self, observed: TxTime) {
        self.clock.tx_time = self.clock.tx_time.max(observed);
        self.clock
            .reservation_high_water
            .set(self.clock.reservation_high_water.get().max(observed));
    }
}
