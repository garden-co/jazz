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
        self.catalogue
            .catalogue_schemas
            .get(&schema_version)
            .and_then(|schema| {
                schema
                    .schema
                    .tables
                    .iter()
                    .find(|candidate| candidate.name == table)
                    .cloned()
            })
            .or_else(|| {
                (schema_version == self.catalogue.current_schema_version_id)
                    .then(|| self.table(table).ok().cloned())
                    .flatten()
            })
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    pub(super) fn shortest_lens_path_ids_cached(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        direction: LensPathDirection,
    ) -> Option<Vec<MigrationLensId>> {
        let key = LensPathCacheKey {
            source,
            target,
            direction,
        };
        if let Some(path) = self.catalogue.lens_path_cache.get(&key) {
            return path.clone();
        }
        let path = self.shortest_lens_path_ids(source, target, direction);
        self.catalogue.lens_path_cache.insert(key, path.clone());
        path
    }

    fn shortest_lens_path_ids(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        direction: LensPathDirection,
    ) -> Option<Vec<MigrationLensId>> {
        if source == target {
            return Some(Vec::new());
        }

        let mut seen = BTreeSet::from([source]);
        let mut queue = VecDeque::from([(source, Vec::<MigrationLensId>::new())]);
        while let Some((schema, path)) = queue.pop_front() {
            for lens in self.ordered_lens_edges(schema, direction) {
                let next = match direction {
                    LensPathDirection::Forward => lens.target,
                    LensPathDirection::Reverse => lens.source,
                };
                if seen.contains(&next) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(lens.id);
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
        direction: LensPathDirection,
        table: &str,
    ) -> Result<Option<CompiledLensPath>, Error> {
        let key = CompiledLensCacheKey {
            source,
            target,
            direction,
            table: table.to_owned(),
        };
        if let Some(path) = self.catalogue.compiled_lens_cache.get(&key) {
            return Ok(path.clone());
        }

        let Some(lens_ids) = self.shortest_lens_path_ids_cached(source, target, direction) else {
            self.catalogue.compiled_lens_cache.insert(key, None);
            return Ok(None);
        };
        let mut current_table = table.to_owned();
        let mut ops = Vec::new();
        for lens_id in lens_ids {
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
        direction: LensPathDirection,
    ) -> Vec<&MigrationLens> {
        let mut edges = self
            .catalogue
            .catalogue_lenses
            .values()
            .filter(|lens| match direction {
                LensPathDirection::Forward => lens.source == schema,
                LensPathDirection::Reverse => lens.target == schema,
            })
            .collect::<Vec<_>>();
        edges.sort_by(|left, right| {
            let left_next = match direction {
                LensPathDirection::Forward => left.target,
                LensPathDirection::Reverse => left.source,
            };
            let right_next = match direction {
                LensPathDirection::Forward => right.target,
                LensPathDirection::Reverse => right.source,
            };
            left_next
                .cmp(&right_next)
                .then_with(|| left.id.cmp(&right.id))
        });
        edges
    }

    fn node_for_alias(&self, alias: NodeAlias) -> Option<NodeUuid> {
        self.node_aliases
            .iter()
            .find_map(|(node, candidate)| (*candidate == alias).then_some(*node))
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
        let table = self.table_in_schema(version.table(), schema_version)?;
        let authored_columns = self.authored_columns_for_version(version)?;
        VersionRecord::from_stored(version, &table, schema_version, authored_columns)
    }

    pub(crate) async fn row_version_payloads_for_refs(
        &mut self,
        requests: &[RowVersionRef],
        identity: AuthorSubject,
    ) -> Result<Vec<VersionBundle>, Error> {
        let mut by_tx = BTreeMap::<TxId, Vec<VersionRow>>::new();
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
            let tx_id = request.tx_id();
            let matching_versions = self
                .query_versions_for_tx(tx_id)
                .await?
                .into_iter()
                .filter_map(|version| {
                    let table_id = self.physical_table_id_for_version(&version).ok()?;
                    (version.row_uuid() == request.row_uuid
                        && version.tx_time() == request.tx_time
                        && self.node_for_alias(version.tx_node_alias()) == Some(request.tx_node_id)
                        && candidate_mappings
                            .iter()
                            .any(|(_, candidate)| *candidate == table_id))
                    .then_some((table_id, version))
                })
                .collect::<Vec<_>>();
            let matching_table_ids = matching_versions
                .iter()
                .map(|(table_id, _)| *table_id)
                .collect::<BTreeSet<_>>();
            let [requested_table_id] = matching_table_ids.iter().copied().collect::<Vec<_>>()[..]
            else {
                return Err(Error::InvalidStoredValue(
                    "repair request row maps to zero or multiple physical tables",
                ));
            };
            let request_schema = [
                self.catalogue.current_write_schema.schema,
                self.catalogue.current_schema_version_id,
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
            if !self.dry_run_read_current_allows_in_schema(
                &request.table,
                request.row_uuid,
                request_schema,
                identity,
            )
            .await?
            {
                continue;
            }
            for (table_id, version) in matching_versions {
                if table_id == requested_table_id {
                    by_tx.entry(tx_id).or_default().push(version);
                    break;
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
    ) -> Result<(), Error> {
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
        for bundle in prevalidated_bundles {
            self.ingest_known_transaction(
                bundle.tx,
                bundle.versions,
                bundle.fate,
                bundle.global_time,
                bundle.durability,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn missing_known_state_row_version_refs(
        &mut self,
        message: &SyncMessage,
    ) -> Result<Vec<RowVersionRef>, Error> {
        let (
            subscription,
            result_member_adds,
            version_carriers,
            program_fact_adds,
        ) = match message {
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                result_member_adds,
                version_carriers,
                program_fact_adds,
                ..
            }) => (
                *subscription,
                result_member_adds,
                version_carriers,
                program_fact_adds,
            ),
            _ => return Ok(Vec::new()),
        };
        let normalized_bundles = expand_version_carriers(version_carriers)
            .map_err(|_| Error::UnsupportedSyncMessage("malformed version-bundle run"))?;
        let incoming_versions = normalized_bundles
            .iter()
            .flat_map(|bundle| {
                bundle
                    .versions
                    .iter()
                    .map(move |version| (bundle.tx.tx_id, version))
            })
            .collect::<Vec<_>>();
        let Some(registered_shape) = self.registered_shape(subscription.shape_id) else {
            // A late update may race a local unsubscribe. There is no live
            // result shape to repair or apply, so preserve the existing
            // stale-update behavior and ignore it rather than turning normal
            // teardown into a protocol error.
            return Ok(Vec::new());
        };
        let result_schema_version = registered_shape.schema_version();
        let mut missing = BTreeSet::new();
        let mut visited_text_ancestors = BTreeSet::new();
        for bundle in &normalized_bundles {
            for version in &bundle.versions {
                self.collect_missing_text_ancestor_refs(
                    version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
            }
        }
        // Only additions require repair. Removals are self-sufficient because
        // the removed version may now be policy-invisible to this receiver, in
        // which case fetching the body is both unnecessary and allowed to
        // return no payload.
        for (table, row_uuid, tx_id) in result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
        {
            let version_ref = RowVersionRef::new(table.to_string(), row_uuid, tx_id);
            if self.inline_version_bundle_covers(
                &version_ref,
                result_schema_version,
                &incoming_versions,
            )? {
                continue;
            }
            let has_body = self.local_version_row_for_ref(&version_ref).await?.is_some()
                && self.query_transaction(tx_id).await?.is_some();
            if !has_body {
                missing.insert(version_ref);
            } else if let Some(version) = self.local_version_record_for_ref(&version_ref).await? {
                self.collect_missing_text_ancestor_refs(
                    &version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
            }
        }
        for (table, row_uuid, tx_id) in program_fact_adds
            .iter()
            .flat_map(|fact| match fact {
                ProgramFactEntry::RelationEdge(edge) => vec![
                    edge.source_version.as_ref().map(|version| {
                        (edge.source_table.to_string(), edge.source_row, version.tx)
                    }),
                    edge.target_version.as_ref().map(|version| {
                        (edge.target_table.to_string(), edge.target_row, version.tx)
                    }),
                ],
                ProgramFactEntry::ContributingMembers(contribution)
                    if contribution
                        .role
                        .as_deref()
                        .is_some_and(|role| role.starts_with("flat_tuple_source:")) =>
                {
                    vec![
                        contribution
                            .contributor
                            .as_real_row()
                            .and_then(RealRowMemberEntry::row_projection)
                            .map(|(table, row, tx)| (table.to_string(), row, tx)),
                    ]
                }
                _ => Vec::new(),
            })
            .flatten()
        {
            let version_ref = RowVersionRef::new(table, row_uuid, tx_id);
            if self.inline_version_bundle_covers(
                &version_ref,
                result_schema_version,
                &incoming_versions,
            )? {
                continue;
            }
            let has_body = self.local_version_row_for_ref(&version_ref).await?.is_some()
                && self.query_transaction(tx_id).await?.is_some();
            if !has_body {
                missing.insert(version_ref);
            } else if let Some(version) = self.local_version_record_for_ref(&version_ref).await? {
                self.collect_missing_text_ancestor_refs(
                    &version,
                    &mut missing,
                    &mut visited_text_ancestors,
                )?;
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
        incoming_versions: &[(TxId, &VersionRecord)],
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
        Ok(incoming_versions.iter().any(|(incoming_tx, version)| {
            *incoming_tx == request.tx_id()
                && version.row_uuid() == request.row_uuid
                && self
                    .physical_table_id_for_schema(version.schema_version(), version.table())
                    .is_ok_and(|table_id| table_id == requested_table_id)
        }))
    }

    fn collect_missing_text_ancestor_refs(
        &mut self,
        _version: &VersionRecord,
        _missing: &mut BTreeSet<RowVersionRef>,
        _visited: &mut BTreeSet<RowVersionRef>,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn local_version_record_for_ref(
        &mut self,
        version_ref: &RowVersionRef,
    ) -> Result<Option<VersionRecord>, Error> {
        let Some(version) = self.local_version_row_for_ref(version_ref).await? else {
            return Ok(None);
        };
        self.version_record_from_row(&version).map(Some)
    }

    async fn local_version_row_for_ref(
        &mut self,
        version_ref: &RowVersionRef,
    ) -> Result<Option<VersionRow>, Error> {
        let Some(tx_node_alias) = self.node_aliases.get(&version_ref.tx_node_id).copied() else {
            return Ok(None);
        };
        for layer in [VersionLayer::Content, VersionLayer::Deletion] {
            if let Some(version) = self.query_version_by_alias(
                &version_ref.table,
                version_ref.row_uuid,
                layer,
                version_ref.tx_time,
                tx_node_alias,
            )
            .await?
            {
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    fn mint_tx_time(&mut self, now_ms: u64) -> Result<TxTime, Error> {
        let made_at = TxTime::tick(self.clock.tx_time, now_ms)?;
        self.clock.tx_time = made_at;
        Ok(made_at)
    }

    fn merge_tx_time(&mut self, observed: TxTime) {
        self.clock.tx_time = self.clock.tx_time.max(observed);
    }
}
