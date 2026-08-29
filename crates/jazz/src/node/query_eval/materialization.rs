//! Query result and relation snapshot materialization.

use super::*;

#[derive(Clone, Copy)]
pub(super) struct RelationSnapshotWindow {
    pub(super) offset: usize,
    pub(super) limit: Option<usize>,
}

#[derive(Default)]
struct LocalMaintainedMaterializationCache {
    tx_versions: BTreeMap<TxId, Vec<VersionRow>>,
}

pub(crate) struct LocalMaintainedRelationSnapshot {
    pub(crate) snapshot: RelationSnapshot,
    pub(crate) root_occurrence_ids: Vec<OutputOccurrenceId>,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) async fn materialize_authoritative_reset_member(
        &mut self,
        query: &crate::query::Query,
        member: &ResultMemberEntry,
        result_payloads: &BTreeMap<ResultMemberEntry, ResultMemberPayloadEntry>,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            query.table.as_str(),
            query.aggregate.is_some(),
        ) && let Some(payload) = result_payloads.get(member)
        {
            return self
                .current_row_from_aggregate_result_payload(query, member, payload)
                .map(Some);
        }
        if (member
            .as_real_row()
            .is_some_and(|row| row.row_digest.is_some())
            || query.flat_join.is_some()
            || member.as_row().is_none())
            && let Some(payload) = result_payloads.get(member)
        {
            let Some(table_name) = member.table_name() else {
                return Err(Error::InvalidStoredValue(
                    "result payload member must name a table",
                ));
            };
            let table = self.table(table_name)?.clone();
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }

        let Some((table_name, row_uuid, tx_id)) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "authoritative reset cannot materialize non-row result without payload",
            ));
        };
        if let Some(row) = self
            .materialize_authoritative_reset_current_row(table_name.as_str(), row_uuid)
            .await?
        {
            return Ok(Some(row));
        }
        self.materialize_authoritative_reset_version_row(table_name.as_str(), row_uuid, tx_id, None)
            .await
    }

    async fn materialize_authoritative_reset_current_row(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let schema_version = self.catalogue.current_schema_version_id;
        let table_id = self.physical_table_id_for_schema(schema_version, table_name)?;
        let content_graph = self.physical_current_source_scan_graph(
            schema_version,
            table_name,
            PhysicalCurrentClass::Global,
            StaticScanSpec::Point(vec![groove::ivm::LiteralValue::from(Value::Uuid(
                row_uuid.0,
            ))]),
        )?;
        let content = self
            .database
            .query_graph(content_graph)
            .await
            .map_err(|error| Self::malformed_current_query_error(table_name, row_uuid, error))?;
        let Some(content_delta) = content.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let content_record = BorrowedRecord::new(&content_delta.record, &content.descriptor);
        let content_tx = self.current_record_sort_key(table_name, row_uuid, content_record)?;
        if let Some(deletion_raw) = self
            .database
            .primary_key_get_raw(
                &physical_register_global_current_table_name(table_id),
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?
        {
            let deletion_record = deletion_raw.record();
            let deletion_tx =
                self.current_record_sort_key(table_name, row_uuid, deletion_record)?;
            let deletion = deletion_event_from_value(
                deletion_record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?;
            if deletion_tx > content_tx && deletion == DeletionEvent::Deleted {
                return Ok(None);
            }
        }
        let row = decode_current_row(&table, content_record)?;
        self.materialize_current_row(&table, row).map(Some)
    }

    pub(super) async fn materialize_authoritative_reset_version_row(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        projection: Option<&[String]>,
    ) -> Result<Option<CurrentRow>, Error> {
        let table = self.table(table_name)?.clone();
        let Some(tx_node_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Err(Error::MissingTransaction(tx_id));
        };
        let version = self
            .query_version_by_alias(
                table_name,
                row_uuid,
                VersionLayer::Content,
                tx_id.time,
                tx_node_alias,
            )
            .await?;
        let version = if let Some(version) = version {
            version
        } else {
            // A row result member names the immutable witness by
            // `(table,row,tx)` while branch identity travels on the bundled
            // version itself. The legacy point lookup above addresses the
            // shared/default branch; fall back to the transaction bundle so a
            // branch-keyed witness can be materialized after selected delivery.
            let versions = self.query_versions_for_tx(tx_id).await?;
            if let Some(version) = self.maintained_witness_for_result_member(
                &versions,
                self.catalogue.current_schema_version_id,
                table_name,
                row_uuid,
            )? {
                version.clone()
            } else {
                if self.query_transaction(tx_id).await?.is_some() {
                    return Ok(None);
                }
                return Err(Error::MissingTransaction(tx_id));
            }
        };
        let mut row = self.current_row_from_materialized_version(&table, &version)?;
        if let Some(columns) = projection {
            row = row.project(&table, columns)?;
        }
        Ok(Some(row))
    }

    pub(super) async fn materialize_authoritative_reset_relation_edge_target(
        &mut self,
        read_schema: SchemaVersionId,
        target_table_name: &str,
        target_row: RowUuid,
        version_ref: &RowVersionRefEntry,
    ) -> Result<Option<CurrentRow>, Error> {
        let version = self
            .resolve_relation_edge_version(target_table_name, target_row, version_ref)
            .await?;
        // Relation-edge facts retain the canonical authored table name.  A
        // read schema can have renamed that table, so resolving the edge name
        // directly against the read descriptor would reject an otherwise
        // complete canonical witness before the lens gets a chance to map it.
        // Resolve and project the immutable version first, then select the
        // projected table from the read schema.
        let authored_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "relation edge witness schema version alias must exist",
            ))?;
        let authored_table = self
            .table_in_schema(version.table(), authored_schema)?
            .clone();
        let mut cells = self.materialized_cells_for_version(&authored_table, &version)?;
        let Some(projected_table_name) =
            self.translate_cells(authored_schema, read_schema, version.table(), &mut cells)?
        else {
            return Ok(None);
        };
        let projected_table = self
            .table_in_schema(&projected_table_name, read_schema)?
            .clone();
        current_row_from_materialized_cells(&projected_table, &version, &cells).map(Some)
    }

    pub(super) async fn project_relation_edge_through_read_schema(
        &mut self,
        edge: &RelationEdgeEntry,
        read_schema: SchemaVersionId,
    ) -> Result<RelationEdge, Error> {
        Ok(RelationEdge {
            source_table: self
                .project_relation_edge_table_through_read_schema(
                    edge.source_table.as_str(),
                    edge.source_row,
                    edge.source_version.as_ref(),
                    read_schema,
                )
                .await?,
            source_row: edge.source_row,
            relation: edge.path.clone(),
            target_table: self
                .project_relation_edge_table_through_read_schema(
                    edge.target_table.as_str(),
                    edge.target_row,
                    edge.target_version.as_ref(),
                    read_schema,
                )
                .await?,
            target_row: edge.target_row,
        })
    }

    async fn project_relation_edge_table_through_read_schema(
        &mut self,
        canonical_table: &str,
        row_uuid: RowUuid,
        version_ref: Option<&RowVersionRefEntry>,
        read_schema: SchemaVersionId,
    ) -> Result<String, Error> {
        let Some(version_ref) = version_ref else {
            // A fact without a concrete version is already required to name
            // the read view. Never relabel it optimistically.
            self.table_in_schema(canonical_table, read_schema)?;
            return Ok(canonical_table.to_owned());
        };
        let version = self
            .resolve_relation_edge_version(canonical_table, row_uuid, version_ref)
            .await?;
        let authored_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "relation edge witness schema version alias must exist",
            ))?;
        let mut cells = BTreeMap::new();
        self.translate_cells(authored_schema, read_schema, version.table(), &mut cells)?
            .ok_or(Error::InvalidStoredValue(
                "relation edge witness does not project into the read schema",
            ))
    }

    pub(super) async fn resolve_relation_edge_version(
        &mut self,
        canonical_table: &str,
        row_uuid: RowUuid,
        version_ref: &RowVersionRefEntry,
    ) -> Result<VersionRow, Error> {
        let tx_node_alias = self
            .node_aliases
            .get(&version_ref.tx.node)
            .copied()
            .ok_or(Error::MissingTransaction(version_ref.tx))?;
        let version = self
            .query_version_by_alias(
                canonical_table,
                row_uuid,
                VersionLayer::Content,
                version_ref.tx.time,
                tx_node_alias,
            )
            .await?
            .ok_or(Error::MissingTransaction(version_ref.tx))?;
        Ok(version)
    }

    pub(super) async fn resolve_relation_terminal_version(
        &mut self,
        emitted_table: &str,
        row_uuid: RowUuid,
        version_ref: &RowVersionRefEntry,
        read_schema: SchemaVersionId,
    ) -> Result<VersionRow, Error> {
        let candidates = self
            .query_versions_for_tx(version_ref.tx)
            .await?
            .into_iter()
            .filter(|version| {
                version.row_uuid() == row_uuid && version.layer() == VersionLayer::Content
            })
            .collect::<Vec<_>>();
        let mut matching = Vec::new();
        for version in candidates {
            let authored_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "relation edge witness schema version alias must exist",
                ))?;
            let mut cells = BTreeMap::new();
            if self.translate_cells(authored_schema, read_schema, version.table(), &mut cells)?
                == Some(emitted_table.to_owned())
            {
                matching.push(version);
            }
        }
        match matching.as_slice() {
            [version] => Ok(version.clone()),
            [] => Err(Error::InvalidStoredValue(
                "relation edge terminal witness does not project to its emitted table",
            )),
            _ => Err(Error::InvalidStoredValue(
                "relation edge terminal witness is ambiguous after projection",
            )),
        }
    }

    #[allow(dead_code)]
    fn relation_edge_branch_id(
        version_ref: &RowVersionRefEntry,
    ) -> Result<Option<SchemaFamilyId>, Error> {
        let Some(bytes) = &version_ref.branch_or_prefix else {
            return Ok(None);
        };
        let branch: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
            Error::InvalidStoredValue("relation edge branch discriminator must be a UUID")
        })?;
        Ok(Some(SchemaFamilyId::from_bytes(branch)))
    }

    pub(super) fn materialize_historical_query_rows(
        &mut self,
        table: TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let row = decode_current_row(&table, record)?;
                rows.push(self.materialize_current_row(&table, row)?);
            }
        }
        Ok(rows)
    }

    pub(super) fn materialize_include_deleted_query_rows(
        &mut self,
        table: TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let deleted_field_idx = current_row_fields(&table).len();
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let deleted = record.get_bool(deleted_field_idx)?;
                let row = decode_current_row(&table, record)?;
                let row = self.materialize_current_row(&table, row)?;
                rows.push(if deleted { row.into_deleted() } else { row });
            }
        }
        Ok(rows)
    }

    pub(super) fn materialize_inline_current_query_rows(
        &mut self,
        table: &TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, weight) in deltas.iter() {
            if weight > 0 {
                let row = decode_current_row(table, record)?;
                rows.push(self.materialize_current_row(table, row)?);
            }
        }
        Ok(rows)
    }

    pub(super) fn materialize_aggregate_query_rows(
        &mut self,
        query: &crate::query::Query,
        _table: &TableSchema,
        deltas: groove::ivm::RecordDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for (record, _weight) in deltas.iter().filter(|(_, weight)| *weight > 0) {
            rows.push(aggregate_current_row_from_record(
                &query.table,
                aggregate_query_row_uuid(query, &record)?,
                &record,
            )?);
        }
        Ok(rows)
    }

    async fn preload_local_maintained_materialization_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<LocalMaintainedMaterializationCache, Error> {
        let mut cache = LocalMaintainedMaterializationCache::default();
        // Storage-backed root subscriptions carry exact result-member
        // identities, not a source-wide witness cache. Loading the whole
        // transaction set here would silently recreate the retained-source
        // cost at every reset; the per-member path below performs an exact
        // `(table, row, tx)` lookup instead.
        if local
            .maintained
            .uses_storage_backed_result_materialization()
        {
            return Ok(cache);
        }
        let mut tx_ids = BTreeSet::new();
        for member in &local.result_set {
            let Some((_, _, tx_id)) = member.as_row() else {
                continue;
            };
            tx_ids.insert(tx_id);
            cache
                .tx_versions
                .entry(tx_id)
                .or_insert_with(|| local.maintained.versions_by_tx(tx_id));
        }
        for fact in &local.program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            let Some(version) = &edge.target_version else {
                continue;
            };
            tx_ids.insert(version.tx);
            cache
                .tx_versions
                .entry(version.tx)
                .or_insert_with(|| local.maintained.versions_by_tx(version.tx));
        }
        self.preload_tx_versions_for_materialization(tx_ids, &mut cache.tx_versions)
            .await?;
        Ok(cache)
    }

    async fn materialize_local_maintained_view_relation_edge_row_with_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        table_name: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Option<CurrentRow>, Error> {
        let tx_versions = self.local_maintained_tx_versions(local, tx_id, cache);
        let Some(version) =
            local_maintained_view_content_witness(tx_versions, table_name, row_uuid)
        else {
            return Ok(None);
        };
        let version = version.clone();
        self.projected_current_row_from_materialized_version_in_read_schema(
            local.result_schema_version,
            &version,
        )
    }

    pub(super) async fn materialize_local_maintained_view_result_member(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        member: &ResultMemberEntry,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            local.result_table.as_str(),
            local.result_query.aggregate.is_some(),
        ) {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "aggregate result member is missing its payload",
                ))?;
            return self
                .current_row_from_aggregate_result_payload(&local.result_query, member, payload)
                .map(Some);
        }
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        // Result-member entries retain the canonical authored table so the
        // membership witness continues to identify the exact immutable
        // version across a rename.  Payloads and application rows, however,
        // are interpreted in this subscription's read schema.
        let table = self.table(local.result_table.as_str())?.clone();
        if local.result_query.flat_join.is_some() {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "flat joined result member is missing its tuple payload",
                ))?;
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }
        if local.result_select.is_some()
            && let Some(payload) = local.result_payloads.get(member)
        {
            let mut row = self.current_row_from_result_payload(&table, payload)?;
            if let Some(columns) = &local.result_select {
                row = row.project(&table, columns)?;
            }
            return Ok(Some(row));
        }
        let mut tx_versions = local.maintained.versions_by_tx(entry.2);
        let version = if let Some(version) = self.maintained_witness_for_result_member(
            &tx_versions,
            local.result_schema_version,
            local.result_table.as_str(),
            entry.1,
        )? {
            version.clone()
        } else {
            let (content_winner, _) = local.maintained.replacement_for(entry.0.as_str(), entry.1);
            if let Some(content_winner) = content_winner {
                if self.version_tx_id(&content_winner)? != entry.2 {
                    return Ok(None);
                }
                tx_versions.push(content_winner);
                tx_versions
                    .last()
                    .ok_or(Error::MissingTransaction(entry.2))?
                    .clone()
            } else {
                // A client-local maintained graph can lag its remote
                // authority's source. The authority ViewUpdate has already
                // authenticated both this exact result member and its content
                // bundle; use that member's exact `(table, row, tx)` witness
                // to materialize the newly admitted row. This is payload
                // lookup, not a facade-side query or recompute.
                let tx_versions = self
                    .storage_backed_maintained_result_versions(entry.0.as_str(), entry.1, entry.2)
                    .await?;
                let Some(version) = self.maintained_witness_for_result_member(
                    &tx_versions,
                    local.result_schema_version,
                    local.result_table.as_str(),
                    entry.1,
                )?
                else {
                    return Ok(None);
                };
                version.clone()
            }
        };
        let mut row = self
            .projected_current_row_from_materialized_version_in_read_schema(
                local.result_schema_version,
                &version,
            )?
            .ok_or(Error::InvalidStoredValue(
                "maintained result witness does not project into the read schema",
            ))?;
        if let Some(columns) = &local.result_select {
            row = row.project(&table, columns)?;
        }
        Ok(Some(row))
    }

    async fn materialize_local_maintained_view_result_member_with_cache(
        &mut self,
        local: &LocalMaintainedViewSubscription,
        member: &ResultMemberEntry,
        cache: &mut LocalMaintainedMaterializationCache,
    ) -> Result<Option<CurrentRow>, Error> {
        if is_public_aggregate_result_member(
            member,
            local.result_table.as_str(),
            local.result_query.aggregate.is_some(),
        ) {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "aggregate result member is missing its payload",
                ))?;
            return self
                .current_row_from_aggregate_result_payload(&local.result_query, member, payload)
                .map(Some);
        }
        let Some(entry) = member.as_row() else {
            return Err(Error::InvalidStoredValue(
                "local maintained subscription cannot materialize non-row result member yet",
            ));
        };
        // See the non-cached path above: the entry label is canonical while
        // the materialized row belongs to the subscription read schema.
        let table = self.table(local.result_table.as_str())?.clone();
        if local.result_query.flat_join.is_some() {
            let payload = local
                .result_payloads
                .get(member)
                .ok_or(Error::InvalidStoredValue(
                    "flat joined result member is missing its tuple payload",
                ))?;
            return self
                .current_row_from_result_payload(&table, payload)
                .map(Some);
        }
        let tx_versions = self
            .local_maintained_tx_versions(local, entry.2, cache)
            .to_vec();
        let version = if let Some(version) = self.maintained_witness_for_result_member(
            &tx_versions,
            local.result_schema_version,
            local.result_table.as_str(),
            entry.1,
        )? {
            version.clone()
        } else {
            let tx_versions = self
                .storage_backed_maintained_result_versions(entry.0.as_str(), entry.1, entry.2)
                .await?;
            let Some(version) = self.maintained_witness_for_result_member(
                &tx_versions,
                local.result_schema_version,
                local.result_table.as_str(),
                entry.1,
            )?
            else {
                return Ok(None);
            };
            version.clone()
        };
        let _ = cache;
        self.projected_current_row_from_materialized_version_in_read_schema(
            local.result_schema_version,
            &version,
        )
    }

    fn local_maintained_tx_versions<'a>(
        &'a mut self,
        local: &LocalMaintainedViewSubscription,
        tx_id: TxId,
        cache: &'a mut LocalMaintainedMaterializationCache,
    ) -> &'a [VersionRow] {
        cache
            .tx_versions
            .entry(tx_id)
            .or_insert_with(|| local.maintained.versions_by_tx(tx_id))
            .as_slice()
    }

    /// Load only the immutable content/deletion cells for one delivered
    /// result-member identity.  This is deliberately narrower than
    /// `query_versions_for_tx`: a multi-row transaction must not turn a
    /// one-row subscription entry into a whole-transaction materialization.
    async fn storage_backed_maintained_result_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        tx_id: TxId,
    ) -> Result<Vec<VersionRow>, Error> {
        let stored_tx = self
            .query_transaction(tx_id)
            .await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        self.query_versions_for_tx_rows_by_alias(
            tx_id,
            stored_tx.node_alias,
            &BTreeSet::from([(table.to_owned(), row_uuid)]),
        )
        .await
    }

    async fn preload_tx_versions_for_materialization(
        &mut self,
        tx_ids: impl IntoIterator<Item = TxId>,
        cache: &mut BTreeMap<TxId, Vec<VersionRow>>,
    ) -> Result<(), Error> {
        let mut by_alias = BTreeMap::<(NodeUuid, NodeAlias), BTreeSet<TxTime>>::new();
        for tx_id in tx_ids {
            if cache
                .get(&tx_id)
                .is_some_and(|versions| !versions.is_empty())
            {
                continue;
            }
            if let Some(versions) = self.cached_tx_versions(tx_id) {
                cache.insert(tx_id, versions);
                continue;
            }
            if let Some(alias) = self.node_aliases.get(&tx_id.node).copied() {
                by_alias
                    .entry((tx_id.node, alias))
                    .or_default()
                    .insert(tx_id.time);
                cache.entry(tx_id).or_default();
            }
        }

        if by_alias.is_empty() {
            return Ok(());
        }

        let tables = self.tx_version_scan_tables();
        for ((node, alias), times) in by_alias {
            for (start, end) in contiguous_tx_time_spans(&times) {
                let Some(end) = end else {
                    let tx_id = TxId::new(start, node);
                    let versions = self.query_versions_for_tx(tx_id).await?;
                    cache.insert(tx_id, versions);
                    continue;
                };
                let mut scanned_sources = BTreeSet::new();
                for table in &tables {
                    for storage_table in self.version_storage_sources(table)? {
                        if !scanned_sources.insert(storage_table.clone()) {
                            continue;
                        }
                        let raws = self
                            .database
                            .index_scan_range_raw(
                                &storage_table,
                                "by_tx",
                                &[Value::U64(start.0), Value::U64(alias.0)],
                                &[Value::U64(end.0), Value::U64(0)],
                            )
                            .await?
                            .into_iter()
                            .map(|raw| raw.owned_record())
                            .collect::<Vec<_>>();
                        for record in raws {
                            let requested_table = if storage_table == SHARED_DELETION_HISTORY_TABLE
                            {
                                ""
                            } else {
                                table
                            };
                            let version = self.decode_history_owned_record(
                                requested_table,
                                &storage_table,
                                record,
                            )?;
                            if version.tx_node_alias() != alias
                                || !times.contains(&version.tx_time())
                            {
                                continue;
                            }
                            let tx_id = TxId::new(version.tx_time(), node);
                            cache.entry(tx_id).or_default().push(version);
                        }
                    }
                }
            }
        }

        for versions in cache.values_mut() {
            versions.sort_by(|left, right| {
                left.table()
                    .cmp(right.table())
                    .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                    .then_with(|| left.layer().cmp(&right.layer()))
            });
        }
        Ok(())
    }

    /// Render a maintained relation witness through the subscription's read
    /// schema before exposing it as an application row.
    ///
    /// Relation-edge witnesses retain the authored immutable record so that
    /// their identity and provenance remain exact.  They are not, however,
    /// rendered directly; apply the complete schema lens before decoding.
    pub(super) fn projected_current_row_from_materialized_version_in_read_schema(
        &mut self,
        read_schema: SchemaVersionId,
        version: &VersionRow,
    ) -> Result<Option<CurrentRow>, Error> {
        let authored_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "relation edge witness schema version alias must exist",
            ))?;
        let authored_table = self
            .table_in_schema(version.table(), authored_schema)?
            .clone();
        let mut cells = self.materialized_cells_for_version(&authored_table, version)?;
        let Some(projected_table) =
            self.translate_cells(authored_schema, read_schema, version.table(), &mut cells)?
        else {
            return Ok(None);
        };
        let read_table = self.table_in_schema(&projected_table, read_schema)?.clone();
        current_row_from_materialized_cells(&read_table, version, &cells).map(Some)
    }

    fn current_row_from_aggregate_result_payload(
        &mut self,
        query: &crate::query::Query,
        member: &ResultMemberEntry,
        payload: &ResultMemberPayloadEntry,
    ) -> Result<CurrentRow, Error> {
        let payload_descriptor = groove::records::decode_record_descriptor(&payload.descriptor)
            .map_err(|_| Error::InvalidStoredValue("result payload descriptor is invalid"))?;
        if payload_descriptor
            .fields()
            .iter()
            .any(|field| field.name.is_none())
        {
            return Err(Error::InvalidStoredValue(
                "result payload descriptor field must be named",
            ));
        }
        let payload_record = BorrowedRecord::new(&payload.record, &payload_descriptor);
        aggregate_current_row_from_record(
            query.table.as_str(),
            aggregate_result_member_row_uuid(member)?,
            &payload_record,
        )
    }

    pub(super) fn current_row_from_result_payload(
        &mut self,
        table: &TableSchema,
        payload: &ResultMemberPayloadEntry,
    ) -> Result<CurrentRow, Error> {
        let payload_descriptor = groove::records::decode_record_descriptor(&payload.descriptor)
            .map_err(|_| Error::InvalidStoredValue("result payload descriptor is invalid"))?;
        if payload_descriptor
            .fields()
            .iter()
            .any(|field| field.name.is_none())
        {
            return Err(Error::InvalidStoredValue(
                "result payload descriptor field must be named",
            ));
        }
        let payload_record = BorrowedRecord::new(&payload.record, &payload_descriptor);
        let row_uuid_idx = payload_descriptor
            .field_index("row_uuid")
            .or_else(|| payload_descriptor.field_index("id"))
            .ok_or(Error::InvalidStoredValue(
                "result payload is missing row identity",
            ))?;
        let row_uuid = payload_record.get_uuid(row_uuid_idx)?;
        let mut descriptor_fields = vec![("row_uuid".to_owned(), ValueType::Uuid)];
        let mut values = vec![Value::Uuid(row_uuid)];
        for (index, field) in payload_descriptor.fields().iter().enumerate() {
            let Some(name) = &field.name else {
                continue;
            };
            if name == "row_uuid" || name == "id" {
                continue;
            }
            descriptor_fields.push((name.clone(), field.value_type.clone()));
            values.push(payload_record.get_idx(index)?);
        }
        let descriptor = RecordDescriptor::new(descriptor_fields);
        let raw = descriptor.create(&values)?;
        let row = CurrentRow::new(table.name.clone(), OwnedRecord::new(raw, descriptor));
        if row.raw_field("__flat_join_row_1").is_some() {
            return Ok(row);
        }
        self.materialize_current_row(table, row)
    }

    pub(super) async fn materialize_relation_snapshot_from_query_engine(
        &mut self,
        shape: &ValidatedQuery,
        read_view: &ReadViewSpec,
        snapshots: &MultisinkDeltas,
    ) -> Result<RelationSnapshot, Error> {
        let root_rows = self.materialize_relation_snapshot_root_rows(shape, snapshots)?;
        let root_count = root_rows.len();
        // Groove's app-rows terminal is the sole structured-output owner.
        // Jazz transports its recursive roots; relation facts are not a second
        // public representation and never participate in tree assembly.
        if !shape.query().array_subqueries.is_empty() {
            return Ok(RelationSnapshot {
                root_count,
                rows: root_rows,
                edges: Vec::new(),
            });
        }
        let mut snapshot = RelationSnapshot {
            root_count,
            rows: root_rows,
            edges: Vec::new(),
        };
        // Aggregate result records intentionally have no application row
        // identity and cannot own relation edges. They are already fully
        // materialized by the root terminal.
        if shape.query().aggregate.is_some() {
            return Ok(snapshot);
        }
        let mut row_keys = snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>();
        let Some(edges) = snapshots.get("maintained.relation_edges") else {
            return Ok(snapshot);
        };
        #[derive(Clone)]
        struct RelationEdgeCandidate {
            edge: RelationEdge,
            canonical_target_table: String,
            target_tx_time: TxTime,
            target_tx_node: NodeAlias,
        }

        let windows = Self::relation_snapshot_no_order_windows(&shape.query().array_subqueries);
        let descriptor = &edges.descriptor;
        let source_table_idx = required_field_idx(descriptor, "source_table")?;
        let source_row_idx = required_field_idx(descriptor, "source_row")?;
        let source_tx_time_idx = required_field_idx(descriptor, "source_tx_time")?;
        let source_tx_node_idx = required_field_idx(descriptor, "source_tx_node_id")?;
        let source_branch_idx = descriptor
            .fields()
            .iter()
            .position(|field| field.name.as_deref() == Some("source_branch_or_prefix"));
        let relation_idx = required_field_idx(descriptor, "path")?;
        let target_table_idx = required_field_idx(descriptor, "target_table")?;
        let target_row_idx = required_field_idx(descriptor, "target_row")?;
        let target_tx_time_idx = required_field_idx(descriptor, "target_tx_time")?;
        let target_tx_node_idx = required_field_idx(descriptor, "target_tx_node_id")?;
        let target_branch_idx = descriptor
            .fields()
            .iter()
            .position(|field| field.name.as_deref() == Some("target_branch_or_prefix"));
        let mut candidates = Vec::new();
        for (record, weight) in edges.iter() {
            if weight <= 0 {
                continue;
            }
            let source_table = record.get_str(source_table_idx)?.to_owned();
            let source_row = RowUuid(record.get_uuid(source_row_idx)?);
            let source_tx_time = TxTime(record.get_u64(source_tx_time_idx)?);
            let source_tx_node = NodeAlias(record.get_u64(source_tx_node_idx)?);
            let relation = record.get_str(relation_idx)?.to_owned();
            let target_table_name = record.get_str(target_table_idx)?.to_owned();
            let target_row = RowUuid(record.get_uuid(target_row_idx)?);
            let target_tx_time = TxTime(record.get_u64(target_tx_time_idx)?);
            let target_tx_node = NodeAlias(record.get_u64(target_tx_node_idx)?);
            let branch_discriminator = |idx| -> Result<Option<Vec<u8>>, Error> {
                let Some(idx) = idx else {
                    return Ok(None);
                };
                match record.get_idx(idx)? {
                    Value::Uuid(value) => Ok(Some(value.as_bytes().to_vec())),
                    Value::Bytes(value) => Ok(Some(value)),
                    Value::Nullable(Some(value)) => match *value {
                        Value::Uuid(value) => Ok(Some(value.as_bytes().to_vec())),
                        Value::Bytes(value) => Ok(Some(value)),
                        _ => Err(Error::InvalidStoredValue(
                            "relation edge branch discriminator must be UUID or bytes",
                        )),
                    },
                    Value::Nullable(None) => Ok(None),
                    _ => Err(Error::InvalidStoredValue(
                        "relation edge branch discriminator must be UUID or bytes",
                    )),
                }
            };
            let version_ref =
                |time, alias, branch_or_prefix| -> Result<RowVersionRefEntry, Error> {
                    let node = self
                        .node_aliases
                        .iter()
                        .find_map(|(node, candidate)| (*candidate == alias).then_some(*node))
                        .ok_or(Error::InvalidStoredValue(
                            "relation edge node alias is missing",
                        ))?;
                    Ok(RowVersionRefEntry {
                        tx: TxId::new(time, node),
                        schema_version: None,
                        layer: ResultRowLayer::Content,
                        batch: None,
                        branch_or_prefix,
                        row_digest: None,
                    })
                };
            let source_version = version_ref(
                source_tx_time,
                source_tx_node,
                branch_discriminator(source_branch_idx)?,
            )?;
            let target_version = version_ref(
                target_tx_time,
                target_tx_node,
                branch_discriminator(target_branch_idx)?,
            )?;
            let canonical_source_version = self
                .resolve_relation_terminal_version(
                    &source_table,
                    source_row,
                    &source_version,
                    shape.schema_version(),
                )
                .await?;
            let canonical_target_version = self
                .resolve_relation_terminal_version(
                    &target_table_name,
                    target_row,
                    &target_version,
                    shape.schema_version(),
                )
                .await?;
            let projected_source_table = self
                .project_relation_edge_table_through_read_schema(
                    canonical_source_version.table(),
                    source_row,
                    Some(&source_version),
                    shape.schema_version(),
                )
                .await?;
            let projected_target_table = self
                .project_relation_edge_table_through_read_schema(
                    canonical_target_version.table(),
                    target_row,
                    Some(&target_version),
                    shape.schema_version(),
                )
                .await?;
            candidates.push(RelationEdgeCandidate {
                edge: RelationEdge {
                    source_table: projected_source_table,
                    source_row,
                    relation,
                    target_table: projected_target_table,
                    target_row,
                },
                canonical_target_table: canonical_target_version.table().to_owned(),
                target_tx_time,
                target_tx_node,
            });
        }
        candidates.sort_by(|left, right| {
            (
                &left.edge.source_table,
                left.edge.source_row,
                &left.edge.relation,
                left.edge.target_row,
            )
                .cmp(&(
                    &right.edge.source_table,
                    right.edge.source_row,
                    &right.edge.relation,
                    right.edge.target_row,
                ))
        });
        let mut counts = BTreeMap::<(String, RowUuid, String), usize>::new();
        for candidate in candidates {
            let group = (
                candidate.edge.source_table.clone(),
                candidate.edge.source_row,
                candidate.edge.relation.clone(),
            );
            let count = counts.entry(group).or_default();
            let window = windows.get(&candidate.edge.relation).copied();
            let ordinal = *count;
            *count += 1;
            if let Some(window) = window {
                if ordinal < window.offset
                    || window
                        .limit
                        .is_some_and(|limit| ordinal >= window.offset.saturating_add(limit))
                {
                    continue;
                }
            }
            if row_keys.insert((
                candidate.edge.target_table.clone(),
                candidate.edge.target_row,
            )) {
                let row = self
                    .materialize_relation_edge_target_row(
                        read_view,
                        shape.schema_version(),
                        &candidate.canonical_target_table,
                        candidate.edge.target_row,
                        candidate.target_tx_time,
                        candidate.target_tx_node,
                    )
                    .await?;
                snapshot.rows.push(row);
            }
            snapshot.edges.push(candidate.edge);
        }
        Ok(snapshot)
    }

    pub(super) async fn materialize_relation_edge_target_row(
        &mut self,
        _read_view: &ReadViewSpec,
        read_schema: SchemaVersionId,
        target_table_name: &str,
        target_row: RowUuid,
        target_tx_time: TxTime,
        target_tx_node: NodeAlias,
    ) -> Result<CurrentRow, Error> {
        if let Some(version) = self
            .query_version_by_alias(
                target_table_name,
                target_row,
                VersionLayer::Content,
                target_tx_time,
                target_tx_node,
            )
            .await?
        {
            return self
                .projected_current_row_from_materialized_version_in_read_schema(
                    read_schema,
                    &version,
                )?
                .ok_or(Error::InvalidStoredValue(
                    "relation edge target version does not project into the read schema",
                ));
        }
        Err(Error::InvalidStoredValue(
            "relation edge target version is missing",
        ))
    }

    pub(super) fn relation_snapshot_no_order_windows(
        subqueries: &[ArraySubquery],
    ) -> BTreeMap<String, RelationSnapshotWindow> {
        let mut windows = BTreeMap::new();
        for subquery in subqueries {
            if subquery.order_by.is_empty() && (subquery.limit.is_some() || subquery.offset != 0) {
                windows.insert(
                    subquery.column_name.clone(),
                    RelationSnapshotWindow {
                        offset: subquery.offset,
                        limit: subquery.limit,
                    },
                );
            }
            windows.extend(Self::relation_snapshot_no_order_windows(
                &subquery.nested_arrays,
            ));
        }
        windows
    }

    fn materialize_relation_snapshot_root_rows(
        &mut self,
        shape: &ValidatedQuery,
        snapshots: &MultisinkDeltas,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(app_rows) = snapshots.get(JAZZ_APP_ROWS_SINK) else {
            return Err(Error::QueryLowering(
                "relation snapshot program did not emit app rows".to_owned(),
            ));
        };
        let table = self
            .table_in_schema(&shape.query().table, shape.schema_version())?
            .clone();
        let mut rows = Vec::new();
        for (record, weight) in app_rows.iter() {
            if weight > 0 {
                let row = decode_current_row(&table, record)?;
                rows.push(self.materialize_current_row(&table, row)?);
            }
        }
        // Multisink records are transport-key ordered. Restore public root rank
        // while retaining the lowered program's membership and window.
        self.apply_query_order_in_schema(shape.query(), shape.schema_version(), &mut rows)?;
        Ok(rows)
    }

    pub(super) fn finish_engine_query_rows_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
        rows: &mut Vec<CurrentRow>,
    ) -> Result<(), Error> {
        if query.aggregate.is_some() {
            self.apply_query_order_in_schema(query, schema_version, rows)?;
            apply_query_window(query, rows);
            return Ok(());
        }
        // Groove lowering owns membership/windowing, but one-shot APIs still
        // return a deterministic Vec. Re-apply ordering to the selected rows
        // without re-applying pagination.
        self.apply_query_order_in_schema(query, schema_version, rows)
    }

    pub(super) fn query_output_table(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
    ) -> Result<TableSchema, Error> {
        let source_table = self.table_in_schema(&query.table, schema_version)?;
        if query.aggregate.is_some() {
            aggregate_result_table(query, &source_table)
        } else {
            Ok(source_table)
        }
    }

    pub(super) fn sort_query_rows_with_occurrences(
        query: &crate::query::Query,
        table: Option<&TableSchema>,
        rows: &mut Vec<CurrentRow>,
        occurrence_ids: &mut Vec<OutputOccurrenceId>,
    ) -> Result<(), Error> {
        if rows.len() != occurrence_ids.len() {
            return Err(Error::InvalidStoredValue(
                "maintained root occurrence sidecar length does not match rows",
            ));
        }
        let mut paired = rows
            .drain(..)
            .zip(occurrence_ids.drain(..))
            .collect::<Vec<_>>();
        if query.order_by.is_empty() {
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    default_query_row_order(left_row, right_row)
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        } else if query.aggregate.is_some() {
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    for order in &query.order_by {
                        let ordering = compare_optional_values(
                            aggregate_row_cell(left_row, query, &order.column),
                            aggregate_row_cell(right_row, query, &order.column),
                        );
                        let ordering = match order.direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    left_row
                        .row_uuid()
                        .to_bytes()
                        .cmp(&right_row.row_uuid().to_bytes())
                        .then_with(|| left_row.record.raw().cmp(right_row.record.raw()))
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        } else {
            let table = table.ok_or(Error::InvalidStoredValue(
                "ordered maintained rows are missing their table schema",
            ))?;
            paired.sort_by(
                |(left_row, left_occurrence), (right_row, right_occurrence)| {
                    for order in &query.order_by {
                        let ordering = compare_optional_values(
                            query_order_value(left_row, &table, &order.column),
                            query_order_value(right_row, &table, &order.column),
                        );
                        let ordering = match order.direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    left_row
                        .row_uuid()
                        .to_bytes()
                        .cmp(&right_row.row_uuid().to_bytes())
                        .then_with(|| left_row.record.raw().cmp(right_row.record.raw()))
                        .then_with(|| left_occurrence.cmp(right_occurrence))
                },
            );
        }
        for (row, occurrence) in paired {
            rows.push(row);
            occurrence_ids.push(occurrence);
        }
        Ok(())
    }

    pub(super) fn apply_query_order_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        if query.order_by.is_empty() {
            sort_query_default_rows(rows);
            return Ok(());
        }
        sort_current_rows(rows);
        if query.aggregate.is_some() {
            rows.sort_by(|left, right| {
                for order in &query.order_by {
                    let ordering = compare_optional_values(
                        aggregate_row_cell(left, query, &order.column),
                        aggregate_row_cell(right, query, &order.column),
                    );
                    let ordering = match order.direction {
                        OrderDirection::Asc => ordering,
                        OrderDirection::Desc => ordering.reverse(),
                    };
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes())
            });
            return Ok(());
        }
        let table = self.table_in_schema(&query.table, schema_version)?;
        rows.sort_by(|left, right| {
            for order in &query.order_by {
                let ordering = compare_optional_values(
                    query_order_value(left, &table, &order.column),
                    query_order_value(right, &table, &order.column),
                );
                let ordering = match order.direction {
                    OrderDirection::Asc => ordering,
                    OrderDirection::Desc => ordering.reverse(),
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.row_uuid().to_bytes().cmp(&right.row_uuid().to_bytes())
        });
        Ok(())
    }

    pub(super) fn apply_projection_in_schema(
        &self,
        query: &crate::query::Query,
        schema_version: SchemaVersionId,
        rows: &mut [CurrentRow],
    ) -> Result<(), Error> {
        let Some(columns) = &query.select else {
            return Ok(());
        };
        let table = self.table_in_schema(&query.table, schema_version)?;
        for row in rows {
            *row = row.project(&table, columns)?;
        }
        Ok(())
    }

    pub(crate) fn relation_snapshot_has_materialized_required_cells(
        &self,
        query: &crate::query::Query,
        snapshot: &RelationSnapshot,
    ) -> Result<bool, Error> {
        if query.aggregate.is_some() || query.flat_join.is_some() {
            return Ok(true);
        }
        for (index, row) in snapshot.rows.iter().enumerate() {
            let table = self.table(row.table())?;
            let projection = (index < snapshot.root_count && row.table() == query.table)
                .then_some(query.select.as_deref())
                .flatten();
            if !current_row_has_required_subscription_cells(row, table, projection) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) async fn materialize_local_maintained_relation_snapshot_with_occurrences(
        &mut self,
        local: &LocalMaintainedViewSubscription,
    ) -> Result<LocalMaintainedRelationSnapshot, Error> {
        if !local.result_query.array_subqueries.is_empty() {
            let mut rows = local
                .maintained
                .structured_app_rows()
                .into_iter()
                .map(|(_, record)| CurrentRow::new(local.result_table.clone(), record))
                .collect::<Vec<_>>();
            self.apply_query_order(&local.result_query, &mut rows)?;
            let root_occurrence_ids = rows
                .iter()
                .map(|row| OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0)))
                .collect();
            return Ok(LocalMaintainedRelationSnapshot {
                snapshot: RelationSnapshot {
                    root_count: rows.len(),
                    rows,
                    edges: Vec::new(),
                },
                root_occurrence_ids,
            });
        }
        let mut cache = self
            .preload_local_maintained_materialization_cache(local)
            .await?;
        let mut rows = Vec::with_capacity(local.result_set.len());
        let mut root_occurrence_ids = Vec::with_capacity(local.result_set.len());
        let mut row_keys = BTreeSet::new();
        for member in &local.result_set {
            if let Some(row) = self
                .materialize_local_maintained_view_result_member_with_cache(
                    local, member, &mut cache,
                )
                .await?
            {
                let occurrence_id = public_result_member_occurrence_id(
                    member,
                    local.result_table.as_str(),
                    local.result_query.aggregate.is_some(),
                )?
                .ok_or(Error::InvalidStoredValue(
                    "maintained root member has no occurrence identity",
                ))?;
                row_keys.insert((row.table().to_owned(), row.row_uuid()));
                rows.push(row);
                root_occurrence_ids.push(occurrence_id);
            }
        }
        // `result_set` is keyed by member identity, so its BTreeSet iteration
        // order cannot be exposed as a query reset order. Do not re-window: the
        // maintained program already chose this result set. Materialize full
        // rows first, because an order key may not be in the public projection.
        self.apply_query_order_with_occurrences(
            &local.result_query,
            &mut rows,
            &mut root_occurrence_ids,
        )?;
        if local.result_query.aggregate.is_some() {
            root_occurrence_ids = rows
                .iter()
                .map(|row| OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0)))
                .collect();
        }
        self.apply_projection(&local.result_query, &mut rows)?;
        let root_count = rows.len();
        let mut edges = Vec::with_capacity(local.program_facts.len());
        for fact in &local.program_facts {
            let ProgramFactEntry::RelationEdge(edge) = fact else {
                continue;
            };
            let read_edge = self
                .project_relation_edge_through_read_schema(edge, local.result_schema_version)
                .await?;
            if row_keys.insert((read_edge.target_table.clone(), read_edge.target_row))
                && let Some(version) = &edge.target_version
                && let Some(row) = self
                    .materialize_local_maintained_view_relation_edge_row_with_cache(
                        local,
                        edge.target_table.as_str(),
                        edge.target_row,
                        version.tx,
                        &mut cache,
                    )
                    .await?
            {
                rows.push(row);
            }
            edges.push(read_edge);
        }
        Ok(LocalMaintainedRelationSnapshot {
            snapshot: RelationSnapshot {
                root_count,
                rows,
                edges,
            },
            root_occurrence_ids,
        })
    }
}
