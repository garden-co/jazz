//! Currency and version-selection reads over local and global storage. This
//! module owns row-version scans, winner lookup, transaction lookup, and
//! history queries that implement the merge/currency rules in `jazz/README.md`;
//! write ingestion lives in [`super::ingest`], read-only global derivations in
//! [`super::global_state`], and storage encoding in [`super::codec`]. It is a
//! node-level read layer over groove tables.

use super::*;
use crate::schema::RuntimeSchema;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) async fn query_row_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        self.query_row_versions_in_branch(table, &BranchKey::default(), row_uuid)
            .await
    }

    pub(super) async fn query_row_versions_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for storage_table in self.version_storage_sources_for_layer(table, VersionLayer::Content)? {
            let raws = self
                .database
                .primary_key_scan_raw(
                    &storage_table,
                    &[
                        Value::Bytes(branch_key.canonical_bytes()),
                        Value::Uuid(row_uuid.0),
                    ],
                )
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for record in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, record)?);
            }
        }
        for storage_table in
            self.version_storage_sources_for_layer(table, VersionLayer::Deletion)?
        {
            let raws = self
                .database
                .primary_key_scan_raw(
                    &storage_table,
                    &self.deletion_storage_prefix_in_branch(table, branch_key, Some(row_uuid))?,
                )
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for record in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, record)?);
            }
        }
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, &aliases).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_table_versions_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for storage_table in self.version_storage_sources_for_layer(table, VersionLayer::Content)? {
            let raws = self
                .database
                .primary_key_scan_raw(
                    &storage_table,
                    &[Value::Bytes(branch_key.canonical_bytes())],
                )
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for raw in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, raw)?);
            }
        }
        for storage_table in
            self.version_storage_sources_for_layer(table, VersionLayer::Deletion)?
        {
            let prefix = self.deletion_storage_prefix_in_branch(table, branch_key, None)?;
            let raws = self
                .database
                .primary_key_scan_raw(&storage_table, &prefix)
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for raw in raws {
                versions.push(self.decode_history_owned_record(table, &storage_table, raw)?);
            }
        }
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, &aliases).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) async fn query_local_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_layer_winner_from_pk(table, row_uuid, layer)
            .await
    }

    pub(super) async fn query_local_layer_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_layer_winner_from_pk_in_branch(table, branch_key, row_uuid, layer)
            .await
    }

    /// Return the newest locally known version for a row/layer except one
    /// candidate transaction.  Authority finalization persists its candidate
    /// before assigning fate, so policy classification must be able to find
    /// the next older pending or accepted version rather than falling straight
    /// through to global current state.
    pub(super) async fn query_local_layer_winner_in_branch_excluding_tx(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
        excluded_tx_id: TxId,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None;
        for candidate in self
            .query_row_versions_in_branch(table, branch_key, row_uuid)
            .await?
        {
            if candidate.layer() != layer || self.version_tx_id(&candidate)? == excluded_tx_id {
                continue;
            }
            let candidate_tx = self.version_tx_id(&candidate)?;
            let replace = match winner.as_ref() {
                None => true,
                Some(current) => {
                    let current_tx = self.version_tx_id(current)?;
                    candidate.tx_time().sort_key(candidate_tx.node)
                        > current.tx_time().sort_key(current_tx.node)
                }
            };
            if replace {
                winner = Some(candidate);
            }
        }
        Ok(winner)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) async fn query_global_layer_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = if self
            .table_in_schema(table, self.catalogue.current_write_schema.schema)
            .is_ok()
        {
            self.catalogue.current_write_schema.schema
        } else {
            self.table_in_schema(table, self.catalogue.current_schema_version_id)?;
            self.catalogue.current_schema_version_id
        };
        self.query_global_layer_winner_in_schema(schema_version, table, row_uuid, layer)
            .await
    }

    pub(super) async fn query_global_layer_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            layer,
            PhysicalCurrentClass::Global,
        )?;
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?;
        let Some(raw) = raw else { return Ok(None) };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_branch(
            schema_version,
            table,
            branch_key,
            row_uuid,
            layer,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_current_layer_winner_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        if let Some(local) = self
            .query_local_layer_winner_in_branch(table, branch_key, row_uuid, layer)
            .await?
        {
            return Ok(Some(local));
        }
        self.query_global_layer_winner_in_branch(table, branch_key, row_uuid, layer)
            .await
    }

    /// Read the global winner through a specified schema's physical lineage.
    /// Incoming historical versions retain their authored table literal, which
    /// need not exist in the currently selected write/read schema after a
    /// rename.
    pub(super) async fn query_global_layer_winner_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_global_layer_winner_in_schema_and_branch(
            schema_version,
            table,
            &BranchKey::default(),
            row_uuid,
            layer,
        )
        .await
    }

    pub(super) async fn query_global_layer_winner_in_schema_and_branch(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let current_table = self.physical_current_table_for_schema(
            schema_version,
            table,
            layer,
            PhysicalCurrentClass::Global,
        )?;
        let raw = self
            .database
            .primary_key_get_raw(
                &current_table,
                &[
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ],
            )
            .await?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record = raw.record();
        let tx_time = TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
        let tx_node_alias =
            NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
        self.query_version_by_alias_in_branch(
            schema_version,
            table,
            branch_key,
            row_uuid,
            layer,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_layer_winner_from_pk(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        self.query_layer_winner_from_pk_in_branch(table, &BranchKey::default(), row_uuid, layer)
            .await
    }

    pub(super) async fn query_layer_winner_from_pk_in_branch(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<VersionRow>, Error> {
        let mut winner = None;
        for storage_table in self.version_storage_sources_for_layer(table, layer)? {
            let prefix = if layer == VersionLayer::Deletion {
                self.deletion_storage_prefix_in_branch(table, branch_key, Some(row_uuid))?
            } else {
                vec![
                    Value::Bytes(branch_key.canonical_bytes()),
                    Value::Uuid(row_uuid.0),
                ]
            };
            let Some(raw) = self
                .database
                .primary_key_last_raw(&storage_table, &prefix)
                .await?
                .map(|raw| raw.owned_record())
            else {
                continue;
            };
            let candidate = self.decode_history_owned_record(table, &storage_table, raw)?;
            let candidate_tx = self.version_tx_id(&candidate)?;
            if winner.as_ref().is_none_or(|existing: &VersionRow| {
                candidate.tx_time().sort_key(candidate_tx.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            }) {
                winner = Some(candidate);
            }
        }
        Ok(winner)
    }

    #[cfg(test)]
    pub(super) async fn query_all_versions(&mut self) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for table in self.catalogue.schema.tables.clone() {
            versions.extend(self.query_table_versions(&table.name).await?);
        }
        versions.sort_by_key(|version| {
            (
                version.table,
                version.row_uuid(),
                self.version_tx_id(version).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_table_versions(
        &mut self,
        table: &str,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions_by_key = BTreeMap::new();
        for storage_table in self.version_storage_sources_for_layer(table, VersionLayer::Content)? {
            let raws = self
                .database
                .primary_key_scan_raw(&storage_table, &[])
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for record in raws {
                let version = self.decode_history_owned_record(table, &storage_table, record)?;
                let tx_id = self.version_tx_id(&version)?;
                versions_by_key.insert(
                    (
                        version.branch_key().clone(),
                        version.row_uuid(),
                        tx_id,
                        version.layer(),
                    ),
                    version,
                );
            }
        }
        for storage_table in
            self.version_storage_sources_for_layer(table, VersionLayer::Deletion)?
        {
            let schema_version = if self
                .table_in_schema(table, self.catalogue.current_write_schema.schema)
                .is_ok()
            {
                self.catalogue.current_write_schema.schema
            } else {
                self.catalogue.current_schema_version_id
            };
            let requested_table_id = self.physical_table_id_for_schema(schema_version, table)?;
            let raws = self
                .database
                .primary_key_scan_raw(&storage_table, &[])
                .await?
                .into_iter()
                .map(|raw| raw.owned_record())
                .collect::<Vec<_>>();
            for record in raws {
                let version = self.decode_history_owned_record("", &storage_table, record)?;
                if self.physical_table_id_for_version(&version)? != requested_table_id {
                    continue;
                }
                let tx_id = self.version_tx_id(&version)?;
                versions_by_key.insert(
                    (
                        version.branch_key().clone(),
                        version.row_uuid(),
                        tx_id,
                        version.layer(),
                    ),
                    version,
                );
            }
        }
        let mut versions = versions_by_key.into_values().collect::<Vec<_>>();
        let aliases = self.node_aliases.clone();
        versions.sort_by_key(|version| {
            (
                version.row_uuid(),
                version_tx_id_from_aliases(version, &aliases).expect("valid version tx id"),
                version.layer(),
            )
        });
        Ok(versions)
    }

    pub(super) async fn query_versions_for_tx(
        &mut self,
        tx_id: TxId,
    ) -> Result<Vec<VersionRow>, Error> {
        #[cfg(test)]
        record_query_versions_for_tx_call();

        let Some(tx) = self.query_transaction(tx_id).await? else {
            return Ok(Vec::new());
        };
        if let Some(mut versions) = self.cached_tx_versions(tx_id) {
            versions.sort_by(|left, right| {
                left.table()
                    .cmp(right.table())
                    .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                    .then_with(|| left.layer().cmp(&right.layer()))
            });
            return Ok(versions);
        }
        let cached_tables = self.cached_tx_version_tables(tx_id);
        let tables = cached_tables
            .clone()
            .unwrap_or_else(|| self.tx_version_scan_tables());
        let mut versions = Vec::new();
        let mut scanned_sources = BTreeSet::new();
        for table in tables {
            let sources = self.version_storage_sources(&table)?;
            for storage_table in sources {
                if !scanned_sources.insert(storage_table.clone()) {
                    continue;
                }
                let raws = self
                    .database
                    .index_scan_raw(
                        &storage_table,
                        "by_tx",
                        &[Value::U64(tx_id.time.0), Value::U64(tx.node_alias.0)],
                    )
                    .await?
                    .into_iter()
                    .map(|raw| raw.owned_record())
                    .collect::<Vec<_>>();
                for record in raws {
                    // The shared deletion index is transaction-scoped, not
                    // table-scoped: one transaction may contain deletion rows
                    // for several physical tables. Decode each row from its
                    // embedded PhysicalTableId instead of imposing whichever
                    // logical table happened to visit the shared source first.
                    let requested_table = if storage_table == SHARED_DELETION_HISTORY_TABLE {
                        ""
                    } else {
                        &table
                    };
                    let version =
                        self.decode_history_owned_record(requested_table, &storage_table, record)?;
                    versions.push(version);
                }
            }
        }
        if cached_tables.is_none() {
            self.cache_tx_version_tables(
                tx_id,
                versions
                    .iter()
                    .map(|version| version.table().to_owned())
                    .collect(),
            );
        }
        versions.sort_by(|left, right| {
            left.table()
                .cmp(right.table())
                .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                .then_with(|| left.layer().cmp(&right.layer()))
        });
        Ok(versions)
    }

    pub(super) async fn query_versions_for_tx_physical_coordinate(
        &mut self,
        tx_id: TxId,
        physical_table_id: PhysicalTableId,
        row_uuid: RowUuid,
    ) -> Result<Vec<VersionRow>, Error> {
        if let Some(cached) = self.query.tx_versions_cache.get(&tx_id) {
            let aliases = self
                .catalogue
                .physical_mappings
                .iter()
                .flat_map(|(schema_version, mapping)| {
                    let schema_alias = self.catalogue.schema_version_aliases.get(schema_version);
                    mapping.tables.iter().filter_map(move |(table, mapping)| {
                        (mapping.table_id == physical_table_id)
                            .then(|| schema_alias.copied().map(|alias| (alias, table.clone())))
                            .flatten()
                    })
                })
                .collect::<BTreeSet<_>>();
            let mut versions = Vec::new();
            for (schema_alias, table) in aliases {
                versions.extend(cached.versions_for_schema_table_row(
                    schema_alias,
                    &table,
                    row_uuid,
                ));
            }
            return Ok(versions);
        }
        let versions = self.query_versions_for_tx(tx_id).await?;
        let mut matching = Vec::new();
        for version in versions {
            if version.row_uuid() == row_uuid
                && self.physical_table_id_for_version(&version)? == physical_table_id
            {
                #[cfg(test)]
                record_parent_version_lookup_materialized_rows(1);
                matching.push(version);
            }
        }
        Ok(matching)
    }

    pub(super) async fn query_versions_for_tx_rows_by_alias(
        &mut self,
        tx_id: TxId,
        tx_node_alias: NodeAlias,
        rows: &BTreeSet<(String, RowUuid)>,
    ) -> Result<Vec<VersionRow>, Error> {
        let mut versions = Vec::new();
        for (table, row_uuid) in rows {
            for layer in [VersionLayer::Content, VersionLayer::Deletion] {
                if let Some(version) = self
                    .query_version_by_alias(table, *row_uuid, layer, tx_id.time, tx_node_alias)
                    .await?
                {
                    versions.push(version);
                }
            }
        }
        versions.sort_by(|left, right| {
            left.table()
                .cmp(right.table())
                .then_with(|| left.row_uuid().cmp(&right.row_uuid()))
                .then_with(|| left.layer().cmp(&right.layer()))
        });
        Ok(versions)
    }

    pub(super) fn tx_version_scan_tables(&self) -> BTreeSet<String> {
        self.catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.keys().cloned())
            .collect()
    }

    pub(super) fn version_storage_sources(&mut self, table: &str) -> Result<Vec<String>, Error> {
        let mut sources = Vec::new();
        sources.extend(self.version_storage_sources_for_layer(table, VersionLayer::Content)?);
        sources.extend(self.version_storage_sources_for_layer(table, VersionLayer::Deletion)?);
        Ok(sources)
    }

    pub(super) fn version_storage_sources_for_layer(
        &mut self,
        table: &str,
        layer: VersionLayer,
    ) -> Result<Vec<String>, Error> {
        let cache_key = (table.to_owned(), layer);
        if let Some(sources) = self.query.version_storage_sources_cache.get(&cache_key) {
            return Ok(sources.clone());
        }
        let mut sources = self.physical_version_storage_sources(table, layer);
        sources.sort();
        sources.dedup();
        if sources.is_empty() {
            return Err(Error::TableNotFound(table.to_owned()));
        }
        self.query
            .version_storage_sources_cache
            .insert(cache_key, sources.clone());
        Ok(sources)
    }

    fn physical_version_storage_sources(&self, table: &str, layer: VersionLayer) -> Vec<String> {
        self.catalogue
            .physical_mappings
            .values()
            .filter_map(|mapping| mapping.tables.get(table))
            .map(|mapping| match layer {
                VersionLayer::Content => physical_history_table_name(mapping.table_id),
                VersionLayer::Deletion => SHARED_DELETION_HISTORY_TABLE.to_owned(),
            })
            .collect()
    }

    pub(super) fn deletion_storage_prefix_in_branch(
        &self,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: Option<RowUuid>,
    ) -> Result<Vec<Value>, Error> {
        let schema_version = if self
            .table_in_schema(table, self.catalogue.current_write_schema.schema)
            .is_ok()
        {
            self.catalogue.current_write_schema.schema
        } else {
            self.catalogue.current_schema_version_id
        };
        self.deletion_storage_prefix_in_schema_and_branch(
            schema_version,
            table,
            branch_key,
            row_uuid,
        )
    }

    #[allow(dead_code)]
    pub(super) fn deletion_storage_prefix_in_schema(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: Option<RowUuid>,
    ) -> Result<Vec<Value>, Error> {
        self.deletion_storage_prefix_in_schema_and_branch(
            schema_version,
            table,
            &BranchKey::default(),
            row_uuid,
        )
    }

    fn deletion_storage_prefix_in_schema_and_branch(
        &self,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: Option<RowUuid>,
    ) -> Result<Vec<Value>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        let mut prefix = vec![
            Value::Bytes(branch_key.canonical_bytes()),
            Value::U64(table_id.0),
        ];
        if let Some(row_uuid) = row_uuid {
            prefix.push(Value::Uuid(row_uuid.0));
        }
        Ok(prefix)
    }

    #[allow(dead_code)] // Stage 1 read primitive; production reads switch in Stage 2.
    pub(super) fn decode_history_record(
        &mut self,
        table: &str,
        record: BorrowedRecord<'_>,
    ) -> Result<VersionRow, Error> {
        self.decode_history_owned_record(
            table,
            "",
            OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
        )
    }

    pub(super) fn decode_history_owned_record(
        &mut self,
        requested_table: &str,
        storage_table: &str,
        record: OwnedRecord,
    ) -> Result<VersionRow, Error> {
        if storage_table == SHARED_DELETION_HISTORY_TABLE {
            let shared = record.to_values()?;
            let Value::U64(table_id) = shared.get(1).ok_or(Error::InvalidStoredValue(
                "shared deletion physical table id missing",
            ))?
            else {
                return Err(Error::InvalidStoredValue(
                    "shared deletion physical table id must be u64",
                ));
            };
            let Value::U64(alias) = shared.get(5).ok_or(Error::InvalidStoredValue(
                "shared deletion schema alias missing",
            ))?
            else {
                return Err(Error::InvalidStoredValue(
                    "shared deletion schema alias must be u64",
                ));
            };
            let table_id = PhysicalTableId(*table_id);
            let alias = SchemaVersionAlias(*alias);
            let schema_version =
                self.schema_version_for_alias(alias)
                    .ok_or(Error::InvalidStoredValue(
                        "shared deletion schema version alias must exist",
                    ))?;
            let stored_table = self
                .catalogue
                .physical_mappings
                .get(&schema_version)
                .and_then(|mapping| {
                    mapping.tables.iter().find_map(|(logical_table, mapping)| {
                        (mapping.table_id == table_id).then(|| logical_table.clone())
                    })
                })
                .ok_or(Error::InvalidStoredValue(
                    "shared deletion physical table mapping missing",
                ))?;
            let table = if requested_table.is_empty() || requested_table == stored_table {
                stored_table.clone()
            } else {
                let requested_schema = if self
                    .table_in_schema(requested_table, self.catalogue.current_write_schema.schema)
                    .is_ok()
                {
                    self.catalogue.current_write_schema.schema
                } else {
                    self.catalogue.current_schema_version_id
                };
                (self.physical_table_id_for_schema(requested_schema, requested_table)? == table_id)
                    .then(|| requested_table.to_owned())
                    .ok_or(Error::InvalidStoredValue(
                        "shared deletion row escaped requested physical-table prefix",
                    ))?
            };
            let logical_table = self.table_in_schema(&stored_table, schema_version)?;
            let descriptor = logical_table.register_storage_table().record_schema();
            let branch_key = RuntimeSchema::decode_persisted_branch_key(
                &logical_table,
                record
                    .borrowed()
                    .get_bytes(SharedDeletionHistoryRowRecord::FIELD_BRANCH_KEY_IDX)?,
            )
            .map_err(|_| Error::InvalidStoredValue("invalid stored branch key"))?;
            let logical_values = std::iter::once(shared[0].clone())
                .chain(shared[2..].iter().cloned())
                .collect::<Vec<_>>();
            let logical = OwnedRecord::new(descriptor.create(&logical_values)?, descriptor);
            let version = VersionRow {
                table: groove::Intern::new(table),
                branch_key,
                record: logical,
            };
            version.validate_canonical()?;
            return Ok(version);
        }
        let record_view = record.borrowed();
        let is_deletion = record_view.descriptor().field_index("_deletion").is_some();
        let schema_alias = SchemaVersionAlias(record_view.get_u64(if is_deletion {
            RegisterRowRecord::FIELD_SCHEMA_VERSION_IDX
        } else {
            HistoryRowRecord::FIELD_SCHEMA_VERSION_IDX
        })?);
        let schema_version =
            self.schema_version_for_alias(schema_alias)
                .ok_or(Error::InvalidStoredValue(
                    "version storage schema version alias must exist",
                ))?;
        let table = if !storage_table.starts_with("jazz_physical_") {
            requested_table.to_owned()
        } else {
            self.catalogue
                .physical_mappings
                .get(&schema_version)
                .and_then(|mapping| {
                    mapping.tables.iter().find_map(|(logical_table, mapping)| {
                        let root = if is_deletion {
                            physical_register_table_name(mapping.table_id)
                        } else {
                            physical_history_table_name(mapping.table_id)
                        };
                        (root == storage_table).then(|| logical_table.clone())
                    })
                })
                .ok_or(Error::InvalidStoredValue(
                    "physical version storage logical table mapping missing",
                ))?
        };
        let table_schema = self.table_in_schema(&table, schema_version)?;
        let record = record.borrowed();
        let tx_node_alias = if is_deletion {
            NodeAlias(record.get_u64(RegisterRowRecord::FIELD_TX_NODE_ID_IDX)?)
        } else {
            NodeAlias(record.get_u64(HistoryRowRecord::FIELD_TX_NODE_ID_IDX)?)
        };
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "history tx node alias must exist",
            ))?;
        let tx_time = if is_deletion {
            TxTime(record.get_u64(RegisterRowRecord::FIELD_TX_TIME_IDX)?)
        } else {
            TxTime(record.get_u64(HistoryRowRecord::FIELD_TX_TIME_IDX)?)
        };
        let _ = TxId::new(tx_time, tx_node);
        let version = VersionRow {
            table: groove::Intern::new(table),
            branch_key: RuntimeSchema::decode_persisted_branch_key(
                &table_schema,
                record.get_bytes(if is_deletion {
                    RegisterRowRecord::FIELD_BRANCH_KEY_IDX
                } else {
                    HistoryRowRecord::FIELD_BRANCH_KEY_IDX
                })?,
            )
            .map_err(|_| Error::InvalidStoredValue("invalid stored branch key"))?,
            record: OwnedRecord::new(record.raw().to_vec(), record.descriptor()),
        };
        version.validate_canonical()?;
        Ok(version)
    }

    pub(super) async fn query_transaction(
        &mut self,
        tx_id: TxId,
    ) -> Result<Option<StoredTransaction>, Error> {
        if let Some(alias) = self.node_aliases.get(&tx_id.node).copied()
            && let Some(tx) = self.query_transaction_by_alias(tx_id, alias).await?
        {
            return Ok(Some(tx));
        }
        let mut aliases = Vec::new();
        for raw in self
            .database
            .primary_key_scan_raw("jazz_nodes", &[])
            .await?
        {
            let record = raw.record();
            if record.get_uuid(NodeAliasRowRecord::FIELD_UUID_IDX)? == tx_id.node.0 {
                let alias = NodeAlias(record.get_u64(NodeAliasRowRecord::FIELD_ID_IDX)?);
                aliases.push(alias);
            }
        }
        for expected_alias in aliases {
            if let Some(tx) = self
                .query_transaction_by_alias(tx_id, expected_alias)
                .await?
            {
                self.node_aliases.insert(tx_id.node, expected_alias);
                return Ok(Some(tx));
            }
        }
        Ok(None)
    }

    pub(super) async fn preload_transaction_memo(
        &mut self,
        tx_ids: impl IntoIterator<Item = TxId>,
        context: &mut super::policy::ViewEvaluationContext,
    ) -> Result<(), Error> {
        let mut by_alias = BTreeMap::<(NodeUuid, NodeAlias), BTreeSet<TxTime>>::new();
        for tx_id in tx_ids {
            if context.tx_rows.contains_key(&tx_id) {
                continue;
            }
            if let Some(alias) = self.node_aliases.get(&tx_id.node).copied() {
                by_alias
                    .entry((tx_id.node, alias))
                    .or_default()
                    .insert(tx_id.time);
            } else {
                let tx = self.query_transaction(tx_id).await?;
                context.tx_rows.insert(tx_id, tx);
            }
        }

        for ((node, alias), times) in by_alias {
            if times.len() == 1 {
                let time = *times.iter().next().expect("non-empty time set");
                let tx_id = TxId::new(time, node);
                let tx = self.query_transaction_by_alias(tx_id, alias).await?;
                context.tx_rows.insert(tx_id, tx);
                continue;
            }

            let min_time = times.iter().next().expect("non-empty time set");
            let max_time = times.iter().next_back().expect("non-empty time set");
            let Some(end_time) = max_time.0.checked_add(1) else {
                for time in times {
                    let tx_id = TxId::new(time, node);
                    let tx = self.query_transaction_by_alias(tx_id, alias).await?;
                    context.tx_rows.insert(tx_id, tx);
                }
                continue;
            };

            for time in &times {
                context.tx_rows.insert(TxId::new(*time, node), None);
            }
            for raw in self
                .database
                .primary_key_scan_range_raw(
                    "jazz_transactions",
                    &[Value::U64(min_time.0), Value::U64(0)],
                    &[Value::U64(end_time), Value::U64(0)],
                )
                .await?
            {
                let record = raw.record();
                let row_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
                let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
                if row_alias != alias || !times.contains(&time) {
                    continue;
                }
                let tx_id = TxId::new(time, node);
                let tx = self.stored_transaction_from_record(tx_id, alias, record)?;
                context.tx_rows.insert(tx_id, Some(tx));
            }
        }
        Ok(())
    }

    async fn query_transaction_by_alias(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
    ) -> Result<Option<StoredTransaction>, Error> {
        let Some(raw) = self
            .database
            .primary_key_get_raw(
                "jazz_transactions",
                &[Value::U64(tx_id.time.0), Value::U64(expected_alias.0)],
            )
            .await?
        else {
            return Ok(None);
        };
        let record = raw.record();
        let node_alias = NodeAlias(record.get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)?);
        let time = TxTime(record.get_u64(TransactionRowRecord::FIELD_TIME_IDX)?);
        if node_alias != expected_alias || time != tx_id.time {
            return Ok(None);
        }
        self.stored_transaction_from_record(tx_id, expected_alias, record)
            .map(Some)
    }

    fn stored_transaction_from_record(
        &self,
        tx_id: TxId,
        expected_alias: NodeAlias,
        record: BorrowedRecord<'_>,
    ) -> Result<StoredTransaction, Error> {
        let tx = Transaction {
            tx_id,
            kind: tx_kind_from_discriminant(
                record.get_enum(TransactionRowRecord::FIELD_KIND_IDX)?,
            )?,
            n_total_writes: record.get_u32(TransactionRowRecord::FIELD_N_TOTAL_WRITES_IDX)?,
            made_by: AuthorSubject::from_canonical(
                record.get_str(TransactionRowRecord::FIELD_MADE_BY_IDX)?,
            )
            .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
            permission_subject: record
                .get_nullable_string(TransactionRowRecord::FIELD_PERMISSION_SUBJECT_IDX)?
                .map(AuthorSubject::from_canonical)
                .transpose()
                .map_err(|_| groove::records::Error::NonCanonicalRecord)?,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: record
                .get_nullable_string(TransactionRowRecord::FIELD_USER_METADATA_IDX)?
                .map(str::to_owned),
            contribution_merge: <Option<OwnedRecord> as records::RecordField>::read(
                &record,
                TransactionRowRecord::FIELD_CONTRIBUTION_MERGE_IDX,
            )?
            .map(|record| self.contribution_merge_from_storage_record(record))
            .transpose()?,
        };
        // Recovery and ordinary durable reads must fail closed on malformed
        // strategy-defined contribution identities before they can influence
        // a later merge calculation.
        self.validate_contribution_merge_operation_identities(&tx)?;
        let fate = fate_from_encoded_fields(record)?;
        Ok(StoredTransaction {
            tx,
            node_alias: expected_alias,
            fate,
            global_time: record
                .get_nullable_u64(TransactionRowRecord::FIELD_GLOBAL_TIME_IDX)?
                .map(GlobalTime),
            durability: durability_from_discriminant(
                record.get_enum(TransactionRowRecord::FIELD_DURABILITY_IDX)?,
            )?,
            view_scoped_cardinality: record
                .get_nullable_string(TransactionRowRecord::FIELD_MERGE_STRATEGY_IDX)?
                .is_some_and(|value| value == "view-scoped-cardinality"),
        })
    }

    pub(super) async fn transaction_exists(&self, tx_id: TxId) -> Result<bool, Error> {
        let Some(expected_alias) = self.node_aliases.get(&tx_id.node).copied() else {
            return Ok(false);
        };
        Ok(self
            .database
            .primary_key_get_raw(
                "jazz_transactions",
                &[Value::U64(tx_id.time.0), Value::U64(expected_alias.0)],
            )
            .await?
            .is_some())
    }

    pub(super) async fn transaction_exists_memo(
        &mut self,
        tx_id: TxId,
        memo: &mut IngestMemo,
    ) -> Result<bool, Error> {
        if let Some(exists) = memo.tx_exists.get(&tx_id) {
            return Ok(*exists);
        }
        let exists = self.transaction_exists(tx_id).await?;
        memo.tx_exists.insert(tx_id, exists);
        Ok(exists)
    }

    pub(super) async fn transaction_made_at(&self, tx_id: TxId) -> Result<Option<TxTime>, Error> {
        if !self.node_aliases.contains_key(&tx_id.node) {
            return Ok(None);
        }
        if self.transaction_exists(tx_id).await? {
            return Ok(Some(tx_id.time));
        }
        Ok(None)
    }

    pub(super) async fn transaction_made_at_memo(
        &mut self,
        tx_id: TxId,
        memo: &mut IngestMemo,
    ) -> Result<Option<TxTime>, Error> {
        if let Some(made_at) = memo.tx_made_at.get(&tx_id) {
            return Ok(*made_at);
        }
        let made_at = self.transaction_made_at(tx_id).await?;
        memo.tx_made_at.insert(tx_id, made_at);
        if made_at.is_some() {
            memo.tx_exists.insert(tx_id, true);
        }
        Ok(made_at)
    }

    pub(super) async fn query_version_by_alias(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        for storage_table in self.version_storage_sources_for_layer(table, layer)? {
            if let Some(version) = self
                .query_version_by_alias_with_storage(
                    table,
                    &storage_table,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                )
                .await?
            {
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    /// Resolve an exact historical witness through the schema that authored
    /// its table literal. Shared deletion history is keyed by physical table,
    /// so an old `todos` witness must not construct its prefix via renamed
    /// current `tasks` schema state.
    pub(super) async fn query_version_by_alias_in_branch(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let storage_table = match layer {
            VersionLayer::Content => physical_history_table_name(
                self.physical_table_id_for_schema(schema_version, table)?,
            ),
            VersionLayer::Deletion => SHARED_DELETION_HISTORY_TABLE.to_owned(),
        };
        self.query_version_by_alias_with_storage_in_schema(
            schema_version,
            table,
            &storage_table,
            branch_key,
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_version_by_alias_with_storage(
        &mut self,
        table: &str,
        storage_table: &str,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let schema_version = if self
            .table_in_schema(table, self.catalogue.current_write_schema.schema)
            .is_ok()
        {
            self.catalogue.current_write_schema.schema
        } else {
            self.catalogue.current_schema_version_id
        };
        self.query_version_by_alias_with_storage_in_schema(
            schema_version,
            table,
            storage_table,
            &BranchKey::default(),
            row_uuid,
            tx_time,
            tx_node_alias,
        )
        .await
    }

    pub(super) async fn query_version_by_alias_with_storage_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        storage_table: &str,
        branch_key: &BranchKey,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) -> Result<Option<VersionRow>, Error> {
        let key = if storage_table == SHARED_DELETION_HISTORY_TABLE {
            let mut key = self.deletion_storage_prefix_in_schema_and_branch(
                schema_version,
                table,
                branch_key,
                Some(row_uuid),
            )?;
            key.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
            key
        } else {
            vec![
                Value::Bytes(branch_key.canonical_bytes()),
                Value::Uuid(row_uuid.0),
                Value::U64(tx_time.0),
                Value::U64(tx_node_alias.0),
            ]
        };
        let raw = self
            .database
            .primary_key_get_raw(storage_table, &key)
            .await?
            .map(|raw| raw.owned_record());
        let Some(record) = raw else {
            return Ok(None);
        };
        // The lookup prefix is selected from the authored schema. For the
        // shared deletion table, decode the record under its stored schema as
        // well: the current winner may be a later `tasks` deletion occupying
        // the same physical lineage as this v1 `todos` probe.
        let requested_table = if storage_table != SHARED_DELETION_HISTORY_TABLE {
            table
        } else {
            ""
        };
        self.decode_history_owned_record(requested_table, storage_table, record)
            .map(Some)
    }
}
