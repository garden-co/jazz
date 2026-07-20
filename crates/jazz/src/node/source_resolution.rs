//! Source row fabrication used by query-engine source resolution.
//!
//! These helpers are the remaining compatibility bridge for schema/lens
//! projected sources. Query lowering still sees an explicit source graph; this
//! module owns the temporary row materialization behind those graph leaves.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        if read_schema_version == self.catalogue.current_schema_version_id
            && !self.catalogue.partitions.iter().any(|(logical, version)| {
                logical == table && *version != self.catalogue.current_schema_version_id
            })
        {
            return self.current_rows(table, tier);
        }
        let read_table = self.table_in_schema(table, read_schema_version)?;
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            let visible_at_tier = match tier {
                DurabilityTier::Global => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Global
                }
                DurabilityTier::Edge => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Edge
                }
                DurabilityTier::None | DurabilityTier::Local => {
                    !matches!(tx.fate, Fate::Rejected(_))
                }
            };
            if !visible_at_tier {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let replace = target.get(&version.row_uuid()).is_none_or(|existing| {
                version.tx_time().sort_key(tx_id.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            });
            if replace {
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, version) in content {
            if deletions.get(&row_uuid).is_some_and(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > version.tx_time()
            }) {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &version)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?;
            if projected_table == table {
                rows.push(current_row_from_cells(&read_table, row_uuid, &cells)?);
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) fn projected_historical_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        position: GlobalSeq,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        let mut tx_ids = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            if !matches!(tx.fate, Fate::Accepted)
                || tx.durability < DurabilityTier::Global
                || tx.global_seq.is_none_or(|global_seq| global_seq > position)
            {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let key = (version.row_uuid(), version.layer());
            let replace = tx_ids.get(&key).is_none_or(|existing_tx_id| {
                version.tx_time().sort_key(tx_id.node)
                    > target
                        .get(&version.row_uuid())
                        .expect("tracked version exists")
                        .tx_time()
                        .sort_key(existing_tx_id.node)
            });
            if replace {
                tx_ids.insert(key, tx_id);
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, content) in content {
            if deletions.get(&row_uuid).is_some_and(|deletion| {
                deletion.deletion() == Some(DeletionEvent::Deleted)
                    && deletion.tx_time() > content.tx_time()
            }) {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(content.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(content.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &content)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                content.table(),
                &mut cells,
            )?;
            if projected_table == table {
                rows.push(current_row_from_cells(&read_table, row_uuid, &cells)?);
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) fn include_deleted_current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<(CurrentRow, bool)>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            let visible_at_tier = match tier {
                DurabilityTier::Global => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Global
                }
                DurabilityTier::Edge => {
                    matches!(tx.fate, Fate::Accepted) && tx.durability >= DurabilityTier::Edge
                }
                DurabilityTier::None | DurabilityTier::Local => {
                    !matches!(tx.fate, Fate::Rejected(_))
                }
            };
            if !visible_at_tier {
                continue;
            }
            let target = match version.layer() {
                VersionLayer::Content => &mut content,
                VersionLayer::Deletion => &mut deletions,
            };
            let replace = target.get(&version.row_uuid()).is_none_or(|existing| {
                version.tx_time().sort_key(tx_id.node)
                    > existing.tx_time().sort_key(
                        self.version_tx_id(existing)
                            .expect("valid version tx id")
                            .node,
                    )
            });
            if replace {
                target.insert(version.row_uuid(), version);
            }
        }
        let mut rows = Vec::new();
        for (row_uuid, version) in content {
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &version)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?;
            if projected_table == table {
                let deleted = deletions.get(&row_uuid).is_some_and(|deletion| {
                    deletion.deletion() == Some(DeletionEvent::Deleted)
                        && deletion.tx_time() > version.tx_time()
                });
                rows.push((
                    current_row_from_cells(&read_table, row_uuid, &cells)?,
                    deleted,
                ));
            }
        }
        rows.sort_by(|(left, _), (right, _)| {
            left.row_uuid()
                .to_bytes()
                .cmp(&right.row_uuid().to_bytes())
                .then_with(|| left.record.raw().cmp(right.record.raw()))
        });
        Ok(rows)
    }

    fn translate_cells(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<String, Error> {
        if source == target {
            return Ok(table.to_owned());
        }
        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let forward_table = apply_compiled_lens_path(&path, cells);
            return Ok(forward_table);
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let reverse_table = apply_compiled_lens_path(&path, cells);
            return Ok(reverse_table);
        }
        Err(Error::InvalidCatalogueUpdate("lens chain is unknown"))
    }
}
