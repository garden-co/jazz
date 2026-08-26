//! Snapshot projection used by query-engine source lowering.
//!
//! Live current relations lower to Groove graph sources in `query_eval`; this
//! module is restricted to frozen historical, transaction-overlay, and other
//! explicitly snapshot-valued inputs. It must not launch an ordinary Jazz
//! query to materialize another query's source.

use super::*;
use crate::node::query_engine::BranchViewSourceBase;
use crate::protocol::{BranchViewBase, SnapshotRef};

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Return every persisted spelling of a logical branch key that can occur
    /// across the table's monotone schema history. Older spellings omit later
    /// branch columns and are equivalent only when the selector supplies those
    /// columns' immutable defaults.
    pub(super) fn equivalent_stored_branch_keys(
        &self,
        table: &str,
        read_schema_version: SchemaVersionId,
        selected: &BranchKey,
    ) -> Result<BTreeSet<BranchKey>, Error> {
        let table_id = self.physical_table_id_for_schema(read_schema_version, table)?;
        let read_schema = self
            .catalogue
            .catalogue_schemas
            .get(&read_schema_version)
            .ok_or(Error::InvalidStoredValue("read schema is missing"))?;
        let read_table = read_schema
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or(Error::TableNotFound(table.to_owned()))?;
        let read_mapping = self
            .catalogue
            .physical_mappings
            .get(&read_schema_version)
            .ok_or(Error::InvalidStoredValue(
                "read schema physical mapping is missing",
            ))?;
        let read_table_mapping = read_mapping
            .tables
            .get(table)
            .ok_or(Error::TableNotFound(table.to_owned()))?;
        let selected_by_name = selected.values.iter().cloned().collect::<BTreeMap<_, _>>();
        let selected_by_physical = read_table
            .branch_by
            .iter()
            .map(|column_name| {
                let physical = read_table_mapping.columns.get(column_name).ok_or(
                    Error::InvalidStoredValue("branch column physical mapping is missing"),
                )?;
                let value = selected_by_name.get(column_name).cloned().ok_or_else(|| {
                    Error::InvalidBranchKey(format!("selected branch key missing {column_name}"))
                })?;
                Ok((*physical, value))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let mut keys = BTreeSet::new();
        for (schema_version, catalogue_schema) in &self.catalogue.catalogue_schemas {
            let Some(mapping) = self.catalogue.physical_mappings.get(schema_version) else {
                continue;
            };
            let Some((logical_table, table_mapping)) = mapping
                .tables
                .iter()
                .find(|(_, mapping)| mapping.table_id == table_id)
            else {
                continue;
            };
            let Some(table) = catalogue_schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
            else {
                continue;
            };
            let historical_physical_columns = table
                .branch_by
                .iter()
                .filter_map(|name| table_mapping.columns.get(name))
                .copied()
                .collect::<BTreeSet<_>>();
            let missing_branch_value_is_non_default =
                read_table.branch_by.iter().any(|column_name| {
                    let Some(physical) = read_table_mapping.columns.get(column_name) else {
                        return true;
                    };
                    if historical_physical_columns.contains(physical) {
                        return false;
                    }
                    let default = read_table
                        .columns
                        .iter()
                        .find(|column| column.name == *column_name)
                        .and_then(|column| column.default.clone())
                        .map(crate::protocol::BranchColumnValue::from);
                    selected_by_physical.get(physical) != default.as_ref()
                });
            if missing_branch_value_is_non_default {
                continue;
            }
            let mut values =
                table
                    .branch_by
                    .iter()
                    .map(|column_name| {
                        let physical = table_mapping.columns.get(column_name).ok_or(
                            Error::InvalidStoredValue(
                                "historical branch column physical mapping is missing",
                            ),
                        )?;
                        selected_by_physical
                            .get(physical)
                            .cloned()
                            .map(|value| (column_name.clone(), value))
                            .ok_or(Error::InvalidStoredValue(
                                "selected branch key missing historical table branch column",
                            ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            values.sort_by(|left, right| left.0.cmp(&right.0));
            keys.insert(BranchKey { values });
        }
        if keys.is_empty() {
            keys.insert(BranchKey {
                values: selected_by_name.into_iter().collect(),
            });
        }
        Ok(keys)
    }

    /// Resolve one row through a live head-over-base branch view for mutation helpers.
    pub async fn visible_current_cells_in_branch_view(
        &mut self,
        table: &str,
        head: &BranchSelector,
        base: Option<&BranchViewBase>,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let schema = self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("current write schema missing"))?
            .schema
            .clone();
        let table_schema = self.table_in_schema(table, schema_version)?;
        let (head, _) = schema
            .project_branch_view_selector(&table_schema, head)
            .map_err(Error::InvalidBranchKey)?;
        let base = base
            .map(|base| match base {
                BranchViewBase::Current(selector) => schema
                    .project_branch_view_selector(&table_schema, selector)
                    .map(|(key, _)| BranchViewSourceBase::Current(key)),
                BranchViewBase::Snapshot { branch, snapshot } => schema
                    .project_branch_view_selector(&table_schema, branch)
                    .map(|(key, _)| BranchViewSourceBase::Snapshot(key, snapshot.clone())),
            })
            .transpose()
            .map_err(Error::InvalidBranchKey)?;
        Ok(self
            .branch_view_rows_for_schema(
                table,
                schema_version,
                DurabilityTier::Local,
                &head,
                base.as_ref(),
            )
            .await?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
            .map(|row| {
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(&table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    async fn branch_winners_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
        key: &BranchKey,
        snapshot: Option<&SnapshotRef>,
    ) -> Result<(BTreeMap<RowUuid, VersionRow>, BTreeMap<RowUuid, VersionRow>), Error> {
        let mut content = BTreeMap::new();
        let mut deletions = BTreeMap::new();
        let stored_keys = self.equivalent_stored_branch_keys(table, read_schema_version, key)?;
        for stored_key in stored_keys {
            for version in self
                .query_table_versions_in_branch(table, &stored_key)
                .await?
            {
                let tx_id = self.version_tx_id(&version)?;
                let Some(tx) = self.query_transaction(tx_id).await? else {
                    continue;
                };
                let visible = if let Some(snapshot) = snapshot {
                    let snapshot = Snapshot {
                        owner: snapshot.owner,
                        global_base: snapshot.global_base,
                        local_base: snapshot.local_base,
                        dots: snapshot.dots.clone(),
                    };
                    self.snapshot_covers(tx_id, &snapshot).await
                } else {
                    match tier {
                        DurabilityTier::Global => {
                            matches!(tx.fate, Fate::Accepted)
                                && tx.durability >= DurabilityTier::Global
                        }
                        DurabilityTier::Edge => {
                            matches!(tx.fate, Fate::Accepted)
                                && tx.durability >= DurabilityTier::Edge
                        }
                        DurabilityTier::None | DurabilityTier::Local => {
                            !matches!(tx.fate, Fate::Rejected(_))
                        }
                    }
                };
                if !visible {
                    continue;
                }
                let target = match version.layer() {
                    VersionLayer::Content => &mut content,
                    VersionLayer::Deletion => &mut deletions,
                };
                let replace =
                    target
                        .get(&version.row_uuid())
                        .is_none_or(|existing: &VersionRow| {
                            version.tx_time().sort_key(tx_id.node)
                                > existing.tx_time().sort_key(
                                    self.version_tx_id(existing).expect("valid version tx").node,
                                )
                        });
                if replace {
                    target.insert(version.row_uuid(), version);
                }
            }
        }
        Ok((content, deletions))
    }

    pub(super) async fn branch_view_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
        head: &BranchKey,
        base: Option<&BranchViewSourceBase>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (head_content, head_deletions) = self
            .branch_winners_for_schema(table, read_schema_version, tier, head, None)
            .await?;
        let (base_content, base_deletions) = match base {
            None => (BTreeMap::new(), BTreeMap::new()),
            Some(BranchViewSourceBase::Current(key)) if key == head => {
                (BTreeMap::new(), BTreeMap::new())
            }
            Some(BranchViewSourceBase::Current(key)) => {
                self.branch_winners_for_schema(table, read_schema_version, tier, key, None)
                    .await?
            }
            Some(BranchViewSourceBase::Snapshot(key, _)) if key == head => {
                (BTreeMap::new(), BTreeMap::new())
            }
            Some(BranchViewSourceBase::Snapshot(key, snapshot)) => {
                self.branch_winners_for_schema(
                    table,
                    read_schema_version,
                    tier,
                    key,
                    Some(snapshot),
                )
                .await?
            }
        };
        self.materialize_branch_view_winners(
            table,
            read_schema_version,
            head,
            head_content,
            head_deletions,
            base_content,
            base_deletions,
        )
    }

    /// Materialize only the frozen base relation for a snapshot-backed branch
    /// view. The live head is deliberately absent: query lowering overlays its
    /// maintained content and deletion registers separately.
    pub(super) async fn branch_snapshot_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        output_branch: &BranchKey,
        base_branch: &BranchKey,
        snapshot: &SnapshotRef,
    ) -> Result<Vec<CurrentRow>, Error> {
        let (base_content, base_deletions) = self
            .branch_winners_for_schema(
                table,
                read_schema_version,
                DurabilityTier::Local,
                base_branch,
                Some(snapshot),
            )
            .await?;
        self.materialize_branch_view_winners(
            table,
            read_schema_version,
            output_branch,
            BTreeMap::new(),
            BTreeMap::new(),
            base_content,
            base_deletions,
        )
    }

    fn materialize_branch_view_winners(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        output_branch: &BranchKey,
        head_content: BTreeMap<RowUuid, VersionRow>,
        head_deletions: BTreeMap<RowUuid, VersionRow>,
        base_content: BTreeMap<RowUuid, VersionRow>,
        base_deletions: BTreeMap<RowUuid, VersionRow>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?;
        let row_ids = head_content
            .keys()
            .chain(base_content.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        let mut rows = Vec::new();
        for row_uuid in row_ids {
            let Some(version) = head_content
                .get(&row_uuid)
                .or_else(|| base_content.get(&row_uuid))
            else {
                continue;
            };
            let deletion = head_deletions
                .get(&row_uuid)
                .or_else(|| base_deletions.get(&row_uuid));
            if deletion.is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted)) {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, version)?;
            let Some(projected_table) = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?
            else {
                continue;
            };
            if projected_table != table {
                continue;
            }
            for column_name in &read_table.branch_by {
                let (_, encoded) = output_branch
                    .values
                    .iter()
                    .find(|(name, _)| name == column_name)
                    .ok_or_else(|| {
                        Error::InvalidBranchKey("head branch key missing table column".to_owned())
                    })?;
                cells.insert(
                    column_name.clone(),
                    encoded.decode().map_err(|_| {
                        Error::InvalidBranchKey("invalid head branch value".to_owned())
                    })?,
                );
            }
            let updated = match deletion {
                Some(deletion)
                    if self
                        .version_tx_id(deletion)?
                        .time
                        .sort_key(self.version_tx_id(deletion)?.node)
                        > self
                            .version_tx_id(version)?
                            .time
                            .sort_key(self.version_tx_id(version)?.node) =>
                {
                    deletion
                }
                _ => version,
            };
            rows.push(current_row_from_materialized_cells_with_layer_provenance(
                &read_table,
                version,
                version,
                updated,
                &cells,
            )?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    #[allow(dead_code)]
    pub(super) async fn current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        if read_schema_version == self.catalogue.current_schema_version_id {
            return self.current_rows(table, tier).await;
        }
        let read_table = self.table_in_schema(table, read_schema_version)?;
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        for version in self.query_table_versions(table).await? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
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
            if deletions
                .get(&row_uuid)
                .is_some_and(|deletion| deletion.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(version.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(version.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &version)?;
            let Some(projected_table) = self.translate_cells(
                source_schema,
                read_schema_version,
                version.table(),
                &mut cells,
            )?
            else {
                continue;
            };
            if projected_table == table {
                match current_row_from_cells(&read_table, row_uuid, &cells) {
                    Ok(row) => rows.push(row),
                    Err(error) if is_unrepresentable_enum_projection(&error) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) async fn projected_historical_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        position: GlobalTime,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        let mut tx_ids = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
        for version in self.query_table_versions(table).await? {
            let tx_id = self.version_tx_id(&version)?;
            let Some(tx) = self.query_transaction(tx_id).await? else {
                continue;
            };
            if !matches!(tx.fate, Fate::Accepted)
                || tx.durability < DurabilityTier::Global
                || tx
                    .global_time
                    .is_none_or(|global_time| global_time > position)
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
            if deletions
                .get(&row_uuid)
                .is_some_and(|deletion| deletion.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(content.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(content.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &content)?;
            let Some(projected_table) = self.translate_cells(
                source_schema,
                read_schema_version,
                content.table(),
                &mut cells,
            )?
            else {
                continue;
            };
            if projected_table == table {
                match current_row_from_cells(&read_table, row_uuid, &cells) {
                    Ok(row) => rows.push(row),
                    Err(error) if is_unrepresentable_enum_projection(&error) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) async fn projected_snapshot_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        snapshot: &Snapshot,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        let mut tx_ids = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
        for version in self.query_table_versions(table).await? {
            let tx_id = self.version_tx_id(&version)?;
            if !self.snapshot_covers(tx_id, snapshot).await {
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
            if deletions
                .get(&row_uuid)
                .is_some_and(|deletion| deletion.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let source_schema = self
                .schema_version_for_alias(content.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "history schema version alias must exist",
                ))?;
            let source_table = self.table_in_schema(content.table(), source_schema)?;
            let mut cells = self.materialized_cells_for_version(&source_table, &content)?;
            let Some(projected_table) = self.translate_cells(
                source_schema,
                read_schema_version,
                content.table(),
                &mut cells,
            )?
            else {
                continue;
            };
            if projected_table == table {
                match current_row_from_cells(&read_table, row_uuid, &cells) {
                    Ok(row) => rows.push(row),
                    Err(error) if is_unrepresentable_enum_projection(&error) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) fn translate_cells(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
        cells: &mut BTreeMap<String, Value>,
    ) -> Result<Option<String>, Error> {
        if source == target {
            return Ok(Some(table.to_owned()));
        }
        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let forward_table = apply_compiled_lens_path(&path, cells);
            return Ok(Some(forward_table));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let reverse_table = apply_compiled_lens_path(&path, cells);
            return Ok(Some(reverse_table));
        }
        Ok(None)
    }
}
