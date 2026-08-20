//! Source row fabrication used by query-engine source resolution.
//!
//! These helpers are the remaining compatibility bridge for schema/lens
//! projected sources. Query lowering still sees an explicit source graph; this
//! module owns the temporary row materialization behind those graph leaves.

use super::*;
use crate::node::query_engine::BranchViewSourceBase;
use crate::protocol::{BranchViewBase, SnapshotRef};

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Resolve one row through a live head-over-base branch view for mutation helpers.
    pub fn visible_current_cells_in_branch_view(
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
            )?
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

    pub(super) fn branch_view_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
        head: &BranchKey,
        base: Option<&BranchViewSourceBase>,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?;
        let mut winners = |key: &BranchKey,
                           snapshot: Option<&SnapshotRef>|
         -> Result<
            (BTreeMap<RowUuid, VersionRow>, BTreeMap<RowUuid, VersionRow>),
            Error,
        > {
            let mut content = BTreeMap::new();
            let mut deletions = BTreeMap::new();
            for version in self.query_table_versions_in_branch(table, key)? {
                let tx_id = self.version_tx_id(&version)?;
                let Some(tx) = self.query_transaction(tx_id)? else {
                    continue;
                };
                let visible = if let Some(snapshot) = snapshot {
                    let snapshot = Snapshot {
                        owner: snapshot.owner,
                        global_base: snapshot.global_base,
                        local_base: snapshot.local_base,
                        dots: snapshot.dots.clone(),
                    };
                    self.snapshot_covers(tx_id, &snapshot)
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
            Ok((content, deletions))
        };
        let (head_content, head_deletions) = winners(head, None)?;
        let (base_content, base_deletions) = match base {
            None => (BTreeMap::new(), BTreeMap::new()),
            Some(BranchViewSourceBase::Current(key)) if key == head => {
                (BTreeMap::new(), BTreeMap::new())
            }
            Some(BranchViewSourceBase::Current(key)) => winners(key, None)?,
            Some(BranchViewSourceBase::Snapshot(key, _)) if key == head => {
                (BTreeMap::new(), BTreeMap::new())
            }
            Some(BranchViewSourceBase::Snapshot(key, snapshot)) => winners(key, Some(snapshot))?,
        };
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
            for binding in &read_table.branch_by {
                let (_, encoded) = head
                    .dimensions
                    .iter()
                    .find(|(dimension, _)| *dimension == binding.dimension)
                    .ok_or_else(|| {
                        Error::InvalidBranchKey(
                            "head branch key missing table dimension".to_owned(),
                        )
                    })?;
                cells.insert(
                    binding.column.clone(),
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
    pub(super) fn current_rows_for_schema(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        tier: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        if read_schema_version == self.catalogue.current_schema_version_id {
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

    pub(super) fn projected_historical_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        position: GlobalTime,
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

    pub(super) fn projected_snapshot_current_rows(
        &mut self,
        table: &str,
        read_schema_version: SchemaVersionId,
        snapshot: &Snapshot,
    ) -> Result<Vec<CurrentRow>, Error> {
        let read_table = self.table_in_schema(table, read_schema_version)?.clone();
        let mut content = BTreeMap::<RowUuid, VersionRow>::new();
        let mut deletions = BTreeMap::<RowUuid, VersionRow>::new();
        let mut tx_ids = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
        for version in self.query_table_versions(table)? {
            let tx_id = self.version_tx_id(&version)?;
            if !self.snapshot_covers(tx_id, snapshot) {
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
                let deletion = deletions.get(&row_uuid);
                let deleted = deletion
                    .is_some_and(|deletion| deletion.deletion() == Some(DeletionEvent::Deleted));
                let updated = deletion
                    .filter(|deletion| {
                        deletion.tx_time().sort_key(
                            self.version_tx_id(deletion)
                                .expect("valid deletion version tx id")
                                .node,
                        ) > version.tx_time().sort_key(
                            self.version_tx_id(&version)
                                .expect("valid content version tx id")
                                .node,
                        )
                    })
                    .unwrap_or(&version);
                match current_row_from_materialized_cells_with_layer_provenance(
                    &read_table,
                    &version,
                    &version,
                    updated,
                    &cells,
                ) {
                    Ok(row) => rows.push((row, deleted)),
                    Err(error) if is_unrepresentable_enum_projection(&error) => {}
                    Err(error) => return Err(error),
                }
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
