//! Write-policy admission and policy-pinned row projection. Policy predicates,
//! joins, inheritance, reachability, and alternatives execute through the query
//! program in [`super::query_eval`]; this module selects the operation clause,
//! projects old/candidate data into the pinned policy schema, and fail-closes
//! write ingest. It also retains the transaction memo used by view emission.

use super::query_engine::{NormalizedRowSetShape, RowSetExpr};
use super::*;

#[derive(Default)]
pub(super) struct ViewEvaluationContext {
    pub(super) tx_rows: BTreeMap<TxId, Option<StoredTransaction>>,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn write_policy_allows_version_record(
        &mut self,
        version: &VersionRecord,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let (policy_schema_version, table, cells) =
            self.policy_projection_for_version_record(version)?;
        if version.deletion() == Some(DeletionEvent::Deleted) {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(true);
            };
            let current = match self.policy_delete_subject_row(&table, version)? {
                Some(current) => current,
                None => current_row_from_cells(&table, version.row_uuid(), &cells)?,
            };
            let current_cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    current
                        .cell(&table, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            return self.write_policy_query_allows_candidate_for_schema(
                policy_schema_version,
                &table,
                &policy,
                current.row_uuid(),
                &current_cells,
                author,
                false,
                None,
            );
        }
        let is_update = self
            .policy_previous_content_subject_row(&table, version)?
            .is_some();
        if is_update {
            let Some(previous) = self.policy_previous_content_subject_row(&table, version)? else {
                return Ok(false);
            };
            let previous_cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    previous
                        .cell(&table, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            if let Some(policy) = table.write_policies.update_using.clone() {
                if !self.write_policy_query_allows_candidate_for_schema(
                    policy_schema_version,
                    &table,
                    &policy,
                    previous.row_uuid(),
                    &previous_cells,
                    author,
                    false,
                    None,
                )? {
                    return Ok(false);
                }
            }
            let Some(policy) = table.write_policies.update_check.clone() else {
                return Ok(true);
            };
            let mut effective_cells = previous_cells;
            effective_cells.extend(cells.clone());
            return self.write_policy_query_allows_candidate_for_schema(
                policy_schema_version,
                &table,
                &policy,
                version.row_uuid(),
                &effective_cells,
                author,
                false,
                None,
            );
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(true);
        };
        self.write_policy_query_allows_candidate_for_schema(
            policy_schema_version,
            &table,
            &policy,
            version.row_uuid(),
            &cells,
            author,
            true,
            None,
        )
    }

    pub(crate) fn dry_run_insert_allows(&mut self, commit: MergeableCommit) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, commit.effective_permission_subject())
    }

    pub(crate) fn dry_run_mergeable_write_allows(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, commit.effective_permission_subject())
    }

    pub(crate) fn dry_run_read_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let shape = crate::query::Query::from(table_name)
            .filter(crate::query::eq(
                crate::query::col("id"),
                crate::query::lit(Value::Uuid(row_uuid.0)),
            ))
            .validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows_for_link(&shape, &binding, DurabilityTier::Local, identity)
            .map(|rows| !rows.is_empty())
    }

    pub(crate) fn dry_run_write_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        let Some(row) = self
            .current_rows(table_name, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.update_using.clone() else {
            return Ok(false);
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
    }

    pub(crate) fn dry_run_delete_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        let Some(row) = self
            .current_rows(table_name, DurabilityTier::Local)?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.delete_using.clone() else {
            return Ok(true);
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
    }

    fn policy_projection_for_version_row(
        &mut self,
        version: &VersionRow,
    ) -> Result<(SchemaVersionId, TableSchema, BTreeMap<String, Value>), Error> {
        let source_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "history schema version alias must exist",
            ))?;
        let source_table = self.table_in_schema(version.table(), source_schema)?;
        self.translate_policy_cells(
            source_schema,
            version.table(),
            &source_table,
            version.cells(&source_table)?,
        )
    }

    fn policy_projection_for_version_record(
        &mut self,
        version: &VersionRecord,
    ) -> Result<(SchemaVersionId, TableSchema, BTreeMap<String, Value>), Error> {
        let source_schema = version.schema_version();
        let source_table = self.table_in_schema(version.table(), source_schema)?;
        let cells = source_table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                version
                    .optional_cell_at(idx)
                    .map(|value| (column.name.clone(), value))
            })
            .collect::<BTreeMap<_, _>>();
        self.translate_policy_cells(source_schema, version.table(), &source_table, cells)
    }

    fn translate_policy_cells(
        &mut self,
        source: SchemaVersionId,
        table: &str,
        _source_table: &TableSchema,
        mut cells: BTreeMap<String, Value>,
    ) -> Result<(SchemaVersionId, TableSchema, BTreeMap<String, Value>), Error> {
        // Resolve the schema that owns the policy bundle, then project data
        // (including table identity) into it. The bundle itself stays unchanged.
        let target = self.policy_target_schema_for_source(source, table)?;
        if source == target {
            return Ok((target, self.table_in_schema(table, target)?, cells));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let forward_table = apply_compiled_lens_path(&path, &mut cells);
            let table = self.table_in_schema(&forward_table, target)?;
            return Ok((target, table, cells));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let reverse_table = apply_compiled_lens_path(&path, &mut cells);
            let table = self.table_in_schema(&reverse_table, target)?;
            return Ok((target, table, cells));
        }

        let target_table = self.table_in_schema(table, target)?;
        if policy_tables_are_directly_compatible(_source_table, &target_table) {
            return Ok((target, target_table, cells));
        }

        Err(Error::InvalidCatalogueUpdate("lens chain is unknown"))
    }

    fn policy_schema_for_table_name(&self, table: &str) -> SchemaVersionId {
        let write_schema = self.catalogue.current_write_schema.schema;
        if self
            .table_in_schema(table, write_schema)
            .is_ok_and(|table| table.write_policies.any().is_some())
        {
            write_schema
        } else {
            self.catalogue.current_schema_version_id
        }
    }

    pub(super) fn read_policy_schema_for_table_name(
        &self,
        table: &str,
        query_schema: SchemaVersionId,
        shape: &NormalizedRowSetShape,
    ) -> SchemaVersionId {
        let write_schema = self.catalogue.current_write_schema.schema;
        let current_schema = self.catalogue.current_schema_version_id;
        if self
            .table_in_schema(table, write_schema)
            .is_ok_and(|table| table.read_policy.is_some() || table.write_policies.any().is_some())
            && self.policy_schema_resolves_query_sources(write_schema, shape)
        {
            write_schema
        } else if self.policy_schema_resolves_query_sources(current_schema, shape) {
            // Preserve the pinned current policy schema unless it predates a
            // table rename and cannot resolve every queried source.
            current_schema
        } else {
            query_schema
        }
    }

    fn policy_schema_resolves_query_sources(
        &self,
        schema: SchemaVersionId,
        shape: &NormalizedRowSetShape,
    ) -> bool {
        shape
            .nodes
            .values()
            .filter_map(|node| match node {
                RowSetExpr::Source { source, .. } => Some(&source.table),
                _ => None,
            })
            .chain(shape.auxiliary_sources.iter().map(|source| &source.table))
            .all(|table| self.table_in_schema(table, schema).is_ok())
    }

    fn policy_target_schema_for_source(
        &mut self,
        source: SchemaVersionId,
        table: &str,
    ) -> Result<SchemaVersionId, Error> {
        let write_schema = self.catalogue.current_write_schema.schema;
        if self.source_reaches_write_policy_table(source, write_schema, table)? {
            Ok(write_schema)
        } else {
            Ok(self.catalogue.current_schema_version_id)
        }
    }

    fn source_reaches_write_policy_table(
        &mut self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        table: &str,
    ) -> Result<bool, Error> {
        if source == target {
            return Ok(self
                .table_in_schema(table, target)
                .is_ok_and(|table| table.write_policies.any().is_some()));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let mut cells = BTreeMap::new();
            let target_table = apply_compiled_lens_path(&path, &mut cells);
            return Ok(self
                .table_in_schema(&target_table, target)
                .is_ok_and(|table| table.write_policies.any().is_some()));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let mut cells = BTreeMap::new();
            let target_table = apply_compiled_lens_path(&path, &mut cells);
            return Ok(self
                .table_in_schema(&target_table, target)
                .is_ok_and(|table| table.write_policies.any().is_some()));
        }

        Ok(self
            .table_in_schema(table, target)
            .is_ok_and(|table| table.write_policies.any().is_some()))
    }

    fn policy_current_row(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        tier: DurabilityTier,
    ) -> Result<Option<CurrentRow>, Error> {
        Ok(self
            .current_rows_for_schema(
                &table.name,
                self.policy_schema_for_table_name(&table.name),
                tier,
            )?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid))
    }

    fn policy_delete_subject_row(
        &mut self,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        self.policy_previous_content_subject_row(table, version)
    }

    fn policy_previous_content_subject_row(
        &mut self,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        for parent in version.parents() {
            for parent_version in self.query_versions_for_tx(parent)? {
                if parent_version.row_uuid() != version.row_uuid()
                    || parent_version.layer() != VersionLayer::Content
                {
                    continue;
                }
                let (_policy_schema_version, projected_table, cells) =
                    match self.policy_projection_for_version_row(&parent_version) {
                        Ok(projected) => projected,
                        Err(Error::InvalidCatalogueUpdate("lens chain is unknown")) => {
                            let source_schema = self
                                .schema_version_for_alias(parent_version.schema_version_alias())
                                .ok_or(Error::InvalidStoredValue(
                                    "history schema version alias must exist",
                                ))?;
                            let source_table =
                                self.table_in_schema(parent_version.table(), source_schema)?;
                            if !policy_tables_are_directly_compatible(&source_table, table) {
                                return Err(Error::InvalidCatalogueUpdate("lens chain is unknown"));
                            }
                            (
                                self.policy_schema_for_table_name(&table.name),
                                table.clone(),
                                parent_version.cells(&source_table)?,
                            )
                        }
                        Err(error) => return Err(error),
                    };
                if projected_table.name != table.name {
                    continue;
                }
                return current_row_from_cells(table, version.row_uuid(), &cells).map(Some);
            }
        }

        if let Some(current_version) =
            self.query_local_layer_winner(&table.name, version.row_uuid(), VersionLayer::Content)?
        {
            let (_policy_schema_version, projected_table, cells) =
                self.policy_projection_for_version_row(&current_version)?;
            if projected_table.name == table.name {
                return current_row_from_cells(table, version.row_uuid(), &cells).map(Some);
            }
        }

        if let Some(current) =
            self.policy_current_row(table, version.row_uuid(), DurabilityTier::Global)?
        {
            return Ok(Some(current));
        }

        Ok(None)
    }

    pub(super) fn query_transaction_memo(
        &mut self,
        tx_id: TxId,
        context: &mut ViewEvaluationContext,
    ) -> Result<Option<StoredTransaction>, Error> {
        if let std::collections::btree_map::Entry::Vacant(entry) = context.tx_rows.entry(tx_id) {
            entry.insert(self.query_transaction(tx_id)?);
        }
        Ok(context
            .tx_rows
            .get(&tx_id)
            .expect("tx row memo populated")
            .clone())
    }
}

fn policy_tables_are_directly_compatible(source: &TableSchema, target: &TableSchema) -> bool {
    source.name == target.name
        && source.columns.len() == target.columns.len()
        && source
            .columns
            .iter()
            .zip(target.columns.iter())
            .all(|(source, target)| {
                source.name == target.name
                    && source.column_type == target.column_type
                    && source.large_value == target.large_value
            })
}
