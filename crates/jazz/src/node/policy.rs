//! Write-policy admission and policy-pinned row projection. Policy predicates,
//! joins, inheritance, reachability, and alternatives execute through the query
//! program in [`super::query_eval`]; this module selects the operation clause,
//! projects old/candidate data into the pinned policy schema, and fail-closes
//! write ingest. It also retains the transaction memo used by view emission.

use super::query_engine::{NormalizedRowSetShape, RowSetExpr};
use super::*;
use crate::protocol::PermissionAdviceAction;

#[derive(Default)]
pub(super) struct ViewEvaluationContext {
    pub(super) tx_rows: BTreeMap<TxId, Option<StoredTransaction>>,
}

fn version_provenance(version: &VersionRecord) -> RowProvenance {
    RowProvenance {
        created_by: version.created_by(),
        created_at: version.created_at_ms(),
        updated_by: version.updated_by(),
        updated_at: version.updated_at_ms(),
    }
}

fn stored_version_provenance(version: &VersionRow) -> RowProvenance {
    RowProvenance {
        created_by: version.created_by(),
        created_at: version.created_at().physical_ms(),
        updated_by: version.updated_by(),
        updated_at: version.updated_at().physical_ms(),
    }
}

fn reconstructed_policy_subject_row(
    table: &TableSchema,
    row_uuid: RowUuid,
    cells: &BTreeMap<String, Value>,
    version: &VersionRow,
) -> Result<CurrentRow, Error> {
    current_row_from_cells_with_explicit_provenance(
        table,
        row_uuid,
        cells,
        stored_version_provenance(version),
        Some((version.tx_time(), version.tx_node_alias())),
    )
}

/// A reconstructed candidate without retained row metadata cannot prove a
/// provenance ownership clause. Keep that case fail-closed instead of
/// mistaking the incoming writer for the historic creator.
fn unresolved_provenance() -> RowProvenance {
    RowProvenance {
        created_by: AuthorSubject::SYSTEM,
        created_at: 0,
        updated_by: AuthorSubject::SYSTEM,
        updated_at: 0,
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Reconstruct the exact policy operation represented by each incoming
    /// version record.  This is the sole bridge from committed wire data to
    /// authorization support hydration: callers must not substitute a
    /// table-wide or placeholder update action.
    #[cfg(test)]
    pub(crate) async fn authorization_actions_for_versions(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<Vec<PermissionAdviceAction>, Error> {
        self.authorization_actions_for_versions_in_transaction(versions, None)
            .await
    }

    /// Reconstruct operation-specific authorization actions while excluding
    /// the candidate transaction from prior-row lookup.  An authority stores
    /// a pending candidate before it assigns its fate, so treating that row as
    /// prior evidence would turn a new insert into an update.
    pub(crate) async fn authorization_actions_for_versions_in_transaction(
        &mut self,
        versions: &[VersionRecord],
        candidate_tx_id: Option<TxId>,
    ) -> Result<Vec<PermissionAdviceAction>, Error> {
        let mut actions = Vec::with_capacity(versions.len());
        for version in versions {
            let (policy_schema_version, table, cells) =
                self.policy_projection_for_version_record(version)?;
            if version.deletion() == Some(DeletionEvent::Deleted) {
                actions.push(PermissionAdviceAction::Delete {
                    table: table.name.clone(),
                    row: version.row_uuid(),
                });
                continue;
            }
            let is_update = self
                .policy_previous_content_subject_row(
                    policy_schema_version,
                    &table,
                    version,
                    candidate_tx_id,
                )
                .await?
                .is_some();
            if is_update {
                let patch = match version.authored_columns() {
                    Some(authored) => cells
                        .into_iter()
                        .filter(|(column, _)| authored.contains(column))
                        .collect(),
                    None => cells,
                };
                actions.push(PermissionAdviceAction::Update {
                    table: table.name.clone(),
                    row: version.row_uuid(),
                    patch,
                });
            } else {
                actions.push(PermissionAdviceAction::Insert {
                    table: table.name.clone(),
                    cells,
                });
            }
        }
        Ok(actions)
    }

    pub(super) async fn write_policy_allows_version_record(
        &mut self,
        version: &VersionRecord,
        author: AuthorSubject,
        candidate_tx_id: Option<TxId>,
    ) -> Result<bool, Error> {
        self.write_policy_allows_version_record_for_view(version, author, None, candidate_tx_id)
            .await
    }

    /// A session update/upsert of an existing row also requires that the fate
    /// authority can read that previous row. This is deliberately decided at
    /// authority admission, never while a client stages its mergeable
    /// transaction: a replica can retain the target preimage without the
    /// private support rows that make its read policy true.
    pub(super) async fn version_satisfies_read_for_write_visibility(
        &mut self,
        version: &VersionRecord,
        author: AuthorSubject,
        candidate_tx_id: Option<TxId>,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM || version.deletion() == Some(DeletionEvent::Deleted) {
            return Ok(true);
        }
        let (policy_schema_version, table, _) =
            self.policy_projection_for_version_record(version)?;
        let Some(read_policy) = table.read_policy.clone() else {
            return Ok(true);
        };
        let Some(previous) = self
            .policy_previous_content_subject_row(
                policy_schema_version,
                &table,
                version,
                candidate_tx_id,
            )
            .await?
        else {
            // This is an INSERT (including an absent-target UPSERT), for
            // which INV-RLS-20 does not require prior read visibility.
            return Ok(true);
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
        let provenance = previous.provenance()?.unwrap_or_else(unresolved_provenance);
        self.read_policy_query_allows_candidate_with_provenance_for_schema(
            policy_schema_version,
            &table,
            &read_policy,
            previous.row_uuid(),
            &previous_cells,
            author,
            provenance,
        )
        .await
    }

    /// Prove read-for-write visibility of the logical branch-view source that
    /// was copied into a first physical head overlay.
    ///
    /// A normal mergeable update obtains its prior row from the target's
    /// physical history. A first branch overlay has intentionally no
    /// cross-branch parent, so that lookup says "insert". Its separately
    /// versioned evidence lets the authority resolve the inherited source and
    /// evaluate the ordinary read policy against it without turning the source
    /// into a causal dependency or exposing policy support to the client.
    pub(super) async fn branch_view_copy_satisfies_read_for_write_visibility(
        &mut self,
        evidence: &crate::tx::BranchViewCopyEvidence,
        author: AuthorSubject,
        candidate_tx_id: Option<TxId>,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let Some(source) = self
            .resolve_branch_view_copy_evidence(evidence, candidate_tx_id)
            .await?
        else {
            return Ok(false);
        };
        let source = self.version_record_from_row(&source)?;
        let (policy_schema_version, table, cells) =
            self.policy_projection_for_version_record(&source)?;
        let Some(read_policy) = table.read_policy.clone() else {
            return Ok(true);
        };
        let provenance = version_provenance(&source);
        self.read_policy_query_allows_candidate_with_provenance_for_schema(
            policy_schema_version,
            &table,
            &read_policy,
            source.row_uuid(),
            &cells,
            author,
            provenance,
        )
        .await
    }

    async fn write_policy_allows_version_record_for_view(
        &mut self,
        version: &VersionRecord,
        author: AuthorSubject,
        exact_view: Option<&JazzSchema>,
        candidate_tx_id: Option<TxId>,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let (policy_schema_version, table, cells) = if let Some(schema) = exact_view {
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == version.table())
                .cloned()
                .ok_or_else(|| Error::TableNotFound(version.table().to_owned()))?;
            let cells = table
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    version
                        .optional_cell_at(idx)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            (version.schema_version(), table, cells)
        } else {
            self.policy_projection_for_version_record(version)?
        };
        // A table stays open until it declares its first policy clause. From
        // that point on the policy set is closed: a missing operation clause
        // is a denial, rather than an accidental public grant.
        if !table.has_any_policy() {
            return Ok(true);
        }
        if version.deletion() == Some(DeletionEvent::Deleted) {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(false);
            };
            let current = match self
                .policy_delete_subject_row(policy_schema_version, &table, version, candidate_tx_id)
                .await?
            {
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
            let provenance = current.provenance()?.unwrap_or_else(unresolved_provenance);
            return self
                .write_policy_query_allows_candidate_with_provenance_for_schema(
                    policy_schema_version,
                    &table,
                    &policy,
                    current.row_uuid(),
                    &current_cells,
                    author,
                    false,
                    provenance,
                )
                .await;
        }
        let is_update = self
            .policy_previous_content_subject_row(
                policy_schema_version,
                &table,
                version,
                candidate_tx_id,
            )
            .await?
            .is_some();
        if is_update {
            let Some(previous) = self
                .policy_previous_content_subject_row(
                    policy_schema_version,
                    &table,
                    version,
                    candidate_tx_id,
                )
                .await?
            else {
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
            let previous_provenance = previous.provenance()?.unwrap_or_else(unresolved_provenance);
            if let Some(policy) = table.write_policies.update_using.clone() {
                if !self
                    .write_policy_query_allows_candidate_with_provenance_for_schema(
                        policy_schema_version,
                        &table,
                        &policy,
                        previous.row_uuid(),
                        &previous_cells,
                        author,
                        false,
                        previous_provenance,
                    )
                    .await?
                {
                    return Ok(false);
                }
            }
            if table.write_policies.update_using.is_none()
                && table.write_policies.update_check.is_none()
            {
                return Ok(false);
            }
            let Some(policy) = table.write_policies.update_check.clone() else {
                return Ok(true);
            };
            let mut effective_cells = previous_cells;
            effective_cells.extend(cells.clone());
            let update_check_provenance = RowProvenance {
                created_by: previous_provenance.created_by,
                created_at: previous_provenance.created_at,
                updated_by: version.updated_by(),
                updated_at: version.updated_at_ms(),
            };
            return self
                .write_policy_query_allows_candidate_with_provenance_for_schema(
                    policy_schema_version,
                    &table,
                    &policy,
                    version.row_uuid(),
                    &effective_cells,
                    author,
                    false,
                    update_check_provenance,
                )
                .await;
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(false);
        };
        self.write_policy_query_allows_candidate_with_provenance_for_schema(
            policy_schema_version,
            &table,
            &policy,
            version.row_uuid(),
            &cells,
            author,
            true,
            version_provenance(version),
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn dry_run_insert_allows(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(
            &version,
            commit.effective_permission_subject(),
            None,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn advisory_mergeable_write_allows(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        self.dry_run_mergeable_write_allows_in_schema(
            self.catalogue.current_write_schema.schema,
            commit,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn dry_run_mergeable_write_allows_in_schema(
        &mut self,
        write_schema_version: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let table = self.table_in_schema(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(
            &version,
            commit.effective_permission_subject(),
            None,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn dry_run_mergeable_write_allows_for_view(
        &mut self,
        exact_view: &JazzSchema,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let write_schema_version = exact_view.version_id();
        let table = exact_view
            .tables
            .iter()
            .find(|table| table.name == commit.table)
            .ok_or_else(|| Error::TableNotFound(commit.table.clone()))?;
        let version = VersionRecord::from_commit(&commit, table, write_schema_version)?;
        self.write_policy_allows_version_record_for_view(
            &version,
            commit.effective_permission_subject(),
            Some(exact_view),
            None,
        )
        .await
    }

    pub(crate) async fn dry_run_read_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        self.dry_run_read_current_allows_in_schema(
            table_name,
            row_uuid,
            self.catalogue.current_schema_version_id,
            identity,
        )
        .await
    }

    /// Evaluate a point-read policy in the schema that named the wire
    /// request.  Repair requests may use a projected table from a catalogue
    /// schema newer than this node's base API schema.
    pub(crate) async fn dry_run_read_current_allows_in_schema(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        let schema = if schema_version == self.catalogue.current_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&schema_version)
                .ok_or(Error::InvalidStoredValue(
                    "repair request schema is missing from catalogue",
                ))?
                .schema
        };
        // `id` resolves to a declared user column when a table has one, so an
        // internal physical-row probe must use the dedicated access-path API.
        let shape = crate::query::Query::from(table_name)
            .validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows_for_link_physical_row(
            &shape,
            &binding,
            DurabilityTier::Local,
            identity,
            row_uuid,
        )
        .await
        .map(|rows| rows.into_iter().any(|row| row.row_uuid() == row_uuid))
    }

    #[cfg(test)]
    pub(crate) async fn dry_run_write_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorSubject,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        if !table.has_any_policy() {
            return Ok(true);
        }
        let Some(row) = self
            .policy_local_current_subject_row(&table, row_uuid)
            .await?
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.update_using.clone() else {
            // An update that only has a WITH CHECK clause is still an
            // explicitly declared update operation. The caller asking about
            // the old-row clause has nothing further to prove here.
            return Ok(table.write_policies.update_check.is_some());
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
            .await
    }

    pub(crate) async fn dry_run_delete_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorSubject,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
        if !table.has_any_policy() {
            return Ok(true);
        }
        let Some(row) = self
            .policy_local_current_subject_row(&table, row_uuid)
            .await?
        else {
            return Ok(false);
        };
        let Some(policy) = table.write_policies.delete_using.clone() else {
            return Ok(false);
        };
        self.write_policy_query_allows_current_row(&policy, row.row_uuid(), author)
            .await
    }

    async fn policy_local_current_subject_row(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        if self
            .query_local_layer_winner(&table.name, row_uuid, VersionLayer::Deletion)
            .await?
            .is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted))
        {
            return Ok(None);
        }
        let Some(version) = self
            .query_local_layer_winner(&table.name, row_uuid, VersionLayer::Content)
            .await?
        else {
            return Ok(None);
        };
        let (_policy_schema_version, projected_table, cells) =
            self.policy_projection_for_version_row(&version)?;
        if projected_table.name != table.name {
            return Ok(None);
        }
        reconstructed_policy_subject_row(table, row_uuid, &cells, &version).map(Some)
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

    pub(super) fn policy_schema_for_table_name(&self, table: &str) -> SchemaVersionId {
        let write_schema = self.catalogue.current_write_schema.schema;
        if self
            .table_in_schema(table, write_schema)
            .is_ok_and(|table| table.has_any_policy())
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
            .is_ok_and(|table| table.has_any_policy())
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
        } else if self.source_reaches_write_policy_table(
            source,
            self.catalogue.current_schema_version_id,
            table,
        )? || self
            .table_in_schema(table, self.catalogue.current_schema_version_id)
            .is_ok()
        {
            Ok(self.catalogue.current_schema_version_id)
        } else {
            Ok(source)
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
                .is_ok_and(|table| table.has_any_policy()));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Forward, table)?
        {
            let mut cells = BTreeMap::new();
            let target_table = apply_compiled_lens_path(&path, &mut cells);
            return Ok(self
                .table_in_schema(&target_table, target)
                .is_ok_and(|table| table.has_any_policy()));
        }

        if let Some(path) =
            self.compiled_lens_path(source, target, LensPathDirection::Reverse, table)?
        {
            let mut cells = BTreeMap::new();
            let target_table = apply_compiled_lens_path(&path, &mut cells);
            return Ok(self
                .table_in_schema(&target_table, target)
                .is_ok_and(|table| table.has_any_policy()));
        }

        Ok(self
            .table_in_schema(table, target)
            .is_ok_and(|table| table.has_any_policy()))
    }

    async fn policy_delete_subject_row(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        version: &VersionRecord,
        candidate_tx_id: Option<TxId>,
    ) -> Result<Option<CurrentRow>, Error> {
        self.policy_previous_content_subject_row(
            policy_schema_version,
            table,
            version,
            candidate_tx_id,
        )
        .await
    }

    async fn policy_previous_content_subject_row(
        &mut self,
        _policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        version: &VersionRecord,
        candidate_tx_id: Option<TxId>,
    ) -> Result<Option<CurrentRow>, Error> {
        let subject_table = if self
            .table_in_schema(version.table(), self.catalogue.current_write_schema.schema)
            .is_ok()
        {
            version.table()
        } else {
            &table.name
        };
        for parent in version.parents() {
            for parent_version in self.query_versions_for_tx(parent).await? {
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
                return reconstructed_policy_subject_row(
                    table,
                    version.row_uuid(),
                    &cells,
                    &parent_version,
                )
                .map(Some);
            }
        }

        let local_previous = match candidate_tx_id {
            Some(candidate_tx_id) => {
                self.query_local_layer_winner_in_branch_excluding_tx(
                    subject_table,
                    version.branch_key(),
                    version.row_uuid(),
                    VersionLayer::Content,
                    candidate_tx_id,
                )
                .await?
            }
            None => {
                self.query_local_layer_winner_in_branch(
                    subject_table,
                    version.branch_key(),
                    version.row_uuid(),
                    VersionLayer::Content,
                )
                .await?
            }
        };
        if let Some(current_version) = local_previous {
            let (_policy_schema_version, projected_table, cells) =
                self.policy_projection_for_version_row(&current_version)?;
            if projected_table.name == table.name {
                return reconstructed_policy_subject_row(
                    table,
                    version.row_uuid(),
                    &cells,
                    &current_version,
                )
                .map(Some);
            }
        }

        if let Some(current_version) = self
            .query_global_layer_winner_in_branch(
                subject_table,
                version.branch_key(),
                version.row_uuid(),
                VersionLayer::Content,
            )
            .await?
        {
            if candidate_tx_id != Some(self.version_tx_id(&current_version)?) {
                let (_policy_schema_version, projected_table, cells) =
                    self.policy_projection_for_version_row(&current_version)?;
                if projected_table.name == table.name {
                    return reconstructed_policy_subject_row(
                        table,
                        version.row_uuid(),
                        &cells,
                        &current_version,
                    )
                    .map(Some);
                }
            }
        }

        Ok(None)
    }

    pub(super) async fn query_transaction_memo(
        &mut self,
        tx_id: TxId,
        context: &mut ViewEvaluationContext,
    ) -> Result<Option<StoredTransaction>, Error> {
        if let std::collections::btree_map::Entry::Vacant(entry) = context.tx_rows.entry(tx_id) {
            entry.insert(self.query_transaction(tx_id).await?);
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
                source.name == target.name && source.column_type == target.column_type
            })
}
