//! Write-policy admission and policy-pinned row projection. Policy predicates,
//! joins, inheritance, reachability, and alternatives execute through the query
//! program in [`super::query_eval`]; this module selects the operation clause,
//! projects old/candidate data into the pinned policy schema, and fail-closes
//! write ingest. It also retains the transaction memo used by view emission.

use super::query_engine::{NormalizedRowSetShape, RowSetExpr};
use super::query_eval::TransactionWriteOverlay;
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

/// One row a candidate commit unit writes on the main branch, prepared as
/// write-policy evidence for the unit's other writes (`INV-RLS-9`).
struct CandidateEvidenceRow {
    /// Logical table as the authored versions name it.
    authored_table: String,
    row_uuid: RowUuid,
    /// Policy schema and table the row is projected into.
    policy_schema: SchemaVersionId,
    policy_table: String,
    /// The row after the transaction, or `None` when the unit deletes it.
    after: Option<CurrentRow>,
    /// The committed row an update replaces. It stays evidence until the
    /// update's own checks pass.
    before: Option<CurrentRow>,
}

/// The rows of one candidate commit unit, keyed for the per-write overlays.
#[derive(Default)]
struct CandidateUnitEvidence {
    rows: Vec<CandidateEvidenceRow>,
}

impl CandidateUnitEvidence {
    /// The overlay one write's WITH CHECK clause reads. The written row itself
    /// is the inline candidate, so it is never overlaid. A deleted row is
    /// hidden. A row whose checks have passed shows its post-transaction
    /// content; until then an updated row shows its committed content and an
    /// inserted or restored row is hidden, so writes cannot justify each
    /// other in a cycle.
    fn overlay_for(
        &self,
        version: &VersionRecord,
        grounded: &BTreeSet<(String, RowUuid)>,
    ) -> TransactionWriteOverlay {
        let mut overlay = TransactionWriteOverlay::default();
        for row in &self.rows {
            if row.authored_table == version.table() && row.row_uuid == version.row_uuid() {
                continue;
            }
            let shown = match &row.after {
                None => None,
                Some(after) if grounded.contains(&(row.authored_table.clone(), row.row_uuid)) => {
                    Some(after.clone())
                }
                Some(_) => row.before.clone(),
            };
            overlay.set(row.policy_schema, &row.policy_table, row.row_uuid, shown);
        }
        overlay
    }
}

fn current_row_cells(table: &TableSchema, row: &CurrentRow) -> BTreeMap<String, Value> {
    table
        .columns
        .iter()
        .filter_map(|column| {
            row.cell(table, &column.name)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Build a typed row-preview for advisory policy evaluation without
    /// letting durable attribution replace the selected policy capability.
    fn durable_author_policy_preview(
        &self,
        mut commit: MergeableCommit,
    ) -> Result<(MergeableCommit, AuthorSubject), Error> {
        let permission_subject = commit.effective_permission_subject();
        commit.made_by = RowAuthor::from_session(commit.made_by, self.node_uuid)
            .map_err(|_| Error::UnadmittedWriteAuthor)?
            .as_author_subject();
        Ok((commit, permission_subject))
    }
    /// Reconstruct the exact policy operation represented by each incoming
    /// version record.  This is the sole bridge from committed wire data to
    /// authorization support hydration: callers must not substitute a
    /// table-wide or placeholder update action.
    #[cfg(test)]
    pub async fn authorization_actions_for_versions(
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
    pub async fn authorization_actions_for_versions_in_transaction(
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
        candidate_versions: &[VersionRecord],
    ) -> Result<bool, Error> {
        self.write_policy_allows_version_record_for_view(
            version,
            author,
            None,
            candidate_tx_id,
            candidate_versions,
            &TransactionWriteOverlay::default(),
        )
        .await
    }

    /// Decide every write policy of one commit unit (`INV-RLS-9`).
    ///
    /// A transaction reads its own writes, so its policy checks do too. Each
    /// write's WITH CHECK clause (insert check, update check) reads committed
    /// state overlaid with the unit's other writes, as described by
    /// [`CandidateUnitEvidence::overlay_for`]. A commit unit carries its
    /// writes as a canonical set, not in write order, so the checks run to a
    /// fixpoint: a row's post-transaction content becomes evidence once its
    /// own checks pass, and the unit is accepted exactly when every check
    /// passes. That is the case when some order of the writes lets each one be
    /// checked after the writes it depends on. USING clauses judge the rows
    /// the transaction acts on as committed. Other transactions' writes are
    /// never evidence.
    pub(super) async fn commit_unit_write_policies_allow(
        &mut self,
        versions: &[VersionRecord],
        author: AuthorSubject,
        candidate_tx_id: TxId,
    ) -> Result<bool, Error> {
        let evidence = self
            .candidate_unit_evidence(versions, author, candidate_tx_id)
            .await?;
        let mut passed = vec![false; versions.len()];
        let mut pending = (0..versions.len()).collect::<Vec<_>>();
        let mut grounded = BTreeSet::new();
        loop {
            let mut failed = Vec::new();
            for index in pending {
                let version = &versions[index];
                let overlay = evidence.overlay_for(version, &grounded);
                #[cfg(any(test, feature = "testing"))]
                WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(count.get() + 1));
                if self
                    .write_policy_allows_version_record_for_view(
                        version,
                        author,
                        None,
                        Some(candidate_tx_id),
                        versions,
                        &overlay,
                    )
                    .await?
                {
                    passed[index] = true;
                } else {
                    failed.push(index);
                }
            }
            if failed.is_empty() {
                return Ok(true);
            }
            // A row is grounded once every version the unit writes for it
            // has passed. Only newly grounded rows can change a failed
            // check's evidence; without one the failure is final.
            let mut next_grounded = BTreeSet::new();
            for row in &evidence.rows {
                let complete = versions.iter().zip(&passed).all(|(version, passed)| {
                    *passed
                        || version.table() != row.authored_table
                        || version.row_uuid() != row.row_uuid
                });
                if complete {
                    next_grounded.insert((row.authored_table.clone(), row.row_uuid));
                }
            }
            if next_grounded.len() == grounded.len() {
                return Ok(false);
            }
            grounded = next_grounded;
            pending = failed;
        }
    }

    /// Prepare the main-branch rows of a candidate unit as evidence for its
    /// other writes. A unit that writes one row has nothing to overlay.
    async fn candidate_unit_evidence(
        &mut self,
        versions: &[VersionRecord],
        author: AuthorSubject,
        candidate_tx_id: TxId,
    ) -> Result<CandidateUnitEvidence, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(CandidateUnitEvidence::default());
        }
        let mut by_row = BTreeMap::<(String, RowUuid), Vec<&VersionRecord>>::new();
        for version in versions {
            // Branch-local writes are evaluated in their own branch view,
            // which this overlay does not model.
            if !version.branch_key().values.is_empty() {
                continue;
            }
            by_row
                .entry((version.table().to_owned(), version.row_uuid()))
                .or_default()
                .push(version);
        }
        if by_row.len() < 2 {
            return Ok(CandidateUnitEvidence::default());
        }
        let mut rows = Vec::with_capacity(by_row.len());
        for ((authored_table, row_uuid), row_versions) in by_row {
            let deleted = row_versions
                .iter()
                .any(|version| version.deletion() == Some(DeletionEvent::Deleted));
            let content = row_versions
                .iter()
                .copied()
                .find(|version| version.deletion().is_none());
            let subject = content.unwrap_or(row_versions[0]);
            let (policy_schema, table, cells) =
                self.policy_projection_for_version_record(subject)?;
            let (after, before) = if deleted {
                (None, None)
            } else {
                let previous = self
                    .policy_previous_content_subject_row(
                        policy_schema,
                        &table,
                        subject,
                        Some(candidate_tx_id),
                    )
                    .await?;
                match content {
                    Some(content) => {
                        let mut after_cells = previous
                            .as_ref()
                            .map(|previous| current_row_cells(&table, previous))
                            .unwrap_or_default();
                        after_cells.extend(cells);
                        let provenance = match &previous {
                            Some(previous) => {
                                let previous =
                                    previous.provenance()?.unwrap_or_else(unresolved_provenance);
                                RowProvenance {
                                    created_by: previous.created_by,
                                    created_at: previous.created_at,
                                    updated_by: content.updated_by(),
                                    updated_at: content.updated_at_ms(),
                                }
                            }
                            None => version_provenance(content),
                        };
                        let after = current_row_from_cells_with_explicit_provenance(
                            &table,
                            row_uuid,
                            &after_cells,
                            provenance,
                            None,
                        )?;
                        (Some(after), previous)
                    }
                    // A restore brings back the row's current content, which
                    // committed state still reports as deleted.
                    None => (previous, None),
                }
            };
            rows.push(CandidateEvidenceRow {
                authored_table,
                row_uuid,
                policy_schema,
                policy_table: table.name.clone(),
                after,
                before,
            });
        }
        Ok(CandidateUnitEvidence { rows })
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
        let read_policy = super::query_eval::authorization_query_from_read_policy(&table);
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
        authored_schema: SchemaVersionId,
        author: AuthorSubject,
        candidate_tx_id: Option<TxId>,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let Some(source) = self
            .resolve_branch_view_copy_evidence(evidence, authored_schema, candidate_tx_id)
            .await?
        else {
            return Ok(false);
        };
        let source = self.version_record_from_row(&source)?;
        let (policy_schema_version, table, cells) =
            self.policy_projection_for_version_record(&source)?;
        let read_policy = super::query_eval::authorization_query_from_read_policy(&table);
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
        candidate_versions: &[VersionRecord],
        transaction_overlay: &TransactionWriteOverlay,
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
        // Every user operation requires an explicit grant, including on a
        // table with no policies at all. SYSTEM is the explicit bypass above.
        if version.deletion() == Some(DeletionEvent::Deleted) {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(false);
            };
            let current = match self
                .policy_delete_subject_row(
                    policy_schema_version,
                    &table,
                    version,
                    candidate_tx_id,
                    candidate_versions,
                )
                .await?
            {
                Some(current) => current,
                None => return Ok(false),
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
            // WITH CHECK judges the row the transaction leaves behind, so its
            // evidence includes the transaction's other writes (INV-RLS-9).
            return self
                .write_policy_query_allows_candidate_over_transaction(
                    policy_schema_version,
                    &table,
                    &policy,
                    version.row_uuid(),
                    &effective_cells,
                    author,
                    false,
                    update_check_provenance,
                    transaction_overlay,
                )
                .await;
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(false);
        };
        self.write_policy_query_allows_candidate_over_transaction(
            policy_schema_version,
            &table,
            &policy,
            version.row_uuid(),
            &cells,
            author,
            true,
            version_provenance(version),
            transaction_overlay,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[doc(hidden)]
    pub async fn dry_run_insert_allows(&mut self, commit: MergeableCommit) -> Result<bool, Error> {
        let write_schema_version = self.catalogue.active_schema.schema;
        let (commit, permission_subject) = self.durable_author_policy_preview(commit)?;
        let table = self.table_in_schema_ref(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, permission_subject, None, &[])
            .await
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub async fn advisory_mergeable_write_allows(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        self.dry_run_mergeable_write_allows_in_schema(self.catalogue.active_schema.schema, commit)
            .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[doc(hidden)]
    pub async fn dry_run_mergeable_write_allows_in_schema(
        &mut self,
        write_schema_version: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let (commit, permission_subject) = self.durable_author_policy_preview(commit)?;
        let table = self.table_in_schema_ref(&commit.table, write_schema_version)?;
        let version = VersionRecord::from_commit(&commit, &table, write_schema_version)?;
        self.write_policy_allows_version_record(&version, permission_subject, None, &[])
            .await
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn dry_run_mergeable_write_allows_for_view(
        &mut self,
        exact_view: &JazzSchema,
        commit: MergeableCommit,
    ) -> Result<bool, Error> {
        let write_schema_version = exact_view.version_id();
        let (commit, permission_subject) = self.durable_author_policy_preview(commit)?;
        let table = exact_view
            .tables
            .iter()
            .find(|table| table.name == commit.table)
            .ok_or_else(|| Error::TableNotFound(commit.table.clone()))?;
        let version = VersionRecord::from_commit(&commit, table, write_schema_version)?;
        self.write_policy_allows_version_record_for_view(
            &version,
            permission_subject,
            Some(exact_view),
            None,
            &[],
            &TransactionWriteOverlay::default(),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn dry_run_read_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        identity: AuthorSubject,
    ) -> Result<bool, Error> {
        self.dry_run_read_current_allows_in_schema(
            table_name,
            row_uuid,
            self.catalogue.local_schema_version_id,
            identity,
            false,
        )
        .await
    }

    /// Evaluate a point-read policy in the schema that named the wire
    /// request.  Repair requests may use a projected table from a catalogue
    /// schema newer than this node's base API schema.
    pub async fn dry_run_read_current_allows_in_schema(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
        identity: AuthorSubject,
        include_deleted: bool,
    ) -> Result<bool, Error> {
        let schema = if schema_version == self.catalogue.active_schema.schema {
            &self.catalogue.active_schema.compiled
        } else if schema_version == self.catalogue.local_schema_version_id {
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
        let rows = if include_deleted {
            // Repair authorizes disclosure, not membership in the default
            // non-deleted query. The ordinary includeDeleted serving path
            // still enforces the current row policy against its content.
            self.query_readable_current_row_including_deleted(
                &shape,
                &binding,
                DurabilityTier::Local,
                identity,
                row_uuid,
            )
            .await?
        } else {
            // Only row identity is inspected: keep large values physical.
            self.query_row_visibility_for_link_physical_row(
                &shape,
                &binding,
                DurabilityTier::Local,
                identity,
                row_uuid,
            )
            .await?
        };
        Ok(rows.into_iter().any(|row| row.row_uuid() == row_uuid))
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub async fn dry_run_write_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorSubject,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
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

    #[doc(hidden)]
    pub async fn dry_run_delete_current_allows(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        author: AuthorSubject,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let table = self.table(table_name)?.clone();
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

    /// Whether `row_uuid` is a live local row of `table`, by point lookup of
    /// its content and deletion winners (the lookup delete advice uses).
    pub async fn local_current_row_exists(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
    ) -> Result<bool, Error> {
        let table = self.table(table_name)?.clone();
        Ok(self
            .policy_local_current_subject_row(&table, row_uuid)
            .await?
            .is_some())
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
        let cells = version.cells(self.table_in_schema_ref(version.table(), source_schema)?)?;
        self.translate_policy_cells(source_schema, version.table(), cells)
    }

    fn policy_projection_for_version_record(
        &mut self,
        version: &VersionRecord,
    ) -> Result<(SchemaVersionId, TableSchema, BTreeMap<String, Value>), Error> {
        let source_schema = version.schema_version();
        let source_table = self.table_in_schema_ref(version.table(), source_schema)?;
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
        self.translate_policy_cells(source_schema, version.table(), cells)
    }

    fn translate_policy_cells(
        &mut self,
        source: SchemaVersionId,
        table: &str,
        mut cells: BTreeMap<String, Value>,
    ) -> Result<(SchemaVersionId, TableSchema, BTreeMap<String, Value>), Error> {
        // Resolve the schema that owns the policy bundle, then project data
        // (including table identity) into it. The bundle itself stays unchanged.
        let target = self.policy_target_schema_for_source(source, table)?;
        if source == target {
            return Ok((target, self.table_in_schema(table, target)?, cells));
        }

        if let Some(path) = self.compiled_lens_path(source, target, table)? {
            let translated_table = apply_compiled_lens_path(&path, &mut cells);
            let table = self.table_in_schema(&translated_table, target)?;
            return Ok((target, table, cells));
        }

        let target_table = self.table_in_schema_ref(table, target)?;
        if policy_tables_are_directly_compatible(
            self.table_in_schema_ref(table, source)?,
            target_table,
        ) {
            return Ok((target, target_table.clone(), cells));
        }

        Err(Error::InvalidCatalogueUpdate("lens chain is unknown"))
    }

    pub(super) fn policy_schema_for_table_name(&self, table: &str) -> SchemaVersionId {
        let write_schema = self.catalogue.active_schema.schema;
        if self.table_in_schema_ref(table, write_schema).is_ok() {
            write_schema
        } else {
            self.catalogue.local_schema_version_id
        }
    }

    pub(super) fn read_policy_schema_for_table_name(
        &self,
        table: &str,
        query_schema: SchemaVersionId,
        shape: &NormalizedRowSetShape,
    ) -> SchemaVersionId {
        let write_schema = self.catalogue.active_schema.schema;
        let current_schema = self.catalogue.local_schema_version_id;
        if self.table_in_schema_ref(table, write_schema).is_ok()
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
            .all(|table| self.table_in_schema_ref(table, schema).is_ok())
    }

    fn policy_target_schema_for_source(
        &mut self,
        source: SchemaVersionId,
        table: &str,
    ) -> Result<SchemaVersionId, Error> {
        let write_schema = self.catalogue.active_schema.schema;
        if self.source_reaches_write_policy_table(source, write_schema, table)? {
            Ok(write_schema)
        } else if self.source_reaches_write_policy_table(
            source,
            self.catalogue.local_schema_version_id,
            table,
        )? || self
            .table_in_schema_ref(table, self.catalogue.local_schema_version_id)
            .is_ok()
        {
            Ok(self.catalogue.local_schema_version_id)
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
            return Ok(self.table_in_schema_ref(table, target).is_ok());
        }

        if let Some(path) = self.compiled_lens_path(source, target, table)? {
            let mut cells = BTreeMap::new();
            let target_table = apply_compiled_lens_path(&path, &mut cells);
            return Ok(self.table_in_schema_ref(&target_table, target).is_ok());
        }

        Ok(self.table_in_schema_ref(table, target).is_ok())
    }

    async fn policy_delete_subject_row(
        &mut self,
        policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        version: &VersionRecord,
        candidate_tx_id: Option<TxId>,
        candidate_versions: &[VersionRecord],
    ) -> Result<Option<CurrentRow>, Error> {
        if let Some(previous) = self
            .policy_previous_content_subject_row(
                policy_schema_version,
                table,
                version,
                candidate_tx_id,
            )
            .await?
        {
            return Ok(Some(previous));
        }
        // An insert followed by a delete in one transaction has no prior
        // content row. Admission runs before persistence, so inspect the
        // same incoming unit for its content and real creation provenance.
        for candidate in candidate_versions {
            if candidate.row_uuid() != version.row_uuid()
                || candidate.deletion().is_some()
                || candidate.branch_key() != version.branch_key()
            {
                continue;
            }
            let (_, projected_table, cells) =
                self.policy_projection_for_version_record(candidate)?;
            if projected_table.name == table.name {
                return current_row_from_cells_with_explicit_provenance(
                    table,
                    version.row_uuid(),
                    &cells,
                    version_provenance(candidate),
                    None,
                )
                .map(Some);
            }
        }
        Ok(None)
    }

    async fn policy_previous_content_subject_row(
        &mut self,
        _policy_schema_version: SchemaVersionId,
        table: &TableSchema,
        version: &VersionRecord,
        candidate_tx_id: Option<TxId>,
    ) -> Result<Option<CurrentRow>, Error> {
        let subject_table = if self
            .table_in_schema_ref(version.table(), self.catalogue.active_schema.schema)
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
                                self.table_in_schema_ref(parent_version.table(), source_schema)?;
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
