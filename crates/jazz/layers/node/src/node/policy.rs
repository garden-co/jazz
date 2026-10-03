//! Write-policy admission and policy-pinned row projection. Policy predicates,
//! joins, inheritance, reachability, and alternatives execute through the query
//! program in [`super::query_eval`]; this module selects the operation clause,
//! projects old/candidate data into the pinned policy schema, and fail-closes
//! write ingest. It also retains the transaction memo used by view emission.

use super::query_engine::{NormalizedRowSetShape, RowSetExpr};
use super::query_eval::{TransactionOverlayTable, TransactionWriteOverlay};
use super::*;
use crate::protocol::PermissionAdviceAction;
use std::sync::{Arc, Mutex};

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
    /// The row after the transaction. `None` only for a restore whose row
    /// has no committed content to bring back.
    after: Option<CurrentRow>,
    /// The committed row an update replaces. It stays evidence until the
    /// update's own checks pass.
    before: Option<CurrentRow>,
}

impl CandidateEvidenceRow {
    /// Whether this row is an update: its post-state replaces committed
    /// content that ungrounded checks still read.
    fn replaces_committed(&self) -> bool {
        self.before.is_some() && self.after.is_some() && self.before != self.after
    }

    fn table_key(&self) -> (SchemaVersionId, String) {
        (self.policy_schema, self.policy_table.clone())
    }

    /// What a check reads for this row: its post-transaction content once
    /// the row is grounded, otherwise its committed content for an update
    /// and nothing for an insert or restore. A restore with nothing to bring
    /// back shows nothing either way.
    fn shown(&self, grounded: bool) -> Option<CurrentRow> {
        match &self.after {
            None => None,
            Some(after) if grounded => Some(after.clone()),
            Some(_) => self.before.clone(),
        }
    }
}

/// The rows of one candidate commit unit, in `(authored_table, row_uuid)`
/// order.
#[derive(Default)]
struct CandidateUnitEvidence {
    rows: Vec<CandidateEvidenceRow>,
}

type OverlayTables = BTreeMap<(SchemaVersionId, String), Arc<TransactionOverlayTable>>;

impl CandidateUnitEvidence {
    fn row_index(&self, table: &str, row_uuid: RowUuid) -> Option<usize> {
        self.rows
            .binary_search_by(|row| {
                (row.authored_table.as_str(), row.row_uuid).cmp(&(table, row_uuid))
            })
            .ok()
    }

    /// Rebuild the shared overlay tables named by `changed` (every table when
    /// `None`) for the given grounded rows. Rows the unit deletes are not in
    /// the evidence at all: a committed row stays visible as it was, and a
    /// row the unit both inserts and deletes never counts.
    fn rebuild_tables(
        &self,
        tables: &mut OverlayTables,
        grounded: &[bool],
        changed: Option<&BTreeSet<(SchemaVersionId, String)>>,
    ) {
        let mut rebuilt = BTreeMap::<(SchemaVersionId, String), BTreeMap<RowUuid, _>>::new();
        for (index, row) in self.rows.iter().enumerate() {
            let key = row.table_key();
            if changed.is_some_and(|changed| !changed.contains(&key)) {
                continue;
            }
            rebuilt
                .entry(key)
                .or_default()
                .insert(row.row_uuid, row.shown(grounded[index]));
        }
        for (key, rows) in rebuilt {
            tables.insert(key, Arc::new(TransactionOverlayTable::new(rows)));
        }
    }
}

/// Whether a write-policy clause can only grant more as rows are added:
/// existential joins, inner relation joins, reachability, inheritance and
/// row-level filters. `NOT` anywhere, a non-inner relation join, aggregates
/// and limits or offsets count as non-monotone, conservatively.
fn write_policy_query_is_monotone(query: &crate::query::Query) -> bool {
    query.aggregate.is_none()
        && query.limit.is_none()
        && query.offset == 0
        && query.array_subqueries.is_empty()
        && predicates_are_monotone(&query.filters)
        && query.joins.iter().all(join_is_monotone)
        && query.reachable.iter().all(reachable_is_monotone)
        && query.policy_branches.iter().all(|branch| {
            predicates_are_monotone(&branch.filters)
                && branch.joins.iter().all(join_is_monotone)
                && branch.reachable.iter().all(reachable_is_monotone)
        })
        && query
            .relation
            .as_ref()
            .is_none_or(|relation| relation_is_monotone(&relation.rel))
}

fn query_inherits(query: &crate::query::Query) -> bool {
    !query.inherits.is_empty()
        || query
            .policy_branches
            .iter()
            .any(|branch| !branch.inherits.is_empty())
}

fn predicates_are_monotone(predicates: &[crate::query::Predicate]) -> bool {
    predicates.iter().all(predicate_is_monotone)
}

fn predicate_is_monotone(predicate: &crate::query::Predicate) -> bool {
    use crate::query::Predicate;
    match predicate {
        Predicate::Not(_) => false,
        Predicate::All(predicates) | Predicate::Any(predicates) => {
            predicates_are_monotone(predicates)
        }
        Predicate::EnumMatch { payload, .. } => predicate_is_monotone(payload),
        _ => true,
    }
}

fn join_is_monotone(join: &crate::query::JoinVia) -> bool {
    predicates_are_monotone(&join.filters) && join.nested_joins.iter().all(join_is_monotone)
}

fn reachable_is_monotone(reachable: &crate::query::ReachableVia) -> bool {
    predicates_are_monotone(&reachable.access_filters)
        && predicates_are_monotone(&reachable.edge_filters)
}

fn relation_is_monotone(relation: &crate::query::RelationExpr) -> bool {
    use crate::query::{RelationExpr, RelationJoinKind};
    match relation {
        RelationExpr::TableScan { .. } => true,
        RelationExpr::Filter { input, predicate } => {
            relation_is_monotone(input) && relation_predicate_is_monotone(predicate)
        }
        RelationExpr::Union { inputs } => inputs.iter().all(|arm| relation_is_monotone(&arm.input)),
        RelationExpr::Join {
            left,
            right,
            join_kind,
            ..
        } => {
            matches!(join_kind, RelationJoinKind::Inner)
                && relation_is_monotone(left)
                && relation_is_monotone(right)
        }
        RelationExpr::Project { input, .. }
        | RelationExpr::Distinct { input, .. }
        | RelationExpr::OrderBy { input, .. } => relation_is_monotone(input),
        RelationExpr::Gather { seed, step, .. } => {
            relation_is_monotone(seed) && relation_is_monotone(step)
        }
        RelationExpr::Offset { .. } | RelationExpr::Limit { .. } => false,
    }
}

fn relation_predicate_is_monotone(predicate: &crate::query::RelationPredicate) -> bool {
    use crate::query::RelationPredicate;
    match predicate {
        RelationPredicate::Not(_) => false,
        RelationPredicate::And(predicates) | RelationPredicate::Or(predicates) => {
            predicates.iter().all(relation_predicate_is_monotone)
        }
        RelationPredicate::EnumMatch { payload, .. } => relation_predicate_is_monotone(payload),
        _ => true,
    }
}

/// The outcome of deciding every write policy of one commit unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum UnitWritePolicyDecision {
    Allowed,
    Denied,
    /// The unit's policy checks would read more of its own writes than the
    /// evidence budget allows; the message names the unsupported pattern.
    Unsupported(String),
}

/// How many overlaid rows of its own transaction a unit's write-policy
/// checks may read in total, summed over every check. Each check reads the
/// unit's rows in the tables its policy reads, so a unit whose policies read
/// its own large table grows quadratically; above this bound the unit is
/// rejected as not supported yet rather than evaluated at unbounded cost.
pub(super) const MAX_TRANSACTION_POLICY_EVIDENCE_ROWS: usize = 1 << 18;

fn unsupported_transaction_policy_evidence() -> UnitWritePolicyDecision {
    UnitWritePolicyDecision::Unsupported(format!(
        "Reading more than {MAX_TRANSACTION_POLICY_EVIDENCE_ROWS} rows of a transaction's own \
         writes in its write-policy checks is not supported yet"
    ))
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

/// Operation selection is reused by candidate capability dispatch and policy
/// execution. A table's unselected INSERT clause cannot route an UPDATE.
pub(in crate::node) struct SelectedWritePolicyVersion {
    pub(in crate::node) schema: SchemaVersionId,
    pub(in crate::node) table: TableSchema,
    pub(in crate::node) row: RowUuid,
    pub(in crate::node) cells: BTreeMap<String, Value>,
    pub(in crate::node) provenance: RowProvenance,
    operation: SelectedWriteOperation,
}

enum SelectedWriteOperation {
    Insert,
    Update(CurrentRow),
    Delete(CurrentRow),
    Denied,
}

impl SelectedWritePolicyVersion {
    pub(in crate::node) fn is_insert(&self) -> bool {
        matches!(self.operation, SelectedWriteOperation::Insert)
    }

    pub(in crate::node) fn uses_authorized_created_sources(&self) -> bool {
        self.is_insert()
            && self
                .table
                .write_policies
                .insert_check
                .as_ref()
                .is_some_and(JazzQuery::uses_authorized_created_sources)
    }
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
    /// state overlaid with the unit's other inserts, restores and updates;
    /// the unit's deletes are not overlaid, and the written row itself is
    /// the inline candidate. A commit unit carries its writes as a canonical
    /// set, not in write order, so the checks run to a fixpoint: a row's
    /// post-transaction content becomes evidence once all of its own checks
    /// pass ("grounded"), until then an update shows its committed content
    /// and an insert or restore shows nothing, so writes cannot justify each
    /// other in a cycle. When the fixpoint passes every write, each write
    /// that passed before all the unit rows it reads were grounded is checked
    /// once more against the unit's full post-state, and the unit is accepted
    /// only if those checks pass too. USING clauses judge the rows the
    /// transaction acts on as committed. Other transactions' writes are never
    /// evidence.
    ///
    /// Each round shares one overlay per table among its checks, and a
    /// failed write is re-checked only when a table its policy reads gained
    /// grounded rows. A unit whose checks would read more than
    /// [`MAX_TRANSACTION_POLICY_EVIDENCE_ROWS`] overlaid rows in total is
    /// not supported yet.
    pub(super) async fn commit_unit_write_policies_allow(
        &mut self,
        versions: &[VersionRecord],
        author: AuthorSubject,
        tx: &Transaction,
    ) -> Result<UnitWritePolicyDecision, Error> {
        let mut created = if author == AuthorSubject::SYSTEM {
            None
        } else {
            match self
                .prepare_authorized_created_evidence(tx, versions, author)
                .await
            {
                Ok(evidence) => evidence,
                Err(Error::QueryCapability(_)) => return Ok(UnitWritePolicyDecision::Denied),
                Err(error) => return Err(error),
            }
        };
        let evidence = Box::pin(self.candidate_unit_evidence(versions, author, tx.tx_id)).await?;
        let mut post_state = None;
        let decision = match self
            .ground_commit_unit_write_policies(
                versions,
                author,
                tx.tx_id,
                &evidence,
                created.as_mut(),
                &mut post_state,
            )
            .await
        {
            Ok(decision) => decision,
            Err(Error::QueryCapability(_)) if created.is_some() => {
                return Ok(UnitWritePolicyDecision::Denied);
            }
            Err(error) => return Err(error),
        };
        if decision != UnitWritePolicyDecision::Allowed {
            return Ok(decision);
        }
        if let Some(created) = created.as_mut()
            && evidence
                .rows
                .iter()
                .any(CandidateEvidenceRow::replaces_committed)
        {
            let overlay = match post_state {
                Some(overlay) => overlay,
                None => {
                    let mut tables = OverlayTables::new();
                    evidence.rebuild_tables(&mut tables, &vec![true; evidence.rows.len()], None);
                    TransactionWriteOverlay::from_tables(Arc::new(tables))
                }
            };
            match self
                .revalidate_authorized_created_evidence(created, author, &overlay)
                .await
            {
                Ok(true) => {}
                Ok(false) | Err(Error::QueryCapability(_)) => {
                    return Ok(UnitWritePolicyDecision::Denied);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(UnitWritePolicyDecision::Allowed)
    }

    async fn ground_commit_unit_write_policies(
        &mut self,
        versions: &[VersionRecord],
        author: AuthorSubject,
        candidate_tx_id: TxId,
        evidence: &CandidateUnitEvidence,
        mut created: Option<&mut super::query_eval::AuthorizedCreatedEvidence>,
        post_state: &mut Option<TransactionWriteOverlay>,
    ) -> Result<UnitWritePolicyDecision, Error> {
        if evidence.rows.is_empty() {
            // No write can see another unit row: committed state alone
            // decides each write.
            for version in versions {
                #[cfg(any(test, feature = "testing"))]
                WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(count.get() + 1));
                if !Box::pin(self.write_policy_allows_version_record_for_view(
                    version,
                    author,
                    None,
                    Some(candidate_tx_id),
                    versions,
                    &TransactionWriteOverlay::accepted_state(),
                ))
                .await?
                {
                    return Ok(UnitWritePolicyDecision::Denied);
                }
            }
            return Ok(UnitWritePolicyDecision::Allowed);
        }

        let own_row = versions
            .iter()
            .map(|version| {
                if version.branch_key().values.is_empty() {
                    evidence.row_index(version.table(), version.row_uuid())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        // Versions still to pass per row, and per table the rows not yet
        // grounded whose post-state differs from what an ungrounded row shows.
        let mut unpassed = vec![0usize; evidence.rows.len()];
        for own in own_row.iter().flatten() {
            unpassed[*own] += 1;
        }
        // `ungrounded_live`: rows whose post-state differs from what they show
        // while ungrounded (inserts, restores and updates). `ungrounded_updates`:
        // the subset that replaces committed content (updates), the only rows
        // whose grounding can take evidence away from a monotone policy.
        let mut ungrounded_live = BTreeMap::<(SchemaVersionId, String), usize>::new();
        let mut ungrounded_updates = BTreeMap::<(SchemaVersionId, String), usize>::new();
        for row in &evidence.rows {
            if row.after.is_some() {
                *ungrounded_live.entry(row.table_key()).or_default() += 1;
            }
            if row.replaces_committed() {
                *ungrounded_updates.entry(row.table_key()).or_default() += 1;
            }
        }
        // Whether a passing check already saw everything the post-state
        // could change for it. A monotone policy can only lose a pass to an
        // update grounded later; any other policy to any ungrounded row.
        let saw_post_state =
            |own: Option<usize>,
             monotone: bool,
             reads: &BTreeSet<(SchemaVersionId, String)>,
             grounded: &[bool],
             ungrounded_live: &BTreeMap<(SchemaVersionId, String), usize>,
             ungrounded_updates: &BTreeMap<(SchemaVersionId, String), usize>| {
                reads.iter().all(|table| {
                    let own_row = own
                        .filter(|own| !grounded[*own])
                        .map(|own| &evidence.rows[own])
                        .filter(|row| row.table_key() == *table);
                    if monotone {
                        let own_counted = own_row.is_some_and(|row| row.replaces_committed());
                        ungrounded_updates.get(table).copied().unwrap_or(0)
                            == usize::from(own_counted)
                    } else {
                        let own_counted = own_row.is_some_and(|row| row.after.is_some());
                        ungrounded_live.get(table).copied().unwrap_or(0) == usize::from(own_counted)
                    }
                })
            };
        let monotone = self.unit_write_policies_monotone(versions)?;
        let mut grounded = vec![false; evidence.rows.len()];
        let mut post_state_checked = vec![false; versions.len()];
        let mut reads = vec![BTreeSet::<(SchemaVersionId, String)>::new(); versions.len()];
        // The first round has nothing grounded, so every unit row shows what
        // committed state already shows: it runs on committed state alone,
        // with no overlaid rows and nothing charged to the evidence budget,
        // and only records which tables each policy reads.
        let mut tables = OverlayTables::new();
        let mut first_round = true;
        let mut spent = 0usize;
        let mut failed = BTreeSet::new();
        let mut pending = (0..versions.len()).collect::<Vec<_>>();
        loop {
            let overlay = TransactionWriteOverlay::from_tables(Arc::new(tables.clone()));
            let mut newly_grounded = Vec::new();
            for index in pending {
                let Some(allowed) = Box::pin(self.unit_write_policy_check(
                    versions,
                    index,
                    own_row[index],
                    &evidence,
                    &overlay,
                    author,
                    candidate_tx_id,
                    &mut reads[index],
                    &mut spent,
                    &own_row,
                    &grounded,
                    created.as_deref_mut(),
                ))
                .await?
                else {
                    return Ok(unsupported_transaction_policy_evidence());
                };
                if allowed {
                    failed.remove(&index);
                    post_state_checked[index] = saw_post_state(
                        own_row[index],
                        monotone[index],
                        &reads[index],
                        &grounded,
                        &ungrounded_live,
                        &ungrounded_updates,
                    );
                    // A row is grounded once every version the unit writes
                    // for it has passed; it becomes evidence next round.
                    if let Some(own) = own_row[index] {
                        unpassed[own] -= 1;
                        if unpassed[own] == 0 {
                            newly_grounded.push(own);
                        }
                    }
                } else {
                    failed.insert(index);
                }
            }
            if failed.is_empty() {
                break;
            }
            let mut changed = BTreeSet::new();
            for own in newly_grounded {
                grounded[own] = true;
                let row = &evidence.rows[own];
                if row.after.is_some() {
                    changed.insert(row.table_key());
                    if let Some(count) = ungrounded_live.get_mut(&row.table_key()) {
                        *count -= 1;
                    }
                }
                if row.replaces_committed()
                    && let Some(count) = ungrounded_updates.get_mut(&row.table_key())
                {
                    *count -= 1;
                }
            }
            // Only a table that gained grounded rows can change a failed
            // check's evidence. Without one, every failure is final.
            if changed.is_empty() {
                return Ok(UnitWritePolicyDecision::Denied);
            }
            if first_round {
                evidence.rebuild_tables(&mut tables, &grounded, None);
                first_round = false;
            } else {
                evidence.rebuild_tables(&mut tables, &grounded, Some(&changed));
            }
            pending = failed
                .iter()
                .copied()
                .filter(|index| !reads[*index].is_disjoint(&changed))
                .collect();
        }

        // Every write passed, so every row is grounded and the tables now
        // show the unit's post-state. Re-check each write whose passing check
        // read a unit row that was not grounded yet.
        let final_pass = (0..versions.len())
            .filter(|index| !post_state_checked[*index])
            .collect::<Vec<_>>();
        if final_pass.is_empty() {
            return Ok(UnitWritePolicyDecision::Allowed);
        }
        grounded.fill(true);
        evidence.rebuild_tables(&mut tables, &grounded, None);
        let overlay = TransactionWriteOverlay::from_tables(Arc::new(tables));
        if created.is_some() {
            *post_state = Some(overlay.clone());
        }
        for index in final_pass {
            let Some(allowed) = Box::pin(self.unit_write_policy_check(
                versions,
                index,
                own_row[index],
                &evidence,
                &overlay,
                author,
                candidate_tx_id,
                &mut reads[index],
                &mut spent,
                &own_row,
                &grounded,
                created.as_deref_mut(),
            ))
            .await?
            else {
                return Ok(unsupported_transaction_policy_evidence());
            };
            if !allowed {
                return Ok(UnitWritePolicyDecision::Denied);
            }
        }
        Ok(UnitWritePolicyDecision::Allowed)
    }

    /// Whether each version's WITH CHECK policies are monotone in the rows
    /// present (`INV-RLS-9`), decided statically per policy table and cached
    /// per unit. A table whose insert or update check uses a non-monotone
    /// construct, or inherits through a schema that has one, is not.
    fn unit_write_policies_monotone(
        &mut self,
        versions: &[VersionRecord],
    ) -> Result<Vec<bool>, Error> {
        let mut by_table = BTreeMap::<(SchemaVersionId, String), bool>::new();
        let mut monotone = Vec::with_capacity(versions.len());
        for version in versions {
            let key = (version.schema_version(), version.table().to_owned());
            if let Some(known) = by_table.get(&key) {
                monotone.push(*known);
                continue;
            }
            let (policy_schema, table, _) = self.policy_projection_for_version_record(version)?;
            let checks = [
                table.write_policies.insert_check.as_ref(),
                table.write_policies.update_check.as_ref(),
            ];
            let mut known = checks
                .iter()
                .flatten()
                .all(|policy| write_policy_query_is_monotone(policy));
            if known && checks.iter().flatten().any(|policy| query_inherits(policy)) {
                // An inherited clause evaluates another table's policy; accept
                // it as monotone only when every policy of the schema is.
                known = self
                    .policy_schema_for_monotonicity(policy_schema)
                    .is_some_and(|schema| {
                        schema.tables.iter().all(|table| {
                            table
                                .read_policy
                                .iter()
                                .chain(table.write_policies.iter().map(|(_, policy)| policy))
                                .all(write_policy_query_is_monotone)
                        })
                    });
            }
            by_table.insert(key, known);
            monotone.push(known);
        }
        Ok(monotone)
    }

    fn policy_schema_for_monotonicity(&self, schema: SchemaVersionId) -> Option<&JazzSchema> {
        if schema == self.catalogue.active_schema.schema {
            Some(&self.catalogue.active_schema.compiled)
        } else if schema == self.catalogue.local_schema_version_id {
            Some(&self.catalogue.schema)
        } else {
            self.catalogue
                .catalogue_schemas
                .get(&schema)
                .map(|entry| &entry.schema)
        }
    }

    /// Check one write of a unit against a round's shared overlay, leaving
    /// its own row out by lookup and recording the tables its policy reads.
    /// Returns `None` once the unit's evidence budget is spent.
    #[allow(clippy::too_many_arguments)]
    async fn unit_write_policy_check(
        &mut self,
        versions: &[VersionRecord],
        index: usize,
        own_row: Option<usize>,
        evidence: &CandidateUnitEvidence,
        overlay: &TransactionWriteOverlay,
        author: AuthorSubject,
        candidate_tx_id: TxId,
        reads: &mut BTreeSet<(SchemaVersionId, String)>,
        spent: &mut usize,
        own_rows: &[Option<usize>],
        grounded: &[bool],
        created: Option<&mut super::query_eval::AuthorizedCreatedEvidence>,
    ) -> Result<Option<bool>, Error> {
        if *spent > MAX_TRANSACTION_POLICY_EVIDENCE_ROWS {
            return Ok(None);
        }
        let version = &versions[index];
        // Branch-local policy checks never borrow main-branch writes from the
        // same unit, through either the ordinary overlay or marked sources.
        let accepted = TransactionWriteOverlay::accepted_state();
        let branch_local = !version.branch_key().values.is_empty();
        let overlay = if branch_local { &accepted } else { overlay };
        let created = if branch_local { None } else { created };
        let recorder = Arc::new(Mutex::new(BTreeSet::new()));
        let mut overlay = overlay.recording(Arc::clone(&recorder));
        if let Some(own) = own_row {
            let row = &evidence.rows[own];
            overlay = overlay.excluding(row.policy_schema, &row.policy_table, row.row_uuid);
        }
        #[cfg(any(test, feature = "testing"))]
        WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(count.get() + 1));
        let allowed = if let Some(created) = created
            && created.selected[index].uses_authorized_created_sources()
        {
            if !created.allowed[index] {
                false
            } else {
                let selection = &created.selected[index];
                let policy = selection
                    .table
                    .write_policies
                    .insert_check
                    .as_ref()
                    .expect("selected marked insert policy");
                self.charge_candidate_policy(selection.schema, policy, true, &mut created.budget)?;
                let mut sources = BTreeMap::new();
                for (source, rows) in &created.sources[index] {
                    created.budget.charge(rows.len())?;
                    let rows = rows
                        .iter()
                        .filter(|&(dependency, _)| {
                            own_rows[*dependency].is_some_and(|own| grounded[own])
                        })
                        .map(|(_, row)| row.clone())
                        .collect::<Vec<_>>();
                    if !rows.is_empty() {
                        sources.insert(source.clone(), rows);
                    }
                }
                match self
                    .policy_query_allows_candidate_with_provenance_for_schema(
                        selection.schema,
                        &selection.table,
                        policy,
                        selection.row,
                        &selection.cells,
                        author,
                        true,
                        selection.provenance,
                        super::query_engine::PolicyDecisionRole::Write,
                        Some(sources),
                        &overlay.accepted_updates(),
                    )
                    .await
                {
                    Ok(allowed) => allowed,
                    Err(Error::QueryCapability(_)) => false,
                    Err(error) => return Err(error),
                }
            }
        } else {
            Box::pin(self.write_policy_allows_version_record_for_view(
                version,
                author,
                None,
                Some(candidate_tx_id),
                versions,
                &overlay,
            ))
            .await?
        };
        *reads = std::mem::take(
            &mut *recorder
                .lock()
                .map_err(|_| Error::InvalidStoredValue("transaction overlay reads poisoned"))?,
        );
        *spent += overlay.rows_in(reads);
        Ok(Some(allowed))
    }

    /// Prepare the main-branch rows of a candidate unit as evidence for its
    /// other writes. A unit that writes one row has nothing to overlay. A row
    /// the unit deletes contributes nothing, so committed state shows it as
    /// committed (and a row the unit both inserts and deletes not at all).
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
            if deleted {
                continue;
            }
            let subject = content.unwrap_or(row_versions[0]);
            let (policy_schema, table, cells) =
                self.policy_projection_for_version_record(subject)?;
            let (after, before) = {
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
        let selected = self
            .select_version_write_policy(version, exact_view, candidate_tx_id, candidate_versions)
            .await?;
        self.evaluate_selected_write_policy(&selected, author, false, None, transaction_overlay)
            .await
    }

    pub(in crate::node) async fn select_version_write_policy(
        &mut self,
        version: &VersionRecord,
        exact_view: Option<&JazzSchema>,
        candidate_tx_id: Option<TxId>,
        candidate_versions: &[VersionRecord],
    ) -> Result<SelectedWritePolicyVersion, Error> {
        let (schema, table, cells) = if let Some(schema) = exact_view {
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
        let operation = if version.deletion() == Some(DeletionEvent::Deleted) {
            if table.write_policies.delete_using.is_none() {
                SelectedWriteOperation::Denied
            } else {
                match self
                    .policy_delete_subject_row(
                        schema,
                        &table,
                        version,
                        candidate_tx_id,
                        candidate_versions,
                    )
                    .await?
                {
                    Some(current) => SelectedWriteOperation::Delete(current),
                    None => SelectedWriteOperation::Denied,
                }
            }
        } else {
            match self
                .policy_previous_content_subject_row(schema, &table, version, candidate_tx_id)
                .await?
            {
                Some(previous) => SelectedWriteOperation::Update(previous),
                None => SelectedWriteOperation::Insert,
            }
        };
        Ok(SelectedWritePolicyVersion {
            schema,
            table,
            cells,
            operation,
            row: version.row_uuid(),
            provenance: version_provenance(version),
        })
    }

    pub(in crate::node) async fn evaluate_selected_write_policy(
        &mut self,
        selected: &SelectedWritePolicyVersion,
        author: AuthorSubject,
        global_only: bool,
        mut budget: Option<&mut super::query_eval::CandidateProofBudget>,
        transaction_overlay: &TransactionWriteOverlay,
    ) -> Result<bool, Error> {
        if author == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        let table = &selected.table;
        match &selected.operation {
            SelectedWriteOperation::Denied => Ok(false),
            SelectedWriteOperation::Insert => {
                let Some(policy) = &table.write_policies.insert_check else {
                    return Ok(false);
                };
                self.write_policy_query_allows_candidate_in_proof_scope(
                    selected.schema,
                    table,
                    policy,
                    selected.row,
                    &selected.cells,
                    author,
                    true,
                    selected.provenance,
                    global_only,
                    budget,
                    transaction_overlay,
                )
                .await
            }
            SelectedWriteOperation::Delete(current) => {
                let policy = table
                    .write_policies
                    .delete_using
                    .as_ref()
                    .expect("selected delete policy");
                let cells = table
                    .columns
                    .iter()
                    .filter_map(|column| {
                        current
                            .cell(table, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect();
                self.write_policy_query_allows_candidate_in_proof_scope(
                    selected.schema,
                    table,
                    policy,
                    current.row_uuid(),
                    &cells,
                    author,
                    false,
                    current.provenance()?.unwrap_or_else(unresolved_provenance),
                    global_only,
                    budget,
                    &transaction_overlay.committed_view(),
                )
                .await
            }
            SelectedWriteOperation::Update(previous) => {
                let mut cells = table
                    .columns
                    .iter()
                    .filter_map(|column| {
                        previous
                            .cell(table, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect::<BTreeMap<_, _>>();
                let provenance = previous.provenance()?.unwrap_or_else(unresolved_provenance);
                if let Some(policy) = &table.write_policies.update_using {
                    if !self
                        .write_policy_query_allows_candidate_in_proof_scope(
                            selected.schema,
                            table,
                            policy,
                            previous.row_uuid(),
                            &cells,
                            author,
                            false,
                            provenance,
                            global_only,
                            budget.as_deref_mut(),
                            &transaction_overlay.committed_view(),
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
                let Some(policy) = &table.write_policies.update_check else {
                    return Ok(true);
                };
                cells.extend(selected.cells.clone());
                self.write_policy_query_allows_candidate_in_proof_scope(
                    selected.schema,
                    table,
                    policy,
                    selected.row,
                    &cells,
                    author,
                    false,
                    RowProvenance {
                        created_by: provenance.created_by,
                        created_at: provenance.created_at,
                        updated_by: selected.provenance.updated_by,
                        updated_at: selected.provenance.updated_at,
                    },
                    global_only,
                    budget,
                    transaction_overlay,
                )
                .await
            }
        }
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
        current_row_from_materialized_cells(table, &version, &cells).map(Some)
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

    pub(super) fn policy_projection_for_version_record(
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
                return current_row_from_materialized_cells(table, &parent_version, &cells)
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
                return current_row_from_materialized_cells(table, &current_version, &cells)
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
                    return current_row_from_materialized_cells(table, &current_version, &cells)
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

pub(in crate::node) fn policy_tables_are_directly_compatible(
    source: &TableSchema,
    target: &TableSchema,
) -> bool {
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
