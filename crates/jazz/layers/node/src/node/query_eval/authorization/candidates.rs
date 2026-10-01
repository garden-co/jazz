//! Monotone, commit-local INSERT proofs. The query engine remains the only
//! predicate evaluator; this module schedules complete policy evaluations and
//! grants individual source occurrences already-authorized evidence.

use super::*;
use crate::query::CandidateSourceMode;
use std::collections::VecDeque;

const MAX_CANDIDATE_DEPENDENCIES: usize = 16_384;
const MAX_CANDIDATE_PROOF_WORK: usize = 16_384;
const PROOF_LIMIT: &str = "authorized-created proof exceeds its bounded work or dependency limit";

pub(in crate::node) struct CandidateProofBudget(usize);

impl CandidateProofBudget {
    pub(in crate::node) fn charge(&mut self, work: usize) -> Result<(), Error> {
        self.0 = self
            .0
            .checked_sub(work)
            .ok_or_else(|| Error::QueryCapability(PROOF_LIMIT.into()))?;
        Ok(())
    }

    fn predicate(&mut self, predicate: &Predicate) -> Result<(), Error> {
        self.charge(1)?;
        match predicate {
            Predicate::All(children) | Predicate::Any(children) => {
                for child in children {
                    self.predicate(child)?;
                }
            }
            Predicate::Not(child) | Predicate::EnumMatch { payload: child, .. } => {
                self.predicate(child)?
            }
            _ => {}
        }
        Ok(())
    }

    fn joins(&mut self, joins: &[JoinVia]) -> Result<(), Error> {
        for join in joins {
            self.charge(1 + join.correlated_filters.len())?;
            for predicate in &join.filters {
                self.predicate(predicate)?;
            }
            self.joins(&join.nested_joins)?;
        }
        Ok(())
    }

    pub(in crate::node) fn policy(&mut self, policy: &JazzQuery) -> Result<(), Error> {
        self.charge(1)?;
        for predicate in &policy.filters {
            self.predicate(predicate)?;
        }
        self.joins(&policy.joins)?;
        self.charge(policy.inherits.len())?;
        for reachable in &policy.reachable {
            self.charge(1)?;
            for predicate in reachable
                .access_filters
                .iter()
                .chain(&reachable.edge_filters)
            {
                self.predicate(predicate)?;
            }
            if let Some(seed) = &reachable.seed {
                for predicate in &seed.filters {
                    self.predicate(predicate)?;
                }
            }
        }
        for branch in &policy.policy_branches {
            self.charge(1 + branch.inherits.len())?;
            for predicate in &branch.filters {
                self.predicate(predicate)?;
            }
            self.joins(&branch.joins)?;
            for reachable in &branch.reachable {
                self.charge(1)?;
                for predicate in reachable
                    .access_filters
                    .iter()
                    .chain(&reachable.edge_filters)
                {
                    self.predicate(predicate)?;
                }
                if let Some(seed) = &reachable.seed {
                    for predicate in &seed.filters {
                        self.predicate(predicate)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn policy_tree(
        &mut self,
        policy: &JazzQuery,
        schema: &RuntimeSchema,
        insert_candidate: bool,
        path: &super::super::normalization::InheritanceExpansionPath,
    ) -> Result<(), Error> {
        self.policy(policy)?;
        let table = schema
            .tables
            .iter()
            .find(|table| table.name == policy.table)
            .ok_or_else(|| Error::TableNotFound(policy.table.clone()))?;
        for inherits in policy.inherits.iter().chain(
            policy
                .policy_branches
                .iter()
                .flat_map(|branch| &branch.inherits),
        ) {
            self.charge(1)?;
            let mut inherits = inherits.clone();
            if insert_candidate && inherits.operation == crate::query::InheritsOperation::Select {
                inherits.operation = crate::query::InheritsOperation::Update;
            }
            let Some(path) = path.descend(&table.name, &inherits) else {
                continue;
            };
            let parent_name = table
                .references
                .get(&inherits.parent_column)
                .ok_or_else(|| {
                    Error::QueryCapability("candidate inheritance reference is unavailable".into())
                })?;
            let parent = schema
                .tables
                .iter()
                .find(|table| &table.name == parent_name)
                .ok_or_else(|| Error::TableNotFound(parent_name.clone()))?;
            let policy = match inherits.operation {
                crate::query::InheritsOperation::Select => parent.read_policy.as_ref(),
                crate::query::InheritsOperation::Insert => {
                    parent.write_policies.insert_check.as_ref()
                }
                crate::query::InheritsOperation::Update => {
                    parent.write_policies.update_using.as_ref()
                }
                crate::query::InheritsOperation::Delete => {
                    parent.write_policies.delete_using.as_ref()
                }
            };
            if let Some(policy) = policy {
                self.policy_tree(policy, schema, false, &path)?;
            }
        }
        Ok(())
    }
}

struct Candidate<'a> {
    selection: &'a crate::node::policy::SelectedWritePolicyVersion,
    policy: Option<JazzQuery>,
    physical: PhysicalTableId,
    version_index: usize,
    baseline_allowed: bool,
}

// Indexed once per consumer schema/table/key, rather than scanning the complete
// transaction for each edge. Values are indices into the projected table rows.
#[derive(Default)]
struct CandidateIndex {
    by_table: BTreeMap<PhysicalTableId, Vec<usize>>,
    rows: BTreeMap<(SchemaVersionId, String), Vec<(usize, CurrentRow)>>,
    keys: BTreeMap<(SchemaVersionId, String, String), BTreeMap<uuid::Uuid, Vec<usize>>>,
}

type OccurrenceDependencies = BTreeMap<SourceId, Vec<(usize, CurrentRow)>>;

pub(in crate::node) type AuthorizedCreatedSources = BTreeMap<SourceId, Vec<(usize, CurrentRow)>>;

/// Unit-local proof ceiling and occurrence capabilities. These do not admit
/// ordinary writes; the canonical unit evaluator still grounds every row.
pub(in crate::node) struct AuthorizedCreatedEvidence {
    pub(in crate::node) selected: Vec<crate::node::policy::SelectedWritePolicyVersion>,
    pub(in crate::node) allowed: Vec<bool>,
    pub(in crate::node) sources: Vec<AuthorizedCreatedSources>,
    qualified: Vec<bool>,
    pub(in crate::node) budget: CandidateProofBudget,
}

fn row_value(row: &CurrentRow, table: &TableSchema, column: &str) -> Option<Value> {
    if column == "id" {
        Some(Value::Uuid(row.row_uuid().0))
    } else {
        row.cell(table, column)
    }
}

fn scalar_uuid(value: &Value) -> Option<uuid::Uuid> {
    match value {
        Value::Uuid(value) => Some(*value),
        Value::Nullable(Some(value)) => scalar_uuid(value),
        _ => None,
    }
}

fn has_created(joins: &[JoinVia], budget: &mut CandidateProofBudget) -> Result<bool, Error> {
    for join in joins {
        budget.charge(1)?;
        if join.source_mode == CandidateSourceMode::IncludeAuthorizedCreatedV1
            || has_created(&join.nested_joins, budget)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

impl<S: OrderedKvStorage> NodeState<S> {
    pub(in crate::node) fn charge_candidate_policy(
        &self,
        schema: SchemaVersionId,
        policy: &JazzQuery,
        insert_candidate: bool,
        budget: &mut CandidateProofBudget,
    ) -> Result<(), Error> {
        let schema = if schema == self.catalogue.active_schema.schema {
            &self.catalogue.active_schema.compiled
        } else if schema == self.catalogue.local_schema_version_id {
            &self.catalogue.schema
        } else {
            &self
                .catalogue
                .catalogue_schemas
                .get(&schema)
                .ok_or(Error::InvalidStoredValue("policy schema payload missing"))?
                .schema
        };
        budget.policy_tree(
            policy,
            schema.runtime(),
            insert_candidate,
            &super::super::normalization::InheritanceExpansionPath::default(),
        )
    }

    /// A conservative schema-owned prefilter keeps all-unmarked schemas on
    /// their existing path. Exact operation selection, never this flag, grants
    /// entry to the candidate proof.
    pub(in crate::node) async fn select_possible_created_policy_versions(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
    ) -> Result<Option<Vec<crate::node::policy::SelectedWritePolicyVersion>>, Error> {
        let catalogue = &self.catalogue;
        let possible = catalogue
            .active_schema
            .compiled
            .may_have_authorized_created_insert_sources()
            || catalogue
                .schema
                .may_have_authorized_created_insert_sources()
            || versions.iter().any(|version| {
                catalogue
                    .catalogue_schemas
                    .get(&version.schema_version())
                    .is_some_and(|schema| {
                        schema.schema.may_have_authorized_created_insert_sources()
                    })
            });
        if !possible {
            return Ok(None);
        }
        let mut selected = Vec::with_capacity(versions.len());
        for version in versions {
            selected.push(
                self.select_version_write_policy(version, None, Some(tx.tx_id), versions)
                    .await?,
            );
        }
        Ok(Some(selected))
    }

    pub(in crate::node) async fn prepare_authorized_created_evidence(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        identity: AuthorSubject,
    ) -> Result<Option<AuthorizedCreatedEvidence>, Error> {
        let Some(selected) = self
            .select_possible_created_policy_versions(tx, versions)
            .await?
        else {
            return Ok(None);
        };
        if !selected
            .iter()
            .any(|version| version.uses_authorized_created_sources())
        {
            return Ok(None);
        }
        let mut evidence = AuthorizedCreatedEvidence {
            allowed: vec![false; selected.len()],
            sources: vec![BTreeMap::new(); selected.len()],
            qualified: vec![false; selected.len()],
            selected,
            budget: CandidateProofBudget(MAX_CANDIDATE_PROOF_WORK),
        };
        self.authorized_created_commit_proof(tx, versions, identity, &mut evidence)
            .await?;
        Ok(Some(evidence))
    }

    async fn authorized_created_commit_proof(
        &mut self,
        tx: &Transaction,
        versions: &[VersionRecord],
        identity: AuthorSubject,
        evidence: &mut AuthorizedCreatedEvidence,
    ) -> Result<(), Error> {
        let selected = &evidence.selected;
        let budget = &mut evidence.budget;
        let mut baseline_allowed = vec![false; selected.len()];
        // Only selected marked INSERTs take this strict baseline. Ordinary
        // writes are decided by the modern unit grounding/post-state evaluator.
        for (index, selection) in selected.iter().enumerate() {
            if selection.uses_authorized_created_sources() {
                baseline_allowed[index] = self
                    .evaluate_selected_write_policy(
                        selection,
                        identity,
                        true,
                        Some(&mut *budget),
                        &TransactionWriteOverlay::accepted_state(),
                    )
                    .await?;
            }
        }
        evidence.allowed.clone_from(&baseline_allowed);
        if tx.kind != TxKind::Exclusive
            || versions.is_empty()
            || versions.len() > crate::protocol_limits::MAX_COMMIT_UNIT_VERSIONS
            || versions.len() != tx.n_total_writes as usize
            || !tx.has_complete_exclusive_evidence()
            || !self.validate_exclusive_commit_unit(tx, versions).await?
        {
            return Ok(());
        }
        let mut candidates = Vec::with_capacity(versions.len());
        let mut coordinates = BTreeSet::new();
        let mut duplicates = BTreeSet::new();
        for version in versions {
            budget.charge(1)?;
            let coordinate = (
                self.physical_table_id_for_schema(version.schema_version(), version.table())?,
                version.row_uuid(),
            );
            if !coordinates.insert(coordinate) {
                duplicates.insert(coordinate);
            }
        }
        let mut index = CandidateIndex::default();
        let mut absent_coordinates = BTreeSet::new();
        for absent in tx.absent_read_set.as_ref().expect("complete evidence") {
            budget.charge(1)?;
            for (schema, mapping) in &self.catalogue.physical_mappings {
                budget.charge(1)?;
                if mapping.tables.contains_key(&absent.table) {
                    absent_coordinates.insert((
                        self.physical_table_id_for_schema(*schema, &absent.table)?,
                        absent.row_uuid,
                    ));
                }
            }
        }
        for (version_index, ((version, selection), allowed)) in versions
            .iter()
            .zip(selected)
            .zip(baseline_allowed)
            .enumerate()
        {
            budget.charge(1)?;
            let physical =
                self.physical_table_id_for_schema(version.schema_version(), version.table())?;
            let coordinate = (physical, version.row_uuid());
            let eligible = selection.is_insert()
                && version.branch_key().values.is_empty()
                && version.parents().is_empty()
                && version.deletion().is_none()
                && !duplicates.contains(&coordinate)
                && absent_coordinates.contains(&coordinate);
            if !eligible {
                continue;
            }
            let Some(policy) = selection.table.write_policies.insert_check.as_ref() else {
                continue;
            };
            let policy = if !selection.uses_authorized_created_sources() {
                None
            } else {
                budget.policy(policy)?;
                Some(
                    self.candidate_policy_shape_for_schema(selection.schema, policy, true)?
                        .query()
                        .clone(),
                )
            };
            index
                .by_table
                .entry(physical)
                .or_default()
                .push(candidates.len());
            candidates.push(Candidate {
                selection,
                policy,
                version_index,
                physical,
                baseline_allowed: allowed,
            });
        }
        let mut authorized_candidate_evidence = candidates
            .iter()
            .map(|candidate| {
                candidate.baseline_allowed && candidate.selection.uses_authorized_created_sources()
            })
            .collect::<Vec<_>>();
        let mut dependencies = Vec::with_capacity(candidates.len());
        let mut dependents = vec![Vec::new(); candidates.len()];
        let mut dependency_count = 0;
        let mut needed_witnesses = BTreeSet::new();
        for (candidate_id, candidate) in candidates.iter().enumerate() {
            if !candidate.selection.uses_authorized_created_sources() {
                dependencies.push(BTreeMap::new());
                continue;
            }
            let selection = candidate.selection;
            let policy = candidate.policy.as_ref().expect("selected marked policy");
            let root = current_row_from_cells_with_explicit_provenance(
                &selection.table,
                selection.row,
                &selection.cells,
                selection.provenance,
                None,
            )?;
            let mut occurrences = BTreeMap::new();
            let prefix = if policy.policy_branches.is_empty() {
                "query"
            } else {
                "policy_branch:base"
            };
            self.candidate_join_dependencies(
                candidate,
                &candidates,
                &mut index,
                budget,
                &mut dependency_count,
                &selection.table,
                std::slice::from_ref(&root),
                &policy.joins,
                prefix,
                &mut occurrences,
            )
            .await?;
            for (branch_id, branch) in policy.policy_branches.iter().enumerate() {
                self.candidate_join_dependencies(
                    candidate,
                    &candidates,
                    &mut index,
                    budget,
                    &mut dependency_count,
                    &selection.table,
                    std::slice::from_ref(&root),
                    &branch.joins,
                    &format!("policy_branch:{branch_id}"),
                    &mut occurrences,
                )
                .await?;
            }
            let mut seen = BTreeSet::new();
            for entries in occurrences.values() {
                for (dependency, _) in entries {
                    budget.charge(1)?;
                    needed_witnesses.insert(*dependency);
                    if seen.insert(*dependency) {
                        dependents[*dependency].push(candidate_id);
                    }
                }
            }
            dependencies.push(occurrences);
        }
        // An ordinary write can witness a marked occurrence only after its
        // entire selected policy qualifies against accepted Global evidence.
        for id in needed_witnesses {
            budget.charge(1)?;
            let candidate = &candidates[id];
            if !candidate.selection.uses_authorized_created_sources() {
                authorized_candidate_evidence[id] = self
                    .evaluate_selected_write_policy(
                        candidate.selection,
                        identity,
                        true,
                        Some(&mut *budget),
                        &TransactionWriteOverlay::accepted_state(),
                    )
                    .await?;
            }
        }
        let mut queued = vec![false; candidates.len()];
        let mut queue = VecDeque::new();
        for (id, entries) in dependencies.iter().enumerate() {
            for (dependency, _) in entries.values().flatten() {
                budget.charge(1)?;
                if authorized_candidate_evidence[*dependency] && !queued[id] {
                    queued[id] = true;
                    queue.push_back(id);
                }
            }
        }
        while let Some(id) = queue.pop_front() {
            queued[id] = false;
            if authorized_candidate_evidence[id] {
                continue;
            }
            let candidate = &candidates[id];
            let selection = candidate.selection;
            let policy = candidate
                .policy
                .as_ref()
                .expect("queued unresolved marked policy");
            self.charge_candidate_policy(selection.schema, policy, true, budget)?;
            let mut sources = BTreeMap::new();
            for (source, entries) in &dependencies[id] {
                let mut rows = Vec::new();
                for (dependency, row) in entries {
                    budget.charge(1)?;
                    if authorized_candidate_evidence[*dependency] {
                        rows.push(row.clone());
                    }
                }
                if !rows.is_empty() {
                    sources.insert(source.clone(), rows);
                }
            }
            if self
                .policy_query_allows_candidate_with_provenance_for_schema(
                    selection.schema,
                    &selection.table,
                    policy,
                    selection.row,
                    &selection.cells,
                    identity,
                    true,
                    selection.provenance,
                    PolicyDecisionRole::Write,
                    Some(sources),
                    &TransactionWriteOverlay::accepted_state(),
                )
                .await?
            {
                authorized_candidate_evidence[id] = true;
                for dependent in &dependents[id] {
                    budget.charge(1)?;
                    if !authorized_candidate_evidence[*dependent] && !queued[*dependent] {
                        queued[*dependent] = true;
                        queue.push_back(*dependent);
                    }
                }
            }
        }
        for (id, candidate) in candidates.iter().enumerate() {
            let version = candidate.version_index;
            evidence.qualified[version] = authorized_candidate_evidence[id];
            if candidate.selection.uses_authorized_created_sources() {
                evidence.allowed[version] = authorized_candidate_evidence[id];
                for (source, entries) in &dependencies[id] {
                    budget.charge(entries.len())?;
                    let rows = entries
                        .iter()
                        .filter_map(|(dependency, row)| {
                            authorized_candidate_evidence[*dependency]
                                .then(|| (candidates[*dependency].version_index, row.clone()))
                        })
                        .collect::<Vec<_>>();
                    if !rows.is_empty() {
                        evidence.sources[version].insert(source.clone(), rows);
                    }
                }
            }
        }
        Ok(())
    }

    /// Revoke strict capabilities against accepted-row replacements. Rebuild
    /// the fixed point from independent baselines, not the earlier grants, so
    /// a seed removed by an update cannot leave a self-justifying cycle.
    pub(in crate::node) async fn revalidate_authorized_created_evidence(
        &mut self,
        evidence: &mut AuthorizedCreatedEvidence,
        identity: AuthorSubject,
        overlay: &TransactionWriteOverlay,
    ) -> Result<bool, Error> {
        let overlay = overlay.accepted_updates();
        let mut allowed = vec![false; evidence.selected.len()];
        let mut qualified = vec![false; evidence.selected.len()];
        for (index, selection) in evidence.selected.iter().enumerate() {
            if !selection.uses_authorized_created_sources() && !evidence.qualified[index] {
                continue;
            }
            let passes = self
                .evaluate_selected_write_policy(
                    selection,
                    identity,
                    true,
                    Some(&mut evidence.budget),
                    &overlay,
                )
                .await?;
            allowed[index] = passes && evidence.allowed[index];
            qualified[index] = passes && evidence.qualified[index];
        }
        let mut dependents = vec![BTreeSet::new(); evidence.selected.len()];
        for (index, sources) in evidence.sources.iter().enumerate() {
            for rows in sources.values() {
                evidence.budget.charge(rows.len())?;
                for (dependency, _) in rows {
                    dependents[*dependency].insert(index);
                }
            }
        }
        let mut queue = VecDeque::new();
        let mut queued = vec![false; evidence.selected.len()];
        for (dependency, consumers) in dependents.iter().enumerate() {
            if qualified[dependency] {
                for consumer in consumers {
                    if !queued[*consumer] {
                        queued[*consumer] = true;
                        queue.push_back(*consumer);
                    }
                }
            }
        }
        while let Some(index) = queue.pop_front() {
            queued[index] = false;
            if allowed[index] || !evidence.allowed[index] {
                continue;
            }
            let selection = &evidence.selected[index];
            let policy = selection
                .table
                .write_policies
                .insert_check
                .as_ref()
                .expect("selected marked insert policy");
            self.charge_candidate_policy(selection.schema, policy, true, &mut evidence.budget)?;
            let mut sources = BTreeMap::new();
            for (source, rows) in &evidence.sources[index] {
                evidence.budget.charge(rows.len())?;
                let rows = rows
                    .iter()
                    .filter_map(|(dependency, row)| qualified[*dependency].then(|| row.clone()))
                    .collect::<Vec<_>>();
                if !rows.is_empty() {
                    sources.insert(source.clone(), rows);
                }
            }
            if self
                .policy_query_allows_candidate_with_provenance_for_schema(
                    selection.schema,
                    &selection.table,
                    policy,
                    selection.row,
                    &selection.cells,
                    identity,
                    true,
                    selection.provenance,
                    PolicyDecisionRole::Write,
                    Some(sources),
                    &overlay,
                )
                .await?
            {
                allowed[index] = true;
                qualified[index] = evidence.qualified[index];
                for consumer in &dependents[index] {
                    evidence.budget.charge(1)?;
                    if !queued[*consumer] && !allowed[*consumer] {
                        queued[*consumer] = true;
                        queue.push_back(*consumer);
                    }
                }
            }
        }
        Ok(evidence
            .selected
            .iter()
            .enumerate()
            .all(|(index, selection)| {
                !selection.uses_authorized_created_sources() || allowed[index]
            }))
    }

    async fn candidate_index_key(
        &mut self,
        schema: SchemaVersionId,
        table: &TableSchema,
        column: &str,
        candidates: &[Candidate<'_>],
        index: &mut CandidateIndex,
        budget: &mut CandidateProofBudget,
    ) -> Result<(), Error> {
        let row_key = (schema, table.name.clone());
        if !index.rows.contains_key(&row_key) {
            let physical = self.physical_table_id_for_schema(schema, &table.name)?;
            let mut rows = Vec::new();
            if let Some(ids) = index.by_table.get(&physical) {
                for id in ids {
                    budget.charge(1)?;
                    let selection = candidates[*id].selection;
                    let mut cells = std::borrow::Cow::Borrowed(&selection.cells);
                    if selection.schema != schema {
                        if let Some(path) = self.compiled_lens_path(
                            selection.schema,
                            schema,
                            &selection.table.name,
                        )? {
                            if apply_compiled_lens_path(&path, cells.to_mut()) != table.name {
                                return Err(Error::QueryCapability(
                                    "candidate policy projection changes lineage".into(),
                                ));
                            }
                        } else if !crate::node::policy::policy_tables_are_directly_compatible(
                            &selection.table,
                            table,
                        ) {
                            return Err(Error::QueryCapability(
                                "candidate policy schema projection is unavailable".into(),
                            ));
                        }
                    }
                    rows.push((
                        *id,
                        current_row_from_cells_with_explicit_provenance(
                            table,
                            selection.row,
                            &cells,
                            selection.provenance,
                            None,
                        )?,
                    ));
                }
            }
            index.rows.insert(row_key.clone(), rows);
        }
        let key = (schema, table.name.clone(), column.to_owned());
        if !index.keys.contains_key(&key) {
            let mut postings = BTreeMap::<uuid::Uuid, Vec<usize>>::new();
            for (position, (_, row)) in index.rows[&row_key].iter().enumerate() {
                budget.charge(1)?;
                if let Some(value) = row_value(row, table, column).as_ref().and_then(scalar_uuid) {
                    postings.entry(value).or_default().push(position);
                }
            }
            index.keys.insert(key, postings);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn candidate_join_dependencies(
        &mut self,
        protected: &Candidate<'_>,
        candidates: &[Candidate<'_>],
        index: &mut CandidateIndex,
        budget: &mut CandidateProofBudget,
        dependency_count: &mut usize,
        parent_table: &TableSchema,
        parents: &[CurrentRow],
        joins: &[JoinVia],
        prefix: &str,
        output: &mut OccurrenceDependencies,
    ) -> Result<(), Error> {
        let selection = protected.selection;
        for (join_id, join) in joins.iter().enumerate() {
            budget.charge(1)?;
            let nested_created = has_created(&join.nested_joins, budget)?;
            if join.source_mode == CandidateSourceMode::AcceptedOnly && !nested_created {
                continue;
            }
            let path = if let Some(parent) = prefix.strip_suffix(":nested") {
                nested_join_source_path(parent, join_id)
            } else {
                join_via_source_path(prefix, join_id)
            };
            let source = nested_join_source_id(join, &path);
            let table = self.table_in_schema(&join.table, selection.schema)?;
            let marked = join.source_mode == CandidateSourceMode::IncludeAuthorizedCreatedV1;
            if marked {
                self.candidate_index_key(
                    selection.schema,
                    &table,
                    &join.on_column,
                    candidates,
                    index,
                    budget,
                )
                .await?;
            }
            let mut child_rows = BTreeMap::new();
            let mut seen = BTreeSet::new();
            'parents: for parent in parents {
                budget.charge(1)?;
                let Some(mut key) = row_value(
                    parent,
                    parent_table,
                    join.source_column.as_deref().unwrap_or("id"),
                ) else {
                    continue;
                };
                while let Value::Nullable(Some(value)) = key {
                    key = *value;
                }
                if matches!(key, Value::Nullable(None)) {
                    continue;
                }
                if marked {
                    if let Some(uuid) = scalar_uuid(&key) {
                        if let Some(postings) = index.keys
                            [&(selection.schema, table.name.clone(), join.on_column.clone())]
                            .get(&uuid)
                        {
                            for position in postings {
                                budget.charge(1)?;
                                let (id, row) =
                                    &index.rows[&(selection.schema, table.name.clone())][*position];
                                let candidate = &candidates[*id];
                                if (candidate.physical, candidate.selection.row)
                                    == (protected.physical, selection.row)
                                {
                                    continue;
                                }
                                let mut matches = true;
                                for correlation in &join.correlated_filters {
                                    budget.charge(1)?;
                                    let left =
                                        row_value(parent, parent_table, &correlation.source_column);
                                    let right = row_value(row, &table, &correlation.join_column);
                                    if left.is_none() || left != right {
                                        matches = false;
                                        break;
                                    }
                                }
                                if matches && seen.insert(*id) {
                                    if *dependency_count == MAX_CANDIDATE_DEPENDENCIES {
                                        return Err(Error::QueryCapability(PROOF_LIMIT.into()));
                                    }
                                    *dependency_count += 1;
                                    output
                                        .entry(source.clone())
                                        .or_default()
                                        .push((*id, row.clone()));
                                    child_rows.insert(row.row_uuid(), row.clone());
                                }
                            }
                        }
                    }
                }
                // Only nested occurrences need a possible parent domain. This
                // is an equality-indexed accepted query, not a second predicate
                // evaluator. The complete original policy still checks every
                // local filter and correlation before publishing any row.
                if nested_created {
                    let mut query = JazzQuery::from(table.name.as_str())
                        .filter(eq(col(&join.on_column), lit(key)));
                    for correlation in &join.correlated_filters {
                        let Some(value) =
                            row_value(parent, parent_table, &correlation.source_column)
                        else {
                            continue 'parents;
                        };
                        query = query.filter(eq(col(&correlation.join_column), lit(value)));
                    }
                    budget.policy(&query)?;
                    // Reserve the materialization allowance before querying.
                    // No over-budget sentinel row is requested; exhausting
                    // this allowance leaves no budget for a dependent proof.
                    let row_allowance = budget.0;
                    if row_allowance == 0 {
                        return Err(Error::QueryCapability(PROOF_LIMIT.into()));
                    }
                    budget.charge(row_allowance)?;
                    query = query.limit(row_allowance);
                    let schema = if selection.schema == self.catalogue.active_schema.schema {
                        &self.catalogue.active_schema.compiled
                    } else if selection.schema == self.catalogue.local_schema_version_id {
                        &self.catalogue.schema
                    } else {
                        &self
                            .catalogue
                            .catalogue_schemas
                            .get(&selection.schema)
                            .ok_or(Error::InvalidStoredValue("policy schema payload missing"))?
                            .schema
                    };
                    let shape = query.validate_with_schema_version(schema, selection.schema)?;
                    let binding = shape.bind(BTreeMap::new())?;
                    let rows = self
                        .query_rows_with_prepared_plan_for_identity(
                            &shape,
                            &binding,
                            DurabilityTier::Global,
                            None,
                            AuthorSubject::SYSTEM,
                        )
                        .await?;
                    budget.0 = row_allowance
                        .checked_sub(rows.len())
                        .ok_or_else(|| Error::QueryCapability(PROOF_LIMIT.into()))?;
                    for row in rows {
                        child_rows.entry(row.row_uuid()).or_insert(row);
                    }
                }
            }
            if !child_rows.is_empty() && nested_created {
                Box::pin(self.candidate_join_dependencies(
                    protected,
                    candidates,
                    index,
                    budget,
                    dependency_count,
                    &table,
                    &child_rows.into_values().collect::<Vec<_>>(),
                    &join.nested_joins,
                    &format!("{path}:nested"),
                    output,
                ))
                .await?;
            }
        }
        Ok(())
    }
}
