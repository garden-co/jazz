//! Durable branch metadata and branch-local write/read helpers for
//! `jazz/BRANCHES.md`. This module owns branch creation, lifecycle records, and
//! partitioned branch storage access; base snapshot semantics use
//! [`crate::tx::Snapshot`], recovery lives in [`super::recovery`], and ordinary
//! global/local currency logic remains in [`super::currency`]. It is a node
//! sublayer beside the main global history path.

use super::*;
use crate::schema::branch_metadata_table_schema;
use crate::tx::{
    BranchLineage, BranchMergeProvenance, ContributionComponent, ContributionCoordinate,
    ContributionDot, ContributionSubstitution, MergeAspect,
};

/// Durable branch metadata recovered from `jazz_branches`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchRecord {
    /// Branch identity.
    pub branch_id: BranchId,
    /// Authenticated session identity that created the branch.
    pub created_by: AuthorId,
    /// Parent branch, or `None` for a root branch.
    pub parent: Option<BranchId>,
    /// Frozen parent settled cut. Root branches have no base.
    pub base: Option<Snapshot>,
    /// Branch lifecycle state.
    pub state: codec::BranchState,
}

impl From<&BranchRecord> for crate::protocol::BranchMetadata {
    fn from(record: &BranchRecord) -> Self {
        Self {
            branch_id: record.branch_id,
            created_by: record.created_by,
            parent: record.parent,
            base: record.base.clone(),
            open: record.state == codec::BranchState::Open,
        }
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Create a snapshot-base branch over this node's current settled watermark.
    ///
    /// Creation writes only one metadata row; overlay tables are created lazily
    /// on the first branch write.
    pub fn create_branch(&mut self, branch_id: BranchId) -> Result<BranchRecord, Error> {
        self.create_branch_as(branch_id, AuthorId(uuid::Uuid::nil()))
    }

    /// Create a snapshot-base branch attributed to an authenticated session.
    pub fn create_branch_as(
        &mut self,
        branch_id: BranchId,
        created_by: AuthorId,
    ) -> Result<BranchRecord, Error> {
        if let Some(existing) = self.branches.branches.get(&branch_id) {
            if existing.created_by == created_by
                && existing.parent.is_none()
                && existing.base.is_some()
                && existing.state == codec::BranchState::Open
            {
                return Ok(existing.clone());
            }
            return Err(Error::InvalidStoredValue("conflicting branch creation"));
        }
        let record = BranchRecord {
            branch_id,
            created_by,
            parent: None,
            base: Some(
                Snapshot::exclusive_base(
                    NodeUuid(uuid::Uuid::nil()),
                    self.clock.applied_global_watermark,
                    TxTime::default(),
                    Vec::new(),
                )
                .map_err(Error::InvalidStoredValue)?,
            ),
            state: codec::BranchState::Open,
        };
        self.persist_branch_record(&record, true)?;
        self.branches.branches.insert(branch_id, record.clone());
        Ok(record)
    }

    /// Declare a root branch with no parent fallback.
    pub fn create_root_branch(&mut self, branch_id: BranchId) -> Result<BranchRecord, Error> {
        let record = BranchRecord {
            branch_id,
            created_by: AuthorId(uuid::Uuid::nil()),
            parent: None,
            base: None,
            state: codec::BranchState::Open,
        };
        // The distinguished root is local storage scaffolding, not session-authored
        // branch metadata, so it must never enter the sync outbox.
        self.persist_branch_record(&record, false)?;
        self.branches.branches.insert(branch_id, record.clone());
        Ok(record)
    }

    /// Return recovered branch metadata.
    pub fn branch_record(&self, branch_id: BranchId) -> Option<&BranchRecord> {
        self.branches.branches.get(&branch_id)
    }

    /// Durable locally-authored or session-relayed metadata awaiting an exact
    /// acknowledgement from this node's upstream.
    pub fn pending_branch_metadata_uploads(&self) -> Vec<crate::protocol::BranchMetadata> {
        self.branches
            .pending_metadata_uploads
            .iter()
            .filter_map(|branch| self.branches.branches.get(branch))
            .map(Into::into)
            .collect()
    }

    /// Clear a durable metadata outbox item after an exact upstream echo.
    pub fn acknowledge_branch_metadata(
        &mut self,
        metadata: &crate::protocol::BranchMetadata,
    ) -> Result<(), Error> {
        if !self
            .branches
            .pending_metadata_uploads
            .contains(&metadata.branch_id)
        {
            // An unsolicited lifecycle update is not an acknowledgement. Let
            // normal inbound admission validate and persist it.
            return Ok(());
        }
        let Some(record) = self.branches.branches.get(&metadata.branch_id).cloned() else {
            return Ok(());
        };
        if crate::protocol::BranchMetadata::from(&record) != *metadata {
            return Err(Error::InvalidStoredValue(
                "branch metadata acknowledgement does not match local record",
            ));
        }
        self.persist_branch_record(&record, false)?;
        Ok(())
    }

    /// Idempotently admit durable branch routing metadata received before a
    /// branch-target unit. Conflicting redefinitions are rejected: branch
    /// identity is immutable once observed.
    pub fn admit_branch_metadata(
        &mut self,
        metadata: crate::protocol::BranchMetadata,
    ) -> Result<(), Error> {
        self.admit_branch_metadata_with_upstream_relay(metadata, false)
    }

    fn admit_branch_metadata_with_upstream_relay(
        &mut self,
        metadata: crate::protocol::BranchMetadata,
        relay_upstream: bool,
    ) -> Result<(), Error> {
        let record = BranchRecord {
            branch_id: metadata.branch_id,
            created_by: metadata.created_by,
            parent: metadata.parent,
            base: metadata.base,
            state: if metadata.open {
                codec::BranchState::Open
            } else {
                codec::BranchState::Discarded
            },
        };
        if let Some(existing) = self.branches.branches.get(&record.branch_id) {
            if existing == &record {
                return Ok(());
            }
            if existing.branch_id == record.branch_id
                && existing.created_by == record.created_by
                && existing.parent == record.parent
                && existing.base == record.base
                && existing.state == codec::BranchState::Open
                && record.state == codec::BranchState::Discarded
            {
                self.persist_branch_record(&record, relay_upstream)?;
                self.branches.branches.insert(record.branch_id, record);
                return Ok(());
            }
            return Err(Error::InvalidStoredValue("conflicting branch metadata"));
        }
        self.persist_branch_record(&record, relay_upstream)?;
        self.branches.branches.insert(record.branch_id, record);
        Ok(())
    }

    /// Admit locally-authored branch metadata from an authenticated session.
    /// The link identity, not the self-asserted payload alone, authenticates the
    /// immutable creator. Dependencies must already be locally readable.
    pub fn admit_session_branch_metadata(
        &mut self,
        metadata: crate::protocol::BranchMetadata,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if metadata.created_by != identity {
            return Err(Error::InvalidStoredValue(
                "branch metadata creator does not match authenticated session",
            ));
        }
        if metadata.parent.is_some() {
            return Err(Error::InvalidStoredValue(
                "v1 session branch metadata must be parentless",
            ));
        }
        if !self.branches.branches.contains_key(&metadata.branch_id) && !metadata.open {
            return Err(Error::InvalidStoredValue(
                "first-seen session branch metadata must be open",
            ));
        }
        let Some(base) = metadata.base.as_ref() else {
            return Err(Error::InvalidStoredValue(
                "session branch metadata requires a snapshot base",
            ));
        };
        if base.owner != NodeUuid(uuid::Uuid::nil())
            || base.local_base != TxTime::default()
            || !base.dots.is_empty()
        {
            return Err(Error::InvalidStoredValue(
                "unsupported v1 branch snapshot shape",
            ));
        }
        if base.global_base > self.clock.applied_global_watermark {
            return Ok(false);
        }
        // New metadata and lifecycle advances received from a client session
        // become a durable upstream relay. Exact downstream retries preserve
        // the existing pending/acknowledged state instead of reopening a
        // completed relay and creating an echo loop.
        self.admit_branch_metadata_with_upstream_relay(metadata, true)?;
        Ok(true)
    }

    /// Discard an open branch without deleting its overlay history.
    pub fn discard_branch(&mut self, branch_id: BranchId) -> Result<(), Error> {
        let mut record = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        if record.state != codec::BranchState::Open {
            return Err(Error::BranchClosed(branch_id));
        }
        record.state = codec::BranchState::Discarded;
        self.persist_branch_record(&record, true)?;
        self.branches.branches.insert(branch_id, record);
        Ok(())
    }

    /// Calculate and commit an ordinary mergeable write from a branch into root.
    pub fn merge_back_branch(&mut self, branch_id: BranchId) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        self.merge_back_branch_as(branch_id, AuthorId::SYSTEM)
    }

    /// Calculate a branch merge under the initiating identity's source-read and
    /// ordinary target-write permissions.
    pub fn merge_back_branch_as(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        self.merge_lineage_into_as(
            BranchLineage::Branch(branch_id),
            BranchLineage::Root,
            identity,
        )
    }

    /// Calculate an ordinary transaction which transfers the source's novel
    /// contributions into the target lineage.
    pub fn merge_lineage_into(
        &mut self,
        source: BranchLineage,
        target: BranchLineage,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        self.merge_lineage_into_as(source, target, AuthorId::SYSTEM)
    }

    /// Identity-aware form of [`Self::merge_lineage_into`].
    pub fn merge_lineage_into_as(
        &mut self,
        source: BranchLineage,
        target: BranchLineage,
        identity: AuthorId,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        if source == target {
            return Err(Error::BranchMergeCalculation(
                "source and target lineage must differ",
            ));
        }
        let source_branch = match source {
            BranchLineage::Root => None,
            BranchLineage::Branch(branch_id) => Some(
                self.branches
                    .branches
                    .get(&branch_id)
                    .cloned()
                    .ok_or(Error::BranchNotFound(branch_id))?,
            ),
        };
        if let Some(branch) = &source_branch
            && branch.state != codec::BranchState::Open
        {
            return Err(Error::BranchClosed(branch.branch_id));
        }
        if identity != AuthorId::SYSTEM
            && let Some(branch) = &source_branch
            && !self.branch_read_policy_allows(branch, identity)?
        {
            return Err(Error::AuthorizationDenied);
        }
        if let BranchLineage::Branch(target_branch) = target {
            self.ensure_branch_open(target_branch)?;
            if !self.branch_write_policy_allows(target_branch, identity)? {
                return Err(Error::AuthorizationDenied);
            }
        }

        let mut versions = Vec::new();
        let mut through_frontier = BTreeSet::new();
        let mut substitution_sources =
            BTreeMap::<ContributionCoordinate, BTreeSet<ContributionDot>>::new();
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let write_schema = self
            .catalogue
            .catalogue_schemas
            .get(&write_schema_version)
            .ok_or(Error::InvalidStoredValue("branch write schema missing"))?
            .schema
            .clone();
        let known_source_dots = self.validated_target_source_dots(source, target)?;
        for table in write_schema.tables.clone() {
            let table_schema = self.table_in_schema(&table.name, write_schema_version)?;
            let overlay_rows = self.lineage_row_ids(&table.name, source)?;
            if identity != AuthorId::SYSTEM && !overlay_rows.is_empty() {
                let shape = crate::query::Query::from(table.name.as_str())
                    .validate(&write_schema)
                    .map_err(|_| Error::BranchMergeCalculation("source read shape is invalid"))?;
                let binding = shape
                    .bind(BTreeMap::new())
                    .map_err(|_| Error::BranchMergeCalculation("source read binding is invalid"))?;
                let readable = match source {
                    BranchLineage::Root => {
                        self.query_rows_for_link(&shape, &binding, DurabilityTier::Local, identity)?
                    }
                    BranchLineage::Branch(branch_id) => {
                        self.query_rows_on_branch_for_link(branch_id, &shape, &binding, identity)?
                    }
                }
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
                if !overlay_rows.is_subset(&readable) {
                    return Err(Error::AuthorizationDenied);
                }
            }
            for row_uuid in overlay_rows {
                for layer in [VersionLayer::Content, VersionLayer::Deletion] {
                    let source_versions =
                        self.lineage_layer_versions(&table.name, row_uuid, layer, source)?;
                    let candidates = (0..source_versions.len()).collect::<Vec<_>>();
                    let Some(winner_idx) = current_version_index(
                        &source_versions,
                        &candidates,
                        layer,
                        &self.node_aliases,
                    ) else {
                        continue;
                    };
                    let winner = source_versions[winner_idx].clone();
                    through_frontier.insert(self.version_tx_id(&winner)?);
                    let parents = self.target_layer_heads(&table.name, row_uuid, layer, target)?;
                    let cells = table_schema
                        .columns
                        .iter()
                        .map(|column| winner.cell(&table_schema, &column.name))
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut authored_columns = BTreeSet::new();
                    let mut layer_has_novel_contribution = false;
                    for source_version in &source_versions {
                        let source_tx_id = self.version_tx_id(source_version)?;
                        through_frontier.insert(source_tx_id);
                        match layer {
                            VersionLayer::Content => {
                                let source_schema = self
                                    .schema_version_for_alias(source_version.schema_version_alias())
                                    .ok_or(Error::InvalidStoredValue(
                                        "branch row schema version alias missing",
                                    ))?;
                                let source_table = source_version.table().to_owned();
                                let source_table_schema =
                                    self.table_in_schema(&source_table, source_schema)?.clone();
                                let source_authored = source_version
                                    .authored_columns(&source_table_schema)?
                                    .unwrap_or_else(|| {
                                        source_table_schema
                                            .columns
                                            .iter()
                                            .filter(|column| {
                                                source_version
                                                    .cell(&source_table_schema, &column.name)
                                                    .is_ok_and(|value| value.is_some())
                                            })
                                            .map(|column| column.name.clone())
                                            .collect()
                                    });
                                for column in self.project_branch_authored_columns(
                                    source_schema,
                                    &source_table,
                                    write_schema_version,
                                    &table.name,
                                    source_authored,
                                )? {
                                    let column_schema = table_schema
                                        .columns
                                        .iter()
                                        .find(|candidate| candidate.name == column)
                                        .ok_or(Error::BranchMergeCalculation(
                                            "authored source column is absent from current schema",
                                        ))?;
                                    if table_schema.merge_strategy(&column)
                                        != crate::schema::MergeStrategy::Lww
                                        || column_schema.large_value.is_some()
                                        || column_schema.text_merge_spec.is_some()
                                    {
                                        return Err(Error::BranchMergeCalculation(
                                            "column strategy lacks branch contribution capabilities",
                                        ));
                                    }
                                    let target = ContributionCoordinate {
                                        table: table.name.clone(),
                                        row_uuid,
                                        layer: MergeAspect::Content,
                                        component: ContributionComponent::Column(column.clone()),
                                    };
                                    let source_coordinate = ContributionCoordinate {
                                        table: table.name.clone(),
                                        row_uuid,
                                        layer: MergeAspect::Content,
                                        component: ContributionComponent::Column(column.clone()),
                                    };
                                    let source_dots = self.expanded_contribution_dots(
                                        source,
                                        source_tx_id,
                                        source_coordinate,
                                    )?;
                                    let novel = source_dots
                                        .into_iter()
                                        .filter(|dot| !known_source_dots.contains(dot))
                                        .collect::<BTreeSet<_>>();
                                    if novel.is_empty() {
                                        continue;
                                    }
                                    layer_has_novel_contribution = true;
                                    authored_columns.insert(column.clone());
                                    substitution_sources
                                        .entry(target)
                                        .or_default()
                                        .extend(novel);
                                }
                            }
                            VersionLayer::Deletion => {
                                let target = ContributionCoordinate {
                                    table: table.name.clone(),
                                    row_uuid,
                                    layer: MergeAspect::Deletion,
                                    component: ContributionComponent::Register,
                                };
                                let source_dots = self.expanded_contribution_dots(
                                    source,
                                    source_tx_id,
                                    target.clone(),
                                )?;
                                let novel = source_dots
                                    .into_iter()
                                    .filter(|dot| !known_source_dots.contains(dot))
                                    .collect::<BTreeSet<_>>();
                                if novel.is_empty() {
                                    continue;
                                }
                                layer_has_novel_contribution = true;
                                substitution_sources
                                    .entry(target)
                                    .or_default()
                                    .extend(novel);
                            }
                        }
                    }
                    if !layer_has_novel_contribution {
                        continue;
                    }
                    let authored_cells = table_schema
                        .columns
                        .iter()
                        .zip(cells)
                        .filter_map(|(column, value)| {
                            authored_columns
                                .contains(&column.name)
                                .then_some(value.map(|value| (column.name.clone(), value)))
                                .flatten()
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut commit = MergeableCommit::new(&table.name, row_uuid, 0)
                        .made_by(identity)
                        .parents(parents);
                    match layer {
                        VersionLayer::Content => {
                            commit = commit
                                .cells(authored_cells)
                                .authored_columns(authored_columns);
                        }
                        VersionLayer::Deletion => {
                            commit = commit.deletion(winner.deletion().ok_or(
                                Error::BranchMergeCalculation(
                                    "deletion source version has no register event",
                                ),
                            )?);
                        }
                    }
                    versions.push(commit);
                }
            }
        }
        if versions.is_empty() {
            return Err(Error::BranchMergeCalculation(
                "merge calculation requires at least one branch overlay write",
            ));
        }
        let substitutions = substitution_sources
            .into_iter()
            .map(|(target, sources)| ContributionSubstitution {
                target,
                sources: sources.into_iter().collect(),
            })
            .collect();
        let branch_merge = BranchMergeProvenance::canonical(
            source,
            Vec::new(),
            through_frontier.into_iter().collect(),
            substitutions,
        )
        .map_err(Error::BranchMergeCalculation)?;
        self.commit_merge_transaction(target, branch_merge, versions)
    }

    /// Branch-scoped exclusives are intentionally not implemented in v1.
    pub fn open_exclusive_on_branch(&mut self, _branch_id: BranchId) -> Result<OpenTxId, Error> {
        Err(Error::UnsupportedBranchExclusive)
    }

    /// Commit a mergeable write into a branch overlay partition.
    pub fn commit_mergeable_on_branch(
        &mut self,
        branch_id: BranchId,
        commit: MergeableCommit,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        self.commit_mergeable_many_on_branch(branch_id, vec![commit])
    }

    /// Commit multiple ordinary mergeable writes atomically into one branch
    /// target. The resulting transaction differs from a root commit only in
    /// its explicit target lineage and the target-relative policy/storage view.
    pub fn commit_mergeable_many_on_branch(
        &mut self,
        branch_id: BranchId,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.ensure_branch_open(branch_id)?;
        let permission_subject = commits[0].effective_permission_subject();
        if !self.branch_write_policy_allows(branch_id, permission_subject)? {
            return Err(Error::AuthorizationDenied);
        }
        for commit in &commits {
            for parent in &commit.parents {
                self.merge_tx_time(parent.time);
            }
        }
        let write_schema_version = self.catalogue.current_write_schema.schema;
        for table in commits
            .iter()
            .map(|commit| commit.table.clone())
            .collect::<BTreeSet<_>>()
        {
            self.persist_branch_partition(table, write_schema_version, branch_id)?;
        }
        let made_at = self.mint_tx_time(commits[0].now_ms);
        self.commit_mergeable_many_on_branch_at(branch_id, commits, made_at, None)
    }

    fn commit_mergeable_many_on_branch_at(
        &mut self,
        branch_id: BranchId,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
        branch_merge: Option<BranchMergeProvenance>,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        let permission_subject = commits[0].effective_permission_subject();
        for commit in &commits {
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let version = VersionRecord::from_commit(commit, &table_schema, write_schema_version)?;
            if !self.branch_table_write_policy_allows_version_record(
                &branch,
                &table_schema,
                &version,
                permission_subject,
            )? {
                return Err(Error::AuthorizationDenied);
            }
        }
        for table in commits
            .iter()
            .map(|commit| commit.table.clone())
            .collect::<BTreeSet<_>>()
        {
            self.persist_branch_partition(table, write_schema_version, branch_id)?;
        }
        let tx_id = TxId::new(made_at, self.node_uuid);
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: commits.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by: commits[0].made_by,
            permission_subject: commits[0].permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: commits[0].user_metadata_json.clone(),
            target_lineage: BranchLineage::Branch(branch_id),
            branch_merge,
            merge_strategy: commits[0].merge_strategy.clone(),
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node)?;
        let schema_version_alias = self.ensure_schema_version_alias(write_schema_version)?;
        let mut batch = self.database.open_batch();
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                DurabilityTier::Local,
            ),
        );
        let mut transaction_tables = BTreeSet::new();
        for commit in commits {
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: commit.table.clone(),
                    row_uuid: commit.row_uuid,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: made_at,
                    parents: commit.parents,
                    created_by: commit.made_by,
                    created_at: TxTime(commit.now_ms),
                    updated_by: commit.made_by,
                    updated_at: TxTime(commit.now_ms),
                    authored_columns: Some(
                        commit
                            .authored_columns
                            .clone()
                            .unwrap_or_else(|| commit.cells.keys().cloned().collect()),
                    ),
                    cells: commit.cells,
                    deletion: commit.deletion,
                },
                None,
            )?;
            transaction_tables.insert(table_schema.name.clone());
            let (branch_table, branch_record) =
                self.branch_version_storage_write_binding(&stored, branch_id)?;
            batch.insert_raw(
                branch_table.as_ref(),
                history_primary_key(&stored),
                branch_record,
            );
        }
        self.database.commit_batch(batch)?;
        self.cache_tx_version_tables(tx_id, transaction_tables);
        Ok(tx_id)
    }

    /// Read a validated query in a branch view: overlay rows first, then the
    /// frozen parent `at(base)` read for rows absent from the overlay.
    pub fn query_rows_on_branch(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.query_rows_on_branch_for_link(branch_id, shape, binding, AuthorId::SYSTEM)
    }

    /// Read a validated query in a branch view for a peer identity. The branch
    /// metadata row is the first-level access symbol; if it is not readable, no
    /// branch overlay/base view is exposed. Rows that pass the branch row gate
    /// are then narrowed by ordinary table read policy evaluated in the branch
    /// view.
    pub fn query_rows_on_branch_for_link(
        &mut self,
        branch_id: BranchId,
        shape: &ValidatedQuery,
        binding: &Binding,
        identity: AuthorId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let Some(branch) = self.branches.branches.get(&branch_id).cloned() else {
            // Remote/session branch reads deliberately conflate an unknown
            // lineage with a lineage the caller is not allowed to enumerate.
            if identity != AuthorId::SYSTEM {
                return Ok(Vec::new());
            }
            return Err(Error::BranchNotFound(branch_id));
        };
        if !self.branch_read_policy_allows(&branch, identity)? {
            return Ok(Vec::new());
        }
        let mut rows =
            self.query_rows_on_branch_query_engine(branch_id, shape, binding, identity)?;
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn branch_read_policy_allows(
        &mut self,
        branch: &BranchRecord,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        self.branch_read_policy_authorized_branch_ids(branch.branch_id, identity)
            .map(|branches| branches.contains(&RowUuid(branch.branch_id.0)))
    }

    /// Whether an authenticated link may learn a branch routing record. This
    /// is deliberately the same first-level branch gate as branch reads, so
    /// metadata cannot become a branch-existence oracle when an empty result
    /// is otherwise legitimate.
    pub(crate) fn branch_metadata_visible_to(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        let Some(branch) = self.branches.branches.get(&branch_id).cloned() else {
            return Ok(false);
        };
        self.branch_read_policy_allows(&branch, identity)
    }

    pub(super) fn branch_write_policy_allows(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        if identity == AuthorId::SYSTEM {
            return Ok(true);
        }
        let Some(policy) = self.catalogue.schema.branch_write_policy.clone() else {
            return Ok(true);
        };
        let branch = self
            .branches
            .branches
            .get(&branch_id)
            .cloned()
            .ok_or(Error::BranchNotFound(branch_id))?;
        let table = branch_metadata_table_schema();
        let cells = table
            .columns
            .iter()
            .filter_map(|column| {
                branch_metadata_value(&branch, &column.name)
                    .map(|value| (column.name.clone(), value))
            })
            .collect();
        self.branch_write_policy_query_allows_candidate(
            branch_id,
            &table,
            &policy,
            RowUuid(branch.branch_id.0),
            &cells,
            identity,
            false,
        )
    }

    pub(super) fn branch_table_write_policy_allows_version_record(
        &mut self,
        branch: &BranchRecord,
        table: &TableSchema,
        version: &VersionRecord,
        author: AuthorId,
    ) -> Result<bool, Error> {
        if author == AuthorId::SYSTEM {
            return Ok(true);
        }
        if version.deletion().is_some() {
            let Some(policy) = table.write_policies.delete_using.clone() else {
                return Ok(false);
            };
            let Some(row) = self.branch_delete_subject_row(branch, table, version)? else {
                return Ok(false);
            };
            let cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    row.cell(table, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            return self.branch_write_policy_query_allows_candidate(
                branch.branch_id,
                table,
                &policy,
                row.row_uuid(),
                &cells,
                author,
                false,
            );
        }
        let previous = self.branch_delete_subject_row(branch, table, version)?;
        if let Some(previous) = previous {
            let previous_cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    previous
                        .cell(table, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            if let Some(policy) = table.write_policies.update_using.clone() {
                if !self.branch_write_policy_query_allows_candidate(
                    branch.branch_id,
                    table,
                    &policy,
                    previous.row_uuid(),
                    &previous_cells,
                    author,
                    false,
                )? {
                    return Ok(false);
                }
            }
            let Some(policy) = table.write_policies.update_check.clone() else {
                return Ok(false);
            };
            let cells = table
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    version
                        .cell_at(idx)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            return self.branch_write_policy_query_allows_candidate(
                branch.branch_id,
                table,
                &policy,
                version.row_uuid(),
                &cells,
                author,
                false,
            );
        }
        let Some(policy) = table.write_policies.insert_check.clone() else {
            return Ok(true);
        };
        let cells = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                version
                    .cell_at(idx)
                    .map(|value| (column.name.clone(), value))
            })
            .collect();
        self.branch_write_policy_query_allows_candidate(
            branch.branch_id,
            table,
            &policy,
            version.row_uuid(),
            &cells,
            author,
            true,
        )
    }

    fn branch_delete_subject_row(
        &mut self,
        branch: &BranchRecord,
        table: &TableSchema,
        version: &VersionRecord,
    ) -> Result<Option<CurrentRow>, Error> {
        if let Some(row) = self
            .branch_current_rows_for_schema(&table.name, branch, version.schema_version())?
            .into_iter()
            .find(|row| row.row_uuid() == version.row_uuid())
        {
            return Ok(Some(row));
        }

        for parent in version.parents() {
            for parent_version in self.query_versions_for_tx(parent)? {
                if parent_version.table() != table.name
                    || parent_version.row_uuid() != version.row_uuid()
                    || parent_version.layer() != VersionLayer::Content
                {
                    continue;
                }
                return self
                    .current_row_from_materialized_version(table, &parent_version)
                    .map(Some);
            }
        }

        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn evaluate_branch_metadata_write_policy_for_test(
        &mut self,
        branch_id: BranchId,
        identity: AuthorId,
    ) -> Result<bool, Error> {
        self.branch_write_policy_allows(branch_id, identity)
    }

    pub(super) fn branch_current_rows(
        &mut self,
        table: &str,
        branch: &BranchRecord,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.branch_current_rows_for_schema(table, branch, self.catalogue.current_schema_version_id)
    }

    fn branch_current_rows_for_schema(
        &mut self,
        table: &str,
        branch: &BranchRecord,
        read_schema_version: SchemaVersionId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, read_schema_version)?.clone();
        let overlay =
            self.branch_overlay_rows(table, &table_schema, branch.branch_id, read_schema_version)?;
        let overlay_row_ids = overlay
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<BTreeSet<_>>();
        let mut by_row = overlay
            .into_iter()
            .map(|row| (row.row_uuid(), row))
            .collect::<BTreeMap<_, _>>();
        if let Some(base) = &branch.base {
            let base_rows = if read_schema_version == self.catalogue.current_schema_version_id {
                self.current_rows_at(table, base.global_base)?
            } else {
                self.projected_historical_current_rows(
                    table,
                    read_schema_version,
                    base.global_base,
                )?
            };
            for row in base_rows {
                if !overlay_row_ids.contains(&row.row_uuid()) {
                    by_row.insert(row.row_uuid(), row);
                }
            }
        }
        let mut rows = by_row.into_values().collect::<Vec<_>>();
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    pub(super) fn branch_metadata_current_rows(&self) -> Result<Vec<CurrentRow>, Error> {
        let table = branch_metadata_table_schema();
        self.branches
            .branches
            .values()
            .map(|branch| branch_metadata_current_row(&table, branch))
            .collect()
    }

    fn branch_overlay_rows(
        &mut self,
        table: &str,
        table_schema: &TableSchema,
        branch_id: BranchId,
        read_schema_version: SchemaVersionId,
    ) -> Result<Vec<CurrentRow>, Error> {
        let mut rows = Vec::new();
        for row_uuid in
            self.branch_overlay_row_ids_for_schema(table, branch_id, read_schema_version)?
        {
            if self
                .branch_overlay_layer_winner_for_schema(
                    table,
                    row_uuid,
                    VersionLayer::Deletion,
                    branch_id,
                    read_schema_version,
                )?
                .is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted))
            {
                continue;
            }
            let Some(content) = self.branch_overlay_layer_winner_for_schema(
                table,
                row_uuid,
                VersionLayer::Content,
                branch_id,
                read_schema_version,
            )?
            else {
                continue;
            };
            let source_schema = self
                .schema_version_for_alias(content.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "branch row schema version alias missing",
                ))?;
            let source_table = self
                .table_in_schema(content.table(), source_schema)?
                .clone();
            let mut cells = self.materialized_cells_for_version(&source_table, &content)?;
            let projected_table = self.translate_cells(
                source_schema,
                read_schema_version,
                content.table(),
                &mut cells,
            )?;
            if projected_table.as_deref() != Some(table) {
                continue;
            }
            rows.push(current_row_from_materialized_cells(
                table_schema,
                &content,
                &cells,
            )?);
        }
        sort_current_rows(&mut rows);
        Ok(rows)
    }

    fn branch_overlay_row_ids_for_schema(
        &mut self,
        table: &str,
        branch_id: BranchId,
        schema_version: SchemaVersionId,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        if !self
            .branches
            .branch_partitions
            .contains(&(table_id, branch_id))
        {
            return Ok(BTreeSet::new());
        }
        let mut row_ids = BTreeSet::new();
        for storage_table in [
            physical_branch_history_table_name(table_id, branch_id),
            physical_branch_register_table_name(table_id, branch_id),
        ] {
            for raw in self.database.primary_key_scan_raw(&storage_table, &[])? {
                row_ids.insert(RowUuid(
                    raw.record()
                        .get_uuid(HistoryRowRecord::FIELD_ROW_UUID_IDX)?,
                ));
            }
        }
        Ok(row_ids)
    }

    fn branch_overlay_row_ids(
        &mut self,
        table: &str,
        branch_id: BranchId,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        self.branch_overlay_row_ids_for_schema(
            table,
            branch_id,
            self.catalogue.current_write_schema.schema,
        )
    }

    fn lineage_row_ids(
        &mut self,
        table: &str,
        lineage: BranchLineage,
    ) -> Result<BTreeSet<RowUuid>, Error> {
        match lineage {
            BranchLineage::Root => Ok(self
                .query_table_versions(table)?
                .into_iter()
                .map(|version| version.row_uuid())
                .collect()),
            BranchLineage::Branch(branch_id) => self.branch_overlay_row_ids(table, branch_id),
        }
    }

    fn lineage_layer_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        lineage: BranchLineage,
    ) -> Result<Vec<VersionRow>, Error> {
        match lineage {
            BranchLineage::Root => Ok(self
                .query_row_versions(table, row_uuid)?
                .into_iter()
                .filter(|version| version.layer() == layer)
                .collect()),
            BranchLineage::Branch(branch_id) => {
                self.branch_overlay_layer_versions(table, row_uuid, layer, branch_id)
            }
        }
    }

    fn expanded_contribution_dots(
        &mut self,
        lineage: BranchLineage,
        tx_id: TxId,
        coordinate: ContributionCoordinate,
    ) -> Result<BTreeSet<ContributionDot>, Error> {
        let Some(stored) = self.query_transaction(tx_id)? else {
            return Err(Error::BranchMergeCalculation(
                "source contribution transaction is unavailable",
            ));
        };
        if stored.tx.target_lineage != lineage {
            return Err(Error::BranchMergeCalculation(
                "source contribution is stored in another lineage",
            ));
        }
        if let Some(provenance) = &stored.tx.branch_merge
            && let Some(substitution) = provenance
                .substitutions
                .iter()
                .find(|substitution| substitution.target == coordinate)
                .cloned()
        {
            let emitted = self.query_versions_for_tx(tx_id)?;
            if !self.validate_lww_branch_substitution(
                provenance.source_lineage,
                provenance,
                &substitution,
                &emitted,
            )? {
                return Err(Error::BranchMergeCalculation(
                    "branch merge provenance cannot be validated",
                ));
            }
            return Ok(substitution.sources.into_iter().collect());
        }
        Ok(BTreeSet::from([ContributionDot {
            lineage,
            tx_id,
            coordinate,
        }]))
    }

    fn project_branch_authored_columns(
        &mut self,
        source_schema: SchemaVersionId,
        source_table: &str,
        target_schema: SchemaVersionId,
        target_table: &str,
        mut columns: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, Error> {
        if source_schema == target_schema && source_table == target_table {
            return Ok(columns);
        }
        let mut path = None;
        for direction in [LensPathDirection::Forward, LensPathDirection::Reverse] {
            if let Some(candidate) =
                self.compiled_lens_path(source_schema, target_schema, direction, source_table)?
                && candidate.target_table == target_table
            {
                path = Some(candidate);
                break;
            }
        }
        let Some(path) = path else {
            return Err(Error::BranchMergeCalculation(
                "authored source column has no lens path to current schema",
            ));
        };
        for op in path.ops {
            match op {
                CompiledLensOp::Rename { from, to } => {
                    if columns.remove(&from) {
                        columns.insert(to);
                    }
                }
                CompiledLensOp::Copy { from, to } => {
                    if columns.contains(&from) {
                        columns.insert(to);
                    }
                }
                CompiledLensOp::Add { column, .. } | CompiledLensOp::Drop { column } => {
                    columns.remove(&column);
                }
            }
        }
        Ok(columns)
    }

    fn branch_overlay_layer_winner_for_schema(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        branch_id: BranchId,
        schema_version: SchemaVersionId,
    ) -> Result<Option<VersionRow>, Error> {
        let versions = self.branch_overlay_layer_versions_for_schema(
            table,
            row_uuid,
            layer,
            branch_id,
            schema_version,
        )?;
        let candidates = (0..versions.len()).collect::<Vec<_>>();
        Ok(
            current_version_index(&versions, &candidates, layer, &self.node_aliases)
                .map(|idx| versions[idx].clone()),
        )
    }

    fn target_layer_heads(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        target: BranchLineage,
    ) -> Result<Vec<TxId>, Error> {
        let versions = self.lineage_layer_versions(table, row_uuid, layer, target)?;
        let mut candidates = Vec::new();
        for (idx, version) in versions.iter().enumerate() {
            if version.layer() != layer {
                continue;
            }
            let tx_id = self.version_tx_id(version)?;
            let Some(tx) = self.transaction_record(tx_id) else {
                continue;
            };
            if matches!(tx.fate, Fate::Accepted | Fate::Pending) {
                candidates.push(idx);
            }
        }
        let mut heads = content_head_indices(&versions, &candidates, &self.node_aliases)
            .into_iter()
            .map(|idx| self.version_tx_id(&versions[idx]))
            .collect::<Result<Vec<_>, _>>()?;
        heads.sort();
        heads.dedup();
        Ok(heads)
    }

    fn validated_target_source_dots(
        &mut self,
        source: BranchLineage,
        target: BranchLineage,
    ) -> Result<BTreeSet<ContributionDot>, Error> {
        let records = self
            .database
            .primary_key_scan_raw("jazz_transactions", &[])?
            .into_iter()
            .filter_map(|raw| {
                let record = raw.record();
                let bytes = record
                    .get_nullable_bytes(TransactionRowRecord::FIELD_BRANCH_MERGE_IDX)
                    .ok()??
                    .to_vec();
                let alias = record
                    .get_u64(TransactionRowRecord::FIELD_NODE_ID_IDX)
                    .ok()?;
                let time = record.get_u64(TransactionRowRecord::FIELD_TIME_IDX).ok()?;
                Some((bytes, alias, time))
            })
            .collect::<Vec<_>>();
        let mut known = BTreeSet::new();
        for (bytes, alias, time) in records {
            let provenance: BranchMergeProvenance = serde_json::from_slice(&bytes)
                .map_err(|_| Error::InvalidStoredValue("invalid branch merge provenance"))?;
            if provenance.source_lineage != source {
                continue;
            }
            let Ok(canonical) = BranchMergeProvenance::canonical(
                provenance.source_lineage,
                provenance.from_frontier.clone(),
                provenance.through_frontier.clone(),
                provenance.substitutions.clone(),
            ) else {
                continue;
            };
            if canonical != provenance {
                continue;
            }
            let alias = NodeAlias(alias);
            let Some(node_uuid) = self
                .node_aliases
                .iter()
                .find_map(|(node, candidate)| (*candidate == alias).then_some(*node))
            else {
                continue;
            };
            let tx_id = TxId::new(TxTime(time), node_uuid);
            let Some(target_tx) = self.query_transaction(tx_id)? else {
                continue;
            };
            if target_tx.tx.target_lineage != target {
                continue;
            }
            let emitted = self.query_versions_for_tx(tx_id)?;
            let mut validated = BTreeSet::new();
            let mut valid = true;
            for substitution in &provenance.substitutions {
                if !self.validate_lww_branch_substitution(
                    source,
                    &provenance,
                    substitution,
                    &emitted,
                )? {
                    valid = false;
                    break;
                }
                validated.extend(substitution.sources.iter().cloned());
            }
            if valid {
                known.extend(validated);
            }
        }
        // Native target contributions are represented without provenance. A
        // previously imported transaction expands through its validated
        // substitution, preventing A→B→C→A from echoing A's own writes.
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let write_tables = self
            .catalogue
            .catalogue_schemas
            .get(&write_schema_version)
            .ok_or(Error::InvalidStoredValue("branch write schema missing"))?
            .schema
            .tables
            .clone();
        for table in write_tables {
            let table_schema = self.table_in_schema(&table.name, write_schema_version)?;
            for row_uuid in self.lineage_row_ids(&table.name, target)? {
                for layer in [VersionLayer::Content, VersionLayer::Deletion] {
                    for version in
                        self.lineage_layer_versions(&table.name, row_uuid, layer, target)?
                    {
                        let tx_id = self.version_tx_id(&version)?;
                        let coordinates = match layer {
                            VersionLayer::Content => version
                                .authored_columns(&table_schema)?
                                .unwrap_or_default()
                                .into_iter()
                                .map(|column| ContributionCoordinate {
                                    table: table.name.clone(),
                                    row_uuid,
                                    layer: MergeAspect::Content,
                                    component: ContributionComponent::Column(column),
                                })
                                .collect::<Vec<_>>(),
                            VersionLayer::Deletion => vec![ContributionCoordinate {
                                table: table.name.clone(),
                                row_uuid,
                                layer: MergeAspect::Deletion,
                                component: ContributionComponent::Register,
                            }],
                        };
                        for coordinate in coordinates {
                            known.extend(
                                self.expanded_contribution_dots(target, tx_id, coordinate)?,
                            );
                        }
                    }
                }
            }
        }
        Ok(known)
    }

    pub(super) fn validate_lww_branch_substitution(
        &mut self,
        source: impl Into<BranchLineage>,
        provenance: &BranchMergeProvenance,
        substitution: &ContributionSubstitution,
        emitted: &[VersionRow],
    ) -> Result<bool, Error> {
        let source = source.into();
        let target = &substitution.target;
        let layer = match target.layer {
            MergeAspect::Content => VersionLayer::Content,
            MergeAspect::Deletion => VersionLayer::Deletion,
        };
        let Some(emitted_version) = emitted.iter().find(|version| {
            version.table() == target.table
                && version.row_uuid() == target.row_uuid
                && version.layer() == layer
        }) else {
            return Ok(false);
        };
        let source_versions =
            self.lineage_layer_versions(&target.table, target.row_uuid, layer, source)?;
        let through = provenance
            .through_frontier
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let table = self.table(&target.table)?.clone();
        let mut expected = BTreeSet::new();
        let mut winning_source: Option<(TxId, VersionRow)> = None;
        for source_version in source_versions {
            let source_tx_id = self.version_tx_id(&source_version)?;
            if !through.contains(&source_tx_id) {
                continue;
            }
            let authored = match &target.component {
                ContributionComponent::Column(column) => {
                    source_version.authored_columns(&table)?.map_or_else(
                        || {
                            source_version
                                .cell(&table, column)
                                .map(|value| value.is_some())
                        },
                        |columns| Ok(columns.contains(column)),
                    )?
                }
                ContributionComponent::Register => source_version.deletion().is_some(),
                ContributionComponent::Operation(_) => return Ok(false),
            };
            if !authored {
                continue;
            }
            expected.extend(self.expanded_contribution_dots(
                source,
                source_tx_id,
                target.clone(),
            )?);
            if winning_source
                .as_ref()
                .is_none_or(|(winner, _)| source_tx_id > *winner)
            {
                winning_source = Some((source_tx_id, source_version));
            }
        }
        if expected != substitution.sources.iter().cloned().collect() {
            return Ok(false);
        }
        let Some((_, winning_source)) = winning_source else {
            return Ok(false);
        };
        match &target.component {
            ContributionComponent::Column(column) => Ok(emitted_version
                .authored_columns(&table)?
                .is_some_and(|columns| columns.contains(column))
                && emitted_version.cell(&table, column)? == winning_source.cell(&table, column)?),
            ContributionComponent::Register => {
                Ok(emitted_version.deletion() == winning_source.deletion())
            }
            ContributionComponent::Operation(_) => Ok(false),
        }
    }

    fn branch_overlay_layer_versions(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        branch_id: BranchId,
    ) -> Result<Vec<VersionRow>, Error> {
        self.branch_overlay_layer_versions_for_schema(
            table,
            row_uuid,
            layer,
            branch_id,
            self.catalogue.current_write_schema.schema,
        )
    }

    fn branch_overlay_layer_versions_for_schema(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        branch_id: BranchId,
        schema_version: SchemaVersionId,
    ) -> Result<Vec<VersionRow>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, table)?;
        if !self
            .branches
            .branch_partitions
            .contains(&(table_id, branch_id))
        {
            return Ok(Vec::new());
        }
        let storage_table = physical_branch_version_storage_table_name(table_id, layer, branch_id);
        let raws = self
            .database
            .primary_key_scan_raw(&storage_table, &[Value::Uuid(row_uuid.0)])?
            .into_iter()
            .map(|raw| (SchemaVersionAlias(raw.schema_version()), raw.raw().to_vec()))
            .collect::<Vec<_>>();
        let mut versions = Vec::with_capacity(raws.len());
        for (schema_alias, raw) in raws {
            let schema_version =
                self.schema_version_for_alias(schema_alias)
                    .ok_or(Error::InvalidStoredValue(
                        "branch row schema version alias missing",
                    ))?;
            let logical_table = self.logical_table_for_physical_alias(table_id, schema_alias)?;
            let schema_table = self.table_in_schema(&logical_table, schema_version)?;
            let descriptor = match layer {
                VersionLayer::Content => schema_table.history_storage_table().record_schema(),
                VersionLayer::Deletion => schema_table.register_storage_table().record_schema(),
            };
            let version = VersionRow {
                table: groove::Intern::new(logical_table),
                record: OwnedRecord::new(raw, descriptor),
            };
            let tx_id = self.version_tx_id(&version)?;
            if self
                .transaction_record(tx_id)
                .is_some_and(|tx| !matches!(tx.fate, Fate::Rejected(_)))
            {
                versions.push(version);
            }
        }
        Ok(versions)
    }

    fn ensure_branch_open(&self, branch_id: BranchId) -> Result<(), Error> {
        match self.branches.branches.get(&branch_id) {
            Some(record) if record.state == codec::BranchState::Open => Ok(()),
            Some(_) => Err(Error::BranchClosed(branch_id)),
            None => Err(Error::BranchNotFound(branch_id)),
        }
    }

    fn commit_merge_transaction(
        &mut self,
        target: BranchLineage,
        branch_merge: BranchMergeProvenance,
        commits: Vec<MergeableCommit>,
    ) -> Result<TxId, Error>
    where
        S: ReopenableStorage,
    {
        match target {
            BranchLineage::Root => {
                let made_at = self.mint_tx_time(0);
                self.commit_mergeable_many_at_with_branch_merge(
                    commits,
                    made_at,
                    Some(branch_merge),
                )
            }
            BranchLineage::Branch(branch_id) => {
                let write_schema_version = self.catalogue.current_write_schema.schema;
                for table in commits
                    .iter()
                    .map(|commit| commit.table.clone())
                    .collect::<BTreeSet<_>>()
                {
                    self.persist_branch_partition(table, write_schema_version, branch_id)?;
                }
                let made_at = self.mint_tx_time(0);
                self.commit_mergeable_many_on_branch_at(
                    branch_id,
                    commits,
                    made_at,
                    Some(branch_merge),
                )
            }
        }
    }

    fn persist_branch_record(
        &mut self,
        record: &BranchRecord,
        metadata_pending: bool,
    ) -> Result<(), Error> {
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_branches",
            vec![
                Value::Uuid(record.branch_id.0),
                Value::Uuid(record.created_by.0),
                Value::Nullable(record.parent.map(|id| Box::new(Value::Uuid(id.0)))),
                Value::Nullable(record.base.as_ref().map(|base| {
                    Box::new(Value::Bytes(
                        serde_json::to_vec(base).expect("snapshot is serializable"),
                    ))
                })),
                Value::String(branch_state_string(record.state).to_owned()),
                Value::Bool(metadata_pending),
            ],
        );
        self.database.commit_batch(batch)?;
        if metadata_pending {
            self.branches
                .pending_metadata_uploads
                .insert(record.branch_id);
        } else {
            self.branches
                .pending_metadata_uploads
                .remove(&record.branch_id);
        }
        Ok(())
    }

    pub(super) fn recover_branch_record(
        &mut self,
        record: BorrowedRecord<'_>,
    ) -> Result<(), Error> {
        let branch_id = BranchId(record.get_uuid(BranchRowRecord::FIELD_BRANCH_ID_IDX)?);
        let created_by = AuthorId(record.get_uuid(BranchRowRecord::FIELD_CREATED_BY_IDX)?);
        let parent = record
            .get_nullable_uuid(BranchRowRecord::FIELD_PARENT_IDX)?
            .map(BranchId);
        let base = record
            .get_nullable_bytes(BranchRowRecord::FIELD_BASE_SNAPSHOT_IDX)?
            .map(|bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|_| Error::InvalidStoredValue("invalid stored branch snapshot"))
            })
            .transpose()?;
        let state =
            branch_state_from_discriminant(record.get_enum(BranchRowRecord::FIELD_STATE_IDX)?)?;
        let metadata_pending = record.get_bool(BranchRowRecord::FIELD_METADATA_PENDING_IDX)?;
        self.branches.branches.insert(
            branch_id,
            BranchRecord {
                branch_id,
                created_by,
                parent,
                base,
                state,
            },
        );
        if metadata_pending {
            self.branches.pending_metadata_uploads.insert(branch_id);
        }
        Ok(())
    }

    fn persist_branch_partition(
        &mut self,
        table: String,
        schema_version: SchemaVersionId,
        branch_id: BranchId,
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        let table_id = self.physical_table_id_for_schema(schema_version, &table)?;
        if !self
            .branches
            .branch_partitions
            .insert((table_id, branch_id))
        {
            return Ok(());
        }
        let mut batch = self.database.open_batch();
        batch.update(
            "jazz_branch_partitions",
            vec![Value::U64(table_id.0), Value::Uuid(branch_id.0)],
        );
        self.database.commit_batch(batch)?;
        if let Err(rebuild_error) = self.rebuild_database_slot() {
            self.branches
                .branch_partitions
                .remove(&(table_id, branch_id));
            let mut rollback = self.database.open_batch();
            rollback.delete(
                "jazz_branch_partitions",
                PrimaryKeyValue::Composite(vec![
                    PrimaryKeyValue::U64(table_id.0),
                    PrimaryKeyValue::Uuid(branch_id.0),
                ]),
            );
            self.database.commit_batch(rollback)?;
            self.rebuild_database_slot()?;
            return Err(rebuild_error);
        }
        Ok(())
    }

    pub(super) fn ensure_branch_target_partitions(
        &mut self,
        branch_id: BranchId,
        versions: &[VersionRecord],
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        self.ensure_branch_open(branch_id)?;
        let partitions = versions
            .iter()
            .map(|version| (version.table().to_owned(), version.schema_version()))
            .collect::<BTreeSet<_>>();
        // Validate the complete set before persisting any metadata. This makes
        // a malformed mixed-table unit all-or-nothing at the partition layer.
        for (table, schema_version) in &partitions {
            self.table_in_schema(table, *schema_version)?;
        }
        for (table, schema_version) in partitions {
            self.persist_branch_partition(table, schema_version, branch_id)?;
        }
        Ok(())
    }
}

fn branch_state_string(state: codec::BranchState) -> &'static str {
    match state {
        codec::BranchState::Open => "open",
        codec::BranchState::Merged => "merged",
        codec::BranchState::Discarded => "discarded",
    }
}

fn branch_state_from_discriminant(value: u8) -> Result<codec::BranchState, Error> {
    match value {
        0 => Ok(codec::BranchState::Open),
        1 => Ok(codec::BranchState::Merged),
        2 => Ok(codec::BranchState::Discarded),
        _ => Err(Error::InvalidStoredValue("unknown branch state")),
    }
}

fn branch_metadata_value(branch: &BranchRecord, column: &str) -> Option<Value> {
    match column {
        "branch_id" => Some(Value::Uuid(branch.branch_id.0)),
        "parent" => Some(Value::Nullable(
            branch.parent.map(|id| Box::new(Value::Uuid(id.0))),
        )),
        "base_global" => Some(Value::Nullable(
            branch
                .base
                .as_ref()
                .map(|base| Box::new(Value::U64(base.global_base.0))),
        )),
        "state" => Some(Value::String(branch_state_string(branch.state).to_owned())),
        _ => None,
    }
}

fn branch_metadata_current_row(
    table: &TableSchema,
    branch: &BranchRecord,
) -> Result<CurrentRow, Error> {
    let mut cells = BTreeMap::new();
    for column in &table.columns {
        if let Some(value) = branch_metadata_value(branch, &column.name) {
            cells.insert(column.name.clone(), value);
        }
    }
    current_row_from_cells(table, RowUuid(branch.branch_id.0), &cells)
}
