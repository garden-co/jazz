//! Independent brute-force semantic models for implementation tests. This
//! module owns expected merge, currency, rejection, and lens materialization
//! behavior used by seeded harnesses; production storage, validation, and query
//! execution deliberately live in [`crate::node`] and groove. It is outside the
//! runtime layer map and exists to check that the node conforms to
//! `jazz/README.md` and `jazz/SPEC/10_lenses_migrations.md` semantics.

use std::collections::{BTreeMap, BTreeSet};

use crate::ids::{AuthorId, RowUuid, SchemaVersionId};
use crate::protocol::{LensOp, MigrationLens};
use crate::schema::MergeStrategy;
use crate::time::{GlobalSeq, TxTime};
pub use crate::tx::DeletionEvent;
use crate::tx::{AbsentRead, DurabilityTier, Fate, RowRead, TxId};
use groove::records::Value;

/// Omniscient in-memory oracle; intentionally independent of groove.
///
/// The oracle assumes complete history for every row it reasons about. Direct
/// parent domination is sufficient only under that oracle contract; partial
/// downstream nodes must park orphans instead of reusing this logic blindly.
#[derive(Clone, Debug, Default)]
pub struct Oracle {
    versions: Vec<ModelRowVersion>,
    tx_states: BTreeMap<TxId, OracleTxState>,
    merge_strategies: BTreeMap<String, MergeStrategy>,
}

impl Oracle {
    /// Construct an empty oracle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an oracle with explicit per-column merge strategies.
    pub fn with_merge_strategies(
        merge_strategies: impl IntoIterator<Item = (String, MergeStrategy)>,
    ) -> Self {
        Self {
            merge_strategies: merge_strategies.into_iter().collect(),
            ..Self::default()
        }
    }

    /// Add a model row version.
    pub fn add_version(&mut self, version: ModelRowVersion) {
        self.versions.push(version);
    }

    /// Record a transaction state.
    pub fn record_tx_state(&mut self, tx_id: TxId, state: OracleTxState) {
        self.tx_states.insert(tx_id, state);
    }

    /// Return a recorded transaction state.
    pub fn tx_state(&self, tx_id: TxId) -> Option<&OracleTxState> {
        self.tx_states.get(&tx_id)
    }

    /// Return a model version by exact transaction and row identity.
    pub fn version_by_tx_and_row(
        &self,
        tx_id: TxId,
        row_uuid: RowUuid,
    ) -> Option<&ModelRowVersion> {
        self.versions
            .iter()
            .find(|version| version.tx_id == tx_id && version.row_uuid == row_uuid)
    }

    /// Merge cells from the row-scoped parent versions.
    pub fn merged_cells_for_parents(
        &self,
        row_uuid: RowUuid,
        parents: &[TxId],
    ) -> BTreeMap<String, Value> {
        let parent_set = parents.iter().copied().collect::<BTreeSet<_>>();
        let heads = self
            .versions
            .iter()
            .filter(|version| version.row_uuid == row_uuid && parent_set.contains(&version.tx_id))
            .collect::<Vec<_>>();
        self.merged_cells_from_heads(&heads)
    }

    /// Add a merge version for concurrent heads, if needed.
    pub fn merge_concurrent_heads(
        &mut self,
        row_uuid: RowUuid,
        tx_id: TxId,
        made_at: impl Into<TxTime>,
    ) -> Option<&ModelRowVersion> {
        let made_at = made_at.into();
        let heads = content_head_versions(self.row_versions(row_uuid));
        if heads.len() < 2 {
            return None;
        }
        let mut parents = heads
            .iter()
            .map(|version| version.tx_id)
            .collect::<Vec<_>>();
        parents.sort();
        let mut version = ModelRowVersion::new(row_uuid, tx_id, made_at);
        version.parents = parents;
        version.cells = self.merged_cells_from_heads(&heads);
        self.versions.push(version);
        self.versions.last()
    }

    /// Return the current content version for a row.
    pub fn current_version(&self, row_uuid: RowUuid) -> Option<&ModelRowVersion> {
        let row_versions = self.row_versions(row_uuid);
        current_content_version(row_versions)
    }

    /// Return the visible current version for a row.
    pub fn visible_current_version(&self, row_uuid: RowUuid) -> Option<&ModelRowVersion> {
        if matches!(
            self.current_deletion_event(row_uuid),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        self.current_version(row_uuid)
    }

    fn merged_cells_from_heads(&self, versions: &[&ModelRowVersion]) -> BTreeMap<String, Value> {
        merged_cells_with_history(versions, &self.versions, &self.merge_strategies)
    }

    /// Return visible current versions for all rows.
    pub fn visible_current_versions(&self) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| self.visible_current_version(row_uuid))
            .collect()
    }

    /// Return visible current versions restricted to delivered transactions.
    pub fn visible_current_versions_known_to(
        &self,
        known_tx_ids: &BTreeSet<TxId>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .filter(|version| known_tx_ids.contains(&version.tx_id))
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| {
                self.visible_current_version_with_states(
                    row_uuid,
                    Some(known_tx_ids),
                    Some(known_states),
                )
            })
            .collect()
    }

    /// Return visible current versions restricted to delivered transaction or row coverage.
    pub fn visible_current_versions_with_coverage(
        &self,
        known_tx_ids: &BTreeSet<TxId>,
        known_row_version_keys: &BTreeSet<(TxId, RowUuid)>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .filter(|version| {
                known_tx_ids.contains(&version.tx_id)
                    || known_row_version_keys.contains(&(version.tx_id, version.row_uuid))
            })
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| {
                self.visible_current_version_with_coverage(
                    row_uuid,
                    known_tx_ids,
                    known_row_version_keys,
                    known_states,
                )
            })
            .collect()
    }

    /// Return visible globally current versions restricted to known states.
    pub fn visible_global_current_versions_known_to(
        &self,
        known_tx_ids: &BTreeSet<TxId>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .filter(|version| known_tx_ids.contains(&version.tx_id))
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| {
                self.visible_global_current_version_with_states(
                    row_uuid,
                    known_tx_ids,
                    known_states,
                )
            })
            .collect()
    }

    /// Return the globally current content version for a row.
    pub fn global_current_version(&self, row_uuid: RowUuid) -> Option<&ModelRowVersion> {
        current_content_version(self.global_row_versions(row_uuid))
    }

    /// Return the visible globally current version for a row.
    pub fn visible_global_current_version(&self, row_uuid: RowUuid) -> Option<&ModelRowVersion> {
        if matches!(
            self.global_current_deletion_event(row_uuid),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        self.global_current_version(row_uuid)
    }

    /// Return visible globally current versions for all rows.
    pub fn visible_global_current_versions(&self) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| self.visible_global_current_version(row_uuid))
            .collect()
    }

    /// Return the current deletion-register event for a row.
    pub fn current_deletion_event(&self, row_uuid: RowUuid) -> Option<DeletionEvent> {
        self.row_versions_with_states(row_uuid, None, None)
            .into_iter()
            .filter_map(|version| version.deletion.map(|event| (version, event)))
            .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
            .map(|(_, event)| event)
    }

    /// Return the globally current deletion-register event for a row.
    pub fn global_current_deletion_event(&self, row_uuid: RowUuid) -> Option<DeletionEvent> {
        self.global_row_versions(row_uuid)
            .into_iter()
            .filter_map(|version| version.deletion.map(|event| (version, event)))
            .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
            .map(|(_, event)| event)
    }

    fn visible_current_version_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: Option<&BTreeSet<TxId>>,
        known_states: Option<&BTreeMap<TxId, OracleTxState>>,
    ) -> Option<&ModelRowVersion> {
        if matches!(
            self.current_deletion_event_with_states(row_uuid, known_tx_ids, known_states),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        current_content_version(self.row_versions_with_states(row_uuid, known_tx_ids, known_states))
    }

    fn visible_current_version_with_coverage(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_row_version_keys: &BTreeSet<(TxId, RowUuid)>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Option<&ModelRowVersion> {
        if matches!(
            self.current_deletion_event_with_coverage(
                row_uuid,
                known_tx_ids,
                known_row_version_keys,
                known_states
            ),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        current_content_version(self.row_versions_with_coverage(
            row_uuid,
            known_tx_ids,
            known_row_version_keys,
            known_states,
        ))
    }

    fn visible_global_current_version_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Option<&ModelRowVersion> {
        if matches!(
            self.global_current_deletion_event_with_states(row_uuid, known_tx_ids, known_states),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        current_content_version(self.global_row_versions_with_states(
            row_uuid,
            known_tx_ids,
            known_states,
        ))
    }

    fn global_current_deletion_event_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Option<DeletionEvent> {
        self.global_row_versions_with_states(row_uuid, known_tx_ids, known_states)
            .into_iter()
            .filter_map(|version| version.deletion.map(|event| (version, event)))
            .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
            .map(|(_, event)| event)
    }

    fn current_deletion_event_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: Option<&BTreeSet<TxId>>,
        known_states: Option<&BTreeMap<TxId, OracleTxState>>,
    ) -> Option<DeletionEvent> {
        self.row_versions_with_states(row_uuid, known_tx_ids, known_states)
            .into_iter()
            .filter_map(|version| version.deletion.map(|event| (version, event)))
            .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
            .map(|(_, event)| event)
    }

    fn current_deletion_event_with_coverage(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_row_version_keys: &BTreeSet<(TxId, RowUuid)>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Option<DeletionEvent> {
        self.row_versions_with_coverage(
            row_uuid,
            known_tx_ids,
            known_row_version_keys,
            known_states,
        )
        .into_iter()
        .filter_map(|version| version.deletion.map(|event| (version, event)))
        .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
        .map(|(_, event)| event)
    }

    fn row_versions(&self, row_uuid: RowUuid) -> Vec<&ModelRowVersion> {
        self.row_versions_with_states(row_uuid, None, None)
    }

    fn row_versions_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: Option<&BTreeSet<TxId>>,
        known_states: Option<&BTreeMap<TxId, OracleTxState>>,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .filter(|version| version.row_uuid == row_uuid)
            .filter(|version| {
                known_tx_ids.is_none_or(|known_tx_ids| known_tx_ids.contains(&version.tx_id))
            })
            .filter(|version| {
                let states = known_states.unwrap_or(&self.tx_states);
                !states
                    .get(&version.tx_id)
                    .is_some_and(OracleTxState::is_rejected)
            })
            .collect()
    }

    fn row_versions_with_coverage(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_row_version_keys: &BTreeSet<(TxId, RowUuid)>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .filter(|version| version.row_uuid == row_uuid)
            .filter(|version| {
                known_tx_ids.contains(&version.tx_id)
                    || known_row_version_keys.contains(&(version.tx_id, version.row_uuid))
            })
            .filter(|version| {
                !known_states
                    .get(&version.tx_id)
                    .is_some_and(OracleTxState::is_rejected)
            })
            .collect()
    }

    fn global_row_versions(&self, row_uuid: RowUuid) -> Vec<&ModelRowVersion> {
        self.row_versions(row_uuid)
            .into_iter()
            .filter(|version| {
                self.tx_states
                    .get(&version.tx_id)
                    .is_some_and(OracleTxState::is_globally_accepted)
            })
            .collect()
    }

    fn global_row_versions_with_states(
        &self,
        row_uuid: RowUuid,
        known_tx_ids: &BTreeSet<TxId>,
        known_states: &BTreeMap<TxId, OracleTxState>,
    ) -> Vec<&ModelRowVersion> {
        self.row_versions_with_states(row_uuid, Some(known_tx_ids), Some(known_states))
            .into_iter()
            .filter(|version| {
                known_states
                    .get(&version.tx_id)
                    .is_some_and(OracleTxState::is_globally_accepted)
            })
            .collect()
    }

    /// Return visible global content at a serialization base.
    pub fn visible_global_content_set_at(
        &self,
        global_base: GlobalSeq,
    ) -> BTreeSet<(RowUuid, TxId)> {
        self.versions
            .iter()
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| {
                self.visible_global_current_version_at(row_uuid, global_base)
                    .map(|version| (row_uuid, version.tx_id))
            })
            .collect()
    }

    /// Return visible global row versions at a serialization base.
    pub fn visible_global_current_versions_at(
        &self,
        global_base: GlobalSeq,
    ) -> Vec<&ModelRowVersion> {
        self.versions
            .iter()
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| self.visible_global_current_version_at(row_uuid, global_base))
            .collect()
    }

    /// Return visible global content at a serialization base, filtered by owner cell.
    pub fn visible_global_content_set_at_owner(
        &self,
        global_base: GlobalSeq,
        owner: AuthorId,
    ) -> BTreeSet<(RowUuid, TxId)> {
        self.versions
            .iter()
            .map(|version| version.row_uuid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|row_uuid| {
                self.visible_global_current_version_at(row_uuid, global_base)
                    .filter(|version| version.cells.get("owner") == Some(&Value::Uuid(owner.0)))
                    .map(|version| (row_uuid, version.tx_id))
            })
            .collect()
    }

    /// Return visible global current version at a serialization base.
    pub fn visible_global_current_version_at(
        &self,
        row_uuid: RowUuid,
        global_base: GlobalSeq,
    ) -> Option<&ModelRowVersion> {
        if matches!(
            self.global_current_deletion_event_at(row_uuid, global_base),
            Some(DeletionEvent::Deleted)
        ) {
            return None;
        }
        current_content_version(self.global_row_versions_at(row_uuid, global_base))
    }

    /// Return whether an exclusive row read matches at a serialization base.
    pub fn exclusive_row_read_matches_at(&self, read: &RowRead, global_base: GlobalSeq) -> bool {
        self.visible_global_current_version_at(read.row_uuid, global_base)
            .map(|version| version.tx_id)
            == Some(read.version)
    }

    /// Return whether an exclusive absent-row read matches at a serialization base.
    pub fn exclusive_absent_read_matches_at(
        &self,
        absent: &AbsentRead,
        global_base: GlobalSeq,
    ) -> bool {
        self.visible_global_current_version_at(absent.row_uuid, global_base)
            .is_none()
    }

    fn global_current_deletion_event_at(
        &self,
        row_uuid: RowUuid,
        global_base: GlobalSeq,
    ) -> Option<DeletionEvent> {
        self.global_row_versions_at(row_uuid, global_base)
            .into_iter()
            .filter_map(|version| version.deletion.map(|event| (version, event)))
            .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
            .map(|(_, event)| event)
    }

    fn global_row_versions_at(
        &self,
        row_uuid: RowUuid,
        global_base: GlobalSeq,
    ) -> Vec<&ModelRowVersion> {
        self.row_versions(row_uuid)
            .into_iter()
            .filter(|version| {
                self.tx_states
                    .get(&version.tx_id)
                    .and_then(|state| state.global_seq)
                    .is_some_and(|global_seq| global_seq <= global_base)
            })
            .collect()
    }
}

/// Independent multi-schema materialization oracle for migration-lens tests.
#[derive(Clone, Debug, Default)]
pub struct ParallelMaterializationOracle {
    schemas: BTreeSet<SchemaVersionId>,
    lenses: Vec<MigrationLens>,
    rows: BTreeMap<SchemaVersionId, BTreeMap<RowUuid, BTreeMap<String, Value>>>,
}

impl ParallelMaterializationOracle {
    /// Construct an empty parallel materialization oracle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a schema version into the materialization set.
    pub fn publish_schema(&mut self, schema: SchemaVersionId) {
        self.schemas.insert(schema);
        self.rows.entry(schema).or_default();
    }

    /// Publish a lens into the oracle's independent translation graph.
    pub fn publish_lens(&mut self, lens: MigrationLens) {
        self.lenses.push(lens);
    }

    /// Apply an accepted row write authored in `author_schema` to every known
    /// schema materialization and assert the translate/apply commutation law.
    pub fn apply_accepted_write(
        &mut self,
        author_schema: SchemaVersionId,
        row_uuid: RowUuid,
        cells: BTreeMap<String, Value>,
    ) {
        self.publish_schema(author_schema);
        let before = self.rows.clone();
        for schema in self.schemas.clone() {
            let projected = self
                .translate_cells(author_schema, schema, &cells)
                .unwrap_or_else(|| cells.clone());
            self.rows
                .entry(schema)
                .or_default()
                .insert(row_uuid, projected);
        }
        for source in self.schemas.clone() {
            for target in self.schemas.clone() {
                let mut translate_then_apply = before.get(&source).cloned().unwrap_or_default();
                let projected = self
                    .translate_cells(author_schema, source, &cells)
                    .unwrap_or_else(|| cells.clone());
                translate_then_apply.insert(row_uuid, projected);
                let translate_then_apply =
                    self.translate_materialization(source, target, &translate_then_apply);
                let apply_then_translate = self.rows.get(&target).cloned().unwrap_or_default();
                assert_eq!(
                    translate_then_apply, apply_then_translate,
                    "lens commutation failed from {source:?} to {target:?}"
                );
            }
        }
    }

    /// Return the visible rows for one schema version.
    pub fn rows(&self, schema: SchemaVersionId) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
        self.rows.get(&schema).cloned().unwrap_or_default()
    }

    fn translate_materialization(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        rows: &BTreeMap<RowUuid, BTreeMap<String, Value>>,
    ) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
        rows.iter()
            .filter_map(|(row, cells)| {
                self.translate_cells(source, target, cells)
                    .map(|cells| (*row, cells))
            })
            .collect()
    }

    fn translate_cells(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        cells: &BTreeMap<String, Value>,
    ) -> Option<BTreeMap<String, Value>> {
        if source == target {
            return Some(cells.clone());
        }
        self.translate_cells_forward(source, target, cells)
            .or_else(|| self.translate_cells_reverse(source, target, cells))
    }

    fn translate_cells_forward(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        cells: &BTreeMap<String, Value>,
    ) -> Option<BTreeMap<String, Value>> {
        let mut schema = source;
        let mut cells = cells.clone();
        let mut guard = 0usize;
        while schema != target {
            guard += 1;
            if guard > self.lenses.len() {
                return None;
            }
            let lens = self.lenses.iter().find(|lens| lens.source == schema)?;
            apply_oracle_lens_forward(lens, &mut cells)?;
            schema = lens.target;
        }
        Some(cells)
    }

    fn translate_cells_reverse(
        &self,
        source: SchemaVersionId,
        target: SchemaVersionId,
        cells: &BTreeMap<String, Value>,
    ) -> Option<BTreeMap<String, Value>> {
        let mut schema = source;
        let mut cells = cells.clone();
        let mut guard = 0usize;
        while schema != target {
            guard += 1;
            if guard > self.lenses.len() {
                return None;
            }
            let lens = self.lenses.iter().find(|lens| lens.target == schema)?;
            apply_oracle_lens_reverse(lens, &mut cells)?;
            schema = lens.source;
        }
        Some(cells)
    }
}

fn apply_oracle_lens_forward(
    lens: &MigrationLens,
    cells: &mut BTreeMap<String, Value>,
) -> Option<()> {
    let table_lens = lens.table_lenses.first()?;
    for op in &table_lens.ops {
        match op {
            LensOp::RenameTable { .. } => {}
            LensOp::RenameColumn { from, to } => {
                if let Some(value) = cells.remove(from) {
                    cells.insert(to.clone(), value);
                }
            }
            LensOp::CopyColumn { from, to } => {
                if let Some(value) = cells.get(from).cloned() {
                    cells.insert(to.clone(), value);
                }
            }
            LensOp::AddColumn { column, default } => {
                cells
                    .entry(column.clone())
                    .or_insert_with(|| default.clone());
            }
            LensOp::DropColumn { column, .. } => {
                cells.remove(column);
            }
            LensOp::TransformColumn { .. } | LensOp::RejectSourceDelta { .. } => return None,
        }
    }
    Some(())
}

fn apply_oracle_lens_reverse(
    lens: &MigrationLens,
    cells: &mut BTreeMap<String, Value>,
) -> Option<()> {
    let table_lens = lens.table_lenses.first()?;
    for op in table_lens.ops.iter().rev() {
        match op {
            LensOp::RenameTable { .. } => {}
            LensOp::RenameColumn { from, to } => {
                if let Some(value) = cells.remove(to) {
                    cells.insert(from.clone(), value);
                }
            }
            LensOp::CopyColumn { to, .. } | LensOp::AddColumn { column: to, .. } => {
                cells.remove(to);
            }
            LensOp::DropColumn {
                column,
                backwards_default,
            } => {
                cells
                    .entry(column.clone())
                    .or_insert_with(|| backwards_default.clone());
            }
            LensOp::TransformColumn { .. } | LensOp::RejectSourceDelta { .. } => return None,
        }
    }
    Some(())
}

fn current_content_version(row_versions: Vec<&ModelRowVersion>) -> Option<&ModelRowVersion> {
    content_head_versions(row_versions)
        .into_iter()
        .max_by_key(|version| version.tx_id.time.sort_key(version.tx_id.node))
}

/// Merge cells from model versions using HLC-LWW per column.
pub fn merged_cells(versions: &[&ModelRowVersion]) -> BTreeMap<String, Value> {
    merged_cells_with_strategies(versions, &BTreeMap::new())
}

fn merged_cells_with_strategies(
    versions: &[&ModelRowVersion],
    merge_strategies: &BTreeMap<String, MergeStrategy>,
) -> BTreeMap<String, Value> {
    let history = versions
        .iter()
        .map(|version| (*version).clone())
        .collect::<Vec<_>>();
    merged_cells_with_history(versions, &history, merge_strategies)
}

fn merged_cells_with_history(
    versions: &[&ModelRowVersion],
    history: &[ModelRowVersion],
    merge_strategies: &BTreeMap<String, MergeStrategy>,
) -> BTreeMap<String, Value> {
    let column_names = versions
        .iter()
        .flat_map(|version| version.cells.keys().cloned())
        .collect::<BTreeSet<_>>();
    let mut cells = BTreeMap::new();
    for column in column_names {
        match merge_strategies.get(&column).copied().unwrap_or_default() {
            MergeStrategy::Lww => {
                let mut value = versions
                    .iter()
                    .filter_map(|version| version.cells.get(&column).map(|value| (*version, value)))
                    .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
                    .map(|(_, value)| value.clone());
                if value.is_none() {
                    let parent_union = versions
                        .iter()
                        .flat_map(|version| version.parents.iter().copied())
                        .collect::<BTreeSet<_>>();
                    value = history
                        .iter()
                        .filter(|version| parent_union.contains(&version.tx_id))
                        .filter_map(|version| {
                            version.cells.get(&column).map(|value| (version, value))
                        })
                        .max_by_key(|(version, _)| version.tx_id.time.sort_key(version.tx_id.node))
                        .map(|(_, value)| value.clone());
                }
                if let Some(value) = value {
                    cells.insert(column, value);
                }
            }
            MergeStrategy::Counter => {
                let txs = versions
                    .iter()
                    .map(|version| version.tx_id)
                    .collect::<Vec<_>>();
                let by_tx = history
                    .iter()
                    .map(|version| (version.tx_id, version))
                    .collect::<BTreeMap<_, _>>();
                let mut memo = BTreeMap::new();
                let merged = oracle_counter_merge_value(&column, &by_tx, &txs, &mut memo);
                cells.insert(column, Value::U64(merged as u64));
            }
        }
    }
    cells
}

fn oracle_counter_merge_value(
    column: &str,
    versions_by_tx: &BTreeMap<TxId, &ModelRowVersion>,
    tx_ids: &[TxId],
    memo: &mut BTreeMap<Vec<TxId>, i128>,
) -> i128 {
    let mut key = tx_ids.to_vec();
    key.sort();
    key.dedup();
    key = oracle_counter_head_tx_ids(versions_by_tx, &key);
    if key.is_empty() {
        return 0;
    }
    if let Some(value) = memo.get(&key) {
        return *value;
    }

    let parent_union = key
        .iter()
        .filter_map(|tx_id| versions_by_tx.get(tx_id))
        .flat_map(|version| version.parents.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut merged = oracle_counter_merge_value(column, versions_by_tx, &parent_union, memo);
    for tx_id in &key {
        let Some(version) = versions_by_tx.get(tx_id) else {
            continue;
        };
        let Some(value) = version.cells.get(column) else {
            continue;
        };
        let parent_value =
            oracle_counter_merge_value(column, versions_by_tx, &version.parents, memo);
        merged += oracle_counter_value(value) - parent_value;
    }
    memo.insert(key, merged);
    merged
}

fn oracle_counter_head_tx_ids(
    versions_by_tx: &BTreeMap<TxId, &ModelRowVersion>,
    tx_ids: &[TxId],
) -> Vec<TxId> {
    let present = tx_ids.iter().copied().collect::<BTreeSet<_>>();
    let dominated = tx_ids
        .iter()
        .filter_map(|tx_id| versions_by_tx.get(tx_id))
        .flat_map(|version| version.parents.iter().copied())
        .filter(|parent| present.contains(parent))
        .collect::<BTreeSet<_>>();
    tx_ids
        .iter()
        .copied()
        .filter(|tx_id| !dominated.contains(tx_id))
        .collect()
}

fn oracle_counter_value(value: &Value) -> i128 {
    match value {
        Value::U8(value) => i128::from(*value),
        Value::U16(value) => i128::from(*value),
        Value::U32(value) => i128::from(*value),
        Value::U64(value) => i128::from(*value),
        other => panic!("counter oracle expected integer, got {other:?}"),
    }
}

fn content_head_versions(row_versions: Vec<&ModelRowVersion>) -> Vec<&ModelRowVersion> {
    let txs = row_versions
        .iter()
        .map(|version| version.tx_id)
        .collect::<BTreeSet<_>>();
    let dominated = row_versions
        .iter()
        .flat_map(|version| {
            version
                .parents
                .iter()
                .copied()
                .filter(|parent| txs.contains(parent))
        })
        .collect::<BTreeSet<_>>();

    row_versions
        .into_iter()
        .filter(|version| !version.cells.is_empty())
        .filter(|version| !dominated.contains(&version.tx_id))
        .collect()
}

/// One immutable row-version payload in the oracle.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelRowVersion {
    /// Row identity.
    pub row_uuid: RowUuid,
    /// Transaction id.
    pub tx_id: TxId,
    /// Parent transaction ids.
    pub parents: Vec<TxId>,
    /// Version timestamp.
    pub made_at: TxTime,
    /// Deletion-register event, if any.
    pub deletion: Option<DeletionEvent>,
    /// User cells.
    pub cells: BTreeMap<String, Value>,
}

/// Upstream-decided transaction state known to the oracle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleTxState {
    /// Transaction fate.
    pub fate: Fate,
    /// Global sequence, if accepted globally.
    pub global_seq: Option<GlobalSeq>,
    /// Durability tier.
    pub durability: DurabilityTier,
}

impl OracleTxState {
    /// Construct an oracle transaction state.
    pub fn new(fate: Fate, global_seq: Option<GlobalSeq>, durability: DurabilityTier) -> Self {
        Self {
            fate,
            global_seq,
            durability,
        }
    }

    fn is_globally_accepted(&self) -> bool {
        matches!(self.fate, Fate::Accepted) && self.global_seq.is_some()
    }

    fn is_rejected(&self) -> bool {
        matches!(self.fate, Fate::Rejected(_))
    }
}

impl ModelRowVersion {
    /// Construct a model row version.
    pub fn new(row_uuid: RowUuid, tx_id: TxId, made_at: impl Into<TxTime>) -> Self {
        Self {
            row_uuid,
            tx_id,
            parents: Vec::new(),
            deletion: None,
            cells: BTreeMap::new(),
            made_at: made_at.into(),
        }
    }

    /// Add a parent transaction id.
    pub fn with_parent(mut self, parent: TxId) -> Self {
        self.parents.push(parent);
        self
    }

    /// Add a user cell.
    pub fn with_cell(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.cells.insert(name.into(), Value::String(value.into()));
        self
    }

    /// Add a typed user cell.
    pub fn with_cell_value(mut self, name: impl Into<String>, value: Value) -> Self {
        self.cells.insert(name.into(), value);
        self
    }

    /// Add a deletion-register event.
    pub fn with_deletion(mut self, deletion: DeletionEvent) -> Self {
        self.deletion = Some(deletion);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeUuid;

    fn node(byte: u8) -> NodeUuid {
        NodeUuid::from_bytes([byte; 16])
    }

    fn row(byte: u8) -> RowUuid {
        RowUuid::from_bytes([byte; 16])
    }

    fn tx(node: NodeUuid, seq: u64) -> TxId {
        TxId::new(TxTime::from(seq), node)
    }

    #[test]
    fn parent_versions_dominate_their_ancestors() {
        let row = row(7);
        let n = node(1);
        let parent = tx(n, 1);
        let child = tx(n, 2);
        let mut oracle = Oracle::new();
        oracle.add_version(ModelRowVersion::new(row, parent, 10).with_cell("title", "a"));
        oracle.add_version(
            ModelRowVersion::new(row, child, 11)
                .with_parent(parent)
                .with_cell("title", "b"),
        );

        assert_eq!(
            oracle
                .current_version(row)
                .unwrap()
                .cells
                .get("title")
                .unwrap(),
            &Value::String("b".to_owned())
        );
    }

    #[test]
    fn concurrent_heads_use_hlc_lww_with_node_tiebreak() {
        let row = row(7);
        let low_node = node(1);
        let high_node = node(2);
        let mut oracle = Oracle::new();
        oracle
            .add_version(ModelRowVersion::new(row, tx(low_node, 1), 10).with_cell("title", "low"));
        oracle.add_version(
            ModelRowVersion::new(row, tx(high_node, 1), 10).with_cell("title", "high"),
        );

        assert_eq!(
            oracle
                .current_version(row)
                .unwrap()
                .cells
                .get("title")
                .unwrap(),
            &Value::String("high".to_owned())
        );
    }

    #[test]
    fn merge_versions_dominate_concurrent_heads() {
        let row = row(7);
        let n1 = node(1);
        let n2 = node(2);
        let core = node(9);
        let left = tx(n1, 1);
        let right = tx(n2, 1);
        let merge = tx(core, 1);
        let mut oracle = Oracle::new();
        oracle.add_version(ModelRowVersion::new(row, left, 10).with_cell("title", "older"));
        oracle.add_version(ModelRowVersion::new(row, right, 11).with_cell("body", "newer-body"));

        let merged = oracle.merge_concurrent_heads(row, merge, 11).unwrap();

        assert_eq!(merged.parents, vec![left, right]);
        assert_eq!(
            merged.cells,
            BTreeMap::from([
                ("body".to_owned(), Value::String("newer-body".to_owned())),
                ("title".to_owned(), Value::String("older".to_owned())),
            ])
        );
        assert_eq!(oracle.current_version(row).unwrap().tx_id, merge);
    }

    #[test]
    fn deletion_register_hides_newer_concurrent_content_until_restore() {
        let row = row(7);
        let n = node(1);
        let mut oracle = Oracle::new();
        oracle.add_version(ModelRowVersion::new(row, tx(n, 1), 10).with_cell("title", "base"));
        oracle.add_version(
            ModelRowVersion::new(row, tx(n, 2), 12).with_deletion(DeletionEvent::Deleted),
        );
        oracle.add_version(
            ModelRowVersion::new(row, tx(n, 3), 13).with_cell("title", "concurrent-update"),
        );

        assert!(oracle.visible_current_version(row).is_none());

        oracle.add_version(
            ModelRowVersion::new(row, tx(n, 4), 14).with_deletion(DeletionEvent::Restored),
        );

        assert_eq!(
            oracle
                .visible_current_version(row)
                .unwrap()
                .cells
                .get("title")
                .unwrap(),
            &Value::String("concurrent-update".to_owned())
        );
    }
}
