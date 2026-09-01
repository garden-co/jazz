//! Transaction, fate, durability, snapshot, and read-set vocabulary shared by
//! the facade, node, and protocol layers. This module owns the semantic data
//! structures for mergeable/exclusive transactions and authority outcomes; the
//! code that validates, stores, and syncs them lives in [`crate::node::ingest`],
//! [`crate::node::open_tx`], and [`crate::protocol`]. Merge and currency rules
//! are grounded in `jazz/README.md`.

use crate::ids::{AuthorSubject, NodeUuid, PhysicalTableId, RowUuid, SchemaVersionId};
use crate::protocol::{BranchKey, SnapshotRef};
use crate::query::{BindingId, Query, ShapeId};
use crate::schema::TableSchema;
use crate::time::{GlobalTime, TxTime};
use groove::records::{OwnedRecord, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Immutable transaction payload before upstream fate state are learned.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Transaction {
    /// Transaction id.
    pub tx_id: TxId,
    /// Transaction kind.
    pub kind: TxKind,
    /// Number of row-version records in the original commit unit.
    pub n_total_writes: u32,
    /// Author that made the transaction.
    pub made_by: AuthorSubject,
    /// Optional identity used for write-policy evaluation.
    ///
    /// When absent, policy is evaluated as `made_by`. Trusted serving-node
    /// flows use this to preserve user provenance while validating writes
    /// under a terminated request/session identity.
    pub permission_subject: Option<AuthorSubject>,
    /// Exclusive transaction snapshot, if any.
    pub base_snapshot: Option<Snapshot>,
    /// Exclusive point reads, if any.
    pub row_read_set: Option<Vec<RowRead>>,
    /// Exclusive absent-row reads, if any.
    pub absent_read_set: Option<Vec<AbsentRead>>,
    /// Exclusive predicate reads, if any.
    pub predicate_read_set: Option<Vec<PredicateRead>>,
    /// Optional application metadata attached at commit time.
    pub user_metadata_json: Option<String>,
    /// Non-causal field-grained provenance for a calculated branch-view merge.
    #[serde(default)]
    pub contribution_merge: Option<ContributionMergeProvenance>,
}

/// Non-causal evidence attached to an ordinary calculated merge transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ContributionMergeProvenance {
    /// Exact source branch coordinate read by the calculator.
    pub source: BranchKey,
    /// Exact target branch coordinate written by the calculator.
    pub target: BranchKey,
    /// Field-grained substitutions emitted by this transaction.
    pub substitutions: Vec<ContributionSubstitution>,
    /// Non-causal authorization evidence for a first head overlay copied from
    /// an inherited branch-view row.  It deliberately shares the existing
    /// non-causal transaction-provenance carrier with calculated merge
    /// substitutions: neither form is a history edge, merge dependency, read
    /// set, or CAS condition.
    pub branch_view_copies: Vec<BranchViewCopyEvidence>,
    /// Mandatory, canonical operation classification for every non-root
    /// branch write in this transaction. This is authorization metadata, not
    /// a history edge or a caller-trusted hint: admission matches it to the
    /// exact authored version and independently proves the declared case.
    #[serde(default)]
    pub branch_write_intents: Vec<BranchWriteIntent>,
}

/// Version-one operation intent for one branch-local row version.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BranchWriteIntent {
    /// Explicit durable layout version for this intent.
    pub version: u8,
    /// Stable physical table identity, paired with the schema that authored
    /// the version so renamed logical tables remain resolvable.
    pub physical_table_id: PhysicalTableId,
    /// Schema version that supplied the logical coordinate.
    pub authored_schema: SchemaVersionId,
    /// Row coordinate within the physical table and head.
    pub row_uuid: RowUuid,
    /// Canonical exact target head coordinate.
    pub head: BranchKey,
    /// Authority-verifiable operation classification.
    pub operation: BranchWriteOperation,
}

/// The authority-verifiable meaning of a branch-local write.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum BranchWriteOperation {
    /// A genuinely new row at this exact physical head.
    ExactHeadInsert,
    /// A write replacing an already-present exact head row.
    ExactHeadUpdate,
    /// A first head overlay that copied an inherited source from a view base.
    ViewUpdateCopy(BranchViewCopyEvidence),
}

/// Version-one, authority-verifiable logical source of a first head overlay.
///
/// A physical first write in `head` has no legal cross-branch history parent.
/// This descriptor therefore records the independently verifiable *logical*
/// source used for read-for-write authorization without changing causality.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BranchViewCopyEvidence {
    /// Explicit durable schema for this evidence; never infer a serializer
    /// layout or unversioned variant from the enclosing transaction.
    pub version: u8,
    /// Newly materialized target branch coordinate.
    pub head: BranchKey,
    /// Live or frozen branch-view base from which the source was selected.
    pub base: BranchViewCopyBase,
    /// Logical table containing the inherited source row.
    pub table: String,
    /// Logical row identity copied into the head.
    pub row_uuid: RowUuid,
    /// Exact admitted source content-version identity.
    pub source_version: TxId,
}

/// Canonical branch coordinates of the inherited source, with the only two
/// branch-view base modes represented explicitly.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum BranchViewCopyBase {
    /// Read the current winner in this base branch.
    Current(BranchKey),
    /// Read the winner at this exact frozen base snapshot.
    Snapshot {
        /// Base branch projected through the write schema.
        branch: BranchKey,
        /// Exact frozen frontier used to select the inherited source.
        snapshot: SnapshotRef,
    },
}

impl ContributionMergeProvenance {
    /// Canonicalize and validate locally calculated provenance.
    pub fn canonical(
        source: BranchKey,
        target: BranchKey,
        mut substitutions: Vec<ContributionSubstitution>,
    ) -> Result<Self, &'static str> {
        for substitution in &mut substitutions {
            substitution.sources.sort();
            substitution.sources.dedup();
            if substitution.sources.is_empty() {
                return Err("contribution substitution requires a source dot");
            }
        }
        substitutions.sort_by(|left, right| left.target.cmp(&right.target));
        if substitutions
            .windows(2)
            .any(|pair| pair[0].target == pair[1].target)
        {
            return Err("contribution substitution targets must be unique");
        }
        Ok(Self {
            source,
            target,
            substitutions,
            branch_view_copies: Vec::new(),
            branch_write_intents: Vec::new(),
        })
    }

    /// Construct the non-causal provenance carried by an inherited
    /// branch-view update or existing-target upsert.
    pub fn branch_view_copy(evidence: BranchViewCopyEvidence) -> Self {
        Self {
            // Calculated contribution merges use these coordinates as part of
            // their substitution algebra. A branch-view copy is only
            // authorization evidence, and its per-write coordinates live in
            // `branch_view_copies`; do not accidentally make a multi-table
            // branch-view transaction pretend it has one shared source or
            // target branch.
            source: BranchKey::default(),
            target: BranchKey::default(),
            substitutions: Vec::new(),
            branch_view_copies: vec![evidence],
            branch_write_intents: Vec::new(),
        }
    }

    /// Reject non-canonical or incomplete provenance received from a helper.
    pub fn validate(&self) -> Result<(), &'static str> {
        let canonical = Self::canonical(
            self.source.clone(),
            self.target.clone(),
            self.substitutions.clone(),
        )?;
        // Contribution substitutions retain their own canonical contract even
        // when a transaction also carries branch-write metadata. Do not let
        // the latter bypass duplicate-source/target validation.
        if canonical.source != self.source
            || canonical.target != self.target
            || canonical.substitutions != self.substitutions
        {
            return Err("contribution merge provenance must be canonical");
        }
        if !self.branch_view_copies.is_empty()
            && (!self.substitutions.is_empty()
                || self.source != BranchKey::default()
                || self.target != BranchKey::default())
        {
            return Err("branch-view copy evidence must not carry contribution substitutions");
        }
        let mut seen = BTreeSet::new();
        for evidence in &self.branch_view_copies {
            if evidence.version != 1
                || !seen.insert((
                    evidence.table.clone(),
                    evidence.row_uuid,
                    evidence.head.clone(),
                ))
            {
                return Err("branch-view copy evidence must be canonical v1 provenance");
            }
        }
        let mut intents = self.branch_write_intents.clone();
        intents.sort_by(|left, right| {
            (
                left.physical_table_id,
                left.authored_schema,
                left.row_uuid,
                &left.head,
            )
                .cmp(&(
                    right.physical_table_id,
                    right.authored_schema,
                    right.row_uuid,
                    &right.head,
                ))
        });
        if intents != self.branch_write_intents
            || intents.windows(2).any(|pair| {
                pair[0].physical_table_id == pair[1].physical_table_id
                    && pair[0].authored_schema == pair[1].authored_schema
                    && pair[0].row_uuid == pair[1].row_uuid
                    && pair[0].head == pair[1].head
            })
            || intents
                .iter()
                .any(|intent| intent.version != 1 || intent.head.values.is_empty())
        {
            return Err("branch write intents must be canonical v1 coordinates");
        }
        // The storage representation de-duplicates copy payloads and has
        // ViewUpdateCopy intents refer to them by index. Make the side-list
        // the exact ordered projection of already-canonical intents. This
        // binds the indirection by full value (not merely table/row/head),
        // makes every copy used exactly once, and gives the on-disk indices a
        // single canonical order independent of public write order.
        let mut expected_copies = Vec::new();
        for intent in &self.branch_write_intents {
            let BranchWriteOperation::ViewUpdateCopy(evidence) = &intent.operation else {
                continue;
            };
            if evidence.row_uuid != intent.row_uuid || evidence.head != intent.head {
                return Err("branch write copy evidence is not bound to its intent");
            }
            expected_copies.push(evidence.clone());
        }
        if self.branch_view_copies != expected_copies {
            return Err("branch write copy evidence must be the canonical intent projection");
        }
        Ok(())
    }
}

/// One derived target field and the exact native contribution dots it represents.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ContributionSubstitution {
    /// Field or register emitted by the target transaction.
    pub target: ContributionCoordinate,
    /// Canonically sorted source dots represented by the target field.
    pub sources: Vec<ContributionDot>,
}

/// Stable field-grained coordinate within one branch-keyed row branch-local row.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContributionCoordinate {
    /// Exact branch key containing the field.
    pub branch_key: BranchKey,
    /// Logical table name.
    pub table: String,
    /// Global object identity.
    pub row_uuid: RowUuid,
    /// Independent content or deletion layer.
    pub layer: MergeAspect,
    /// Column, operation, or register identity.
    pub component: ContributionComponent,
}

/// Field or strategy-operation identity within one row layer.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum ContributionComponent {
    /// Ordinary named content column.
    Column(String),
    /// Strategy-defined stable operation identity.
    Operation {
        /// Column whose merge strategy defines the operation.
        column: String,
        /// Canonical strategy-specific bytes interpreted using the declared column type.
        identity: Vec<u8>,
    },
    /// Deletion/restore register.
    Register,
}

/// Stable identity of one native contribution.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContributionDot {
    /// Transaction that introduced the native contribution.
    pub tx_id: TxId,
    /// Exact branch-keyed field or register introduced by the transaction.
    pub coordinate: ContributionCoordinate,
}

/// Index of non-causal substitutions used to compare contribution histories.
///
/// The index deliberately contains no branch lifecycle or merge cursor. It is
/// reconstructed from ordinary visible transaction metadata for each local
/// calculation.
#[derive(Clone, Debug, Default)]
pub struct ContributionSubstitutionIndex {
    substitutions: BTreeMap<ContributionDot, Vec<ContributionDot>>,
}

impl ContributionSubstitutionIndex {
    /// Add the substitutions introduced by one calculated merge transaction.
    pub fn observe(
        &mut self,
        tx_id: TxId,
        provenance: &ContributionMergeProvenance,
    ) -> Result<(), &'static str> {
        for substitution in &provenance.substitutions {
            let target = ContributionDot {
                tx_id,
                coordinate: substitution.target.clone(),
            };
            if self
                .substitutions
                .insert(target, substitution.sources.clone())
                .is_some()
            {
                return Err("contribution substitution target observed twice");
            }
        }
        Ok(())
    }

    /// Recursively replace derived dots with their native contribution roots.
    pub fn expand(
        &self,
        dots: impl IntoIterator<Item = ContributionDot>,
    ) -> Result<BTreeSet<ContributionDot>, &'static str> {
        fn visit(
            index: &ContributionSubstitutionIndex,
            dot: ContributionDot,
            visiting: &mut BTreeSet<ContributionDot>,
            expanded: &mut BTreeSet<ContributionDot>,
        ) -> Result<(), &'static str> {
            let Some(sources) = index.substitutions.get(&dot) else {
                expanded.insert(dot);
                return Ok(());
            };
            if !visiting.insert(dot.clone()) {
                return Err("contribution substitution cycle");
            }
            for source in sources {
                visit(index, source.clone(), visiting, expanded)?;
            }
            visiting.remove(&dot);
            Ok(())
        }

        let mut expanded = BTreeSet::new();
        for dot in dots {
            visit(self, dot, &mut BTreeSet::new(), &mut expanded)?;
        }
        Ok(expanded)
    }

    /// Return source roots not already represented by target history.
    pub fn novel(
        &self,
        source: impl IntoIterator<Item = ContributionDot>,
        target: impl IntoIterator<Item = ContributionDot>,
    ) -> Result<BTreeSet<ContributionDot>, &'static str> {
        let source = self.expand(source)?;
        let target = self.expand(target)?;
        Ok(source.difference(&target).cloned().collect())
    }
}

/// Deletion register event carried by a row version.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum DeletionEvent {
    /// Row was deleted.
    Deleted,
    /// Row was restored.
    Restored,
}

/// Transaction identity: HLC time plus creating node tie-breaker.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct TxId {
    /// Node-minted HLC time.
    pub time: TxTime,
    /// Node that created the transaction.
    pub node: NodeUuid,
}

impl TxId {
    /// Construct a transaction id.
    pub fn new(time: TxTime, node: NodeUuid) -> Self {
        Self { time, node }
    }

    /// Return the time's physical milliseconds component.
    pub fn physical_ms(self) -> u64 {
        self.time.physical_ms()
    }
}

/// The two transaction isolation/fate regimes.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum TxKind {
    /// Mergeable transaction validated by CRDT-style merge rules.
    Mergeable,
    /// Exclusive transaction validated by authority-side read sets.
    Exclusive,
}

/// Fate assigned by the authority for a committed transaction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum Fate {
    /// Fate is not yet known.
    Pending,
    /// Transaction was accepted.
    Accepted,
    /// Transaction was rejected with a structured reason.
    Rejected(RejectionReason),
}

/// Structured rejection cause surfaced to applications.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum RejectionReason {
    /// Client timestamp exceeded the admission tolerance.
    ClientClockTooFarAhead,
    /// Author or write policy rejected the transaction.
    AuthorizationDenied,
    /// Exclusive validation detected a read/write conflict.
    ExclusiveConflict,
    /// A version timestamp was not strictly greater than every parent.
    CausalityViolation,
    /// Transaction was rejected because an ancestor was rejected.
    Cascade {
        /// Root rejected transaction.
        root: TxId,
    },
    /// Commit payload was malformed.
    MalformedCommit(String),
}

/// Highest durability tier this node has observed for a transaction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum DurabilityTier {
    /// Not durable outside the local process.
    None,
    /// Stored locally.
    Local,
    /// Stored at an edge tier.
    Edge,
    /// Accepted and stored at the global authority.
    Global,
}

/// Stored history layer for a row version.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum MergeAspect {
    /// User content cell version.
    Content,
    /// Deletion or restore register event.
    Deletion,
}

/// Durable transaction audit record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionRecord {
    /// Transaction id.
    pub tx_id: TxId,
    /// Author that made the transaction.
    pub made_by: AuthorSubject,
    /// Transaction kind.
    pub kind: TxKind,
    /// Number of row-version records in the original commit unit.
    pub n_total_writes: u32,
    /// Latest known fate.
    pub fate: Fate,
    /// Assigned global timestamp, when accepted globally.
    pub global_time: Option<GlobalTime>,
    /// Highest observed durability tier.
    pub durability: DurabilityTier,
    /// Optional application metadata attached at commit time.
    pub user_metadata_json: Option<String>,
    /// Non-causal field-grained provenance for a calculated branch-view merge.
    pub contribution_merge: Option<ContributionMergeProvenance>,
}

/// Stored edit-history entry for a row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEntry {
    table: String,
    version: OwnedRecord,
    transaction: TransactionRecord,
    is_locally_current: bool,
    is_globally_current: bool,
}

impl HistoryEntry {
    /// Construct a history entry from encoded storage rows and transaction state.
    pub(crate) fn new(
        table: impl Into<String>,
        version: OwnedRecord,
        transaction: TransactionRecord,
        is_locally_current: bool,
        is_globally_current: bool,
    ) -> Self {
        Self {
            table: table.into(),
            version,
            transaction,
            is_locally_current,
            is_globally_current,
        }
    }

    /// Logical table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Transaction id that wrote this version.
    pub fn tx_id(&self) -> TxId {
        self.transaction.tx_id
    }

    /// Author that made the transaction.
    pub fn made_by(&self) -> AuthorSubject {
        self.transaction.made_by
    }

    /// Transaction HLC timestamp.
    pub fn made_at(&self) -> TxTime {
        self.transaction.tx_id.time
    }

    /// Transaction kind.
    pub fn kind(&self) -> TxKind {
        self.transaction.kind
    }

    /// Latest known fate.
    pub fn fate(&self) -> Fate {
        self.transaction.fate.clone()
    }

    /// Assigned global timestamp, when accepted globally.
    pub fn global_time(&self) -> Option<GlobalTime> {
        self.transaction.global_time
    }

    /// Highest observed durability tier.
    pub fn durability(&self) -> DurabilityTier {
        self.transaction.durability
    }

    /// Direct parent transaction ids for this version.
    pub fn parents(&self) -> Vec<TxId> {
        let field = self
            .version
            .descriptor()
            .field_index("parents")
            .expect("history record has parents");
        tx_ids_from_value(
            self.version
                .borrowed()
                .get_idx(field)
                .expect("valid history parents"),
        )
        .expect("valid history parent refs")
    }

    /// Cell value by application-schema column position.
    pub fn cell_at(&self, column_position: usize) -> Option<Value> {
        if self.is_register_record() {
            return None;
        }
        let user_cells = self
            .version
            .descriptor()
            .field_index("updated_at")
            .map_or(HistoryRowRecord::USER_CELLS, |idx| idx + 1);
        self.version
            .borrowed()
            .get_idx(user_cells + column_position)
            .expect("valid history cell")
            .nullable_value()
            .expect("valid nullable history cell")
    }

    /// Cell value by application column name using the table schema to resolve position.
    pub fn cell(&self, table: &TableSchema, column: &str) -> Option<Value> {
        table
            .columns
            .iter()
            .position(|candidate| candidate.name == column)
            .and_then(|idx| self.cell_at(idx))
    }

    /// Deletion-register event, if this is a deletion layer version.
    pub fn deletion(&self) -> Option<DeletionEvent> {
        if !self.is_register_record() {
            return None;
        }
        let field = self
            .version
            .descriptor()
            .field_index("_deletion")
            .expect("register history has deletion field");
        deletion_from_value(
            self.version
                .borrowed()
                .get_idx(field)
                .expect("valid history deletion"),
        )
        .expect("valid history deletion")
    }

    /// Storage/history layer for this version.
    pub fn layer(&self) -> MergeAspect {
        if self.deletion().is_some() {
            MergeAspect::Deletion
        } else {
            MergeAspect::Content
        }
    }

    /// Whether this version is locally current on this node.
    pub fn is_locally_current(&self) -> bool {
        self.is_locally_current
    }

    /// Whether this version is globally current on this node.
    pub fn is_globally_current(&self) -> bool {
        self.is_globally_current
    }

    fn is_register_record(&self) -> bool {
        self.version.descriptor().field_index("_deletion").is_some()
    }
}

/// Durable rejected transaction payload retained on its originating node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedTransaction {
    tx_id: TxId,
    record: OwnedRecord,
    versions: Vec<RejectedVersion>,
}

impl RejectedTransaction {
    /// Construct a rejected transaction wrapper from an encoded storage row.
    pub(crate) fn new(tx_id: TxId, record: OwnedRecord, versions: Vec<RejectedVersion>) -> Self {
        Self {
            tx_id,
            record,
            versions,
        }
    }

    /// Transaction id.
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Transaction kind.
    pub fn kind(&self) -> TxKind {
        tx_kind_from_discriminant(
            self.record
                .borrowed()
                .get_enum(RejectedTransactionRowRecord::FIELD_KIND_IDX)
                .expect("valid rejected kind"),
        )
        .expect("valid rejected kind")
    }

    /// Author that made the transaction.
    pub fn made_by(&self) -> AuthorSubject {
        AuthorSubject::from_canonical(
            self.record
                .borrowed()
                .get_str(RejectedTransactionRowRecord::FIELD_MADE_BY_IDX)
                .expect("valid rejected author"),
        )
        .expect("canonical rejected author")
    }

    /// Transaction HLC timestamp.
    pub fn made_at(&self) -> TxTime {
        self.tx_id.time
    }

    /// Structured rejection reason.
    pub fn reason(&self) -> RejectionReason {
        rejection_reason_from_rejected_record(self.record.borrowed())
            .expect("valid rejected reason")
    }

    /// Root rejected transaction when this is a cascade rejection.
    pub fn cascade_root(&self) -> Option<TxId> {
        match self.reason() {
            RejectionReason::Cascade { root } => Some(root),
            _ => None,
        }
    }

    /// Optional application metadata attached at commit time.
    pub fn user_metadata_json(&self) -> Option<&str> {
        self.record
            .borrowed()
            .get_nullable_string(RejectedTransactionRowRecord::FIELD_USER_METADATA_IDX)
            .expect("valid rejected metadata")
    }

    /// Rejected version payloads for application-level retry derivation.
    pub fn versions(&self) -> &[RejectedVersion] {
        &self.versions
    }
}

/// Durable rejected version payload retained on its originating node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedVersion {
    table: String,
    record: OwnedRecord,
}

impl RejectedVersion {
    /// Construct a rejected version wrapper from an encoded storage row.
    pub(crate) fn new(table: impl Into<String>, record: OwnedRecord) -> Self {
        Self {
            table: table.into(),
            record,
        }
    }

    /// Logical table name.
    pub fn table(&self) -> String {
        self.table.clone()
    }

    /// Row written by the rejected version.
    pub fn row_uuid(&self) -> RowUuid {
        RowUuid(
            self.record
                .borrowed()
                .get_uuid(RejectedVersionRowRecord::FIELD_ROW_UUID_IDX)
                .expect("valid rejected row uuid"),
        )
    }

    /// Direct parent transaction ids.
    pub fn parents(&self) -> Vec<TxId> {
        tx_ids_from_value(
            self.record
                .borrowed()
                .get_idx(RejectedVersionRowRecord::FIELD_PARENTS_IDX)
                .expect("valid rejected parents"),
        )
        .expect("valid rejected parents")
    }

    /// Cell value by application-schema column position.
    pub fn cell_at(&self, column_position: usize) -> Option<Value> {
        self.record
            .borrowed()
            .get_idx(RejectedVersionRowRecord::USER_CELLS + column_position)
            .expect("valid rejected user cell")
            .nullable_value()
            .expect("valid nullable rejected user cell")
    }

    /// Cell value by application column name using the table schema to resolve position.
    pub fn cell(&self, table: &TableSchema, column: &str) -> Option<Value> {
        table
            .columns
            .iter()
            .position(|candidate| candidate.name == column)
            .and_then(|idx| self.cell_at(idx))
    }

    /// Deletion-register event, if any.
    pub fn deletion(&self) -> Option<DeletionEvent> {
        deletion_from_value(
            self.record
                .borrowed()
                .get_idx(RejectedVersionRowRecord::FIELD__DELETION_IDX)
                .expect("valid rejected deletion"),
        )
        .expect("valid rejected deletion")
    }

    #[cfg(test)]
    pub(crate) fn test_cells(&self, table: &TableSchema) -> BTreeMap<String, Value> {
        table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| self.cell_at(idx).map(|value| (column.name.clone(), value)))
            .collect()
    }
}

trait NullableValue {
    fn nullable_value(self) -> Result<Option<Value>, &'static str>;
}

impl NullableValue for Value {
    fn nullable_value(self) -> Result<Option<Value>, &'static str> {
        match self {
            Value::Nullable(None) => Ok(None),
            Value::Nullable(Some(value)) => Ok(Some(*value)),
            _ => Err("nullable value expected"),
        }
    }
}

/// Compact dotted view description captured by the node that created it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Snapshot {
    /// Node that opened the transaction.
    pub owner: NodeUuid,
    /// Contiguous global base visible at open time.
    pub global_base: GlobalTime,
    /// Local base visible at open time.
    pub local_base: TxTime,
    /// Additional visible transaction dots.
    pub dots: Vec<TxId>,
}

impl Snapshot {
    /// Create an exclusive base snapshot.
    pub fn exclusive_base(
        owner: NodeUuid,
        global_base: GlobalTime,
        local_base: TxTime,
        dots: Vec<TxId>,
    ) -> Result<Self, &'static str> {
        Ok(Self {
            owner,
            global_base,
            local_base,
            dots,
        })
    }
}

/// Point read captured by an open exclusive transaction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct RowRead {
    /// Table read.
    pub table: String,
    /// Row read.
    pub row_uuid: RowUuid,
    /// Version observed by the read.
    pub version: TxId,
}

/// Absent-row read captured by an open exclusive transaction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct AbsentRead {
    /// Table read.
    pub table: String,
    /// Row proven absent.
    pub row_uuid: RowUuid,
}

/// Predicate read captured by an open exclusive transaction.
///
/// M3 v0 records whole-table current-row reads as degenerate query shapes.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PredicateRead {
    /// Table read by the predicate.
    pub table: String,
    /// Content-addressed query shape id.
    pub shape_id: ShapeId,
    /// Canonical query AST carried so validators do not need a prior shape registration.
    pub shape: Query,
    /// Binding id for the captured parameter values.
    pub binding_id: BindingId,
    /// Binding values carried so validators do not need a prior binding registration.
    pub binding_values: BTreeMap<String, Value>,
}

groove::define_record! {
    struct HistoryRowRecord {
        0 => row_uuid: RowUuid,
        1 => tx_time: u64,
        2 => tx_node_id: u64,
        3 => schema_version: u64,
        4 => parents: ParentRefs,
        5 => created_by: AuthorSubject,
        6 => created_at: u64,
        7 => updated_by: AuthorSubject,
        8 => updated_at: u64,
        .. user_cells,
    }
}

groove::define_record! {
    struct RegisterRowRecord {
        0 => row_uuid: RowUuid,
        1 => tx_time: u64,
        2 => tx_node_id: u64,
        3 => schema_version: u64,
        4 => parents: ParentRefs,
        5 => created_by: AuthorSubject,
        6 => created_at: u64,
        7 => updated_by: AuthorSubject,
        8 => updated_at: u64,
        9 => _deletion: Value,
    }
}

groove::define_record! {
    struct RejectedTransactionRowRecord {
        0 => time: u64,
        1 => node_id: u64,
        2 => kind: TxKind,
        3 => made_by: AuthorSubject,
        4 => rejection_reason: RejectionReasonTag,
        5 => cascade_root: Option<Value>,
        6 => reason_detail: Option<String>,
        7 => user_metadata: Option<String>,
    }
}

groove::define_record! {
    struct RejectedVersionRowRecord {
        0 => tx_time: u64,
        1 => tx_node_id: u64,
        2 => row_uuid: RowUuid,
        3 => layer: Vec<u8>,
        4 => parents: ParentRefs,
        5 => _deletion: Option<Value>,
        .. user_cells,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParentRefs(Vec<TxId>);

impl groove::records::RecordField for ParentRefs {
    fn read(
        record: &groove::records::BorrowedRecord<'_>,
        idx: usize,
    ) -> Result<Self, groove::records::Error> {
        tx_ids_from_value(record.get_idx(idx)?)
            .map(Self)
            .map_err(|_| groove::records::Error::TypeMismatch {
                expected: groove::records::ValueType::Array(Box::new(
                    groove::records::ValueType::Tuple(vec![
                        groove::records::ValueType::U64,
                        groove::records::ValueType::Uuid,
                    ]),
                )),
            })
    }

    fn to_value(&self) -> Value {
        Value::Array(self.0.iter().map(|parent| tx_id_value(*parent)).collect())
    }

    const COLUMN_KIND: groove::records::FieldKind = groove::records::FieldKind::Array;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RejectionReasonTag {
    ClientClockTooFarAhead,
    AuthorizationDenied,
    ExclusiveConflict,
    CausalityViolation,
    Cascade,
    MalformedCommit,
}

groove::impl_record_field_enum!(RejectionReasonTag {
    RejectionReasonTag::ClientClockTooFarAhead = 0,
    RejectionReasonTag::AuthorizationDenied = 1,
    RejectionReasonTag::ExclusiveConflict = 2,
    RejectionReasonTag::CausalityViolation = 3,
    RejectionReasonTag::Cascade = 4,
    RejectionReasonTag::MalformedCommit = 5,
});

fn tx_kind_from_discriminant(value: u8) -> Result<TxKind, &'static str> {
    match value {
        0 => Ok(TxKind::Mergeable),
        1 => Ok(TxKind::Exclusive),
        _ => Err("unknown tx kind"),
    }
}

fn tx_ids_from_value(value: Value) -> Result<Vec<TxId>, &'static str> {
    match value {
        Value::Array(values) => values.into_iter().map(tx_id_from_value).collect(),
        _ => Err("parents"),
    }
}

fn tx_id_from_value(value: Value) -> Result<TxId, &'static str> {
    match value {
        Value::Tuple(values) if values.len() == 2 => {
            let mut values = values.into_iter();
            let Value::U64(time) = values.next().expect("len checked") else {
                return Err("tx id time");
            };
            let Value::Uuid(node) = values.next().expect("len checked") else {
                return Err("tx id node");
            };
            Ok(TxId::new(TxTime(time), NodeUuid(node)))
        }
        _ => Err("tx id tuple"),
    }
}

fn tx_id_value(tx_id: TxId) -> Value {
    Value::Tuple(vec![Value::U64(tx_id.time.0), Value::Uuid(tx_id.node.0)])
}

fn deletion_from_value(value: Value) -> Result<Option<DeletionEvent>, &'static str> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => deletion_from_value(*value),
        Value::EnumTag(0) => Ok(Some(DeletionEvent::Deleted)),
        Value::EnumTag(1) => Ok(Some(DeletionEvent::Restored)),
        Value::U8(0) => Ok(Some(DeletionEvent::Deleted)),
        Value::U8(1) => Ok(Some(DeletionEvent::Restored)),
        _ => Err("deletion"),
    }
}

fn nullable_tx_id_value(value: Value) -> Result<Option<TxId>, &'static str> {
    match value {
        Value::Nullable(None) => Ok(None),
        Value::Nullable(Some(value)) => tx_id_from_value(*value).map(Some),
        _ => Err("tx id nullable"),
    }
}

fn rejection_reason_from_rejected_record(
    record: groove::records::BorrowedRecord<'_>,
) -> Result<RejectionReason, &'static str> {
    let tag = record
        .get_enum(RejectedTransactionRowRecord::FIELD_REJECTION_REASON_IDX)
        .map_err(|_| "reason")?;
    match tag {
        0 => Ok(RejectionReason::ClientClockTooFarAhead),
        1 => Ok(RejectionReason::AuthorizationDenied),
        2 => Ok(RejectionReason::ExclusiveConflict),
        3 => Ok(RejectionReason::CausalityViolation),
        4 => Ok(RejectionReason::Cascade {
            root: nullable_tx_id_value(
                record
                    .get_idx(RejectedTransactionRowRecord::FIELD_CASCADE_ROOT_IDX)
                    .map_err(|_| "cascade root")?,
            )?
            .ok_or("cascade root")?,
        }),
        5 => Ok(RejectionReason::MalformedCommit(
            record
                .get_nullable_string(RejectedTransactionRowRecord::FIELD_REASON_DETAIL_IDX)
                .map_err(|_| "reason detail")?
                .unwrap_or_default()
                .to_owned(),
        )),
        _ => Err("reason"),
    }
}

#[cfg(test)]
mod contribution_tests {
    use super::*;

    fn tx(time: u64) -> TxId {
        TxId::new(TxTime(time), NodeUuid::from_bytes([time as u8; 16]))
    }

    fn coordinate(branch: &str) -> ContributionCoordinate {
        ContributionCoordinate {
            branch_key: BranchKey::default(),
            table: "todos".to_owned(),
            row_uuid: RowUuid::from_bytes([7; 16]),
            layer: MergeAspect::Content,
            component: ContributionComponent::Column(format!("{branch}:title")),
        }
    }

    fn provenance(target: &str, source: ContributionDot) -> ContributionMergeProvenance {
        ContributionMergeProvenance::canonical(
            BranchKey::default(),
            BranchKey::default(),
            vec![ContributionSubstitution {
                target: coordinate(target),
                sources: vec![source],
            }],
        )
        .unwrap()
    }

    fn branch_intent(row: u8) -> BranchWriteIntent {
        BranchWriteIntent {
            version: 1,
            physical_table_id: PhysicalTableId(1),
            authored_schema: SchemaVersionId(uuid::Uuid::from_bytes([1; 16])),
            row_uuid: RowUuid::from_bytes([row; 16]),
            head: BranchKey {
                values: vec![(
                    "branch".to_owned(),
                    crate::protocol::BranchColumnValue::from(Value::Uuid(uuid::Uuid::from_bytes(
                        [2; 16],
                    ))),
                )],
            },
            operation: BranchWriteOperation::ExactHeadInsert,
        }
    }

    fn view_copy_intent(row: u8) -> (BranchViewCopyEvidence, BranchWriteIntent) {
        let mut intent = branch_intent(row);
        let evidence = BranchViewCopyEvidence {
            version: 1,
            head: intent.head.clone(),
            base: BranchViewCopyBase::Current(BranchKey::default()),
            table: "todos".to_owned(),
            row_uuid: intent.row_uuid,
            source_version: tx(u64::from(row)),
        };
        intent.operation = BranchWriteOperation::ViewUpdateCopy(evidence.clone());
        (evidence, intent)
    }

    #[test]
    fn branch_write_intents_reject_noncanonical_order_and_duplicates() {
        let mut provenance = ContributionMergeProvenance {
            source: BranchKey::default(),
            target: BranchKey::default(),
            substitutions: Vec::new(),
            branch_view_copies: Vec::new(),
            branch_write_intents: vec![branch_intent(2), branch_intent(1)],
        };
        assert!(provenance.validate().is_err());
        provenance
            .branch_write_intents
            .sort_by_key(|intent| intent.row_uuid);
        assert!(provenance.validate().is_ok());
        provenance.branch_write_intents.push(branch_intent(1));
        assert!(provenance.validate().is_err());
    }

    #[test]
    fn branch_write_copy_evidence_is_exact_canonical_intent_projection() {
        let (stored, intent) = view_copy_intent(7);
        let mut provenance = ContributionMergeProvenance {
            source: BranchKey::default(),
            target: BranchKey::default(),
            substitutions: Vec::new(),
            branch_view_copies: vec![stored.clone()],
            branch_write_intents: vec![intent.clone()],
        };
        assert!(provenance.validate().is_ok());

        // Same coordinate, different source: accepting the intent but storing
        // this side-list entry would alter authority-admitted meaning on reopen.
        let mut different_source = stored.clone();
        different_source.source_version = tx(99);
        provenance.branch_write_intents[0].operation =
            BranchWriteOperation::ViewUpdateCopy(different_source);
        assert!(provenance.validate().is_err());

        let mut different_base = stored.clone();
        different_base.base = BranchViewCopyBase::Snapshot {
            branch: BranchKey::default(),
            snapshot: SnapshotRef {
                owner: NodeUuid::from_bytes([8; 16]),
                global_base: GlobalTime(0),
                local_base: TxTime::from(8),
                dots: Vec::new(),
            },
        };
        provenance.branch_write_intents[0].operation =
            BranchWriteOperation::ViewUpdateCopy(different_base);
        assert!(provenance.validate().is_err());

        let mut different_version = stored.clone();
        different_version.version = 2;
        provenance.branch_write_intents[0].operation =
            BranchWriteOperation::ViewUpdateCopy(different_version);
        assert!(provenance.validate().is_err());

        provenance.branch_write_intents[0] = intent;
        provenance.branch_view_copies.push(stored);
        assert!(provenance.validate().is_err(), "orphan copy must reject");
    }

    #[test]
    fn contribution_dot_closure_prevents_a_b_c_a_echo() {
        let root = ContributionDot {
            tx_id: tx(1),
            coordinate: coordinate("a"),
        };
        let b_tx = tx(2);
        let c_tx = tx(3);
        let mut index = ContributionSubstitutionIndex::default();
        index.observe(b_tx, &provenance("b", root.clone())).unwrap();
        index
            .observe(
                c_tx,
                &provenance(
                    "c",
                    ContributionDot {
                        tx_id: b_tx,
                        coordinate: coordinate("b"),
                    },
                ),
            )
            .unwrap();

        let novel = index
            .novel(
                [ContributionDot {
                    tx_id: c_tx,
                    coordinate: coordinate("c"),
                }],
                [root],
            )
            .unwrap();
        assert!(novel.is_empty());
    }

    #[test]
    fn contribution_dot_closure_rejects_cycles() {
        let first = ContributionDot {
            tx_id: tx(1),
            coordinate: coordinate("a"),
        };
        let second = ContributionDot {
            tx_id: tx(2),
            coordinate: coordinate("b"),
        };
        let mut index = ContributionSubstitutionIndex::default();
        index
            .observe(tx(1), &provenance("a", second.clone()))
            .unwrap();
        index
            .observe(tx(2), &provenance("b", first.clone()))
            .unwrap();
        assert_eq!(
            index.expand([first]),
            Err("contribution substitution cycle")
        );
    }
}
