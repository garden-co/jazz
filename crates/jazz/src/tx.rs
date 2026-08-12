//! Transaction, fate, durability, snapshot, and read-set vocabulary shared by
//! the facade, node, and protocol layers. This module owns the semantic data
//! structures for mergeable/exclusive transactions and authority outcomes; the
//! code that validates, stores, and syncs them lives in [`crate::node::ingest`],
//! [`crate::node::open_tx`], and [`crate::protocol`]. Merge and currency rules
//! are grounded in `jazz/README.md`.

use crate::ids::{AuthorId, BranchId, NodeUuid, RowUuid};
use crate::merge_strategy::ColumnSpecHash;
use crate::query::{BindingId, Query, ShapeId};
use crate::schema::TableSchema;
use crate::time::{GlobalSeq, TxTime};
use groove::records::{OwnedRecord, Value};
use std::collections::BTreeMap;

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
    pub made_by: AuthorId,
    /// Optional identity used for write-policy evaluation.
    ///
    /// When absent, policy is evaluated as `made_by`. Trusted serving-node
    /// flows use this to preserve user provenance while validating writes
    /// under a terminated request/session identity.
    pub permission_subject: Option<AuthorId>,
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
    /// Operational lineage where this transaction's row versions are stored.
    pub target_lineage: BranchLineage,
    /// Non-causal provenance for a locally calculated branch merge.
    ///
    /// Receivers persist and forward this metadata but do not use it for
    /// transaction admission, parent prerequisites, fate, or row merging.
    pub branch_merge: Option<BranchMergeProvenance>,
    /// Strategy runtime that produced this transaction when it is a recorded merge.
    pub merge_strategy: Option<RecordedMergeStrategy>,
}

/// Canonical operational history lineage for transaction routing and local
/// branch-merge contribution calculation.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BranchLineage {
    /// The ordinary non-branch database history.
    Root,
    /// A snapshot-base branch overlay.
    Branch(BranchId),
}

impl From<BranchId> for BranchLineage {
    fn from(branch_id: BranchId) -> Self {
        Self::Branch(branch_id)
    }
}

/// Non-causal source evidence attached to an ordinary calculated merge write.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BranchMergeProvenance {
    /// Source lineage read by the local calculator.
    pub source_lineage: BranchLineage,
    /// Maximal source cut already represented by the calculator's target view.
    pub from_frontier: Vec<TxId>,
    /// Maximal source cut incorporated by this calculated transaction.
    pub through_frontier: Vec<TxId>,
    /// Field-grained derived-output substitutions validated by future local
    /// calculators before they use this metadata for contribution subtraction.
    pub substitutions: Vec<ContributionSubstitution>,
}

impl BranchMergeProvenance {
    /// Construct canonical locally minted provenance. Source-frontier
    /// maximality is established by the history calculator before this
    /// structural canonicalization step.
    pub fn canonical(
        source_lineage: BranchLineage,
        mut from_frontier: Vec<TxId>,
        mut through_frontier: Vec<TxId>,
        mut substitutions: Vec<ContributionSubstitution>,
    ) -> Result<Self, &'static str> {
        from_frontier.sort();
        from_frontier.dedup();
        through_frontier.sort();
        through_frontier.dedup();
        for substitution in &mut substitutions {
            substitution.sources.sort();
            substitution.sources.dedup();
            if substitution.sources.is_empty() {
                return Err("branch merge substitution requires a source dot");
            }
        }
        substitutions.sort_by(|left, right| left.target.cmp(&right.target));
        if substitutions
            .windows(2)
            .any(|pair| pair[0].target == pair[1].target)
        {
            return Err("branch merge substitution targets must be unique");
        }
        Ok(Self {
            source_lineage,
            from_frontier,
            through_frontier,
            substitutions,
        })
    }
}

/// One derived target field and the exact source contribution dots it encodes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct ContributionSubstitution {
    /// Field or operation emitted by the ordinary target transaction.
    pub target: ContributionCoordinate,
    /// Canonically sorted and de-duplicated source dots represented by `target`.
    pub sources: Vec<ContributionDot>,
}

/// Stable field-grained coordinate in a transaction lineage.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContributionCoordinate {
    /// Logical table in the calculator's exact schema view.
    pub table: String,
    /// Logical row identity.
    pub row_uuid: RowUuid,
    /// Independent content or deletion history layer.
    pub layer: MergeAspect,
    /// Column, register, or strategy-operation identity within that layer.
    pub component: ContributionComponent,
}

/// Field or operation identity within one row history layer.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum ContributionComponent {
    /// An ordinary named content column.
    Column(String),
    /// A strategy-defined stable operation identifier.
    Operation(Vec<u8>),
    /// The deletion/restore register.
    Register,
}

/// Stable native contribution identity used by the local merge calculator.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct ContributionDot {
    /// Lineage in which the native contribution originated.
    pub lineage: BranchLineage,
    /// Transaction that introduced the native contribution.
    pub tx_id: TxId,
    /// Exact field or operation introduced by that transaction.
    pub coordinate: ContributionCoordinate,
}

/// Runtime strategy tag recorded on system-created merge transactions.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RecordedMergeStrategy {
    /// Stable strategy id.
    pub id: String,
    /// Strategy implementation version.
    pub version: u32,
    /// Hash of the declared column merge spec in force.
    pub column_spec_hash: ColumnSpecHash,
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
    pub made_by: AuthorId,
    /// Transaction kind.
    pub kind: TxKind,
    /// Number of row-version records in the original commit unit.
    pub n_total_writes: u32,
    /// Latest known fate.
    pub fate: Fate,
    /// Assigned global sequence, when accepted globally.
    pub global_seq: Option<GlobalSeq>,
    /// Highest observed durability tier.
    pub durability: DurabilityTier,
    /// Optional application metadata attached at commit time.
    pub user_metadata_json: Option<String>,
    /// Operational lineage where this transaction's row versions are stored.
    pub target_lineage: BranchLineage,
    /// Non-causal provenance for a locally calculated branch merge.
    pub branch_merge: Option<BranchMergeProvenance>,
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
    pub fn made_by(&self) -> AuthorId {
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

    /// Assigned global sequence, when accepted globally.
    pub fn global_seq(&self) -> Option<GlobalSeq> {
        self.transaction.global_seq
    }

    /// Highest observed durability tier.
    pub fn durability(&self) -> DurabilityTier {
        self.transaction.durability
    }

    /// Operational lineage containing this transaction's row versions.
    pub fn target_lineage(&self) -> BranchLineage {
        self.transaction.target_lineage
    }

    /// Direct parent transaction ids for this version.
    pub fn parents(&self) -> Vec<TxId> {
        let field = if self.is_register_record() {
            RegisterRowRecord::FIELD_PARENTS_IDX
        } else {
            HistoryRowRecord::FIELD_PARENTS_IDX
        };
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
        self.version
            .borrowed()
            .get_idx(HistoryRowRecord::USER_CELLS + column_position)
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
        deletion_from_value(
            self.version
                .borrowed()
                .get_idx(RegisterRowRecord::FIELD__DELETION_IDX)
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
    pub fn made_by(&self) -> AuthorId {
        AuthorId(
            self.record
                .borrowed()
                .get_uuid(RejectedTransactionRowRecord::FIELD_MADE_BY_IDX)
                .expect("valid rejected author"),
        )
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
    pub global_base: GlobalSeq,
    /// Local base visible at open time.
    pub local_base: TxTime,
    /// Additional visible transaction dots.
    pub dots: Vec<TxId>,
}

impl Snapshot {
    /// Create an exclusive base snapshot.
    pub fn exclusive_base(
        owner: NodeUuid,
        global_base: GlobalSeq,
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
        5 => created_by: AuthorId,
        6 => created_at: u64,
        7 => updated_by: AuthorId,
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
        5 => created_by: AuthorId,
        6 => created_at: u64,
        7 => updated_by: AuthorId,
        8 => updated_at: u64,
        9 => _deletion: Value,
    }
}

groove::define_record! {
    struct RejectedTransactionRowRecord {
        0 => time: u64,
        1 => node_id: u64,
        2 => kind: TxKind,
        3 => made_by: AuthorId,
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
mod branch_merge_provenance_tests {
    use super::*;

    fn tx(byte: u8) -> TxId {
        TxId::new(TxTime(byte.into()), NodeUuid::from_bytes([byte; 16]))
    }

    fn coordinate(column: &str) -> ContributionCoordinate {
        ContributionCoordinate {
            table: "todos".to_owned(),
            row_uuid: RowUuid::from_bytes([1; 16]),
            layer: MergeAspect::Content,
            component: ContributionComponent::Column(column.to_owned()),
        }
    }

    #[test]
    fn canonical_branch_merge_provenance_sorts_and_deduplicates() {
        let target = coordinate("title");
        let dot = ContributionDot {
            lineage: BranchLineage::Root,
            tx_id: tx(1),
            coordinate: target.clone(),
        };
        let provenance = BranchMergeProvenance::canonical(
            BranchLineage::Root,
            vec![tx(2), tx(1), tx(2)],
            vec![tx(2), tx(1)],
            vec![ContributionSubstitution {
                target,
                sources: vec![dot.clone(), dot],
            }],
        )
        .unwrap();
        assert_eq!(provenance.from_frontier, vec![tx(1), tx(2)]);
        assert_eq!(provenance.substitutions[0].sources.len(), 1);
    }

    #[test]
    fn canonical_branch_merge_provenance_rejects_duplicate_targets() {
        let target = coordinate("title");
        let substitution = ContributionSubstitution {
            target: target.clone(),
            sources: vec![ContributionDot {
                lineage: BranchLineage::Root,
                tx_id: tx(1),
                coordinate: target,
            }],
        };
        assert!(
            BranchMergeProvenance::canonical(
                BranchLineage::Root,
                Vec::new(),
                vec![tx(1)],
                vec![substitution.clone(), substitution],
            )
            .is_err()
        );
    }
}
