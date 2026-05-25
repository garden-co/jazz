use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TxId(String);

impl TxId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for TxId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TxId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TxStatus {
    LocalPending,
    EdgeDurable,
    GlobalDurableAccepted,
    Rejected,
}

impl TxStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }

        matches!(
            (self, next),
            (Self::LocalPending, Self::EdgeDurable)
                | (Self::LocalPending, Self::GlobalDurableAccepted)
                | (Self::LocalPending, Self::Rejected)
                | (Self::EdgeDurable, Self::GlobalDurableAccepted)
                | (Self::EdgeDurable, Self::Rejected)
        )
    }

    pub fn transition_to(self, next: Self) -> Result<Self, TxStatusTransitionError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(TxStatusTransitionError {
                from: self,
                to: next,
            })
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::GlobalDurableAccepted | Self::Rejected)
    }

    pub fn is_rejected(self) -> bool {
        self == Self::Rejected
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TxStatusTransitionError {
    pub from: TxStatus,
    pub to: TxStatus,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TxKind {
    Data,
    BranchMetadata,
    SchemaMetadata,
    PermissionMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxCoordinate {
    pub tx_id: TxId,
    pub node_id: NodeId,
    pub local_epoch: u64,
    pub global_epoch: Option<u64>,
    pub status: TxStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxAccepted {
    pub tx_id: TxId,
    pub node_id: NodeId,
    pub local_epoch: u64,
    pub global_epoch: u64,
}

impl TxAccepted {
    pub fn new(
        tx_id: impl Into<TxId>,
        node_id: impl Into<NodeId>,
        local_epoch: u64,
        global_epoch: u64,
    ) -> Self {
        Self {
            tx_id: tx_id.into(),
            node_id: node_id.into(),
            local_epoch,
            global_epoch,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxAcceptanceError {
    CoordinateMismatch { expected: TxRef, actual: TxRef },
    GlobalEpochMismatch { existing: u64, accepted: u64 },
    InvalidStatusTransition(TxStatusTransitionError),
}

impl TxCoordinate {
    pub fn local(
        tx_id: impl Into<TxId>,
        node_id: impl Into<NodeId>,
        local_epoch: u64,
        status: TxStatus,
    ) -> Self {
        Self {
            tx_id: tx_id.into(),
            node_id: node_id.into(),
            local_epoch,
            global_epoch: None,
            status,
        }
    }

    pub fn global(
        tx_id: impl Into<TxId>,
        node_id: impl Into<NodeId>,
        local_epoch: u64,
        global_epoch: u64,
    ) -> Self {
        Self {
            tx_id: tx_id.into(),
            node_id: node_id.into(),
            local_epoch,
            global_epoch: Some(global_epoch),
            status: TxStatus::GlobalDurableAccepted,
        }
    }

    pub fn accept(&mut self, accepted: &TxAccepted) -> Result<(), TxAcceptanceError> {
        if self.tx_id != accepted.tx_id {
            return Err(TxAcceptanceError::CoordinateMismatch {
                expected: TxRef::tx_id(self.tx_id.clone()),
                actual: TxRef::tx_id(accepted.tx_id.clone()),
            });
        }

        let expected_local = TxRef::node_local(self.node_id.clone(), self.local_epoch);
        let actual_local = TxRef::node_local(accepted.node_id.clone(), accepted.local_epoch);
        if expected_local != actual_local {
            return Err(TxAcceptanceError::CoordinateMismatch {
                expected: expected_local,
                actual: actual_local,
            });
        }

        if let Some(existing) = self.global_epoch
            && existing != accepted.global_epoch
        {
            return Err(TxAcceptanceError::GlobalEpochMismatch {
                existing,
                accepted: accepted.global_epoch,
            });
        }

        self.status = self
            .status
            .transition_to(TxStatus::GlobalDurableAccepted)
            .map_err(TxAcceptanceError::InvalidStatusTransition)?;
        self.global_epoch = Some(accepted.global_epoch);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TxRef {
    TxId(TxId),
    Global(u64),
    NodeLocal { node_id: NodeId, local_epoch: u64 },
}

impl TxRef {
    pub fn tx_id(tx_id: impl Into<TxId>) -> Self {
        Self::TxId(tx_id.into())
    }

    pub fn node_local(node_id: impl Into<NodeId>, local_epoch: u64) -> Self {
        Self::NodeLocal {
            node_id: node_id.into(),
            local_epoch,
        }
    }

    pub fn matches_coordinate(&self, tx: &TxCoordinate) -> bool {
        match self {
            Self::TxId(tx_id) => tx_id == &tx.tx_id,
            Self::Global(global_epoch) => tx.global_epoch == Some(*global_epoch),
            Self::NodeLocal {
                node_id,
                local_epoch,
            } => node_id == &tx.node_id && *local_epoch == tx.local_epoch,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalBase {
    pub node_id: NodeId,
    pub local_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct VersionVector {
    pub global_base: u64,
    pub local_bases: Vec<LocalBase>,
    pub include: Vec<TxRef>,
}

impl VersionVector {
    pub fn new(global_base: u64) -> Self {
        Self {
            global_base,
            local_bases: Vec::new(),
            include: Vec::new(),
        }
    }

    pub fn local_base(&self, node_id: &NodeId) -> Option<u64> {
        self.local_bases
            .iter()
            .find(|base| &base.node_id == node_id)
            .map(|base| base.local_epoch)
    }

    pub fn set_local_base(&mut self, node_id: impl Into<NodeId>, local_epoch: u64) {
        let node_id = node_id.into();
        match self
            .local_bases
            .binary_search_by(|base| base.node_id.cmp(&node_id))
        {
            Ok(index) => self.local_bases[index].local_epoch = local_epoch,
            Err(index) => self.local_bases.insert(
                index,
                LocalBase {
                    node_id,
                    local_epoch,
                },
            ),
        }
    }

    pub fn with_local_base(mut self, node_id: impl Into<NodeId>, local_epoch: u64) -> Self {
        self.set_local_base(node_id, local_epoch);
        self
    }

    pub fn include(&mut self, tx_ref: TxRef) {
        if !self.include.contains(&tx_ref) {
            self.include.push(tx_ref);
        }
    }

    pub fn with_include(mut self, tx_ref: TxRef) -> Self {
        self.include(tx_ref);
        self
    }

    pub fn includes_ref(&self, tx_ref: &TxRef) -> bool {
        self.include.contains(tx_ref)
    }

    pub fn explicitly_includes(&self, tx: &TxCoordinate) -> bool {
        self.include
            .iter()
            .any(|tx_ref| tx_ref.matches_coordinate(tx))
    }

    pub fn is_visible(&self, tx: &TxCoordinate) -> bool {
        if tx.status.is_rejected() {
            return false;
        }

        if tx.status == TxStatus::GlobalDurableAccepted
            && tx
                .global_epoch
                .is_some_and(|global_epoch| global_epoch <= self.global_base)
        {
            return true;
        }

        if self
            .local_base(&tx.node_id)
            .is_some_and(|local_base| tx.local_epoch <= local_base)
        {
            return true;
        }

        self.explicitly_includes(tx)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReadSet {
    pub entries: Vec<ReadSetEntry>,
}

impl ReadSet {
    pub fn new(entries: Vec<ReadSetEntry>) -> Self {
        Self { entries }
    }

    pub fn push(&mut self, entry: ReadSetEntry) {
        self.entries.push(entry);
    }

    pub fn validate_against(&self, state: &AuthorityReadState) -> Result<(), ReadValidationError> {
        for entry in &self.entries {
            match entry {
                ReadSetEntry::Row(read) => {
                    let current = state.visible_tx(&read.table, &read.row_id);
                    if current != read.visible_tx_id.as_ref() {
                        return Err(ReadValidationError::StaleRow {
                            table: read.table.clone(),
                            row_id: read.row_id.clone(),
                            expected: read.visible_tx_id.clone(),
                            actual: current.cloned(),
                        });
                    }
                }
                ReadSetEntry::Range(read) => {
                    let matching = state.matching_visible_rows(read);
                    if !matching.is_empty() {
                        return Err(ReadValidationError::StaleRange {
                            table: read.table.clone(),
                            index: read.index.clone(),
                            predicate: read.predicate.clone(),
                            matching_row_ids: matching,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct AuthorityReadState {
    rows: Vec<AuthorityRowState>,
}

impl AuthorityReadState {
    pub fn new(rows: Vec<AuthorityRowState>) -> Self {
        Self { rows }
    }

    pub fn visible_tx(&self, table: &str, row_id: &str) -> Option<&TxId> {
        self.rows
            .iter()
            .find(|row| row.table == table && row.row_id == row_id)
            .and_then(|row| row.visible_tx_id.as_ref())
    }

    pub fn matching_visible_rows(&self, read: &RangeRead) -> Vec<String> {
        self.rows
            .iter()
            .filter(|row| {
                row.table == read.table
                    && row.visible_tx_id.is_some()
                    && predicate_matches_row(&read.predicate, row)
            })
            .map(|row| row.row_id.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityRowState {
    pub table: String,
    pub row_id: String,
    pub visible_tx_id: Option<TxId>,
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadValidationError {
    StaleRow {
        table: String,
        row_id: String,
        expected: Option<TxId>,
        actual: Option<TxId>,
    },
    StaleRange {
        table: String,
        index: String,
        predicate: JsonValue,
        matching_row_ids: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadSetEntry {
    Row(RowRead),
    Range(RangeRead),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowRead {
    pub table: String,
    pub row_id: String,
    pub visible_tx_id: Option<TxId>,
    pub reason: ReadReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadReason {
    Direct,
    PreviousVersionForWrite,
    PolicyDependency,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeRead {
    pub table: String,
    pub index: String,
    pub predicate: JsonValue,
    pub snapshot: VersionVector,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct WriteSet {
    pub entries: Vec<WriteSetEntry>,
}

impl WriteSet {
    pub fn new(entries: Vec<WriteSetEntry>) -> Self {
        Self { entries }
    }

    pub fn push(&mut self, entry: WriteSetEntry) {
        self.entries.push(entry);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteSetEntry {
    pub table: String,
    pub row_id: String,
    pub op: WriteOp,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<JsonField>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonField {
    pub key: String,
    pub value: JsonValue,
}

impl JsonField {
    pub fn new(key: impl Into<String>, value: JsonValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

fn predicate_matches_row(predicate: &JsonValue, row: &AuthorityRowState) -> bool {
    let JsonValue::Object(fields) = predicate else {
        return false;
    };

    fields.iter().all(|field| match field.key.as_str() {
        "rowId" => field.value == JsonValue::String(row.row_id.clone()),
        "isDeleted" => field.value == JsonValue::Bool(row.is_deleted),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_transitions_follow_the_small_state_machine() {
        assert_eq!(
            TxStatus::LocalPending.transition_to(TxStatus::EdgeDurable),
            Ok(TxStatus::EdgeDurable)
        );
        assert_eq!(
            TxStatus::LocalPending.transition_to(TxStatus::GlobalDurableAccepted),
            Ok(TxStatus::GlobalDurableAccepted)
        );
        assert_eq!(
            TxStatus::EdgeDurable.transition_to(TxStatus::Rejected),
            Ok(TxStatus::Rejected)
        );

        assert_eq!(
            TxStatus::Rejected.transition_to(TxStatus::GlobalDurableAccepted),
            Err(TxStatusTransitionError {
                from: TxStatus::Rejected,
                to: TxStatus::GlobalDurableAccepted,
            })
        );
        assert_eq!(
            TxStatus::GlobalDurableAccepted.transition_to(TxStatus::Rejected),
            Err(TxStatusTransitionError {
                from: TxStatus::GlobalDurableAccepted,
                to: TxStatus::Rejected,
            })
        );
    }

    #[test]
    fn acceptance_maps_an_existing_local_transaction_to_a_global_epoch() {
        let mut alice_tx = TxCoordinate::local(
            "tx_alice_create_todo",
            "alice_laptop",
            21,
            TxStatus::EdgeDurable,
        );

        alice_tx
            .accept(&TxAccepted::new(
                "tx_alice_create_todo",
                "alice_laptop",
                21,
                1057,
            ))
            .unwrap();

        assert_eq!(alice_tx.global_epoch, Some(1057));
        assert_eq!(alice_tx.status, TxStatus::GlobalDurableAccepted);
        assert!(TxRef::Global(1057).matches_coordinate(&alice_tx));
        assert!(TxRef::node_local("alice_laptop", 21).matches_coordinate(&alice_tx));
    }

    #[test]
    fn acceptance_rejects_mismatched_or_already_rejected_coordinates() {
        let mut alice_tx = TxCoordinate::local(
            "tx_alice_create_todo",
            "alice_laptop",
            21,
            TxStatus::EdgeDurable,
        );

        assert_eq!(
            alice_tx.accept(&TxAccepted::new(
                "tx_alice_create_todo",
                "alice_laptop",
                22,
                1057,
            )),
            Err(TxAcceptanceError::CoordinateMismatch {
                expected: TxRef::node_local("alice_laptop", 21),
                actual: TxRef::node_local("alice_laptop", 22),
            })
        );

        let mut rejected =
            TxCoordinate::local("tx_bob_update_todo", "bob_phone", 4, TxStatus::Rejected);

        assert_eq!(
            rejected.accept(&TxAccepted::new("tx_bob_update_todo", "bob_phone", 4, 1058)),
            Err(TxAcceptanceError::InvalidStatusTransition(
                TxStatusTransitionError {
                    from: TxStatus::Rejected,
                    to: TxStatus::GlobalDurableAccepted,
                }
            ))
        );
    }

    #[test]
    fn global_base_makes_accepted_global_transactions_visible() {
        let alice_tx = TxCoordinate::global("tx_alice_create_todo", "alice_laptop", 7, 42);
        let bob_tx = TxCoordinate::global("tx_bob_rename_todo", "bob_phone", 3, 43);

        let snapshot = VersionVector::new(42);

        assert!(snapshot.is_visible(&alice_tx));
        assert!(!snapshot.is_visible(&bob_tx));
    }

    #[test]
    fn local_vector_references_still_see_accepted_transactions_after_global_mapping() {
        let mut alice_tx = TxCoordinate::local(
            "tx_alice_offline_create_todo",
            "alice_laptop",
            21,
            TxStatus::EdgeDurable,
        );
        alice_tx
            .accept(&TxAccepted::new(
                "tx_alice_offline_create_todo",
                "alice_laptop",
                21,
                1057,
            ))
            .unwrap();

        let by_local_base = VersionVector::new(0).with_local_base("alice_laptop", 21);
        let by_local_dot =
            VersionVector::new(0).with_include(TxRef::node_local("alice_laptop", 21));
        let by_global_base = VersionVector::new(1057);

        assert!(by_local_base.is_visible(&alice_tx));
        assert!(by_local_dot.is_visible(&alice_tx));
        assert!(by_global_base.is_visible(&alice_tx));
    }

    #[test]
    fn local_base_makes_node_local_transactions_visible() {
        let alice_tx = TxCoordinate::local(
            "tx_alice_offline_edit",
            "alice_laptop",
            8,
            TxStatus::EdgeDurable,
        );
        let bob_tx =
            TxCoordinate::local("tx_bob_offline_edit", "bob_phone", 8, TxStatus::EdgeDurable);

        let snapshot = VersionVector::new(0).with_local_base("alice_laptop", 8);

        assert!(snapshot.is_visible(&alice_tx));
        assert!(!snapshot.is_visible(&bob_tx));
    }

    #[test]
    fn include_dots_can_name_transactions_by_any_coordinate() {
        let alice_tx = TxCoordinate::local(
            "tx_alice_sparse_edit",
            "alice_laptop",
            12,
            TxStatus::LocalPending,
        );
        let bob_tx = TxCoordinate::global("tx_bob_sparse_edit", "bob_phone", 4, 99);

        let snapshot = VersionVector::new(0)
            .with_include(TxRef::tx_id("tx_alice_sparse_edit"))
            .with_include(TxRef::Global(99))
            .with_include(TxRef::node_local("carol_tablet", 2));

        assert!(snapshot.is_visible(&alice_tx));
        assert!(snapshot.is_visible(&bob_tx));
        assert!(snapshot.includes_ref(&TxRef::node_local("carol_tablet", 2)));
    }

    #[test]
    fn rejected_transactions_are_never_visible() {
        let alice_rejected = TxCoordinate {
            tx_id: TxId::from("tx_alice_rejected_edit"),
            node_id: NodeId::from("alice_laptop"),
            local_epoch: 5,
            global_epoch: Some(3),
            status: TxStatus::Rejected,
        };

        let snapshot = VersionVector::new(10)
            .with_local_base("alice_laptop", 10)
            .with_include(TxRef::tx_id("tx_alice_rejected_edit"));

        assert!(!snapshot.is_visible(&alice_rejected));
    }

    #[test]
    fn local_bases_are_stored_canonically_by_node() {
        let mut snapshot = VersionVector::new(12);
        snapshot.set_local_base("carol_tablet", 3);
        snapshot.set_local_base("alice_laptop", 9);
        snapshot.set_local_base("bob_phone", 1);
        snapshot.set_local_base("alice_laptop", 10);

        let nodes: Vec<&str> = snapshot
            .local_bases
            .iter()
            .map(|base| base.node_id.as_str())
            .collect();

        assert_eq!(nodes, vec!["alice_laptop", "bob_phone", "carol_tablet"]);
        assert_eq!(snapshot.local_base(&NodeId::from("alice_laptop")), Some(10));
    }

    #[test]
    fn read_and_write_sets_hold_jsonish_usage_shapes() {
        let snapshot = VersionVector::new(1057);
        let read_set = ReadSet::new(vec![
            ReadSetEntry::Row(RowRead {
                table: "todos".to_owned(),
                row_id: "todo_blue".to_owned(),
                visible_tx_id: Some(TxId::from("tx_alice_create_todo")),
                reason: ReadReason::Direct,
            }),
            ReadSetEntry::Range(RangeRead {
                table: "todos".to_owned(),
                index: "todos_done_created_at".to_owned(),
                predicate: JsonValue::Object(vec![
                    JsonField::new("done", JsonValue::Bool(false)),
                    JsonField::new(
                        "$createdAt",
                        JsonValue::Object(vec![JsonField::new("gt", JsonValue::Number(123))]),
                    ),
                ]),
                snapshot,
            }),
        ]);
        let write_set = WriteSet::new(vec![WriteSetEntry {
            table: "todos".to_owned(),
            row_id: "todo_blue".to_owned(),
            op: WriteOp::Update,
            columns: vec!["title".to_owned(), "$updatedAt".to_owned()],
        }]);

        assert_eq!(read_set.entries.len(), 2);
        assert_eq!(write_set.entries[0].columns, vec!["title", "$updatedAt"]);
    }

    #[test]
    fn row_read_sets_validate_against_authority_current_state() {
        let read_set = ReadSet::new(vec![ReadSetEntry::Row(RowRead {
            table: "todos".to_owned(),
            row_id: "todo_launch_checklist".to_owned(),
            visible_tx_id: Some(TxId::from("tx_base")),
            reason: ReadReason::PreviousVersionForWrite,
        })]);
        let state = AuthorityReadState::new(vec![AuthorityRowState {
            table: "todos".to_owned(),
            row_id: "todo_launch_checklist".to_owned(),
            visible_tx_id: Some(TxId::from("tx_base")),
            is_deleted: false,
        }]);

        assert_eq!(read_set.validate_against(&state), Ok(()));
    }

    #[test]
    fn stale_row_read_sets_are_rejectable_without_parent_pointers() {
        let read_set = ReadSet::new(vec![ReadSetEntry::Row(RowRead {
            table: "todos".to_owned(),
            row_id: "todo_launch_checklist".to_owned(),
            visible_tx_id: Some(TxId::from("tx_base")),
            reason: ReadReason::PreviousVersionForWrite,
        })]);
        let state = AuthorityReadState::new(vec![AuthorityRowState {
            table: "todos".to_owned(),
            row_id: "todo_launch_checklist".to_owned(),
            visible_tx_id: Some(TxId::from("tx_newer")),
            is_deleted: false,
        }]);

        assert_eq!(
            read_set.validate_against(&state),
            Err(ReadValidationError::StaleRow {
                table: "todos".to_owned(),
                row_id: "todo_launch_checklist".to_owned(),
                expected: Some(TxId::from("tx_base")),
                actual: Some(TxId::from("tx_newer")),
            })
        );
    }

    #[test]
    fn absence_range_reads_validate_until_a_matching_row_appears() {
        let absent_project_read = ReadSet::new(vec![ReadSetEntry::Range(RangeRead {
            table: "projects".to_owned(),
            index: "projects_by_row_id_deleted".to_owned(),
            predicate: JsonValue::Object(vec![
                JsonField::new("rowId", JsonValue::String("project_missing".to_owned())),
                JsonField::new("isDeleted", JsonValue::Bool(false)),
            ]),
            snapshot: VersionVector::new(42),
        })]);

        assert_eq!(
            absent_project_read.validate_against(&AuthorityReadState::new(vec![])),
            Ok(())
        );

        let state_with_concurrent_insert = AuthorityReadState::new(vec![AuthorityRowState {
            table: "projects".to_owned(),
            row_id: "project_missing".to_owned(),
            visible_tx_id: Some(TxId::from("tx_project_insert")),
            is_deleted: false,
        }]);

        assert_eq!(
            absent_project_read.validate_against(&state_with_concurrent_insert),
            Err(ReadValidationError::StaleRange {
                table: "projects".to_owned(),
                index: "projects_by_row_id_deleted".to_owned(),
                predicate: JsonValue::Object(vec![
                    JsonField::new("rowId", JsonValue::String("project_missing".to_owned())),
                    JsonField::new("isDeleted", JsonValue::Bool(false)),
                ]),
                matching_row_ids: vec!["project_missing".to_owned()],
            })
        );
    }
}
