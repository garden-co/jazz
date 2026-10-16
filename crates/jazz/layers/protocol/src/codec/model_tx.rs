//! Sync-wire codecs for the transaction vocabulary (`jazz_model::tx`) and the
//! branch coordinates (`jazz_model::model::branch`) it carries.

use jazz_model::model::branch::{
    BranchColumnValue, BranchKey, BranchSelector, BranchViewBase, SnapshotRef,
};
use jazz_model::tx::{
    AbsentRead, AggregateReadResult, BranchViewCopyBase, BranchViewCopyEvidence, BranchWriteIntent,
    BranchWriteOperation, ContributionComponent, ContributionCoordinate, ContributionDot,
    ContributionMergeProvenance, ContributionSubstitution, DurabilityTier, Fate, MergeAspect,
    PredicateRead, RejectionReason, RowRead, Snapshot, Transaction, TxId, TxKind,
};

use super::{Bytes, CodecError, Wire, WireCodec};

crate::wire_struct! { for TxId { time, node } }

crate::wire_enum! { for TxKind { 0 => Mergeable, 1 => Exclusive } }

// Tags are declaration order: Global is 2 since alpha.60.
crate::wire_enum! { for DurabilityTier { 0 => None, 1 => Local, 2 => Global } }

crate::wire_enum! { for MergeAspect { 0 => Content, 1 => Deletion } }

crate::wire_enum! {
    for Fate { 0 => Pending, 1 => Accepted, 2 => Rejected(reason) }
}

crate::wire_enum! {
    for RejectionReason {
        0 => ClientClockTooFarAhead,
        1 => AuthorizationDenied,
        2 => ExclusiveConflict,
        3 => CausalityViolation,
        4 => Cascade { root },
        5 => MalformedCommit(message),
    }
}

crate::wire_struct! {
    for Transaction {
        tx_id,
        kind,
        n_total_writes,
        made_by,
        permission_subject,
        base_snapshot,
        row_read_set,
        absent_read_set,
        predicate_read_set,
        user_metadata_json,
        contribution_merge,
    }
}

crate::wire_struct! {
    for ContributionMergeProvenance {
        source,
        target,
        substitutions,
        branch_view_copies,
        branch_write_intents,
    }
}

crate::wire_struct! {
    for BranchWriteIntent {
        version,
        physical_table_id,
        authored_schema,
        row_uuid,
        head,
        operation,
    }
}

crate::wire_enum! {
    for BranchWriteOperation {
        0 => ExactHeadInsert,
        1 => ExactHeadUpdate,
        2 => ViewUpdateCopy(evidence),
    }
}

crate::wire_struct! {
    for BranchViewCopyEvidence { version, head, base, table, row_uuid, source_version }
}

crate::wire_enum! {
    for BranchViewCopyBase { 0 => Current(branch), 1 => Snapshot { branch, snapshot } }
}

crate::wire_struct! { for ContributionSubstitution { target, sources } }

crate::wire_struct! {
    for ContributionCoordinate { branch_key, table, row_uuid, layer, component }
}

crate::wire_enum! {
    for ContributionComponent {
        0 => Column(column),
        // A plain `Vec<u8>`: postcard spelled it as a length and the bytes.
        1 => Operation { column, identity: Bytes },
        2 => Register,
    }
}

crate::wire_struct! { for ContributionDot { tx_id, coordinate } }

crate::wire_struct! { for Snapshot { owner, global_base, local_base, dots } }

crate::wire_struct! { for RowRead { table, row_uuid, version } }

crate::wire_struct! { for AbsentRead { table, row_uuid } }

crate::wire_struct! {
    for PredicateRead { table, shape_id, shape, binding_id, binding_values, aggregate_result }
}

/// Newtype over a byte vector (`serde_bytes`): a length and the bytes.
macro_rules! byte_newtype {
    ($($ty:ident),+) => {$(
        impl WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                Bytes.put(buf, &self.0);
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                Bytes.get(data, offset).map($ty)
            }
        }
    )+};
}

// `BranchColumnValue` has no `serde_bytes`, but postcard writes a `Vec<u8>`
// sequence as the same length and bytes.
byte_newtype!(AggregateReadResult, BranchColumnValue);

crate::wire_struct! { for BranchSelector { values } }

crate::wire_struct! { for BranchKey { values } }

crate::wire_enum! {
    for BranchViewBase { 0 => Current(branch), 1 => Snapshot { branch, snapshot } }
}

crate::wire_struct! { for SnapshotRef { owner, global_base, local_base, dots } }

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use groove::records::Value;
    use jazz_model::query::{BindingId, Query, ShapeId};
    use jazz_types::ids::{AuthorSubject, NodeUuid, PhysicalTableId, RowUuid, SchemaVersionId};
    use jazz_types::time::{GlobalTime, TxTime};

    use super::*;
    use crate::codec::{decode_exact, encode};

    fn same<T: WireCodec + serde::Serialize + PartialEq + Debug>(value: T) {
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert_eq!(encode(&value), bytes, "{value:?}");
        assert_eq!(decode_exact::<T>(&bytes).unwrap(), value);
    }

    fn tx(time: u64) -> TxId {
        TxId::new(TxTime(time), NodeUuid::from_bytes([time as u8; 16]))
    }

    fn key(names: &[&str]) -> BranchKey {
        BranchKey {
            values: names
                .iter()
                .map(|name| {
                    (
                        (*name).to_owned(),
                        BranchColumnValue::from(Value::String(format!("v-{name}"))),
                    )
                })
                .collect(),
        }
    }

    fn snapshot_ref(dots: Vec<TxId>) -> SnapshotRef {
        SnapshotRef {
            owner: NodeUuid::from_bytes([3; 16]),
            global_base: GlobalTime(u64::MAX),
            local_base: TxTime(128),
            dots,
        }
    }

    fn coordinate(component: ContributionComponent) -> ContributionCoordinate {
        ContributionCoordinate {
            branch_key: key(&["branch"]),
            table: "todos".to_owned(),
            row_uuid: RowUuid::from_bytes([7; 16]),
            layer: MergeAspect::Deletion,
            component,
        }
    }

    fn evidence(base: BranchViewCopyBase) -> BranchViewCopyEvidence {
        BranchViewCopyEvidence {
            version: 1,
            head: key(&["a", "b"]),
            base,
            table: "todos".to_owned(),
            row_uuid: RowUuid::from_bytes([5; 16]),
            source_version: tx(9),
        }
    }

    fn intent(operation: BranchWriteOperation) -> BranchWriteIntent {
        BranchWriteIntent {
            version: 255,
            physical_table_id: PhysicalTableId(u64::MAX),
            authored_schema: SchemaVersionId(uuid::Uuid::from_bytes([1; 16])),
            row_uuid: RowUuid::from_bytes([2; 16]),
            head: key(&["branch"]),
            operation,
        }
    }

    fn provenance() -> ContributionMergeProvenance {
        let current = evidence(BranchViewCopyBase::Current(key(&[])));
        ContributionMergeProvenance {
            source: key(&["s"]),
            target: BranchKey::default(),
            substitutions: vec![ContributionSubstitution {
                target: coordinate(ContributionComponent::Register),
                sources: vec![
                    ContributionDot {
                        tx_id: tx(1),
                        coordinate: coordinate(ContributionComponent::Column("title".into())),
                    },
                    ContributionDot {
                        tx_id: tx(200),
                        coordinate: coordinate(ContributionComponent::Operation {
                            column: "tags".into(),
                            identity: vec![0, 1, 255],
                        }),
                    },
                ],
            }],
            branch_view_copies: vec![current.clone()],
            branch_write_intents: vec![
                intent(BranchWriteOperation::ExactHeadInsert),
                intent(BranchWriteOperation::ExactHeadUpdate),
                intent(BranchWriteOperation::ViewUpdateCopy(current)),
                intent(BranchWriteOperation::ViewUpdateCopy(evidence(
                    BranchViewCopyBase::Snapshot {
                        branch: key(&["x"]),
                        snapshot: snapshot_ref(vec![tx(4)]),
                    },
                ))),
            ],
        }
    }

    fn predicate_read(aggregate_result: Option<AggregateReadResult>) -> PredicateRead {
        PredicateRead {
            table: "todos".to_owned(),
            shape_id: ShapeId(uuid::Uuid::from_bytes([4; 16])),
            shape: Query::from("todos"),
            binding_id: BindingId(uuid::Uuid::from_bytes([6; 16])),
            binding_values: BTreeMap::from([
                ("a".to_owned(), Value::U64(300)),
                ("b".to_owned(), Value::String("x".into())),
            ]),
            aggregate_result,
        }
    }

    fn transaction() -> Transaction {
        Transaction {
            tx_id: tx(1),
            kind: TxKind::Mergeable,
            n_total_writes: 0,
            made_by: AuthorSubject::for_test_bytes([0xa1; 16]),
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: None,
        }
    }

    #[test]
    fn transactions_match_postcard() {
        same(transaction());
        same(Transaction {
            tx_id: tx(u64::MAX),
            kind: TxKind::Exclusive,
            n_total_writes: u32::MAX,
            made_by: AuthorSubject::for_test_bytes([0xa1; 16]),
            permission_subject: Some(AuthorSubject::for_test_bytes([0xb2; 16])),
            base_snapshot: Some(Snapshot {
                owner: NodeUuid::from_bytes([8; 16]),
                global_base: GlobalTime(0),
                local_base: TxTime(127),
                dots: vec![tx(2), tx(3)],
            }),
            row_read_set: Some(vec![RowRead {
                table: "todos".to_owned(),
                row_uuid: RowUuid::from_bytes([9; 16]),
                version: tx(5),
            }]),
            absent_read_set: Some(vec![]),
            predicate_read_set: Some(vec![
                predicate_read(None),
                predicate_read(Some(AggregateReadResult(vec![]))),
                predicate_read(Some(AggregateReadResult(vec![7; 200]))),
            ]),
            user_metadata_json: Some("{\"k\":1}".to_owned()),
            contribution_merge: Some(provenance()),
        });
        same(AbsentRead {
            table: String::new(),
            row_uuid: RowUuid::from_bytes([0; 16]),
        });
        same(ContributionMergeProvenance {
            source: BranchKey::default(),
            target: BranchKey::default(),
            substitutions: vec![],
            branch_view_copies: vec![],
            branch_write_intents: vec![],
        });
    }

    #[test]
    fn fates_and_tiers_match_postcard() {
        for tier in [
            DurabilityTier::None,
            DurabilityTier::Local,
            DurabilityTier::Global,
        ] {
            same(tier);
        }
        assert_eq!(encode(&DurabilityTier::Global), [2]);
        for reason in [
            RejectionReason::ClientClockTooFarAhead,
            RejectionReason::AuthorizationDenied,
            RejectionReason::ExclusiveConflict,
            RejectionReason::CausalityViolation,
            RejectionReason::Cascade { root: tx(42) },
            RejectionReason::MalformedCommit("bad parent".into()),
            RejectionReason::MalformedCommit(String::new()),
        ] {
            same(Fate::Rejected(reason));
        }
        same(Fate::Pending);
        same(Fate::Accepted);
        same(MergeAspect::Content);
        assert!(decode_exact::<DurabilityTier>(&[3]).is_err());
        assert!(decode_exact::<Fate>(&[3]).is_err());
    }

    #[test]
    fn branch_coordinates_match_postcard() {
        same(BranchSelector::default());
        same(BranchSelector::new([
            ("branch", Value::Uuid(uuid::Uuid::from_bytes([2; 16]))),
            ("n", Value::I64(-1)),
            ("tag", Value::EnumTag(3)),
        ]));
        same(key(&["a", "b", "c"]));
        same(BranchColumnValue(vec![1, u8::MAX]));
        same(BranchColumnValue(vec![0; 300]));
        same(snapshot_ref(vec![]));
        same(snapshot_ref(vec![tx(1), tx(2)]));
        same(BranchViewBase::current(BranchSelector::new([(
            "branch",
            Value::U8(1),
        )])));
        same(BranchViewBase::snapshot(
            BranchSelector::default(),
            snapshot_ref(vec![tx(7)]),
        ));
        assert!(decode_exact::<BranchViewBase>(&[2]).is_err());
    }
}
