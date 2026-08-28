//! End-to-end behavior guards for the database facade and IVM integration.
//!
//! These tests own broad public-surface coverage: commits, queries,
//! subscriptions, joins, recursion, indices, prepared shapes, and persistence
//! through the [`super::Database`] API. Lower-level record-layout tests live in
//! [`crate::records::tests`]; runtime-specific regression tests live near the
//! runtime module.

use super::*;
use std::sync::mpsc::TryRecvError;
use std::time::Instant;

use crate::ivm::{
    AggregateExpr, AggregateFunction, CollectByField, CollectBySlotBuilder, IvmRuntimeError,
    LiteralValue, PlanExpr, PredicateExpr, ProjectField, StaticScanSpec, TerminalEdit,
    TerminalPathSegment, TopByLimit, TopByOrder,
};
use crate::queries::{
    BinaryOp, ColumnRef, Cte, Expr, JoinConstraint, JoinKind, Query, Select, SelectItem, TableRef,
    UnaryOp, WithQuery,
};
use crate::records::{
    EnumCase, EnumSchema, EnumValue, RecordDescriptor, ScalarEnumSchema, ValueType,
};
use crate::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType,
    PrimaryKey, PrimaryKeyColumn, PrimaryKeyType, TableVariant, TableVariantField,
};
use crate::storage::{
    MemoryStorage, OrderedKvStorage, StorageLayout, TestStorage, TestStorageOperation,
};

use support::*;

mod batches;
mod graphs;
mod indices;
mod persistence;
mod queries;
mod schema;
mod subscriptions;
mod support;

// These are intentionally internal codec assertions: durable key bytes are the
// storage boundary below Groove's public row API, so a public API round-trip
// could not distinguish a coordinated encoder/decoder regression from a stable
// persisted format. The fixtures below keep hard-coded epoch-1 bytes on both
// sides of that boundary.
#[test]
fn epoch_1_primary_and_index_key_fixtures_are_exact_and_fail_closed() {
    use super::encoding::{
        decode_index_key_part, decode_primary_key_part, encode_index_prefix_part,
        encode_primary_key_part,
    };

    let uuid = uuid::Uuid::from_bytes([0x10; 16]);
    let primary_values = [
        Value::U8(0xaa),
        Value::U16(0x1234),
        Value::U32(0x1234_5678),
        Value::U64(0x0102_0304_0506_0708),
        Value::I64(-2),
        Value::I32(-3),
        Value::Bool(true),
        Value::String("a\0b".to_owned()),
        Value::Bytes(vec![0, 0xff]),
        Value::Uuid(uuid),
        Value::Tuple(vec![Value::U16(2), Value::Bool(false)]),
    ];
    let frozen_primary = [
        0x00, 0xaa, // U8
        0x01, 0x12, 0x34, // U16
        0x02, 0x12, 0x34, 0x56, 0x78, // U32
        0x03, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // U64
        0x0d, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, // I64 sign-flipped
        0x0e, 0x7f, 0xff, 0xff, 0xfd, // I32 sign-flipped
        0x05, 0x01, // Bool
        0x06, b'a', 0x00, 0xff, b'b', 0x00, 0x00, // String
        0x07, 0x00, 0xff, 0xff, 0x00, 0x00, // Bytes
        0x0a, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10,
        0x10, 0x10, 0x0b, 0x01, 0x00, 0x02, 0x05, 0x00, // fixed Tuple(U16, Bool)
    ];

    let mut encoded = Vec::new();
    for value in &primary_values {
        encode_primary_key_part(&mut encoded, value).unwrap();
    }
    assert_eq!(encoded, frozen_primary);

    let mut remaining = frozen_primary.as_slice();
    for (value, value_type) in primary_values.iter().zip([
        ValueType::U8,
        ValueType::U16,
        ValueType::U32,
        ValueType::U64,
        ValueType::I64,
        ValueType::I32,
        ValueType::Bool,
        ValueType::String,
        ValueType::Bytes,
        ValueType::Uuid,
        ValueType::Tuple(vec![ValueType::U16, ValueType::Bool]),
    ]) {
        let decoded = decode_primary_key_part(&mut remaining, &value_type).unwrap();
        assert_eq!(decoded, *value);
    }
    assert!(remaining.is_empty());

    let nullable_string = ColumnType::Nullable(Box::new(ColumnType::String));
    let frozen_index_part = [0x09, 0x06, b'a', 0x00, 0xff, b'b', 0x00, 0x00];
    let mut encoded_index = Vec::new();
    encode_index_prefix_part(
        &mut encoded_index,
        &Value::Nullable(Some(Box::new(Value::String("a\0b".to_owned())))),
        &nullable_string,
    )
    .unwrap();
    assert_eq!(encoded_index, frozen_index_part);
    let mut remaining = frozen_index_part.as_slice();
    assert_eq!(
        decode_index_key_part(&mut remaining, &nullable_string, "fixture").unwrap(),
        Value::Nullable(Some(Box::new(Value::String("a\0b".to_owned()))))
    );
    assert!(remaining.is_empty());

    let mut malformed_escape = &[0x06, b'a', 0x00, 0x01][..];
    assert!(decode_primary_key_part(&mut malformed_escape, &ValueType::String).is_err());
    let mut trailing = &[0x05, 0x01, 0xff][..];
    assert_eq!(
        decode_primary_key_part(&mut trailing, &ValueType::Bool).unwrap(),
        Value::Bool(true)
    );
    assert!(
        !trailing.is_empty(),
        "callers must reject a decoded key with trailing bytes"
    );

    let mut positive_quiet_nan = &[0x04, 0xff, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..];
    assert!(decode_index_key_part(&mut positive_quiet_nan, &ColumnType::F64, "fixture").is_err());
    let mut positive_infinity = &[0x04, 0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..];
    assert_eq!(
        decode_index_key_part(&mut positive_infinity, &ColumnType::F64, "fixture").unwrap(),
        Value::F64(f64::INFINITY)
    );
    assert!(positive_infinity.is_empty());

    // The ordered F64 transform is a separate persisted format from record
    // values. These literal receipts pin its boundary order, including signed
    // zero and infinities; NaNs are deliberately outside the format.
    let f64_receipts: &[(f64, [u8; 9])] = &[
        (
            f64::NEG_INFINITY,
            [0x04, 0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ),
        (-0.0, [0x04, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        (0.0, [0x04, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
        (
            f64::INFINITY,
            [0x04, 0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ),
    ];
    let mut previous = None;
    for (value, receipt) in f64_receipts {
        let mut actual = Vec::new();
        encode_index_prefix_part(&mut actual, &Value::F64(*value), &ColumnType::F64).unwrap();
        assert_eq!(actual, receipt);
        let mut remaining = receipt.as_slice();
        let decoded =
            decode_index_key_part(&mut remaining, &ColumnType::F64, "f64 receipt").unwrap();
        assert_eq!(decoded, Value::F64(*value));
        assert!(remaining.is_empty());
        if let Some(previous) = previous {
            assert!(
                previous < *receipt,
                "frozen receipt order must be lexicographic"
            );
        }
        previous = Some(*receipt);
    }
}

#[test]
fn large_value_metadata_keys_use_the_canonical_node_ref_record() {
    let node_ref = crate::large_values::NodeRef {
        object_hash: crate::large_values::ContentHash([13; 32]),
        locator: crate::large_values::Locator::from_seed(b"metadata key NodeRef"),
    };
    let encoded = crate::large_values::encode_node_ref(&node_ref).unwrap();

    for (prefix, key) in [
        (
            b"root/".as_slice(),
            large_value_root_key(&node_ref).unwrap(),
        ),
        (
            b"node/".as_slice(),
            large_value_node_key(&node_ref).unwrap(),
        ),
        (
            b"reclaim/".as_slice(),
            large_value_reclaim_key(&node_ref).unwrap(),
        ),
        (
            b"install/".as_slice(),
            large_value_pending_install_key(&node_ref).unwrap(),
        ),
    ] {
        assert_eq!(key.strip_prefix(prefix), Some(encoded.as_slice()));
    }
}

// These are intentionally internal codec assertions: the metadata column
// family is an engine-owned crash journal/reference ledger and cannot be
// observed or malformed through Groove's public row API. The public
// `stage_large_value_chunk_batch` / reopen / reclaim receipts below exercise
// the same records through the user-visible lifecycle.
#[test]
fn large_value_metadata_records_are_canonical_groove_records() {
    fn to_hex(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    let first = crate::large_values::NodeRef {
        object_hash: crate::large_values::ContentHash([0x11; 32]),
        locator: crate::large_values::Locator::from_seed(b"metadata codec first"),
    };
    let second = crate::large_values::NodeRef {
        object_hash: crate::large_values::ContentHash([0x22; 32]),
        locator: crate::large_values::Locator::from_seed(b"metadata codec second"),
    };
    let value_ref = crate::large_values::LargeValueRef {
        kind: crate::large_values::LargeValueKind::Bytes,
        format_version: crate::large_values::FORMAT_VERSION,
        logical_hash: crate::large_values::ContentHash([0x33; 32]),
        root: first.clone(),
        byte_length: 7,
        utf16_length: None,
        edit_tail: Vec::new(),
    };
    let root = LargeValueRootReferences {
        durable: 3,
        staged: 5,
        node_active: true,
    };
    let node = LargeValueNodeReferences {
        references: 7,
        upload_references: 11,
        children: vec![second.clone(), first.clone()],
    };
    let staged = crate::large_values::StagedLargeValue {
        id: crate::large_values::StagedLargeValueId([0x44; 16]),
        value_ref: value_ref.clone(),
        accounting: crate::large_values::StagedLargeValueAccounting {
            encoded_bytes: 13,
            node_count: 2,
        },
        created_at_ms: 17,
    };
    let pending = crate::large_values::PendingLargeValueUpload {
        id: crate::large_values::StagedLargeValueId([0x55; 16]),
        descriptor: Some(value_ref.clone()),
        receipt_id: Some(crate::large_values::StagedLargeValueId([0x66; 16])),
        accounting: crate::large_values::StagedLargeValueAccounting {
            encoded_bytes: 19,
            node_count: 2,
        },
        created_at_ms: 23,
        chunks: vec![second.clone(), first.clone()],
    };

    let encoded_value_ref = crate::large_values::encode_large_value_ref(&value_ref).unwrap();
    assert_eq!(
        crate::large_values::decode_large_value_ref(&encoded_value_ref).unwrap(),
        value_ref
    );
    for (expected_tag, kind, bytes) in [
        (
            0,
            crate::large_values::LargeValueKind::Bytes,
            b"x".as_slice(),
        ),
        (
            1,
            crate::large_values::LargeValueKind::String,
            b"x".as_slice(),
        ),
        (
            2,
            crate::large_values::LargeValueKind::Json,
            b"{}".as_slice(),
        ),
    ] {
        let prepared = crate::large_values::prepare(kind, bytes).unwrap();
        let encoded = crate::large_values::encode_large_value_ref(&prepared.value_ref).unwrap();
        assert_eq!(
            crate::records::split_variant_record(&encoded).unwrap().0,
            expected_tag
        );
        assert_eq!(
            crate::large_values::decode_large_value_ref(&encoded).unwrap(),
            prepared.value_ref
        );
    }

    let encoded_root = encode_large_value_root_references(&root).unwrap();
    let encoded_node = encode_large_value_node_references(&node).unwrap();
    let encoded_staged = encode_staged_large_value(&staged).unwrap();
    let encoded_pending = encode_pending_large_value_upload(&pending).unwrap();
    for (schema, encoded) in [
        (large_value_root_references_schema(), &encoded_root),
        (large_value_node_references_schema(), &encoded_node),
        (staged_large_value_schema(), &encoded_staged),
        (pending_large_value_upload_schema(), &encoded_pending),
    ] {
        assert!(
            !encoded.starts_with(b"GLVM"),
            "metadata is a standard Groove record, not a private envelope"
        );
        let values = schema.descriptor.bind(encoded).to_values().unwrap();
        assert_eq!(schema.descriptor.create(&values).unwrap(), *encoded);
    }
    assert_eq!(
        large_value_root_references_schema()
            .slots
            .iter()
            .filter_map(|slot| match slot {
                DurableMetadataRecordSlot::Known(id) => Some(*id),
                DurableMetadataRecordSlot::Reserved(_) => None,
            })
            .collect::<Vec<_>>(),
        [
            ROOT_REF_DURABLE_FIELD,
            ROOT_REF_STAGED_FIELD,
            ROOT_REF_NODE_ACTIVE_FIELD,
        ]
    );
    assert_eq!(
        large_value_node_references_schema()
            .slots
            .iter()
            .filter_map(|slot| match slot {
                DurableMetadataRecordSlot::Known(id) => Some(*id),
                DurableMetadataRecordSlot::Reserved(_) => None,
            })
            .collect::<Vec<_>>(),
        [
            NODE_REF_REFERENCES_FIELD,
            NODE_REF_UPLOAD_REFERENCES_FIELD,
            NODE_REF_CHILDREN_FIELD,
        ]
    );
    assert_eq!(
        decode_large_value_root_references(&encoded_root).unwrap(),
        root
    );
    assert_eq!(
        decode_large_value_node_references(&encoded_node).unwrap(),
        LargeValueNodeReferences {
            children: vec![first.clone(), second.clone()],
            ..node
        }
    );
    assert_eq!(decode_staged_large_value(&encoded_staged).unwrap(), staged);
    assert_eq!(
        decode_pending_large_value_upload(&encoded_pending).unwrap(),
        crate::large_values::PendingLargeValueUpload {
            chunks: vec![first.clone(), second.clone()],
            ..pending
        }
    );

    let duplicate_children = encode_large_value_metadata_record(
        large_value_node_references_schema(),
        [
            (NODE_REF_REFERENCES_FIELD, records::Value::U64(1)),
            (NODE_REF_UPLOAD_REFERENCES_FIELD, records::Value::U64(0)),
            (
                NODE_REF_CHILDREN_FIELD,
                records::Value::Array(vec![
                    crate::large_values::node_ref_value(&first),
                    crate::large_values::node_ref_value(&first),
                ]),
            ),
        ],
        "large-value node references",
    )
    .unwrap();
    assert!(decode_large_value_node_references(&duplicate_children).is_err());

    let reversed_children = encode_large_value_metadata_record(
        large_value_node_references_schema(),
        [
            (NODE_REF_REFERENCES_FIELD, records::Value::U64(1)),
            (NODE_REF_UPLOAD_REFERENCES_FIELD, records::Value::U64(0)),
            (
                NODE_REF_CHILDREN_FIELD,
                records::Value::Array(vec![
                    crate::large_values::node_ref_value(&second),
                    crate::large_values::node_ref_value(&first),
                ]),
            ),
        ],
        "large-value node references",
    )
    .unwrap();
    assert!(decode_large_value_node_references(&reversed_children).is_err());

    assert!(decode_large_value_root_references(&encoded_root[..encoded_root.len() - 1]).is_err());
    assert!(decode_large_value_node_references(&encoded_node[..encoded_node.len() - 1]).is_err());
    assert!(decode_staged_large_value(&encoded_staged[..encoded_staged.len() - 1]).is_err());
    assert!(
        decode_pending_large_value_upload(&encoded_pending[..encoded_pending.len() - 1]).is_err()
    );
    let mut trailing_root = encoded_root.clone();
    trailing_root.push(0);
    assert!(decode_large_value_root_references(&trailing_root).is_err());
    let mut trailing_node = encoded_node.clone();
    trailing_node.push(0);
    assert!(decode_large_value_node_references(&trailing_node).is_err());
    let mut trailing_staged = encoded_staged.clone();
    trailing_staged.push(0);
    assert!(decode_staged_large_value(&trailing_staged).is_err());
    let mut trailing_pending = encoded_pending.clone();
    trailing_pending.push(0);
    assert!(decode_pending_large_value_upload(&trailing_pending).is_err());
    let mut unknown_value_ref_tag = encoded_value_ref.clone();
    unknown_value_ref_tag[0] = 3;
    assert!(crate::large_values::decode_large_value_ref(&unknown_value_ref_tag).is_err());
    assert!(decode_large_value_root_references(&[0; 16]).is_err());

    // This intentionally internal receipt proves the engine-owned durable
    // layout uses numeric IDs as physical record slots. It is not observable
    // through Groove's row API; the lifecycle receipts below cover it there.
    let children_at_three = durable_metadata_record_schema([
        (1, "references", records::ValueType::U64),
        (2, "upload_references", records::ValueType::U64),
        (3, "children", records::ValueType::U64),
    ]);
    let children_at_three_reordered = durable_metadata_record_schema([
        (3, "children", records::ValueType::U64),
        (1, "references", records::ValueType::U64),
        (2, "upload_references", records::ValueType::U64),
    ]);
    let children_at_four = durable_metadata_record_schema([
        (1, "references", records::ValueType::U64),
        (2, "upload_references", records::ValueType::U64),
        (4, "children", records::ValueType::U64),
    ]);
    let at_three = encode_large_value_metadata_record(
        &children_at_three,
        [
            (1, records::Value::U64(7)),
            (2, records::Value::U64(11)),
            (3, records::Value::U64(13)),
        ],
        "children-at-three",
    )
    .unwrap();
    let reordered = encode_large_value_metadata_record(
        &children_at_three_reordered,
        [
            (3, records::Value::U64(13)),
            (1, records::Value::U64(7)),
            (2, records::Value::U64(11)),
        ],
        "children-at-three",
    )
    .unwrap();
    let at_four = encode_large_value_metadata_record(
        &children_at_four,
        [
            (1, records::Value::U64(7)),
            (2, records::Value::U64(11)),
            (4, records::Value::U64(13)),
        ],
        "children-at-four",
    )
    .unwrap();
    assert_eq!(
        at_three, reordered,
        "source declaration order is not physical"
    );
    assert_eq!(
        to_hex(&at_three),
        "07000000000000000b000000000000000d00000000000000"
    );
    assert_eq!(
        to_hex(&at_four),
        "07000000000000000b000000000000000d0000000000000000"
    );
    assert!(
        decode_large_value_metadata_record(&at_three, &children_at_four, "children-at-four")
            .is_err(),
        "renumbering CHILDREN from slot 3 to 4 must reject old physical bytes"
    );
    let nonempty_reserved = children_at_four
        .descriptor
        .create(&[
            records::Value::U64(7),
            records::Value::U64(11),
            records::Value::Nullable(Some(Box::new(records::Value::Bytes(vec![1])))),
            records::Value::U64(13),
        ])
        .unwrap();
    assert!(
        decode_large_value_metadata_record(
            &nonempty_reserved,
            &children_at_four,
            "children-at-four"
        )
        .is_err(),
        "reserved slots remain permanently empty"
    );

    assert_eq!(
        [
            &encoded_value_ref,
            &encoded_root,
            &encoded_node,
            &encoded_staged,
            &encoded_pending,
        ]
        .map(to_hex),
        [
            "000207000000000000000000000000000000003a0000007e0000003333333333333333333333333333333333333333333333333333333333333333240000001111111111111111111111111111111111111111111111111111111111111111ee96d2a87be5c1dd447125981a7bf47f0d48d127b8b8fbea50b16a8ecf6fdfff00000000",
            "0300000000000000050000000000000001",
            "07000000000000000b00000000000000020000004c000000240000001111111111111111111111111111111111111111111111111111111111111111ee96d2a87be5c1dd447125981a7bf47f0d48d127b8b8fbea50b16a8ecf6fdfff2400000022222222222222222222222222222222222222222222222222222222222222226c51cfbc9a018998ba52dec05284df710cb836a95d75d802ac9471e096c59292",
            "0d00000000000000020000000000000011000000000000002c00000044444444444444444444444444444444000207000000000000000000000000000000003a0000007e0000003333333333333333333333333333333333333333333333333333333333333333240000001111111111111111111111111111111111111111111111111111111111111111ee96d2a87be5c1dd447125981a7bf47f0d48d127b8b8fbea50b16a8ecf6fdfff00000000",
            "13000000000000000200000000000000170000000000000034000000b8000000c90000005555555555555555555555555555555501000207000000000000000000000000000000003a0000007e0000003333333333333333333333333333333333333333333333333333333333333333240000001111111111111111111111111111111111111111111111111111111111111111ee96d2a87be5c1dd447125981a7bf47f0d48d127b8b8fbea50b16a8ecf6fdfff000000000166666666666666666666666666666666020000004c000000240000001111111111111111111111111111111111111111111111111111111111111111ee96d2a87be5c1dd447125981a7bf47f0d48d127b8b8fbea50b16a8ecf6fdfff2400000022222222222222222222222222222222222222222222222222222222222222226c51cfbc9a018998ba52dec05284df710cb836a95d75d802ac9471e096c59292",
        ]
        .map(str::to_owned)
    );
    assert_eq!(
        to_hex(Database::descriptor_upload_id(&value_ref).unwrap().0),
        "b3070a544dd101d3d4d46716dd8dd6b1"
    );
}
