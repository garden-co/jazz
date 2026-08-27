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
// observed or malformed through Groove's public row API.
#[test]
fn large_value_metadata_records_are_versioned_canonical_groove_values() {
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
    for (tag, encoded) in [
        (LARGE_VALUE_ROOT_REFERENCES_TAG, &encoded_root),
        (LARGE_VALUE_NODE_REFERENCES_TAG, &encoded_node),
        (STAGED_LARGE_VALUE_TAG, &encoded_staged),
        (PENDING_LARGE_VALUE_UPLOAD_TAG, &encoded_pending),
    ] {
        assert_eq!(&encoded[..4], LARGE_VALUE_METADATA_MAGIC);
        assert_eq!(encoded[4], LARGE_VALUE_METADATA_VERSION);
        assert_eq!(encoded[5], tag);
    }
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
        LARGE_VALUE_NODE_REFERENCES_TAG,
        large_value_node_references_descriptor(),
        &[
            records::Value::U64(1),
            records::Value::U64(0),
            records::Value::Array(vec![
                crate::large_values::node_ref_value(&first),
                crate::large_values::node_ref_value(&first),
            ]),
        ],
        "large-value node references",
    )
    .unwrap();
    assert!(decode_large_value_node_references(&duplicate_children).is_err());

    let mut wrong_version = encoded_root.clone();
    wrong_version[4] = LARGE_VALUE_METADATA_VERSION + 1;
    assert!(decode_large_value_root_references(&wrong_version).is_err());
    assert!(decode_large_value_root_references(&[0; 16]).is_err());

    assert_eq!(
        [
            &encoded_value_ref,
            &encoded_root,
            &encoded_node,
            &encoded_staged,
            &encoded_pending,
        ]
        .map(|encoded| blake3::hash(encoded).to_hex().to_string()),
        [
            "7b4260ad792ca009bc0545aa9ad410b6a17e0a272039f92c368c274dd62d8092",
            "7f2ef6c6815aa9cfdaf2fddd64c1d02cef060f13874d62d287ce08df59f8acaf",
            "45b4d2d881f84ef9402b14c9bf1a2c0965cdedf0f99712f5b90a91015119b003",
            "530554ffae216bacd195d0575c9d5b0c3c12ad05cee3f38dd7fac80c8e7057bb",
            "282844f56e020418663781963d4ecf6d47b99ebe489c1749a8ce7b945bf27aaa",
        ]
        .map(str::to_owned)
    );
    assert_eq!(
        to_hex(Database::descriptor_upload_id(&value_ref).unwrap().0),
        "b3070a544dd101d3d4d46716dd8dd6b1"
    );
}
