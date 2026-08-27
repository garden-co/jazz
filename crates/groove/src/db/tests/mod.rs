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
