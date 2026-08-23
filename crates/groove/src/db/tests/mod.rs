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
