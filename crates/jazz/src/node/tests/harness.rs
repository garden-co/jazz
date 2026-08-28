//! Test harness and seeded regressions for node semantics. This module owns
//! fixtures that drive the `NodeState` through sync, merge, query, policy, branch,
//! and lens scenarios; production logic stays in the node submodules, while
//! model comparisons use [`crate::oracle`].

use super::*;
use crate::oracle::{ModelRowVersion, Oracle, OracleTxState, ParallelMaterializationOracle};
use crate::peer::{PeerEvictionPins, PeerMetrics, PeerState};
use crate::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, RegisterShapeOptions, SchemaLineagePublication,
    SchemaVersion, TableLens, VersionRecord,
};
use crate::query::{
    ArraySubquery, Binding, BindingId, Query, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationProjectColumn, RelationProjectExpr,
    RelationQuery, ShapeId, ValidatedQuery, claim, col, contains, eq, gt, lit, ne, not, param,
};
use crate::tools::public_schema::{
    CmpOp as PublicCmpOp, ColumnDescriptor as PublicColumnDescriptor,
    ColumnMergeStrategy as PublicColumnMergeStrategy, ColumnType as PublicColumnType,
    EnumCaseDescriptor as PublicEnumCaseDescriptor, Operation as PublicOperation,
    PolicyExpr as PublicPolicyExpr, PolicyValue as PublicPolicyValue,
    RowDescriptor as PublicRowDescriptor, Schema as PublicSchema,
    SchemaBuilder as PublicSchemaBuilder, TableName as PublicTableName,
    TablePolicies as PublicTablePolicies, TableSchema as PublicTableSchema,
    TableSchemaBuilder as PublicTableSchemaBuilder, Value as PublicValue,
};
use crate::tx::MergeAspect;
use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{
    MemoryStorage, OrderedKvStorage, ReopenableStorage, Value as StorageValue, YieldingStorage,
};
use jazz_storage_rocksdb::RocksDbStorage as ImmediateRocksDbStorage;
use std::path::Path;

type RocksDbStorage = YieldingStorage<ImmediateRocksDbStorage>;

trait TestRocksOpen: Sized {
    fn open(
        path: impl AsRef<Path>,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error>;
}

impl TestRocksOpen for RocksDbStorage {
    fn open(
        path: impl AsRef<Path>,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error> {
        ImmediateRocksDbStorage::open(path, column_families).map(YieldingStorage::wrap)
    }
}
use std::collections::{BTreeMap, BTreeSet, VecDeque};

include!("support.rs");
include!("catalogue_lenses/mod.rs");
include!("lens_projected_maintained.rs");
include!("time_travel.rs");
include!("branch_views.rs");
include!("queries.rs");
include!("exclusive_transactions.rs");
include!("mergeable_open_transactions.rs");
include!("policies_rls/mod.rs");
include!("persistence_contracts.rs");
include!("write_policy_lowering.rs");
include!("sync/mod.rs");
include!("m3_differential.rs");
include!("counter_merge.rs");
include!("merge_heads.rs");
include!("recovery.rs");
include!("edge_authority.rs");
include!("general.rs");
include!("view_update_capture.rs");
include!("native_storage_corpus.rs");
