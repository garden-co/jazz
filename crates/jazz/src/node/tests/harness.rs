//! Test harness and seeded regressions for node semantics. This module owns
//! fixtures that drive the `NodeState` through sync, merge, query, policy, branch,
//! and lens scenarios; production logic stays in the node submodules, while
//! model comparisons use [`crate::oracle`].

use super::*;
use crate::legacy_test_future::{OptionFutureExt as _, ResultFutureExt as _};
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
use crate::schema::{MergeStrategy, Policy};
use crate::tx::{
    BranchLineage, ContributionComponent, ContributionCoordinate, ContributionDot, MergeAspect,
};
use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{
    BtreeSyncPolicy, ColumnFamilyName, Key, MemoryStorage, NativeBtreeStorage, OrderedKvStorage,
    ReopenableStorage, ScanVisitor, Value as StorageValue, WriteOperation,
};
use jazz_storage_rocksdb::RocksDbStorage;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

include!("support.rs");
include!("catalogue_lenses/mod.rs");
include!("lens_projected_maintained.rs");
include!("branching.rs");
include!("time_travel.rs");
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
