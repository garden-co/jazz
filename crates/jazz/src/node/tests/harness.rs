//! Test harness and seeded regressions for node semantics. This module owns
//! fixtures that drive the `NodeState` through sync, merge, query, policy, branch,
//! and lens scenarios; production logic stays in the node submodules, while
//! model comparisons use [`crate::oracle`].

use super::*;
use crate::oracle::{ModelRowVersion, Oracle, OracleTxState, ParallelMaterializationOracle};
use crate::peer::{PeerMetrics, PeerState};
use crate::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, RegisterShapeOptions, SchemaVersion, TableLens,
    VersionRecord,
};
use crate::query::{
    ArraySubquery, Binding, BindingId, Query, ShapeId, ValidatedQuery, claim, col, contains, eq,
    lit, ne, not, param,
};
use crate::schema::{MergeStrategy, Policy};
use crate::tx::MergeAspect;
use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{
    ColumnFamilyName, Key, MemoryStorage, OrderedKvStorage, ReopenableStorage, RocksDbStorage,
    ScanVisitor, Value as StorageValue, WriteOperation,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

include!("support.rs");
include!("catalogue_lenses.rs");
include!("branching.rs");
include!("time_travel.rs");
include!("queries.rs");
include!("exclusive_transactions.rs");
include!("policies_rls.rs");
include!("sync.rs");
include!("counter_merge.rs");
include!("merge_heads.rs");
include!("recovery.rs");
include!("content_store.rs");
include!("edge_authority.rs");
include!("general.rs");
include!("view_update_capture.rs");
