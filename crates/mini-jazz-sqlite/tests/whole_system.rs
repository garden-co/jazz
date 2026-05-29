use mini_jazz_sqlite::{
    sync::QueryReadRecord, BuiltQuery, ReadTier, RejectionInfo, RowDiff, Runtime, SchemaDef,
    Storage, SubscriptionRowDelta,
};
use serde_json::json;
use std::collections::BTreeMap;
use tempfile::tempdir;

mod support;
use support::todo_app::FixtureRuntimeExt;

#[path = "whole_system/branches.rs"]
mod branches;
#[path = "whole_system/generic_schema.rs"]
mod generic_schema;
#[path = "whole_system/invariant_coverage.rs"]
mod invariant_coverage;
#[path = "whole_system/policies.rs"]
mod policies;
#[path = "whole_system/query_matrix.rs"]
mod query_matrix;
#[path = "whole_system/recursive_queries.rs"]
mod recursive_queries;
#[path = "whole_system/schema_lenses.rs"]
mod schema_lenses;
#[path = "whole_system/storage_projection.rs"]
mod storage_projection;
#[path = "whole_system/subscriptions.rs"]
mod subscriptions;
#[path = "whole_system/sync_fate.rs"]
mod sync_fate;
#[path = "whole_system/transactions.rs"]
mod transactions;
