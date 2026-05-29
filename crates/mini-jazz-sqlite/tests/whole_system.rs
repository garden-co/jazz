use mini_jazz_sqlite::{
    HistoryBlockExport, HistoryCompactionPolicy, RejectionInfo, RowDiff, Runtime, SchemaDef,
    Storage, TopFieldHistoryDeltaOptions,
};
use std::collections::BTreeMap;
use tempfile::tempdir;

macro_rules! json {
    ($($json:tt)+) => {
        mini_jazz_sqlite::Value::from(serde_json::json!($($json)+))
    };
}

mod support;
use support::FixtureRuntimeExt;

#[path = "whole_system/branches.rs"]
mod branches;
#[path = "whole_system/generic_schema.rs"]
mod generic_schema;
#[path = "whole_system/invariant_coverage.rs"]
mod invariant_coverage;
#[path = "whole_system/policies.rs"]
mod policies;
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
