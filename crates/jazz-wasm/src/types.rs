//! Shared boundary types used at the WASM boundary.

use serde::Serialize;

pub use jazz_tools::query_manager::policy::{
    CmpOp, Operation as PolicyOperation, PolicyExpr, PolicyValue,
};
pub use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, OperationPolicy, Schema, TablePolicies, TableSchema, Value,
};
use jazz_tools::sync_manager::MutationRejectCode;
pub use jazz_tools::wire_types::{
    SubscriptionRow, SubscriptionRowAdded, SubscriptionRowChange, SubscriptionRowDelta,
    SubscriptionRowRemoved, SubscriptionRowUpdated,
};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WasmObjectOutcomeState {
    Pending {
        mutation_id: String,
    },
    Accepted {
        mutation_id: String,
    },
    Errored {
        mutation_id: String,
        code: MutationRejectCode,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WasmObjectOutcomeEvent {
    pub object_id: String,
    pub outcome: Option<WasmObjectOutcomeState>,
}
