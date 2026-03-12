use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::object::BranchName;
use crate::query_manager::encoding::decode_column;
use crate::query_manager::graph_nodes::filter::FilterNode;
use crate::query_manager::graph_nodes::policy_filter::PolicyFilterNode;
use crate::query_manager::graph_nodes::sort::SortDirection;
use crate::query_manager::types::{
    ColumnType, Row, RowDescriptor, TableName, TupleDescriptor, Value,
};
use crate::storage::IndexScanDirection;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedRowKey {
    pub logical_column: String,
    pub local_col_index: Option<usize>,
    pub use_row_id: bool,
    pub expand_array: bool,
}

impl ResolvedRowKey {
    pub(crate) fn from_descriptor(descriptor: &RowDescriptor, raw: &str) -> Option<Self> {
        let column = raw.split('.').next_back().unwrap_or(raw);
        if let Some(local_col_index) = descriptor.column_index(column) {
            let expand_array = matches!(
                descriptor.columns[local_col_index].column_type,
                ColumnType::Array { .. }
            );
            return Some(Self {
                logical_column: column.to_string(),
                local_col_index: Some(local_col_index),
                use_row_id: false,
                expand_array,
            });
        }

        (column == "id" || column == "_id").then_some(Self {
            logical_column: "_id".to_string(),
            local_col_index: None,
            use_row_id: true,
            expand_array: false,
        })
    }

    pub(crate) fn index_column(&self) -> &str {
        if self.use_row_id {
            "_id"
        } else {
            self.logical_column.as_str()
        }
    }

    pub(crate) fn matches_selector(&self, descriptor: &RowDescriptor, selector: &str) -> bool {
        let column = selector.split('.').next_back().unwrap_or(selector);
        if self.use_row_id {
            (column == "id" || column == "_id") && descriptor.column_index(column).is_none()
        } else {
            column == self.logical_column
        }
    }

    pub(crate) fn extract_value(&self, row: &Row, descriptor: &RowDescriptor) -> Option<Value> {
        if self.use_row_id {
            return Some(Value::Uuid(row.id));
        }

        decode_column(descriptor, &row.data, self.local_col_index?).ok()
    }

    pub(crate) fn extract_lookup_values(
        &self,
        row: &Row,
        descriptor: &RowDescriptor,
    ) -> Vec<Value> {
        let Some(value) = self.extract_value(row, descriptor) else {
            return Vec::new();
        };
        if value == Value::Null {
            return Vec::new();
        }

        if self.expand_array
            && let Value::Array(values) = value
        {
            return values
                .into_iter()
                .filter(|value| *value != Value::Null)
                .collect();
        }

        vec![value]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedSortKey {
    pub target: ResolvedSortTarget,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedSortTarget {
    Column {
        element_index: usize,
        descriptor: RowDescriptor,
        local_col_index: usize,
    },
    RowId {
        element_index: usize,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ExactMatchProbe {
    pub translated_column: String,
    pub value: Value,
}

#[derive(Debug)]
pub(crate) struct OrderedDriverSourceSpec {
    pub branch: BranchName,
    pub table: TableName,
    pub driver_descriptor: RowDescriptor,
    pub driver_key: ResolvedRowKey,
    pub direction: IndexScanDirection,
    pub translated_driver_column: String,
    pub start: Bound<Value>,
    pub end: Bound<Value>,
    pub required_probes: Vec<ExactMatchProbe>,
    pub policy_evaluator: Option<PolicyFilterNode>,
    pub desired_prefix_len: Option<usize>,
    pub enable_prefix_short_circuit: bool,
    pub max_direct_required_ids: usize,
}

#[derive(Debug)]
pub(crate) struct OrderedDriverSourcePlan {
    pub spec: Arc<OrderedDriverSourceSpec>,
    pub ordered_scan_column: String,
    pub exact_match_probe_columns: Vec<String>,
    pub policy_dependencies: Vec<TableName>,
}

#[derive(Debug)]
pub(crate) struct MergeOrderedSpec {
    pub direction: IndexScanDirection,
    pub driver_descriptor: RowDescriptor,
    pub driver_key: ResolvedRowKey,
}

#[derive(Debug)]
pub(crate) struct JoinLookupSpec {
    pub left_scope_index: usize,
    pub right_scope_index: usize,
    pub left_table: TableName,
    pub right_table: TableName,
    pub left_key: ResolvedRowKey,
    pub right_key: ResolvedRowKey,
    pub left_translated_columns_by_branch: HashMap<BranchName, String>,
    pub right_translated_columns_by_branch: HashMap<BranchName, String>,
}

#[derive(Debug)]
pub(crate) struct ScopedPolicySpec {
    pub scope_index: usize,
    pub dependency_tables: Vec<TableName>,
    pub evaluators_by_branch: HashMap<BranchName, PolicyFilterNode>,
}

#[derive(Debug)]
pub(crate) struct ProbeJoinSpec {
    pub driver_scope_index: usize,
    pub table_descriptors: Vec<RowDescriptor>,
    pub join_edges: Vec<JoinLookupSpec>,
    pub residual_filter: Option<FilterNode>,
    pub policies: Vec<ScopedPolicySpec>,
}

#[derive(Debug)]
pub(crate) struct TieSortSpec {
    pub driver_scope_index: usize,
    pub driver_descriptor: RowDescriptor,
    pub driver_key: ResolvedRowKey,
    pub sort_keys: Vec<ResolvedSortKey>,
    pub desired_prefix_len: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct IndexedTopKGraphPlan {
    pub base_descriptor: RowDescriptor,
    pub combined_descriptor: RowDescriptor,
    pub table_descriptors: Vec<RowDescriptor>,
    pub tuple_descriptor: TupleDescriptor,
    pub source_plans: Vec<OrderedDriverSourcePlan>,
    pub merge_spec: Arc<MergeOrderedSpec>,
    pub probe_join_spec: Arc<ProbeJoinSpec>,
    pub probe_join_lookup_columns: Vec<(TableName, String)>,
    pub probe_join_policy_dependencies: Vec<TableName>,
    pub tie_sort_spec: Arc<TieSortSpec>,
    pub limit: Option<usize>,
    pub offset: usize,
}
