# project_row Memcpy Optimization — TODO (Later)

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

> `crates/groove/src/query_manager/graph_nodes/project.rs`
