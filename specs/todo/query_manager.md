# Query Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/query_manager.md](../status-quo/query_manager.md)

## High Priority

### UPDATE with INHERITS chains

UPDATE may fail with PolicyDenied even when an INHERITS chain should grant access. Needs investigation of PolicyGraph wiring for UPDATE USING.

> `crates/groove/src/query_manager/policy_graph.rs`

### ORDER BY + LIMIT subscriptions

Subscriptions with `ORDER BY ... LIMIT N` don't receive incremental updates when new rows enter the top N. LimitOffsetNode may need rework or a dedicated TopN node.

> `crates/groove/src/query_manager/graph_nodes/limit_offset.rs`

## Medium Priority

### Self-referential INHERITS

Currently disallowed. Common pattern for hierarchical data (folders → parent_id) needs iterative settlement with depth limit.

> `crates/groove/src/query_manager/policy_graph.rs`

### UUID format mismatch

Session claim comparison uses Debug format which may differ from JSON string format, causing false negatives in IN checks.

> `crates/groove/src/query_manager/session.rs`

## Low Priority

### project_row memcpy optimization

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

> `crates/groove/src/query_manager/graph_nodes/project.rs`

### subscribe_full API

Only delta-mode subscriptions exposed. `OutputMode::Full` exists but isn't wired to API.

> `crates/groove/src/query_manager/graph_nodes/output.rs`

### NOT(complex_clause) semantics

Meaning of `NOT(INHERITS(...))` is unclear; needs documentation or semantic fix.
