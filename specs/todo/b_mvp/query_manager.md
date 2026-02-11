# Query Manager — TODO

Remaining work items and known limitations.

> Status quo: [specs/status-quo/query_manager.md](../../status-quo/query_manager.md)

## Phasing

- **MVP**: UPDATE with INHERITS chains, ORDER BY + LIMIT subscriptions
- **Launch**: Self-referential INHERITS, UUID format mismatch
- **Later**: project_row optimization, subscribe_full API, NOT(complex_clause) semantics

## MVP: UPDATE with INHERITS Chains

UPDATE may fail with PolicyDenied even when an INHERITS chain should grant access. Needs investigation of PolicyGraph wiring for UPDATE USING.

> `crates/groove/src/query_manager/policy_graph.rs`

## MVP: ORDER BY + LIMIT Subscriptions

Subscriptions with `ORDER BY ... LIMIT N` don't receive incremental updates when new rows enter the top N. LimitOffsetNode may need rework or a dedicated TopN node.

> `crates/groove/src/query_manager/graph_nodes/limit_offset.rs`

## Launch: Self-Referential INHERITS

Currently disallowed. Common pattern for hierarchical data (folders → parent_id) needs iterative settlement with depth limit.

> `crates/groove/src/query_manager/policy_graph.rs`

## Launch: UUID Format Mismatch

Session claim comparison uses Debug format which may differ from JSON string format, causing false negatives in IN checks.

> `crates/groove/src/query_manager/session.rs`

## Later: project_row Memcpy Optimization

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

> `crates/groove/src/query_manager/graph_nodes/project.rs`

## Later: subscribe_full API

Only delta-mode subscriptions exposed. `OutputMode::Full` exists but isn't wired to API.

> `crates/groove/src/query_manager/graph_nodes/output.rs`

## Later: NOT(complex_clause) Semantics

Meaning of `NOT(INHERITS(...))` is unclear; needs documentation or semantic fix.
