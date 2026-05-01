# UPDATE with INHERITS policy incorrectly denies access

## What

UPDATE operations fail with PolicyDenied even when an INHERITS chain should grant access.

## Priority

high

## Notes

- Where: `crates/jazz-tools/src/query_manager/policy_graph.rs`
- Repro is not minimized yet. The likely path is an UPDATE on a table with an INHERITS policy where the granting permission comes from a parent in the inheritance chain.
- Expected: UPDATE succeeds when the INHERITS chain grants access.
- Actual: UPDATE returns `PolicyDenied`.
- Likely root cause: `PolicyGraph` wiring for `UPDATE USING`, where the inheritance chain may not be fully traversed or may differ from the SELECT path.
