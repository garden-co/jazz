# UPDATE with INHERITS policy incorrectly denies access

## What

UPDATE operations fail with PolicyDenied even when an INHERITS chain should grant access.

## Where

`crates/jazz-tools/src/query_manager/policy_graph.rs`

## Steps to reproduce

N/A — needs investigation to build a minimal repro. Likely involves an UPDATE on a table with an INHERITS policy where the granting permission comes from a parent in the inheritance chain.

## Expected

UPDATE succeeds when the INHERITS chain grants access.

## Actual

UPDATE returns PolicyDenied.

## Priority

high

## Notes

Root cause likely in PolicyGraph wiring for UPDATE USING — the inheritance chain may not be fully traversed or the UPDATE policy evaluation path may differ from SELECT.
