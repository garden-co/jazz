# Ordinary-row JSON documents: first vertical-slice receipt

## Scope

This receipt measures the first public `JsonDocumentStore` representation. It
uses only ordinary Jazz rows and public operations:

- one mutable document row containing `root_id`;
- one immutable root row containing the complete ordered `part_ids` array;
- one immutable part row per JSON container or scalar;
- one ordinary mutable projection row per declared JSON Pointer;
- one staged Jazz transaction per logical create or scalar edit.

The flat root is intentionally a correctness-first bootstrap. It gives every
retained root a complete independently readable snapshot, but rewrites O(parts)
reference metadata for a localized edit. The spec's scale target is a
bounded-fanout persistent tree.

## Command and environment

```sh
JAZZ_JSON_DOCUMENTS=50 JAZZ_JSON_BYTES=10240 \
cargo test -p jazz --no-default-features --features test \
  --test ordinary_json_documents \
  ordinary_json_document_representative_benchmark_receipt \
  -- --ignored --exact --nocapture
```

Environment: x86_64 Linux, Rust debug test profile, in-memory
`JazzClient::test_client`, current-thread Tokio runtime, no network or durability
wait. Each 10 KiB logical document has 37 immutable parts and two declared
scalar projections. These are feasibility/attribution numbers, not production
latency claims.

## Results

| Documents | Create / doc | Current read / doc | Declared-path filter | Scalar edit / doc | Retained-root read |
| --------: | -----------: | -----------------: | -------------------: | ----------------: | -----------------: |
|        10 |    25.162 ms |          21.495 ms |             3.899 ms |         32.874 ms |          19.479 ms |
|        50 |    24.721 ms |          74.514 ms |             7.424 ms |         95.224 ms |          73.689 ms |

One localized scalar edit creates one new part and one new immutable root, then
versions the document row and both declared projection rows: two new rows plus
three ordinary row versions in this fixture. The old root remains readable and
still reconstructs the old scalar value.

## Findings

1. The semantic vertical slice works entirely in userland. Root advancement,
   affected projection values, and immutable parts commit atomically, and a
   retained root reconstructs without parent-row history replay.
2. Declared-path filtering is already the cheapest measured operation and grows
   modestly from 10 to 50 documents because it touches the compact projection
   relation rather than every JSON part.
3. Current and retained-root hydration scale badly with the total part table in
   this build. The facade issues one query filtered by `document_id`, but 50-doc
   reads are roughly 3.5× slower per document than 10-doc reads. Localized edits
   inherit that cost because they first hydrate the current root's part set.
4. The public facade's implicit `id`/`_id` filter was rejected before this work,
   despite the public query model and core planner supporting row-id operands.
   The feature repairs that general lowering gap and uses point filters for
   document/root lookup.
5. A persistent root removes O(parts) root rewrite bytes, but it does not by
   itself solve hydration. The next general-purpose performance dependency is a
   scale-independent indexed FK filter or one ordinary recursive/reachable query
   over immutable referenced rows.

## Sensitivity and next gates

- Debug timings are sensitive to query compilation/refresh overhead and should
  be rerun under the repository perf profile before setting budgets.
- The current facade loads all immutable parts for one `document_id`, then
  selects the ids reachable from the requested root. Thus it does not scan Jazz
  row-version history, but accumulated immutable parts from prior roots still
  increase current-table work. A persistent traversal query should read only
  reachable nodes.
- Array insertion/deletion and new object paths are not yet public mutations;
  the current vertical slice intentionally exposes only scalar replacement.
- Server synchronization, untrusted-client policies, branches, time-travel root
  selection, subscriptions, wire bytes, and persisted bytes remain required
  before this is a production replacement for the retired special value path.

Tooling friction: `dev/t` captures benchmark output, so raw receipts require a
direct `cargo test -- --nocapture` invocation. The public row-id lowering mismatch
also initially converted every root/part lookup into an accidental table scan.
