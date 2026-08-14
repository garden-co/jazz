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
versions the document row and the one affected declared projection row: two new
rows plus two ordinary row versions in this fixture. The old root remains readable and
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

## Compressed RocksDB storage receipt

The storage receipt compares three representations in separate fresh RocksDB
directories, using the same schema, generated documents, status edits, and one
transaction per logical edit:

- a single ordinary JSON column rewritten in full;
- mutable scalar-part rows plus a declared-path projection row;
- the vertical slice above: immutable parts and roots plus declared projections.

The active RocksDB profile compresses history append column families with Zstd,
current/index/meta column families with LZ4, and bottommost files with Zstd. Run:

```sh
JAZZ_JSON_STORAGE_DOCS=20 JAZZ_JSON_STORAGE_EDITS=10 \
JAZZ_JSON_STORAGE_BYTES=10240 \
cargo test -p jazz --features test,rocksdb \
  --test ordinary_json_storage_receipt \
  ordinary_json_compressed_rocksdb_storage_receipt \
  -- --ignored --exact --nocapture
```

The figures below subtract an empty database created with the identical schema.
“Seed open” and “edited open” include the live WAL. “Edited closed” is measured
after orderly client shutdown. The public storage API does not expose a safe
manual flush or compaction operation, so none was forced or claimed.

| Workload                  | Representation               | Seed open apparent | Edited open apparent | Edited closed apparent | Edited closed allocated |
| ------------------------- | ---------------------------- | -----------------: | -------------------: | ---------------------: | ----------------------: |
| 20 × 10 KiB, 10 edits/doc | whole-row rewrite            |          443,531 B |          4,908,068 B |            4,886,731 B |             4,882,432 B |
| 20 × 10 KiB, 10 edits/doc | mutable parts + projections  |        1,008,186 B |          1,759,089 B |            1,737,752 B |             1,732,608 B |
| 20 × 10 KiB, 10 edits/doc | immutable root + projections |        1,109,372 B |          2,245,573 B |            2,245,534 B |             2,244,608 B |
| 10 × 100 KiB, 5 edits/doc | whole-row rewrite            |        2,065,381 B |         12,399,585 B |           12,378,246 B |            12,374,016 B |
| 10 × 100 KiB, 5 edits/doc | mutable parts + projections  |        2,347,553 B |          2,535,162 B |            2,513,823 B |             2,510,848 B |
| 10 × 100 KiB, 5 edits/doc | immutable root + projections |        2,398,555 B |          2,682,614 B |            2,661,275 B |             2,658,304 B |

For 10 KiB documents and ten localized edits, mutable parts used 35% and the
immutable-root model 46% of the whole-row representation's closed apparent
growth. For 100 KiB documents and five edits, those ratios fell to 20% and 22%.
The immutable model pays modest extra root/part metadata for independent retained
roots, while still avoiding repeated large-value bytes for localized edits.

RocksDB preallocates live files: the empty open database had 78,184,448 allocated
bytes, larger than these populated open stores, so independently measured
baseline-subtracted open _allocated_ values can be negative and are not useful.
Closed allocated values track apparent values and are the meaningful disk receipt.
The empty baselines were 157,399 apparent/78,184,448 allocated bytes while open
and about 178,920 apparent/204,800 allocated bytes after close.

An early version closed and reopened between seeding and edits. That uncovered a
separate persistent-client conflict (`row visible parent changed since transaction
write was staged`) on the first post-reopen mutable update. The receipt now keeps
one warm client handle through both phases so it measures representation cost
rather than that lifecycle issue. It is not a cold-restart correctness receipt;
the restart conflict is a tracked prerequisite for adding that lifecycle gate.
Chromium OPFS was not added: reproducing the same
three low-level layouts through the browser facade would add a different runtime
and persistence implementation, whereas this question is specifically answered
by the production RocksDB compression profile.
