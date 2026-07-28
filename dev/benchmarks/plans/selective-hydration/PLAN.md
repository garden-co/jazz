# Plan: selective and bounded maintained-query hydration

Status: design and disposable direct-Groove A/B evidence only. No production
worktree change.

## Goal

Make “latest 100 filtered documents for my current team” hydrate in work
proportional to that team and page, rather than total database rows multiplied
by retained terminals.

## Evidence

Fixture: 500,000 documents, 5,000 teams, one 30,000-document hot team,
53.3% residual-filter selectivity, ordered by
`(updated_at DESC, id DESC)`, limit 100. Every lane asserted the exact same
ordered IDs. Times below are one optimized timed hydration per lane over one
shared in-memory seed; read counts are the stronger evidence.

| Hydration path            | Representative wall time | Rows read |
| ------------------------- | -----------------------: | --------: |
| Current dynamic binding   |               267.315 ms |   500,000 |
| Warm second binding       |               195.624 ms |   500,000 |
| Static team-prefix access |                28.427 ms |    30,000 |
| Bounded reverse access    |                 0.315 ms |       188 |

The static prefix is 9.4x faster than the dynamic binding. The bounded ordered
walk is 849x faster and reads 2,660x fewer rows. These are same-process
microbench comparisons, not a comparison with the full Jazz policy benchmark.

For simultaneously retained routes:

| Path                        | Representative wall time |  Rows read |
| --------------------------- | -----------------------: | ---------: |
| 32 current dynamic bindings |                  7.729 s | 16,000,000 |
| 32 static team prefixes     |                34.650 ms |     33,008 |

That is a 223x wall-time and 485x read-count reduction. Five overlapping sinks
read 2.5 million rows versus 500,000 for one sink despite hydration-memo hits:
memoization skips evaluator work after sources have already been read.

Retained state is also source-sized. The 32 dynamic bindings held 283,634
arrangement rows / 12.72 MB encoded plus 35.99 MB of hydration memo. Prefix
specialization held 17,601 arrangement rows / 0.70 MB plus 2.09 MB of memo.
The hot-team `TopBy(100)` retained all 16,000 qualifying candidates, not 100.
Encoded sizes exclude allocator and hash-table overhead.

The exact-500k distribution has 94 documents for most cold teams because 5,000
teams with at least 100 documents plus one 30,000-document team cannot fit in
500,000 rows. One cold team has 188 rows to test the bounded walk.

## Source mechanism

1. Groove snapshots every ancestor table before evaluation/memo lookup:
   `crates/groove/src/ivm/runtime/mod.rs:516-557` and
   `crates/groove/src/ivm/runtime/recursion.rs:371-411`.
2. Multi-sink hydration invokes that path per terminal:
   `crates/groove/src/ivm/runtime/mod.rs:560-576,622-638`.
3. Binding adds late route filters rather than source bounds:
   `crates/groove/src/ivm/runtime/mod.rs:3361-3379`.
4. Jazz binds subscription values through a binding source:
   `crates/jazz/src/node/query_eval.rs:7971-8000,8067-8118`.
5. Policy authorization is compiled with an empty access-path map and then
   joined to the full protected base by `row_uuid`:
   `crates/jazz/src/node/query_eval.rs:5177-5193,5224-5253,8121-8162`.
6. `TopBy` arranges every input candidate before selecting the finite window:
   `crates/groove/src/ivm/runtime/mod.rs:5743-5825`.
7. Jazz schema metadata exposes independent indexed columns, while Groove
   already models composite indexes:
   `crates/jazz/src/schema.rs:649-674,873-895` and
   `crates/groove/src/schema.rs:500-540`.

Existing secondary-index selection is one-shot/global-only, rejects joins and
policy branches, and materializes results into inline records
(`query_eval.rs:4841-4975`). It is not a maintained bound source.

## H1: one lazy hydration session for all terminals

Replace eager `Vec<TableDelta>` construction with a per-hydration-session
source provider keyed by exact source identity:

```text
(table/index, scan bounds, descriptor, snapshot frontier)
```

- Evaluate terminal roots against one consistent storage cut.
- Load a source only when evaluator traversal misses memoized output.
- Reuse one materialized source snapshot across overlapping roots.
- Keep terminal descriptors separate; share only exact source payloads.
- Start with exact-range reuse. Range coalescing needs a separate proof.

Acceptance:

- five identical sinks perform one physical scan;
- a hydration memo hit performs zero physical reads;
- mixed terminals observe one logical frontier;
- scan-counting tests cover identical, overlapping, and disjoint sources.

This removes exact sink amplification but does not make the first binding
selective.

## H2: binding- and claim-aware maintained access paths

Add a maintained source descriptor whose bounds come from prepared binding
fields without embedding tenant values in shared graph identity.

- Recognize sargable equality prefixes from user params and trusted claims.
- Use existing global-current indexes first.
- Hydrate matching base records through the index.
- Apply identical bounds to old/new live deltas so team moves and filter
  transitions route correctly.
- Cache the plan by access-path shape; keep values in binding scope.
- Feed access paths into policy compilation as well as the root query.
- Keep a full-scan fallback and explain why it was chosen.

Representative paths:

- documents by `team_id`;
- membership by `(user_id, team_id)`;
- explicit ACL by `(user_id, document_id)` or the inverse selected by cost.

Acceptance:

- hot-team hydration reads no more than team cardinality;
- 32 active teams read the sum of their cardinalities, not
  `32 * total_documents`;
- identity/team routes cannot leak into one another;
- graph growth follows shape, not distinct tenant values;
- params, claims, nulls, local/global tiers, team moves, and revocation pass.

## H3: composite ordered indexes and bounded cursors

Add a resumable ordered source that can stop after enough
authorization/filter-qualified rows.

- Preserve `row_uuid` as canonical identity; use maintained secondary indexes.
- Add composite key direction/null-order metadata and optional included
  payloads only where justified.
- Add forward/reverse, continuation, limit, and clean early-stop to the storage
  cursor contract.
- Do not eagerly materialize the whole range as `InlineRecords`.
- Push only proven-compatible predicates. Authorization and residual filters
  must run before a candidate counts toward `offset + limit`.
- Retain a continuation boundary and refill from the index after deletion,
  revocation, team move, or rank change.

Initial benchmark candidate:

```text
(team_id ASC, archived ASC, updated_at ASC, row_uuid ASC)
```

Scan in reverse for descending update time and treat status as residual, or
cost multiple status ranges. This is an example, not a hard-coded planner rule.

Acceptance:

- hot-team Top-100 examines work near
  `100 / residual_selectivity`—188 candidates in the probe;
- deleting/revoking the 100th row immediately backfills the correct 101st;
- full-scan differential tests cover ties, nulls, offsets, pagination, all
  mutations, and policy changes;
- low-selectivity scans expose a cap/fallback rather than unbounded point seeks.

## H4: costed policy semijoins

Plan authorization from the smallest claim-bound relation when possible:

- point-check team membership/role/tenant state before opening a document
  range;
- choose authorized-ID-to-document or document-to-ACL probing by estimated
  cardinality;
- deduplicate authorization branches before `TopBy`;
- record cardinality, join direction, candidates visited, and policy rejects.

The authorization-correctness plan must land first; a faster multiplicity leak
is still wrong.

## Correctness invariants

1. Authorization and residual filters precede offset/limit.
2. Ordering is total and stable, ending in unique `row_uuid`.
3. Snapshot and live stream share one atomic frontier; no scan/subscribe gap.
4. Deletes, key changes, team moves, filter changes, and revocations retract
   and refill exactly.
5. Global indexes cannot answer local/open-transaction views without merging
   their overlays.
6. Base/index maintenance is atomic and stale index entries fail closed.
7. Policy branches have set semantics before the window.
8. Bounded state has a guaranteed refill path; a finite spill is only an
   optimization.

## Verification and landing

Use public black-box integration tests, scan-counting storage, and a randomized
full-scan differential oracle. Expose:

- physical rows/ranges read by source;
- hydration source cache hit/miss;
- access path and fallback reason;
- candidates rejected by residual filter/policy;
- continuation/refill work;
- retained arrangement and memo bytes.

Land H1 through H4 separately. Receipts should cover 1/32/1,000 bindings,
hot/cold tenants, policy allow/deny, low selectivity, and overlapping
terminals. Counter bounds, not latency, are the canonical gates.

Tooling friction: a checked-in scan-counting fixture with reusable large seed
data and bound-index cursors would avoid disposable harnesses and repeated
seeding.
