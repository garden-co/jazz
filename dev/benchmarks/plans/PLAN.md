# SaaS database scale plan

Status: benchmark evidence and disposable prototypes only. This plan set does
not change production engine behavior.

Baseline: `5a2aa0666` on 2026-07-29. Integrated receipts use the checked-in
`saas_permission_fanout` harness; algorithm and graph-rewrite A/Bs used
isolated, public-API probes against the same revision. Exact methods, sample
counts, caveats, and raw summaries are in
[`SAAS_DEEP_DIVE_RECEIPT_20260729.md`](../SAAS_DEEP_DIVE_RECEIPT_20260729.md).

## Workload and target

The target is a conventional multi-tenant SaaS workload:

- thousands of customers and teams;
- 500,000 to millions of documents;
- 100 to 30,000 documents per team;
- team, organization-admin, direct-ACL, public, and claim-based grants;
- `ORDER BY updated_at DESC, id ASC LIMIT 100`;
- hundreds to thousands of retained subscriptions;
- matching, unrelated, boundary-losing, permission, and batched writes.

Correctness means exact ordered pages and exact incremental deltas. Performance
means that work follows the active team, changed rows, and page size—not total
database rows multiplied by identities or subscriptions.

## Findings

| Priority    | Finding                                                                                      | Strongest evidence                                                                                                                                                      | Plan                                                           |
| ----------- | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| P0          | Authorization multiplicity reaches `TopBy`, although grants are existential                  | Two documents with `LIMIT 2` return one unique row when one document has two valid grant derivations; a disposable semijoin prototype restores exact results            | [Authorization correctness](authorization-correctness/PLAN.md) |
| P0          | A prepared graph embeds the first identity while using a value-independent shared shape name | In a two-identity/two-team query, the first one-shot route wins and the second returns zero; the outer binding descriptor contains `team` but omits runtime claim `sub` | [Authorization correctness](authorization-correctness/PLAN.md) |
| P0 security | Existing subscriptions retain revoked claims                                                 | After admin `true -> false`, one-shot is empty but the live stream keeps the old row and emits a later insert                                                           | [Claim rebinding](claim-rebinding/PLAN.md)                     |
| P1          | Every Jazz subscription causes another Groove empty tick after each write                    | At 1,000 subscriptions, unrelated writes fall from 37.55 s to 132 ms when a disposable prototype flushes once, with exact stream results                                | [Subscription refresh](subscription-refresh/PLAN.md)           |
| P1          | Initial binding hydration scans far beyond the requested tenant/page                         | On a 500k-row probe, current dynamic binding reads 500k rows; a bounded team/order cursor reads 188 and is 849x faster                                                  | [Selective hydration](selective-hydration/PLAN.md)             |
| P1          | Maintained `TopBy` sorts whole partitions and route expansion duplicates them per viewer     | At 100k rows, winner and loser both cost about 139 ms; factoring a 10k-row team page across 100 viewers is 98x faster and retains 99x fewer rows                        | [Maintained TopBy](maintained-topby/PLAN.md)                   |
| P1          | Local stream drop and one-shot completion do not detach Groove outputs                       | Four reads grow 100 outputs to 104; dropping 99 streams leaves all 104 until a relevant nonempty delta reaps some of them                                               | [Subscription lifecycle](subscription-lifecycle/PLAN.md)       |

The positive controls matter: membership revoke/restore, rank changes, delete,
and moving a document between teams all produced exact live deltas in the
minimized probes. The failures are bounded mechanisms, not a claim that every
incremental update is incorrect.

## Recommended order

1. Land existential authorization semantics before broadening policy
   optimization. It fixes an exact-page correctness bug and is a prerequisite
   for safe policy-branch factoring.
2. Parameterize every authorization claim in shared prepared graphs, then add
   claim revision invalidation/rebinding. The latter is a live revocation
   security issue.
3. Flush Groove once per refresh cycle. It is a small, independently verified
   change with the largest immediate write-latency reduction.
4. Add deterministic local/one-shot unsubscribe. This bounds stale work before
   optimizing retained state.
5. Add selective, ordered Local hydration and expose access-path work counters.
6. Replace whole-bucket `TopBy` rebuilding with maintained ordered state.
7. After authorization deduplication is proven, factor partition-constant team
   pages before viewer routing while retaining a row-ACL fallback.

Items 1–4 should be separate reviewable pull requests. Items 5–7 need staged
implementation behind differential oracles because they change query planning
and maintained operator state.

## Shared invariants

Every implementation plan must preserve:

1. Exact one-shot and maintained results for every identity, parameter binding,
   and read view.
2. Existential authorization: multiple valid proofs grant a row once.
3. Total ordering: declared directions, ascending tie fields, then encoded row
   bytes; no arrival-order dependence.
4. One consolidated subscription event per completed tick, with no event for a
   page-neutral write.
5. Revocation is fail-closed for both existing rows and later writes.
6. State is released after the final reader/stream/route disappears.
7. Performance assertions use rows examined, partitions touched, sorts,
   refresh ticks, and retained bytes. Wall-clock values remain receipts rather
   than CI thresholds.

## Scale acceptance scenario

The final cross-plan gate should exercise:

- at least 2 million documents, 15,000 teams, 5,000 organizations, 900,000 team
  memberships, and 100,000 direct ACL rows;
- 1,000 simultaneous routes with mixed authorization paths;
- a 30,000-document hot team with at least 200 viewers;
- one matching insert, one unrelated insert, one losing insert, a 100-row
  same-team batch, and a 100-team spread batch;
- membership and claim revoke/restore while streams stay open;
- repeated one-shot reads and stream churn.

The gate should check exact pages/deltas and bounded work counters. A customer
fixture, schema, identifier, or domain must never enter the public repository.

Tooling friction: a stable plan dump plus per-operator rows-read, retained-state,
tick, and delivery counters would replace most disposable source probes.
