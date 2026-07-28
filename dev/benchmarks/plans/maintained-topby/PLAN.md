# Plan: scale maintained `TopBy` and shared team pages

Status: design plus disposable exact-result A/B probes. No production worktree
change.

## Problem

For a finite page, current `TopBy` state is an unordered hash bucket. A touched
partition is cloned, every sort key is encoded, and the whole bucket is sorted
twice: once for the after-window and once for reconstructed before-state. A row
known to lose the Top-100 boundary therefore costs almost the same as a winner.

Jazz also expands binding/viewer routes before the window and includes route
fields in the `TopBy` group. One team-wide page is then maintained once per
viewer even when every viewer has the identical authorized row set.

## Source

Whole-partition rebuild:

- `crates/groove/src/ivm/runtime/join.rs:33-37,459-465` stores unordered
  `key -> HashMap<record, weight>` buckets and clones positive records.
- `crates/groove/src/ivm/runtime/mod.rs:5792-5821` materializes touched buckets
  and derives before/after windows.
- `runtime/mod.rs:7866-7884` encodes/sorts every positive row.
- `runtime/mod.rs:7914-7930` reconstructs before-state and sorts again.
- `runtime/mod.rs:7933-7948,7975-7985` repeatedly allocates/encodes sort keys.

Route multiplication:

- `crates/jazz/src/node/query_engine/lowering.rs:3757-3821` lowers
  param/claim equality through a synthetic route.
- `lowering.rs:2998-3005` passes routed data to window lowering.
- `lowering.rs:4010-4015` appends all route fields to the `TopBy` group.
- `crates/groove/src/ivm/runtime/mod.rs:3361-3379` installs bound filters after
  the shared routed terminal.

Identical graph nodes already share work through
`crates/groove/src/ivm/graph.rs:601-618` and per-tick evaluator memoization
(`runtime/mod.rs:5003-5025`). The issue is route-expanded data, not the mere
receiver count.

The intended design already appears in
`crates/groove/SPEC/3_queries_operators.md:206-213,239-243`: a maintained
ordered index per partition, not permission to rescan on every delta.

## Evidence

All disposable probes used `MemoryStorage`, optimized builds, and exact ID/delta
assertions. Engine tables report one timed write after hydration per seeded
scenario; repeated route-rewrite runs are shown as a range. The standalone
algorithm table reports p50 from 31 one-row or 11 batch iterations.

### Partition cardinality dominates

One route, `LIMIT 100`:

|    Rows | Winning insert | Losing insert | 100-row transaction |
| ------: | -------------: | ------------: | ------------------: |
|   1,000 |       0.857 ms |      0.841 ms |            1.193 ms |
|  10,000 |      10.002 ms |     10.124 ms |           10.363 ms |
|  30,000 |      35.416 ms |     35.265 ms |           35.489 ms |
| 100,000 |     138.997 ms |    138.767 ms |          138.120 ms |

A guaranteed loser has no boundary shortcut. One and 100 same-partition rows
cost nearly the same because the two full sorts dominate.

### Identical sinks already share

One shared 10k-row `TopBy` with 1, 10, and 100 identical sinks took 9.69,
9.95, and 11.23 ms and retained the same 10,000 rows / 590 KB. Delivery fan-out
is modest when the data partition is genuinely shared.

### Route-before versus route-after

The probe compared:

```text
current:
  TopK BY viewer_route (documents JOIN viewer_team_routes)

candidate:
  (TopK BY team documents) JOIN viewer_team_routes
```

At 10,000 team documents and 100 viewers:

| Graph                | One-row write | Arrangement rows | Encoded bytes |
| -------------------- | ------------: | ---------------: | ------------: |
| Route before `TopBy` |     985.22 ms |        1,010,100 |      67.59 MB |
| Route after `TopBy`  |      10.08 ms |           10,200 |      0.598 MB |

The candidate is about 98x faster, retains 99x fewer rows, and uses 113x fewer
encoded bytes. Repeated runs were 98–160x faster; below-boundary writes were
90–101x. A 100-row page replacement remained 14–18x faster but still paid the
unavoidable `200 page deltas * viewers` delivery cost.

Exact initial Top-100 IDs, winner `-old/+new`, 100-row replacement, and silent
loser behavior matched between graph shapes.

### Ordered-state algorithm prototype

At 100,000 rows, a disposable `BTreeMap<total_key, weight>` plus cached Top-100
compared with the current two-sort shape:

| Change      | Two sorts | Ordered state |
| ----------- | --------: | ------------: |
| One winner  |   4.95 ms |       1.33 us |
| One loser   |   5.01 ms |       0.21 us |
| 100 winners |   5.05 ms |      16.38 us |
| 100 losers  |   5.13 ms |       8.83 us |

This excludes Groove routing, encoding, and notification overhead; it proves
the intended complexity direction, not production latency.

## T1: persistent ordered multiset plus cached window

Add shareable ordered operator state:

```text
OrderedArrangementKey {
  scope,
  input_node,
  group_fields,
  order_fields_and_directions,
  tie_fields,
  descriptor,
}

partition -> OrderedMultiset<total_key, positive_weight>
partition -> cached_prefix_or_window
```

The total order must remain:

```text
declared order fields,
tie fields ascending,
full encoded record bytes ascending
```

Own encoded record bytes once. Precompute the total key when a delta arrives;
never decode and encode every row on every tick.

For each touched partition:

1. Snapshot the previous cached prefix/window.
2. Consolidate and apply the complete tick's deltas to ordered state.
3. Rebuild only the required prefix.
4. Diff before/after windows into one minimal consolidated delta.
5. Publish the cache only after the complete batch.

Target for finite `offset=0, limit=K`:

```text
O(D log M + K)
```

where `D` is changed distinct records and `M` is partition cardinality.

For finite offsets, cache/iterate through `offset + limit`. For an unbounded
suffix, derive `output = input - first_offset`; an unbounded zero-offset window
can be an identity path. Large offsets ultimately need order-statistic subtree
counts. Until then, expose ordinals examined.

## T2: optional boundary-loser shortcut

If ordered state lands in stages, persist the exact current boundary and skip
materialization when:

1. a finite prefix is full;
2. every changed record's exact total key is strictly worse;
3. old/new copy counts remain on that side;
4. no record changes into another group/route.

Equality is unsafe under bag semantics. Underfilled/unbounded windows,
prefix retractions, and uncertain updates retain the current fallback. This
milestone is optional if T1 can land directly.

## T3: proof-gated active-partition factoring

Do not blindly move windows before policies: that can maintain every tenant and
is wrong for row-specific ACLs.

For a proven partition-constant grant:

```text
authorized_routes(identity, team)
  -> distinct/refcounted active_team(team)

filtered_documents
  SEMI JOIN active_team
  -> TopK BY team
  -> JOIN authorized_routes
  -> bound route delivery
```

The first viewer activates one team page; later viewers reuse it; the final
viewer removal tears it down. State becomes roughly:

```text
documents in active distinct teams + viewers * K
```

instead of `documents * viewers`.

Required proof:

- grant is set-semantic per `(route, partition)`;
- the branch grants every row of the partition;
- row filters/order/ties/page do not reference route/identity;
- partition key is preserved;
- no row-level ACL/reachable predicate remains below the movement.

Safe initial candidates are team membership, organization/admin expanded to
teams, and viewer-independent public branches. Direct document ACL, owner-only,
identity-dependent predicates, route-dependent order, and multiplicity-varying
joins must use the current fallback.

Mixed policies may factor safe branches and retain private candidates, but must
deduplicate authorization before the final per-route window. The authorization
correctness plan is therefore a prerequisite.

## Implementation

1. Add metrics:
   `top_by_partitions_touched`, `top_by_index_updates`,
   `top_by_prefix_ordinals_examined`, `top_by_full_bucket_rows_ranked`,
   `top_by_boundary_short_circuits`, and `top_by_key_bytes_encoded`.
2. Keep current sorting as a differential reference.
3. Land finite offset-zero ordered state with bulk hydration and exact state
   ownership/GC.
4. Extend to offsets and unbounded suffixes.
5. Remove the full-sort path after differential soak.
6. Add an analyzer result such as
   `PartitionConstantAuthorization { partition_fields, route_graph, proof }`.
7. Enable route factoring one policy class at a time; otherwise fall back.

## Invariants and gates

Preserve:

- weighted bag ordinals and exact boundary straddling;
- nullable/mixed direction keys and total tie order;
- `-old,+new` changes across order and partition keys;
- complete-batch-before-diff and net-zero silence;
- accumulate/replace modes and scope ordering;
- one minimal tick diff;
- state release after the last retainer;
- route isolation whenever no factoring proof exists.

Public black-box/differential tests must cover insert/delete/update on both
boundaries, equal keys, multiplicity, finite/unbounded windows, large offsets,
100-row/net-zero batches, partition moves, hydration, rebinding, and GC.

Route gates cover 1/10/100 viewers, two teams, duplicate membership derivations,
grant/revoke, final-viewer teardown, organization-wide access, direct-ACL
fallback, and mixed shared/private candidates.

Mechanism acceptance:

- a 100k-row losing insert ranks zero existing rows after hydration;
- one winner examines `O(K + D)`, not 100k;
- 100 rows in one partition cause one partition diff;
- 100 viewers maintain one document page partition;
- a second viewer does not rehydrate the team's full document set.

Tooling friction: per-operator timers/counters and a plan printer distinguishing
binding routes from shareable data partitions would eliminate disposable graph
probes.
