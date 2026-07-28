# SaaS read, write, Top-K, and correctness deep-dive receipt

Date: 2026-07-29

Baseline: `5a2aa0666` (`codex/saas-documents-benchmark`)

Status: directional developer-machine evidence and disposable prototypes, not
a performance promise or production fix.

## Environment and interpretation

All probes used `MemoryStorage`, optimized Rust builds where timing mattered,
and anonymized deterministic data on the same Apple M1 Max / 32 GiB machine as
`SAAS_PERMISSION_FANOUT_RECEIPT_20260728.md`.

Three evidence classes are intentionally distinguished:

1. **Integrated current-engine receipt.** Uses the checked-in public Jazz
   benchmark and independent subscription oracle.
2. **Disposable engine correction.** A minimal source change was compiled in an
   isolated worktree and exercised by the unchanged integrated harness.
3. **Disposable algorithm/graph probe.** Uses public Groove/Jazz APIs to test a
   proposed data path or relational rewrite. It establishes direction and
   invariants, not end-to-end production latency.

The disposable source programs were not added to this PR. Their complete
fixture, graph, iteration, and correctness contracts are recorded below; the
plans require checked-in counter-based regressions before any production
optimization lands.

## 1. Flush-once subscription refresh

### Current command

```sh
env \
  JAZZ_SAAS_PROFILE=baseline \
  JAZZ_SAAS_DOCUMENTS=2000 \
  JAZZ_SAAS_ORGANIZATIONS=1 \
  JAZZ_SAAS_TEAMS=1001 \
  JAZZ_SAAS_HOT_DOCUMENTS=1000 \
  JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=1 \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=1000 \
  JAZZ_SAAS_MATCHING_WRITES=3 \
  JAZZ_SAAS_UNRELATED_WRITES=3 \
  JAZZ_SAAS_BATCHED_WRITE_ROWS=100 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench \
    --bench saas_permission_fanout --quiet
```

Separate-write rows are means of three transactions. Batch, spread, and
boundary rows are one transaction.

### Disposable change

The isolated worktree made only this mechanism change:

```text
refresh_subscriptions_in:
  Groove flush once
  for every Jazz subscription:
    drain receiver without flushing again
```

Specifically, it added a `NodeState` method that calls `database.flush()`,
invoked it once immediately before the Jazz subscription loop, and removed the
same call from
`drain_local_maintained_view_subscription_transitions`. A trace-only timer
reported the shared flush and complete refresh duration. No benchmark, query,
policy, fixture, or oracle behavior changed.

### Result

| Write phase                              | Current mean | Prototype mean | Improvement |
| ---------------------------------------- | -----------: | -------------: | ----------: |
| Matching, separate one-row commits       |     37.671 s |     134.779 ms |      279.5x |
| Unsubscribed team, separate commits      |     37.557 s |     132.209 ms |      284.1x |
| 100 rows, matching team, one transaction |     37.845 s |     268.159 ms |      141.1x |
| 100 rows spread over subscribed teams    |     37.541 s |     270.978 ms |      138.5x |
| Matching team, below Top-100 boundary    |     37.767 s |     121.909 ms |      309.8x |

Every write phase reported `exact_oracle_match=true`. The nine prototype
refreshes measured:

- shared flush: 51.5–74.4 ms;
- complete refresh: 55.5–78.2 ms.

The prototype's overall JSON field is still `ok=false`. That field currently
aliases the separate one-shot ordering canary, and three later identity routes
still return zero in both current and prototype engines. The maintained-stream
oracle is exact; this optimization neither fixes nor hides the independent
one-shot correctness bug.

Hydration was not improved: the prototype still took 54.48 s to open 1,000
routes and reported approximately 2.36 GB of private maintained state.

At 100 routes, the same change reduced matching, unrelated, and boundary
commits from 202.8/203.7/237.1 ms to 7.04/5.50/7.32 ms. A 100-row batch fell
from 235.6 to 32.8 ms. All maintained phases remained exact.

## 2. Selective and bounded hydration

### Fixture

- 500,000 documents;
- 5,000 teams;
- one hot team with 30,000 documents;
- 4,998 teams with 94 documents and one with 188, preserving the exact total;
- query filters `archived=false` and status in active/draft, over fixture values
  that cycle archived/status;
- about 53.3% residual selectivity;
- `updated_at DESC, id DESC LIMIT 100`.

The probe seeded once, opened each candidate path against cloned
`MemoryStorage`, reset Groove's physical-read counters before every lane, and
asserted identical ordered IDs. Each table value is one optimized timed
hydration; read counts and exact IDs are the durable mechanism evidence.

### Paths

```text
current dynamic:
  full documents source
  JOIN runtime binding(team)
  TopBy(route_team, 100)

static prefix model:
  table scan with team prefix
  residual filters
  TopBy(100)

bounded ordered model:
  reverse predecessor walk over (team, updated_at, id)
  apply residual filters
  stop after 100 accepted rows
```

The bounded model used an ideal composite primary-key layout solely to exercise
the cursor contract. Production must keep Jazz `row_uuid` identity and provide
an ordered secondary source.

### Result

| Path                              |       Wall | Physical rows read |
| --------------------------------- | ---------: | -----------------: |
| Current dynamic binding, hot team | 267.315 ms |            500,000 |
| Warm second dynamic binding       | 195.624 ms |            500,000 |
| Static hot-team prefix            |  28.427 ms |             30,000 |
| Bounded reverse walk              |   0.315 ms |                188 |
| 32 retained dynamic bindings      |    7.729 s |         16,000,000 |
| 32 retained static prefixes       |  34.650 ms |             33,008 |

One versus five overlapping terminals read 500,000 versus 2,500,000 source
rows despite hydration-memo hits. At 32 routes:

- dynamic: 283,634 arrangement rows / 12.72 MB encoded and 35.99 MB memo;
- static prefixes: 17,601 arrangement rows / 0.70 MB and 2.09 MB memo.

The hot-team `TopBy(100)` retained all 16,000 qualifying candidates
(634,076 encoded bytes), not only the visible page. Byte estimates exclude
allocator/container overhead.

## 3. Maintained `TopBy` and route factoring

### Current operator curve

One public Groove `TopBy(100)` partition was hydrated, then received one timed
write. Every lane asserted the exact post-write window.

| Partition rows |     Winner | Boundary loser | 100-row transaction |
| -------------: | ---------: | -------------: | ------------------: |
|          1,000 |   0.857 ms |       0.841 ms |            1.193 ms |
|         10,000 |  10.002 ms |      10.124 ms |           10.363 ms |
|         30,000 |  35.416 ms |      35.265 ms |           35.489 ms |
|        100,000 | 138.997 ms |     138.767 ms |          138.120 ms |

A losing row and a 100-row same-partition batch pay almost the same
partition-sized rebuild as one winner.

With one shared 10,000-row `TopBy`, 1/10/100 identical sinks took
9.69/9.95/11.23 ms and retained the same 10,000 rows / 590 KB. Receivers are
not inherently the problem when their data partition is actually shared.

### Relational graph A/B

```text
current:
  TopK BY viewer_route (documents JOIN viewer_team_routes)

candidate:
  (TopK BY team documents) JOIN viewer_team_routes
```

At 10,000 documents and 100 viewers:

| Graph               | One-row write | Arrangement rows | Encoded bytes |
| ------------------- | ------------: | ---------------: | ------------: |
| Route before window |     985.22 ms |        1,010,100 |      67.59 MB |
| Route after window  |      10.08 ms |           10,200 |      0.598 MB |

The candidate was about 98x faster, retained 99x fewer rows, and used 113x
fewer encoded bytes. Repeated runs under concurrent compiler load remained
98–160x faster for a winner and 90–101x for a loser. A 100-row replacement was
14–18x faster because final delivery still scales with changed page rows times
viewers.

The A/B asserted exact initial Top-100 IDs, one-winner `-old/+new`, exact
100-row replacement, and silence for a loser.

### Standalone ordered-state model

The model compared two full sorts with
`BTreeMap<total_order_key, weight> + cached Top-100`. P50 uses 31 iterations for
one-row cases and 11 for 100-row cases:

| 100k-row change | Two sorts | Ordered state |
| --------------- | --------: | ------------: |
| One winner      |   4.95 ms |       1.33 us |
| One loser       |   5.01 ms |       0.21 us |
| 100 winners     |   5.05 ms |      16.38 us |
| 100 losers      |   5.13 ms |       8.83 us |

This model excludes Groove encoding, routing, storage, and delivery. It
validates `O(D log M + K)` structure, not an end-to-end latency forecast.

## 4. Correctness probes

All cases used public `jazz::db::Db` schema/write/query/subscription APIs in an
isolated clone.

### Existential authorization

With two documents and `LIMIT 2`, both of these return one unique row in
one-shot and subscription reset:

- the newest row matches membership and public/published branches;
- the reader has two valid membership rows for the same team.

Changing policy-only atom and authorization-boundary joins from inner to
route-aware existence semantics makes both minimal cases exact. It also makes
fresh 1,200-document public/admin policy tiers return exact `100 / 100`.
Partial duplicate-grant revoke remains quiet until the final proof disappears.

### Mixed identity and application parameter

Two identities, two memberships, two teams, and one document per team reuse:

```text
documents WHERE team = $team ORDER BY updated_at DESC, id DESC LIMIT 2
```

The first `(identity, team)` returns one; the second returns zero. Reversing
call order reverses the winner. Claim-only multi-identity and
same-identity/multi-team controls pass. Explicitly unsubscribing the first
one-shot Groove output immediately after `recv()` does not change the failure.

Trace evidence:

- the outer prepared parameter domain contains user `team`;
- its claim parameter domain is empty;
- the graph contains the first built-in `sub` UUID as a literal;
- both calls bind the same Groove `PreparedShapeId`.

Separating prepared identities is a diagnostic workaround, not the target.
Per `SPEC/14_lowering_to_groove.md`, `sub` must be a runtime claim binding/route
under a shape keyed by claim paths, not claim values.

Source tracing narrowed the propagation gap: normalized policy predicates keep
`sub` as a claim operand, but `binding_claim_params_for_shape` does not discover
that form before the outer team-only binding source is finalized.

### Dynamic claim revocation

After `isAdmin: true -> false`, a new one-shot is empty, but the existing live
stream emits no removal and later emits a newly inserted protected document.
Membership-row revoke/restore is an exact positive control.

### Lifecycle

Four one-shots grow 100 Groove outputs to 104. Dropping 99 live streams leaves
all 104 until a nonempty notification reaps the closed live receivers; the four
one-shot outputs persist.

Positive controls also pass for rank changes, deleting a winner, and moving a
document between team subscriptions.

## Next gates

Before any prototype becomes production code:

1. Check in each minimized public regression.
2. Add physical source-read, refresh-flush, `TopBy` rows-ranked, route-partition,
   and output-owner counters.
3. Convert A/B claims into full-scan/current-operator differential tests.
4. Use mechanism bounds in CI; keep wall-clock figures as receipts.
5. Run the canonical Rust, wire, oracle, incremental-delivery, smoke, and
   sensitive-data gates required by `AGENTS.md`.

Tooling friction: durable raw JSON capture and checked-in scan/plan probes would
make the next investigation reproducible without isolated source instrumentation.
