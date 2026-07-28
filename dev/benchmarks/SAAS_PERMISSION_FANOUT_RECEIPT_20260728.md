# SaaS permission and subscription fan-out benchmark receipt

Date: 2026-07-28

Status: directional developer-machine results, not a performance promise.

## Workload

The new public-API harness is
`crates/jazz-tools/benches/saas_permission_fanout.rs`. It models:

- 5,000 organizations and 15,000 teams at the full permission scale;
- 2,000,000 documents;
- 600,000 team memberships and 300,000 organization memberships;
- 100,000 direct document ACL rows;
- active team membership, organization-admin, and direct-ACL policy branches;
- opt-in public/published and trusted-admin branches;
- a parameterized current-team query with `archived = false`,
  `status IN (active, draft)`, `updated_at DESC, id DESC`, and `LIMIT 100`;
- one-row matching and unrelated writes, a 100-row hot-team transaction, a
  transaction spread across subscribed teams, and a below-Top-100 no-op.

Every initial subscription reset and later add/remove delta is checked for
exact membership against an independent deterministic oracle. Sampled
one-shot reads separately check ordering. Commit timing excludes stream
draining and oracle work. The harness reports the primary Groove commit
storage/IVM timers separately from the remaining Jazz facade, repeated empty
ticks, and subscription-refresh time.

Primary command:

```sh
cargo bench --profile perf -p jazz-tools \
  --features saas-permission-bench \
  --bench saas_permission_fanout --quiet
```

All results below use `MemoryStorage`, Local reads, Rust 1.93.1, and
`aarch64-apple-darwin` on an Apple M1 Max (10 cores, 32 GiB), macOS 15.6.1.
The working tree was based on `745b51fba`; the benchmark source is included in
this change.

These are single end-to-end runs with no warm-up. Hydration has one sample per
binding. Separate-write medians contain three samples except the 5,000-team
single-binding lane, which contains five; batch and boundary figures are one
transaction each. Treat the results as directional, not as stable latency
percentiles.

## Reproducing the lanes

Each shell block below is a complete command.

The distinct-route curve repeated this command with active subscriptions set
to 1, 10, 100, and 1,000:

```sh
env \
  JAZZ_SAAS_DOCUMENTS=2000 \
  JAZZ_SAAS_ORGANIZATIONS=1 \
  JAZZ_SAAS_TEAMS=1001 \
  JAZZ_SAAS_HOT_DOCUMENTS=1000 \
  JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=1 \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=100 \
  JAZZ_SAAS_MATCHING_WRITES=3 \
  JAZZ_SAAS_UNRELATED_WRITES=3 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench \
    --bench saas_permission_fanout --quiet
```

The exact 5,000-team baseline used the source defaults, changing only
`JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS` to `1` or `10`; the 10-binding run also set
both separate-write counts to `3`. The hot-customer lane used:

```sh
env \
  JAZZ_SAAS_TOPOLOGY=hot_team \
  JAZZ_SAAS_DOCUMENTS=30200 \
  JAZZ_SAAS_ORGANIZATIONS=1 \
  JAZZ_SAAS_TEAMS=2 \
  JAZZ_SAAS_HOT_DOCUMENTS=30000 \
  JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=200 \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=200 \
  JAZZ_SAAS_MATCHING_WRITES=3 \
  JAZZ_SAAS_UNRELATED_WRITES=3 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench \
    --bench saas_permission_fanout --quiet
```

The 2M-document permission run used
`JAZZ_SAAS_PROFILE=real_world`. The 100-route permission run additionally set
10,000 documents, 100 organizations, 1,001 teams, 5,000 hot-team documents,
five team and organization members per scope, 10,000 direct ACL rows, and 100
active subscriptions.

The churn run used the distinct-route configuration with 2,000 documents, 101
organizations/teams, 100 active subscriptions, 99 drops, one matching and
unrelated write, and a 10-row batch:

```sh
env \
  JAZZ_SAAS_DOCUMENTS=2000 \
  JAZZ_SAAS_ORGANIZATIONS=101 \
  JAZZ_SAAS_TEAMS=101 \
  JAZZ_SAAS_HOT_DOCUMENTS=1000 \
  JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=1 \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=100 \
  JAZZ_SAAS_DROP_SUBSCRIPTIONS=99 \
  JAZZ_SAAS_MATCHING_WRITES=1 \
  JAZZ_SAAS_UNRELATED_WRITES=1 \
  JAZZ_SAAS_BATCHED_WRITE_ROWS=10 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench \
    --bench saas_permission_fanout --quiet
```

## Distinct customer-route fan-out

This curve intentionally fixes the small source at 2,000 documents, 1,001
teams, and 1,001 memberships. Team 0 has 1,000 documents; the other teams have
one each. It isolates active-route control cost and does **not** claim that
every route has a full page.

| Active routes | Total hydration | Median subscribe | Matching one-row commit | Unrelated one-row commit | Matching 100-row transaction | Approx. local subscription footprint |
| ------------: | --------------: | ---------------: | ----------------------: | -----------------------: | ---------------------------: | -----------------------------------: |
|             1 |         0.116 s |         116.2 ms |                 1.31 ms |                  0.33 ms |                     14.76 ms |                              5.68 MB |
|            10 |         0.361 s |          26.9 ms |                 3.60 ms |                  2.75 ms |                     19.58 ms |                             26.87 MB |
|           100 |         2.910 s |          28.1 ms |               194.02 ms |                189.25 ms |                    231.68 ms |                            238.77 MB |
|         1,000 |        49.820 s |          49.0 ms |                 37.72 s |                  37.55 s |                      37.84 s |                              2.36 GB |

At 1,000 routes, the unrelated write's primary commit tick spent 43 ms in
Groove IVM; about 37.51 s landed in the residual, which includes the repeated
Groove empty ticks invoked by Jazz subscription refresh. It emitted no client
event. Matching and unrelated writes therefore have essentially the same
pathological cost.

The 5,000-route lane was deliberately not run on this 32 GiB host. The
1,000-route result is already operationally unusable, and source inspection
shows an N-by-N empty-tick path described in the findings note. Running 5,000
would add OOM risk without changing that conclusion.

## Full-row hydration

The exact 5,000-team, 529,900-row baseline keeps one 30,000-document team and
100 documents in every other team.

| Active routes |    Seed | Total hydration | Median binding hydration | Matching write | Unrelated write | 100-row transaction | Approx. local subscription footprint |
| ------------: | ------: | --------------: | -----------------------: | -------------: | --------------: | ------------------: | -----------------------------------: |
|             1 | 37.73 s |         11.84 s |                  11.84 s |       33.28 ms |         0.34 ms |            48.23 ms |                            121.39 MB |
|            10 | 37.15 s |        118.00 s |                  11.82 s |       36.52 ms |         2.89 ms |            53.97 ms |                            335.00 MB |

The tenth binding is not cheap: it still takes 11.82 s. A shared shape does
not currently avoid binding-local full-source hydration.

The full real-world permission fixture contained 3,020,000 rows across its six
tables:

| Phase                    |          Rows |         Time |
| ------------------------ | ------------: | -----------: |
| Organizations            |         5,000 |       0.21 s |
| Teams                    |        15,000 |       0.63 s |
| Team memberships         |       600,000 |      28.19 s |
| Organization memberships |       300,000 |      14.16 s |
| Documents                |     2,000,000 |     146.11 s |
| Direct ACLs              |       100,000 |       4.82 s |
| **Total seed**           | **3,020,000** | **194.12 s** |

Three policy bindings—team member, organization admin, and direct ACL—then
took 66.75 s, 63.13 s, and 61.45 s to hydrate. Total hydration was 191.33 s.
Their exact initial membership and every add/remove delta passed.

With only three active subscriptions, a matching write was 35.11 ms, an
unrelated write was 0.85 ms, and a matching 100-row transaction was 54.32 ms.
The three local subscriptions' private maintained/control structures had an
approximate 240 MB footprint. Shared Groove arrangements had a separate
5.47 GB encoded-size estimate.

## One hot customer with 200 viewers

This lane used 30,200 documents, one 30,000-document hot team, and 200 distinct
member identities subscribed to that same team.

| Metric                                   |   Result |
| ---------------------------------------- | -------: |
| Total hydration                          | 250.32 s |
| Median hydration per viewer              |  1.252 s |
| Approx. local subscription footprint     | 19.62 GB |
| Matching one-row write                   |   8.23 s |
| Matching write: initial Groove IVM tick  |   7.16 s |
| Unrelated-team one-row write             |   1.18 s |
| Unrelated write: initial Groove IVM tick |   8.9 ms |
| Matching 100-row transaction             |  10.20 s |
| Below-Top-100 write, no client event     |   8.34 s |

All 200 matching subscriptions received the exact add/remove delta. The
unrelated write notified none. The below-boundary write changed no client
snapshot, yet still paid nearly the full matching maintenance cost. The
19.62 GB value is a structural estimate for private local-subscription state,
not process RSS; it excludes a separate 5.73 GB Groove arrangement
encoded-size estimate, storage, queues, and allocator overhead.

## Permission complexity

A 1,200-document canary added policy alternatives cumulatively:

| Policy tier          | Initial hydration | Exact Top-100 membership         |
| -------------------- | ----------------: | -------------------------------- |
| Team membership      |           79.7 ms | Pass                             |
| + organization admin |           88.9 ms | Pass                             |
| + direct ACL         |           96.7 ms | Pass                             |
| + public/published   |                 — | **Fail: 99 rows instead of 100** |
| + trusted admin      |                 — | **Blocked by the same failure**  |

The public-branch failure is deterministic with one subscription:

```text
actual=99, expected=100, one boundary row missing
```

Consequently the scale receipts use the first three policy branches. A
combined 100-route RBAC/ACL lane with 10,000 documents, 5,005 team
memberships, 500 organization memberships, and 10,000 ACL rows produced:

- 9.40 s total hydration, 90.5 ms median per binding;
- 268.15 ms matching writes;
- 256.17 ms unrelated writes;
- 296.18 ms for a matching 100-row transaction;
- 281 MB approximate private local-subscription footprint;
- exact initial and incremental subscription membership.

This is a route-control lane: its first team has 5,000 documents, while most
other teams have only five. The 100 subscriptions returned 471 rows in total,
not 100 full pages.

The tier ladder used the same 1,200-document fixture for each run: two teams
and organizations, 1,000 hot-team documents, five members per scope, 100
direct ACL rows, one subscription, no separate writes, and no batch. Only
`JAZZ_SAAS_PERMISSION_BRANCHES=<1..5>` changed.

## Subscription and one-shot lifecycle

The 100-route churn lane exposed two distinct stale-output paths:

- before sampled one-shot reads: Jazz/Groove both had 100 live outputs;
- four sampled `all_for_identity` reads raised Groove to 104 while Jazz
  remained at 100;
- dropping 99 live streams left Jazz at 1 and Groove at 104;
- updating an organization unused by every document subscriber took
  158.15 ms, emitted 100 notifications / 400 records in the initial Groove
  commit tick but no client membership change, and reaped the 99 deliberately
  dropped outputs;
- Groove still retained the four one-shot outputs;
- a later unbound-team document write took 4.58 ms and did not reap them.

Prepared-shape metadata increased from 200 to 205 during the four one-shot
reads and remained at 205 after churn cleanup. This lane does not prove an
unbounded process-memory leak, but it does prove that local stream drops and
one-shot reads do not synchronously detach their Groove outputs.

## Batching

The refresh cost is mostly per transaction, not per inserted row:

- at 1,000 routes: one row was 37.72 s; 100 rows in one transaction were
  37.84 s;
- at 200 hot-team viewers: one row was 8.23 s; 100 rows in one transaction
  were 10.20 s;
- at one full 529,900-row binding: one row was 33.28 ms; 100 rows were
  48.23 ms.

At 1,000 routes, 100 separate matching commits would be roughly 63 minutes if
the measured per-commit median held. The measured one-transaction result is
37.84 s.

## Correctness receipts

- All benchmarked subscription reset membership and document add/remove
  deltas passed the independent oracle through team, organization-admin, and
  direct-ACL policy branches.
- Public/published as an additional OR branch fails the initial Top-100
  membership canary.
- With multiple policy-bound routes, later sampled `all_for_identity` one-shot
  reads returned zero rows for non-first routes while the corresponding live
  subscriptions remained exact. This is a separate correctness issue and is
  reported rather than used as a performance oracle.

## Scope boundaries

The lanes deliberately separate dimensions: the 2M-document run has three
active subscriptions, the 100-route permission run has 10,000 documents, and
the 1,000-route control run has 2,000 documents. No result combines every
maximum.

The policy fixture measures hydration and document writes through complex read
policies. It does not yet time membership/role revocation, ACL grant/revoke,
organization or team suspension, document moves, reconnect, or one user
belonging to many teams.

Tooling friction: a direct timer around `Db::refresh_subscriptions` and
phase-progress output during long benchmark runs would shorten attribution
work.
