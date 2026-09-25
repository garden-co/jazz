# Authority-served SaaS first pages (2026-09-25)

## Why ship this

A cold, permissioned Global one-shot read used to settle a receiver-side
maintained graph and supporting row history before returning a small page.
For eligible serialized reads, the admitted Core now evaluates and hydrates the
page under the admitted identity and claims, then sends the binding-encoded
result directly. A client with pending local writes uses the existing coverage
path. Unsupported queries and older peers use that path too. Live subscriptions,
history, deletions, and local-write semantics are unchanged.

On the anonymized SaaS fixture with no deleted rows, the 39-query, 100-row-per-
query relay workload drops from a median **1,679 ms to 502 ms (3.34×)** over
three matched runs on main after #3352 merged. All 879 returned row IDs match a separate authority
evaluation. The authority route also matches its exact binding-encoded bytes.

## Measurement

Initial base: `origin/main` at `c0537a9a9`. Native Cargo `perf` profile with
`cold-settle-attribution`, identity `member`, RocksDB, and the same seeded
fixture and session-aware transport. Each run independently copies the seeded
Core store and evaluates the same public queries before the read timer; the
control checks exact page row IDs, while the candidate checks exact encoded
result bytes. Seeding, copy, and oracle evaluation are excluded. The control
binary has only the authority-route eligibility condition forced false; query
shape, permission checks, transport, and instrumentation are otherwise the
same. Candidate SHA-256: `c401ec451421d1491c9e42a3067c47b927ef7b6ca288ecb76a152da1b926b1c0`.
Control SHA-256: `94eb5dd3098f946fd29e4e755ed438e8b8b518bb1188e2ef088791b946341512`.

| Global one-shot workload                |  Control | Authority result | Speedup |
| --------------------------------------- | -------: | ---------------: | ------: |
| One child page, 100 rows, direct Core   |   642 ms |           318 ms |   2.02× |
| One child page, 100 rows, through relay |   689 ms |           356 ms |   1.94× |
| 39 pages, 879 rows, direct Core         | 1,317 ms |           449 ms |   2.93× |
| 39 pages, 879 rows, through relay       | 1,645 ms |           488 ms |   3.37× |

The table uses the first saved matched run for each case. In two additional
39-page relay runs, control/candidate times were 1,635/492 ms and
1,627/492 ms. The median ratio is 3.32×. The JSON receipts are in
[`saas-authority-page-20260925`](saas-authority-page-20260925/).

After #3352 and #3409 merged into main at `a6925a903`, the matched 39-page
relay times were **1,645/506, 1,679/496, and 1,708/502 ms**. Medians are
**1,679/502 ms (3.34×)**. The fixture declares no composite indexes. This
candidate/control pair has SHA-256
`fb097d6da4df7f2a82f771c009d0ebb35f749e496ba107d4cdf101b1ee54509e` /
`481d28490665a8c169296d3348c41eeab563bb2ae79bee69ac37462f22dcf979`.
Receipts are prefixed `latest-main-` in the same directory.

Set `JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold
JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 JAZZ_CUSTOMER_CLIENT_ONESHOT=1
JAZZ_CUSTOMER_QUERY_LIMIT=100 JAZZ_CUSTOMER_MAX_TICKS=200000`; add
`JAZZ_CUSTOMER_EXPECT_REMOTE_READ=1` for the candidate assertion. Add
`JAZZ_CUSTOMER_DIRECT_CORE=1` for direct topology, and
`JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3` for the child-only case. The binary is
`target/perf/permissioned-resources-profile` after building the example
benchmark with `--profile perf --features cold-settle-attribution`.

## Limits and next win

The new route covers flat current Global reads with a limit of 1–1,000, a
query envelope at most 32 KiB, and a result at most 1 MiB. It does not yet
help the unbounded 39-query read: the same fixture still takes **6,912 ms**
through the relay on `a6925a903` for 27,518 rows, with exact authorized row IDs checked.
That is the next independent performance target. The current change does not
measure or claim a live-subscription opening speedup.

Tooling friction: the nested worktree requires package-specific `cargo fmt`;
the documented `dev/benchmarks/smoke.sh` moved to
`dev/gates/benchmark-smoke.sh`, the private guard is unavailable here, and
the optimized benchmark relinks for about two minutes per binary.
