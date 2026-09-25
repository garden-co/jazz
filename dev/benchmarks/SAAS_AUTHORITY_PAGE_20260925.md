# Authority-served SaaS first pages (2026-09-25)

## Why ship this

A cold, permissioned Global one-shot read settles a receiver-side maintained
graph and supporting row history before returning a small page. With the
explicit `resultOnly: true` option, the admitted Core evaluates and hydrates
the page under the admitted identity and claims, then sends the binding-encoded
result directly. This result does not populate the receiver's offline cache.
Ordinary reads continue to materialize local data. A client with pending local
writes, unsupported queries, and older peers use the coverage path. Live
subscriptions, history, and deletions are unchanged.

On the anonymized SaaS fixture with no deleted rows, the 39-query, 100-row-per-
query relay workload drops from a median **1,549 ms to 475 ms (3.26×)** over
three matched runs on main `f629fa7a9`. All 879 returned row IDs match a
separate authority evaluation. The authority route also matches its exact
binding-encoded bytes.

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

After #3451 (ordered pages), #3354 (incremental attach), and the bulk ingest
changes merged into main `5b6417fe5`, the same opt-in source was rebased and
rerun through the relay. The control differs only by disabling the authority
route in the read eligibility condition:

| 39 pages, 879 rows | Run 1 | Run 2 | Run 3 | Median |
| ------------------ | ----: | ----: | ----: | -----: |
| Receiver coverage  | 1,634 | 1,562 | 1,594 |  1,594 |
| Authority result   |   498 |   483 |   482 |    483 |

The median speedup is **3.30×**. Candidate/control binary SHA-256 values are
`4e9a2f1729390b17c7a2540147c9c813c520f6dd941006e2afe10577be1242f7` /
`2cc3e71e29112e6ed2bf6dd566f47768f517f9e93ecda689ac6ef25d1a973d8b`.
The six `main-5b6417-` receipts record row counts and phase timings.

On main `f629fa7a9`, which also contains the merged subscription order-scan
and per-tick memo/state-scan fixes (#3452 and #3460), the matched relay runs
were:

| 39 pages, 879 rows | Run 1 | Run 2 | Run 3 | Median |
| ------------------ | ----: | ----: | ----: | -----: |
| Receiver coverage  | 1,523 | 1,549 | 1,612 |  1,549 |
| Authority result   |   481 |   473 |   475 |    475 |

The median gain is **3.26×**. Both arms request `resultOnly: true`; the
control disables only the authority eligibility check. The candidate binary
includes stacked #3489, whose unbounded path is not exercised by these
100-row-limited queries. Candidate/control SHA-256 values are
`3042a1514bab3ff67cd100c7c4191200d42c5a2deacbae96f14deac8ce38d21c` /
`c77fefb0a9c1130515e7914ab7c2d3151dfc31cf22ced08f37d73ea1d4b430f8`.
The six `main-f629fa7-` receipts contain row counts, bytes, and phases.

On main `c5e405fa0`, after the merged projection, subscription root-position,
SQLite, wire-encoding, and transaction-read changes, the stack was rebased
without conflicts. A new matched native `perf` A/B used three alternating
control/candidate runs. Both arms requested `resultOnly: true`; only the
authority eligibility check was disabled in the control:

| 39 pages, 879 rows | Run 1 | Run 2 | Run 3 | Median |
| ------------------ | ----: | ----: | ----: | -----: |
| Receiver coverage  | 1,562 | 1,609 | 1,543 |  1,562 |
| Authority result   |   552 |   487 |   482 |    487 |

The median gain is **3.21×**. All row and binding-byte oracle assertions pass;
each run reports 368,989 total encoded result bytes. Candidate/control binary
SHA-256 values are
`c8bd9c2b95689004635504d2f20fc7f27f4da5974fa56dc14546db7242f31267` /
`586300a66164044d7857487f168d165e9168f00ff19624e2431078b51d0e8093`.
The six `main-c5e405f-` receipts record rows, bytes, and phase timings. The
candidate binary includes stacked #3489, whose unbounded path is not exercised
by these limited queries. The ordinary Global and live subscription paths do
not request result-only delivery and are unchanged by this branch.

Set `JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold
JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 JAZZ_CUSTOMER_CLIENT_ONESHOT=1
JAZZ_CUSTOMER_QUERY_LIMIT=100 JAZZ_CUSTOMER_MAX_TICKS=200000`; add
`JAZZ_CUSTOMER_EXPECT_REMOTE_READ=1` for the candidate assertion. Add
`JAZZ_CUSTOMER_DIRECT_CORE=1` for direct topology, and
`JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3` for the child-only case. The binary is
`target/perf/permissioned-resources-profile` after building the example
benchmark with `--profile perf --features cold-settle-attribution`.
The benchmark requests result-only delivery in both arms; the matched control
temporarily disables its authority route and uses coverage instead.

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
