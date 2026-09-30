# Bound flat Global pages before policy evaluation

## Decision

Ship the bounded UUID page probe. A one-shot trusted Global read of a flat
`limit(100)` query scanned 43,000 physical rows to return 100. The query's
default order is UUID order, which is the physical current-table key order
within the Global branch. The probe reads only the first 101 physical
candidates, applies the existing deletion and policy graph, and accepts the
page only after it proves that later candidates cannot enter it. It widens
twice for sparse visibility, then uses the complete read path.

Groove previously discarded `PrefixLimit` and `ReversePrefixLimit` when the
source was a table, so the first version of this probe read all 43,000 rows
and regressed. Preserving the limit in `table_source_request` makes the
physical cap effective. The focused Groove test checks both key directions.

## Receipt

Anonymized permissioned-resource fixture, RocksDB, `perf` profile, member
identity, no deleted rows. Each result is an independently opened trusted
host over the same seeded store. All readings are microseconds inside the
one-shot read; preparation and fixture loading are excluded. The control and
candidate were run alternately, three times each, from the same instrumented
binary (`SHA-256 f172112c493f824e252ed9332615f98a67f26c30e6b4092be48da37ada4db5c3`).
It was built from #3489 commit `92810e091` with this PR's source change and
measurement-only spans. A temporary environment guard disabled the probe for
control runs; neither the guard nor spans are shipped in this PR.

| Query set                         |      Control read (µs) |      Bounded read (µs) | Median gain | Rows |
| --------------------------------- | ---------------------: | ---------------------: | ----------: | ---: |
| Largest child table, `limit(100)` | 338143, 338888, 339653 |    19650, 20119, 19733 |  **17.17×** |  100 |
| All 39 tables, `limit(100)` each  | 483388, 492634, 507942 | 173665, 174797, 174660 |   **2.82×** |  879 |

Every run had the same row-set hash for each table. The candidate returned
the same 879 rows across all 39 queries. In the dominant table the physical
source and deletion lookup reported 101 candidates, zero deletion registers,
and about 1 ms of source scanning; the control read's complete graph took
roughly 339 ms. The new path is therefore a substantial win even where
history/deletion density is zero.

The same instrumented binary also ran the result-only Core → relay → client
route with the probe alternately disabled and enabled:

| Query set                        | Control read (ms) | Bounded read (ms) | Median gain | Rows / encoded bytes |
| -------------------------------- | ----------------: | ----------------: | ----------: | -------------------: |
| All 39 tables, `limit(100)` each |     557, 552, 555 |     273, 269, 272 |   **2.04×** |         879 / 368989 |

Median Core ticks fell from 471 ms to 187 ms. The single dominant relayed
page dropped from 390 ms to 99 ms with the same tracing instrumentation.
The relay total is limited by work outside the Core after this change.

## Reproduction

The direct trusted-host benchmark runner is on #3489, not this PR's `main`
parent. Build that native harness with and without this PR's source change:

```sh
cargo build -p jazz-example-permissioned-resources-benchmark \
  --bin permissioned-resources-profile --profile perf
```

For the dominant trusted-host page:

```sh
JAZZ_CUSTOMER_IDENTITY=member \
JAZZ_CUSTOMER_PHASES=cold \
JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 \
JAZZ_CUSTOMER_SERVER_ONESHOT=1 \
JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3 \
JAZZ_CUSTOMER_QUERY_LIMIT=100 \
target/perf/permissioned-resources-profile
```

Set `JAZZ_CUSTOMER_SERVER_ONESHOT_ALL=1` and omit
`JAZZ_CUSTOMER_ONLY_TABLE` for the 39-query set. Compare `read_us` and
`row_set_hash` in the JSON result. The scope is one-shot trusted Global
reads of flat, unfiltered UUID-ordered pages with no projection, joins, or
subqueries. Subscriptions and other query shapes continue through the
existing path; this does not claim to fix the six-second live-subscription
settle case.

Tooling friction: a built-in, per-query physical source-row count in the
benchmark receipt would have exposed the discarded table cap immediately.
