# 500k SaaS documents benchmark receipt

Date: 2026-07-28

Status: directional developer-machine result, not a performance promise.

## Workload

- 500,000 documents
- 5,000 teams
- 10 memberships per team
- one 30,000-document hot team
- one 100-document small team
- `ORDER BY updated_at DESC LIMIT 100`
- optional `status = active AND archived = false`
- membership-based document read policy

The requested cardinalities are not simultaneously satisfiable: 5,000 teams
with at least 100 documents already consume all 500,000 rows. Keeping a 30,000
row team requires 529,900 total documents. The default fixture preserves the
500,000 total and gives the remaining teams 94-95 rows; setting the document
count to 529,900 preserves the lower bound.

## Harnesses

- `crates/groove/benches/saas_documents.rs` isolates storage access, TopBy, and
  subscribed-write maintenance against RocksDB with WAL and no per-write sync.
- `crates/jazz-tools/benches/saas_documents.rs` exercises the public `Db`
  facade, literal and parameterized queries, policies, Local and Global read
  modes, and correctness canaries.

Both harnesses validate exact result identities against an independent oracle.

Primary commands:

```sh
cargo bench --profile perf -p groove --bench saas_documents --quiet
JAZZ_SAAS_QUERY_ITERS=1 \
  cargo bench --profile perf -p jazz-tools --bench saas_documents --quiet

GROOVE_SAAS_QUERY_ITERS=1 GROOVE_SAAS_WRITE_ITERS=100 \
  cargo bench --profile perf -p groove --bench saas_documents --quiet
```

Environment:

- Rust 1.93.1
- `aarch64-apple-darwin`
- `[profile.perf]`

## Groove and RocksDB results

The primary query run used 10 repeated samples. Times below are p50.

| Query | Team rows | Candidate rows | Time | Storage records / ranges |
| --- | ---: | ---: | ---: | ---: |
| Full scan, latest | 30,000 | 500,000 documents + 50,000 memberships | 181.9 ms | 550,000 / 2 |
| Indexed candidates, latest | 30,000 | 30,000 documents + 1 membership | 105.9 ms | 60,002 / 30,003 |
| Full scan, active/unarchived | 30,000 | 500,000 documents + 50,000 memberships | 236.5 ms | 550,000 / 2 |
| Indexed candidates, active/unarchived | 30,000 | 7,106 documents + 1 membership | 24.2 ms | 14,214 / 7,109 |
| Full scan, latest | 100 | 500,000 documents + 50,000 memberships | 197.9 ms | 550,000 / 2 |
| Indexed candidates, latest | 100 | 100 documents + 1 membership | 0.310 ms | 202 / 103 |
| Indexed candidates, active/unarchived | 100 | 24 documents + 1 membership | 0.109 ms | 50 / 27 |

The current durable index is non-covering: each index entry causes a base-row
lookup. A full 30,000-row prefix scan took 45.8 ms. A single seek to the last
entry in that prefix took 7 us and read two records, demonstrating that ordered
storage is available but bounded reverse iteration is not exposed to the query
path.

Fixture seeding took 4.41 s.

## Subscribed writes

The subscription targets the active/unarchived latest 100 rows of the
30,000-document team.

| Write | Samples | Commit wall | IVM tick | Storage |
| --- | ---: | ---: | ---: | ---: |
| One matching-team row | 100 | 8.45 ms p50 / 9.26 ms p95 | 8.42 ms p50 | 0.041 ms p50 |
| One unrelated-team row | 100 | 0.046 ms p50 / 0.071 ms p95 | 0.019 ms p50 | 0.010 ms p50 |
| `commit_batch` for 100 matching rows | 1 | 9.39 ms | 8.93 ms | 0.175 ms |

The 9.39 ms measurement starts immediately before `commit_batch`; constructing
and staging the 100 rows is outside that timer. The run started with 30,000
team rows, performed 20 matching one-row writes, and committed the batch at a
pre-batch cardinality of 30,020. The batch incurs one IVM tick. One hundred
separate commits repeat the approximately 8-10 ms maintenance work 100 times,
so batching is substantially more efficient even though this receipt does not
measure end-to-end row construction.

These numbers use one active subscription. The many-binding case is currently
blocked by the correctness issue documented in
`SAAS_DOCUMENTS_ENGINE_FINDINGS_20260728.md`.

## Jazz public-API results

The full 500k Local fixture used `MemoryStorage` and batched local writes.
Seeding 5,000 teams, 50,000 memberships, and 500,000 documents took 31.14 s.
Each first/repeated cell below is a single observation
(`JAZZ_SAAS_QUERY_ITERS=1`), not a latency distribution.

| Scenario | Predicate | First read | Repeated read |
| --- | --- | ---: | ---: |
| Hot team, literal | team only | 8.58 s | 8.52 s |
| Hot team, parameterized | team only | 7.99 s | 171.5 ms |
| Hot team, parameterized | team + active/unarchived | 187.8 ms | 162.0 ms |
| Small team, literal | team only | 170.7 ms | 166.7 ms |
| Small team, parameterized | team only | 231.0 ms | 237.1 ms |

The parameterized hot-team result exposes a large first-binding hydration cost:
7.99 s initially and 171.5 ms on reuse. The literal hot-team path remains at
about 8.5 s on the repeated observation, so its cost is persistent rather than
first-hydration-only. Later shapes can reuse already hydrated graph fragments.

A 20k-row Global control confirmed team-cardinality sensitivity: latest-100
literal reads were 88.1 ms for a 3,000-row team and 15.9 ms for a 100-row team.
Global bootstrap was kept small because the public settled import API is
row-at-a-time.

## Correctness canaries

Authorization denial passed: an identity without a membership saw zero rows.

Two canaries failed:

1. `CurrentRow::cell("team")` returned `None` for a parameterized result even
   though the exact-row oracle passed.
2. Two simultaneous bindings of the same parameterized latest-100 shape did not
   produce isolated initial snapshots; the hot-team subscription returned 0
   rows instead of 100.

## Validation

- both benchmark targets compile and pass Clippy
- Rust formatting and diff checks pass
- both `saas_documents` targets were run separately at full fixture size
- the existing repository benchmark smoke suite passed as a regression check;
  it does not invoke these new targets
- query and subscription outputs are checked against exact-ID oracles

Tooling friction: bounded reverse-prefix iteration and batched settled bootstrap
would have made the intended production paths directly measurable at full size.
