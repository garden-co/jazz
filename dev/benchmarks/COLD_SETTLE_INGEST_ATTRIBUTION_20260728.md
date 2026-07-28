# Cold settle ingest attribution — 2026-07-28

## Decision

**CLEARLY-BAD for rank 1 as a product optimization.** The benchmark-only
in-process transport probe costs 159.510 ms of a 12.626 s cold settle (1.26%),
including 32.177 ms in its `postcard::to_allocvec` call. The harness invokes no
real `WireTransportAdapter`, so all 293 of those probe calls are outside the
product's wire path. The apparent serialization hot frame is therefore mostly
an attribution trap, not a path to the cold-start target.

**CLEARLY-GOOD measurement; CLEARLY-INSUFFICIENT for rank 3 alone.** Two
oversized view updates produced 347 candidate constructions but only 137
selected payloads (112 updates fit outright and the two split updates emitted
25 chunks). Candidate construction plus its sizing encode took 1.503 s in the
13.117 s confirmation run: **11.46% of semantic cold-settle work**. This
is an upper bound on the removable share of an incremental sizing design,
because it includes the candidate measurements needed to select a prefix.

The counts were identical in the 13.117 s confirmation run. A third 15.322 s
run measured 1.653 s. A fourth 24.598 s run was
discarded as shared-host contention (all wall-clock counters inflated), as
required by the brief.

Even granting the full 11.46% upper bound to rank 3 and incorrectly counting
the entire 1.26% probe as removable product work only reaches 12.72% of cold
settle, well below the roughly 30% ingest improvement needed. The honest next
path is rank 2 (policy-scoped source delivery) or rank 4 (dynamic semi-join),
not days spent on ranks 1 and 3.

## Probe versus real serialization

The cold harness is deliberately a semantic `SyncMessage` queue. It never
constructs `WireTransportAdapter`; real-wire encodes were therefore **0**.
The split is:

| Class | Calls | Wall time | Meaning |
| --- | ---: | ---: | --- |
| Semantic `Db::tick` work | 4 ticks/node | 12.622 s | Core 2.928 s, relay 5.588 s, client 4.106 s; sequential and consistent with 12.626 s settle. |
| Sender preflight sizing | 539 payload + 461 frame encodes | 991.097 ms | Required preflight serialization for resume/chunk sizing; 956 MB payload and 922 MB framed bytes were encoded. |
| Benchmark-only probe | 293 messages | 159.510 ms | In-memory transport's postcard/clone/compress/decompress probe; no real wire adapter call. |
| Probe postcard serialization only | 293 messages | 32.177 ms | The part most directly represented by the sampled serialization frame. |

The probe was also split by the two delivery hops: core→relay had 54 calls / 107.492 ms, and relay→client had 83 calls / 50.745 ms. The remaining calls are control traffic in the reverse directions.

## Oversized-update attribution

| Counter | Result |
| --- | ---: |
| ViewUpdates fitting without split | 112 |
| ViewUpdates split | 2 |
| Emitted chunks / selected payloads | 137 |
| Candidate builds | 347 |
| Excess candidate builds over selected payloads | 210 (60.5% of candidates) |
| Candidate encoded bytes | 866,969,236 |
| Selected-payload bytes | 54,796,149 |
| Candidate build + encode wall time | 1.503 s |

`candidate_build + encode` deliberately measures the actual clone/repack and
framed-size probe together. It is the useful upper bound for rank 3; it should
not be added again to sender preflight sizing, which contains those encodes.

## Operator/cardinality counters

The disabled feature also counted `update_map_project` and keyed join calls,
input rows, and output rows by hop and hydration/tick mode. The requested
core→relay and relay→client aggregate counters were emitted in the JSON receipt.
For example, core→relay hydration processed 428,241 map-project input/output
records and 95,905 + 97,454 join-side records into 94,739 outputs; relay→client
tick work processed 708,894 map-project records and 262,277 + 76,599 join-side
records into 211,616 outputs.

**UNCLEAR for rank 4 from this run.** The anonymous dominant-child tag remained
zero: recursive hydration reaches this work via indexed/lowered nodes whose
source descriptor does not retain the subscription table name or the child
shape fields. The aggregate per-hop/mode counts are sound, but assigning them
to `res_l_child_3` would be false precision. This does not change the rank-1/3
decision above; it leaves rank 4 as a separately instrumented follow-up.

## Commands and instrumentation

The temporary counters remain behind the disabled Cargo feature
`cold-settle-attribution` in `groove`, `jazz`, and `jazz-sim`; ordinary builds
do not compile them.

```text
cargo check -p jazz-sim --bench customer_cold_start --features cold-settle-attribution
cargo check -p jazz-sim --bench customer_cold_start
cargo bench -p jazz-sim --bench customer_cold_start --features cold-settle-attribution -j 2 --no-run
JAZZ_CUSTOMER_PHASES=cold target/release/deps/customer_cold_start-ff3423cf4a91a036
```

The final executable suffix is Cargo-artifact-specific; the run used the one
emitted by the preceding `--no-run` command. Measurements used the anonymized
in-repo fixture with a warm seed cache. `-j 2` was selected because this is a
shared box. Counter equality across the two accepted runs is the evidence for
counts; timing conclusions use the 12.626 s run and exclude the contended
24.598 s run.

Tooling-friction: a per-hop production-wire counter in the native benchmark
would avoid needing to prove that the existing in-memory codec probe is not a
real wire encode.
