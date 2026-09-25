# Bounded deletion checks for policy-scoped pages (2026-09-24)

## Workload and provenance

The native `policy-read-receipt` harness seeded 100,000 anonymized documents in
RocksDB. These figures are for a runtime-cold, OS-cache-warm `Db::all_for_identity`
read at Global tier: `OwnerOrOrg` policy, `Org(0)`, descending `updated_at`,
`LIMIT 10`. Deletions and restores affect only trailing rows outside Org(0).
Every receipt uses fixture revision 3 and reports open, prepare, query, close,
storage-read, and allocation phases separately.

Control source: `f13c8cc6b` (before bounded deletion checks). Candidate source:
`e7f0c3cb544ee2f4b04b901a5ef01fc2c4782edf`. Candidate optimized binary:
`target/perf/policy-read-receipt`, SHA-256
`c58a6a7b7517c12d7eca241a8c89bb720628496e52a97172bf7da64d207ed8ce`.
The control executable was not preserved; its raw JSONL receipts are included
below. These are single-run directional measurements, not controlled ABBA
medians. Another cold-load benchmark occupied one CPU core during this run.

| Current-register state            | Control query | Candidate query | Speedup | Register row reads, control → candidate |
| --------------------------------- | ------------: | --------------: | ------: | --------------------------------------: |
| None; single-column index control |     34.042 ms |        2.958 ms |   11.5× |                                  0 → 11 |
| None; composite index control     |      2.881 ms |        2.958 ms |   0.97× |                                  0 → 11 |
| 50,000 unrelated deleted rows     |     37.820 ms |        3.799 ms |   10.0× |                             50,000 → 11 |
| 10,000 unrelated restored rows    |      8.821 ms |        2.844 ms |    3.1× |                             10,000 → 11 |

All four comparisons are query phase only; reopen time varies separately.
The composite-index zero-deletion comparison exposes the extra candidate probe
cost. In the 50,000-deletion case, index reads rise from 11 to 22 and current
row reads from 36 to 47. History row reads are zero in every listed run.

## Mechanism and boundary

The ordered-page path first reads at most `LIMIT + 1` content candidates from
the composite index, then point-reads their current deletion registers. It
feeds those register records to both the main query and the policy proof.
The policy proof was the remaining full-scan source: an early trial made 11
point reads **plus** all 300 unrelated register rows in a 1,000-row test.
Passing the same bounded register to that proof reduced this test to bounded
reads. The one-shot program bypasses the compiled program cache because these
inline records belong to one storage snapshot. Maintained subscriptions and
other query shapes keep their existing complete sources.

The current page probe still falls back to the complete query when deleted or
denied candidates leave no extra visible row to prove the page boundary.
Sparse or underfilled pages need a separate visibility-aware cursor or bounded
widening design; this receipt does not claim a universal page-size bound.
Removing history alone would not address this current-register scan.

## Verification

- `cargo test --profile perf -p jazz-example-policy-scoped-documents-benchmark --test pages -q`: 5 passed.
- `dev/gates/benchmark-smoke.sh --ci`: four phases passed.
- `dev/gates/benchmark-smoke.sh --compile-ci`: three phases passed.
- `cargo test -p jazz --features testing --test incremental_delivery_canary`: 3 passed.
- `JAZZ_SEED_COUNT=20 dev/t --exact node::tests::harness::m3_maintained_one_shot_differential_oracle -- --ignored`: passed.
- `cargo fmt --all -- --check` and the local pre-commit Clippy check: passed.

Raw receipts: [control single](receipts/policy-read-20260924/control-single.jsonl),
[control composite empty](receipts/policy-read-20260924/control-composite-no-deletions.jsonl),
[control 50k deleted](receipts/policy-read-20260924/control-composite-50k-deleted.jsonl),
[control 10k restored](receipts/policy-read-20260924/control-composite-10k-restored.jsonl),
[bounded empty](receipts/policy-read-20260924/bounded-no-deletions.jsonl),
[bounded 50k deleted](receipts/policy-read-20260924/bounded-50k-deleted.jsonl),
and [bounded 10k restored](receipts/policy-read-20260924/bounded-10k-restored.jsonl).

Tooling friction: optimized Rust recompilation and the concurrent cold-load
benchmark made each feedback cycle slower; a preserved control binary and an
idle measurement host would make the next ABBA comparison cheaper and cleaner.
