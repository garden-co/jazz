# Sparse ordered-page probe receipt (2026-09-24)

The public-API `policy-read-receipt` fixture uses 10,000 anonymized documents,
`OwnerOrOrg` policy, and a composite equality/order index. These are
runtime-cold, OS-cache-warm Global `Db::all_for_identity` reads. Each receipt
separately reports seed, mutation, reopen, prepare, query, close, storage-read,
and allocation data. The measured binary SHA-256 is
`9ba37b68ed72a0497347d84d41b904f1ad07b439487228161b25f56f13b444a4`.

| Case                                                        | Result rows | Query time | Index reads | Current-row reads | Deletion-register reads |
| ----------------------------------------------------------- | ----------: | ---------: | ----------: | ----------------: | ----------------------: |
| No deletions, Org(0), LIMIT 10                              |          10 |   3.257 ms |          22 |                47 |                      11 |
| 3,000 unrelated deletions, Owner(2), LIMIT 150              |         100 |   5.649 ms |         300 |               225 |                     100 |
| 3,000 unrelated deletions, empty Org(25), LIMIT 10          |           0 |   2.891 ms |           0 |                25 |                       0 |
| 3,000 unrelated plus 20 top-row deletions, Org(0), LIMIT 10 |          10 |   6.620 ms |         110 |               160 |                      55 |

The prior implementation fell back to the complete source when a bounded
probe found no strict extra visible row. In the 1,000-row public-API test,
the short Owner(2) page read 310 deletion registers before this change.
The new test checks exact row IDs and at most 100 register reads for short,
empty, and deleted-prefix pages. The harness also asserts exact IDs for its
new scenarios. A second physical-index count proves exhaustion when schema
projection may hide a candidate; otherwise the probe retries at up to three
bounded prefix sizes. It still falls back to the complete query if it cannot
prove a page boundary within that budget.

The table above contains single-run candidate-only timings, not a before/after
speedup claim. A matched direct-parent comparison follows.
The register counts show that unrelated tombstones no longer dominate the
measured sparse cases. History, deletion, restore, policy, and IVM semantics
remain in place. The earlier no-deletion 100,000-row comparison is in
[`BOUNDED_DELETION_REGISTER_READ_20260924.md`](BOUNDED_DELETION_REGISTER_READ_20260924.md).

## Matched direct-parent comparison on latest main

The control is `cf39aa59f` (PR #3316 tip) with the identical revision-4
receipt harness copied into its checkout. The candidate is `d48d23853`
(PR #3317). Optimized binaries were built separately and verified to differ:
control SHA-256 `c142eee659cf1ce468a8f0e2804f93d8f699111393770597e2286e8139a4ac9a`;
candidate `a248430215f63d1d3cf416b512c3a34a8c18c3f308b64c99f3cc9f98c81c2e7a`.
An initial shared-target build reused the control binary and was discarded;
the candidate was forced to rebuild and its distinct hash checked before
measurement.

Each run creates the same 100,000-row synthetic `OwnerOrOrg` fixture with
50,000 unrelated deletions, using the system allocator. Each arm used two
runtime-cold, OS-cache-warm processes in A/B/B/A order per scenario (the
different scenarios are interleaved). The medians below compare query phase
only; raw receipts retain seed, mutation, storage/Jazz reopen, preparation,
query, close, and logical storage-read phases separately. The benchmark
asserts the exact returned IDs for each scenario.

| Scenario                                | Control query, two runs | Candidate query, two runs | Median speedup | Register row reads, control → candidate |
| --------------------------------------- | ----------------------: | ------------------------: | -------------: | --------------------------------------: |
| Short Owner(2), LIMIT 1,050; 1,000 rows |     85.423 / 103.551 ms |        26.267 / 24.395 ms |           3.7× |                          51,000 → 1,000 |
| Empty Org(25), LIMIT 10                 |      45.771 / 45.108 ms |          2.672 / 3.270 ms |          15.3× |                              50,000 → 0 |
| Org(0), LIMIT 10; 20 newest deleted     |      81.394 / 83.106 ms |          5.712 / 6.237 ms |          13.8× |                             50,031 → 55 |

Raw control/candidate pairs: [short control 1](receipts/policy-read-sparse-20260924/parent-short-100k-r1.jsonl),
[short control 2](receipts/policy-read-sparse-20260924/parent-short-100k-r2.jsonl),
[short candidate 1](receipts/policy-read-sparse-20260924/candidate-short-100k-r1.jsonl),
[short candidate 2](receipts/policy-read-sparse-20260924/candidate-short-100k-r2.jsonl);
[empty control 1](receipts/policy-read-sparse-20260924/parent-empty-100k-r1.jsonl),
[empty control 2](receipts/policy-read-sparse-20260924/parent-empty-100k-r2.jsonl),
[empty candidate 1](receipts/policy-read-sparse-20260924/candidate-empty-100k-r1.jsonl),
[empty candidate 2](receipts/policy-read-sparse-20260924/candidate-empty-100k-r2.jsonl);
[sparse control 1](receipts/policy-read-sparse-20260924/parent-sparse-100k-r1.jsonl),
[sparse control 2](receipts/policy-read-sparse-20260924/parent-sparse-100k-r2.jsonl),
[sparse candidate 1](receipts/policy-read-sparse-20260924/candidate-sparse-100k-r1.jsonl),
[sparse candidate 2](receipts/policy-read-sparse-20260924/candidate-sparse-100k-r2.jsonl).

Raw receipts: [standard](receipts/policy-read-sparse-20260924/standard-10k.jsonl),
[short](receipts/policy-read-sparse-20260924/short-10k.jsonl),
[empty](receipts/policy-read-sparse-20260924/empty-10k.jsonl), and
[sparse](receipts/policy-read-sparse-20260924/sparse-10k.jsonl).

Tooling friction: recompiling the optimized Rust binary dominates each small
iteration; a reusable fixture and preserved control binary would make paired
wall-time comparisons faster.
