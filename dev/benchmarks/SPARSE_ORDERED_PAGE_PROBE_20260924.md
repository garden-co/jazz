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

These are single-run directional timings, not before/after speedup claims.
The register counts show that unrelated tombstones no longer dominate the
measured sparse cases. History, deletion, restore, policy, and IVM semantics
remain in place. The earlier no-deletion 100,000-row comparison is in
[`BOUNDED_DELETION_REGISTER_READ_20260924.md`](BOUNDED_DELETION_REGISTER_READ_20260924.md).

Raw receipts: [standard](receipts/policy-read-sparse-20260924/standard-10k.jsonl),
[short](receipts/policy-read-sparse-20260924/short-10k.jsonl),
[empty](receipts/policy-read-sparse-20260924/empty-10k.jsonl), and
[sparse](receipts/policy-read-sparse-20260924/sparse-10k.jsonl).

Tooling friction: recompiling the optimized Rust binary dominates each small
iteration; a reusable fixture and preserved control binary would make paired
wall-time comparisons faster.
