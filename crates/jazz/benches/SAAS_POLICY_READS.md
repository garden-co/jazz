# SaaS policy-scoped read benchmark

`saas_policy_reads` measures what a client asks a central authority for when
row-level security is in play: a page of rows the requesting user owns, or rows
shared through the organisation that owns them.

It complements [`selective_global_hydration`](../../../dev/benchmarks/SELECTIVE_GLOBAL_HYDRATION.md)
rather than replacing it. Keep both.

| dimension       | `selective_global_hydration` | `saas_policy_reads`                                                       |
| --------------- | ---------------------------- | ------------------------------------------------------------------------- |
| read policy     | none                         | `owner == @user.account`, or `owner == @user.account OR INHERITS via org` |
| identity        | `AuthorSubject::SYSTEM`      | the requesting user                                                       |
| equality bucket | 100 rows                     | `table_rows / 100` (default 10,000 at 1M)                                 |
| cases           | one equality, two orderings  | owner page, owner top-10, org page                                        |
| page sizes      | fixed at 50                  | 1 / 10 / 50                                                               |
| relations       | one table                    | documents plus org membership                                             |

Both are worth running together, because the pair is the signal. The policy-free
arm selects the declared index; a policy-scoped arm does not. A trend on either
one alone hides that difference.

## What it reports

Per case: `index_reads`, `current_row_reads`, `history_row_reads`,
`total_reads`, `query_us`, and `reads_per_result_row`.

`reads_per_result_row` is the boundedness check. A page-bounded read is roughly
constant in this metric as `table_rows` grows; a scope-bounded read grows with
the owner's share. Today it grows, which is the finding this bench exists to
keep visible.

## Modes

Divan wall-time benchmarks, which is the CodSpeed lane:

```sh
cargo bench -p jazz --features testing --bench saas_policy_reads
```

Three fixed cases: `saas_owner_page_10k`, `saas_owner_page_100k`,
`saas_owner_top10_100k`.

The JSONL scale receipt, for read-count attribution across a ladder:

```sh
JAZZ_SAAS_RECEIPT=1 \
JAZZ_SAAS_TABLE_LADDER=10000,100000,1000000 \
JAZZ_SAAS_LIMIT_LADDER=1,10,50 \
cargo bench -p jazz --features testing --bench saas_policy_reads
```

## Knobs

| variable                    | default                | meaning                                          |
| --------------------------- | ---------------------- | ------------------------------------------------ |
| `JAZZ_SAAS_PER_OWNER`       | derived                | documents per owner; derived from the table size |
| `JAZZ_SAAS_TABLE_LADDER`    | `10000,100000,1000000` | table sizes for the receipt                      |
| `JAZZ_SAAS_LIMIT_LADDER`    | `1,10,50`              | page sizes for the receipt                       |
| `JAZZ_SAAS_SEED_BATCH_ROWS` | `5000`                 | rows per seeding transaction                     |
| `JAZZ_SAAS_POLICY`          | `owner`                | `owner` or `owner_or_org`                        |
| `JAZZ_SAAS_RECEIPT`         | unset                  | emit the JSONL receipt instead of Divan          |

## Measurement boundaries

- RocksDB only, `WalNoSync` on both sides. No in-memory storage.
- Every case **reopens the database** before timing. A second query on the same
  instance is served from the retained maintained view and reports zero reads;
  that measures the cache rather than query planning.
- Fixture seeding happens outside the timed closure and is reported separately
  as `saas_seed`.
- The receipt asserts the returned row count per case; correctness of the
  ordered result is checked by construction of the fixture's `updated_at`
  values rather than by digest.

## Known limitations

- `index_only(["owner_id", "updated_at"])` declares **independent
  single-column** global-current indexes, because
  `TableSchema::global_current_indexed_columns()` is a set. There is no ordered
  `(owner_id, updated_at)` access path, so an `ORDER BY updated_at DESC` page
  cannot be capped before the sort, and the equality bucket must be
  materialised. The bench measures the planner that exists; it does not assume
  a compound path.
- The org arm uses `INHERITS via org_id`, which the planner currently keeps on
  the full-scan path by design. See `read_sources.rs`, and
  `policy_access_path_planner_falls_back_for_or_and_non_equality` in
  `crates/jazz/src/node/tests/queries.rs`.
- `owner_filter_diagnostic` covers adjacent ground but is born-red on `main`
  (`policy_only row count changed`), so it is not a usable control today.

## Tooling friction

Optimised rebuilds of `jazz` for a single bench take ~30 s with a warm target
directory. Seeding dominates the receipt: 1M rows takes minutes, and reseeding
per rung is the largest cost in a full ladder run. A reusable settled fixture
per rung would cut that materially.
