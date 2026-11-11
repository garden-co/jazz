# BigLabel benchmark variant

[metadata.ts](metadata.ts) owns the wall-clock descriptions, timing boundaries
and work denominators used by the examples page. CodSpeed measures both suites
(`loads` and `ingest_walltime`) in wall-clock mode on its macro runner.

This package is a self-contained Rust model of BigLabel's read-heavy record-label
workload. It intentionally duplicates the schema and deterministic fixture needed
for measurement; it does not import application runtime helpers.

The fixture creates four catalogues, eight labels, 32 artists, and either 512 or
4,096 releases. Release rows carry indexed label, primary-artist, catalogue, and
release-time fields. The measured workloads are prepared, ordered Jazz reads for:

- a label's releases at 4,096 releases (`big_label_label_load`);
- a 1,000-row import at batch sizes 1, 100, and 1,000 for performance
  thesis [#1964](https://github.com/garden-co/jazz/issues/1964);
- a label's live release view over 100,000 persisted releases of many tenants
  (`big_label_releases_live_view_100k`, formerly
  `maintained_subscription_hydration_100k`), whose hydration must follow the
  `label` index;
- a release plan's first edit under a sign-off policy with 16 and 64 branches
  (`big_label_sign_off_first_edit`, formerly `update_support_branches`): a
  fresh database compiles the update policy and evaluates it. The policy
  needs a lead or deputy grant on each of 4 or 6 sign-off desks.

The artist and catalogue loads were dropped (same query shape as the label
load), as were the 512-release points and batch size 10. The 100k-row import at
batch size 1,000 lives in `ingest_walltime`; the 10k import was dropped.

Database opening, schema compilation, fixture insertion, local-durability waits,
and query preparation happen before each measured closure. The returned row count
is black-boxed so the read cannot be optimized away.

The ingest benchmark measures release construction and insertion. Schema
compilation, database opening, and dimension-row seeding are supplied as untimed
per-iteration Divan inputs. Read fixtures keep their one-transaction-per-release
history shape.

## Contention (`benches/contention.rs`, nightly)

Two labels share one durable server, as tenants of one app do. A member of
label B opens their newest releases (`big_label_open_releases_*`) or renames
one (`big_label_rename_release_*`, timed until the server acknowledges it).
Each runs idle and `during_search`, while a member of label A searches its
300,000-release back catalogue back to back. The search is the app's
`contains` filter on `search_key` with a term that matches nothing, so it reads
every release of label A. All clients are ordinary JWT users over WebSockets,
each on its own thread, so every request passes the row policies. The fixture
is in [src/contention.rs](src/contention.rs); the server and client threads come
from `jazz_testkit::contention`.

The difference between the two arms is how long one tenant's slow query holds
up another tenant's cheap request. On a 4-core development VM on 2026-10-28,
a search took about 85 ms, and both requests went from about 3 ms idle to
about 80 to 90 ms during the search.

Run locally with:

```sh
cargo test -p jazz-example-big-label-benchmark
cargo bench -p jazz-example-big-label-benchmark --bench loads
cargo bench -p jazz-example-big-label-benchmark --bench contention
```
