# SQLite Core Performance Decision Log

## 2026-05-26 21:06 PDT

Started the performance benchmark sprint branch from
`codex/mini-jazz-sqlite-spec` after drafting the scenario list. First
implementation target: create a benchmark/reporting harness that measures
whole-topology, client-perceived latency before adding increasingly realistic
policy and cache-mode scenarios.

## 2026-05-26 21:07 PDT

The current mini runtime already supports query-scoped export/apply for
`eq_top_created_at_desc`, but not arbitrary user-column ordering like
`updated_at DESC`. First benchmark slice will use system created-time ordering
to get whole-topology metrics flowing, then add exact user-column ordered
pagination as the next de-risking step.

## 2026-05-26 21:09 PDT

First benchmark example works for a small core-only cold topology. A 1k-row
seed with 200 target-owner rows and page size 50 produced 200 synced history
rows but 1k synced transaction rows, because query-scope export currently
includes all transaction metadata. This is an important early whole-stack
finding: scoped history without scoped transaction metadata will still make
cold query bundles grow with total database history.

## 2026-05-26 21:10 PDT

Scoped query bundles now filter transaction metadata down to the history/read
records actually exported. The same 1k/200/page-50 run dropped transaction
records from 1k to 200, bundle bytes from about 298 KB to 124 KB, and
api-to-first-result from about 214 ms to 149 ms. This was a good example of a
benchmark immediately exposing a missing sync-scope feature.

## 2026-05-26 21:13 PDT

Added a first-pass user-column ordered top-N feature:
`WHERE field = value ORDER BY order_field DESC LIMIT n`. Covered it with a
whole-system durable refresh test that verifies page-boundary repair after a
new higher-ranked row arrives while the worker is closed. The implementation
still materializes and sorts rows in Rust for reads; the next performance step
is to lower this path into SQLite `ORDER BY ... LIMIT` over projection/index
tables so the benchmark matches PR #898's intent.

## 2026-05-26 21:15 PDT

Ran the first larger smoke profile: 10k total documents, 1k target-owner rows,
page size 50, core-only cold through durable edge/worker and memory tab.
Result: about 787 ms api-to-first-result; export was about 156 ms, each apply
hop about 208-213 ms, and final tab query about 0.5 ms. Current bottleneck is
bundle export/apply volume and repeated hop materialization, not local final
query latency.

## 2026-05-26 21:17 PDT

Lowered the user-column ordered read path for the main branch into SQLite
`ORDER BY order_field DESC LIMIT n`. The 10k/1k/page-50 smoke improved export
only modestly, from about 156 ms to 144 ms, because the sync bundle still
contains the full 1k owner predicate slice. Next sharp feature gap: top-page
query-scoped sync needs page-boundary/observed-row repair instead of broadening
history export to the whole equality predicate.

## 2026-05-26 21:20 PDT

Added configurable fixture seed batching to the benchmark example. With batch
size 100, the 10k/1k/page-50 smoke seed time dropped from about 8.5 s to
0.87 s. Because all 1k target-owner rows now come from 10 transactions instead
of 1k, bundle bytes dropped from about 622 KB to 402 KB and api-to-first-result
from about 767 ms to 562 ms. This is a fixture-generation knob, not a product
claim; reports now include `seed_batch_size` so comparisons do not mix shapes.

## 2026-05-26 21:21 PDT

Ran the first PR-898-default-scale-ish profile: 100k total documents, 10k
target-owner rows, page size 50, seed batch size 100. Result: about 5.7 s
api-to-first-result, 4.1 MB bundle, 10k history rows synced, 100 transaction
records, and about 1.8 s apply time per hop. Final tab query was still only
about 0.4 ms. This strongly confirms that top-page sync/apply volume dominates
the current whole-topology path.

## 2026-05-26 21:21 PDT

Next feature attempt: make top-page query reads carry observed row ids. Refresh
can then export current top rows plus previously observed rows, instead of
broadening top-page history export to the entire equality predicate. Expected
result: initial page sync scales with page size while boundary replacement and
delete/rejection repair still have the old observed ids needed for cleanup.

## 2026-05-26 21:26 PDT

Implemented observed-row top-page query repair for both `$createdAt`-ordered
and user-column ordered page descriptors. Query reads now persist the current
observed page row ids. Refresh exports the new page plus the previously
observed ids so displaced/deleted boundary rows can be repaired without shipping
the whole equality predicate.

New tests cover that initial user-column ordered page export ships only the
observed page rows, and that reconnect refresh repairs a previously observed row
that was deleted upstream. The full whole-system suite passes.

The 100k-row / 10k-owner-row / page-50 cold topology benchmark changed from
about 4.1 MB, 10k history rows, 100 tx records, and 5.7 s api-to-first-result to
26 KB, 50 history rows, 1 tx record, and 63 ms api-to-first-result. Apply time
per hop dropped to roughly 10-12 ms. This is the strongest evidence so far that
the embedded-DB shape can support the small-page-over-large-scope workload if
query scopes encode observed page boundaries precisely.

## 2026-05-26 21:30 PDT

Extended the perf example with a cached-page reconnect/update phase: after the
initial page is synced through core -> edge -> worker -> tab, core inserts 50
newer owner rows and each tier refreshes observed query reads downstream.

On the 100k-row / 10k-owner-row / page-50 profile, the refresh phase shipped 100
history records from core: 50 new visible page rows plus 50 previously observed
repair rows. It used 2 transaction records, 49 KB of bundle JSON, and about 103
ms api-to-updated-result through all three hops. This confirms the repair shape
is bounded by page size plus previously observed page size, not by matching
predicate cardinality.

## 2026-05-26 21:32 PDT

Moved the perf scenario from owner-shaped data without actual policy to a real
policy shape: documents reference orgs, orgs are readable if created by the
current user, and documents are readable if their org is readable. Trusted core
and edge exports now run as `alice`, while writes are still seeded by trusted
admin/attribution paths.

On the 100k-row / 10k-owner-row / page-50 profile, cold first page now ships 100
history records: 50 document rows plus 50 policy dependency org rows. It uses 2
transaction records, 46 KB of bundle JSON, and about 161 ms api-to-first-result
through core -> edge -> worker -> tab. Refresh after 50 newer rows ships 200
history records, 3 transaction records, 79 KB, and takes about 206 ms
api-to-updated-result. Policy dependencies are visible in the cost model but
still scale with observed page size, not with the 10k matching owner rows.

## 2026-05-26 21:33 PDT

Ran the same policy-scoped 100k/10k/page-50 benchmark with edge and worker as
in-memory SQLite nodes instead of file-backed durable nodes. Results were very
close: about 158 ms cold api-to-first-result and 202 ms refresh
api-to-updated-result. For these page-sized bundles, intermediate durable writes
are not the dominant cost; export and logical apply/rebuild work matter more.

## 2026-05-26 21:34 PDT

Added a direct core query timing to the perf report. On the policy-scoped
100k/10k/page-50 profile, the SQLite top-page read itself is about 0.86 ms while
bundle export is about 98 ms. That strongly points at our export/policy/history
materialization path, not SQLite query planning for the page query, as the next
optimization target.

## 2026-05-26 21:36 PDT

Added approximate raw JSON payload bytes to the perf report. For the
policy-scoped 100k document + 100 org profile, raw serialized user values are
about 9.9 MB and the core SQLite database is about 38 MB, roughly 3.8x raw JSON
payload. This includes current projection, history tables, tx/read/query/system
metadata, ids, indexes, and SQLite page overhead.

## 2026-05-26 21:44 PDT

Starting the autonomous overnight performance sprint. Timebox target is
2026-05-27 03:44 PDT, six hours from the current timestamp. I will keep checking
clock time with `date`, continue even at good stopping points, prefer concrete
benchmark/implementation discoveries, and commit coherent green slices as I go.

## 2026-05-26 21:45 PDT

First export hot-path patch: query-scope tx export now fetches only tx ids needed
by the scoped history/read set instead of exporting every local transaction and
filtering in Rust. On the policy-scoped 100k/10k/page-50 profile, cold export
moved from about 98 ms to about 88-90 ms in one run, with no bundle shape change.
This is a modest win because the seed batch size is 100 (about 1k tx rows), but
it should matter much more for workloads with one write call per row.

## 2026-05-26 21:48 PDT

Tried the duplicate-history export hypothesis carefully. Query-scope export now
separates visible result row nums from repair row nums and only runs the
unfiltered history fallback for repair rows. This preserves deleted/hidden row
repair while avoiding one redundant history pass for pure current result rows.

Targeted ordered-page and query-scope tests pass. The benchmark improvement was
small in this scenario: cold export remained around 87 ms and refresh around 98
ms. This suggests the bigger cost is not simply duplicate child history export;
policy dependency export, SQL policy checks, JSON/ref conversion, or apply
rebuild remain more likely.

## 2026-05-26 21:51 PDT

Added user-column ordered page subscriptions (`eq_top_field_desc`) so the perf
scenario can measure client-visible subscription diff cost for the same
`owner_id ORDER BY updated_at DESC LIMIT n` shape. Added a whole-system test for
boundary displacement on that subscription kind.

The benchmark now creates a tab subscription before reconnect refresh. After 50
new top rows arrive, polling the subscription over the refreshed tab takes about
0.7 ms and reports exactly 50 added / 50 removed / 0 updated diffs. This is a
good sign: after sync/apply, semantic diffing of a page-sized result is cheap and
deterministic. The expensive parts remain upstream export and per-hop apply.

## 2026-05-26 21:52 PDT

Added warm-cache boot subreports. After the initial cold core -> edge -> worker
-> tab page sync, the benchmark now measures a fresh worker+tab booting from a
warm edge and a fresh tab booting from a warm worker.

On the policy-scoped 100k/10k/page-50 profile, edge-warm worker-cold is about 50
ms api-to-first-result and worker-warm tab-cold is about 24 ms. The same bundle
shape is exported, but warm edge/worker export is only about 3.6 ms versus
core-cold export around 86 ms. This is an important product-shaped result: once
an intermediate has the scoped page and policy dependencies, downstream cold
starts are dominated by apply, not query/export.

## 2026-05-26 21:54 PDT

Bathed tx receipt lookup in `export_txs_by_ids` so scoped tx metadata export no
longer runs a receipt query per tx. For the current page benchmark this did not
move the main numbers: cold export is still about 87 ms and refresh export about
98 ms. That makes sense because these scoped bundles only include 2-3 tx records.
Keep the change because it removes an obvious N+1 shape for larger scoped tx
sets, but the current bottleneck is elsewhere.

## 2026-05-26 21:57 PDT

Tried ref/policy indexing. One early run with an explicit `documents.org` index
looked dramatically better, but after turning it into an automatic ref index and
rerunning, export and seed times regressed badly and repeated runs confirmed the
regression. I reverted the automatic index and benchmark-only explicit index.

Learning: policy/ref indexing is definitely important, but blind `is_deleted,
ref` indexes are not safe to add yet. We need EXPLAIN QUERY PLAN output for the
policy export SQL and probably more targeted composite indexes that match the
actual predicates: branch, deletion, owner/order page index, and parent ref
checks. Treat this as a concrete derisking topic, not a settled decision.

## 2026-05-26 21:59 PDT

Added a small tx-granularity probe to the perf runner. It compares a 5k document
/ 500 owner-row profile seeded in batches of 100 vs one transaction per row. Both
export the same page-50 policy-scoped query.

Result: one-write-per-row seeding is about 4.1 s vs 0.47 s batched, database size
is 2.34 MB vs 1.95 MB, and the page bundle grows from 45 KB / 2 tx records to 52
KB / 51 tx records. Export time stays close (5.9 ms vs 5.2 ms) because scoped tx
metadata now only exports the transactions touched by the page/policy rows. This
supports the transaction model for read-time scope, while making write/seeding
cost and tx table size a separate concern.

## 2026-05-26 22:01 PDT

Added a recursive policy probe: teams are readable if created by the user,
projects are readable if their team is readable, and documents are readable if
their project is readable. The query remains the same owner ordered page over
documents. Profile is 20k docs / 2k owner docs / page 50 / policy depth 3.

Result: core top-page query stays under 1 ms, export is about 17 ms, bundle is 54
KB, and history rows are 110 (50 docs + 50 projects + 10 teams). This is
encouraging for recursively lowerable policy chains at page-sized scopes. It
also shows dependency cardinality follows the distinct ancestors of the observed
page, not the total table size.

## 2026-05-26 22:02 PDT

Added a multi-tab fanout probe: one durable worker receives a page from core,
exports the scoped page once, and the same bundle is applied to 8 fresh in-memory
tabs. Profile is 20k docs / 2k owner docs / page 50.

Result: worker boot from core is about 38 ms, worker export is about 3.8 ms,
bundle size is 46 KB, and each tab apply averages about 19 ms with about 0.65 ms
query time. This supports a broker/coalescing model: the expensive part should
be done once in the worker, then tabs pay mostly local apply. It also highlights
tab apply as an optimization target if we expect many simultaneous tabs.

## 2026-05-26 22:08 PDT

Narrowed rejected-transaction fate export. The previous scoped tx export still
scanned the entire table history for rejected txs so query refreshes could
deliver rejections even when no new history row was needed. Removing it broke the
rejection-refresh invariant, so I replaced it with query-scoped rejected fate
lookup: top-page queries use observed row ids, id predicates use id-derived row
nums, and ordinary predicates use the lowered predicate over rejected history.

`query_scope` tests pass, including the rejection refresh case. On the
policy-scoped 100k/10k/page-50 benchmark, cold export improved from roughly
86-87 ms to about 73 ms, and refresh export from roughly 95 ms to about 81 ms.
This confirms whole-table rejected fate scanning was one real cold-core cost,
but not the only one.

## 2026-05-26 22:10 PDT

Tried skipping policy rechecks for rows returned by the immediately preceding
query: visible result rows now export their row history directly, while repair
rows still use policy-aware visible export plus history fallback. This preserved
the query-scope test suite.

The benchmark did not materially move beyond the rejected-fate narrowing: cold
export remains about 72 ms and refresh export about 81 ms. So repeated policy
checks for the 50 result rows are not the remaining dominant cost.

## 2026-05-26 22:12 PDT

Added a many-user page probe: 100 users, 50k total documents, 500 documents per
user, each user has their own policy-readable org, and core exports page-20
queries for 20 sampled users.

Result: seeding takes about 4.9 s, core DB is about 21 MB, and 20 sampled page
exports take about 266 ms total, averaging 13.3 ms per user. Average bundle is
about 19 KB, 21 history rows, and 2 tx records. This is a strong product-shaped
data point for “many users each reading small policy-scoped pages”: per-user
query export scales with page/dependency size, not total table size, once the
predicate is selective.

## 2026-05-26 22:17 PDT

Added a mixed-mutation refresh probe to exercise a warm client page under
realistic write noise. The probe seeds 20k documents with 2k matching the
subscribed owner, boots a page-50 tab, then commits one transaction containing
25 new top rows, 10 updates to rows currently on the page, 100 updates to
matching-owner rows below the page, and 100 unrelated-owner updates.

Result: refresh export is about 18 ms, tab apply about 34 ms, query about 0.7 ms,
and subscription diffing about 0.8 ms. The diff is exactly the expected product
shape: 25 added, 10 updated, 25 removed. The bundle still contains 160 history
rows and 87 KB, so observed-id repair plus top-page re-export is correct and
fast enough at this scale, but replacement-page refreshes still resend enough
history to make apply cost the dominant piece.

## 2026-05-26 22:18 PDT

Optimized bundle apply to cache tx id -> tx num mappings while applying each
bundle, instead of repeatedly looking up the same transaction row for receipts
and read-set rows. The query-scope test suite passes.

Result: the effect is real but small at the current scoped-page bundle sizes.
Cold first-result moved from roughly 133-135 ms to about 130 ms, warm tab apply
from about 19.4 ms to about 18.9 ms, and mixed-mutation apply from about 34 ms
to about 32 ms. This confirms apply cost is not primarily tx lookup overhead;
the next apply targets should be per-history-row current projection writes,
row-id lookups, and query-scope repair passes.

## 2026-05-26 22:21 PDT

Scoped the apply-time rejected-current cleanup to tables touched by the bundle
instead of sweeping every schema table on every apply. The query-scope test suite
passes.

Result: the current document/org benchmark barely moves, as expected, because it
only has two tables. This is still an important product-shaped guardrail for
wide schemas: a small query refresh should not pay O(number of app tables) just
to remove rejected current rows. A dedicated wide-schema probe is still worth
adding to quantify the protected case.

## 2026-05-26 22:22 PDT

Removed the pre-history query-scope repair pass from bundle apply and kept the
post-history repair as the single source of final query-scope cleanup. This is a
semantic simplification: bundles first record observed queries, then materialize
history, then repair current state against the final local history view. The
full `whole_system` suite passes.

Result: benchmark movement is modest in the current page-shaped workload: cold
first-result stays around 130-131 ms and mixed refresh apply is around 32.5 ms.
Still, this removes a whole duplicate repair phase from every scoped apply, so
it should matter more as the number or complexity of observed queries grows.

## 2026-05-26 22:23 PDT

Added a wide-schema apply probe: 42 schema tables installed, but the synced page
touches only `documents` and `orgs`. This is the product shape behind the
touched-table rejected cleanup optimization: a screen should not pay per-table
maintenance costs for unrelated relations.

Result after the scoped cleanup and post-only repair changes: applying a
page-50 bundle with 100 history rows and 2 tx rows in the 42-table schema takes
about 19 ms, essentially the same as the narrower schema. This validates the
direction: current apply cost is dominated by rows actually synced, not by total
schema width.

## 2026-05-26 22:25 PDT

Tried caching physical row num -> public row id conversion while exporting
history records. This targets ref-heavy schemas where many rows point at the
same parent, such as document pages with repeated org/project refs. The
query-scope test suite passes.

Result: no visible win in the current benchmark; cold export remains around
71-72 ms and mixed refresh export around 18 ms. This means public ref id lookup
is not the current dominant export cost for page-sized bundles. I am keeping the
cache because it is local, simple, and should protect more ref-dense pages, but
future profiling should focus on policy dependency SQL and history row
materialization.

## 2026-05-26 22:26 PDT

Added a storage topology probe that compares the same 20k/2k/page-50 sync over
core -> edge -> worker -> tab with durable vs in-memory edge/worker
intermediaries.

Result: all-memory intermediaries take about 78 ms API-to-first-result, durable
intermediaries about 82 ms. The difference is only about 4 ms for page-sized
bundles. This suggests the current main-thread latency is dominated by SQL work
and repeated bundle apply/export mechanics rather than SQLite file I/O for
edge/worker caches.

## 2026-05-26 22:28 PDT

Added a multi-query refresh probe for a screen with four observed page queries.
The probe compares applying four refresh bundles separately with merging and
deduping them into one bundle before apply.

Result: separate bundles total about 162 KB and apply in about 40 ms; the merged
bundle is about 79 KB and applies in about 27 ms. Deduping collapses tx rows from
15 to 5 and keeps all 4 observed query facts. This is a strong architectural
signal: brokers/edges/workers should batch refreshes for a downstream peer into
one deduped apply unit whenever possible, especially for dashboards with several
live queries sharing policy dependencies and transaction metadata.

## 2026-05-26 22:30 PDT

Added apply-local caches for tx ids, branch ids, row ids, and ref row ids while
applying history records. The query-scope test suite passes.

Result: this is the largest direct runtime win so far. Cold first-result drops
from about 130-132 ms to about 116 ms, warm worker-to-tab drops from about 23 ms
to about 19 ms, refresh after 50 new top rows drops from about 175-178 ms to
about 150 ms, and mixed refresh apply drops from about 32 ms to about 24 ms.
Multi-tab fanout average tab apply falls from roughly 18.5-19 ms to about
13.3 ms. The plateau was substantially repeated identity lookup during apply.

## 2026-05-26 22:35 PDT

Added a subscription storm probe for one user with 20 live owner-filtered
top-page subscriptions over data they are allowed to read. The refresh inserts 5
new top rows per subscription, merges all refresh bundles, applies once, then
polls all subscriptions.

Result: merged refresh bundle is about 381 KB, apply takes about 82 ms, and
polling all 20 subscriptions takes about 5 ms total / 0.25 ms each. Diffs are
correct: 100 added and 100 removed. The important learning is that subscription
diffing itself is cheap; the expensive part remains applying the merged refresh
history. I initially modeled this as 20 different users in one runtime, which
correctly produced no diffs because query reads are bound to the runtime's user
context rather than storing per-query auth. That is a useful semantic reminder:
multi-user fanout belongs at the broker/edge layer, not inside one app runtime.

## 2026-05-26 22:37 PDT

Switched dynamic history/current projection inserts to `prepare_cached` so
repeated same-shape inserts reuse SQLite prepared statements. The query-scope
test suite passes.

Result: another small but consistent apply-path improvement. Cold first-result
drops to about 113 ms, refresh after 50 top rows to about 144 ms, mixed refresh
apply to about 23 ms, and subscription storm apply to about 79 ms. The biggest
apply win remains identity caching; statement caching helps most as bundle row
count grows.

## 2026-05-26 22:40 PDT

Added a branch overlay probe: 20k main rows, 2k matching owner rows, then a
`draft` branch sourced from `main` with 100 sparse overlay updates. The first
attempt created an empty branch with no source and correctly failed to update
main rows; the probe now uses an explicit source branch.

Result: main top-page query is about 0.6 ms, but branch top-page query is about
26 ms and branch export about 45 ms for a page-50 bundle with 150 history rows.
This is the clearest branch-specific performance risk found tonight. The sparse
overlay semantics work, but the current branch query path falls back to a
materialize/filter/sort style rather than a fully SQL-lowered indexed top-page
plan. This should become a top derisk target before we rely on branch-heavy
product workflows.

## 2026-05-26 22:44 PDT

Promoted the benchmark-local bundle merge helper into `mini_jazz_sqlite::sync`
and added a whole-system test that applying a merged multi-query refresh bundle
produces the same visible result as applying the individual bundles separately.

Result: this turns the multi-query perf learning into a reusable runtime/sync
primitive. The first compile caught that the crate error type does not convert
serde errors, so the merge helper now wraps JSON keying failures explicitly.

## 2026-05-26 22:47 PDT

Implemented the smallest branch top-page SQL fast path: non-pinned branch,
directly sourced from `main`, equality predicate plus ordered top-K. Harder
branch shapes still use the existing fallback. Banach's review identified the
root cause: non-main `read_rows_where_eq_top_field_desc` materialized every
matching effective row, sorted in Rust, then truncated.

Result: the branch overlay probe improves from about 26 ms query / 45 ms export
to about 1.5 ms query / 16 ms export, with the full `whole_system` suite green.
This substantially derisks the common sparse branch overlay case while leaving
pinned snapshots and multi-source merge branches as explicit future work.

## 2026-05-26 22:58 PDT

Added a profiling export path for the canonical owner-scoped top page. It breaks
export time into query, row-id resolution, repair rows, visible history, policy
dependency history, reads, transactions, branch records, and bundle creation.

The first profile showed that policy dependency export dominated the cold page:
it was about 67 ms out of a roughly 70 ms export because it rescanned all visible
children for a referenced parent table even when the query was already scoped to
50 child rows. I changed scoped policy dependency export to derive parent row
ids from the concrete child row ids by point lookup, then export just those
parent versions.

Result: primary cold export is now about 4.8 ms, API-to-first-result about
46 ms, refresh export about 7.2 ms, and the profile shows policy dependency
history at about 0.7 ms. Many-user sampled exports average about 2.35 ms.
Recursive policy export is about 4.9 ms and branch overlay export about 6.3 ms.
The full `mini-jazz-sqlite --test whole_system` suite is green.

This optimization is semantics-sensitive but currently matches the intended
scoped-sync rule: once a query has selected concrete child rows, dependency
export may use those child rows to find the referenced parents instead of
rediscovering all readable children. Future deeper policy cases should keep
testing for no leakage from hidden/unrelated repair rows.

## 2026-05-26 23:09 PDT

Added a pinned branch snapshot probe after a false start. The first version made
the branch base too early, so the branch attempted to update rows that did not
exist at its base snapshot. The fixed probe seeds 10k documents in one globally
accepted base transaction, creates a pinned branch at epoch 1, adds later main
rows that should be ignored by the pinned base, then applies a sparse branch
overlay.

Result: this is the first clearly bad branch number after the direct-main-source
fast path. For 10k base rows, 1k owner rows, 100 later main inserts, and 50
branch overlay updates, the pinned branch top-page query is about 105 ms and the
export is about 60 ms. The bundle is about 1.36 MB with 1200 history rows and
103 transactions.

Learning: pure-query pinned snapshot reads are acceptable for small historical
lookups but not yet good enough for page-shaped branch workflows. The broad
history payload is especially suspicious: the page result is 50 rows, but export
pulls the whole 1000-row owner base plus policy dependencies. This makes pinned
branch query/export the next branch-specific optimization target.

## 2026-05-26 23:15 PDT

Added an apply profiler and used it on the 20-subscription storm merged refresh
bundle. Before the small optimization, the 381 KB bundle took about 78 ms to
apply: reads were about 36.5 ms, history/current maintenance about 37.7 ms,
query-scope repair about 1.7 ms, and commit/revalidation were effectively
noise.

Applied two conservative changes: cache prepared statements for read/write-set
inserts, and cache each bundle transaction's node/outcome/conflict-mode metadata
so the history loop does not re-query it for every row.

Result: primary cold API-to-first-result improves from roughly 46-48 ms to about
43 ms, mixed mutation apply from roughly 22-23 ms to about 20.5 ms, subscription
storm apply from roughly 79 ms to about 73.6 ms. The apply profile now shows
reads still at about 36.2 ms and history/current maintenance down to about
32.9 ms.

Learning: per-row transaction metadata was a real but modest cost. The remaining
apply bottleneck is mostly raw read-set insertion volume plus per-history-row
current maintenance/newest checks. The next serious apply optimization is likely
bulk read-set insert and/or bulk current repair by touched `(table, row,
branch)`, not more tiny statement-cache tweaks.

## 2026-05-26 23:19 PDT

Added file-footprint stats to `StorageStats` so the benchmark can distinguish
SQLite page-count bytes from actual main/WAL/SHM files. This matters before
testing WAL, page size, and cache pragmas because `PRAGMA page_count * page_size`
can otherwise hide WAL overhead.

Current default result on a 1k-row scaled-down smoke run: file-backed core,
edge, and worker report matching page-count and file bytes; in-memory tab
reports page-count bytes but zero file bytes. This confirms the stat is useful
and does not change semantics. The full whole-system suite remains green.

## 2026-05-26 23:24 PDT

Added env-driven SQLite tuning pragmas: page size, cache size, journal mode,
synchronous mode, and temp store. Ran a small whole-harness matrix with 1k
primary rows but the heavier fixed probes still enabled:

- default: primary first result ~41.3 ms, refresh ~46.2 ms, core file ~500 KB,
  edge/worker file ~139 KB, storm apply ~72.9 ms, pinned query ~107 ms
- WAL + synchronous=NORMAL: primary ~40.6 ms, refresh ~45.4 ms, but core file
  footprint ~1.79 MB and edge/worker ~432 KB because WAL/SHM bytes are now
  counted
- 1 KiB pages: primary ~42.7 ms, refresh ~47.9 ms, core file ~420 KB,
  edge/worker ~74 KB, storm apply ~75 ms, pinned query ~111 ms
- 8 KiB pages: primary ~43.1 ms, refresh ~47.5 ms, core file ~590 KB,
  edge/worker ~205 KB, storm apply ~74.7 ms, pinned query ~106 ms

Learning: WAL/NORMAL is a small latency win in this harness but a big file
footprint increase unless we checkpoint/truncate aggressively. 1 KiB pages save
disk for small cached subsets but slightly hurt latency. Default 4 KiB pages
still look like the best general default for now. Page-level compression is not
available directly through SQLite here; if we want compression, it likely has to
come from VFS/page-layer choices later rather than payload-level JSON
compression.

## 2026-05-26 23:29 PDT

Added layout toggles for current tables and indexes, then compared:

- default rowid current table: primary ~41.4 ms, core file ~500 KB, edge ~139
  KB, storm apply ~74 ms, branch query ~1.37 ms, pinned query ~106 ms
- branch-first current indexes: slightly smaller branch query (~1.21 ms) but
  slightly worse primary and no storage win
- current table `WITHOUT ROWID`: primary ~41.2 ms, core file ~487 KB, edge ~127
  KB, mixed apply roughly flat, pinned query noisy/slightly worse
- branch-first primary key + `WITHOUT ROWID` + branch-first indexes: no clear
  win and storm apply got worse in the sample

Decision: make current projection tables `WITHOUT ROWID` by default with the
existing `(row_num, j_branch_num)` primary-key order. It saves disk for cached
subsets without meaningful latency cost in the page workload. Keep branch-first
current indexes and branch-first primary-key order as env experiments for now;
they are not clearly better.

## 2026-05-26 23:36 PDT

Profiled pinned branch export and found a worse sync amplification than the
history count alone suggested. A 50-row pinned page exported 150 history rows
but 10,150 read rows because one globally accepted base transaction inserted
10k rows, and exporting any writes from that tx pulled the whole transaction
read set. That made the bundle about 1 MB even after history was scoped.

Changed read-set export to keep absent-row reads only for `(tx, table, row)`
keys that are also present in the exported history, while preserving non-absent
reads. This keeps policy/update evidence but avoids unrelated create-absence
reads from the same large transaction. The full whole-system suite remains
green.

Result: the pinned branch profiled bundle drops from about 1.0 MB / 10,150 read
rows to about 57 KB / 150 read rows. Export time is still about 131 ms because
the pinned branch query itself takes about 107 ms and `export_reads_for_history`
still scans then filters the broad tx read set in Rust. The next pinned-branch
work is SQL-lowered snapshot top-K plus SQL-filtered read-set export.

Tried pushing the read-set filter into SQL with a `VALUES` CTE of exported row
keys. It preserved the tiny bundle and kept downstream apply fast, but made the
pinned export much slower (~280 ms total, ~169 ms in read export), likely
because SQLite used a poor plan against the large read set. Reverted to the Rust
filter for now. If we want SQL-side filtering, use a real temporary table or a
persistent export-scope table, not a giant inline CTE.

## 2026-05-26 23:45 PDT

Added a SQL-lowered top-K equality query for pinned-base branches with no extra
source branches. It unions a small current-branch overlay with the latest main
history rows at `base_global_epoch`, excludes branch-shadowed base rows, applies
snapshot policy to base rows, and orders/limits in SQLite.

Regression covered: top query over a pinned branch must combine base rows and a
sparse overlay, ignore later main rows beyond the base epoch, and preserve
deterministic ordering.

Result on the pinned snapshot probe: branch query dropped from roughly 107 ms to
~5.9 ms; profiled export dropped from roughly 132 ms to ~30 ms. The remaining
export cost is now dominated by read-set export (~20 ms), not query evaluation.
Full whole-system tests remained green.

## 2026-05-26 23:50 PDT

Optimized bundle apply current-maintenance conservatively. First, skipped the
rejected-current cleanup scan entirely for bundles that do not carry rejected
transaction fates. Second, added a current-projection comparison fast path:
when an incoming history row competes with an existing current row and both
transactions have the same global-epoch shape, compare against the current
visible tx directly instead of scanning all history for a newer version.

Important semantic catch: edge-accepted transactions with no global epoch can
still supersede a globally accepted current row in current prototype semantics.
A first attempt at direct global-epoch ordering broke
`edge_accepted_remote_pending_update_repairs_peer_current_projection`, so the
fast path now falls back to the existing full scan whenever one side has a
global epoch and the other does not.

Result on the apply profile probe: apply dropped from ~42.7 ms to ~37.8 ms,
with `history_ms` dropping from ~32.5 ms to ~28.1 ms and rejected cleanup from
~0.16 ms to effectively zero. This is a useful win but confirms that dynamic
per-row history/current inserts remain the larger apply cost.

## 2026-05-26 23:53 PDT

Tried a naive opt-in mirror index on history tables for declared user indexes:
`(j_branch_num, op, indexed_columns...)`. It was a bad shape for the pinned
snapshot top-K query. The pinned query regressed from ~5.9 ms to ~970 ms, and
profiled export regressed from ~30 ms to ~1052 ms. SQLite likely chose the new
index for the base snapshot arm even though `op != 3`, latest-version
anti-joins, and snapshot policy made it a poor plan.

Decision: do not keep naive history mirror indexes. If we revisit history
indexes, use `EXPLAIN QUERY PLAN` against the exact snapshot CTEs and try a
more targeted shape, likely involving predicate/order columns without leading
low-selectivity `op`, or partial indexes if the embedded targets support them
well enough.

## 2026-05-26 23:56 PDT

Added observability for the next layout experiments: `StorageStats` now reports
per-object page bytes from SQLite `dbstat` when available, and the perf harness
reports process RSS at start/end. On the small 1k-row smoke run, `dbstat` is
available and shows object-level entries such as
`documents__schema_v1_history`, `documents__schema_v1_current`, and
`documents_current_owner_updated`.

The RSS number is coarse process-level memory, not per-node memory, but it is
good enough to catch large representation regressions while we run full-topology
benchmarks.

## 2026-05-27 00:05 PDT

Replaced textual table names in `jazz_tx_read` and `jazz_tx_write` with small
integer `table_num`s backed by a `jazz_table` catalog. This directly tests the
"tiny system representation" hypothesis for read/write sets, which showed up in
`dbstat` as a meaningful part of core storage.

First version was semantically green but slowed apply because every read/write
record did a catalog lookup. Added table-number caching for bundle apply/import
paths and numeric write/read recording helpers.

Result on the 100k-row primary core: `jazz_tx_read` dropped from ~2.71 MB to
~1.68 MB and `jazz_tx_write` from ~2.48 MB to ~1.45 MB, with only a 4 KiB
`jazz_table` catalog. Total core file footprint dropped from ~37.7 MB to
~35.7 MB. After caching, apply profile is roughly back to the optimized
baseline (~38.1 ms total, `history_ms` ~27.8 ms, `reads_ms` ~6.6 ms).

Decision: keep table numbers for tx read/write sets. Do not yet convert
`jazz_row_id.table_name`; that table has different public identity semantics
and its unique row-id index is the larger share of its footprint.

## 2026-05-27 00:10 PDT

Revisited SQL-side read-set filtering using real temporary scope tables instead
of the earlier inline `VALUES` CTE. This works: the pinned branch snapshot
export keeps the same 150 read rows but `reads_ms` drops from ~20.0 ms to ~8.8
ms, and total profiled export drops from ~30.3 ms to ~18.7 ms.

The same broad perf run showed primary first-result/refresh numbers noisier and
slower (~51 ms / ~70 ms in that sample), so this may need a heuristic later
rather than unconditional temp-table use for every scoped export. For now the
code keeps the temp-table path as the default because it fixes the worst
observed broad-transaction read-set case, keeps whole-system tests green, and
the next step is to collect more scenario-specific numbers before deciding
whether to gate it by scope size or broad-read detection.

## 2026-05-27 00:12 PDT

Made read-set export adaptive instead of always using temp scope tables. It now
counts candidate read rows for the exported transactions and uses the simple
query/Rust filter for ordinary small read sets, switching to temp scope tables
only when candidate reads are much larger than exported history.

Result: ordinary export profile stays cheap (`reads_ms` ~0.6 ms for 100 reads),
while the pinned broad-read case keeps most of the temp-table win (`reads_ms`
~9.3 ms, total export ~19.1 ms). Full whole-system tests remain green. The
primary refresh sample was still noisy/high in one run, so more repeated
scenario sampling is needed before calling the heuristic final.

## 2026-05-27 00:13 PDT

Bumped the prototype storage format from 4 to 5 after changing the physical
layout of `jazz_tx_read` and `jazz_tx_write` from table names to table numbers.
The storage-format tests now assert version 5 and reject future version 6. This
keeps durable spike databases honest while we are still free to break format
compatibility between experiments.

## 2026-05-27 00:17 PDT

Repeated the key perf sample three times after adaptive read-set export:

- default temp store: first result ~43.0 / 38.5 / 39.5 ms, refresh ~60.2 /
  58.5 / 58.6 ms, pinned export ~18.9 / 19.7 / 19.6 ms, apply ~38.0 / 40.1 /
  38.4 ms
- `MINI_JAZZ_SQLITE_TEMP_STORE=MEMORY`: first result ~41.1 / 38.7 / 38.6 ms,
  refresh ~58.2 / 60.3 / 58.4 ms, pinned export ~19.2 / 19.5 / 19.6 ms, apply
  ~38.5 / 39.2 / 38.7 ms

Learning: the scary ~136 ms refresh sample was noise. Adaptive read-set export
looks stable around the earlier baseline for ordinary top-page work while
keeping the pinned branch broad-read fix. `temp_store=MEMORY` does not matter
enough to default.

## 2026-05-27 00:21 PDT

Removed redundant `table_name` storage from `jazz_row_id`. Row ids are already
globally unique in the model, and the remaining table-scoped id repair/export
queries can derive their scope from the per-table history/current tables.

Result on the 100k-row primary core: total file footprint dropped from ~35.7 MB
to ~34.6 MB. `jazz_row_id` dropped from ~2.8 MB to ~1.8 MB while the unique
row-id index stayed ~1.95 MB. First-result/refresh/apply remained in the normal
range (~38.8 ms, ~56.9 ms, ~38.2 ms). Whole-system tests remained green.

This means row identity is now much closer to the spec's "globally unique row
id" assumption physically as well as semantically. Bumped storage format again
from 5 to 6.

## 2026-05-27 00:24 PDT

Ran a release-mode full perf sample to separate debug overhead from the actual
shape:

- primary 100k-row core-only cold page: seed ~3.8 s, API-to-first-result ~11.2
  ms, refresh ~15.4 ms, core file ~34.6 MB
- many-user permissioned page: 50k-row seed ~1.85 s, average export over 20
  users ~0.87 ms
- pinned branch snapshot export: ~7.5 ms total, with read-set export ~3.65 ms
- apply profile probe: ~9.4 ms
- mixed mutation refresh: export ~2.6 ms, apply ~3.6 ms

Learning: debug numbers are useful for relative deltas, but the release numbers
are much more encouraging for the product story. The remaining obvious
non-release outlier is bulk seeding/import speed, not steady-state page read,
permissioned export, or client apply.

## 2026-05-27 00:27 PDT

Optimized bulk creates by teaching row-id allocation to report whether the row
id was newly inserted. For brand-new row ids, create transactions can record the
absence read directly instead of probing current state first. If a row id was
already allocated, the old current-state read remains in place, preserving
staleness semantics for references to future rows and upsert-like cases.

Release seed numbers improved:

- primary 100k-row seed: ~3.80 s -> ~3.58 s
- many-user 50k-row seed: ~1.85 s -> ~1.58 s
- 5k rows batched by 100: ~461 ms debug earlier / now ~143 ms release
- 5k one-write-per-row: still ~3.47 s release, confirming transaction
  granularity dominates that case

Full whole-system tests remained green.

## 2026-05-27 00:30 PDT

Tried changing `jazz_row_id` to a `WITHOUT ROWID` table keyed by public row id,
with `row_num` assigned manually. The hope was to remove one btree from the row
identity mapping after we had already dropped table names.

Result: worse on both space and speed. On the paired release sample, the current
`row_num INTEGER PRIMARY KEY, row_id UNIQUE` layout used ~34.6 MB total with
`jazz_row_id` ~1.8 MB and the row-id index ~1.95 MB. The `WITHOUT ROWID` shape
used ~34.9 MB total with the table ~1.95 MB and the row-num index ~2.0 MB, and
seed/page timings also regressed. Reverted the hook and kept the row-num-primary
layout.

Learning: for our hot lookup directions we need both public-row-id -> row-num
and row-num -> public-row-id. SQLite's ordinary integer primary key table is the
right primitive here for now.

## 2026-05-27 00:34 PDT

Added a user-id footprint probe to quantify the cost of storing repeated user ids
as text in both user-visible fields and row system columns before implementing a
user-id interning table.

Release sample with 100 users x 200 rows:

- short ids (`user-0` shape, representative 6 bytes): ~7.68 MB core database,
  current table ~2.48 MB, history table ~2.48 MB
- long ids (representative 74 bytes): ~12.45 MB core database, current table
  ~4.08 MB, history table ~4.08 MB
- delta: ~238 additional database bytes per logical row for +68 bytes of user-id
  payload, because each id is repeated through app owner fields and row system
  metadata in both current and history

Learning: user-id interning is likely a real storage win for production-shaped
JWT/account ids, but it will touch query lowering, policy SQL, sync encoding, row
materialization, and the current/history write paths. This is worth trying with
tests rather than assuming text ids are good enough.

## 2026-05-27 00:36 PDT

Sampled SQLite page-size tuning on a 50k-row release profile with the existing
`MINI_JAZZ_SQLITE_PAGE_SIZE` hook:

- 4096 bytes: core ~17.21 MB, first result ~11.39 ms, refresh ~15.65 ms,
  pinned branch export ~7.34 ms
- 8192 bytes: core ~17.31 MB, first result ~11.41 ms, refresh ~15.97 ms,
  pinned branch export ~7.18 ms
- 16384 bytes: core ~17.55 MB, first result ~13.59 ms, refresh ~16.16 ms,
  pinned branch export ~7.40 ms

Learning: larger pages do not look like an easy win for this shape. The default
4K page size is smallest and at least as fast for client-perceived page work.
SQLite core itself also does not give us transparent page compression as a
portable pragma; useful compression likely belongs either below SQLite in a VFS
or at the transport stream layer.

## 2026-05-27 00:39 PDT

Added a raw SQLite projection probe for system-user interning. This does not
change the runtime yet; it compares two equivalent physical projections with
long production-shaped user ids:

- text system users: store `j_created_by`/`j_updated_by` as text on each row
- interned system users: store `j_created_by_num`/`j_updated_by_num` and join a
  tiny `jazz_user` table when materializing rows

Release sample with 100 users x 500 rows:

- text: ~24.50 MB, seed ~197 ms, materialize 50-row page ~0.085 ms
- interned: ~16.71 MB, seed ~161 ms, materialize 50-row page ~0.091 ms
- savings: ~156 bytes per row for system metadata alone, while app-level
  `owner_id` remains text

Learning: a narrow runtime implementation that interns only system users looks
worth trying. It should leave user/app fields alone, preserve string bundle and
public API boundaries, and translate only in storage/query/policy lowering.

## 2026-05-27 00:44 PDT

Implemented the narrow system-user interning slice in the runtime: added
`jazz_user(user_num, user_id)`, changed per-row system metadata
`j_created_by`/`j_updated_by` to integer user nums, and kept public rows,
history bundles, auth metadata, and app-level fields such as `owner_id` as
strings. Bumped storage format to 7.

Coverage: full `cargo test -p mini-jazz-sqlite` is green: 284 whole-system tests
passed. Added explicit storage-shape coverage that verifies `j_created_by` is
INTEGER while an app `owner_id` field remains TEXT, and existing `$createdBy`,
policy, subscription repair, sync fate, branch, recursive query, and schema lens
tests continued to pass.

Release perf after the runtime change on the 100k primary profile:

- core file footprint: ~34.6 MB before -> ~29.3 MB after
- documents current table: ~11.6 MB before -> ~8.9 MB after
- documents history table: ~11.6 MB before -> ~8.9 MB after
- first result: ~11.2 ms before -> ~12.6 ms after in this run
- refresh: ~15.4 ms before -> ~17.7 ms after in this run
- many-user 50k core footprint: ~19.2 MB before -> ~16.5 MB after, average
  export still ~0.89 ms

Learning: system-user interning is a real storage win and the semantic surface
can stay clean. There is a small release latency regression in this single run,
likely from scalar subqueries used to materialize user ids; a next optimization
candidate is joining or caching user ids in hot row materialization/export paths
if repeated samples confirm the regression.

## 2026-05-27 00:46 PDT

Added a permissioned dashboard perf probe that combines several previously
separate dimensions: recursive read policy (`documents -> projects -> teams`),
24 concurrent top-page subscriptions, a full core -> edge -> worker -> memory-tab
topology, merged initial bundles, and refresh after new top rows.

Release sample:

- seed: 50k recursive-policy documents in ~1.74 s
- initial core export for 24 pages: ~86.9 ms total, merged bundle ~86 KB,
  188 history rows and 7 transactions after dedupe
- apply: edge ~8.0 ms, worker ~7.2 ms, tab ~5.8 ms
- subscribe existing local pages: ~1.8 ms for 24 subscriptions
- refresh export after 72 inserted top rows: ~93.4 ms, tab apply ~8.1 ms,
  polling all subscriptions ~2.4 ms

Learning: the whole-topology subscription side looks cheap once data is scoped,
but recursive-policy export across many query descriptors is now a visible
server-side cost. This is a good benchmark for future policy-dependency and
multi-query export dedupe work.

## 2026-05-27 00:50 PDT

Tried replacing SQL scalar user-id materialization with Rust-side lookup: select
integer `j_created_by`/`j_updated_by` values and turn them back into strings at
row/history materialization time.

Result: not better in the current shape. The full test suite stayed green, but
the 100k release sample was slightly slower again (first result ~13.0 ms,
refresh ~18.2 ms, dashboard export ~89-95 ms). The likely cause is that the
naive Rust path did many tiny lookup queries without a shared cache, while the
SQL scalar subqueries stay inside one SQLite statement.

Decision: reverted the uncommitted Rust lookup experiment. If we optimize this
later, use explicit joins or a per-query user cache, not per-row lookup queries.

## 2026-05-27 00:52 PDT

Tried replacing recursive policy dependency parent lookup from one prepared query
per child row with a single `IN (...)` query per policy level.

Result: major regression on the permissioned dashboard release sample. Initial
core export for 24 recursive-policy pages went from ~87 ms to ~382 ms, and
refresh export similarly went to ~388 ms. The targeted recursive single-query
probe stayed fine, which means the regression is specific to the many-query
policy export shape and SQLite's plan for this `IN` + branch filter query.

Decision: reverted the uncommitted batched-`IN` change. The simple prepared
point lookup is not pretty, but it is currently the faster path. Future work
should inspect `EXPLAIN QUERY PLAN` before changing this again; a temp table of
child row nums or a different composite index may be a better batched shape.

## 2026-05-27 00:58 PDT

Added a project-board shaped perf probe and the missing runtime API it wanted:
`export_query_where_eq_top_field_desc_with_ref_include`. The probe models orgs,
members, projects, tasks, and comments with task read policy flowing through
`task -> project -> org`, and exports 10 assignee pages with project includes.

First unpaged version was intentionally too blunt: 10 users x all matching tasks
exported ~4k task rows, produced a ~1.7 MB bundle, and took ~135 ms to apply on
the tab. This proved the stress path but not the product path.

Paged version with 10 users x 40 tasks:

- seed: 50 members, 100 projects, 20k tasks, 2k comments in ~0.79 s
- export: ~89 ms
- merged bundle: ~188 KB, 421 history rows, 21 transactions
- tab apply: ~14.2 ms
- local tab query over the 10 pages: ~2.9 ms
- tab DB footprint: ~287 KB

Learning: page-bounded board views are plausible, but multi-query export with
policy dependencies is still the visible server-side cost. Also, adding small
missing product-shaped APIs while building benchmarks is paying off: the new
paged ref-include export has a focused whole-system test.

## 2026-05-27 01:00 PDT

Added a batched export API for a common dashboard shape: many top-field pages on
the same table/predicate/order with different equality values and the same ref
include. The API reads each page separately but exports visible history, policy
dependencies, ref includes, read sets, txs, branches, and query descriptors as
one bundle before serialization.

Added a whole-system equivalence test against applying individual exports for
the same two query descriptors.

Project-board release sample improved:

- paged individual exports: ~89 ms export for 10 assignee pages
- batched export: ~66 ms for the same 10 pages
- bundle size/rows unchanged: ~188 KB, 421 history rows, 21 txs
- tab apply unchanged around ~14-15 ms, as expected

Learning: batching before export-time dependency/read/tx collection is a real
win and maps to product behavior (`useCoStates`/multiple subscriptions booting
together) better than just merging bundles after the fact.

## 2026-05-27 01:03 PDT

Added a branch fan-in perf probe: seed 5k rows, create 100 source branches with
sparse one-row overlays, then create a fan-in branch over 20 sources and run a
50-row top-page query/export from that branch.

Release sample:

- creating 100 source branches with one update each: ~237 ms total
- creating/checking out the 20-source fan-in branch: ~16 ms
- fan-in branch top-page query: ~2.36 ms
- fan-in branch export: ~5.4 ms, ~51 KB bundle, 120 history rows, 23 txs

Learning: this branch shape is encouraging. Query/export over a 20-source sparse
fan-in branch is not remotely the scary part at these cardinalities; branch
metadata/source maintenance and larger fan-in counts are the next things to
scale if needed.

## 2026-05-27 01:06 PDT

Sampled SQLite cache-size tuning on a 50k-row release profile:

- default-ish `cache_size=2000`: first ~12.8 ms, refresh ~17.9 ms, dashboard
  export ~88.3 ms, board export ~58.1 ms, RSS delta ~30.6 MB
- `cache_size=-8000` (~8 MB): first ~12.5 ms, refresh ~17.6 ms, dashboard
  export ~87.9 ms, board export ~58.2 ms, RSS delta ~30.4 MB
- `cache_size=-32000` (~32 MB): first ~12.4 ms, refresh ~18.0 ms, dashboard
  export ~89.2 ms, board export ~59.8 ms, RSS delta ~55.9 MB

Learning: larger SQLite page cache does not materially improve the current
benchmarks, and the 32 MB setting just spends memory. Default/small cache is a
reasonable posture for now; our bottlenecks are query/export shape, not page
cache starvation.

## 2026-05-27 01:08 PDT

Sampled journal/synchronous modes on the 50k-row release profile.

With `synchronous=OFF`:

- DELETE: seed ~1.64 s, first ~12.7 ms, refresh ~17.6 ms, core files ~14.6 MB
- WAL: seed ~1.12 s, first ~10.9 ms, refresh ~15.7 ms, core files ~18.2 MB
- MEMORY: seed ~1.04 s, first ~11.2 ms, refresh ~16.4 ms, core files ~14.6 MB
- OFF: seed ~1.02 s, first ~11.1 ms, refresh ~16.7 ms, core files ~14.6 MB

With `synchronous=NORMAL`:

- DELETE: seed ~1.74 s, first ~12.7 ms, refresh ~17.6 ms
- WAL: seed ~1.19 s, first ~11.3 ms, refresh ~16.2 ms

Learning: WAL is a real speed win for file-backed durable nodes even with
`NORMAL`, at the cost of WAL file footprint. MEMORY/OFF are useful only as
unsafe benchmark lower bounds. We should likely default durable worker/edge/core
SQLite connections toward WAL+NORMAL once the prototype stops needing exact file
size comparability on every run.

## 2026-05-27 01:09 PDT

Changed file-backed SQLite storage to default to WAL journal mode with
`synchronous=NORMAL`; memory storage remains unchanged and env vars still
override both pragmas.

Validation:

- storage projection tests pass
- 50k release sample with defaults now matches the WAL+NORMAL sample: seed
  ~1.19 s, first result ~11.2 ms, refresh ~16.0 ms, core files ~18.2 MB
- dashboard export in the same run ~83.4 ms, board batched export ~59.4 ms

Learning: this is the first SQLite tuning knob that looked worth making default
for durable nodes. The cost is extra WAL footprint, so storage stats must keep
reporting total file bytes, not only main database page bytes.

## 2026-05-27 01:12 PDT

Added a focused repeat mode to the perf harness:
`MINI_JAZZ_PERF_REPEAT_PRIMARY=N` runs only the primary whole-topology scoped
page scenario N times and reports samples plus medians. This avoids rerunning all
secondary probes when we only need a stable comparison for core page/read/apply
work.

Quick release validation with 20k rows x 3 repeats produced stable medians around
~10.9 ms first result and ~15.7 ms refresh on the WAL default path.

## 2026-05-27 01:12 PDT

Ran a current full 100k-row release profile after system-user interning, batched
board export, branch fan-in probe, and WAL defaults.

Primary scoped-page profile:

- raw synthetic JSON payload estimate: ~9.93 MB
- SQLite main database bytes: ~29.31 MB (~2.95x raw)
- total core files including WAL/SHM: ~33.18 MB
- seed: ~2.49 s
- first result across core -> edge -> worker -> tab: ~11.34 ms
- refresh after new top rows: ~16.29 ms

Largest page users in the core database:

- documents current: ~8.91 MB
- documents history: ~8.91 MB
- current owner/updated index: ~4.45 MB
- row-id unique index: ~1.95 MB
- row-id table: ~1.79 MB
- tx read set: ~1.68 MB
- tx write set: ~1.45 MB

Learning: after the easy metadata wins, the dominant overhead is exactly where
expected: current/history duplication plus the app index. Read/write-set and id
catalog overhead is now secondary. Further large storage wins require either
compressing/co-locating SQLite pages below SQLite, changing projection strategy,
or reducing duplicated app/system columns in current/history, not shaving small
system tables.

## 2026-05-27 01:14 PDT

Added a raw SQLite current-projection tradeoff probe. It compares a simplified
`docs_history + docs_current` layout against history-only latest-row querying for
100k rows plus 10k updates.

Release sample:

- current projection: ~23.7 MB, seed/build ~419 ms, top page query ~0.062 ms
- history-only: ~13.2 MB, seed ~264 ms, latest top page query ~0.078 ms
- storage saved without current: ~10.5 MB
- simple query slowdown: only ~1.26x in this narrow shape

Learning: this is more nuanced than expected. For a simple indexed current-page
query with only one update wave, history-only can be quite fast and saves a lot
of space. This does not invalidate the main-branch current projection yet,
because the real runtime also needs policy filtering, rejected/pending fate,
branch overlays, repair/export, and arbitrary update depth. But it makes a
hybrid strategy worth exploring: current projection for hot/main tables or hot
queries, history-only for cold tables/snapshots, or lazily maintained projections.

## 2026-05-27 01:16 PDT

Extended the raw current-projection tradeoff probe with a deeper history-only
case: 20k logical rows and 100k update versions. The indexed latest-row top-page
query still stayed fast in the raw model (~0.066 ms in the sample), while the
100k-row shallow current-vs-history comparison remained around ~0.058 ms current
projection vs ~0.075 ms history-only.

Learning: for one very narrow query shape, a good history index can make
history-only reads surprisingly competitive even with deeper version chains. The
open risk shifts from "can SQLite query history fast at all?" to "can we encode
all Jazz visibility semantics, policy dependencies, branches, rejected fates,
and repair/export over history-only without complex plans or many bespoke
indices?" This deserves a later focused attempt rather than an assumption.

## 2026-05-27 01:17 PDT

Extended the mixed mutation refresh probe to include deletes of rows from the
currently observed page, not only inserts and updates.

Release sample for the refreshed probe:

- mutation mix: 25 top inserts, 10 current-page updates, 5 current-page deletes,
  100 off-page owner updates, 100 unrelated updates
- refresh bundle: ~81 KB, 160 history rows, 5 txs
- export: ~2.0 ms, apply: ~4.4 ms, local query: ~0.21 ms, subscription poll:
  ~0.22 ms
- semantic diff: 25 added, 10 updated, 25 removed. The removals are larger than
  the explicit delete count because top inserts also displace old page-boundary
  rows.

Learning: page-boundary churn is the right thing to measure for subscriptions;
explicit deletes are only one cause of removals.

## 2026-05-27 01:19 PDT

Added apply-profile output to the project-board probe. Release sample for the
~188 KB / 421-history-row board bundle:

- total tab apply: ~14.5 ms
- history insertion/current maintenance: ~10.8 ms
- query-scope repair: ~2.1 ms
- read-set writes: ~1.2 ms
- tx insertion: ~0.27 ms
- validation/branches/query-read bookkeeping/commit/revalidate are all tiny

Learning: for medium page bundles, apply cost is dominated by applying history
records and maintaining current state. Query-scope repair is visible but
secondary. This points future apply optimization toward prepared dynamic inserts,
current maintenance batching, and avoiding unnecessary current probes rather than
transaction metadata work.

## 2026-05-27 01:21 PDT

Cached `jazz_user` lookups during bundle apply. Before this, every history
record resolved `created_by` and `updated_by` through `INSERT OR IGNORE` +
`SELECT`, even when hundreds of rows shared the same author.

Board apply profile improved in the release sample:

- total apply: ~14.5 ms -> ~12.8 ms
- history phase: ~10.8 ms -> ~9.3 ms
- other phases stayed roughly unchanged

Learning: small storage normalization choices need matching apply-time caches.
The remaining history cost is now less about user metadata lookup and more about
dynamic history/current insertion and visibility maintenance.

## 2026-05-27 01:26 PDT

Generalized the batched same-shape top-page export helper so it works without a
ref include, added a whole-system equivalence test against individual exports,
and switched the permissioned dashboard initial sync through core -> edge ->
worker -> tab to use the batched export at each hop.

Release sample for the 50k-row / 24-query permissioned dashboard probe:

- initial core export: previously in the ~80-90 ms band, now ~5.0 ms
- merged initial bundle: ~86 KB, 188 history rows, 7 transactions
- edge apply: ~5.8 ms; worker apply: ~5.7 ms; tab apply: ~5.4 ms
- refresh export after subscribed mutations is still ~87.8 ms

Learning: the old dashboard initial export number was dominated by per-query
export/finalization overhead rather than SQLite row lookup. Batching same-shape
query descriptors is a core primitive, not merely a benchmark trick. The next
visible dashboard bottleneck is refresh over persisted observed query reads.

## 2026-05-27 01:29 PDT

Made `export_query_read_refreshes` batch compatible observed
`eq_top_field_desc` reads by table/field/order/limit and carry each read's
previous observed row IDs into the batched repair set. Added a whole-system test
that proves two same-shape observed page queries refresh as a single bundle and
still remove displaced old page rows.

Release sample for the same 50k-row / 24-query dashboard probe:

- initial core export: ~5.0 ms
- refresh export after subscribed mutations: ~87.8 ms -> ~6.6 ms
- refresh apply: ~6.4 ms

Learning: observed query refresh must be a batched operation. Replaying each
query descriptor independently reproduces the exact perf trap that hurt initial
sync, just later in the reconnect/subscription lifecycle.

## 2026-05-27 01:33 PDT

Added a dashboard query-count scaling probe over the same recursive-policy
schema and 50k-row core. It reuses one seeded core and measures fresh memory
tabs subscribing to 1, 4, 12, 24, and 48 owner pages, then refreshes each tab
after one new top row per subscribed owner.

Release sample:

- 1 query: initial export ~1.0 ms, refresh export ~1.0 ms, refresh apply ~1.0 ms
- 4 queries: initial export ~4.9 ms, refresh export ~4.9 ms, refresh apply ~1.5 ms
- 12 queries: initial export ~6.0 ms, refresh export ~6.6 ms, refresh apply ~2.7 ms
- 24 queries: initial export ~4.8 ms, refresh export ~5.6 ms, refresh apply ~4.5 ms
- 48 queries: initial export ~8.4 ms, refresh export ~10.4 ms, refresh apply ~8.3 ms

Learning: after batching, dashboard perf scales with delivered rows/history and
not catastrophically with the number of logical page subscriptions. The
non-monotonic export timings suggest cache/planner noise at this size, so this
probe should eventually gain repeats/medians.

## 2026-05-27 01:34 PDT

Ran a larger release sample with `MINI_JAZZ_PERF_TOTAL_ROWS=200000` and
`MINI_JAZZ_PERF_TARGET_OWNER_ROWS=20000`. Important caveat: the primary scenario
honors those env vars, while the dashboard/project-board probes currently use
fixed local sizes.

Primary result at 200k rows:

- seed: ~5.38 s
- first client-visible page: ~10.6 ms
- refresh after new top rows: ~13.9 ms
- core main DB: ~59.0 MB; core files including WAL: ~63.8 MB

The fixed-size dashboard numbers stayed near the previous run:

- 24-query dashboard initial export: ~4.6 ms
- 24-query dashboard refresh export: ~6.6 ms
- 48-query scaling refresh export: ~10.5 ms

Learning: the primary current-projection indexed page path remains stable at
200k rows. The fixed-size dashboard probes should grow env knobs or a separate
large-dashboard mode before we claim anything about 200k-row recursive-policy
dashboard behavior.

## 2026-05-27 01:36 PDT

Added dashboard-specific env knobs:

- `MINI_JAZZ_PERF_DASHBOARD_TOTAL_ROWS`
- `MINI_JAZZ_PERF_DASHBOARD_TARGET_OWNER_ROWS`
- `MINI_JAZZ_PERF_DASHBOARD_QUERY_COUNT`
- `MINI_JAZZ_PERF_DASHBOARD_PAGE_SIZE`

Then ran the recursive-policy dashboard probes at 200k rows / 20k target-owner
rows.

24-query full topology sample:

- seed: ~5.65 s
- initial core export: ~25.8 ms
- initial bundle: ~231 KB, 487 history rows, 20 transactions
- edge/worker/tab apply: ~13-14 ms each
- refresh export: ~13.1 ms
- refresh apply: ~12.3 ms
- subscription poll: ~3.2 ms

Scaling sample at 200k:

- 1 query: refresh export ~1.1 ms, apply ~1.1 ms
- 4 queries: refresh export ~17.1 ms, apply ~2.2 ms
- 12 queries: refresh export ~21.3 ms, apply ~5.8 ms
- 24 queries: refresh export ~12.5 ms, apply ~10.5 ms
- 48 queries: refresh export ~22.7 ms, apply ~19.4 ms

Learning: the 200k recursive-policy dashboard path is still usable but no longer
"basically free". Export timing is noisy and sometimes non-monotonic, likely
from cache shape and shared policy dependency reuse. Apply scales more
predictably with history rows. Repeats/medians and explain plans are now worth
adding for this probe.

## 2026-05-27 01:38 PDT

Added `MINI_JAZZ_PERF_REPEAT_DASHBOARD_SCALING=N`, an early-exit repeat/median
mode for the dashboard query-count scaling probe.

Two-sample 50k-row median smoke run:

- 1 query: refresh export ~1.0 ms, apply ~1.1 ms
- 4 queries: refresh export ~4.7 ms, apply ~1.6 ms
- 12 queries: refresh export ~6.3 ms, apply ~3.1 ms
- 24 queries: refresh export ~6.1 ms, apply ~4.9 ms
- 48 queries: refresh export ~10.8 ms, apply ~8.3 ms

Learning: even tiny repeats make this probe much easier to reason about. The
next useful refinement is probably explain-plan capture for the cases where
export timing jumps with only a modest increase in delivered rows.

## 2026-05-27 01:40 PDT

Changed the permissioned dashboard refresh probe to propagate through the full
core -> edge -> worker -> tab topology instead of refreshing core -> tab
directly. This follows Zeno's review note and better matches the browser +
cloud setup we care about.

50k-row / 24-query full-topology refresh sample:

- core refresh export: ~6.7 ms
- edge refresh apply: ~7.1 ms
- edge refresh export: ~6.3 ms
- worker refresh apply: ~7.9 ms
- worker refresh export: ~5.9 ms
- tab refresh apply: ~6.5 ms
- subscription poll: ~2.2 ms
- refresh bundle from core: ~139 KB, 309 history rows, 8 transactions

Learning: batching keeps every hop healthy, but the topology multiplies the
budget. The useful product number is not just one export/apply pair; it is the
sum of each durable cache's refresh/export and downstream apply. Future probes
should report both per-hop and end-to-end API-to-visible-result latency.

## 2026-05-27 01:44 PDT

Added a recursive tree subscription probe over a `folders(parent)` schema with
created-by read policy. The probe cold-loads a recursive query rooted at
`folder-0`, subscribes to the observed recursive query, then applies a mutation
mix: 25 added descendants, 25 renamed descendants, 10 deleted descendants, and
10 reparented descendants.

Important implementation note: seeding had to run `as_user(OWNER)`, not as
admin attribution, because the schema uses created-by read policy. This is a
good reminder that perf fixtures need realistic authorship or they accidentally
benchmark empty/hidden data.

2k-node / branch-factor-5 release sample:

- seed: ~34 ms
- initial recursive export: ~762 ms
- initial bundle: ~672 KB, 2000 history rows
- initial apply: ~47 ms
- subscribe: ~742 ms
- refresh export: ~777 ms
- refresh bundle: ~696 KB, 2070 history rows
- refresh apply: ~34 ms
- subscription poll/diff: ~759 ms
- diff: 25 added, 35 updated, 10 removed

Learning: recursive subscriptions are the first clearly bad perf shape in this
spike. SQLite storage/apply is not the main issue here; repeated recursive
query execution, subscription diffing, and full recursive-scope export are. We
need explain plans and likely a more incremental recursive query/read-set
strategy before treating large trees as product-safe.

## 2026-05-27 01:48 PDT

Split the recursive tree probe into direct read vs export vs subscribe/poll,
and added an admin/bypass read comparison.

2k-node sample after the split:

- direct recursive read as user: ~743 ms
- direct recursive read with policy bypass: ~738 ms
- export: ~764 ms
- subscribe: ~732 ms
- refresh direct read: ~757 ms
- refresh export: ~771 ms
- poll/diff: ~750 ms

Learning: this is not created-by policy overhead. The recursive CTE read itself
dominates, and export/subscribe/poll are slow because they rerun the same slow
recursive read. The most likely next risk is missing or unusable indexing for
`current.parent -> subtree.row_num` traversal, or an inefficient recursive CTE
shape.

## 2026-05-27 01:51 PDT

Tried two quick recursive-query hypotheses:

1. `MINI_JAZZ_SQLITE_BRANCH_FIRST_INDEXES=1`, which changes user indexes from
   `(is_deleted, ...)` to `(j_branch_num, is_deleted, ...)`.
2. Replacing the recursive CTE `UNION` with `UNION ALL`.

Results:

- Branch-first indexes only moved the 2k-node direct recursive read from
  ~743 ms to ~716 ms, so index prefix is not enough.
- `UNION ALL` is invalid with current semantics: self-parent roots and possible
  cycles can make recursive queries non-terminating. The experiment hung the
  recursive correctness tests and was immediately reverted.

Learning: recursive query optimization cannot simply drop duplicate tracking
unless we also change root/cycle semantics or add explicit visited-depth guards.
The next serious move should be `EXPLAIN QUERY PLAN` plus a dedicated closure,
path, or descendant index strategy.

## 2026-05-27 02:00 PDT

Added a raw recursive-layout comparison probe:

- edge-only table with `(parent_num, row_num)` index queried through a recursive
  CTE
- closure table with `(ancestor_num, descendant_num, depth)`

2k-node / branch-factor-5 sample:

- raw edge-only recursive CTE count: ~0.69 ms
- raw closure query count: ~0.09 ms
- edge-only DB: ~90 KB
- closure DB: ~344 KB
- closure rows: 11,025
- closure build: ~15 ms

Also tried a scoped `row_num -> row_id` materialization cache for the real Jazz
recursive read. It did not improve the 2k-node probe, so I reverted it.

Learning: SQLite recursive CTEs are not inherently the 700 ms problem. The raw
shape is fast. The slowness likely comes from the richer Jazz query shape:
joining current rows to txs/ids, materializing full `RowView`s, ordering, and
possibly export repair/policy machinery. Closure tables could still help, but
the next sharper step is to profile the actual Jazz recursive SQL plan and row
materialization path rather than assuming "recursive CTE bad".

## 2026-05-27 02:02 PDT

Added a cold/reopen profile probe for the ordinary indexed owner page path. It
seeds a durable core, closes/reopens it, compares first export vs immediate
second export, applies the bundle into a durable worker, queries once warm,
reopens the worker, then queries again.

50k-row / 5k-owner-row sample:

- cold core export: ~1.37 ms
- warm core export: ~1.19 ms
- cold export read phase: ~0.29 ms
- warm export read phase: ~0.20 ms
- bundle: ~36.7 KB, 100 history rows
- cold worker apply: ~3.0 ms, with ~2.2 ms in history
- warm worker query: ~0.21 ms
- reopened worker query: ~0.21 ms
- observed query reads survive reopen: 1

Learning: for the normal indexed page path, SQLite page-cache coldness and
worker reopen are not scary at this scale. The dangerous path remains recursive
tree reads/subscriptions, not ordinary durable restart.

## 2026-05-27 02:04 PDT

Ran the expanded full release harness after adding dashboard full-topology
refresh, recursive tree subscriptions, raw closure comparison, and cold/reopen
profiling.

Key all-up sample:

- primary 100k-row first page: ~10.3 ms
- primary refresh: ~14.6 ms
- dashboard initial core export: ~4.9 ms
- dashboard full-topology refresh hop costs: core export ~6.3 ms, edge apply
  ~7.4 ms, edge export ~5.5 ms, worker apply ~7.9 ms, worker export ~5.7 ms,
  tab apply ~6.9 ms
- recursive tree direct read: ~740 ms
- recursive tree initial export: ~764 ms
- recursive tree refresh export: ~780 ms
- recursive tree subscription poll: ~766 ms
- raw recursive CTE count: ~0.74 ms
- raw closure query count: ~0.08 ms
- cold/reopen ordinary page query: ~0.26 ms after worker reopen

Learning: this is now a very clear split. The relational SQLite approach looks
healthy for indexed page subscriptions and durable topology refresh. Recursive
tree subscriptions are the one severe product-shaped gap, and the raw CTE
comparison says the issue is in Jazz's full query/materialization/export shape,
not in SQLite recursion as a primitive.

## 2026-05-27 02:06 PDT

Added an experimental env switch,
`MINI_JAZZ_SQLITE_RECURSIVE_VISIBLE_ROWS=1`, that forces recursive reads through
the existing visible-rows + in-memory traversal fallback instead of the
recursive SQL CTE. This is intentionally not the default yet because it scans
all visible rows in the table, which may be bad for tiny subtrees inside huge
tables.

2k-node tree sample with the switch enabled:

- direct recursive read: ~740 ms -> ~7 ms
- initial export: ~764 ms -> ~25 ms
- subscribe: ~758 ms -> ~7 ms
- refresh direct read: ~765 ms -> ~7 ms
- refresh export: ~780 ms -> ~27 ms
- subscription poll: ~766 ms -> ~8 ms
- apply unchanged: ~34-49 ms depending on initial vs refresh bundle

Learning: this is the biggest discovery of the sprint. Recursive subscriptions
do not require a bespoke closure table to be viable for tree-sized reads; the
current SQL CTE shape is the problem. A table-scan + in-memory traversal is
already dramatically faster for a 2k-row tree. The next decision is choosing a
real strategy: improve the recursive SQL plan, use a heuristic between SQL CTE
and visible-row traversal, or maintain a descendant/closure index for large
tables with small subtrees.

## 2026-05-27 02:08 PDT

Added `MINI_JAZZ_PERF_RECURSIVE_TREE_ROOT_ID` so the recursive tree probe can
measure small subtrees inside the same table.

2k-node table, leaf root `folder-1999`:

- default SQL CTE direct read: ~0.8 ms for 1 row
- visible-row traversal direct read: ~5.2 ms for 1 row
- default SQL CTE export: ~1.7 ms
- visible-row traversal export: ~6.3 ms

Learning: the visible-row traversal is a rescue path for broad recursive reads,
not a universal replacement. For tiny subtrees, the SQL CTE is clearly better.
The likely product strategy is adaptive: use SQL CTE for narrow subtrees, use
visible-row traversal or a closure/descendant index for broad observed trees.

## 2026-05-27 02:13 PDT

Added an opt-in `MINI_JAZZ_SQLITE_POLICY_FIRST_INDEXES=1` layout experiment
that prefixes declared current-table indexes with
`j_branch_num, is_deleted, j_created_by`.

2k-node tree from root `folder-0`, default recursive SQL CTE:

- default index shape direct read: ~741 ms
- policy-first direct read: ~679 ms
- default export: ~765 ms
- policy-first export: ~696 ms
- database size: ~528 KiB -> ~532 KiB

Learning: policy-first indexes help a little, but only by roughly 8-9% in this
shape. They are not the explanation for the 700 ms recursive CTE. The larger
issue is still the recursive SQL/materialization strategy.

## 2026-05-27 02:15 PDT

Turned the recursive visible-row fallback into a default heuristic:

- if the branch is a pinned-base branch, use visible-row traversal as before;
- if the local current table has at most 10k visible rows and the root has at
  least one direct child, use visible-row traversal;
- otherwise keep the SQL CTE path.

The threshold is overrideable via
`MINI_JAZZ_SQLITE_RECURSIVE_VISIBLE_ROWS_MAX_TABLE_ROWS`; `0` disables the
heuristic.

2k-node tree:

- root `folder-0`: direct read ~741 ms -> ~7.0 ms, export ~765 ms -> ~24.5 ms
- leaf `folder-1999`: direct read remains ~1.2 ms, export ~2.2 ms

Learning: this crude heuristic already captures the most important distinction
from the experiments: broad small/medium local trees want visible-row
traversal; tiny subtrees want the SQL CTE. It is not the final planner. It will
be wrong for large tables with broad subtrees and for small tables where a
recursive CTE can be made cheap, but it makes the prototype much more honest for
client-perceived recursive subscriptions.

## 2026-05-27 02:20 PDT

Refactored visible-row recursive traversal from repeated frontier scans into a
parent-to-children map. Then stretched the recursive probe to 10k rows.

Important failure with the original 10k threshold:

- initial root read at exactly 10k rows used visible traversal: ~29.5 ms
- refresh after the mutation had 10,015 visible rows, crossed the threshold,
  fell back to the SQL CTE, and exploded to ~20.8 s

Raised the prototype threshold to 50k rows. 10k-node tree with map traversal and
50k threshold:

- initial root read: ~28.4 ms
- initial export: ~124.7 ms
- refresh read: ~28.6 ms
- refresh export: ~129.6 ms
- subscription poll: ~34.0 ms
- apply initial bundle: ~243.6 ms
- apply refresh bundle: ~164.0 ms

Learning: thresholds without hysteresis can create catastrophic cliffs in
reconnect/refresh paths. The map traversal scales well enough to keep 10k-row
tree reads plausible, but export/apply of 10k recursive history rows is now the
dominant client-perceived cost.

## 2026-05-27 02:24 PDT

Added full `ApplyBundleProfile` output to the recursive tree subscription
probe.

10k-node recursive bundle apply profile:

- initial total apply: ~240 ms
- initial reads: ~36.9 ms for 10k read rows
- initial history/current projection: ~202.7 ms for 10k history rows
- refresh total apply: ~162.6 ms
- refresh reads: ~33.7 ms for 10,070 read rows
- refresh history/current projection: ~128.3 ms for 10,070 history rows

Learning: once recursive reads avoid the pathological SQL CTE, receiving a large
recursive query scope is dominated by writing history/current rows and read-set
rows. The benchmark should keep exposing the profile breakdown so we can tell
whether future wins come from bundle scope reductions, faster apply mechanics,
or transport/payload changes.

## 2026-05-27 02:27 PDT

Added a recursive full-topology probe: core -> durable edge -> memory worker ->
memory tab. It exports the same recursive subscription scope through every
tier, then mutates the core tree and forwards observed-query refreshes through
the same path.

2k-node tree:

- initial core export: ~23.4 ms
- initial edge apply: ~47.5 ms
- initial edge export: ~24.1 ms
- initial worker apply: ~46.0 ms
- initial worker export: ~21.9 ms
- initial tab apply: ~46.8 ms
- refresh core export: ~25.1 ms
- refresh edge apply: ~34.4 ms
- refresh edge export: ~24.8 ms
- refresh worker apply: ~32.8 ms
- refresh worker export: ~22.7 ms
- refresh tab apply: ~32.8 ms
- subscription poll: ~6.3 ms

Learning: topology multiplication matters. Even after fixing local recursive
read time, a cold recursive scope pays full export/apply costs at each tier.
This makes bundle delta/scoping and apply throughput at intermediaries at least
as important as local query speed for broad subscriptions.

## 2026-05-27 02:29 PDT

Ran the recursive full-topology probe at 10k nodes.

- initial core export: ~125.7 ms
- initial edge apply: ~249.1 ms
- initial edge export: ~130.0 ms
- initial worker apply: ~236.1 ms
- initial worker export: ~114.9 ms
- initial tab apply: ~237.5 ms
- refresh core export: ~126.9 ms
- refresh edge apply: ~198.9 ms
- refresh edge export: ~134.1 ms
- refresh worker apply: ~163.7 ms
- refresh worker export: ~118.5 ms
- refresh tab apply: ~161.8 ms
- subscription poll on tab: ~34.1 ms
- bundle size: ~3.37 MiB initial, ~3.40 MiB refresh

Learning: the system no longer falls off the 20s CTE cliff, but 10k broad
recursive subscriptions still imply roughly one second of cumulative
export/apply work across core, edge, worker, and tab. This is probably not a
"make SQLite query faster" problem anymore; it is a scope delta, apply
throughput, and topology forwarding problem.

## 2026-05-27 02:32 PDT

Added optional gzip-size reporting for recursive topology bundles using the
system `gzip` command when available.

10k-node recursive topology:

- initial JSON bundle: ~3.37 MiB
- initial gzip bundle: ~101.9 KiB
- refresh JSON bundle: ~3.40 MiB
- refresh gzip bundle: ~104.7 KiB

Learning: transport stream compression should make broad recursive JSON bundles
much less scary on the wire. The remaining hard problem is CPU and SQLite write
work at every forwarding tier, not raw transport bytes.

## 2026-05-27 02:34 PDT

Changed `scoped_policy_parent_row_nums` from one query per child row to a single
`IN (...)` query over the child row set. Targeted policy and recursive tests
still pass.

10k recursive topology remained effectively unchanged:

- initial core export: ~125.2 ms
- refresh core export: ~131.4 ms

The existing 100k export-profile probe still shows `policy_dependency_history`
as the dominant bucket at ~23.3 ms for a 50-row page with policy dependencies.

Learning: the trivial parent-row N+1 fix is correct but not a silver bullet for
the current benchmark shapes. The remaining policy export cost is deeper in
history extraction / policy-dependency traversal rather than just resolving
parent row ids one at a time.

## 2026-05-27 02:38 PDT

Added `ApplyBundleProfile` output to the dashboard query-scaling probe so we can
see whether many same-shape page subscriptions are dominated by query-scope
repair.

50k rows, 48 dashboard subscriptions:

- initial apply: ~10.6 ms total
- initial history/current projection: ~7.8 ms
- initial reads: ~1.1 ms
- initial query-scope repair: ~1.3 ms
- refresh apply: ~8.9 ms total
- refresh history/current projection: ~6.0 ms
- refresh reads: ~1.1 ms
- refresh query-scope repair: ~1.4 ms

Learning: grouped top-query repair may still be worth doing eventually, but it
is not tonight's dominant dashboard bottleneck. History/current projection and
read-set writes are consistently larger apply buckets.

## 2026-05-27 02:40 PDT

Changed bundle apply's read-set insertion loop to reuse one prepared
`jazz_tx_read` insert statement rather than going through a per-row helper.

10k recursive subscription apply profile:

- initial reads: ~36.9 ms before, ~33.4 ms after
- refresh reads: ~33.7 ms before, ~33.3 ms after
- total apply did not meaningfully improve because history/current projection
  moved around in noise and still dominates

Learning: statement reuse helps a little, but read-set insertion remains a
non-trivial 33 ms for 10k rows. Meaningful improvements likely need either
fewer read-set rows for broad query scopes or a more compact/bulk read-set
representation, not just preparing SQL more carefully.

## 2026-05-27 02:44 PDT

Added a cold-apply fast path for row ids first created during the current bundle
apply. If a history record has no current row and its public row id was created
while applying this bundle, then there cannot be an older local current row or a
newer local history version to protect, so we skip the per-row
`is_newest_version_for_current` scan.

10k recursive subscription:

- initial apply total: ~240 ms before, ~145 ms after
- initial history/current projection: ~203-209 ms before, ~109 ms after
- refresh apply stayed ~160 ms because those row ids already exist locally

Learning: cold-cache apply was paying a huge cost to prove new rows were safe to
project. This is a strong win for the "only core has data, client/worker cold"
case. Warm refresh still needs separate work because existing rows cannot use
this proof.

## 2026-05-27 02:46 PDT

Reran the 10k recursive full-topology probe after the cold-apply fast path.

- initial edge apply: ~249 ms -> ~150 ms
- initial worker apply: ~236 ms -> ~142 ms
- initial tab apply: ~238 ms -> ~141 ms
- refresh applies remain ~159-195 ms, as expected

Learning: the cold-apply fast path compounds across the topology and removes
roughly 280 ms of total initial forwarding latency for a 10k recursive scope.
The remaining warm-refresh cost is still history/read-set work for existing row
ids.

## 2026-05-27 02:48 PDT

Added an idempotent-history apply fast path: if `(row_num, tx_num)` already
exists in a table's history, skip rewriting that history row, tx-write row, and
current projection for that record.

10k recursive subscription:

- refresh apply total: ~160 ms -> ~42 ms
- refresh history/current projection: ~128 ms -> ~9.7 ms
- read-set writes still cost ~32 ms

10k recursive topology:

- refresh edge apply: ~195 ms -> ~65 ms
- refresh worker apply: ~159 ms -> ~42 ms
- refresh tab apply: ~162 ms -> ~42 ms

Learning: this is the warm-refresh counterpart to the cold-apply fast path.
Because broad recursive refresh bundles currently resend mostly already-known
history, idempotent apply must be cheap. The next remaining warm-refresh apply
cost is read-set insertion for the repeated 10k-row scope.

## 2026-05-27 02:52 PDT

Tried changing `jazz_tx_read` / `jazz_tx_write` inserts from
`INSERT OR REPLACE` to `INSERT OR IGNORE`, based on the idea that transaction
read/write sets are immutable.

10k recursive subscription:

- refresh read-set writes stayed ~32 ms
- refresh total apply was noise-worse (~42 ms -> ~48 ms)

Decision: reverted the code change. Even if the immutability assumption is
mostly right, the measured benefit is not there. The read-set cost probably
comes from index probes and per-row SQLite calls rather than replace churn.

## 2026-05-27 02:55 PDT

Tried batching `jazz_tx_read` writes into multi-row `INSERT OR REPLACE ...
VALUES (...), ...` chunks of 400 rows.

10k recursive subscription:

- initial reads: ~34 ms -> ~35 ms
- refresh reads: ~32 ms -> ~34 ms
- total apply was noise-worse

Decision: reverted the code change. Multi-row VALUES batches do not improve this
path in SQLite/rusqlite as used here. The remaining read-set cost likely needs a
representation change, such as compact query-scope read-set facts, not just
fewer SQL statements.

## 2026-05-27 02:59 PDT

Tried preloading public row ids for all bundle read/history rows before apply,
because refresh bundles repeatedly resolve row ids that are already known.

10k recursive subscription with preload:

- refresh reads improved: ~32 ms -> ~25 ms
- refresh total barely moved: ~42 ms -> ~41 ms
- cold initial apply got worse: ~145-150 ms -> ~158 ms, because the receiver has
  no existing row ids and pays an extra failed lookup pass

Decision: kept the code as an opt-in experiment behind
`MINI_JAZZ_SQLITE_PRELOAD_ROW_IDS=1`, but disabled it by default. Cold-client
startup matters too much to pay this cost speculatively. A real version would
need a better signal, for example peer capability/state saying the receiver is
warm for the scope.

## 2026-05-27 03:02 PDT

Added a no-op recursive refresh measurement after the mutation refresh has
already been applied and polled.

10k recursive subscription, no further mutations:

- no-op refresh export: ~129.7 ms
- no-op refresh history rows: 10,070
- no-op apply: ~46.0 ms
- no-op apply reads: ~30.9 ms
- no-op apply history/current projection: ~14.6 ms
- no-op subscription poll: ~34.3 ms
- emitted diffs: 0 added, 0 updated, 0 removed

Learning: the system is now much better at idempotently applying broad refreshes,
but it still resends and rechecks the whole recursive scope even when nothing
changed. For large observed queries, the next big design target is delta-shaped
refresh/export and/or compact read-set refresh facts, not more local SQL
micro-optimizations.

## 2026-05-27 03:04 PDT

Compared declared index prefixes on the 50k-row dashboard scaling probe:

- default indexes: 48-query initial export ~21.5 ms, refresh export ~23.3 ms
- `MINI_JAZZ_SQLITE_BRANCH_FIRST_INDEXES=1`: initial export ~21.4 ms, refresh
  export ~24.0 ms
- `MINI_JAZZ_SQLITE_POLICY_FIRST_INDEXES=1`: initial export ~545 ms, refresh
  export ~548 ms

Learning: branch-first indexes are roughly neutral in this workload, but
policy-first index prefixes are catastrophic for owner/page queries. The earlier
recursive CTE policy-first experiment gave only a small improvement, so the
overall decision is: do not prefix ordinary user-declared indexes with policy
columns by default. If we need policy-specific acceleration, it should be a
separate targeted index/plan, not a blanket index shape.

## 2026-05-27 03:09 PDT

Tried a subscription diff no-op fast path: if the freshly read ordered rows are
exactly equal to the previous ordered rows, return no diffs before building
`BTreeMap`s.

10k recursive subscription after an unchanged refresh:

- no-op poll stayed around ~32 ms
- no-op apply stayed around ~48 ms in this sample

Learning: this is still a reasonable local cleanup, but it is not the lever.
The no-op poll cost is dominated by rerunning the recursive read over ~10k rows,
not by constructing the semantic diff. Large-query subscriptions need an
invalidation/version signal that can avoid rerunning the query at all when no
possibly relevant transactions arrived.

## 2026-05-27 03:11 PDT

Ran a final default release harness after the overnight optimization slices.

Selected default-scale numbers:

- dashboard 50k / 48 owner-page queries: initial export ~21.8 ms, refresh export
  ~26.1 ms, tab apply ~6.8 ms, refresh apply ~4.0 ms
- recursive tree 2k: initial read ~5.3 ms, initial export ~23.5 ms, initial
  apply ~28.7 ms
- recursive tree 2k refresh: export ~24.1 ms, apply ~10.2 ms, poll ~6.2 ms
- recursive tree 2k no-op refresh: export ~25.5 ms, apply ~9.8 ms, poll
  ~5.7 ms
- recursive topology 2k, core -> edge -> worker -> tab: initial apply per tier
  ~27-31 ms; refresh apply per tier ~10-11 ms
- process RSS over the full benchmark run: ~1.7 MiB start, ~64.3 MiB end

Learning: the current prototype is now plausibly fast at the default synthetic
scales, including whole-topology propagation. The remaining uncomfortable
pattern is not ordinary page refresh or cold apply anymore; it is broad
unchanged recursive scopes, where every tier still re-exports, re-applies, and
re-polls thousands of rows to discover nothing changed.

## 2026-05-27 03:12 PDT

Ran the final code with the recursive tree scaled to 10k rows.

10k direct subscription probe:

- initial read ~28.4 ms
- initial export ~123.7 ms
- initial apply ~260.6 ms in this run
- mutation refresh read ~28.7 ms
- mutation refresh export ~126.7 ms
- mutation refresh apply ~48.0 ms
- mutation subscription poll ~33.4 ms
- no-op refresh export ~128.6 ms
- no-op refresh apply ~49.2 ms
- no-op subscription poll ~31.4 ms
- bundle size ~3.37 MiB initial / ~3.40 MiB refresh

10k topology probe, core -> edge -> worker -> tab:

- gzipped bundle size was ~102 KiB initial / ~105 KiB refresh over ~3.4 MiB JSON
- initial apply per tier was ~146-158 ms
- refresh apply per tier was ~48-71 ms
- full benchmark RSS ended around ~213 MiB with the 10k recursive setting

Learning: 10k recursive scopes are no longer catastrophic, but the shape remains
expensive enough that we should treat "refresh huge unchanged recursive scope"
as a first-class design problem. Stream compression makes transport bytes look
surprisingly good, while CPU stays dominated by full-scope export/apply/poll.

## 2026-05-27 03:15 PDT

Added RSS checkpoints to the recursive subscription and topology probes, then
ran the default harness once to sanity-check the fields.

Default 2k recursive subscription RSS:

- start ~16.5 MiB
- after seed ~16.6 MiB
- after initial apply ~25.0 MiB
- after mutation refresh apply ~34.9 MiB
- after no-op refresh apply ~41.1 MiB

Default 2k recursive topology RSS:

- start ~41.2 MiB
- after seed ~41.3 MiB
- after initial edge/worker/tab flow ~43.0 MiB
- after refresh flow ~55.2 MiB

Learning: memory growth is now easier to attribute per scenario. The recursive
subscription path retains enough intermediate state across export/apply/refresh
that memory deserves its own follow-up pass, especially for repeated large
refreshes and merged bundle lifetimes.

## 2026-05-27 03:16 PDT

Added a repeated no-op recursive refresh probe. By default it runs three more
unchanged refresh/apply/poll loops after the first no-op refresh.

Default 2k recursive subscription, three repeated unchanged refreshes:

- total export time ~75.7 ms
- total apply time ~29.3 ms
- total poll time ~17.8 ms
- total history rows resent/re-applied: 6,210
- total emitted diffs: 0
- RSS after first no-op refresh ~41.1 MiB
- RSS after three more no-op refreshes ~47.3 MiB

Learning: repeated unchanged refreshes keep doing real CPU work and appear to
ratchet memory in the benchmark process. This strengthens the case for
delta-shaped refreshes or query-settlement/invalidation tokens; the current
full-scope refresh model scales with observed scope size even when semantics do
not change.

## 2026-05-27 03:17 PDT

Ran the repeated no-op recursive refresh probe at 10k rows.

10k recursive subscription, three repeated unchanged refreshes:

- total export time ~377.9 ms
- total apply time ~142.5 ms
- total poll time ~92.3 ms
- total history rows resent/re-applied: 30,210
- total emitted diffs: 0
- RSS after first no-op refresh ~158.3 MiB
- RSS after three more no-op refreshes ~194.8 MiB

Learning: the unchanged-refresh issue is very visible at 10k. The transport
payload compresses well, but the CPU and memory cost remains roughly proportional
to full observed history scope. Avoiding unchanged full-scope work is likely the
highest-leverage next performance topic for recursive subscriptions.

## 2026-05-27 03:18 PDT

Reran the 10k repeated no-op recursive probe with
`MINI_JAZZ_SQLITE_PRELOAD_ROW_IDS=1`.

Compared to the immediately previous default run:

- initial apply was ~156.7 ms with preload
- mutation refresh apply was ~39.8 ms with preload
- first no-op refresh apply was ~38.6 ms with preload
- three repeated no-op applies totaled ~120.2 ms with preload, versus ~142.5 ms
  without preload
- three repeated no-op polls were noisy/worse in this run: ~189.5 ms with preload
  versus ~92.3 ms without preload
- repeated no-op RSS ended ~187.1 MiB with preload versus ~194.8 MiB without
  preload

Learning: row-id preload may be useful for warm broad applies, but it does not
change the underlying problem and the full-harness noise makes it hard to judge
without a narrower repeated-refresh benchmark. Keep it experimental, not default.

## 2026-05-27 03:19 PDT

Added `MINI_JAZZ_PERF_ONLY_RECURSIVE_TREE=1` so recursive subscription/topology
stress can be run without the rest of the benchmark harness.

Focused 10k recursive-only comparison:

- default initial apply ~149.4 ms; preload initial apply ~156.0 ms
- default mutation refresh apply ~48.4 ms; preload ~41.1 ms
- default first no-op apply ~48.0 ms; preload ~40.8 ms
- default three repeated no-op applies ~142.9 ms; preload ~120.0 ms
- default three repeated no-op polls ~92.8 ms; preload ~90.6 ms
- RSS after repeated no-ops was similar: ~184.9 MiB default, ~183.4 MiB preload

Learning: in the isolated recursive benchmark, row-id preload consistently helps
warm refresh apply by about 15-17%, while still slightly hurting cold initial
apply. That suggests a real optimization opportunity if peers can know a scope is
warm, but it is still not a substitute for avoiding unchanged full-scope
refreshes.

## 2026-05-27 03:20 PDT

Used the focused recursive mode to compare SQLite page sizes on the 10k recursive
topology probe.

Default page size from the previous focused run:

- core DB ~2.15 MiB
- edge DB ~2.12 MiB
- initial edge apply ~162.6 ms
- refresh edge apply ~71.2 ms

`MINI_JAZZ_SQLITE_PAGE_SIZE=8192`:

- core DB ~2.24 MiB
- edge DB ~2.20 MiB
- initial edge apply ~154.6 ms
- refresh edge apply ~79.0 ms

`MINI_JAZZ_SQLITE_PAGE_SIZE=16384`:

- core DB ~2.46 MiB
- edge DB ~2.42 MiB
- initial edge apply ~162.7 ms
- refresh edge apply ~89.7 ms

Learning: larger SQLite page sizes did not reduce disk footprint for this
history-heavy recursive workload and did not clearly improve speed. The default
page size looks at least competitive here; page-size tuning is not an obvious
early win.

## 2026-05-27 03:21 PDT

Compared the focused 10k recursive topology with unsafe durability pragmas:
`MINI_JAZZ_SQLITE_JOURNAL_MODE=OFF MINI_JAZZ_SQLITE_SYNCHRONOUS=OFF`.

Default WAL/NORMAL from the previous focused run:

- initial edge apply ~162.6 ms
- refresh edge apply ~71.2 ms

Unsafe OFF/OFF:

- initial edge apply ~156.5 ms
- refresh edge apply ~67.6 ms
- core/edge DB bytes were unchanged in the logical page-count measurement

Learning: disabling SQLite durability only improved this workload by a few
milliseconds. The hot costs are CPU/query/application mechanics, not fsync or WAL
overhead. This is good news for keeping normal durable settings on edges/core.

## 2026-05-27 03:22 PDT

Re-read the final default harness storage-footprint probes.

User id interning / row metadata:

- 20k rows across 100 users with short user ids: DB ~6.63 MiB
- same shape with long user ids: DB ~11.45 MiB
- implied extra footprint from long ids: ~241 bytes/row

Current projection tradeoff:

- 100k rows + 10k updates with current projection: DB ~24.03 MiB, page query
  ~0.062 ms
- same logical shape history-only: DB ~13.41 MiB, page query ~0.087 ms
- saved bytes without current projection: ~10.63 MiB
- history-only page query slowdown in this synthetic case: ~1.4x, but still
  sub-millisecond

Raw recursive closure comparison at 2k rows:

- edge-only adjacency table: ~90 KiB
- closure table: ~344 KiB and ~11k closure rows
- recursive CTE query ~0.72 ms; closure query ~0.08 ms

Learning: interned identities matter a lot for disk when user ids are long.
Current projection has real space cost, but the speed tradeoff still looks worth
keeping for main-branch current reads. Closure tables are very fast but multiply
storage; they should stay as targeted/derived acceleration, not the default
recursive representation.

## 2026-05-27 03:29 PDT

Subagent review found three benchmark-validity issues and I fixed them:

- primary topology first-result and refresh latency now include intermediary
  edge/worker export time, not only apply time
- subscription diffs now expose order-only changes as `RowDiff::Moved`, while
  preserving existing add/update/remove semantics when membership or row content
  changes
- dashboard query-scaling cases now build a fresh seeded core per query-count
  case, so later cases are not contaminated by mutations from earlier cases

Learning: the benchmark harness is useful enough that its measurement semantics
need the same rigor as product code. The review also caught an important product
semantics gap: ordered subscriptions need explicit order-change events, not just
row add/update/remove events.

## 2026-05-27 03:30 PDT

Reran corrected focused measurements after the review fixes.

Focused 10k recursive-only:

- direct subscription initial apply ~150.3 ms
- mutation refresh apply ~48.3 ms
- first no-op refresh apply ~46.8 ms
- three repeated no-op applies ~140.9 ms
- three repeated no-op polls ~92.2 ms
- topology initial exports: core ~124.0 ms, edge ~128.0 ms, worker ~115.1 ms
- topology refresh exports: core ~126.0 ms, edge ~130.9 ms, worker ~117.8 ms
- topology initial applies per tier ~145-162 ms
- topology refresh applies per tier ~47.8-69.9 ms

Corrected dashboard scaling with fresh core per query-count case:

- 1 query: initial export ~12.6 ms, refresh ~13.6 ms, refresh apply ~0.4 ms
- 4 queries: initial export ~17.5 ms, refresh ~17.7 ms, refresh apply ~0.6 ms
- 12 queries: initial export ~18.5 ms, refresh ~18.9 ms, refresh apply ~1.2 ms
- 24 queries: initial export ~17.9 ms, refresh ~18.6 ms, refresh apply ~2.1 ms
- 48 queries: initial export ~21.5 ms, refresh ~22.5 ms, refresh apply ~3.7 ms

Learning: the dashboard scaling story survived the stricter harness and is a bit
cleaner now: export time grows slowly with query count while apply scales more
linearly with delivered rows. The recursive topology story is sharper too:
per-hop export is as expensive as apply and must be included in perceived
latency discussions.

## 2026-05-27 03:30 PDT

Second subagent review flagged an existing current-Jazz Criterion benchmark:
`crates/jazz-tools/benches/server_authorization_scope_benchmark.rs`.

Findings:

- the "wide schema catalogue" cases add 256 metadata columns to every benchmark
  row and then subscribe to full rows, so they measure row materialization and
  outbox payload size in addition to schema catalogue overhead
- each Criterion batch rebuilds 500 schemas and 2k wide rows, which may create
  allocator/cache noise around each sample even if setup is outside the measured
  closure

Decision: did not change the current-Jazz benchmark during this SQLite-core
sprint. It is still useful as an inspiration/source signal, but if we use it as
a direct comparator we should split "many known schemas" from "wide returned row
payload" so the measured bottleneck is unambiguous.

## 2026-05-27 03:32 PDT

Ran one corrected full default harness after the review fixes.

Selected final default numbers:

- primary page topology API-to-first-result, now including edge/worker export:
  ~32.8 ms
- primary refresh API-to-updated-result, now including edge/worker export:
  ~36.8 ms
- primary initial intermediary exports were small at default page scale:
  edge ~1.2 ms, worker ~1.1 ms
- primary refresh intermediary exports: edge ~1.4 ms, worker ~1.7 ms
- corrected dashboard 48-query case: initial export ~22.0 ms, refresh export
  ~22.8 ms, tab apply ~6.0 ms, refresh apply ~3.6 ms
- recursive 2k: initial apply ~29.5 ms, refresh apply ~10.1 ms, three repeated
  no-op applies ~28.7 ms, three repeated no-op polls ~17.0 ms

Learning: including intermediary exports changes the primary topology headline
but not the conclusion at page scale, because exports are small for bounded page
queries. The correction matters much more for broad recursive scopes, where
per-hop export is on the same order as apply.

## 2026-05-27 03:32 PDT

Ran two more focused 10k recursive topology pragma probes.

`MINI_JAZZ_SQLITE_TEMP_STORE=MEMORY`:

- initial core export ~125.4 ms
- initial edge export ~132.1 ms
- refresh core export ~126.1 ms
- refresh edge export ~136.1 ms
- initial edge apply ~158.1 ms
- refresh edge apply ~78.4 ms

`MINI_JAZZ_SQLITE_CACHE_SIZE=-64000` (roughly 64 MiB cache):

- initial core export ~125.0 ms
- initial edge export ~122.6 ms
- refresh core export ~126.4 ms
- refresh edge export ~125.8 ms
- initial edge apply ~157.5 ms
- refresh edge apply ~50.7 ms

Learning: `temp_store=MEMORY` did not help. A larger SQLite page cache may help
warm broad refresh apply materially, though this needs repeated isolated runs to
separate signal from noise. This is a plausible runtime tuning knob for
edge/worker processes, but still secondary to avoiding full-scope no-op work.
