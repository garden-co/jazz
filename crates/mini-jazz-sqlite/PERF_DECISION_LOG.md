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
