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
