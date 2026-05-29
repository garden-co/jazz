# Architecture Cleanup Decision Log

## 2026-05-28 22:44 PDT

Goal: spend at least 6 hours moving the SQLite core spike toward a real generic
Jazz core rather than a prototype full of test-specific seams. Target end time:
2026-05-29 04:44 PDT or later.

Initial constraints:

- work on a new stacked branch from `codex/mini-jazz-sqlite-spec`;
- inspect open PRs against the spike in detail before and during changes;
- selectively merge/cherry-pick useful work, especially tests, while keeping
  the branch coherent;
- watch #973 because it is moving in another session;
- keep commits small enough to review and push after each coherent slice;
- prefer general runtime APIs and reusable test harness fixtures over hardcoded
  scenario helpers;
- keep this log timestamped with `date` before adding entries.

Open stacked PRs to inspect:

- #973 `codex/sqlite-core-deep-history-efficiency` - moving performance PR by
  Anselm;
- #972 `guido/mini-sqlite-sql-scoped-query-export` - SQL scope export work;
- #952 `guido/transplant-branch-permissions-query` - branch permissions query;
- #947 `guido/mini-jazz-sqlite-subscription-invariants` - subscription tier
  invariants;
- #945 `nico/jazz-sqlite-tx-isolation` - transaction isolation tests/WIP impl.

## 2026-05-28 22:46 PDT

First local code-shape read after pulling the latest spike: the main broken
window is still `src/runtime.rs` at roughly 7.9k lines. It mixes auth/session
state, local writes, transaction builder, authority apply, branch APIs,
query-scope export, repair, subscriptions, and diagnostics.

Useful first cleanup slice: extract auth/session semantics into a focused module
before bigger refactors. This is low risk and product-shaped: the distinction
between an untrusted client user, a trusted peer running as a user, trusted
admin/system work, and privileged attribution should be a crisp API boundary.

## 2026-05-28 22:48 PDT

Parallel PR inspection returned:

- #973 deep-history/perf PR: do not merge/cherry-pick wholesale yet. It is a
  moving 173-commit draft with dirty merge state. Keep the lessons: hot current
  projection, open operational history, sealed cold history blocks, and
  sidecar/deep-text ops as separate concerns. Maybe port narrow invariants/docs
  later.
- #945 transaction isolation: implementation is throwaway, tests/spec are
  valuable. The right model is transaction start snapshot plus own staged write
  overlay, excluding other transactions' staged writes and later commits.
  Reimplement through read contexts/visibility machinery rather than custom SQL.
- #947 subscription tiers: valuable D.8 semantics, but implementation duplicates
  visibility SQL. Port tests later; implement tier as visibility/query mode.
- #952 branch permissions: most semantically important Guido PR. Keep tests and
  branch-policy API ideas; port through `policy`/`ReadVisibility` rather than a
  parallel route.
- #972 SQL-scoped query export: strongest near-term performance/product value.
  CTE row-scope direction is right, but port carefully against current query
  repair/read-visibility code instead of merging blindly.

Integration strategy for this cleanup PR: first reduce broken-window pressure in
the base by extracting crisp generic modules. Then pull in tests from #945/#952/
#947/#972 as red/green slices. Avoid wholesale merges unless a PR becomes clean
and narrow.

## 2026-05-28 22:49 PDT

Second cleanup target: remove stale "attempt3 fixture" naming from the core.
The default todo app schema may still exist as a convenience/example while the
tests are migrated, but names like `attempt3_fixture` invite test-specific
thinking in core code. Rename to `todo_app_schema` as an incremental cleanup.

## 2026-05-28 22:50 PDT

Started incorporating #945 in the style we want: port the transaction isolation
tests/semantics, not the throwaway runtime SQL. First target is
`TransactionBuilder::read_rows`: snapshot semantic rows at transaction start and
overlay the builder's own staged writes. This is deliberately generic and uses
the existing row API instead of adding table-specific code paths.

Known limitation for this slice: commit materialization still uses current
write paths, not the transaction start snapshot. The #945 "writes applied to
start snapshot" semantics remain a later red/green slice.

## 2026-05-28 22:53 PDT

Checked #973 again. Head moved to `bc06bf3c1d55bb00784af55ad249df0777c326f5`
and merge state remains dirty. Continue to avoid integrating its code directly.

## 2026-05-28 22:53 PDT

Next cleanup target: subscription convenience APIs. They currently preserve old
per-operator subscription query variants even though the generic `BuiltQuery`
path exists. This encourages future PRs to add subscription-specific hardcoded
logic. Move ordinary predicate/page subscription constructors onto
`BuiltQuery`; keep special observed/recursive compatibility only where needed.

## 2026-05-28 22:55 PDT

Continue #945 integration: transaction reads now snapshot correctly, but commit
materialization still risks merging patch updates against latest current state.
Next slice: use the transaction start snapshot as the effective base for staged
row updates, so omitted fields come from the start row, not from a later commit.

## 2026-05-28 22:57 PDT

Folded the #945 isolation semantics into the split spec after getting green
tests: start-snapshot transaction reads, own staged write overlay, isolation
from other staged writes/later commits, and patch update materialization against
the start snapshot. Remaining gap: branch conflict candidate preservation inside
transaction reads still needs a focused test/implementation pass.

## 2026-05-28 22:58 PDT

Refactor follow-up: move the pure transaction snapshot/staged-read overlay out
of `runtime.rs`. The first implementation worked, but leaving it in the giant
runtime file would preserve the broken-window pattern. Target module:
`src/transaction.rs` owns `TransactionSnapshot`; runtime/builder orchestration
can stay in `runtime.rs` until a larger write-pipeline extraction is safe.

## 2026-05-28 23:00 PDT

Start a small #952 integration slice: direct branch queries. This is valuable
even before full branch backing-row permission policies because it makes branch
context an explicit query parameter instead of forcing tests to mutate checkout.
Implementation should be generic over `BuiltQuery` and must restore the previous
checkout even if the query fails.

## 2026-05-28 23:05 PDT

Start #947 integration through current architecture: introduce `ReadTier` as an
explicit read/query parameter and lower tier predicates in the generic current
query path. First scope is main-branch current table reads and built queries:
local sees local optimistic mergeables, edge sees edge/global-settled rows, and
global sees globally accepted rows. This creates the right seam for subscription
tier gating later without copying old custom subscription SQL.

## 2026-05-28 23:09 PDT

Continue #947 tier reads after compaction. Keep the first implementation focused on generic current reads and built queries. Do not thread tier into `ReadVisibility` yet: policy visibility is orthogonal to settlement tier for this slice, and mixing them would make the new boundary less clear.

## 2026-05-28 23:14 PDT

#947 first slice is green: `ReadTier` now gates generic main-branch table reads and built queries. This deliberately models settlement as a query/read parameter, not as auth or policy state. Current limitation: subscription delivery and branch snapshot reads still need to reuse this same tier predicate in later slices; the spec now calls this out.

## 2026-05-28 23:18 PDT

Start #972 as a red/green test slice. The valuable behavior is not the exact implementation from the draft branch, but the generic contract: query-scoped export for `BuiltQuery` must export enough rows for offset/limit pagination and refresh must remove stale page rows while adding new boundary rows. Try the behavior tests first against the current generic query/export code.

## 2026-05-28 23:18 PDT

#972 red/green result: initial offset-page export already works in current code, which is encouraging. Refresh does not: after the first page changes, the receiver keeps the stale row and lacks the new boundary row. Missing behavior is not broad initial export; it is refresh repair support for windowed built queries using previously observed ids plus a support query broad enough to include replacement boundary rows.

## 2026-05-28 23:19 PDT

#972 refresh slice is green without importing the draft implementation. Built-query observed read descriptors now carry `observed_ids` beside the query JSON, and refresh exports use those ids as explicit repair rows while querying the support window needed for offset pages. This keeps the API generic and avoids a SQL-scope rewrite for now; SQL-scoped export remains an optimization candidate, not a semantic prerequisite.

## 2026-05-28 23:22 PDT

#952 inspection: much of the branch work already exists in current spike state (branch backing rows, provenance sync, branch policy contexts, direct branch query). The still-open product gap is true app-visible branch backing-row permissions: checkout/export should fail if the backing row is unreadable. That needs schema/API design rather than a blind port. Next safe PR-derived slice: #945 branch conflict transaction read semantics through the new `TransactionSnapshot` module.

## 2026-05-28 23:24 PDT

Architecture cleanup follow-up: extracted query observed-descriptor JSON helpers into `query_observation.rs`. This gives query refresh metadata a named boundary (`built_query_from_read`, `built_query_read_value`, `observed_ids`, support-window query) instead of adding more ad hoc runtime helpers.

## 2026-05-28 23:25 PDT

Next #947 slice: tiered one-shot reads are in place, but subscription semantics still implicitly use local reads. Add tier as subscription state so first delivery and later deltas can match `query_at_tier`/`read_rows_at_tier`. Keep it generic in `RowsSubscription`, not as bespoke test helpers.

## 2026-05-28 23:27 PDT

#947 second slice is green: tier now lives on subscription state for table and built-query subscriptions. First delivery and later deltas use the same `ReadTier` path as one-shot reads. Predicate convenience subscriptions still route through their existing APIs and should eventually be rebuilt on `BuiltQuery` or tier-aware predicates.

## 2026-05-28 23:29 PDT

Subscription architecture cleanup: collapsed the duplicated rerun logic used by `subscription_delta` and legacy `poll_subscription` into one `Runtime::subscription_rows` helper. This should make future subscription/tier/query changes hit one boundary instead of two nearly identical matches.

## 2026-05-28 23:32 PDT

#945 branch-conflict isolation slice is green. `TransactionSnapshot` no longer keys rows by id because that collapsed conflict candidates; it preserves candidate multiplicity and rejects ambiguous implicit base lookup when committing an update. This is a concrete example where a "clean" map abstraction was semantically wrong for Jazz branch views.

## 2026-05-28 23:33 PDT

Maintenance: filesystem free space dropped below 600 MiB after repeated focused Rust builds. Removed the local `target/` directory to keep the overnight run able to compile and test additional slices.

## 2026-05-28 23:36 PDT

Subagent scan identified the top remaining broken-window areas: legacy query/export op matrix, repeated query SQL scaffolding, apply-bundle monolith, transaction write materialization, and todo-shaped test support. I spawned a worker for the disjoint test-support fixture split while I continue locally on query/export architecture.

## 2026-05-28 23:37 PDT

Tried routing legacy predicate export APIs directly through `BuiltQuery`. Reverted that local change after focused tests showed two semantic contracts would break: persisted/observed query descriptors intentionally retain legacy `field/op/value` shape, and query-scope contraction/repair differs from built-query descriptors. Lesson: the next cleanup needs an explicit adapter layer that preserves external descriptor shape while sharing lower-level row-scope mechanics; not a silent migration to `$query`.

## 2026-05-28 23:39 PDT

Test support cleanup integrated locally after the worker left `mod todo_app` without the file in this workspace. `tests/support/mod.rs` is now generic topology/schema helpers plus a re-export, while todo-app helpers live in `tests/support/todo_app.rs`. Also adjusted built-query observed descriptor metadata to preserve the previous public JSON shape and add `observed_ids` inline, instead of nesting under `query`; this preserves existing query-read contracts while supporting refresh repair.

## 2026-05-28 23:42 PDT

Folded #973 lessons into spec as contracts, not implementation: sealed history blocks must preserve export/point-read/branch-base semantics, branch bases are compaction anchors, and sync should eventually apply open history + sealed blocks + sidecars as one coherent history delta. Avoid porting #973 code until apply/storage boundaries are cleaner.

## 2026-05-28 23:44 PDT

Next cleanup direction: attack the apply/export monolith by extracting small
cross-cutting helpers first, then larger phases. Start with profiling timers
because they are generic infrastructure used by both apply and export profiles
and do not belong to runtime orchestration. This is intentionally modest but
keeps moving code toward named boundaries before changing behavior.

## 2026-05-28 23:46 PDT

Next apply-boundary slice: bundle validation/classification. The runtime should
not have ad hoc knowledge of which tables appear in policy reads versus history
records inline inside `apply_bundle_inner`; that makes every sync/apply feature
tempting to implement as another local scan. Introduce an `apply` module that
can validate protocol/schema/policy compatibility and expose touched table sets
as named facts for apply phases.

## 2026-05-28 23:48 PDT

Continue extracting apply phases: transaction record application is a coherent
phase that produces exactly the identity map later phases need. Move tx import,
receipt import, rejection detail import, and `ApplyTxInfo` lookup into the
`apply` module so history/read application can depend on a named `AppliedTxs`
result instead of a cluster of locals.

## 2026-05-28 23:50 PDT

Next apply-boundary cleanup: row/user identity caches. These caches are shared
across read and history application to avoid repeated SQLite lookups and to
track rows created by the current apply. They should be explicit apply state,
not free-floating `BTreeMap`s in runtime. This also prepares a cleaner future
`apply_read_records` and `apply_history_records` phase split.

## 2026-05-28 23:52 PDT

Extract read-record application as its own phase. This is semantically useful
because read-set import will become central to exclusive validation,
dependency-awaiting, query settlement, and future compact read-set
representations. The phase should consume `AppliedTxs` plus table identity
maps, not reach back into arbitrary runtime locals.

## 2026-05-28 23:53 PDT

Extract query-read import as an apply phase too. Query descriptors are durable
sync facts with their own replay/repair semantics; even if the current phase is
small, naming it keeps subscription/query-scope behavior from being another
hidden side effect of runtime bundle application.

## 2026-05-28 23:55 PDT

Spawned a read-only explorer for #952 branch-permission semantics while I keep
working locally. Parallel question: what is the smallest valuable branch-policy
slice to port without reintroducing hardcoded paths?

Local cleanup continues with branch-record import. Branch metadata application
already has phase shape: ensure branch records, sync source lists, and produce
`branch_id -> branch_num` for later history application. Extract that so branch
catalogue behavior is not an inline prelude to row history import.

## 2026-05-28 23:57 PDT

#952 explorer recommendation: the valuable minimal semantic slice is read-only
`forBranch`: app-declared branch backing table, backing-row visibility, and row
policy matching against backing-row fields for branch reads/direct branch
queries. Defer branch writes/inheritance/export filtering until a clean
`PolicyContext { branch }` boundary exists. Current spec already describes two
policy layers for branch access, but implementation only has system
`jazz_branch_backing`, not app-declared backing rows. I should avoid copying
#952's route-layer-heavy policy code until policy lowering is cleaned up.

## 2026-05-28 23:57 PDT

Policy prep slice: replace anonymous `Option<branch_num>` policy lowering with
an explicit read-scope enum. This does not implement app-declared branch backing
rows yet, but it makes the next slice much less likely to blur main/current,
branch, and snapshot semantics. This is the same lesson as auth/user naming:
if the type only says `Option<i64>`, agents will guess and add hardcoded paths.

## 2026-05-28 23:59 PDT

Apply-refactor reviewer found no correctness issues, but noted one misleading
boundary: `encode_optional_json` is generic rejection-detail/tx fate machinery,
not apply-specific. Move it to `tx.rs` where `reject_with_detail_json` already
lives.

## 2026-05-29 00:04 PDT

Ported the first #952 semantic slice: read-only `forBranch` policy. Schema can
declare `read_for_branch_if_field_matches(branch_table, row_field,
branch_field)`. Branch reads/direct branch queries for such tables now require
the app backing row whose id equals the branch id to be visible on main and the
row field to equal the backing-row field. This is deliberately narrower than
#952: branch writes, inherit-main branch policies, branch snapshot/base policy,
and export filtering are not implemented yet. The first green test covers the
product-shaped read case via generic `query_branch`, not a bespoke
`read_rows_on_branch` API.

## 2026-05-29 00:07 PDT

Follow-up: the initial read-only `forBranch` slice is too narrow if it cannot
read pinned-base rows, because branch bases are a core Jazz concept. Next slice
should make branch-field read policy lower for branch base snapshots too, and
add a test where the matching todo lives in main at the branch base rather than
in the branch overlay.

## 2026-05-29 00:10 PDT

Pinned-base `forBranch` reads are now green. Branch snapshot policy lowering
uses the same app-visible backing row check and branch-field equality as current
branch reads, so a row inherited from a pinned main base is visible only if it
matches the backing branch row. Remaining branch-policy gaps: writes,
inherit-main shorthand, query-scope export/repair tests, and multi-branch-table
policy selection if we allow more than one branch backing table per target.

## 2026-05-29 00:13 PDT

Added query-scoped export for branch-policy backing rows. The red test proved
the important sync bug: without exporting the app backing row, the receiver gets
the branch metadata and todo history but hides the todo because its branch
policy dependency is missing. Export now treats branch backing rows as policy
dependencies and sends their main-branch visible history alongside the result
scope.

## 2026-05-29 00:16 PDT

Ported the narrow `forBranch` write-policy shape too: branch writes can require
the proposed row field to match a field on the app backing row, and hidden
backing rows deny. Local denied writes still follow the existing prototype
pattern: append rejected history, hide current, and expose rejection info rather
than throwing synchronously. Remaining gap: branch backing rows used by write
policy should be recorded as policy read-set facts for exclusive validation and
sync diagnostics.

## 2026-05-29 00:21 PDT

Turned that branch-write read-set gap into a failing whole-system assertion:
the accepted branch-view write should record the backing branch row as a policy
read. This keeps the branch policy model aligned with ordinary
`write_if_ref_readable`, where permission-influencing rows are not just checked
but also exported and revalidated.

## 2026-05-29 00:22 PDT

Green slice: branch-view writes now record the app backing branch row as a
policy read-set fact before evaluating the write. This is intentionally generic
inside the existing branch-policy slot rather than hardcoded to the test schema:
any active branch write policy records the branch backing row on main, plus
recursive policy dependencies of that backing row.

## 2026-05-29 00:24 PDT

Starting a cleanup slice to extract policy read-set recording out of
`runtime.rs`. The pattern to preserve: local write code should record write
read facts, then delegate policy-dependency recording to a policy/read-set
boundary that knows about ordinary ref-readable policies, recursive policy
dependencies, branch bases, and branch backing rows.

## 2026-05-29 00:28 PDT

Follow-on boundary cleanup: the inverse branch lookup should live in
`branch.rs`, not be duplicated by whichever module happens to need to turn a
physical branch number back into the public branch id. This is exactly the kind
of tiny centralization that prevents future hardcoded helper drift.

## 2026-05-29 00:30 PDT

Next cleanup target is query refresh planning. The runtime should orchestrate
observed-query refreshes, but the batching decision for predicates, recursive
reads, and windowed pages is pure planner logic. Moving it behind a
`query_refresh` module should make later replacement of legacy query-read ops
with built-query descriptors less invasive.

## 2026-05-29 00:34 PDT

Removing the public `Runtime::open(...)` todo-schema shortcut. Keeping a
convenient todo app fixture in test support is fine, but the core runtime's
entry point should require a schema so new agents don't infer that the runtime
itself is a todo-shaped product.

## 2026-05-29 00:36 PDT

Continuing that cleanup by removing `SchemaDef::todo_app_schema()` from the core
API as well. Tests and the demo wrapper can define a todo schema fixture, but
the production-facing schema builder should stay generic.

## 2026-05-29 00:39 PDT

Checked stacked PR state again. #973 has moved to
`d3bca86eb35478dd71e9c5f9e55d087eddf98dfd` and remains conflicting; #972 is
mergeable but already mostly represented here by built-query export/refresh
work; #945/#947/#952 are still the main semantic input branches already being
ported in slices. Keep watching #973 before trying to ingest deep-history code.

## 2026-05-29 00:41 PDT

Found one small #972 behavior that was not ported: query-scope repair can prune
a row from current while retaining its history, and later query hydration of the
same history must restore current projection. The existing `history_exists`
short-circuit treats "history exists, current absent" as already applied, which
is wrong for query-scoped caches.

## 2026-05-29 00:44 PDT

Porting another contained #972 cleanup: remove the temp-table fallback from
`export_txs_by_ids`. The fallback made the export helper mutate hidden SQLite
state depending on input size. For the spike, the simpler scan-and-filter path
is a better abstraction boundary; if it becomes too slow, we should replace it
with a deliberate scoped-export representation rather than an implicit fallback.

## 2026-05-29 00:49 PDT

Found unported #947 value: the runtime has tier-aware reads/subscriptions, but
the PR's subscription-tier invariants are missing locally. Adding the tests now
with current `ReadTier` terminology so edge/global visibility cannot silently
regress.

## 2026-05-29 00:57 PDT - Ported subscription tier invariants and fixed tiered table reads

- Ported the valuable #947 subscription tier tests into the current whole-system subscription suite.
- The tests exposed a real generic bug: `ReadTier::Global`/`ReadTier::Edge` table reads were filtering the current projection by tier, so a newer local/pending current row could hide the latest row visible at the requested tier.
- Changed non-local table reads to reconstruct the latest visible version from history at the requested tier, including branch-source shadowing and main-branch snapshot overlay behavior.
- This keeps current projection as the local hot path while making tiered reads/subscriptions semantic snapshot reads instead of lossy current-row filters.
- Focused subscription tests now pass.

## 2026-05-29 00:59 PDT - Batched refresh ordering must apply contractions before broad refreshes

- A full crate test run after the subscription-tier slice exposed a broader refresh-planner invariant: batched refreshes must not let a page/top-query contraction run after broader predicate refreshes, because the contraction can delete rows that the broad refresh already proved should stay locally.
- Individual refresh order happened to be more forgiving. The generic planner should make the safe ordering explicit: contraction-shaped refreshes first, broad predicate refreshes after, so broad reads can rehydrate rows still justified by active subscriptions.

## 2026-05-29 01:06 PDT - Ported SQL-lowered built-query export scopes

- Ported the central #972 idea without wholesale merging the PR: built-query exports can now pass a SQL-lowered row scope through export helpers instead of first materializing the visible result row numbers and then building large `IN (...)` lists.
- `QueryContext::lower_built_query_row_scope` is the new boundary. It lowers the same query semantics used for local execution into a reusable row-number scope CTE, including windows, ordering, current-policy SQL, branch overlays, and tier visibility where relevant.
- `Runtime::export_built_query_read_scope_sql` uses that scope for visible history export and policy dependency export, while keeping repair rows explicit because they represent previous observed rows and rows that left the result.
- Kept the existing fallback when branch effective policy requires post-filter pagination. That is a real semantic edge where SQL-only lowering would be wrong until the policy/window boundary is made sharper.
- Query matrix tests pass, including large built-query export, policy-filtered exports, and page refreshes.

## 2026-05-29 01:10 PDT - Avoiding deep-history feature ingestion; extracting SQL-scope boundary instead

- #973 is still moving and remains draft/dirty. The new scout pass confirms the sealed-history/deep-text work is coherent but too feature-shaped for this architecture-boundary PR.
- The small #973 candidates are apply profiling, incremental read export, and batching local history/current writes. I am only taking these if they can land as clean generic boundaries with tests.
- Because the #972 SQL-scope port added a lot of code to `runtime.rs`, I delegated a bounded refactor to move that logic into a child module. That is more aligned with this PR's purpose than piling on another runtime-local feature path.

## 2026-05-29 01:14 PDT - Added example-level reload/filter regression from #972

- Ported the remaining #972 product-shaped test as an example-level regression: a memory main runtime flips several page-five todos to done, syncs through the worker in id batches, then a freshly reloaded main runtime hydrates done/open pages and subscriptions.
- This is intentionally not a new core abstraction; it protects the browser todo app's real reload/filter failure mode now that query hydration and SQL-scoped built-query exports are in place.
- Running the example test initially hit the disk limit. Cleared generated Rust `target/` output, reran, and the focused example test passed.

## 2026-05-29 01:18 PDT - Extracting SQL-scope export out of the runtime body

- The #972 SQL-scoped export port worked but made `runtime.rs` larger, which works against the "stop looking like a test-shaped prototype" goal.
- Created a `runtime::query_scope_export` child module and moved the built-query SQL row-scope bundle export entry point there.
- I deliberately left the lower-level history/policy helper functions in `runtime.rs` for this slice. They are shared by legacy row-id export and SQL-scope export, so moving them safely needs a broader export-boundary refactor rather than a purely mechanical copy.
- Focused `query_matrix` tests pass after the extraction.

## 2026-05-29 01:23 PDT - Transaction builder gets its own runtime module

- The explicit transaction constructor had become a large tail section of `runtime.rs`, mixing the public transaction API with mutation normalization, read-your-writes snapshot semantics, exclusive conflict checks, and delete materialization.
- Moved that builder implementation into `runtime::transaction_builder`, re-exporting only the public `TransactionBuilder` type from `runtime`.
- This is mostly a shape improvement, but it matters: transactions are a core semantic layer, not a miscellaneous appendage on the runtime facade.
- Focused transaction tests pass after the move.

## 2026-05-29 01:25 PDT - Ported #973 batched logical write boundary generically

- Took the useful non-sealed idea from #973: multiple independent Jazz transactions can be written inside one SQLite transaction for commit overhead without changing Jazz transaction granularity.
- Implemented this as `runtime::write_batch` with generic `insert_rows_batched` and `update_rows_batched`, rather than a test-specific helper.
- Tests cover distinct tx ids, sync replay, previous-read tracking for later updates in the same SQLite commit, and full rollback when validation fails halfway through the batch.
- This is still intentionally narrower than a general write-call batch builder. Deletes, upserts, and mixed table batches are now obvious next increments if this API proves useful.

## 2026-05-29 01:26 PDT - Ported hyphen-safe tx-id invariant without sealed-history coupling

- #973 had useful coverage that hyphenated node ids must not break transaction lookup, but the test was coupled to sealed history compaction.
- Added a non-sealed whole-system test using a UUID-shaped node id and normal transaction lookup/read-set APIs.
- This guards the generic invariant directly: transaction identity is opaque text and must never be recovered by splitting `tx-{node}-{epoch}` on hyphens.

## 2026-05-29 01:28 PDT - Added native bundle codec boundary without adopting #973 columnar codec

- #973's columnar binary bundle codec is too broad for this architecture PR, but the missing abstraction is real: callers should not have to know whether the native sync representation is JSON today or a compact binary format later.
- Added `sync::encode_bundle` / `sync::decode_bundle` as the native codec boundary, currently implemented as JSON bytes.
- Updated one existing serialization test to use that boundary and added smoke tests for protocol metadata and equality query-read roundtrips.
- Also derived record equality for sync records, which makes codec roundtrip tests less awkward and gives future protocol changes sharper assertions.

## 2026-05-29 01:30 PDT - Generalized batched logical writes to upserts

- Added `upsert_rows_batched` and a `BatchedWriteMode` inside the write-batch module.
- This keeps batched writes from being a pair of one-off insert/update helpers. The mode is still intentionally small and private, but the module now has enough shape to grow into a generic write-call batching boundary.
- Focused batched logical write tests pass.

## 2026-05-29 01:32 PDT - Branch runtime API moved behind a branch module

- Moved branch creation, checkout, source-list mutation, branch-scoped queries, and branch backing-row inspection into `runtime::branches`.
- This is another facade cleanup: branch behavior already has a storage module, but the runtime public API was still parked in the middle of unrelated write and transaction code.
- Focused branch tests pass after the move.

## 2026-05-29 01:35 PDT - Subscription runtime API moved behind a subscription module

- Moved subscription construction, observed-query subscription reconstruction, polling, and rejection polling into `runtime::subscriptions`.
- This keeps reactive behavior findable and separate from sync export/apply and storage maintenance.
- Focused subscription tests pass after the move.

## 2026-05-29 01:37 PDT - Transaction fate and introspection moved behind a transaction-status module

- Moved accept/reject, rejection listing, transaction info, physical tx lookup, and transaction read/write-set inspection into `runtime::transaction_status`.
- The explicit transaction constructor stays in `runtime::transaction_builder`; this split mirrors the semantic distinction between creating transactions and later assigning/inspecting fate.
- Focused transaction and fate-introspection tests pass after the move.

## 2026-05-29 01:39 PDT - Storage/projection administration moved behind a small module

- Moved storage stats/version, local policy fingerprint, physical row lookup, and projection rebuild helpers into `runtime::storage_admin`.
- The `clear_current_projection_for_test` name is still a smell, but at least it is now isolated with other storage/projection maintenance APIs instead of buried in the read/query block.
- Focused storage projection tests pass after the move.

## 2026-05-29 01:43 PDT - Runtime read surface moved behind a read module

- Moved table reads, tiered reads, predicate reads, required-ref reads, recursive reads, and conflict-candidate reads into `runtime::reads`.
- This creates a clearer separation between local read semantics and sync/export mechanics.
- The first test run hit the disk ceiling after several recompiles; cleared generated `target/` output and reran focused generic, recursive, and conflict-candidate read tests successfully.

## 2026-05-29 01:50 PDT - Query export facade moved behind a module

- Extracted the public predicate/window query export and profiling methods into `runtime::query_export`, while leaving the shared query-scope bundle assembly in `runtime.rs` for now.
- This gives callers and future work a named API layer for "export a query-shaped sync bundle" without pretending the underlying scope machinery is already clean enough to be a standalone component.
- Focused query-scope export tests pass. This moved another roughly 425 lines out of the central runtime file and made the next cleanup decision sharper: either extract the internal query-scope assembly as its own subsystem, or extract writes/apply first.

## 2026-05-29 01:56 PDT - Row write API moved behind a write module

- Moved generic insert/update/upsert/conflict-resolution/delete/restore runtime APIs into `runtime::writes`.
- Left the lower-level history materialization helpers in `runtime.rs` for now because they are shared with transaction builders, batched writes, and bundle apply. The module boundary still helps by making the caller-facing row mutation semantics findable.
- Focused storage/projection tests pass, including delete/restore, batched writes, and hyphen-safe transaction identity.

## 2026-05-29 01:59 PDT - Runtime session/auth API moved behind a session module

- Moved runtime constructors, trusted/user session switching, attribution switching, and internal auth accessors into `runtime::session`.
- The compiler forced an explicit boundary: `policy_user`, `attribution_user`, and `bypasses_policy` are now `pub(super)` runtime-internal helpers instead of invisible methods accidentally shared by everything in one giant file.
- Focused auth/policy tests pass, including trusted admin writes, untrusted policy validation, and same-user/multiple-node authorship.

## 2026-05-29 02:01 PDT - Observed-query refresh API moved behind a query-refresh module

- Moved observed query read listing, query-read refresh export, and forgotten observed-query reads into `runtime::query_refresh`.
- This is a product-shaped boundary: durable intermediaries replay observed query descriptors to reconcile downstream clients. The code now has one obvious place for predicate/window/recursive refresh planning instead of burying that behavior in the sync/apply block.
- Focused query-read refresh tests pass across predicate, in/contains/ne, top-created, top-field, recursive, branch, and durable-restart cases.

## 2026-05-29 02:04 PDT - Sync export API moved behind a sync-export module

- Moved table-history export, exclusive-transaction forwarding export, recursive query-scope export, and batched recursive refresh export into `runtime::sync_export`.
- This keeps bundle production separate from bundle application. The remaining apply path has policy/fate/awaiting-dependency concerns and should move separately if it stays coherent.
- Focused recursive-query export/sync tests pass, including branch snapshots, tombstones, recursive policy ancestors, and refresh repair.

## 2026-05-29 02:06 PDT - Sync apply API moved behind a sync-apply module

- Moved bundle apply, profiled apply, untrusted apply, exclusive forwarding staging, and awaiting-dependency revalidation into `runtime::sync_apply`.
- This is the first extraction that names the authority-facing apply path directly. It separates "produce a bundle" from "validate and materialize a bundle into local SQLite state", which should make future policy/fate fixes less likely to land in export code by accident.
- Focused sync/fate tests pass, including fail-closed validation, protocol versions, stale pending/rejected fate, page-boundary repair, required ref repair, idempotence, and out-of-order global epochs.

## 2026-05-29 02:10 PDT - Small misplaced runtime methods moved to their owning modules

- Moved top-N read helpers into `runtime::reads`, the explicit `transaction()` constructor into `runtime::transaction_builder`, and `session_user_for_test` into `runtime::session`.
- These were leftovers from earlier extractions and are a good example of the cleanup rule for this PR: if a method has an obvious semantic owner, move it there even if the raw line count is small.
- Focused transaction tests pass after the move.

## 2026-05-29 02:11 PDT - Full mini-core test passes after the runtime split

- Ran `cargo test -p mini-jazz-sqlite` after the session/read/write/query-export/query-refresh/sync-export/sync-apply module extractions.
- Result: 434 passed, 19 ignored placeholders, 0 failed.
- Disk is still tight on this machine, but the warm Rust target cache was enough for a full validation pass.

## 2026-05-29 02:12 PDT - Noted query-descriptor persistence as a semantic cleanup target

- While checking for remaining broken-window patterns, I found the old durable observed-query descriptor behavior is still present: query reads are stored in SQLite and several tests assert refresh after runtime restart.
- This conflicts with the newer design direction that downstream clients should replay/resubscribe queries on reconnect and that query descriptors should not be persisted on disk.
- I am not flipping that behavior inside this broad architecture PR because it would require a dedicated semantic migration of restart/reconnect tests. The module split makes the later change more tractable: `runtime::query_refresh` owns listing/forget/export of observed descriptors, while apply-side recording remains an apply concern.

## 2026-05-29 02:14 PDT - Read context construction moved into the read module

- Moved `query_context`, `query_context_at_tier`, and `read_visibility` into `runtime::reads`.
- These are read-surface constructors used by several sibling modules, so they are now explicit `pub(super)`/`pub(crate)` helpers instead of tail-end methods in the central runtime file.
- Focused query-matrix tests pass after the move.

## 2026-05-29 02:17 PDT - Shared write materialization moved behind write-core

- Extracted the shared row write materialization path into `runtime::write_core`: effective value normalization, field validation, create/update history insertion, current projection insertion, local write-policy checks, row-id collision checks, exclusive-write conflict checks, and write-set recording.
- This boundary is behaviorally important because simple writes, batched writes, explicit transactions, and apply-history all need the same low-level write semantics.
- Focused storage/projection tests pass after the move.

## 2026-05-29 02:22 PDT - Delete staging now uses the shared write-core path

- Factored duplicate tombstone/current-projection delete materialization out of ordinary deletes and explicit transaction deletes into `runtime::write_core::stage_delete_row_in_tx`.
- Preserved the intentional API difference: simple deletes do not currently record a previous-row read set, while explicit transaction deletes do because transaction validation depends on the deleted row version being stable.
- Focused transaction and storage/projection tests pass after the move.

## 2026-05-29 02:24 PDT - History export helpers moved into an explicit module

- Lifted the ambient history/export helper family out of `runtime.rs` into `runtime::history_export`: transaction export, read-set export, branch provenance export, policy dependency export, query-scope repair candidates, visible/history version export, bundle construction, and SQL helper utilities used by export paths.
- This is still not the final abstraction: many helpers remain broad `pub(super)` because neighboring query/sync modules call them directly. The important improvement is that future work now has a named export boundary instead of adding more tail-end functions to the central runtime file.
- Focused query-matrix, recursive-query, and sync tests pass after the move.

## 2026-05-29 02:25 PDT - Apply-side repair methods moved to sync-apply

- Moved bundle application's query-scope repair and history-record materialization methods from the central runtime file into `runtime::sync_apply`.
- This makes `sync_apply` the owner of both the top-level apply APIs and the repair/materialization machinery they invoke, instead of splitting the apply path across an impl block in one file and helper methods in another.
- Focused sync-fate and query-matrix tests pass after the move.

## 2026-05-29 02:27 PDT - Query export orchestration moved to query-export

- Moved the remaining query-scope export orchestration methods from `runtime.rs` into `runtime::query_export`: batched predicate refreshes, top-N refresh exports, built-query export, query-read scope export, batched query scopes, and ref-include history collection.
- I briefly botched the mechanical lift by removing the methods before inserting them; restored the exact block from HEAD before continuing. This reinforced that large moves should stay uncommitted until the focused tests are green.
- Focused query-matrix and sync-fate tests pass after the move.

## 2026-05-29 02:29 PDT - Central runtime file reduced to wiring and tiny shared utilities

- Moved apply/dependency helper functions and `ApplyHistoryContext` into `runtime::sync_apply`, and moved `BatchedQueryScopeItem` into `runtime::query_export`.
- `runtime.rs` is now about 117 lines and mostly contains imports, module wiring, `Runtime`, `QueryScopeOptions`, and tiny display/SQL helpers that are still shared across modules.
- Focused sync-fate and transaction tests pass after the move.

## 2026-05-29 02:30 PDT - Full mini-core validation still passes

- Ran `cargo test -p mini-jazz-sqlite` after the runtime split and helper moves.
- Result: 434 passed, 19 ignored placeholders, 0 failed.
- This is an important checkpoint: the PR is no longer just a line shuffle, it has a full green baseline after the central runtime file was reduced to wiring.

## 2026-05-29 02:33 PDT - Temporary auth and branch scopes are panic-safe

- Added regression tests proving `run_as_user` and branch-scoped query execution restore the previous runtime state even if the caller closure panics.
- Implemented restore-then-resume semantics with `catch_unwind`/`resume_unwind`.
- This is a small API hardening slice but it matters for the cleanup theme: scoped runtime APIs should behave like real boundaries, not best-effort fixture conveniences.
- Focused restore/auth/branch-query tests pass.

## 2026-05-29 02:36 PDT - Started removing broad runtime wildcard imports

- Replaced `use super::*` with explicit imports in the newly touched `runtime::session`, `runtime::branches`, and `runtime::storage_admin` modules.
- This is intentionally incremental: the large modules still use broad imports, but new/modified boundary modules should show future agents the desired direction.
- Focused restore and storage/projection tests pass.

## 2026-05-29 02:37 PDT - Rechecked moving deep-history perf PR

- PR #973 head is now `f54c83c1d9ada44cf1bae407cac801462217b25f`, updated 02:33 PDT.
- It is still a draft spike centered on sealed history blocks, text op sidecars, block codecs/caches, and deep-history benchmark docs.
- Decision: continue avoiding wholesale merge into this architecture cleanup branch. The generic slices that fit this PR have already been ported; the remaining work should stay in the perf lane until its abstractions stabilize.

## 2026-05-29 02:40 PDT - Removed test-only user switching from runtime API

- Replaced branch tests that mutated a runtime's user with the real trusted-peer `run_as_user` API.
- Removed `Runtime::session_user_for_test`, so future tests must model user switching through the same API trusted peers use in production-shaped topologies.
- Focused branch tests pass after the replacement.

## 2026-05-29 02:42 PDT - Fixed tiered built-query reads over newer local current

- The review agent found a real semantic gap: built queries at `ReadTier::Edge`/`Global` filtered the current projection, so a newer local version could hide the older edge/global-visible row instead of reconstructing the tier-visible version.
- Added a regression test where a globally accepted pinned row is later locally updated to unpinned; local query hides it while global query still returns the global pinned version.
- Implemented the conservative tiered built-query path by reconstructing tier-visible rows from history, then applying built-query predicates/order/window to the semantic rows. This favors correctness over hot-path speed for non-local tier reads.
- Focused tiered policy/subscription tests pass.

## 2026-05-29 02:45 PDT - Made the single branch-policy table assumption explicit

- The branch policy API shape can hold multiple policy backing tables, but the current runtime evaluation path used the first map entry. That was an attractive broken-window trap.
- Added schema validation that rejects more than one branch policy backing table per row table until we define composition/selection semantics.
- Renamed the internal helper from `active_branch_policy` to `single_branch_policy` so the limitation is visible at call sites.
- Focused schema validation test passes.

## 2026-05-29 02:48 PDT - Replaced runtime write op integers with a semantic enum

- The storage/protocol layer still encodes create/update/delete as integer op codes, but runtime write construction should not pass raw `1`, `2`, and `3` as intent.
- Added `WriteOp` at the shared write-core boundary and converted simple writes, batched writes, explicit transactions, deletes, and write-set recording through it.
- This keeps numeric encoding at the SQLite/protocol edge while giving future write-pipeline work a typed vocabulary.
- Focused storage/projection and transaction tests pass.

## 2026-05-29 02:50 PDT - Named delete read-set behavior instead of passing a boolean

- `stage_delete_row_in_tx` no longer accepts `record_row_read: bool`; it takes `DeleteReadSetMode`.
- Simple delete calls now state that their read-set shape is already covered by write-call semantics, while explicit transaction deletes state that they record the previous row.
- This preserves current behavior but removes another ambiguous write-core flag from the API future work will copy.
- Focused delete-related tests pass.

## 2026-05-29 02:52 PDT - Tightened imports for the write-facing runtime modules

- Replaced wildcard runtime imports in `runtime::writes` and `runtime::write_batch` with explicit dependencies.
- This is deliberately small but useful: these modules are now examples of the desired post-split shape, where write APIs declare the runtime helpers they actually depend on.
- Focused storage/projection and transaction tests pass.

## 2026-05-29 02:54 PDT - Tightened transaction-builder imports

- Replaced the transaction-builder wildcard runtime import with explicit dependencies on transaction snapshots, write-core staging helpers, runtime state, and projection repair.
- This makes the explicit transaction layer read more like its own API boundary instead of an extension of the old monolithic runtime namespace.
- Focused transaction tests pass.

## 2026-05-29 02:57 PDT - Made observed query descriptors connection-local

- Implemented the newer sync design direction: runtime open clears `jazz_query_read`, so query interest does not persist on disk across durable node restarts.
- Existing cached rows/history remain durable; downstream clients or tabs must replay query descriptors after reconnect/restart, and applying those refresh bundles records the descriptors for the live connection again.
- Converted restart tests from "worker remembers query descriptors" to "client replays query descriptors" while preserving the same refresh/repair assertions.
- Unignored the invariant that durable observed query reads are connection-local.
- Focused `after_restart`, `query_read`, and restarted subscription tests pass.
