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
