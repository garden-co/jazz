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
