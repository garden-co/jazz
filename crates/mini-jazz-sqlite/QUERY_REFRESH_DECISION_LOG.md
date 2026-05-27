# Query Refresh Decision Log

## 2026-05-27 11:36 PDT

Goal: turn the small `$createdAt` refresh batching PR into the ambitious version:
an explicit, generic query-scoped refresh/export planner that can group compatible
observed query descriptors, avoid one-bundle-per-read behavior where possible, and
leave the implementation easier to extend for future descriptor families.

Current branch: `codex/sqlite-query-refresh-batching`, stacked on
`codex/sqlite-core-perf-benchmarks`.

Starting point:

- same-shape `eq_top_field_desc` observed reads already batched;
- same-shape `eq_top_created_at_desc` was added as a narrow helper;
- the two helpers duplicate most of the export mechanics;
- non-page predicates still refresh one bundle at a time.

Plan:

- first refactor the page refresh batching into a small descriptor/planner shape
  without changing behavior;
- then add red/green tests for broader compatible descriptor batching;
- finally run the whole mini-jazz test suite and push the stacked PR forward.

## 2026-05-27 11:37 PDT

Design discovery: the existing batched top-field helper and the newly added
top-created-at helper are really instances of one lower-level operation:
collect several query-scope items for one table/field into one bundle, while
deduping visible rows, repair rows, rejected txs, policy dependencies, reads,
txs, branches, and query read records.

Next implementation move: introduce that internal operation and make the page
helpers call it. Then ordinary observed predicates (`eq`, `ne`, `contains`,
`in`) can use the same planner/grouping path where their table/field/op match.

## 2026-05-27 11:40 PDT

First green slice:

- added `export_batched_query_scopes` as the shared export primitive;
- routed top-field, top-created-at, and ordinary predicate refresh grouping
  through it;
- added a test that two same-shape equality subscriptions refresh as one
  bundle;
- full `cargo test -p mini-jazz-sqlite` is green.

Notable correction: because the batched page helpers now share the same export
primitive as ordinary query scopes, branch base snapshot history is included for
batched page refreshes too. That was easy to miss while the helpers were
duplicated.

## 2026-05-27 11:41 PDT

Second slice: recursive query refreshes can batch too, but they should not be
forced through the ordinary predicate export primitive. Recursive exports have
extra semantics: deleted descendant tombstones and recursive scope repair
history. Added a separate same-shape recursive grouping path and a regression
test with two subscribed roots refreshing through one bundle.

## 2026-05-27 11:43 PDT

Release perf sanity: the existing `multi_query_refresh_probe` now naturally
reports one exported refresh bundle for four same-shape page subscriptions:

```json
{
  "query_count": 4,
  "separate_bundle_count": 1,
  "separate_bundle_bytes": 49373,
  "merged_bundle_bytes": 49373,
  "separate_apply_ms": 1.551542,
  "merged_apply_ms": 1.683125,
  "separate_history_rows": 126,
  "merged_history_rows": 126,
  "separate_transaction_rows": 5,
  "merged_transaction_rows": 5,
  "merged_observed_facts": 4
}
```

The old field names are now misleading: this is no longer "separate bundles
then merge" for same-shape refreshes. Next small cleanup is to rename those
metrics so the benchmark describes the new planner behavior directly.

## 2026-05-27 11:45 PDT

Renamed the multi-query refresh probe metrics from "separate" to "refresh" /
"equivalent merged" so the benchmark matches the planner behavior. Re-ran the
release probe:

```json
{
  "query_count": 4,
  "refresh_bundle_count": 1,
  "refresh_bundle_bytes": 49373,
  "equivalent_merged_bundle_bytes": 49373,
  "refresh_apply_ms": 1.526792,
  "equivalent_merged_apply_ms": 1.431542,
  "refresh_history_rows": 126,
  "equivalent_merged_history_rows": 126,
  "refresh_transaction_rows": 5,
  "equivalent_merged_transaction_rows": 5,
  "refresh_observed_facts": 4
}
```

Spec updates made:

- query descriptors should be planned in compatible groups before bundle
  assembly;
- prototype-proven batchable families are ordinary predicates, ordered pages,
  and recursive ref descriptors.

## 2026-05-27 11:46 PDT

PR pushed and description updated. One more architecture cleanup before calling
this done: the behavior is now planner-shaped, but the code still has the
planner logic inline in `export_query_read_refreshes`. I want the implementation
to expose an explicit refresh plan enum so adding new descriptor families does
not mean growing a pile of ad hoc maps in the runtime method.

## 2026-05-27 11:50 PDT

Added `QueryRefreshPlan` and `plan_query_read_refreshes`. The runtime method now
does two clearer phases:

1. classify compatible observed query descriptors into refresh plans;
2. lower each plan into one bundle using descriptor-family-specific export
   semantics.

Targeted batching tests are green after the refactor.
