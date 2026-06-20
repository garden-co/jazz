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

## 2026-05-27 11:57 PDT

Next goal: try true multi-value SQL lowering for at least one hot descriptor
family. Start with ordered page refreshes because dashboard/page subscriptions
are the most important shape. Desired end state: one SQL statement can return
top-N rows per bound predicate value, using a `VALUES` CTE plus a window
function, instead of looping through values and issuing one query per value.

## 2026-05-27 12:01 PDT

Implemented true multi-value SQL lowering for main-branch ordered page reads:

- `eq_top_field_desc`: `VALUES` CTE of predicate values plus
  `row_number() over (partition by value_index order by order_field desc,
row_num)` to return top-N per value in one statement.
- `eq_top_created_at_desc`: same shape, ordered by `j_created_at desc`.

Branch overlays deliberately fall back to the old per-value path because sparse
overlay visibility/source precedence is more subtle than main-branch current
projection reads. This keeps the first SQL-lowered slice correct while still
covering the hot dashboard/reconnect path.

Targeted query refresh batching tests are green.

## 2026-05-27 12:03 PDT

Added `refresh_export_ms` to the multi-query refresh probe so this optimization
has an observable metric. Release sanity:

```json
{
  "query_count": 4,
  "refresh_bundle_count": 1,
  "refresh_bundle_bytes": 49373,
  "refresh_export_ms": 25.232792,
  "refresh_apply_ms": 1.788167,
  "refresh_history_rows": 126,
  "refresh_transaction_rows": 5,
  "refresh_observed_facts": 4
}
```

The metric is still whole-export time, not isolated SQL read time. It includes
policy dependency export, repair history, read-set export, tx export, and bundle
assembly. That is useful product-wise, but we may still want a more focused
profile later if reviewers ask whether the SQL part itself improved.

## 2026-05-27 12:38 PDT

Subagent review found three useful issues:

1. main-branch multi-value `$createdAt` lowering accidentally stopped accepting
   semantic fields supported by `read_rows_where_eq`, such as `id` and
   `$createdBy`;
2. unbounded grouped values could create huge SQL statements / too many bind
   parameters;
3. ordinary predicate batching tests only covered `eq`, despite planning `ne`,
   `contains`, and `in`.

Decision: fix all three in this PR.

## 2026-05-27 12:41 PDT

Review fixes:

- chunk multi-value ordered page SQL at `400` values per statement;
- preserve the previous semantic-field behavior for `$createdAt` ordered
  queries over `id` and `$createdBy` by falling back to the semantic equality
  path;
- extend apply-side query-scope repair for `$createdAt` ordered descriptors
  over `id` and `$createdBy`;
- add focused tests for `$createdBy` ordered pages and for batched `ne`,
  `contains`, and `in` predicate refreshes.
