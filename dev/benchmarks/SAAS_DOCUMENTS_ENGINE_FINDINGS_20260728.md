# SaaS documents engine findings

This note interprets
`SAAS_DOCUMENTS_500K_RECEIPT_20260728.md` and scopes the next work.

## Why the initial query takes 8 seconds

The 8-second observations are Local query work on a node holding all 500,000
documents. They are not database-open or network time. The parameterized hot
query drops from 7.99 s on its first binding to 171.5 ms on reuse, which isolates
a large shape/binding-hydration component. The literal hot query remains at
8.58 s initially and 8.52 s on its repeated observation, so that path also has
a persistent per-evaluation cost; it should not be described as cold-only.

Several costs stack:

1. Local query planning explicitly returns no secondary access paths
   (`crates/jazz/src/node/query_eval.rs:4850-4857`). The root document source
   therefore starts from all 500,000 rows rather than the selected team.
2. Local visible-current evaluation unions global and ahead-current rows,
   computes the winning version with `ArgMax`, and anti-joins deletion winners
   (`crates/jazz/src/node/codec.rs:1492-1595`).
3. Authenticated reads build and join the membership-policy graph. The fixture
   adds 50,000 membership rows to the cold source work.
4. Groove eagerly hydrates referenced sources and builds join arrangements for
   a new graph shape/binding.
5. `TopBy(100)` copies the complete touched bucket, builds sort keys, and fully
   sorts it. It reconstructs and sorts both the before and after windows
   (`crates/groove/src/ivm/runtime/mod.rs:5815-5821`,
   `crates/groove/src/ivm/runtime/mod.rs:7866-7930`).

The isolated RocksDB full scan is roughly 0.18-0.24 s. The remaining difference
is therefore in Jazz visible-current, policy, graph hydration, arrangement, and
materialization work rather than simply reading 500k keys. This benchmark does
not yet attribute an exact percentage to each phase.

A partial client holding only its subscribed rows avoids the 500k local source,
but a history-complete serving node still needs a selective initial access path.

## The simultaneous-binding correctness issue

The parameterized latest-100 query is intended to share one maintained graph
across team bindings. It currently does not partition the window by the binding:

- the top-level slice has no explicit partition key;
- `lower_window` groups TopBy only by that slice partition
  (`crates/jazz/src/node/query_engine/lowering.rs:3992-4047`);
- Groove applies the binding route filter after the shared terminal graph
  (`crates/groove/src/ivm/runtime/mod.rs:3361-3380`).

Consequently multiple team bindings can share one global Top-100 window and only
then filter by team. The two-binding canary observed the hot binding returning 0
rows instead of 100.

This is a correctness blocker for the many-active-team scenario. It can also
turn the TopBy bucket into the sum of documents across active bindings, making
write maintenance increasingly expensive.

The separate `CurrentRow::cell` failure appears to be a nullable/projection
descriptor mismatch for a parameter-constrained field. It should receive its
own regression test and fix.

## Writes with active subscriptions

For one correct, literal subscription over a team that starts with 30,000 rows:

- one matching insert costs 8.45 ms p50;
- aggregate metrics place almost all of that time in the IVM tick rather than
  storage; source inspection implicates TopBy, but there is no TopBy-only timer;
- one unrelated-team insert costs 0.046 ms p50;
- `commit_batch` for 100 matching inserts costs 9.39 ms after row construction
  and staging; that run had 30,020 team rows before the batch;
- 100 matching inserts as separate commits repeat the full touched-bucket work
  per commit and are approximately two orders of magnitude more expensive than
  the measured batch commit phase.

With many parameterized team subscriptions, the intended Groove architecture is
better than the old engine: graph nodes and arrangements are shared, and only
the matching route should touch a TopBy group. Today the routing/window bug
invalidates that expectation. Runtime delivery also visits active subscription
states, so fan-out still needs a 1/100/1,000-binding measurement after the
correctness fix.

## Comparison with the previous engine

There is no apples-to-apples retained old-engine receipt for this workload.
Source inspection of the pre-swap tree (`489474ed4`) shows:

- an explicit literal team predicate could use the single-column team index;
- it still enumerated and materialized every document in that team, sorted all
  of them, and only then applied the limit;
- a policy-only query without the explicit team predicate scanned all
  documents;
- every subscription owned a separate graph and sorted candidate set;
- every documents-table mutation dirtied every documents subscription,
  including subscriptions for unrelated teams.

The old engine may therefore have hydrated an explicit-team literal faster than
today's broken prepared Local path, but it was structurally worse for many
active subscriptions. Neither engine has the desired bounded ordered Top-K
path.

## Recommended order of work

1. **Fix route-aware window correctness.** Add routing fields to the TopBy
   partition for maintained parameterized queries. Add a black-box regression
   with two teams, two simultaneous bindings, isolated initial snapshots, and
   mutations to each team.
2. **Fix parameterized row projection.** Make `CurrentRow::cell` agree with the
   public nullable schema for constrained route fields.
3. **Add the fan-out benchmark.** Sweep 1, 100, and 1,000 active team bindings;
   compare one 100-row transaction with 100 one-row transactions and verify that
   unrelated subscribers receive no events.
4. **Make initial reads selective.** Preserve secondary access paths in
   prepared/maintained plans and support the relevant Local/current source.
5. **Add a bounded ordered index path.** Use a composite index such as
   `(team, status, archived, updated_at, id)`, reverse-scan only the first 100
   matches, then fetch at most 100 base rows.
6. **Replace full TopBy sorting.** Maintain ordered per-team Top-K state so a
   matching update is logarithmic rather than two full team sorts.

Correctness is the immediate priority. After that, bounded initial index access
has the largest read-latency payoff; ordered incremental Top-K is the next
steady-state write optimization.

Tooling friction: an old-engine retained receipt and phase timers around Jazz
source hydration, policy evaluation, and TopBy would remove the remaining
source-level inference.
