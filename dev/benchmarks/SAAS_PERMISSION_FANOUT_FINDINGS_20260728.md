# SaaS permission and fan-out engine findings

This note interprets
`SAAS_PERMISSION_FANOUT_RECEIPT_20260728.md` and ranks the next work.

Follow-up probing on 2026-07-29 minimized the two correctness failures,
validated three disposable optimization directions, and split the work into
implementation-ready documents at [`plans/PLAN.md`](plans/PLAN.md). In
particular, the 99-row public result is authorization multiplicity reaching
`TopBy`, and the later zero-row one-shots come from a shared prepared graph that
embeds the first identity instead of binding its claim value at runtime.
Methods and qualifications are in
[`SAAS_DEEP_DIVE_RECEIPT_20260729.md`](SAAS_DEEP_DIVE_RECEIPT_20260729.md).

## 1. Writes run a Jazz-subscription × Groove-output empty-tick path

This is the largest measured steady-state bottleneck.

`Db::refresh_subscriptions_in` iterates every live Jazz subscription after
every local write (`crates/jazz/src/db.rs`). For each maintained subscription,
`drain_local_maintained_view_subscription_transitions` calls
`self.database.flush()` (`crates/jazz/src/node/query_eval.rs`). Groove
`Database::flush()` runs a complete empty IVM tick, and that tick iterates all
multisink subscriptions and outputs
(`crates/groove/src/ivm/runtime/mod.rs`).

The resulting structure is:

```text
one write
  -> for each live Jazz subscription (J)
       -> Groove empty tick
            -> for each multisink subscription/output (G)
```

The cost is O(J × G plus other tick work). In the benchmark J and G are both
N, so this becomes N-by-N.

The receipt matches that source path:

- 100 routes: unrelated write = 189 ms;
- 1,000 routes: unrelated write = 37.55 s;
- only 43 ms was in the initial commit's Groove IVM timer; the repeated empty
  Groove ticks land in the 37.51 s residual;
- no subscriber received an event.

The first performance fix should flush Groove once before the Jazz
subscription loop, then drain each subscription receiver without another
tick. Add a black-box canary proving that one matching route changes and all
unrelated routes stay quiet.

## 2. Initial hydration is full-source and binding-local

One 529,900-row binding takes 11.84 s. Ten bindings take 118.00 s, and the last
binding still takes 11.82 s. Three 2M-document policy bindings each take
61–67 s.

The current Local query path has no selected secondary access path, so a
current-team query hydrates the whole local visible-current source and policy
relations. Shape sharing does not make a new binding selective.

After the empty-tick fix, initial reads need:

1. a Local/current access path for the team predicate;
2. a composite ordered path such as
   `(team, archived, status, updated_at, id)`;
3. bounded reverse iteration that stops after 100 authorized matches;
4. binding hydration from those candidates rather than every document and
   permission row.

## 3. Private maintained state is far larger than the visible page

Representative receipts:

- one 529,900-row/Top-100 binding: approximately 121 MB;
- 200 viewers of one 30k-document team: approximately 19.62 GB, about
  98 MB/viewer;
- 1,000 small customer routes: approximately 2.36 GB despite only 849 total
  visible rows.

These are structural estimates for private local-subscription
maintained/control state, not process RSS or total database memory. They
exclude shared Groove arrangements, whose encoded-size estimates were 5.73 GB
in the hot-200 lane and 5.47 GB in the 2M-document lane, as well as storage,
queues, and allocator overhead.

Per-binding state retains source/version indexes far beyond the 100-row
output. Shared graph structure alone is therefore insufficient. Binding-local
state should retain route-specific candidates, Top-K state, and small delivery
snapshots—not another large visible-current index.

## 4. TopBy maintenance recomputes too much

For 200 viewers of one 30k-document team:

- a matching write spends 7.16 s in Groove IVM;
- a below-boundary write spends 7.24 s in Groove IVM and emits no client
  event;
- a 100-row transaction spends 7.92 s in Groove IVM.

The desired design maintains an ordered per-route Top-K structure. A new
boundary-losing row should be logarithmic and should not rebuild/sort the
30k-row bucket for every viewer route.

## 5. Local drops and one-shot reads leave stale Groove outputs

The integrated 100-route churn lane first sampled four one-shot reads. Jazz
still reported 100 live local subscriptions, while Groove grew from 100 to
104 outputs. Dropping 99 live streams then left Jazz at one and Groove at 104.

Updating an organization unused by every subscriber took 158.15 ms. Its
initial Groove commit tick emitted 100 notifications / 400 records, while no
client page membership changed. It reaped the 99 deliberately dropped
outputs, leaving the four one-shot outputs. A later unbound-team document
write did not reap those four. Prepared-shape metadata grew from 200 to 205
during the one-shot samples and remained there after churn cleanup.

Local stream cleanup is only installed for subscriptions with upstream
handles (`crates/jazz/src/db.rs`). A local-only `SubscriptionStream` drop does
not directly unsubscribe its Groove multisink. Groove discovers a closed
receiver only when it tries to send a non-empty delta
(`crates/groove/src/ivm/runtime/mod.rs`).

This can make G larger than J in the J × G refresh cost. Both one-shot
completion and local stream drop should explicitly detach their multisinks and
release binding/graph state instead of relying on a future non-empty sender
attempt.

## 6. Two correctness issues still block broader policy claims

### Public OR branch changes Top-100 membership

Team membership, organization admin, and direct ACL compose correctly. Adding
the public/published policy alternative makes a single subscriber return 99
rows instead of the independently expected 100, with one boundary row missing.

The minimized two-document regression proves the mechanism: overlapping grant
derivations retain bag multiplicity through inner authorization joins and
consume weighted `TopBy` slots. Duplicate grants inside one policy branch fail
the same way. A disposable route-aware semijoin correction makes the minimal
cases and realistic public/admin tiers exact. See
[`plans/authorization-correctness/PLAN.md`](plans/authorization-correctness/PLAN.md).

### Later one-shot policy routes return zero rows

Live subscriptions for multiple identities remain exact, but sampled
`all_for_identity` reads after opening several policy-bound routes return zero
rows for non-first routes. The same query with one route passes.

The minimized case needs no application live subscriptions. Its winner depends
on call order and requires both a changed identity and an application
parameter. The outer prepared descriptor contains the user `team` parameter
but omits built-in claim `sub`, while the graph embeds the first subject UUID
as a literal and is reused under one stable shape name. Segregating identities
makes the probe pass but conflicts with the intended sharing model; the plan
instead propagates the claim path as a runtime binding/route field.

## What to focus on next

Recommended order:

1. Fix existential authorization and complete prepared claim routing.
2. Rebind or close live subscriptions on claim revision; the minimized
   revocation case leaks later rows through the stale stream.
3. Move the Groove flush outside the per-subscription refresh loop.
4. Make local subscription drop and one-shot completion detach Groove outputs.
5. Make hydration selective and bounded by a composite ordered access path.
6. Replace full `TopBy` bucket rebuilding with maintained ordered state.
7. After authorization deduplication, factor team-wide pages before viewer
   routing.

The next benchmark extension should mutate permissions directly: membership
and role revoke/restore, direct ACL grant/revoke, organization/team suspension,
document moves, reconnect, and one user belonging to many teams.

Batching is already essential: one 100-row transaction pays refresh once,
whereas 100 separate inserts pay it 100 times. It is a mitigation, not a
substitute for fixing the refresh path.

Tooling friction: per-phase timers for Jazz refresh, Groove empty ticks, route
delivery, and TopBy maintenance would turn the remaining source attribution
into direct measurements.
