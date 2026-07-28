# Plan: deterministic one-shot and local-stream teardown

Status: source-attributed lifecycle reproduction. No production worktree
change.

## Invariant

Completing a one-shot read or dropping a local subscription releases its Groove
output, binding reference, and private maintained state synchronously—or via a
bounded cleanup queue that does not depend on a future matching write.

## Evidence

The 100-route benchmark observed:

- four sampled one-shot reads grew Groove outputs from 100 to 104 while Jazz
  still reported 100 live streams;
- dropping 99 streams left Jazz at one and Groove at 104;
- a later nonempty unrelated update reaped the 99 dropped-stream outputs but
  retained the four one-shot outputs;
- an unbound-team document write did not reap those four.

Prepared-shape metadata also grew during the one-shots and remained after churn.
Stale outputs amplify the subscription-refresh cost and retain private state.

## Source

- One-shot execution at
  `crates/jazz/src/node/query_eval.rs:6012-6024` calls
  `bind_shape(...).recv()` and drops the receiver without
  `Database::unsubscribe`.
- `MultisinkSubscription` at
  `crates/groove/src/ivm/runtime/mod.rs:3071-3089` has no drop cleanup.
- Local-only stream construction at `crates/jazz/src/db.rs:1086-1101` installs
  no cleanup closure; cleanup exists only when there are upstream handles.
- Groove discovers a closed receiver only when a nonempty send fails
  (`crates/groove/src/ivm/runtime/mod.rs:356-390`).
- Explicit unsubscribe and binding retraction already exist at
  `crates/groove/src/ivm/runtime/mod.rs:1117-1157`.

## L1: explicit ownership and unsubscribe

1. Return an owned local multisink handle/id with every Jazz local stream.
2. Compose its idempotent unsubscribe guard with existing upstream cleanup.
3. Add the same guard around every one-shot prepared binding.
4. Run cleanup on success, stream close, materialization/decode error, and
   panic unwind where supported.
5. Retract the binding reference and collect unretained ephemeral nodes after
   the last usage handle leaves.
6. Never require a future IVM notification to discover receiver death.

Drop must not borrow an already-mutably-borrowed `Db` and panic. If synchronous
cleanup is impossible from `Drop`, enqueue an idempotent cleanup token in a
bounded queue and drain it at deterministic API boundaries.

## L2: a non-retained one-shot primitive

Add a Groove operation such as:

```text
bind_shape_snapshot / query_prepared_once
```

It should hydrate a bound output at one logical frontier without registering a
long-lived sender or output retainer.

- Reuse shared graph nodes and arrangements.
- Never enter the active-output fan-out collection.
- Release binding params immediately after materialization.
- Bound prepared-shape metadata through explicit ownership/refcounts or an LRU
  keyed by semantic plan identity.

This removes lifecycle risk instead of relying solely on careful unsubscribe
around a retained API.

## Gates

- N identical one-shots leave active outputs, binding params, graph nodes, and
  retained bytes at baseline;
- N distinct bindings do the same;
- dropping a local stream decrements outputs without another write;
- success and injected error/materialization paths clean up;
- dropping one of several siblings cannot reset or retract surviving routes;
- repeated read/drop loops keep memory and unrelated-write work bounded;
- cleanup is idempotent under explicit close plus `Drop`;
- client/edge/upstream cleanup composes with local output cleanup;
- claim-revocation closure cannot leave an old authorized output behind.

Expose output ownership by class—one-shot, local stream, served stream—and
counts for explicit unsubscribe, queued cleanup, cleanup failure, retained
binding params, and graph collection.

## Acceptance

After four one-shots and dropping 99 of 100 local streams, Groove must report
exactly one output without any intervening write. Repeating the cycle must not
increase graph nodes, prepared params, retained bytes, or unrelated-write
latency.

Tooling friction: runtime stats need per-owner output counts and a deterministic
“drain cleanup queue” test hook.
