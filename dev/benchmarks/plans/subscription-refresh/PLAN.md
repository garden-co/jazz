# Plan: one Groove flush per Jazz subscription refresh

Status: design and disposable A/B evidence only. No production worktree change.

## Problem

After a write, Jazz iterates every live subscription. Draining each local
maintained subscription currently calls `Groove::Database::flush()`, which runs
another complete IVM tick over Groove outputs.

The effective path is:

```text
one committed write
  -> Jazz refresh loop over J live subscriptions
       -> Groove empty tick
            -> output/subscription work
```

The initial commit already ran the tick containing the write. Repeating an
empty tick for each Jazz receiver dominates write latency even when the write
is unrelated and no client receives an event.

Source:

- `crates/jazz/src/db.rs`, `refresh_subscriptions_in`;
- `crates/jazz/src/node/query_eval.rs`,
  `drain_local_maintained_view_subscription_transitions`;
- `crates/groove/src/db/mod.rs`, `Database::flush`;
- `crates/groove/src/ivm/runtime/mod.rs`, output delivery in an IVM tick.

## Evidence

The disposable prototype moves the flush before the Jazz loop and removes it
from individual receiver drains. The existing public benchmark and oracle were
unchanged.

### 100 active routes

| Phase                    |  Current | Flush-once prototype | Improvement |
| ------------------------ | -------: | -------------------: | ----------: |
| Matching one-row commit  | 202.8 ms |              7.04 ms |       28.8x |
| Unrelated one-row commit | 203.7 ms |              5.50 ms |       37.0x |
| 100-row same-team batch  | 235.6 ms |              32.8 ms |        7.2x |
| Below-boundary insert    | 237.1 ms |              7.32 ms |       32.4x |

### 1,000 active routes

| Phase                    | Current | Flush-once prototype | Improvement |
| ------------------------ | ------: | -------------------: | ----------: |
| Matching one-row commit  | 37.72 s |             134.8 ms |        280x |
| Unrelated one-row commit | 37.55 s |             132.2 ms |        284x |
| 100-row same-team batch  | 37.84 s |             268.2 ms |        141x |
| Below-boundary insert    | 37.77 s |             121.9 ms |        310x |

All maintained-stream resets and deltas matched the independent oracle. At
1,000 routes, direct tracing attributed about 52–74 ms to the single shared
flush and 56–78 ms to the complete refresh. The remaining commit time was
primarily the initial nonempty IVM tick.

This prototype does not improve hydration: 1,000 small-team subscriptions still
took 54.48 s to open and retained an estimated 2.36 GB of private maintained
state. It also intentionally does not mask the independent multi-identity
one-shot correctness failure.

## Proposed design

Split refresh into an explicit cycle:

```text
refresh_subscriptions_in:
  prune/upgrade live subscription handles
  if no local maintained receiver needs draining:
      skip Groove flush
  else:
      flush Groove exactly once
  for each live Jazz subscription:
      drain its already-produced messages
      consolidate transitions
      update snapshot and emit at most one Jazz event
```

The drain method must not itself advance Groove. Name the two responsibilities
accordingly, for example:

- `flush_local_maintained_views`;
- `drain_local_maintained_view_transitions_without_flush`.

Keep authoritative-reset, remote-settled, and non-maintained subscription paths
unchanged. Do not hold a `RefCell` borrow across stream delivery.

## Proof obligations

Before landing, establish:

1. Applying a receiver's transitions cannot enqueue Groove work required by a
   later receiver in the same loop.
2. Every relevant write/inbound-sync path reaches the one shared flush before
   any maintained receiver is drained.
3. A receiver opened, dropped, or rejected during the cycle cannot be skipped
   or double-drained.
4. Multiple Groove messages for one receiver are consolidated into the same
   final Jazz event as before.
5. A cycle with zero local maintained receivers performs zero empty ticks.
6. Errors leave queued receiver messages recoverable or fail the whole cycle
   consistently; there is no partially acknowledged state.

## Implementation phases

### Phase A: observability

Add counters:

- `jazz_subscription_refresh_cycles`;
- `groove_flushes_from_jazz_refresh`;
- `jazz_subscription_receivers_scanned`;
- `jazz_subscription_receivers_changed`;
- `jazz_subscription_refresh_us`;
- `jazz_subscription_drain_us`.

Expose them through test runtime stats and the SaaS receipt.

### Phase B: batch the flush

- Upgrade/prune handles first.
- Detect whether any retained local maintained receiver needs the flush.
- Flush once.
- Drain all receivers without another tick.
- Preserve the old path behind a test-only differential helper until the
  transition matrix passes.

### Phase C: route dirty work

After the flush-once change lands, use Groove output notifications or a dirty
receiver set so Jazz does not inspect every quiet stream. This is a separate
optimization: flush-once removes repeated global work; dirty routing removes
the remaining `O(J)` scan.

## Tests

Use public black-box Jazz APIs and the independent page oracle:

- 0, 1, 100, and 1,000 live subscriptions;
- one team with many viewers and many teams with one viewer;
- matching, unrelated, below-boundary, same-team batch, and spread batch;
- add, remove, update, team move, membership revoke/restore;
- local write and inbound sync application;
- remote-settled and authoritative-reset transitions;
- dropped/disconnected receiver during refresh;
- multiple writes before a drain and net-zero batches.

Mechanism gates:

- exactly one refresh-origin Groove flush per completed write cycle with local
  maintained receivers;
- zero refresh-origin flushes with none;
- one unrelated write emits zero Jazz events;
- work does not grow as `J * Groove outputs`.

## Acceptance

On the existing 1,000-route benchmark:

- every stream and one-shot control that is independently valid remains exact;
- matching and unrelated one-row writes stay below two IVM ticks of work;
- a below-boundary write does not pay a per-subscription empty tick;
- the counter ratio is one refresh flush per cycle, never one per receiver.

Tooling friction: the benchmark had to infer repeated empty ticks from residual
time; direct refresh/flush counters should be present before implementation.
