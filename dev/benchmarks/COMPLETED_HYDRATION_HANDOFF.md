# Reuse completed hydration in queued subscriptions

A subscription captures its evaluator state when it enters the queue. An older
subscription can then finish preparing a shared node while the new subscription
waits. Previously, releasing the temporal barrier did not refresh the waiting
snapshot, so the later subscription could compute that node again.

After a successful retained hydration installs its complete state, its immediate
hydration successor can receive a compatible node snapshot before the temporal
barrier releases. The handoff includes the root operator state, arrangements,
metadata and memo. Copying only the returned rows would not preserve the
physical aggregate/index state required for later updates.

## Eligibility and lifecycle

The recipient must still be waiting on the node, and the node cannot belong to
its borrowed prepared-shape state. Captured input generation, live generation
and memo watermark must match. Nodes depending on prepared bindings or recursive
frontiers keep their private snapshots. The existing evaluator still validates
producer readiness after the handoff.

Only a successfully installed retained hydration donates state. Incremental
completion, first-result completion, cancellation and failure retain their
existing temporal release behavior. A failed output snapshot cannot donate.
The handoff stays within one runtime and one graph node identity; it introduces
no cross-identity result cache or additional storage requests.

For example, eight queued subscriptions to the same grouped aggregate now
compute its three nodes once: 3 computations instead of 24. Every subscriber
still receives the full initial groups and exact later insertion/deletion
deltas, including deletion of the last row in a group. A write queued behind
four blocked aggregate hydrations is also delivered exactly once to each.

The new public-API integration tests are in
`crates/groove/tests/completed_hydration_handoff.rs`. Exact values establish
semantics; runtime work counters separately establish bounded shared work.
The fanout assertion fails on the control (24 versus 3) and passes with the fix.
No existing test assertions were changed.

## Native comparison

The [complete receipt](receipts/completed-hydration-handoff.json) retains all
24 timed observations, phase breakdowns, exact-result signatures and source/
binary hashes. The host scheduler comparisons use ABBAABBA, four runs per arm
and width, plus excluded warmups. The direct scheduler regression check uses
two runs per arm and width. Both binaries use identical benchmark files and
Rust 1.93.1, `--profile perf`, no default features, and
`testing,transport-compression-zstd`. The control is the frozen unchanged
runtime binary; the final implementation contains no diagnostic switches.

The measured native baseline is the local performance integration `8eb69d5330a1`
plus the diagnostic harness, including the separate #3545 first-result changes.
The port onto this PR stack has the same handoff patch but a different surrounding
baseline. These measurements establish the effect in that integration; they are
not an exact comparison of this PR head against its GitHub base. Qualification
of the port and application endpoint remains separate.

The staged Local fixture has 600 synthetic rows, 37 subscriptions and 17 tables.
It opens the detail phase after the full list completes. Setup is excluded.
Memory storage and manually driven owner turns isolate runtime work; this is
not browser/IndexedDB latency or an application startup speedup measurement.

| Host scheduling, total completion |    Control |    Handoff | Speedup |
| --------------------------------- | ---------: | ---------: | ------: |
| Small strings                     | 404.867 ms | 379.775 ms |  1.066x |
| 256-byte strings                  | 477.614 ms | 438.132 ms |  1.090x |

The wider-row full list improves from 386.033 to 350.099 ms. Its owner phase
falls from 171.291 to 140.417 ms and foreground work from 134.780 to 129.620 ms.
These are active poll wall times; they are not OS CPU counters. Direct scheduling
stays essentially flat: 356.783 to 356.466 ms for small strings and 408.708 to
405.853 ms for wider strings, with only two samples per arm.

Owner hydration computations fall from 1680 to 1453 for small strings and from
1711 to 1499 for wider strings, over the same 1453 distinct nodes. The receiver
falls from 1641 to 1635. All expected rows, fields and signatures match; final
graph and arrangement row counts are unchanged. The wider owner's logical memo
payload increases from 130.1 to 133.5 MB. These counters count encoded payloads
per memo entry, not unique allocations, peak memory or storage I/O.

This fixes duplicated work in overlapping initial subscriptions. It does not
remove the other startup work or establish a general 1.09x application gain.
Browser endpoint validation and the full landing gates remain separate.

Reproduction:

```sh
JAZZ_FAIR_LAYOUT=mixed-local JAZZ_FAIR_HOST_SCHEDULER=1 \
JAZZ_FAIR_ROWS=600 JAZZ_FAIR_MIXED_WIDTH=256 JAZZ_FAIR_REPEATS=1 \
cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd --bench publication_fairness
```

Durable context and follow-up: [#3569](https://github.com/garden-co/jazz/issues/3569).
No persisted or wire encoding is introduced; all transferred state is private
in-memory evaluator state.

Tooling friction: modeling host wake ownership exposed this duplication; direct
resident-only fixtures hid most of the repeated work.
