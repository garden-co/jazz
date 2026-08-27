# Example topology harness

`packages/jazz-tools/tests/topology/harness.ts` owns deterministic seeds,
bounded named phases, disconnect/reconnect/restart/failure callbacks, receipts,
and replay commands. App scenarios continue to own schemas, fixtures,
operations, and assertions.

## Envelope faults

The same module also provides `TopologyEnvelopeScheduler`, a test-only virtual
time boundary around an app test transport's _delivery callback_. It does not
instrument, patch, or otherwise change Jazz's runtime transport. Give messages
stable endpoint names and non-sensitive test labels, then wrap delivery:

```ts
const envelopes = new TopologyEnvelopeScheduler(seed);
await envelopes.intercept(
  { from: "browser", to: "edge", label: "write" },
  encodedMessage,
  (message) => testTransport.deliver(message),
);
```

Arm faults before the next intercepted envelope with
`duplicateNext`, `delayNext(ticks)`, `reorderNext`, or
`dropNextThenRetry(ticks)`. `partition(a, b)` holds traffic in both directions;
`heal(a, b)` releases ready traffic. `advance(ticks)` moves only virtual time,
so tests never need wall-clock sleeps to reproduce a schedule. Delivery order,
attempt number, virtual tick, endpoint names, labels, and every fault action
are retained in the payload-free scheduler receipt. Pass each scheduler through
`envelopeSchedulers` on `runTopologyScenario`; the runner closes it after app
cleanup and records discarded held envelopes, preventing a test fault from
leaking into another scenario. Delivery callbacks receive an `AbortSignal` and
must stop work when it aborts: teardown awaits all in-flight callbacks up to
the scenario's bounded fault timeout. If a callback ignores cancellation, that
timeout is recorded as a cleanup failure, but teardown still drains the callback
before returning so it cannot quietly mutate a later scenario.
Callbacks that need to enqueue a follow-up use their scoped
`deliveryContext.intercept(...)`; ordinary concurrent callers continue to use
the scheduler directly and their promise does not settle before their ready
delivery has run. A synchronous callback loses that capability immediately on
return, before any microtask it queued can run; an async callback retains it
until its returned promise settles.

Envelope descriptors are deliberately narrow, immutable snapshots of
`{ from, to, label? }`: endpoint names and labels are bounded, unknown metadata
and NUL-containing endpoint names are rejected, and payloads are never recorded.
Only one fault kind can be armed for an envelope (multiple duplicate copies are
the sole composable case). The scheduler also bounds copies, virtual ticks, and
outstanding work, partition links, and receipt activities to keep a bad randomized soak schedule finite. A logical
envelope id remains stable across copies/retry while each actual delivery has a
separate sequence and attempt; retry is recorded when the retry is delivered,
not merely scheduled. A callback failure fail-stops the scheduler and records
discarded remaining deliveries deterministically. Delivery failures are recorded
only as a bounded `error`/`non-error` classification; exception messages and
payload values never enter scheduler receipts.

The scheduler deliberately models message delivery only. Connection lifecycle,
real transport retry policy, and application assertions remain app-owned. An
app should use an explicit test transport seam (or its existing simulation
link), rather than adding app-specific production hooks just for topology tests.

Register a scenario in `dev/example-topology-scenarios.json` with a stable id,
topology labels, working directory, and an argv array. Run the bounded smoke
locally with:

```sh
node dev/gates/run-example-topology-soak.mjs --seed-count 1
```

Copy a failing case's `replay` field from
`target/example-topology-soak/summary.json`. The runner never evaluates shell
strings; registry commands remain explicit argv arrays.

The registry is trusted repository code, not user input. Its path, every
scenario working directory, and the output directory are confined to the
repository after resolving symlinks and before execution. The outer watchdog
enumerates and terminates the scenario's full descendant process tree, including
children that create a new session. In-process phases, faults, and cleanup
receive an `AbortSignal`; they must
stop scheduled work when it aborts. Scenarios that acquire servers, clients, or
other resources provide `cleanup`, which runs in `finally` and is independently
bounded and recorded. Receipts retain attempted, completed, and failed
activities, durations, and errors.

This is currently a tested scaffold and local soak entrypoint. It does not
claim continuous app coverage until app-owned scenarios are registered and the
browser-capable CI job invokes it.

## Open app-branch integration

The product apps are not on `main` yet. Their integration remains deliberately
small and contains no app logic in this harness:

- BandChat (`29d384798`): import `runTopologyScenario` and
  `browserTopologyReporter` in `topology.e2e.test.ts`, express its existing
  disconnect/reconnect/restart calls as fault targets, then register its focused
  Vitest argv.
- BigLabel (`3d6787b3b`): replace its local `browserTopologyPhase` helper with a
  phase in the shared runner and register the focused browser test.
- MusicAgent (`997894311`): use the shared browser reporter for its bounded
  import/server/client phases and register its focused browser topology test.

Those patches should land with their app branches so the public repository does
not copy or prematurely own adopter schemas and workload semantics.
