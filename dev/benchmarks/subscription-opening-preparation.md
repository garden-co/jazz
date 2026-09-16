# Subscription opening: compile the program that executes

Thesis and outcomes: [#2913](https://github.com/garden-co/jazz/issues/2913#issuecomment-5702313366).

A local `Db::subscribe` opening used to:

1. compile a maintained program solely as a support check and discard it;
2. normalize the binding, compile/prepare a separate AppRows program, and retain it;
3. compile and install the actual maintained program with the receiver's settled
   inputs, pending overlay, read view, and authorization mode.

The first two programs are not the live evaluator. The actual opener already
validates its complete request and installs an owned Groove subscription. The
trial normalizes the binding and goes directly to that opener. Catalogue/runtime
reopening follows the same path. No shared cross-identity program cache is added.

## Correctness boundary

The actual compiler still validates query capabilities and serving policies;
its strict claim binding, receiver-input cleanup, cold-storage wake route,
pending overlay and publication gates are unchanged. Upstream coverage and peer
RegisterShape/Subscribe preflights remain: those have independent admission and
failure-reporting responsibilities. They are not assumed equivalent to the
local receiver's program. One-shot query planning is unchanged.

The legacy plan-producing internal helper is test-only; existing internal
tests can still exercise that path without altering their assertions. Ordinary
public subscription tests exercise the new path. Dedicated checks include
claim-scoped subscriptions, invalid policy rejection, independent bound stream
lifetimes, and catalogue-triggered rehydration/teardown.

## Evidence required

Compare identical-source-workload native W1 first/resubscription and route
opening receipts, with todo and permissioned first-sync controls. CodSpeed
confirms endpoints and full graphs; an optimized WASM checkpoint is required
before extrapolating to browser startup. A dev trace's overlapping samples are
not an end-to-end latency estimate. Record source and executable provenance.

The changed premise relative to rejected #2853/#2861 is removal of whole unused
Jazz preparation passes at small-result opening, not generic Groove compiler
memoization. Storage and wire encodings are untouched.
