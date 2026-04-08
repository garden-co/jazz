# Duplicated sync transport state machines

## What

Main-thread client and worker each implement similar reconnect/auth/streaming logic, creating divergence risk and duplicated bug-fix cost.

## Priority

medium

## Notes

- Where:
  - `packages/jazz-tools/src/runtime/client.ts`
  - `packages/jazz-tools/src/worker/jazz-worker.ts`
- This is an architectural smell rather than a runtime repro.
- Expected: a single shared sync engine or state machine used by both client and worker.
- Actual: two near-identical implementations are maintained in parallel.
- Direction: consolidate into a shared sync engine or state machine. Listed as an immediate priority in the architecture audit.
