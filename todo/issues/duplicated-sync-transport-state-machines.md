# Duplicated sync transport state machines

## What

Main-thread client and worker each implement similar reconnect/auth/streaming logic, creating divergence risk and duplicated bug-fix cost.

## Where

- `packages/jazz-tools/src/runtime/client.ts`
- `packages/jazz-tools/src/worker/jazz-worker.ts`

## Steps to reproduce

N/A — architectural smell, not a runtime bug.

## Expected

Single shared sync engine/state machine used by both client and worker.

## Actual

Two near-identical implementations maintained in parallel.

## Priority

high

## Notes

Direction: consolidate into a shared sync engine/state machine. Listed as immediate priority in architecture audit.
