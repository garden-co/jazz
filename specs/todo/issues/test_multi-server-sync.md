# Multi-server sync integration tests

## What

Missing integration tests simulating client -> edge -> server communication topology.

## Priority

high

## Notes

- Where: RuntimeCore and SyncManager integration coverage.
- This is a coverage gap rather than a runtime repro.
- Expected coverage: client writes syncing through an edge node to a central server, query forwarding and deduplication across hops, subscription updates flowing back through the chain, and reconnection or recovery at each hop.
- Actual: existing sync tests cover direct client-server topology only.
- Tests should include ASCII diagrams for the multi-hop topology under test.
