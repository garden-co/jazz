# Multi-server sync integration tests

## What

Missing integration tests simulating client -> edge -> server communication topology.

## Where

RuntimeCore / SyncManager integration tests

## Steps to reproduce

N/A — missing test coverage.

## Expected

Tests covering: client writes syncing through an edge node to a central server, query forwarding and deduplication across hops, subscription updates flowing back through the chain, reconnection and state recovery at each hop.

## Actual

Existing sync tests use direct client-server topology only.

## Priority

high

## Notes

Should include ASCII diagrams showing the multi-hop topology being tested.
