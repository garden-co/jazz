# Client State Cleanup

## What

Garbage collection of server-side state (sync cursors, query subscriptions, session records) for permanently disconnected clients.

## Why

Clients can disappear permanently (uninstall, cleared browser data), leaving orphaned state on the server that accumulates over time.

## Who

Server operators and indirectly all users (storage/performance impact).

## Rough appetite

medium

## Notes

Open questions include TTL-based vs lazy cleanup, reconnect-after-cleanup handling, and per-app retention policies.
