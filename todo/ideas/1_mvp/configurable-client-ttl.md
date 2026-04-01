# Configurable Client TTL

## What

Expose the client state TTL (how long a disconnected client's server-side state survives before reaping) as a configurable option per app. Currently hardcoded to 5 minutes in `ServerBuilder` with a `set_client_ttl` runtime setter that nothing calls outside of tests.

Missing pieces:

- `ServerBuilder::client_ttl(Duration)` method to set it at build time for standalone servers
- Per-app TTL in the cloud server — the cloud server is single-process multi-tenant (`jazz-cloud-server`), so the TTL needs to live on `AppEntry` or `AppConfig`, not on the process-level `ServerState`. The current `client_ttl` field on `jazz-tools::ServerState` only works for the standalone case.
- Admin API endpoint to change it at runtime per-app
- Documentation for operators on how to tune it

## Why

The client-state-lifecycle spec describes per-app TTL as a feature, but the implementation only has plumbing (`set_client_ttl` on `ServerState`) with no way to actually configure it outside of test code. The spec assumed each app has its own server instance, but the cloud server multiplexes many apps in one process.

## Rough appetite

medium

## Notes

The runtime setter and sweep integration already exist in `jazz-tools` — the standalone case is small. The cloud server case requires deciding where the TTL lives (`AppConfig`?) and wiring it through the worker dispatch layer.
