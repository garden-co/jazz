# Configurable Client TTL

## What

Expose the client state TTL as a configurable option per app instead of leaving it hardcoded to 5 minutes and only adjustable through test-only plumbing.

## Notes

- The spec describes per-app TTL as a feature, but the current implementation only has `set_client_ttl` plumbing with no real operator-facing configuration path.
- Missing pieces:
  - `ServerBuilder::client_ttl(Duration)` for standalone servers.
  - Per-app TTL in `jazz-cloud-server`, likely on `AppEntry` or `AppConfig` rather than process-level `ServerState`.
  - An admin API to change TTL at runtime per app.
  - Operator documentation for tuning.
- The standalone case is small because the runtime setter and sweep integration already exist in `jazz-tools`.
