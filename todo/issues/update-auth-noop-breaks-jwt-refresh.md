# update-auth message is a no-op, breaking JWT refresh

## What

The `update-auth` message is now a no-op, so JWT refreshes from `Db.applyAuthUpdate()` never reach the worker's Rust transport. Once the original token expires (or auth context changes), the worker keeps using stale credentials and cannot recover without a full worker restart, which breaks long-lived authenticated sessions.

## Priority

critical

## Notes
