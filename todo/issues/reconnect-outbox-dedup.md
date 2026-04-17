# Reconnect outbox dedup is not implemented

## What

After a reconnect, the client replays any outbox entries that were in flight when the connection dropped. The server has no mechanism to detect and drop duplicates — if a payload was received and acked but the ack didn't reach the client before the disconnect, the server re-applies it on reconnect. CRDT semantics usually make this idempotent, but certain operation types (e.g. counters if added) would double-apply.

## Priority

medium

## Notes

- Options:
  - Client-side: tag every outbox entry with a monotonic client sequence; server tracks last-accepted-seq per client and ignores duplicates.
  - Server-side: deduplicate by payload content hash within a short window.
- Deferred follow-up from the transport-rewrite PR.
- Relevant files: `crates/jazz-tools/src/transport_manager.rs` (client outbox drain), `crates/jazz-tools/src/server/mod.rs` (ConnectionEventHub dispatch / ingest).
