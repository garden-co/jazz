# WS transport sends one frame per outbox entry (no batching)

## What

The `TransportManager` run loop drains `outbox_rx` one entry at a time and writes a separate WS frame per entry. The old HTTP path bundled multiple payloads into a single `SyncBatchRequest` POST. Write-heavy clients that emit N row-version payloads in one tick now send N WS frames (N TCP writes, N JSON envelopes) where HTTP sent 1 request.

## Priority

medium

## Notes

- Relevant code: `crates/jazz-tools/src/transport_manager.rs:491-497` (Tokio) and `:691-697` (WASM).
- Server already accepts the batched shape (`routes.rs:1335-1348`), so the client change is straightforward.
- Fix: drain the channel non-greedily inside `run_connected`, accumulate entries for a single tick, and emit one `SyncBatchRequest` frame per drained batch.
