# WS transport sends one frame per outbox entry (no batching)

## What

The `TransportManager` run loop drains `outbox_rx` one entry at a time and writes a separate WS frame per entry. The old HTTP path bundled multiple payloads into a single `SyncBatchRequest` POST. Write-heavy clients that emit N row-version payloads in one tick now send N WS frames (N TCP writes, N JSON envelopes) where HTTP sent 1 request.

## Priority

medium

## Notes

- Relevant code: `crates/jazz-tools/src/transport_manager.rs` run_connected (Tokio) and wasm_run_connected (WASM).
- Server already accepts the batched shape (`server/mod.rs` `process_ws_client_frame` falls back from `OutboxEntry` to `SyncBatchRequest`).
- Blocker discovered in a first attempt: naïve batching breaks `policies_integration::session_cases::single_client_operations_reach_server_in_causal_order`. The server's permission check snapshots `old_content` from storage at payload-park time (`sync_manager/inbox.rs:388-400`), before any payload in the batch is applied. With per-frame delivery, the tokio scheduler interleaves ticks between frames so each payload's USING policy sees prior applications; with batching, every payload in a frame checks against the same pre-batch snapshot → stale USING decisions → causal-order violation.
- Prerequisite: move `old_content` resolution from park-time to apply-time and serialize permission checks per (table, branch, row_id) so the apply ordering within a batch is honoured. After that, the transport-level batching can land safely.
