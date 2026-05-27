# Sync accepts unbounded payload sizes

## What

The sync protocol imposes no byte budget on a frame, a row batch, or a row. A frame batches up to 256 payloads (`MAX_OUTBOUND_SYNC_PAYLOADS_PER_FRAME` / `MAX_WS_SYNC_UPDATES_PER_FRAME`) by **count only**, and a row batch carries a **full-row** snapshot (`StoredRowBatch.data`) whose `text` / array / column-count values are uncapped (only `bytea` *scalars* are bounded, at `BYTEA_MAX_BYTES` = 1 MiB). So a single legitimate frame is effectively unbounded — e.g. 256 rows × a 1 MiB `bytea` each ≈ 256 MiB — and post-handshake `frame_decode` will LZ4-decompress whatever an authenticated peer sends, with no ceiling on the decompressed output.

This means an authenticated peer can drive a large (potentially multi-GB on a constrained host, or a WASM-client trap) allocation via the post-handshake sync path, and there is no principled value at which to cap inbound frames without risking silent rejection of legitimate traffic.

The unauthenticated vector is closed separately: the pre-auth handshake frame is now decompress-capped at 1 MiB (`frame_decode_capped` + `MAX_HANDSHAKE_DECOMPRESSED_BYTES`). This issue is about the **post-handshake, authenticated** path and the absence of any frame/row size discipline.

## Priority

medium

## Notes

- On a 64-bit overcommit host an oversized count/length usually surfaces as a clean decode error rather than an OOM (lazy reservation + a fast read failure). The genuinely memory-*committing* path is LZ4 decompression (it writes the output), so the real exposure is: a constrained / no-overcommit server, or a 32-bit **WASM client** (linear-memory growth traps → the client bricks irrecoverably).
- Failure mode today is poor: an over-cap or malformed frame on the steady-state loops is **silently dropped** (`frame_decode` → `None` → `continue` in `websocket.rs` and `transport_manager.rs`), with no error or ack to the sender. For a legitimate over-cap frame that means silent data loss / a stalled sync, not a recoverable error.
- Because legitimate frames are unbounded, a hardcoded receiver cap is the wrong shape — it is an invented policy that either rejects valid traffic (too low) or barely helps (too high). A fixed post-auth cap was deliberately **not** added for this reason.
- Desired direction (the real fix): **byte-budgeted framing**.
  - Add a byte budget `B` to the sender-side batchers (`transport_manager.rs` outbound, `websocket.rs` outbound — both currently count-only) so a batch is split across multiple frames to fit `B`. A 256 × 2 MiB batch becomes ⌈512 MiB / B⌉ frames automatically.
  - Receiver cap then = `B` + LZ4 framing margin — tight and safe, because frames are bounded *by construction* rather than by guesswork.
  - Splitting is only possible down to **one row** (a batch is atomic full-row data; sub-row chunking would be a deep CRDT/wire-format change). So `B` doubles as an effective max-row size, and a row larger than `B` should fail with a **loud, recoverable** send-side error (e.g. `RowExceedsFrameBudget { size, max }`) — never a panic or a silent drop.
  - Make `B` configurable (e.g. `JAZZ_MAX_FRAME_BYTES`, matching the existing `JAZZ_*` / `NODE_ENV` config pattern) so constrained deployments can tighten it and bulk-binary workloads can raise it.
- Row history does not bloat a single payload: replacing a 1 MiB value N times produces N separate ~1 MiB batches linked by parent IDs, not one N MiB payload — so the parent closure splits cleanly across frames. The oversized-single-row case only arises when one *version* is itself huge.
- Constraint for any solution: a bad/oversized payload must surface as a clean, recoverable error and must never crash a client irrecoverably (no WASM trap, no panic, no silent loss).
