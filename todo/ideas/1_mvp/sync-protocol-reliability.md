# Sync Protocol Reliability

## What

Fix critical reliability gaps in the HTTP/SSE sync path: out-of-order sends, early outbox clearing, poisoned incremental sync, ignored per-message results, asymmetric reconnect.

## Why

A local change can look successful but never reach other devices. Lost messages poison later incremental sync. Reconnect restores receive but not send, hiding client-to-server divergence.

## Who

All users — any app relying on Jazz sync for durability and consistency.

## Rough appetite

big

## Notes

Six gaps identified: (1) outbound messages can arrive out of order (one async task per payload), (2) outbox drained before delivery confirmed, (3) lost message poisons later incremental sync, (4) server returns per-message results but client ignores response body, (5) reconnect repairs receive side better than send side, (6) data and control messages share the same fragile path. There's an ignored regression test: `subscription_reflects_final_state_after_rapid_bulk_updates`.
