# /ws handler clones every inbound frame unnecessarily

## What

`crates/jazz-tools/src/routes.rs:1345` does `let inner = inner.to_vec();` before handing the payload to `process_ws_client_frame(&inner)`. The borrow from `frame_decode(&data)` would survive the `.await` because `data` owns the bytes, so the clone is avoidable — pass `&[u8]` through directly.

## Priority

low

## Notes

- One extra allocation + memcpy per inbound frame per connection per second at whatever sync rate the client drives.
- Trivial fix; gets amplified at server fan-out scale.
