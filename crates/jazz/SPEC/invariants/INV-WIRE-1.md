# INV-WIRE-1

- Status: now
- Coverage: ✓

## Invariant

A wire-protocol v1 `WireFrame`, its `WireEnvelope.payload`, each WebSocket frame batch, and each independently versioned v1 NAPI/WASM binding payload MUST be exactly one complete postcard value: malformed, unsupported, corrupt-compressed, fragmented, or trailing-byte input MUST be rejected at its owning boundary before it can acquire a second interpretation. `WireHello.authority.node` MUST use postcard's length-prefixed 16-byte `NodeUuid` representation and be consumed before the exact-frame EOF check. Frozen frame, Hello, semantic-payload, branch/catalogue/large-value, and binding corpus cases MUST decode independently and re-encode to their exact approved bytes.

## Enforced by (tests)

`jazz::wire::tests::{canonical_wire_decoders_reject_suffixes_and_overlong_varints,dual_compression_negotiation_gives_outbound_lz4_precedence,zstd_stream_decoder_rejects_corrupt_compressed_payload}`; `wire_fixtures::{wire_hello_frame_fixtures_decode_exactly,wire_message_frame_fixtures_decode_to_expected_messages}`; `packages/jazz-tools/src/runtime/native-runtime/{websocket.ts,binding-codec-golden.test.ts}`

## Implementation

`jazz/src/wire.rs::{decode_frame,decode_sync_message,decode_postcard_exact}`; `jazz/src/db/wire_transport.rs::LogicalMessageReassembler`; `jazz/src/binding_codec.rs`; `packages/jazz-tools/src/runtime/native-runtime/{websocket.ts,native-row-codec.ts}`
