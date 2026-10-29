# Include-deleted read views

`ReadViewSpec` gains `include_deleted: bool`, so a subscription registration
can ask for deleted root rows. This changes one wire encoding and adds one
durable-key version.

## Wire: `RegisterShapeOptions`

Postcard encodes the options as `tier | read_view.source |
read_view.include_deleted | binding_source`. The flag is one byte (`00` or
`01`) between the source and the binding source, so a default current
registration's options change from `02 00 00` to `02 00 00 00`.
`crates/jazz/fixtures/wire_message_frames_v6.json` (`register_shape_todos`) and
the `wire_v6_registration_options_bytes` test pin the new bytes.

The change is made in place in wire v6, which has not shipped. The protocol
version does not change, so clients, relays and servers upgrade together. A
peer built before the field sends a three-byte options tail. Because the
options are the last field of `RegisterShape`, the new decoder fails at end of
input on `binding_source` instead of misreading a field. `#[serde(default)]`
on the field has no effect under postcard.

## Durable identity: `JRVK` read-view keys

Read-view keys are UUIDv5 over a hand-written `JRVK` preimage and are stored
in settled-result rows. Every read view without the flag keeps the V2
preimage `"JRVK" | 2 | tier | binding_source | source`, so every existing key
is unchanged and no stored row needs migrating. An include-deleted read view
uses the V3 preimage `"JRVK" | 3 | tier | binding_source | source | 0x01`.
Ordinary and include-deleted subscriptions of the same shape therefore never
share coverage or a settled result. The
`include_deleted_registration_encoding_and_identity` test pins both preimages.
SPEC 6 §6.5, SPEC 8 and SPEC 9 describe the encodings.
