# SQLite semantic ingress validation

Each semantic frame is one complete, canonical postcard `SyncMessage`. The
shared decoder checks the existing logical-message size budget, consumes all
input and rejects alternate varint spellings using `decode_postcard_exact`.
NAPI and WASM validate every frame in a batch before queueing any of it. The
server closes the malformed session; independent sessions remain usable.

The existing `Unsubscribe { id: 128 }` field bytes remain `01 80 01`. An appended
`00`, the overlong tag `81 00`, or the overlong ID `80 81 00` is rejected. Normal
encoders already produce canonical bytes. No message fields, tags, framing
header, storage layout or correctly encoded traffic changes in this extraction.
It does not introduce the E2EE stack's versioned codec or additional messages.

Direct WASM negotiated-session admission checks protocol version 6 and the
artifact's compiled feature mask before allocating a link. Invalid values
produce a typed `protocol` error; zero optional features and supported subsets
remain valid. Hello bytes and existing feature bits are unchanged. Endpoint
metadata confers no identity or permission authority.

The tests pin independent valid/malformed field bytes, check synchronous
single/batch admission in the real NAPI and WASM artifacts, and exercise server
session termination through an ordinary read. A valid answer followed by a bad
frame must leave its remote read pending; resending the answer completes it.
A rejected negotiated session can be retried with valid parameters. Tests use
no E2EE schemas or history APIs.
