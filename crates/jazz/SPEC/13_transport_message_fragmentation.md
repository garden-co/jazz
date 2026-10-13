# Logical messages and physical transport frames

## Decision

Jazz sync operations are logical messages. The link transports those messages
as bounded physical channel extents. A query result, catalogue publication,
commit, or repair response need not fit in one frame; semantic layers must not
split otherwise-atomic operations merely to satisfy a link frame budget.

The sender encodes a semantic message, adds its routing envelope, and queues it
on an ordered channel. Compression state persists across messages within that
channel generation. Each codec flush consumes at most `CHANNEL_CHUNK_BYTES`
decoded bytes. The resulting extent carries authenticated connection metadata,
channel/generation/sequence, first/last markers, the first extent's total decoded
message length, and the exact decoded length of this extent. Chapter 14 defines
these fields, channel scheduling, and receiver credits.

Two independent payload budgets apply: `D = MAX_LOGICAL_MESSAGE_BYTES` caps the
complete decoded byte message, including its routing envelope, while
`E = MAX_ENCODED_MESSAGE_BYTES` caps accumulated compressed bytes for that
message. The routed semantic payload limit reserves `MAX_ENVELOPE_OVERHEAD`
within `D`. Neither limit makes the physical frame budget a semantic-message
ceiling.

The receiver validates authenticated metadata and extent bounds before codec
admission. Channel sequence numbers must be contiguous; duplicate or reordered
extents fail rather than being retained for later assembly. The active codec
cannot change within a channel generation. Each extent must produce exactly
its declared decoded length, and the complete message must match its declared
total length. Corrupt compressed input and excess decoded output fail before
semantic decoding. No partial semantic message reaches `Db`.

Physical-frame credits and decoded-buffer leases bound different resources.
Consuming an extent returns physical-frame capacity; retaining its decoded
message holds a buffer lease until the consumer releases it. Channel staging
and queued-message limits provide aggregate bounds. There is no separate old
fragment-id table, encoded-fragment staging pool, or completion-digest cache.

Reconnect discards channel and codec state. An incomplete message fails its
connection after 30 seconds without progress or five minutes total lifetime.
The retained constants are `MAX_FRAGMENT_REASSEMBLY_IDLE_MS` and
`MAX_FRAGMENT_REASSEMBLY_AGE_MS`; despite their historical names, they govern
channel-message reassembly. Recovery uses reconnect and normal known-state
replay, not late fragments restarting an expired assembly.

### Oversized view updates

`D` is a transport budget, but a `ViewUpdate` is semantically atomic and its
size grows with the subscription (a whole-table snapshot of ~110k small rows
already exceeds 256 MiB). The wire adapter therefore sends a `ViewUpdate`
whose semantic payload exceeds the routed payload limit (`D` minus the routed
envelope allowance) as a sequence of bounded parts rather than rejecting it:

- The sender splits the update at supporting-row and transaction-bundle
  granularity. Every part repeats the header (subscription, `settled_through`,
  transition kind and revisions). All parts but the last are
  `SyncMessage::ViewUpdatePart` (tag 28); the last is an ordinary `ViewUpdate`
  and alone carries the real `peer_payload_inventory`. Concatenating the parts'
  lists and carriers in order reproduces the update.
- A single supporting row or transaction bundle that does not fit one part,
  or a header that alone exceeds the limit, is unsupported: the send fails with
  an explicit error. Nothing is sent, and there is no whole-table fallback and
  no per-version body fetch.
- The adapter owns the accepted update's remaining parts and admits them to
  the subscription's delivery stream as channel credit allows. Until the last
  part is admitted it refuses every later canonical offer with backpressure,
  so it retains at most the one logical update it accepted, and the parts are
  contiguous on their stream.
- The receiving adapter buffers parts by subscription, releases each part's
  receive credit immediately, and yields only the reassembled `ViewUpdate`
  when the final part arrives. Every part must match the final update's
  header, and a supporting row repeated across parts is rejected; either
  violation terminates the link. Open part sequences are bounded by the
  number of delivery streams. No layer above the adapter sees a part.
- A subscription unsubscribed or replaced mid-sequence does not cut the
  sequence short: its remaining parts are already on the stream, so the update
  is reassembled and then handled as any late update for a view the link no
  longer serves (including its pending-transaction bookkeeping). A reconnect
  drops the adapter and every buffered part with it; the new link's subscribe
  or resume produces a fresh, complete update.

Wire v6, like v5, carries messages only through channels. The old whole-message and
fragment frames, per-message compression wrappers, and digest-based fragment
reassembler are removed. Removing unused wrappers and limits does not change
channel bytes, codec selection, wire version, or storage encodings.

## Limit inventory

Limits retained for semantic or adversarial-resource reasons:

- `MAX_LOGICAL_MESSAGE_BYTES`: complete decoded byte-message bound (`D`);
- `MAX_ENCODED_MESSAGE_BYTES`: accumulated encoded-message bound (`E`);
- `CHANNEL_CHUNK_BYTES` and `MAX_CHANNEL_FRAME_PAYLOAD`: per-flush decoded and
  encoded extent bounds;
- `MAX_CHANNEL_BUFFER_BYTES`, channel credits, buffer leases, and queued-message
  limits: aggregate retained-state and peer-fairness bounds;
- `MAX_WIRE_FRAME_BYTES` and `MAX_WIRE_BATCH_FRAMES`: allocation bounds before
  physical-frame and WebSocket-batch decoding;
- `MAX_SHAPE_REGISTRATION_BYTES`: retained query/shape AST and read-view option
  admission budget;
- commit-version, repair-ref, known-state-ref, and structured-result depth/width
  limits: CPU/fan-out/state bounds independent of framing.

Array subqueries and result parents are subject to semantic support and resource
limits; transport fragmentation does not authorize an otherwise unsupported or
unbounded query shape.
