# Ordered transport channels

## Overview

Wire v3 separates control, query requests, independent query or authorization
intent deliveries, the initial single authored-write FIFO, and independent
immutable large-value transfers. Each channel is reliable and ordered. There
is no implied order across channels. The ordered WebSocket carrier multiplexes
bounded channel frames; a future QUIC adapter can map the same logical channels
to reliable streams. This is the mandatory current-v3 transport contract,
not an optional fallback to independent per-message compression.

## Details

### Channel identity, ownership and compression

An admitted connection direction owns 64 slots: control 0, requests 1, writes 2,
dynamic deliveries/transfers 3–62, and immutable auxiliary chunk traffic 63.
Generation and contiguous frame sequence are scoped to that connection and
direction. Reusing a completely drained dynamic slot explicitly increments its
generation and starts sequence zero with a new codec. A stale generation,
sequence gap, duplicate compressed extent, or reset over an incomplete message
fails closed.
Incomplete messages retain the 30-second idle and five-minute absolute bounds.
Timeout releases partial storage and terminates the connection: skipping
compressed bytes and continuing is forbidden. This slice cancels subscriptions
semantically while draining ordered bytes; it does not add mid-message reset. Reconnect discards every prior generation and codec.

Each channel owns an independent streaming codec in each direction. LZ4 uses
linked 64 KiB blocks; zstd uses a 64 KiB window. Flushing an extent preserves
history across logical messages. The decoder is bounded and incremental; it
never retains and replays the complete connection history. A decode-only browser
advertises its codec support and emits uncompressed channel bytes when it cannot
encode that negotiated codec. It does not substitute per-message codecs.

The auxiliary endpoint is shared by adapter and lock-independent I/O pump. Once
a pump takes ownership, it is the only physical auxiliary writer. A reservation
retains exact encoded bytes and the original semantic obligation until the last
extent is physically accepted. Dropping a reservation never re-encodes a message
against advanced codec state. No endpoint mutex is held across an awaited chunk
read. Auxiliary frames must decode to ChunkRequestBatch or ChunkResponseBatch;
using their channel to carry canonical state fails before auxiliary dispatch.
Channels, resets and credits confer no authorization and never replace admitted
session metadata or semantic permission checks.

### Routing and dependency table

| Messages                                                                                                                                        | Channel                                                  | Dependency                                                                             |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| SessionClaims, PublishSchema, PublishSchemaWithLens, PublishLens, CatalogueAck, CatalogueSnapshot                                               | Control                                                  | Bilateral global canonical barrier                                                     |
| RegisterShape, Subscribe, Unsubscribe, AuthorizationScopeSubscribe                                                                              | Requests                                                 | One FIFO preserves registration-before-subscription and request cancellation order     |
| FetchRowVersions, PermissionAdviceRequest, AuthorizationScopeIntent, CurrentRowsRequest, CurrentRowsCancel                                      | Requests                                                 | Request FIFO; connection/request identities remain semantic correlation                |
| CommitUnit, AuthorityPublication                                                                                                                | Writes                                                   | One FIFO; authority publication cannot overtake its member commits                     |
| ViewUpdate, SubscribeRejected, AuthorizationScopeReceipt                                                                                        | Delivery keyed by complete SubscriptionKey               | View precedes its scope receipt                                                        |
| AuthorizationScopeView, AuthorizationScopeAggregateReceipt, AuthorizationScopeUnavailable, AuthorizationScopeDecision, PermissionAdviceResponse | Delivery keyed by intent/request id                      | All clause views precede aggregate proof                                               |
| CurrentRowsReceipt                                                                                                                              | Delivery keyed by request id in a separate key namespace | Existing authority/session validation remains mandatory                                |
| RowVersionPayloads                                                                                                                              | Shared repair delivery                                   | No invented per-query ownership for a response lacking request id                      |
| ChunkUploadStart, ChunkUploadNodes, ChunkUploadResult                                                                                           | Transfer keyed by immutable root hash                    | Root-first upload order; referencing writes retain semantic Staged prerequisite        |
| ChunkRequestBatch, ChunkResponseBatch                                                                                                           | Reserved auxiliary                                       | Immutable storage objects only; independent progress while canonical application waits |

FateUpdate conservatively uses the Writes channel with a bilateral canonical barrier. Applying a fate requires the transaction to exist. Direct write acknowledgements refer to an already-authored transaction, but until every cascaded-fate routing path is proven independent of prior deliveries, the transport preserves the existing enqueue order across those deliveries. This is a conservative dependency rule, not a claim that a production sender has been demonstrated to emit an otherwise unsafe sequence. It can be narrowed after that routing proof or with explicit transaction dependencies.

A bilateral barrier drains every earlier canonical logical message before its
first extent and prevents every later canonical message from starting until it
completes. The ordered carrier delivers those complete messages into the existing
single canonical consumer FIFO. A consumer may stage messages before applying
them, but may not reorder that FIFO: deferred catalogue activation stays at its
front. This preserves semantic application order without mislabeling decode as
an application acknowledgement. Immutable auxiliary reads remain independent so
a blocked semantic operation can obtain the chunks needed to complete.

### Resource and scheduling contract

One codec turn receives at most 65,536 decoded bytes and emits at most 72,153
encoded payload bytes. The scheduler retains the semantic allocation once,
shrinking caller Vec spare capacity by storing a boxed slice. It compresses only
the selected extent and retains at most one canonical encoded extent through
physical backpressure. A large upload is not eagerly compressed into all its
future frames.

Queued bulk payloads are bounded by `MAX_LOGICAL_MESSAGE_BYTES` (D). The aggregate
budget adds 8 MiB for bounded interactive traffic and 1 MiB reserved for control.
Each channel is bounded by D; at most 1,024 messages are queued, with eight count
slots reserved for control. Receiver declared-message reservations are bounded
by the same aggregate ceiling before accumulation.

Scheduling weights apply to classes, then round-robin among channels within a
class. Each finite round assigns control eight frames, requests four, delivery
two, writes two, large values one and auxiliary one. A newly admitted request
therefore does not wait behind a full round for every busy bulk channel. A class
without receiver credit is excluded before selecting or advancing a codec.
Actual lower-transport backpressure retains the exact selected encoded extent.

A semantic send accepts ownership once admitted even when its first physical
extent encounters backpressure. A bounded output turn reports Idle, MoreReady,
or Backpressured. Peer ticks schedule another cooperative turn only for
MoreReady. A lone large send continues progressing; a blocked lower queue waits
for its writable/credit wake instead of spinning.

### Receiver-consumption credit

Ordered carrier completion alone does not bound raw canonical frames queued
behind a suspended semantic operation. Mandatory receiver credits bound that
queue while allowing auxiliary traffic to pass independently.

Each frame costs `max(encoded_frame_bytes, 16 KiB)`, including metadata. Initial
windows are control 256 KiB, requests 512 KiB, delivery 1 MiB, shared writes/large
values 4 MiB, and auxiliary 1 MiB: 6.75 MiB total, below the 8 MiB / 512-frame
raw binding guard. The floor bounds tiny-frame count as well as bytes. Query,
control and auxiliary windows cannot be consumed by a bulk upload.

The sender charges exactly once on lower-queue acceptance. The receiver returns
credit only after popping the raw physical frame, not merely reading it into a
paused canonical queue and not after semantic application. Grants bypass the
semantic lock, are uncompressed, and retain exact bytes through backpressure.
Every grant validates admitted session metadata, a contiguous connection-scoped
grant sequence, checked arithmetic and an amount no greater than outstanding
charges. Reconnect creates fresh balances and sequence numbers. Credits are
buffer receipts, not authorization or durability receipts.

### Explicit v3 postcard byte contract

The outer `WireFrame` is encoded with postcard-v1. Channel is appended enum tag
4 and ChannelCredit tag 5; prior tags remain corpus/handshake identities, not an
alternate live message transport. A Channel envelope encodes protocol version
(u16), features (u64), optional WireSession, then the channel extent. The extent's
field order is slot (u16), generation (u64), sequence (u64), class enum, first
(bool), last (bool), semantic message size (u32), decoded extent size (u32), and
length-prefixed payload bytes. The first extent carries a nonzero semantic size;
continuations carry zero. Unsigned integers and enum tags use unsigned LEB128;
booleans are exactly 0 or 1. There are no native-width wire integers.

Class tags are control 0, requests 1, delivery 2, writes 3, large value 4 and
auxiliary 5. Credit fields are protocol version (u16), features (u64), optional
WireSession, class, grant sequence (u64), and consumed byte charges (u64). A
shared bulk grant canonically names the Writes class.

Physical size is checked before postcard decode. Exact decode rejects trailing
bytes, unknown tags and malformed encodings. Endpoint admission additionally
validates declared decoded sizes, slot/class consistency, session metadata,
generation and sequence before codec state or semantic dispatch. Byte contracts
are pinned by the channel and credit fixture tests in `wire::channels::tests`
and `wire::channel_credit::tests`.

## Open Questions

None.
