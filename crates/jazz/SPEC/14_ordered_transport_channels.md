# Ordered transport channels

## Overview

Wire v3 separates control, query requests, independent query or authorization
intent deliveries, the initial single authored-write FIFO, and independent
immutable large-value transfers. Each channel is reliable and ordered. There
is no implied order across channels. The ordered WebSocket carrier multiplexes
bounded channel frames. Cross-channel dependencies are enforced above the carrier,
so a future QUIC adapter can map stable logical channels to reliable streams
without changing database semantics. This is the mandatory current-v3 transport contract,
not an optional fallback to independent per-message compression.

## Details

### Channel identity, ownership and compression

An admitted connection direction owns 64 slots: control 0, requests 1, writes 2,
dynamic deliveries/transfers 3–61, progress replies 62, and immutable auxiliary chunk traffic 63.
Generation and contiguous frame sequence are scoped to that connection and
direction. Reusing a completely drained dynamic slot explicitly increments its
generation and starts sequence zero with a new codec. The generation advances
only when semantic queue admission succeeds: an ordinary backpressure rejection
must not consume a generation or discard its old codec. A stale generation,
sequence gap, duplicate compressed extent, or reset over an incomplete message
fails closed.
Incomplete messages retain the 30-second idle and five-minute absolute bounds.
Receiving an incomplete extent arms a real host deadline even if the sender
never sends another byte. Canonical ticks re-arm the earliest remaining bound;
hosts coalesce at their earliest deadline and accept earlier replacements.
Independent auxiliary drivers service their own receive deadline while the
semantic node is blocked. Timeout releases partial storage and terminates the
connection: skipping compressed bytes and continuing is forbidden. This slice cancels subscriptions
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

FateUpdate uses the Writes channel with a bilateral canonical barrier. A local-first
query can deliver another author's Pending transaction carrier. After accepting
that view for delivery, the sender registers a fate observer for its recipient;
a later settlement therefore legitimately follows a query delivery even when
that recipient never authored a write. The transaction must be introduced before
its fate applies. Bare supporting-row references do not register such observers,
and Pending transactions are pinned against cache eviction. If a view is parked
because another row body is missing, fates for its included carriers wait for
that view or its non-authoritative carrier caching. No additional body fetch or
permission capability is inferred solely from a fate.

### Two independent layers

The byte-stream backend accepts opaque messages, a stable stream ID and a
priority. It owns framing, per-stream compression, flow control and codec
lifetime. It does not inspect Jazz messages, epochs or barrier dependencies.
One stable stream remains FIFO across codec generations: generation reset is
inline stream lifecycle, not permission to reorder a replacement stream.

The Jazz routing layer encodes a versioned envelope above that backend. Each
canonical message carries an epoch and a per-stream logical ordinal, which
never resets with the codec. A barrier snapshots the last accepted ordinal on
each canonical stream, belongs to the current epoch, and advances the sender
only after enqueue succeeds. Rejected enqueue changes none of these values.

The receiver admits ordinary messages from the current epoch independently.
For example, a fast query result does not wait for an unrelated large result
in that epoch. A barrier waits until every predecessor watermark has entered
the single canonical FIFO. It enters that FIFO next and opens the following
epoch; complete messages received early from that epoch remain bounded staged
inputs. This is not a global reorder of every message.

The canonical consumer preserves that FIFO across catalogue front deferral.
Views parked for body repair retain their reservation, and fates referring to
those views wait in a bounded semantic queue while repair replies continue.
FIFO admission plus these consumer dependency rules preserve ordering;
it is not an acknowledgement of query completion, durability or transaction
settlement. There is no round trip per barrier. Immutable auxiliary reads and
transport credits bypass canonical epochs so an operation waiting for storage
chunks can still finish.

RowVersionPayloads replies use the reserved progress stream 62. They retain
ordinary epoch ordering but have independent byte capacity so a view holding
ordinary capacity can receive the body needed to release it.

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
slots reserved for control. Whole-message reservations are acquired transactionally in semantic enqueue
order, before priority scheduling can select a later stream. They remain charged
until the receiver's last owner of the decoded message releases its lease.
Receiver leases begin at the first extent and survive upper-layer staging,
canonical FIFO dequeue, batching and front deferral. Thus moving a message
between queues cannot evade the bound or starve an earlier predecessor of
already-reserved capacity. Auxiliary buffers have their independent D-byte
window. A view waiting for missing row bodies retains its lease in the repair
queue. A later fate referring to a transaction in that view retains its own
lease and waits for repair/application; unrelated authored transaction fates
continue normally. Repair responses remain receivable and apply before the
deferred fates. This semantic repair queue is distinct from catalogue front
deferral, so FIFO admission alone is not claimed to complete a parked view.
When a newer complete view supersedes a parked view, its already-received
Pending carrier identities are retained as zero-body, view-scoped transaction
fragments before their envelope is discarded. No discarded row body or current
index is installed. Later fates can settle these identities without making old
rows visible; a later ordinary carrier can extend the fragment without
regressing its fate or durability. Only the replacement view controls membership
and authority.
Detaching a connection drops repair and fate queues. Closing a connection retires its credit state; old lease drops cannot
fund a new connection.

Scheduling weights apply to classes, then round-robin among channels within a
class. Each finite canonical round assigns control eight frames, requests four,
delivery two, writes two, large values one and progress replies two. The independent auxiliary owner
uses its bounded output pump; without an external binding owner, the adapter
admits a ready auxiliary extent after at most four canonical extents. A newly admitted request
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
values 4 MiB, auxiliary 1 MiB, and progress 1 MiB: 7.75 MiB total, below the 8 MiB / 512-frame
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

A separate grant scope releases decoded byte-message buffers and their message
counts. Canonical buffers share D plus 8 MiB of interactive capacity and 1 MiB reserved
for control. Bulk buffers, including large catalogue messages, share the D-byte
bulk limit; the control reserve is not a maximum catalogue size. Auxiliary
buffers have a separate D-byte limit. The canonical count limit is
1,024, with eight slots reserved for control; auxiliary has its own 1,024 limit.
Progress replies have a separate D-byte/eight-message reservation and a fixed
stream (slot 62); dynamic delivery slots stop before that slot. Only Jazz's
RowVersionPayloads is routed to this generic byte priority. This extra maximum
message is necessary when a retained maximum-size view needs a maximum-size
repair response. A second such response cannot be retained simultaneously.
The raw scheduler also preserves progress admission beside a full ordinary queue.
Frame grants and buffer grants share the same reliable ordered control stream
and grant sequence. Holding a decoded buffer does not hold physical-frame
credit, and releasing physical-frame credit does not release the decoded lease.

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

Class tags are control 0, requests 1, delivery 2, writes 3, large value 4,
auxiliary 5 and progress 6. Credit fields are protocol version (u16), features (u64), optional
WireSession, class, grant sequence (u64), consumed byte charges (u64), and scope.
Scope tag 0 releases physical frames; tag 1 additionally carries a released
message count (u32) and bulk flag (bool). A shared bulk grant canonically names
the Writes class.

The canonical byte-message envelope is also postcard-v1: envelope version
(u8, currently 1), epoch (u64), logical ordinal (u64), optional sorted predecessor
vector of (slot u16, ordinal u64), and semantic payload bytes. At most 64
predecessors are allowed. A bounded 1,024-byte allowance for this envelope is
included within the raw D-byte message limit. Auxiliary payloads do not carry
canonical dependency metadata.

Physical size is checked before postcard decode. Exact decode rejects trailing
bytes, unknown tags and malformed encodings. Endpoint admission additionally
validates declared decoded sizes, slot/class consistency, session metadata,
generation and sequence before codec state or semantic dispatch. Byte contracts
are pinned by the channel and credit fixture tests in `wire::channels::tests`
and `wire::channel_credit::tests`.

## Open Questions

QUIC/WebTransport is not implemented here. Its adapter must preserve FIFO for a
stable stream across codec generations and carry credit grants on one ordered
control stream. Separate transport streams need no new Jazz dependency rules:
the receiver-enforced epoch contract already handles cross-stream reordering.
