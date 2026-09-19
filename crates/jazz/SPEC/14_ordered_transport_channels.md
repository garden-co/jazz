# Ordered transport channels

## Overview

Wire v3's channel carrier separates control, query requests, independent query
or authorization-intent deliveries, the initial single authored-write FIFO,
and independent immutable large-value transfers. Each logical channel is
reliable and ordered. There is no implied order across logical channels.
An ordered WebSocket multiplexes bounded frames today; a future QUIC adapter
may map the same channel contract to reliable streams.

This chapter specifies the channel primitives. Activation requires the adapter
and every server pump to use the same persistent endpoint before auxiliary and
canonical traffic are separated. Merely decoding a frame is not an application
acknowledgement. The existing message adapter remains active until that
integration is complete.

## Details

### Resource and scheduling contract

Each admitted connection direction owns at most 64 channel slots, independent
codec contexts and contiguous frame sequences. Slot zero is reserved for
control. An encoder receives at most 65,536 decoded bytes per turn, then flushes
its persistent stream without ending it. A physical frame carries at most
72,153 encoded bytes (65,536 + floor(65,536/10) + 64). The total queued semantic
allocation is capped at `MAX_LOGICAL_MESSAGE_BYTES` plus one MiB of reserved
control capacity. Ordinary channels cannot consume that reserve. Each channel
is capped at `MAX_LOGICAL_MESSAGE_BYTES`, and the connection at 1,024 queued
messages, including tiny messages. Eight queue slots are also reserved for control. Staging a partial upload must not compress
or allocate every remaining frame eagerly.

A scheduling round gives control eight frame turns, requests four, delivery
and writes two each, and large-value transfers one. Every nonempty eligible
channel receives its finite allocation before the next round. A retained frame
rejected by the lower transport remains exactly the selected frame, even if
higher-priority traffic arrives. Compression state cannot be rolled back or
advanced again on retry. Admission into another channel is allowed while that
frame awaits acceptance, within byte and message-count budgets.

### Semantic dependencies

RegisterShape and Subscribe share the request FIFO. Authored writes initially
share one FIFO. A delivery's ViewUpdate and matching AuthorizationScopeReceipt
share a channel. All AuthorizationScopeView clauses and their AggregateReceipt
share the intent's channel. A channel never supplies authority: authenticated
connection/session admission and semantic authorization remain mandatory.

CatalogueSnapshot, catalogue publication and SessionClaims transitions require
conservative barriers. A carrier barrier drains every earlier logical message
before it begins and excludes every later message until it is carried. The
semantic dispatch host must additionally apply the barrier before dispatching
later dependent traffic. In particular, a new claims message must not overtake
previously queued writes. Immutable auxiliary chunks may progress outside the
semantic application barrier, using the same channel-aware framing endpoint;
they cannot mutate claims or confer row visibility.

Reset retires one channel generation and its codec. A receiver rejects bytes
from retired generations or a gap/duplicate in the contiguous sequence; it must
never skip compressed bytes and continue decoding. Slot reuse requires an
explicit generation transition. Reconnect creates fresh connection-scoped
channel state. Semantic cancellation still uses its subscription/request
identity and cannot cancel a different generation or connection.

### Explicit v3 byte contract

Channel metadata uses postcard-v1 with declaration-order fields: channel slot
(u16), generation (u64), frame sequence (u64), class (enum), first (bool), last
(bool), first-extent semantic message size (u32), exact decoded extent size
(u32), payload (length-prefixed byte sequence). Unsigned integers and enum tags
use postcard's unsigned LEB128, booleans are exactly 0 or 1. The class tags are
control 0, requests 1, delivery 2, writes 3, and large value 4. No native-width
integers occur in the encoding. The outer wire-v3 frame supplies framing/version
admission; this is not an independent magic-header protocol.

A complete encoded extent is capped before postcard decode. Unknown tags,
truncation, trailing bytes, out-of-range channel identifiers and decoded sizes
are rejected. A decoded extent is nonempty and at most 65,536 bytes. First-extent
message size is nonzero and at most `MAX_LOGICAL_MESSAGE_BYTES`. Admission
separately reserves aggregate incomplete storage before accumulating decoded
extents. Negotiated compression and authenticated connection context do not
change when a frame names a channel.

The byte corpus is pinned by
`wire::channels::tests::channel_frame_has_explicit_byte_contract_and_rejects_declared_size_bombs`.

## Open Questions

None.
