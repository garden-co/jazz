# jazz — Specification · 8. Sync protocol

## Overview

One protocol carries everything between nodes: a browser tab and its worker, a
worker or native client and the server, a relay and its upstream. This chapter
defines it as implemented by the SQLite engine (`crates/jazz/layers/db/src/sync.rs`).

The model is small:

- The **authority** (the server, or a node with no upstream) is a database
  holding complete history. Every accepted commit gets the next server seq.
- A **replica** is a database whose store is a partial copy of server versions
  plus its own optimistic writes. Replica rows carry the server seq they were
  shipped at, so cuts and history on the replica are over server versions.
  Optimistic local writes number from `LOCAL_BASE` up (a non-durable client
  from `NON_DURABLE_BASE` up), above every server seq, so no cut at a server
  seq sees them.
- **Writes travel up** as one `Commit` per transaction and come back as one
  `Fate`.
- **Reads travel up** as one `Read` per query and come back as rows plus
  settlement: `Rows` and `AccessLost` messages, then a `Settled` naming the
  server seq the result is complete through. Results are never sent; the replica
  derives every result with its own compiled queries (`INV-SYNC-44`).
- **Coherence is per subscription.** The server keeps a row current for a
  session only while one of its live reads covers it. A cached row no live read
  covers is a cold copy; it becomes current again once a read covers it. A live
  read resumes from the seq it last settled through, and its opening ships only
  what changed after that.

Invariant digest (the registry, `crates/invariant-registry.jsonl`, is the
source of truth and names each invariant's tests):

- `INV-SYNC-5`: Applying a fate or a settled seq MUST NOT move a node's settled seq backward or lower a write's reached durability tier; a lost fate is recovered on resume rather than guessed.
- `INV-SYNC-7`: Rows MUST travel as self-contained row images (table, id, branch key, seq, deleted flag, content, cells), and result membership is per row: a subscription never holds a transaction as a unit of membership.
- `INV-SYNC-8`: A server MUST track per session which row versions the peer holds and ship each version at most once across all of that peer's subscriptions; a peer it has no record of receives every row its results need.
- `INV-SYNC-10`: A replica MUST apply a message whole: a message larger than one wire frame travels in transport parts whose last part ends it, and nothing of it is visible before the end.
- `INV-SYNC-11`: A reset after reconnect MUST replace the subscription's whole result (members no longer listed leave) while the server keeps skipping versions the peer is known to hold.
- `INV-SYNC-12`: A server MUST ship other peers accepted rows only, never another node's pending or refused writes.
- `INV-SYNC-13`: A server MUST apply the peer identity's read policy before shipping any row image, support row or count to it.
- `INV-SYNC-14`: When a reader loses read access to a row (through a write or a read-policy change), the server MUST evict it; the replica drops its local copy. Local copies are not a redaction boundary, so eviction is about coherence, not secrecy.
- `INV-SYNC-15`: A serializable commit MUST be decided and applied whole at the authority and reach replicas whole.
- `INV-SYNC-16`: Mergeable writes MUST apply per column without waiting for other writes.
- `INV-SYNC-17`: A row image MUST carry its deleted flag, so a replica can tell a deleted row from one it was never sent, including for rows it cached from an ended read.
- `INV-SYNC-20`: A replica's synced result MUST equal the authority's read of the same query for that reader after any interleaving of writes, entering and leaving rows included.
- `INV-SYNC-21`: Wire messages MUST name nodes by UUID, authors by identity and schemas by version id; node-local codes never leave their database.
- `INV-SYNC-24`: A fresh subscription MUST receive its whole result; only a resumed one may be sent just the rows changed after the seq it held.
- `INV-SYNC-25`: A resumed subscription MUST leave the replica with the same rows as a fresh subscription to the same query.
- `INV-SYNC-27`: Per-session delivery state MUST live only in the serving process and be released on disconnect.
- `INV-SYNC-28`: Results MUST travel as rows and be assembled into result trees by the receiver.
- `INV-SYNC-30`: A replica MUST treat a subscription as settled only when its connected upstream has confirmed it with a settled seq; a node with no upstream settles locally.
- `INV-SYNC-44`: The read protocol MUST be rows plus settlement.
- `INV-SYNC-45`: Rows MUST be shipped raw, by the authority's stored table and column names, whatever schema version the subscriber declared; a replica mirrors its upstream's catalogue and applies the lenses locally.
- `INV-SYNC-46`: A replica missing the chunks of a large value MUST request them and install the row only once its chunks are present; a writer pushes a value's new chunks before the commit that uses them.
- `INV-SYNC-48`: A Remote read MUST NOT report rows the server's answer no longer lists.
- `INV-SYNC-49`: A server MUST process a session's messages in the order sent, so a read sent after a commit is served after that commit.
- `INV-SYNC-50`: A non-durable client's local seqs MUST NOT collide with its relay's.
- `INV-TX-2`, `INV-TX-11`, `INV-TX-23`: commit, seq and relay rules (ch. 3).
- `INV-WIRE-1`, `INV-WIRE-2`: exact postcard frames and Hello version negotiation (§8.1.1).

The Groove-era rows `INV-SYNC-9`, `INV-SYNC-23`, `INV-SYNC-26` and the target
rows `INV-SYNC-31` to `INV-SYNC-43` describe the retired view-update protocol
(supporting snapshots and deltas, known-state declarations, closure
manifests). They no longer describe this protocol and are left for the
registry cleanup.

## Encoder trust, bounded decoding, and failure containment

Encoder trust is established by the local authenticated topology, never by a
message claiming to be trusted.

| Direction                                                     | Encoder contract                                      |
| ------------------------------------------------------------- | ----------------------------------------------------- |
| Server → client, including its local relay                    | Trusted encoder; decode directly                      |
| Client → server, including bytes forwarded by its local relay | Untrusted encoder; bounded connection-scoped decoding |

For an untrusted encoder, prechecks are only for resource amplification:
frame sizes, joined-message size and decode allocation must fit the
connection's budget (§8.8). A frame that does not decode ends that link, not
the process: the transport reports the failure once and the server closes the
session (`Transport::take_failure`). Failure stays inside that scope.

Authentication, write authorization, schema constraints and fate rules still
apply. They are semantic admission, not defensive decoding. Authentication of
a local relay does not authorize its client's writes.

## Details

### 8.1 One protocol, roles not code

Every link exchanges the same `SyncMessage` vocabulary. A node's behaviour
comes from its role: a node with an upstream link is a replica (and, if it
serves sessions, a relay); a node without one is the authority for its
sessions. Only the global authority assigns `Global` fates (ch. 3, ch. 9).
Only one relay tier is supported: an authority, optionally a durable relay
(a worker), and its non-durable sessions (tabs) (SPEC 9 §9.5).

Messages, with their postcard variant tags:

| Tag | Message           | Direction       | Purpose                                                          |
| --- | ----------------- | --------------- | ---------------------------------------------------------------- |
| 0   | `Read`            | client → server | A read, live or once (§8.4)                                      |
| 1   | `Unsubscribe`     | client → server | End a live read (§8.5)                                           |
| 2   | `ResendWhole`     | client → server | Rows whose shipped deltas did not apply: ship them whole (§8.12) |
| 3   | `Settled`         | server → client | A read is complete through a seq (§8.4)                          |
| 4   | `Commit`          | client → server | One transaction's writes (§8.2)                                  |
| 5   | `ChunkRequests`   | client → server | Pull large-value nodes (§8.12)                                   |
| 6   | `PushChunks`      | client → server | Large-value nodes ahead of a commit (§8.12)                      |
| 7   | `Chunks`          | server → client | Pulled nodes, and the ones the server cannot answer              |
| 8   | `Rows`            | server → client | Row images, with the tx ids of history versions                  |
| 9   | `AccessLost`      | server → client | Drop a row the reader may no longer read                         |
| 10  | `Refused`         | server → client | A read the server cannot serve, with why                         |
| 11  | `Fate`            | server → client | A commit's outcome (§8.3)                                        |
| 12  | `CheckAllowed`    | client → server | Permission preflight                                             |
| 13  | `Allowed`         | server → client | Its answer                                                       |
| 14  | `Schema`          | client → server | The schema version the session reads and writes by (§8.9)        |
| 15  | `SchemaAdmission` | server → client | Whether that version is admitted                                 |
| 16  | `Stored`          | relay → session | A relay holds the session's commit and uploads it (§8.3)         |
| 17  | `KnownCatalogue`  | client → server | The schema versions and names the replica keeps (§8.10)          |
| 18  | `Catalogue`       | server → client | Admitted versions, stored names and lens cells (§8.10)           |

Tags before `Commit` keep their numbers: a replica stores each pending
upload's header as an encoded `Commit` message (`_jazz_pending`, see §8.3),
so `Commit` stays tag 4 across stores. No server-sent message has tag 0 or 1, which lets a client tell a
sync frame from a `WireFrame::Hello` (tag 0) or `WireFrame::Error` (tag 1) by
its first byte.

#### 8.1.1 Frames, batches and wire versions

A WebSocket binary message is one postcard `Vec<Vec<u8>>` batch of frames.
The first frame each way is a `WireFrame::Hello`; the server may send a
`WireFrame::Error` and close. Every other frame is a sync frame: a postcard
`SyncMessage`, or a part of one (§8.8). Each whole message, unsplit or
joined from its parts, is exactly one complete postcard value: a decoder
rejects trailing bytes (`INV-WIRE-1`).

Every endpoint advertises exactly `min_protocol_version = 6,
max_protocol_version = 6`. A receiver reads a Hello's version range before
its version-scoped body and answers a foreign range with
`UnsupportedProtocolVersion`/`Never` (`INV-WIRE-2`). The `WireFrame::Channel`
and `WireFrame::ChannelCredit` variants keep their tags in the enum but no
SQLite endpoint sends them. Clients, relays and servers upgrade together.

The sync frames are pinned by `crates/jazz/fixtures/sqlite_sync_wire_v1.json`,
which every shipped artifact (native, NAPI, WASM) validates
(`jazz::sync_wire_corpus`). The corpus may be re-pinned until alpha.60 ships;
after that, a layout change needs a compatibility write-up.

In-process links (a worker and its tab, tests) carry `SyncMessage` values
directly (`sync::duplex`); a byte link frames them with postcard
(`WireTransportAdapter`).

#### 8.1.2 WebSocket admission prelude

Before the binary carrier begins, a client sends exactly one UTF-8 JSON object
as its first WebSocket message (text, or binary with the same bytes). The
server then requires one binary message that decodes to exactly one
`WireFrame::Hello`. The prelude and that Hello each have the two-second
handshake read deadline. Only the first message is a prelude.

The prelude object has these server-owned fields:

- `peer_identity`: required canonical `AuthorSubject` JSON string.
- `auth`: required `AuthConfig` object (`jwt_token`, `backend_secret`,
  `admin_secret`, `backend_session`).
- `bootstrap_catalogue`: decode-only legacy boolean; `true` is rejected with
  `UnsupportedFeature/Never`.
- `requested_link`: optional, `ordinary_session` (the default) or
  `scope_isolated_client_relay`, which is admitted only for an authenticated
  session that negotiated `FEATURE_SCOPE_ISOLATED_CLIENT_RELAY`.

Unknown fields are ignored; an unknown `requested_link` is rejected; missing
required fields, malformed JSON or a trailing suffix are rejected before
admission. `crates/jazz/fixtures/websocket_prelude_v1.json` pins the writer
strings and the server's parse. Axum caps inbound WebSocket frames and
messages at 2 MiB, the prelude included.

### 8.2 Upstream: commits

A committed transaction travels as one `Commit { node, tx, kind, ops, … }`;
open transactions never ship (ch. 3, `INV-TX-2`). `ops` are the writes
(`Insert`, `Update`, `Upsert`, `Delete`, `Restore`, `Move`) by the session
schema's table and column names; the authority resolves them to stored names
under the schema version the commit was written under (§8.9). The other
fields:

- `kind`: `Mergeable`, or `Serializable { snapshot, reads, own }`: the
  snapshot seq, each read with the rows and server versions it returned, and
  the writer's own commits that were unsettled when the transaction began. The
  authority re-runs each read under the writer's identity and applies the
  writes only if every result and written row is unchanged since the snapshot.
  A stale or partial replica can only cause a conflict, never a
  non-serializable commit.
- `bases`: for each row written over an image the writer held, the base of
  that image (SPEC 4 §4.6). Only the authority's conflict derivation reads it.
- `expect`: a digest of each written row as the writer's replica holds it
  afterwards. The authority does not echo a row whose new version matches;
  the writer installs its own as that version (§8.3).
- `deltas`, `sources`: chunked cells travelling as edits over the writer's
  newest settled version, and the settled versions that trees' unpushed
  nodes sit under (SPEC 19.8).
- `author`, `session`: provenance or a session a trusted link acts for. Only
  a trusted link may set one other than its own identity.
- `origin`: the session commit a relay uploads this one for.

A replica keeps every unsettled commit and uploads it in order on each
connection. It is stored a row per op, not as one encoded message:

- `_jazz_pending (tx, header)`: the `Commit` with its lists left empty.
- `_jazz_pending_items (tx, part, n, item)`: each op, cell delta, source,
  base and serializable read, by list (`part`) and position (`n`).
- `_jazz_pending_rows (tx, n, tbl, id, key)`: the local rows it wrote.

`expect` is not kept: after a reopen the server ships those rows. A row's
values over the inline limit live in large-value storage, so a stored op is
about the size of the row it writes, however large the commit. A store with
a 2 MiB value limit (a Durable Object) therefore keeps any commit whose rows
it can hold, even one that travels in several frames. An op is bounded by
its row, not by a limit of its own: a row of 33 or more inline cells near
64 KiB each is over 2 MiB, and such a store refuses it as a row too. A refused commit moves
to the retry store, `_jazz_rejected (tx, error, serializable)` with its ops
in `_jazz_rejected_ops (tx, n, op)`, until the host discards it. A build
from before this layout cannot open a store written by one after it, nor the
other way around. That is acceptable only because the SQLite store format
has not shipped (alpha.60).

Uploads are pipelined: no node waits for a fate before sending its next
commit, and a writer's commits reach the authority in the order written
(`INV-HIST-23`). The authority orders commits by arrival.

A relay that receives a session's commit stores it as its own pending commit
and uploads it with `origin` set. It does not decide it (`INV-TX-23`).

#### 8.2.1 Order and read-your-writes (`INV-SYNC-49`)

A link is one ordered stream, and the server processes a session's messages in
the order sent: a read sent after a commit is served after that commit. A
client sends its staged commits ahead of the reads asked since its last send.

When a session's large-value ingress limit holds a commit back, the server
holds every later message of that session that could observe it: a `Read`,
`Unsubscribe` or `ResendWhole` whose tables (its query's and those its
policies read) the held commits write, a read frozen at a snapshot (which may name the held
commits), and anything whose effect is not known. Other messages are served
ahead. Each hop guarantees its own order, so client → relay → authority gives
read-your-writes end to end.

### 8.3 Fates downstream

`Fate { tx, through, result, held, global }` answers a commit:

- `result`: `Ok`, or `Conflict`, `Rejected { reason, message }` (a `Reason`
  by stable name; an unknown name arrives as a rejection without a reason) or
  `Other`.
- `through`: the server seq the session is complete through after the
  commit's rows shipped. Applying a fate never moves the replica's settled seq
  backward (`INV-SYNC-5`).
- `held`: the `expect` rows the server did not ship, each with the
  authority's `$createdAt`/`$updatedAt` stamps, so the writer can install its
  own image as the accepted version. A keystroke-grade edit therefore costs
  one `Fate` and one `Settled` downstream.
- `global`: the fate is the global authority's (directly or relayed). A
  non-global fate is the final word of a replica with no upstream, which
  orders its sessions' commits but cannot make them `Global`.

`Stored { tx }` is durability progress from a relay to a non-durable session:
the relay holds the commit in its store and uploads it, so it is `Local` for a
session whose own store is memory. It is never a decision: a stored commit can
still be rejected, and a `Fate` always follows it, unlike a non-global `Fate`,
which is final.

A fate lost to a disconnect is recovered on the next connection: the replica
re-uploads its pending commits, and the authority answers a commit it already
decided with the known fate.

### 8.4 Downstream: reads and rows

`Read { id, query, binding, include_deleted, tier, wait, span }` asks for one
query's result:

- `span: Live { since }` keeps it current until `Unsubscribe` (§8.5);
  `since` resumes it (§8.11).
- `span: Once { at, view }` answers it once, at the current state or as of
  server seq `at`, in branch view `view`.
- `tier` says how far the read reaches past a relay: `LocalOnly` is served
  from the relay's store, `LocalFirst` from it too while the relay asks its
  upstream, `Remote` once its upstream answered. An authority's store is the
  answer either way.
- `wait` bounds how long a relay waits for its upstream's answer before it
  serves its store (a LocalFirst read's first-load wait, or a one-shot read
  that takes the relay's view when the authority cannot answer).

The server ships the result's rows and the rows that support them (joined and
included rows, the rows its policies read where the replica needs them), then
`Settled { id, through, members, global }`, or `Refused { id, code, message }`
when serving it fails (an unsupported shape, a storage error): the read
ends with that error. `members` is the result's row ids in order. It is
always present for a one-shot read, and for a live read's opening and window
changes when the read is limited or the opening could not tell what left it.
Cached rows `members` leaves out are no longer in that result; the replica
evicts them (a row a pending write touches waits for the server's word on it).
`global` says the answer is the global authority's (directly, or through
relays that settle by their upstream), not a relay's own store.

A replica treats a read as settled only once its connected upstream confirmed
it with `Settled`; while the upstream is unanswered, offline or failing the
read stays unsettled, and a node with no upstream settles locally
(`INV-SYNC-30`). A `Remote` read never reports rows the answer no longer lists
(`INV-SYNC-48`); `LocalFirst` and `LocalOnly` reads keep them.

#### 8.4.1 Raw rows and local derivation

`Rows { images, tx_ids }` carries row images. An `Image` is self-contained
(`INV-SYNC-7`): stored table name, row id, branch key, seq, deleted flag,
whether the version wrote content, and its stored cells by stored column name.
Images travel raw: by the authority's stored names, untranslated, whatever
schema version the session declared (`INV-SYNC-45`). The replica mirrors the
authority's physical layout (§8.10) and applies its lenses locally, so an old
client reads a new server's rows through the same lenses the server would use.

`tx_ids` names the transactions behind history versions among the images, as
(seq, writer node, writer tx): a replica lists a history version only once it
knows its `$transactionId`. It is empty for current rows.

The replica installs images into its own store and derives every result,
include tree and count with its own compiled queries (`INV-SYNC-28`,
`INV-SYNC-44`). No result sets, result keys or completeness facts travel.

#### 8.4.2 Atomic installation

A message is applied whole: nothing of it is visible before it is complete
(`INV-SYNC-10`). A message larger than one frame is split by the transport,
not by the sync layer (§8.8). A serializable commit is decided and applied
whole and reaches replicas whole (`INV-SYNC-15`). Mergeable writes apply per
column without waiting for other writes (`INV-SYNC-16`).

### 8.5 Live reads: watches, unsubscribe and cold rows

The server keeps one fan-out subscription per live read, under the session's
identity, and ships images for rows entering or changing in it. A row leaving
it ships again if the reader may still read it (a deleted row ships as its
deleted version, `INV-SYNC-17`), else it is evicted.

Per session, the server keeps refcounted watches: each live read holds the
rows of its result and their supporting rows, and the session itself
(`SESSION_OWNER`) holds the rows its one-shot reads returned and the rows its
own commits wrote, so those stay current for the rest of the session. A row
version ships at most once per session across all its reads (`INV-SYNC-8`).
This state lives only in the serving process and is released on disconnect
(`INV-SYNC-27`).

`Unsubscribe { id }` releases the read's watches. A row no watch holds stops
being kept current: the replica keeps it as a cold copy, which local reads may
still return, and a later read that covers it brings it up to date (its
opening ships what changed, and its `members` drops what left).

### 8.6 Policy narrowing and eviction

The server applies the session identity's read policy before shipping any
image, supporting row or count (`INV-SYNC-13`), and ships accepted rows only,
never another node's pending or refused writes (`INV-SYNC-12`).

When a reader loses read access to a row it holds, through a write or a policy
change, the server sends `AccessLost { table, id, key }` and the replica drops
its copy (`INV-SYNC-14`). This includes a row the reader's own accepted commit
wrote and cannot read (a write-only insert): the commit's fate reports its
acceptance. Local copies are not a redaction boundary; eviction keeps the
replica coherent, not secret.

`CheckAllowed { id, action, author, session }` asks whether the session would be
allowed an action now, without writing; `Allowed` answers it.

### 8.7 Mergeable and serializable delivery

Delivery is per row. A mergeable commit's rows ship as they change, and a
reader may see some of its rows before others reach a read it has no
subscription covering. A serializable commit is applied whole at the
authority, so every read that covers its rows sees all of them at the same
seq. A subscription never holds a transaction as a unit of membership
(`INV-SYNC-7`).

### 8.8 Transport parts and size limits

`encode_frames` turns a message into one frame, unless its encoding outgrows
one frame (a large first result, one commit's fan-out, a large commit). Then
it travels in as many parts as it takes: each part is a frame whose first byte
is `0xFF` (more parts follow) or `0xFE` (the last part), followed by a slice of
the message's postcard bytes. `FrameJoiner` joins them back into one message.
Postcard variant tags are below `0x80`, so a part marker never begins a whole
message. Any message may be split, and no message knows it was.

A receiver treats these as the peer's error and ends the link: a frame that
does not decode, a whole message between a split message's parts, a last part
with no leading ones, and parts beyond the joined-message ceiling.

A sender hands a split message's parts on in order and never restarts one: a
part a full queue refuses waits ahead of anything sent later
(`WireTransportAdapter`). A disconnect drops a half-received message with the
connection.

Limits:

- An encoded frame is at most `MAX_WIRE_FRAME_BYTES` (2 MiB); a part carries
  at most that less the carrier's prefix room.
- A joined message is at most `MAX_LOGICAL_MESSAGE_BYTES` (256 MiB); a
  sender refuses a larger one ("not supported yet") rather than send it, and
  a server whose message is refused ends the session with a never-retry
  error. A server joins an untrusted session's split messages only up to
  `MAX_UNTRUSTED_MESSAGE_BYTES` (8 MiB, its connection's queue budget); an
  untrusted replica rejects its own larger commit before sending it, as a
  rejected fate, so the session survives.
- A split message spans at most `MAX_WIRE_BATCH_FRAMES` parts, and each frame
  is within the frame limit. A refused message's parts are dropped at once.
- A server ships a session's rows as `Rows` of at most a quarter of the
  joined-message ceiling of images each (live and one-shot answers alike), so
  a result of any size travels. One commit's versions (one seq) stay in one
  `Rows`.
- A WebSocket batch carries at most `MAX_WIRE_BATCH_FRAMES` frames; senders
  split batches by bytes across binary messages.
- `PushChunks` and `Chunks` flush at half a frame of node bytes, so each fits
  one frame. A range pull is answered with at most 4 MiB of node bytes, one
  `ChunkRequests` with at most 16 MiB; a read past them pulls again.
- A session's large-value ingress is rate-limited per window by server
  policy; commits over it are held, with the messages that could observe them
  (§8.2.1).

There is no transport compression on the SQLite path.

### 8.9 Schema versions and write admission

`Schema { version }` is the first message on every connection: the schema
version the session reads and writes by. Its queries and writes use that
version's names, which the server resolves to stored names; images and
evictions travel by stored names either way (§8.4.1). A version the server has
not admitted is named like the server's current schema.

`SchemaAdmission { admitted }` tells the session whether its version is
admitted, whenever that changes. Until it is, the server holds the session's
messages (a draft awaiting its lens), so a read that needs the server fails
with the reason rather than waiting out its budget.

Each pending commit records the schema version it was written under
(`_jazz_pending_versions`). Before uploading pending commits staged under
another version than the session's current one, the replica sends a `Schema`
switch for them, so an offline write made before an upgrade is resolved under
the names it was written with.

The authority evaluates every write against its own policy data. A relay may
keep and retry pending writes but cannot accept them or make them `Global`.

### 8.10 Catalogue

After its first `Schema`, a replica sends `KnownCatalogue { versions }`: the
schema versions whose stored names it keeps, each with `names_digest` of those names.
The server answers with `Catalogue { namespace, versions, cells, confirmed }`,
carrying every admitted version the replica lacks or names otherwise, in
admission order, ahead of any row; and again whenever its store admits a
version or learns more of its authority's catalogue.

- A `CatalogueVersion` is a structural schema (without permissions) plus, per
  table, its name and stored name and, per column, its name and stored name.
- A `CatalogueCell` is a stored column a lens fills: a copy of stored column
  `source`, or a dropped column's backwards default, from version `since` on.
- `namespace` is the authority's, once the sender knows it. Table and column
  identities are UUIDs derived from it and the stored names, so the authority
  mints them and replicas only mirror them. A replica that keeps another
  namespace is another app's store and refuses the upstream.
- `confirmed` lists the versions whose names are the authority's, each with its
  names' digest: every version the authority admitted, or, from a relay, those
  its upstream confirmed. Only a confirmed version's tables and columns have an
  identity; a version only the replica admitted has none until the authority
  admits it too.

The replica admits each version under the authority's stored names, renaming
tables and columns it had named otherwise while it did not know them (for
example, a table created offline under a newer local schema). It then runs the
lens fills itself, since a copy lens's fill does not bump row seqs and
therefore never ships. Lens semantics are chapter 10.

A relay that mirrors a new catalogue owes its own sessions the versions they
now name otherwise, so a tab follows its worker's names. Mirroring is whole or
not at all. A catalogue the replica cannot mirror drops the link, and every remote read
fails with why; local reads are served as before. That is a table it stores
under the name the upstream stores another table as, another app's namespace,
or a stored column it keeps with another type than the upstream's (each added
a column again under the same fresh stored name while apart): values of one
type are never read as another's.

### 8.11 Resuming live reads from `since`

A replica persists, per live read, the server seq its result last settled
through, in `_jazz_watermarks` keyed by a digest of the query, binding,
`include_deleted` flag and reader identity (not the tier or wait, and not the
schema version: the key is over stored names). On reconnect or reopen, it
sends `Read { span: Live { since } }` with that seq.

The authority opens a resumed read one of three ways:

- **Fresh**, when it cannot resume: the store is a relay (its store may be
  cold, and its seqs are not the ones its sessions resume from), the read is
  windowed, a flat join, an aggregate, a relation or a history read, the
  session has its own claims (policies read them, and they are not part of
  what the client resumes from), `since` is past what the store serves (a
  reset store), or `since` is at or before the current schema's activation.
  The opening ships the whole result (`INV-SYNC-24`).
- **Since**, when no membership input moved: no table the read, its supporting
  rows or their policies read has a version after `since`. The opening ships
  only rows with a seq after `since`.
- **Diff**, when a membership input moved: rows may have entered the result
  without changing. The authority evaluates the result as of `since` and ships
  every row not in it, rows that changed after `since`, rows it held then that
  have since left (as they are now, or their eviction), and supporting rows
  whose access changed.

Each ends with a `Settled` (and its `members` where §8.4 calls for one), so a
resumed read leaves the replica with the same rows as a fresh one
(`INV-SYNC-25`, `INV-SYNC-11`). A replica that cannot read its watermark opens
the read fresh.

A watermark claims the replica holds the result as it settled. Two events make
that untrue for results the replica cannot name, so the replica forgets every
watermark when either happens, and each read opens fresh until it settles
again:

- an `AccessLost`: a result settled earlier may have held the row, and a resume
  from it would count the row as held after it became readable again;
- a row asked for whole (§8.12): until its whole image arrives the replica holds
  a stale version, so it also stores no watermark meanwhile.

### 8.12 Large values

Chunked large values (SPEC 19.8) travel by reference. A writer pushes a new
value's nodes in `PushChunks` before the commit that uses them; the server
holds them under a receipt until the commit arrives or the receipt expires. A
replica that lacks nodes of a value it reads sends `ChunkRequests { pulls }`,
each citing the row version that holds the value (by stored names), and the
server answers only for a version the reader may read, with nodes of that
version's tree (`Chunks { nodes, missing }`). A row is installed only once its
chunks are present (`INV-SYNC-46`).

Within a session the server ships later versions' chunked cells as deltas over
the version it last shipped of that row. A replica that cannot apply a delta
(it lacks that base version) sends `ResendWhole { rows }` for those rows, and
the server ships each whole or evicts it. The replica sends the `ResendWhole`
again on every new connection until the whole image arrives, and stores no watermark
meanwhile (§8.11). A live read's rows are never declared this way: its opening
brings them up to date from `since`.

### 8.13 Relays

A relay (a worker) is a replica of its upstream and the authority-facing store
for its sessions (tabs). It serves a session's reads from its store per the
read's tier, asking its upstream with the same `Read` where the tier needs
the authority's answer, and tells its sessions the authority's seq it knows
(`served_through`), so a session's snapshot is one the authority validates as
it is. It numbers its unsettled versions from `LOCAL_BASE` and ships them to
its sessions; a non-durable session numbers its own writes from
`NON_DURABLE_BASE` (`INV-SYNC-50`). A relay stores a session's commit as its
own pending commit, answers `Stored`, and relays the authority's `Fate`
(§8.3).

## Open Questions

- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Relay aggregation of equal reads from several sessions onto one upstream read, and coverage between reads.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Catalogue admission and synchronization beyond the mirrored catalogue (draft schemas, admission order across relays).
