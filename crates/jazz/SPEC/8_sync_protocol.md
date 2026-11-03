# jazz — Specification · 8. Sync protocol

## Overview

One protocol carries everything between nodes: a browser tab and its worker, a
worker or native client and the server, a relay and its upstream. This chapter
defines it as implemented by the SQLite engine (`crates/jazz/layers/db/src/sync.rs`).

The model is small:

- The **authority** (the server, or a node with no upstream) is a database
  holding complete history. Every accepted commit gets the next server seq.
- A **replica** is a database whose store is a partial copy of server versions
  plus its own optimistic writes. Every stored version sits at one scalar
  position `_clock` (below 2^53), of one of two kinds: accepted (`_clock` is the
  authority position the version was shipped at, below 2^52) or pending
  (`_clock` is `1<<52 | ` its writer's commit HLC). The commit HLC is a
  hybrid clock: milliseconds since `HLC_EPOCH_MS` (2025-01-01) above
  `HLC_COUNTER_BITS` bits of counter, each commit's past its writer's last
  and every one it has seen, carried as `Commit.clock`. The
  same `_clock`, wherever the version is stored. Unsettled versions of a row
  order last-writer-wins by `_clock`, then the writer's node and its tx: two
  writers' versions at one `_clock` are told apart by their writer (`_writer` in
  history, §4). Every store orders the same unsettled commits the same
  way, and the authority's order replaces it once they settle, so a row
  may change then. A write's explicit time (`updated_at_ms`) is provenance
  only and does not order it. Cuts and history on the replica are over
  server versions: `_clock < 1<<52` is every accepted version, so no cut at a
  server seq sees a pending version.
- **Writes travel up** as one `Commit` per transaction and come back as one
  `Fate`.
- **Reads travel up** as one `Read` per query and come back as rows plus
  settlement: `Rows` and `AccessLost` messages, then a `Settled` naming the
  server seq the result is complete through. Results are never sent; the replica
  derives every result with its own compiled queries (`INV-SYNC-44`).
- **Coherence is per subscription.** The server keeps a row current for a
  session only while one of its live reads covers it. A cached row no live read
  covers is a cold copy; it becomes current again once a read covers it. A live
  read resumes from the position it last settled through, and its opening ships only
  what changed after that.

Invariant digest (the registry, `crates/invariant-registry.jsonl`, is the
source of truth and names each invariant's tests):

- `INV-SYNC-5`: Applying a fate, a read's `Settled` or a `ConnectionSettled` MUST NOT move backward where a commit settled, a read's settle or the connection position (`known`), nor lower a write's reached durability tier; a lost fate is recovered on resume rather than guessed.
- `INV-SYNC-7`: Rows MUST travel as self-contained row images (table, id, branch key, seq, deleted flag, content, cells), and result membership is per row: a subscription never holds a transaction as a unit of membership.
- `INV-SYNC-8`: A server MUST track per session which row versions the peer holds and ship each version at most once across all of that peer's subscriptions; a peer it has no record of receives every row its results need.
- `INV-SYNC-10`: A replica MUST apply a message whole: a message larger than one wire frame travels in transport parts whose last part ends it, and nothing of it is visible before the end.
- `INV-SYNC-11`: A reset after reconnect MUST replace the subscription's whole result (members no longer listed leave) while the server keeps skipping versions the peer is known to hold.
- `INV-SYNC-12`: A server MUST ship other peers accepted rows only, never another node's pending or refused writes.
- `INV-SYNC-13`: A server MUST apply the peer identity's read policy before shipping any row image, support row or count to it.
- `INV-SYNC-14`: When a reader loses read access to a row covered by an active subscription (through a write or a read-policy change), the server MUST evict it; the replica drops its local copy. Cold cached rows are reconciled on the next covering remote read. Local copies are not a redaction boundary, so eviction is about coherence, not secrecy.
- `INV-SYNC-15`: A serializable commit MUST be decided and applied whole at the authority and reach replicas whole.
- `INV-SYNC-16`: Mergeable writes MUST apply per column without waiting for other writes.
- `INV-SYNC-17`: A row image MUST carry its deleted flag, so a replica can tell a deleted row from one it was never sent, including for rows it cached from an ended read.
- `INV-SYNC-20`: A replica's synced result MUST equal the authority's read of the same query for that reader after any interleaving of writes, entering and leaving rows included.
- `INV-SYNC-21`: Wire messages MUST name nodes by UUID, authors by identity and schemas by version id; node-local codes never leave their database.
- `INV-SYNC-24`: A fresh subscription MUST receive its whole result; only a resumed one may be sent just the rows changed after the seq it held.
- `INV-SYNC-25`: A resumed subscription MUST leave the replica with the same rows as a fresh subscription to the same query.
- `INV-SYNC-27`: Per-session delivery state MUST live only in the serving process and be released on disconnect.
- `INV-SYNC-28`: Results MUST travel as rows and be assembled into result trees by the receiver.
- `INV-SYNC-30`: A replica MUST treat a subscription as settled only when its connected upstream has confirmed that subscription with its own `Settled`; a node with no upstream settles locally.
- `INV-SYNC-44`: The read protocol MUST be rows plus settlement.
- `INV-SYNC-45`: Rows MUST be shipped raw, by the authority's stored table and column names, whatever schema version the subscriber declared; a replica mirrors its upstream's catalogue and applies the lenses locally.
- `INV-SYNC-46`: A replica missing the chunks of a large value MUST request them and install the row only once its chunks are present; a writer pushes a value's new chunks before the commit that uses them.
- `INV-SYNC-48`: A Remote read MUST NOT report rows the server's answer no longer lists.
- `INV-SYNC-49`: A server MUST process a session's messages in the order sent, so a read sent after a commit is served after that commit.
- `INV-SYNC-50`: A non-durable client's local versions MUST NOT collide with its relay's: each is stored at the position of the node that numbered it.
- `INV-SYNC-51`: A store that committed writes before it became a replica MUST keep them and refuse every upstream, saying why.
- `INV-SYNC-52`: A replica MUST upload each commit's ops by stored names, as the schema view that wrote them stores them, keeping it pending while offline; a server MUST refuse a commit naming a stored table or column of a version it has not deployed, at once, with reason `schema_version_not_deployed`.
- `INV-SYNC-53`: The authority MUST NOT name a writer's own row as written (instead of shipping it) unless the commit applied unmerged over the very version the writer built on and that version is still current (§8.3); every other row ships, and a writer whose image differs from the digest asks for the row whole.
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

A store becomes a replica before it writes anything it will upload
(`become_replica`, which connecting or a remote link hint does): its writes
then take local seqs and are recorded as pending commits, uploaded on the
first connection. A store that committed writes before it became a replica
has them at seqs its upstream numbers its own versions by, never recorded for
upload; syncing would let the upstream's rows replace them. Uploading them is
not supported yet, so such a store keeps them and refuses every upstream,
failing its remote reads (live and one-shot `Remote` ones) with why, across
reopens (`INV-SYNC-51`).

Positions travel as scalars: a `through`, a `since`, a serializable
snapshot, a one-shot read's `at` and an accepted fate's `at` are each one
authority seq (`since` and `at` optional), as is `ConnectionSettled`'s
`through`. Only the authority numbers accepted positions; a relay with no
server never does (§8.13). A row image's `at` is its `_clock`, which the
receiver stores as is: accepted, or pending at its writer's commit HLC,
when the image names that writer (`Image.written`). A receiver refuses as
malformed a pending image with no writer, a `_clock` of neither kind, a
`Commit.clock` of 2^52 or more, and an authority seq of 2^52 or more where
it names an accepted version: a `since`, a serializable snapshot, a
delta's or an access proof's version, and a `BuiltOn::Seq`.

Messages, with their postcard variant tags:

| Tag | Message              | Direction       | Purpose                                                          |
| --- | -------------------- | --------------- | ---------------------------------------------------------------- |
| 0   | `Read`               | client → server | A read, live or once (§8.4)                                      |
| 1   | `Unsubscribe`        | client → server | End a live read (§8.5)                                           |
| 2   | `ResendWhole`        | client → server | Rows whose shipped deltas did not apply: ship them whole (§8.12) |
| 3   | `Settled`            | server → client | A read is complete through positions (§8.4)                      |
| 4   | `Commit`             | client → server | One transaction's writes (§8.2)                                  |
| 5   | `ChunkRequests`      | client → server | Pull large-value nodes (§8.12)                                   |
| 6   | `PushChunks`         | client → server | Large-value nodes ahead of a commit (§8.12)                      |
| 7   | `Chunks`             | server → client | Pulled nodes, and the ones the server cannot answer              |
| 8   | `Rows`               | server → client | Row images, with the tx ids of history versions                  |
| 9   | `AccessLost`         | server → client | Drop a row the reader may no longer read                         |
| 10  | `Refused`            | server → client | A read the server cannot serve, with why                         |
| 11  | `Fate`               | server → client | A commit's outcome (§8.3)                                        |
| 12  | `CheckAllowed`       | client → server | Permission preflight                                             |
| 13  | `CheckAllowedResult` | server → client | Its answer: `Allowed`, `Denied` or `Unknown`                     |
| 14  | `SchemaAdmission`    | server → client | That version's admission changed (§8.9)                          |
| 15  | `Stored`             | relay → session | A relay holds the session's commit and uploads it (§8.3)         |
| 16  | `Join`               | client → server | Open: the session's version and the catalogue it keeps (§8.9)    |
| 17  | `Catalogue`          | server → client | Admitted versions, stored names and lens cells (§8.10)           |
| 18  | `Welcome`            | server → client | The answer to `Join`: admission and catalogue (§8.9, §8.10)      |
| 19  | `ConnectionSettled`  | server → client | The position the whole connection is settled through (§8.4)      |

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

A committed transaction travels as one `Commit { id, kind, ops, … }`, `id`
its writer's transaction id `tx@node`, which it keeps at every hop;
open transactions never ship (ch. 3, `INV-TX-2`). Each op is a write
(`Insert`, `Update`, `Upsert`, `Delete`, `Restore`, `Move`) by stored table
and column names (branch selectors included), as the schema view that wrote it
stores them, so a commit is the same whichever version wrote it (§8.9). An
`Insert` names every column of the writer's version, its default or null where
the app gave none, so a receiver fills only what that version lacks. Each op
carries what its writer knew of the row it writes:

- `bases`: on the commit's first op that writes a row, for each branch key it
  writes the row under over an image the writer held, the base of that image
  (SPEC 4 §4.6). A move writes two. Only the authority's conflict derivation
  reads them. A base on any other op, or two naming one row and branch key,
  refuse the commit.
- `deltas`: the op's chunked cells travelling as edits over the writer's
  newest settled version of the row (SPEC 19.8). Each stands in for one
  column whose cell holds NULL in its place; a second delta for a column, or
  one over a cell that is not that NULL, refuses the commit.
- `access_proof`: when the op's chunked values travel as trees, a version of
  the row the writer may read whose trees vouch for the nodes it did not push
  (SPEC 19.4).

The commit's other fields:

- `kind`: `Mergeable`, or `Serializable { snapshot, reads, own }`: the
  snapshot's positions, each read with the rows and server versions it returned, and
  the writer's own commits that were unsettled when the transaction began. The
  authority re-runs each read under the writer's identity and applies the
  writes only if every result and written row is unchanged since the snapshot.
  A stale or partial replica can only cause a conflict, never a
  non-serializable commit.
- `acting`: who the writes are. `Own`: the sender's, checked and attributed as
  the link. `Attributed { author }`: provenance only, checked as the sender.
  `Session { author, claims }`: a session's writes with its claims. Over a
  trusted link the authority checks them as that session; over a session link
  it checks and attributes them as the link's own identity, whatever author
  they name. A session link's `Attributed` commit naming another author is
  refused.

A link may send a commit only under a node its identity writes as (the
first identity to send one under a node claims it); a trusted link may send
any.

A replica keeps every unsettled commit and uploads it in order on each
connection. It is stored a row per op, not as one encoded message:

- `_jazz_pending (tx, header)`: the `Commit` with its ops and reads left
  out.
- `_jazz_pending_items (tx, part, n, item)`: each op (with its bases, deltas
  and access proof) and serializable read, by list (`part`) and position
  (`n`).
- `_jazz_pending_rows (tx, n, tbl, id, key)`: the local rows it wrote.

What each commit wrote stays in the store at the commit's local seq (the
row's current version, or history under a later pending write), so a
reopened replica can still install what the authority holds back (§8.3). A
row's
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
and uploads it under the session's id. It does not decide it (`INV-TX-23`).

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

`Fate { id, outcome, global }` answers a commit, by its id. The `outcome` is one of:

- `Accepted { at, accepted_at, as_written }`: the commit landed at position
  `at`, which the replica records as where that commit settled; applying a
  fate never moves it backward (`INV-SYNC-5`). A fate speaks only for its
  commit: it never moves the connection position `known` (§8.4). `accepted_at` is the time
  (Unix ms) the authority's clock stamped the commit's writes with: the
  `$createdAt`/`$updatedAt` of every write without an explicit time. It is
  absent only on a resend's recorded fate, which names no rows.
  Its `as_written` lists the commit's rows the server did not ship, each named
  as a base is (the commit's first op that writes it) with its branch key, and
  carrying the authority's digest of its version, so the writer can install
  its own image as the accepted version: stamped at `accepted_at` where its
  own clock stamped it (a row's `$updatedAt`, and the `$createdAt` of a row
  the commit created), and with the create stamp of the row's settled
  versions over a row it did not. A keystroke-grade edit therefore costs one
  `Fate` and one `Settled` downstream.
- `Refused { through, error }`: the commit was refused and landed nowhere.
  `error` is `Conflict`, `Rejected { reason, message }` (a `Reason` by
  stable name; an unknown name arrives as a rejection without a reason) or
  `Other`. The rows the commit wrote shipped back ahead of the fate as they
  are, and the session is complete through position `through` with them: a
  retry's snapshot reads at it, so a serializable retry sees what the commit
  conflicted with.

`global` says the fate is the global authority's (directly or relayed). A
non-global fate is the final word of a replica with no upstream, which orders
its sessions' commits but cannot make them `Global`.

The authority infers, rather than compares, that a row's new version is
likely the writer's image of it (`INV-SYNC-53`). It names a row as written
only if all of these hold:

1. The row's version just before the commit is the one the writer built on:
   the version its base names (an accepted one, or the version the writer's
   own earlier commit landed as); or, with no base, no version at all, and
   every op on the row is an insert or an upsert of an unbranched row. (An
   upsert into a branch may copy a parent branch's version, create stamp
   included, which the writer may hold otherwise.)
2. No text merge changed a value the commit sent (SPEC 4 §4.3), and no op of
   the commit moves the row between branches.
3. The version is still the row's current one when fan-out ships the
   session's rows.
   The writer checks the inference on every such row: each carries the
   authority's 16-byte digest of its version (everything but its seq and the
   stamps). A row whose image matches is installed. One whose image differs, or
   that the writer holds no image of to compare (a catalogue with new versions
   arrived while it was pending), or whose create stamp it lacks, is asked for
   with `ResendWhole` (§8.12), naming the fate's seq: the server ships the row
   whole and, if a later version superseded it, that seq's version too. The fate
   waits until those rows arrive, and every later fate waits behind it, so the
   commit stays pending meanwhile: its writes never drop out locally and later
   writes build on it as on any pending commit. A miss therefore costs one fetch
   of the row, never a divergent cache. A replica whose configured author is not
   the identity its link was accepted as has every row stamped otherwise, so each
   of its writes costs that fetch. Mismatches are counted per replica
   (`as_written_mismatches`); tests of honest writers assert there are none.

`Stored { id }` is durability progress from a relay to a non-durable session:
the relay holds the commit in its store and uploads it, so it is `Local` for a
session whose own store is memory. It is never a decision: a stored commit can
still be rejected, and a `Fate` always follows it, unlike a non-global `Fate`,
which is final.

A fate lost to a disconnect is recovered on the next connection: the replica
re-uploads its pending commits, and the authority answers a commit it already
decided with the known fate, shipping the rows that commit wrote as they now
are, and as the commit left them if since superseded (a resend's writer may
not hold them either way).

### 8.4 Downstream: reads and rows

`Read { id, query, binding, include_deleted, tier, wait, view, span }` asks
for one query's result:

- `view` is the read view the result is of: the current state, a branch view
  (head over an optional live or frozen base), or a snapshot. It is part of
  the read's identity (`INV-BVIEW-12`): the server keeps a live read in its
  view, as ch. 11 §11.5 maintains it. A replica asks for a branch view as it is, over
  a live or a frozen base; a snapshot read asks for the current rows and
  applies the cut itself.
- A base row can reach a replica through any of its reads, and a head
  image the view's query would not match (a tombstone, a version outside
  its filter) still masks it there. So for a branch view over a base the
  server also ships, for each branched table the read reads, the head's
  rows whose base row the reader may see through the view: of the root
  table, base rows the query matches, read without its window on the base
  alone (as of a frozen base's cut) and narrowed to the head's ids; of a
  table it reads besides (a relation, a join, a policy's table), base rows
  the reader may read. It ships all of them with a live read's opening (a
  resumed one's too, since whether the reader may read one can change
  without it changing) and with a one-shot read's answer, and one again
  when the head or the base writes it, unless the reader already knows it
  hidden (below) and still may not read it. A row whose base row the reader
  may not see is never shipped or evicted for this, so a view tells the
  reader nothing of rows it could not see anyway. The server follows these
  rows for the read that masks with them, and stops when that read ends. These rows follow from the
  view's query and the server's state, not from what the session shipped,
  so they hold across reconnects.
- A head row of these the reader may not read is **hidden**, not absent:
  in the view its effective row is the head's version, which the reader's
  policy denies, so the view leaves the row out, as the authority's own
  evaluation of the view does. The server sends `AccessLost { hidden: true
}` for it (§8.6) once: a later write to either version sends nothing
  while the reader still may not read the head's, so the reader learns that
  the row is hidden, not when it is edited. The replica keeps a marker of it in
  `_jazz_withheld` that masks the base's row of the id in every branch view
  over a base with that head; reads of any other view (the base itself,
  the head alone, the current state) ignore it, and no read lists it, deleted
  rows included. The marker reveals only what the eviction does: that a
  version exists. A write through the view to a hidden row is refused
  (`WriteRejected`): it would land over a version the replica cannot see,
  as is a write to that version through any target.
  An eviction is hidden only for the head of one of the session's branch
  views over a base; any other read's access loss keeps no marker. It lifts when an image of the row arrives (the head
  writes it readably again, or a policy input change grants access: the
  server rechecks masking rows as it does supporting rows, §8.6), or when
  an `AccessLost { hidden: false }` says the row is gone. A view opened or
  resumed later re-derives it from the server's state, and a relay passes
  its markers on to its sessions as hidden evictions. Known limit: the
  replica evaluates a view's read policies over policy tables as their
  current state, not in the view.
- A listing judges only the rows the view holds. A cached row it leaves out
  that the head supplies is evicted; one the live base supplies may be
  another read's, so the replica asks for the head's and the base's row
  whole (`ResendWhole`) instead, and the answer brings the head row that masks it
  or the base row as it is now, or their evictions.
- `span: Live` keeps it current until `Unsubscribe` (§8.5).
- `since` on `Read` resumes a live or current-state one-shot read (§8.11).
- `span: Once { at }` answers it once, at the current state (`at` empty) or
  as of position `at`.
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

Settles are per read: a `Settled` speaks for its read alone, and an upstream
may settle two reads at different positions (a sharded gateway settles each
as far as the shards it reads, SPEC 15). The position the whole connection is
settled through travels as `ConnectionSettled { through }`: every read the
upstream settled on the connection is complete through `through`, and nothing
it later sends for those reads lands at or below it. Only it moves the
position a replica knows its upstream through (`known`, which snapshot
references and resumes count from); neither a read's settle nor a commit's
fate does. A replica's own commits accepted past `known` are named by
transaction in the snapshot references it captures and the serializable
transactions it begins, until a `ConnectionSettled` covers them. Every
upstream sends it, an unsharded authority included (there it names the same
seq as the tick's settles), after a tick's `Rows`, `AccessLost`, `Settled`
and `Fate` messages whenever it moved, so a session that hears of a version,
a settle or a fate hears it too, and an idle session hears nothing.

A replica reads by these positions. A live result reads as of its upstream
read's settle (or `known`, when that is further), a one-shot read as of
`known`, while the store holds another writer's accepted versions of the
tables it reads past that position. The read is then a cut at the position,
which history makes exact (each row as its newest version at or before it),
plus this replica's own writes, unsettled or accepted past it. Otherwise it
reads the current state, as it always does against an unsharded authority,
whose settles and connection position move together. So a result never shows
part of a commit its read is not settled through, two results over different
upstream reads may show one commit at different moments, and a writer always
sees its own writes. A relay serves its sessions' results as its store holds
them and leaves the cut to the session, which reads by the positions the relay
passes on (§8.13). Known limit: a branch view's read reads the current state.

`known` is the connection's position, not any one result's: a live result may
read further (its own read's settle), never less far. It is persisted, and a
reopened replica rebuilds what reading by positions needs from its store
alone: the newest accepted version each table holds (an indexed `MAX(_clock)` in the accepted band),
and its own commits accepted past `known`, from its settled-transaction
records. Until its upstream settles again, every read of a reopened replica is
as of `known`, its own writes included. A replica's history pruning must keep,
for each row, its newest version at or before the lowest position a live read
or `known` reads at; a table that keeps less (latest-only) cannot be read
through a cut.

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
rows of its result and their supporting rows. One-shot reads collect their
supporting rows using temporary bookkeeping and retain no watches after
their answer. Writes reconcile their rows at acceptance or refusal, including
upstream settlement through a relay, and retain no watches afterward.
A relay reconciles the same way every session a one-shot answer shipped one
of its unsettled versions to (another session's pending write): once that
version settles, the row ships again as it is now, or is evicted. The
replica orders such a version after every authority position, so no later
answer's `members` can retire it.
A row version ships at most once per session across all its reads (`INV-SYNC-8`).
This state lives only in the serving process and is released on disconnect
(`INV-SYNC-27`).

`Unsubscribe { id }` releases the read's watches. A row no watch holds stops
being kept current: the replica keeps it as a cold copy, which local reads may
still return, and a later read that covers it brings it up to date (its
opening ships what changed, and its `members` drops what left). The same
cold-cache behavior applies after a one-shot answer or a settled write when
no live read covers the row. Its `since` checkpoint remains usable: a later
read reconciles changes, deletions and access changes before settling.

### 8.6 Policy narrowing and eviction

The server applies the session identity's read policy before shipping any
image, supporting row or count (`INV-SYNC-13`), and ships accepted rows only,
never another node's pending or refused writes (`INV-SYNC-12`).

When a reader loses read access to a watched row it holds, through a write
or a policy change, the server sends `AccessLost { table, id, key, hidden }` and the
replica drops its copy (`INV-SYNC-14`). `hidden` says the row still exists as
a version the reader may not read, rather than being gone: in a branch view
over a base it still overrides the base's row, so the replica keeps a marker
that masks that row (§8.4). An eviction of a version that still exists keeps
the session watching the row's id, since the replica may hold the row on
another branch, and later writes that leave that version unreadable send
nothing more, whichever of the session's reads watches the id; a version
that becomes readable ships again while a live read keeps it watched. A row
the reader's own accepted commit wrote and cannot read (a write-only insert)
is reconciled at settlement: the commit's fate reports acceptance without
creating a persistent watch. Local copies are not a redaction boundary; eviction keeps the
replica coherent, not secret.

`CheckAllowed { id, action, acting }` asks whether the session would be
allowed an action now, without writing; `CheckAllowedResult { id, result }`
answers it.

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

The version handshake is one exchange, on every link (a socket, or a worker
and its tab). `Join { version, known }` is the first message on every
connection: `version` is the schema version the session reads by, and `known`
the catalogue the replica keeps (§8.10). This `Join.known` lists schema
versions; it is unrelated to the connection position `known` of §8.4. Its queries use that version's names,
which the server resolves to stored names, and the server serves them in that
version (its result order and lens). Writes, images and evictions travel by
stored names whatever the session's version (§8.4.1), so no message switches
it. A version the server has not admitted is named like the server's current
schema. The server never holds a `Join` behind an unadmitted version, and
answers it with one `Welcome { admitted, catalogue }` ahead of any row:
whether the version is admitted, and the catalogue the replica lacks. This is
a sync message, not part of `WireFrame::Hello`: in-process links have no wire
Hello, and a catalogue may need splitting (§8.8), which a Hello cannot.

`SchemaAdmission { admitted }` tells the session when its version's admission
changes after the `Welcome`. Until it is admitted, the server holds the session's
reads (a draft awaiting its lens), so a read that needs the server fails
with the reason rather than waiting out its budget, and refuses its commits
(below).

A commit names no schema version. Its writer resolves each op's table and
column names to stored names under the schema view that wrote it when it
records the commit, so a pending commit is the same after an upgrade or a
reopen, and one transaction may write through views of several versions. An
insert or upsert names every stored column its writer's version has: the value
given, else that version's default, and a column an earlier lens dropped with
that lens's backwards default (the writer's version descends from it, so the
writer knows it). An upsert's defaults apply only if it creates the row.
The receiver (the authority, a relay, or the replica replaying its own
pending commits) applies ops under its stored view: the current schema named
by stored names, with the stored columns only other admitted versions name,
governed by the current policies. A column an insert leaves out, which the
writer's version lacks, is one a later version added: a copy of a source it
gives takes the source's value, any other takes the stored column's default (a
lens-added column's lens default); a branch column it leaves out keys the row
by its immutable default (`INV-BVIEW-19`). The receiver never applies a
dropped column's backwards default: only a writer that descends from the drop
names it. Offline, a pending commit replays the same way, so the columns only
its version has stay visible through that version's view. A serializable
commit's reads stay in a version: each names the version its query is in, and
the authority evaluates it there.

A commit naming a stored table or column a server does not hold was written
under a version it has not deployed (`INV-SYNC-52`), and is refused there, at
once, with reason `schema_version_not_deployed` (code `write_rejected`): the
app that wrote it was released ahead of its schema, a deploy error that
waiting would hide. So is every commit of a session whose `Join` declared a
version the server has not deployed. The replica rolls it back like any
refused write; the session goes on, and the commits after it upload. A write
through a view of an undeployed version, from a session on a deployed one,
that names only stored columns the server holds (the version changed only a
default, a nullability or an enum's values) is not refused for its version:
its stored names say all it means, and the server validates it under its
current schema (required columns, types, enum values, policies) like any
other write. A server holding a session's reads behind an unadmitted version
refuses the commits behind them the same way. A relay (a worker serving tabs)
refuses only while it is linked to an upstream that sent its catalogue on that
connection, so it knows every version the authority deployed; until then, and
while offline, it holds such commits. A store with no upstream at all is the
top of its tree and refuses at once, so a worker that will sync becomes a
replica (or is given a link hint) before its tabs write. Its `Join` lists only
the versions whose schema it holds in this process, so a version an earlier
process mirrored is sent again and the relay serves it as before. A refusal's
message names tables and columns as the current schema does.

The authority evaluates every write against its own policy data. A relay may
keep and retry pending writes but cannot accept them or make them `Global`.

### 8.10 Catalogue

A replica's `Join` lists in `known` the schema versions whose stored names it
keeps, each with `names_digest` of those names. The server answers in its
`Welcome` with the catalogue `{ namespace, versions, cells, confirmed }`,
carrying every admitted version the replica lacks or names otherwise, in
admission order, ahead of any row; and again, as a `Catalogue` of the same
fields, whenever its store admits a version or learns more of its authority's
catalogue.

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

### 8.11 Resuming reads from `since`

A replica persists, per read identity, the positions its result last settled
through, in `_jazz_watermarks` (one `through` seq) keyed by a digest of the query, binding,
`include_deleted` flag, the view it asked for and reader identity (not the tier or wait, and not the
schema version: the key is over stored names). On reconnect or reopen, it
sends `Read { since, span }` with that position.
Repeated current-state one-shot reads reuse the same checkpoint as live reads.
Historical one-shot reads (`at` set) and one-shot reads of non-current
views always open fully.

A store with an upstream connection ignores downstream `since`: its partial
cache may lack the history needed to reconstruct prior membership and access.
Thus a SharedWorker sends tabs full supporting-row sets for both read spans,
but uses its own checkpoints to reduce responses from the authority.

The responder opens a resumed read one of three ways:

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

- an `AccessLost` for a row the replica held: a result settled earlier may
  have held the row, and a resume from it would count the row as held after it
  became readable again. One for a row it holds no image of changes no
  result, so the watermarks stay (a hidden one still writes its marker, which
  masks rows but holds none);
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
the server ships each whole or evicts it. A row also names a seq when the
replica's own commit produced it there and the replica could not install it
(§8.3): the server ships that version too, if superseded and still readable,
but only if a commit by one of the session identity's nodes produced it. Any
other seq is ignored: it could name a version the session could not read when
it was current. The replica sends the `ResendWhole`
again on every new connection until the whole image arrives, and stores no watermark
meanwhile (§8.11). A live read's rows are never declared this way: its opening
brings them up to date from `since`.

### 8.13 Relays

A relay (a worker) is a replica of its upstream and the authority-facing store
for its sessions (tabs). It serves a session's reads from its store per the
read's tier, asking its upstream with the same `Read` where the tier needs
the authority's answer, and tells its sessions the authority's seq it knows
(`served_through`), so a session's snapshot is one the authority validates as
it is. It ships its unsettled versions at their pending `_clock`, each with its
writer (`Image.written`), and a session stores them there: a relay keeps
a session's commit clock, so a version is at the same `_clock` in every store,
and versions at one `_clock` are told apart by writer (`INV-SYNC-50`). The relay replays its pending commits (its own and the
sessions' it carries) in that order, so a commit written earlier but
delivered later goes under the later one. A session replays its own pending
commits over the relay's version of a row only when they order after it,
each branch of the row on its own, and holds its ordering clock past every
order it is shipped, so a write it makes afterwards orders last. A relay
holds its own commit HLC past every session commit it relays, so a write it
makes afterwards orders after those too. A store counts a received HLC no
further ahead than its own wall clock plus 500 ms (`HLC_SKEW_MS`, the
shard offset bound), so a far-ahead peer cannot carry its next writes past
every later write. The commit HLC is not the provenance clock: a clock seen
from another writer moves no `$createdAt` or `$updatedAt` this store
stamps. A relay uploads its
pending commits in the order it got them, which the authority's order then
follows; where that differs from the commits' clock order, a row's settled
version can differ from the one it showed unsettled (it flips to the
authority's), which is expected. On a new link a session drops the relay
versions it shows (pending versions other writers wrote) back to the newest
accepted version it holds: the link may be to another relay, or to its
relay wiped, and the one it reaches ships its unsettled versions again as
it answers the session's reads. A baseline under the session's own pending
writes stays until they settle. A session names a
relay's unsettled version it built on by the commit that wrote it
(`BuiltOn::Commit { node, tx }`). A relay stores a session's commit as its own pending
commit, answers `Stored`, and relays the authority's `Fate` (§8.3). A relay
whose host says it has no server never numbers accepted positions: its
sessions' commits stay pending (it answers `Stored`, and the `Fate` comes
once a server is added and decides them), a serializable commit waits for
that server rather than being validated by the relay, and it settles its
sessions' reads at the last authority position it holds. Its sessions still
converge on concurrent edits of a row, by the commits' order. A relay's `ConnectionSettled` to its sessions names the position it knows its
upstream through (`served_through`). A relayed result's `Settled` names its
upstream read's own settle, when that is past `served_through`, so the session
reads each result as of its own upstream read (§8.4). The relay passes each
move on: a later settle of the upstream read, with no change to the result's
rows here (they may have arrived through another read), goes out as a
`Settled` without members, and a move of `served_through` as a
`ConnectionSettled` to every session it has sent one before.

## Open Questions

- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Relay aggregation of equal reads from several sessions onto one upstream read, and coverage between reads.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Catalogue admission and synchronization beyond the mirrored catalogue (draft schemas, admission order across relays).
