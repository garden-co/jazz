# SQLite private owner projection

A generic SQLite relay journals uploads without exposing pending rows in its
shared cache. An explicitly admitted private owner can now reconstruct its one
subject's pending projection. Foreground queries receive complete, identified
pending units for the rows they observe, together with ordinary accepted inputs.
A reload does not need the original tab to remain alive.

The browser adapter activates this capability for its existing subject-isolated
worker store. A foreground can write once its current owner acknowledges the
same subject and schema. Authority connectivity is not required. This port does
not establish that every E2EE application path or pending snapshot is supported.

A new offline account opens with the public, policy-free schema. Until its
private owner receives its first authority policy descriptor, the same admitted
identity
can also recover its pending previews from a sibling or reopened foreground.
The ordinary query adds an exact pending-coordinate index probe for provisional
rows; it does not enumerate the outbox or grant SELECT on accepted rows. Query
filters still select the roots, and the existing bounded input closure supplies
the complete matching transactions with their original attribution and receipts.
Another identity, a generic relay, historical reads and permission evidence
queries do not gain this preview capability. Policy dependencies are unchanged.

A structural catalogue capture contains no read policies, so installing it does
not close this bootstrap preview path. The first authority policy descriptor
closes it and revalidates subscriptions, including when that descriptor denies
every read. A retained descriptor keeps the preview path closed even when a
newer catalogue head has invalidated it and the replacement policy is still in
flight. Offline reopening preserves that restriction. Accepted live and frozen
bases always require their existing accepted read evidence, even while a pending
preview is selected. This changes no storage
or wire bytes. The cold-owner tests in `sqlite_owner_inputs.rs` cover filtered
complete-unit recovery, durable reopen, cross-identity refusal and an accepted
insert whose SELECT is denied, alongside the existing unreadable-base tests.
The split-policy regression in `sqlite_owner_session_reads.rs` keeps a pending
row physically present while checking that a newer head revokes its preview
before the new descriptor arrives, including after reopening.

An admitted same-schema foreground also receives the owner's retained authority
position when a cached query has accepted rows but no pending input units. The
existing `OwnerInputsV1` envelope carries empty unit/base lists in this case.
This lets a new local accepted-at-begin observation use the proofs delivered
with those rows. The cached query still settles at Local, pending writes still
need their own receipts, and a transaction begun before hydration does not gain
later evidence. The public reopen regression in `sqlite_owner_inputs.rs` checks
all three boundaries. The message encoding and durable values are unchanged.

An owner can retain its opened schema while the authority renames tables or
columns. It resolves the current policy's names through accepted catalogue
identities to the same stored table and column epochs. Retained reads, newly
connected foregrounds and offline reopens use that policy governor without
changing the owner's public schema. A new head revokes stale policies
immediately; a replacement grant becomes visible only after the complete
metadata batch commits. A later invalid frame cannot leak a rolled-back grant
through subscription events. The catalogue rename regression and authenticated
view revocation regression cover these boundaries.

Forwarded catalogue requests pin the owner's structural schema and upstream
connection. Installing an authority policy does not change either, so the first
policy handshake cannot cancel an otherwise valid in-flight request. The reply
must still match the exact accepted head and requested opened schema. Neither
change adds storage or wire fields.

## Storage contracts

Three additive SQL encodings have independent versioned names. Existing
`local-commit.v1`, relay journal, accepted-row and fate encodings are unchanged.

- `jazz.sqlite.owner-position.v1`: `_jazz_owner_positions_v1` maps an integer
  publication position to a 16-byte original node UUID and positive integer
  transaction time. Positions and original `(node, tx)` pairs are unique.
  A recovered tab's original transaction number is never used as the owner's
  publication position merely because the numbers happen to match.
- `jazz.sqlite.owner-scope.v1`: `_jazz_owner_scope_v1` has one row, keyed by
  integer `singleton=1`, containing canonical subject TEXT and a 16-byte schema
  identifier BLOB. Opening under another subject fails before schema names or
  physical tables change. A different schema also fails at that boundary while
  either the owner's outbox or its relay journal contains pending writes.
- `jazz.sqlite.owner-input-row.v1`: `_jazz_owner_input_rows_v1` stores the exact
  `(stored table, row UUID, branch bytes, original node, original tx)` coordinate
  with integer pending and writes flags. Read dependencies are retained with
  `writes=0`. Row and node UUIDs are 16-byte BLOBs; an unbranched
  key is an empty BLOB. The complete coordinate is the primary key. A partial
  coordinate index contains only pending writers; another index supports exact
  original-identity lookups. Terminal receipts set `pending=0` in their fate
  transaction, retaining coordinates for missed-receipt reconciliation.

`sqlite_owner_projection_v1.json` pins identity and subject bytes, SQL types,
positions and root branch bytes through a real journalled write. Runtime tests
also verify the active lookup uses the partial index. This unreleased stack
requires a coordinated upgrade; these new private-owner tables have no legacy
reader contract. Generic relay files do not acquire a private subject merely by
creating the empty tables.

## Wire contracts

`jazz.sqlite.sync.owner-projection.v1` is append-only postcard tag 29:
`OwnerProjectionV1 { known }`, where `known` is a vector of original 16-byte node
UUIDs and unsigned transaction-number varints. A foreground sends it after its
schema declaration on every connection. It declares decoder support and the
pending inputs already observed by that foreground. It grants no identity or
trust, and it does not request a sweep of the owner's pending journal.

`jazz.sqlite.sync.owner-inputs.v1` is tag 30:
`OwnerInputsV1 { schema, through, bases, frozen, units }`. The schema is a
16-byte identifier. `through` is an unsigned accepted authority prefix, strictly
below the optimistic publication range. It normally follows in Answer/Settled;
including it here lets frozen inputs resolve their snapshot before replay.
Each live base names a logical table (UTF-8 string), a 16-byte row ID, a byte-vector
branch key and an optional unsigned accepted sequence. `None` means there is no
accepted image at that coordinate. `Some(seq)` requires that exact accepted
image and its accepted-content evidence before replay. The base list must match
the complete union of the units' live input coordinates, including live inherited
bases; duplicate, omitted and unrelated coordinates are refused.

Each frozen entry contains the same base descriptor followed by an unsigned
`at` snapshot cut. The descriptor identifies the exact accepted image (or
absence) selected at that cut. A selected sequence cannot exceed `at`, and `at`
cannot exceed `through`. The frozen entries must exactly match the units'
frozen dependencies; different cuts of the same row are distinct. Historical
images and accepted-content evidence travel through the normal row protocol
before the envelope. Live and historical base manifests have separate checks,
so an older frozen image cannot substitute for a required current base.

Each unit
contains an unsigned owner publication position followed by a length-prefixed
byte vector containing exactly one existing Commit message. Trailing bytes,
other message variants, another subject or schema, repeated positions and
conflicting copies of an observed unit fail closed. The complete nested Commit
retains the original node, transaction number, operations and transaction kind.
The owner describes an owner-local write with its admitted subject without
altering that write's original durable upload.

`sqlite_owner_inputs_v1.json` independently pins both tags, field order,
transaction varint boundaries, publication position, base field order and nested
Commit bytes. Base fixtures include absence, a non-empty branch key and an
accepted sequence at the 127/128 varint boundary, an accepted prefix and a
frozen base descriptor with its snapshot cut.
Older peers cannot decode these appended tags; mixed-version operation is not
negotiated. A peer without the input declaration receives an explicit refusal
when its query needs pending units. A foreground rejects optimistic sequence
numbers in ordinary Rows messages.

`jazz.sqlite.sync.owner-ready.v1` is append-only postcard tag 31:
`OwnerReadyV1 { schema, subject, node }`. It contains a raw 16-byte schema ID,
a length-prefixed canonical subject string and the owner's raw 16-byte node ID.
`sqlite_owner_ready_v1.json` independently pins these bytes. Only an explicitly
admitted private owner responds, after checking the peer's identity and schema.
The authenticated host transport supplies trust; this message grants no identity
or permission and is neither a storage receipt nor authority acceptance.

The foreground pins that owner node for its lifetime. Each connection must
acknowledge again; another owner, subject or schema fails closed. New writes
remain unsupported before acknowledgement and after detachment. A foreground
that has already admitted a private owner also holds retry payloads during
reconnection until that owner acknowledges. Older queued writes which entered
foreground mode before any private-owner admission retain their existing relay
receipt behaviour. The SDK's browser connection keeps its normal wire pump
running while waiting for this reply; startup fails if the connection closes.

## Input and receipt boundaries

Normal query roots, supporting rows and watched-coordinate changes select units
through the partial coordinate index. Selecting a transaction includes its whole
input footprint; pending transactions writing those coordinates join
the closure. Inherited branch bases are inputs: querying a draft includes its
base, while querying a base does not collect unrelated drafts. A frozen base
uses its exact historical image, without collecting later pending writers at
that coordinate. Snapshots whose dot lists name pending writes are still
explicitly unsupported in this recovery envelope. Unrelated pending writes are
not enumerated. A closure exceeding
4,096 coordinates or units, or a complete input envelope exceeding the wire
frame budget minus 64 KiB, is refused explicitly.

Pending images are removed from ordinary Rows output. Retained accepted bases
travel through the existing Rows, Origins, content witness and accepted-row
pipeline before the pending envelope, which precedes the query answer. The owner
checks accepted bases against accepted policy dependencies; a pending grant does
not supply accepted read evidence. Before the first authority policy descriptor, pending
coordinates for the admitted identity may qualify as the private previews
described above; accepted bases cannot. A unit with another unreadable coordinate
or base is refused rather than partially disclosed. Referenced large-value nodes use
the ordinary authenticated chunk request/response path. Before installing an
input envelope, the foreground fetches the verified chunks needed to reconstruct
its index keys. Later semantic frames, including query answers and receipts,
wait behind that envelope. This backlog is capped at eight MiB. Missing or corrupt
required chunks fail explicitly; unsolicited chunk responses are not installed.
The remainder of a large value is fetched normally when a read needs it.

The foreground replays the original units over its accepted shadow. Receiving
its own echo changes the existing unit's publication position instead of adding
a second copy of its counter delta. Owner positions occupy the prefix starting
at `2^52`; new foreground publications occupy the suffix starting at
`2^52 + 2^51`. Both floors are within JavaScript's exact integer range. Original
handles and snapshot dots retain their original transaction identities. Each
received input envelope is replayed before its following answer or receipt. A
required unit whose replay fails is a protocol error; its failure cannot be
silently turned into a successful partial query result. Later authority fates
still reconcile speculative descendants through the ordinary rebase path.

A received pending unit does not manufacture a Local receipt, Global receipt or
accepted fate. Those remain the existing owner-storage and authority receipt
protocols. The real owner-storage barrier captures both complete relay uploads
and the private owner's own durable uploads. Only captured, still-pending
identities receive Local receipts after the backing-store flush. Later arrivals
require another barrier; rejection during a flush cannot be undone by its late
confirmation. Reopen reconstructs the journals and requires a new barrier.

A standalone empty update keeps the existing row receipt and does not publish
another upload under that receipt's transaction identity. Previously that upload
could replace a pending insert's journal entry with an update that the authority
could not apply after restart. Operations inside an open transaction remain part
of that complete unit. The `sqlite_write_state.rs` regressions cover accepted and
rejected inserts across reopen and a transaction containing an empty update;
neither case changes receipt or journal encodings.
A session observes only identities actually delivered to it or declared
as already known on reconnect. Those observers receive the same original-pair
OwnerStoredV1 receipts as uploaders, including replay after a lost receipt.
The existing tag 28 and its byte fixture are unchanged; barrier membership is
in-memory state and changes no stored encoding. A terminal authority receipt
sends the affected accepted coordinates before the original-pair FateV1. A newly attached tab is not
sent historical rejection callbacks for transactions it never observed.

For example, Alice has pending writes at two old tab nodes with the same numeric
transaction time. Her new tab queries both and receives two different original
identities at different owner positions. The authority accepts one and rejects
the other. The rejected projection disappears while the accepted one remains.
If the new tab disconnects first, its known-identity declaration recovers the
missed receipt from the owner's retained fate and coordinate records.

## Serializable read content

Renumbering an optimistic publication does not necessarily change a read. Rebase
can also change a counter's value without changing its original transaction ID
or even its current publication number. Local serializable validation therefore
compares both original versions and the content observed by each read. When a
read takes place after promotion, its wire version is translated back to the
publication position declared by the transaction's original dependency map.

`jazz.sqlite.serializable-observed.v1` appends variant 3 to `WireKind`, inside the
existing Commit encoding. Its field order is `snapshot`, `reads`, `own`,
`content`. `reads` keeps the existing WireRead layout. `own` keeps the
serializable-origins-v1 triples of publication position, 16-byte original node
and original transaction time. `content` is a vector per read, containing one
32-byte digest for each row in that read set. Roots precede relation children,
which follow the existing depth-first traversal. Both vector dimensions must
match the read sets. Original dependencies still undergo the existing identity,
position and uniqueness validation.

Each digest is BLAKE3 derive-key with context
`jazz.sqlite.serializable-read-content.v1`. Its postcard preimage, in order, is:
logical table string; 16-byte row ID; deletion boolean; vector of projected
`(column name, Option<Value>)` cells; vector of joined occurrences; optional
root history identity. A joined occurrence is a 16-byte row ID and optional
history identity. A history identity is an unsigned original transaction time
and 16-byte original node ID. Strings and vectors use postcard's length prefix;
unsigned integers use its varints; options use its 0/1 discriminant. Logical
Value uses its existing postcard enum encoding, including its fixed variant
order and nested record encoding. A change to that encoding requires a new
read-content version; it must not silently change these digest preimages.
Publication aliases are excluded. Content, deletion, occurrence and original
history identities remain part of the digest.

Every newly emitted serializable upload with reads uses the observed variant,
including ordinary replicas and private owners. Write-only serializable uploads
keep their existing encoding. The authority reruns the query under the admitted
session and verifies both the resolved row versions and content digests. Thus,
if Alice reads 2 after her pending counter increment, but Bob's earlier authority
increment makes that same pending write materialise as 5, Alice's transaction
conflicts. Pure owner promotion that leaves the value at 2 passes read validation.

`sqlite_owner_read_content_v1.json` independently pins the digest preimage for a
selected counter row and the appended commit-kind bytes, including varint
boundaries and original dependencies. Tests compare a real emitted digest with
those fixed preimage bytes, and verify accepted/rejected outcomes after durable
owner reopen. Legacy commit variants and their byte fixtures remain readable.
An older binary cannot read the newly appended variant; this unreleased stack
still requires the coordinated release already described above.

A current relay fetch now reports its accepted prefix as the snapshot boundary.
Its private local publication numbers cannot become an authority snapshot.

## Failure boundaries and validation

Only an ordinary authenticated durable replica can enter private owner mode,
before admitting peers or transactions. The stored subject persists across
reopen. A foreground which has received private inputs remains bound to its
subject after those inputs settle. Repurposing the durable owner role, projection
failure after journalling, or failed sync persistence closes access until reopen;
no fabricated rejection can replace an upload already durably retained.

Rust integration tests cover owner reopen, query-relevant recovery, LocalOnly
isolation, complete multi-row units, equal numeric transaction times, independent
terminal fates, missed-fate reconnect, accepted-base update rejection, counter
echo and snapshot identity, subject isolation, malformed envelopes, schema
boundaries, exact accepted-base and provenance requirements, recovered Local
waits, concurrent storage barriers, lost receipts, reopen, rejection during flush
and fixed format fixtures. Transaction tests cover pure promotion, counter
changes at the same position, promotion over a foreign counter, authority
validation, malformed evidence and durable reopen. Frozen-base tests cover
owner reopen, multiple historical cuts of one coordinate, later authority
acceptance and refusal of missing or altered history/provenance. Existing
mutation-blocker expectations remain unchanged for clients without a qualified
owner. Admission tests cover reconnect, detached owners, wrong identities,
atomic write recovery and separate Local and Global receipts. Real Chromium
checks exercise the WASM wire path and IndexedDB close/reopen with a large
pending write recovered by a fresh foreground. These are focused local checks,
not CI-equivalent validation or a complete E2EE application qualification.
