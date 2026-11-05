# Original writer ownership through trusted relays

A writer keeps its ownership when it switches between a direct connection and
trusted SYSTEM relays. The host must explicitly trust each SYSTEM relay to
attest the original owner.

Alice can write directly to an authority, continue through two trusted relays,
reopen those relays and return to the direct connection. Her original node and
transaction numbers remain valid. The authority still settles each write once.
Previously, the authority attributed the writer node to the connection identity,
so the relayed route could bind it to SYSTEM and conflict with Alice's direct
route.

A trusted relay now declares the admitted original owner before each forwarded
commit. This owner determines who may use the writer node. The commit retains
its existing author, permission subject, claims and operation bytes. For example,
a SYSTEM backend's attributed write still belongs to the backend's writer node;
the attributed principal does not become that node's owner.

A runtime sends no declaration for its own writer. The authority binds that
writer to the connection's authenticated identity as before. A SYSTEM-configured
runtime can therefore use an ordinary session connection: session writes are
checked as that connection, and backend-only attribution remains refused.

Only a connection already authenticated by the host as SYSTEM may send the
declaration. An ordinary session cannot gain this capability by naming SYSTEM
in its payload. Wrong-node declarations and malformed owner spellings close the
connection. A declaration conflicting with an existing node owner rejects the
write before transaction deduplication, leaving the original receipt intact.
Existing rules for linked principals within the same admitted account continue
to apply. This trusts a SYSTEM relay to attest an original owner; it does not add
independent cryptographic proof of that ownership.

The declaration applies to the next commit on that connection. It neither
publishes rows nor settles a write. Permission checks, private-owner admission
and real authority receipts remain required. A new connection starts without a
declaration; reconnect reconstructs declarations from durable node ownership.

## Queuing and recovery

The relay resolves ownership by indexed node lookup and adds a declaration when
it first queues an upload unit. Backpressure retains the remaining messages in
order. If the declaration has been sent but the commit is blocked, the next turn
retries the commit without adding another declaration. A one-message transport
budget can therefore make progress. The earlier prototype added declarations
again on each retry and could stall indefinitely.

The relay persists ownership in the existing `_jazz_nodes` table. Its durable
upload journal still stores the original commit. Restart reconstructs the wire
declaration from that node's stored owner, including through multiple relays.
Work is proportional to queued uploads and their chunk batches.

## Encoding and release

The new append-only message is **jazz.sqlite.sync.writer-owner.v1**, tag 47 in
**jazz.sqlite.sync.v3**. Its exact layout is:

```text
JSQL03 | unsigned varint tag 47 | 16 node bytes |
unsigned varint owner UTF-8 byte length | canonical owner UTF-8
```

`crates/jazz/fixtures/sqlite_writer_owner_v1.json` specifies independent bytes for
an accountless principal, an account-bound principal, SYSTEM and a system origin.
The boundary regression checks both public decoders, every truncated prefix and
trailing bytes. Existing tags 0–46 keep their layouts. The separate fixture
extends the existing 47-message corpus without rewriting its historical samples.

Commit bodies, the `jazz.sqlite.relay-upload.v2` journal, storage format 3 and
existing receipt encodings do not change. Stored nodes have the same ownership
table and author encoding. No migration is provided for nodes incorrectly bound
to a relay by an intermediate unreleased build. Deploy matching authorities and
trusted relays together: an older reader refuses the new tag. Native, NAPI and
WASM runtime artifacts must be built from that same source revision.

## Local qualification

New regressions cover bounded queue progress, ordinary-session forgery,
wrong-node and conflicting declarations, single-commit consumption, malformed
or abandoned declarations, accountless and account-bound owners through two
reopened relays, and separation of ownership from permissions and provenance.
The unchanged direct/relay route regression now passes. Existing relay tests
also exercise private-owner refusal, delegated claims, pending dependencies,
large-value restart and interrupted sends.

The NAPI/WASM boundary tests load actual matching artifacts and exercise the
new fixture through both semantic and WebSocket batch decoders. They reject
truncated frames, trailing bytes, noncanonical tag varints, unknown tags and
older semantic headers. The tests run alongside the existing format-3 corpus.
Installed Android/iOS execution is outside this qualification.
