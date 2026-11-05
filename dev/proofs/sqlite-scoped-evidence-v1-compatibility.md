# SQLite scoped permission evidence v1

A trusted SYSTEM backend can now read a flat table whose SELECT policy follows
bounded, indexed dependencies, including recursive relation helpers. Previously
these delegated reads returned an unsupported-query error. Both a direct backend
and a trusted relay re-evaluate the requested identity and immutable claims using
the authority's exact dependency versions.

For example, Alice's document may require an active account membership, a path
through parent groups and a resource grant. The authority first authorizes her
query normally. It collects the bounded permission inputs for the returned roots,
sends their images through ordinary row and chunk sync, and names their exact
versions in a receipt. The backend evaluates the original query and policies over
that evidence only. Clearing Alice's account membership makes her next remote
read empty, even if her backend still caches the old grant and document.

## Trust and result boundaries

Only a connection already authenticated as SYSTEM may request delegated reads or
receive the additional dependency images. Such a host is explicitly trusted with
raw policy data and session authentication. Ordinary session connections are
refused before evidence is collected. Reading a private dependency table as Alice
still uses that table's SELECT policy; the evidence receipt grants no application
read capability.

The backend obtains policies from its authenticated catalogue descriptor rather
than trusting a policy-free bootstrap schema. The request's identity and claims
remain unchanged. A private, transient SQLite table limits every participating
source to the receipt's exact `(table, id, branch, sequence)` coordinates. Both
current rows and retained historical versions can satisfy those coordinates.
Other cached rows, local writes and later dependency versions cannot enter the
recheck. Every relay repeats it before forwarding the receipt. The final ordered
root IDs must match the authority's answer exactly, or the read returns
`NotObserved`. The authority applies the requested window once.

If Bob changes a grant after the authority answered Alice, a delayed receipt can
still use the named historical version. Alice's next request observes the new
authority state. Request IDs are connection-local; cancellation removes their
slots, and the existing reconnect guard interrupts pending reads. Incoming fetch
answers are staged until the containing sync batch commits. A malformed later
frame cannot leave a successful in-memory receipt for rolled-back evidence.

## Encoding and deployment

The append-only postcard variant `ScopedEvidenceAnswerV1` has tag 36 (`0x24`)
and role `jazz.sqlite.sync.scoped-evidence-answer.v1`. Its fields, in order, are:

| Field      | Type                   | Meaning                                                 |
| ---------- | ---------------------- | ------------------------------------------------------- |
| `id`       | `u64`                  | Pending delegated request ID                            |
| `through`  | `u64`                  | Authority sequence, below the optimistic sequence range |
| `rows`     | `Vec<[u8; 16]>`        | Ordered authorized root IDs                             |
| `evidence` | `Vec<ScopedReadRowV1>` | Complete bounded permission input set                   |

Each `ScopedReadRowV1` contains, in order, `table: String`, `id: [u8; 16]`,
`key: Vec<u8>` and `seq: u64`. Table names follow the peer's admitted schema.
Duplicate coordinates, zero or future sequences and oversized evidence sets are
rejected. A schema view missing a dependency table receives a typed refusal.

`crates/jazz/fixtures/sqlite_scoped_evidence_v1.json` pins the exact bytes, including
a multi-byte authority sequence. The wire test checks both serialization and
complete decoding. Existing scoped-fetch and accepted-evidence fixtures continue
to pin their old tags and layouts. Row images, chunks, durable storage and all
previous variants keep their existing encodings. The evidence table is TEMP and
its contents are rolled back after each recheck; receipts are not persisted.

The authority, backend and every relay must be upgraded together. There is no
capability negotiation: an older endpoint cannot decode tag 36. A failed scoped
read must never be retried as an unscoped read. This is an unreleased stack and
makes no general mixed-version compatibility promise.

## Bounded work and supported shapes

Evidence collection reuses the indexed candidate-permission input walker. A
shared budget of 16,384 units charges root fields, dependency fields and recursive
SQL work. Dependencies require a scalar declared-reference correlation or an
indexed UUID bound. Recursive relation inputs retain their depth bound. The
recheck drives physical row lookups from the bounded evidence table and prepares
large-value comparisons and ordering only for those cells; it never loads
unrelated cells to repair missing evidence. Text ordering uses the existing
logical-value ranker over that bounded cell set. Equal stored prefixes are
compared using their chunks, equal logical values share a rank, and ordinary
row-ID tie-breaking and authority pagination remain unchanged. An unselected
sort column still participates in ordering. The authority's initial query still follows the normal
query and permission execution path.

This ordinary receipt covers flat, unbranched scoped Remote reads. The later
[request-scoped relation port](sqlite-scoped-relations-compatibility.md) also
supports bounded existential traversals, including nested traversals and root
windows, with the same receipt. Joined or included relationship output, arrays,
aggregates and history queries remain unsupported.

The [accepted scoped extension](sqlite-accepted-scoped-evidence-v1-compatibility.md)
adds a distinct receipt for paired reads at pinned authority cuts; paired
accepted relation reads retain their explicit refusal. Negative cross-row
policies remain outside the public schema converter's supported shapes. Missing
chunks hydrate through normal sync; missing versions or policy metadata fail
closed. Relay schema translation remains unsupported.

Public API regressions cover direct and relayed recursive permissions,
revocation, concurrent claims, private dependency policies, chunked claim
comparisons, deleted and retargeted cached grants, and delayed historical
receipts. Ordering regressions cover both directions, pagination, unselected sort
columns, long shared prefixes and equal values, directly and through a relay. A
backend with damaged chunks for an unrelated cached document can complete the
scoped sort while that unrelated value remains unhydrated. Wire fault tests cover ordinary-session forgery and atomic receipt
publication after a failed sync batch. These checks do not qualify the browser,
mobile or unpublished native runtime port.
