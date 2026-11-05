# SQLite accepted scoped permission evidence v1

A trusted SYSTEM backend can now obtain paired accepted rows and content
settlements when a SELECT policy depends on indexed rows in other tables. Direct
and relayed reads use the authority's pinned transaction cut. Previously these
reads refused the dependency shape. They still require the enclosing transaction
to reach Global durability before the caller acts on a decision.

This extends the [paired read contract](sqlite-paired-reads-compatibility.md) and
[scoped permission evidence](sqlite-scoped-evidence-v1-compatibility.md).

## Pinned permission decisions

Alice reads a document allowed by a private grant. Her first accepted read pins
an authority sequence and carries the exact root and dependency coordinates.
If Bob later retargets that grant to another document, Alice's second read still
uses the old grant at the pinned sequence. Its content settlements also remain
unchanged. Global commit checks the original query against current authority
state and rejects the revoked decision. It also detects a new matching document
that Alice's backend has never cached.

The authority runs the requested query with its normal SELECT policies. At a
historical cut, each bounded dependency lookup chooses the current image if it
is old enough, or the latest historical image at that cut. A foreign key that
moved after the cut therefore remains discoverable under its old value. Both
scalar correlations and recursive relation helpers use these indexed sources.
The shared evidence budget also bounds SQLite work during the authority query.

Backends and relays re-evaluate the original policies and immutable claims over
the exact evidence coordinates. Policies come from the authenticated catalogue,
including when the local bootstrap schema has no policies. The receipt covers
the bounded prefix needed for pagination; the backend applies the original
window once. Large values hydrate through ordinary chunk transport. Text sorting
uses only the bounded evidence cells, including unselected sort keys.

Local commit checks the admitted snapshot and any known newer root versions.
It records the original full query and content read set for authority validation;
it never replaces that query with a list of cached IDs. This is essential for
phantom detection with a partial cache. A successful local check is not a Global
acceptance receipt.

## Trust and remaining limits

Only an explicitly trusted SYSTEM connection receives private dependency images
or a scoped evidence receipt. An ordinary authenticated connection may request
its own accepted rows with matching claims. A trusted relay can use private
evidence upstream, but sends only authorised root images, their content proof
and the ordinary accepted answer downstream. The receipt grants no application
permission to read a private dependency table.

The [accepted overlay check](sqlite-accepted-overlay-v1-compatibility.md) handles
staged writes to root or permission dependency tables. The authority simulates
the staged prefix under normal write permissions and verifies that the original
accepted query is unchanged, then rolls the simulation back. A grant that
exposes an uncached root still refuses the paired read. Writes to unrelated
tables need no additional check.

LocalOnly semantics and the accepted-at-begin admission frontier remain
unchanged. A new Remote receipt cannot retroactively satisfy the begin-snapshot
requirement. Queries remain limited to stored, flat, unbranched roots; joins,
relationship output, aggregates and history queries remain unsupported. The
public schema converter still excludes negative cross-row policies.

## Wire encoding

`jazz.sqlite.sync.accepted-scoped-evidence-answer.v1` is the append-only postcard
variant `AcceptedScopedEvidenceAnswerV1`, tag 37 (`0x25`). Its fields are:

| Field      | Type                   | Meaning                                               |
| ---------- | ---------------------- | ----------------------------------------------------- |
| `id`       | `u64`                  | Pending accepted scoped request ID                    |
| `through`  | `u64`                  | Pinned authority sequence, below the optimistic range |
| `rows`     | `Vec<[u8; 16]>`        | Ordered authorised root prefix                        |
| `evidence` | `Vec<ScopedReadRowV1>` | Bounded root and permission coordinates               |

Each evidence entry retains tag 36's field layout: table string, 16-byte row ID,
branch-key byte vector and sequence. Tag 36 is ordinary scoped coverage and
cannot satisfy an accepted request. Tag 37 cannot satisfy an ordinary request.
Both responses become visible only after their containing sync batch commits.
Existing tags, row images, chunks, accepted content bytes and transaction read
sets retain their encodings.

`crates/jazz/fixtures/sqlite_accepted_scoped_evidence_v1.json` pins the exact
message bytes, including a multi-byte sequence. Tests check complete decoding
and re-encoding, and reject substitution of an ordinary scoped response.

## Derived history index

`jazz.sqlite.permission-history-index.v1` adds a derived SQLite index for each
leading declared UUID index column on an unbranched table. Nullable UUID columns
qualify too. The name uses stored table and column names:
`<table>__h_permission_v1(<column>)`. Its columns are the indexed UUID, `id`, `_b`
and `_seq`. For example:

```sql
CREATE INDEX IF NOT EXISTS "grants__h_permission_v1(resource)"
ON "grants__h"("resource", id, _b, _seq);
```

Fresh tables create the index with their other DDL. Opening or admitting an
existing schema installs it idempotently. This performs a one-time index build
over that table's retained history, uses additional disk space and adds index
maintenance to later historical writes. These are migration and write costs;
permission reads use indexed bounds and never fall back to scanning an
unrelated table. Row bytes and existing primary keys are unchanged.

`crates/jazz/fixtures/sqlite_permission_history_index_v1.json` pins the index name
and exact SQLite catalogue SQL. A reopen regression removes the derived index
to simulate a previous store, then verifies that reopening reinstalls it while
preserving every physical historical row.

## Deployment and validation

Upgrade the authority, trusted backends and every relay together. Tags 36 and 37
have no capability negotiation; an older endpoint cannot decode them. Do not
retry a failed scoped request without its scope. Intermediate unreleased E2EE
schemas have no mixed-version compatibility guarantee.

Public API regressions cover direct and relayed pinned grants, recursive
historical graph changes, read-only Global validation, uncached phantoms,
chunked ordering and windows, evidence admission at begin, staged-write checks,
and ordinary-session evidence isolation. These checks do not qualify the
unpublished native port or the full browser and mobile runtime.
