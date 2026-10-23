# SQLite scoped fetch v1 compatibility

A trusted backend can now make a one-shot authority read as a served session.
Previously, a remote read with a served identity or request-scoped claims returned
an unsupported-query error. For example, Alice's request with an `owner = alice`
claim can fetch Alice's permitted file rows over the backend's SYSTEM connection
without changing that connection's identity or another request's claims.

## Encoding

The wire role is `jazz.sqlite.sync.scoped-fetch.v1`. Its postcard message is the
append-only `SyncMessage::ScopedFetchV1` variant, tag 19 (`0x13`), with these fields
in order:

| Field             | Rust type                 | Meaning                                        |
| ----------------- | ------------------------- | ---------------------------------------------- |
| `id`              | `u64`                     | Connection-local one-shot request ID           |
| `query`           | `Query`                   | Original query, including its requested window |
| `binding`         | `BTreeMap<String, Value>` | Prepared query parameters                      |
| `author`          | `String`                  | Canonical `AuthorSubject`                      |
| `claims`          | `BTreeMap<String, Value>` | Immutable claims for this request              |
| `include_deleted` | `bool`                    | Whether permitted tombstones are included      |

The query and value payloads use the existing SQLite protocol encodings. This
change does not alter variants 0–18 or their field layouts. Ordinary `Fetch`
remains tag 3. Responses use the existing `Rows`, `Answer` and `Refused` messages;
there is no second row-transfer path or materialized-result response.

`crates/jazz/fixtures/sqlite_scoped_fetch_v1.json` pins exact bytes for an ordinary
fetch and a scoped fetch with a projection, window, binding, canonical author,
claim and deleted-row flag. `sqlite_scoped_fetch_wire` checks both serialization
and decoding/re-encoding. Changes to these bytes require another compatibility
review; the serializer's default layout alone is not a contract.

## Deployment and storage

The backend and authority must both run the updated protocol before a scoped
request is sent. There is no capability negotiation or mixed-version guarantee:
an older peer cannot decode tag 19, and an older adapter may abort the connection
when decoding it. Upgrade both endpoints together. Existing unscoped request
bytes remain unchanged, but that does not establish general cross-version
compatibility for the rest of this unreleased stack.

This follow-up adds no persisted schema or storage encoding. Requests and answers
are transient; fetched rows use the existing SQLite row and history storage.

## Admission and result boundaries

Only an authority connection authenticated as SYSTEM may submit this variant.
An ordinary Alice connection cannot supply Bob's identity, replace Alice's claims,
or forge SYSTEM. A malformed author returns a typed query error. The serving
backend remains explicitly trusted to authenticate its sessions and claims.

The authority evaluates the query and SELECT policy with the request's identity
and claims. It ships authorized root row images through normal sync and reports
their IDs and authority sequence in `Answer`. The backend reads exactly those IDs
at that sequence and evaluates the same request scope locally. Other cached rows,
revoked rows absent from the answer and optimistic local writes cannot enter the
result. The authority applies limit/offset once; the backend does not apply that
window again. An empty authority answer remains empty.

The connection identity and cached identity claims are unchanged. Cancellation
cleans up unsent requests and answer slots. Reconnection interrupts a pending read
with `NotObserved`. If the authority is unavailable, an explicit remote read also
returns `NotObserved`; a local-first read may still return its locally permitted
cache under its existing deadline semantics.

## Supported boundary and validation

This version supports a single table with row-local SELECT policies, ordinary
filters, projection, ordering, pagination and explicit deleted-row reads. It
rejects relationship and cross-row permission evidence, joins, aggregates,
history and branched-table reads with typed query errors. A relay rejects scoped
requests rather than forwarding them under its own identity. Chunk availability
continues to follow the existing large-value sync contract; this change does not
add a chunk hydration or retry mechanism.

The host integration tests exercise concurrent claims, authoritative empty
answers, revocation, pending writes, pagination, tombstones, unavailable links,
cancellation and reconnection. Raw-wire rejection tests cover compromised peers
that bypass the SDK's checks. This scoped-fetch role is not E2EE accepted-history
proof or catalogue identity, and does not qualify the browser or native E2EE
runtime.
