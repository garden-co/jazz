# SQLite UNION occurrence answers

An ordinary remote UNION read now returns an explicit occurrence
receipt. A row can occur in several arms, and each arm or nested UNION can
have its own offset and limit. Root IDs alone cannot identify that result;
reapplying those windows to a cold cache can skip the rows just delivered.

The authority executes the original query with the connection's admitted
identity and claims. It supplies the selected rows, their readable indexed
join witnesses at the answer cut, and the ordered `(row ID, leaf arm)` pairs.
The replica derives a materialisation query that restricts each arm to its
named IDs and removes the windows already applied by the authority. Original
filters, ordering, projection and ordinary permission checks remain active.
The result must reproduce the receipt exactly before it is returned. This
does not load whole arm inputs to reconstruct missing page predecessors.

An ordinary relay forwards the original request under its admitted upstream
identity and claims. It waits for the upstream receipt and required content,
checks its exact occurrences at that cut, then forwards the same occurrences.
Connection, schema and claim fencing remain part of the existing fetch path.

Live subscriptions use the same occurrence receipt, followed by an existing
`Settled` message with the same request ID and accepted-prefix cut. The client
stages both messages until the sync savepoint commits. A missing receipt,
different cut, duplicate occurrence or unknown arm cannot settle the result.
A reconnect discards any unmatched receipt from the previous connection.
Coverage changes refresh only the groups attached to that upstream query;
an empty answer can open a subscription without any row writes.

Strict remote groups materialise the exact received occurrences at that cut.
Local-first groups may also show the ordinary permitted local result while
relevant writes are pending, labelled as a local preview. The two modes have
separate fanout groups. Preview detection probes existing indexes for each
query input table, and upload recording refreshes only affected UNION groups.
Relays pair a receipt with the settlement of their latest result, so a later
local preview cannot inherit an earlier global settlement.

A cold foreground can materialise its private owner's pending previews only
after the owner acknowledges its exact identity and schema. The existing
indexed pending-input predicate excludes accepted rows and stops applying
once an authority catalogue is present. A pending write can therefore be
previewed offline and then disappear when the authority accepts it under a
policy that denies the writer read access.

## Named encoding

**`jazz.sqlite.sync.union-answer.v1`** appends `SyncMessage::UnionAnswerV1`
at postcard discriminant **41**. Its complete field order is:

```text
tag:uleb128(41) | request_id:u64 | through:u64 |
occurrences:Vec<(row:[u8;16], arm:String)>
```

Integers and vector/string lengths use canonical unsigned LEB128; strings
contain UTF-8 bytes. Arm names use the existing semantic leaf-path encoding
from the UNION query compiler. A row may occur under different arms, but a
duplicate pair or unknown arm is invalid. An empty vector is an exact empty
answer, not missing coverage.

The answer must match an outstanding UNION fetch or subscription.
Ordinary row-ID answers cannot satisfy it. A UNION receipt alone cannot satisfy
an accepted or request-scoped read. Optimistic authority cuts are invalid.
The production exact decoder rejects trailing bytes, truncation and alternate
varint spellings. `crates/jazz/fixtures/sqlite_union_answer_v1.json` pins a
duplicate-root response and an empty response independently of the encoder.

**`jazz.sqlite.sync.union-coverage.v1`** names the live sequence of two
individually framed messages: the receipt above, followed by the existing
`Settled` discriminant **11** with this field order:

```text
tag:uleb128(11) | request_id:u64 | through:u64 | window:None | global:bool
```

Both IDs and cuts must match. `global=false` represents owner-local coverage;
the cut still names an accepted prefix, while the existing owner-input protocol
carries pending writes. The fixture pins complete global and empty local pairs
with independently assembled bytes. No existing tag or field changes.

**`jazz.sqlite.sync.scoped-union-coverage.v1`** names a trusted backend's
one-shot scoped reply: tag **41**, followed by the existing
`ScopedEvidenceAnswerV1` tag **36**. The second frame retains its field order:

```text
tag:uleb128(36) | request_id:u64 | through:u64 |
rows:Vec<[u8;16]> | evidence:Vec<(table:String,id:[u8;16],key:Vec<u8>,seq:u64)>
```

Both IDs and cuts must match. The row vector must match the occurrence vector
exactly, including duplicates and order. Even row-local policies and empty
results require the qualified evidence frame. The receiver stages frames until
the sync savepoint commits, pairs replies across ticks, and discards partial
receipts on cancellation or reconnect. A malformed reply cannot be overwritten
by a later valid pair in the same batch. Reversed, mismatched and downgraded
replies cannot complete the request. If an invalid batch also rolls back the
initial catalogue, reconnecting replays that metadata before a fresh read can
complete. Independent fixtures in
`crates/jazz/fixtures/sqlite_scoped_union_coverage_v1.json` pin populated and
empty pairs using the unchanged individual message encodings.

Each arm's indexed join inputs and their permission dependencies share the
existing scoped evidence budget. The replica and every relay evaluate only the
exact occurrence IDs and evidence versions at that cut, under the request's
immutable claims. An ordinary session cannot request trusted scoped evidence.
The backend may receive unreadable permission witnesses through that existing
trusted route, but they do not become readable query results.

**`jazz.sqlite.sync.accepted-union-coverage.v1`** names a paired accepted
reply: tag **41**, followed by the existing `AcceptedAnswerV1` tag **23** for
an unscoped request, or `AcceptedScopedEvidenceAnswerV1` tag **37** for a scoped
request. Tag 23 retains `request_id:u64 | through:u64 | rows:Vec<[u8;16]>`;
tag 37 retains the tag-36 fields above. Each frame remains independently
framed. IDs, cuts and ordered roots, including duplicates, must match before
publication. Tag 41 alone and the ordinary scoped tag 36 cannot establish
accepted coverage. The existing accepted-row messages must also establish each
returned root's content transaction and Global coordinate. A duplicated root
has one matching settlement for each semantic occurrence.

The SDK checks one settlement per returned occurrence. Repeated roots are
allowed only for a UNION query, and every occurrence of that root must carry
the same content transaction and Global coordinate. Missing or extra receipts,
substituted roots, conflicting content evidence and invalid u64 coordinates
remain errors. Ordinary queries still reject duplicate rows. The existing
`RowSettlement` fields and binding encodings are unchanged.

The accepted receipt is retained in an in-memory qualified snapshot. Local
materialisation uses the exact occurrences at the accepted cut, while the
transaction read set keeps the original full query and existing UNION content
evidence. Authority commit validation therefore still detects inserted page
predecessors and changed join witnesses. No durable transaction field changes.
Relays preserve the original downstream qualifier kind, even when forwarding
an unscoped SYSTEM read required an explicit scope upstream.

A staged write touching this query requires rollback-only authority validation
of the **original query**, including its windows. Restricting that validation
to the received IDs would hide page changes caused by rows absent from the cold
cache. The existing bounded overlay check compares the query before and after
staging, checks write permissions, and publishes no simulated writes. Its
current admission still requires a trusted backend and a non-SYSTEM session
identity. Ordinary sessions and SYSTEM transactions with touching staged writes
remain refused. Writes to unrelated tables do not require this simulation.

Independent bytes in
`crates/jazz/fixtures/sqlite_accepted_union_coverage_v1.json` pin populated and
empty tag-41/tag-23 and tag-41/tag-37 pairs. The accepted regressions cover cold
duplicate pages through ordinary and trusted direct/relay connections; split,
missing and malformed qualification; cancellation; reconnect; sync rollback;
empty-result phantom conflicts; pinned authority-begin reads; and observation
at begin with a complete prior result. A cold late proof cannot establish prior
local observation. This does not claim that a cache containing only a previous
page can reconstruct the original query's full local result at begin.

UNION arms can now return matching narrowed projections. Renamed output keeps
its source type, and large-value ordering uses the physical source column for
chunk ranking while sorting the projected output cells. Sorting one output
column drawn from different large-value sources across arms remains explicitly
unsupported; it needs a combined ranking contract.

## Compatibility and supported paths

Existing tags 0–40 and all stored layouts are unchanged. Pending serializable
UNION evidence retains its existing encoding. Older SQLite builds cannot
decode tag 41; peers must upgrade together before using this remote read
capability. This is part of the coordinated unreleased SQLite/E2EE port.
Readers from the earlier unpublished ordinary-UNION port reject a scoped tag-41
reply, and readers from the scoped-only port reject an accepted tag-41 reply.
They cannot silently treat these replies as qualified coverage; these peers
must also upgrade together. No stored query, transaction evidence or account layout changes.

The implemented one-shot path is an explicit `Remote` read through ordinary
connections, including ordinary and private-owner relays. It supports nested
and arm-local windows, duplicate roots, large values and readable indexed
existential joins. Live subscriptions now also use occurrence coverage;
foreground and durable local-first waits use temporary normal subscriptions.
Local regressions cover cold live pages, arm changes, foreground nested and
empty answers, reconnects, and isolation of local previews from strict reads.
They also cover split and malformed coverage, batch rollback, cancelled reads,
joined-witness revocation, large cells, private-owner previews, accepted-row
revocation and refusal of substituted relay identities or claims.

Trusted request-scoped one-shot reads now support the same bounded UNION arms,
including narrowed output, renamed columns, cold windows, private permission
witnesses and relays. Regressions cover concurrent claims, evidence arriving
later than rows, empty results, revocation, malformed pairs, cancellation,
reconnect and sync-batch rollback.

The public SDK retains each arm's own projection scope, stored-column selection,
order and window, including windows around nested UNIONs. Result types retain
the selected columns through named, array and nested unions. The SDK does not put a
root-table projection over a relationship arm's differently named output scope.
Seed modifiers that the relation format cannot represent, including partial
large-value selections, includes and deleted-row output, are rejected before
execution. The encrypted-query guard remains unchanged.

**`jazz.sdk.union-arm-output.v1`** names the optional `output_v1` object in the
typed canonical input used to derive array-arm labels. It contains the present
`select`, `orderBy`, `limit` and `offset` fields. Object keys retain the existing
sorted canonical encoding; array order and typed scalar values remain exact.
The label remains `derived:` followed by lowercase hex of BLAKE3 over the UTF-8
canonical input. `packages/jazz-tools/fixtures/union-arm-output-v1.json` pins
literal preimages, bytes and labels for an unchanged seed and two selected pages.
Unmodified seeds keep their previous labels. Named labels stay caller-defined.
Older SDKs discarded these arm modifiers; their already serialized queries keep
their original meaning. New queries use existing relation nodes and require no
query-codec or stored-layout change.

Paired accepted reads now support the bounded paths above. Online serializable
UNION hydration through the ordinary transaction-read API, request-scoped live
subscriptions and recursive UNION inputs retain explicit unsupported guards. Local UNION reads and offline
serializable UNION validation retain their existing behaviour. The public SDK
regression uses a real server and cold NAPI backend to check concurrent claims,
narrowed output, duplicate roots, revocation and arm-local and nested windows.
Native integration coverage and a WASM build do not qualify browser or installed
mobile runtime behaviour.
