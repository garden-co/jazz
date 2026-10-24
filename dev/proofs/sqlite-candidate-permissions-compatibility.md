# SQLite same-unit creation permissions

The SQLite authority now evaluates positive INSERT policies containing
`ExistsIncludingCreated`. For example, a transaction can create a parent and
child together when the parent has an independent permission grant. The parent
need not be staged first. An ungrounded self-reference or mutual cycle cannot
manufacture permission, and rejecting any required check rejects the whole unit.

## Permission contract

The ordinary write executor captures each selected permission and its exact root
image, including UPDATE USING, UPDATE WITH CHECK and provenance checks. The
checker uses the authenticated session; a row's claimed author is not a grant.
At E17, deferred compilation retains the accepted catalogue identity provider;
reserved table-identity paths cannot fall back to caller claims. At E14 and
above, the origin-mapped serializable variant also receives the same proof,
including transactions following a sealed initialization.

The same executor handles authority-local session transactions and ordinary
serializable uploads. Existing trusted backend attribution admission remains in
force. Replicas keep optimistic writes, with authority settlement deciding fate.

Only a coordinate absent from current storage on every branch can become a new
witness. It must have exactly one INSERT or absent UPSERT in the complete
serializable unit and belong to an unbranched table. Tombstones, updates,
restores, branched rows and multiply written coordinates cannot supply new
witnesses. SQLite's existing serializable conflict validation, indexed absence
lookup and enclosing savepoint keep absence and publication atomic. A mergeable
unit can use accepted evidence or an independent grant, but cannot use its new
rows as witnesses.

Each new row earns its own selected permissions before it becomes evidence.
The checker computes a least fixed point from independently authorised seeds.
It performs this proof twice, starting from no new witnesses each time: once
against the accepted baseline and once against the resulting state. The proof
orders may differ. Removing or demoting an accepted grant in the same unit must
still leave a valid final proof. Intermediate writes retain their own selected
root images and checks.

Each source occurrence keeps its own mode. Ordinary EXISTS and unmarked nested
sources see accepted rows only; marked sources add independently authorised
creates. Accepted witnesses remain visible alongside those creates. Merely
having a marker in another operation's policy cannot turn ordinary updates into
mutual grants. Schema admission still rejects negative and non-INSERT markers.
Caller-supplied query candidate-source flags do not acquire this private scope.

Scalar forward and reverse permission inheritance also uses accepted roots.
Each inherited check selects the requested operation's policy, with the usual
default depth of ten and any tighter explicit depth limit. Inheriting UPDATE
uses its USING grant; an immutable parent's WITH CHECK refusal does not prevent
it from granting a capability. A newly created administration row cannot grant
inherited permission until acceptance. An inherited policy containing
`ExistsIncludingCreated` returns an explicit unsupported error rather than
exposing another table's creation-aware predicate.

For example, a founder can create a space, its administration row and its
designated initial grant in one unit, using independent and marked INSERT
permissions. Later lifecycle records can inherit through the accepted space
and administration row. A different initial grant cannot borrow that new
administration row's capability, and removing an accepted capability in the
same unit rejects dependent lifecycle writes.

While staging a local transaction, SELECT dependencies use the accepted baseline.
An unproved membership cannot unlock an existing secret before commit. Provisional
reads requiring large-value preprocessing return a typed unsupported error.

## Bounded work and large values

Each unit has a 16,384-step proof/dependency budget, including repeated checks
and scoped preprocessing. Each batch of 256 SQLite VM instructions during a
permission check also consumes one budget step. This bounds cyclic relation
walks as well as Rust-side proof passes. Exhaustion rejects the whole unit with
the existing bounded-work error; the connection's progress callback is removed
on success, failure or unwind. There is no unchecked remainder.

Every policy dependency requires a mandatory UUID equality whose target is a
primary key or the leading column of a declared index. This includes references
to a shared account table and generic scope identifiers; the public policy
validator still checks reference compatibility. All additional predicates remain
part of the proof. Each current-source UNION arm contains that key filter and
uses its index. Baseline history is driven by this unit's publication sequence,
then looked up by exact coordinate; it never starts from all accepted rows.
Uncorrelated dependency sources and array-reference inheritance in these scoped
units return an explicit unsupported error. Reverse references into branched
tables also require an unavailable branch-bound index shape and are refused.

Relation helpers support inner joins, filters, projected UUID keys and bounded
recursive gathers. Each physical source must have a mandatory indexed UUID
equality to a session/outer key, recursion frontier or an already bounded join
input. Equalities below OR/NOT do not qualify as lookup bounds. Additional
predicates, including nullable membership endpoints, remain in the query.
Ordinary relation occurrences see accepted evidence, never same-unit creates.
Baseline checks use exact historical images for rows changed by the unit;
post-state checks use the surviving graph while excluding new coordinates.

The compiler distributes current/history alternatives over each complete
SELECT, so a join or recursive frontier remains in the same SQL scope as its
lookup. CROSS JOIN fixes the bounded lookup order. History starts with this
unit's publication-sequence index and then reads each exact prior coordinate.
At most 64 physical alternatives are emitted for one relation SELECT; larger
shapes return a typed unsupported error. No source falls back to a table scan.
The managed group-membership schema explicitly indexes `recipientId` alongside
its existing reference endpoints, so account-seeded group traversal has a
bounded entry point on SQLite.

String and byte comparisons prepare only exact root and related dependency
cells, including historical images used by the baseline proof. Dependency
lookups require the same mandatory indexed UUID equality. They stop at
the remaining budget rather than enumerating a whole large-value column. A
corrupted unrelated chunked value cannot poison an independent proof. Equal
chunked values compare by logical content; a common prefix is not equality.

Scalar inheritance uses the same bounded preparation, operation selection and
depth limits as the permission compiler. Baseline preparation includes exact
historical operands when the unit also updates accepted grants. Matching final
values cannot manufacture a grant absent from the baseline.

Large-value preprocessing through uncorrelated dependencies or relation helpers
is explicitly unsupported. Branch-view dependencies and branch moves in a marked-schema unit are
also explicitly unsupported. These errors identify the unsupported pattern;
they do not fall back to loading unrelated data.

## Storage and wire compatibility

This port changes no durable encoding, query/catalogue encoding or wire layout.
The existing `ExistsIncludingCreated` policy representation, relation IR and
serializable commit framing remain unchanged. The managed membership index is
declared through existing schema index metadata; deploy that schema with the
authority so its physical index exists. An older schema without a bounded seed
index is rejected rather than scanned. No client-supplied proof is trusted or stored:
the authority reconstructs the proof from its own transaction and session.

Two connection-local TEMP tables, `_jazz_candidate_created_v1` and
`_jazz_candidate_qualified_v1`, contain only `(stored table name, raw row UUID)`
keys. They are cleared between units and phases, share the enclosing savepoint,
and are absent from durable storage and uploads. Large-value comparison scratch
uses the existing format and is cleared before leaving its savepoint. Failure
cannot leave a qualified witness available to the next transaction.

Deploy the SQLite stack together. An older authority still rejects this policy
shape, so new application transactions cannot rely on it during a mixed-version
rollout. No retired engine or unreleased E2EE migration guarantee is added.

The core contracts in `sqlite_candidates.rs` cover allowed and denied graphs,
occurrence isolation, selected operation checks, both proof phases, provisional
reads, scalar inheritance, chunked comparisons and rollback. The public
`sqlite_candidate_relations.rs` tests cover account/resource isolation, projected
recursive keys, joined current/history combinations, rejected new witnesses,
revocation, historical baseline grants, missing indexes, bounded SQL work and
rollback recovery. They also check nullable endpoint revocation through ordinary
reads and a cached replica's strict remote read. Real Chromium tests exercise both an
authenticated wire upload and an authority-local session through the public WASM
binding. These checks qualify the permission primitive; they do not qualify the
complete encrypted application, browser worker, native binding or journal flow.
