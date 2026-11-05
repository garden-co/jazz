# SQLite catalogue policy intrinsics

E17's `CatalogueTableIdentityV1` policy value now resolves from the accepted
catalogue. The scope-binding predicate emitted by E2EE space administration can
compare a row's `scopeId` with the published physical table UUID. Previously the
SQLite compiler rejected this intrinsic, even when the accepted manifest was
already available. An unpublished local view still cannot supply an identity.

## Resolution and trust boundary

The compiler receives a catalogue lookup capability from the database, separate
from identity-provider claims. It resolves the reserved policy path
`["__jazz_table_identity_v1", table]` and the lowered query operand
`"\0jazz-table-identity-v1:" + table` against the policy schema's version and names.
On a server serving an older schema view, this is the governing schema. An
independent fixed local view uses its own schema. Neither stored SQLite names
nor a hash of the schema can substitute for the accepted UUID.

Resolution uses indexed catalogue records and produces an ordinary bound UUID
parameter. It does not load rows or scan catalogue history. Every production
query-compiler entry point receives the same lookup capability: preparation,
execution, nested output, subscriptions, write checks, readable-version checks
and sync policy-dependency analysis. Prepared queries retain their abstract
query; execution resolves metadata again. Immutable manifests keep the value
stable within a published schema version, without a separate identity cache.

Reserved paths never enter user or provider-claim resolution. This applies to
UUID comparison shortcuts, scalar/list predicates, null predicates, inherited
and nested policies, and relational expressions. Missing accepted metadata or
an unknown policy table fails with a Query error naming `CatalogueTableIdentityV1`.
Malformed stored records remain Storage errors. A missing identity cannot become
null, an empty result, a guessed UUID or a caller-supplied claim. `IS NULL` is
false only after successfully resolving a non-null accepted UUID. Malformed
reserved paths are rejected by schema admission or by the compiler.

The ordinary policy engine still decides access. An accepted table identity
only binds a scope; it does not itself authorize a session, qualify row history,
or acknowledge a transaction. SYSTEM authority and explicit trusted delegation
retain their existing boundaries. Candidate-created permission evidence remains
unsupported.

## Worked cases and validation

Alice's row names the authority-issued UUID for `documents`. Mallory supplies
provider and raw reserved-key claims naming another UUID. Reads, insert checks
and both update clauses still compare against the accepted manifest. Reassigning
an accepted row to Mallory's scope rejects and rolls back. Removing accepted
metadata makes even an `IS NULL` policy reject rather than authorize a row.

After publication of `documents` as `tasks`, a client opened on `documents`
continues reading by the old names. Server policies use `tasks` and resolve its
UUID in the governing descriptor. Fixed local views continue resolving their
own published names. Live subscription and native deployment tests exercise
this distinction. A typed `drafts` view registered only locally has no accepted
identity and its intrinsic-dependent reads reject.

Paired accepted reads use the same policy binding, including through a cold
trusted relay. Changing a row's scope after a serializable observation conflicts
at validation. Existing anti-spoofing and missing-metadata tests remain unchanged.
Additional tests exercise scalar/list/null paths, nested EXISTS, inheritance,
relation IR, ordinary subscriptions, writes, renamed views, native deployment
and the public browser binding.

## Encoding and deployment

This change adds no durable or wire fields, tags, UUID allocation, or binding
ABI. It uses the encodings pinned by `sqlite-catalogue-compatibility.md` and the
existing policy/query representation. The prior all-peers-together SQLite
upgrade requirement still applies. An earlier SQLite binary rejects these
intrinsics, so deployments relying on them must use the ported runtime.

Authenticated catalogue cache seals, offline journals, browser-worker storage
admission, NAPI/native relay and the other explicit E2EE runtime boundaries are
not implemented by this compiler port. The focused checks do not qualify the
complete encrypted SDK application path.
