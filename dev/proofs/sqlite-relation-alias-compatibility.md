# SQLite relation aliases

A non-UNION relation can retain a terminal projection such as `title AS label`.
The prepared query keeps the existing relation AST, so serializable read
evidence contains both the selected source and its output name. SQL reads the
source column with its original type and publishes the alias. A renamed row ID
is a UUID cell; the row's identity remains a separate carrier.

Filters and ordering still refer to source columns. A projected alias called
`rank` cannot stand in for a different physical `rank` sort key. Maintained
queries use the existing bounded source-key lookup when that distinction
matters. Hydration requests carry source selections; the reader applies its
own retained projection after normal permission and coverage checks. Joined
reads retain independently readable witnesses and their revocation checks.

Bare relations and the public normaliser's canonical envelopes prepare to the
same query. An envelope with conflicting ordinary clauses is rejected. The
existing model restriction on adding `select` or nested arrays over a narrowed
or renamed relation remains in force, as do reserved output names. Renaming
deletion metadata still requires `includeDeleted`.

## Existing named encodings

This port adds no wire discriminant, field, storage layout or digest role.
Renamed output uses **`jazz.sqlite.serializable-read-content.v1`**. Its existing
preimage already includes each projected cell's name and typed value. Thus
`title AS label` and `title AS other` have different content evidence without
changing the preimage layout. UNION occurrences continue to use their separate
**`jazz.sqlite.serializable-union-read-content.v1`** role.

As with other retained relation ASTs, an aliased read requires
**`jazz.sqlite.serializable-union-observed.v1`**, existing postcard discriminant 4. Its `WireRead.query` retains the existing relation AST. Admission validates
the canonical envelope, executes the projection under the admitted identity,
and compares both row versions and named content before applying writes. It
does not reconstruct aliases from a hydration request's physical selection.

`crates/jazz/fixtures/sqlite_relation_alias_evidence_v1.json` pins the exact
plain-row preimage for `docs.title AS label` and a complete commit containing
that retained query. The regression checks a real upload's content against
the fixture and pins both encoding and decoding of the complete message.
Existing ordinary and UNION byte fixtures remain unchanged.

## Compatibility

Older SQLite builds decode the same AST and message layout but refuse renamed
projections. Peers must therefore upgrade together to use this capability; a
mixed deployment must not claim aliases are supported. No new migration or
downgrade rewrite is introduced. This remains part of the coordinated,
unreleased SQLite/E2EE port.

Coverage includes quoted and repeated aliases, exact integers, large values,
UUID identity, provenance, maintained windows, direct and relayed permissions,
snapshot reads, paired accepted reads, offline commit validation and forged
query evidence. Remote UNION hydration and history aliases remain explicitly
unsupported by their existing shape guards.
