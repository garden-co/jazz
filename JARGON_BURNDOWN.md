# Jargon burndown

This file tracks internal terms that make Jazz harder to understand or review.
Its purpose is not to establish a larger official vocabulary. Each entry should
eventually disappear because we adopted plain language, renamed the code, or
replaced the underlying mechanism with a simpler abstraction.

## How to use this file

- Add a term when it requires source-code knowledge to understand a design,
  review, error, or test report.
- State what the term actually denotes, not merely what its name suggests.
- Prefer the plain-language replacement in PRs, specifications, diagnostics,
  and new code.
- Record the design smell the jargon may be hiding.
- Remove an entry only after its exit criteria are met. Git history remains the
  archive; this file should shrink over time.

## Active terms

### Typed policy claim

**Meaning today:** A value from the session's claims after the query compiler
has associated it with the database type required by a particular policy
comparison. The type is part of the internal binding identity. This prevents a
claim used as a string in one place from being confused with a claim used as a
UUID or nullable value elsewhere.

**Why it is opaque:** “Typed” sounds like a different kind of permission or a
public claim category. It actually describes compiler validation and coercion
at a use site.

**Prefer:** “session claim” when the type is irrelevant; “schema-checked
session claim” when the distinction matters.

**Design debt:** Claim paths, expected column types, compiler aliases, and
runtime values are carried through several loosely coupled maps. That makes an
internal representation detail visible in authorization explanations.

**Exit criteria:** One claim-binding abstraction owns the path, expected type,
coercion, identity, and value. Public diagnostics and design prose no longer
need the adjective “typed.”

### Maintained witness

**Meaning today:** A row-version payload carried with an incremental query
update so the receiving database can materialize a returned row or an included
related row. It is not inherently a policy-evaluation row, and it may describe
either a root result or supporting relation content. Authorization has already
selected what the update may reveal.

**Why it is opaque:** “Witness” suggests proof used to decide permission. In
this context it usually means data needed to reconstruct an already-authorized
live result.

**Prefer:** “row version needed to materialize the live result,” shortened to
“result row version” when unambiguous.

**Design debt:** Incremental membership, relation structure, ordering, and row
contents travel through closely related but differently named channels. The
receiver should consume one explicit whole-row result-update model instead of
needing “witness” terminology.

**Exit criteria:** The protocol and implementation expose an explicit result
row-version payload type with a name that describes its role. Tests and errors
no longer refer to maintained witnesses.

### Padded projection

**Meaning today:** An internal fixed-shape query record in which only some
fields came from the source operation and the remaining positions contain
absent placeholders. It is a sparse intermediate query record, not an authored
row and not a valid partial synchronization unit.

**Why it is opaque:** Both words are overloaded. “Projection” can mean a public
column selection, a schema migration, or an internal record transformation;
“padded” hides the important fact that some cells have no source value.

**Prefer:** “sparse intermediate query record.” When describing the bug, say
that this intermediate record was incorrectly treated as an authored whole
row.

**Design debt:** Internal sparse records can share enough representation with
stored row versions to be confused for sync payloads. Rows are the unit of
permissions and synchronization, so that conversion should be structurally
impossible rather than repaired downstream.

**Exit criteria:** Authored and synchronized row versions can only be
constructed from complete canonical rows. Sparse query-engine records have a
distinct type and cannot enter row-version or wire-payload APIs.

### Projection

**Meaning today:** Depending on the call site, this can mean selecting public
columns, translating between schema versions, reshaping a query-engine record,
or decoding physical enum storage.

**Why it is opaque:** These operations have different correctness rules, but a
single word makes them sound interchangeable.

**Prefer:** Name the operation: “column selection,” “schema translation,”
“record reshape,” or “enum decoding.”

**Design debt:** APIs and helpers inherit the generic name even when they
implement only one of these operations, making ordering and whole-row
invariants difficult to see.

**Exit criteria:** New APIs use role-specific names, existing ambiguous APIs
are renamed as touched, and architecture documents reserve “projection” for at
most one clearly defined operation.
