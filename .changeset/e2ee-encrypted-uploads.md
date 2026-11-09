---
"jazz-tools": patch
---

Publish encrypted Bytea uploads and whole-value replacements together with their
metadata. Declare exact scopes and recipients, consume each source once before
opening the publication transaction, then attach the opaque staged values using
the separately owned native/SDK capability. Real same-transaction authority
validation precedes publication; changed epochs, devices or membership refuse
without replaying sources.

`Db.streamingTransaction` can publish a new scope and its first encrypted files
atomically. Ordinary streamed fields stay ordinary, including plaintext fields
inside encrypted tables and updates to existing scope rows. Scope creation from
streamed fields, branch targets, encrypted Text/JSON, equality-indexed streams and
incremental encrypted-file edits remain unsupported.

Metadata creation preserves schema defaults and stable row IDs. Stored dependency
preparation is provisional until the same indexed predicate and typed cells are
validated under the real publication transaction's policy, snapshot and read set.
Policy-filtered emptiness is not absence. Implicit stored dependencies after an
earlier declaration for the same row, encrypted stored dependencies and a
metadata dependency on its streamed field refuse before source consumption;
callers can supply the supported dependency columns explicitly. User transforms
are not replayed after consuming a source.

Encrypted stream records have the named `jazz.e2ee.stream-record` v1 contract and
byte fixtures, separately from ordinary encrypted cells. Typed queries still
materialise and authenticate the whole value before returning it. No migration
or compatibility claim is made for unreleased intermediate E2EE encodings.

Existing prior-online-bound keys may support the approved provisional offline
path. Local durability does not establish current membership, absence of
revocation, complete history or Global acceptance; reconnect validation can
reject the pending write. Previously disclosed ciphertext and keys cannot be
clawed back. Native staging, binding lifecycle and Inspector protection remain
owned by their separate prerequisite changes.
