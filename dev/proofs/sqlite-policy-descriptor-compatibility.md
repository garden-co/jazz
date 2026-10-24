# SQLite authority policy descriptors

The browser SDK opens private storage owners with structural schemas. Policies
are installed separately at the authority. Previously a private owner evaluated
session reads using its empty local policies, so accepted account identities
could be stored correctly yet remain invisible. The owner now receives and
persists the authority's active policy descriptor through ordinary upstream sync.

## Trust and revision rules

The descriptor binds the complete policy-bearing schema to the exact accepted
catalogue head: namespace root, publication position, active revision and schema
UUID. Its structural schema must equal the already admitted structural descriptor.
A schema UUID alone is insufficient because policies can change without changing
that UUID. Two policy meanings at the same revision are rejected. Older positions,
foreign namespaces and descriptors received before their matching catalogue head
are rejected inside the same sync savepoint.

Only the admitted upstream can supply descriptors. Subscriber uploads cannot
install them. Private owners keep their original authenticated subject and claims;
receiving policies grants neither SYSTEM privilege nor delegated session access.
An ordinary relay stores and forwards metadata but does not become an authority.

A newer catalogue head immediately removes the private owner's previous policies
until its matching descriptor arrives. This also applies when a transport splits
the two messages across ticks. Subscriptions are reset and serving sessions are
revalidated. Reopen restores the exact accepted policies; a stale descriptor
cannot restore an earlier grant. Publication under a different structural schema
still fails with the existing explicit unsupported-shape boundary for private
owners. Policy evaluation uses the ordinary permission engine and indexed
catalogue records, without scanning unrelated data.

The SQLite compiler also resolves `session.authMode` from the admitted author
issuer, using the same mapping as the public policy API. Provider claims cannot
override this intrinsic. Tests cover local-first, external and anonymous issuers
with forged raw and namespaced claims. This compiler fix changes no encodings.

## Encoding and deployment

The new durable role is `jazz.sqlite.policy-descriptor.v1`, stored as the sole
`slot=1` BLOB in `_jazz_policy_descriptor_v1`. The new wire role is
`jazz.sqlite.sync.policy-descriptor.v1`, appended as SyncMessage tag 32. Both
payloads contain `CatalogueHeadV1`, followed by `SchemaVersion`. The head uses the
existing root bytes and unsigned varints for through/revision, followed by its
UUID. SchemaVersion uses its existing UUID and length-prefixed canonical public
schema JSON, including policy expressions. The wire form prefixes the durable
payload with byte `0x20`. No existing catalogue or sync tag changes meaning.

`sqlite_policy_descriptor_v1.json` independently pins both forms using the
existing `notes.text` catalogue fixture's namespace and UUID, revision 128, and a
SELECT True policy. Its hex was assembled from the documented fields and literal
public-schema JSON, without copying serializer output. A test compares encoding,
decoding and the actual persisted BLOB. Existing catalogue-v1 fixtures remain
unchanged.

Deploy these unreleased SQLite peers together. Older peers cannot decode tag 32.
A cold private owner talking to a peer that supplies no descriptor retains empty
policies after catalogue admission. Existing stores gain the separate table;
structural catalogue captures are unchanged. A fresh owner restored only from
an app-wide structural capture has no policy descriptor and cannot use that
capture as permission evidence. Same-root offline reopen uses its own durable
descriptor. No record here asserts fresh authority coverage or transaction fate.

## Validation

Public API tests cover policy-free owner/foreground schemas, serializable account
creation, account-scoped reads, a foreign subject's rejection, offline reopen,
policy revocation without a schema UUID change, split metadata delivery, conflicting
revisions and malformed descriptors rolling back the accompanying catalogue.
The real encrypted SDK browser experiment now passes account bootstrap and
identity reads; chat creation reaches the separate foreground initialization-seal
boundary. It is not an end-to-end encrypted application pass.
