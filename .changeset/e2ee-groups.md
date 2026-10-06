---
"jazz-tools": patch
---

Add explicit authenticated groups with nested membership, signed key epochs,
public topology separated from application administration, recovery coverage and
repair, and permanently sealed empty lineages. Membership follows accepted
account/group history rather than possession of a delivered key.

Export the group schema and topology-permission helpers from `jazz-tools/e2ee`.
Reject historically ineligible authors without breaking unrelated groups, allow
interrupted recovery to reuse an identical staged key, and propagate historical
verification failures instead of reporting them as unusable recovery deliveries.
Group authority observations use the current global tier rather than the
retired edge query tier.
Group operations reuse session-owned device keys and scoped private-store
decoding. Optional group tables remain available in schema-only application
configuration.

Require explicit `{ kind: "account" | "group", id }` selectors for group
membership changes; `leave()` always removes the current account. Limit SDK
history collection to relevant historical group components, retain complete
descendant epoch revisions, reject competing successors at one authority
position, and validate initial root coordinates.

Discover recovery groups from accepted membership rather than delivery
proposals. Reject loss of required group coverage, handle synchronous and
asynchronous key-envelope failures consistently, and try later recovery
protectors after group-owned unusable-delivery failures without hiding
operational errors.

Batch scoped topology reads instead of reopening root and epoch queries for
every visited group. Drain bounded UUID-input chunks without limiting graph
coverage or weakening exclusive acceptance.
