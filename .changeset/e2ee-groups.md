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
