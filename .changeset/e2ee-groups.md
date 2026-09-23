---
"jazz-tools": minor
---

Add explicit authenticated groups with nested membership, signed key epochs,
public topology separated from application administration, recovery coverage and
repair, and permanently sealed empty lineages. Membership follows accepted
account/group history rather than possession of a delivered key.

Export the group schema and topology-permission helpers from `jazz-tools/e2ee`.
Reject historically ineligible authors without breaking unrelated groups, allow
interrupted recovery to reuse an identical staged key, and propagate historical
verification failures instead of reporting them as unusable recovery deliveries.
