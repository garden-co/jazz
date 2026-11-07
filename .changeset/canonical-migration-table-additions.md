---
"jazz-tools": patch
---

Reject a migration that adds a table already present in the complete published
source schema, even when its partial source witness omits that table. Legitimate
new tables and untouched omitted tables keep their existing behaviour.

Resolve partial migration witnesses against their canonical published endpoints.
A direct deployment without an active schema may use the sole published graph
tip; ambiguous sources require an explicit hash. Historical migrations retain
their own endpoints rather than inheriting the requested deployment head.
