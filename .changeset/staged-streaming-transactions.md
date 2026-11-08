---
"jazz-tools": patch
---

Allow a completed large-value stream to be attached once to a serializable
transaction, keeping its row and companion writes atomic. Bind the staged upload
to its database, schema, target and identity, and retain its chunk lease until the
transaction commits or is abandoned. Normal permissions and sync still apply.
