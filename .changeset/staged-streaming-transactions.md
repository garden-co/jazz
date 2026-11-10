---
"jazz-tools": patch
---

Allow a completed large-value stream to be attached once to a serializable
transaction, keeping its row and companion writes atomic. Bind the staged upload
to its database, schema, target and identity, and retain its chunk lease until the
transaction commits or is abandoned. Normal permissions and sync still apply.

The SDK can prepare and discard that opaque upload before opening its publication
transaction. Accepted publication without an explicit session keeps the actual
native opening owner; an explicit session remains explicit. Failed attachment,
including wrong-client, actor and retired-owner refusals, consumes the upload.
