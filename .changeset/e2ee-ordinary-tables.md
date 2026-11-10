---
"jazz-tools": patch
---

Declare encrypted columns with `s.table(...).encrypted({ space, columns })`.
Ordinary insert, update, upsert, restore and selected-value reads perform their
preparation through the existing transaction handles. Scope, root, grants and
initial ciphertext retain one atomic core outcome and a Global activation floor.
Application policies still decide data access and administration; possessing a
key does not grant permission to write.

Existing scope checks retain branch and deletion coordinates. Mutable inputs and
preview bytes are isolated from later encryption. Plaintext-only applications
and projections do not require encryption keys. Unsupported encrypted predicate,
index, nested-result, subscription, partial-write and migration shapes fail closed.

A private device-bound cache populated by prior online validation supports
retained-content-only reads after persistent offline reopen. It does not replay
complete membership, prove absent revocation, select a latest epoch or authorise
new writes. Known denial and owner retirement invalidate use. The optional local
cache has its own named v1 format; old stores without it need online validation.
The existing encrypted-cell record format and its cryptographic identity bindings
are preserved.
