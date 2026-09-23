---
"jazz-tools": patch
---

Declare encrypted table columns through the public schema builder and compose
managed lifecycle schemas and policies automatically. Previously, encrypted
scope preflight could mistake pending-deleted rows for absent rows, branch
restores could inspect the wrong branch, and mutable preview bytes could change
the values later encrypted. Plaintext-only transaction reads could also trigger
key-store enrolment.
Ordinary synchronous mutation handles continue to own encryption preparation,
new-scope recipients, and authoritative acceptance.

Now scope checks include pending-deleted rows and use the requested branch
coordinates; a missing upsert inserts rather than overwriting an unobserved
row. Preview values are isolated from caller buffers, and plaintext-only
transaction reads avoid key-store admission. Ordinary reads still decrypt
selected logical values; plaintext operations remain independent of key
readiness. Unsupported encrypted query/subscription and migration paths fail
closed until their owning layers.
