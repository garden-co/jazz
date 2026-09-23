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
row. Preview values are isolated from caller buffers. Applications with encrypted
tables prepare their device during database startup, rather than delaying the
snapshot of an exclusive transaction. Plaintext-only applications do not enrol
a device, and plaintext transaction reads do not repeat key-store setup.

An established device can reopen offline using retained keys and accepted local
history. Call `disconnect()` after opening to use local encrypted reads without
waiting for the server. Local readiness does not prove current server approval;
first-time enrolment still needs a connection.

Ordinary reads decrypt selected logical values. Unsupported encrypted query,
subscription and migration paths fail closed until their owning layers.

Native transaction waits no longer spin on queued inbound frames or accumulate
settlement callbacks while waiting for server acceptance.
