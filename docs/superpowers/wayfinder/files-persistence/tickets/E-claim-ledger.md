# Core claim ledger

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

How does the grant ledger persist, and who owns it?

Per [Descriptor persistence](A-descriptor-persistence.md) the ledger is
small: file id → uploader identity + granted/claimed (+ object key),
permanent so an id is never grantable twice. It is consulted at grant
issuance (id never seen), at release (mark claimed; idempotent for
retries), and at delete (uploader check). There is no verify+claim+accept
coupling and no sweep. Decide: does the ledger live at the core (edges ask
it) or at the issuing edge with core replication; entry schema and home
(`__`-prefixed raw-table namespace vs dedicated `Storage` trait methods);
idempotency of mark-claimed under retried release; the growth story for a
permanent ledger (size math at realistic upload volumes; compaction
never?); and what the delete path (ticket G) reads from it.
