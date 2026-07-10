# Explicit-delete execution

Type: `wayfinder:grilling`
Status: open
Assignee: guido (claimed 2026-07-10)
Blocked by: [E — grant ledger](E-claim-ledger.md)

## Question

How does the explicit delete API (`jazz.files.delete(fileId)`) execute
durably?

Per [Descriptor persistence](A-descriptor-persistence.md) and
[Grant ledger](E-claim-ledger.md) (as amended by identity-bound ids):
deletion is an explicit sync-protocol request, authorized by comparing the
key's identity segment against the session (backend surface skips), and
executes as ONE idempotent DELETE against the bucket — no tombstone, no
ledger, no settle observation. Decide: whether the DELETE runs
synchronously in the request (timeout/answer semantics; SDK retry loop on
failure?) or lands in a small durable server-side retry record first;
idempotency (deleting an already-absent key succeeds); what the API
returns while CDN copies still exist; and operator observability for
failed deletes (a bucket that 403s forever).
