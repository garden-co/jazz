# Explicit-delete execution

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10)
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

## Resolution (2026-07-10)

**Synchronous attempt + client-persisted intent; the server stays
stateless.**

1. The delete request authorizes by identity-segment comparison (backend
   surface skips) and issues ONE idempotent DELETE against the bucket;
   an already-absent key answers success. The server persists nothing and
   has no queue — observability is metrics/logs on delete failures.
2. The SDK persists a **pending-delete intent** locally before/with the
   first attempt and retries with backoff **across restarts** until the
   origin confirms. Retryable failures (network, 5xx, timeouts) keep the
   intent; permanent denials (foreign identity, malformed id) reject the
   call and drop it. Repeated `delete(fileId)` calls dedupe onto one
   intent; confirmation clears it.
3. `jazz.files.delete()` returns a promise that resolves on origin
   confirmation and rejects on permanent denial; fire-and-forget is safe
   because the intent, not the promise, carries the retry obligation.
4. "Deleted" means gone at origin; CDN copies aging out stays a
   documented semantic, not a return state.
5. The intent record's shape and storage home are client-persistence
   details owned by the resume-records ticket (same store, same
   durability expectations).

Assets: PRD + explainer updated in the same commit.
