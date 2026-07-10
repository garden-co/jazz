# Explicit-delete execution

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [E — grant ledger](E-claim-ledger.md)

## Question

How does the explicit delete API (`jazz.files.delete(fileId)`) execute
durably?

Per [Descriptor persistence](A-descriptor-persistence.md): deletion is an
explicit sync-protocol request authorized for the uploader identity (from
the ledger) or the backend/admin surface — never inferred from cell death.
Decide: whether the DELETE against the bucket runs synchronously in the
request (with what timeout/answer semantics) or lands in a durable retried
queue first (entry schema and home); idempotency (delete of
already-deleted id succeeds); poison-entry handling (a bucket that 403s
forever); whether the ledger entry records the deleted state (and serves
as the bodyless-history marker); what the API returns while CDN copies
still exist; and operator observability (pending deletes, oldest age).

Blocked by E because auth and the deleted-state marker are ledger reads
and writes.
