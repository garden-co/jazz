# Explicit-delete execution

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [E — grant ledger](E-claim-ledger.md)

## Question

How does the explicit delete API (`jazz.files.delete(fileId)`) execute
durably?

Per [Descriptor persistence](A-descriptor-persistence.md) and
[Grant ledger](E-claim-ledger.md): deletion is an explicit sync-protocol
request, authorized by comparing the requester against the blinded
uploader metadata on the object (backend surface skips the check), and
executes directly against the bucket — DELETE the final key + PUT the
zero-byte tombstone. No ledger, no settle observation. Decide: whether the
two bucket ops run synchronously in the request (timeout/answer semantics;
what the client does on failure — retry loop in the SDK?) or land in a
small durable server-side retry record first; the op order and crash
semantics (tombstone-then-delete vs delete-then-tombstone — which partial
state is safer); idempotency (deleting an already-deleted/tombstoned id
succeeds); what the API returns while CDN copies still exist; and operator
observability for failed deletes (a bucket that 403s forever).
