# Core deletion queue

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [E — core claim ledger](E-claim-ledger.md)

## Question

How does the core's durable deletion queue persist and drain?

The PRD requires: cell death (overwrite, null, row delete) observed at
settle enqueues the file id; DELETEs are idempotent and retried; the
one-live-cell rule makes "unreferenced" exact; bodyless history keeps
descriptors readable after the object is gone. Decide: queue entry schema
and home (raw-table namespace vs trait methods); how enqueue commits
atomically with the settling write; retry/backoff state and poison-entry
handling (an object store that 403s forever); dedup against double
enqueue (idempotency key = file id?); interaction with ledger states from
ticket E (claimed → deleted — does the ledger entry record deletion, and is
that the bodyless-history marker?); and observability (queue depth,
oldest-entry age) for operators.

Blocked by E because enqueue/drain transitions are ledger-state
transitions.
