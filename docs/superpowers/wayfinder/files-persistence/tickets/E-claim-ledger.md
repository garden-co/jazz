# Core claim ledger

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

How does the core's permanent claim ledger persist, and how is
verify+claim+accept made one atomic step against the existing storage
semantics?

The PRD requires: grants registered at issuance (before presigned URLs are
returned), ids never grantable twice, claims consumed atomically with
acceptance, sweep marks-expired before deleting, idempotent claim outcomes
for retried releases. The server uses the same `Storage` trait (RocksDB
default). Decide: ledger entry schema (file id → granted | claimed |
expired, issuing identity, lease deadline, UploadId?); home (raw-table
namespace vs dedicated trait methods); how the claim write and the batch
fate/acceptance write commit atomically (the trait is a sync KV with lazy
transactions — is one storage transaction across both guaranteed?); the
growth story for a permanent, append-only ledger (compaction? never?
size math at realistic upload volumes); and what the deletion queue
(ticket G) reads from it.
