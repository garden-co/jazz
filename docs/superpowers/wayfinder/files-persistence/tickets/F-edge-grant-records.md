# Edge grant records

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [E — core claim ledger](E-claim-ledger.md)

## Question

Does the edge persist any grant state of its own, or is the core's ledger
the only durable home with the edge stateless?

The PRD has the edge: issuing grants (after registering them at the core),
initiating multipart uploads (owning the UploadId), serving part-URL
refreshes within the lease, completing multiparts at release, and running
the sweep. Decide: where the UploadId and presign-refresh state durably
live (in the core's ledger entry, in edge-local storage, or both); what an
edge restart means for in-flight grants (can any edge serve a refresh, or
only the issuer?); who drives the sweep scan (edge polling its own grants
vs core scanning the ledger and delegating deletes); and whether
edge-local persistence uses the same `Storage` backends as the server main
store or stays in memory by design.

Blocked by E because the ledger entry's contents decide what is left for
the edge to hold.
