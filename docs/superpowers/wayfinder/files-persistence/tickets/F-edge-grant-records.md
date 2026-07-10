# Edge grant records

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [E — core claim ledger](E-claim-ledger.md)

## Question

Does the edge persist any grant state of its own, or is the ledger
(ticket E) the only durable home with the edge stateless?

Per [Descriptor persistence](A-descriptor-persistence.md) the edge:
issues grants, initiates multipart uploads (owning the `UploadId`), serves
part-URL refreshes within the lease, and on release completes the
multipart, server-side-copies `pending/{app}/{id}` → `{app}/{id}`, and
marks the grant claimed. There is no sweep (bucket lifecycle TTL owns
cleanup). Decide: where the `UploadId` and presign-refresh state durably
live (in the ledger entry, in edge-local storage, or both); what an edge
restart means for in-flight grants (can any edge serve a refresh and
perform the release copy, or only the issuer?); crash-during-release
semantics (complete/copy/mark-claimed is three steps against two systems —
idempotent replay order); and whether edge-local persistence uses the same
`Storage` backends as the server main store or stays in memory by design.

Blocked by E because the ledger entry's contents decide what is left for
the edge to hold.
