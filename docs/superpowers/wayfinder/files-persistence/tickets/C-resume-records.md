# Upload-resume records

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [B — staging store](B-staging-store.md)

## Question

What is the shape and home of the durable upload-resume record, so an app
restart resumes an upload from the last completed part within the lease?

Contents to pin: grant identity, object key, lease expiry, multipart
UploadId (or reference to it), completed part ETags, single-PUT vs
multipart mode, and state-machine position
(`local → uploading → released → accepted | rejected`). Decide: keyed by
file id or `BatchId`; stored as a `__`-prefixed raw-table namespace vs new
`Storage` trait methods alongside the batch records
(`crates/jazz-tools/src/batch_fate.rs`); transactional coupling with the
staged body (ticket B) and with batch records; lifecycle — created at grant,
updated per part, deleted at acceptance/rejection/lease-expiry restart (PRD:
fresh file id after expiry — the old record must die).

Blocked by B because the record references the staged body and shares its
crash-consistency contract.
