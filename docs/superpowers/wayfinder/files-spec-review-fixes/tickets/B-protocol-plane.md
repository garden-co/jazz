# Protocol-plane decisions (grant, ids, auth, serving)

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [A — backend support matrix](A-backend-support-matrix.md)

## Question

One grilling over the server/protocol half of the review findings
([consolidated](../notes/review-consolidated.md), criticals 1–4 + related
majors/minors). Decide and amend the spec pair:

- **Grant payload & serving hardening:** grant message fields
  (`mime_type`, `name` — needed to pin `Content-Type`/`Content-Disposition`
  at presign); reconcile US 28 (zero-lookup 302) vs US 29 (disposition
  policy); how nosniff/disposition are guaranteed per deployment mode
  (serving endpoint vs CDN-straight-at-bucket), using ticket A's facts;
  mandate CopyObject metadata preservation at release; `name` sanitization
  for the disposition header; state the untrusted-`mime_type` semantic.
- **File id grammar & URL derivation:** concrete id encoding (delimiters,
  charset constraints on identity ids and class names, classed-vs-classless
  disambiguation); what write-path "well-formed" validation checks
  (including whether bogus class segments pass); where `{app}` comes from
  and whether the grant authorizes it; how `url()` learns its base host
  (deployment config) — squaring "pure local string construction" with
  reality; resolve the builder-vs-grant `ttl` validation contradiction
  between the docs.
- **Identity & backend-surface authentication:** how a sync session binds
  to an identity id (look up the existing sync-auth facts in the codebase
  first — this may be mostly "name the existing mechanism"); whether
  identity is account- or device-scoped for grants/deletes; how the
  privileged backend surface is recognized.
- **Release message & belt:** add the file id to the PRD's release shape
  (slice-1 already has it); specify the single-PUT release path (no
  UploadId/parts); decide the conditional-completion belt's fate per
  ticket A's matrix; state whether the release copy is guarded or the
  "immutability at the bucket" claim is reworded to name SDK fresh-randoms
  as the mechanism.
- **Staging MPU cleanup (new from ticket A):** the matrix found
  `AbortIncompleteMultipartUpload` lifecycle is NOT portable (S3/R2 only;
  MinIO has a server-global 24h purge instead, Tigris documents nothing) —
  decide whether the spec drops the lifecycle assumption for abandoned
  multipart uploads and mandates a portable fallback (e.g. a
  `ListMultipartUploads`+`Abort` sweeper) or states per-backend behavior.
