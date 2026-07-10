# Backend conditional-write support matrix

Type: `wayfinder:research`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

What do S3, R2, minio, and Tigris actually support, today, for the
S3-compatible operations the file plane leans on? Produce a support-matrix
note (linked asset) covering:

- `If-None-Match: *` conditional writes on plain PUT and on
  `CompleteMultipartUpload` (the self-collision belt; the review flags
  conditional completion as unevenly supported — verify per backend, with
  doc citations and version caveats for minio).
- `CopyObject` metadata behavior: COPY vs REPLACE directives, whether
  pinned `Content-Type`/`Content-Disposition`/`Cache-Control` survive the
  release copy by default, and whether a conditional (`If-None-Match`)
  copy exists anywhere.
- Which response headers a public-read bucket can be made to emit
  natively (per-object metadata vs bucket config) — specifically whether
  `X-Content-Type-Options: nosniff` can come from the store itself in the
  CDN-straight-at-bucket mode, per backend and for common CDNs in front
  (CloudFront, Cloudflare).
- Prefix-scoped lifecycle expiry + incomplete-multipart abort parity
  (believed portable; confirm, note R2's no-tag-filter constraint still
  holds).

Output feeds [B — protocol-plane decisions](B-protocol-plane.md): the
belt's fate (mandate, per-backend degrade, or drop) and the nosniff
deployment requirement wording.
