# Backend conditional-write support matrix

Type: `wayfinder:research`
Status: closed (2026-07-10)
Assignee: guido
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

## Resolution (2026-07-10)

Asset: [backend support matrix](../notes/backend-support-matrix.md) —
summary table + per-capability detail with primary-source citations
(vendor docs, release notes, MinIO source), unconfirmable claims marked
**unverified**.

Key facts for ticket B:

- **Conditional `CompleteMultipartUpload` is NOT uniform** (review
  critical #3 confirmed): solid on S3 (GA 2024-08-20) and MinIO
  (PR #19713, ≥ RELEASE.2024-05-27T19-17-46Z); ambiguous on R2 (the
  condition attaches at `CreateMultipartUpload`, a publish-time failure
  auto-aborts the upload, and the current compat matrix is silent);
  undocumented on Tigris. The belt cannot be mandated for MPUs — degrade
  per-backend, condition-at-Create for R2, or drop it for MPUs.
- **Conditional single-part `PutObject` (`If-None-Match: *`, 412) is
  portable across all four** — a single-PUT release path keeps the belt
  everywhere (MinIO wildcard ≥ RELEASE.2024-05-07T06-41-25Z).
- **Destination-guarded copy** exists only on S3 (plain `If-None-Match`,
  GA Oct 2025) and R2 (via `cf-copy-destination-if-none-match` extension
  headers — plain header not honored); MinIO/Tigris unverified.
  `x-amz-copy-source-if-*` conditions the SOURCE and is not a substitute.
  Copy caps at ~5 GB single-shot everywhere it's documented.
- **CopyObject default COPY directive preserves
  `Content-Type`/`Content-Disposition`/`Cache-Control`** per docs on
  S3/R2, per maintainers on MinIO, unverified on Tigris — re-sending
  metadata with `REPLACE` at release is the belt-and-braces portable
  mandate.
- **nosniff cannot be a store-level requirement**: only Tigris emits
  `X-Content-Type-Options: nosniff` natively (per-bucket Additional
  Headers). S3 needs CloudFront SecurityHeadersPolicy, R2 needs a custom
  domain + Managed Transform (not bare r2.dev), MinIO needs a reverse
  proxy. Word it as a deployment requirement on the public serving layer.
  Per-object `Content-Type`/`Content-Disposition`/`Cache-Control` CAN be
  required store-level everywhere.
- **Lifecycle**: prefix + days expiration portable on all four (Tigris
  multi-rule/prefix only since 2026-05, max 10 rules; R2 still has no tag
  filters — moot, R2 has no object tags at all; day granularity, UTC
  midnight rounding everywhere). **`AbortIncompleteMultipartUpload` is
  NOT portable**: S3/R2 yes; MinIO's ILM has an explicit unsupported
  FIXME in source (server-global 24h stale-upload purge instead); Tigris
  documents nothing — the spec can't assume lifecycle MPU cleanup and may
  need a `ListMultipartUploads`+`Abort` sweeper as portable fallback.
