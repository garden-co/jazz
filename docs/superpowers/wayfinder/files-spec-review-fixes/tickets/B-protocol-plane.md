# Protocol-plane decisions (grant, ids, auth, serving)

Type: `wayfinder:grilling`
Status: closed (2026-07-10)
Assignee: guido
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

User input (2026-07-10, ahead of the grilling):

- Explore declaring supported mime types on the schema (per file column,
  exact list or `image/*` patterns). Grant-time validation against the
  declared set replaces the free-string untrusted-`mime_type` semantic
  (same seam as class-set validation); a client can still misdeclare
  within the set, which pinned Content-Type + nosniff absorbs. Needs a
  stated semantic for descriptors copied into a column with a different
  declared set (enforcement is grant-time only).
- nosniff direction pre-decided: word it as an infra/deployment
  requirement per mode (serving endpoint emits it; CDN-straight-at-bucket
  requires CDN/proxy config — CloudFront SecurityHeadersPolicy, Cloudflare
  Managed Transform on custom domain, reverse proxy for MinIO, Tigris
  bucket Additional Headers), and the spec must not present nosniff as
  the primary XSS control — that's origin isolation, disposition policy,
  and pinned Content-Type.

- **Staging MPU cleanup (new from ticket A):** the matrix found
  `AbortIncompleteMultipartUpload` lifecycle is NOT portable (S3/R2 only;
  MinIO has a server-global 24h purge instead, Tigris documents nothing) —
  decide whether the spec drops the lifecycle assumption for abandoned
  multipart uploads and mandates a portable fallback (e.g. a
  `ListMultipartUploads`+`Abort` sweeper) or states per-backend behavior.

## Resolution (2026-07-10)

Grilled with the user; all seven decisions applied directly to the spec
pair in this commit (the edits are the deliverable, per the map's
plan-don't-do override).

1. **Grant names the destination column; server validates against the
   schema.** Grant = `(file id, size, mime_type, name, destination
column)`. The server validates `mime_type` against the column's
   declared type set (new schema feature: `s.file({ types: [...] })`,
   exact types + `type/*` patterns; no declared set = any type) and
   cross-checks the id's class segment against the column's `ttl`
   declaration. Stated semantic: enforcement is grant-time only — copies
   and hand-rolled descriptors escape it; the disposition policy
   protects serving. "Pure comparison" reworded to "pure computation."
2. **Serving hardening is two-tier.** Tier 1: per-object
   `Content-Type`/`Content-Disposition`/`Cache-Control` pinned at grant,
   emitted by the store (portable everywhere); disposition computed
   server-side from the fixed render-safe allowlist, `name` sanitized
   (CRLF/control stripped, RFC 6266); release copy re-sends headers with
   `REPLACE`. Tier 2: nosniff is a deployment requirement on the public
   object host in BOTH modes — bytes never transit the 302 endpoint —
   with the per-backend infra list from the support matrix. US 28/29
   contradiction dissolved.
3. **Id grammar pinned; identity segment is
   `UUIDv5(files-namespace, user_id)`.** Uniform, URL-safe, locally
   computable; fixes the external-JWT `sub` problem (arbitrary, often
   email-like strings never appear raw in URLs). Id = `/`-joined key
   suffix `[t{class}/]{identity-uuid}/{random-uuidv4}`; class grammar
   `^[a-z0-9]{1,15}$`; parse unambiguous (UUID never matches
   `^t[a-z0-9]+$`). "Well-formed" = grammar only; bogus classes pass
   shape check by design. `{app}` is not in the id — it comes from the
   per-app sync connection, which implicitly authorizes it.
4. **`filesUrl` client config** (JazzContext + `JAZZ_FILES_URL` env
   family), default `{serverUrl}/files`; `url()` = id + static config.
   TTL builder-vs-grant contradiction resolved PRD-ward: builder checks
   grammar only, membership checked at grant (slice-1 corrected).
5. **Identity & backend surface = existing mechanisms, named.** Session
   identity is `Session.user_id` from the existing sync auth; account-
   scoped (uploader = account, any device). Backend surface = the
   existing backend-secret mechanism; an impersonating backend acts in
   the impersonated user's namespace — "delete anything" applies only
   when connected as backend.
6. **Release = `(file id, UploadId?, part ETags?)`;** single-PUT path =
   HEAD + copy + delete (no completion). Belt: `If-None-Match:*`
   mandated on single PUT (portable), best-effort on multipart
   completion (S3/minio at Complete, R2 at Create, Tigris none) and on
   the release copy (S3/R2 only). "Immutability at the bucket" reworded
   to immutability by construction (namespaces + fresh randoms +
   HEAD-first). Three-write small-file cost stated.
7. **Incomplete-multipart cleanup is a per-backend deployment
   requirement** (S3/R2 lifecycle rule; minio stale-uploads purge raised
   to ≥ lease; Tigris external sweep or accepted accrual). Jazz stays
   sweep-free.

Assets: amended `docs/superpowers/specs/2026-07-09-files-spec.md` (new
US 59-60) and `2026-07-10-files-persistence-spec.md`; codebase facts
from the sync-auth scout are summarized in the resolution of this
grilling's transcript and cited inline in the specs.
