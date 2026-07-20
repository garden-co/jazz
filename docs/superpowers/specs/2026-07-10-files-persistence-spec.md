# Files Persistence — Slice 1: File Column & Stateless File Plane

Date: 2026-07-10
Status: ready for implementation
Triage: ready-for-agent
Scope: the resolved half of the files-persistence wayfinder map
(`docs/superpowers/wayfinder/files-persistence/map.md`) — the
[Descriptor persistence](../wayfinder/files-persistence/tickets/A-descriptor-persistence.md)
and [Grant ledger](../wayfinder/files-persistence/tickets/E-claim-ledger.md)
resolutions. Updated for the 2026-07-10 invisible-core pivot and the MVP
delete cut: core keeps zero durable client-side file-plane state (no
staging store, no resume records, no durable outbox hold, no delete
intent — see the PRD's amendment 9). The wayfinder map is complete; this
spec plus the amended PRD are its destination deliverable.
Feature-level authority: the files PRD
(`2026-07-09-files-spec.md`).

## Problem Statement

The files feature is fully designed, but nothing of it exists in the
codebase: there is no file column type in the schema system, no descriptor
value that can persist through the row stack, and no file-plane protocol a
client could talk to. Implementers need the durable half pinned — how a
file descriptor exists from schema declaration down to storage and across
the bindings, and how the server-side file plane works — precisely enough
to build without inventing protocol details.

## Solution

Two deliverables, buildable together and testable end to end:

1. **The file column.** `s.file()` / `s.file({ ttl: "7d" })` in the public
   schema builder declares a file column. At the core it is a new
   schema-level column kind that **lowers onto the existing text value
   type** — the exact facade pattern JSON columns use — carrying the
   descriptor as canonical JSON (`v:1` + `id`, `name`, `mime_type`,
   `size`). Shape is strictly validated on write; nothing else is
   enforced (no immutability, no verification — per the PRD). Because no
   new value variant exists, the row format, storage engines, and all
   bindings carry descriptors with zero changes.
2. **The stateless file plane.** Sync-protocol request/response messages —
   grant, part-URL refresh, release, delete — implemented at the server
   against the S3-compatible backend contract. File ids are
   identity-bound — the key is `{app}[/t{class}]/{identity}/{random}`,
   where `{identity}` is `UUIDv5(JAZZ_FILES_NAMESPACE, user_id)` — so every
   authorization is a pure computation (identity-segment derivation plus
   class-set, column-declaration, and mime-type checks): the server keeps
   no file-plane state and reads nothing from the bucket at issuance.
   Unclaimed uploads expire via the `pending/` prefix lifecycle rule;
   released bodies are copied to their final, immutably-cacheable key.

## User Stories

### Schema & column type

1. As an app developer, I want to declare `avatar: s.file()` on any table
   through the public schema builder, so that files are columns like any
   other, with no side tables or new entities.
2. As an app developer, I want to declare `attachment: s.file({ ttl:
"7d" })`, so that a column's files expire by policy declared once in
   the schema, not per call site.
3. As an app developer, I want the file column's wire form to travel like
   JSON columns do (a DDL string carrying the class), so that schema sync,
   hashing, and catalogue machinery need nothing new.
4. As an app developer, I want a descriptor write that is not
   shape-valid canonical JSON (`v:1`, exactly `id`, `name`, `mime_type`,
   `size`, sorted keys, compact) to be rejected at write time like a
   JSON-schema violation, so that garbage never lands in a file cell.
5. As an app developer, I want readers to parse descriptors leniently
   (unknown future fields/versions tolerated; `url()` needs only the id),
   so that older clients keep working when the descriptor evolves.
6. As an app developer, I want in-place descriptor edits, copies into
   other cells, and hand-rolled descriptors to be ordinary policy-gated
   writes with no file-specific enforcement, so that file cells behave
   exactly like the data they are.
7. As an app developer, I want file cells to be opaque to the query layer
   (whole-value equality and null checks only), so that queryable
   metadata lives in sibling columns and the query engine needs no
   file-specific work in v1.
8. As an app developer, I want historical and branch reads to decode
   descriptors at any past cut like ordinary text, so that history needs
   no file-specific machinery (the URL may 404; that is normal).
9. As an SDK user on any platform, I want descriptors to cross the
   WASM/NAPI/RN boundaries as the text values they are, so that no
   binding gains a new value variant.

### Ids, minting & URLs

10. As an app developer, I want the SDK to mint the file id synchronously
    when I call `fromBlob` with the destination column — TTL class from
    that column's declaration,
    the identity segment derived (UUIDv5) from the session identity,
    random from a CSPRNG — so that ids are
    correct by construction, belong to the file rather than a row, and I
    never assemble one by hand.
11. As an end user, I want id minting to work fully offline from the
    first moment (my identity id is always known locally), so that
    offline creation has no first-contact caveat.
12. As an app developer, I want `file.url()` to be pure local string
    construction from the id and static client config — `filesUrl`
    (default `{serverUrl}/files`) plus the app id — with no schema
    lookup and no server call,
    so that rendering is synchronous everywhere.
13. As an app developer, I want a descriptor copied into a
    differently-declared column to keep its baked-in class, so that
    expiry is fixed at upload and never mutated by placement.
14. As an app developer, I want it stated that every file URL publicly
    carries a stable pseudonymous UUIDv5 derivation of the uploader's
    identity id (cross-file linkable, never the raw id), so that I
    don't put files where that linkage is
    unacceptable.

### The file plane

15. As an end user, I want an upload grant issued after nothing more than
    pure computation — identity-segment derivation, class-set and
    column-declaration checks, and a mime-type check against the
    destination column's declared set — zero bucket
    reads, zero records written — so that grants are fast and the server
    stays stateless.
16. As an attacker-shaped user, I want a grant request for a key outside
    my identity namespace refused by comparison alone, so that taking
    over another uploader's URL is impossible by construction.
17. As an app developer, I want the presigned single PUT to carry
    `If-None-Match: *` (multipart completion too, where the backend
    supports it) and pinned
    `Content-Type`/`Content-Disposition`/`Cache-Control` headers, so that
    my own retries can't clobber and clients can't smuggle headers.
18. As an app developer, I want the grant response to hand me the
    multipart `UploadId` and lease expiry, so that my session owns the
    upload state (in memory, per the invisible-core amendment) and any
    edge can later serve me statelessly.
19. As an app developer, I want to request fresh presigned part URLs for
    an existing upload within its lease from any edge, so that presign
    windows never strand a long upload.
20. As an app developer, I want release to be HEAD-final-first (already
    there → success), then complete (multipart only) + copy `pending/…`
    → final (re-sending pinned headers with `REPLACE`) + delete
    pending, so that retried and concurrent releases converge
    idempotently from any edge.
21. As an operator, I want unreleased uploads cleaned by the `pending/`
    prefix lifecycle rule, with incomplete multiparts reaped by my
    backend's own mechanism (lifecycle rule on S3/R2, stale-uploads
    purge raised to ≥ the lease window on minio, external scheduled
    sweep on Tigris),
    so that abandoned uploads cost nothing past the lease window with no
    sweep code in Jazz anywhere.
22. As an end user, I want files in a TTL'd column to expire when the
    bucket's class rule fires (clock starting at the release copy), so
    that ephemeral content leaves by itself; the descriptor remains and
    its URL 404s.
23. As an end user, I want to delete my own file with one call authorized
    by the same identity-segment comparison, executed as one idempotent
    bucket DELETE, so that removal is mine and simple; the backend
    surface can delete anything.
24. As an operator, I want permanent files served with immutable cache
    headers and TTL'd files with `max-age` capped to their class, so that
    CDNs cache correctly for both.
25. As an operator, I want the serving path to keep mirroring the object
    key exactly (now with class and identity segments), so that downloads
    stay a zero-lookup redirect.

## Implementation Decisions

- **Column kind, not value kind.** A new schema-level column kind for
  files joins the existing column-type enum; its runtime value is the
  existing text value type. Encode/decode routes through the text path
  exactly as JSON columns do; no new value variant, no row-format change,
  no storage-format version bump, no WASM/NAPI/RN value plumbing.
- **Validation lives in the existing column-kind validation layer** (the
  same place JSON columns parse and JSON-Schema-check): parse the cell as
  canonical JSON, require `v:1` and exactly the four fields with correct
  types, **and require the `id` field to match the file-id grammar**
  (`[t{class}/]{identity-uuid}/{random-uuidv4}`, class `^[a-z0-9]{1,15}$`)
  — reject otherwise. The grammar check is load-bearing: it guarantees the
  identity segment is parseable for authorization. Class-set membership is
  checked at grant time, not here. No previous-value comparison, no grant
  awareness — shape only.
- **Schema builder & wire:** the TS builder gains the file column factory
  with an optional `ttl` option (validated for grammar `^[a-z0-9]{1,15}$`
  only — set membership is checked at grant time, resolving the earlier
  contradiction with the PRD) and an optional declared mime-type set
  (exact types and `type/*` patterns); the DDL wire form carries the
  class and the type set (the JSON-column precedent for
  parameterized kinds). The Rust schema parser gains the matching kind.
- **Id format:** one opaque string — the `/`-joined key suffix
  `[t{class}/]{identity}/{random}` — where `{identity}` is
  `UUIDv5(JAZZ_FILES_NAMESPACE, user_id)` and `{random}` a canonical CSPRNG
  UUIDv4; class names match `^[a-z0-9]{1,15}$`, so parsing is
  unambiguous (a UUID never matches `^t[a-z0-9]+$`; two segments =
  classless, three = classed). Write-path "well-formed" means this
  grammar and nothing more. The object key
  and serving path are `{app}[/t{class}]/{identity}/{random}`, with
  `{app}` supplied by the per-app sync connection, never by the id. The
  SDK
  finalizes the id synchronously inside `fromBlob`, from the destination
  column passed to it (`fromBlob(blob, { for })`) — the id is the file's
  own, not a row's, and exists before any cell write; `url()` derives
  from the id plus static client config — `filesUrl` and the app id —
  and is valid the instant the handle returns (always the public URL —
  the `canonical` option was
  retired with the invisible-core amendment).
- **File-plane protocol messages** on the authenticated sync connection:
  grant `(file id, size, mime_type, name, column)` → object
  key, lease expiry, presigned URL(s),
  `UploadId` where multipart (`column` is the handle's `for`, referenced
  by its stable schema-level column id, not its display name); part-URL
  refresh `(file id, UploadId,
part numbers)`; release `(file id, UploadId?, part ETags?)` — both
  optional fields absent on the single-PUT path; delete
  `(file id)`. Every one authorizes by comparing the id's identity
  segment to the UUIDv5 derivation of the session identity
  (`Session.user_id` as the existing sync auth establishes it; the
  backend-secret surface bypasses); grant
  additionally checks the class segment against the deployment class set
  and the named column's `ttl` declaration, and the `mime_type` (a
  well-formed RFC 9110 media type) against
  that column's declared type set (no declared set = any type). A column
  re-declared between the id's mint and the grant is validated against
  the current schema and may refuse (→ handle `failed`; no schema version
  is pinned into the grant).
  Nothing is persisted server-side; nothing is read from the bucket at
  issuance (bar the multipart path's single `CreateMultipartUpload`).
- **Bucket layout and rules:** uploads land at
  `pending/{app}[/t{class}]/{identity}/{random}` under conditional
  writes; release copies to the final key (starting the TTL clock,
  re-sending the pinned headers with the `REPLACE` metadata directive) —
  a single `CopyObject` below the ~5 GB single-copy cap, an
  `UploadPartCopy` multipart copy above it, with the destination guard
  applied only where the backend supports it (S3 plain `If-None-Match`,
  R2 `cf-copy-destination-if-none-match`), else unconditional — and
  deletes the pending object (best-effort; the `pending/` lifecycle rule
  is the backstop); lifecycle rules = one expiry per TTL class
  prefix, one expiry on `pending/`; incomplete-multipart cleanup is a
  per-backend deployment requirement (see the PRD's operator section),
  not a portable rule. A release with no completed upload finds no pending
  source and is an idempotent no-op, not an error. The
  serving endpoint 302s to the public object URL; bucket policy is
  anonymous GetObject with listing denied.
- **Backend contract additions** to the S3-compatible abstraction:
  conditional single PUT, multipart completion (conditional only where
  the backend supports it — best-effort hardening, not contract; see
  the support matrix in the spec-review-fixes map notes), server-side
  copy — `CopyObject` with metadata `REPLACE` below the single-copy cap,
  `UploadPartCopy` multipart copy (pinned headers on the destination
  `CreateMultipartUpload`) above it — presigned part URLs for an existing
  `UploadId`, HEAD, DELETE —
  implemented by the real backend and the in-process fake alike.
- **Delete execution (decided after this spec's first cut, amended by the
  MVP delete cut):** the server side is synchronous and stateless — one
  idempotent DELETE in-request. Client side, `delete()` returns a Promise
  resolving on origin confirmation and rejecting on failure; there is no
  durable intent record and no SDK retry machinery — the caller re-calls
  if it needs the guarantee, which idempotence makes always safe. Both
  halves are in this slice.
- **Nothing is deferred to open map tickets** — the map is complete.
  Everything it once deferred here was resolved by the 2026-07-10
  invisible-core pivot and the MVP delete cut: no staged bodies, no
  resume records, an in-memory-only outbox hold, and no delete intent
  (PRD amendment 9). This slice may stub the client upload driver to the
  point where protocol and column behavior are fully testable.

## Testing Decisions

- **Black-box integration tests through public APIs only** — schema via
  the public builders, permissions via the public policy API, effects
  asserted through queries, subscription deltas, and write settlement.
  The Rust testing guidelines in the jazz-tools crate are binding; no
  JSON-like definitions anywhere.
- **Seams (confirmed):** the two existing surfaces — Rust jazz-tools
  integration tests (`JazzServer` + `TestingClient`s or `test_client`)
  and the TS runtime `.test.ts` style — plus the single new seam: the
  S3-compatible backend contract with the in-process fake (minio
  optional).
- **Scenarios:** file column declared and synced in a schema (with and
  without `ttl`); shape-valid descriptor accepted, malformed rejected;
  in-place edit and cross-cell copy accepted as plain writes; copy into a
  differently-declared column keeps its class; descriptor readable at a
  historical cut; single-PUT grant issued with zero bucket calls for own
  namespace (multipart grant with exactly one, `CreateMultipartUpload`);
  foreign-namespace grant and delete refused; out-of-set class refused at
  grant; mime type outside the destination column's declared set refused
  at grant; id class contradicting the column's declaration refused at
  grant; conditional PUT 412s a same-key retry race; part-URL refresh
  within lease from a second edge; release idempotency (double release,
  release-after-crash converge via HEAD); the single-PUT release path
  (no `UploadId`: HEAD + copy + delete); pending object expiry via the
  fake's lifecycle simulation; TTL'd final object expiry with the
  descriptor cell intact; delete by uploader succeeds / stranger denied /
  backend succeeds; permanent vs class-capped cache headers on serving;
  grant-time serving hardening — `Content-Disposition: attachment` pinned
  for a non-render-safe type (e.g. `text/html` is never served inline)
  and `Content-Type` pinned from the validated `mime_type`; the served
  endpoint returning bytes with no auth for a released file, including one
  whose host row the fetching identity's read policy would hide
  (asserting the public-bytes-despite-hidden-row semantic).
- **Prior art:** the JSON-column validation behavior is the direct
  template for the column kind; the permissions/claims/client-restart
  integration suites for the Rust protocol tests; the runtime db/client
  `.test.ts` suites for the TS surface.

## Out of Scope

- A durable pending-delete intent record (cut from the MVP; the deferred
  design is preserved in the map's pending-delete-intent ticket).
- The opt-in offline package (2026-07-10 invisible-core pivot): durable
  staging, upload resume, the web SW, the RN loopback server, the
  read-through cache, and the core hook surface — see the PRD's
  amendment 9 and the map's design-inventory note.
- Everything the files PRD already rules out: private files, hashing and
  dedup, per-call TTL, TTL extension, rate limits/quotas, a standalone
  file service, URL blinding.

## Further Notes

- Decisions in this spec are recorded canonically in the wayfinder
  tickets named in the header; the files PRD and the human-first
  explainer describe the same design at feature level. If this spec and
  the PRD ever disagree, the PRD wins and this spec should be corrected.
- The descriptor's canonical bytes matter (equality/digests): compact,
  sorted keys, no extra whitespace — treat canonicalization as part of
  the write-path validation, not a client courtesy.
- The identity segment is `UUIDv5(JAZZ_FILES_NAMESPACE, user_id)` — one
  uniform, URL-safe derivation covering both self-signed identities
  (already UUIDs) and external-JWT `sub` strings (arbitrary,
  app-controlled, sometimes email-like — never embeddable raw in a
  public URL). This is a public deterministic derivation, not the
  deferred HMAC blinding recorded in the PRD. `JAZZ_FILES_NAMESPACE` is a
  fixed protocol constant (`UUIDv5(DNS, "files.jazz.tools")`, frozen as a
  literal) shared verbatim by the client (which mints the segment) and
  the server (which recomputes it to authorize) — not per-deployment.
