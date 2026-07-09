# Files — Feature Spec

Date: 2026-07-09
Status: ready for implementation
Triage: ready-for-agent
Derived from: the approved files design (`2026-07-08-files-design.md`, sharpened
by grilling review). That design doc is the authority on rationale and rejected
alternatives, with two deliberate amendments in this spec:

1. **Private files are dropped.** Every file body is public by URL; there is
   no `published` column, no URL minting, and no signed URLs. Permissions
   gate metadata only.
2. **Files are a column type, not a file table.** This restores the shape the
   core vocabulary already records (a file is "a column type whose cell holds
   a descriptor of exactly one immutable body"); the design doc's file-table
   data model is superseded. Upload, verification, leases, serving, caching,
   and deletion mechanics carry over unchanged — only the addressing and the
   data model move from row-keyed to file-id-keyed.

Where the design doc and this spec disagree on those points, this spec wins.

## Problem Statement

We want first-class support for immutable files, that enables us to do cheap billing
for file storage and uploads.

Currently Jazz supports file storage only through large blobs, but that
comes with a few downsides:

- it's expensive for us, on both storage and compute
- URL addressing isn't built-in

Developers coming from classic Jazz expect a FileStream-grade experience — create a file offline, keep working, have it
upload in the background, read it back on any device — and additionally expect
files to behave like normal web resources: a URL that works in an `<img>` tag,
a `<video>` element, or a shared link, served cheaply from a CDN.

## Solution

Files become first-class in Jazz through a **file column**: a column type,
usable on any table, whose cell holds a **file descriptor** — an immutable
value naming exactly one body: client-minted **file id**, `name`,
`mime_type`, `size`. The cell updates like any column (swapping in a new
descriptor is how you "replace" a file, gated by the ordinary update
policy), but each descriptor-body pair is immutable forever. Because the
file lives _in_ the row that owns it — an avatar on the profile row, an
attachment on the message row — the **permission surface is exactly the host
table's row policies**, gating metadata sync, descriptor swaps, and deletion
the same way they gate every other column. They govern metadata only.

The **file body** — the bytes — lives on an S3-compatible object store,
keyed by file id, uploaded directly by the client under a server-issued,
time-limited **upload grant**, and served back via one HTTP **serving
endpoint** (`GET /files/{file-id}`) that redirects to the object store.
Creation is fully offline-capable: `fromBlob` stores the body in the device
file store and yields a descriptor to write into a cell; the transaction
that first writes a fresh descriptor commits locally at once and holds at
the outbox (without blocking later, independent writes) until the body is
confirmed uploaded (**release**); the accepting authority verifies the body
exists with the declared size before accepting (**body verification**), and
claims the grant — each grant is claimable exactly once. Consequence: **any
descriptor a reader can see has its bytes present on the object store** —
dangling file references are not a protocol state.

Every accepted file has a stable, unauthenticated, public URL. Bodies are
immutable and URLs never change meaning, so every response carries
long-lived immutable cache headers — trivially CDN-cacheable, cheap to
serve, cheap to bill. There are no private files, no signed URLs, and no
per-download policy checks: the value Jazz provides is the integrated
experience (files as values in your own rows, synced and permission-gated as
metadata) plus offline capabilities, not byte-level access control. Bytes
never transit Jazz nodes on the hot path and never enter Jazz storage, the
sync lane, or the content channel.

## User Stories

### Schema & data model

1. As an app developer, I want to declare a file column on any table through
   the public schema builder, so that a file lives in the row that owns it —
   an avatar on the profile, an attachment on the message — with no side
   table or foreign-key ceremony.
2. As an app developer, I want the descriptor to carry `name`, `mime_type`,
   and `size` (server-verified), so that I don't design file metadata myself
   and can render lists of files without touching bodies.
3. As an app developer, I want the descriptor to be an immutable value —
   replacing a file means swapping the whole cell to a newly uploaded
   descriptor — so that a body's identity can never be quietly rewritten
   under a reader, and every downstream cache can treat bodies as
   never-changing.
4. As an app developer, I want app-level metadata (captions, tags, display
   names that change) to be ordinary sibling columns on the same row, so
   that mutable naming and file identity never fight.
5. As an app developer, I want multiple file columns on one table when I
   need them (avatar and banner), so that cardinality is my schema's choice.
6. As an app developer, I want every accepted file to have a stable public
   URL derived from its file id, so that rendering a file is local string
   construction — no server round-trip, no async URL step.

### Creating & uploading

7. As an end user, I want to attach a file while offline and keep using the
   app, so that flaky connectivity never blocks my work.
8. As an end user, I want the file I just attached to be immediately
   readable on the device that created it, so that my own UI never shows my
   own file as missing.
9. As an end user, I want my other, independent writes to keep syncing while
   a large file is still uploading, so that one slow video doesn't stall my
   whole session.
10. As an end user, I want an interrupted upload to resume from the last
    completed part after an app restart, so that a 2 GB upload doesn't start
    over because I closed the laptop.
11. As an end user, I want to see upload progress and state
    (`local → uploading → released → accepted | rejected`) on the file
    handle, so that the app can show me what's pending and warn me before I
    abandon a device holding unreleased files.
12. As an app developer, I want `fromBlob`/`fromStream` to return a usable
    descriptor handle immediately while upload continues in the background,
    so that I can write it into a cell in the same breath and my UI code
    stays simple.
13. As an app developer, I want uploads to go directly from the client to
    the object store under a presigned grant, so that my server's bandwidth
    bill doesn't scale with upload traffic.
14. As an app developer, I want large bodies to upload via multipart with
    per-part progress persistence, so that big files are as reliable as
    small ones.
15. As an app developer, I want the transaction that writes a fresh
    descriptor to become visible to other subscribers only once the body is
    verified present, so that a reader who can see the descriptor can always
    fetch the bytes — no placeholder discipline required.
16. As an app developer, I want to _choose_ early visibility when I need it
    — by putting the file cell in its own row (an attachments row) so the
    referencing row syncs immediately — so that "message text now,
    attachment when uploaded" is my app's decision, not a forced protocol
    semantic.

### Reading & serving

17. As an end user, I want every file's URL to work in an `<img>` tag, a
    `<video>` element, or a pasted link with no auth, cookies, or headers,
    so that files behave like normal web resources.
18. As an end user, I want file bytes served through a CDN with long-lived
    immutable caching, so that media-heavy apps load fast.
19. As an app developer, I want it stated plainly that file bytes are
    readable by anyone holding the URL — the unguessable file id is the only
    barrier — so that I never mistake the row policies (which gate metadata)
    for byte confidentiality, and keep genuinely sensitive content out of
    files or encrypt it myself.
20. As an operator, I want downloads to be a redirect to the object store or
    CDN with zero policy evaluation and zero Jazz DB involvement, so that
    serving cost is flat and storage/egress is what I bill.
21. As an app developer, I want `toBlob`/`toStream` to read my own pinned
    bodies locally and otherwise fetch by URL, so that one read API covers
    the creating device and everyone else without a parallel byte store.
22. As an end user, I want files I created pinned locally at least until the
    writing transaction is accepted upstream, so that the only copy of my
    file can't be evicted before it's safe elsewhere — and so my own files
    read back offline.
23. As an app developer, I want downloaded bodies to ride the browser/OS
    HTTP cache (which the immutable cache headers make effective), so that
    repeat views are fast and often work offline without Jazz maintaining a
    second byte store it can't guarantee anyway.
24. As an app developer, I want a typed "body unavailable offline" error on
    an offline read that has no local body, so that I can render a sensible
    fallback.

### Permissions & integrity

25. As an app developer, I want the host table's row policies — read,
    update, delete — to be the only permission surface for files, gating
    metadata sync, descriptor swaps, and deletion exactly as on any column,
    so that there is nothing file-specific to learn and nothing that can
    silently disagree with row permissions.
26. As an app developer, I want each upload grant claimable exactly once —
    a descriptor is accepted only with its own unclaimed grant — so that one
    file id lives in exactly one cell and deletion stays well-defined
    without refcounting.
27. As an app developer, I want a transaction whose body is absent or whose
    size mismatches the descriptor to be rejected whole and surfaced on the
    write handle like any rejected transaction, so that integrity failures
    are loud and local state is cleaned up.
28. As an operator, I want unclaimed upload grants to expire as leases whose
    objects the issuing edge sweeps, so that an authorized identity farming
    grants accumulates nothing past the lease horizon.
29. As an operator, I want the lease window to be my knob trading
    abuse-window against resume-window, so that I can tune it per
    deployment.

### Deletion & history

30. As an end user, I want a file's body deleted when its cell dies —
    overwritten with a new descriptor, set to null, or the row deleted
    (all policy-gated, ordinary writes) — so that removing a file is one
    action with no second cleanup step.
31. As an operator, I want the core to be the single owner of object
    deletion — observing the cell death settle, appending to a durable
    queue, issuing idempotent retried DELETEs — so that there are no racing
    edge deletes and no orphaned responsibility.
32. As an app developer, I want historical reads and branches to surface a
    descriptor at a past cut even after its object is deleted, with body
    reads failing via the same typed missing-body error, so that bodyless
    history is a defined semantic rather than a crash.
33. As an end user, I want a deleted file's URL to stop serving bytes once
    the object is deleted (with CDN-cached copies aging out on their own),
    so that killing the cell is the one way to withdraw a file from the web.

### Operations & deployment

34. As an operator, I want the backend contract to be exactly the
    S3-compatible API (presigned single/multipart PUT, presigned GET, HEAD,
    DELETE), so that S3, R2, minio, and Tigris all work unchanged.
35. As an operator, I want edges and the core to hold the object-store
    credentials (edges verify, grant, and sweep; the core deletes), so that
    clients never see store credentials.
36. As an operator, I want a recommended S3 lifecycle rule expiring
    incomplete multipart uploads, so that half-finished uploads don't
    accumulate cost outside the lease sweep.
37. As a developer running tests or local dev, I want the file plane to run
    against minio or an in-process fake, so that no cloud account is needed
    to develop or CI-test file features.

## Implementation Decisions

- **The file plane is split between the sync protocol and one HTTP
  endpoint.** Grant requests and release confirmation are request/response
  message pairs on the client's already-authenticated sync connection — no
  second credential system; whatever admission bound the session to an
  identity authorizes file operations. The only HTTP surface is the serving
  endpoint `GET /files/{file-id}`, which is public and unauthenticated: it
  302-redirects to the object store by default and streams when the backend
  cannot presign. There is no URL-mint operation anywhere — the URL is a
  pure function of the file id, computable locally.
- **File is a column type; the cell holds a file descriptor.** The
  descriptor is an immutable value: client-minted opaque file id, `name`,
  `mime_type`, `size` (server-verified). The cell itself updates like any
  column — a swap to a newly uploaded descriptor is the "replace" operation,
  and setting it null is removal — but the fate authority rejects any write
  that mutates a descriptor in place (same file id, different fields).
  Mutable, queryable metadata belongs in ordinary sibling columns. Multiple
  file columns per table are allowed; each cell holds at most one
  descriptor.
- **Permissions are the host table's row policies, unchanged and
  fail-closed, and they govern metadata only.** Read policy gates descriptor
  sync (a descriptor is metadata). Update policy gates descriptor swaps and
  removal, like any column write. Delete policy gates row deletion. File
  bytes are not permission-gated: anyone holding the URL can fetch them, and
  the unguessable file id is the only barrier. This is a deliberate product
  decision — Jazz's value for files is the integrated relational experience
  and offline capability, not byte-level access control.
- **Object addressing is file-id-keyed:** `{app}/{file-id}`, never
  content-derived. No `hash` field, no dedup, no refcounting in v1 — bodies
  are single-writer and immutable, so a declared hash would protect only the
  uploader's own readers.
- **Each grant is claimable exactly once, and a descriptor is accepted only
  by claiming its grant.** The fate authority accepts a fresh descriptor
  only when an unclaimed grant for that file id (issued to the writing
  identity) exists, and acceptance consumes it. Copying a descriptor into a
  second cell therefore rejects — one file id, one live cell, ever — which
  is what makes unreference-triggered deletion sound without refcounting.
  Descriptor values are created only by the SDK's upload path.
- **Upload flow:** (1) create fully offline — `fromBlob` writes the body
  into the device file store, measures `size`, mints the file id, and
  returns a descriptor handle; writing it into a cell is an ordinary local
  transaction, immediately usable on the creating device; (2) that
  transaction holds at the outbox until release, while later independent
  commit units bypass it (writes causally depending on it queue behind it);
  (3) grant request `(file id, size)` over sync; the edge checks the write
  policy would plausibly admit the write, returns the object key, lease
  expiry, and presigned URLs (single PUT below a tens-of-MB implementation
  constant, multipart above); (4) the client PUTs directly to the store,
  persisting completed part ETags locally for restart-resume within the
  lease window; (5) release confirmation over sync frees the transaction
  into the ordinary lane; (6) at the acceptance gate the fate authority
  verifies via one HEAD that the object exists and its size matches the
  descriptor, and claims the grant — mismatch, absence, or a missing/spent
  grant rejects the whole commit unit.
- **Visible descriptor ⇒ bytes present.** Because the writing transaction is
  held until release and verified at acceptance, no subscriber ever sees a
  descriptor whose body is missing. Dangling file references are not a
  protocol state; apps that want a referencing row to sync before the upload
  finishes model the file cell in a separate row and accept an app-level
  pending state — their choice, not a forced semantic.
- **Grants are leases.** A grant unclaimed within its lease window (default
  on the order of days) expires; the issuing edge — which holds the grant
  record and credentials — deletes the uploaded object. This is the
  storage-abuse bound; there is no general GC.
- **Download hot path never touches the Jazz DB — no policy checks, no
  auth.** One case for every file: stable URL, unauthenticated, long-lived
  immutable cache headers (bodies are immutable and never change meaning),
  redirect to object store or CDN. `Content-Type` and download filename are
  set as object metadata at upload from the descriptor. Serving cost is
  flat; storage and egress are the billable dimensions.
- **The device file store is upload staging, not a download mirror.** It
  holds **pinned bodies**: bodies this device created, kept at least until
  the writing transaction is accepted upstream (they may be the only copy
  in existence), and readable locally — the creating device's files work
  offline by construction. There is no SDK-managed download cache in v1:
  the primary read path is the URL straight into `<img>`/`<video>`, whose
  bytes never pass through the SDK, so an SDK byte cache could not cover
  real usage — it would double-store bytes next to the browser's HTTP cache
  and still miss most reads. Downloaded bodies instead ride the browser/OS
  HTTP cache, which the immutable cache headers make effective; offline
  reads of downloaded files are opportunistic, not guaranteed. `toBlob`/
  `toStream` read pinned bodies locally and otherwise fetch by URL
  (benefiting from the HTTP cache); an offline read with no local body
  fails with the typed "body unavailable offline" error. Guaranteed offline
  availability of downloaded files (an explicit pin/prefetch API, a service
  worker story) is future work.
- **The core owns object deletion, triggered by cell death:** when a
  descriptor's cell is overwritten, nulled, or its row deleted — and the
  core observes that settle globally — it appends the file id to a durable
  deletion queue and issues idempotent, retried DELETEs. The metadata change
  is visible immediately; the object disappears eventually. The
  one-live-cell rule makes "unreferenced" exact. Bodyless history is
  first-class: descriptors remain readable at past cuts, body reads fail
  with the same typed error as any missing body.
- **Backend contract is one abstraction — the S3-compatible API** (presigned
  single PUT, multipart upload, presigned GET, HEAD, DELETE), covering S3,
  R2, minio, Tigris. Edges hold credentials for verify/grant/sweep; the core
  holds them for deletion. Dev and tests run minio or an in-process fake.
- **TS API re-backs the existing file-storage runtime shapes** onto the file
  plane: `fromBlob`/`fromStream` (create; returns a descriptor handle to
  write into cells, background upload), `toBlob`/`toStream` (pinned-local
  or URL-fetched reads, taking a descriptor or handle), `file.url()` (the stable public
  URL, computed synchronously and locally from the file id), and an
  observable upload state on the handle:
  `local → uploading(progress) → released → accepted | rejected`.
- **Vocabulary amendments to the Files section of the core context doc**
  (part of this work): the **File**, **file descriptor**, **file id**,
  **file body**, **file store**, **orphan**, **release**, and **serving
  endpoint** entries stand as written (the column shape is confirmed);
  **upload grant** pins size only (no hash) and is a sweepable lease;
  **body verification** is presence + size match (no hash); the descriptor
  loses its `content hash` and `visibility` fields; the **publish** and
  **capability URL** entries are removed — every file body is world-readable
  by URL, and row policies gate only the metadata.

## Testing Decisions

- **Good tests here are black-box integration tests through public APIs
  only**: schema via the public schema builder, permissions via the public
  policy builders, effects asserted through queries, subscription deltas,
  and accepted/rejected write settlement — never through internal state or
  JSON-like definitions. The Rust testing guidelines in the jazz-tools crate
  are binding.
- **Two existing seams plus exactly one new seam** (confirmed with the
  developer):
  - Rust: jazz-tools-style integration tests (a `JazzServer` with
    `TestingClient`s, or `test_client` where one runtime suffices) covering
    grant/release/acceptance, descriptor immutability enforcement (in-place
    mutation rejected; swap accepted under update policy), single-claim
    grants (descriptor copy rejected), body verification failures (absence,
    size mismatch), lease-expiry sweep, and cell-death-triggered core-owned
    deletion.
  - TS: tests through the public file-storage surface against a
    really-served endpoint, in the style of the existing client/db
    integration tests in the TS SDK.
  - The single new seam: the S3-compatible object-store backend contract,
    with an in-process fake under the file plane in tests and minio as an
    optional real target.
- **Explicit scenarios to cover:** resume-after-restart within the lease;
  restart after lease expiry (re-grant and re-upload); the serving endpoint
  returning bytes with no auth for an accepted file, including one whose
  host row the fetching identity's read policy would hide (deliberately
  asserting the public-bytes semantic); an independent transaction bypassing
  a held file-writing transaction, and a dependent one queuing behind it;
  offline create → later release; offline read of an own pinned body, and
  the typed error on an offline read with no local body; descriptor swap uploading the new body and deleting the
  old one; bodyless historical read after object deletion; the URL 404ing
  after cell death once the object is gone.
- **Prior art:** the existing jazz-tools integration suites for permissions,
  claims, client restart, and large-blob permissions are the closest
  templates on the Rust side; the client/db `.test.ts` suites in the TS SDK
  runtime are the template on the TS side.

## Out of Scope

- General orphan GC beyond the grant-lease sweep and the recommended S3
  lifecycle rule for incomplete multipart uploads.
- Content hashing, content-hash dedup, and refcounting (no `hash` field;
  apps wanting tamper-evidence add their own metadata column).
- Moving or copying a descriptor between cells (needs refcounting or a
  transfer protocol; v1 is one file id, one live cell — re-upload if you
  need the same content twice).
- Lists of files in one cell (`list(file)` columns); model one file column
  per cell, or rows in a side table, in v1.
- An SDK-managed download cache, and any guarantee that downloaded files
  are readable offline. The primary read path (URL into `<img>`/`<video>`)
  bypasses the SDK, so it cannot honestly make that guarantee; downloads
  ride the browser/OS HTTP cache opportunistically. Guaranteed offline
  media — an explicit pin/prefetch API, possibly a service-worker
  integration — is future work.
- Per-identity storage quotas (grant leases bound abuse; accounting is
  future work).
- A standalone file service (second deployable, inter-service tokens,
  duplicated policy evaluation) — deferred until serving traffic warrants
  it.
- Private files and any byte-level access control (signed URLs, capability
  URLs, per-download policy checks). Every body is public by URL in v1; if
  byte confidentiality is ever wanted it can be layered on later without
  changing the public URL scheme. Apps with sensitive content keep it out
  of files or encrypt client-side.
- Upload proxying through the server, body transport over the content
  channel, HTTP grant endpoints, content-addressed keys, and strict-FIFO
  outbox holds — all considered and rejected in the design doc.

## Further Notes

- **Stated, accepted semantics** implementers must not "fix": file bytes
  readable by anyone holding the URL regardless of row policies, with the
  unguessable file id as the only barrier; sibling columns written in the
  same transaction as a fresh descriptor becoming visible only at
  acceptance (early visibility is modeled by the app, not the protocol);
  CDN-cached copies of a deleted file's bytes persisting until cache
  eviction (immutable caching makes purge best-effort at most); permanent
  local-first data loss if the creating device dies before release (the
  handle's upload state is the app's warning surface); a manually deleted
  object after acceptance serving 404/410 (operator error, not a protocol
  state).
- The single-PUT vs multipart size threshold is an implementation constant
  on the order of tens of MB, not configuration.
- The lease window default is on the order of days and is operator-facing.
- The design doc's "Rejected alternatives" section is required reading
  before proposing any deviation from the shapes above — with the caveats
  that this spec's removal of private files supersedes the published/private
  split, and the file-column model supersedes the file-table data model
  (returning to the shape the core vocabulary records).
