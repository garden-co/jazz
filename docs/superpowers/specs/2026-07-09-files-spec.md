# Files — Feature Spec

Date: 2026-07-09
Status: ready for implementation
Triage: ready-for-agent
Derived from: the approved files design (`2026-07-08-files-design.md`, sharpened
by grilling review). That design doc is the authority on rationale and rejected
alternatives; this spec is the implementation-facing PRD. This spec amends the
design in one deliberate way: **private files are dropped**. Every file body is
public by URL; there is no `published` column, no URL minting, and no signed
URLs. Row policies on the file table govern metadata only. Where the design doc
and this spec disagree on that point, this spec wins.

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

Files become first-class in Jazz through a **file plane** that keeps bodies
out of the database entirely. A **file table** is an ordinary table declared
as a file table through the public schema builder; its rows carry metadata
(built-in `name`, `mime_type`, `size` columns plus any app columns) and sync
like any rows, governed by the file table's row policies — that is the entire
permission surface, and it governs metadata only. The **file body** — the
bytes — lives on an S3-compatible object store, uploaded directly by the
client under a server-issued, time-limited **upload grant**, and served back
via one HTTP **serving endpoint** (`GET /files/{table}/{row}`) that redirects
to the object store. Creation is fully offline-capable: the row commits
locally at once, the commit unit holds at the outbox (without blocking later
writes) until the body is confirmed uploaded (**release**), and the accepting
authority verifies the body exists with the declared size before accepting
the row (**body verification**).

Every accepted file has a stable, unauthenticated, public URL. Bodies are
immutable and URLs never change meaning, so every response carries long-lived
immutable cache headers — trivially CDN-cacheable, cheap to serve, cheap to
bill. There are no private files, no signed URLs, and no per-download policy
checks: the value Jazz provides is the integrated experience (files as
ordinary relational rows, synced and permission-gated as metadata) plus
offline capabilities, not byte-level access control. Bytes never transit Jazz
nodes on the hot path and never enter Jazz storage, the sync lane, or the
content channel.

## User Stories

### Schema & data model

1. As an app developer, I want to declare a file table through the public
   schema builder, so that files are ordinary rows I can query, subscribe to,
   and relate to other tables.
2. As an app developer, I want the file table to come with built-in `name`,
   `mime_type`, and `size` columns, so that I don't have to design file
   metadata myself.
3. As an app developer, I want to add my own metadata columns to a file table
   (captions, tags, checksums), so that app-specific data rides on the same
   row with ordinary column behavior.
4. As an app developer, I want `mime_type` and `size` to be frozen after the
   row exists, so that a file's identity can't be quietly rewritten after
   readers have seen it.
5. As an app developer, I want `name` to stay freely updatable, so that users
   can rename files without touching the body.
6. As an app developer, I want every accepted file to have a stable public
   URL derived from its row identity, so that rendering a file is local
   string construction — no server round-trip, no async URL step.

### Creating & uploading

7. As an end user, I want to attach a file while offline and keep using the
   app, so that flaky connectivity never blocks my work.
8. As an end user, I want the file I just attached to be immediately readable
   on the device that created it, so that my own UI never shows my own file
   as missing.
9. As an end user, I want my other writes to keep syncing while a large file
   is still uploading, so that one slow video doesn't stall my whole session.
10. As an end user, I want an interrupted upload to resume from the last
    completed part after an app restart, so that a 2 GB upload doesn't start
    over because I closed the laptop.
11. As an end user, I want to see upload progress and state
    (`local → uploading → released → accepted | rejected`) on the file
    handle, so that the app can show me what's pending and warn me before I
    abandon a device holding unreleased files.
12. As an app developer, I want `fromBlob`/`fromStream` to return a usable
    handle immediately while upload continues in the background, so that my
    UI code stays simple.
13. As an app developer, I want uploads to go directly from the client to the
    object store under a presigned grant, so that my server's bandwidth bill
    doesn't scale with upload traffic.
14. As an app developer, I want large bodies to upload via multipart with
    per-part progress persistence, so that big files are as reliable as small
    ones.

### Reading & serving

15. As an end user, I want every file's URL to work in an `<img>` tag, a
    `<video>` element, or a pasted link with no auth, cookies, or headers, so
    that files behave like normal web resources.
16. As an end user, I want file bytes served through a CDN with long-lived
    immutable caching, so that media-heavy apps load fast.
17. As an app developer, I want it stated plainly that file bytes are
    readable by anyone holding the URL — the unguessable row id is the only
    barrier — so that I never mistake the row policies (which gate metadata)
    for byte confidentiality, and keep genuinely sensitive content out of
    files or encrypt it myself.
18. As an operator, I want downloads to be a redirect to the object store or
    CDN with zero policy evaluation and zero Jazz DB involvement, so that
    serving cost is flat and storage/egress is what I bill.
19. As an app developer, I want `toBlob`/`toStream` reads to go through a
    device cache before the network, so that any file opened once is
    readable offline.
20. As an end user, I want files I created pinned locally at least until the
    row is accepted upstream, so that the only copy of my file can't be
    evicted before it's safe elsewhere.
21. As an app developer, I want the device cache LRU-evicted under a
    configurable budget, so that offline availability doesn't grow device
    storage without bound.
22. As an app developer, I want a typed "body unavailable offline" error on a
    cold-cache offline read, so that I can render a sensible fallback.

### Permissions & integrity

23. As an app developer, I want the file table's row policies — read,
    update, delete — to be the only permission surface for files, gating
    metadata sync, metadata updates, and deletion exactly as on any table,
    so that there is nothing file-specific to learn and nothing that can
    silently disagree with row permissions.
24. As an app developer, I want any file row a subscriber can see to be
    guaranteed to have its body present on the object store (acceptance
    includes body verification), so that readers never observe a
    "row exists, bytes missing" state on the file row itself.
25. As an app developer, I want a transaction that references a
    not-yet-released file row to sync normally and possibly arrive before the
    file row does, so that unrelated work is never held hostage — and I
    accept rendering a pending/placeholder state for dangling file
    references, as with any local-first read.
26. As an app developer, I want a commit unit whose body is absent or whose
    size mismatches the declared column to be rejected whole and surfaced on
    the write handle like any rejected transaction, so that integrity
    failures are loud and local state is cleaned up.
27. As an operator, I want unclaimed upload grants to expire as leases whose
    objects the issuing edge sweeps, so that an authorized identity farming
    grants accumulates nothing past the lease horizon.
28. As an operator, I want the lease window to be my knob trading
    abuse-window against resume-window, so that I can tune it per deployment.

### Deletion & history

29. As an end user, I want deleting a file row (policy-gated, ordinary
    delete) to make the row disappear from views immediately and the object
    disappear eventually, so that deletion is one action with no second
    cleanup step.
30. As an operator, I want the core to be the single owner of object
    deletion — observing the delete settle, appending to a durable queue,
    issuing idempotent retried DELETEs — so that there are no racing edge
    deletes and no orphaned responsibility.
31. As an app developer, I want historical reads and branches to surface a
    file row at a past cut even after its object is deleted, with body reads
    failing via the same typed missing-body error, so that bodyless history
    is a defined semantic rather than a crash.
32. As an end user, I want a deleted file's URL to stop serving bytes once
    the object is deleted (with CDN-cached copies aging out on their own),
    so that deleting the row is the one way to withdraw a file from the web.

### Operations & deployment

33. As an operator, I want the backend contract to be exactly the
    S3-compatible API (presigned single/multipart PUT, presigned GET, HEAD,
    DELETE), so that S3, R2, minio, and Tigris all work unchanged.
34. As an operator, I want edges and the core to hold the object-store
    credentials (edges verify, grant, and sweep; the core deletes), so that
    clients never see store credentials.
35. As an operator, I want a recommended S3 lifecycle rule expiring
    incomplete multipart uploads, so that half-finished uploads don't
    accumulate cost outside the lease sweep.
36. As a developer running tests or local dev, I want the file plane to run
    against minio or an in-process fake, so that no cloud account is needed
    to develop or CI-test file features.

## Implementation Decisions

- **The file plane is split between the sync protocol and one HTTP
  endpoint.** Grant requests and release confirmation are request/response
  message pairs on the client's already-authenticated sync connection — no
  second credential system; whatever admission bound the session to an
  identity authorizes file operations. The only HTTP surface is the serving
  endpoint `GET /files/{table}/{row}`, which is public and unauthenticated:
  it 302-redirects to the object store by default and streams when the
  backend cannot presign. There is no URL-mint operation anywhere — the URL
  is a pure function of row identity, computable locally.
- **File tables are ordinary tables** declared as file tables via the public
  schema builder. Built-ins: `name` (text, unfrozen), `mime_type` (text,
  frozen), `size` (integer, frozen, server-verified). The fate authority
  enforces frozen columns (rejects any post-creation update). There is no
  `published` column: every accepted file is public by URL from the start.
- **Permissions are the file table's row policies, unchanged and
  fail-closed, and they govern metadata only.** Read policy gates metadata
  sync. Update policy gates `name` and app columns. Delete policy gates
  deletion. File bytes are not permission-gated: anyone holding the URL can
  fetch them, and the unguessable row id is the only barrier. This is a
  deliberate product decision — Jazz's value for files is the integrated
  relational experience and offline capability, not byte-level access
  control.
- **Object addressing is row-keyed, not content-addressed:**
  `{app}/{table}/{row-uuid}`. No `hash` column, no dedup, no refcounting in
  v1 — bodies are single-writer and immutable, so a declared hash would
  protect only the uploader's own readers.
- **Upload flow:** (1) create fully offline — body into the device file
  store, `size` measured, row committed locally; (2) the file commit unit
  holds at the outbox until release while later commit units bypass it —
  dangling file references at other subscribers are a first-class,
  app-handled semantic; (3) grant request `(table, row, size)` over sync;
  the edge checks the write policy would plausibly admit the row, returns
  object key, lease expiry, and presigned URLs (single PUT below a
  tens-of-MB implementation constant, multipart above); (4) client PUTs
  directly to the store, persisting completed part ETags locally for
  restart-resume within the lease window; (5) release confirmation over
  sync frees the commit unit into the ordinary lane; (6) at the acceptance
  gate the fate authority (the core, for exclusive units) verifies via one
  HEAD that the object exists and its size matches the declared column —
  mismatch or absence rejects the whole commit unit.
- **Grants are leases.** A grant unclaimed by an accepted row within its
  lease window (default on the order of days) expires; the issuing edge —
  which holds the grant record and credentials — deletes the uploaded
  object. This is the storage-abuse bound; there is no general GC.
- **Download hot path never touches the Jazz DB — no policy checks, no
  auth.** One case for every file: stable URL, unauthenticated, long-lived
  immutable cache headers (bodies are immutable and never change meaning),
  redirect to object store or CDN. Serving cost is flat; storage and egress
  are the billable dimensions.
- **Device file store holds pinned bodies** (created here, kept at least
  until upstream acceptance) **and cached bodies** (downloaded, keyed by row
  identity, LRU under a configurable budget; eviction is reversible since
  bodies are refetchable by URL). SDK reads check the cache first and write
  fetched bodies through it. No automatic prefetch in v1.
- **The core owns object deletion:** it observes a row delete settle
  globally, appends to a durable deletion queue, and issues idempotent,
  retried DELETEs. Row disappears immediately; object eventually. Bodyless
  history is first-class: metadata remains readable at past cuts, body reads
  fail with the same typed error as any missing body.
- **Backend contract is one abstraction — the S3-compatible API** (presigned
  single PUT, multipart upload, presigned GET, HEAD, DELETE), covering S3,
  R2, minio, Tigris. Edges hold credentials for verify/grant/sweep; the core
  holds them for deletion. Dev and tests run minio or an in-process fake.
- **TS API re-backs the existing file-storage runtime shapes** onto the file
  plane: `fromBlob`/`fromStream` (create, immediate handle, background
  upload), `toBlob`/`toStream` (cache-through reads), `file.url()` (the
  stable public URL, computed synchronously and locally), and an observable
  upload state on the handle:
  `local → uploading(progress) → released → accepted | rejected`.
- **Vocabulary amendments to the Files section of the core context doc**
  (part of this work): upload grant pins size only (no hash) and is a
  sweepable lease; body verification is presence + size match; the
  **publish** and **capability URL** entries are removed — every file body
  is world-readable by URL, and row policies gate only the metadata; `name`
  stays the unfrozen column.

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
    grant/release/acceptance, frozen-column enforcement, body verification
    failures (absence, size mismatch), lease-expiry sweep, and core-owned
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
  metadata the fetching identity's read policy would hide (deliberately
  asserting the public-bytes semantic); a referencing transaction bypassing
  the held file commit unit; offline create → later release; offline read
  from warm cache and the typed cold-cache error; bodyless historical read
  after object deletion; the URL 404ing after row deletion once the object
  is gone.
- **Prior art:** the existing jazz-tools integration suites for permissions,
  claims, client restart, and large-blob permissions are the closest
  templates on the Rust side; the client/db `.test.ts` suites in the TS SDK
  runtime are the template on the TS side.

## Out of Scope

- General orphan GC beyond the grant-lease sweep and the recommended S3
  lifecycle rule for incomplete multipart uploads.
- Content hashing, content-hash dedup, and refcounting (no `hash` column;
  apps wanting tamper-evidence add their own metadata column).
- Automatic body prefetch for subscriptions — offline reads rely on the
  read-through cache.
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

- **Stated, accepted semantics** implementers must not "fix": dangling file
  references (a referencing transaction can arrive before its file row —
  apps render pending states); file bytes readable by anyone holding the
  URL regardless of row policies, with the unguessable row id as the only
  barrier; CDN-cached copies of a deleted file's bytes persisting until
  cache eviction (immutable caching makes purge best-effort at most);
  permanent local-first data loss if the creating device dies before
  release (the handle's upload state is the app's warning surface); a
  manually deleted object after acceptance serving 404/410 (operator error,
  not a protocol state).
- The single-PUT vs multipart size threshold is an implementation constant
  on the order of tens of MB, not configuration.
- The lease window default is on the order of days and is operator-facing.
- The design doc's "Rejected alternatives" section is required reading
  before proposing any deviation from the shapes above — with the caveat
  that this spec's removal of private files supersedes the design doc's
  published/private split.
