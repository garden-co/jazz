# Files — Feature Spec

Date: 2026-07-09
Status: ready for implementation
Triage: ready-for-agent
Derived from: the approved files design (`2026-07-08-files-design.md`, sharpened
by grilling review, then hardened by a three-model adversarial review). That
design doc remains the authority on rationale and rejected alternatives, with
these deliberate amendments in this spec:

1. **Private files are dropped.** Every file body is public by URL; there is
   no `published` column, no URL minting, and no signed URLs. Permissions
   gate metadata only.
2. **Files are a column type, not a file table.** This restores the shape the
   core vocabulary already records (a file is "a column type whose cell holds
   a descriptor of exactly one immutable body"); the design doc's file-table
   data model is superseded.
3. **Reads are URL-only, with offline provided below the URL.** The SDK has
   no byte-read API (`toBlob`/`toStream` are gone); apps fetch the URL and
   derive blobs in userland. Offline reads ship in v1 as URL interceptors:
   a service worker on web, a loopback HTTP server inside the native module
   on React Native — both serving staged bodies (own files, pre-acceptance)
   and a read-through body cache.
4. **The bucket is public-read** (GetObject only, listing denied). The
   serving path is `GET /files/{app}/{file-id}` and redirects to the plain
   public object URL — no presigned GETs anywhere.

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
value naming exactly one body: client-minted **file id** (mandated
cryptographically random, UUIDv4-grade), `name`, `mime_type`, `size`. The
cell updates like any column (swapping in a new descriptor is how you
"replace" a file, gated by the ordinary update policy), but each
descriptor-body pair is immutable forever. Because the file lives _in_ the
row that owns it, the **permission surface is exactly the host table's row
policies**, gating metadata sync, descriptor swaps, and deletion the same
way they gate every other column. They govern metadata only.

The **file body** — the bytes — lives on a public-read S3-compatible bucket,
keyed `{app}/{file-id}`, uploaded directly by the client under a
server-issued, time-limited **upload grant** whose presigned PUT pins the
response headers and carries a conditional-write guard so existing objects
can never be overwritten. Creation is fully offline-capable: `fromBlob`
stores the body in the device file store (upload staging) and yields a
descriptor to write into a cell; the transaction that writes a fresh
descriptor commits locally at once and holds at the outbox (without blocking
later, independent writes) until the body is confirmed uploaded
(**release**); at acceptance the core — the single claim authority —
verifies the body and **claims the grant in one atomic step**. Each grant is
claimable exactly once, ever. Consequence: any descriptor a reader can see
had its bytes verified present at acceptance (the stated exceptions are
bodyless history and post-deletion reads).

Reading a file _is the web_: one stable, unauthenticated, public URL —
`GET /files/{app}/{file-id}` — redirecting to the public object URL. Bodies
are immutable, so every response carries long-lived immutable cache headers
with no signature expiry asterisk: trivially CDN-cacheable, cheap to serve,
cheap to bill. The SDK offers no byte-read API: apps `fetch` the URL and
derive blobs in userland. Offline reads are provided _below_ the URL by
per-platform interceptors — a Jazz-shipped service worker on web, a
loopback HTTP server inside the native module on React Native — each
serving this device's staged bodies (so `url()` renders own files
immediately, offline and pre-acceptance) and a read-through, LRU-bounded
body cache of downloads. There are no private files, no signed URLs, and no
per-download policy checks: the value Jazz provides is the integrated
experience (files as values in your own rows, synced and permission-gated
as metadata) plus offline capability, not byte-level access control. Bytes never transit Jazz nodes and never enter Jazz storage,
the sync lane, or the content channel.

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
   URL derived from app and file id, so that rendering a file is local
   string construction — no server round-trip, no async URL step.

### Creating & uploading

7. As an end user, I want to attach a file while offline and keep using the
   app, so that flaky connectivity never blocks my work.
8. As an app developer, I want to preview a just-created file from the Blob
   I already hold (the URL goes live at acceptance), so that my UI shows the
   file immediately without any SDK read machinery.
9. As an end user, I want my other, independent writes to keep syncing while
   a large file is still uploading, so that one slow video doesn't stall my
   whole session.
10. As an end user, I want an interrupted upload to resume from the last
    completed part after an app restart, so that a 2 GB upload doesn't start
    over because I closed the laptop.
11. As an app developer, I want the client to be able to request fresh
    presigned part URLs for an existing grant within its lease, so that
    uploads longer than a presign window still complete and resume.
12. As an end user, I want to see upload progress and state
    (`local → uploading(progress) → released → accepted | rejected`) on the
    file handle, so that the app can show me what's pending and warn me
    before I abandon a device holding unreleased files.
13. As an app developer, I want `fromBlob` to return a usable descriptor
    handle immediately while upload continues in the background, so that I
    can write it into a cell in the same breath and my UI code stays simple.
14. As an app developer, I want uploads to go directly from the client to
    the object store under a presigned grant, so that my server's bandwidth
    bill doesn't scale with upload traffic.
15. As an app developer, I want the transaction that writes a fresh
    descriptor to become visible to other subscribers only once the body is
    verified present, so that a reader who can see the descriptor can always
    fetch the bytes — no placeholder discipline required.
16. As an app developer, I want to _choose_ early visibility when I need it
    — by putting the file cell in its own row (an attachments row) so the
    referencing row syncs immediately — so that "message text now,
    attachment when uploaded" is my app's decision, not a forced protocol
    semantic.
17. As an app developer, I want the body staged in the device file store at
    least until the writing transaction is accepted upstream (never evicted
    before then — it may be the only copy), so that upload resume works
    across restarts and my own files render offline from day one.

### Reading & serving

18. As an end user, I want every file's URL to work in an `<img>` tag, a
    `<video>` element (including Range/seeking, served natively by the
    store), or a pasted link with no auth, cookies, or headers, so that
    files behave like normal web resources.
19. As an end user, I want file bytes served from a public-read bucket
    through a CDN with long-lived immutable caching and no signature expiry,
    so that media-heavy apps load fast and caches never sour.
20. As an app developer, I want reads to be URL-only — I `fetch` the URL and
    derive blobs in userland — so that the SDK surface stays tiny and the
    read path is entirely the web platform's.
21. As an app developer, I want it stated plainly that file bytes are
    readable by anyone holding the URL — the unguessable file id is the only
    barrier — so that I never mistake the row policies (which gate metadata)
    for byte confidentiality, and keep genuinely sensitive content out of
    files or encrypt it myself.
22. As an operator, I want downloads to be a redirect to the public object
    URL with zero policy evaluation and zero Jazz DB involvement, so that
    serving cost is flat and storage/egress is what I bill.
23. As an operator, I want the serving layer to send
    `X-Content-Type-Options: nosniff` and enforce a Content-Disposition
    policy (inline only for an allowlist of render-safe types; everything
    else `attachment`), so that my files domain cannot be turned into an XSS
    or phishing host.
24. As an end user on web, I want a Jazz-shipped service worker
    intercepting `/files/*` — serving this device's staged bodies and a
    read-through cache of downloads — so that my own files render
    immediately (offline, pre-acceptance) and any file opened once is
    readable offline, all through plain `<img>` tags.
25. As an end user on React Native, I want the native module to run a
    loopback HTTP server with the same serve-staged/serve-cached/
    proxy-and-cache behavior, with `file.url()` returning the loopback URL
    on RN, so that `<Image>`, video components, and WebViews get the same
    offline story with no per-component code.
26. As an app developer on RN, I want a canonical-URL accessor alongside
    the loopback `url()` (e.g. `url({ canonical: true })`), so that URLs I
    store, sync, or share never leak a device-local address.
27. As an operator, I want the loopback server bound to 127.0.0.1 on a
    random port with a per-boot secret path segment, so that other apps on
    the device cannot enumerate or fetch bodies through it.
28. As an app developer, I want the interceptor's body cache LRU-evicted
    under a configurable budget (staged bodies exempt until acceptance),
    so that offline availability never grows device storage without bound —
    eviction is reversible; bodies are refetchable by URL.

### Permissions & integrity

29. As an app developer, I want the host table's row policies — read,
    update, delete — to be the only permission surface for files, gating
    metadata sync, descriptor swaps, and deletion exactly as on any column,
    so that there is nothing file-specific to learn and nothing that can
    silently disagree with row permissions.
30. As an app developer, I want file ids mandated to be minted from a
    cryptographic RNG with at least UUIDv4 entropy, so that the one value
    guarding all bytes is a real barrier, not a `Math.random()` accident.
31. As an app developer, I want grant issuance to refuse any file id the
    claim ledger has ever seen, and the presigned PUT to carry a
    conditional-write guard (`If-None-Match: *`), so that no client —
    however malicious — can ever overwrite an existing body.
32. As an app developer, I want each upload grant claimable exactly once,
    atomically at the core's acceptance step, so that one file id lives in
    exactly one cell — across edges, races, and retries — and deletion stays
    well-defined without refcounting.
33. As an app developer, I want release and acceptance to be idempotent — a
    retried release for an already-claimed grant returns the recorded
    outcome — so that a dropped ack never makes an accepted file look
    rejected.
34. As an app developer, I want a transaction whose body is absent or whose
    size mismatches the descriptor to be rejected whole and surfaced on the
    write handle like any rejected transaction (with the local cell reverted
    and the staged body dropped), so that integrity failures are loud and
    local state is cleaned up.
35. As an operator, I want unclaimed upload grants to expire as leases —
    atomically marked expired at the core before their objects and
    multipart uploads are swept — so that an identity farming grants
    accumulates nothing past the lease horizon and the sweep can never race
    an acceptance.
36. As an operator, I want the lease window to be my knob trading
    abuse-window against resume-window, so that I can tune it per
    deployment; per-identity rate limits and quotas on grant issuance come
    later.

### Deletion & history

37. As an end user, I want a file's body deleted when its cell dies —
    overwritten with a new descriptor, set to null, or the row deleted
    (all policy-gated, ordinary writes) — so that removing a file is one
    action with no second cleanup step.
38. As an operator, I want the core to be the single owner of object
    deletion — observing the cell death settle, appending to a durable
    queue, issuing idempotent retried DELETEs — so that there are no racing
    edge deletes and no orphaned responsibility.
39. As an app developer, I want historical reads and branches to surface a
    descriptor at a past cut even after its object is deleted, so that
    bodyless history is a defined semantic rather than a crash (the URL
    404s; there is no SDK body read to error).
40. As an end user, I want a deleted file's URL to stop serving bytes once
    the object is deleted (with CDN-cached copies aging out on their own),
    so that killing the cell is the way to withdraw a file, with the CDN
    caveat stated honestly.
41. As an app developer, I want two devices concurrently swapping the same
    cell offline to resolve like any conflicting column write — one
    descriptor wins, the loser's accepted-then-overwritten descriptor is
    ordinary cell death and its body is queued for deletion — so that
    concurrency needs no file-specific rules.

### Operations & deployment

42. As an operator, I want the backend contract to be exactly the
    S3-compatible API (conditional presigned single/multipart PUT, public
    GET, HEAD, DELETE, multipart create/complete/abort), so that S3, R2,
    minio, and Tigris all work unchanged.
43. As an operator, I want the bucket policy to be public GetObject with
    listing denied, so that unguessable ids actually protect bodies and the
    bucket can sit directly behind a CDN.
44. As an operator, I want edges and the core to hold the object-store
    credentials (edges presign and verify; the core deletes and sweeps),
    so that clients never see store credentials.
45. As an operator, I want a recommended S3 lifecycle rule expiring
    incomplete multipart uploads — set longer than the lease window so it
    never aborts a live upload — as a backstop behind the lease sweep.
46. As a developer running tests or local dev, I want the file plane to run
    against minio or an in-process fake, so that no cloud account is needed
    to develop or CI-test file features.

## Implementation Decisions

- **The file plane is split between the sync protocol and one HTTP
  endpoint.** Grant requests, part-URL refresh, and release confirmation
  are request/response message pairs on the client's already-authenticated
  sync connection — no second credential system. The only HTTP surface is
  the serving endpoint `GET /files/{app}/{file-id}`, public and
  unauthenticated, which 302-redirects to the public object URL. The path
  mirrors the object key `{app}/{file-id}` exactly, so serving needs no
  lookup — deployments may equally point a CDN straight at the bucket.
  There is no URL-mint operation anywhere — the URL is a pure function of
  app and file id, computable locally.
- **The bucket is public-read: GetObject allowed anonymously, listing
  denied.** No presigned GETs exist, so immutable cache headers carry no
  signature-expiry contradiction; Range requests (video seeking) are served
  natively by the store/CDN.
- **Serving is hardened against content-type abuse.** The presigned PUT
  pins `Content-Type`, `Content-Disposition`, and `Cache-Control` to
  grant-time values (a client deviating from them fails the upload). The
  serving layer/CDN adds `X-Content-Type-Options: nosniff` on every
  response. Disposition policy: `inline` only for an implementation-owned
  allowlist of render-safe types (image, video, audio, PDF — never
  `text/html` or `image/svg+xml`); everything else is served as
  `attachment`.
- **File is a column type; the cell holds a file descriptor.** The
  descriptor is an immutable value: client-minted opaque file id, `name`,
  `mime_type`, `size` (server-verified). **File ids are mandated to be
  minted from a cryptographic RNG with at least UUIDv4 entropy** — the id
  is the only byte-confidentiality barrier and the object key, so this is a
  protocol requirement, not SDK guidance. The cell updates like any column —
  a swap to a newly uploaded descriptor is "replace", null is removal — but
  the fate authority rejects any write that mutates a descriptor in place.
  Mutable, queryable metadata belongs in sibling columns. Multiple file
  columns per table are allowed; each cell holds at most one descriptor.
- **Permissions are the host table's row policies, unchanged and
  fail-closed, and they govern metadata only.** Read policy gates
  descriptor sync. Update policy gates descriptor swaps and removal. Delete
  policy gates row deletion. File bytes are not permission-gated: anyone
  holding the URL can fetch them; the unguessable file id is the only
  barrier. Deliberate product decision — Jazz's value for files is the
  integrated relational experience and offline-capable creation, not
  byte-level access control.
- **The core owns the claim ledger; verify + claim + accept is one atomic
  step there.** Grant issuance (at the edge) registers the grant durably
  with the core before presigned URLs are returned; issuance refuses any
  file id the ledger has ever seen (granted, claimed, or expired). At
  acceptance the core HEADs the object (exists, size matches the
  descriptor), checks the grant is unclaimed and unexpired, and claims it —
  atomically with accepting the commit unit. The ledger is permanent: an id
  is never grantable twice, so "one file id, one live cell, ever" holds
  across edges, races, and re-grants (after lease expiry the SDK restarts
  with a fresh id and rewrites the still-local descriptor). Copying a
  descriptor into a second cell rejects (no unclaimed grant). Release and
  claim are idempotent: a retried release for an already-claimed grant
  returns the recorded outcome, never a spurious rejection.
- **Overwrite is impossible by construction.** Object addressing is
  `{app}/{file-id}`, never content-derived. Grant issuance rejects known
  ids (above), and every presigned PUT carries `If-None-Match: *`
  (S3 conditional write): if an object already exists at the key, the store
  itself fails the PUT with 412. No `hash` field, no dedup, no refcounting
  in v1.
- **Upload flow:** (1) create fully offline — `fromBlob` writes the body
  into the device file store (staging), measures `size`, mints the file id,
  returns a descriptor handle; writing it into a cell is an ordinary local
  transaction; (2) that transaction holds at the outbox until release,
  while later independent commit units bypass it (causally dependent writes
  queue behind it); (3) grant request `(file id, size)` over sync — any
  authenticated session may request grants; abuse is bounded by the lease
  sweep, with per-identity rate limits as future work; the edge initiates
  the multipart upload where needed (it owns the `UploadId`, stored in the
  grant record), registers the grant at the core, and returns object key,
  lease expiry, and conditional presigned URLs (single PUT below a
  tens-of-MB implementation constant, multipart above); (4) the client PUTs
  directly to the store, persisting completed part ETags locally; it may
  request fresh part URLs for the same grant within the lease (presign
  windows are hours, leases are days); (5) release over sync carries the
  part ETag list; the edge completes the multipart upload, then the commit
  unit enters the ordinary lane; (6) the core performs the atomic
  verify+claim+accept. Absence, size mismatch, or a missing/spent/expired
  grant rejects the whole commit unit; rejection reverts the local cell and
  drops the staged body.
- **Grants are leases, swept without races.** A grant unclaimed within its
  lease window (default on the order of days) is first atomically marked
  expired at the core's ledger — after which it can never be claimed — and
  only then are its object (DELETE) and any incomplete multipart
  (AbortMultipartUpload via the stored `UploadId`) cleaned up. The
  recommended S3 lifecycle rule for incomplete multiparts is a backstop and
  must be configured longer than the lease window.
- **The device file store holds staged bodies and a read-through cache,
  read only by the interceptors.** Staged: bodies this device created, from
  `fromBlob` until the writing transaction is accepted upstream (surviving
  restarts for resume; never evicted before acceptance — they may be the
  only copy). Cached: bodies fetched through an interceptor, keyed by file
  id (safe — immutable and 1:1), LRU-evicted under a configurable budget;
  eviction is reversible since bodies are refetchable by URL. The store
  must live where the platform's interceptor can read it (browser:
  SW-readable — OPFS/Cache API). The SDK itself never exposes reads from
  it.
- **Reads are URL-only; offline reads are interceptors below the URL.**
  The SDK exposes no `toBlob`/`toStream`: apps `fetch(file.url())` and
  derive blobs in userland. Offline capability ships in v1 as per-platform
  URL interceptors with identical behavior — serve staged (own files:
  `url()` renders immediately, offline and pre-acceptance), else serve
  cached, else fetch through and write the cache:
  - **Web:** a Jazz-shipped service worker intercepting `/files/*`; the
    app registers it. On the very first page load (no controlling SW yet)
    requests fall through to the network — the Blob-in-hand preview
    (`URL.createObjectURL`) remains the guaranteed pre-acceptance path.
  - **React Native:** a loopback HTTP server inside the Jazz native
    module (one Rust implementation over the same device file store),
    bound to 127.0.0.1 on a random port with a per-boot secret path
    segment (localhost is reachable by other apps); it dies with the app
    process, which is fine — nothing renders then. On RN, `file.url()`
    returns the loopback URL so `<Image>`, video components, and WebViews
    work unmodified; `url({ canonical: true })` returns the shareable
    public URL — device-local addresses must never be stored in rows or
    shared. Cleartext-to-localhost exemptions apply (ATS allows localhost;
    Android needs the manifest exemption).
- **The core owns object deletion, triggered by cell death:** when a
  descriptor's cell is overwritten, nulled, or its row deleted — and the
  core observes that settle globally — it appends the file id to a durable
  deletion queue and issues idempotent, retried DELETEs. The one-live-cell
  rule makes "unreferenced" exact; concurrent offline swaps of the same
  cell resolve like any conflicting column write, the losing descriptor's
  cell death queueing its object. Bodyless history is first-class:
  descriptors remain readable at past cuts; their URLs 404.
- **Backend contract is one abstraction — the S3-compatible API**
  (conditional presigned single PUT, multipart create/upload/complete/
  abort, public GET, HEAD, DELETE), covering S3, R2, minio, Tigris. Edges
  hold credentials for presign/verify; the core holds them for
  sweep-mark-and-delete and deletion. Dev and tests run minio or an
  in-process fake.
- **TS API:** `fromBlob(blob, opts)` (create; returns a descriptor handle
  to write into cells, background upload; creation input is a Blob so
  `size` is always known — there is no `fromStream`), `file.url()` (the
  stable public URL on web/server, the loopback URL on RN; computed
  synchronously and locally; `url({ canonical: true })` always returns the
  public one), and an observable upload state on the handle:
  `local → uploading(progress) → released → accepted | rejected`. Nothing
  else: reads, previews, and blob derivation are userland.
- **Vocabulary amendments to the Files section of the core context doc**
  (part of this work): **upload grant** pins size only (no hash), is a
  sweepable lease, and is registered in the core's permanent claim ledger;
  **body verification** is presence + size match, atomic with the claim;
  the descriptor loses its `content hash` and `visibility` fields; the
  **publish** and **capability URL** entries are removed — every file body
  is world-readable by URL, and row policies gate only the metadata.

## Testing Decisions

- **Good tests here are black-box integration tests through public APIs
  only**: schema via the public schema builder, permissions via the public
  policy builders, effects asserted through queries, subscription deltas,
  and accepted/rejected write settlement — never through internal state or
  JSON-like definitions. The Rust testing guidelines in the jazz-tools
  crate are binding.
- **Two existing seams plus exactly one new seam** (confirmed with the
  developer):
  - Rust: jazz-tools-style integration tests (a `JazzServer` with
    `TestingClient`s, or `test_client` where one runtime suffices) covering
    grant/release/acceptance, descriptor immutability enforcement,
    single-claim atomicity (descriptor copy rejected; same-id race through
    two edges accepts exactly one), overwrite refusal (grant for a known id
    rejected; conditional PUT 412s against an existing object), body
    verification failures (absence, size mismatch), the sweep
    (mark-expired-then-delete; an expired grant is unclaimable; incomplete
    multiparts aborted), idempotent release retry, and
    cell-death-triggered core-owned deletion.
  - TS: tests through the public `fromBlob`/`url()`/upload-state surface
    against a really-served endpoint, in the style of the existing
    client/db integration tests in the TS SDK.
  - The single new seam: the S3-compatible object-store backend contract
    (including conditional writes and multipart abort), with an in-process
    fake under the file plane in tests and minio as an optional real
    target.
- **Explicit scenarios to cover:** resume-after-restart within the lease,
  including part-URL refresh past a presign window; restart after lease
  expiry (fresh id, restart); the serving endpoint returning bytes with no
  auth for an accepted file, including one whose host row the fetching
  identity's read policy would hide (deliberately asserting the
  public-bytes semantic); nosniff and disposition policy on served
  responses (an HTML upload is never served inline); an independent
  transaction bypassing a held file-writing transaction, and a dependent
  one queuing behind it; offline create → later release; the URL 404ing
  before acceptance and after cell death once the object is gone;
  descriptor swap uploading the new body and deleting the old one;
  concurrent same-cell swaps resolving to one live descriptor and one
  queued deletion; bodyless historical read after object deletion;
  interceptor behavior (web SW and RN loopback): staged body served
  offline and pre-acceptance, cached body served offline after one online
  read, fetch-through writing the cache, LRU eviction respecting the
  budget and never evicting pre-acceptance staged bodies, and the RN
  loopback refusing requests without the per-boot secret path.
- **Prior art:** the existing jazz-tools integration suites for
  permissions, claims, client restart, and large-blob permissions are the
  closest templates on the Rust side; the client/db `.test.ts` suites in
  the TS SDK runtime are the template on the TS side.

## Out of Scope

- Any SDK byte-read API (`toBlob`/`toStream` — blob derivation is
  userland; offline reads are the interceptors' job, not an API).
- Interceptors beyond web SW and RN loopback (e.g. desktop webview
  shells); they can reuse the loopback design later.
- `fromStream` / unknown-length uploads (creation takes a Blob; size must
  be known at descriptor time).
- Per-identity rate limits and quotas on grant issuance and download
  egress (leases bound storage abuse in v1; rate limits are planned future
  work).
- General orphan GC beyond the ledger-coordinated lease sweep and the
  lifecycle-rule backstop.
- Content hashing, content-hash dedup, and refcounting (no `hash` field;
  apps wanting tamper-evidence add their own metadata column).
- Moving or copying a descriptor between cells (needs refcounting or a
  transfer protocol; v1 is one file id, one live cell — re-upload if you
  need the same content twice).
- Lists of files in one cell (`list(file)` columns); use multiple columns
  or rows in a side table in v1.
- Private files and any byte-level access control (signed URLs, capability
  URLs, per-download policy checks). Every body is public by URL in v1;
  byte confidentiality can be layered on later without changing the URL
  scheme. Apps with sensitive content keep it out of files or encrypt
  client-side.
- A standalone file service — deferred until serving traffic warrants it.
- Upload proxying through the server, body transport over the content
  channel, HTTP grant endpoints, content-addressed keys, and strict-FIFO
  outbox holds — all considered and rejected in the design doc.

## Further Notes

- **Stated, accepted semantics** implementers must not "fix": file bytes
  readable by anyone holding the URL regardless of row policies, with the
  unguessable (mandated-CSPRNG) file id as the only barrier; the URL 404ing
  until acceptance (apps preview from the Blob they hold); sibling columns
  written in the same transaction as a fresh descriptor becoming visible
  only at acceptance — and being lost with it if verification rejects the
  unit (apps wanting early visibility or blast-radius isolation model the
  file cell in its own row); on web, no SW controls the very first page
  load, so requests then fall through to the network and the Blob-in-hand
  preview remains the guaranteed pre-acceptance path; CDN-cached copies of a
  deleted file's bytes persisting until cache eviction (immutable caching
  makes purge best-effort at most); permanent local-first data loss if the
  creating device dies before release (the handle's upload state is the
  app's warning surface); a manually deleted object after acceptance
  serving 404/410 (operator error, not a protocol state).
- The single-PUT vs multipart size threshold, the presign window for part
  URLs, and the inline-safe type allowlist are implementation constants,
  not configuration.
- The lease window default is on the order of days and is operator-facing;
  the incomplete-multipart lifecycle rule must exceed it.
- The design doc's "Rejected alternatives" section is required reading
  before proposing any deviation from the shapes above — with the caveats
  listed in this spec's header.
