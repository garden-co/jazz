# Files — Feature Spec

Date: 2026-07-09
Status: ready for implementation
Triage: ready-for-agent
Derived from: the approved files design (`2026-07-08-files-design.md`, sharpened
by grilling review, then hardened by a three-model adversarial review, then
simplified by the descriptor-persistence, grant-ledger, and id-management
grillings of 2026-07-10, then reshaped by the invisible-core pivot of the
device-file-store grilling later that day). That design doc remains the
authority on rationale and rejected alternatives, with these deliberate
amendments in this spec:

1. **Private files are dropped.** Every file body is public by URL; there is
   no `published` column, no URL minting, and no signed URLs. Permissions
   gate metadata only.
2. **Files are a column type, not a file table.** This restores the shape the
   core vocabulary already records (a file is "a column type whose cell holds
   a descriptor of exactly one immutable body"); the design doc's file-table
   data model is superseded.
3. **Reads are URL-only.** The SDK has no byte-read API (`toBlob`/`toStream`
   are gone); apps fetch the URL and derive blobs in userland. Offline reads
   are not a core concern (see amendment 9).
4. **The bucket is public-read** (GetObject only, listing denied). The
   serving path mirrors the object key and redirects to the plain public
   object URL — no presigned GETs anywhere.
5. **The descriptor is a convention, not an enforced invariant.** There is
   no descriptor immutability enforcement and no body verification anywhere.
   Only the descriptor's _shape_ is validated on write (canonical `v:1`
   JSON). Body immutability is enforced at the bucket alone; `name`/
   `mime_type`/`size` are app-trusted metadata. In-place edits, copies
   between cells, and hand-rolled descriptors are all legal, ordinary
   policy-gated writes whose URLs simply 404 if no body exists.
6. **Deletion is an explicit API and cleanup is bucket TTL.** Cell death
   never deletes objects. `jazz.files.delete(fileId)` — authorized for the
   uploader identity or the backend — is the one way to remove a body by
   hand. Unclaimed uploads are garbage-collected by bucket lifecycle rules
   on a `pending/` prefix, not by a server sweep.
7. **File ids are identity-bound, and the server keeps no file-plane state
   of any kind.** A file id embeds the uploader's identity id plus a random
   part; the object key is `{app}/{identity}/{random}`. Grant and delete
   authorization is a pure comparison — "is the key's identity segment
   yours?" — with **zero bucket calls at issuance and zero records
   anywhere**: no ledger, no tombstones, no uploader metadata. Taking over
   another identity's URL is impossible by construction.
8. **TTL is declared on the schema.** A file column may name a TTL class —
   `s.file({ ttl: "7d" })` — from a deployment-declared set (each class a
   key prefix with one bucket lifecycle rule); there is no per-call TTL.
   The class is embedded in the file id itself, so the descriptor stays
   four-field and `url()` needs no schema. Identity-bound keys make expiry
   safe — an expired id can only ever be re-claimed by its original owner.
9. **The core is invisible: no device file store, no offline machinery
   (2026-07-10 pivot).** `fromBlob` keeps the Blob in memory and uploads
   in-session; the outbox hold is an in-memory courtesy that deliberately
   does not survive restart; an upload interrupted by a restart is lost —
   the committed descriptor syncs bodyless and its URL 404s, the ordinary
   documented state; `url()` returns the public URL on every platform. The
   only durable client-side file-plane record is the pending-delete intent.
   Offline upload durability (staged bodies, resume records, durable holds)
   and offline reads (web service worker, RN loopback server, read-through
   cache) move to a **future opt-in package** — any offline footprint is
   added willingly by the app, never by the core.

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
usable on any table, whose cell holds a **file descriptor** — a small
canonical-JSON value naming one body: required `v: 1` plus **file id**,
`name`, `mime_type`, `size`. The file id is **identity-bound**: one opaque
string embedding the column's TTL class (when one is declared), the
uploader's identity id, and a cryptographically random part. The SDK
finalizes the id at the **first cell write** — the destination column's
schema names the class — entirely locally and fully offline, from day
zero, because a client always knows its own identity id. At the type layer
this is a schema-level `ColumnType::File` that lowers onto the existing
text value type — the same facade pattern JSON columns already use — so no
value-enum, row-format, or binding changes exist anywhere. The write path
validates the descriptor's _shape_ strictly (canonical form, known fields,
well-formed id); readers parse leniently (future versions still sync and
serve `url()`, which needs only the id). That shape check is the only file-specific validation in the
system: the descriptor is a **convention**. Its fields are app-trusted
metadata, in-place edits and copies are ordinary policy-gated writes, and
the **permission surface is exactly the host table's row policies**, same
as any column.

The **file body** — the bytes — lives on a public-read S3-compatible
bucket, keyed `{app}/{identity}/{random}` (or
`{app}/t{class}/{identity}/{random}` for TTL'd files), uploaded directly by
the client under a server-issued **upload grant**. The identity-bound key
is what makes the whole plane stateless and lookup-free: **grant issuance
is a pure comparison** — the requested key's identity segment must equal
the requesting session's identity — with zero bucket calls and nothing
written or recorded anywhere. Nobody can ever be granted a key outside
their own namespace, so overwriting or taking over another identity's URL
is impossible by construction; the conditional-write guard
(`If-None-Match: *`) on the presigned PUT and multipart completion remains
only as a self-collision belt. Uploads land under a
`pending/{app}/…` key; the grant response hands the client the multipart
`UploadId`, which the client holds for the life of the upload (in memory
in core; the opt-in package persists it durably) — edges keep nothing, so
any edge can refresh part URLs or perform the release.
**Release** is stateless and idempotent: HEAD the final key (already there
→ success), complete the multipart, server-side-copy the object to its
final key, and delete the pending object. No size check, no verification,
no acceptance-time role at all. A client that lies about releasing harms
only itself: its URL 404s. Unreleased uploads are garbage-collected by a
bucket lifecycle rule expiring the `pending/` prefix (plus the native
incomplete-multipart abort rule) — the lease window _is_ the lifecycle
expiry, and no server sweep exists.

Creation is offline-capable within a session: `fromBlob` keeps the Blob in
memory, measures `size`, and yields a descriptor to write into a cell; the
upload runs in-session — grant → PUT → release — straight from that Blob.
The SDK holds the writing transaction at the outbox until release — an
**in-memory** client-side courtesy so that descriptors written through the
upload path have bytes present when they sync, not a server-enforced gate
— and later, independent writes bypass the held unit. A restart mid-upload
loses the body: the committed descriptor syncs and its URL 404s — the
ordinary bodyless state, documented as the interrupted-upload outcome.
Nothing re-uploads; durable staging and resume belong to the opt-in
offline package.

Reading a file _is the web_: one stable, unauthenticated, public URL —
`GET /files/{app}/{identity}/{random}` — redirecting to the public object
URL. Bodies are immutable, so permanent files carry long-lived
`Cache-Control: immutable` headers with no signature expiry asterisk, and
TTL'd files carry `max-age` capped to their class — trivially
CDN-cacheable, cheap to serve, cheap to bill. The SDK offers no byte-read
API: apps `fetch` the URL and derive blobs in userland. Offline reads are
not a core concern: a future opt-in package provides them below the URL
(per-platform interceptors and a read-through cache); the core stays
invisible, and apps that want offline availability add it willingly.
There are no private
files, no signed URLs, and no per-download policy checks: the value Jazz
provides is the integrated experience (files as values in your own rows,
synced and permission-gated as metadata), not
byte-level access control. One privacy semantic is stated plainly: **every
file URL publicly carries the uploader's identity id** — pseudonymous (an
opaque id), but stable and linkable across all of that uploader's files.
Bytes never transit Jazz nodes and never enter Jazz storage, the sync
lane, or the content channel.

Bodies leave the bucket two ways. **Explicitly**:
`jazz.files.delete(fileId)`, authorized by the same identity-segment
comparison (the backend surface skips it) and executed as one idempotent
DELETE against the bucket. **By TTL**: a file written into a column
declared with a TTL class (`s.file({ ttl: "7d" })`) lands under that
class's key prefix, whose lifecycle rule expires it — the clock starts at
the release copy. Either way, descriptors remain and their
URLs 404 — an ordinary, legal state — and an expired or deleted id can
only ever be re-claimed by its original owner (which the SDK never does:
fresh randoms every time). Cell death — overwrites, nulls, row deletes —
never deletes objects.

## User Stories

### Schema & data model

1. As an app developer, I want to declare a file column on any table through
   the public schema builder, so that a file lives in the row that owns it —
   an avatar on the profile, an attachment on the message — with no side
   table or foreign-key ceremony.
2. As an app developer, I want the descriptor to carry `name`, `mime_type`,
   and `size` as app-trusted metadata, so that I don't design file metadata
   myself and can render lists of files without touching bodies.
3. As an app developer, I want the descriptor to be a versioned canonical
   value (`v: 1`, sorted-key compact JSON) whose shape is strictly validated
   on write and leniently parsed on read, so that malformed cells can't
   exist, digests are byte-stable, and future descriptor versions still
   sync and render on old clients.
4. As an app developer, I want the file column implemented as a schema-level
   facade lowering onto the existing text value type (the JSON-column
   precedent), so that no value-enum, row-format, or WASM/NAPI/RN binding
   changes ride along with the feature.
5. As an app developer, I want descriptor writes to be ordinary policy-gated
   column writes — in-place edits, swaps, copies, even hand-rolled
   descriptors — with no immutability enforcement, so that there is no
   file-specific write machinery and no previous-value comparison on the
   write path; the _body_ stays immutable at the bucket regardless.
6. As an app developer, I want app-level metadata (captions, tags, display
   names that change) to be ordinary sibling columns on the same row, so
   that anything I need to query or index lives in real columns.
7. As an app developer, I want multiple file columns on one table when I
   need them (avatar and banner), so that cardinality is my schema's choice.
8. As an app developer, I want every file to have a stable public URL
   derived purely from the descriptor, so that rendering a file is local
   string construction — no server round-trip, no async URL step.
9. As an app developer, I want file ids minted entirely on-device — the
   destination column's TTL class, my identity id, and a CSPRNG random
   part, finalized at the first cell write — so that offline creation works
   from the very first moment an identity exists, with no server handshake
   before `url()` is usable.
10. As an app developer, I want file cells to be opaque to the query layer
    in v1 (text-column semantics: whole-value equality, null checks), so
    that the query path carries no descriptor-specific code; field access
    can arrive later as read-only virtual columns without storage changes.

### Creating & uploading

11. As an end user, I want to attach a file while offline and keep using the
    app, so that flaky connectivity never blocks my work.
12. As an app developer, I want to preview a just-created file from the Blob
    I already hold (the URL goes live once the upload is released), so that
    my UI shows the file immediately without any SDK read machinery.
13. As an end user, I want my other, independent writes to keep syncing while
    a large file is still uploading, so that one slow video doesn't stall my
    whole session.
14. _(Moved to the opt-in offline package by the 2026-07-10 invisible-core
    amendment: resume-after-restart requires durable staging and resume
    records, which core deliberately does not keep. In core, a restart
    mid-upload yields a bodyless descriptor whose URL 404s.)_
15. As an app developer, I want the client to be able to request fresh
    presigned part URLs for an in-flight upload within its lease — from any
    edge, by presenting the `UploadId` the grant handed it — so that
    uploads longer than a presign window, or interrupted by network blips
    within a session, still complete with no edge affinity.
16. As an end user, I want to see upload progress and state
    (`local → uploading(progress) → released → accepted | rejected`) on the
    file handle, so that the app can show me what's pending and warn me
    before I abandon a device holding unreleased files.
17. As an app developer, I want `fromBlob` to return a usable descriptor
    handle immediately while upload continues in the background, so that I
    can write it into a cell in the same breath and my UI code stays simple.
18. As an app developer, I want TTL declared on the schema —
    `s.file({ ttl: "7d" })`, with no per-call option — so that a column's
    role decides its files' lifetime once, ephemeral attachments clean
    themselves up without my app scheduling anything, and call sites stay
    clean; a descriptor copied into a differently-declared column keeps
    the class baked into its id.
19. As an app developer, I want uploads to go directly from the client to
    the object store under a presigned grant, so that my server's bandwidth
    bill doesn't scale with upload traffic.
20. As an app developer, I want the SDK to hold the transaction that writes
    a fromBlob descriptor at the outbox until the upload is released, so
    that files created through the upload path are fetchable by the time
    other devices see them — as an in-memory courtesy of the SDK, not a
    server gate: after a restart the hold is gone and the transaction syncs
    normally, bodyless if the upload never released.
21. As an app developer, I want to _choose_ early visibility when I need it
    — by putting the file cell in its own row (an attachments row) so the
    referencing row syncs immediately — so that "message text now,
    attachment when uploaded" is my app's decision, not a forced protocol
    semantic.
22. As an app developer, I want it stated plainly that core keeps no copy
    of the body beyond the in-memory Blob — an upload interrupted by a
    restart is lost and its descriptor's URL 404s — so that I know exactly
    what durability I get for free, and reach for the opt-in offline
    package when my app needs staged bodies and resume-across-restarts.
    _(Durable staging moved to the package by the 2026-07-10 invisible-core
    amendment.)_

### Reading & serving

23. As an end user, I want every file's URL to work in an `<img>` tag, a
    `<video>` element (including Range/seeking, served natively by the
    store), or a pasted link with no auth, cookies, or headers, so that
    files behave like normal web resources.
24. As an end user, I want file bytes served from a public-read bucket
    through a CDN — permanent files with long-lived immutable caching and
    no signature expiry, TTL'd files with `max-age` capped to their class —
    so that media-heavy apps load fast and caches never sour.
25. As an app developer, I want reads to be URL-only — I `fetch` the URL and
    derive blobs in userland — so that the SDK surface stays tiny and the
    read path is entirely the web platform's.
26. As an app developer, I want it stated plainly that file bytes are
    readable by anyone holding the URL — the unguessable random part of the
    file id is the only barrier — so that I never mistake the row policies
    (which gate metadata) for byte confidentiality, and keep genuinely
    sensitive content out of files or encrypt it myself.
27. As an app developer, I want it stated equally plainly that every file
    URL carries the uploader's identity id — pseudonymous but stable and
    linkable across that uploader's files — so that I treat identity ids as
    public material or keep such apps off the file plane.
28. As an operator, I want downloads to be a redirect to the public object
    URL with zero policy evaluation and zero Jazz DB involvement, so that
    serving cost is flat and storage/egress is what I bill.
29. As an operator, I want the serving layer to send
    `X-Content-Type-Options: nosniff` and enforce a Content-Disposition
    policy (inline only for an allowlist of render-safe types; everything
    else `attachment`), so that my files domain cannot be turned into an XSS
    or phishing host.
30. _(Moved to the opt-in offline package by the 2026-07-10 invisible-core
    amendment: the web service worker, its staged-body serving, and its
    read-through cache are package concerns, not core.)_
31. _(Moved to the opt-in offline package: the RN loopback HTTP server. In
    core, `file.url()` returns the public URL on every platform — `<img>`,
    `<Image>`, and video components fetch it like any web resource.)_
32. _(Retired by the same amendment: with no loopback URL in core, `url()`
    is always the canonical public URL and no `canonical` option exists;
    the package reintroduces the distinction if and when it rewrites URLs.)_
33. _(Moved to the opt-in offline package: loopback binding and per-boot
    secret are package operational concerns.)_
34. _(Moved to the opt-in offline package: the body cache and its LRU
    budget are package concerns; core caches nothing.)_

### Permissions & integrity

35. As an app developer, I want the host table's row policies — read,
    update, delete — to be the only permission surface for file cells,
    gating descriptor sync and every descriptor write exactly as on any
    column, so that there is nothing file-specific to learn and nothing
    that can silently disagree with row permissions.
36. As an app developer, I want the random part of file ids mandated to be
    minted from a cryptographic RNG with at least UUIDv4 entropy, so that
    the one value guarding byte confidentiality is a real barrier, not a
    `Math.random()` accident.
37. As an app developer, I want grant issuance to authorize by pure
    comparison — the requested key's identity segment must equal the
    requesting session's identity — with zero bucket calls and zero
    records, so that nobody can ever upload into another identity's
    namespace and no server state or lookup exists on the issue path.
38. As an app developer, I want takeover of another identity's URL to be
    impossible by construction — after deletion, after TTL expiry, even for
    ids that never finished uploading — so that a dangling reference can go
    bodyless but can never start serving someone else's content.
39. As an app developer, I want the presigned PUT and multipart completion
    to carry a conditional-write guard (`If-None-Match: *`) as a
    self-collision belt, so that even a buggy SDK reusing its own random
    part cannot overwrite its own existing body mid-flight.
40. As an app developer, I want release to be idempotent by construction —
    a retried release HEADs the final key, finds the object already there,
    and succeeds — so that a dropped ack never makes a completed upload
    look failed and no server has to remember an outcome.
41. As an app developer, I want a client that lies about its upload (wrong
    size in the descriptor, release without bytes) to harm only itself —
    its own URL 404s or misdescribes its own body — so that no verification
    machinery exists on anyone else's path.
42. As an operator, I want unreleased uploads garbage-collected by the
    bucket itself — a lifecycle rule expiring the `pending/` prefix after
    the lease window, plus the native incomplete-multipart abort rule — so
    that grant farming accumulates nothing and no server sweep machinery
    exists.
43. As an operator, I want the lease window (= the `pending/` lifecycle
    expiry) to be my knob trading abuse-window against resume-window, so
    that I can tune it per deployment; per-identity rate limits and quotas
    on grant issuance come later.

### Deletion, TTL & history

44. As an end user, I want deleting a file to be an explicit act —
    `jazz.files.delete(fileId)` — so that removing a body is deliberate and
    auditable, never a side effect of editing rows.
45. As an app developer, I want file deletion authorized by the same
    identity-segment comparison as grants — the uploader may delete their
    own files, the backend surface may delete anything, richer rules stay
    app-backend logic — so that delete authorization needs no server state,
    no object metadata, and no lookup.
46. As an operator, I want explicit deletes to be one idempotent DELETE
    against the bucket that any server can execute and safely retry, so
    that a requested delete reliably converges with nothing to coordinate.
47. As an end user, I want my delete to survive flaky connectivity — the
    SDK persists a pending-delete intent locally and retries across
    restarts until the origin confirms (permanent denials drop it; calls
    dedupe; the promise resolves on confirmation) — so that
    fire-and-forget deletion is safe without any server-side record.
48. As an end user, I want a file created with a TTL class to disappear on
    schedule — the bucket's lifecycle rule deletes the body; descriptors
    remain and 404 — so that ephemeral content cleans itself up with no
    server machinery and no app cron.
49. As an operator, I want the TTL class set declared per deployment
    (recommended defaults 1d/7d/30d) with exactly one lifecycle rule per
    class, so that expiry behavior is auditable in the bucket configuration
    itself.
50. As an app developer, I want cell death (overwrite, null, row delete) to
    never touch objects, so that copies and history stay coherent by
    default and storage reclamation is always the explicit API or the TTL
    class; bodies persist until one of those acts.
51. As an app developer, I want historical reads and branches to surface a
    descriptor at a past cut even after its body is deleted or expired, so
    that bodyless history is a defined semantic rather than a crash (the
    URL 404s; there is no SDK body read to error).
52. As an end user, I want a deleted file's URL to stop serving bytes once
    the object is deleted (with CDN-cached copies aging out on their own),
    so that deletion means withdrawal, with the CDN caveat stated honestly.
53. As an app developer, I want two devices concurrently swapping the same
    cell offline to resolve like any conflicting column write — one value
    wins, nothing file-specific happens, no body is deleted — so that
    concurrency needs no file-specific rules.

### Operations & deployment

54. As an operator, I want the backend contract to be exactly the
    S3-compatible API (conditional presigned single/multipart PUT, multipart
    create/complete/abort with conditional completion, server-side copy,
    public GET, HEAD, DELETE, and prefix-scoped lifecycle rules for
    `pending/` and each TTL class), so that S3, R2, minio, and Tigris all
    work unchanged.
55. As an operator, I want the bucket policy to be public GetObject with
    listing denied, so that unguessable random parts actually protect
    bodies and the bucket can sit directly behind a CDN.
56. As an operator, I want servers to hold no file-plane state of any kind
    — no grant records, no ledger, no tombstones, no queues; authorization
    is a string comparison, and the grant response hands the `UploadId` to
    the client — so that edges restart, scale, and load-balance with
    nothing to replicate or reconcile.
57. As an operator, I want servers to hold the object-store credentials
    (presign, complete, copy, delete) with clients never seeing them, so
    that the only client-facing capabilities are the time-limited presigned
    URLs of their own grants.
58. As a developer running tests or local dev, I want the file plane to run
    against minio or an in-process fake (including conditional writes,
    conditional multipart completion, server-side copy, and
    manually-triggerable lifecycle expiry), so that no cloud account is
    needed to develop or CI-test file features.

## Implementation Decisions

- **The file plane is split between the sync protocol and one HTTP
  endpoint.** Grant requests, part-URL refresh, release confirmation, and
  file deletion are request/response message pairs on the client's
  already-authenticated sync connection — no second credential system. The
  only HTTP surface is the serving endpoint
  `GET /files/{app}/{identity}/{random}` (with a `t{class}` segment after
  `{app}` for TTL'd files), public and unauthenticated, which 302-redirects
  to the public object URL. The path mirrors the object key exactly, so
  serving needs no lookup — deployments may equally point a CDN straight at
  the bucket. There is no URL-mint operation anywhere — the URL is a pure
  function of the descriptor, computable locally.
- **The bucket is public-read: GetObject allowed anonymously, listing
  denied.** No presigned GETs exist, so cache headers carry no
  signature-expiry contradiction; Range requests (video seeking) are served
  natively by the store/CDN.
- **Serving is hardened against content-type abuse.** The presigned PUT
  pins `Content-Type`, `Content-Disposition`, and `Cache-Control` to
  grant-time values (a client deviating from them fails the upload). The
  serving layer/CDN adds `X-Content-Type-Options: nosniff` on every
  response. Disposition policy: `inline` only for an implementation-owned
  allowlist of render-safe types (image, video, audio, PDF — never
  `text/html` or `image/svg+xml`); everything else is served as
  `attachment`. Cache policy: long-lived `Cache-Control: immutable` for
  permanent files; `max-age` capped to the class duration for TTL'd files —
  both pinned at grant time.
- **File ids are identity-bound and carry their TTL class.** A file id is
  one opaque string whose segments encode the destination column's TTL
  class (when one is declared), the uploader's identity id, and a CSPRNG
  random part (UUIDv4-grade — a protocol requirement, not SDK guidance:
  the random part is the only byte-confidentiality barrier). The object
  key renders it as path segments: `{app}/{identity}/{random}`, or
  `{app}/t{class}/{identity}/{random}` for TTL'd files. The id is
  **finalized at the first cell write** — `fromBlob` keeps the Blob in
  memory and
  returns a handle; when the descriptor first lands in a file column, the
  SDK mints the full id using that column's declared class. Minting is
  entirely on-device and local — a client always knows its own identity id
  and its schema — so ids and `url()` work offline from the moment an
  identity exists (before the first write there is no id or URL; apps
  preview from the Blob they hold). Writing the same handle to a second
  column is an ordinary copy of the already-finalized descriptor. The
  stated privacy trade: every URL publicly carries the uploader's identity
  id, pseudonymous but stable and linkable across that uploader's files.
- **File is a schema-level column type lowering onto text — the JSON-column
  facade precedent.** A new `ColumnType::File` at the schema layer; the
  cell value is `Value::Text` carrying the descriptor as canonical JSON. No
  new value variant, no row-format change, no WASM/NAPI/RN binding change;
  the storage format version is untouched. The write path gets one
  validation branch beside the existing JSON one: strict _shape_ validation
  — canonical form (compact, sorted keys), required `v: 1`, exactly the
  fields `id`, `name`, `mime_type`, `size`, id well-formed (its class
  segment is validated against the deployment's set at grant time, not
  here — the schema is the client's business, the class set the server's).
  Readers are lenient: unknown future fields/versions are tolerated and
  `url()` needs only the id. On the schema wire, the file column's DDL
  form carries its class — `FILE('7d')` — exactly as JSON columns carry
  their JSON-Schema.
- **The descriptor is a convention, not an enforced invariant.** No
  immutability enforcement: in-place field edits, copies into other cells,
  and hand-rolled descriptors are ordinary writes under the ordinary update
  policy — no previous-value comparison exists on any path. `name`,
  `mime_type`, and `size` are app-trusted metadata (the same class as the
  deliberately dropped `hash`). Anything queryable belongs in sibling
  columns; file cells are opaque to the query layer in v1 (text-column
  semantics), with read-only virtual/magic columns as a compatible future
  extension.
- **Permissions are the host table's row policies, unchanged and
  fail-closed, and they gate cells only.** Read policy gates descriptor
  sync. Update policy gates descriptor writes. Delete policy gates row
  deletion. File bytes are not permission-gated: anyone holding the URL can
  fetch them. Body _deletion_ has its own rule (below) because bodies
  outlive any particular cell.
- **Authorization is a string comparison — the server keeps no file-plane
  state and issues grants with zero bucket calls.** At grant time the
  server checks that the requested key's identity segment equals the
  requesting session's identity, validates the key's class segment (if
  any) against the deployment's class set, and
  presigns — nothing is read, written, or recorded. Nobody can be granted a
  key outside their own namespace, so overwrite and takeover of another
  identity's URL are impossible by construction; there are no issuance
  HEADs, no tombstones, and no uploader metadata. The conditional-write
  guard (`If-None-Match: *`) on the presigned PUT and the multipart
  completion remains purely as a self-collision belt. Delete authorization
  is the same comparison (backend skips it). Only the original owner can
  ever re-claim one of their own ids — after a delete or a TTL expiry —
  and the SDK never does (fresh randoms every time); self-resurrection is
  the owner's own footgun and is stated as such.
- **Upload flow:** (1) create offline-capable — `fromBlob` keeps the Blob
  in memory, measures `size`, and returns a
  handle; at the **first cell write** the SDK finalizes the identity-bound
  id (class segment from the destination column's declaration + identity +
  fresh random) and the write is an ordinary local transaction; (2) the SDK holds that transaction
  at the outbox until release — an in-memory client-side courtesy so
  upload-path
  descriptors have bytes when they sync — while later independent commit
  units bypass it (causally dependent writes queue behind it); (3) grant
  request `(file id, size)` over sync — the class travels inside the id;
  any authenticated session may request grants for its own namespace;
  abuse is bounded by the `pending/` lifecycle expiry, with per-identity
  rate limits as future work; the server verifies the identity segment and
  the class segment by pure
  computation, initiates the multipart upload where needed, and returns the
  pending object key, lease expiry, the `UploadId` (which the client
  holds in memory — no server remembers it), and conditional
  presigned URLs (single PUT below a tens-of-MB implementation constant,
  multipart above); (4) the client PUTs directly to the pending key,
  tracking completed part ETags in memory; it may request fresh part URLs
  from any edge within the lease by presenting the `UploadId` (presign
  windows are hours, leases are days); (5) **release** over sync carries
  the `UploadId` and part ETag list; any edge HEADs the final key (already
  present → success), completes the multipart (conditional),
  server-side-copies the pending object to its final key — for TTL'd files
  that key sits under the class prefix, and the copy is what starts the
  expiry clock — and deletes the pending object — idempotent end to end by
  construction; the held transaction then enters the ordinary lane. There
  is no step six: no size check, no file-specific acceptance. A client that
  never releases leaves only a pending object the bucket will expire; a
  client that lies harms only its own URL.
- **Unreleased-upload cleanup is bucket TTL, not a server sweep.** A
  lifecycle rule expires the `pending/` prefix after the lease window
  (day-granularity, matching the "order of days" lease), and the native
  `AbortIncompleteMultipartUpload` rule covers half-finished multiparts.
  This is prefix-based and therefore portable across S3, R2, minio, and
  Tigris (R2 lifecycle cannot filter by tag). After lease expiry the SDK
  restarts the upload with a fresh id and rewrites the still-local
  descriptor. No sweep code exists anywhere.
- **File TTL is a fixed set of classes, declared on the schema and
  realized as key prefixes.** A deployment declares its class set
  (recommended defaults 1d/7d/30d); permanent is the default. The schema
  names a column's class — `s.file({ ttl: "7d" })` — and there is no
  per-call TTL anywhere. The class is baked into the file id at the first
  cell write and routes the file to `{app}/t7d/{identity}/{random}`,
  covered by that class's one lifecycle expiration rule; the descriptor
  carries no `ttl` field — the id alone determines key, URL, and expiry.
  The expiry clock starts at the release copy (CopyObject creates the
  object). Expiry deletes the body only: descriptors remain, URLs 404 —
  the ordinary bodyless state. The class is fixed at upload: a descriptor
  copied into a differently-declared column keeps its baked-in class
  (columns never re-class existing files), and changing a column's `ttl`
  declaration affects only files written after the change. Extension is
  out of scope. Identity-bound keys make expiry safe: an expired id is
  re-claimable only by its original owner.
- **There is no device file store in core (2026-07-10 invisible-core
  pivot).** Core stages nothing, caches nothing, and keeps no durable
  upload state: the Blob in memory is the only client-side copy of a body,
  and the only durable client-side file-plane record is the pending-delete
  intent. An upload interrupted by a restart is lost; the committed
  descriptor syncs bodyless and its URL 404s — the ordinary documented
  state. Durable staging, upload resume, and the read-through cache belong
  to a future **opt-in offline package**, whose design inventory (store
  home per platform, layout, crash-consistency contract, lifecycle) is
  preserved in the wayfinder map's notes
  (`docs/superpowers/wayfinder/files-persistence/notes/offline-package-inventory.md`).
- **Reads are URL-only.**
  The SDK exposes no `toBlob`/`toStream`: apps `fetch(file.url())` and
  derive blobs in userland, and `url()` returns the public URL on every
  platform. Offline reads are the opt-in package's job — per-platform URL
  interceptors below the URL (a web service worker, an RN loopback
  server), feasibility-proven by the map's spike; two spike findings
  outlive the pivot as standing constraints on any future package:
  a SW intercepts only same-origin, in-scope requests, so web deployments
  wanting SW offline must expose `/files/*` on the app's own origin
  (proxy or CDN path-through); and pre-release rendering is always the
  Blob-in-hand preview (`URL.createObjectURL`) — in core that is the
  _only_ pre-release path, since nothing serves staged bodies.
- **Deletion is an explicit, authorized API — never a side effect.**
  `jazz.files.delete(fileId)` travels over the sync connection like grant
  and release. Authorization is the identity-segment comparison — the
  uploader may delete their own files; the backend/admin surface may
  delete anything; richer rules (e.g. "album owners may delete") are
  app-backend logic ending in a backend delete call. Execution is one
  idempotent DELETE against the bucket at whichever server handles the
  request — deleting an already-deleted id succeeds; a requested delete
  converges. No tombstone, no metadata, no queue: takeover of a deleted
  id is already impossible because the id's namespace belongs to its
  owner. Cell death — overwrite, null, row delete — never deletes objects,
  so there is no settle-observation machinery, copies never strand a body
  they share, and concurrent same-cell swaps are just conflicting column
  writes. The flip side is stated plainly: storage persists until an
  explicit delete or a TTL expiry. Bodyless descriptors — after deletion
  or expiry, from copies, from hand-rolled ids — are ordinary legal states
  whose URLs 404; historical and branch reads behave identically.
- **Backend contract is one abstraction — the S3-compatible API**
  (conditional presigned single PUT, multipart create/upload/abort and
  conditional complete, server-side copy, public GET, HEAD, DELETE,
  prefix-scoped lifecycle rules), covering S3, R2, minio, Tigris. Servers
  hold the object-store credentials (presign, complete, copy, delete);
  clients never see them. Dev and tests run minio or an in-process fake
  that also fakes lifecycle expiry (manually triggerable in tests).
- **TS API:** `fromBlob(blob, opts)` (create; returns a handle to write
  into cells — the id is finalized at the first cell write from the
  column's declared class; background upload; no per-call `ttl` — TTL is
  the schema's; creation input is a Blob so `size` is always known — there
  is no `fromStream`), `file.url()` (the stable public URL on every
  platform; computed synchronously and locally; no `canonical` option —
  it is retired until an opt-in package rewrites URLs),
  `jazz.files.delete(fileId)`, and an observable upload state on the
  handle: `local → uploading(progress) → released → accepted | rejected`
  (accepted/rejected are the ordinary transaction fates — nothing
  file-specific). Nothing else: reads, previews, and blob derivation are
  userland.
- **Vocabulary amendments to the Files section of the core context doc**
  (part of this work): **file id** becomes identity-bound (uploader
  identity id + CSPRNG random part; the object key derives from it);
  **upload grant** pins size only for presigning, authorizes by namespace
  comparison, leaves no record anywhere, and its lease is realized as
  bucket lifecycle expiry on the pending prefix; **body verification** is
  removed as a concept — nothing verifies bodies; **release** becomes
  HEAD + complete-multipart + copy-to-final-key + delete-pending,
  idempotent by construction; a **TTL class** entry is added (a
  deployment-declared key prefix with one lifecycle expiration rule,
  named per column in the schema and baked into the file id); the
  descriptor loses its `content hash` and `visibility` fields; the
  **publish** and **capability URL** entries are removed — every file body
  is world-readable by URL, and row policies gate only the metadata; the
  **device file store** and **interceptor** entries move out of core
  vocabulary to the opt-in offline package (2026-07-10 pivot) — core keeps
  no staged bodies, no cache, and no durable upload state.

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
    descriptor shape validation (malformed rejected; in-place edit, copy,
    and hand-rolled descriptor all accepted), grant authorization
    (own-namespace grant issued with zero bucket calls; a grant naming
    another identity's segment refused; a grant whose id names a class
    outside the deployment's set refused),
    grant/release flow (release = HEAD + complete + copy to the final —
    possibly classed — key + delete pending; idempotent release retry
    converging via HEAD; conditional PUT and conditional multipart
    completion 412 against an existing own object), lifecycle expiry
    (fake-triggered: unreleased pending objects expire; TTL'd released
    files expire under their class prefix with descriptors intact;
    permanent files untouched), and explicit deletion (uploader allowed,
    other identity denied by comparison, backend allowed; delete
    idempotent; URL 404s after; the owner re-granting their own deleted id
    succeeds — stated semantic).
  - TS: tests through the public `fromBlob`/`url()`/`delete`/upload-state
    surface against a really-served endpoint, in the style of the existing
    client/db integration tests in the TS SDK.
  - The single new seam: the S3-compatible object-store backend contract
    (including conditional writes, server-side copy, multipart abort, and
    manually-triggerable lifecycle expiry), with an in-process fake under
    the file plane in tests and minio as an optional real target.
- **Explicit scenarios to cover:** part-URL refresh past a presign window
  within a live session; in-session restart after lease expiry (fresh id,
  minted from the still-in-memory Blob); an upload interrupted by a client
  restart — the descriptor syncs bodyless and its URL 404s, with no resume
  machinery anywhere (the documented core outcome); URL 404 before release and live after the
  release copy; a file written into a TTL-declared column landing under
  its class prefix, serving with class-capped `max-age`, then 404ing after
  fake-triggered expiry while its descriptor cells stay intact; a
  descriptor copied into a differently-declared column keeping its
  baked-in class and expiry; the serving endpoint returning bytes
  with no auth for a released file, including one whose host row the
  fetching identity's read policy would hide (deliberately asserting the
  public-bytes semantic); nosniff and disposition policy on served
  responses (an HTML upload is never served inline); an independent
  transaction bypassing a held file-writing transaction, and a dependent
  one queuing behind it; offline create → later release; offline id
  minting with no prior server contact; a copied descriptor serving bytes,
  then 404ing after the uploader deletes; delete authorization (uploader
  yes, stranger no, backend yes — all by comparison); a lying release (no
  bytes uploaded) accepted and 404ing only for its own descriptor;
  concurrent same-cell swaps resolving to one winner with no object
  deleted; bodyless historical read after explicit deletion and after TTL
  expiry. (Interceptor scenarios — staged/cached serving, fetch-through,
  LRU budgets, loopback secret — moved to the opt-in offline package with
  the 2026-07-10 invisible-core amendment.)
- **Prior art:** the existing jazz-tools integration suites for
  permissions, claims, client restart, and large-blob permissions are the
  closest templates on the Rust side; the client/db `.test.ts` suites in
  the TS SDK runtime are the template on the TS side.

## Out of Scope

- **All offline machinery — the opt-in offline package (2026-07-10
  invisible-core pivot).** Durable staging, upload resume across restarts,
  the web service worker, the RN loopback server, the read-through LRU
  cache, and the core hook surface they plug into. Core stays invisible;
  any offline footprint is added willingly by the app. Design inventory
  preserved at
  `docs/superpowers/wayfinder/files-persistence/notes/offline-package-inventory.md`.
- Any SDK byte-read API (`toBlob`/`toStream` — blob derivation is
  userland; offline reads are the opt-in package's job, not an API).
- `fromStream` / unknown-length uploads (creation takes a Blob; size must
  be known at grant time).
- Per-identity rate limits and quotas on grant issuance and download
  egress (the pending-prefix TTL bounds storage abuse in v1; rate limits
  are planned future work).
- Blinding the identity segment in URLs (an HMAC'd owner prefix was
  considered; raw identity ids are accepted for v1 — pseudonymous, stated
  plainly — and blinding could be layered on later at the cost of a
  first-contact handshake).
- Per-file TTL overrides (`fromBlob`-level `ttl`) — TTL is a property of
  the column's role, declared once in the schema; deferred.
- TTL extension or shortening after creation (a re-copy would reset the
  clock; deferred), and exact per-file expiry timestamps (portable
  lifecycle rules are day-granular and prefix-scoped).
- Descriptor immutability enforcement and body verification — consciously
  removed, not deferred: they protected only an app from itself, at the
  cost of previous-value comparisons and object-store round-trips on the
  write path.
- Automatic deletion on cell death, and any GC of released-but-unreferenced
  bodies. Explicit `jazz.files.delete` and TTL classes are the only
  reclamation; apps that want tidy storage delete when their domain says
  so.
- Content hashing, content-hash dedup, and refcounting (no `hash` field;
  apps wanting tamper-evidence add their own metadata column).
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
  unguessable (mandated-CSPRNG) random part of the id as the only barrier;
  every file URL publicly carrying the uploader's identity id —
  pseudonymous, stable, and linkable across that uploader's files (apps
  that treat identity ids as sensitive must front their files or stay off
  the file plane); descriptors are conventions — `name`/`mime_type`/`size`
  are app-trusted, and a descriptor with no body behind it (hand-rolled,
  copied past a deletion, expired, lying uploader, pre-release) is an
  ordinary state whose URL 404s; only the original owner can ever re-claim
  one of their own ids after a delete or expiry — third-party takeover is
  impossible by construction, and the SDK never reuses ids, so an owner
  resurrecting their own URL (with stale CDN copies still floating) is
  their own footgun; the URL 404ing until release (apps preview from the
  Blob they hold — in core the only pre-release render path); sibling
  columns written in the same transaction as a
  fromBlob descriptor becoming visible only when the in-memory hold
  releases — after a restart the hold is gone and they sync regardless,
  bodyless if the upload never released (apps
  wanting early visibility model the file cell in its own row); storage
  persisting until an explicit delete or TTL expiry — cell death never
  reclaims it; TTL'd bodies possibly served by CDNs up to one class-length
  past expiry (`max-age` = class duration, pinned at upload); the TTL
  expiry clock starting at the release copy, not at creation; a
  descriptor's class being fixed at upload — copies into differently-
  declared columns keep it, and re-declaring a column affects only new
  files; CDN-cached copies of a deleted permanent file's bytes
  persisting until cache eviction (immutable caching makes purge
  best-effort at most); permanent local-first data loss if the creating
  app restarts or the device dies before release — core keeps no copy
  beyond the in-memory Blob (the handle's upload state is the app's
  warning surface, and the opt-in offline package is the durability
  upgrade); a manually deleted object serving 404/410 (operator
  error, not a protocol state).
- The single-PUT vs multipart size threshold, the presign window for part
  URLs, and the inline-safe type allowlist are implementation constants,
  not configuration. The TTL class set is deployment configuration; the
  schema names a class per column (`FILE('7d')` on the schema wire) and
  the server validates each granted id's class segment against the set.
- The lease window default is on the order of days, realized as the
  `pending/` prefix lifecycle expiry (day granularity), and is
  operator-facing alongside the incomplete-multipart abort rule and one
  lifecycle rule per declared TTL class.
- The design doc's "Rejected alternatives" section is required reading
  before proposing any deviation from the shapes above — with the caveats
  listed in this spec's header.
