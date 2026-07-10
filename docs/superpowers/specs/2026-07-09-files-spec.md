# Files — Feature Spec

Date: 2026-07-09
Status: ready for implementation
Triage: ready-for-agent
Derived from: the approved files design (`2026-07-08-files-design.md`, sharpened
by grilling review, then hardened by a three-model adversarial review, then
simplified by the descriptor-persistence and grant-ledger grillings of
2026-07-10). That design doc remains the authority on rationale and rejected
alternatives, with these deliberate amendments in this spec:

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
5. **The descriptor is a convention, not an enforced invariant.** There is
   no descriptor immutability enforcement and no body verification anywhere.
   Only the descriptor's _shape_ is validated on write (canonical `v:1`
   JSON). Body immutability is enforced at the bucket alone (one grant per
   file id ever + conditional PUT); `name`/`mime_type`/`size` are
   app-trusted metadata. In-place edits, copies between cells, and
   hand-rolled descriptors are all legal, ordinary policy-gated writes whose
   URLs simply 404 if no body exists.
6. **Deletion is an explicit API and cleanup is bucket TTL.** Cell death
   never deletes objects. `jazz.files.delete(fileId)` — authorized for the
   uploader identity or the backend — is the one way to remove a body by
   hand. Unclaimed uploads are garbage-collected by bucket lifecycle rules
   on a `pending/` prefix, not by a server sweep.
7. **Files can carry a TTL.** A file may opt into a **TTL class** at
   creation (`fromBlob(blob, { ttl: "7d" })`) from a fixed,
   deployment-declared set; the class rides in the descriptor and the
   object key (`{app}/t7d/{id}`), and expiry is the same bucket lifecycle
   mechanism — one rule per class, no server machinery. Absent class =
   permanent.
8. **The server keeps no file-plane state — there is no grant ledger.**
   Every check derives from the bucket: grant issuance HEADs the final key
   and a tombstone key (either exists → refuse); the `pending/` phase
   self-guards via conditional writes; release is HEAD + copy + delete,
   idempotent and executable by any edge (the client holds the multipart
   `UploadId` in its resume record); delete authorization reads blinded
   uploader metadata pinned onto the object at grant time; explicit deletes
   leave a permanent zero-byte tombstone that blocks id resurrection.
   TTL-expired ids remain re-grantable — stated, accepted.

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
canonical-JSON value naming one body: required `v: 1` plus client-minted
**file id** (mandated cryptographically random, UUIDv4-grade), `name`,
`mime_type`, `size`, and an optional `ttl` (a TTL class from the
deployment's declared set; absent = permanent). At the type layer this is a
schema-level
`ColumnType::File` that lowers onto the existing text value type — the same
facade pattern JSON columns already use — so no value-enum, row-format, or
binding changes exist anywhere. The write path validates the descriptor's
_shape_ strictly (canonical form, known fields); readers parse leniently
(future versions still sync and serve `url()`, which needs only the id).
That shape check is the only file-specific validation in the system: the
descriptor is a **convention**. Its fields are app-trusted metadata,
in-place edits and copies are ordinary policy-gated writes, and the
**permission surface is exactly the host table's row policies**, same as
any column.

The **file body** — the bytes — lives on a public-read S3-compatible bucket,
keyed `{app}/{file-id}` (or `{app}/t{class}/{file-id}` for TTL'd files),
uploaded directly by the client under a server-issued **upload grant** —
and the bucket is the _only_ server-side state the file plane has. Body
immutability is enforced by the bucket, not by validation: grant issuance
refuses any id whose final object or tombstone already exists (two HEADs —
nothing is written or recorded at issuance), and every presigned PUT and
multipart completion carries a conditional-write guard, so an existing
object can never be overwritten. Uploads land under a
`pending/{app}/{file-id}` key; the grant response hands the client the
multipart `UploadId`, which the client persists in its resume record —
edges keep nothing, so any edge can refresh part URLs or perform the
release. **Release** is stateless and idempotent: HEAD the final key
(already there → success), complete the multipart, server-side-copy the
object to its final key — permanent or TTL-class prefix — and delete the
pending object. No size check, no verification, no acceptance-time role at
all. A client that lies about releasing harms only itself: its URL 404s.
Unreleased uploads are garbage-collected by a bucket lifecycle rule expiring
the `pending/` prefix (plus the native incomplete-multipart abort rule) —
the lease window _is_ the lifecycle expiry, and no server sweep exists.
TTL'd files ride the very same mechanism: one lifecycle rule per declared
class expires its prefix, with the clock starting at release (the copy is
what creates the object).

Creation is fully offline-capable: `fromBlob` stages the body in the device
file store and yields a descriptor to write into a cell; the SDK holds that
transaction at the outbox until the upload completes — a client-side
courtesy so that descriptors written through the upload path have bytes
present when they sync, not a server-enforced gate. Later, independent
writes bypass the held unit.

Reading a file _is the web_: one stable, unauthenticated, public URL —
`GET /files/{app}/{file-id}`, with the TTL class in the path for classed
files — redirecting to the public object URL. Bodies are immutable, so
permanent files carry long-lived immutable cache headers with no signature
expiry asterisk, and TTL'd files carry `Cache-Control: max-age` capped to
their class duration: trivially CDN-cacheable, cheap to serve, cheap to
bill. The SDK offers no byte-read API: apps `fetch` the URL and
derive blobs in userland. Offline reads are provided _below_ the URL by
per-platform interceptors — a Jazz-shipped service worker on web, a
loopback HTTP server inside the native module on React Native — each
serving this device's staged bodies (so `url()` renders own files
immediately, offline and pre-acceptance) and a read-through, LRU-bounded
body cache of downloads. There are no private files, no signed URLs, and no
per-download policy checks: the value Jazz provides is the integrated
experience (files as values in your own rows, synced and permission-gated
as metadata) plus offline capability, not byte-level access control. Bytes
never transit Jazz nodes and never enter Jazz storage, the sync lane, or
the content channel.

Removing a body is an explicit act — `jazz.files.delete(fileId)`,
authorized for the uploader identity (proven by blinded uploader metadata
pinned onto the object at grant time) or the backend surface — or time's: a
TTL'd file's body is deleted by the bucket when its class expires. An
explicit delete also writes a permanent zero-byte **tombstone**, which
grant issuance checks — so a deleted file's URL can never later serve
someone else's content. Cell death — overwrites, nulls, row deletes —
never deletes objects; descriptors left pointing at a deleted or expired
body are an ordinary, legal state whose URL 404s.

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
   derived from app and file id, so that rendering a file is local string
   construction — no server round-trip, no async URL step.
9. As an app developer, I want file cells to be opaque to the query layer in
   v1 (text-column semantics: whole-value equality, null checks), so that
   the query path carries no descriptor-specific code; field access can
   arrive later as read-only virtual columns without storage changes.

### Creating & uploading

10. As an end user, I want to attach a file while offline and keep using the
    app, so that flaky connectivity never blocks my work.
11. As an app developer, I want to preview a just-created file from the Blob
    I already hold (the URL goes live once the upload is released), so that
    my UI shows the file immediately without any SDK read machinery.
12. As an end user, I want my other, independent writes to keep syncing while
    a large file is still uploading, so that one slow video doesn't stall my
    whole session.
13. As an end user, I want an interrupted upload to resume from the last
    completed part after an app restart, so that a 2 GB upload doesn't start
    over because I closed the laptop.
14. As an app developer, I want the client to be able to request fresh
    presigned part URLs for an in-flight upload within its lease — from any
    edge, by presenting the `UploadId` the grant handed it — so that
    uploads longer than a presign window still complete and resume, with no
    edge affinity.
15. As an end user, I want to see upload progress and state
    (`local → uploading(progress) → released → accepted | rejected`) on the
    file handle, so that the app can show me what's pending and warn me
    before I abandon a device holding unreleased files.
16. As an app developer, I want `fromBlob` to return a usable descriptor
    handle immediately while upload continues in the background, so that I
    can write it into a cell in the same breath and my UI code stays simple.
17. As an app developer, I want uploads to go directly from the client to
    the object store under a presigned grant, so that my server's bandwidth
    bill doesn't scale with upload traffic.
18. As an app developer, I want `fromBlob(blob, { ttl: "7d" })` to pick a
    TTL class from the deployment's declared set — recorded in the
    descriptor, validated at shape-check and grant time, fixed for the
    file's life — so that ephemeral files clean themselves up without my
    code remembering to delete them.
19. As an app developer, I want the SDK to hold the transaction that writes
    a fromBlob descriptor at the outbox until the upload is released, so
    that files created through the upload path are fetchable by the time
    other devices see them — as a courtesy of the SDK, not a server gate.
20. As an app developer, I want to _choose_ early visibility when I need it
    — by putting the file cell in its own row (an attachments row) so the
    referencing row syncs immediately — so that "message text now,
    attachment when uploaded" is my app's decision, not a forced protocol
    semantic.
21. As an app developer, I want the body staged in the device file store at
    least until the writing transaction is accepted upstream (never evicted
    before then — it may be the only copy), so that upload resume works
    across restarts and my own files render offline from day one.

### Reading & serving

22. As an end user, I want every file's URL to work in an `<img>` tag, a
    `<video>` element (including Range/seeking, served natively by the
    store), or a pasted link with no auth, cookies, or headers, so that
    files behave like normal web resources.
23. As an end user, I want file bytes served from a public-read bucket
    through a CDN with no signature expiry — long-lived immutable caching
    for permanent files, `Cache-Control: max-age` capped to the class
    duration for TTL'd ones — so that media-heavy apps load fast and caches
    never sour or outlive a file's intent by more than one class length.
24. As an app developer, I want reads to be URL-only — I `fetch` the URL and
    derive blobs in userland — so that the SDK surface stays tiny and the
    read path is entirely the web platform's.
25. As an app developer, I want it stated plainly that file bytes are
    readable by anyone holding the URL — the unguessable file id is the only
    barrier — so that I never mistake the row policies (which gate metadata)
    for byte confidentiality, and keep genuinely sensitive content out of
    files or encrypt it myself.
26. As an operator, I want downloads to be a redirect to the public object
    URL with zero policy evaluation and zero Jazz DB involvement, so that
    serving cost is flat and storage/egress is what I bill.
27. As an operator, I want the serving layer to send
    `X-Content-Type-Options: nosniff` and enforce a Content-Disposition
    policy (inline only for an allowlist of render-safe types; everything
    else `attachment`), so that my files domain cannot be turned into an XSS
    or phishing host.
28. As an end user on web, I want a Jazz-shipped service worker
    intercepting `/files/*` — serving this device's staged bodies and a
    read-through cache of downloads — so that my own files render
    immediately (offline, pre-release) and any file opened once is
    readable offline, all through plain `<img>` tags.
29. As an end user on React Native, I want the native module to run a
    loopback HTTP server with the same serve-staged/serve-cached/
    proxy-and-cache behavior, with `file.url()` returning the loopback URL
    on RN, so that `<Image>`, video components, and WebViews get the same
    offline story with no per-component code.
30. As an app developer on RN, I want a canonical-URL accessor alongside
    the loopback `url()` (e.g. `url({ canonical: true })`), so that URLs I
    store, sync, or share never leak a device-local address.
31. As an operator, I want the loopback server bound to 127.0.0.1 on a
    random port with a per-boot secret path segment, so that other apps on
    the device cannot enumerate or fetch bodies through it.
32. As an app developer, I want the interceptor's body cache LRU-evicted
    under a configurable budget (staged bodies exempt until acceptance),
    so that offline availability never grows device storage without bound —
    eviction is reversible; bodies are refetchable by URL.

### Permissions & integrity

33. As an app developer, I want the host table's row policies — read,
    update, delete — to be the only permission surface for file cells,
    gating descriptor sync and every descriptor write exactly as on any
    column, so that there is nothing file-specific to learn and nothing
    that can silently disagree with row permissions.
34. As an app developer, I want file ids mandated to be minted from a
    cryptographic RNG with at least UUIDv4 entropy, so that the one value
    guarding all bytes is a real barrier, not a `Math.random()` accident.
35. As an app developer, I want grant issuance to refuse any file id whose
    final object or tombstone already exists (two bucket HEADs — no server
    record anywhere), and every presigned PUT and multipart completion to
    carry a conditional-write guard (`If-None-Match: *`), so that no
    client — however malicious — can ever overwrite an existing body:
    immutability is the bucket's law, not a validation promise.
36. As an app developer, I want release to be idempotent by construction —
    a retried release HEADs the final key, finds the object already there,
    and succeeds — so that a dropped ack never makes a completed upload
    look failed and no server has to remember an outcome.
37. As an app developer, I want a client that lies about its upload (wrong
    size in the descriptor, release without bytes) to harm only itself —
    its own URL 404s or misdescribes its own body — so that no verification
    machinery exists on anyone else's path.
38. As an operator, I want unreleased uploads garbage-collected by the
    bucket itself — a lifecycle rule expiring the `pending/` prefix after
    the lease window, plus the native incomplete-multipart abort rule — so
    that grant farming accumulates nothing and no server sweep machinery
    exists.
39. As an operator, I want the lease window (= the `pending/` lifecycle
    expiry) to be my knob trading abuse-window against resume-window, so
    that I can tune it per deployment; per-identity rate limits and quotas
    on grant issuance come later.

### Deletion & history

40. As an end user, I want deleting a file to be an explicit act —
    `jazz.files.delete(fileId)` — so that removing a body is deliberate and
    auditable, never a side effect of editing rows.
41. As an app developer, I want file deletion authorized for the uploader
    identity — proven by blinded uploader metadata
    (`HMAC(server_secret, identity ‖ file_id)`) pinned onto the object at
    grant time, opaque and unlinkable on the public bucket — and for the
    backend surface, so that users can remove what they uploaded, apps can
    moderate and clean up through their backend, richer rules stay app
    logic, and delete authorization needs no server state.
42. As an app developer, I want an explicit delete to leave a permanent
    zero-byte tombstone (`tombstones/{app}/{id}`) that grant issuance
    checks, so that a deleted file's URL can never later serve someone
    else's content.
43. As an app developer, I want it stated plainly that a TTL-expired file's
    id becomes re-grantable (lifecycle deletion writes no tombstone), so
    that I never treat a dangling TTL'd reference as trustworthy content.
44. As an operator, I want explicit deletes to be idempotent bucket
    operations (HEAD + compare + DELETE + tombstone PUT) that any server
    can execute and safely retry, so that a requested delete reliably
    converges with no deleter to coordinate and no state to reconcile.
45. As an app developer, I want cell death (overwrite, null, row delete) to
    never touch objects, so that copies and history stay coherent by
    default and storage reclamation is always the explicit API or a TTL
    class; bodies persist until someone — or time — deletes them.
46. As an end user, I want a TTL'd file's body deleted by the bucket itself
    when its class expires (the clock starting at release), with the
    descriptor cells remaining and the URL 404ing, so that ephemeral
    content leaves no storage bill behind and expiry needs no server
    machinery.
47. As an app developer, I want historical reads and branches to surface a
    descriptor at a past cut even after its body is deleted or expired, so
    that bodyless history is a defined semantic rather than a crash (the
    URL 404s; there is no SDK body read to error).
48. As an end user, I want a deleted file's URL to stop serving bytes once
    the object is deleted (with CDN-cached copies aging out on their own),
    so that deletion means withdrawal, with the CDN caveat stated honestly.
49. As an app developer, I want two devices concurrently swapping the same
    cell offline to resolve like any conflicting column write — one value
    wins, nothing file-specific happens, no body is deleted — so that
    concurrency needs no file-specific rules.

### Operations & deployment

50. As an operator, I want the backend contract to be exactly the
    S3-compatible API (conditional presigned single/multipart PUT, multipart
    create/complete/abort with conditional completion, server-side copy,
    public GET, HEAD, DELETE, and prefix-scoped lifecycle expiry +
    incomplete-multipart abort rules), so that S3, R2, minio, and Tigris
    all work unchanged.
51. As an operator, I want the bucket policy to be public GetObject with
    listing denied, so that unguessable ids actually protect bodies and the
    bucket can sit directly behind a CDN.
52. As an operator, I want to declare the deployment's TTL class set
    (recommended defaults: 1d, 7d, 30d) and get exactly one lifecycle rule
    per class alongside the `pending/` and incomplete-multipart rules, so
    that file expiry is a handful of bucket rules I can audit, not a
    service I run.
53. As an operator, I want servers to hold no file-plane state of any kind
    — no grant records, no ledger, no queues; the grant response hands the
    `UploadId` to the client, and every check derives from the bucket — so
    that edges restart, scale, and load-balance with nothing to replicate
    or reconcile.
54. As an operator, I want servers to hold the object-store credentials
    (presign, complete, copy, delete) with clients never seeing them, so
    that the only client-facing capabilities are the time-limited presigned
    URLs of their own grants.
55. As a developer running tests or local dev, I want the file plane to run
    against minio or an in-process fake (including conditional writes,
    conditional multipart completion, server-side copy, and
    manually-triggerable lifecycle expiry), so that no cloud account is
    needed to develop or CI-test file features.

## Implementation Decisions

- **The file plane is split between the sync protocol and one HTTP
  endpoint.** Grant requests, part-URL refresh, release confirmation, and
  file deletion are request/response message pairs on the client's
  already-authenticated sync connection — no second credential system. The
  only HTTP surface is the serving endpoint `GET /files/{app}/{file-id}`
  (TTL'd files: `GET /files/{app}/t{class}/{file-id}`), public and
  unauthenticated, which 302-redirects to the public object URL. The path
  mirrors the object key exactly, so serving needs no lookup — deployments
  may equally point a CDN straight at the bucket. There is no URL-mint
  operation anywhere — the URL is a pure function of the descriptor (app,
  ttl class, file id), computable locally.
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
  `attachment`. Cache policy: permanent files get long-lived
  `Cache-Control: immutable`; TTL'd files get `max-age` capped to their
  class duration — both pinned at grant time.
- **File is a schema-level column type lowering onto text — the JSON-column
  facade precedent.** A new `ColumnType::File` at the schema layer; the
  cell value is `Value::Text` carrying the descriptor as canonical JSON. No
  new value variant, no row-format change, no WASM/NAPI/RN binding change;
  the storage format version is untouched. The write path gets one
  validation branch beside the existing JSON one: strict _shape_ validation
  — canonical form (compact, sorted keys), required `v: 1`, exactly `id`,
  `name`, `mime_type`, `size` plus an optional `ttl` (one of the
  deployment's declared class names), id well-formed. Readers are lenient:
  unknown future fields/versions are tolerated and `url()` needs only the
  id (and ttl class, when present).
- **The descriptor is a convention, not an enforced invariant.** No
  immutability enforcement: in-place field edits, copies into other cells,
  and hand-rolled descriptors are ordinary writes under the ordinary update
  policy — no previous-value comparison exists on any path. `name`,
  `mime_type`, and `size` are app-trusted metadata (the same class as the
  deliberately dropped `hash`). **File ids are mandated to be minted from a
  cryptographic RNG with at least UUIDv4 entropy** — the id is the only
  byte-confidentiality barrier and the object key, so this is a protocol
  requirement, not SDK guidance. Anything queryable belongs in sibling
  columns; file cells are opaque to the query layer in v1 (text-column
  semantics), with read-only virtual/magic columns as a compatible future
  extension.
- **Permissions are the host table's row policies, unchanged and
  fail-closed, and they gate cells only.** Read policy gates descriptor
  sync. Update policy gates descriptor writes. Delete policy gates row
  deletion. File bytes are not permission-gated: anyone holding the URL can
  fetch them. Body _deletion_ has its own rule (below) because bodies
  outlive any particular cell.
- **Body immutability is the bucket's law.** Object addressing is
  `{app}/{file-id}` (permanent) or `{app}/t{class}/{file-id}` (TTL'd),
  never content-derived. Grant issuance refuses any id whose final object
  or tombstone already exists, and every presigned PUT and multipart
  completion carries `If-None-Match: *` (S3 conditional write), so the
  store itself fails any write to an occupied key with 412. No `hash`
  field, no dedup, no refcounting.
- **File TTL is a fixed set of classes, realized as key prefixes.** The
  deployment declares its class set (recommended defaults: 1d, 7d, 30d);
  permanent is the default and keeps the unprefixed key. Each class gets
  exactly one bucket lifecycle expiration rule on its prefix — the same
  mechanism that already cleans `pending/`, so file expiry adds zero server
  machinery. The class is chosen at creation (`fromBlob(blob, { ttl })`),
  recorded in the descriptor, validated against the class set at
  shape-validation and grant time, and fixed for the file's life. The
  expiry clock starts at release — the server-side copy is what creates the
  object under the class prefix. Expiry deletes the body only: descriptor
  cells remain and their URLs 404, the already-legal bodyless state.
- **There is no grant ledger — the server keeps no file-plane state, and
  every check derives from the bucket.** At grant issuance the server HEADs
  the final key and the tombstone key: either exists → refuse; nothing is
  written or recorded. The `pending/` phase self-guards: only one body can
  ever land at `pending/{app}/{file-id}` because both the presigned PUT and
  the multipart completion are conditional. Release is stateless and
  idempotent — HEAD final (already there → success), complete, copy, delete
  pending — so retried and concurrent releases converge on the same
  outcome with nothing to mark. Uploader identity for delete authorization
  rides on the object itself as blinded metadata (below). Edges hold no
  records: the grant response gives the client the `UploadId`, the client
  persists it in its resume record, and any edge can serve a part-URL
  refresh or perform the release. Transaction acceptance is entirely
  ordinary, file-blind machinery apart from descriptor shape validation.
- **Upload flow:** (1) create fully offline — `fromBlob` writes the body
  into the device file store (staging), measures `size`, mints the file id,
  returns a descriptor handle; writing it into a cell is an ordinary local
  transaction; (2) the SDK holds that transaction at the outbox until
  release — a client-side courtesy so upload-path descriptors have bytes
  when they sync — while later independent commit units bypass it (causally
  dependent writes queue behind it); (3) grant request
  `(file id, size, ttl class?)` over sync — any authenticated session may
  request grants; abuse is bounded by the `pending/` lifecycle expiry, with
  per-identity rate limits as future work; the server validates any
  requested ttl against the deployment's class set, HEADs the final and
  tombstone keys (either exists → refused; nothing is recorded), initiates
  the multipart upload where needed, and returns the pending object key
  `pending/{app}/{file-id}`, lease expiry, the `UploadId` (which the client
  persists in its resume record — no server remembers it), and conditional
  presigned URLs (single PUT below a tens-of-MB implementation constant,
  multipart above) with the blinded uploader metadata pinned in; (4) the
  client PUTs directly to the pending key, persisting completed part ETags
  locally; it may request fresh part URLs from any edge within the lease by
  presenting the `UploadId` (presign windows are hours, leases are days);
  (5) **release** over sync carries the `UploadId` and part ETag list; any
  edge HEADs the final key (already present → success), completes the
  multipart (conditional), server-side-copies `pending/{app}/{file-id}` to
  the final key — `{app}/{file-id}`, or `{app}/t{class}/{file-id}` for
  TTL'd files, whose expiry clock starts at this copy — (multipart copy
  above the single-copy size limit), and deletes the pending object —
  idempotent end to end by construction; the held transaction then enters
  the ordinary lane. There is no step six: no size check, no file-specific
  acceptance. A client that never releases leaves only a pending object the
  bucket will expire; a client that lies harms only its own URL.
- **Unreleased-upload cleanup is bucket TTL, not a server sweep.** A
  lifecycle rule expires the `pending/` prefix after the lease window
  (day-granularity, matching the "order of days" lease), and the native
  `AbortIncompleteMultipartUpload` rule covers half-finished multiparts.
  This is prefix-based and therefore portable across S3, R2, minio, and
  Tigris (R2 lifecycle cannot filter by tag). After lease expiry the SDK
  restarts the upload with a fresh id and rewrites the still-local
  descriptor. No sweep code exists anywhere.
- **The device file store holds staged bodies and a read-through cache,
  read only by the interceptors.** Staged: bodies this device created, from
  `fromBlob` until the writing transaction is accepted upstream (surviving
  restarts for resume; never evicted before acceptance — they may be the
  only copy). Cached: bodies fetched through an interceptor, keyed by file
  id (safe — bodies are immutable and 1:1), LRU-evicted under a
  configurable budget; eviction is reversible since bodies are refetchable
  by URL. The store must live where the platform's interceptor can read it
  (browser: SW-readable — OPFS/Cache API). The SDK itself never exposes
  reads from it.
- **Reads are URL-only; offline reads are interceptors below the URL.**
  The SDK exposes no `toBlob`/`toStream`: apps `fetch(file.url())` and
  derive blobs in userland. Offline capability ships in v1 as per-platform
  URL interceptors with identical behavior — serve staged (own files:
  `url()` renders immediately, offline and pre-release), else serve
  cached, else fetch through and write the cache:
  - **Web:** a Jazz-shipped service worker intercepting `/files/*`; the
    app registers it. On the very first page load (no controlling SW yet)
    requests fall through to the network — the Blob-in-hand preview
    (`URL.createObjectURL`) remains the guaranteed pre-release path.
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
- **Deletion is an explicit, authorized API — never a side effect.**
  `jazz.files.delete(fileId)` travels over the sync connection like grant
  and release. Authorization: the uploader identity, proven against
  **blinded uploader metadata** pinned onto the object by the presigned PUT
  at grant time — `HMAC(server_secret, identity ‖ file_id)`, blinded
  because `x-amz-meta-*` headers are served publicly on a public-read
  bucket (opaque, and per-file-salted so files by the same uploader aren't
  linkable) — or the backend/admin surface, which skips the check; richer
  rules (e.g. "album owners may delete") are app-backend logic ending in a
  backend delete call. Execution is direct, idempotent bucket work at
  whichever server handles the request: HEAD the object, compare the HMAC,
  DELETE the final key, and PUT a permanent zero-byte **tombstone** at
  `tombstones/{app}/{file-id}` — deleting an already-deleted id succeeds.
  Whether the two bucket writes run synchronously in the request or via a
  small retry mechanism is an implementation choice; the contract is that a
  requested delete converges. Grant issuance checks tombstones, so a
  deleted id can never be re-granted — a dead URL never comes back to life
  with someone else's content. Cell death — overwrite, null, row delete —
  never deletes objects, so there is no settle-observation machinery,
  copies never strand a body they share, and concurrent same-cell swaps are
  just conflicting column writes. The flip side is stated plainly: storage
  persists until explicitly deleted. Bodyless descriptors — after deletion,
  from copies, from hand-rolled ids — are ordinary legal states whose URLs
  404; historical and branch reads behave identically.
- **Backend contract is one abstraction — the S3-compatible API**
  (conditional presigned single PUT, multipart create/upload/abort and
  conditional complete, server-side copy, public GET, HEAD, DELETE,
  prefix-scoped lifecycle rules), covering S3, R2, minio, Tigris. Servers
  hold the object-store credentials (presign, complete, copy, delete);
  clients never see them. Dev and tests run minio or an in-process fake
  that also fakes lifecycle expiry (manually triggerable in tests).
- **TS API:** `fromBlob(blob, opts)` (create; returns a descriptor handle
  to write into cells, background upload; creation input is a Blob so
  `size` is always known — there is no `fromStream`; `opts.ttl` picks a
  TTL class from the deployment's set), `file.url()` (the
  stable public URL on web/server, the loopback URL on RN; computed
  synchronously and locally; `url({ canonical: true })` always returns the
  public one), `jazz.files.delete(fileId)`, and an observable upload state
  on the handle:
  `local → uploading(progress) → released → accepted | rejected`
  (accepted/rejected are the ordinary transaction fates — nothing
  file-specific). Nothing else: reads, previews, and blob derivation are
  userland.
- **Vocabulary amendments to the Files section of the core context doc**
  (part of this work): **upload grant** pins size only for presigning,
  leaves no server record (issuance is two bucket HEADs), and its lease is
  realized as bucket lifecycle expiry on the pending prefix; **body
  verification** is removed as a concept — nothing verifies bodies;
  **release** becomes HEAD + complete-multipart + copy-to-final-key +
  delete-pending, idempotent by construction; a **TTL class** entry is
  added (a deployment-declared expiry class realized as a key prefix with
  one lifecycle rule); a **tombstone** entry is added (a permanent
  zero-byte object recording an explicit delete; grant issuance refuses
  tombstoned ids); the descriptor loses its `content hash` and `visibility`
  fields and gains an optional `ttl`; the **publish** and **capability
  URL** entries are removed — every file body is world-readable by URL, and
  row policies gate only the metadata.

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
    descriptor shape validation (malformed rejected; a `ttl` outside the
    deployment's class set rejected; in-place edit, copy, and hand-rolled
    descriptor all accepted), grant/release flow (release = HEAD + complete
    - copy to the final key, permanent or class prefix, + delete pending;
      idempotent release retry converging via HEAD; issuance refuses an id
      whose final object or tombstone exists; conditional PUT and conditional
      multipart completion 412 against an existing object), lifecycle expiry
      of unreleased uploads and of TTL'd files (fake-triggered; permanent and
      unexpired files untouched; an expired TTL id re-grantable — the stated
      semantic), and explicit deletion (uploader allowed via metadata HMAC,
      other identity denied on HMAC mismatch, backend allowed; tombstone
      written; the tombstoned id refused at grant; delete idempotent; URL
      404s after).
  - TS: tests through the public `fromBlob`/`url()`/`delete`/upload-state
    surface against a really-served endpoint, in the style of the existing
    client/db integration tests in the TS SDK.
  - The single new seam: the S3-compatible object-store backend contract
    (including conditional writes, server-side copy, multipart abort, and
    manually-triggerable lifecycle expiry), with an in-process fake under
    the file plane in tests and minio as an optional real target.
- **Explicit scenarios to cover:** resume-after-restart within the lease,
  including part-URL refresh past a presign window; restart after lease
  expiry (fresh id, restart); URL 404 before release and live after the
  release copy; the serving endpoint returning bytes with no auth for a
  released file, including one whose host row the fetching identity's read
  policy would hide (deliberately asserting the public-bytes semantic);
  nosniff and disposition policy on served responses (an HTML upload is
  never served inline); an independent transaction bypassing a held
  file-writing transaction, and a dependent one queuing behind it; offline
  create → later release; a copied descriptor serving bytes, then 404ing
  after the uploader deletes; delete authorization (uploader yes, stranger
  no via HMAC mismatch, backend yes); a deleted id refused at re-grant
  (tombstone) while a TTL-expired id re-grants successfully (stated
  semantic); a TTL'd file landing under its class prefix, serving
  with `max-age` capped to the class, and 404ing after fake-triggered
  expiry while its descriptor cells remain; a lying release (no bytes
  uploaded) accepted and
  404ing only for its own descriptor; concurrent same-cell swaps resolving
  to one winner with no object deleted; bodyless historical read after
  explicit deletion; interceptor behavior (web SW and RN loopback): staged
  body served offline and pre-release, cached body served offline after
  one online read, fetch-through writing the cache, LRU eviction
  respecting the budget and never evicting pre-acceptance staged bodies,
  and the RN loopback refusing requests without the per-boot secret path.
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
  be known at grant time).
- Per-identity rate limits and quotas on grant issuance and download
  egress (the pending-prefix TTL bounds storage abuse in v1; rate limits
  are planned future work).
- Descriptor immutability enforcement and body verification — consciously
  removed, not deferred: they protected only an app from itself, at the
  cost of previous-value comparisons and object-store round-trips on the
  write path.
- Automatic deletion on cell death, and any GC of released-but-unreferenced
  bodies. Explicit `jazz.files.delete` and TTL-class expiry are the only
  reclamation; apps that want tidy storage delete when their domain says
  so or create with a `ttl`.
- Extending or shortening a file's TTL after creation — a re-copy would
  reset the lifecycle clock; deferred until a real need appears. Exact
  per-file expiry timestamps likewise (they need scheduled-delete
  machinery; classes ride bucket rules).
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
  unguessable (mandated-CSPRNG) file id as the only barrier; descriptors
  are conventions — `name`/`mime_type`/`size` are app-trusted, and a
  descriptor with no body behind it (hand-rolled, copied past a deletion,
  lying uploader, pre-release) is an ordinary state whose URL 404s; a
  TTL-expired file's id is re-grantable by anyone (lifecycle deletion
  writes no tombstone) — a dangling TTL'd reference is not trustworthy
  content, while deleted permanent ids are tombstoned and never come back;
  the blinded uploader metadata is publicly visible on the object (opaque
  and per-file-unlinkable by construction); the
  URL 404ing until release (apps preview from the Blob they hold); sibling
  columns written in the same transaction as a fromBlob descriptor
  becoming visible only when the hold releases (apps wanting early
  visibility model the file cell in its own row); storage persisting until
  an explicit delete or TTL-class expiry — cell death never reclaims it; a
  TTL'd file's expiry clock starting at release, not creation, and its
  CDN-cached copies serving up to one class-length past expiry (`max-age`
  is capped to the class, not synchronized with it — apps needing tighter
  bounds pick a shorter class); on web, no SW
  controls the very first page load, so requests then fall through to the
  network and the Blob-in-hand preview remains the guaranteed pre-release
  path; CDN-cached copies of a deleted file's bytes persisting until cache
  eviction (immutable caching makes purge best-effort at most); permanent
  local-first data loss if the creating device dies before release (the
  handle's upload state is the app's warning surface); a manually deleted
  object serving 404/410 (operator error, not a protocol state).
- The single-PUT vs multipart size threshold, the presign window for part
  URLs, and the inline-safe type allowlist are implementation constants,
  not configuration.
- The lease window default is on the order of days, realized as the
  `pending/` prefix lifecycle expiry (day granularity), and is
  operator-facing alongside the incomplete-multipart abort rule and the
  per-class TTL rules. The TTL class set (recommended defaults: 1d, 7d,
  30d) is deployment configuration, not an implementation constant;
  lifecycle granularity makes classes day-grained.
- The design doc's "Rejected alternatives" section is required reading
  before proposing any deviation from the shapes above — with the caveats
  listed in this spec's header.
