# Grant ledger

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10)
Assignee: guido (claimed 2026-07-10)
Blocked by: (none)

## Question

How does the grant ledger persist, and who owns it?

Per [Descriptor persistence](A-descriptor-persistence.md) the ledger is
small: file id → uploader identity + granted/claimed (+ object key),
permanent so an id is never grantable twice. It is consulted at grant
issuance (id never seen), at release (mark claimed; idempotent for
retries), and at delete (uploader check). There is no verify+claim+accept
coupling and no sweep. Decide: does the ledger live at the core (edges ask
it) or at the issuing edge with core replication; entry schema and home
(`__`-prefixed raw-table namespace vs dedicated `Storage` trait methods);
idempotency of mark-claimed under retried release; the growth story for a
permanent ledger (size math at realistic upload volumes; compaction
never?); and what the delete path (ticket G) reads from it.

## Resolution (2026-07-10)

**There is no ledger.** The bucket is the only durable state; every former
ledger job is bucket-derived:

1. **Issue-time uniqueness:** HEAD the final key (`{app}[/tCLASS]/{id}`)
   and the tombstone key; either exists → refuse the grant. The pending
   phase self-guards: presigned PUT and CompleteMultipartUpload both carry
   `If-None-Match: *`, so only one body ever lands at `pending/{app}/{id}`.
2. **Release (idempotent, stateless):** HEAD final (exists → success),
   CopyObject pending→final, delete pending. Concurrent/retried releases
   converge (one pending object; same source, same destination).
3. **Delete auth:** uploader identity rides as object metadata pinned into
   the presigned PUT at grant time — blinded as
   `HMAC(server_secret, identity ‖ file_id)` because metadata is publicly
   served on a public-read bucket (opaque and per-file-unlinkable).
   `jazz.files.delete` = HEAD + compare (backend skips) + DELETE final +
   PUT zero-byte `tombstones/{app}/{id}`.
4. **Tombstones close deleted-id resurrection:** issuance checks them;
   they are permanent zero-byte objects (negligible growth). Only
   never-released ids (pending/ expired before release) remain
   re-grantable — accepted, stated semantic: don't trust a dangling
   reference that never uploaded. _(An interim file-TTL-classes feature
   made TTL-expired ids re-grantable too; it was reverted the same day —
   released ids are now always protected by final key or tombstone.)_
5. **Edges are fully stateless:** the grant response hands the multipart
   `UploadId` to the client, which persists it in its resume record; any
   edge can refresh part URLs or perform the release. No edge storage, no
   core storage, no sweep, no replication.

Growth story: zero. Server-side file-plane state: zero.

Assets: PRD + explainer amended in the same commit.

## Amendment (2026-07-10, later): identity-bound ids supersede the bucket-derived checks

A user-driven grilling ("id management brings a lot of complexity")
replaced entropy-only ids with **identity-bound ids**: the object key is
`{app}[/t{class}]/{identity}/{random}`, and grant/delete authorization is
a pure identity-segment comparison against the session. Consequences,
recorded in the PRD: **no issuance HEADs, no tombstones, no blinded
uploader metadata** (all three dissolved); third-party URL takeover is
impossible by construction; only the original owner can re-claim their own
id; TTL classes were reinstated (schema-declared, class embedded in the id
string); ids are offline-mintable from day zero; URLs publicly carry the
uploader's identity id (stated privacy semantic). "Zero server-side
file-plane state" still holds — now with zero bucket reads at issuance
too. Edges remain stateless; the client still holds the `UploadId`.
