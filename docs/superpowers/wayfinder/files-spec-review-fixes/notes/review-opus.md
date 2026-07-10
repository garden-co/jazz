# Opus review — raw report (2026-07-10)

Reviewer: Claude Opus agent, adversarial spec review of the files PRD
(`2026-07-09-files-spec.md`) + slice-1 (`2026-07-10-files-persistence-spec.md`).

## Findings

`SEVERITY | doc §section | issue | why it matters`

**critical | PRD §Solution + §Impl.Decisions "Upload flow" / slice §File-plane protocol | Grant request is `(file id, size)` in BOTH docs, yet the server must pin `Content-Type`, `Content-Disposition`, and `Cache-Control` into the presigned PUT at grant time, and must apply the inline-vs-attachment disposition allowlist by MIME type. The grant never receives `mime_type`. | The server cannot pin a Content-Type it was never told, so either the entire content-type/disposition hardening (US 29) is unbuildable as specified, or the grant message shape is wrong. Load-bearing contradiction between the message shape and the security hardening.**

**critical | PRD §Solution/§Impl "File ids" / slice §Id format | The "file id" is described as "one opaque string" that separately "renders as path segments," but its concrete encoding — delimiters, how `{app}`/`t{class}`/`{identity}`/`{random}` are packed into the id string, and what the write-path "id well-formed" check actually validates — is never defined. | Strict write-path shape validator must reject ids that are not "well-formed," but well-formedness is undefined. Two implementation agents will pick incompatible encodings; `url()` depends on it. Blocks the column-validation deliverable.**

**major | Key grammar `{app}[/t{class}]/{identity}/{random}` disambiguates classed vs classless keys by the `t` sentinel on segment 2, but nothing guarantees an identity id never begins with `t` (or that segments never contain `/`). | A classless key whose identity begins with `t` mis-parses as a TTL class, corrupting expiry routing and the identity-segment authorization comparison. Must be segment-count + no-`/` guarantee; neither stated.**

**major | PRD §Amendment 5 vs §Impl "Upload flow" step 5 | "Body immutability is enforced at the bucket alone," but `If-None-Match:*` sits only on PUT-to-pending and multipart completion — the release `CopyObject` to the FINAL key is unguarded. | Owner self-resurrection overwrites the existing final object via the unconditional copy. Final-key immutability is enforced only by SDK fresh-randoms, NOT "at the bucket." Stated invariant false as mechanized.**

**major | slice §"Schema builder & wire" vs PRD §"File is a schema-level column type" | Slice says the TS builder validates `ttl` "against the deployment's class set"; PRD says class is validated "at grant time, not here." | Direct cross-doc contradiction; client can't know the server-side class set at schema-definition time.**

**major | The authorization model rests on "the requesting session's identity," but neither doc states how the sync connection binds a session to an authenticated identity id, nor whether "identity" is account-level or device-level. | If a session can assert an arbitrary identity id, authorization collapses. "The uploader may delete their own files" ambiguous across devices of one account.**

**major | "The backend surface skips/bypasses" the identity comparison, but no doc specifies how the server recognizes a request as the privileged backend surface. | Security-critical bypass with unspecified recognition mechanism.**

**major | Conditional `CompleteMultipartUpload` (`If-None-Match:*`) mandated across S3, R2, minio, Tigris. | Recent, unevenly-supported feature; "all work unchanged" portability claim may be false; the belt is load-bearing for the self-collision guard.**

**major | `X-Content-Type-Options: nosniff` added by "the serving layer/CDN," but in the endorsed "CDN straight at bucket" mode S3 will not emit nosniff from object metadata. | Anti-XSS header silently disappears in that deployment mode; only Disposition/Cache-Control survive as S3 system metadata.**

**major | `jazz.files.delete()` keeps no durable intent; a crash between call and confirmation silently leaves the body. | For a public-by-URL feature, silent delete no-op on crash is a privacy surprise: user believes content withdrawn while it persists. Not flagged as a data-retention risk.**

**major | slice "may stub the client upload driver" vs PRD §TS API | PRD lists `fromBlob`/upload-state/in-memory hold as delivered API; slice permits stubbing the driver and doesn't test the state machine or hold. | Implementer can't tell how much TS upload surface ships in slice 1; the hold IS the driver behavior — circular scope.**

**major | Upload trigger ambiguity: id (hence key, hence grant) can't exist until first cell write reveals the column's class, yet US 17 reads as if `fromBlob` starts upload immediately. | Upload MUST wait for the first cell write; needs stating crisply.**

**minor | "the SDK restarts the upload with a fresh id and rewrites the still-local descriptor" — last surviving resume-flavored fragment; only holds in-session; re-minting a "finalized" id underspecified.**

**minor | A body can reach `released` while its transaction is `rejected` → orphan body at final key with no referencing descriptor and no reclamation for permanent files. | Silent storage leak on release-then-reject; not called out.**

**minor | Write-path validator accepts arbitrary `t{class}` segments (class-set checked only at grant); hand-rolled descriptors can carry bogus classes; `url()` then points under a nonexistent lifecycle prefix.**

**minor | Removing a class from the deployment set orphans existing `t{oldclass}/` bodies — lifecycle rule vanishes, "ephemeral" bodies become permanent. | Operational footgun; undiscussed.**

**minor | Release message `(file id, UploadId, part ETags)` vs single-PUT uploads (no UploadId/parts, no CompleteMultipartUpload); every small file still pays PUT+copy+delete (3 ops). | Divergent release shapes unspecified; write amplification unstated.**

**minor | Day-granular UTC lifecycle rules: a `1d` file may live ~2 days or die within hours of wall-clock expectations. | Distinct from the CDN max-age caveat; will surprise developers.**

**minor | Only the identity segment is authorized at grant; `{app}` segment authorization and how the client learns `{app}` unspecified. | Multi-tenant-by-app buckets would be requestable cross-app within one's own identity.**

**minor | First-write-wins class: writing a handle to a `ttl:1d` column then a `permanent` column leaves the permanent column holding a body that expires in 1 day. | Stated as accepted but deserves a louder callout.**

**minor | Slice-1 omits the PRD's serving-hardening scenarios (nosniff/disposition, HTML-never-inline, public-bytes-despite-hidden-row). | Security-relevant serving behavior under-tested; hardening can regress silently.**

## Verdict

Not yet ready for hands-off implementation, though close; the invisible-core
pivot is threaded through cleanly. Blockers are concrete protocol gaps:
grant omits `mime_type` (critical); file-id encoding/validation undefined
(critical); release copy unguarded vs "immutability at the bucket" (major);
ttl-validation cross-doc contradiction (major); session-identity and
backend-surface authentication assumed (major). Conditional-multipart
portability and nosniff-on-CDN-direct are real risks. Slice-1/PRD boundary
on the upload driver needs pinning. Resolve the two criticals and the auth
foundation before an agent starts; the stateless plane and column facade
are coherent and buildable once decisions are written down.
