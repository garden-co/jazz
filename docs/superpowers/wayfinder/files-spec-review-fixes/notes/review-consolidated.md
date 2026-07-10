# Three-model spec review — consolidated findings (2026-07-10)

Reviewers: Claude Opus (agent), GLM-5.2 (`glm` wrapper), DeepSeek v4 Pro
(`pi` + opencode-go). Raw reports: [review-opus.md](review-opus.md),
[review-glm.md](review-glm.md), [review-deepseek.md](review-deepseek.md).
Subject: `docs/superpowers/specs/2026-07-09-files-spec.md` (PRD) +
`docs/superpowers/specs/2026-07-10-files-persistence-spec.md` (slice-1),
post-invisible-core amendment.

Unanimous verdict: architecturally sound, not build-ready — the gaps are
specification gaps, not design flaws.

## Consensus criticals

1. **Grant payload can't support serving hardening.** Grant is
   `(file id, size)` but the presigned PUT must pin
   `Content-Type`/`Content-Disposition` from `mime_type`/`name`, which it
   never receives. GLM: US 28 (zero-lookup 302) vs US 29 (disposition
   policy) directly contradict — the only XSS control is un-implementable
   as written. (Opus critical, GLM critical ×2, DeepSeek adjacent)
2. **File id wire format undefined and security-load-bearing.** No
   encoding/delimiter/charset rules; identity-vs-class segment parsing
   ambiguous (`t` sentinel); "well-formed" validation unspecified; the
   identity-segment comparison rests on unambiguous parse. (Opus, DeepSeek,
   GLM)
3. **Conditional `CompleteMultipartUpload` portability unverified** across
   S3/R2/minio/Tigris; the self-collision belt may not exist on some
   backends; "all work unchanged" possibly false. (all three)
4. **`{app}` segment and `url()` base hand-waved.** The id is
   class+identity+random — `{app}` isn't in it — yet keys/URLs start with
   it and `url()` claims "from the id alone"; base host differs per
   deployment mode; app-segment authorization unspecified. (GLM, DeepSeek,
   Opus)

## Consensus majors

- Session→identity binding and backend-surface recognition assumed, never
  specified. (Opus ×2, DeepSeek)
- Released-then-rejected orphan body: release copies to final key before
  the transaction's fate; rejection strands a permanent body. (GLM major,
  Opus minor)
- Cross-doc contradiction: slice-1 says TS builder validates `ttl` against
  the deployment class set; PRD says grant-time only. (Opus, GLM)
- Slice-1 "may stub the client upload driver" circular vs PRD TS API
  deliverables and slice-1's own TS test seam. (Opus, GLM)
- Final-key immutability not actually "at the bucket": release CopyObject
  unguarded; owner self-resurrection overwrites. (Opus)
- `nosniff` silently lost in CDN-straight-at-bucket mode. (Opus)
- `CopyObject` must be mandated to preserve pinned metadata (S3 copy can
  replace it). (DeepSeek)
- Canonical-JSON algorithm never named despite "digests are byte-stable"
  (name RFC 8785 or "same as JSON columns"). (DeepSeek)
- Stale framing: Problem Statement still promises FileStream-grade
  cross-device offline; "restarts the upload with a fresh id / rewrites
  the still-local descriptor" reads as cross-restart resume; part-URL
  refresh "within its lease" implies cross-restart; US 16
  "abandon a device" overstates. (GLM major + all three minor)
- Release message shape: PRD's step 5 omits the file id that slice-1's
  message includes; single-PUT release path (no UploadId/parts)
  unspecified; small-file 3-op write amplification unstated. (DeepSeek
  critical, Opus minor)
- Upload start trigger ambiguous: can't start at `fromBlob` (class unknown
  until first cell write). (Opus)
- Delete crash window is a privacy semantic: no durable intent means a
  crash leaves world-readable bytes the user believes withdrawn — state
  it. (Opus)
- Copy-descriptor-equals-byte-access belongs in stated semantics.
  (DeepSeek)

## Minors (decision-bearing)

- Constants unpinned: lease default, single-PUT/multipart threshold,
  inline-safe type allowlist enumeration, TTL class-set config surface.
  (DeepSeek, GLM)
- `url()` on a pre-first-write handle undefined (throw/null/lazy?). (GLM)
- Outbox hold's "causally dependent" undefined. (DeepSeek)
- Class-set evolution: removing/renaming a class orphans `t{old}/` bodies
  into de-facto permanence. (Opus)
- Write-path validator accepts bogus class segments in hand-rolled ids
  (class checked only at grant) — scope of "well-formed" fuzzy. (Opus)
- Untrusted `mime_type` pins the served Content-Type (lying client serves
  HTML as image/png; nosniff mitigates). (DeepSeek)
- `Content-Disposition` filename from descriptor `name` unsanitized
  (CRLF/control chars). (GLM)
- `fromBlob` input type across platforms (web Blob / Node Buffer / RN).
  (DeepSeek)

## Minors (doc-hygiene / stated-semantics additions)

- TTL day-granularity variance (±24h effective lifetime) as a stated
  semantic. (Opus, DeepSeek)
- First-write-wins class footgun (permanent column holding an expiring
  body) deserves a louder callout. (Opus)
- Slice-1 omits the PRD's serving-hardening test scenarios
  (nosniff/disposition, public-bytes-despite-hidden-row). (Opus)
- Slice-1 header still names "Grant ledger" ticket / "files-persistence"
  map without marking them resolved-to-nothing. (GLM)
- Rate-limit absence as an operator cost vector (accepted, but say so).
  (GLM)
- PRD/slice-1 story numbering has no mapping. (DeepSeek — accepted noise,
  likely won't fix)
