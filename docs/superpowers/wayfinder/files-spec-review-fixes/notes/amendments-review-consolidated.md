# Amendments review — consolidated (2026-07-20)

Three models reviewed the protocol- + client-plane amendments (post the
original three-model review): **GLM-5.2** (`glm` wrapper), **DeepSeek v4
Pro** and **MiMo v2.5 Pro** (both via `pi`/opencode-go). Raw outputs in
the session scratchpad (`review-glm-full.md`, `review-deepseek-full.md`,
`review-mimo-full.md`).

All three agree: the amendments are architecturally sound and resolve the
original four criticals. The core new schemes (UUIDv5 identity segment,
id grammar/parsing, grant-names-column validation, two-tier serving,
belt demotion, B′ fromBlob-mint) were each independently checked and
declared correct. What follows is what survived.

## Confirmed defects — pure corrections (applied in this pass)

- **`files-namespace` UUID never pinned** (DeepSeek C1, GLM m2). The id's
  identity segment is `UUIDv5(files-namespace, user_id)`, recomputed on
  both client (mint) and server (authorize) — if they pick different
  namespaces, every grant fails. Fix: pin `JAZZ_FILES_NAMESPACE =
UUIDv5(DNS, "files.jazz.tools")`, computed once and frozen as a literal
  in both client and server.
- **Orphan "never written" case has no `rejected` cue** (MiMo M1,
  DeepSeek m6, GLM m5 — 3-way). `rejected` only fires on a rejected
  descriptor-write transaction; a released-then-abandoned handle sits in
  `released` forever. Fix: narrow the stated cue to the rejection path;
  the abandoned-handle mechanism is decided in the upload-lifecycle
  grilling (dropping the handle cancels a pre-release upload).
- **My own `url()`-throws clause is vacuous under B′** (GLM m1). I added
  "only throwing if called on a handle whose `fromBlob` has not returned"
  — unreachable, since the handle doesn't exist until `fromBlob` returns.
  Fix: drop it.
- **Release copy has no path for files > 5 GB** (GLM M1). Single
  `CopyObject` caps ~5 GB (matrix §2) but uploads reach 5 TB. Fix:
  size-thresholded release — single copy below the cap, `UploadPartCopy`
  multipart copy above.
- **Release-flow copy step doesn't mention the best-effort dest guard**
  (DeepSeek C5, MiMo m6). Backend contract offers it; release flow reads
  unconditional. Fix: note the S3/R2 guard inline in the release step.
- **Slice-1 validation section omits the id-grammar check** it later
  says the write path performs (DeepSeek M6). Fix: add grammar to the
  validation description.
- **Slice-1 omits the serving-hardening test scenarios** the PRD lists
  (MiMo m4, GLM m6). Fix: port disposition/Content-Type-pin and
  public-bytes-despite-hidden-row into slice-1.
- **Amendment 5 still says immutability is "enforced at the bucket
  alone"** — the phrasing the by-construction rewording replaced
  (GLM m9). Fix: align to by-construction.

## New decisions surfaced — for grilling (client-plane ticket C)

- **Upload trigger + hold-on-failure + terminal-failure state**
  (DeepSeek C3/M3). B′ made upload eager; nothing says what state the
  handle enters on grant-rejection / network death / lease expiry, nor
  when the outbox hold releases on failure. A held descriptor could hang
  the transaction for the session. THE big one.
- **Which descriptor-write transaction is held** when the handle is
  written into multiple cells (DeepSeek M2) — "the transaction"
  (singular) is ambiguous.
- **"Lying release accepted" vs CopyObject-fails-on-missing-source**
  (DeepSeek C2). If nothing was PUT, the pending object doesn't exist and
  the copy errors. Either the server catches missing-source as idempotent
  success, or the "accepted" claim is struck. (Pre-existing design
  tension, surfaced by the matrix.)
- **Release retry after complete-then-crash-before-copy** (DeepSeek M1) —
  the idempotence state machine past the HEAD is unspecified.
- **Grant's "destination column" wire format + tie to `for` + schema
  race** (DeepSeek C4, GLM m4) — column ref format (stable id vs name);
  the grant column must be the handle's `for`; the fromBlob-mint vs
  grant-time schema-change race.
- **`mime_type` source** — from `blob.type`, with an empty-type policy
  (MiMo M2, GLM m4).
- **Anonymous-session `fromBlob`** (DeepSeek M4) — resolvable: anonymous
  identities are keypair-backed and DO have a `user_id`; clarify wording.

## Already scheduled (ticket C constants / ticket D hygiene)

- Canonical-JSON algorithm still unnamed (GLM m8) — ticket C; note the
  scout found NO existing engine canonicalization, so it's a real new
  decision, not a reuse.
- Inline-safe allowlist as an enumerated list (DeepSeek m2) — ticket C.
- `type/*` glob match semantics (DeepSeek m7) — ticket C (schema types).
- 302 redirect target vs `filesUrl` config (DeepSeek m4) — ticket C.
- Non-TTL `for` → TTL column = permanent file footgun (DeepSeek M5) —
  ticket C/D stated semantic.
- Delete crash-window privacy caveat (GLM m7) — ticket D (listed).
- Slice-1 header "Grant ledger" resolved-to-nothing (GLM m10) — ticket D.
- Problem Statement stale framing (MiMo m1), US 16 "abandon a device"
  (MiMo m3) — ticket D.
- Pre-release 404 CDN caching (DeepSeek m1), pending-delete best-effort
  annotation (DeepSeek m3), DELETE-204 idempotence cite (DeepSeek m5),
  Tigris REPLACE unverified (GLM m3) — ticket D / backend-contract notes.
  </content>
