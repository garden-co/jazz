Adversarial review of the two specs. No files were modified; this is analysis of the provided text.

---

**CRITICAL**

`CRITICAL | PRD:Implementation Decisions (Upload flow; "Serving is hardened") + Amendment 5/6 | The grant request is specified as (file id, size), but the presigned PUT is required to pin Content-Type, Content-Disposition, and Cache-Control "to grant-time values," with a deviating client failing the upload. | Only Cache-Control is derivable from the grant inputs (class lives in the id → immutable vs max-age=class). Content-Type needs `mime_type`and Content-Disposition needs`name`; neither is in `(file id, size)` and neither is derivable from the id. The server cannot sign headers it does not possess at presign time. The implementer must invent extending the grant payload — a protocol decision the spec should have made.`

`CRITICAL | PRD:User Stories 28 vs 29 (and "Serving is hardened") | Story 28 demands downloads be a redirect with "zero policy evaluation and zero Jazz DB involvement"; story 29 demands the serving layer "enforce a Content-Disposition policy" (inline/attachment by a content-type allowlist) and add nosniff. | A zero-lookup 302 to the public object URL cannot apply a per-content-type policy — it only has the key. Baking disposition at upload instead requires mime_type at grant, which the payload omits (see above). The XSS/phishing protection in story 29 is un-implementable as written, and the two stories contradict unless disposition is pinned at PUT, which the grant payload makes impossible.`

---

**MAJOR**

`MAJOR | PRD:Problem Statement | It still sells "Developers coming from classic Jazz expect a FileStream-grade experience — create a file offline, keep working, have it upload in the background, read it back on any device." | Amendment 9 retracts cross-restart upload durability and offline reads (both moved to the opt-in package). The Problem Statement was never amended and now overpromises what core delivers; app developers will read it and expect device-spanning offline files the spec no longer provides.`

`MAJOR | PRD:Solution + Implementation Decisions (File ids) + User Story 12 | The file id's stated composition is class + identity + random (stated three times, never `app`), yet the object key and serving URL are `{app}/…`, and `url()`is claimed to be "pure local string construction from the id alone." | The`{app}` segment's source is unspecified — "from the id alone" is simply false. The implementer must decide the app comes from client/deployment context, and a descriptor rendered in a different app context would derive the wrong key with no defined behavior.`

`MAJOR | PRD:Implementation Decisions (Upload flow, step 5) | Release copies the body to the final key *before* the held descriptor transaction "enters the ordinary lane" (is submitted to row-policy evaluation). | If that transaction is then rejected, a live body sits at the final key with no referencing descriptor and — for permanent files — no TTL reclamation path. This orphan state is never addressed; the spec is explicit there is "no step six" (no acceptance, no rollback). An implementer must invent cleanup semantics for released-but-rejected files.`

`MAJOR | PRD:Implementation Decisions + User Stories 9/12 | The id is "finalized at the first cell write"; before that "there is no id or URL" and apps "preview from the Blob they hold." But `file.url()` is a documented public method on the handle. | Behavior on a pre-first-write handle (throw? empty string? lazy-finalize on read?) is unspecified. The handle lifecycle is fuzzy enough that the URL API contract is undefined for its most natural early use.`

`MAJOR | slice-1:header + ticket references | The slice names the "Grant ledger" resolution and links ticket E-claim-ledger.md, and both docs carry the "files-persistence" map/title — all leftovers from before the invisible-core pivot resolved them to no-ledger / no-persistence. | The slice body is correct, but an implementation agent following the ticket or map names will expect a ledger or persistence layer that the design explicitly eliminated. These are amendment artifacts that should be renamed or explicitly marked resolved-to-nothing.`

`MAJOR | PRD:Implementation Decisions (Backend contract) + User Story 54 | The self-collision belt relies on If-None-Match: * conditional writes, including on CompleteMultipartUpload, and claims "S3, R2, minio, and Tigris all work unchanged." | Conditional CompleteMultipartUpload and conditional-PUT semantics are not uniformly supported across these stores (R2/minio gaps are real). The "all work unchanged" portability claim is unverified and is load-bearing for the stated collision guard; an implementer cannot assume the belt exists on every backend.`

---

**MINOR**

`MINOR | PRD:Implementation Decisions (Authorization) | Only the key's identity segment is compared; the `app` segment is never validated against the session. | A session could request grants under a foreign app prefix within its own identity. Blast radius is small (own identity, own random), but app-segment authorization is an unspecified gap in the "pure comparison" model.`

`MINOR | slice-1:Implementation Decisions (Schema builder & wire) | The TS builder is said to validate `ttl` "against the deployment's class set," but that set is server-side config the client may not know offline. | Real validation happens at grant (server). Offline schema building cannot reliably validate class membership; the spec never describes how the client learns the class set, contradicting the offline-from-day-zero id-minting promise.`

`MINOR | PRD:User Story 19 + Implementation Decisions | Part-URL refresh is framed as available "within its lease" (days), but the UploadId is held in memory only (invisible-core). | Refresh works within a live process, not across a restart, even though the lease outlives typical sessions. The "within its lease" wording will mislead an implementer into building cross-restart refresh that the in-memory-only UploadId cannot support.`

`MINOR | slice-1:Implementation Decisions (last bullet) vs Testing Decisions | The slice "may stub the client upload driver," yet testing requires exercising the public fromBlob/url()/delete/upload-state surface against a really-served endpoint. | Whether the real client upload driver is in scope for slice-1 or may be stubbed is ambiguous; an agent cannot tell if TS client upload is deliverable or test scaffolding.`

`MINOR | PRD:Implementation Decisions (Serving hardening) + Further Notes | Content-Disposition may carry the descriptor's `name`as filename; no sanitization is specified. | A`name` containing CRLF/control characters could inject into the disposition header if it reaches a pinned value. Linked to CRITICAL C1/C2 — the disposition path is underspecified on top of being unrootable.`

`MINOR | PRD:User Story 16 + Further Notes | "Warn me before I abandon a device holding unreleased files" is a leftover from the device-file-store era. | Post-pivot the upload state is in-memory only and lost on restart, so the "device"/"abandon" warning can only ever fire within a single session — the story's framing overstates the observability surface.`

`MINOR | PRD:Implementation Decisions (grant abuse) + Out of Scope | Grant issuance and download egress have no rate limits/quotas in v1. | A valid identity can drive unbounded upload bandwidth/egress until the pending-prefix TTL (and future rate limits) bound it. Accepted risk, but a concrete cost vector an operator should not assume is handled.`

`MINOR | PRD:Implementation Decisions (id format) | Neither TTL class names nor identity ids are constrained to a path-safe charset. | A class name or identity id containing `/`would corrupt the`{app}/t{class}/{identity}/{random}` segmentation and silently break the identity-segment comparison the entire security model rests on.`

---

**Verdict.** The invisible-core amendment was applied thoroughly to the mechanics (offline machinery, delete intent, durable holds are all consistently cut and re-pointed at the opt-in package), but it left the _framing_ stale — the Problem Statement and several user stories still describe the FileStream-grade, cross-device experience the core no longer delivers, and the slice-1/map/ticket names still advertise a "ledger" and "persistence" that were resolved to nothing. The deeper problem is not amendment residue but a load-bearing inconsistency in the stateless plane itself: the grant payload `(file id, size)` cannot supply the `mime_type`/`name` that the mandated grant-time header pinning and the content-type disposition policy both require, which in turn makes user stories 28 and 29 directly contradict and leaves the spec's only XSS control un-implementable as written. Add to that three genuinely undefined behaviors an implementer would have to invent (the `{app}` segment's role in `url()`, `url()` on a pre-write handle, and the fate of a body released just before its descriptor transaction is rejected) and the pair is not yet build-ready. None of these are hard to fix — carry `mime_type`/`name` in the grant (or move disposition to a serving-time content-type lookup and drop the "zero evaluation" claim), state where `app` comes from, define pre-write `url()` and the released-then-rejected case, and sweep the stale Problem-Statement/ticket naming — but until they are resolved, an implementation agent will be making protocol decisions that should be the spec's.
