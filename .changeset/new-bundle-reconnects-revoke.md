---
"jazz-tools": patch
"@jazz/rust": patch
---

New permissions reach every session at once. When a backend publishes a permissions bundle the core has not seen, the core ends every session pinned to another bundle with a retryable error (`NotReady`, retry later). Each client reconnects through whichever backend serves it now and is pinned again by that backend. Publishing a bundle the core already has (a cold start, a deployment running beside another, a rollback to an earlier bundle) ends no one. Schema-only changes still reach sessions through lenses, without a reconnect.

An admin can revoke a bundle with `POST /apps/<appId>/admin/bundles/<bundle>/revoke`. Every session pinned to it ends with a terminal auth failure (`AuthFailed`, no retry), no backend can pin a session to it again, and publishing it again is refused with HTTP 409 `bundle_revoked`. Use it to stop a deployment whose permissions leak without waiting for its backends to be retired. The bundle stays listed by `GET /apps/<appId>/admin/bundles`, with its `revokedAt` time.
