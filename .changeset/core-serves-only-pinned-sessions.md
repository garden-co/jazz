---
"jazz-tools": patch
"jazz-napi": patch
"@jazz/rust": patch
---

A Jazz core now serves only sessions pinned to a published permissions bundle. Every user session it governs is governed by the bundle the app's backend pinned it to, never by permissions activated on the core, so a core refuses a stated session without a bundle even before the app published one. `createJazz` already pins every session it opens, so apps on `createJazz` need no change. The refusal arrives as a terminal auth failure (`AuthFailed`, no retry) instead of a retryable internal error.

Rust: `AppContext` gains `bundle`, the hex id of the published bundle a `backend_session` is pinned to; the client sends it in its socket prelude. A Rust backend that acts for a user sets it to the bundle the app published. A permissions change reaches a session when it reconnects pinned to the new bundle.

Fixed: a subscription pinned to a bundle kept that bundle's permissions only until the schema changed; it was then re-run under the core's own permissions.
