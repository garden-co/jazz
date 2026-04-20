# Jazz dev server CORS rejects `Authorization` header under spec-compliant browsers

## What

The dev server spawned by `startLocalJazzServer` (and the production
`jazz-cloud-server`) both use `CorsLayer::permissive()`, which sets
`Access-Control-Allow-Headers: *`. Per the CORS spec, the `*` wildcard in
`Allow-Headers` does **not** cover the `Authorization` header — it must be
listed explicitly. Firefox now enforces this as a warning and will soon block
affected requests.

Symptom in the browser console when a JWT-authenticated client talks to the
Jazz dev server:

```
Cross-Origin Request Warning: The Same Origin Policy will disallow reading the
remote resource at http://127.0.0.1:XXXXX/sync soon. (Reason: When the
`Access-Control-Allow-Headers` is `*`, the `Authorization` header is not
covered. To include the `Authorization` header, it must be explicitly listed in
CORS header `Access-Control-Allow-Headers`).
```

Reproducer: `starters/next-betterauth` with `pnpm dev`, sign up, open the
dashboard in Firefox, check the console.

## Priority

medium

## Notes

**Affected files** (both use `CorsLayer::permissive()`):

- `crates/jazz-tools/src/routes.rs:79` — dev server (the one that actually
  serves `/sync` in `startLocalJazzServer`)
- `crates/jazz-cloud-server/src/server.rs:2670` — production cloud server

**Fix**: replace `.layer(CorsLayer::permissive())` with either:

```rust
// Option A: mirror whatever headers the preflight asked for — most permissive,
// handles all custom X-Jazz-* headers without enumerating them.
.layer(CorsLayer::permissive().allow_headers(AllowHeaders::mirror_request()))
```

```rust
// Option B: explicit list — more auditable, but needs updating when a new
// custom header is introduced.
.layer(
    CorsLayer::permissive().allow_headers([
        AUTHORIZATION, CONTENT_TYPE, ACCEPT,
        // plus the X-Jazz-* custom headers used in middleware/auth.rs:
        HeaderName::from_static("x-jazz-backend-secret"),
        HeaderName::from_static("x-jazz-session"),
        HeaderName::from_static("x-jazz-admin-secret"),
        HeaderName::from_static("x-jazz-local-mode"),
        HeaderName::from_static("x-jazz-local-token"),
        HeaderName::from_static("x-jazz-client-schema-hash"),
    ]),
)
```

Option A is less fragile. Requires importing `AllowHeaders` from
`tower_http::cors`.

**Why the wildcard fails**: the CORS spec treats `Authorization` as a "special"
forbidden/credentialed-only header. `Access-Control-Allow-Headers: *` is
defined to expand to "all headers _except_ `Authorization`". Spec reference:
<https://fetch.spec.whatwg.org/#cors-safelisted-request-header>.

**Side effects**: none expected — `mirror_request()` is strictly more
permissive than the current behaviour for `Authorization` and equivalent for
all other headers.
