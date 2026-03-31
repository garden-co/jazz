# Backend Context Helper Cleanup — TODO (MVP)

This follow-up tracks the remaining API/docs cleanup after switching the server-connected TypeScript
docs example to `context.asBackend()`.

## Goal

Make the `createJazzContext(...)` helper surface legible and intentional:

- `context.asBackend()` for server-owned work against an upstream Jazz server
- `context.forRequest(req)` / `context.forSession(session)` for user-scoped work
- `context.db()` only for clearly documented embedded or local-only runtime usage, if it remains

## Scope

- Audit backend docs and examples so server-connected flows do not present `context.db()` as the
  default or as interchangeable with `context.asBackend()`.
- Decide whether `context.db()` should remain public, be renamed, or be documented explicitly as an
  embedded-runtime helper.
- Split example coverage so server-connected and embedded/local runtime patterns are demonstrated
  separately instead of being blended together.
- Add focused tests around helper selection and startup expectations for each supported pattern.

Out of scope:

- Auth-provider changes or JWT format changes.
- New backend permission semantics.

## Desired Semantics

### Server-connected backend

- `context.asBackend()` is the default unscoped helper for trusted server-owned work.
- `context.forRequest(req)` is the default helper for request handlers acting on behalf of callers.
- `context.forSession(session)` is the explicit impersonation/testing path.

### Embedded or local-only runtime

- If `context.db()` remains, docs should describe it as the embedded/local-runtime path only.
- Embedded usage should not be confused with upstream-connected backend auth.

## Invariants

- Server-connected docs/examples must not imply that `context.db()` and `context.asBackend()` are
  equivalent.
- Request-scoped examples must continue to use `forRequest(req)` or `forSession(session)`.
- Any remaining `context.db()` examples must be obviously local-only.

## Testing Strategy

- Keep a targeted boot test for the upstream-backed docs server example.
- Add or preserve separate coverage for embedded/local runtime usage if `context.db()` remains part
  of the public API.
- Avoid relying on a single example to cover both helper patterns.
