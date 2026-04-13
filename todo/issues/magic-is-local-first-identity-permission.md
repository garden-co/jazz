# $isLocalFirstIdentity magic permission check

## What

Add a `$isLocalFirstIdentity: true` magic check in the permissions DSL that resolves to `true` when the current session was established via local-first auth (i.e. `claims.auth_mode === "local-first"`). This gives policy authors a first-class shorthand instead of manually matching on `"claims.auth_mode": "local-first"`.

## Priority

medium

## Notes

- Today you'd write `session.where({ "claims.auth_mode": "local-first" })` — the magic check would be sugar for this
- Mirrors patterns like `$createdBy` that already exist in the permissions DSL
- Useful for gating mutations that should only be allowed before a user has linked an external identity
