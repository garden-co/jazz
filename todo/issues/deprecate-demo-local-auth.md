# Deprecate and Remove demoAuth / localMode / Synthetic Users

## What

Remove the legacy local auth path (`demoAuth`, `localMode`, `X-Jazz-Local-Mode`, `X-Jazz-Local-Token`) and the synthetic users system once local-first Ed25519 JWT auth is stable and adopted.

This is the second phase of the local-first auth redesign (see `docs/superpowers/specs/2026-04-09-local-first-auth-design.md`).

## Priority

medium

## Notes

### Client-side removals

- `resolveLocalAuthDefaults` (`packages/jazz-tools/src/runtime/local-auth.ts`)
- `localAuthMode` / `localAuthToken` fields on `AppContext`
- `LocalAuthMode` type (`"anonymous" | "demo"`)
- `X-Jazz-Local-Mode` / `X-Jazz-Local-Token` transport headers in `sync-transport.ts`
- Synthetic user system:
  - `packages/jazz-tools/src/synthetic-users.ts`
  - `packages/jazz-tools/src/synthetic-user-switcher.ts`
  - Framework-specific synthetic user components (`react/synthetic-user-switcher.tsx`, `vue/synthetic-user-switcher.ts`, `svelte/SyntheticUserSwitcher.svelte`)
  - `SyntheticUserStore`, `SyntheticUserProfile`, `ActiveSyntheticAuth` types
- `useLinkExternalIdentity` hooks (react, vue, svelte) — the linking concept goes away entirely

### Server-side removals

- `allow_anonymous` / `allow_demo` on `AuthConfig` (`crates/jazz-tools/src/middleware/auth.rs`)
- `LOCAL_MODE_HEADER` / `LOCAL_TOKEN_HEADER` constants and their handling
- Local-mode session resolution path

### Migration path

- Apps using `demoAuth` / `localMode` switch to local-first secret auth (same zero-friction DX, but with real Ed25519 identity)
- Apps using synthetic users for dev/testing switch to local-first secrets with deterministic seed fixtures
- Existing localStorage tokens become inert; no migration needed since these are ephemeral device identities

### Sequencing

- Only begin after local-first auth is fully functional and tested
- Deprecation warnings first, hard removal in a subsequent release
