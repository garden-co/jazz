# Cross-device sign-in orphans anonymous data

## What

When a user has already signed up on device A (pinning their BetterAuth `user.id` to Jazz user_id `X`), opens the app on device B, creates data under a fresh anonymous Jazz user_id `Y`, then signs in to BetterAuth on device B — the subsequent JWT carries `sub: X`, and any rows on the server created under `Y` become orphaned. They're still on the server but inaccessible to the signed-in principal, since all policies match on `$createdBy: session.user_id` (or equivalent).

## Priority

medium

## Notes

- Architecturally, the current local-first auth model is 1 external account ↔ 1 Jazz id, pinned at sign-up via `databaseHooks.user.create.before` in the BetterAuth hooks. Sign-in always authenticates as that one pinned id.
- There is no link table, no `jazz_principal_id` carrying a secondary id, and no server-side concept of "multiple linked identities per principal". See `packages/jazz-tools/src/backend/request-auth.ts` `resolveRequestSession` — the session's `user_id` comes directly from whatever JWT is presented.
- The legacy `useLinkExternalIdentity` in `packages/jazz-tools/src/{react,svelte,vue}/use-link-external-identity.ts` targets the old synthetic-users/demoAuth path and is being removed (`todo/issues/deprecate-demo-local-auth.md`). It is not a solution.
- Possible directions worth considering:
  - A server-side identity-link table mapping `principal_jazz_id → [linked_jazz_ids]`, plus a mechanism for the server to treat policies as matching _any_ linked id for reads/writes by that principal.
  - A client-side "re-parent" flow on sign-in that re-inserts rows owned by the anonymous id under the signed-in id, before the anonymous identity is discarded. Only works for data the client still has a reference to.
  - Surfacing the orphaned id to the user ("you have data from a previous anonymous session; import it?") and letting them opt in.
- Surfaces in the `next-localfirst` starter as a documented known limitation. Starter does not attempt to handle it.
