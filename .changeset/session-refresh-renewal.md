---
"jazz-tools": minor
---

Add `refresh(request, next)` to `createJazz`: middleware for apps whose sessions need refreshing (short access tokens, single-use refresh tokens, as with WorkOS, Supabase or Auth0). Jazz runs it on the one ordinary request a browser makes to ask who is signed in, one window at a time, so the rotated cookie can be set on its response.

Browser sync sessions on the app's cookie are now renewed in place before their `expiresAt`: the browser makes that request, and its answer carries a short-lived signed ticket that extends the open socket instead of reconnecting. A renewal that fails falls back to reconnecting at the expiry, as before. Returning the access token's `exp` from `authenticate` is now cheap, so a sign-out reaches open sessions within one access token's lifetime. Tickets are signed with a key derived from the app's secret; with `vercel()`, or `remote()` without a `secret`, each instance signs with its own random key, so a renewal that reaches another instance falls back to reconnecting. Older clients and servers keep reconnecting at the expiry, and bearer-token clients are unchanged.
