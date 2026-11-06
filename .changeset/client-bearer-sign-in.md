---
"jazz-tools": patch
---

Node, Deno, Bun and CLI hosts can sign in through the app's backend with the app's own token: pass `bearer` to `createJazzClient` (or `createAccountManager`) from `jazz-tools/client`. It is sent as `Authorization: Bearer` on sign-in, on Jazz's checks of who is signed in, and on every sync connection, thin and local-first, so the backend's `authenticate` reads it there. Not supported yet in a browser, where the backend's cookie names the user.
