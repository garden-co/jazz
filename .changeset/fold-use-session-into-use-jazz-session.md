---
"jazz-tools": patch
---

One session hook: `useJazzSession()` (Svelte `getJazzSession()`). Its `user` is the user your permissions see as `session`, so the permission-session hooks are deprecated. They still work in this release, and warn once in development:

| Deprecated                                 | Use instead                                                                       |
| ------------------------------------------ | --------------------------------------------------------------------------------- |
| React / React Native / Expo `useSession()` | `useJazzSession().user`                                                           |
| Vue `useSession()`                         | `useJazzSession().snapshot.value.user`                                            |
| Solid `useSession()`                       | `useJazzSession().snapshot().user`                                                |
| Svelte `getSession().current`              | `getJazzSession()`, then `$session.user`                                          |
| `session.user`                             | `user.accountId`                                                                  |
| `session.identity`, `session.claims`       | `user.identity`, `user.claims`                                                    |
| `session.authMode`                         | `user.kind` (`"backend"` or `"device"`); `user` is `null` for an anonymous reader |

`useJazzSession()` now also works under `<JazzClientProvider client>` (Svelte `<JazzSvelteClientProvider client>`), for a client you opened yourself. There it's `ready` with that client's user, and its session operations (`signOut()`, `refresh()`, …) reject, naming why: the client has no session to sign in or out.
