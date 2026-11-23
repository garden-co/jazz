---
"jazz-tools": patch
---

**Breaking:** the permission-session hooks deprecated in alpha.66 are removed. `useSession()` (React, React Native, Expo, Vue, Solid) and Svelte's `getSession()` are no longer exported. Read the session's user from the session hook instead:

| Removed                                    | Use instead                                              |
| ------------------------------------------ | -------------------------------------------------------- |
| React / React Native / Expo `useSession()` | `useJazzSession().user`                                  |
| Vue `useSession()`                         | `useJazzSession().snapshot.value.user`                   |
| Solid `useSession()`                       | `useJazzSession().snapshot().user`                       |
| Svelte `getSession().current`              | `const session = getJazzSession()`, then `$session.user` |

On the user, `session.user` is `user.accountId`; `identity` and `claims` keep their names; `session.authMode` is `user.kind` (`"backend"` or `"device"`), with `user === null` for an anonymous reader. The Svelte "used outside a provider" error now reads `getDb must be used within <JazzProvider> or <JazzSvelteClientProvider>`.
