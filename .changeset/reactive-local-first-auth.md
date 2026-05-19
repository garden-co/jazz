---
"jazz-tools": patch
---

Added reactive local-first auth helpers for Svelte and Vue, matching `useLocalFirstAuth` in `jazz-tools/react`:

- **Svelte:** new `LocalFirstAuth` reactive class in `jazz-tools/svelte`. Exposes `secret`, `isLoading`, `login`, and `signOut`; the secret store is read inside `$effect`, so SvelteKit server renders never touch `localStorage`.
- **Vue:** new `useLocalFirstAuth()` composable in `jazz-tools/vue`. Returns `Ref<string | null>` and `Ref<boolean>` for the secret/loading state plus async `login`/`signOut`; gated on `typeof window` so SSR setup never touches `localStorage`.

In both frameworks `login`/`signOut` notify every live instance backed by the same store, and a `console.warn` surfaces secret-store failures that previously fell through silently. `BrowserAuthSecretStore` now also throws a clearer error when used outside a browser environment, with `localStorage` resolved lazily so server-side imports of the module-level singleton don't break.

Note for anyone copying the documented Svelte backup/restore snippets: their signatures changed to take a `LocalFirstAuth` instance (e.g. `createRecoveryPhraseRestore(auth)` returning a callback) so they can call `auth.login()` directly instead of `BrowserAuthSecretStore.saveSecret()` + `location.reload()`.
