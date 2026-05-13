---
"jazz-tools": patch
---

Added `LocalFirstAuth` reactive class to `jazz-tools/svelte` for SSR-safe parity with `useLocalFirstAuth` in `jazz-tools/react`. The class exposes `secret`, `isLoading`, `login`, and `signOut`; the secret store is read inside `$effect`, so SvelteKit server renders never touch `localStorage`. `login`/`signOut` notify every live instance backed by the same store, and a `console.warn` surfaces secret-store failures that previously fell through silently. `BrowserAuthSecretStore` now also throws a clearer error when used outside a browser environment.

Note for anyone copying the documented Svelte backup/restore snippets: their signatures changed to take a `LocalFirstAuth` instance (e.g. `createRecoveryPhraseRestore(auth)` returning a callback) so they can call `auth.login()` directly instead of `BrowserAuthSecretStore.saveSecret()` + `location.reload()`.
