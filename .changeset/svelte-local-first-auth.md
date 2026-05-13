---
"jazz-tools": patch
---

Added `LocalFirstAuth` reactive class to `jazz-tools/svelte` for SSR-safe parity with `useLocalFirstAuth` in `jazz-tools/react`. The class exposes `secret`, `isLoading`, `login`, and `signOut`; the secret store is read inside `$effect`, so SvelteKit server renders never touch `localStorage`. `login`/`signOut` notify every live instance backed by the same store. `BrowserAuthSecretStore` now also throws a clearer error when used outside a browser environment.
