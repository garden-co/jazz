---
"jazz-tools": patch
---

Browser apps now start on the thin client: `<JazzProvider>` (and the Vue, Svelte and Solid providers, through `createJazzSession`) opens a thin client unless told otherwise, so the first load fetches no WASM, worker or local storage. Pass `client={() => import("jazz-tools/local-first")}` to upgrade to the local-first client in the background once the page has loaded, or call `useDb().upgradeToLocalFirst(() => import("jazz-tools/local-first"))` to upgrade on demand. A browser that upgraded an account before opens it local-first straight away, so it works offline. `jazz-tools/react` (and the other framework entries) no longer import the local-first client statically; `createJazzClient`, `createAccountManager` and a provider given an explicit `config` load it on first use.
