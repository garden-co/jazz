---
"jazz-tools": patch
---

Start local-first sooner on a reload: a browser session that will open local-first straight away (`localFirst: "now"`, or an account that upgraded before) now starts the page's WASM and the last store's worker before it resolves the account, instead of after. The early worker only receives the existing lease probe and prefetches its WASM; it admits nothing and opens no store. The account the session then opens goes through the same lease and admission as before, so another account's store is never opened for it. Sessions with `clearOnSignOut` start only the page's WASM early.
