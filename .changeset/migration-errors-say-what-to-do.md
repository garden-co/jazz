---
"jazz-tools": patch
---

Schema-history errors now say what led there and what to do, instead of "is not supported yet". A deploy refused because the store's first schema version differs from the app's names both versions by the short hashes `jazz-tools` prints, and names the ways out. A store opened without the app's migrations names the usual causes (`{ migrations: "none" }`, a stale `migrations/index.ts`, React Native before alpha.61) and the fix. In development (`NODE_ENV=development`), a Jazz route whose store can't open now answers 503 with the reason, and the browser logs it once, instead of the reason showing only in the server log.
