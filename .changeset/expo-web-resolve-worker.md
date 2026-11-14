---
"jazz-tools": patch
---

Fix Jazz's local-first client on Expo web, which always failed with "needs Expo's require.unstable_resolveWorker" (alpha.62 release candidates). Expo resolves that call at build time, so the property never exists at runtime, and a check for it rejected every Expo web bundle. Jazz now only makes the call, and names the setup only when the call itself fails.
