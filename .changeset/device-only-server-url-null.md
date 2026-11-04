---
"jazz-tools": patch
---

`serverUrl: null` now keeps Jazz on this device only, as documented. It used to fall back to the same-origin `/jazz` endpoint. A device-only session opens the local-first client on a local-first account straight away, so it needs `client: () => import("jazz-tools/local-first")`. It never contacts a backend: `loginWithBackend()` and `linkWithBackend()` fail with a clear error, and a signed-out start is refused, because nothing could ever sign it in. The same holds in the React Native and Expo sessions.
