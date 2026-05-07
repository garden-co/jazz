---
"jazz-tools": patch
---

Drop the `fetch` polyfill from `jazz-tools/expo`.

Nothing in jazz-tools consumes a streaming `response.body`, sync runs over WebSocket, and every fetch call site uses buffered methods like `.json()`/`.text()`. The `expo/fetch` swap (and its accompanying `fetchSpecCompliant` URL/Request coercion) was working around `expo/fetch`'s string-only native bridge, but with `expo/fetch` gone, RN's default fetch — which is `whatwg-fetch` under the hood — already accepts URL and Request inputs natively. Better-auth's URL-input path therefore works without the wrapper.

The `ReadableStream` polyfill stays — it's still consumed by `runtime/file-storage.ts` for the chunked-file API, which Hermes can't service without it.
