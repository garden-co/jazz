---
"jazz-tools": patch
---

Drop no-op `Headers`/`Request`/`Response` polyfills from `jazz-tools/expo`.

These were imported from `react-native/Libraries/Network/fetch`, which just re-exports `global.*` — so each polyfill collapsed to `globalThis.X = globalThis.X` at best, and to `globalThis.X = undefined` if the polyfill module evaluated before RN installed its networking globals. The latter case broke any consumer that touched `Headers.prototype` (e.g. Clerk's `new Headers(...)` in `fapiClient`), which surfaced as `TypeError: Cannot read property 'prototype' of undefined`. The `fetch` and `ReadableStream` polyfills, which do real work, are kept.
