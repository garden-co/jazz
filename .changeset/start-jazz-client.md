---
"jazz-tools": patch
---

New `startJazzClient(config)`: opens a Jazz session and returns it at once, while it starts (`status: "starting"`), so an app without a framework provider can show a loading screen and a failed-to-start screen from the session's own state. A failed start leaves the session in `error` (the cause in `snapshot.error`) and `retry()` starts it again; unlike `createJazzClient`, it doesn't close the session, so call `close()` when you are done. Available in the browser, React Native and Expo entries, and works with `<JazzProvider session>`. `createJazzClient` is unchanged: it resolves once the session started and rejects on a failed start.

Fix: a `retry()` called from a session observer the moment a failed start shows (for example an app that retries on its own) now starts again; before, it got the failed attempt back.
