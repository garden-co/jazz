---
"jazz-rn": patch
"jazz-tools": patch
---

The Expo web target now fails with a clear error instead of `TypeError: Cannot read properties of undefined (reading 'get')`: `jazz-tools/react-native` and `jazz-tools/expo` run only in iOS and Android builds, and the error points web at the browser client from `jazz-tools/react`. `<JazzProvider>` and `<JazzSessionProvider config>` also log a startup failure with `console.error`, so an app without an `error` or `fallback` view no longer shows a blank screen with nothing in the console.
