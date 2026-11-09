---
"jazz-tools": patch
---

`jazz-tools/expo`: `createJazzClient` and `<JazzProvider>` accept `bearer` in their types, like `jazz-tools/react-native`. It already reached the account manager at runtime, but TypeScript rejected it.
