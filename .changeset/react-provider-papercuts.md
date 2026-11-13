---
"jazz-tools": patch
---

Clearer errors for three easy React mistakes: `<JazzProvider serverUrl=…>` given a `fallback` prop now says to use `loading` (before, sign-in failed with "could not be cloned"); `useJazzSession()` under `<JazzProvider>` now points to `useJazzAuth()`; and a destructured `const { upgradeToLocalFirst } = db` now throws instead of resolving without upgrading.
