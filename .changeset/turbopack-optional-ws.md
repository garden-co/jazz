---
"jazz-tools": patch
---

Apps that don't install the optional `ws` package build and run again under Turbopack (`next dev` and `next build` with Next 16): jazz-tools now loads `ws` with a literal import inside `try`, which Turbopack leaves to fail at runtime, where only the features that need `ws` notice (on Vercel, `next()` logs once and clients use the HTTP stream). Before, Turbopack failed every route that imported jazz-tools with "Module not found: Can't resolve 'ws'".
