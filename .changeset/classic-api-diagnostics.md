---
"jazz-tools": patch
---

Reject common Jazz Classic APIs with actionable Jazz 2 migration guidance. Diagnostic-only exports cover the package root and the React, React Core, React Native, and Expo entrypoints; TypeScript rejects their use, while JavaScript throws `JAZZ_CLASSIC_API_REMOVED` on use. Supported Jazz 2 APIs remain unchanged.
