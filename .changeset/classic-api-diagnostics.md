---
"jazz-tools": patch
---

Reject common Jazz Classic APIs with actionable Jazz 2 migration guidance. Diagnostic-only exports cover the package root and the React, React Core, React Native, Expo, Svelte, and Vue entrypoints; TypeScript rejects their use, while JavaScript throws `JAZZ_CLASSIC_API_REMOVED` on use. Supported Jazz 2 APIs remain unchanged.

Preserve the current Svelte and Vue provider names while rejecting Classic `sync` and `AccountSchema` configuration, including reactive updates and pending client creation. Rejected newly created clients are shut down before they can be exposed to descendants.
