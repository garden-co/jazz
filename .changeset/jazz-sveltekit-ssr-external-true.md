---
"jazz-tools": patch
---

`jazzSvelteKit` from `jazz-tools/dev/sveltekit` now accepts the full Vite `ssr.external` shape (`true | string[]`). Previously the inline parameter type only allowed `string[]`, so `defineConfig({ plugins: [jazzSvelteKit()] })` failed to typecheck under `strict: true`. The `true` sentinel is preserved verbatim to keep externalise-everything semantics.
