---
"jazz-tools": patch
---

Generated bindings in every language (`jazz.generated.ts`, `src/jazz_app.rs`, `JazzApp.swift`, `JazzApp.kt`, `jazz_app.go`) now say "Edit schema.ts and run `npx jazz-tools generate`" instead of "Regenerate it after changing jazz.app.json". Bindings with the old header stay current for `generate --check`; the next `generate` rewrites them.
