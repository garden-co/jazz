---
"jazz-tools": patch
---

Running a `jazz-tools` command in a directory with `jazz.app.json` but no `schema.ts` no longer points apps in other languages at the standalone `jazz-tools` binary. It now says that `npx jazz-tools upgrade` converts an alpha.65 app in any language, and that from alpha.66 every language edits `schema.ts` beside its manifest and runs `npx jazz-tools generate`.
