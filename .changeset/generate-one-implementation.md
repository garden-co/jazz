---
"jazz-tools": patch
---

`npx jazz-tools generate [--check]` and `migrations create` pick the app's languages and write its bindings with the same code as the `jazz-tools` binary (jazz-codegen's `project`, through the new jazz-napi `generateForDir` and `migrationsCreateForDir`). An app whose directory has a `Package.swift`, Gradle build or `go.mod` no longer gets a `jazz.generated.ts` from the npm CLI. Where nothing names a language, the npm CLI still assumes TypeScript. A TS app's `jazz.app.json` may sit in `src/` or `src/lib/` below its `package.json`.
