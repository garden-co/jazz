---
"jazz-tools": patch
---

`jazz.app.json` declares computed fields (`"computed": { "<field>": { "type", "from" } }` per table) and transforms (top-level `"transforms": { "<name>": { "stored", "view" } }`, named by a column's `"transform"`), replacing `tsOnly`. A file from alpha.63 with `tsOnly` or `slices` loads with a warning that they are ignored. Every loader refuses a file that still holds a `"TODO: …"` string, listing each by its JSON path.

New `jazz-codegen` crate: typed bindings generated from `jazz.app.json` (`jazz.generated.ts`, embedding the compiled schema so runtimes never parse the readable file), and `migrations create` in Rust: it moves the version the committed bindings were generated from into `history` and writes the migration, leaving `"TODO: …"` where only the author can decide. jazz-napi exposes `generate`, `compileApp`, `schemaSourceHash`, `migrationsCreate`, `validateBindings` and `loadBackendApp`; `loadAppArtifact` also returns the permissions, the deployment body and warnings. The `jazz-tools` binary gains `generate [--check]` and `migrations create --name`, so apps without Node need nothing else.
