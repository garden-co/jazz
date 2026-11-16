---
"jazz-tools": patch
---

`jazz-tools generate` and `jazz-tools migrations create` write only from `schema.ts`. An app with `jazz.app.json` but no `schema.ts` is refused with "TS is the source in alpha.66: this app has jazz.app.json but no schema.ts. Run `npx jazz-tools upgrade` to write schema.ts from it (it keeps the schema hashes and bundle id), then `npx jazz-tools generate`." The standalone `jazz-tools` binary (Rust) no longer writes at all: its `generate` and `migrations create` refuse and point to `npx jazz-tools …`, while its `generate --check` still checks committed files and bindings.
