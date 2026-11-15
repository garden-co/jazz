---
"jazz-tools": patch
---

An edit to `jazz.app.json` that only adds or changes indexes no longer needs a migration: `jazz-tools generate` regenerates the bindings instead of refusing with "the schema changed since the last version", and `jazz-tools migrations create` writes nothing and says "only indexes changed: no migration needed". An index changes the schema's hash but not its version or its rows.
