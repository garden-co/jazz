---
"jazz-tools": patch
---

An edit to `jazz.app.json` that only adds or changes single-column indexes no longer needs a migration: `jazz-tools generate` regenerates the bindings instead of refusing with "the schema changed since the last version", and `jazz-tools migrations create` writes nothing and says "only single-column indexes changed: no migration needed". A single-column index changes the schema's hash but not its version or its rows. A composite index is part of the version, so it still needs `migrations create`, which writes an empty step.
