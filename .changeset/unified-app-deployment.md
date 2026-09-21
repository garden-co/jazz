---
"jazz-tools": patch
---

BREAKING CHANGE: Simplify the `jazz-tools` CLI.

- Rename `schema export` to `schema compile`. Update scripts to use `jazz-tools schema compile [--schema-dir <path>]`. It compiles the local `schema.ts`, prints JSON, and saves a local schema snapshot. Exporting historical schemas by hash is no longer supported; remove `--schema-hash`, the positional app ID, `--server-url`, and `--admin-secret` from these invocations. `migrations create` still fetches and saves missing historical snapshots when supplied with a hash and server credentials.
- Remove the `schema hash` and `permissions status` commands.
