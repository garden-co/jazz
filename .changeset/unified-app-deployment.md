---
"jazz-tools": patch
---

BREAKING CHANGE: `deploy` is now the single operation for publishing schemas, permissions, and migrations, both through the CLI and programmatically. Deployment now requires explicit permissions and fails before publication when a required migration is missing. Empty permissions remain valid and deny all access. First deployments, unchanged schemas, and compatible transitions do not require a reviewed migration file. The `noVerify` / `--no-verify` bypass is no longer supported.

- Rename `schema export` to `schema compile`. Update scripts to use `jazz-tools schema compile [--schema-dir <path>]`. It compiles the local `schema.ts`, prints JSON, and saves a local schema snapshot. Exporting historical schemas by hash is no longer supported; remove `--schema-hash`, the positional app ID, `--server-url`, and `--admin-secret` from these invocations. `migrations create` still fetches and saves missing historical snapshots when supplied with a hash and server credentials.
- Remove the `schema hash` and `permissions status` commands.
