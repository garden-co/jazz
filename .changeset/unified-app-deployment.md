---
"jazz-tools": patch
---

BREAKING CHANGE: `deploy` is now the single operation for publishing schemas, permissions, and migrations, both through the CLI and programmatically. Deployment now requires explicit permissions and fails before publication when a required migration is missing. Empty permissions remain valid and deny all access. First deployments, unchanged schemas, and compatible transitions do not require a reviewed migration file. The `noVerify` / `--no-verify` bypass is no longer supported.

Removed the public `pushSchema`, `pushPermissions`, and `pushMigration` exports, the package-root `publishStoredSchema` and `publishStoredPermissions` exports, and the `migrations push` CLI command.
