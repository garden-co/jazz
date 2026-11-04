---
"jazz-tools": patch
---

Self-hosted alpha.59 servers can move their data to this release. Download `jazz-alpha59-export-<platform>` (Linux x64/arm64, macOS arm64) from this release's GitHub assets, checked against `jazz-alpha59-export.sha256`, and export the stopped server's data dir with it, then run `jazz-tools migrate-alpha59 import --dump <dump> --data-dir <new dir> --app-id <app id>` and `jazz-tools migrate-alpha59 verify` with the same flags. The import keeps every row with its authors and timestamps, the accounts, and the active schema and permissions, and links each user's existing identity to the `createJazz` identity their backend will use, so returning users keep their accounts.
