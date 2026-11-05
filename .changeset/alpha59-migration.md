---
"jazz-tools": patch
---

Self-hosted alpha.59 servers can move their data to this release. Download `jazz-alpha59-alpha60-export-<platform>` and `jazz-alpha59-alpha60-import-<platform>` (Linux x64/arm64, macOS arm64) from this release's GitHub assets, checked against `jazz-alpha59-alpha60-export.sha256` and `jazz-alpha59-alpha60-import.sha256`, and export the stopped server's data dir with the exporter, then run `jazz-alpha59-alpha60-import import --dump <dump> --data-dir <new dir> --app-id <app id>` and `jazz-alpha59-alpha60-import verify` with the same flags. The import keeps every row with its authors and timestamps, the accounts, and the active schema and permissions, and links each user's existing identity to the `createJazz` identity their backend will use, so returning users keep their accounts.
