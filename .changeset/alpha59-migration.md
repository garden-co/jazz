---
"jazz-tools": patch
---

Self-hosted alpha.59 servers can move their data to this release. Download `jazz-alpha59-export` from this release's GitHub assets and export the stopped server's data dir with it, then run `jazz-tools migrate-alpha59 import --dump <dump> --data-dir <new dir> --app-id <app id>` and `jazz-tools migrate-alpha59 verify` with the same flags. The import keeps every row with its authors and timestamps, the accounts, and the active schema and permissions, and links each user's existing identity to the `createJazz` identity their backend will use, so returning users keep their accounts.
