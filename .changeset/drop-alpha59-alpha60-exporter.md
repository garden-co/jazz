---
"jazz-tools": patch
---

Releases no longer ship the alpha.59 exporter and importer. To move a self-hosted alpha.59 server's data, use `jazz-alpha59-alpha60-export-<platform>` and `jazz-alpha59-alpha60-import-<platform>` from the [v2.0.0-alpha.65 release](https://github.com/garden-co/jazz/releases/tag/v2.0.0-alpha.65), then upgrade from alpha.65 as usual. The `jazz-tools alpha59-alpha60` subcommand of the native CLI and the `jazz-alpha59-alpha60-dump` crate are gone. `jazz-tools migrate-alpha59` now points to the alpha.65 assets.
