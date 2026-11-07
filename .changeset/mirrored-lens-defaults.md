---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

A column a migration adds with a default now gets that default on a local-first device that heard about the new version from the server while an older tab was still open. Before, the device's existing rows read `null` for that column even after reloading onto the new version, while a fresh device read the default. The sync server now sends each added column's default with the catalogue. A device that already added such a column as `null` keeps it that way until its local store is cleared.
