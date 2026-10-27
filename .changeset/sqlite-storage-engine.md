---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Jazz stores and queries data in SQLite on every platform: a SQLite file on Node, React Native and the server, and a SQLite file on IndexedDB or OPFS in the browser. Local stores written by alpha.59 and earlier (native, native relay, Core and browser) are refused with `UnsupportedStorageCodecs` (`unsupported_storage_codecs`) and left unchanged; delete them and resync from Core.
