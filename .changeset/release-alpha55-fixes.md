---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
"create-jazz": patch
---

Fix JSON values in relation includes, historical aggregate reads, nested relation pagination, SQL NULL joins, and typed scalar filters. Improve scoped query availability and identity admission retries, reject ambiguous schema and aggregate names, and roll back failed query preparation safely.

Stabilize browser workers, Inspector connections, and Svelte/Vue provider lifecycles; bound prepared-query caches and clean up interrupted native uploads. Restore WASM tracing, preserve native account decoding and schema defaults, and provide actionable diagnostics for removed Jazz Classic APIs. Harden streamed JSON validation and JWKS transport, reject unsupported session-claim ranges, and fix application-scoped server storage and starter app-name parsing.
