---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Adopt the namespaced TypeScript schema API (`import { schema as s } from "jazz-tools"`), split current `permissions.ts` from the structural schema and migration lifecycle, and rename the local schema preflight command to `jazz-tools validate`.

Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.
