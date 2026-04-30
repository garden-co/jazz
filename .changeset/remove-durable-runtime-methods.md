---
"jazz-tools": patch
---

Remove the unused `insertDurable` / `insertDurableWithSession` / `updateDurable` / `updateDurableWithSession` / `deleteDurable` / `deleteDurableWithSession` methods from the `Runtime` interface and from the jazz-napi, jazz-wasm, and React Native runtime adapters. These were superseded by the `insert(...).wait({ tier })` / `update(...).wait({ tier })` / `delete(...).wait({ tier })` API and had no remaining callers.
