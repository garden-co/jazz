---
"jazz-tools": patch
---

Speed up include-subscription child edits: native bindings hand terminal-operation bytes to JavaScript as `Uint8Array`s, React Native decodes them without a per-byte round trip, and large multi-table sync streams no longer re-decode row descriptors.
