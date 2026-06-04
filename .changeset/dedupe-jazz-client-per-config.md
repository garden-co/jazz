---
"jazz-tools": patch
---

Deduplicate Jazz clients per config so a single page runs one runtime per identity. Previously the Svelte and Vue bindings created an independent runtime for every `createJazzClient` call, so mounting several components for the same identity in one page produced coexisting runtimes in the shared WASM heap; abruptly tearing one down during active sync could corrupt the others' heap and surface as a `memory access out of bounds` trap. Client lifecycle now goes through a shared, refcounted, `Map`-keyed registry used by the React, Svelte, and Vue bindings (replacing the React binding's single-slot cache, which could not hold two distinct configs at once). Clients with the same config share one runtime; distinct identities (e.g. two principals on one screen) keep their own.
