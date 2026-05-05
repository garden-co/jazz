---
"jazz-tools": patch
---

`jazz-wasm` and `jazz-rn` are now optional peer dependencies of `jazz-tools` instead of regular dependencies. Consumers must add the runtime that matches their target — `jazz-wasm` for web/Node apps, `jazz-rn` for React Native/Expo apps — to their own `package.json`. If the matching peer is missing, `loadWasmModule` / `loadJazzRn` and the Vite/SvelteKit/Next dev plugins now surface a clear install hint instead of a generic "Failed to resolve import".
