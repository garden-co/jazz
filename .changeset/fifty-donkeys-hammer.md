---
"cojson-core-wasm": patch
---

Fix a rare WASM crash caused by serializing borrowed known-state data across the JS boundary while streaming state is being checked.
