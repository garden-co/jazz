---
"jazz-tools": patch
---

Fix the React Native build: shared session code no longer imports the generated WASM/NAPI fingerprints (which React Native builds don't emit) to recognise a WASM mismatch. `NativeArtifactMismatchError` now lives in its own dependency-free module.
