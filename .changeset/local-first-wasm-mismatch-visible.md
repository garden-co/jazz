---
"jazz-tools": patch
---

A browser session that can't open its local-first client now says why instead of quietly running thin. When the loaded `.wasm` doesn't match this jazz-tools build (a stale asset, a cache, mixed package versions), the console warning names the WASM fingerprint mismatch with both fingerprints and what to do. The session snapshot also gets a new `localFirstUnavailable` field (`{ reason: "wasm-mismatch" | "load-failed", message, expectedFingerprint?, actualFingerprint?, error }`), cleared once an upgrade succeeds. The fingerprint check now throws `NativeArtifactMismatchError`, which is exported.
