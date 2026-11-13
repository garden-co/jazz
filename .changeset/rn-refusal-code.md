---
"jazz-tools": patch
"jazz-rn": patch
---

React Native now tells a relay's refused upstream apart by its error code, as Node and browser clients do. When the server serves another authority history than the device's store, the client reports an `AuthorityChangedError` and the link reads `{ state: "offline", reason: "authority-changed" }`, not a bare "stopped". This raises the native relay ABI to 6, so ship a new native build with this release.
