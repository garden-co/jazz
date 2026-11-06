---
"jazz-tools": patch
---

A read the app made on the thin client while the server was down no longer hangs once the app upgrades to local-first: the local-first client answers it. A write the server may have taken before the thin client stopped for good now fails with `ThinConnectionLostError` (its outcome unknown), not as rejected. The standalone `upgradeToLocalFirst` from `jazz-tools/thin` returns even while the thin client still waits for the server to answer its writes.
