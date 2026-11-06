---
"jazz-tools": patch
---

After the in-place upgrade to local-first (under a Jazz provider or `createJazzClient`), a read the app made on the thin client while the server was down no longer waits for that server. The local-first client answers it from what it has, even while the thin client still holds unanswered writes. A write the server may have taken before the thin client stopped for good now fails as of unknown outcome, not as rejected. A write that never reached the server fails as not sent. The standalone `upgradeToLocalFirst` from `jazz-tools/thin` now returns even while the thin client waits for the server to answer its writes. A read on the old thin `Db` that never reached the server fails with `ThinRequestNotSentError`, so it can be made again on the new `Db`. Shutting the new `Db` down also closes the old thin runtime.
