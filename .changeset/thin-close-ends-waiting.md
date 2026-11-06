---
"jazz-tools": patch
---

After the in-place upgrade to local-first (under a Jazz provider or `createJazzClient`), a read the app made on the thin client while the server was down no longer stays with the thin client, which waits for its own unanswered writes. The local-first client takes the read over. A `local-first` tier read answers from what that client has. A read at the remote tier still waits for the server, now through the local-first client.

On the thin client, a write that may have reached the server before the client stopped for good now fails as having an unknown outcome, not as rejected. A write that never reached the server fails with `ThinRequestNotSentError`. A mutation-error listener reports it with code `not_sent`, not `outcome_unknown`.

The standalone `upgradeToLocalFirst` from `jazz-tools/thin` now returns even while the thin client waits for the server to answer its writes. A read on the old thin `Db` that never reached the server fails with `ThinRequestNotSentError`, now exported from `jazz-tools/thin`, so it can be made again on the new `Db`. Shutting the new `Db` down also closes the old thin runtime.
