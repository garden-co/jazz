---
"jazz-tools": patch
---

**Sync: serializable transactions that read through a branch view commit to a server** ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). Each read in such a commit carries its branch view, and the server validates the read through that view. A new optional wire feature, `FEATURE_SERIALIZABLE_BRANCH_READS` (`1 << 12`), negotiates this in the `Hello`, and the protocol version stays 6:
- A commit whose reads all went through the root view is encoded exactly as before.
- A commit with a branch read is made like any serializable commit, offline included. It is only sent over a link that negotiated the feature. On another link (an older server), it is refused with "not supported yet by this server" instead of being validated against the wrong rows.

Pending commits keep their reads' views across restarts, in a new `_jazz_pending_read_views` table. The Rust native transport, the native relay, and the napi and wasm `connectUpstreamWithSession` pass on the features they negotiated.
