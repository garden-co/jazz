---
"jazz-tools": patch
"jazz-napi": patch
---

A thin (server-first) client no longer loses a write when its connection drops before the server answers. Before, such a write ended as `outcome_unknown`: it may or may not have been saved, and the app couldn't tell. Now each write carries the client's own id for it, and the client sends unanswered writes again, in order, once it reconnects. The server recognises a write it already made, through any server of the same core, and answers with the first outcome instead of writing it twice. So a resent update never undoes someone else's later change. An explicit `disconnect()`, large-value updates, and streams still end unanswered writes as `outcome_unknown`.
