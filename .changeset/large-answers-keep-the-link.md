---
"jazz-tools": patch
---

A large query answer no longer breaks the connection for good. An answer too large for one frame travels in parts, and the client read each part's marker byte as the start of a message, so a part whose next byte was 0 failed with "postcard u64 is not minimally encoded". The link then closed for good, and every later query on that database failed the same way until the process restarted (a large `offset` with `orderBy` on a 30,000-row table, on the backend's `jazz.db` and in browsers alike). The client now tells frames apart by their first byte, the way the Rust client does.
