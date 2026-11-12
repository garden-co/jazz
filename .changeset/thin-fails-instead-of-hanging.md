---
"jazz-tools": patch
---

A thin client whose server can't serve its schema (a bundle built against a schema that isn't deployed) now fails its reads and writes with the server's error instead of leaving them pending forever. A `db.all()` or `db.one()` that was running on the thin client when the session upgraded to local-first now gets its answer from the local-first client instead of rejecting with "the thin runtime is closed". Other one-shot calls cut off by the upgrade still reject.
