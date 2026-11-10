---
"jazz-tools": patch
---

Store a pending upload a row per op instead of as one encoded message, so a
store with a small value limit (a Durable Object's 2 MiB) can keep commits
that travel in several sync frames. Refused commits kept for retry store
their ops the same way. This changes the unshipped SQLite store format: a
build from before it cannot open a store written after it, and the other
way around.
