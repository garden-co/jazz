---
"jazz-tools": patch
---

Store a pending upload a row per op instead of as one encoded message, so a
store with a small value limit (a Durable Object's 2 MiB) can keep commits
that travel in several sync frames. Refused commits kept for retry store
their ops the same way.
