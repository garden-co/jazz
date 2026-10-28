---
"jazz-tools": patch
---

Store a pending upload larger than 1 MiB in 1 MiB parts, so a store with a
small value limit (a Durable Object's 2 MiB) can keep commits that travel in
several sync frames. This changes the unshipped SQLite store format: a build
from before it fails with a storage error on a store that keeps such a
commit.
