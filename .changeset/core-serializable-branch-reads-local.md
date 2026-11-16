---
"jazz-tools": patch
---

**Core: serializable transactions read through branch views, in a store without a server** ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). A serializable read with a branch view records the rows of its view: the head's, or the base's rows it inherits. Its commit conflicts when any of them changed, including when a head row now hides a base row. Writes to other branches don't conflict, and neither do later writes to a frozen base. Committing such a transaction to a server is refused with "not supported yet" until the sync protocol carries the view.
