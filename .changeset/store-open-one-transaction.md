---
"jazz-tools": patch
---

Open a store's schema in one transaction. Creating its tables, indexes and enum tags committed (and synced to disk) once per statement, so opening or deploying a schema with a few hundred tables took tens of seconds on a busy disk; it is now one commit, and a failed statement leaves no half-created schema.
