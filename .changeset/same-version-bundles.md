---
"jazz-tools": patch
---

Fix every session being refused with "permissions bundle … is published, but this store never admitted its write schema" after a deploy that only adds an index or changes a column default. The core now builds the new index, and serves sessions pinned to either deployment's bundle, before and after a restart; each writes with its own schema's defaults.
