---
"jazz-tools": patch
---

Skip catalogue replay from clients that are not authenticated with catalogue publish authority, avoiding harmless `CatalogueWriteDenied` sync errors.
