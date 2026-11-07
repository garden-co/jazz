---
"jazz-tools": patch
---

Name each new stored column after the schema version that introduces it, as `name~` and a short hash, instead of in the order a store admits versions. An offline client now stores its schema's new columns where the server does, even when the server admitted another version adding a column of the same name first, so its pending writes no longer have to be moved when it syncs.
