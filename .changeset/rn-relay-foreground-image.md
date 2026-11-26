---
"jazz-rn": patch
---

React Native: a relay foreground's in-memory store opens from an image of the first fresh one, saved beside the relay's SQLite file and keyed by the engine build, the schema and every migration. Later foregrounds and later launches skip the genesis DDL and the migration ALTERs. A damaged or mismatched image is deleted and the store opens fresh, and deleting the store deletes its images.
