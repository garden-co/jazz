---
"jazz-tools": patch
---

Fix a self-hosted Core server in dynamic-schema mode crashing on every restart with `opened schema is absent from the durable catalogue`. This happened when the newest schema was published without a lens, or its lens bridge failed. The server now reopens with the newest schema its store already holds and never admits the unbridged schema.
