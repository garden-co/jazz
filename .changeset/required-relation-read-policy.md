---
"jazz-tools": patch
---

Fix required includes (`requireIncludes`) admitting a row whose required related row the reader can't see: the row came back with an empty relation. Required relations now count only related rows the reader may read and that meet their own nested requirements, so such a row is left out, as it is when the related row doesn't exist.
