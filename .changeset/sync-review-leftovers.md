---
"jazz-tools": patch
---

A serializable commit refused before it is sent is reported as serializable. A server that cannot send to a session now records why for its host before closing it. Pending uploads stored in another layout are refused as an unsupported storage format, and a settled upload's stored rows are removed in one statement.
