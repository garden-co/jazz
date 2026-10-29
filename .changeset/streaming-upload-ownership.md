---
"jazz-tools": patch
---

Keep streamed uploads bound to their original database, schema and target column. Reclaim abandoned uploads during normal maintenance while preserving live streams and committed values. Cleanup waits until an open transaction ends, so rollback cannot lose an abandoned upload’s reclamation request.
