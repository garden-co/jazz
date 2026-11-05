---
"jazz-tools": patch
---

Stop a durable replica from reusing transaction ids after its file reopens without commits it already uploaded (a browser tab or worker killed before IndexedDB persisted them, power loss, or a restored backup). The authority deduplicated the new commit as a resend and the write was silently lost; file-backed replicas now number their commits no lower than their wall clock.
