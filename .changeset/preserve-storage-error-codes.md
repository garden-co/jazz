---
"jazz-tools": patch
---

A write the server refuses because its storage failed now reports the error code `storage`, not `protocol`, including when the refusal is replayed after a reconnect. The refusal is still final: resending the same write doesn't apply it. The message is unchanged. Servers send the new code only to clients that support it, so older clients keep receiving `protocol`. A store that has recorded such a refusal can't be opened by alpha.67 or earlier.
