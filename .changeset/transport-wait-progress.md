---
"jazz-tools": patch
---

Keep global write waits from blocking timers and application callbacks when an
inbound frame is suspended with later frames queued. Wait for actual transport
progress while preserving frame order and authority settlement.
