---
"jazz-tools": patch
---

Use wire protocol v5 for linear row-state history. Row payloads no longer carry `parents` (the `JVRR` row blob is now version 2 and ends with each write's `base` and `counter_signs`), the retired exact-version-set declaration tag is reserved, and a subscriber's persisted watermark uses its own tag. Wire v5 keeps v4's multiple-predecessor schema publications. Wire v3 and v4 peers now fail the Hello handshake with `UnsupportedProtocolVersion`/`Never` instead of failing on their first affected message; upgrade clients, relays and Core servers together.
