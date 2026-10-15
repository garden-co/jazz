---
"jazz-tools": patch
---

Use wire protocol v6 for linear row-state history. Row payloads no longer carry `parents` (the `JVRR` row blob is now version 2 and ends with each write's `base` and `counter_signs`), the retired exact-version-set declaration tag is reserved, and a subscriber's persisted watermark uses its own tag. Wire v6 keeps v5's compact durability tags, Edge-free peer roles and flagless shape registration, and v4's multiple-predecessor schema publications. Wire v3, v4 and v5 peers now fail the Hello handshake with `UnsupportedProtocolVersion`/`Never` instead of failing on their first affected message; upgrade clients, relays and Core servers together.
