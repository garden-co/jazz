---
"cojson": patch
---

Fixed an issue where CoValues could remain stuck in a loading state when using persistent server peers.

Closed persistent peers are now marked unavailable after a grace timeout, and load requests are no longer considered complete when a peer replies with `KNOWN` and `header: true` but never sends content.
