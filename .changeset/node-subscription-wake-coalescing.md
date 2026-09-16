---
"jazz-napi": patch
---

Coalesce queued Node subscription-startup wake callbacks to avoid redundant native ticks while preserving synchronous local write echoes.
