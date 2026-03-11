---
"jazz-tools": patch
---

Harden browser-backed local storage so reloads or crashes during active writes recover cleanly instead of leaving OPFS state unreadable.
