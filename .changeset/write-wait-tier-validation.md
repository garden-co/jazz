---
"jazz-tools": patch
---

Reject an unknown `wait({ tier })` value (for example a typo from plain JavaScript) with a clear `TypeError` that says the write was already applied, instead of a native "unknown durability tier" rejection that looks like a rejected write.
