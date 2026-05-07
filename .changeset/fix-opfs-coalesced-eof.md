---
"jazz-tools": patch
---

Fix OPFS-backed storage reads so coalesced disk reads stop at the current file length instead of reading past EOF after uncheckpointed growth.
