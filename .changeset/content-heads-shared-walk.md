---
"jazz-tools": patch
---

Finding a row's current version now walks its edit history once instead of once per version, so reading or committing a row that has been edited many times (such as a counter or balance) no longer slows down quadratically with its history. Results are unchanged.
