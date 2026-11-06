---
"jazz-tools": patch
---

Read browser auth secrets from storage on every get-or-create call so replacements, removals and storage failures are visible even after an earlier successful read.
