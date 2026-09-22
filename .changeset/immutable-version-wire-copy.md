---
"jazz-tools": patch
---

Copy immutable version-row wire bytes in bulk instead of rebuilding their fields during synchronization, preserving the existing encoded values.

[PR #2923](https://github.com/garden-co/jazz/pull/2923).
