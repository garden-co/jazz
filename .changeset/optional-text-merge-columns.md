---
"jazz-tools": patch
---

Text merge columns can now be optional: `s.string().optional().merge("text")` (and `"markdown"` or `"html"`) no longer throws. Concurrent additions to a `null` column are all kept, the earlier write first, as over an empty string. A write that sets the column to `null`, or a write that lands on a `null` someone else set meanwhile, is not merged: the later write wins the whole value.
