---
"jazz-tools": patch
---

`select(...)` now accepts column names and partial selections in any order, for example `select("title", { content: { from: 0, to: 100 } })` or several range objects, and keeps every range. Selecting one column both by name and with a range, or with two ranges, now throws instead of silently picking one.
