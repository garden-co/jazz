---
"jazz-tools": patch
---

The Inspector no longer hides rows that hold a large value. A value whose JSON is over 32 KB (a photo, a long text, a big JSON document) is summarized in Inspector answers as `{"$large": {kind, size, preview}}`, so `query` lists its row and `get` finds it, and the data grid shows it as "‹large bytes value, 5.2 MB›" (not editable there). `get` with `column` answers the value itself (CLI: `--column`, with `--from` and `--to` for part of a bytes or text value; MCP: `column`, `from`, `to`). A row that exists is never answered as none, even when it alone is larger than an answer carries.
