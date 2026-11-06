---
"jazz-tools": patch
---

Typing into a long text document now costs a few hundred bytes per keystroke on the wire and in history, not a copy of the document. A whole value written over a chunked one is stored as the splice that makes it, a text merge on the server is written the same way, and text columns with a `text`, `markdown` or `html` merge strategy are chunked from 4 KB instead of 64 KB. Filters and sorting do not read those columns' values over 4 KB, as for other values over 64 KB.
