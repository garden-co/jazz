---
"jazz-tools": patch
---

Add opt-in text merging for string columns: `s.string().merge("text")`, `.merge("markdown")` or `.merge("html")`. The column still stores a plain string (large values included), and concurrent edits to it merge instead of the last write winning. Disjoint edits all survive, an offline chain of edits merges each one over what its writer saw, and where two writes replace the same span the later one wins. A write built on an edit the server rejected is rejected too. Markdown and HTML merges keep delimiters, link targets, tags and entities whole, but are not tree-aware yet. A schema naming a merge strategy this version does not know is now refused by name.
