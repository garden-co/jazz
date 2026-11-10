---
"jazz-tools": patch
---

Text merge columns now keep an edit with its paragraph, list item or sentence when someone else moves that block at the same time, line up renumbered lists by their words, keep a trailing newline at the end, and never leave an unmatched tag in HTML columns.

Known limitation: when two people move the same section to different places at once, the section ends up once but a stray `## .` is left where it was (#4646).
