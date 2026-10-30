---
"jazz-tools": patch
---

Add `db.textAnchors()`: capture cursors, selections and comments on a text value as plain `{ before, after }` anchors and `{ before, matched, after }` ranges, and find them again in later values with a confidence and an ambiguity flag. Tools and LLMs can write them from the current value alone. `jazz-tools/prosemirror` adds `captureAnchor`, `resolveAnchor`, `captureRange` and `resolveRange` for editor positions. Browser, Node and React Native runtimes support them.
