---
"jazz-tools": patch
---

`jazz-tools/prosemirror` adds overlays and selection presence: comments, highlights and collaborators' carets and selections are stored as text anchors and ranges and drawn as decorations, never written into the markdown. Each one follows its text through merged values, and comes back orphaned when its text is gone or drifted when it should be stored again.
