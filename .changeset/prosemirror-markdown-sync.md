---
"jazz-tools": patch
---

Add `jazz-tools/prosemirror`: keep a ProseMirror editor in step with a markdown text column. `toMarkdown` writes a document as canonical markdown with a map between editor positions and markdown offsets, and `markdownSync` with `applyMarkdown` writes local changes as whole values and applies merged values as the smallest change, keeping the cursor on its text. With `writeEvery`, writes are throttled and a merged value that arrives between writes keeps the edits not written yet.
