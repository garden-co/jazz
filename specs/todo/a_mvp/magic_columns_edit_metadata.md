# Magic Columns for Edit Metadata — TODO

Automatic system columns that track edit provenance and history.

## Overview

Every row could expose virtual/magic columns containing edit metadata without the user explicitly declaring them:

- `_created_at` — timestamp of row creation
- `_updated_at` — timestamp of last modification
- `_created_by` — session/user that created the row
- `_updated_by` — session/user that last modified it
- `_edit_count` — number of edits
- `_version` — logical version counter or vector clock summary

These should be derivable from the underlying CRDT/object history without separate storage.

## Open Questions

- Which columns to support initially vs. later?
- Performance of deriving from history vs. materializing eagerly
- How do these interact with schema lenses?
- Should they be opt-in per table or always available?
