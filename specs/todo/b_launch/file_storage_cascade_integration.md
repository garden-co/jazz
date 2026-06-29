# Built-in File Storage: CASCADE Integration — TODO

Integrate `ON DELETE CASCADE` with the built-in file storage table. When this lands, app developers no longer need to manually delete file rows referenced by parent rows.

Jazz's file-storage convention diverges from the historical alpha `files` +
`file_parts` model. Files are ordinary application rows whose payload lives in
a binary large-value column; `mime_type` and `data` are only naming
conventions. Permissions therefore follow normal row/column policy for that
row, rather than a special chunk/table permission model.

Depends on the future row-level cascade semantics work. Can be deferred to `c_later/` if cascade isn't ready at launch.

See also: [Mutable files and smart chunking](../c_later/mutable_files_and_smart_chunking.md).

## What Changes

The built-in schema gains `ON DELETE CASCADE` declarations:

```sql
create table files (
  name text,
  mime_type text not null,
  data bytea large value
);

-- App schema
create table todos (
  title text,
  done bool,
  image uuid references files on delete cascade
);
```

Deleting a todo cascades to its file row.

## Reference-Counted Cascade

Shared binary rows would need refcount-aware cascade behavior:

- Only soft-delete a file row when ALL live references to it are soft-deleted.
- Only hard-delete a file row when ALL references (including soft-deleted ones) are hard-deleted.

This likely needs distributed refcounting semantics (eager soft delete, authoritative hard delete) rather than a naive FK-only implementation.

## Migration from Phase 1

Apps that manually delete files can:

1. Add `ON DELETE CASCADE` to their FK declarations in schema.
2. Remove manual file deletion code.

No data migration needed — the schema change adds cascade behavior to existing FKs.
