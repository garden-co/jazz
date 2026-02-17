# Built-in File Storage: CASCADE Integration — TODO

Integrate `ON DELETE CASCADE` with the built-in file storage tables. When this lands, app developers no longer need to manually delete files and file_parts — deletion cascades automatically from parent rows.

Depends on: [on_delete_cascade.md](../a_mvp/on_delete_cascade.md). Can be deferred to `c_later/` if cascade isn't ready at launch.

See also: [Phase 1: static files](../a_mvp/built_in_file_storage.md), [Phase 3: mutable files](../c_later/mutable_files_and_smart_chunking.md).

## What Changes

The built-in schema gains `ON DELETE CASCADE` declarations:

```sql
create table files (
  name text,
  mime text not null,
  parts uuid[] references file_parts not null on delete cascade,
  part_sizes integer[] not null
);

create table file_parts (
  data bytea
);

-- App schema
create table todos (
  title text,
  done bool,
  image uuid references files on delete cascade
);
```

Deleting a todo cascades to its file, which cascades to its file_parts.

## Reference-Counted Cascade

Content-addressed parts can be shared across multiple files (same bytes = same UUIDv5 = same row). Cascade must be refcount-aware:

- Only soft-delete a part when ALL live references to it are soft-deleted.
- Only hard-delete a part when ALL references (including soft-deleted ones) are hard-deleted.

See `on_delete_cascade.md` for distributed refcounting semantics (eager soft delete, authoritative hard delete).

## Migration from Phase 1

Apps that manually delete files can:

1. Add `ON DELETE CASCADE` to their FK declarations in schema.
2. Remove manual file/file_parts deletion code.

No data migration needed — the schema change adds cascade behavior to existing FKs.
