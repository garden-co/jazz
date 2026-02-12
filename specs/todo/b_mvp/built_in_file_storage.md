# Built-in File Storage: Static Files — TODO

First-class file storage using standard relational tables, typed binary columns, and content-addressed chunking. Assumes files are write-once (immutable after upload). No automatic cascade deletion yet — app developers manually delete files and file_parts when they delete parent rows.

See also: [Phase 2: cascade integration](../c_launch/file_storage_cascade_integration.md), [Phase 3: mutable files](../d_later/mutable_files_and_smart_chunking.md).

## Design

Binary data is modeled as regular rows in built-in framework-provided tables:

```sql
-- Built-in tables (framework-provided)
create table files (
  name text,
  mime text not null,
  parts uuid[] references file_parts not null,
  part_sizes integer[] not null
);

create table file_parts (
  data bytea
);

-- App schema references files via FK
create table todos (
  title text,
  done bool,
  image uuid references files
);
```

Apps reference `files` from their own tables via FK. The framework handles chunking, content addressing, and reassembly.

### Content-Addressed Part IDs

Part IDs are UUIDv5 derived from chunk content: `UUIDv5(JAZZ_FILES_NS, sha256(chunk_bytes))`. This gives:

- **Automatic deduplication** — same bytes = same UUID = same row, across all files.
- **Integrity verification** — any chunk can be verified by rehashing its content against its ID.
- **Sync efficiency** — peers that already have a chunk (from any file) skip it.

### `part_sizes` Array

`part_sizes: integer[]` parallels `parts: uuid[]` — entry `i` is the byte length of part `i`. This enables:

- **Byte-range reads** without fetching chunk data (compute offsets from cumulative sizes).
- **Total file size** from `sum(part_sizes)` without reading parts.
- **Progress reporting** during download.
- **Sparse downloads** — skip chunks outside the requested range.

### Chunking Strategy

Simple fixed-size chunking. Target chunk size ~256KB. Small files (under chunk size) are a single part. Sufficient for upload/download of static files.

## Why This Works

- **Uniform sync** — file_parts are rows, they flow through query-scoped sync like everything else. No separate binary transfer protocol.
- **Uniform permissions** — policies use the same system as all other data (see policy inheritance below).
- **Schema-visible chunking** — the parts/part_sizes arrays are explicit and queryable.
- **Dedup for free** — content-addressed chunks are never stored twice.

## New Features Required

### 1. `bytea` Column Type

Binary column for storing raw bytes inline. Max size: 1MB (enforced; aligns with max chunk size).

### 2. `uuid[]` Array References

Ordered 1:N relationships as an array column with FK semantics. Each element is a FK to a `file_parts` row. Ordering preserved.

### 3. Policy Inheritance via FK Declarations

Policies declared at the pointing side:

```
-- "files referenced from todos inherit todo's policies"
todos.image -> files : INHERIT POLICY
```

At access time for a `files` row: collect all FK declarations across all tables that point at `files` and declare policy inheritance. For each, find rows that reference this file. OR the policies together — if _any_ referencing row grants access, the file is accessible.

For `file_parts`, the chain is two hops: `todos.image → files` (inherit) + `files.parts → file_parts` (inherit). In most cases only a single table points at any given file, so the OR is trivial.

### 4. Built-in `files` + `file_parts` Schema with Helpers

- **Upload**: accept bytes + metadata, chunk into parts, content-address each chunk, create `file_parts` rows (dedup against existing), create `files` row with parts/part_sizes arrays.
- **Download/read**: reassemble parts in order, stream to caller. Support byte-range reads via part_sizes.
- **Lazy sync**: clients don't eagerly sync all file_parts — only when a query touches them (query-scoped sync handles this naturally).

## Open Questions

- How do `bytea` columns interact with lenses/schema migration?
