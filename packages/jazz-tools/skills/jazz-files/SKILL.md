---
name: jazz-files
description: Add and troubleshoot file or blob storage in Jazz TypeScript applications. Use for image and attachment uploads, Blob or ReadableStream storage, the conventional files and file_parts tables, chunking, file permissions, upload durability, incomplete file data, downloads, object URLs, and safe deletion while automatic cascade cleanup is unavailable.
---

# Jazz Files

Implement large binary storage through Jazz's conventional chunked file tables and public `Db`
helpers. Use `s.bytes()` directly only when the value is small and should always load with its row.

## Start from the data owner

1. Read the installed `jazz-tools` version and inspect the file helper types when uncertain.
2. Locate the app row that owns or references the file, plus its schema, permissions, upload UI,
   download path, and deletion flow.
3. Check whether the project already has `files` and `file_parts` tables and preserve their exact
   established shape.
4. Decide which sync tier must contain every chunk before the product reports upload or download
   success.
5. Read [file-storage.md](references/file-storage.md) for the conventional schema, permissions,
   helper calls, error handling, and deletion order.

## Preserve the storage model

- Use the exact conventional table names `files` and `file_parts` with `mimeType`, `partIds`,
  `partSizes`, and `data` columns when calling the built-in helpers.
- Store the returned file row ID on an application-owned parent row such as an upload, message, or
  profile.
- Treat file and part rows as write-once. Replacing a file normally creates a new file and parts,
  updates the parent reference, and removes the old rows safely.
- Use `db.createFileFromBlob(...)` for `Blob` or `File` input and
  `db.createFileFromStream(...)` for binary `ReadableStream` input.
- Use `db.loadFileAsBlob(...)` when the consumer needs a complete browser `Blob`; use
  `db.loadFileAsStream(...)` for sequential consumption.

## Make access and settlement explicit

- Let the application parent row own access. Inherit file read/delete access from rows referencing
  the file, and part read/delete access from the file's `partIds` relation.
- File parts and the file row are inserted before the parent exists, so inherited insert access does
  not naturally apply. Grant narrowly considered direct inserts or upload through a trusted backend.
- Pass a write `tier` when success means all chunks have reached that tier. Pass a read tier when
  local storage may not yet contain the whole file.
- Handle `FileNotFoundError` and `IncompleteFileDataError` as different product states.

## Delete in dependency order

Until automatic cascade cleanup exists, load the parent and file while their inherited policies
still match, then delete every part, then the file row, then the parent row. Do not delete the parent
first.

When rendering browser object URLs, revoke the previous URL when the blob changes and on component
cleanup.

## Cross into adjacent work deliberately

- Load `jazz-schema-evolution` when adding or changing file tables, parent references, or permissions
  in an established app.
- Load `jazz-backend` when uploads should run with trusted backend authority.
- Load `jazz-testing` only when the requested work includes file test code.

## Verify the change

1. Test empty, single-chunk, and multi-chunk files.
2. Test upload and download at the product's required sync tier.
3. Test a missing or wrong-sized part and surface the incomplete-data state.
4. Test allowed and denied access through the parent relationship.
5. Test replacement and deletion without orphaning chunks.
6. Confirm object URLs, streams, subscriptions, and database handles are cleaned up.

## Avoid these failure modes

- Do not store large files in one `s.bytes()` column by default.
- Do not rename conventional columns while continuing to use the built-in helpers.
- Do not assume a locally complete upload is remotely complete.
- Do not grant broad file-table reads merely because direct inserts are temporarily needed.
- Do not update file or part rows as though the helper implemented mutable files.
- Do not delete the parent before inherited file cleanup.
