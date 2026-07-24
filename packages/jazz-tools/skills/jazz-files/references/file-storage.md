# Chunked file storage

## Conventional schema

The built-in `Db` helpers expect exact table and column names:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  file_parts: s.table({
    data: s.bytes(),
  }),
  files: s.table({
    name: s.string().optional(),
    mimeType: s.string(),
    partIds: s.array(s.ref("file_parts")),
    partSizes: s.array(s.int()),
  }),
  uploads: s.table({
    owner_id: s.string(),
    label: s.string(),
    fileId: s.ref("files"),
  }),
};
```

`file_parts.data` contains raw bytes. `partIds` defines chunk order and `partSizes` records the
expected byte length of each corresponding part. The optional `name` is populated from `File.name`
when available; `mimeType` defaults to `application/octet-stream` when no type is supplied.

The default chunk size is 256 KiB. A part cannot exceed the 1 MiB byte-column limit. Use the helper's
`chunkSizeBytes` option rather than hand-splitting a Blob unless the application has a separate
protocol requirement.

## Parent-owned permissions

Use an application row as the access root:

```ts
export default s.definePermissions(app, ({ policy, allowedTo, session }) => {
  policy.uploads.allowRead.where({ owner_id: session.user_id });
  policy.uploads.allowInsert.where({ owner_id: session.user_id });
  policy.uploads.allowUpdate.where({ owner_id: session.user_id });
  policy.uploads.allowDelete.where({ owner_id: session.user_id });

  // The helper creates parts and the file before the parent upload exists.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});

  policy.files.allowRead.where(allowedTo.readReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.uploads, "fileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
```

Review direct inserts against the product's threat model. If untrusted clients must not create
arbitrary chunks, expose a trusted backend upload path. Do not respond by making file reads public.
Files are write-once, so file and part update grants are normally unnecessary.

## Upload

```ts
const file = await db.createFileFromBlob(app, blob, { tier: "edge" });

const upload = await db
  .insert(app.uploads, {
    owner_id: session.user_id,
    label: "Profile photo",
    fileId: file.id,
  })
  .wait({ tier: "edge" });
```

For a binary stream:

```ts
const file = await db.createFileFromStream(app, stream, {
  name: "camera.raw",
  mimeType: "application/octet-stream",
  tier: "edge",
});
```

The helper inserts every part before inserting the `files` row. A requested write tier is applied to
those inserts. If creating the parent later fails, application cleanup may be required for the
already-created file rows.

## Download and incomplete data

```ts
const blob = await db.loadFileAsBlob(app, upload.fileId, { tier: "edge" });
const stream = await db.loadFileAsStream(app, upload.fileId, { tier: "edge" });
```

Reads load the file metadata first, then query parts sequentially. Handle:

- `FileNotFoundError` when no file row is visible.
- `IncompleteFileDataError` with reason `invalid-file-record`, `missing-part`, or
  `part-size-mismatch` when metadata or chunks are incomplete.

A stronger read tier can wait for a more complete remote snapshot, but it does not repair corrupt
metadata or a genuinely missing upstream part.

For browser rendering, create and revoke object URLs within the owning component lifecycle:

```ts
const url = URL.createObjectURL(blob);
try {
  image.src = url;
} finally {
  // Revoke after the consumer no longer needs the URL, not before loading completes.
  URL.revokeObjectURL(url);
}
```

Adapt cleanup timing to the framework rather than revoking synchronously when the element still uses
the URL.

## Replacement and deletion

Files are currently immutable at the helper level. To replace one:

1. Upload a new file and wait for the required tier.
2. Update the parent reference.
3. Once the replacement is accepted, delete the old parts and file using a path whose permissions
   still authorize that old relationship.

For deletion, preserve the parent until inherited cleanup finishes:

```ts
const upload = await db.one(app.uploads.where({ id: uploadId }), { tier: "edge" });
if (!upload) return;

const file = await db.one(app.files.where({ id: upload.fileId }), { tier: "edge" });
if (file) {
  for (const partId of file.partIds) db.delete(app.file_parts, partId);
  db.delete(app.files, file.id);
}
db.delete(app.uploads, upload.id);
```

Do not delete the parent first: inherited permissions can disappear before the file and parts have
been cleaned up.
