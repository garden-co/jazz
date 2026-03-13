import type { Db } from "jazz-tools";
import { app } from "../schema/app.js";

const EXAMPLE_OWNER_ID = "local:example-owner";

// #region files-create-from-blob-ts
export async function createUploadFromBlob(db: Db, blob: Blob) {
  const namedBlob = blob as Blob & { name?: unknown };
  const file = await db.createFileFromBlob(app, blob, {
    tier: "edge",
    name: typeof namedBlob.name === "string" ? namedBlob.name : "upload.bin",
    mimeType: blob.type || "application/octet-stream",
  });

  return db.insertDurable(
    app.uploads,
    {
      owner_id: EXAMPLE_OWNER_ID,
      label: "Profile photo",
      file: file.id,
    },
    { tier: "edge" },
  );
}
// #endregion files-create-from-blob-ts

// #region files-create-from-stream-ts
export async function createUploadFromStream(db: Db, stream: ReadableStream<Uint8Array>) {
  const file = await db.createFileFromStream(app, stream, {
    tier: "edge",
    name: "camera.raw",
    mimeType: "application/octet-stream",
  });

  return db.insertDurable(
    app.uploads,
    {
      owner_id: EXAMPLE_OWNER_ID,
      label: "Camera import",
      file: file.id,
    },
    { tier: "edge" },
  );
}
// #endregion files-create-from-stream-ts

// #region files-load-blob-ts
export async function loadUploadBlob(db: Db, uploadId: string) {
  const upload = await db.one(app.uploads.where({ id: uploadId }), { tier: "edge" });
  if (!upload) {
    return null;
  }

  return db.loadFileAsBlob(app, upload.file, { tier: "edge" });
}
// #endregion files-load-blob-ts

// #region files-load-stream-ts
export async function loadUploadStream(db: Db, uploadId: string) {
  const upload = await db.one(app.uploads.where({ id: uploadId }), { tier: "edge" });
  if (!upload) {
    return null;
  }

  return db.loadFileAsStream(app, upload.file, { tier: "edge" });
}
// #endregion files-load-stream-ts

// #region files-delete-ts
export async function deleteUploadWithFile(db: Db, uploadId: string) {
  const upload = await db.one(app.uploads.where({ id: uploadId }), { tier: "edge" });
  if (!upload) {
    return;
  }

  const file = await db.one(app.files.where({ id: upload.file }), { tier: "edge" });

  if (file) {
    // Delete chunks and the file while the parent upload row still exists.
    for (const partId of file.parts) {
      db.delete(app.file_parts, partId);
    }
    db.delete(app.files, file.id);
  }

  db.delete(app.uploads, uploadId);
}
// #endregion files-delete-ts
