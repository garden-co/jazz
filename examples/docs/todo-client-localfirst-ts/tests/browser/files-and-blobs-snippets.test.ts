import { describe, expect, it, vi } from "vitest";
import type { Db } from "jazz-tools";
import { app } from "../../schema/app.js";
import { fileBlobPermissions } from "../../schema/files-and-blobs-permissions.js";
import {
  createUploadFromBlob,
  createUploadFromStream,
  deleteUploadWithFile,
  loadUploadBlob,
  loadUploadStream,
} from "../../src/files-and-blobs-snippets.js";

function makeStream(text: string) {
  const bytes = new TextEncoder().encode(text);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

function makeDb() {
  return {
    createFileFromBlob: vi.fn(async () => ({ id: "file-blob-1" })),
    createFileFromStream: vi.fn(async () => ({ id: "file-stream-1" })),
    insertDurable: vi.fn(async (_table, data) => ({ id: "upload-1", ...data })),
    one: vi.fn(),
    loadFileAsBlob: vi.fn(async () => new Blob(["downloaded"], { type: "text/plain" })),
    loadFileAsStream: vi.fn(async () => makeStream("downloaded")),
    delete: vi.fn(),
  };
}

describe("files and blobs docs snippets", () => {
  it("creates an upload from a Blob via the Db file helper", async () => {
    const db = makeDb();
    const file = new File(["hello"], "hello.txt", { type: "text/plain" });

    await createUploadFromBlob(db as unknown as Db, file);

    expect(db.createFileFromBlob).toHaveBeenCalledWith(app, file, { tier: "edge" });
    expect(db.insertDurable).toHaveBeenCalledWith(
      app.uploads,
      {
        ownerId: "local:example-owner",
        label: "Profile photo",
        fileId: "file-blob-1",
      },
      { tier: "edge" },
    );
  });

  it("creates an upload from a stream via the Db file helper", async () => {
    const db = makeDb();
    const stream = makeStream("camera");

    await createUploadFromStream(db as unknown as Db, stream);

    expect(db.createFileFromStream).toHaveBeenCalledWith(app, stream, {
      tier: "edge",
      name: "camera.raw",
      mimeType: "application/octet-stream",
    });
    expect(db.insertDurable).toHaveBeenCalledWith(
      app.uploads,
      {
        ownerId: "local:example-owner",
        label: "Camera import",
        fileId: "file-stream-1",
      },
      { tier: "edge" },
    );
  });

  it("loads an uploaded file as a Blob", async () => {
    const db = makeDb();
    db.one.mockResolvedValueOnce({
      id: "upload-1",
      ownerId: "local:example-owner",
      label: "x",
      fileId: "file-1",
    });

    const blob = await loadUploadBlob(db as unknown as Db, "upload-1");

    expect(db.loadFileAsBlob).toHaveBeenCalledWith(app, "file-1", { tier: "edge" });
    expect(blob).toBeInstanceOf(Blob);
  });

  it("loads an uploaded file as a stream", async () => {
    const db = makeDb();
    db.one.mockResolvedValueOnce({
      id: "upload-1",
      ownerId: "local:example-owner",
      label: "x",
      fileId: "file-1",
    });

    const stream = await loadUploadStream(db as unknown as Db, "upload-1");

    expect(db.loadFileAsStream).toHaveBeenCalledWith(app, "file-1", { tier: "edge" });
    expect(stream).toBeInstanceOf(ReadableStream);
  });

  it("deletes file parts, then the file, then the parent upload", async () => {
    const db = makeDb();
    db.one
      .mockResolvedValueOnce({
        id: "upload-1",
        ownerId: "local:example-owner",
        label: "x",
        fileId: "file-1",
      })
      .mockResolvedValueOnce({
        id: "file-1",
        name: "hello.txt",
        mimeType: "text/plain",
        partIds: ["part-1", "part-2"],
        partSizes: [5, 5],
      });

    await deleteUploadWithFile(db as unknown as Db, "upload-1");

    expect(db.delete).toHaveBeenNthCalledWith(1, app.file_parts, "part-1");
    expect(db.delete).toHaveBeenNthCalledWith(2, app.file_parts, "part-2");
    expect(db.delete).toHaveBeenNthCalledWith(3, app.files, "file-1");
    expect(db.delete).toHaveBeenNthCalledWith(4, app.uploads, "upload-1");
  });

  it("compiles the documented file permission chain", () => {
    expect(fileBlobPermissions.files.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "uploads",
      via_column: "fileId",
    });
    expect(fileBlobPermissions.file_parts.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "files",
      via_column: "partIds",
    });
    expect(fileBlobPermissions.files.delete?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Delete",
      source_table: "uploads",
      via_column: "fileId",
    });
    expect(fileBlobPermissions.file_parts.delete?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Delete",
      source_table: "files",
      via_column: "partIds",
    });
  });
});
