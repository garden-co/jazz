import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { app } from "../../schema.js";
import { fileListQuery } from "../../src/file-list-query.js";
import { APP_ID } from "./test-constants.js";

const dbs: Db[] = [];

afterEach(async () => {
  await Promise.all(dbs.splice(0).map((db) => db.shutdown()));
});

async function openDb(label: string): Promise<Db> {
  const db = await createDb({
    appId: APP_ID,
    driver: { type: "persistent", dbName: `epic-drop-${label}-${crypto.randomUUID()}` },
  });
  dbs.push(db);
  return db;
}

describe("EpicDrop streamed upload foundation", () => {
  it("streams multiple browser chunks and exposes only selected folder metadata", async () => {
    const db = await openDb("metadata");
    const folder = db.insert(app.folders, { name: "Demos", owner_id: "anonymous" });
    const uploaded = await db.insertStreaming(app.files, {
      folder_id: folder.value.id,
      name: "set-list.wav",
      content_type: "audio/wav",
      size_bytes: 9,
      owner_id: "anonymous",
      contents: (async function* () {
        yield new Uint8Array([1, 2, 3]);
        yield new Uint8Array([4, 5]);
        yield new Uint8Array([6, 7, 8, 9]);
      })(),
    });

    const listed = await db.all(fileListQuery(folder.value.id)!);
    expect(listed).toEqual([
      {
        id: uploaded.value.id,
        name: "set-list.wav",
        content_type: "audio/wav",
        size_bytes: 9,
      },
    ]);
    // This is intentionally a projection boundary, not a promise of a typed
    // range preview. #1833 owns that public API.
    expect("contents" in listed[0]!).toBe(false);
  });

  it("does not publish a cancelled stream and permits a clean retry", async () => {
    const db = await openDb("retry");
    const folder = db.insert(app.folders, { name: "Interrupted", owner_id: "anonymous" });

    await expect(
      db.insertStreaming(app.files, {
        folder_id: folder.value.id,
        name: "partial.wav",
        content_type: "audio/wav",
        size_bytes: 64 * 1024,
        owner_id: "anonymous",
        contents: (async function* () {
          yield new Uint8Array(40 * 1024).fill(7);
          throw new Error("simulated browser stream cancellation");
        })(),
      }),
    ).rejects.toThrow("simulated browser stream cancellation");
    await expect(db.all(app.files)).resolves.toEqual([]);

    const retried = await db.insertStreaming(app.files, {
      folder_id: folder.value.id,
      name: "retry.wav",
      content_type: "audio/wav",
      size_bytes: 64 * 1024,
      owner_id: "anonymous",
      contents: (async function* () {
        yield new Uint8Array(32 * 1024).fill(3);
        yield new Uint8Array(32 * 1024).fill(5);
      })(),
    });
    await expect(db.all(fileListQuery(folder.value.id)!)).resolves.toEqual([
      {
        id: retried.value.id,
        name: "retry.wav",
        content_type: "audio/wav",
        size_bytes: 64 * 1024,
      },
    ]);
  });
});
