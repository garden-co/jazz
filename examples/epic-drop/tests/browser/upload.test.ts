import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "jazz-tools";
import { app } from "../../schema.js";

let db: Db | undefined;

afterEach(async () => {
  await db?.shutdown();
  db = undefined;
});

describe("EpicDrop upload", () => {
  it("creates a bytes file from multiple chunks and lists its metadata", async () => {
    db = await createDb({
      appId: "epic-drop-upload-test",
      driver: { type: "persistent", dbName: `epic-drop-${crypto.randomUUID()}` },
    });

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

    await expect(db.all(app.files)).resolves.toEqual([
      expect.objectContaining({
        id: uploaded.value.id,
        folder_id: folder.value.id,
        name: "set-list.wav",
        content_type: "audio/wav",
        size_bytes: 9,
        owner_id: "anonymous",
      }),
    ]);
  });
});
