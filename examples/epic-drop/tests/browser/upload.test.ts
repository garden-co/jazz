import { afterEach, describe, expect, it } from "vitest";
import { createDb, generateAuthSecret, type Db } from "jazz-tools";
import { app } from "../../schema.js";
import { fileListQuery } from "../../src/file-list-query.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

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

async function openRemoteDb(label: string, secret: string): Promise<Db> {
  const db = await createDb({
    appId: APP_ID,
    serverUrl: `http://127.0.0.1:${TEST_PORT}`,
    secret,
    driver: { type: "persistent", dbName: `epic-drop-${label}-${crypto.randomUUID()}` },
  });
  dbs.push(db);
  return db;
}

function userId(db: Db): string {
  const id = db.getAuthState().session?.user_id;
  if (!id) throw new Error("expected local-first user session");
  return id;
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

  it("requires both file ownership and folder authority for attach and moves", async () => {
    const alice = await openRemoteDb("alice", generateAuthSecret());
    const bob = await openRemoteDb("bob", generateAuthSecret());
    const aliceId = userId(alice);
    const bobId = userId(bob);
    const aliceFolder = alice.insert(app.folders, { name: "Alice", owner_id: aliceId });
    await aliceFolder.wait({ tier: "edge" });
    const bobFolder = bob.insert(app.folders, { name: "Bob", owner_id: bobId });
    await bobFolder.wait({ tier: "edge" });

    const aliceFile = await alice.insertStreaming(app.files, {
      folder_id: aliceFolder.value.id,
      name: "owned.wav",
      content_type: "audio/wav",
      size_bytes: 3,
      owner_id: aliceId,
      contents: (async function* () {
        yield new Uint8Array([1, 2, 3]);
      })(),
    });
    await aliceFile.wait({ tier: "edge" });

    await expect(
      alice
        .insertStreaming(app.files, {
          folder_id: bobFolder.value.id,
          name: "forged-attach.wav",
          content_type: "audio/wav",
          size_bytes: 1,
          owner_id: aliceId,
          contents: (async function* () {
            yield new Uint8Array([9]);
          })(),
        })
        .then((write) => write.wait({ tier: "edge" })),
    ).rejects.toThrow();
    await expect(
      alice
        .update(app.files, aliceFile.value.id, { folder_id: bobFolder.value.id })
        .wait({ tier: "edge" }),
    ).rejects.toThrow();
    await expect(
      alice.all(fileListQuery(aliceFolder.value.id)!, { tier: "edge" }),
    ).resolves.toEqual([
      {
        id: aliceFile.value.id,
        name: "owned.wav",
        content_type: "audio/wav",
        size_bytes: 3,
      },
    ]);
  });
});
