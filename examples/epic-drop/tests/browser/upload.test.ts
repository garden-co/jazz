import { afterEach, describe, expect, it } from "vitest";
import { createJazzClient, type Db, type JazzSession } from "jazz-tools";
import { memoryAccountStore } from "../../../testing/accounts.js";
import { app } from "../../schema.js";
import { fileTableQuery } from "../../src/file-list-query.js";
import { SCOPE, SERVER_URL } from "./test-constants.js";

const sessions: JazzSession<unknown>[] = [];

afterEach(async () => {
  await Promise.all(sessions.splice(0).map((session) => session.close()));
});

/**
 * A Db for a new device account of its own, on the local-first client:
 * synced through the app's backend, or (`serverUrl: null`) on this device only.
 */
async function openDb(label: string, serverUrl: string | null = null): Promise<Db> {
  const session = await createJazzClient({
    scope: SCOPE,
    serverUrl,
    initial: "device",
    store: memoryAccountStore(),
    client: () => import("jazz-tools/local-first"),
    driver: { type: "persistent", dbName: `epic-drop-${label}-${crypto.randomUUID()}` },
  });
  sessions.push(session);
  return session.db;
}

const openRemoteDb = (label: string) => openDb(label, SERVER_URL);

function sessionUser(db: Db): string {
  const id = db.getAuthState().session?.user;
  if (!id) throw new Error("expected local-first user session");
  return id;
}

describe("EpicDrop streamed upload foundation", () => {
  it("streams multiple browser chunks and exposes only selected folder metadata", async () => {
    const db = await openDb("metadata");
    const folder = db.insert(app.folders, { name: "Demos", owner_id: sessionUser(db) });
    const uploaded = await db.insertStreaming(app.files, {
      folder_id: folder.value.id,
      name: "set-list.wav",
      content_type: "audio/wav",
      size_bytes: 9,
      owner_id: sessionUser(db),
      contents: (async function* () {
        yield new Uint8Array([1, 2, 3]);
        yield new Uint8Array([4, 5]);
        yield new Uint8Array([6, 7, 8, 9]);
      })(),
    });

    const listed = await db.all(fileTableQuery(folder.value.id)!);
    expect(listed).toMatchObject([
      {
        id: uploaded.value.id,
        name: "set-list.wav",
        content_type: "audio/wav",
        size_bytes: 9,
      },
    ]);
    // The table's query is a projection boundary: listing a folder never
    // selects the large `contents` value.
    expect("contents" in listed[0]!).toBe(false);
    expect(listed[0]!.$updatedAt).toBeInstanceOf(Date);
  });

  it("does not publish a cancelled stream and permits a clean retry", async () => {
    const db = await openDb("retry");
    const folder = db.insert(app.folders, { name: "Interrupted", owner_id: sessionUser(db) });

    await expect(
      db.insertStreaming(app.files, {
        folder_id: folder.value.id,
        name: "partial.wav",
        content_type: "audio/wav",
        size_bytes: 64 * 1024,
        owner_id: sessionUser(db),
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
      owner_id: sessionUser(db),
      contents: (async function* () {
        yield new Uint8Array(32 * 1024).fill(3);
        yield new Uint8Array(32 * 1024).fill(5);
      })(),
    });
    await expect(db.all(fileTableQuery(folder.value.id)!)).resolves.toMatchObject([
      {
        id: retried.value.id,
        name: "retry.wav",
        content_type: "audio/wav",
        size_bytes: 64 * 1024,
      },
    ]);
  });

  it("requires both file ownership and folder authority for attach and moves", async () => {
    const alice = await openRemoteDb("alice");
    const bob = await openRemoteDb("bob");
    const aliceId = sessionUser(alice);
    const bobId = sessionUser(bob);
    const aliceFolder = alice.insert(app.folders, { name: "Alice", owner_id: aliceId });
    await aliceFolder.wait({ tier: "global" });
    const bobFolder = bob.insert(app.folders, { name: "Bob", owner_id: bobId });
    await bobFolder.wait({ tier: "global" });

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
    await aliceFile.wait({ tier: "global" });

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
        .then((write) => write.wait({ tier: "global" })),
    ).rejects.toThrow();
    await expect(
      alice
        .update(app.files, aliceFile.value.id, { folder_id: bobFolder.value.id })
        .wait({ tier: "global" }),
    ).rejects.toThrow();
    await expect(
      alice.all(fileTableQuery(aliceFolder.value.id)!, { tier: "remote" }),
    ).resolves.toMatchObject([
      {
        id: aliceFile.value.id,
        name: "owned.wav",
        content_type: "audio/wav",
        size_bytes: 3,
      },
    ]);
  });
});
