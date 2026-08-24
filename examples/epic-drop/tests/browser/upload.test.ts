import { afterEach, describe, expect, it } from "vitest";
import { createDb, generateAuthSecret, type Db } from "jazz-tools";
import { app } from "../../schema.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

const dbs: Db[] = [];

afterEach(async () => {
  await Promise.all(dbs.splice(0).map((db) => db.shutdown()));
});

async function openDb(label: string, serverUrl?: string, secret?: string): Promise<Db> {
  const db = await createDb({
    appId: APP_ID,
    driver: { type: "persistent", dbName: `epic-drop-${label}-${crypto.randomUUID()}` },
    serverUrl,
    secret,
  });
  dbs.push(db);
  return db;
}

async function waitFor<T>(check: () => Promise<T | undefined>, description: string): Promise<T> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const value = await check();
    if (value !== undefined) return value;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

describe("EpicDrop upload", () => {
  it("creates a bytes file from multiple chunks and lists its metadata", async () => {
    const db = await openDb("local");

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

  it("streams a large upload through edge A and converges at a peer edge subscription", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const secret = generateAuthSecret();
    const writer = await openDb("edge-a", serverUrl, secret);
    const reader = await openDb("edge-b", serverUrl, secret);
    await waitFor(
      async () =>
        writer.getAuthState().authMode === "local-first" &&
        reader.getAuthState().authMode === "local-first"
          ? true
          : undefined,
      "both app instances to establish local-first sessions",
    );
    await expect(reader.all(app.files, { tier: "edge" })).resolves.toEqual([]);
    const ownerId = writer.getAuthState().session?.user_id;
    expect(ownerId).toBeDefined();
    const observed: string[] = [];
    const unsubscribe = reader.subscribeAll(
      app.files,
      (delta) => {
        observed.splice(0, observed.length, ...delta.all.map((file) => file.id));
      },
      { tier: "edge" },
    );

    try {
      const folder = writer.insert(app.folders, {
        name: "Tour recordings",
        owner_id: ownerId!,
      });
      await folder.wait({ tier: "global" });
      const expected = new Uint8Array(96 * 1024 + 19);
      for (let index = 0; index < expected.length; index += 1) expected[index] = index % 251;
      const uploaded = await writer.insertStreaming(app.files, {
        folder_id: folder.value.id,
        name: "live-set.wav",
        content_type: "audio/wav",
        size_bytes: expected.length,
        owner_id: ownerId!,
        contents: (async function* () {
          yield expected.subarray(0, 7);
          yield expected.subarray(7, 32 * 1024 + 5);
          yield expected.subarray(32 * 1024 + 5);
        })(),
      });
      await uploaded.wait({ tier: "global" });

      await waitFor(
        async () => (observed.includes(uploaded.value.id) ? uploaded.value.id : undefined),
        "the peer-edge subscription to receive the streamed file",
      );
      const peerFile = await waitFor(async () => {
        const files = await reader.all(app.files, { tier: "edge" });
        return files.find((file) => file.id === uploaded.value.id);
      }, "the peer edge to materialize the streamed bytes");
      expect(peerFile.contents).toEqual(expected);
    } finally {
      unsubscribe();
    }
  });

  it.skip("reads a bounded file range through the public typed Db API (#1833)", () => {
    // #1833 adds the bytes-only range API. Keep this app-level contract visible
    // rather than reaching into the private JazzClient runtime from an example.
  });
});
