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

  it("abandons a failed streamed upload without publishing a file, then permits a clean retry", async () => {
    const db = await openDb("retry");
    const ownerId = "anonymous";
    const folder = db.insert(app.folders, { name: "Interrupted uploads", owner_id: ownerId });

    await expect(
      db.insertStreaming(app.files, {
        folder_id: folder.value.id,
        name: "partial.wav",
        content_type: "audio/wav",
        size_bytes: 64 * 1024,
        owner_id: ownerId,
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
      owner_id: ownerId,
      contents: (async function* () {
        yield new Uint8Array(32 * 1024).fill(3);
        yield new Uint8Array(32 * 1024).fill(5);
      })(),
    });
    await expect(db.all(app.files)).resolves.toEqual([
      expect.objectContaining({ id: retried.value.id, name: "retry.wav" }),
    ]);
  });

  it("converges concurrent inline file metadata mutations through public subscriptions", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const secret = generateAuthSecret();
    const writer = await openDb("metadata-writer", serverUrl, secret);
    const collaborator = await openDb("metadata-collaborator", serverUrl, secret);
    await waitFor(
      async () =>
        writer.getAuthState().authMode === "local-first" &&
        collaborator.getAuthState().authMode === "local-first"
          ? true
          : undefined,
      "both collaborators to establish local-first sessions",
    );
    const ownerId = writer.getAuthState().session?.user_id;
    expect(ownerId).toBeDefined();
    const folder = writer.insert(app.folders, { name: "Shared demos", owner_id: ownerId! });
    await folder.wait({ tier: "global" });

    const observed = new Set<string>();
    const unsubscribe = collaborator.subscribeAll(
      app.files,
      (delta) => {
        for (const file of delta.all) observed.add(file.id);
      },
      { tier: "edge" },
    );
    try {
      const [fromWriter, fromCollaborator] = await Promise.all([
        writer.insertStreaming(app.files, {
          folder_id: folder.value.id,
          name: "writer-note.txt",
          content_type: "text/plain",
          size_bytes: 3,
          owner_id: ownerId!,
          contents: (async function* () {
            yield new Uint8Array([1, 2, 3]);
          })(),
        }),
        collaborator.insertStreaming(app.files, {
          folder_id: folder.value.id,
          name: "collaborator-note.txt",
          content_type: "text/plain",
          size_bytes: 2,
          owner_id: ownerId!,
          contents: (async function* () {
            yield new Uint8Array([4, 5]);
          })(),
        }),
      ]);
      await Promise.all([
        fromWriter.wait({ tier: "global" }),
        fromCollaborator.wait({ tier: "global" }),
      ]);
      await waitFor(
        async () =>
          observed.has(fromWriter.value.id) && observed.has(fromCollaborator.value.id)
            ? true
            : undefined,
        "both concurrent file inserts to reach the subscription",
      );

      const [folderUpdate, fileUpdate] = [
        writer.update(app.folders, folder.value.id, { name: "Renamed shared demos" }),
        collaborator.update(app.files, fromWriter.value.id, { name: "writer-renamed.txt" }),
      ];
      await Promise.all([
        folderUpdate.wait({ tier: "global" }),
        fileUpdate.wait({ tier: "global" }),
      ]);

      const files = await waitFor(async () => {
        const current = await collaborator.all(app.files, { tier: "edge" });
        return current.length === 2 ? current : undefined;
      }, "the collaborator's metadata view to settle");
      expect(files.map((file) => file.name).sort()).toEqual([
        "collaborator-note.txt",
        "writer-renamed.txt",
      ]);
      await expect(collaborator.all(app.folders, { tier: "edge" })).resolves.toEqual([
        expect.objectContaining({ id: folder.value.id, name: "Renamed shared demos" }),
      ]);
    } finally {
      unsubscribe();
    }
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
