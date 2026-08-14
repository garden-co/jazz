import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { createDb, type Db } from "./db.js";
import {
  createConventionalStreamStorage,
  InvalidStreamDataError,
  MAX_STREAM_PART_BYTES,
  type StreamSnapshot,
} from "./stream-storage.js";

const app = s.defineApp({
  streams: s.table({
    rootId: s.string(),
    prefixBytes: s.int(),
    inlineTail: s.bytes(),
  }),
  stream_nodes: s.table({
    childIds: s.array(s.string()),
    childLengths: s.array(s.int()),
    height: s.int(),
  }),
  stream_parts: s.table({
    data: s.bytes(),
  }),
});

const databases: Db[] = [];
const adminSecret = "stream-storage-integration-admin";
const servers: LocalJazzServerHandle[] = [];

async function setup(options: { inlineTailBytes?: number; fanout?: number } = {}) {
  const appId = randomUUID();
  const server = await startLocalJazzServer({ appId, adminSecret });
  servers.push(server);
  await deploy({
    serverUrl: server.url,
    appId,
    adminSecret,
    schema: app.wasmSchema,
    permissions: {},
  });
  const db = await createDb({
    appId,
    driver: { type: "memory" },
    adminSecret,
    serverUrl: server.url,
  });
  databases.push(db);
  return { db, storage: createConventionalStreamStorage(db, app, options) };
}

afterEach(async () => {
  await Promise.all(databases.splice(0).map((db) => db.shutdown()));
  await Promise.all(servers.splice(0).map((server) => server.stop()));
});

describe("ordinary-row stream storage", () => {
  it("atomically consolidates a bounded tail and preserves old snapshots", async () => {
    const { db, storage } = await setup({ inlineTailBytes: 4, fanout: 4 });
    const stream = await storage.create({ tier: "edge" });
    const first = await storage.append(stream, Uint8Array.from([1, 2]), { waitForAuthority: true });
    await storage.append(stream.id, Uint8Array.from([3, 4]), { waitForAuthority: true });
    const consolidated = await storage.append(stream.id, Uint8Array.from([5]), {
      waitForAuthority: true,
    });
    await storage.append(stream.id, Uint8Array.from([6, 7]), { waitForAuthority: true });

    expect(Array.from(await storage.read(stream.id))).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(Array.from(await storage.readRange(stream.id, 3, 6))).toEqual([4, 5, 6]);

    // Both snapshots are complete roots. Later stream-row history is irrelevant.
    expect(Array.from(await storage.read(first))).toEqual([1, 2]);
    expect(Array.from(await storage.read(consolidated))).toEqual([1, 2, 3, 4, 5]);
    expect(consolidated.prefixBytes).toBe(5);
    expect(consolidated.inlineTail).toHaveLength(0);

    expect(await db.all(app.stream_parts.where({}), { tier: "local" })).toHaveLength(1);
    expect(await db.all(app.stream_nodes.where({}), { tier: "local" })).toHaveLength(1);
  });

  it("splits oversized consolidation into bounded immutable parts and a persistent tree", async () => {
    const { db, storage } = await setup({ inlineTailBytes: 8, fanout: 4 });
    const stream = await storage.create({ tier: "edge" });
    const input = new Uint8Array(MAX_STREAM_PART_BYTES + 3);
    input.fill(9);
    input[MAX_STREAM_PART_BYTES] = 1;
    input[MAX_STREAM_PART_BYTES + 1] = 2;
    input[MAX_STREAM_PART_BYTES + 2] = 3;

    const snapshot = await storage.append(stream, input, { waitForAuthority: true });
    const parts = await db.all(app.stream_parts.where({}), { tier: "local" });
    expect(parts.map((part) => part.data.length).sort((a, b) => a - b)).toEqual([
      3,
      MAX_STREAM_PART_BYTES,
    ]);
    expect(Array.from(await storage.readRange(snapshot, MAX_STREAM_PART_BYTES - 1))).toEqual([
      9, 1, 2, 3,
    ]);
  });

  it("copies a bounded right spine while sampled old roots remain independently readable", async () => {
    const { db, storage } = await setup({ inlineTailBytes: 0, fanout: 4 });
    const stream = await storage.create({ tier: "edge" });
    const snapshots: StreamSnapshot[] = [];
    for (let value = 0; value < 20; value += 1) {
      snapshots.push(await storage.append(stream.id, Uint8Array.of(value)));
    }

    const current = snapshots.at(-1)!;
    const root = await db.one(app.stream_nodes.where({ id: current.rootId }), { tier: "local" });
    expect(root?.height).toBeGreaterThan(0);
    expect(Array.from(await storage.read(current))).toEqual(
      Array.from({ length: 20 }, (_, index) => index),
    );
    for (const index of [0, 7, 19]) {
      expect(Array.from(await storage.read(snapshots[index]!))).toEqual(
        Array.from({ length: index + 1 }, (_, value) => value),
      );
    }
  }, 20_000);

  it("never reports two concurrent appends accepted while losing one", async () => {
    const { storage } = await setup({ inlineTailBytes: 16 });
    const stream = await storage.create({ tier: "edge" });
    const results = await Promise.allSettled([
      storage.append(stream.id, Uint8Array.of(1)),
      storage.append(stream.id, Uint8Array.of(2)),
    ]);
    const accepted = results.filter((result) => result.status === "fulfilled").length;
    const bytes = Array.from(await storage.read(stream.id));
    expect(bytes).toHaveLength(accepted);
    expect(bytes.every((value) => value === 1 || value === 2)).toBe(true);
    expect(new Set(bytes).size).toBe(bytes.length);
  });

  it("subscribes through the ordinary stream row", async () => {
    const { storage } = await setup({ inlineTailBytes: 16 });
    const stream = await storage.create({ tier: "edge" });
    const observed: StreamSnapshot[] = [];
    const appended = new Promise<void>((resolve) => {
      const unsubscribe = storage.subscribe(stream.id, (snapshot) => {
        if (!snapshot) return;
        observed.push(snapshot);
        if (snapshot.length === 3) {
          unsubscribe();
          resolve();
        }
      });
    });

    await storage.append(stream, Uint8Array.from([4, 5, 6]), { waitForAuthority: true });
    await appended;
    expect(observed.at(-1)?.inlineTail).toEqual(Uint8Array.from([4, 5, 6]));
  });

  it("detects corrupted child-length metadata before returning wrong bytes", async () => {
    const { db, storage } = await setup({ inlineTailBytes: 0, fanout: 4 });
    const stream = await storage.create({ tier: "edge" });
    const snapshot = await storage.append(stream, Uint8Array.from([1, 2, 3]), {
      waitForAuthority: true,
    });
    const root = await db.one(app.stream_nodes.where({ id: snapshot.rootId }), { tier: "local" });
    expect(root).toBeTruthy();

    await db
      .update(app.stream_nodes, snapshot.rootId, { childLengths: [2] })
      .wait({ tier: "local" });
    await expect(storage.read(snapshot)).rejects.toBeInstanceOf(InvalidStreamDataError);
  });

  it("rejects invalid ranges instead of silently truncating", async () => {
    const { storage } = await setup();
    const stream = await storage.create({ tier: "edge" });
    await storage.append(stream, Uint8Array.from([1, 2, 3]), { waitForAuthority: true });
    await expect(storage.readRange(stream.id, 2, 4)).rejects.toThrow("for 3 bytes");
  });
});
