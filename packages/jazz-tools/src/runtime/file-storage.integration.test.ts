import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { createDb, type Db } from "./db.js";
import {
  createConventionalFileStorage,
  InvalidFileDataError,
  MAX_FILE_PART_BYTES,
} from "./file-storage.js";

const app = s.defineApp({
  files: s.table({ rootId: s.string(), byteLength: s.int(), inlineBytes: s.bytes() }),
  file_nodes: s.table({
    childIds: s.array(s.string()),
    childLengths: s.array(s.int()),
    height: s.int(),
  }),
  file_parts: s.table({ data: s.bytes() }),
});
const databases: Db[] = [],
  servers: LocalJazzServerHandle[] = [];
async function setup(inlineBytes = 4) {
  const appId = randomUUID(),
    adminSecret = "ordinary-file-admin";
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
    adminSecret,
    serverUrl: server.url,
    driver: { type: "memory" },
  });
  databases.push(db);
  return { db, storage: createConventionalFileStorage(db, app, { inlineBytes, fanout: 2 }) };
}
afterEach(async () => {
  await Promise.all(databases.splice(0).map((db) => db.shutdown()));
  await Promise.all(servers.splice(0).map((server) => server.stop()));
});

describe("ordinary-row files", () => {
  it("keeps a complete historical root through append, overwrite, and insert", async () => {
    const { storage } = await setup();
    const file = await storage.create({ tier: "edge" });
    const initial = await storage.append(file, Uint8Array.from([1, 2, 3, 4, 5]));
    await storage.overwrite(file, 1, Uint8Array.of(9, 8));
    await storage.insert(file, 3, Uint8Array.of(7));
    expect(Array.from(await storage.read(file))).toEqual([1, 9, 8, 7, 4, 5]);
    expect(Array.from(await storage.readRange(file, 2, 5))).toEqual([8, 7, 4]);
    expect(Array.from(await storage.read(initial))).toEqual([1, 2, 3, 4, 5]);
  });
  it("uses bounded immutable parts and a fanout tree", async () => {
    const { db, storage } = await setup(0);
    const file = await storage.create({ tier: "edge" });
    const bytes = new Uint8Array(MAX_FILE_PART_BYTES * 2 + 1);
    bytes.fill(3);
    const saved = await storage.append(file, bytes);
    const parts = await db.all(app.file_parts.where({}), { tier: "local" });
    expect(parts.map((part) => part.data.length).sort((a, b) => a - b)).toEqual([
      1,
      MAX_FILE_PART_BYTES,
      MAX_FILE_PART_BYTES,
    ]);
    expect(
      (await db.one(app.file_nodes.where({ id: saved.rootId }), { tier: "local" }))?.height,
    ).toBeGreaterThan(0);
  });
  it("rejects corruption before returning a range and never advances a corrupt head", async () => {
    const { db, storage } = await setup(0);
    const file = await storage.create({ tier: "edge" });
    const saved = await storage.append(file, Uint8Array.from([1, 2, 3, 4, 5]));
    await db.update(app.file_nodes, saved.rootId, { childLengths: [999] }).wait({ tier: "local" });
    await expect(storage.read(saved)).rejects.toBeInstanceOf(InvalidFileDataError);
    await expect(storage.append(file, Uint8Array.of(6))).rejects.toBeInstanceOf(
      InvalidFileDataError,
    );
  });
  it("serializes concurrent writers without accepting a lost update", async () => {
    const { storage } = await setup();
    const file = await storage.create({ tier: "edge" });
    const results = await Promise.allSettled([
      storage.append(file, Uint8Array.of(1)),
      storage.append(file, Uint8Array.of(2)),
    ]);
    const bytes = await storage.read(file);
    expect(bytes.length).toBe(results.filter((x) => x.status === "fulfilled").length);
  });
});
