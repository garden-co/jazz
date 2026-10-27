import { createHash } from "node:crypto";
import * as idb from "fake-indexeddb";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
const node = new Uint8Array(16).fill(83);
const author = testAuthorBytes("sqlite-reopen-alice");
type Db = Awaited<ReturnType<typeof loadWasmModuleForTest>>["WasmDb"]["prototype"];
type Block = { path: string; offset: number; data: Uint8Array };
const opened = new Set<Db>();
const paths = new Set<string>();

beforeAll(() => {
  for (const [name, value] of Object.entries(idb))
    if (name.startsWith("IDB")) vi.stubGlobal(name, value);
  vi.stubGlobal("indexedDB", new idb.IDBFactory());
});
afterAll(() => vi.unstubAllGlobals());
afterEach(async () => {
  for (const db of opened) await db.close();
  opened.clear();
  if (paths.size) {
    const { WasmDb } = await loadWasmModuleForTest();
    for (const path of paths)
      await WasmDb.destroyBrowserStorage({ backend: "indexeddb", path }, path);
    paths.clear();
  }
});

async function open(path: string, requireExisting = false) {
  paths.add(path);
  const { WasmDb } = await loadWasmModuleForTest();
  const db = await WasmDb.openBrowser(
    { backend: "indexeddb", path, requireExisting },
    encodeSchema(app.wasmSchema),
    openConfig(node, author, undefined, true),
    path,
  );
  opened.add(db);
  return db;
}

async function close(db: Db) {
  await db.close();
  opened.delete(db);
}

async function insert(db: Db, text: string) {
  const write = db.insert(
    "notes",
    encodeCellsForRow(app.wasmSchema.notes!, { text: { type: "Text", value: text } }),
  );
  try {
    await write.wait("local");
  } finally {
    write.close();
  }
}

async function texts(db: Db) {
  const runtime = NativeRuntimeAdapter.fromDb(db, app.wasmSchema, node, author, 1, true);
  const rows = await runtime.query(JSON.stringify({ table: "notes" }), null, "local-only");
  if (!Array.isArray(rows)) throw new Error("Expected query rows");
  return rows.map((row) => row.values);
}

function request<T>(value: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    value.onsuccess = () => resolve(value.result);
    value.onerror = () => reject(value.error);
  });
}

async function blocks(path: string): Promise<Block[]> {
  const db = await request(indexedDB.open("jazz-idb"));
  try {
    return await request(
      db
        .transaction("blocks")
        .objectStore("blocks")
        .getAll(idb.IDBKeyRange.bound([path, 0], [path, Infinity])),
    );
  } finally {
    db.close();
  }
}

// Simulate a different physical owner while every WASM handle is closed.
// Alter persisted records directly so the old realm's VFS cache cannot hide it.
async function replace(path: string, records: Block[]) {
  const db = await request(indexedDB.open("jazz-idb"));
  try {
    const transaction = db.transaction("blocks", "readwrite");
    const committed = new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error);
    });
    const store = transaction.objectStore("blocks");
    store.delete(idb.IDBKeyRange.bound([path, 0], [path, Infinity]));
    for (const block of records) store.put({ ...block, path, data: new Uint8Array(block.data) });
    await committed;
  } finally {
    db.close();
  }
}

function digests(records: Block[]) {
  return records.map(({ offset, data }) => ({
    offset,
    bytes: data.byteLength,
    sha256: createHash("sha256").update(data).digest("hex"),
  }));
}

describe.skipIf(!hasJazzWasmBuild())("SQLite browser file reopening", () => {
  it("retires retained views and transports before the next owner opens the file", async () => {
    const path = "/alice-retired-owner.sqlite";
    const owner = await open(path);
    const view = owner.registerSchema(encodeSchema(app.wasmSchema));
    const link = await view.connectUpstream();
    const write = view.insert(
      "notes",
      encodeCellsForRow(app.wasmSchema.notes!, {
        text: { type: "Text", value: "before handoff" },
      }),
    );
    try {
      await write.wait("local");
      // Populate SQLite's statement cache before revoking the connection.
      expect(await texts(view)).toEqual([[{ type: "Text", value: "before handoff" }]]);
      await owner.tick();
      const global = write.wait("global").then(
        () => ({ ok: true }),
        (error: unknown) => ({ ok: false, error }),
      );
      await close(owner);
      await expect(global).resolves.toMatchObject({ ok: false });
      await expect(write.wait("local")).rejects.toBeDefined();
      expect(() =>
        view.insert(
          "notes",
          encodeCellsForRow(app.wasmSchema.notes!, {
            text: { type: "Text", value: "late write" },
          }),
        ),
      ).toThrow(/closed/);
      await expect(texts(view)).rejects.toThrow(/closed/);
      await expect(link.tick()).rejects.toBeDefined();

      const next = await open(path, true);
      expect(await texts(next)).toEqual([[{ type: "Text", value: "before handoff" }]]);
      await view.close();
      await insert(next, "after handoff");
      const current = await texts(next);
      expect(current).toHaveLength(2);
      expect(current).toEqual(
        expect.arrayContaining([
          [{ type: "Text", value: "before handoff" }],
          [{ type: "Text", value: "after handoff" }],
        ]),
      );
    } finally {
      write.close();
      link.close();
      await view.close();
    }
  });

  it("prevents an upload retained across owner retirement from publishing into the file", async () => {
    const path = "/alice-retired-upload.sqlite";
    const owner = await open(path);
    const upload = owner.beginStreamingMutation(
      "notes",
      new Uint8Array(16).fill(84),
      // The upload supplies the only column; there are no other cells.
      encodeCellsForRow({ columns: [] }, {}),
      "text",
      "insert",
      author,
    );
    try {
      await upload.push(new TextEncoder().encode("late upload"));
      await close(owner);
      const next = await open(path, true);
      await expect(upload.finish()).rejects.toBeDefined();
      expect(await texts(next)).toEqual([]);
      await insert(next, "new owner");
      await close(next);
      const reopened = await open(path, true);
      expect(await texts(reopened)).toEqual([[{ type: "Text", value: "new owner" }]]);
    } finally {
      await upload.abort();
    }
  });

  it("refuses an absent required file, while an explicit first open can create it", async () => {
    const path = "/alice-required-file.sqlite";
    await expect(open(path, true)).rejects.toMatch(/SQLite browser file is missing/);
    expect(await blocks(path)).toEqual([]);

    const created = await open(path);
    await insert(created, "created explicitly");
    await close(created);
    const reopened = await open(path, true);
    expect(await texts(reopened)).toEqual([[{ type: "Text", value: "created explicitly" }]]);
  });

  it("reads a replacement file instead of the previous owner's cached rows", async () => {
    const path = "/alice-replaced-file.sqlite";
    const replacementPath = "/alice-replacement-source.sqlite";
    const first = await open(path);
    await insert(first, "old");
    expect(await texts(first)).toEqual([[{ type: "Text", value: "old" }]]);
    await close(first);
    const replacement = await open(replacementPath);
    await insert(replacement, "new");
    await close(replacement);
    await replace(path, await blocks(replacementPath));

    const reopened = await open(path, true);
    expect(await texts(reopened)).toEqual([[{ type: "Text", value: "new" }]]);
    await close(reopened);
    const unaffected = await open(replacementPath, true);
    expect(await texts(unaffected)).toEqual([[{ type: "Text", value: "new" }]]);
  });

  it("rejects external corruption without replacing the damaged persisted bytes", async () => {
    const path = "/alice-corrupted-file.sqlite";
    const first = await open(path);
    await insert(first, "durable");
    await close(first);
    const damaged = await blocks(path);
    const header = damaged.find((block) => block.offset === 0)!;
    expect(header).toBeDefined();
    header.data.fill(0x55, 0, 16);
    await replace(path, damaged);
    const before = digests(await blocks(path));

    await expect(open(path, true)).rejects.toThrow(/file is not a database/);
    expect(digests(await blocks(path))).toEqual(before);
  });

  it("detects external deletion despite a cached image and does not recreate a required file", async () => {
    const path = "/alice-deleted-file.sqlite";
    const first = await open(path);
    await insert(first, "durable");
    await close(first);
    await replace(path, []);

    await expect(open(path, true)).rejects.toMatch(/SQLite browser file is missing/);
    expect(await blocks(path)).toEqual([]);
    const recreated = await open(path);
    expect(await texts(recreated)).toEqual([]);
  });
});
