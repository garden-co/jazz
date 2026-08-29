/**
 * A real-Chromium, public-Jazz compatibility receipt for a durable browser
 * replica.  The companion raw-page receipt proves the adapter's epoch/page
 * framing; this file deliberately drives that adapter through `createDb`, the
 * SharedWorker, and the released WASM runtime instead.
 *
 * Keep this corpus on the canonical browser test project.  It is intentionally
 * not a second fake-IDB or MemoryPageStore harness: a durable fixture must be
 * readable by the same public path an application uses.
 */

import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { createDb, type Db } from "../../src/runtime/db.js";
import {
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";
import { getJazzServerInfo } from "./testing-server.js";
import { uniqueDbName, waitForQuery, withTimeout } from "./support.js";

const app = s.defineApp({
  projects: s.table({
    name: s.string(),
  }),
  documents: s
    .table({
      branch: s.string(),
      title: s.string(),
      projectId: s.ref("projects"),
      body: s.string(),
    })
    .branchBy("branch"),
});

const permissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.documents.allowRead.always(),
  policy.documents.allowInsert.always(),
  policy.documents.allowUpdate.always(),
  policy.documents.allowDelete.always(),
]);

describe("browser Jazz storage compatibility corpus", () => {
  const databaseNames: string[] = [];
  const openDbs: Db[] = [];

  afterEach(async () => {
    await Promise.all(
      openDbs
        .splice(0)
        .reverse()
        .map((db) => db.shutdown()),
    );
    await Promise.all(databaseNames.splice(0).map((name) => IndexedDbPageStore.destroy(name)));
  });

  it("persists a catalogue-backed current/history/branch/large-value replica through the public browser path", async () => {
    const server = await getJazzServerInfo(uniqueDbName("storage-compat-server"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app.wasmSchema,
      permissions,
    });

    const dbName = databaseName();
    const secret = generateAuthSecret();
    let db = await openPersistentDb(dbName, secret, server);
    const project = db.insert(app.projects, { name: "compat project" }).value;
    await withTimeout(
      db
        .insert(app.documents, {
          branch: "main",
          title: "first title",
          projectId: project.id,
          body: "large value ".repeat(20_000),
        })
        .wait({ tier: "global" }),
      20_000,
      "initial compatibility fixture did not settle",
    );

    const main = await waitForQuery(
      db,
      app.documents.where({ branch: "main" }),
      (rows) => rows.length === 1,
      "main branch fixture did not become readable",
      20_000,
      "edge",
    );
    const document = main[0]!;
    await withTimeout(
      db
        .update(app.documents, document.id, { title: "current title" }, { branch: "main" })
        .wait({ tier: "global" }),
      20_000,
      "current history update did not settle",
    );
    await withTimeout(
      db
        .update(
          app.documents,
          document.id,
          {
            title: "draft override",
          },
          { branch: "draft", base: "main" },
        )
        .wait({ tier: "global" }),
      20_000,
      "draft branch update did not settle",
    );

    await db.shutdown();
    openDbs.splice(openDbs.indexOf(db), 1);
    const rawBeforeReadOnlyInspection = await rawRecords(dbName);

    db = await openPersistentDb(dbName, secret, server);
    const reopenedMain = await db.one(app.documents.where({ id: document.id }), { branch: "main" });
    const reopenedDraft = await db.one(app.documents.where({ id: document.id }), {
      branch: "draft",
    });
    expect(reopenedMain).toMatchObject({
      id: document.id,
      branch: "main",
      title: "current title",
      projectId: project.id,
      body: "large value ".repeat(20_000),
    });
    expect(reopenedDraft).toMatchObject({
      id: document.id,
      branch: "draft",
      title: "draft override",
      projectId: project.id,
      body: "large value ".repeat(20_000),
    });
    await db.shutdown();
    openDbs.splice(openDbs.indexOf(db), 1);
    expect(await rawRecords(dbName)).toEqual(rawBeforeReadOnlyInspection);
  }, 90_000);

  it("rejects a corrupt durable epoch before handing a public Jazz handle to the app", async () => {
    const server = await getJazzServerInfo(uniqueDbName("storage-compat-corruption-server"));
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app.wasmSchema,
      permissions,
    });

    const dbName = databaseName();
    const secret = generateAuthSecret();
    const db = await openPersistentDb(dbName, secret, server);
    await withTimeout(
      db.insert(app.projects, { name: "corruption sentinel" }).wait({ tier: "global" }),
      20_000,
      "corruption fixture did not settle",
    );
    await db.shutdown();
    openDbs.splice(openDbs.indexOf(db), 1);

    await replaceManifest(dbName, { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 2 });
    const rawBeforeRejectedOpen = await rawRecords(dbName);
    await expect(openPersistentDb(dbName, secret, server)).rejects.toThrow(
      "Missing or invalid IndexedDB storage epoch manifest",
    );
    expect(openDbs).toHaveLength(0);
    expect(await rawRecords(dbName)).toEqual(rawBeforeRejectedOpen);
  }, 90_000);

  function databaseName(): string {
    const name = uniqueDbName("jazz-storage-compat");
    databaseNames.push(name);
    return name;
  }

  async function openPersistentDb(
    dbName: string,
    secret: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
  ): Promise<Db> {
    const db = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      secret,
      driver: { type: "persistent", dbName },
    });
    openDbs.push(db);
    return db;
  }
});

/**
 * Snapshot the production adapter's complete physical surface after it has
 * closed.  This is a deliberately raw inspection: if a future read-only open
 * rewrites a page, metadata, or manifest, the receipt exposes it rather than
 * hiding it behind a decoded logical value.
 */
async function rawRecords(name: string): Promise<Record<string, string>> {
  const database = await requestResult(indexedDB.open(name));
  const storeNames = [
    INDEXEDDB_BTREE_PAGES_STORE,
    INDEXEDDB_BTREE_METADATA_STORE,
    INDEXEDDB_STORAGE_MANIFEST_STORE,
  ];
  const transaction = database.transaction(storeNames, "readonly");
  const records = Object.fromEntries(
    await Promise.all(
      storeNames.map(async (storeName) => {
        const store = transaction.objectStore(storeName);
        const [keys, values] = await Promise.all([
          requestResult(store.getAllKeys()),
          requestResult(store.getAll()),
        ]);
        return [
          storeName,
          JSON.stringify(keys.map((key, index) => [key, serializeRawRecord(values[index])])),
        ] as const;
      }),
    ),
  );
  await transactionDone(transaction);
  database.close();
  return records;
}

async function replaceManifest(name: string, manifest: unknown): Promise<void> {
  const database = await requestResult(indexedDB.open(name));
  const transaction = database.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
  transaction
    .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
    .put(manifest, INDEXEDDB_STORAGE_MANIFEST_KEY);
  await transactionDone(transaction);
  database.close();
}

function serializeRawRecord(value: unknown): unknown {
  if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
  if (ArrayBuffer.isView(value)) {
    return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  }
  return value;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
  });
}
