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
import { createDb, type Db, type DbConfig } from "../../src/runtime/db.js";
import {
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";
import { getJazzServerInfo } from "./testing-server.js";
import { sleep, TestCleanup, uniqueDbName, withTimeout } from "./support.js";

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

// Keep the corrupt-open receipt deliberately small. The broader corpus above
// exercises branches and large values; this isolates storage admission from
// any schema-level query work that cannot run after an invalid epoch anyway.
const corruptionApp = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
});

describe("browser Jazz storage compatibility corpus", () => {
  // `driver.dbName` is a caller-selected logical base. Browser persistence
  // deliberately derives a distinct physical root for the complete auth
  // scope, so raw compatibility inspection and cleanup must use the resolved
  // root that the public Db actually opened.
  const databaseNames = new Set<string>();
  const openDbs: Db[] = [];

  afterEach(async () => {
    await Promise.all(
      openDbs
        .splice(0)
        .reverse()
        .map((db) => db.shutdown()),
    );
    await Promise.all([...databaseNames].map((name) => IndexedDbPageStore.destroy(name)));
    databaseNames.clear();
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
    const config = persistentConfig(dbName, secret, server);
    let db = await openPersistentDb(config);
    const physicalDbName = await trackPhysicalDatabase(dbName);
    const initialFixture = await db.transaction((tx) => {
      const project = tx.insert(app.projects, { name: "compat project" });
      const document = tx.insert(
        app.documents,
        {
          branch: "main",
          title: "first title",
          projectId: project.id,
          body: "large value ".repeat(20_000),
        },
        { branch: "main" },
      );
      return { project, document };
    });
    await withTimeout(
      initialFixture.wait({ tier: "global" }),
      20_000,
      "initial compatibility fixture did not settle",
    );
    const { project, document } = initialFixture.value;

    const main = await withTimeout(
      db.all(app.documents, { branch: "main", tier: "edge" }),
      20_000,
      "main branch fixture did not become readable",
    );
    expect(main).toHaveLength(1);
    expect(main[0]!.id).toBe(document.id);
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
    await sleep(100);
    const rawBeforeReadOnlyInspection = await rawRecords(physicalDbName);

    db = await openPersistentDb(config);
    // Reopen must materialize the durable local replica without depending on
    // a fresh remote-coverage round trip. The earlier edge read proves the
    // synced fixture; this is specifically the offline persistence boundary.
    const reopenedMain = await db.one(app.documents.where({ id: document.id }), {
      branch: "main",
      tier: "local",
    });
    const reopenedDraft = await db.one(app.documents.where({ id: document.id }), {
      branch: "draft",
      tier: "local",
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
    await sleep(100);
    expect(await rawRecords(physicalDbName)).toEqual(rawBeforeReadOnlyInspection);
  }, 90_000);

  it("rejects a corrupt durable epoch before handing a public Jazz handle to the app", async () => {
    const cleanup = new TestCleanup();
    const dbName = databaseName();
    const appId = "browser-storage-compat-corruption";
    const config = { appId, driver: { type: "persistent" as const, dbName } } satisfies DbConfig;
    const db = cleanup.track(await createDb(config));
    const physicalDbName = await trackPhysicalDatabase(dbName);
    await withTimeout(
      db
        .insert(corruptionApp.todos, { title: "corruption sentinel", done: false })
        .wait({ tier: "local" }),
      5_000,
      "corruption fixture did not settle",
    );
    await db.shutdown();
    cleanup.untrack(db);
    await sleep(100);

    await replaceManifest(physicalDbName, { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 2 });
    const rawBeforeRejectedRead = await rawRecords(physicalDbName);
    // Schema selection is lazy, but persistent construction must first obtain
    // a foreground-node lease. Its worker opens the physical root before it
    // can mint a transaction identity, making createDb the durable admission
    // boundary. Reject the exact original epoch error rather than leaking a
    // worker error or returning a handle backed by corrupt state.
    try {
      await expect(
        withTimeout(createDb(config), 5_000, "corrupt epoch open did not reject"),
      ).rejects.toThrow("Missing or invalid IndexedDB storage epoch manifest");
      expect(await rawRecords(physicalDbName)).toEqual(rawBeforeRejectedRead);
    } finally {
      await cleanup.cleanup();
    }
  }, 90_000);

  function databaseName(): string {
    const name = uniqueDbName("jazz-storage-compat");
    return name;
  }

  async function trackPhysicalDatabase(logicalBase: string): Promise<string> {
    const prefix = `${logicalBase}::jazz-browser-v1::`;
    const names = (await indexedDB.databases())
      .map((database) => database.name)
      .filter((name): name is string => typeof name === "string" && name.startsWith(prefix));
    if (names.length !== 1) {
      throw new Error(
        `expected one physical Jazz IndexedDB root for ${logicalBase}, found ${names.length}`,
      );
    }
    const [name] = names;
    if (!name) throw new Error(`missing physical Jazz IndexedDB root for ${logicalBase}`);
    databaseNames.add(name);
    return name;
  }

  function persistentConfig(
    dbName: string,
    secret: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
  ): DbConfig {
    return {
      appId: server.appId,
      serverUrl: server.serverUrl,
      secret,
      driver: { type: "persistent", dbName },
    };
  }

  async function openPersistentDb(config: DbConfig): Promise<Db> {
    const db = await createDb(config);
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
